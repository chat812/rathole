use crate::config::{ApiConfig, ClientServiceConfig, MaskedString, ServerServiceConfig};
use crate::config_watcher::{ClientServiceChange, ConfigChange, ServerServiceChange};
use crate::pending::{self, PendingMap};
use crate::registry::ServiceRegistry;

use anyhow::{Context, Result};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

type Body = http_body_util::Full<hyper::body::Bytes>;

fn json_response(status: StatusCode, body: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn ok_json(body: impl serde::Serialize) -> Response<Body> {
    json_response(
        StatusCode::OK,
        &serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
    )
}

fn bad_request(msg: &str) -> Response<Body> {
    json_response(
        StatusCode::BAD_REQUEST,
        &serde_json::json!({"error": msg}).to_string(),
    )
}

fn not_found() -> Response<Body> {
    json_response(
        StatusCode::NOT_FOUND,
        &serde_json::json!({"error": "not found"}).to_string(),
    )
}

fn unauthorized() -> Response<Body> {
    json_response(
        StatusCode::UNAUTHORIZED,
        &serde_json::json!({"error": "unauthorized"}).to_string(),
    )
}

/// Shared state for the API request handler.
struct ApiState {
    event_tx: mpsc::UnboundedSender<ConfigChange>,
    registry: Arc<ServiceRegistry>,
    token: Option<String>,
    is_server: bool,
    /// Default token from server/client config, used to auto-fill service tokens
    default_token: Option<MaskedString>,
    /// Allowed port range for tunnel bind_addr
    port_range: Option<(u16, u16)>,
    /// Shared pending connections map
    pending_map: PendingMap,
}

/// Extract the port from a bind address like "0.0.0.0:5022".
fn parse_bind_port(addr: &str) -> Option<u16> {
    addr.rsplit(':').next().and_then(|p| p.parse().ok())
}

/// Read the full request body as bytes.
async fn read_body(req: Request<Incoming>) -> Result<Vec<u8>> {
    use http_body_util::BodyExt;
    let body = req.collect().await?.to_bytes();
    Ok(body.to_vec())
}

/// Parse the request path into segments: /api/v1/services/foo -> ["api", "v1", "services", "foo"]
fn path_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Check bearer token authorization.
fn check_auth(req: &Request<Incoming>, expected_token: &Option<String>) -> bool {
    match expected_token {
        None => true,
        Some(token) => req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                v.strip_prefix("Bearer ")
                    .map(|t| t == token)
                    .unwrap_or(false)
            })
            .unwrap_or(false),
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<ApiState>,
) -> Result<Response<Body>, Infallible> {
    if !check_auth(&req, &state.token) {
        return Ok(unauthorized());
    }

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let segments = path_segments(&path);

    let response = match (method, segments.as_slice()) {
        // GET /api/v1/services - list all services
        (Method::GET, ["api", "v1", "services"]) => {
            let services = state.registry.list().await;
            ok_json(services)
        }

        // GET /api/v1/services/:name - get one service
        (Method::GET, ["api", "v1", "services", name]) => {
            match state.registry.get(name).await {
                Some(info) => ok_json(info),
                None => not_found(),
            }
        }

        // PUT /api/v1/services/:name - add or update a service
        (Method::PUT, ["api", "v1", "services", name]) => {
            let name = name.to_string();
            match read_body(req).await {
                Ok(body) => {
                    if state.is_server {
                        match serde_json::from_slice::<ServerServiceConfig>(&body) {
                            Ok(mut cfg) => {
                                cfg.name = name.clone();
                                // Auto-fill token from default_token if not provided
                                if cfg.token.is_none() {
                                    cfg.token = state.default_token.clone();
                                }
                                if cfg.token.is_none() {
                                    return Ok(bad_request("token is required (set in body or configure default_token)"));
                                }
                                // Validate bind port is within allowed range
                                if let Some((min, max)) = state.port_range {
                                    match parse_bind_port(&cfg.bind_addr) {
                                        Some(port) if port >= min && port <= max => {}
                                        Some(port) => {
                                            return Ok(bad_request(&format!(
                                                "bind port {} is outside allowed range {}-{}", port, min, max
                                            )));
                                        }
                                        None => {
                                            return Ok(bad_request("invalid bind_addr: cannot parse port"));
                                        }
                                    }
                                }
                                let svc_type = format!("{:?}", cfg.service_type).to_lowercase();
                                let bind_addr = cfg.bind_addr.clone();
                                let _ = state
                                    .event_tx
                                    .send(ConfigChange::ServerChange(ServerServiceChange::Add(
                                        cfg,
                                    )));
                                state
                                    .registry
                                    .register(name, bind_addr, svc_type)
                                    .await;
                                json_response(
                                    StatusCode::OK,
                                    &serde_json::json!({"status": "added"}).to_string(),
                                )
                            }
                            Err(e) => bad_request(&format!("invalid server service config: {}", e)),
                        }
                    } else {
                        match serde_json::from_slice::<ClientServiceConfig>(&body) {
                            Ok(mut cfg) => {
                                cfg.name = name.clone();
                                if cfg.token.is_none() {
                                    return Ok(bad_request("token is required"));
                                }
                                let svc_type = format!("{:?}", cfg.service_type).to_lowercase();
                                let local_addr = cfg.local_addr.clone();
                                let _ = state
                                    .event_tx
                                    .send(ConfigChange::ClientChange(ClientServiceChange::Add(
                                        cfg,
                                    )));
                                state
                                    .registry
                                    .register(name, local_addr, svc_type)
                                    .await;
                                json_response(
                                    StatusCode::OK,
                                    &serde_json::json!({"status": "added"}).to_string(),
                                )
                            }
                            Err(e) => bad_request(&format!("invalid client service config: {}", e)),
                        }
                    }
                }
                Err(e) => bad_request(&format!("failed to read body: {}", e)),
            }
        }

        // DELETE /api/v1/services/:name - remove a service
        (Method::DELETE, ["api", "v1", "services", name]) => {
            let name = name.to_string();
            if state.is_server {
                let _ = state
                    .event_tx
                    .send(ConfigChange::ServerChange(ServerServiceChange::Delete(
                        name.clone(),
                    )));
            } else {
                let _ = state
                    .event_tx
                    .send(ConfigChange::ClientChange(ClientServiceChange::Delete(
                        name.clone(),
                    )));
            }
            state.registry.unregister(&name).await;
            json_response(
                StatusCode::OK,
                &serde_json::json!({"status": "deleted"}).to_string(),
            )
        }

        // GET /api/v1/pending - list pending connections
        (Method::GET, ["api", "v1", "pending"]) => {
            let pending = pending::list(&state.pending_map).await;
            ok_json(pending)
        }

        // POST /api/v1/pending/:id/approve - approve a pending connection
        (Method::POST, ["api", "v1", "pending", id, "approve"]) => {
            match pending::approve(&state.pending_map, id).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({"status": "approved"}).to_string(),
                ),
                Err(_) => not_found(),
            }
        }

        // POST /api/v1/pending/:id/deny - deny a pending connection
        (Method::POST, ["api", "v1", "pending", id, "deny"]) => {
            match pending::deny(&state.pending_map, id).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({"status": "denied"}).to_string(),
                ),
                Err(_) => not_found(),
            }
        }

        _ => not_found(),
    };

    Ok(response)
}

/// Start the REST API server.
pub async fn start(
    config: ApiConfig,
    event_tx: mpsc::UnboundedSender<ConfigChange>,
    registry: Arc<ServiceRegistry>,
    mut shutdown_rx: broadcast::Receiver<bool>,
    is_server: bool,
    default_token: Option<MaskedString>,
    pending_map: PendingMap,
) -> Result<()> {
    let addr: SocketAddr = config
        .bind_addr
        .parse()
        .with_context(|| format!("Invalid API bind address: {}", config.bind_addr))?;

    let port_range = match (config.port_range_min, config.port_range_max) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    };

    let state = Arc::new(ApiState {
        event_tx,
        registry,
        token: config.token.map(|t| t.to_string()),
        is_server,
        default_token,
        port_range,
        pending_map,
    });

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind API server to {}", addr))?;

    info!("API server listening at {}", addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let state = state.clone();
                        let io = TokioIo::new(stream);
                        tokio::spawn(async move {
                            let service = service_fn(move |req| {
                                let state = state.clone();
                                handle_request(req, state)
                            });
                            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                                error!("API connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("API accept error: {}", e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("API server shutting down");
                break;
            }
        }
    }

    Ok(())
}
