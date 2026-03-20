use crate::config::{ApiConfig, ClientServiceConfig, MaskedString, ServerServiceConfig, ServiceType};
use crate::config_watcher::{ClientServiceChange, ConfigChange, ServerServiceChange};
use crate::pending::{self, ApprovedMap, PendingMap};
use crate::protocol;
use crate::registry::ServiceRegistry;

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
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

/// A one-time setup code for agent auto-configuration.
struct SetupCode {
    agent_id: String,
    token: String,
    remote_addr: String,
    created_at: std::time::Instant,
}

/// Map of setup_code -> SetupCode, shared and expirable.
type SetupCodeMap = Arc<RwLock<HashMap<String, SetupCode>>>;

/// Map of agent_token -> agent_id, for per-agent authorization.
type AgentTokenMap = Arc<RwLock<HashMap<String, String>>>;

/// Authentication level for API requests.
#[derive(Debug, Clone)]
enum AuthLevel {
    /// Full admin access (matches the global API token)
    Admin,
    /// Agent-scoped access (can only see/manage own resources)
    Agent(String), // agent_id
    /// No valid credentials
    Unauthorized,
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
    /// Approved IPs per service
    approved_map: ApprovedMap,
    /// One-time setup codes for agent auto-configuration
    setup_codes: SetupCodeMap,
    /// Per-agent API tokens (agent_token -> agent_id)
    agent_tokens: AgentTokenMap,
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

/// Extract bearer token from request.
fn extract_bearer(req: &Request<Incoming>) -> Option<&str> {
    req.headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Determine the authentication level for a request.
async fn check_auth(req: &Request<Incoming>, state: &ApiState) -> AuthLevel {
    let bearer = match extract_bearer(req) {
        Some(b) => b,
        None => {
            // No token required = admin access for everyone
            if state.token.is_none() {
                return AuthLevel::Admin;
            }
            return AuthLevel::Unauthorized;
        }
    };

    // Check admin token first
    if let Some(ref admin_token) = state.token {
        if bearer == admin_token {
            return AuthLevel::Admin;
        }
    }

    // Check agent tokens
    let agent_tokens = state.agent_tokens.read().await;
    if let Some(agent_id) = agent_tokens.get(bearer) {
        return AuthLevel::Agent(agent_id.clone());
    }

    AuthLevel::Unauthorized
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<ApiState>,
) -> Result<Response<Body>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let segments = path_segments(&path);

    // Allow unauthenticated access to setup code claim endpoint
    let is_setup_claim = matches!(
        (method.clone(), segments.as_slice()),
        (Method::GET, ["api", "v1", "setup", _])
    );

    let auth = if is_setup_claim {
        AuthLevel::Admin // setup claim uses the code itself as auth
    } else {
        check_auth(&req, &state).await
    };

    if matches!(auth, AuthLevel::Unauthorized) {
        return Ok(unauthorized());
    }

    let response = match (method, segments.as_slice()) {
        // GET /api/v1/services - list all services
        (Method::GET, ["api", "v1", "services"]) => {
            let services = state.registry.list().await;
            match &auth {
                AuthLevel::Admin => ok_json(services),
                AuthLevel::Agent(agent_id) => {
                    let filtered: Vec<_> = services.into_iter()
                        .filter(|s| s.agent_id.as_deref() == Some(agent_id.as_str()))
                        .collect();
                    ok_json(filtered)
                }
                _ => unreachable!(),
            }
        }

        // GET /api/v1/services/:name - get one service
        (Method::GET, ["api", "v1", "services", name]) => {
            match state.registry.get(name).await {
                Some(info) => {
                    if let AuthLevel::Agent(ref agent_id) = auth {
                        if info.agent_id.as_deref() != Some(agent_id.as_str()) {
                            return Ok(not_found());
                        }
                    }
                    ok_json(info)
                }
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
                                // Agent-scoped: force agent_id to match the token's agent
                                if let AuthLevel::Agent(ref agent_id) = auth {
                                    cfg.agent_id = Some(agent_id.clone());
                                }
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
                                let agent_id = cfg.agent_id.clone();
                                let _ = state
                                    .event_tx
                                    .send(ConfigChange::ServerChange(ServerServiceChange::Add(
                                        cfg,
                                    )));
                                state
                                    .registry
                                    .register(name, bind_addr, svc_type, agent_id)
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
                                    .register(name, local_addr, svc_type, None)
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
            // Agent-scoped: verify ownership before deleting
            if let AuthLevel::Agent(ref agent_id) = auth {
                match state.registry.get(&name).await {
                    Some(info) if info.agent_id.as_deref() == Some(agent_id.as_str()) => {}
                    _ => return Ok(unauthorized()),
                }
            }
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
            match &auth {
                AuthLevel::Admin => ok_json(pending),
                AuthLevel::Agent(agent_id) => {
                    let mut filtered = Vec::new();
                    for p in pending {
                        if let Some(info) = state.registry.get(&p.service_name).await {
                            if info.agent_id.as_deref() == Some(agent_id.as_str()) {
                                filtered.push(p);
                            }
                        }
                    }
                    ok_json(filtered)
                }
                _ => unreachable!(),
            }
        }

        // POST /api/v1/pending/:id/approve - approve a pending connection
        (Method::POST, ["api", "v1", "pending", id, "approve"]) => {
            // Agent-scoped: verify the pending connection belongs to this agent
            if let AuthLevel::Agent(ref agent_id) = auth {
                let pending = pending::list(&state.pending_map).await;
                let mut owns = false;
                for p in &pending {
                    if p.id == *id {
                        if let Some(info) = state.registry.get(&p.service_name).await {
                            owns = info.agent_id.as_deref() == Some(agent_id.as_str());
                        }
                        break;
                    }
                }
                if !owns {
                    return Ok(unauthorized());
                }
            }
            match pending::approve(&state.pending_map, &state.approved_map, id).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({"status": "approved"}).to_string(),
                ),
                Err(_) => not_found(),
            }
        }

        // POST /api/v1/pending/:id/deny - deny a pending connection
        (Method::POST, ["api", "v1", "pending", id, "deny"]) => {
            // Agent-scoped: verify the pending connection belongs to this agent
            if let AuthLevel::Agent(ref agent_id) = auth {
                let pending = pending::list(&state.pending_map).await;
                let mut owns = false;
                for p in &pending {
                    if p.id == *id {
                        if let Some(info) = state.registry.get(&p.service_name).await {
                            owns = info.agent_id.as_deref() == Some(agent_id.as_str());
                        }
                        break;
                    }
                }
                if !owns {
                    return Ok(unauthorized());
                }
            }
            match pending::deny(&state.pending_map, id).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({"status": "denied"}).to_string(),
                ),
                Err(_) => not_found(),
            }
        }

        // PUT /api/v1/agents/:agent_id - register an agent (admin only)
        (Method::PUT, ["api", "v1", "agents", agent_id]) => {
            if !matches!(auth, AuthLevel::Admin) {
                return Ok(unauthorized());
            }
            let agent_id = agent_id.to_string();
            let gw_name = protocol::agent_gateway_name(&agent_id);

            // Parse token from body (required — this becomes the agent's API token too)
            let agent_token = match read_body(req).await {
                Ok(body) if !body.is_empty() => {
                    serde_json::from_slice::<serde_json::Value>(&body)
                        .ok()
                        .and_then(|v| v.get("token").and_then(|t| t.as_str().map(String::from)))
                }
                _ => None,
            };

            let token_str = match agent_token {
                Some(t) => t,
                None => {
                    return Ok(bad_request("token is required in body"));
                }
            };

            // Store the agent token for API authorization
            state.agent_tokens.write().await.insert(token_str.clone(), agent_id.clone());

            let token = Some(MaskedString::from(token_str.as_str()));

            let cfg = ServerServiceConfig {
                service_type: ServiceType::Tcp,
                name: gw_name.clone(),
                bind_addr: String::new(),
                token,
                nodelay: None,
                local_addr: None,
                require_approval: false,
                agent_id: Some(agent_id.clone()),
            };

            let _ = state.event_tx.send(ConfigChange::ServerChange(
                ServerServiceChange::Add(cfg),
            ));
            state.registry.register(gw_name, String::new(), "gateway".to_string(), Some(agent_id.clone())).await;

            json_response(
                StatusCode::OK,
                &serde_json::json!({"status": "registered", "agent_id": agent_id}).to_string(),
            )
        }

        // DELETE /api/v1/agents/:agent_id - unregister agent + delete owned services (admin only)
        (Method::DELETE, ["api", "v1", "agents", agent_id]) => {
            if !matches!(auth, AuthLevel::Admin) {
                return Ok(unauthorized());
            }
            let agent_id = agent_id.to_string();
            let gw_name = protocol::agent_gateway_name(&agent_id);

            // Remove the gateway service
            let _ = state.event_tx.send(ConfigChange::ServerChange(
                ServerServiceChange::Delete(gw_name.clone()),
            ));
            state.registry.unregister(&gw_name).await;

            // Remove agent token
            state.agent_tokens.write().await.retain(|_, v| v != &agent_id);

            // Remove all services owned by this agent
            let all_services = state.registry.list().await;
            for svc in all_services {
                if svc.agent_id.as_deref() == Some(agent_id.as_str()) {
                    let _ = state.event_tx.send(ConfigChange::ServerChange(
                        ServerServiceChange::Delete(svc.name.clone()),
                    ));
                    state.registry.unregister(&svc.name).await;
                }
            }

            json_response(
                StatusCode::OK,
                &serde_json::json!({"status": "unregistered", "agent_id": agent_id}).to_string(),
            )
        }

        // GET /api/v1/agents - list agent gateway services (admin only)
        (Method::GET, ["api", "v1", "agents"]) => {
            if !matches!(auth, AuthLevel::Admin) {
                return Ok(unauthorized());
            }
            let all = state.registry.list().await;
            let agents: Vec<_> = all.into_iter()
                .filter(|s| protocol::is_gateway_service(&s.name) && s.name != protocol::GATEWAY_SERVICE_NAME)
                .collect();
            ok_json(agents)
        }

        // POST /api/v1/setup - create a setup code (admin only)
        (Method::POST, ["api", "v1", "setup"]) => {
            if !matches!(auth, AuthLevel::Admin) {
                return Ok(unauthorized());
            }
            match read_body(req).await {
                Ok(body) => {
                    match serde_json::from_slice::<serde_json::Value>(&body) {
                        Ok(v) => {
                            let agent_id = v.get("agent_id").and_then(|v| v.as_str());
                            let token = v.get("token").and_then(|v| v.as_str());
                            let setup_code = v.get("setup_code").and_then(|v| v.as_str());
                            let remote_addr = v.get("remote_addr").and_then(|v| v.as_str());

                            if let (Some(agent_id), Some(token), Some(code), Some(remote_addr)) =
                                (agent_id, token, setup_code, remote_addr)
                            {
                                let entry = SetupCode {
                                    agent_id: agent_id.to_string(),
                                    token: token.to_string(),
                                    remote_addr: remote_addr.to_string(),
                                    created_at: std::time::Instant::now(),
                                };
                                state.setup_codes.write().await.insert(code.to_string(), entry);
                                json_response(
                                    StatusCode::OK,
                                    &serde_json::json!({"status": "created", "setup_code": code}).to_string(),
                                )
                            } else {
                                bad_request("required fields: agent_id, token, setup_code, remote_addr")
                            }
                        }
                        Err(e) => bad_request(&format!("invalid JSON: {}", e)),
                    }
                }
                Err(e) => bad_request(&format!("failed to read body: {}", e)),
            }
        }

        // GET /api/v1/setup/:code - claim a setup code (no auth required)
        (Method::GET, ["api", "v1", "setup", code]) => {
            // Cleanup expired codes (>10 min)
            {
                let mut codes = state.setup_codes.write().await;
                codes.retain(|_, v| v.created_at.elapsed().as_secs() < 600);
            }

            let entry = state.setup_codes.write().await.remove(*code);
            match entry {
                Some(setup) => {
                    ok_json(serde_json::json!({
                        "remote_addr": setup.remote_addr,
                        "token": setup.token,
                        "agent_id": setup.agent_id,
                    }))
                }
                None => not_found(),
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
    approved_map: ApprovedMap,
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
        approved_map,
        setup_codes: Arc::new(RwLock::new(HashMap::new())),
        agent_tokens: Arc::new(RwLock::new(HashMap::new())),
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
