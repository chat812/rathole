#[cfg(feature = "api")]
mod api;
mod cli;
mod config;
mod config_watcher;
mod constants;
mod helper;
mod multi_map;
mod pending;
mod protocol;
mod registry;
mod transport;

pub use cli::Cli;
use cli::KeypairType;
pub use config::Config;
pub use constants::UDP_BUFFER_SIZE;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};

use crate::registry::ServiceRegistry;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
use client::run_client;

#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
use server::run_server;

use crate::config_watcher::{ConfigChange, ConfigWatcherHandle};

const DEFAULT_CURVE: KeypairType = KeypairType::X25519;

fn get_str_from_keypair_type(curve: KeypairType) -> &'static str {
    match curve {
        KeypairType::X25519 => "25519",
        KeypairType::X448 => "448",
    }
}

#[cfg(feature = "noise")]
fn genkey(curve: Option<KeypairType>) -> Result<()> {
    let curve = curve.unwrap_or(DEFAULT_CURVE);
    let builder = snowstorm::Builder::new(
        format!(
            "Noise_KK_{}_ChaChaPoly_BLAKE2s",
            get_str_from_keypair_type(curve)
        )
        .parse()?,
    );
    let keypair = builder.generate_keypair()?;

    println!("Private Key:\n{}\n", base64::encode(keypair.private));
    println!("Public Key:\n{}", base64::encode(keypair.public));
    Ok(())
}

#[cfg(not(feature = "noise"))]
fn genkey(curve: Option<KeypairType>) -> Result<()> {
    crate::helper::feature_not_compile("nosie")
}

pub async fn run(args: Cli, shutdown_rx: broadcast::Receiver<bool>) -> Result<()> {
    if args.genkey.is_some() {
        return genkey(args.genkey.unwrap());
    }

    #[cfg(feature = "api")]
    let args = {
        let mut args = args;
        if let Some(ref server_api_addr) = args.setup {
            let config_path = run_setup(server_api_addr, args.config_path.as_deref()).await?;
            args.config_path = Some(config_path);
            args.setup = None;
            args.client = true;
            info!("Setup complete. Starting client...");
        }
        args
    };

    // Raise `nofile` limit on linux and mac
    fdlimit::raise_fd_limit();

    // Spawn a config watcher. The watcher will send a initial signal to start the instance with a config
    let config_path = args.config_path.as_ref().unwrap();
    let mut cfg_watcher = ConfigWatcherHandle::new(config_path, shutdown_rx).await?;

    // shutdown_tx owns the instance
    let (shutdown_tx, _) = broadcast::channel(1);

    // Service registry shared across API server and instances
    let registry = Arc::new(ServiceRegistry::new());

    // Pending connections map shared across API and server
    let pending_map = pending::new_pending_map();
    let approved_map = pending::new_approved_map();

    // Channel for API-originated config changes
    let (api_event_tx, mut api_event_rx) =
        mpsc::unbounded_channel::<ConfigChange>();

    // (The join handle of the last instance, The service update channel sender)
    let mut last_instance: Option<(tokio::task::JoinHandle<_>, mpsc::Sender<ConfigChange>)> = None;

    loop {
        tokio::select! {
            e = cfg_watcher.event_rx.recv() => {
                let e = match e {
                    Some(e) => e,
                    None => break,
                };
                match e {
                    ConfigChange::General(config) => {
                        if let Some((i, _)) = last_instance.take() {
                            info!("General configuration change detected. Restarting...");
                            shutdown_tx.send(true)?;
                            i.await??;
                        }

                        debug!("{:?}", config);

                        // Start API server if configured
                        #[cfg(feature = "api")]
                        if let Some(api_cfg) = config.api.clone() {
                            let is_server = config.server.is_some();
                            let default_token = config
                                .server
                                .as_ref()
                                .and_then(|s| s.default_token.clone())
                                .or_else(|| {
                                    config
                                        .client
                                        .as_ref()
                                        .and_then(|c| c.default_token.clone())
                                });
                            let api_tx = api_event_tx.clone();
                            let api_registry = registry.clone();
                            let api_shutdown = shutdown_tx.subscribe();
                            let api_pending = pending_map.clone();
                            let api_approved = approved_map.clone();
                            tokio::spawn(async move {
                                if let Err(e) = api::start(
                                    api_cfg,
                                    api_tx,
                                    api_registry,
                                    api_shutdown,
                                    is_server,
                                    default_token,
                                    api_pending,
                                    api_approved,
                                ).await {
                                    error!("API server error: {:#}", e);
                                }
                            });
                        }

                        let (service_update_tx, service_update_rx) = mpsc::channel(1024);

                        // Extract approval config from API section
                        let approval_webhook = config.api.as_ref().and_then(|a| a.approval_webhook.clone());
                        let approval_timeout = config.api.as_ref().map(|a| a.approval_timeout).unwrap_or(60);

                        last_instance = Some((
                            tokio::spawn(run_instance(
                                *config,
                                args.clone(),
                                shutdown_tx.subscribe(),
                                service_update_rx,
                                registry.clone(),
                                pending_map.clone(),
                                approved_map.clone(),
                                approval_webhook,
                                approval_timeout,
                            )),
                            service_update_tx,
                        ));
                    }
                    ev => {
                        info!("Service change detected. {:?}", ev);
                        if let Some((_, service_update_tx)) = &last_instance {
                            let _ = service_update_tx.send(ev).await;
                        }
                    }
                }
            },
            // API-originated config changes
            e = api_event_rx.recv() => {
                if let Some(ev) = e {
                    info!("API service change: {:?}", ev);
                    if let Some((_, service_update_tx)) = &last_instance {
                        let _ = service_update_tx.send(ev).await;
                    }
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);

    Ok(())
}

async fn run_instance(
    config: Config,
    args: Cli,
    shutdown_rx: broadcast::Receiver<bool>,
    service_update: mpsc::Receiver<ConfigChange>,
    registry: Arc<ServiceRegistry>,
    pending_map: pending::PendingMap,
    approved_map: pending::ApprovedMap,
    approval_webhook: Option<String>,
    approval_timeout: u64,
) -> Result<()> {
    match determine_run_mode(&config, &args) {
        RunMode::Undetermine => panic!("Cannot determine running as a server or a client"),
        RunMode::Client => {
            #[cfg(not(feature = "client"))]
            crate::helper::feature_not_compile("client");
            #[cfg(feature = "client")]
            run_client(config, shutdown_rx, service_update, registry).await
        }
        RunMode::Server => {
            #[cfg(not(feature = "server"))]
            crate::helper::feature_not_compile("server");
            #[cfg(feature = "server")]
            run_server(config, shutdown_rx, service_update, registry, pending_map, approved_map, approval_webhook, approval_timeout).await
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
enum RunMode {
    Server,
    Client,
    Undetermine,
}

fn determine_run_mode(config: &Config, args: &Cli) -> RunMode {
    use RunMode::*;
    if args.client && args.server {
        Undetermine
    } else if args.client {
        Client
    } else if args.server {
        Server
    } else if config.client.is_some() && config.server.is_none() {
        Client
    } else if config.server.is_some() && config.client.is_none() {
        Server
    } else {
        Undetermine
    }
}

/// First-run setup: prompt for a setup code, fetch config from server, write config file.
#[cfg(feature = "api")]
async fn run_setup(server_api_addr: &str, config_path: Option<&std::path::Path>) -> Result<std::path::PathBuf> {
    use anyhow::Context;
    use std::io::{self, Write};

    let config_path = config_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("client.toml"));

    if config_path.exists() {
        anyhow::bail!(
            "Config file '{}' already exists. Delete it first to re-run setup.",
            config_path.display()
        );
    }

    print!("Enter setup code: ");
    io::stdout().flush()?;
    let mut code = String::new();
    io::stdin().read_line(&mut code)?;
    let code = code.trim();

    if code.is_empty() {
        anyhow::bail!("Setup code cannot be empty");
    }

    let url = format!("http://{}/api/v1/setup/{}", server_api_addr, code);
    info!("Fetching config from {}...", url);

    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", server_api_addr))?;

    if !resp.status().is_success() {
        anyhow::bail!("Setup code invalid or expired (server returned {})", resp.status());
    }

    let body: serde_json::Value = resp.json().await?;
    let remote_addr = body["remote_addr"].as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing remote_addr in response"))?;
    let token = body["token"].as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing token in response"))?;
    let agent_id = body["agent_id"].as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing agent_id in response"))?;

    let config_content = format!(
        "[client]\nremote_addr = \"{}\"\ndefault_token = \"{}\"\nagent_id = \"{}\"\n",
        remote_addr, token, agent_id
    );

    std::fs::write(&config_path, &config_content)
        .with_context(|| format!("Failed to write config to {}", config_path.display()))?;

    info!("Config written to {}", config_path.display());

    Ok(config_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_run_mode() {
        use config::*;
        use RunMode::*;

        struct T {
            cfg_s: bool,
            cfg_c: bool,
            arg_s: bool,
            arg_c: bool,
            run_mode: RunMode,
        }

        let tests = [
            T {
                cfg_s: false,
                cfg_c: false,
                arg_s: false,
                arg_c: false,
                run_mode: Undetermine,
            },
            T {
                cfg_s: true,
                cfg_c: false,
                arg_s: false,
                arg_c: false,
                run_mode: Server,
            },
            T {
                cfg_s: false,
                cfg_c: true,
                arg_s: false,
                arg_c: false,
                run_mode: Client,
            },
            T {
                cfg_s: true,
                cfg_c: true,
                arg_s: false,
                arg_c: false,
                run_mode: Undetermine,
            },
            T {
                cfg_s: true,
                cfg_c: true,
                arg_s: true,
                arg_c: false,
                run_mode: Server,
            },
            T {
                cfg_s: true,
                cfg_c: true,
                arg_s: false,
                arg_c: true,
                run_mode: Client,
            },
            T {
                cfg_s: true,
                cfg_c: true,
                arg_s: true,
                arg_c: true,
                run_mode: Undetermine,
            },
        ];

        for t in tests {
            let config = Config {
                server: match t.cfg_s {
                    true => Some(ServerConfig::default()),
                    false => None,
                },
                client: match t.cfg_c {
                    true => Some(ClientConfig::default()),
                    false => None,
                },
                api: None,
            };

            let args = Cli {
                config_path: Some(std::path::PathBuf::new()),
                server: t.arg_s,
                client: t.arg_c,
                ..Default::default()
            };

            assert_eq!(determine_run_mode(&config, &args), t.run_mode);
        }
    }
}
