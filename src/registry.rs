use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum ServiceState {
    /// Config present, waiting for client connection
    Registered,
    /// Control channel established, listener bound
    Active,
    /// Was active, client disconnected
    Disconnected,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub name: String,
    pub bind_addr: String,
    pub service_type: String,
    pub state: ServiceState,
    #[serde(skip)]
    pub connected_since: Option<Instant>,
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
}

/// Tracks the runtime state of all services.
pub struct ServiceRegistry {
    services: RwLock<HashMap<String, ServiceInfo>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        ServiceRegistry {
            services: RwLock::new(HashMap::new()),
        }
    }

    /// Register a service as known but not yet connected.
    pub async fn register(&self, name: String, bind_addr: String, service_type: String) {
        let mut map = self.services.write().await;
        map.insert(
            name.clone(),
            ServiceInfo {
                name,
                bind_addr,
                service_type,
                state: ServiceState::Registered,
                connected_since: None,
                last_heartbeat: None,
            },
        );
    }

    /// Mark a service as active (client connected).
    pub async fn set_active(&self, name: &str) {
        let mut map = self.services.write().await;
        if let Some(info) = map.get_mut(name) {
            info.state = ServiceState::Active;
            info.connected_since = Some(Instant::now());
            info.last_heartbeat = Some(Instant::now());
        }
    }

    /// Mark a service as disconnected.
    pub async fn set_disconnected(&self, name: &str) {
        let mut map = self.services.write().await;
        if let Some(info) = map.get_mut(name) {
            info.state = ServiceState::Disconnected;
            info.connected_since = None;
        }
    }

    /// Remove a service entirely.
    pub async fn unregister(&self, name: &str) {
        let mut map = self.services.write().await;
        map.remove(name);
    }

    /// Get info about a single service.
    pub async fn get(&self, name: &str) -> Option<ServiceInfo> {
        let map = self.services.read().await;
        map.get(name).cloned()
    }

    /// List all services.
    pub async fn list(&self) -> Vec<ServiceInfo> {
        let map = self.services.read().await;
        map.values().cloned().collect()
    }

    /// Update the last heartbeat timestamp.
    #[allow(dead_code)]
    pub async fn touch_heartbeat(&self, name: &str) {
        let mut map = self.services.write().await;
        if let Some(info) = map.get_mut(name) {
            info.last_heartbeat = Some(Instant::now());
        }
    }
}

/// Wrapper for sharing the registry with a drop guard that marks
/// a service as disconnected when the control channel is dropped.
pub struct RegistryGuard {
    registry: Arc<ServiceRegistry>,
    service_name: String,
}

impl RegistryGuard {
    pub fn new(registry: Arc<ServiceRegistry>, service_name: String) -> Self {
        RegistryGuard {
            registry,
            service_name,
        }
    }
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        let registry = self.registry.clone();
        let name = self.service_name.clone();
        tokio::spawn(async move {
            registry.set_disconnected(&name).await;
        });
    }
}
