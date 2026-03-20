use anyhow::{anyhow, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct PendingInfo {
    pub id: String,
    pub service_name: String,
    pub visitor_addr: String,
    pub created_at: u64,
}

pub struct PendingConnection {
    pub info: PendingInfo,
    pub response_tx: oneshot::Sender<bool>,
}

pub type PendingMap = Arc<RwLock<HashMap<String, PendingConnection>>>;

pub fn new_pending_map() -> PendingMap {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Insert a new pending connection. Returns (id, receiver).
pub async fn insert(
    map: &PendingMap,
    service_name: &str,
    visitor_addr: String,
) -> (String, oneshot::Receiver<bool>) {
    let id = Uuid::new_v4().to_string();
    let (tx, rx) = oneshot::channel();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let conn = PendingConnection {
        info: PendingInfo {
            id: id.clone(),
            service_name: service_name.to_string(),
            visitor_addr,
            created_at: now,
        },
        response_tx: tx,
    };

    map.write().await.insert(id.clone(), conn);
    (id, rx)
}

/// Approve a pending connection (sends true on the oneshot).
pub async fn approve(map: &PendingMap, id: &str) -> Result<()> {
    let conn = map
        .write()
        .await
        .remove(id)
        .ok_or_else(|| anyhow!("Pending connection not found"))?;
    let _ = conn.response_tx.send(true);
    Ok(())
}

/// Deny a pending connection (sends false on the oneshot).
pub async fn deny(map: &PendingMap, id: &str) -> Result<()> {
    let conn = map
        .write()
        .await
        .remove(id)
        .ok_or_else(|| anyhow!("Pending connection not found"))?;
    let _ = conn.response_tx.send(false);
    Ok(())
}

/// List all pending connections.
pub async fn list(map: &PendingMap) -> Vec<PendingInfo> {
    map.read()
        .await
        .values()
        .map(|c| c.info.clone())
        .collect()
}

/// Remove expired pending connections. Dropping the sender signals denial.
#[allow(dead_code)]
pub async fn cleanup_expired(map: &PendingMap, timeout_secs: u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut m = map.write().await;
    m.retain(|_, conn| now - conn.info.created_at < timeout_secs);
}
