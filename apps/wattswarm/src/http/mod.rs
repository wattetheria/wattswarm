use anyhow::{Result, anyhow};
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use wattswarm_network_discovery::DiscoveryRoutingTable;

pub(crate) mod background;
pub(crate) mod diagnostics;
pub(crate) mod discovery;
pub(crate) mod egress;
pub(crate) mod executors;
pub(crate) mod helpers;
pub(crate) mod network_bootstrap;
pub(crate) mod node;
pub(crate) mod pages;
pub(crate) mod peers;
pub(crate) mod runs;
pub(crate) mod server;
pub(crate) mod startup;
pub(crate) mod swarm;
pub(crate) mod tasks;
pub(crate) mod topics;

#[derive(Clone)]
pub struct UiServerState {
    pub(crate) state_dir: PathBuf,
    pub(crate) db_path: PathBuf,
    pub(crate) discovery_records: Arc<RwLock<DiscoveryRoutingTable>>,
    pub(crate) discovery_records_maintenance: Arc<Mutex<DiscoveryRecordsMaintenance>>,
}

#[derive(Debug, Default)]
pub(crate) struct DiscoveryRecordsMaintenance {
    pub(crate) last_prune_at: Option<Instant>,
    pub(crate) last_snapshot_at: Option<Instant>,
    pub(crate) snapshot_in_progress: bool,
    pub(crate) snapshot_generation: u64,
    pub(crate) persisted_generation: u64,
}

impl UiServerState {
    pub fn new(state_dir: PathBuf, db_path: PathBuf) -> Self {
        let discovery_records = match discovery::load_discovery_records(&state_dir, ui_now_ms()) {
            Ok(table) => table,
            Err(error) => {
                eprintln!("wattswarm discovery records initial load failed: {error:#}");
                DiscoveryRoutingTable::new()
            }
        };
        Self {
            state_dir,
            db_path,
            discovery_records: Arc::new(RwLock::new(discovery_records)),
            discovery_records_maintenance: Arc::new(Mutex::new(
                DiscoveryRecordsMaintenance::default(),
            )),
        }
    }
}

fn ui_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[derive(Debug)]
pub(crate) struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": self.0.to_string()
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

pub(crate) async fn run_blocking<T, F>(f: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| anyhow!("task join error: {err}"))?
        .map_err(Into::into)
}
