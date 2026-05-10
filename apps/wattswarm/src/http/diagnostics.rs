use crate::control::open_node;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::network_bridge::{
    DiagnosticFilter, latest_network_observability_snapshot, list_network_diagnostics,
    network_service_started,
};
use anyhow::Result;
use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub(crate) struct DiagnosticsQuery {
    limit: Option<usize>,
    level: Option<String>,
    component: Option<String>,
    category: Option<String>,
    phase: Option<String>,
    event_id: Option<String>,
    object_id: Option<String>,
    source_node_id: Option<String>,
    search: Option<String>,
}

impl From<DiagnosticsQuery> for DiagnosticFilter {
    fn from(query: DiagnosticsQuery) -> Self {
        Self {
            limit: query.limit,
            level: query.level,
            component: query.component,
            category: query.category,
            phase: query.phase,
            event_id: query.event_id,
            object_id: query.object_id,
            source_node_id: query.source_node_id,
            search: query.search,
        }
    }
}
pub(crate) async fn log_head(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let head = run_blocking(move || -> Result<u64> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.head_seq()
    })
    .await?;
    Ok(Json(json!({"ok": true, "head": head})))
}

pub(crate) async fn log_replay(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.replay_rebuild_projection()?;
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true, "message": "replayed"})))
}

pub(crate) async fn log_verify(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.verify_log()?;
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true, "message": "verified"})))
}

pub(crate) async fn diagnostics(
    State(state): State<UiServerState>,
    Query(query): Query<DiagnosticsQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let filter = DiagnosticFilter::from(query);
    let payload = run_blocking(move || -> Result<Value> {
        let diagnostics = list_network_diagnostics(&state_clone.state_dir, &filter)?;
        let snapshot = latest_network_observability_snapshot(&state_clone.state_dir);
        Ok(json!({
            "ok": true,
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "network_service_started": network_service_started(&state_clone.state_dir),
            "snapshot": snapshot,
            "diagnostics": diagnostics,
        }))
    })
    .await?;
    Ok(Json(payload))
}
