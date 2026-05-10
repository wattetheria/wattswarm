use crate::control::open_node;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::startup_config::core_agent_executor_name;
use crate::swarm_dashboard_engine::{SwarmDashboardState, build_dashboard_state, tick_real_swarm};
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub(crate) struct SwarmTickRequest {
    executor: Option<String>,
    profile: Option<String>,
}
pub(crate) async fn swarm_state(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let dashboard = run_blocking(move || -> Result<SwarmDashboardState> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        build_dashboard_state(&node)
    })
    .await?;
    Ok(Json(json!({"ok": true, "state": dashboard})))
}

pub(crate) async fn swarm_tick(
    State(state): State<UiServerState>,
    Json(req): Json<SwarmTickRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let executor = req
        .executor
        .unwrap_or_else(|| core_agent_executor_name().to_owned());
    let profile = req.profile.unwrap_or_else(|| "default".to_owned());
    let dashboard = tokio::task::spawn_blocking(move || -> Result<SwarmDashboardState> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        tick_real_swarm(&mut node, &state_clone.state_dir, &executor, &profile)
    })
    .await
    .map_err(|err| anyhow!("task join error: {err}"))??;
    Ok(Json(json!({"ok": true, "state": dashboard})))
}
