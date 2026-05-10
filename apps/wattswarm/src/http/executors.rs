use crate::control::{
    ExecutorRegistryEntry, load_executor_registry_state, save_executor_registry_state,
};
use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub(crate) struct ExecutorAddRequest {
    name: String,
    base_url: String,
    #[serde(default)]
    agent_event_callback_base_url: Option<String>,
    #[serde(default)]
    remote: bool,
    #[serde(default)]
    target_node_id: Option<String>,
    #[serde(default)]
    scope_hint: Option<String>,
    #[serde(default)]
    commit_plane_endpoint: Option<String>,
    #[serde(default)]
    commit_plane_token_file: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExecutorCheckRequest {
    name: String,
}

pub(crate) async fn executors_add(
    State(state): State<UiServerState>,
    Json(req): Json<ExecutorAddRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let mut reg = load_executor_registry_state(&state_clone.state_dir)?;
        let kind = if req.remote {
            crate::control::ExecutorKind::Remote
        } else {
            crate::control::ExecutorKind::Local
        };
        reg.entries.retain(|e| e.name != req.name);
        reg.entries.push(ExecutorRegistryEntry {
            name: req.name,
            base_url: req.base_url,
            agent_event_callback_base_url: req.agent_event_callback_base_url,
            kind,
            target_node_id: req.target_node_id,
            scope_hint: req.scope_hint,
            commit_plane_endpoint: req.commit_plane_endpoint,
            commit_plane_token_file: req.commit_plane_token_file,
        });
        save_executor_registry_state(&state_clone.state_dir, &reg)
    })
    .await?;
    Ok(Json(json!({"ok": true})))
}

pub(crate) async fn executors_list(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let reg = run_blocking(move || load_executor_registry_state(&state_clone.state_dir)).await?;
    Ok(Json(json!({"ok": true, "executors": reg.entries})))
}

pub(crate) async fn executors_check(
    State(state): State<UiServerState>,
    Json(req): Json<ExecutorCheckRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let reg = run_blocking(move || load_executor_registry_state(&state_clone.state_dir)).await?;
    let entry = reg
        .entries
        .iter()
        .find(|e| e.name == req.name)
        .ok_or_else(|| anyhow!("executor not found: {}", req.name))?;
    let client = reqwest::Client::new();
    client
        .get(format!("{}/health", entry.base_url.trim_end_matches('/')))
        .send()
        .await?
        .error_for_status()?;
    Ok(Json(json!({"ok": true})))
}
