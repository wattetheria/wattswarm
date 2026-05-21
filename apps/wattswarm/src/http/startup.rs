use crate::http::background::{
    mark_node_running_if_service_started, sync_runtime_node_mode_with_startup_config,
};
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::startup_config::{
    StartupConfig, core_agent_executor_name, load_startup_config, save_startup_config,
    startup_config_path, sync_core_agent_executor,
};
use anyhow::Result;
use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub(crate) struct StartupConfigSaveRequest {
    display_name: String,
    network_mode: crate::startup_config::NetworkMode,
    #[serde(default)]
    bootstrap_contacts: Vec<String>,
    #[serde(default)]
    gateway_urls: Vec<String>,
    #[serde(default)]
    core_agent: Option<crate::startup_config::CoreAgentConfig>,
}
pub(crate) async fn startup_config_get(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let config =
        run_blocking(move || load_startup_config(&startup_config_path(&state_clone.state_dir)))
            .await?;
    Ok(Json(json!({
        "ok": true,
        "config": config,
        "core_agent_executor": core_agent_executor_name()
    })))
}

pub(crate) async fn startup_config_save(
    State(state): State<UiServerState>,
    Json(req): Json<StartupConfigSaveRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let existing_state_dir = state_clone.state_dir.clone();
    let existing =
        run_blocking(move || load_startup_config(&startup_config_path(&existing_state_dir)))
            .await?;
    let (bootstrap_contacts, gateway_urls, servicenet_urls) = match req.network_mode {
        crate::startup_config::NetworkMode::Lan => (
            req.bootstrap_contacts,
            req.gateway_urls,
            existing.servicenet_urls.clone(),
        ),
        crate::startup_config::NetworkMode::Wan => (
            existing.bootstrap_contacts.clone(),
            existing.gateway_urls.clone(),
            existing.servicenet_urls.clone(),
        ),
        crate::startup_config::NetworkMode::Local => (Vec::new(), Vec::new(), Vec::new()),
    };
    let payload = StartupConfig {
        display_name: req.display_name,
        latitude: existing.latitude,
        longitude: existing.longitude,
        network_mode: req.network_mode,
        bootstrap_contacts,
        gateway_urls,
        servicenet_urls,
        core_agent: req.core_agent.clone().unwrap_or(existing.core_agent),
    }
    .normalized();
    payload.validate()?;
    let saved = payload.clone();
    let should_sync_core_agent = req.core_agent.is_some();
    let executor_registered = run_blocking(move || -> Result<bool> {
        let path = startup_config_path(&state_clone.state_dir);
        save_startup_config(&path, &saved)?;
        sync_runtime_node_mode_with_startup_config(&state_clone.state_dir, saved.network_mode)?;
        if should_sync_core_agent {
            sync_core_agent_executor(&state_clone.state_dir, &saved)
        } else {
            Ok(false)
        }
    })
    .await?;
    let network_started = crate::network_bridge::maybe_start_background_network_service_with_hook(
        state.state_dir.clone(),
        state.db_path.clone(),
        Some(Box::new(|node, sd| {
            let _ = crate::run_queue::network_bridge::process_pending_bridge_tasks(node, sd);
            let _ = crate::run_queue::network_bridge::process_pending_run_queue_results(sd);
            let _ = crate::control::topic_interpretation::process_topic_interpretation(node, sd);
            let _ = crate::control::topic_consensus::process_structured_topic_consensus(node, sd);
        })),
    )?;
    mark_node_running_if_service_started(&state.state_dir, network_started)?;
    Ok(Json(json!({
        "ok": true,
        "config": payload,
        "core_agent_executor": core_agent_executor_name(),
        "executor_registered": executor_registered
    })))
}
