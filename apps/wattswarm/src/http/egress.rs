use crate::control::local_node_id;
use crate::egress_agent::{
    EgressAgentConfig, load_egress_agent_config_state, save_egress_agent_config_state,
};
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::run_control;
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

pub(crate) async fn egress_agent_get(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let config =
        run_blocking(move || load_egress_agent_config_state(&state_clone.state_dir)).await?;
    Ok(Json(json!({"ok": true, "config": config})))
}

pub(crate) async fn egress_agent_save(
    State(state): State<UiServerState>,
    Json(config): Json<EgressAgentConfig>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let normalized = config.normalized();
    let saved = normalized.clone();
    run_blocking(move || save_egress_agent_config_state(&state_clone.state_dir, &normalized))
        .await?;
    Ok(Json(json!({"ok": true, "config": saved})))
}

pub(crate) async fn google_a2a_agent_card(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let card = load_google_a2a_agent_card(&state, false).await?;
    Ok(Json(card))
}

pub(crate) async fn google_a2a_well_known(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let card = load_google_a2a_agent_card(&state, true).await?;
    Ok(Json(card))
}

pub(crate) async fn google_a2a_message_send(
    State(state): State<UiServerState>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let request = serde_json::from_value::<crate::a2a::GoogleA2aSendRequest>(payload)
        .map_err(|err| anyhow!("invalid google a2a message/send request: {err}"))?;
    let response = run_blocking(move || {
        let config = load_egress_agent_config_state(&state_clone.state_dir)?;
        if !config.enabled {
            return Err(anyhow!("egress agent is disabled"));
        }
        if !matches!(config.protocol, crate::a2a::InvocationProtocol::GoogleA2a) {
            return Err(anyhow!("egress agent protocol is not google_a2a"));
        }
        let pg_url = run_control::resolve_run_queue_pg_url(None);
        crate::a2a::handle_google_message_send(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &config,
            request,
        )
    })
    .await?;
    Ok(Json(response))
}

async fn load_google_a2a_agent_card(
    state: &UiServerState,
    require_enabled: bool,
) -> Result<Value, ApiError> {
    let state_clone = state.clone();
    let card = run_blocking(move || {
        let config = load_egress_agent_config_state(&state_clone.state_dir)?;
        if require_enabled && !config.enabled {
            return Err(anyhow!("egress agent is disabled"));
        }
        let node_id = local_node_id(&state_clone.state_dir)?;
        crate::a2a::build_agent_card(&config, &node_id)
    })
    .await?;
    Ok(card)
}
