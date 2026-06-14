use crate::control::{
    NodeState, local_node_id, node_state_path, open_configured_node, require_configured_node_mode,
    resolve_node_mode, write_node_state,
};
use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::Result;
use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};
use std::fs;

pub(crate) async fn node_up(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
        let mode = require_configured_node_mode(&state_clone.state_dir)?;
        write_node_state(&state_clone.state_dir, true, mode)?;
        let _ = crate::network_bridge::maybe_start_background_network_service_with_hook(
            state_clone.state_dir.clone(),
            state_clone.db_path.clone(),
            Some(Box::new(|node, sd| {
                crate::network_hooks::run_background_post_tick(node, sd);
            })),
        )?;
        if crate::network_bridge::network_enabled_from_env() {
            crate::udp_announce::announce_startup("node-up-api", None, Some(&node.node_id()));
        }
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true})))
}

pub(crate) async fn node_down(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let mode = resolve_node_mode(&state.state_dir)?;
    write_node_state(&state.state_dir, false, mode)?;
    Ok(Json(json!({"ok": true})))
}

pub(crate) async fn node_status(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let result = run_blocking(move || -> Result<Value> {
        let state_path = node_state_path(&state_clone.state_dir);
        let runtime_state: NodeState = if state_path.exists() {
            serde_json::from_slice(&fs::read(state_path)?)?
        } else {
            NodeState {
                running: false,
                mode: resolve_node_mode(&state_clone.state_dir)?,
            }
        };
        let node_id = local_node_id(&state_clone.state_dir).unwrap_or_default();

        // Only read peer info if node has been explicitly started (has topology in DB).
        let dist = if runtime_state.running {
            match open_configured_node(&state_clone.state_dir, &state_clone.db_path) {
                Ok(node) => {
                    let peers = node
                        .store
                        .peer_protocol_version_distribution(&node.identity.node_id())?;
                    let mut dist = serde_json::Map::new();
                    for (version, count) in peers {
                        dist.insert(version, Value::from(count));
                    }
                    dist
                }
                Err(_) => serde_json::Map::new(),
            }
        } else {
            serde_json::Map::new()
        };
        Ok(json!({
            "ok": true,
            "running": runtime_state.running,
            "node_id": node_id,
            "mode": runtime_state.mode.as_str(),
            "local_protocol_version": crate::constants::LOCAL_PROTOCOL_VERSION,
            "peer_protocol_distribution": dist
        }))
    })
    .await?;
    Ok(Json(result))
}
