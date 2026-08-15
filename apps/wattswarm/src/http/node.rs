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
        let _ = crate::node_runtime::start_node_runtime(
            state_clone.state_dir.clone(),
            state_clone.db_path.clone(),
        )?;
        if crate::network_bridge::network_enabled_from_state_dir(&state_clone.state_dir) {
            crate::udp_announce::announce_startup_with_contact(
                "node-up-api",
                None,
                Some(&node.node_id()),
                &state_clone.state_dir,
            );
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
        let (dist, network_backend) = if runtime_state.running {
            match open_configured_node(&state_clone.state_dir, &state_clone.db_path) {
                Ok(node) => {
                    let peers = node
                        .store
                        .peer_protocol_version_distribution(&node.identity.node_id())?;
                    let mut dist = serde_json::Map::new();
                    for (version, count) in peers {
                        dist.insert(version, Value::from(count));
                    }
                    let scope_id = wattswarm_storage_core::storage::local_control_scope_id(
                        &state_clone.state_dir,
                    );
                    let backend =
                        node.store
                            .load_network_backend_status(&scope_id)?
                            .map(|status| {
                                json!({
                                    "backend": status.backend,
                                    "status": status.status,
                                    "published": status.published,
                                    "received": status.received,
                                    "retries": status.retries,
                                    "last_error": status.reason,
                                    "updated_at": status.updated_at,
                                    "backend_details": serde_json::from_str::<Value>(
                                        &status.backend_details_json,
                                    )
                                    .unwrap_or_else(|_| json!({})),
                                })
                            });
                    (dist, backend)
                }
                Err(_) => (serde_json::Map::new(), None),
            }
        } else {
            (serde_json::Map::new(), None)
        };
        Ok(json!({
            "ok": true,
            "running": runtime_state.running,
            "node_id": node_id,
            "mode": runtime_state.mode.as_str(),
            "local_protocol_version": crate::constants::LOCAL_PROTOCOL_VERSION,
            "peer_protocol_distribution": dist,
            "network_backend": network_backend,
        }))
    })
    .await?;
    Ok(Json(result))
}
