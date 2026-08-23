use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::Result;
use axum::Json;
use axum::extract::State;
use serde_json::Value;

fn should_log_network_enabled(
    permission_was_active: bool,
    permission_is_active: bool,
    network_start_requested: bool,
) -> bool {
    !permission_was_active && permission_is_active && network_start_requested
}

pub(crate) async fn network_permission_checkpoint(
    State(state): State<UiServerState>,
    Json(checkpoint): Json<crate::network_bridge::NetworkPermissionCheckpoint>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<Value> {
        let permission_was_active =
            crate::network_bridge::network_permission_is_active(&state_clone.state_dir);
        let permission_active = crate::network_bridge::update_network_permission_runtime_state(
            &state_clone.state_dir,
            &checkpoint,
        );
        let network_start_requested = if permission_active {
            crate::network_bridge::maybe_start_background_network_service_with_hook(
                state_clone.state_dir.clone(),
                state_clone.db_path.clone(),
                Some(Box::new(|node, sd| {
                    crate::network_hooks::run_background_post_tick(node, sd);
                })),
            )?
        } else {
            false
        };
        if network_start_requested {
            crate::http::background::mark_node_running_if_service_started(
                &state_clone.state_dir,
                true,
            )?;
        }
        if should_log_network_enabled(
            permission_was_active,
            permission_active,
            network_start_requested,
        ) {
            eprintln!("wattswarm p2p network enabled");
        }
        if !permission_active
            && let Ok(mode) = crate::control::resolve_node_mode(&state_clone.state_dir)
        {
            let _ = crate::control::write_node_state(&state_clone.state_dir, false, mode);
        }
        Ok(serde_json::json!({
            "ok": true,
            "network_start_requested": network_start_requested,
            "checkpoint": checkpoint,
        }))
    })
    .await
    .map(Json)
}

#[cfg(test)]
mod tests {
    use super::should_log_network_enabled;

    #[test]
    fn enabled_log_is_emitted_only_for_permission_activation_that_starts_network() {
        assert!(should_log_network_enabled(false, true, true));
        assert!(!should_log_network_enabled(true, true, true));
        assert!(!should_log_network_enabled(false, true, false));
        assert!(!should_log_network_enabled(false, false, false));
    }
}
