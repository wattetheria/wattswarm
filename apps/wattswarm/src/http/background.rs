use crate::control::{NodeState, node_state_path, require_configured_node_mode, write_node_state};
use anyhow::Result;
use std::fs;

pub(crate) fn mark_node_running_if_service_started(
    state_dir: &std::path::Path,
    started: bool,
) -> Result<()> {
    if !started {
        return Ok(());
    }
    let mode = require_configured_node_mode(state_dir)?;
    write_node_state(state_dir, true, mode)
}

pub(crate) fn sync_runtime_node_mode_with_startup_config(
    state_dir: &std::path::Path,
    network_mode: crate::startup_config::NetworkMode,
) -> Result<()> {
    let state_path = node_state_path(state_dir);
    if !state_path.exists() {
        return Ok(());
    }
    let runtime_state: NodeState = serde_json::from_slice(&fs::read(&state_path)?)?;
    write_node_state(
        state_dir,
        runtime_state.running,
        node_mode_from_startup_mode(network_mode),
    )
}

fn node_mode_from_startup_mode(
    network_mode: crate::startup_config::NetworkMode,
) -> crate::control::NodeMode {
    match network_mode {
        crate::startup_config::NetworkMode::Local => crate::control::NodeMode::Local,
        crate::startup_config::NetworkMode::Lan => crate::control::NodeMode::Lan,
        crate::startup_config::NetworkMode::Wan => crate::control::NodeMode::Network,
    }
}
