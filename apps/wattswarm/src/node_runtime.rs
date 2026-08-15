use anyhow::Result;
use std::path::PathBuf;

pub fn start_node_runtime(state_dir: PathBuf, db_path: PathBuf) -> Result<bool> {
    let backend =
        crate::control::network_service::NetworkBackend::from_env_or_startup_config(&state_dir)?;
    let mode = crate::control::configured_node_mode(&state_dir)?;
    let maintenance_required = maintenance_required(mode, backend);

    let pure_local = matches!(mode, Some(crate::control::NodeMode::Local))
        && backend != crate::control::network_service::NetworkBackend::ClientServer;
    if pure_local {
        crate::node_maintenance::assert_no_maintenance_for_local_mode()?;
    } else if maintenance_required
        && crate::control::network_service::node_maintenance_enabled_from_env(true)
    {
        crate::node_maintenance::start_node_maintenance_once(state_dir.clone(), db_path.clone())?;
    }

    crate::network_bridge::maybe_start_background_network_service(state_dir, db_path)
}

fn maintenance_required(
    mode: Option<crate::control::NodeMode>,
    backend: crate::control::network_service::NetworkBackend,
) -> bool {
    (match mode {
        Some(crate::control::NodeMode::Local) | None => false,
        Some(crate::control::NodeMode::Lan | crate::control::NodeMode::Network) => true,
    }) || backend == crate::control::network_service::NetworkBackend::ClientServer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::NodeMode;
    use crate::control::network_service::NetworkBackend;

    #[test]
    fn maintenance_requirement_preserves_the_pure_local_boundary() {
        assert!(!maintenance_required(
            Some(NodeMode::Local),
            NetworkBackend::P2p
        ));
        assert!(!maintenance_required(None, NetworkBackend::P2p));
        assert!(maintenance_required(
            Some(NodeMode::Lan),
            NetworkBackend::P2p
        ));
        assert!(maintenance_required(
            Some(NodeMode::Network),
            NetworkBackend::P2p
        ));
        assert!(maintenance_required(
            Some(NodeMode::Local),
            NetworkBackend::ClientServer
        ));
    }
}
