use super::*;

#[test]
fn configured_node_mode_requires_explicit_selection() {
    let _env_lock = env_lock();
    let prev_mode = clear_node_mode_env();
    let state_dir = temp_test_dir("configured-node-mode");

    assert_eq!(
        configured_node_mode(&state_dir).expect("read configured mode"),
        None
    );

    fs::write(
        state_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "bootstrap_contacts": [],
            "core_agent": {
                "mode": "local_url",
                "base_url": "http://127.0.0.1:8787",
                "provider": "openai-compatible",
                "model": "",
                "api_key": ""
            }
        }))
        .expect("serialize startup config"),
    )
    .expect("write startup config");

    assert_eq!(
        configured_node_mode(&state_dir).expect("read configured startup mode"),
        Some(wattswarm_control_plane::control::NodeMode::Network)
    );

    cleanup_dir(&state_dir);
    restore_node_mode_env(prev_mode);
}

#[test]
fn background_network_service_is_deferred_until_mode_is_configured() {
    let _env_lock = env_lock();
    let prev_mode = clear_node_mode_env();
    let _p2p_enabled = EnvVarGuard::set("WATTSWARM_P2P_ENABLED", "true");
    let state_dir = temp_test_dir("network-service-deferred");
    let db_path = state_dir.join("wattswarm.db");

    let started = maybe_start_background_network_service(state_dir.clone(), db_path)
        .expect("attempt background network start");
    assert!(
        !started,
        "service should not start before node mode is configured"
    );

    cleanup_dir(&state_dir);
    restore_node_mode_env(prev_mode);
}
