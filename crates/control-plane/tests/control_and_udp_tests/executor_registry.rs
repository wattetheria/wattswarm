use super::*;

#[test]
fn executor_registry_roundtrip_and_default_load() {
    let dir = temp_test_dir("executor-registry");
    let path = executor_registry_path(&dir);

    let default_loaded = load_executor_registry(&path).expect("load missing registry");
    assert!(default_loaded.entries.is_empty());

    let reg = ExecutorRegistry {
        entries: vec![ExecutorRegistryEntry {
            name: "rt".to_owned(),
            base_url: "http://127.0.0.1:7788".to_owned(),
            agent_event_callback_base_url: None,
            kind: Default::default(),
            target_node_id: None,
            scope_hint: None,
            commit_plane_endpoint: None,
            commit_plane_token_file: None,
        }],
    };
    save_executor_registry(&path, &reg).expect("save registry");
    let loaded = load_executor_registry(&path).expect("load registry");
    assert_eq!(loaded.entries.len(), 1);
    assert_eq!(loaded.entries[0].name, "rt");

    cleanup_dir(&dir);
}

#[test]
fn executor_registry_state_roundtrip_preserves_remote_metadata() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("executor-registry-state-roundtrip");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![
                ExecutorRegistryEntry {
                    name: "rt".to_owned(),
                    base_url: "http://127.0.0.1:7788".to_owned(),
                    agent_event_callback_base_url: None,
                    kind: wattswarm_control_plane::control::ExecutorKind::Remote,
                    target_node_id: Some("node-remote-a".to_owned()),
                    scope_hint: Some("group:alpha".to_owned()),
                    commit_plane_endpoint: None,
                    commit_plane_token_file: None,
                },
                ExecutorRegistryEntry {
                    name: "local-aux".to_owned(),
                    base_url: "http://127.0.0.1:8899".to_owned(),
                    agent_event_callback_base_url: None,
                    kind: wattswarm_control_plane::control::ExecutorKind::Local,
                    target_node_id: None,
                    scope_hint: None,
                    commit_plane_endpoint: None,
                    commit_plane_token_file: None,
                },
            ],
        },
    )
    .expect("save executor registry state");

    let loaded = load_executor_registry_state(&state_dir).expect("load executor registry state");
    assert_eq!(loaded.entries.len(), 2);

    let remote = loaded
        .entries
        .iter()
        .find(|entry| entry.name == "rt")
        .expect("remote executor present");
    assert_eq!(
        remote.kind,
        wattswarm_control_plane::control::ExecutorKind::Remote
    );
    assert_eq!(remote.target_node_id.as_deref(), Some("node-remote-a"));
    assert_eq!(remote.scope_hint.as_deref(), Some("group:alpha"));

    let local = loaded
        .entries
        .iter()
        .find(|entry| entry.name == "local-aux")
        .expect("local executor present");
    assert_eq!(
        local.kind,
        wattswarm_control_plane::control::ExecutorKind::Local
    );
    assert_eq!(local.target_node_id, None);
    assert_eq!(local.scope_hint, None);

    cleanup_dir(&dir);
}
