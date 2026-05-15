use super::*;

#[test]
fn run_real_task_flow_returns_clear_error_when_executor_missing() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-missing-executor");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let req = RealTaskRunRequest {
        executor: "missing-executor".to_owned(),
        profile: "default".to_owned(),
        task_id: Some(format!("task-{}", Uuid::new_v4().simple())),
        task_file: None,
        task_contract: Some(sample_contract("task-inline", "policy-hash".to_owned())),
    };

    let err =
        run_real_task_flow(&mut node, &state_dir, req).expect_err("executor should be missing");
    assert!(
        err.to_string()
            .contains("executor not found: missing-executor")
    );

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_rejects_unsupported_profile() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-unsupported-profile");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["other-profile"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let req = RealTaskRunRequest {
        executor: "rt".to_owned(),
        profile: "default".to_owned(),
        task_id: Some(format!("task-{}", Uuid::new_v4().simple())),
        task_file: None,
        task_contract: Some(sample_contract("task-inline", "policy-hash".to_owned())),
    };

    let err = run_real_task_flow(&mut node, &state_dir, req)
        .expect_err("profile should be rejected before task run");
    assert!(err.to_string().contains("profile 'default' not supported"));

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_reports_task_file_parse_errors() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-task-file-parse");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let broken_task_file = dir.join("broken-task.json");
    fs::write(&broken_task_file, b"{not-json").expect("write broken task file");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let req = RealTaskRunRequest {
        executor: "rt".to_owned(),
        profile: "default".to_owned(),
        task_id: None,
        task_file: Some(broken_task_file.clone()),
        task_contract: None,
    };
    let err =
        run_real_task_flow(&mut node, &state_dir, req).expect_err("broken task json should fail");
    let err_msg = err.to_string();
    assert!(
        (err_msg.contains("parse task contract from") && err_msg.contains("broken-task.json"))
            || err_msg.contains("key must be a string")
            || err_msg.contains("expected value")
            || err_msg.contains("expected ident")
            || err_msg.contains("line 1 column")
    );

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_completes_with_stub_runtime() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-success");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("task-real-success-{}", Uuid::new_v4().simple());
    let req = RealTaskRunRequest {
        executor: "rt".to_owned(),
        profile: "default".to_owned(),
        task_id: Some(task_id.clone()),
        task_file: None,
        task_contract: Some(sample_contract(&task_id, policy_hash)),
    };

    let out = run_real_task_flow(&mut node, &state_dir, req).expect("run real task flow success");
    assert_eq!(out["task_id"], task_id);
    assert_eq!(out["profile"], "default");
    assert_eq!(out["terminal_state"], "Finalized");

    cleanup_dir(&dir);
}
