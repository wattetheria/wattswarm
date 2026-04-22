use super::*;

#[test]
fn ui_supports_core_cli_operations() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let db_path = state_dir.join("ui.state");
        let app = build_app(UiServerState::new(state_dir.clone(), db_path));

        let up_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/node/up")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(up_res.status(), StatusCode::OK);

        let status_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/node/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let status_json = json_from(status_res).await;
        assert_eq!(status_json["running"].as_bool(), Some(true));
        assert!(status_json["node_id"].as_str().is_some());

        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"rt-ui","base_url":"http://127.0.0.1:8787"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let list_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/executors/list")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_exec_res.status(), StatusCode::OK);
        let list_json = json_from(list_exec_res).await;
        assert_eq!(list_json["executors"][0]["name"].as_str(), Some("rt-ui"));

        let sample_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/sample?task_id=task-ui-e2e")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sample_res.status(), StatusCode::OK);
        let sample_json = json_from(sample_res).await;
        let contract = sample_json["contract"].clone();

        let submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"contract": contract})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_res.status(), StatusCode::OK);

        let watch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/watch/task-ui-e2e")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(watch_res.status(), StatusCode::OK);
        let watch_json = json_from(watch_res).await;
        assert_eq!(watch_json["task_id"].as_str(), Some("task-ui-e2e"));

        let by_task_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/knowledge/export")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"task_id":"task-ui-e2e"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(by_task_res.status(), StatusCode::OK);

        let swarm_page_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/swarm")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(swarm_page_res.status(), StatusCode::OK);

        let swarm_state_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/swarm/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(swarm_state_res.status(), StatusCode::OK);
        let swarm_state_json = json_from(swarm_state_res).await;
        assert!(swarm_state_json["state"]["signals"].is_array());
    });
}

#[test]
fn ui_root_page_serves_startup_view_and_console_route_keeps_legacy_console() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let db_path = state_dir.join("ui.state");
        let app = build_app(UiServerState::new(state_dir, db_path));

        let root_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(root_res.status(), StatusCode::OK);
        let root_body = to_bytes(root_res.into_body(), usize::MAX).await.unwrap();
        let root_html = String::from_utf8(root_body.to_vec()).unwrap();
        assert!(root_html.contains("WattSwarm Startup"));
        assert!(root_html.contains("Open Developer Console"));

        let console_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/console")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(console_res.status(), StatusCode::OK);
        let console_body = to_bytes(console_res.into_body(), usize::MAX).await.unwrap();
        let console_html = String::from_utf8(console_body.to_vec()).unwrap();
        assert!(console_html.contains("WattSwarm Kernel Console"));
        assert!(console_html.contains("Quick Start"));
    });
}

#[test]
fn ui_exposes_local_network_peer_identity_and_listen_addrs() {
    let _guard = env_lock();
    let _p2p_enabled = EnvVarGuard::set("WATTSWARM_P2P_ENABLED", "true");
    let _listen_addrs = EnvVarGuard::set(
        "WATTSWARM_P2P_LISTEN_ADDRS",
        "/ip4/127.0.0.1/tcp/4010,/ip4/0.0.0.0/tcp/4011",
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let db_path = state_dir.join("ui.state");
        let _ = wattswarm::control::local_node_id(&state_dir).expect("create local identity");
        let app = build_app(UiServerState::new(state_dir, db_path));

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/local")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = json_from(res).await;
        assert_eq!(json["ok"].as_bool(), Some(true));
        assert_eq!(json["network_enabled"].as_bool(), Some(true));
        assert!(
            json["local_peer_id"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(
            json["listen_addrs"].as_array().unwrap(),
            &vec![
                Value::String("/ip4/127.0.0.1/tcp/4010".to_owned()),
                Value::String("/ip4/0.0.0.0/tcp/4011".to_owned()),
            ]
        );
    });
}

#[test]
fn ui_exposes_network_bootstrap_bundle() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let genesis = NodeIdentity::random();
    let genesis_node_id = genesis.node_id();
    let network_id = "mainnet:watt-etheria";
    let org_id = "mainnet:watt-etheria:bootstrap";
    let store = wattswarm::storage::PgStore::open(&db_path).unwrap();
    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Watt Etheria",
            &genesis_node_id,
            &genesis_node_id,
            1_700_000_000_000,
        )
        .unwrap();
    let conn = Connection::open("ui-network-bootstrap-setup").unwrap();
    conn.execute(
        "UPDATE org_registry SET name = 'Aether Genesis' WHERE org_id = $1",
        wattswarm_storage_core::params![org_id],
    )
    .unwrap();
    store
        .put_network_protocol_params(network_id, &genesis, &NetworkProtocolParams::default())
        .unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = build_app(UiServerState::new(state_dir, db_path));
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/bootstrap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = json_from(res).await;
        assert_eq!(json["ok"].as_bool(), Some(true));
        assert_eq!(
            json["bundle"]["topology"]["network"]["network_id"].as_str(),
            Some(network_id)
        );
        assert_eq!(
            json["bundle"]["topology"]["network"]["network_name"].as_str(),
            Some("Watt Etheria")
        );
        assert_eq!(
            json["bundle"]["topology"]["org"]["org_id"].as_str(),
            Some(org_id)
        );
        assert_eq!(
            json["bundle"]["topology"]["org"]["network_org_name"].as_str(),
            Some("Aether Genesis")
        );
        assert_eq!(
            json["bundle"]["signed_params"]["signed_by"].as_str(),
            Some(genesis_node_id.as_str())
        );
    });
}
