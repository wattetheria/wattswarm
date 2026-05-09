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

        let task_contract = serde_json::json!({
            "protocol_version": contract["protocol_version"].clone(),
            "task_id": "task-ordinary-e2e",
            "task_type": "generic.analysis",
            "inputs": {
                "request_id": "req-e2e",
                "domain": "analysis"
            },
            "output_schema": contract["output_schema"].clone(),
            "budget": contract["budget"].clone(),
            "assignment": contract["assignment"].clone(),
            "acceptance": contract["acceptance"].clone(),
            "task_mode": contract["task_mode"].clone(),
            "expiry_ms": contract["expiry_ms"].clone(),
            "evidence_policy": contract["evidence_policy"].clone()
        });

        let task_submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "contract": task_contract
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(task_submit_res.status(), StatusCode::OK);

        let announce_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/announce")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "task_id": "task-ordinary-e2e",
                            "feed_key": "tasks.public",
                            "scope_hint": "global",
                            "summary": {"request_id": "req-e2e"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(announce_res.status(), StatusCode::OK);
        let announce_json = json_from(announce_res).await;
        assert_eq!(announce_json["ok"].as_bool(), Some(true));
        assert_eq!(announce_json["subscribed"].as_bool(), Some(true));
        assert_eq!(announce_json["feed_key"].as_str(), Some("tasks.public"));
        assert_eq!(announce_json["scope_hint"].as_str(), Some("global"));
        assert_eq!(announce_json["gossip_kinds"][0].as_str(), Some("events"));

        let claim_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/claim")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"task_id":"task-ordinary-e2e","execution_id":"exec-agent-a"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claim_res.status(), StatusCode::OK);
        let claim_json = json_from(claim_res).await;
        assert_eq!(claim_json["ok"].as_bool(), Some(true));
        assert_eq!(claim_json["subscribed"].as_bool(), Some(true));
        assert!(claim_json["execution_id"].as_str().is_some());

        let propose_candidate_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/propose-candidate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "task_id": "task-ordinary-e2e",
                            "execution_id": "exec-agent-a",
                            "candidate_id": "cand-task-e2e-agent-a",
                            "output": {
                                "request_id": "req-e2e",
                                "result": {"ok": true}
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(propose_candidate_res.status(), StatusCode::OK);

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
fn peer_dm_send_uses_private_group_topic_messages() {
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
        let app = build_app(UiServerState::new(state_dir, db_path));

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

        let send_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/peers/dm/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "remote_node_id": "node-beta",
                            "content": {"text": "hello private group"},
                            "agent_envelope": {
                                "protocol": "google_a2a",
                                "source_agent_id": "agent-alpha",
                                "target_agent_id": "agent-beta",
                                "capability": "peer.dm.message",
                                "message_json": "{\"content\":{\"text\":\"hello private group\"}}"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(send_res.status(), StatusCode::OK);
        let send_json = json_from(send_res).await;
        assert_eq!(send_json["ok"].as_bool(), Some(true));
        assert_eq!(send_json["queued"].as_bool(), Some(false));
        assert_eq!(send_json["feed_key"].as_str(), Some("wattswarm.dm"));
        let scope_hint = send_json["scope_hint"].as_str().unwrap();
        assert!(scope_hint.starts_with("group:dm-"));
        assert_eq!(send_json["gossip_kinds"][0].as_str(), Some("messages"));
        let thread_id = send_json["thread_id"].as_str().unwrap();

        let topic_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/topic/messages?feed_key=wattswarm.dm&scope_hint={scope_hint}&limit=5"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(topic_res.status(), StatusCode::OK);
        let topic_json = json_from(topic_res).await;
        let network_id = topic_json["network_id"].as_str().unwrap();
        assert_eq!(topic_json["messages"].as_array().unwrap().len(), 1);
        assert_eq!(
            topic_json["messages"][0]["network_id"].as_str(),
            Some(network_id)
        );
        assert_eq!(
            topic_json["messages"][0]["content"]["kind"].as_str(),
            Some("direct_message")
        );
        assert_eq!(
            topic_json["messages"][0]["content"]["thread_id"].as_str(),
            Some(thread_id)
        );

        let other_network_topic_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/topic/messages?network_id=other-net&feed_key=wattswarm.dm&scope_hint={scope_hint}&limit=5"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(other_network_topic_res.status(), StatusCode::OK);
        let other_network_topic_json = json_from(other_network_topic_res).await;
        assert_eq!(
            other_network_topic_json["messages"].as_array().unwrap().len(),
            0
        );

        let dm_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/peers/dm/messages?thread_id={thread_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dm_res.status(), StatusCode::OK);
        let dm_json = json_from(dm_res).await;
        assert_eq!(dm_json["messages"].as_array().unwrap().len(), 1);
        assert_eq!(
            dm_json["messages"][0]["content"]["text"].as_str(),
            Some("hello private group")
        );
    });
}

#[test]
fn ui_root_page_serves_startup_view_and_diagnostics_route_redirects_legacy_console() {
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
        assert!(root_html.contains("Open Network Diagnostics"));
        assert!(root_html.contains("href=\"/diagnostics\""));
        assert!(root_html.contains("Bootstrap Contacts"));
        assert!(root_html.contains("node export-contact"));
        assert!(root_html.contains("&lt;node-id&gt;@&lt;host:port&gt;"));
        assert!(
            root_html.contains(
                "WAN discovers Wattetheria bootstrap and gateway endpoints automatically"
            )
        );
        assert!(root_html.contains("Used only for LAN or private test networks"));
        assert!(!root_html.contains("<genesis-node-id>"));
        assert!(!root_html.contains("\"metadata\":{\"route\":\"iroh_direct\""));
        assert!(!root_html.contains("href=\"/console\""));
        assert!(!root_html.contains("Open Swarm Dashboard"));

        let diagnostics_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/diagnostics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(diagnostics_res.status(), StatusCode::OK);
        let diagnostics_body = to_bytes(diagnostics_res.into_body(), usize::MAX)
            .await
            .unwrap();
        let diagnostics_html = String::from_utf8(diagnostics_body.to_vec()).unwrap();
        assert!(diagnostics_html.contains("WattSwarm Network Diagnostics"));
        assert!(diagnostics_html.contains("Iroh transport"));
        assert!(diagnostics_html.contains("Iroh Endpoint"));
        assert!(diagnostics_html.contains("Known Iroh Contacts"));
        assert!(!diagnostics_html.contains("Local Peer"));
        assert!(!diagnostics_html.contains("Connected Peers"));
        assert!(!diagnostics_html.contains("peer id"));
        assert!(diagnostics_html.contains("/api/diagnostics"));
        assert!(!diagnostics_html.contains("Quick Start"));

        let legacy_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/console")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy_res.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            legacy_res
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok()),
            Some("/diagnostics")
        );
    });
}

#[test]
fn ui_diagnostics_api_lists_wattswarm_network_diagnostics() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(state_dir.join("diagnostics")).unwrap();
        std::fs::write(
            state_dir.join("diagnostics/wattswarm_node.jsonl"),
            r#"{"id":"diag-1","timestamp_ms":123,"level":"info","component":"wattswarm.network_bridge","category":"gossip","phase":"publish.event","status":"ok","message":"published local event","event_id":"event-1","object_kind":"task","object_id":"task-1","source_node_id":"peer-a","scope_hint":"node:peer-b","details":{"topic":"events"}}"#,
        )
        .unwrap();
        let db_path = state_dir.join("ui.state");
        let app = build_app(UiServerState::new(state_dir, db_path));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/diagnostics?search=task-1&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload = json_from(response).await;
        assert_eq!(payload["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(
            payload["diagnostics"][0]["phase"].as_str(),
            Some("publish.event")
        );
        assert_eq!(
            payload["network_service_started"].as_bool(),
            Some(false)
        );
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

#[test]
fn ui_exposes_network_join_manifest() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _urls_guard = EnvVarGuard::set(
        "WATTSWARM_PUBLIC_BOOTSTRAP_URLS",
        "https://bootstrap.wattetheria.com",
    );
    let _contacts_guard = EnvVarGuard::set(
        "WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS",
        "iroh-contact-a,iroh-contact-b",
    );
    let _gateway_guard = EnvVarGuard::set(
        "WATTSWARM_PUBLIC_GATEWAY_URLS",
        "https://gateway.wattetheria.com",
    );
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let genesis = NodeIdentity::random();
    let genesis_node_id = genesis.node_id();
    let network_id = "mainnet:wattetheria";
    let store = wattswarm::storage::PgStore::open(&db_path).unwrap();
    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Wattetheria",
            &genesis_node_id,
            &genesis_node_id,
            1_700_000_000_000,
        )
        .unwrap();
    let signed = store
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
                    .uri("/.well-known/wattswarm/join.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = json_from(res).await;
        assert_eq!(json["network_id"].as_str(), Some(network_id));
        assert_eq!(
            json["genesis_node_id"].as_str(),
            Some(genesis_node_id.as_str())
        );
        assert_eq!(
            json["params_hash"].as_str(),
            Some(signed.params_hash.as_str())
        );
        assert_eq!(
            json["bootstrap_urls"][0].as_str(),
            Some("https://bootstrap.wattetheria.com")
        );
        assert_eq!(
            json["bootstrap_contacts"][0].as_str(),
            Some("iroh-contact-a")
        );
        assert_eq!(
            json["bootstrap_contacts"][1].as_str(),
            Some("iroh-contact-b")
        );
        assert_eq!(
            json["gateway_urls"][0].as_str(),
            Some("https://gateway.wattetheria.com")
        );
    });
}
