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
                "domain": "analysis",
                "swarm_scope": "group:task-ordinary-e2e"
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
        assert_eq!(
            claim_json["subscription_scope_hint"].as_str(),
            Some("group:task-ordinary-e2e")
        );
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
fn task_import_contract_populates_projection_without_created_or_announced_events() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test_import_contract");
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
        let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));

        let sample_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/sample?task_id=remote-task-import")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sample_res.status(), StatusCode::OK);
        let sample_json = json_from(sample_res).await;
        let mut contract = sample_json["contract"].clone();
        contract["inputs"]["swarm_scope"] = json!("group:remote-task-import");

        let import_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/import-contract")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"contract": contract})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(import_res.status(), StatusCode::OK);
        let import_json = json_from(import_res).await;
        assert_eq!(import_json["ok"].as_bool(), Some(true));
        assert_eq!(
            import_json["scope_hint"].as_str(),
            Some("group:remote-task-import")
        );

        let watch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/watch/remote-task-import")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(watch_res.status(), StatusCode::OK);

        let events = tokio::task::spawn_blocking({
            let state_dir = state_dir.clone();
            let db_path = db_path.clone();
            move || {
                let node = open_node(&state_dir, &db_path).expect("open node");
                node.store.load_all_events().expect("load events")
            }
        })
        .await
        .expect("join event load");
        assert!(!events.iter().any(|(_, event)| {
            event.task_id.as_deref() == Some("remote-task-import")
                && matches!(
                    &event.payload,
                    EventPayload::TaskCreated(_) | EventPayload::TaskAnnounced(_)
                )
        }));

        tokio::task::spawn_blocking({
            let state_dir = state_dir.clone();
            let db_path = db_path.clone();
            move || {
                let mut node = open_node(&state_dir, &db_path).expect("open node");
                let network_id = if node.store.is_org_configured() {
                    node.store
                        .load_network_topology_for_org(node.store.org_id())
                        .map(|topology| topology.network.network_id)
                        .unwrap_or_else(|_| format!("local:{}", node.node_id()))
                } else {
                    node.store
                        .load_verified_network_protocol_params()
                        .map(|verified| verified.network_id)
                        .unwrap_or_else(|_| format!("local:{}", node.node_id()))
                };
                node.emit_at(
                    1,
                    EventPayload::FeedSubscriptionUpdated(FeedSubscriptionUpdatedPayload {
                        network_id,
                        subscriber_node_id: node.node_id(),
                        feed_key: "task.lifecycle.remote-task-import".to_owned(),
                        scope_hint: "group:remote-task-import".to_owned(),
                        gossip_kinds: vec!["events".to_owned()],
                        provider_capabilities: None,
                        agent_envelope: None,
                        active: true,
                    }),
                    1_700_000_000_000,
                )
                .expect("seed existing subscription");
            }
        })
        .await
        .expect("join subscription seed");

        let claim_res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/claim")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"task_id":"remote-task-import","execution_id":"exec-remote-agent"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claim_res.status(), StatusCode::OK);
        let claim_json = json_from(claim_res).await;
        assert_eq!(claim_json["ok"].as_bool(), Some(true));
        assert_eq!(claim_json["subscribed"].as_bool(), Some(true));
        assert_eq!(
            claim_json["subscription_scope_hint"].as_str(),
            Some("group:remote-task-import")
        );
        assert_eq!(
            claim_json["subscription_feed_key"].as_str(),
            Some("task.lifecycle.remote-task-import")
        );

        let events = tokio::task::spawn_blocking(move || {
            let node = open_node(&state_dir, &db_path).expect("open node");
            node.store.load_all_events().expect("load events")
        })
        .await
        .expect("join event load");
        let claim_events = events
            .iter()
            .filter(|(_, event)| {
                event.task_id.as_deref() == Some("remote-task-import")
                    && matches!(&event.payload, EventPayload::TaskClaimed(_))
            })
            .count();
        assert_eq!(claim_events, 1);
        let subscription_events = events
            .iter()
            .filter(|(_, event)| {
                matches!(
                    &event.payload,
                    EventPayload::FeedSubscriptionUpdated(payload)
                        if payload.feed_key == "task.lifecycle.remote-task-import"
                            && payload.scope_hint == "group:remote-task-import"
                            && payload.gossip_kinds.as_slice() == ["events"]
                            && payload.active
                )
            })
            .count();
        assert_eq!(subscription_events, 1);
        assert!(!events.iter().any(|(_, event)| {
            event.task_id.as_deref() == Some("remote-task-import")
                && matches!(
                    &event.payload,
                    EventPayload::TaskCreated(_) | EventPayload::TaskAnnounced(_)
                )
        }));
    });
}

#[test]
fn peer_dm_send_uses_private_group_topic_messages() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    open_node(&state_dir, &db_path).expect("initialize local node");
    let remote_keypair = wattswarm::crypto::generate_private_message_keypair();
    let remote_public_key_b64 = remote_keypair.public_key_b64.clone();
    wattswarm::control::save_peer_metadata_record_state(
        &state_dir,
        &wattswarm::control::PeerMetadataRecord {
            node_id: "node-beta".to_owned(),
            network_id: None,
            params_version: None,
            params_hash: None,
            agent_version_raw: None,
            agent_version_prefix: None,
            protocol_version: None,
            observed_addr: None,
            listen_addrs: Vec::new(),
            protocols: Vec::new(),
            handshake_status: "identified".to_owned(),
            last_error: None,
            contact_material: Some(json!({
                "encryption": {
                    "private_message": {
                        "scheme": "wattswarm.private.dm.v1",
                        "key_agreement": "x25519",
                        "cipher": "chacha20poly1305",
                        "public_key_b64": remote_public_key_b64,
                    }
                }
            })),
            contact_material_signature: None,
            contact_material_updated_at: Some(1),
            first_identified_at: 1,
            last_identified_at: 1,
        },
    )
    .expect("save peer encryption metadata");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));

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
        let send_status = send_res.status();
        let send_json = json_from(send_res).await;
        assert_eq!(send_status, StatusCode::OK, "send response: {send_json}");
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
        assert!(
            topic_json["messages"][0]["content"]["encrypted"]
                .as_object()
                .is_some()
        );
        let ciphertext_b64 = topic_json["messages"][0]["content"]["encrypted"]["ciphertext_b64"]
            .as_str()
            .expect("encrypted topic payload has ciphertext")
            .to_owned();
        assert!(topic_json["messages"][0]["content"]["content"].is_null());
        assert!(
            !serde_json::to_string(&topic_json["messages"][0]["content"])
                .unwrap()
                .contains("hello private group")
        );
        let diagnostics = wattswarm::network_bridge::list_network_diagnostics(
            &state_dir,
            &wattswarm::network_bridge::DiagnosticFilter {
                phase: Some("private_dm.encrypt".to_owned()),
                ..Default::default()
            },
        )
        .expect("list private dm encryption diagnostics");
        let encrypt_diagnostic = diagnostics
            .iter()
            .find(|entry| {
                entry.object_kind.as_deref() == Some("peer_dm_message")
                    && entry.object_id.as_deref() == send_json["message_id"].as_str()
            })
            .expect("private dm encryption diagnostic recorded");
        assert_eq!(
            encrypt_diagnostic.details["encrypted_payload_present"].as_bool(),
            Some(true)
        );
        assert_eq!(
            encrypt_diagnostic.details["cipher"].as_str(),
            Some("chacha20poly1305")
        );
        let encrypt_raw =
            std::fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
                .expect("read diagnostics log");
        assert!(!encrypt_raw.contains(&remote_keypair.public_key_b64));
        assert!(!encrypt_raw.contains(&ciphertext_b64));

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
fn private_hive_key_share_send_uses_encrypted_dm_and_redacts_local_record() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    open_node(&state_dir, &db_path).expect("initialize local node");
    let remote_keypair = wattswarm::crypto::generate_private_message_keypair();
    let remote_public_key_b64 = remote_keypair.public_key_b64.clone();
    let shared_secret_b64 = wattswarm::crypto::generate_private_group_secret_b64();
    wattswarm::control::upsert_private_hive_key_record_state(
        &state_dir,
        wattswarm::control::PrivateHiveKeyRecord {
            feed_key: "private.hive".to_owned(),
            scope_hint: "group:dm-key-share".to_owned(),
            group_id: "private.hive:dm-key-share".to_owned(),
            epoch: 1,
            shared_secret_b64: shared_secret_b64.clone(),
            updated_at: 1,
        },
    )
    .expect("save private hive key");
    wattswarm::control::save_peer_metadata_record_state(
        &state_dir,
        &wattswarm::control::PeerMetadataRecord {
            node_id: "node-beta".to_owned(),
            network_id: None,
            params_version: None,
            params_hash: None,
            agent_version_raw: None,
            agent_version_prefix: None,
            protocol_version: None,
            observed_addr: None,
            listen_addrs: Vec::new(),
            protocols: Vec::new(),
            handshake_status: "identified".to_owned(),
            last_error: None,
            contact_material: Some(json!({
                "encryption": {
                    "private_message": {
                        "scheme": "wattswarm.private.dm.v1",
                        "key_agreement": "x25519",
                        "cipher": "chacha20poly1305",
                        "public_key_b64": remote_public_key_b64,
                    }
                }
            })),
            contact_material_signature: None,
            contact_material_updated_at: Some(1),
            first_identified_at: 1,
            last_identified_at: 1,
        },
    )
    .expect("save peer encryption metadata");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
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
                    .uri("/api/peers/dm/private-hive-key-shares")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "remote_node_id": "node-beta",
                            "feed_key": "private.hive",
                            "scope_hint": "group:dm-key-share",
                            "display_name": "Agent Beta",
                            "hive_name": "Private Hive",
                            "invite_text": "Hi Agent Beta, you are invited to join the private Hive \"Private Hive\".",
                            "agent_envelope": {
                                "protocol": "google_a2a",
                                "source_agent_id": "agent-alpha",
                                "target_agent_id": "agent-beta",
                                "capability": "hive.private_key_share",
                                "message_json": "{}"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let send_status = send_res.status();
        let send_json = json_from(send_res).await;
        assert_eq!(send_status, StatusCode::OK, "send response: {send_json}");
        assert_eq!(send_json["ok"].as_bool(), Some(true));
        assert_eq!(
            send_json["shared_secret_b64_redacted"].as_bool(),
            Some(true)
        );
        assert!(
            !serde_json::to_string(&send_json)
                .unwrap()
                .contains(&shared_secret_b64)
        );

        let thread_id = send_json["thread_id"].as_str().expect("thread id");
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
        let local_messages = dm_json["messages"].as_array().expect("dm messages");
        assert_eq!(local_messages.len(), 1);
        assert_eq!(
            local_messages[0]["content"]["shared_secret_b64_redacted"].as_bool(),
            Some(true)
        );
        assert_eq!(
            local_messages[0]["content"]["text"].as_str(),
            Some("Hi Agent Beta, you are invited to join the private Hive \"Private Hive\".")
        );
        assert_eq!(
            local_messages[0]["content"]["display_name"].as_str(),
            Some("Agent Beta")
        );
        assert_eq!(
            local_messages[0]["content"]["hive_name"].as_str(),
            Some("Private Hive")
        );
        assert!(
            !serde_json::to_string(&local_messages[0])
                .unwrap()
                .contains(&shared_secret_b64)
        );

        let topic_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/topic/messages?feed_key=wattswarm.dm&scope_hint={}&limit=5",
                        send_json["scope_hint"].as_str().expect("scope hint")
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(topic_res.status(), StatusCode::OK);
        let topic_json = json_from(topic_res).await;
        assert!(
            !serde_json::to_string(&topic_json)
                .unwrap()
                .contains(&shared_secret_b64)
        );
        let diagnostics = wattswarm::network_bridge::list_network_diagnostics(
            &state_dir,
            &wattswarm::network_bridge::DiagnosticFilter {
                phase: Some("private_dm.encrypt".to_owned()),
                ..Default::default()
            },
        )
        .expect("list private dm encryption diagnostics");
        let message_id = send_json["message_id"].as_str().expect("message id");
        assert!(diagnostics.iter().any(|entry| {
            entry.object_kind.as_deref() == Some("peer_dm_message")
                && entry.object_id.as_deref() == Some(message_id)
                && entry.details["encrypted_payload_present"].as_bool() == Some(true)
        }));
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
        let diagnostics_html = String::from_utf8_lossy(&diagnostics_body);
        assert!(
            diagnostics_html.contains(
                r#"<link rel="icon" type="image/png" sizes="64x64" href="/favicon.png">"#
            )
        );
        assert!(diagnostics_html.contains("function formatRawJson(row)"));
        assert!(diagnostics_html.contains("function renderRawJsonDetails(details)"));
        assert!(diagnostics_html.contains("function parseRawJsonString(value)"));
        assert!(diagnostics_html.contains("normalized.endsWith(\"_at\")"));
        assert!(diagnostics_html.contains("Open to render formatted JSON."));

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
            r#"{"id":"diag-1","timestamp_ms":123,"level":"info","component":"wattswarm.network_bridge","category":"gossip","phase":"publish.event","status":"ok","message":"published local event","event_id":"event-1","object_kind":"task","object_id":"task-1","source_node_id":"peer-a","scope_hint":"global","details":{"target_scope_hint":"node:peer-b","feed_key":"task.lifecycle.task-1","subscriber_node_id":"peer-a","topic":"events"}}
{"id":"diag-2","timestamp_ms":124,"level":"info","component":"wattswarm.network_bridge","category":"gossip","phase":"publish.event","status":"ok","message":"published local event","event_id":"event-1","object_kind":"task","object_id":"task-1","source_node_id":"peer-a","scope_hint":"global","details":{"target_scope_hint":"node:peer-b","feed_key":"task.lifecycle.task-1","subscriber_node_id":"peer-a","topic":"events"}}"#,
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
            payload["diagnostics"][0]["scope_hint"].as_str(),
            Some("global")
        );
        assert_eq!(
            payload["diagnostics"][0]["details"]["target_scope_hint"].as_str(),
            Some("node:peer-b")
        );
        assert_eq!(
            payload["network_service_started"].as_bool(),
            Some(false)
        );
        assert_eq!(
            payload["network_service_status"].as_str(),
            Some("stopped")
        );
    });
}

#[test]
fn ui_diagnostics_api_applies_limit_after_mode_filter() {
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
            r#"{"id":"older-error","timestamp_ms":1,"level":"error","component":"wattswarm.network_bridge","category":"callback","phase":"delivery.callback","status":"failed","message":"older callback failed","object_kind":"event","object_id":"event-1","source_node_id":"peer-a","details":{}}
{"id":"older-info","timestamp_ms":2,"level":"info","component":"wattswarm.network_bridge","category":"gossip","phase":"publish.event","status":"ok","message":"published local event","object_kind":"event","object_id":"event-2","source_node_id":"peer-a","details":{}}
{"id":"newer-warn","timestamp_ms":3,"level":"warn","component":"wattswarm.network_bridge","category":"backfill","phase":"request.timeout","status":"failed","message":"backfill request timed out","object_kind":"backfill","object_id":"event-3","source_node_id":"peer-b","details":{}}
{"id":"newest-info","timestamp_ms":4,"level":"info","component":"wattswarm.network_bridge","category":"gossip","phase":"publish.event","status":"ok","message":"newest healthy event","object_kind":"event","object_id":"event-4","source_node_id":"peer-c","details":{}}"#,
        )
        .unwrap();
        let db_path = state_dir.join("ui.state");
        let app = build_app(UiServerState::new(state_dir, db_path));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/diagnostics?mode=errors&limit=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload = json_from(response).await;
        let diagnostics = payload["diagnostics"].as_array().unwrap();
        assert_eq!(diagnostics.len(), 2);
        assert_eq!(diagnostics[0]["id"].as_str(), Some("newer-warn"));
        assert_eq!(diagnostics[1]["id"].as_str(), Some("older-error"));
    });
}

#[test]
fn ui_exposes_local_network_peer_identity_and_listen_addrs() {
    let _guard = env_lock();
    let _p2p_enabled = EnvVarGuard::set("WATTSWARM_P2P_ENABLED", "true");
    let _listen_addrs =
        EnvVarGuard::set("WATTSWARM_P2P_LISTEN_ADDRS", "127.0.0.1:4010,0.0.0.0:4011");
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
                Value::String("127.0.0.1:4010".to_owned()),
                Value::String("0.0.0.0:4011".to_owned()),
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
fn network_discovery_bootnodes_returns_persisted_urls() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    wattswarm::control::save_discovery_bootnode_urls_state(
        &state_dir,
        &[
            " https://bootstrap-a.example.com/ ".to_owned(),
            "https://bootstrap-b.example.com/api/network/discovery".to_owned(),
        ],
    )
    .expect("save discovery bootnode urls");

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
                    .uri("/api/network/discovery/bootnodes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let json = json_from(res).await;
        assert_eq!(json["ok"].as_bool(), Some(true));
        assert_eq!(
            json["urls"].as_array().cloned().unwrap_or_default(),
            vec![
                json!("https://bootstrap-a.example.com"),
                json!("https://bootstrap-b.example.com/api/network/discovery")
            ]
        );
    });
}

#[test]
fn network_discovery_bootnode_accepts_signed_records_and_filters_queries() {
    let _guard = env_lock();
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
        let identity = NodeIdentity::from_seed([91; 32]);
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let node_id = identity.node_id();
        let mut body = DiscoveryNodeRecordBody::new(
            "mainnet:test",
            node_id.clone(),
            node_id.clone(),
            1,
            now_ms,
        );
        body.geo = Some(DiscoveryGeo {
            latitude: 37.78,
            longitude: -122.41,
            radius_km: 50.0,
        });
        body.capabilities =
            DiscoveryRecordCapabilities::new(["wattswarm.node", "discovery.bootnode"]).unwrap();
        body.topic_providers.push(DiscoveryTopicProvider {
            feed_key: "sydney-weather".to_owned(),
            scope_hint: "group:sydney-weather".to_owned(),
            capabilities: DiscoveryTopicProviderCapabilities::local_history_provider(),
            updated_at_ms: now_ms,
        });
        let agent_card = json!({
            "name": "Agent Alpha",
            "metadata": {
                "agent_id": "did:key:zAgentAlpha",
                "node_id": node_id.clone(),
                "public_id": "@agent-alpha",
                "display_name": "Agent Alpha",
            },
        });
        body.source_agent_card = Some(SourceAgentCard {
            agent_id: "did:key:zAgentAlpha".to_owned(),
            node_id: Some(node_id.clone()),
            card_hash: format!(
                "sha256:{}",
                sha256_hex(serde_jcs::to_string(&agent_card).unwrap().as_bytes())
            ),
            issued_at: now_ms,
            card: agent_card,
            signature: Some("agent-card-signature".to_owned()),
        });
        let record = SignedDiscoveryNodeRecord::sign(body, &identity).unwrap();

        let announce_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/network/discovery/records")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&record).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(announce_res.status(), StatusCode::OK);
        let announce_json = json_from(announce_res).await;
        assert_eq!(announce_json["ok"].as_bool(), Some(true));
        assert_eq!(announce_json["status"].as_str(), Some("inserted"));
        assert_eq!(announce_json["node_id"].as_str(), Some(node_id.as_str()));

        let nearby_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/discovery/nearby?network_id=mainnet:test&latitude=37.77&longitude=-122.42&radius_km=50&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(nearby_res.status(), StatusCode::OK);
        let nearby_json = json_from(nearby_res).await;
        assert_eq!(
            nearby_json["records"][0]["record"]["body"]["node_id"].as_str(),
            Some(node_id.as_str())
        );

        let capability_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/discovery/capability?network_id=mainnet:test&capability=WATTSWARM.NODE")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(capability_res.status(), StatusCode::OK);
        let capability_json = json_from(capability_res).await;
        assert_eq!(
            capability_json["records"][0]["body"]["node_id"].as_str(),
            Some(node_id.as_str())
        );

        let topic_provider_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/discovery/topic-providers?network_id=mainnet:test&feed_key=sydney-weather&scope_hint=group%3Asydney-weather")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(topic_provider_res.status(), StatusCode::OK);
        let topic_provider_json = json_from(topic_provider_res).await;
        assert_eq!(
            topic_provider_json["records"][0]["body"]["node_id"].as_str(),
            Some(node_id.as_str())
        );

        let topic_provider_batch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/network/discovery/topic-providers/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "network_id": "mainnet:test",
                            "queries": [{
                                "feed_key": "sydney-weather",
                                "scope_hint": "group:sydney-weather",
                            }],
                            "limit": 10,
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(topic_provider_batch_res.status(), StatusCode::OK);
        let topic_provider_batch_json = json_from(topic_provider_batch_res).await;
        assert_eq!(
            topic_provider_batch_json["records"][0]["body"]["node_id"].as_str(),
            Some(node_id.as_str())
        );

        let agent_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/discovery/agent?network_id=mainnet:test&public_id=%40agent-alpha")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(agent_res.status(), StatusCode::OK);
        let agent_json = json_from(agent_res).await;
        assert_eq!(
            agent_json["records"][0]["body"]["node_id"].as_str(),
            Some(node_id.as_str())
        );
        assert_eq!(
            agent_json["records"][0]["body"]["source_agent_card"]["card"]["metadata"]
                ["public_id"]
                .as_str(),
            Some("@agent-alpha")
        );

        let display_name_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/discovery/agent?network_id=mainnet:test&display_name=%40Agent%20Alpha")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(display_name_res.status(), StatusCode::OK);
        let display_name_json = json_from(display_name_res).await;
        assert_eq!(
            display_name_json["records"][0]["body"]["node_id"].as_str(),
            Some(node_id.as_str())
        );
        assert_eq!(
            display_name_json["records"][0]["body"]["source_agent_card"]["card"]["metadata"]
                ["display_name"]
                .as_str(),
            Some("Agent Alpha")
        );

        let mut tampered = record.clone();
        tampered.body.network_id = "mainnet:other".to_owned();
        let tampered_res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/network/discovery/records")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&tampered).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tampered_res.status(), StatusCode::BAD_REQUEST);
        let tampered_json = json_from(tampered_res).await;
        assert_eq!(tampered_json["ok"].as_bool(), Some(false));
    });
}

#[test]
fn network_discovery_bootnode_recovers_from_corrupt_records_file() {
    let _guard = env_lock();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let records_path = state_dir.join("discovery_records_v1.json");
        std::fs::write(
            &records_path,
            "[{\"body\":{\"protocol_version\":\"wattswarm-discovery/1\"",
        )
        .unwrap();
        let db_path = state_dir.join("ui.state");
        let app = build_app(UiServerState::new(state_dir.clone(), db_path));

        let nearby_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/network/discovery/nearby?network_id=mainnet:test&latitude=37.77&longitude=-122.42&radius_km=50&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(nearby_res.status(), StatusCode::OK);
        let nearby_json = json_from(nearby_res).await;
        assert_eq!(nearby_json["records"].as_array().map(Vec::len), Some(0));
        assert!(!records_path.exists());
        assert!(state_dir.join("discovery_records_v1.json.corrupt").exists());

        let identity = NodeIdentity::from_seed([92; 32]);
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let node_id = identity.node_id();
        let body = DiscoveryNodeRecordBody::new(
            "mainnet:test",
            node_id.clone(),
            node_id.clone(),
            1,
            now_ms,
        );
        let record = SignedDiscoveryNodeRecord::sign(body, &identity).unwrap();
        let announce_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/network/discovery/records")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&record).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(announce_res.status(), StatusCode::OK);

        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&records_path).unwrap()).unwrap();
        assert_eq!(saved[0]["body"]["node_id"].as_str(), Some(node_id.as_str()));
        let leftover_tmp = std::fs::read_dir(&state_dir)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"));
        assert!(!leftover_tmp, "atomic save must not leave temp files");
    });
}

#[test]
fn ui_exposes_network_join_manifest() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let contact_a = format!(
        "{}@127.0.0.1:4001",
        NodeIdentity::from_seed([91_u8; 32]).node_id()
    );
    let contact_b = format!(
        "{}@127.0.0.1:4002",
        NodeIdentity::from_seed([92_u8; 32]).node_id()
    );
    let contacts = format!("{contact_a},{contact_b}");
    let _urls_guard = EnvVarGuard::set(
        "WATTSWARM_PUBLIC_BOOTSTRAP_URLS",
        "https://bootstrap.wattetheria.com",
    );
    let _contacts_guard = EnvVarGuard::set("WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS", &contacts);
    let _gateway_guard = EnvVarGuard::set(
        "WATTSWARM_PUBLIC_GATEWAY_URLS",
        "https://gateway.wattetheria.com",
    );
    let _discovery_guard = EnvVarGuard::set(
        "WATTSWARM_PUBLIC_DISCOVERY_URLS",
        "https://bootstrap.wattetheria.com/api/network/discovery",
    );
    let _relay_guard = EnvVarGuard::set(
        "WATTSWARM_IROH_RELAY_URLS",
        "https://relay.wattetheria.com,https://relay2.wattetheria.com",
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
            Some(contact_a.as_str())
        );
        assert_eq!(
            json["bootstrap_contacts"][1].as_str(),
            Some(contact_b.as_str())
        );
        assert_eq!(
            json["gateway_urls"][0].as_str(),
            Some("https://gateway.wattetheria.com")
        );
        assert_eq!(
            json["discovery_urls"][0].as_str(),
            Some("https://bootstrap.wattetheria.com/api/network/discovery")
        );
        assert_eq!(
            json["relay_urls"][0].as_str(),
            Some("https://relay.wattetheria.com")
        );
        assert_eq!(
            json["relay_urls"][1].as_str(),
            Some("https://relay2.wattetheria.com")
        );
        assert!(json.get("servicenet_urls").is_none());
    });
}

#[test]
fn ui_join_manifest_expands_bare_bootstrap_node_ids_to_relay_only_contacts() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let bootstrap_node_id = NodeIdentity::from_seed([93_u8; 32]).node_id();
    let _contacts_guard =
        EnvVarGuard::set("WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS", &bootstrap_node_id);
    let _relay_guard = EnvVarGuard::set(
        "WATTSWARM_IROH_RELAY_URLS",
        "https://relay.wattetheria.com,https://relay2.wattetheria.com",
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
                    .uri("/.well-known/wattswarm/join.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = json_from(res).await;
        let contact = serde_json::from_str::<Value>(
            json["bootstrap_contacts"][0]
                .as_str()
                .expect("bootstrap contact"),
        )
        .expect("contact json");
        assert_eq!(
            contact["node_id"].as_str(),
            Some(bootstrap_node_id.as_str())
        );
        assert_eq!(
            contact["peer_id"].as_str(),
            Some(bootstrap_node_id.as_str())
        );
        assert_eq!(contact["listen_addrs"].as_array().unwrap().len(), 0);
        assert_eq!(
            contact["transports"][0]["extra"]["direct_addrs"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            contact["transports"][0]["extra"]["relay_urls"][0].as_str(),
            Some("https://relay.wattetheria.com")
        );
        assert_eq!(
            contact["transports"][0]["extra"]["relay_urls"][1].as_str(),
            Some("https://relay2.wattetheria.com")
        );
    });
}

#[test]
fn ui_join_manifest_builds_relay_only_contact_without_exporting_local_iroh_endpoint() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _contacts_guard = EnvVarGuard::set("WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS", "");
    let _relay_guard =
        EnvVarGuard::set("WATTSWARM_IROH_RELAY_URLS", "https://relay.wattetheria.com");
    let _direct_guard = EnvVarGuard::set("WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS", "false");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let seed_hex = [45_u8; 32]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::fs::write(state_dir.join("node_seed.hex"), seed_hex).unwrap();
    let db_path = state_dir.join("ui.state");
    let genesis = NodeIdentity::from_seed([45_u8; 32]);
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
                    .uri("/.well-known/wattswarm/join.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let json = json_from(res).await;
        let contact = serde_json::from_str::<Value>(
            json["bootstrap_contacts"][0]
                .as_str()
                .expect("bootstrap contact"),
        )
        .expect("contact json");
        assert_eq!(contact["listen_addrs"].as_array().unwrap().len(), 0);
        assert_eq!(
            contact["transports"][0]["extra"]["direct_addrs"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            contact["transports"][0]["extra"]["relay_urls"][0].as_str(),
            Some("https://relay.wattetheria.com")
        );
    });
}
