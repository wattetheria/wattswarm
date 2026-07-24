use super::*;

#[test]
fn ui_exposes_egress_agent_config_and_google_a2a_card() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let get_default_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/egress-agent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_default_res.status(), StatusCode::OK);
        let get_default_json = json_from(get_default_res).await;
        assert_eq!(
            get_default_json["config"]["agent_id"].as_str(),
            Some("egress-agent")
        );
        assert_eq!(get_default_json["config"]["enabled"].as_bool(), Some(false));

        let disabled_well_known_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/.well-known/agent.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disabled_well_known_res.status(), StatusCode::BAD_REQUEST);

        let save_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/egress-agent")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "enabled": true,
                            "agent_id": "twilio-egress",
                            "display_name": "Twilio Bridge",
                            "description": "Bridges outbound SMS requests through a local Twilio adapter.",
                            "protocol": "google_a2a",
                            "mode": "direct_gateway",
                            "executor": "twilio-http",
                            "profile": "default",
                            "public_base_url": "https://node.example.com",
                            "publish_to_network": true,
                            "accept_inbound_invocations": true,
                            "skills": [
                                {
                                    "id": "send_sms",
                                    "name": "Send SMS",
                                    "description": "Sends outbound SMS via a local adapter.",
                                    "tags": ["twilio", "sms"]
                                }
                            ]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);
        let save_json = json_from(save_res).await;
        assert_eq!(save_json["config"]["enabled"].as_bool(), Some(true));
        assert_eq!(
            save_json["config"]["executor"].as_str(),
            Some("twilio-http")
        );

        let get_saved_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/egress-agent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let get_saved_json = json_from(get_saved_res).await;
        assert_eq!(
            get_saved_json["config"]["agent_id"].as_str(),
            Some("twilio-egress")
        );
        assert_eq!(
            get_saved_json["config"]["skills"][0]["id"].as_str(),
            Some("send_sms")
        );

        let card_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/a2a/google/agent-card")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(card_res.status(), StatusCode::OK);
        let card_json = json_from(card_res).await;
        assert_eq!(card_json["protocol"].as_str(), Some("google_a2a"));
        assert_eq!(card_json["name"].as_str(), Some("Twilio Bridge"));
        assert_eq!(
            card_json["url"].as_str(),
            Some("https://node.example.com/a2a/google")
        );
        assert_eq!(
            card_json["metadata"]["agent_id"].as_str(),
            Some("twilio-egress")
        );
        assert_eq!(
            card_json["skills"][0]["id"].as_str(),
            Some("send_sms")
        );

        let well_known_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/.well-known/agent.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(well_known_res.status(), StatusCode::OK);
        let well_known_json = json_from(well_known_res).await;
        assert_eq!(well_known_json, card_json);
    });
}

#[test]
fn ui_google_a2a_message_send_supports_direct_and_group_modes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    init_test_run_queue(&state_dir);
    let app = build_app(UiServerState::new(state_dir.clone(), db_path));
    let execute_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let runtime_server = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: "{}".to_owned(),
        capabilities_body: json!({
            "task_types": ["a2a_invoke"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
        execute_body: json!({
            "candidate_output": {"decision":"SENT","answer":"sent"},
            "evidence_inline": [],
            "evidence_refs": []
        })
        .to_string(),
        execute_requests: Some(Arc::clone(&execute_requests)),
    });
    wait_for_stub_listener(&runtime_server);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let disabled_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/a2a/google/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": "req-disabled",
                            "method": "message/send",
                            "params": {
                                "targetAgentId": "egress-agent",
                                "capability": "send_sms",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type":"data","data":{"to":"+61","body":"hello"}}]
                                }
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disabled_res.status(), StatusCode::BAD_REQUEST);

        let save_direct_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/egress-agent")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "enabled": true,
                            "agent_id": "twilio-egress",
                            "display_name": "Twilio Bridge",
                            "description": "Bridge agent",
                            "protocol": "google_a2a",
                            "mode": "direct_gateway",
                            "executor": "twilio-http",
                            "profile": "default",
                            "public_base_url": "https://node.example.com",
                            "publish_to_network": true,
                            "accept_inbound_invocations": true,
                            "skills": [{"id":"send_sms","name":"Send SMS"}]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_direct_res.status(), StatusCode::OK);

        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "name": "twilio-http",
                            "base_url": runtime_server.base_url()
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let card_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/a2a/google/agent-card")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(card_res.status(), StatusCode::OK);
        let card_json = json_from(card_res).await;
        assert_eq!(
            card_json["metadata"]["supported_settlement_rails"][0]["rail"].as_str(),
            Some("x402")
        );

        let direct_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/a2a/google/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": "req-direct",
                            "method": "message/send",
                            "params": {
                                "targetAgentId": "twilio-egress",
                                "capability": "send_sms",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type":"data","data":{"to":"+61400000000","body":"hello"}}]
                                },
                                "extensions": {
                                    "auth_proof": {"kind":"did"},
                                    "payment_proof": {"kind":"watt"},
                                    "settlement": {
                                        "layer": "web3",
                                        "rail": "x402",
                                        "request": {
                                        "facilitator": "https://facilitator.example.com/pay",
                                        "network": "base-sepolia",
                                        "pay_to": "0xabc123",
                                        "payment_account_ref": "payment-account-123"
                                        }
                                    },
                                    "signature": "sig-1"
                                }
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let direct_status = direct_res.status();
        let direct_bytes = to_bytes(direct_res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            direct_status,
            StatusCode::OK,
            "direct gateway body: {}",
            String::from_utf8_lossy(&direct_bytes)
        );
        let direct_json: Value = serde_json::from_slice(&direct_bytes).unwrap();
        assert_eq!(direct_json["jsonrpc"].as_str(), Some("2.0"));
        assert_eq!(direct_json["id"].as_str(), Some("req-direct"));
        assert_eq!(direct_json["result"]["status"].as_str(), Some("completed"));
        assert_eq!(
            direct_json["result"]["extensions"]["receipt"]["mode"].as_str(),
            Some("direct_gateway")
        );
        assert_eq!(
            direct_json["result"]["extensions"]["receipt"]["settlement"]["rail"].as_str(),
            Some("x402")
        );
        assert_eq!(
            direct_json["result"]["artifacts"][0]["candidate_output"]["decision"].as_str(),
            Some("SENT")
        );
        let requests = execute_requests.lock().expect("lock execute requests");
        let execute_request = requests.last().expect("captured execute request");
        assert_eq!(
            execute_request["inputs"]["settlement"]["rail"].as_str(),
            Some("x402")
        );
        assert_eq!(
            execute_request["inputs"]["settlement"]["request"]["protocol"].as_str(),
            Some("x402")
        );
        assert_eq!(
            execute_request["inputs"]["settlement"]["request"]["payment_account_ref"].as_str(),
            Some("payment-account-123")
        );
        drop(requests);

        let save_group_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/egress-agent")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "enabled": true,
                            "agent_id": "team-egress",
                            "display_name": "Team Bridge",
                            "protocol": "google_a2a",
                            "mode": "group_representative",
                            "profile": "default",
                            "public_base_url": "https://node.example.com"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_group_res.status(), StatusCode::OK);

        let group_run_id = "run-a2a-group";
        let group_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/a2a/google/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": "req-group",
                            "method": "message/send",
                            "params": {
                                "targetAgentId": "team-egress",
                                "capability": "resume_review",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type":"data","data":{"resume":"alex"}}]
                                },
                                "extensions": {
                                    "settlement": {
                                        "layer": "web3",
                                        "rail": "x402",
                                        "request": {
                                            "pay_to": "0xdef456"
                                        }
                                    },
                                    "kickoff": true,
                                    "run_spec": sample_run_spec(group_run_id)
                                }
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(group_res.status(), StatusCode::OK);
        let group_json = json_from(group_res).await;
        assert_eq!(group_json["id"].as_str(), Some("req-group"));
        assert_eq!(group_json["result"]["status"].as_str(), Some("accepted"));
        assert_eq!(
            group_json["result"]["extensions"]["receipt"]["mode"].as_str(),
            Some("group_representative")
        );
        assert_eq!(
            group_json["result"]["extensions"]["receipt"]["settlement"]["rail"].as_str(),
            Some("x402")
        );
        assert_eq!(
            group_json["result"]["extensions"]["receipt"]["run_id"].as_str(),
            Some(group_run_id)
        );

        let run_watch_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/watch/{group_run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run_watch_res.status(), StatusCode::OK);
        let run_watch_json = json_from(run_watch_res).await;
        assert_eq!(
            run_watch_json["watch"]["run_id"].as_str(),
            Some(group_run_id)
        );
    });
}

#[test]
fn ui_exposes_wattetheria_sync_http_boundaries() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    init_test_run_queue(&state_dir);
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let runtime_server = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: "{}".to_owned(),
        capabilities_body: json!({
            "task_types": ["a2a_invoke", "resume_review"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
        execute_body: json!({
            "candidate_output": {"decision":"PASS","answer":"sync-ok"},
            "evidence_inline": [],
            "evidence_refs": []
        })
        .to_string(),
        execute_requests: None,
    });
    wait_for_stub_listener(&runtime_server);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let node_up_res = app
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
        assert_eq!(node_up_res.status(), StatusCode::OK);

        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "name": "rt-sync",
                            "base_url": runtime_server.base_url()
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let network_snapshot_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/wattetheria/network/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(network_snapshot_res.status(), StatusCode::OK);
        let network_snapshot = json_from(network_snapshot_res).await;
        assert_eq!(network_snapshot["running"].as_bool(), Some(true));
        assert!(network_snapshot["node_id"].as_str().is_some());
        assert!(network_snapshot["org_id"].as_str().is_some());

        let subscribe_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/subscriptions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "active": true
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(subscribe_res.status(), StatusCode::OK);

        let publish_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wattetheria/brain/publish-topic")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {"text": "hello from wattetheria brain"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(publish_res.status(), StatusCode::OK);

        let seed_state_dir = state_dir.clone();
        tokio::task::spawn_blocking(move || {
            wattswarm::control::save_peer_relationship_record_state(
                &seed_state_dir,
                &wattswarm::control::PeerRelationshipRecord {
                    remote_node_id: "node-sync-peer".to_string(),
                    relationship_state: wattswarm::control::PeerRelationshipState::Accepted,
                    last_action: wattswarm::control::PeerRelationshipAction::Accept,
                    initiated_by: wattswarm::control::PeerRelationshipInitiator::Remote,
                    agent_envelope: None,
                    requested_at: Some(1),
                    responded_at: Some(2),
                    blocked_at: None,
                    cleared_at: None,
                    updated_at: 2,
                },
            )
            .expect("save relationship");
            wattswarm::control::save_peer_dm_thread_record_state(
                &seed_state_dir,
                &wattswarm::control::PeerDmThreadRecord {
                    remote_node_id: "node-sync-peer".to_string(),
                    thread_id: "dm:node-sync-peer".to_string(),
                    thread_kind: wattswarm::control::PeerDmThreadKind::Direct,
                    session_state: wattswarm::control::PeerDmSessionState::Ready,
                    relationship_established_at: Some(2),
                    created_at: 2,
                    updated_at: 3,
                    last_message_at: Some(4),
                },
            )
            .expect("save dm thread");
            wattswarm::control::save_peer_dm_message_record_state(
                &seed_state_dir,
                &wattswarm::control::PeerDmMessageRecord {
                    thread_id: "dm:node-sync-peer".to_string(),
                    message_id: "sync-dm-1".to_string(),
                    remote_node_id: "node-sync-peer".to_string(),
                    message_kind: wattswarm::control::PeerDmMessageKind::Message,
                    direction: wattswarm::control::PeerDmDirection::Inbound,
                    delivery_state: wattswarm::control::PeerDmDeliveryState::Delivered,
                    a2a_protocol: "google_a2a".to_string(),
                    agent_envelope: None,
                    content: json!({"text":"hello from sync"}),
                    created_at: 4,
                    acknowledged_at: None,
                },
            )
            .expect("save dm message");
            wattswarm::control::save_agent_payment_record_state(
                &seed_state_dir,
                &wattswarm::control::AgentPaymentRecord {
                    payment_id: "payment-sync-1".to_string(),
                    remote_node_id: "node-sync-peer".to_string(),
                    summary_id: "summary-payment-sync-1".to_string(),
                    message_kind: "payment_request".to_string(),
                    payment: json!({
                        "payment_id": "payment-sync-1",
                        "sender_did": "did:key:remote",
                        "recipient_did": "did:key:local",
                        "sender_public_id": "remote-public",
                        "recipient_public_id": "local-public",
                        "remote_node_id": "node-sync-peer",
                        "amount": "42",
                        "currency": "USDT",
                        "rail": "x402",
                        "layer": "web3",
                        "status": "proposed",
                        "proposed_at": 10
                    }),
                    updated_at: 10,
                },
            )
            .expect("save payment");
        })
        .await
        .unwrap();

        let sample_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/sample?task_id=task-wattetheria-grpc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sample_res.status(), StatusCode::OK);
        let contract = json_from(sample_res).await["contract"].clone();
        let submit_task_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({ "contract": contract })).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_task_res.status(), StatusCode::OK);
        let publish_json = json_from(publish_res).await;
        assert_eq!(publish_json["ok"].as_bool(), Some(true));
        assert_eq!(publish_json["feed_key"].as_str(), Some("crew.chat"));

        let topic_activity_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/wattetheria/topic/activity?feed_key=crew.chat&scope_hint=group:crew-7&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(topic_activity_res.status(), StatusCode::OK);
        let topic_activity = json_from(topic_activity_res).await;
        let network_id = topic_activity["network_id"].as_str().unwrap();
        assert_eq!(
            topic_activity["messages"][0]["content"]["text"].as_str(),
            Some("hello from wattetheria brain")
        );
        assert_eq!(
            topic_activity["messages"][0]["network_id"].as_str(),
            Some(network_id)
        );
        assert_eq!(topic_activity["feed_key"].as_str(), Some("crew.chat"));

        let other_network_topic_activity_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/wattetheria/topic/activity?network_id=other-net&feed_key=crew.chat&scope_hint=group:crew-7&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(other_network_topic_activity_res.status(), StatusCode::OK);
        let other_network_topic_activity = json_from(other_network_topic_activity_res).await;
        assert_eq!(
            other_network_topic_activity["messages"].as_array().unwrap().len(),
            0
        );

        let run_id = "run-wattetheria-http";
        let submit_run_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wattetheria/brain/submit-run")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "kickoff": true,
                            "spec": sample_run_spec(run_id)
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_run_res.status(), StatusCode::OK);
        let submit_run_json = json_from(submit_run_res).await;
        assert_eq!(submit_run_json["run_id"].as_str(), Some(run_id));
        assert_eq!(submit_run_json["kicked_off"].as_bool(), Some(true));

        let sample_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/sample?task_id=task-sync-http")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sample_res.status(), StatusCode::OK);
        let sample_json = json_from(sample_res).await;
        let contract = sample_json["contract"].clone();

        let submit_task_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"contract": contract})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_task_res.status(), StatusCode::OK);

        let task_decision_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/wattetheria/task/decision/task-sync-http")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(task_decision_res.status(), StatusCode::OK);
        let task_decision_json = json_from(task_decision_res).await;
        assert_eq!(
            task_decision_json["task_id"].as_str(),
            Some("task-sync-http")
        );

        let run_real_sample_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/sample?task_id=task-sync-http-real")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run_real_sample_res.status(), StatusCode::OK);
        let run_real_contract = json_from(run_real_sample_res).await["contract"].clone();
        let run_real_file = state_dir.join("task-sync-http-real.json");
        std::fs::write(
            &run_real_file,
            serde_json::to_vec(&run_real_contract).expect("serialize run-real contract"),
        )
        .expect("write run-real contract");

        let run_real_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wattetheria/brain/run-task-real")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "executor": "rt-sync",
                            "profile": "default",
                            "file_path": run_real_file
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let run_real_status = run_real_res.status();
        let run_real_bytes = to_bytes(run_real_res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            run_real_status,
            StatusCode::OK,
            "brain run-task-real body: {}",
            String::from_utf8_lossy(&run_real_bytes)
        );
        let run_real_json: Value = serde_json::from_slice(&run_real_bytes).unwrap();
        assert_eq!(
            run_real_json["result"]["candidate_output"]["decision"].as_str(),
            Some("PASS")
        );

        let run_result_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/wattetheria/run/result/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run_result_res.status(), StatusCode::OK);
        let run_result_json = json_from(run_result_res).await;
        assert_eq!(
            run_result_json["result"]["run_id"].as_str(),
            Some(run_id)
        );

        let run_events_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/wattetheria/run/events/{run_id}?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run_events_res.status(), StatusCode::OK);
        let run_events_json = json_from(run_events_res).await;
        let run_event_types: Vec<_> = run_events_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["event_type"].as_str())
            .collect();
        assert!(run_event_types.contains(&"RUN_CREATED"));
        assert!(run_event_types.contains(&"RUN_KICKOFF"));

        let knowledge_export_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wattetheria/knowledge/export")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "task_id": "task-sync-http"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(knowledge_export_res.status(), StatusCode::OK);
        let knowledge_export_json = json_from(knowledge_export_res).await;
        assert_eq!(knowledge_export_json["ok"].as_bool(), Some(true));
        assert!(knowledge_export_json["knowledge"].is_object());

        let task_run_snapshot_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/wattetheria/task-run/snapshot?task_limit=10&run_limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(task_run_snapshot_res.status(), StatusCode::OK);
        let task_run_snapshot = json_from(task_run_snapshot_res).await;
        let recent_runs = task_run_snapshot["recent_runs"].as_array().unwrap();
        assert!(recent_runs.iter().any(|run| run["run_id"].as_str() == Some(run_id)));
        let recent_tasks = task_run_snapshot["recent_tasks"].as_array().unwrap();
        assert!(recent_tasks
            .iter()
            .any(|task| task["task_id"].as_str() == Some("task-sync-http")));
    });
}

#[test]
fn ui_exposes_wattetheria_sync_grpc_streams() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    init_test_run_queue(&state_dir);
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let node_up_res = app
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
        assert_eq!(node_up_res.status(), StatusCode::OK);

        let publish_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wattetheria/brain/publish-topic")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-stream",
                            "content": {"text": "stream me"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_res.status(), StatusCode::OK);

        let seed_state_dir = state_dir.clone();
        tokio::task::spawn_blocking(move || {
            wattswarm::control::save_peer_relationship_record_state(
                &seed_state_dir,
                &wattswarm::control::PeerRelationshipRecord {
                    remote_node_id: "node-sync-peer".to_string(),
                    relationship_state: wattswarm::control::PeerRelationshipState::Accepted,
                    last_action: wattswarm::control::PeerRelationshipAction::Accept,
                    initiated_by: wattswarm::control::PeerRelationshipInitiator::Remote,
                    agent_envelope: None,
                    requested_at: Some(1),
                    responded_at: Some(2),
                    blocked_at: None,
                    cleared_at: None,
                    updated_at: 2,
                },
            )
            .expect("save relationship");
            wattswarm::control::save_peer_dm_thread_record_state(
                &seed_state_dir,
                &wattswarm::control::PeerDmThreadRecord {
                    remote_node_id: "node-sync-peer".to_string(),
                    thread_id: "dm:node-sync-peer".to_string(),
                    thread_kind: wattswarm::control::PeerDmThreadKind::Direct,
                    session_state: wattswarm::control::PeerDmSessionState::Ready,
                    relationship_established_at: Some(2),
                    created_at: 2,
                    updated_at: 3,
                    last_message_at: Some(4),
                },
            )
            .expect("save dm thread");
            wattswarm::control::save_peer_dm_message_record_state(
                &seed_state_dir,
                &wattswarm::control::PeerDmMessageRecord {
                    thread_id: "dm:node-sync-peer".to_string(),
                    message_id: "sync-dm-1".to_string(),
                    remote_node_id: "node-sync-peer".to_string(),
                    message_kind: wattswarm::control::PeerDmMessageKind::Message,
                    direction: wattswarm::control::PeerDmDirection::Inbound,
                    delivery_state: wattswarm::control::PeerDmDeliveryState::Delivered,
                    a2a_protocol: "google_a2a".to_string(),
                    agent_envelope: None,
                    content: json!({"text":"hello from sync"}),
                    created_at: 4,
                    acknowledged_at: None,
                },
            )
            .expect("save dm message");
            wattswarm::control::save_agent_payment_record_state(
                &seed_state_dir,
                &wattswarm::control::AgentPaymentRecord {
                    payment_id: "payment-sync-1".to_string(),
                    remote_node_id: "node-sync-peer".to_string(),
                    summary_id: "summary-payment-sync-1".to_string(),
                    message_kind: "payment_request".to_string(),
                    payment: json!({
                        "payment_id": "payment-sync-1",
                        "sender_did": "did:key:remote",
                        "recipient_did": "did:key:local",
                        "sender_public_id": "remote-public",
                        "recipient_public_id": "local-public",
                        "remote_node_id": "node-sync-peer",
                        "amount": "42",
                        "currency": "USDT",
                        "rail": "x402",
                        "layer": "web3",
                        "status": "proposed",
                        "proposed_at": 10
                    }),
                    updated_at: 10,
                },
            )
            .expect("save payment");
        })
        .await
        .unwrap();

        let grpc_addr = reserve_local_addr();
        let grpc_state = UiServerState::new(state_dir.clone(), db_path.clone());
        let grpc_task = tokio::spawn(async move {
            wattetheria_sync::serve_grpc(grpc_state, grpc_addr.to_string())
                .await
                .expect("serve grpc");
        });

        let endpoint = format!("http://{grpc_addr}");
        let mut client = loop {
            match WattetheriaSyncServiceClient::connect(endpoint.clone()).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };

        let geo_response = client
            .update_startup_geo(GrpcRequest::new(UpdateStartupGeoRequest {
                latitude: -33.8399,
                longitude: 151.0583,
            }))
            .await
            .expect("update startup geo")
            .into_inner();
        assert!(geo_response.updated);
        let startup_config = wattswarm::startup_config::load_startup_config(
            &wattswarm::startup_config::startup_config_path(&state_dir),
        )
        .expect("load startup config after geo update");
        assert!(
            startup_config
                .latitude
                .is_some_and(|value| (value - -33.8399).abs() < f64::EPSILON)
        );
        assert!(
            startup_config
                .longitude
                .is_some_and(|value| (value - 151.0583).abs() < f64::EPSILON)
        );

        let network_frame = tokio::time::timeout(Duration::from_secs(15), async {
            client
                .stream_network_projection(GrpcRequest::new(ProjectionStreamRequest {
                    poll_interval_ms: 250,
                    limit: 10,
                    feed_key: String::new(),
                    scope_hint: String::new(),
                    subscriber_node_id: String::new(),
                    network_id: String::new(),
                }))
                .await
                .unwrap()
                .into_inner()
                .message()
                .await
        })
        .await
        .expect("network stream timeout")
        .expect("network stream response")
        .expect("network frame");
        assert_eq!(network_frame.kind, "network_projection");
        let network_json: Value = serde_json::from_str(&network_frame.json_payload).unwrap();
        assert_eq!(network_json["running"].as_bool(), Some(true));

        let topic_frame = tokio::time::timeout(Duration::from_secs(10), async {
            client
                .stream_topic_activity(GrpcRequest::new(ProjectionStreamRequest {
                    poll_interval_ms: 250,
                    limit: 10,
                    feed_key: "crew.chat".to_owned(),
                    scope_hint: "group:crew-stream".to_owned(),
                    subscriber_node_id: String::new(),
                    network_id: String::new(),
                }))
                .await
                .unwrap()
                .into_inner()
                .message()
                .await
        })
        .await
        .expect("topic stream timeout")
        .expect("topic stream response")
        .expect("topic frame");
        assert_eq!(topic_frame.kind, "topic_activity");
        let topic_json: Value = serde_json::from_str(&topic_frame.json_payload).unwrap();
        assert_eq!(
            topic_json["messages"][0]["content"]["text"].as_str(),
            Some("stream me")
        );

        let task_run_frame = tokio::time::timeout(Duration::from_secs(10), async {
            client
                .stream_task_run_projection(GrpcRequest::new(ProjectionStreamRequest {
                    poll_interval_ms: 250,
                    limit: 10,
                    feed_key: String::new(),
                    scope_hint: String::new(),
                    subscriber_node_id: String::new(),
                    network_id: String::new(),
                }))
                .await
                .unwrap()
                .into_inner()
                .message()
                .await
        })
        .await
        .expect("task/run stream timeout")
        .expect("task/run stream response")
        .expect("task/run frame");
        assert_eq!(task_run_frame.kind, "task_run_projection");
        let task_run_json: Value = serde_json::from_str(&task_run_frame.json_payload).unwrap();
        assert!(task_run_json["recent_tasks"].is_array());
        assert!(task_run_json["recent_runs"].is_array());

        let social_frame = tokio::time::timeout(Duration::from_secs(10), async {
            client
                .stream_social_projection(GrpcRequest::new(ProjectionStreamRequest {
                    poll_interval_ms: 250,
                    limit: 10,
                    feed_key: String::new(),
                    scope_hint: String::new(),
                    subscriber_node_id: String::new(),
                    network_id: String::new(),
                }))
                .await
                .unwrap()
                .into_inner()
                .message()
                .await
        })
        .await
        .expect("social stream timeout")
        .expect("social stream response")
        .expect("social frame");
        assert_eq!(social_frame.kind, "social_projection");
        let social_json: Value = serde_json::from_str(&social_frame.json_payload).unwrap();
        let social_relationships = social_json["relationships"].as_array().unwrap();
        assert!(
            social_relationships
                .iter()
                .any(|relationship| relationship["remote_node_id"].as_str()
                    == Some("node-sync-peer"))
        );
        let social_messages = social_json["messages"].as_array().unwrap();
        assert!(social_messages.iter().any(|message| {
            message["message_id"].as_str() == Some("sync-dm-1")
                && message["a2a_protocol"].as_str() == Some("google_a2a")
        }));

        grpc_task.abort();
        let _ = grpc_task.await;
    });
}

#[test]
fn ui_rejects_node_up_until_mode_is_configured() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/node/up")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let json = json_from(res).await;
        assert_eq!(json["ok"].as_bool(), Some(false));
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|value| value.contains("node mode is not configured yet"))
        );
    });

    assert_eq!(count_projection_rows(&db_path, "network_registry"), 0);
    assert_eq!(count_projection_rows(&db_path, "node_registry"), 0);
}
