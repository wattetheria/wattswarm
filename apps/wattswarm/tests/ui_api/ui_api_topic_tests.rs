use super::*;

#[test]
fn ui_exposes_topic_message_history_and_cursor_queries() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let subscriber_node_id = {
        let mut node = open_node(&state_dir, &db_path).expect("open node");
        let subscriber_node_id = node.node_id();
        let network_id = format!("local:{subscriber_node_id}");
        node.emit_at(
            1,
            EventPayload::FeedSubscriptionUpdated(FeedSubscriptionUpdatedPayload {
                network_id: network_id.clone(),
                subscriber_node_id: subscriber_node_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["messages".to_owned()],
                active: true,
            }),
            100,
        )
        .expect("emit topic subscription");
        node.emit_at(
            1,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref("hello-crew", &subscriber_node_id, 110),
                local_content_cache: Some(serde_json::json!({"text":"hello crew"})),
                reply_to_message_id: None,
            }),
            110,
        )
        .expect("emit first topic message");
        node.emit_at(
            1,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref("second-ping", &subscriber_node_id, 120),
                local_content_cache: Some(serde_json::json!({"text":"second ping"})),
                reply_to_message_id: None,
            }),
            120,
        )
        .expect("emit second topic message");
        node.store
            .upsert_topic_cursor(
                &network_id,
                &subscriber_node_id,
                "crew.chat",
                "group:crew-7",
                22,
                130,
            )
            .expect("upsert topic cursor");
        subscriber_node_id
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
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

        let first_page_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/messages?feed_key=crew.chat&scope_hint=group:crew-7&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_page_json = json_from(first_page_res).await;
        assert_eq!(
            first_page_json["ok"].as_bool(),
            Some(true),
            "topic messages first page failed: {first_page_json}"
        );
        assert_eq!(first_page_json["messages"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            first_page_json["messages"][0]["content"]["text"].as_str(),
            Some("second ping")
        );
        let before_created_at = first_page_json["next_anchor"]["before_created_at"]
            .as_u64()
            .expect("next anchor created_at");
        let before_message_id = first_page_json["next_anchor"]["before_message_id"]
            .as_str()
            .expect("next anchor message id");

        let second_page_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/topic/messages?feed_key=crew.chat&scope_hint=group:crew-7&limit=2&before_created_at={before_created_at}&before_message_id={before_message_id}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_page_json = json_from(second_page_res).await;
        assert_eq!(
            second_page_json["ok"].as_bool(),
            Some(true),
            "topic messages second page failed: {second_page_json}"
        );
        assert_eq!(second_page_json["messages"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            second_page_json["messages"][0]["content"]["text"].as_str(),
            Some("hello crew")
        );

        let cursor_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/cursor?feed_key=crew.chat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cursor_json = json_from(cursor_res).await;
        assert_eq!(
            cursor_json["ok"].as_bool(),
            Some(true),
            "topic cursor failed: {cursor_json}"
        );
        assert_eq!(
            cursor_json["subscriber_node_id"].as_str(),
            Some(subscriber_node_id.as_str())
        );
        assert_eq!(cursor_json["cursor"]["last_event_seq"].as_u64(), Some(22));
        assert_eq!(
            cursor_json["cursor"]["scope_hint"].as_str(),
            Some("group:crew-7")
        );
    });
}

#[test]
fn ui_accepts_topic_subscription_and_message_writes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let subscription_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/subscriptions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
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
        assert_eq!(subscription_res.status(), StatusCode::OK);
        let subscription_json = json_from(subscription_res).await;
        assert_eq!(
            subscription_json["gossip_kinds"][0].as_str(),
            Some("messages")
        );

        let message_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {"text": "hello from ui write api"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(message_res.status(), StatusCode::OK);

        let messages_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/messages?feed_key=crew.chat&scope_hint=group:crew-7&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let messages_json = json_from(messages_res).await;
        assert_eq!(messages_json["ok"].as_bool(), Some(true));
        assert_eq!(
            messages_json["messages"][0]["content"]["text"].as_str(),
            Some("hello from ui write api")
        );

        let cursor_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/cursor?feed_key=crew.chat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cursor_json = json_from(cursor_res).await;
        assert_eq!(cursor_json["ok"].as_bool(), Some(true));
        assert!(cursor_json["subscriber_node_id"].as_str().is_some());
    });
}

#[test]
fn structured_topic_consensus_bridge_finalizes_and_publishes_result_topic() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    let network_id = format!("local:{local_node_id}");
    node.store
        .upsert_feed_subscription(
            &network_id,
            &local_node_id,
            "crew.chat",
            "group:crew-7",
            &[],
            true,
            1,
        )
        .expect("upsert topic subscription");
    drop(node);

    let remote_a = NodeIdentity::from_seed([7; 32]);
    let remote_b = NodeIdentity::from_seed([8; 32]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let proposal_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-upgrade-v1",
                                "goal": "decide whether to adopt the upgrade",
                                "participants": [local_node_id, remote_a.node_id(), remote_b.node_id()],
                                "threshold_percent": 60,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal_res.status(), StatusCode::OK);
    });

    let network_id = format!("local:{local_node_id}");
    let mut node = open_node(&state_dir, &db_path).expect("reopen node");
    for (created_at, remote, digest) in [
        (10_u64, remote_a, "sha256:evidence-a"),
        (20_u64, remote_b, "sha256:evidence-b"),
    ] {
        let event = build_event_for_external(
            &remote,
            1,
            created_at,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref(
                    &format!("proposal-upgrade-v1-{created_at}"),
                    &remote.node_id(),
                    created_at,
                ),
                local_content_cache: Some(json!({
                    "kind": "stance",
                    "proposal_id": "proposal-upgrade-v1",
                    "stance": "support",
                    "summary": "evidence supports adoption",
                    "evidence_refs": [{
                        "uri": format!("https://example.com/{digest}"),
                        "digest": digest,
                        "size_bytes": 32,
                        "mime": "application/json",
                        "created_at": created_at,
                        "producer": "discussion-test"
                    }]
                })),
                reply_to_message_id: None,
            }),
        )
        .expect("build remote event");
        node.ingest_remote(event).expect("ingest remote stance");
    }

    let processed = wattswarm::control::topic_consensus::process_structured_topic_consensus(
        &mut node, &state_dir,
    )
    .expect("process structured topic consensus");
    assert_eq!(processed, 1);

    let result_messages = node
        .store
        .list_topic_messages(&network_id, "crew.result", "group:crew-7", 10)
        .expect("list result messages");
    assert!(result_messages.iter().any(|message| {
        message.content["kind"].as_str() == Some("consensus_result")
            && message.content["proposal_id"].as_str() == Some("proposal-upgrade-v1")
            && message.content["decision"].as_str() == Some("support")
    }));

    assert!(
        node.store
            .list_task_ids_recent(10)
            .expect("list task ids")
            .into_iter()
            .all(|task_id| !task_id.starts_with("topic-consensus-"))
    );
}

#[test]
fn topic_consensus_result_exposes_shared_consensus_facts() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    drop(node);

    let remote_a = NodeIdentity::from_seed([21; 32]);
    let remote_b = NodeIdentity::from_seed([22; 32]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let proposal_message_id = runtime.block_on(async {
        let subscription_res = app
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
        assert_eq!(subscription_res.status(), StatusCode::OK);

        let proposal_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-facts-v1",
                                "goal": "shared facts",
                                "participants": [local_node_id, remote_a.node_id(), remote_b.node_id()],
                                "threshold_percent": 60,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal_res.status(), StatusCode::OK);
        json_from(proposal_res).await["message_id"]
            .as_str()
            .unwrap()
            .to_owned()
    });

    let network_id = format!("local:{local_node_id}");
    let mut node = open_node(&state_dir, &db_path).expect("reopen node");
    for (identity, created_at, summary) in [
        (&remote_a, 10_u64, "support from remote a"),
        (&remote_b, 20_u64, "support from remote b"),
    ] {
        let event = build_event_for_external(
            identity,
            1,
            created_at,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref(summary, &identity.node_id(), created_at),
                local_content_cache: Some(json!({
                    "kind": "stance",
                    "proposal_id": "proposal-facts-v1",
                    "proposal_message_id": proposal_message_id,
                    "stance": "support",
                    "summary": summary,
                })),
                reply_to_message_id: Some(proposal_message_id.clone()),
            }),
        )
        .expect("stance event");
        node.ingest_remote(event).expect("ingest remote stance");
    }
    let processed = wattswarm::control::topic_consensus::process_structured_topic_consensus(
        &mut node, &state_dir,
    )
    .expect("process structured topic consensus");
    assert_eq!(processed, 1);
    drop(node);

    runtime.block_on(async {
        let result_messages = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/messages?feed_key=crew.result&scope_hint=group:crew-7&limit=4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let result_json = json_from(result_messages).await;
        let result_message = &result_json["messages"][0]["content"];
        assert_eq!(
            result_message["proposal_id"].as_str(),
            Some("proposal-facts-v1")
        );
        assert_eq!(result_message["decision"].as_str(), Some("support"));
        assert_eq!(result_message["round_index"].as_u64(), Some(1));
        assert_eq!(result_message["threshold_percent"].as_u64(), Some(60));
        assert_eq!(result_message["required_count"].as_u64(), Some(2));
        assert_eq!(
            result_message["observed_participant_count"].as_u64(),
            Some(2)
        );
        assert!(
            result_message["participants"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );
        assert_eq!(
            result_message["source_feed_key"].as_str(),
            Some("crew.chat")
        );
        assert_eq!(
            result_message["source_scope_hint"].as_str(),
            Some("group:crew-7")
        );
    });
}

#[test]
fn task_facts_snapshot_exposes_remote_bridge_facts() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let stub = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: r#"{"ok":true}"#.to_owned(),
        capabilities_body: json!({
            "task_types": ["swarm"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-bridge"
        })
        .to_string(),
        execute_body: serde_json::to_string(&json!({
            "candidate_output": {
                "decision": "PASS",
                "answer": "bridged-ui-ok"
            },
            "evidence_inline": [],
            "evidence_refs": []
        }))
        .unwrap(),
        execute_requests: None,
    });
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
    let local_node_id = node.node_id();
    let remote = NodeIdentity::from_seed([31; 32]);
    let remote_node_id = remote.node_id();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&local_node_id, role);
    }
    membership.grant(&remote_node_id, Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = "remote-bridge-task-ui".to_owned();
    let contract = sample_contract(&task_id, policy_hash);
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", wattswarm::crypto::sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement = remote
        .sign_unsigned_event(&UnsignedEvent::from_payload(
            "0.1.0".to_owned(),
            remote_node_id.clone(),
            1,
            100,
            EventPayload::TaskAnnounced(TaskAnnouncedPayload {
                network_id: topology.network.network_id.clone(),
                task_id: task_id.clone(),
                announcement_id: "ann-task-facts-ui".to_owned(),
                feed_key: "remote-feed".to_owned(),
                scope_hint: "global".to_owned(),
                summary: json!({"title":"remote bridge task ui"}),
                detail_ref: Some(wattswarm::types::ArtifactRef {
                    uri: "ipfs://remote-task-detail-ui".to_owned(),
                    digest: digest.clone(),
                    size_bytes: contract_bytes.len() as u64,
                    mime: "application/json".to_owned(),
                    created_at: 100,
                    producer: remote_node_id.clone(),
                }),
            }),
        ))
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");
    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");
    bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        wattswarm::control::RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("bridge remote task into local execution");
    drop(node);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let facts_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/task/facts/{task_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(facts_res.status(), StatusCode::OK);
        let facts = json_from(facts_res).await;
        let remote_bridges = facts["remote_bridges"]
            .as_array()
            .expect("remote bridges array");
        assert_eq!(remote_bridges.len(), 1);
        assert_eq!(
            remote_bridges[0]["announcement_id"].as_str(),
            Some("ann-task-facts-ui")
        );
        assert_eq!(remote_bridges[0]["executor"].as_str(), Some("core-agent"));
        assert_eq!(remote_bridges[0]["profile"].as_str(), Some("default"));
        assert_eq!(
            remote_bridges[0]["source_node_id"].as_str(),
            Some(remote_node_id.as_str())
        );
        let bridged_candidate_id = remote_bridges[0]["candidate_id"]
            .as_str()
            .expect("bridge candidate id");
        assert!(!bridged_candidate_id.is_empty());
        assert!(
            facts["candidates"]
                .as_array()
                .expect("candidates array")
                .iter()
                .any(|candidate| candidate["candidate_id"].as_str() == Some(bridged_candidate_id))
        );
        assert_eq!(
            remote_bridges[0]["terminal_state"].as_str(),
            Some("Finalized")
        );
        assert_eq!(
            remote_bridges[0]["detail_ref_digest"].as_str(),
            Some(digest.as_str())
        );
    });
}

#[test]
fn structured_topic_consensus_timeout_opens_next_round() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let res = app
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
        assert_eq!(res.status(), StatusCode::OK);
    });

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    let remote_a = NodeIdentity::from_seed([31; 32]);
    let remote_b = NodeIdentity::from_seed([32; 32]);
    let network_id = format!("local:{local_node_id}");
    let proposal_event = node
        .emit_at(
            1,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref("proposal-timeout-round-1", &local_node_id, 10),
                local_content_cache: Some(json!({
                    "kind": "proposal",
                    "proposal_id": "proposal-timeout-v1",
                    "goal": "collect enough support",
                    "participants": [local_node_id, remote_a.node_id(), remote_b.node_id()],
                    "min_participants": 3,
                    "threshold_percent": 100,
                    "result_feed_key": "crew.result",
                    "round_index": 1,
                    "max_rounds": 2,
                    "round_timeout_ms": 1
                })),
                reply_to_message_id: None,
            }),
            10,
        )
        .expect("emit round one proposal");
    let proposal_message_id = proposal_event.event_id.clone();
    let stance = build_event_for_external(
        &remote_a,
        1,
        20,
        EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
            network_id: network_id.clone(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: topic_content_ref("proposal-timeout-support", &remote_a.node_id(), 20),
            local_content_cache: Some(json!({
                "kind": "stance",
                "proposal_id": "proposal-timeout-v1",
                "proposal_message_id": proposal_message_id,
                "stance": "support",
                "summary": "only one support in round one"
            })),
            reply_to_message_id: Some(proposal_message_id.clone()),
        }),
    )
    .expect("build support");
    node.ingest_remote(stance).expect("ingest support");

    let processed = wattswarm::control::topic_consensus::process_structured_topic_consensus(
        &mut node, &state_dir,
    )
    .expect("process topic consensus");
    assert_eq!(processed, 1);

    let messages = node
        .store
        .list_topic_messages(&network_id, "crew.chat", "group:crew-7", 10)
        .expect("list topic messages");
    assert!(messages.iter().any(|message| {
        message.content["kind"].as_str() == Some("proposal")
            && message.content["proposal_id"].as_str() == Some("proposal-timeout-v1")
            && message.content["round_index"].as_u64() == Some(2)
            && message.content["previous_round_proposal_message_id"].as_str()
                == Some(proposal_message_id.as_str())
    }));
    let result_messages = node
        .store
        .list_topic_messages(&network_id, "crew.result", "group:crew-7", 10)
        .expect("list result messages");
    assert!(result_messages.is_empty());
}

#[test]
fn task_facts_snapshot_exposes_parent_run_round_state_for_run_queue_tasks() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let stub = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: r#"{"ok":true}"#.to_owned(),
        capabilities_body: json!({
            "task_types": ["resume_review"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-run-facts"
        })
        .to_string(),
        execute_body: serde_json::to_string(&json!({
            "candidate_output": {
                "decision": "PASS",
                "answer": "run-task-ok"
            },
            "evidence_inline": [],
            "evidence_refs": []
        }))
        .unwrap(),
        execute_requests: None,
    });
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "rt-ui".to_owned(),
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

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let run_id = "task-facts-parent-run";
    runtime.block_on(async {
        let submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/run/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "kickoff": true,
                            "spec": sample_run_spec(run_id),
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_res.status(), StatusCode::OK);
    });

    let pg_url = run_control::resolve_run_queue_pg_url(None);
    run_control::run_worker(
        &state_dir,
        &db_path,
        &pg_url,
        WorkerOptions {
            worker_id: "ui-test-worker".to_owned(),
            concurrency: 4,
            poll_ms: 25,
            lease_ms: 5_000,
            once: true,
        },
    )
    .expect("run worker once");

    let task_id = format!("run-{run_id}-CTO-1");
    runtime.block_on(async {
        let facts_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/task/facts/{task_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(facts_res.status(), StatusCode::OK);
        let facts = json_from(facts_res).await;
        let coordinator_run = &facts["coordinator_run"];
        assert_eq!(coordinator_run["run_id"].as_str(), Some(run_id));
        assert_eq!(coordinator_run["round_status"].as_str(), Some("FINALIZED"));
        assert_eq!(coordinator_run["expected_executor_count"].as_i64(), Some(2));
        assert_eq!(coordinator_run["completed_executors"].as_i64(), Some(2));
        assert_eq!(coordinator_run["active_executors"].as_i64(), Some(0));
        assert!(coordinator_run["next_round_trigger"].is_null());
        assert_eq!(coordinator_run["run"]["run_id"].as_str(), Some(run_id));
        assert_eq!(
            coordinator_run["run"]["counts"]["succeeded"].as_i64(),
            Some(2)
        );
    });
}

#[test]
fn structured_topic_consensus_fallback_finalizes_after_max_rounds() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let res = app
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
        assert_eq!(res.status(), StatusCode::OK);
    });

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    let remote_a = NodeIdentity::from_seed([41; 32]);
    let remote_b = NodeIdentity::from_seed([42; 32]);
    let network_id = format!("local:{local_node_id}");
    let proposal_event = node
        .emit_at(
            1,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref("proposal-fallback-round-1", &local_node_id, 10),
                local_content_cache: Some(json!({
                    "kind": "proposal",
                    "proposal_id": "proposal-fallback-v1",
                    "goal": "force fallback after timeout",
                    "participants": [local_node_id, remote_a.node_id(), remote_b.node_id()],
                    "min_participants": 3,
                    "threshold_percent": 100,
                    "result_feed_key": "crew.result",
                    "round_index": 1,
                    "max_rounds": 1,
                    "round_timeout_ms": 1,
                    "fallback_decision": "reject"
                })),
                reply_to_message_id: None,
            }),
            10,
        )
        .expect("emit fallback proposal");
    let proposal_message_id = proposal_event.event_id.clone();
    let stance = build_event_for_external(
        &remote_a,
        1,
        20,
        EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
            network_id: network_id.clone(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: topic_content_ref("proposal-fallback-support", &remote_a.node_id(), 20),
            local_content_cache: Some(json!({
                "kind": "stance",
                "proposal_id": "proposal-fallback-v1",
                "proposal_message_id": proposal_message_id,
                "stance": "support",
                "summary": "insufficient support before timeout"
            })),
            reply_to_message_id: Some(proposal_message_id.clone()),
        }),
    )
    .expect("build support");
    node.ingest_remote(stance).expect("ingest support");

    let processed = wattswarm::control::topic_consensus::process_structured_topic_consensus(
        &mut node, &state_dir,
    )
    .expect("process topic consensus");
    assert_eq!(processed, 1);

    let result_messages = node
        .store
        .list_topic_messages(&network_id, "crew.result", "group:crew-7", 10)
        .expect("list result messages");
    assert!(result_messages.iter().any(|message| {
        message.content["kind"].as_str() == Some("consensus_result")
            && message.content["proposal_id"].as_str() == Some("proposal-fallback-v1")
            && message.content["decision"].as_str() == Some("reject")
            && message.content["fallback_applied"].as_bool() == Some(true)
            && message.content["close_reason"].as_str() == Some("max_rounds_fallback")
    }));
}

#[test]
fn structured_topic_consensus_bridge_ignores_non_participant_stances() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    drop(node);

    let participant = NodeIdentity::from_seed([9; 32]);
    let outsider = NodeIdentity::from_seed([10; 32]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let subscription_res = app
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
        assert_eq!(subscription_res.status(), StatusCode::OK);

        let proposal_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-upgrade-v2",
                                "goal": "decide whether to adopt the upgrade",
                                "participants": [local_node_id, participant.node_id(), "peer-b"],
                                "threshold_percent": 60,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal_res.status(), StatusCode::OK);
    });

    let network_id = format!("local:{local_node_id}");
    let mut node = open_node(&state_dir, &db_path).expect("reopen node");
    for (created_at, remote, digest) in [
        (10_u64, participant, "sha256:evidence-participant"),
        (20_u64, outsider, "sha256:evidence-outsider"),
    ] {
        let event = build_event_for_external(
            &remote,
            1,
            created_at,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content_ref: topic_content_ref(
                    &format!("proposal-upgrade-v2-{created_at}"),
                    &remote.node_id(),
                    created_at,
                ),
                local_content_cache: Some(json!({
                    "kind": "stance",
                    "proposal_id": "proposal-upgrade-v2",
                    "stance": "support",
                    "summary": "evidence supports adoption",
                    "evidence_refs": [{
                        "uri": format!("https://example.com/{digest}"),
                        "digest": digest,
                        "size_bytes": 32,
                        "mime": "application/json",
                        "created_at": created_at,
                        "producer": "discussion-test"
                    }]
                })),
                reply_to_message_id: None,
            }),
        )
        .expect("build remote event");
        node.ingest_remote(event).expect("ingest remote stance");
    }

    let processed = wattswarm::control::topic_consensus::process_structured_topic_consensus(
        &mut node, &state_dir,
    )
    .expect("process structured topic consensus");
    assert_eq!(processed, 0);

    let result_messages = node
        .store
        .list_topic_messages(&network_id, "crew.result", "group:crew-7", 10)
        .expect("list result messages");
    assert!(result_messages.is_empty());
    assert!(
        node.store
            .list_task_ids_recent(10)
            .expect("list task ids")
            .into_iter()
            .all(|task_id| !task_id.starts_with("topic-consensus-"))
    );
}

#[test]
fn free_chat_is_interpreted_then_sedimented_into_topic_consensus() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let stub = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: "{\"status\":\"ok\"}".to_owned(),
        capabilities_body: json!({
            "task_types": ["swarm", "topic_interpretation"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
        execute_body: json!({
            "candidate_output": {
                "source_message_id": "placeholder-overridden-by-test",
                "proposal_id": "proposal-upgrade-v2",
                "proposal_message_id": "placeholder-proposal-message-id",
                "stance": "support",
                "answer": "support",
                "summary": "I support this upgrade because it reduces rollback risk",
                "confidence": 0.92,
                "needs_review": false,
                "evidence": [{
                    "kind": "message_quote",
                    "message_id": "placeholder-overridden-by-test",
                    "quote": "I support this upgrade because it reduces rollback risk"
                }]
            },
            "evidence_inline": [],
            "evidence_refs": []
        })
        .to_string(),
        execute_requests: None,
    });
    wait_for_stub_listener(&stub);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let free_chat_message_id = runtime.block_on(async {
        let save_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/startup-config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "display_name": "Captain Aurora",
                            "network_mode": "local",
                            "bootstrap_peers": [],
                            "core_agent": {
                                "mode": "remote_url",
                                "base_url": stub.base_url(),
                                "provider": "openclaw",
                                "model": "",
                                "api_key": ""
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);

        let subscription_res = app
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
        assert_eq!(subscription_res.status(), StatusCode::OK);

        let proposal_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-upgrade-v2",
                                "goal": "decide whether to adopt the second upgrade",
                                "threshold_percent": 60,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal_res.status(), StatusCode::OK);

        let free_chat_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "text": "I support this upgrade because it reduces rollback risk"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(free_chat_res.status(), StatusCode::OK);
        json_from(free_chat_res).await["message_id"]
            .as_str()
            .unwrap()
            .to_owned()
    });

    let node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    let network_id = format!("local:{local_node_id}");
    let chat_messages = node
        .store
        .list_topic_messages(&network_id, "crew.chat", "group:crew-7", 20)
        .expect("list topic messages");
    let proposal_message_id = chat_messages
        .iter()
        .find(|message| message.content["kind"].as_str() == Some("proposal"))
        .map(|message| message.message_id.clone())
        .expect("proposal message id");
    assert!(chat_messages.iter().any(|message| {
        message.content["kind"].as_str() == Some("interpreted_stance")
            && message.content["proposal_id"].as_str() == Some("proposal-upgrade-v2")
            && message.content["proposal_message_id"].as_str() == Some(proposal_message_id.as_str())
            && message.content["source_message_id"].as_str() == Some(free_chat_message_id.as_str())
            && message.content["source_author_node_id"].as_str() == Some(local_node_id.as_str())
    }));

    let result_messages = node
        .store
        .list_topic_messages(&network_id, "crew.result", "group:crew-7", 10)
        .expect("list result messages");
    assert!(
        result_messages.iter().any(|message| {
            message.content["kind"].as_str() == Some("consensus_result")
                && message.content["proposal_id"].as_str() == Some("proposal-upgrade-v2")
                && message.content["proposal_message_id"].as_str()
                    == Some(proposal_message_id.as_str())
                && message.content["decision"].as_str() == Some("support")
        }),
        "result_messages={result_messages:?}"
    );

    let interpretation_hits = node
        .store
        .list_local_decision_memory_hits_by_task_type("topic_interpretation", 8)
        .expect("interpretation memory");
    assert!(!interpretation_hits.is_empty());

    assert!(
        node.store
            .list_task_ids_recent(16)
            .expect("recent task ids")
            .into_iter()
            .all(|task_id| !task_id.starts_with("topic-consensus-"))
    );
}

#[test]
fn topic_consensus_scopes_stances_to_proposal_rounds() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let node = open_node(&state_dir, &db_path).expect("open node");
    let local_node_id = node.node_id();
    drop(node);

    let remote_a = NodeIdentity::from_seed([11; 32]);
    let remote_b = NodeIdentity::from_seed([12; 32]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (round_one_proposal_message_id, round_two_proposal_message_id) = runtime.block_on(async {
        let subscription_res = app
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
        assert_eq!(subscription_res.status(), StatusCode::OK);

        let round_one_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-rounds-v1",
                                "goal": "first round",
                                "participants": [local_node_id, remote_a.node_id(), remote_b.node_id()],
                                "threshold_percent": 100,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(round_one_res.status(), StatusCode::OK);
        let round_one_message_id = json_from(round_one_res).await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let round_two_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-rounds-v1",
                                "goal": "second round",
                                "participants": [local_node_id, remote_a.node_id(), remote_b.node_id()],
                                "threshold_percent": 100,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(round_two_res.status(), StatusCode::OK);
        let round_two_message_id = json_from(round_two_res).await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();

        (round_one_message_id, round_two_message_id)
    });

    let network_id = format!("local:{local_node_id}");
    let mut node = open_node(&state_dir, &db_path).expect("reopen node");
    let round_one_support = build_event_for_external(
        &remote_a,
        1,
        10,
        EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
            network_id: network_id.clone(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: topic_content_ref("proposal-round-one-support", &remote_a.node_id(), 10),
            local_content_cache: Some(json!({
                "kind": "stance",
                "proposal_id": "proposal-rounds-v1",
                "proposal_message_id": round_one_proposal_message_id,
                "stance": "support",
                "summary": "support from round one"
            })),
            reply_to_message_id: Some(round_one_proposal_message_id.clone()),
        }),
    )
    .expect("round one stance");
    node.ingest_remote(round_one_support)
        .expect("ingest round one stance");

    node.emit_at(
        1,
        EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
            network_id: network_id.clone(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: topic_content_ref("proposal-round-two-local-support", &local_node_id, 20),
            local_content_cache: Some(json!({
                "kind": "stance",
                "proposal_id": "proposal-rounds-v1",
                "proposal_message_id": round_two_proposal_message_id,
                "stance": "support",
                "summary": "local support in round two"
            })),
            reply_to_message_id: Some(round_two_proposal_message_id.clone()),
        }),
        20,
    )
    .expect("emit round two local stance");

    let round_two_support = build_event_for_external(
        &remote_b,
        1,
        30,
        EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
            network_id: network_id.clone(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: topic_content_ref(
                "proposal-round-two-remote-support",
                &remote_b.node_id(),
                30,
            ),
            local_content_cache: Some(json!({
                "kind": "stance",
                "proposal_id": "proposal-rounds-v1",
                "proposal_message_id": round_two_proposal_message_id,
                "stance": "support",
                "summary": "second support only exists in round two"
            })),
            reply_to_message_id: Some(round_two_proposal_message_id.clone()),
        }),
    )
    .expect("round two stance");
    node.ingest_remote(round_two_support)
        .expect("ingest round two stance");

    let processed = wattswarm::control::topic_consensus::process_structured_topic_consensus(
        &mut node, &state_dir,
    )
    .expect("process structured topic consensus");
    assert_eq!(processed, 0);

    let result_messages = node
        .store
        .list_topic_messages(&network_id, "crew.result", "group:crew-7", 10)
        .expect("list result messages");
    assert!(result_messages.is_empty());
}

#[test]
fn topic_interpretation_receives_prior_deliberations_from_topic_consensus_history() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let execute_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let stub = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: "{\"status\":\"ok\"}".to_owned(),
        capabilities_body: json!({
            "task_types": ["swarm", "topic_interpretation"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
        execute_body: json!({
            "candidate_output": {
                "source_message_id": "placeholder-overridden-by-test",
                "proposal_id": "proposal-history-v1",
                "proposal_message_id": "placeholder-proposal-message-id",
                "stance": "support",
                "answer": "support",
                "summary": "same support as prior deliberation",
                "confidence": 0.92,
                "needs_review": false,
                "evidence": []
            },
            "evidence_inline": [],
            "evidence_refs": []
        })
        .to_string(),
        execute_requests: Some(Arc::clone(&execute_requests)),
    });
    wait_for_stub_listener(&stub);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (first_proposal_message_id, second_proposal_message_id) = runtime.block_on(async {
        let save_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/startup-config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "display_name": "Captain Aurora",
                            "network_mode": "local",
                            "bootstrap_peers": [],
                            "core_agent": {
                                "mode": "remote_url",
                                "base_url": stub.base_url(),
                                "provider": "openclaw",
                                "model": "",
                                "api_key": ""
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);

        let subscription_res = app
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
        assert_eq!(subscription_res.status(), StatusCode::OK);

        let proposal_one_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-history-v1",
                                "goal": "first history round",
                                "threshold_percent": 60,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal_one_res.status(), StatusCode::OK);
        let proposal_one_message_id = json_from(proposal_one_res).await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let first_chat_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "text": "I support the first history round"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_chat_res.status(), StatusCode::OK);

        let proposal_two_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "kind": "proposal",
                                "proposal_id": "proposal-history-v1",
                                "goal": "second history round",
                                "threshold_percent": 60,
                                "result_feed_key": "crew.result"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal_two_res.status(), StatusCode::OK);
        let proposal_two_message_id = json_from(proposal_two_res).await["message_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let second_chat_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {
                                "text": "same as before"
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_chat_res.status(), StatusCode::OK);

        (proposal_one_message_id, proposal_two_message_id)
    });

    let requests = execute_requests.lock().expect("lock execute requests");
    assert!(requests.len() >= 2);
    let second_request = requests.last().expect("second interpretation request");
    let prior_deliberations = second_request["inputs"]["prior_deliberations"]
        .as_array()
        .expect("prior deliberations array");
    assert!(!prior_deliberations.is_empty());
    assert!(prior_deliberations.iter().any(|row| {
        row["proposal_message_id"].as_str() == Some(first_proposal_message_id.as_str())
            && row["decision"].as_str() == Some("support")
    }));
    assert_eq!(
        second_request["inputs"]["candidate_proposals"][0]["proposal_message_id"].as_str(),
        Some(second_proposal_message_id.as_str())
    );
}
