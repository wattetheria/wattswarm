use super::*;

#[test]
fn task_claim_agent_event_uses_generic_task_schema() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy")
        .policy_hash;
    let mut contract = sample_contract("task-1", policy_hash);
    contract.inputs = json!({
        "kind": "generic_task",
        "agent_did": "agent-a"
    });
    node.submit_task(contract, 7, 50).expect("submit task");
    let event = crate::types::Event {
        event_id: "evt-claim".to_owned(),
        protocol_version: "1".to_owned(),
        event_kind: crate::types::EventKind::TaskClaimed,
        task_id: Some("task-1".to_owned()),
        epoch: 7,
        author_node_id: "peer-a".to_owned(),
        created_at: 55,
        payload: crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
            task_id: "task-1".to_owned(),
            role: crate::types::ClaimRole::Propose,
            claimer_node_id: "peer-a".to_owned(),
            execution_id: "exec-agent-a".to_owned(),
            lease_until: 88,
            agent_envelope: None,
        }),
        signature_hex: "sig".to_owned(),
    };
    let crate::types::EventPayload::TaskClaimed(payload) = &event.payload else {
        panic!("expected claim payload");
    };
    let agent_event = task_claim_agent_event(&node, &event, payload).expect("agent event");
    assert_eq!(
        agent_event.event_type,
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived
    );
    assert_eq!(
        agent_event.source_kind,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle
    );
    assert_eq!(agent_event.payload["task_id"].as_str(), Some("task-1"));
    assert!(!agent_event.requires_commit);
    assert!(
        agent_event
            .allowed_actions
            .iter()
            .any(|action| action == "decide_claim")
    );
    assert_eq!(
        agent_event.payload["execution_id"].as_str(),
        Some("exec-agent-a")
    );
}

#[test]
fn task_claim_decision_agent_event_prompts_approved_claimer_to_complete() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy")
        .policy_hash;
    let mut contract = sample_contract("mission-claim-decision", policy_hash);
    contract.inputs = json!({
        "kind": "wattetheria_mission",
        "mission_id": "mission-claim-decision",
        "agent_did": "claimer-agent"
    });
    node.submit_task(contract, 7, 50).expect("submit task");
    let event = crate::types::Event {
        event_id: "evt-claim-decision".to_owned(),
        protocol_version: "1".to_owned(),
        event_kind: crate::types::EventKind::TaskClaimDecided,
        task_id: Some("mission-claim-decision".to_owned()),
        epoch: 7,
        author_node_id: "publisher-node".to_owned(),
        created_at: 55,
        payload: crate::types::EventPayload::TaskClaimDecided(
            crate::types::TaskClaimDecidedPayload {
                task_id: "mission-claim-decision".to_owned(),
                execution_id: "exec-1".to_owned(),
                claimer_node_id: "claimer-node".to_owned(),
                approved: true,
                reason: None,
                agent_envelope: None,
            },
        ),
        signature_hex: "sig".to_owned(),
    };
    let crate::types::EventPayload::TaskClaimDecided(payload) = &event.payload else {
        panic!("expected claim decision payload");
    };
    let agent_event = task_claim_decision_agent_event(&node, &event, payload).expect("agent event");

    assert_eq!(
        agent_event.event_type,
        wattswarm_protocol::types::AgentEventType::TaskClaimDecisionReceived
    );
    assert_eq!(
        agent_event.payload["event_kind"].as_str(),
        Some("task_claim_decided")
    );
    assert_eq!(agent_event.payload["approved"].as_bool(), Some(true));
    assert!(
        agent_event
            .allowed_actions
            .iter()
            .any(|action| action == "complete_mission")
    );
    assert!(
        !agent_event
            .allowed_actions
            .iter()
            .any(|action| action == "inspect_task")
    );
}

#[test]
fn backfill_task_claimed_delivers_local_agent_event() {
    let state_dir = temp_startup_dir("backfill-task-claim-agent-event");
    let publisher_identity = NodeIdentity::random();
    let claimer_identity = NodeIdentity::random();
    let membership =
        membership_with_roles(&[publisher_identity.node_id(), claimer_identity.node_id()]);
    let mut publisher = Node::new(
        publisher_identity,
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("publisher node");
    let policy_hash = publisher
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy")
        .policy_hash;
    let task_id = "task-backfill-claim";
    let scope = SwarmScope::Group(task_id.to_owned());
    let mut contract = sample_contract(task_id, policy_hash);
    contract.inputs = json!({
        "kind": "generic_task",
        "agent_did": "publisher-agent",
        "swarm_scope": scope_hint_label(&scope)
    });
    publisher
        .submit_task(contract, 1, 100)
        .expect("submit task");
    let agent_envelope = wattswarm_protocol::types::AgentEnvelope {
        protocol: "a2a.task.v1".to_owned(),
        source_agent_id: Some("claimer-agent".to_owned()),
        target_agent_id: Some("publisher-agent".to_owned()),
        capability: Some("task.claim".to_owned()),
        message_json: json!({
            "task_id": task_id,
            "action": "claim"
        })
        .to_string(),
        extensions_json: None,
        signature: None,
        ..wattswarm_protocol::types::AgentEnvelope::default()
    };
    let claim_event = build_event_for_external(
        &claimer_identity,
        1,
        110,
        crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
            task_id: task_id.to_owned(),
            role: crate::types::ClaimRole::Propose,
            claimer_node_id: claimer_identity.node_id(),
            execution_id: format!("exec-{task_id}"),
            lease_until: 500,
            agent_envelope: Some(agent_envelope.clone()),
        }),
    )
    .expect("claim event");
    let service_seed = [141u8; 32];
    fs::write(state_dir.join("node_seed.hex"), hex::encode(service_seed)).expect("write node seed");
    fs::write(
        state_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write startup config");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            state_dir.clone(),
            service_seed,
        )
        .expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), state_dir.join("control.sqlite"));
    let peer = random_network_node_id();
    let request_id = BackfillRequestId::new(42);

    let tick = service
        .process_runtime_event(
            &mut publisher,
            NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response: crate::network_p2p::BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 1,
                    feed_key: None,
                    head_event_ids: vec![claim_event.event_id.clone()],
                    events: vec![EventEnvelope {
                        scope,
                        event: claim_event,
                        content_source_node_id: None,
                    }],
                },
            },
        )
        .expect("process backfill response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::BackfillApplied {
            request_id: applied_request,
            events: 1,
            ..
        } if applied_request == request_id
    ));
    let records =
        crate::control::load_agent_event_records_state(&state_dir).expect("agent event records");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].event_type,
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived
    );
    assert_eq!(records[0].payload["task_id"].as_str(), Some(task_id));
    assert_eq!(
        records[0].target_executor.as_deref(),
        Some(CORE_AGENT_EXECUTOR_NAME)
    );
    assert_eq!(records[0].agent_envelope, Some(agent_envelope));
    assert!(records[0].payload.get("agent_envelope").is_none());
}

#[test]
fn deliver_agent_event_writes_local_diagnostics() {
    let state_dir = temp_startup_dir("agent-event-diagnostics");
    let event = build_agent_event(
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
        Some("peer-a".to_owned()),
        None,
        json!({"task_id": "task-1"}),
        false,
        vec!["human_review".to_owned()],
        Some("task-1".to_owned()),
        Some("task_claim:task-1:exec-1".to_owned()),
    );

    deliver_agent_event_to_local_executor(&state_dir, None, &event)
        .expect("deliver without executor");

    let raw = fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostic log");
    assert!(raw.contains("\"phase\":\"delivery.queued\""));
    assert!(raw.contains("\"phase\":\"delivery.executor\""));
    assert!(raw.contains("\"event_id\""));
    assert!(raw.contains("\"object_id\":\"task-1\""));
}

#[test]
fn deliver_agent_event_retries_callback_timeout_before_marking_delivered() {
    let _env_lock = lock_env_test_mutex();
    let _timeout = EnvVarGuard::set("WATTSWARM_AGENT_EVENT_CALLBACK_TIMEOUT_MS", Some("50"));
    let _attempts = EnvVarGuard::set("WATTSWARM_AGENT_EVENT_CALLBACK_MAX_ATTEMPTS", Some("2"));
    let _backoff = EnvVarGuard::set("WATTSWARM_AGENT_EVENT_CALLBACK_RETRY_BACKOFF_MS", Some("1"));
    let state_dir = temp_startup_dir("agent-event-callback-retry");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind executor listener");
    let executor_addr = listener.local_addr().expect("executor addr");
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted_clone = Arc::clone(&accepted);
    std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept executor connection");
            let attempt = accepted_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            std::thread::spawn(move || {
                let raw = read_http_request(&mut stream);
                assert!(raw.contains("POST /agent-events "));
                if attempt == 1 {
                    std::thread::sleep(Duration::from_millis(200));
                    return;
                }
                let body =
                    serde_json::to_string(&wattswarm_protocol::types::AgentEventCallbackResponse {
                        ok: true,
                        acked_at: Some(2),
                        detail: Some("retried".to_owned()),
                        decision: None,
                    })
                    .expect("serialize executor response");
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write executor response");
            });
        }
    });
    crate::control::save_executor_registry_state(
        &state_dir,
        &crate::control::ExecutorRegistry {
            entries: vec![crate::control::ExecutorRegistryEntry {
                name: crate::control::CORE_AGENT_EXECUTOR_NAME.to_owned(),
                base_url: format!("http://{executor_addr}"),
                agent_event_callback_base_url: None,
                kind: crate::control::ExecutorKind::Local,
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");
    let event = build_agent_event(
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
        Some("peer-a".to_owned()),
        None,
        json!({"task_id": "task-retry"}),
        false,
        vec!["human_review".to_owned()],
        Some("task-retry".to_owned()),
        Some("task_claim:task-retry:exec-1".to_owned()),
    );

    deliver_agent_event_to_local_executor(&state_dir, None, &event).expect("deliver with retry");

    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 2);
    let scope_id = local_control_scope_id(&state_dir);
    let rows = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_events(&scope_id)
        .expect("list local agent events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "acked");
    let deliveries = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_event_deliveries(&scope_id, &event.event_id)
        .expect("list local agent event deliveries");
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].delivery_status, "retrying");
    assert!(deliveries[0].next_retry_at.is_some());
    assert_eq!(deliveries[1].delivery_status, "acked");
    assert_eq!(deliveries[1].response_code, Some(200));
    let raw = fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostic log");
    assert!(raw.contains("\"phase\":\"delivery.callback.retry\""));
    assert!(raw.contains("\"request\""));
    assert!(raw.contains("\"response\""));
    assert!(raw.contains("task-retry"));
}

#[test]
fn deliver_agent_event_marks_callback_ack_error_as_failed_with_body() {
    let state_dir = temp_startup_dir("agent-event-callback-ack-error");
    let executor_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind executor listener");
    let executor_addr = executor_listener.local_addr().expect("executor addr");
    std::thread::spawn(move || {
        let (mut stream, _) = executor_listener
            .accept()
            .expect("accept executor connection");
        let raw = read_http_request(&mut stream);
        assert!(raw.contains("POST /agent-events "));
        let body = serde_json::to_string(&wattswarm_protocol::types::AgentEventCallbackResponse {
            ok: false,
            acked_at: Some(1),
            detail: Some("openai-compatible response missing content".to_owned()),
            decision: None,
        })
        .expect("serialize executor response");
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write executor response");
    });
    crate::control::save_executor_registry_state(
        &state_dir,
        &crate::control::ExecutorRegistry {
            entries: vec![crate::control::ExecutorRegistryEntry {
                name: crate::control::CORE_AGENT_EXECUTOR_NAME.to_owned(),
                base_url: format!("http://{executor_addr}"),
                agent_event_callback_base_url: None,
                kind: crate::control::ExecutorKind::Local,
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");
    let event = build_agent_event(
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
        Some("peer-a".to_owned()),
        None,
        json!({"task_id": "task-ack-error"}),
        false,
        vec!["human_review".to_owned()],
        Some("task-ack-error".to_owned()),
        Some("task_claim:task-ack-error:exec-1".to_owned()),
    );

    deliver_agent_event_to_local_executor(&state_dir, None, &event).expect("deliver ack error");

    let scope_id = local_control_scope_id(&state_dir);
    let rows = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_events(&scope_id)
        .expect("list local agent events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "failed");
    let deliveries = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_event_deliveries(&scope_id, &event.event_id)
        .expect("list local agent event deliveries");
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].delivery_status, "failed");
    assert_eq!(deliveries[0].response_code, Some(200));
    assert!(
        deliveries[0]
            .response_body
            .as_deref()
            .is_some_and(|body| body.contains("openai-compatible response missing content"))
    );
    let raw = fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostic log");
    assert!(raw.contains("\"callback_ok\":false"));
    assert!(raw.contains("\"request\""));
    assert!(raw.contains("\"response\""));
    assert!(raw.contains("task-ack-error"));
    assert!(raw.contains("openai-compatible response missing content"));
}

#[test]
fn task_result_agent_event_supports_retry_updates() {
    let event = crate::types::Event {
        event_id: "evt-retry".to_owned(),
        protocol_version: "1".to_owned(),
        event_kind: crate::types::EventKind::TaskRetryScheduled,
        task_id: Some("task-1".to_owned()),
        epoch: 7,
        author_node_id: "peer-a".to_owned(),
        created_at: 99,
        payload: crate::types::EventPayload::TaskRetryScheduled(
            crate::types::TaskRetryScheduledPayload {
                task_id: "task-1".to_owned(),
                attempt: 3,
                run_at: 1234,
                agent_envelope: None,
            },
        ),
        signature_hex: "sig".to_owned(),
    };
    let node = Node::open_in_memory_with_roles(&[Role::Committer]).expect("open node");
    let agent_event = task_result_agent_event(&node, &event)
        .expect("build task result event")
        .expect("task result event");
    assert_eq!(
        agent_event.event_type,
        wattswarm_protocol::types::AgentEventType::TaskResultReceived
    );
    assert_eq!(
        agent_event.payload["event_kind"].as_str(),
        Some("task_retry_scheduled")
    );
    assert_eq!(agent_event.payload["attempt"].as_u64(), Some(3));
}

#[test]
fn task_result_agent_event_supports_ordinary_task_completed() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let store = PgStore::open_in_memory().expect("store");
    let mut node = Node::new(identity.clone(), store, membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-completed-ordinary", policy_hash);
    contract.inputs = json!({
        "kind": "wattetheria_mission",
        "mission_id": "task-completed-ordinary"
    });
    node.submit_task(contract, 1, 10).expect("submit task");

    let event = build_event_for_external(
        &identity,
        1,
        20,
        crate::types::EventPayload::TaskCompleted(crate::types::TaskCompletedPayload {
            task_id: "task-completed-ordinary".to_owned(),
            execution_id: "exec-completed".to_owned(),
            completed_by_node_id: node_id,
            output: json!({
                "kind": "mission_completed",
                "mission_id": "task-completed-ordinary",
                "result": {"ok": true}
            }),
            agent_envelope: None,
        }),
    )
    .expect("event");

    let agent_event = task_result_agent_event(&node, &event)
        .expect("build task result event")
        .expect("task result event");

    assert_eq!(
        agent_event.event_type,
        wattswarm_protocol::types::AgentEventType::TaskResultReceived
    );
    assert_eq!(
        agent_event.payload["event_kind"].as_str(),
        Some("task_completed")
    );
    assert_eq!(
        agent_event.payload["output"]["mission_id"].as_str(),
        Some("task-completed-ordinary")
    );
    assert!(
        agent_event
            .allowed_actions
            .iter()
            .any(|action| action == "accept_result")
    );
    assert!(
        !agent_event
            .allowed_actions
            .iter()
            .any(|action| action == "inspect_task")
    );
}

#[test]
fn task_result_agent_event_uses_generic_task_actions() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let store = PgStore::open_in_memory().expect("store");
    let mut node = Node::new(identity.clone(), store, membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let contract = sample_contract("task-result-generic", policy_hash);
    node.submit_task(contract, 1, 10).expect("submit task");

    let candidate = crate::types::Candidate {
        candidate_id: "cand-generic".to_owned(),
        execution_id: "exec-generic".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: "artifact://reference/cand-generic".to_owned(),
            digest: "sha256:cand-generic".to_owned(),
            size_bytes: 64,
            mime: "application/json".to_owned(),
            created_at: 12,
            producer: node_id.clone(),
        },
        output: json!({
            "request_id": "req-generic",
            "result": {"ok": true}
        }),
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.store
        .put_candidate("task-result-generic", &node_id, &candidate)
        .expect("put candidate");

    let event = build_event_for_external(
        &identity,
        1,
        20,
        crate::types::EventPayload::CandidateProposed(crate::types::CandidateProposedPayload {
            task_id: "task-result-generic".to_owned(),
            candidate: candidate.clone(),
            agent_envelope: None,
        }),
    )
    .expect("candidate proposed event");

    let agent_event = task_result_agent_event(&node, &event)
        .expect("build task result event")
        .expect("task result event");
    assert_eq!(
        agent_event.event_type,
        wattswarm_protocol::types::AgentEventType::TaskResultReceived
    );
    assert!(!agent_event.requires_commit);
    assert_eq!(
        agent_event.allowed_actions,
        vec![
            "human_review".to_owned(),
            "accept_result".to_owned(),
            "reject_result".to_owned(),
            "request_retry".to_owned()
        ]
    );
    assert_eq!(
        agent_event.payload["candidate_id"].as_str(),
        Some("cand-generic")
    );
    assert_eq!(
        agent_event.payload["execution_id"].as_str(),
        Some("exec-generic")
    );
    assert!(
        agent_event.payload.get("mission_id").is_none(),
        "generic task events must not expose mission-specific fields"
    );
}

#[test]
fn topic_message_requires_reply_excludes_consensus_kinds() {
    assert!(topic_message_requires_reply(
        &json!({"text": "hello world"})
    ));
    assert!(!topic_message_requires_reply(&json!({"kind": "proposal"})));
    assert!(!topic_message_requires_reply(&json!({"kind": "stance"})));
    assert!(!topic_message_requires_reply(
        &json!({"kind": "interpreted_stance"})
    ));
    assert!(!topic_message_requires_reply(
        &json!({"kind": "consensus_result"})
    ));
}

#[test]
fn private_dm_topic_agent_event_exposes_direct_message_content() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let local_node_id = local.node_id();
    let remote_node_id = remote.node_id();
    let membership = membership_with_roles(&[local_node_id.clone(), remote_node_id.clone()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let dm_content = json!({
        "kind": "direct_message",
        "thread_id": "dm-thread",
        "message_id": "dm-message",
        "content": "hello private dm",
        "agent_envelope": {
            "protocol": "google_a2a",
            "source_agent_id": "did:key:remote",
            "target_agent_id": "did:key:local",
            "source_node_id": remote_node_id,
            "target_node_id": local_node_id,
            "capability": "social.dm.send",
            "message": {
                "content": "hello private dm",
                "message_id": "dm-message"
            },
            "signature": "sig"
        }
    });
    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: "group:dm-test".to_owned(),
            content_ref: sample_topic_content_ref("sha256:dm-message", &remote.node_id()),
            local_content_cache: Some(dm_content),
            reply_to_message_id: None,
            agent_envelope: None,
        }),
    )
    .expect("signed event");
    let envelope = EventEnvelope {
        scope: SwarmScope::Group("dm-test".to_owned()),
        event: remote_event,
        content_source_node_id: None,
    };

    let ingested = ingest_event_envelope(&mut node, &envelope).expect("ingest dm event");
    let crate::types::EventPayload::TopicMessagePosted(payload) = &ingested.payload else {
        panic!("expected topic message");
    };
    let agent_event = topic_message_agent_event(&node, &ingested, payload)
        .expect("agent event result")
        .expect("agent event");

    assert_eq!(agent_event.payload["content"], json!("hello private dm"));
    assert_eq!(
        agent_event.payload["topic_content"]["kind"].as_str(),
        Some("direct_message")
    );
    assert_eq!(
        agent_event.payload["topic_content"]["message_id"].as_str(),
        Some("dm-message")
    );
    let agent_envelope = agent_event
        .agent_envelope
        .as_ref()
        .expect("embedded dm envelope is promoted to agent event");
    assert_eq!(agent_envelope.capability.as_deref(), Some("social.dm.send"));
    assert_eq!(
        agent_envelope.source_node_id.as_deref(),
        Some(remote_node_id.as_str())
    );
    assert_eq!(
        agent_envelope.target_node_id.as_deref(),
        Some(local_node_id.as_str())
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&agent_envelope.message_json)
            .expect("agent envelope message json parses")["content"]
            .as_str(),
        Some("hello private dm")
    );
}

#[test]
fn inbound_private_dm_topic_is_projected_to_local_dm_store() {
    let state_dir = temp_startup_dir("inbound-private-dm-projection");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let thread_id = crate::control::private_dm_thread_id(&local.node_id(), &remote.node_id());
    let message_id = "dm-message-inbound-1";
    let dm_content = json!({
        "kind": "direct_message",
        "thread_id": thread_id,
        "message_id": message_id,
        "content": {
            "type": "text",
            "text": "hello from remote"
        },
        "agent_envelope": {
            "protocol": "google_a2a",
            "source_agent_id": "did:key:remote",
            "target_agent_id": "did:key:local",
            "source_node_id": remote.node_id(),
            "target_node_id": local.node_id(),
            "capability": "social.dm.send",
            "message": {
                "content": {
                    "type": "text",
                    "text": "wrong envelope fallback"
                },
                "message_id": message_id
            },
            "signature": "sig"
        }
    });
    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: crate::control::private_dm_scope_hint(&local.node_id(), &remote.node_id()),
            content_ref: sample_topic_content_ref("sha256:dm-message", &remote.node_id()),
            local_content_cache: Some(dm_content),
            reply_to_message_id: None,
            agent_envelope: None,
        }),
    )
    .expect("signed event");
    let scope = SwarmScope::Group(crate::control::private_dm_group_id(
        &local.node_id(),
        &remote.node_id(),
    ));
    let service_seed = [142u8; 32];
    fs::write(state_dir.join("node_seed.hex"), hex::encode(service_seed)).expect("write node seed");
    fs::write(
        state_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write startup config");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            state_dir.clone(),
            service_seed,
        )
        .expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path.clone());

    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::Gossip {
                propagation_source: random_network_node_id(),
                message: GossipMessage::Chat(EventEnvelope {
                    scope,
                    event: remote_event,
                    content_source_node_id: None,
                }),
            },
        )
        .expect("ingest inbound private dm");

    let threads =
        crate::control::load_peer_dm_thread_records_state(&state_dir).expect("load dm threads");
    let thread = threads
        .iter()
        .find(|record| record.remote_node_id == remote.node_id())
        .expect("inbound dm thread projected");
    let messages =
        crate::control::load_peer_dm_message_records_state(&state_dir, &thread.thread_id)
            .expect("load dm messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, message_id);
    assert_eq!(messages[0].remote_node_id, remote.node_id());
    assert_eq!(
        messages[0].direction,
        crate::control::PeerDmDirection::Inbound
    );
    assert_eq!(
        messages[0].delivery_state,
        crate::control::PeerDmDeliveryState::Delivered
    );
    assert_eq!(
        messages[0].content["text"].as_str(),
        Some("hello from remote")
    );
    assert_ne!(
        messages[0].content["text"].as_str(),
        messages[0]
            .agent_envelope
            .as_ref()
            .and_then(|envelope| envelope.message.pointer("/content/text"))
            .and_then(serde_json::Value::as_str)
    );
}

#[test]
fn backfill_private_dm_topic_delivers_local_agent_event() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let state_dir = temp_startup_dir("backfill-private-dm-agent-event");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let thread_id = crate::control::private_dm_thread_id(&local.node_id(), &remote.node_id());
    let message_id = "dm-message-backfill-1";
    let message_text = "hello from remote backfill";
    let dm_content = json!({
        "kind": "direct_message",
        "thread_id": thread_id,
        "message_id": message_id,
        "content": {
            "type": "text",
            "text": message_text
        },
        "agent_envelope": {
            "protocol": "google_a2a",
            "source_agent_id": "did:key:remote",
            "target_agent_id": "did:key:local",
            "source_node_id": remote.node_id(),
            "target_node_id": local.node_id(),
            "capability": "social.dm.send",
            "message": {
                "content": {
                    "type": "text",
                    "text": message_text
                },
                "message_id": message_id
            },
            "signature": "sig"
        }
    });
    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: crate::control::private_dm_scope_hint(&local.node_id(), &remote.node_id()),
            content_ref: sample_topic_content_ref("sha256:dm-message-backfill", &remote.node_id()),
            local_content_cache: Some(dm_content),
            reply_to_message_id: None,
            agent_envelope: None,
        }),
    )
    .expect("signed event");
    let scope = SwarmScope::Group(crate::control::private_dm_group_id(
        &local.node_id(),
        &remote.node_id(),
    ));
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path.clone());
    let request_id = BackfillRequestId::new(77);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillResponse {
                peer: random_network_node_id(),
                request_id,
                response: crate::network_p2p::BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 1,
                    feed_key: Some(crate::control::PRIVATE_DM_FEED_KEY.to_owned()),
                    head_event_ids: vec![remote_event.event_id.clone()],
                    events: vec![EventEnvelope {
                        scope,
                        event: remote_event.clone(),
                        content_source_node_id: None,
                    }],
                },
            },
        )
        .expect("process backfill response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::BackfillApplied {
            request_id: applied_request,
            events: 1,
            ..
        } if applied_request == request_id
    ));
    let threads =
        crate::control::load_peer_dm_thread_records_state(&state_dir).expect("load dm threads");
    let thread = threads
        .iter()
        .find(|record| record.remote_node_id == remote.node_id())
        .expect("backfilled dm thread projected");
    let messages =
        crate::control::load_peer_dm_message_records_state(&state_dir, &thread.thread_id)
            .expect("load dm messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, message_id);
    assert_eq!(messages[0].content["text"].as_str(), Some(message_text));

    let records =
        crate::control::load_agent_event_records_state(&state_dir).expect("agent event records");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].event_type,
        wattswarm_protocol::types::AgentEventType::TopicMessageRequiresReply
    );
    assert_eq!(
        records[0].target_executor.as_deref(),
        Some(CORE_AGENT_EXECUTOR_NAME)
    );
    assert_eq!(
        records[0].payload["feed_key"].as_str(),
        Some(crate::control::PRIVATE_DM_FEED_KEY)
    );
    assert_eq!(
        records[0].payload["topic_content"]["kind"].as_str(),
        Some("direct_message")
    );
    assert_eq!(
        records[0].payload["topic_content"]["message_id"].as_str(),
        Some(message_id)
    );
    assert_eq!(
        records[0].payload["content"]["text"].as_str(),
        Some(message_text)
    );
    assert_eq!(
        records[0]
            .agent_envelope
            .as_ref()
            .and_then(|envelope| envelope.capability.as_deref()),
        Some("social.dm.send")
    );
}

#[test]
fn backfill_self_authored_private_dm_topic_skips_agent_event() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let state_dir = temp_startup_dir("backfill-self-dm-agent-event");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let thread_id = crate::control::private_dm_thread_id(&local.node_id(), &remote.node_id());
    let message_id = "dm-message-self-backfill-1";
    let message_text = "local outbound message should not trigger reply";
    let dm_content = json!({
        "kind": "direct_message",
        "thread_id": thread_id,
        "message_id": message_id,
        "content": {
            "type": "text",
            "text": message_text
        },
        "agent_envelope": {
            "protocol": "google_a2a",
            "source_agent_id": "did:key:local",
            "target_agent_id": "did:key:remote",
            "source_node_id": local.node_id(),
            "target_node_id": remote.node_id(),
            "capability": "social.dm.send",
            "message": {
                "content": {
                    "type": "text",
                    "text": message_text
                },
                "message_id": message_id
            },
            "signature": "sig"
        }
    });
    let local_event = build_event_for_external(
        &local,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: crate::control::private_dm_scope_hint(&local.node_id(), &remote.node_id()),
            content_ref: sample_topic_content_ref(
                "sha256:dm-message-self-backfill",
                &local.node_id(),
            ),
            local_content_cache: Some(dm_content),
            reply_to_message_id: None,
            agent_envelope: None,
        }),
    )
    .expect("signed event");
    let scope = SwarmScope::Group(crate::control::private_dm_group_id(
        &local.node_id(),
        &remote.node_id(),
    ));
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path);
    let request_id = BackfillRequestId::new(78);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillResponse {
                peer: random_network_node_id(),
                request_id,
                response: crate::network_p2p::BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 1,
                    feed_key: Some(crate::control::PRIVATE_DM_FEED_KEY.to_owned()),
                    head_event_ids: vec![local_event.event_id.clone()],
                    events: vec![EventEnvelope {
                        scope,
                        event: local_event,
                        content_source_node_id: None,
                    }],
                },
            },
        )
        .expect("process backfill response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::BackfillApplied {
            request_id: applied_request,
            events: 1,
            ..
        } if applied_request == request_id
    ));
    let threads =
        crate::control::load_peer_dm_thread_records_state(&state_dir).expect("load dm threads");
    assert!(threads.is_empty());
    let records =
        crate::control::load_agent_event_records_state(&state_dir).expect("agent event records");
    assert!(records.is_empty());
}

#[test]
fn inbound_private_dm_topic_skips_local_author_echo() {
    let state_dir = temp_startup_dir("inbound-private-dm-self-echo");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let local_node_id = local.node_id();
    let result = save_inbound_private_dm_topic_message(
        &state_dir,
        &local_node_id,
        &local_node_id,
        "self-echo-event",
        &json!({
            "kind": "direct_message",
            "thread_id": "dm:self-echo",
            "message_id": "dm-self-echo",
            "content": "local echo should not be stored"
        }),
        123,
    )
    .expect("self echo projection is ignored");

    assert!(result.is_none());
    let threads =
        crate::control::load_peer_dm_thread_records_state(&state_dir).expect("load dm threads");
    assert!(threads.is_empty());
}

#[test]
fn inbound_private_dm_topic_message_direct_projection_still_saves_remote_author() {
    let state_dir = temp_startup_dir("inbound-private-dm-direct-projection");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let local_node_id = local.node_id();
    let remote_node_id = random_network_node_id().to_string();
    let projection = save_inbound_private_dm_topic_message(
        &state_dir,
        &local_node_id,
        &remote_node_id,
        "remote-dm-event",
        &json!({
            "kind": "direct_message",
            "thread_id": "dm:remote",
            "message_id": "dm-remote",
            "content": "remote message should be stored"
        }),
        456,
    )
    .expect("remote projection is stored")
    .expect("remote projection result");

    assert_eq!(
        projection.topic_content["content"].as_str(),
        Some("remote message should be stored")
    );
    let messages = crate::control::load_peer_dm_message_records_state(&state_dir, "dm:remote")
        .expect("load dm messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].remote_node_id, remote_node_id);
    assert_eq!(messages[0].message_id, "dm-remote");
    let threads =
        crate::control::load_peer_dm_thread_records_state(&state_dir).expect("load dm threads");
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].created_at, 456);
    assert!(threads[0].updated_at >= threads[0].created_at);
    assert_eq!(threads[0].last_message_at, Some(456));
}

#[test]
fn inbound_private_dm_topic_message_decrypts_before_projection() {
    let state_dir = temp_startup_dir("inbound-private-dm-encrypted-projection");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let local_node_id = local.node_id();
    let local_keypair =
        crate::control::load_or_create_private_message_keypair_state(&state_dir).expect("keypair");
    let remote_node_id = random_network_node_id().to_string();
    let remote_keypair = crate::crypto::generate_private_message_keypair();
    let thread_id = "dm:encrypted";
    let message_id = "dm-encrypted";
    let private_payload = json!({
        "content": {"text": "encrypted hello"},
        "agent_envelope": {
            "protocol": "google_a2a",
            "source_agent_id": "agent-remote",
            "target_agent_id": "agent-local",
            "capability": "social.dm.send",
            "message_json": "{\"content\":{\"text\":\"encrypted hello\"}}"
        }
    });
    let encrypted = crate::crypto::encrypt_private_message(
        &remote_keypair.secret_key_b64,
        &local_keypair.public_key_b64,
        &serde_json::to_vec(&private_payload).expect("encode private dm payload"),
        &crate::control::private_dm_encryption_aad(
            &remote_node_id,
            &local_node_id,
            thread_id,
            message_id,
        ),
    )
    .expect("encrypt inbound dm");

    let projection = save_inbound_private_dm_topic_message(
        &state_dir,
        &local_node_id,
        &remote_node_id,
        "remote-encrypted-dm-event",
        &json!({
            "kind": "direct_message",
            "thread_id": thread_id,
            "message_id": message_id,
            "encrypted": encrypted
        }),
        789,
    )
    .expect("remote encrypted projection is stored")
    .expect("remote encrypted projection result");

    assert!(projection.topic_content["encrypted"].is_null());
    assert_eq!(
        projection.topic_content["content"]["text"].as_str(),
        Some("encrypted hello")
    );
    let messages = crate::control::load_peer_dm_message_records_state(&state_dir, thread_id)
        .expect("load encrypted dm messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].remote_node_id, remote_node_id);
    assert_eq!(
        messages[0].content["text"].as_str(),
        Some("encrypted hello")
    );
    assert_eq!(
        messages[0]
            .agent_envelope
            .as_ref()
            .map(|envelope| envelope.capability.as_deref()),
        Some(Some("social.dm.send"))
    );
}

#[test]
fn payment_update_allowed_actions_follow_message_kind() {
    assert_eq!(
        payment_allowed_actions("payment_request"),
        vec![
            "authorize".to_owned(),
            "reject".to_owned(),
            "cancel".to_owned()
        ]
    );
    assert_eq!(
        payment_allowed_actions("payment_authorized"),
        vec!["submit".to_owned(), "cancel".to_owned()]
    );
    assert_eq!(
        payment_allowed_actions("payment_submitted"),
        vec!["settle".to_owned()]
    );
    assert!(payment_allowed_actions("payment_settled").is_empty());
    assert!(payment_allowed_actions("payment_rejected").is_empty());
    assert!(payment_allowed_actions("payment_cancelled").is_empty());
}

fn test_payment_envelope(
    source_node_id: &str,
    target_node_id: &str,
) -> wattswarm_protocol::types::AgentEnvelope {
    wattswarm_protocol::types::AgentEnvelope {
        protocol: "google_a2a".to_owned(),
        source_agent_id: Some("did:key:remote-payment-agent".to_owned()),
        target_agent_id: Some("did:key:local-payment-agent".to_owned()),
        source_node_id: Some(source_node_id.to_owned()),
        target_node_id: Some(target_node_id.to_owned()),
        capability: Some("agent.payment".to_owned()),
        message_json: json!({
            "payment_id": "payment-backfill-1",
            "message_kind": "payment_request"
        })
        .to_string(),
        extensions_json: None,
        signature: None,
        ..wattswarm_protocol::types::AgentEnvelope::default()
    }
}

fn test_payment_payload(
    local_node_id: &str,
    remote_node_id: &str,
) -> crate::types::AgentPaymentPostedPayload {
    crate::types::AgentPaymentPostedPayload {
        network_id: "default".to_owned(),
        remote_node_id: local_node_id.to_owned(),
        message_kind: "payment_request".to_owned(),
        payment: json!({
            "payment_id": "payment-backfill-1",
            "amount": "12.50",
            "currency": "USDC"
        }),
        agent_envelope: test_payment_envelope(remote_node_id, local_node_id),
    }
}

fn test_raw_payment_envelope(source_node_id: &str, target_node_id: &str) -> RawAgentEnvelope {
    RawAgentEnvelope {
        protocol: "google_a2a".to_owned(),
        transport_profile: Some("wattswarm_mesh".to_owned()),
        source_agent_id: Some("did:key:local-payment-agent".to_owned()),
        target_agent_id: Some("did:key:remote-payment-agent".to_owned()),
        source_node_id: Some(source_node_id.to_owned()),
        target_node_id: Some(target_node_id.to_owned()),
        capability: Some("agent.payment".to_owned()),
        source_agent_card: None,
        message_json: json!({
            "payment_id": "payment-outbound-1",
            "message_kind": "payment_request"
        })
        .to_string(),
        extensions_json: None,
        signature: None,
    }
}

#[test]
fn pending_agent_payment_command_records_reliable_event() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let state_dir = temp_startup_dir("pending-payment-event");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("p2p node"),
        &[SwarmScope::Global, SwarmScope::Node(remote.node_id())],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path);
    super::super::peer_interactions::enqueue_agent_payment_command(
        &state_dir,
        &remote.node_id(),
        "payment_request",
        json!({
            "payment_id": "payment-outbound-1",
            "amount": "12.50",
            "currency": "USDC"
        }),
        test_raw_payment_envelope(&local.node_id(), &remote.node_id()),
    )
    .expect("enqueue payment command");

    let processed = super::super::peer_interactions::process_pending_network_commands(
        &mut node,
        &mut service,
        &state_dir,
    )
    .expect("process pending payment command");

    assert_eq!(processed, 1);
    let rows = node.store.load_events_page(0, 10).expect("load events");
    let payment_events = rows
        .iter()
        .filter_map(|(_, event)| match &event.payload {
            crate::types::EventPayload::AgentPaymentPosted(payload) => Some(payload),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(payment_events.len(), 1);
    assert_eq!(payment_events[0].remote_node_id, remote.node_id());
    assert_eq!(payment_events[0].message_kind, "payment_request");
    assert_eq!(
        payment_events[0].payment["payment_id"].as_str(),
        Some("payment-outbound-1")
    );
}

#[test]
fn live_agent_payment_event_delivers_local_agent_event() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let state_dir = temp_startup_dir("live-payment-agent-event");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let payment_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::AgentPaymentPosted(test_payment_payload(
            &local.node_id(),
            &remote.node_id(),
        )),
    )
    .expect("payment event");
    let scope = SwarmScope::Node(local.node_id());
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path);

    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::Gossip {
                propagation_source: random_network_node_id(),
                message: GossipMessage::Event(EventEnvelope {
                    scope,
                    event: payment_event,
                    content_source_node_id: None,
                }),
            },
        )
        .expect("process live payment event");

    let payments =
        crate::control::load_agent_payment_records_state(&state_dir).expect("load payments");
    assert_eq!(payments.len(), 1);
    assert_eq!(payments[0].payment_id, "payment-backfill-1");
    assert_eq!(payments[0].message_kind, "payment_request");
    assert_eq!(payments[0].remote_node_id, remote.node_id());
    let records =
        crate::control::load_agent_event_records_state(&state_dir).expect("agent event records");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].event_type,
        wattswarm_protocol::types::AgentEventType::PaymentRequest
    );
    assert_eq!(
        records[0].payload["message_kind"].as_str(),
        Some("payment_request")
    );
}

#[test]
fn backfill_agent_payment_event_delivers_local_agent_event() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let state_dir = temp_startup_dir("backfill-payment-agent-event");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let payment_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::AgentPaymentPosted(test_payment_payload(
            &local.node_id(),
            &remote.node_id(),
        )),
    )
    .expect("payment event");
    let scope = SwarmScope::Node(local.node_id());
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path);
    let request_id = BackfillRequestId::new(79);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillResponse {
                peer: random_network_node_id(),
                request_id,
                response: crate::network_p2p::BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 1,
                    feed_key: None,
                    head_event_ids: vec![payment_event.event_id.clone()],
                    events: vec![EventEnvelope {
                        scope,
                        event: payment_event,
                        content_source_node_id: None,
                    }],
                },
            },
        )
        .expect("process payment backfill");

    assert!(matches!(
        tick,
        NetworkBridgeTick::BackfillApplied {
            request_id: applied_request,
            events: 1,
            ..
        } if applied_request == request_id
    ));
    let payments =
        crate::control::load_agent_payment_records_state(&state_dir).expect("load payments");
    assert_eq!(payments.len(), 1);
    assert_eq!(payments[0].payment_id, "payment-backfill-1");
    assert_eq!(payments[0].message_kind, "payment_request");
    assert_eq!(payments[0].remote_node_id, remote.node_id());
    let records =
        crate::control::load_agent_event_records_state(&state_dir).expect("agent event records");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].event_type,
        wattswarm_protocol::types::AgentEventType::PaymentRequest
    );
    assert_eq!(
        records[0].dedupe_key.as_deref(),
        Some("payment:payment-backfill-1:payment_request")
    );
}

#[test]
fn agent_payment_event_and_summary_share_agent_event_dedupe() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let state_dir = temp_startup_dir("payment-event-summary-dedupe");
    let db_path = state_dir.join("ui.state");
    let local = crate::control::load_local_identity(&state_dir).expect("local identity");
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let payment_payload = test_payment_payload(&local.node_id(), &remote.node_id());
    let payment_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::AgentPaymentPosted(payment_payload.clone()),
    )
    .expect("payment event");
    let scope = SwarmScope::Node(local.node_id());
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("p2p node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), db_path);

    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::Gossip {
                propagation_source: random_network_node_id(),
                message: GossipMessage::Event(EventEnvelope {
                    scope: scope.clone(),
                    event: payment_event,
                    content_source_node_id: None,
                }),
            },
        )
        .expect("process live payment event");
    let summary = crate::network_p2p::SummaryAnnouncement {
        summary_id: "payment:payment-backfill-1:legacy-summary".to_owned(),
        source_node_id: remote.node_id(),
        scope,
        summary_kind: AGENT_PAYMENT_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: json!({
            "message_kind": payment_payload.message_kind,
            "payment": payment_payload.payment,
            "agent_envelope": payment_payload.agent_envelope,
        }),
    };
    save_agent_payment_summary(&state_dir, &remote.node_id(), &summary)
        .expect("process legacy summary");

    let records =
        crate::control::load_agent_event_records_state(&state_dir).expect("agent event records");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].dedupe_key.as_deref(),
        Some("payment:payment-backfill-1:payment_request")
    );
}

#[test]
fn deliver_agent_event_routes_decision_to_wattetheria_commit_plane() {
    let state_dir = temp_startup_dir("agent-commit-route");
    let db_path = state_dir.join("ui.state");
    let token_file = state_dir.join("control.token");
    fs::write(&token_file, "commit-token").expect("write control token");

    let seen_commit_requests: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_commit_requests_clone = Arc::clone(&seen_commit_requests);
    let commit_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind commit listener");
    let commit_addr = commit_listener.local_addr().expect("commit addr");
    std::thread::spawn(move || {
        let (mut stream, _) = commit_listener.accept().expect("accept commit connection");
        let raw = read_http_request(&mut stream);
        assert!(
            raw.to_ascii_lowercase()
                .contains("authorization: bearer commit-token")
        );
        let body = raw.split("\r\n\r\n").nth(1).expect("commit request body");
        seen_commit_requests_clone
            .lock()
            .expect("commit request mutex")
            .push(serde_json::from_str(body.trim()).expect("parse commit body"));
        let response = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 34\r\n\r\n{\"ok\":true,\"status\":\"accepted\"}";
        stream
            .write_all(response.as_bytes())
            .expect("write commit response");
    });

    let executor_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind executor listener");
    let executor_addr = executor_listener.local_addr().expect("executor addr");
    std::thread::spawn(move || {
        let (mut stream, _) = executor_listener
            .accept()
            .expect("accept executor connection");
        let raw = read_http_request(&mut stream);
        assert!(raw.contains("POST /agent-events "));
        let body = serde_json::to_string(&wattswarm_protocol::types::AgentEventCallbackResponse {
            ok: true,
            acked_at: Some(1),
            detail: Some("decision ready".to_owned()),
            decision: Some(wattswarm_protocol::types::AgentDecision {
                decision_id: "dec-1".to_owned(),
                action: "block".to_owned(),
                route: wattswarm_protocol::types::AgentDecisionRoute::WattetheriaCommit,
                reason: None,
                payload: json!({}),
            }),
        })
        .expect("serialize executor response");
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write executor response");
    });

    crate::control::save_executor_registry_state(
        &state_dir,
        &crate::control::ExecutorRegistry {
            entries: vec![crate::control::ExecutorRegistryEntry {
                name: crate::control::CORE_AGENT_EXECUTOR_NAME.to_owned(),
                base_url: "http://127.0.0.1:65535".to_owned(),
                agent_event_callback_base_url: Some(format!("http://{executor_addr}")),
                kind: crate::control::ExecutorKind::Local,
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: Some(format!("http://{commit_addr}")),
                commit_plane_token_file: Some(token_file.display().to_string()),
            }],
        },
    )
    .expect("save executor registry");

    let agent_envelope = wattswarm_protocol::types::AgentEnvelope {
        protocol: "google_a2a".to_owned(),
        source_agent_id: Some("did:key:remote-agent".to_owned()),
        target_agent_id: Some("did:key:agent".to_owned()),
        capability: Some("peer.relationship.request".to_owned()),
        message_json: json!({
            "source_public_id": "remote-public",
            "target_public_id": "local-public"
        })
        .to_string(),
        extensions_json: None,
        signature: Some("sig-a2a".to_owned()),
        ..wattswarm_protocol::types::AgentEnvelope::default()
    };
    let event = build_agent_event_with_agent_envelope(
        wattswarm_protocol::types::AgentEventType::FriendRequest,
        wattswarm_protocol::types::AgentEventSourceKind::PeerRelationship,
        Some("peer-a".to_owned()),
        Some("did:key:agent".to_owned()),
        Some(agent_envelope),
        json!({}),
        true,
        vec!["accept".to_owned(), "reject".to_owned(), "block".to_owned()],
        Some("friend-request".to_owned()),
        Some("friend_request:peer-a:1".to_owned()),
    );
    deliver_agent_event_to_local_executor(&state_dir, Some(&db_path), &event)
        .expect("deliver event");

    let scope_id = local_control_scope_id(&state_dir);
    let rows = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_events(&scope_id)
        .expect("list local agent events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "completed");

    let commit_requests = seen_commit_requests.lock().expect("read commit requests");
    assert_eq!(commit_requests.len(), 1);
    assert_eq!(
        commit_requests[0]["event"]["event_type"].as_str(),
        Some("friend_request")
    );
    assert_eq!(
        commit_requests[0]["decision"]["action"].as_str(),
        Some("block")
    );
}

#[test]
fn deliver_agent_event_routes_topic_reply_to_wattswarm_store() {
    let state_dir = temp_startup_dir("topic-direct-route");
    let db_path = state_dir.join("ui.state");
    let identity = crate::control::load_local_identity(&state_dir).expect("local identity");
    let membership = membership_with_roles(&[identity.node_id()]);
    let mut node = Node::new(
        identity.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let network_id = node
        .store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| DEFAULT_NETWORK_CONTEXT_ID.to_owned());
    let remote_message = crate::control::emit_topic_message_with_content(
        &mut node,
        &state_dir,
        &network_id,
        "crew.chat",
        "group:crew-7",
        json!({"text": "hello crew"}),
        None,
        10,
    )
    .expect("seed topic message");
    drop(node);

    let executor_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind executor listener");
    let executor_addr = executor_listener.local_addr().expect("executor addr");
    std::thread::spawn(move || {
        let (mut stream, _) = executor_listener
            .accept()
            .expect("accept executor connection");
        let raw = read_http_request(&mut stream);
        assert!(raw.contains("POST /agent-events "));
        let body = serde_json::to_string(&wattswarm_protocol::types::AgentEventCallbackResponse {
            ok: true,
            acked_at: Some(1),
            detail: Some("topic reply".to_owned()),
            decision: Some(wattswarm_protocol::types::AgentDecision {
                decision_id: "dec-topic-direct".to_owned(),
                action: "reply".to_owned(),
                route: wattswarm_protocol::types::AgentDecisionRoute::WattswarmDirect,
                reason: None,
                payload: json!({
                    "content": {
                        "kind": "message",
                        "text": "roger that"
                    }
                }),
            }),
        })
        .expect("serialize executor response");
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write executor response");
    });

    crate::control::save_executor_registry_state(
        &state_dir,
        &crate::control::ExecutorRegistry {
            entries: vec![crate::control::ExecutorRegistryEntry {
                name: crate::control::CORE_AGENT_EXECUTOR_NAME.to_owned(),
                base_url: format!("http://{executor_addr}"),
                agent_event_callback_base_url: None,
                kind: crate::control::ExecutorKind::Local,
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let event = build_agent_event(
        wattswarm_protocol::types::AgentEventType::TopicMessageRequiresReply,
        wattswarm_protocol::types::AgentEventSourceKind::TopicMessage,
        Some("peer-a".to_owned()),
        None,
        json!({
            "network_id": network_id,
            "message_id": remote_message.event_id,
            "feed_key": "crew.chat",
            "scope_hint": "group:crew-7",
            "content": {"text": "hello crew"},
        }),
        false,
        vec!["reply".to_owned(), "ignore".to_owned()],
        Some("crew.chat".to_owned()),
        Some(format!("topic_message:{}", remote_message.event_id)),
    );
    deliver_agent_event_to_local_executor(&state_dir, Some(&db_path), &event)
        .expect("deliver topic event");
    let scope_id = local_control_scope_id(&state_dir);
    let rows = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_events(&scope_id)
        .expect("list local agent events");
    let deliveries = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_event_deliveries(&scope_id, &event.event_id)
        .expect("list local agent event deliveries");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "completed", "{rows:?} {deliveries:?}");

    let node = crate::control::open_node(&state_dir, &db_path).expect("reopen node");
    let reply_network_id = current_network_context_id(&node);
    let messages = node
        .store
        .list_topic_messages(&reply_network_id, "crew.chat", "group:crew-7", 10)
        .expect("list topic messages");
    let reply = messages
        .iter()
        .find(|message| {
            message.reply_to_message_id.as_deref() == Some(remote_message.event_id.as_str())
                && message.content["text"].as_str() == Some("roger that")
        })
        .unwrap_or_else(|| panic!("reply message recorded in wattswarm store: {messages:?}"));
    assert_eq!(reply.feed_key, "crew.chat");
}

#[test]
fn deliver_agent_event_routes_task_result_accept_to_wattswarm_projection() {
    let state_dir = temp_startup_dir("task-result-direct-route");
    let db_path = state_dir.join("ui.state");
    let identity = crate::control::load_local_identity(&state_dir).expect("local identity");
    let membership = membership_with_roles(&[identity.node_id()]);
    let mut node = Node::new(
        identity.clone(),
        PgStore::open(&db_path).expect("store"),
        membership,
    )
    .expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let contract = sample_contract("task-direct-1", policy_hash);
    node.submit_task(contract, 1, 10).expect("submit task");
    node.claim_task(
        "task-direct-1",
        crate::types::ClaimRole::Propose,
        "exec-direct-1",
        50,
        1,
        11,
    )
    .expect("claim task");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-direct-1".to_owned(),
        execution_id: "exec-direct-1".to_owned(),
        output_ref: crate::control::materialize_candidate_output_artifact(
            &state_dir,
            &identity.node_id(),
            &json!({"answer": "ok"}),
            11,
        )
        .expect("candidate artifact"),
        output: json!({"answer": "ok"}),
        evidence_inline: Vec::new(),
        evidence_refs: Vec::new(),
    };
    node.propose_candidate("task-direct-1", candidate, 1, 12)
        .expect("propose candidate");
    drop(node);

    let executor_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind executor listener");
    let executor_addr = executor_listener.local_addr().expect("executor addr");
    std::thread::spawn(move || {
        let (mut stream, _) = executor_listener
            .accept()
            .expect("accept executor connection");
        let raw = read_http_request(&mut stream);
        assert!(raw.contains("POST /agent-events "));
        let body = serde_json::to_string(&wattswarm_protocol::types::AgentEventCallbackResponse {
            ok: true,
            acked_at: Some(1),
            detail: Some("task accepted".to_owned()),
            decision: Some(wattswarm_protocol::types::AgentDecision {
                decision_id: "dec-task-direct".to_owned(),
                action: "accept_result".to_owned(),
                route: wattswarm_protocol::types::AgentDecisionRoute::WattswarmDirect,
                reason: None,
                payload: json!({}),
            }),
        })
        .expect("serialize executor response");
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write executor response");
    });

    crate::control::save_executor_registry_state(
        &state_dir,
        &crate::control::ExecutorRegistry {
            entries: vec![crate::control::ExecutorRegistryEntry {
                name: crate::control::CORE_AGENT_EXECUTOR_NAME.to_owned(),
                base_url: format!("http://{executor_addr}"),
                agent_event_callback_base_url: None,
                kind: crate::control::ExecutorKind::Local,
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let event = build_agent_event(
        wattswarm_protocol::types::AgentEventType::TaskResultReceived,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
        Some("peer-a".to_owned()),
        None,
        json!({
            "event_kind": "candidate_proposed",
            "task_id": "task-direct-1",
            "candidate_id": "cand-direct-1",
        }),
        false,
        vec!["accept_result".to_owned()],
        Some("task-direct-1".to_owned()),
        Some("task_result:task-direct-1:cand-direct-1".to_owned()),
    );
    deliver_agent_event_to_local_executor(&state_dir, Some(&db_path), &event)
        .expect("deliver task result event");
    let scope_id = local_control_scope_id(&state_dir);
    let rows = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_events(&scope_id)
        .expect("list local agent events");
    let deliveries = local_control_store(&state_dir)
        .expect("open local control store")
        .list_local_agent_event_deliveries(&scope_id, &event.event_id)
        .expect("list local agent event deliveries");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "completed", "{rows:?} {deliveries:?}");

    let node = crate::control::open_node(&state_dir, &db_path).expect("reopen node");
    let task = node
        .task_view("task-direct-1")
        .expect("task view")
        .expect("task exists");
    assert_eq!(
        task.committed_candidate_id.as_deref(),
        Some("cand-direct-1")
    );
    assert_eq!(
        task.finalized_candidate_id.as_deref(),
        Some("cand-direct-1")
    );
}
