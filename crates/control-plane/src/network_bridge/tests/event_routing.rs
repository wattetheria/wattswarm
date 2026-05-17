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
        }),
    )
    .expect("claim event");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("p2p node"),
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
        vec!["inspect_task".to_owned()],
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
            "inspect_task".to_owned(),
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

    let event = build_agent_event(
        wattswarm_protocol::types::AgentEventType::FriendRequest,
        wattswarm_protocol::types::AgentEventSourceKind::PeerRelationship,
        Some("peer-a".to_owned()),
        Some("did:key:agent".to_owned()),
        json!({
            "agent_envelope": {
                "message": {
                    "source_public_id": "remote-public",
                    "target_public_id": "local-public"
                }
            }
        }),
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
