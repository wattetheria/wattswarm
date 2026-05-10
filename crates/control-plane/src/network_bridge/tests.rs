use super::*;
use crate::crypto::{NodeIdentity, candidate_hash, sha256_hex, vote_commit_hash};
use crate::network_p2p::PeerDiscoverySourceKind;
use crate::node::{build_event_for_external, finality_sign, sign_membership_quorum};
use crate::storage::{
    DecisionMemoryHitRow, ImportedTaskOutcomeRow, PgStore, ProjectionScope, ReputationSnapshotRow,
    local_control_scope_id, local_control_store,
};
use crate::types::{
    ClaimRole, FinalityProof, Membership, NetworkKind, Role, VoteChoice, VoteCommitPayload,
    VoteRevealPayload,
};
use crate::{node::Node, task_template::sample_contract};
use serde_json::json;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;

fn env_test_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env_test_mutex() -> std::sync::MutexGuard<'static, ()> {
    env_test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn temp_startup_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wattswarm-network-bridge-{prefix}-{}",
        Uuid::new_v4().simple()
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn random_network_node_id() -> NetworkNodeId {
    let uuid = Uuid::new_v4();
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(uuid.as_bytes());
    bytes[16..].copy_from_slice(uuid.as_bytes());
    crate::network_p2p::network_peer_id_from_ed25519_public_key(
        NodeIdentity::from_seed(bytes).verifying_key().to_bytes(),
    )
    .expect("valid test network node id")
}

fn sample_topic_content_ref(digest: &str, producer: &str) -> crate::types::ArtifactRef {
    crate::types::ArtifactRef {
        uri: format!("artifact://topic-message/{digest}"),
        digest: digest.to_owned(),
        size_bytes: 64,
        mime: "application/json".to_owned(),
        created_at: 10,
        producer: producer.to_owned(),
    }
}

fn read_http_request(stream: &mut std::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut header_end = None;
    loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).expect("read request");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if header_end.is_none()
            && let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
        {
            header_end = Some(position + 4);
            break;
        }
    }

    let header_end = header_end.expect("header terminator");
    let header_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                Some(value.trim().parse::<usize>().expect("content length"))
            } else {
                None
            }
        })
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let mut chunk = vec![0_u8; header_end + content_length - buffer.len()];
        let read = stream.read(&mut chunk).expect("read request body");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(buffer).expect("utf8 request")
}

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

fn membership_with_roles(node_ids: &[String]) -> Membership {
    let mut membership = Membership::new();
    for node_id in node_ids {
        for role in [
            Role::Proposer,
            Role::Verifier,
            Role::Committer,
            Role::Finalizer,
        ] {
            membership.grant(node_id, role);
        }
    }
    membership
}

fn mainnet_node(identity: NodeIdentity) -> Node {
    let genesis_node_id = identity.node_id();
    mainnet_node_with_genesis(identity, &genesis_node_id)
}

fn mainnet_node_with_genesis(identity: NodeIdentity, genesis_node_id: &str) -> Node {
    let store = PgStore::open_in_memory().expect("store");
    let topology = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet-auth-test",
            "Mainnet Auth Test",
            genesis_node_id,
            genesis_node_id,
            100,
        )
        .expect("mainnet topology");
    Node::new(
        identity,
        store.for_org(topology.org.org_id),
        Membership::new(),
    )
    .expect("node")
}

struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let prev = env::var(key).ok();
        // SAFETY: unit tests in this module only mutate a small set of env vars locally.
        unsafe {
            if let Some(value) = value {
                env::set_var(key, value);
            } else {
                env::remove_var(key);
            }
        }
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: unit tests in this module only mutate a small set of env vars locally.
        unsafe {
            if let Some(prev) = &self.prev {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }
}

#[test]
fn ingest_event_envelope_applies_remote_event_to_local_node() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-1", policy_hash);
    contract.inputs = json!({"prompt":"hello from network"});
    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TaskCreated(contract.clone()),
    )
    .expect("signed event");
    let envelope = EventEnvelope {
        scope: SwarmScope::Global,
        event: remote_event,
        content_source_node_id: None,
    };

    ingest_event_envelope(&mut node, &envelope).expect("ingest envelope");

    let task = node
        .task_view("task-network-1")
        .expect("task view")
        .expect("task exists");
    assert_eq!(task.contract.task_id, "task-network-1");
    assert_eq!(task.contract.inputs["prompt"], json!("hello from network"));
}

#[test]
fn event_envelope_from_gossip_rejects_non_event_messages() {
    let message = GossipMessage::Rule(crate::network_p2p::RuleAnnouncement {
        scope: SwarmScope::Global,
        rule_set: "galaxy".to_owned(),
        rule_version: 2,
        activation_epoch: Some(7),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    });
    assert!(event_envelope_from_gossip(&message).is_err());
}

#[test]
fn ingest_chat_gossip_applies_remote_topic_message_to_local_store() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");

    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:hello-crew", &remote.node_id()),
            local_content_cache: Some(json!({"text":"hello crew"})),
            reply_to_message_id: None,
        }),
    )
    .expect("signed event");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            bootstrap_peers: Vec::new(),
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())],
        &crate::types::NetworkProtocolParams::default(),
    )
    .expect("service");

    let tick = service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::Gossip {
                propagation_source: random_network_node_id(),
                message: GossipMessage::Chat(EventEnvelope {
                    scope: SwarmScope::Group("crew-7".to_owned()),
                    event: remote_event.clone(),
                    content_source_node_id: None,
                }),
            }),
        )
        .expect("apply runtime event");

    assert!(matches!(
        tick,
        NetworkBridgeTick::EventIngested { event_id, .. } if event_id == remote_event.event_id
    ));
    let messages = node
        .store
        .list_topic_messages("default", "crew.chat", "group:crew-7", 10)
        .expect("list topic messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, remote_event.event_id);
    assert_eq!(messages[0].content["text"], json!("hello crew"));
}

#[test]
fn global_event_gossip_rejects_non_global_scope() {
    let envelope = EventEnvelope {
        scope: SwarmScope::Region("sol-1".to_owned()),
        event: build_event_for_external(
            &NodeIdentity::random(),
            1,
            10,
            crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
                checkpoint_id: "cp-1".to_owned(),
                up_to_seq: 1,
            }),
        )
        .expect("event"),
        content_source_node_id: None,
    };

    assert!(global_event_gossip(envelope).is_err());
}

#[test]
fn backfill_response_for_request_wraps_global_events() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-backfill-1", policy_hash);
    contract.inputs = json!({"prompt":"backfill me"});
    node.submit_task(contract, 1, 100).expect("submit task");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert_eq!(response.events.len(), 1);
    assert_eq!(response.scope, SwarmScope::Global);
    assert_eq!(response.next_from_event_seq, 1);
}

#[test]
fn iroh_backfill_stream_repairs_missed_global_event_and_ingest_is_idempotent() {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestIrohBackfillStreamRequest {
        request: BackfillRequest,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestIrohBackfillStreamResponse {
        response: crate::network_p2p::BackfillResponse,
    }

    const IROH_CONTROL_KIND_BACKFILL: &str = "backfill.v1";

    let dir_a = temp_startup_dir("iroh-backfill-a");
    let dir_b = temp_startup_dir("iroh-backfill-b");
    crate::control::local_node_id(&dir_a).expect("seed node a");
    crate::control::local_node_id(&dir_b).expect("seed node b");
    let peer_a = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_a)
        .expect("endpoint a")
        .to_string();
    let peer_b = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_b)
        .expect("endpoint b")
        .to_string();

    let subscriber_identity = NodeIdentity::random();
    let publisher_identity = NodeIdentity::random();
    let membership =
        membership_with_roles(&[subscriber_identity.node_id(), publisher_identity.node_id()]);
    let mut subscriber = Node::new(
        subscriber_identity,
        PgStore::open_in_memory().expect("subscriber store"),
        membership.clone(),
    )
    .expect("subscriber node");
    let mut publisher = Node::new(
        publisher_identity,
        PgStore::open_in_memory().expect("publisher store"),
        membership,
    )
    .expect("publisher node");
    let policy_hash = publisher
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-iroh-backfill-1", policy_hash);
    contract.inputs = json!({"prompt":"repair me over iroh"});
    publisher
        .submit_task(contract, 1, 100)
        .expect("publisher submits task");

    wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id(
        &dir_a, &peer_a, 1,
    )
    .expect("contact a");
    let contact_b =
        wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id(
            &dir_b, &peer_b, 1,
        )
        .expect("contact b");

    let bridge_response = backfill_response_for_request(
        &publisher,
        &peer_b,
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("bridge backfill response");
    let bridge_response_payload = serde_json::to_vec(&TestIrohBackfillStreamResponse {
        response: bridge_response,
    })
    .expect("encode bridge response");
    wattswarm_network_transport_iroh::set_local_control_stream_handler_for_network_peer_id(
        &dir_b,
        &peer_b,
        IROH_CONTROL_KIND_BACKFILL,
        Some(
            move |request: wattswarm_network_transport_iroh::IrohControlStreamRequest| {
                if request.kind != IROH_CONTROL_KIND_BACKFILL {
                    return wattswarm_network_transport_iroh::IrohControlStreamResponse {
                        ok: false,
                        error: Some(format!("unexpected control kind {}", request.kind)),
                        payload: Vec::new(),
                    };
                }
                let decoded =
                    match serde_json::from_slice::<TestIrohBackfillStreamRequest>(&request.payload)
                    {
                        Ok(decoded) => decoded,
                        Err(err) => {
                            return wattswarm_network_transport_iroh::IrohControlStreamResponse {
                                ok: false,
                                error: Some(err.to_string()),
                                payload: Vec::new(),
                            };
                        }
                    };
                if decoded.request.scope != SwarmScope::Global {
                    return wattswarm_network_transport_iroh::IrohControlStreamResponse {
                        ok: false,
                        error: Some("unexpected scope".to_owned()),
                        payload: Vec::new(),
                    };
                }
                wattswarm_network_transport_iroh::IrohControlStreamResponse {
                    ok: true,
                    error: None,
                    payload: bridge_response_payload.clone(),
                }
            },
        ),
    )
    .expect("install backfill handler");

    wattswarm_network_transport_iroh::register_remote_contact_material_for_network_peer_id(
        &dir_a, &peer_a, &contact_b,
    )
    .expect("register remote contact material");

    let response =
        wattswarm_network_transport_iroh::send_control_stream_request_for_network_peer_id(
            &dir_a,
            &peer_a,
            &contact_b,
            &wattswarm_network_transport_iroh::IrohControlStreamRequest {
                kind: IROH_CONTROL_KIND_BACKFILL.to_owned(),
                payload: serde_json::to_vec(&TestIrohBackfillStreamRequest {
                    request: BackfillRequest {
                        scope: SwarmScope::Global,
                        from_event_seq: 0,
                        limit: 8,
                        feed_key: None,
                        known_event_ids: Vec::new(),
                    },
                })
                .expect("encode backfill request"),
            },
        )
        .expect("request iroh backfill page");
    assert!(response.ok, "{:?}", response.error);
    let response = serde_json::from_slice::<TestIrohBackfillStreamResponse>(&response.payload)
        .expect("decode backfill response")
        .response;
    assert_eq!(response.events.len(), 1);

    let applied = ingest_backfill_response(&mut subscriber, &response).expect("ingest response");
    let duplicate_applied =
        ingest_backfill_response(&mut subscriber, &response).expect("ingest duplicate response");
    assert_eq!(applied, 1);
    assert_eq!(duplicate_applied, 0);
    assert!(
        subscriber
            .store
            .task_projection("task-iroh-backfill-1")
            .expect("load task projection")
            .is_some()
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_a);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_b);
    std::fs::remove_dir_all(dir_a).expect("cleanup a");
    std::fs::remove_dir_all(dir_b).expect("cleanup b");
}

#[test]
fn ingest_backfill_response_rejects_scope_mismatch() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let response = crate::network_p2p::BackfillResponse {
        scope: SwarmScope::Global,
        next_from_event_seq: 1,
        feed_key: None,
        head_event_ids: Vec::new(),
        events: vec![EventEnvelope {
            scope: SwarmScope::Region("sol".to_owned()),
            event: build_event_for_external(
                &NodeIdentity::random(),
                1,
                10,
                crate::types::EventPayload::CheckpointCreated(
                    crate::types::CheckpointCreatedPayload {
                        checkpoint_id: "cp-2".to_owned(),
                        up_to_seq: 0,
                    },
                ),
            )
            .expect("event"),
            content_source_node_id: None,
        }],
    };

    assert!(ingest_backfill_response(&mut node, &response).is_err());
}

#[test]
fn topic_backfill_response_filters_by_feed_key() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    node.emit_at(
        1,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:hello-crew", "node-local"),
            local_content_cache: Some(json!({"text":"hello crew"})),
            reply_to_message_id: None,
        }),
        10,
    )
    .expect("emit topic message");
    node.emit_at(
        1,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "market.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:ignore-me", "node-local"),
            local_content_cache: Some(json!({"text":"ignore me"})),
            reply_to_message_id: None,
        }),
        11,
    )
    .expect("emit other topic message");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Group("crew-7".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: Some("crew.chat".to_owned()),
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("topic backfill response");

    assert_eq!(response.feed_key.as_deref(), Some("crew.chat"));
    assert_eq!(response.events.len(), 1);
    assert!(matches!(
        &response.events[0].event.payload,
        crate::types::EventPayload::TopicMessagePosted(payload) if payload.feed_key == "crew.chat"
    ));
}

#[test]
fn topic_backfill_response_advances_local_cursor() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("node");
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local.node_id(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["messages".to_owned()],
                active: true,
            },
        ),
        5,
    )
    .expect("subscribe");

    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:cursor-me", &remote.node_id()),
            local_content_cache: Some(json!({"text":"cursor me"})),
            reply_to_message_id: None,
        }),
    )
    .expect("remote event");
    let response = crate::network_p2p::BackfillResponse {
        scope: SwarmScope::Group("crew-7".to_owned()),
        next_from_event_seq: 7,
        feed_key: Some("crew.chat".to_owned()),
        head_event_ids: Vec::new(),
        events: vec![EventEnvelope {
            scope: SwarmScope::Group("crew-7".to_owned()),
            event: remote_event,
            content_source_node_id: None,
        }],
    };

    ingest_backfill_response(&mut node, &response).expect("apply topic backfill");
    maybe_record_topic_cursor_for_response(&node, &local.node_id(), &response, 100)
        .expect("record topic cursor");

    let cursor = node
        .store
        .get_topic_cursor("default", &local.node_id(), "crew.chat")
        .expect("get topic cursor")
        .expect("cursor exists");
    assert_eq!(cursor.last_event_seq, 2);
    assert_eq!(cursor.scope_hint, "group:crew-7");
}

#[test]
fn network_config_defaults_to_enabled_with_fixed_tcp_port() {
    let _lock = lock_env_test_mutex();
    let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, None);
    let _local_discovery = EnvVarGuard::set(ENV_P2P_LOCAL_DISCOVERY, None);
    let _port = EnvVarGuard::set(ENV_P2P_PORT, None);
    let _listen = EnvVarGuard::set(ENV_P2P_LISTEN_ADDRS, None);
    let _bootstrap = EnvVarGuard::set("WATTSWARM_P2P_BOOTSTRAP_PEERS", None);
    assert!(network_enabled_from_env());
    let config = network_config_from_env();
    assert!(config.enable_local_discovery);
    assert_eq!(config.listen_addrs, vec!["/ip4/0.0.0.0/tcp/4001"]);
    assert!(config.bootstrap_peers.is_empty());
}

#[test]
fn network_enabled_can_be_explicitly_disabled() {
    let _lock = lock_env_test_mutex();
    let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, Some("false"));
    assert!(!network_enabled_from_env());
}

#[test]
fn configured_network_scopes_include_global_by_default() {
    let _lock = lock_env_test_mutex();
    let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, None);
    let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, None);
    let _nodes = EnvVarGuard::set(ENV_P2P_NODE_IDS, None);
    assert_eq!(
        configured_network_scopes_from_env(),
        vec![SwarmScope::Global]
    );
}

#[test]
fn configured_network_scopes_include_region_and_node_aliases() {
    let _lock = lock_env_test_mutex();
    let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, Some("sol-1,sol-2"));
    let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, Some("lab-a"));
    let _nodes = EnvVarGuard::set(ENV_P2P_NODE_IDS, Some("lab-b"));
    let scopes = configured_network_scopes_from_env();
    assert_eq!(scopes.len(), 5);
    assert_eq!(scopes[0], SwarmScope::Global);
    assert!(scopes.contains(&SwarmScope::Region("sol-1".to_owned())));
    assert!(scopes.contains(&SwarmScope::Region("sol-2".to_owned())));
    assert!(scopes.contains(&SwarmScope::Node("lab-a".to_owned())));
    assert!(scopes.contains(&SwarmScope::Node("lab-b".to_owned())));
}

#[test]
fn dynamic_subscription_scopes_merge_with_configured_scopes() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: node_id.clone(),
                feed_key: "market.alpha".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let scopes = scope::merge_scopes(
        configured_network_scopes_from_env()
            .into_iter()
            .chain(scope::dynamic_subscription_scopes_for_node(&node, &node_id).expect("scopes")),
    );
    assert!(scopes.contains(&SwarmScope::Global));
    assert!(scopes.contains(&SwarmScope::Region("sol-1".to_owned())));
}

#[test]
fn publish_pending_updates_subscribes_runtime_for_local_feed_subscription() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let local_node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.beta".to_owned(),
                scope_hint: "node:lab-9".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            bootstrap_peers: Vec::new(),
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    publish_pending_scoped_updates(&mut service, &node, &local_node_id, 0)
        .expect("publish pending updates");

    assert!(
        service
            .subscribed_scopes()
            .contains(&SwarmScope::Node("lab-9".to_owned()))
    );
    let subscribed_kinds = service.subscribed_gossip_kinds(&SwarmScope::Node("lab-9".to_owned()));
    assert!(subscribed_kinds.contains(&GossipKind::Events));
    assert!(!subscribed_kinds.contains(&GossipKind::Messages));
}

#[test]
fn publish_pending_updates_unsubscribes_scope_when_local_subscription_is_disabled() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let local_node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.gamma".to_owned(),
                scope_hint: "region:sol-8".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    )
    .expect("subscription on");
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.gamma".to_owned(),
                scope_hint: "region:sol-8".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: false,
            },
        ),
        101,
    )
    .expect("subscription off");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    publish_pending_scoped_updates(&mut service, &node, &local_node_id, 0)
        .expect("publish pending updates");

    assert!(
        !service
            .subscribed_scopes()
            .contains(&SwarmScope::Region("sol-8".to_owned()))
    );
}

#[test]
fn invalid_scope_hints_are_rejected_for_network_substrate_events() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    let result = node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id,
                feed_key: "market.invalid".to_owned(),
                scope_hint: "bad-scope".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    );

    assert!(result.is_err());
}

#[test]
fn empty_network_id_is_rejected_for_network_substrate_events() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    let result = node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: String::new(),
                subscriber_node_id,
                feed_key: "market.invalid".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    );

    assert!(result.is_err());
}

#[test]
fn mismatched_network_id_is_rejected_for_network_substrate_events() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    let result = node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "mainnet:other".to_owned(),
                subscriber_node_id,
                feed_key: "market.invalid".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    );

    assert!(result.is_err());
}

#[test]
fn network_substrate_projection_canonicalizes_scope_hints() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: subscriber_node_id.clone(),
                feed_key: "market.canonical".to_owned(),
                scope_hint: " local:lab-9 ".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let row = node
        .store
        .get_feed_subscription("default", &subscriber_node_id, "market.canonical")
        .expect("load subscription")
        .expect("subscription exists");
    assert_eq!(row.scope_hint, "node:lab-9");
    assert_eq!(row.scope(), Some(ProjectionScope::Node("lab-9".to_owned())));
}

#[test]
fn task_announcement_event_persists_summary_and_detail_reference() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-announced-1", policy_hash);
    contract.inputs = json!({"prompt":"lightweight discovery"});
    node.submit_task(contract.clone(), 1, 90)
        .expect("submit task detail");
    node.emit_at(
        1,
        crate::types::EventPayload::TaskAnnounced(crate::types::TaskAnnouncedPayload {
            network_id: "default".to_owned(),
            task_id: "task-announced-1".to_owned(),
            announcement_id: "announce-1".to_owned(),
            feed_key: "venue.market".to_owned(),
            scope_hint: "region:sol-2".to_owned(),
            summary: json!({"reward": 42, "headline": "Explore relay beacon"}),
            detail_ref: Some(crate::types::ArtifactRef {
                uri: "ipfs://task-detail-1".to_owned(),
                digest: "digest-task-detail-1".to_owned(),
                size_bytes: 128,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: node.node_id(),
            }),
        }),
        100,
    )
    .expect("announce task");

    let announcement = node
        .store
        .get_task_announcement("announce-1")
        .expect("load announcement")
        .expect("announcement exists");
    assert_eq!(announcement.task_id, "task-announced-1");
    assert_eq!(announcement.feed_key, "venue.market");
    assert_eq!(announcement.scope_hint, "region:sol-2");
    assert_eq!(
        announcement.scope(),
        Some(ProjectionScope::Region("sol-2".to_owned()))
    );
    assert_eq!(announcement.summary["reward"], json!(42));
    assert_eq!(
        announcement.detail_ref.expect("detail ref").uri,
        "ipfs://task-detail-1"
    );

    let detail = node
        .store
        .get_task_announcement_detail_for_task("task-announced-1")
        .expect("load announcement detail")
        .expect("announcement detail exists");
    assert_eq!(detail.task_id(), "task-announced-1");
    assert_eq!(
        detail.contract.as_ref().expect("task contract").task_id,
        contract.task_id
    );
    assert_eq!(
        detail.detail_ref().expect("detail ref").digest,
        "digest-task-detail-1"
    );
}

#[test]
fn execution_set_events_persist_intent_and_confirmation() {
    let mut intent_node = Node::open_in_memory_with_roles(&[]).expect("intent node");
    let participant_node_id = intent_node.node_id();
    intent_node
        .emit_at(
            1,
            crate::types::EventPayload::ExecutionIntentDeclared(
                crate::types::ExecutionIntentDeclaredPayload {
                    network_id: "default".to_owned(),
                    task_id: "task-execution-1".to_owned(),
                    execution_set_id: "exec-set-1".to_owned(),
                    participant_node_id: participant_node_id.clone(),
                    role_hint: "writer".to_owned(),
                    scope_hint: "region:sol-3".to_owned(),
                    intent: "interested".to_owned(),
                },
            ),
            100,
        )
        .expect("intent event");

    let members = intent_node
        .store
        .list_execution_set_members("task-execution-1", "exec-set-1")
        .expect("members after intent");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].status, "interested");
    assert_eq!(
        members[0].scope(),
        Some(ProjectionScope::Region("sol-3".to_owned()))
    );

    let mut confirmer = Node::open_in_memory_with_roles(&[Role::Committer]).expect("confirmer");
    let confirmed_by_node_id = confirmer.node_id();
    confirmer
        .emit_at(
            1,
            crate::types::EventPayload::ExecutionSetConfirmed(
                crate::types::ExecutionSetConfirmedPayload {
                    network_id: "default".to_owned(),
                    task_id: "task-execution-1".to_owned(),
                    execution_set_id: "exec-set-1".to_owned(),
                    confirmed_by_node_id: confirmed_by_node_id.clone(),
                    scope_hint: "region:sol-3".to_owned(),
                    members: vec![crate::types::ExecutionSetMember {
                        participant_node_id: "peer-a".to_owned(),
                        role_hint: "writer".to_owned(),
                    }],
                },
            ),
            101,
        )
        .expect("confirmation event");

    let members = confirmer
        .store
        .list_execution_set_members("task-execution-1", "exec-set-1")
        .expect("members after confirmation");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].status, "confirmed");
    assert_eq!(
        members[0].scope(),
        Some(ProjectionScope::Region("sol-3".to_owned()))
    );
    assert_eq!(
        members[0].confirmed_by_node_id.as_deref(),
        Some(confirmed_by_node_id.as_str())
    );
}

#[test]
fn rule_and_checkpoint_gossip_are_applied_to_projection_store() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");

    apply_rule_announcement(
        &mut node,
        &crate::network_p2p::RuleAnnouncement {
            scope: SwarmScope::Region("sol-4".to_owned()),
            rule_set: "market-routing".to_owned(),
            rule_version: 3,
            activation_epoch: Some(9),
            authority_signer_node_id: None,
            authority_signature_hex: None,
        },
    )
    .expect("apply rule");
    apply_checkpoint_announcement(
        &mut node,
        &crate::network_p2p::CheckpointAnnouncement {
            scope: SwarmScope::Region("sol-4".to_owned()),
            checkpoint_id: "cp-sol-4".to_owned(),
            artifact_path: "ipfs://checkpoint-sol-4".to_owned(),
            authority_signer_node_id: None,
            authority_signature_hex: None,
        },
    )
    .expect("apply checkpoint");

    let rule = node
        .store
        .latest_rule_announcement("region.sol-4", "market-routing")
        .expect("load rule")
        .expect("rule exists");
    assert_eq!(rule.rule_version, 3);
    assert_eq!(rule.activation_epoch, Some(9));

    let checkpoint = node
        .store
        .get_checkpoint_announcement("region.sol-4", "cp-sol-4")
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.artifact_path, "ipfs://checkpoint-sol-4");
}

#[test]
fn mainnet_global_rule_and_checkpoint_require_genesis_authority() {
    let genesis = NodeIdentity::random();
    let genesis_node_id = genesis.node_id();
    let non_genesis = NodeIdentity::random();
    let mut node = mainnet_node(genesis);

    let unsigned_rule = crate::network_p2p::RuleAnnouncement {
        scope: SwarmScope::Global,
        rule_set: "mainnet-routing".to_owned(),
        rule_version: 1,
        activation_epoch: Some(1),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    };
    assert!(apply_rule_announcement(&mut node, &unsigned_rule).is_err());

    let signed_rule = announcements::sign_rule_announcement(&node, unsigned_rule)
        .expect("sign rule")
        .expect("genesis signs rule");
    apply_rule_announcement(&mut node, &signed_rule).expect("signed rule applies");

    let unsigned_checkpoint = crate::network_p2p::CheckpointAnnouncement {
        scope: SwarmScope::Global,
        checkpoint_id: "mainnet-cp-1".to_owned(),
        artifact_path: "finality://mainnet/task/1/candidate".to_owned(),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    };
    assert!(apply_checkpoint_announcement(&mut node, &unsigned_checkpoint).is_err());

    let signed_checkpoint = announcements::sign_checkpoint_announcement(&node, unsigned_checkpoint)
        .expect("sign checkpoint")
        .expect("genesis signs checkpoint");
    apply_checkpoint_announcement(&mut node, &signed_checkpoint)
        .expect("signed checkpoint applies");

    let non_genesis_node = mainnet_node_with_genesis(non_genesis, &genesis_node_id);
    let non_genesis_rule = crate::network_p2p::RuleAnnouncement {
        scope: SwarmScope::Global,
        rule_set: "mainnet-routing".to_owned(),
        rule_version: 2,
        activation_epoch: Some(2),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    };
    assert!(
        announcements::sign_rule_announcement(&non_genesis_node, non_genesis_rule)
            .expect("non-genesis auth check")
            .is_none()
    );
}

#[test]
fn mainnet_governance_events_require_genesis_author() {
    let genesis = NodeIdentity::random();
    let non_genesis = NodeIdentity::random();
    let mut node = mainnet_node(genesis.clone());

    let mut new_membership = Membership::new();
    new_membership.grant(&non_genesis.node_id(), Role::Verifier);
    let membership_event = build_event_for_external(
        &genesis,
        1,
        9,
        crate::types::EventPayload::MembershipUpdated(crate::types::MembershipUpdatedPayload {
            new_membership: new_membership.clone(),
            quorum_threshold: 1,
            quorum_signatures: vec![
                sign_membership_quorum(&genesis, &new_membership).expect("genesis quorum sig"),
            ],
        }),
    )
    .expect("genesis membership update");
    node.ingest_remote(membership_event)
        .expect("genesis membership update applies on empty mainnet membership");

    let non_genesis_event = build_event_for_external(
        &non_genesis,
        1,
        10,
        crate::types::EventPayload::NodePenalized(crate::types::NodePenalizedPayload {
            penalized_node_id: "node-bad".to_owned(),
            reason: "bad summary".to_owned(),
            revoked_event_ids: Vec::new(),
            revoked_summary_ids: vec!["summary-bad".to_owned()],
            block_summaries: true,
        }),
    )
    .expect("non-genesis event");
    assert!(node.ingest_remote(non_genesis_event).is_err());

    let genesis_event = build_event_for_external(
        &genesis,
        1,
        11,
        crate::types::EventPayload::NodePenalized(crate::types::NodePenalizedPayload {
            penalized_node_id: "node-bad".to_owned(),
            reason: "bad summary".to_owned(),
            revoked_event_ids: Vec::new(),
            revoked_summary_ids: vec!["summary-bad".to_owned()],
            block_summaries: true,
        }),
    )
    .expect("genesis event");
    node.ingest_remote(genesis_event)
        .expect("genesis governance event applies without membership role");

    let policy_tuned_event = build_event_for_external(
        &genesis,
        1,
        12,
        crate::types::EventPayload::PolicyTuned(crate::types::PolicyTunedPayload {
            policy_id: "vp.schema_only.v1".to_owned(),
            from_policy_hash: "policy:old".to_owned(),
            to_policy_hash: "policy:new".to_owned(),
            advisory_id: "advisory-mainnet-policy".to_owned(),
        }),
    )
    .expect("policy tuned event");
    let err = node
        .ingest_remote(policy_tuned_event)
        .expect_err("policy tuning still uses membership governance path");
    assert!(err.to_string().contains("author lacks role Finalizer"));
}

#[test]
fn ordinary_task_and_topic_events_do_not_require_membership_roles() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let mut node = Node::new(
        local,
        PgStore::open_in_memory().expect("store"),
        Membership::new(),
    )
    .expect("node");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TaskCreated(sample_contract("task-no-role", policy_hash)),
    )
    .expect("task event");
    node.ingest_remote(task_event)
        .expect("task event applies without membership role");

    let topic_event = build_event_for_external(
        &remote,
        1,
        11,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "agent.chat".to_owned(),
            scope_hint: "global".to_owned(),
            content_ref: sample_topic_content_ref("sha256:no-role-topic", &remote.node_id()),
            local_content_cache: None,
            reply_to_message_id: None,
        }),
    )
    .expect("topic event");
    node.ingest_remote(topic_event)
        .expect("topic event applies without membership role");
}

#[test]
fn ordinary_task_finalization_does_not_require_membership_finalizer_role() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let mut node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        Membership::new(),
    )
    .expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let contract = sample_contract("task-finality-no-role", policy_hash);
    node.submit_task(contract, 1, 10).expect("submit task");
    node.claim_task(
        "task-finality-no-role",
        ClaimRole::Propose,
        "exec-propose-no-role",
        100,
        1,
        11,
    )
    .expect("claim propose");
    let output = json!({"answer": "ok"});
    let output_bytes = serde_json::to_vec(&output).expect("candidate output bytes");
    let output_digest = format!("sha256:{}", sha256_hex(&output_bytes));
    let candidate = crate::types::Candidate {
        candidate_id: "cand-finality-no-role".to_owned(),
        execution_id: "exec-propose-no-role".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: format!("inline://candidate-output/{output_digest}"),
            digest: output_digest,
            size_bytes: output_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 12,
            producer: node_id,
        },
        output,
        evidence_inline: Vec::new(),
        evidence_refs: Vec::new(),
    };
    let candidate_hash = candidate_hash(&candidate).expect("candidate hash");
    node.propose_candidate("task-finality-no-role", candidate, 1, 12)
        .expect("propose candidate");
    node.claim_task(
        "task-finality-no-role",
        ClaimRole::Verify,
        "exec-verify-no-role",
        100,
        1,
        13,
    )
    .expect("claim verify");
    let verifier_result_hash = "sha256:verify-finality-no-role".to_owned();
    let salt = "salt-finality-no-role".to_owned();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, &salt, &verifier_result_hash);
    node.submit_vote_commit(
        VoteCommitPayload {
            task_id: "task-finality-no-role".to_owned(),
            candidate_id: "cand-finality-no-role".to_owned(),
            candidate_hash: candidate_hash.clone(),
            execution_id: "exec-verify-no-role".to_owned(),
            verifier_result_hash: verifier_result_hash.clone(),
            commit_hash,
        },
        1,
        14,
    )
    .expect("vote commit");
    node.submit_vote_reveal(
        VoteRevealPayload {
            task_id: "task-finality-no-role".to_owned(),
            candidate_id: "cand-finality-no-role".to_owned(),
            candidate_hash,
            execution_id: "exec-verify-no-role".to_owned(),
            verifier_result_hash,
            vote: VoteChoice::Approve,
            salt,
        },
        1,
        15,
    )
    .expect("vote reveal");
    node.commit_decision("task-finality-no-role", 1, "cand-finality-no-role", 16)
        .expect("commit decision");
    node.finalize_decision(
        "task-finality-no-role",
        1,
        "cand-finality-no-role",
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(
                &identity,
                "task-finality-no-role",
                1,
                "cand-finality-no-role",
            )],
        },
        17,
    )
    .expect("finalize without membership finalizer role");
}

#[test]
fn mainnet_non_genesis_finalization_does_not_break_outcome_summary_generation() {
    let genesis = NodeIdentity::random();
    let non_genesis = NodeIdentity::random();
    let mut node = mainnet_node_with_genesis(non_genesis.clone(), &genesis.node_id());
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let contract = sample_contract("task-mainnet-non-genesis-summary", policy_hash);
    node.submit_task(contract, 1, 10).expect("submit task");
    node.claim_task(
        "task-mainnet-non-genesis-summary",
        ClaimRole::Propose,
        "exec-mainnet-non-genesis-summary",
        100,
        1,
        11,
    )
    .expect("claim propose");
    let output = json!({"answer": "ok"});
    let output_bytes = serde_json::to_vec(&output).expect("candidate output bytes");
    let output_digest = format!("sha256:{}", sha256_hex(&output_bytes));
    let candidate = crate::types::Candidate {
        candidate_id: "cand-mainnet-non-genesis-summary".to_owned(),
        execution_id: "exec-mainnet-non-genesis-summary".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: format!("inline://candidate-output/{output_digest}"),
            digest: output_digest,
            size_bytes: output_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 12,
            producer: non_genesis.node_id(),
        },
        output,
        evidence_inline: Vec::new(),
        evidence_refs: Vec::new(),
    };
    let winning_candidate_hash = candidate_hash(&candidate).expect("candidate hash");
    node.propose_candidate("task-mainnet-non-genesis-summary", candidate, 1, 12)
        .expect("propose candidate");

    let finalized = build_event_for_external(
        &non_genesis,
        1,
        13,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: "task-mainnet-non-genesis-summary".to_owned(),
            epoch: 1,
            candidate_id: "cand-mainnet-non-genesis-summary".to_owned(),
            winning_candidate_hash,
            finality_proof: FinalityProof {
                threshold: 1,
                signatures: vec![finality_sign(
                    &non_genesis,
                    "task-mainnet-non-genesis-summary",
                    1,
                    "cand-mainnet-non-genesis-summary",
                )],
            },
        }),
    )
    .expect("finalized event");
    let summary = task_outcome_summary_for_event(&node, &finalized, &SwarmScope::Global)
        .expect("non-genesis mainnet finalization does not error");
    assert!(summary.is_none());
}

#[test]
fn administrative_task_lifecycle_events_require_roles() {
    let identity = NodeIdentity::random();
    let mut node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        Membership::new(),
    )
    .expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    node.submit_task(sample_contract("task-admin-no-role", policy_hash), 1, 10)
        .expect("submit task");

    let retry_event = build_event_for_external(
        &identity,
        1,
        11,
        crate::types::EventPayload::TaskRetryScheduled(crate::types::TaskRetryScheduledPayload {
            task_id: "task-admin-no-role".to_owned(),
            attempt: 1,
            run_at: 12,
        }),
    )
    .expect("retry event");
    let retry_err = node
        .ingest_remote(retry_event)
        .expect_err("retry requires committer");
    assert!(
        retry_err
            .to_string()
            .contains("author lacks role Committer")
    );

    for payload in [
        crate::types::EventPayload::EpochEnded(crate::types::EpochEndedPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: crate::types::EpochEndReason::TimeboxReached,
        }),
        crate::types::EventPayload::TaskStopped(crate::types::TaskStoppedPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: "admin stop".to_owned(),
        }),
        crate::types::EventPayload::TaskSuspended(crate::types::TaskSuspendedPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: "admin suspend".to_owned(),
        }),
        crate::types::EventPayload::TaskKilled(crate::types::TaskKilledPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: "admin kill".to_owned(),
        }),
    ] {
        let event = build_event_for_external(&identity, 1, 12, payload).expect("admin event");
        let err = node
            .ingest_remote(event)
            .expect_err("admin lifecycle event requires finalizer");
        assert!(err.to_string().contains("author lacks role Finalizer"));
    }
}

#[test]
fn lan_open_does_not_seed_default_protocol_roles() {
    let local_dir = temp_startup_dir("local-default-roles");
    let local_node = crate::control::open_node_in_mode(
        &local_dir,
        &local_dir.join("node.state"),
        crate::control::NodeMode::Local,
    )
    .expect("open local node");
    let local_raw = local_node
        .store
        .load_membership()
        .expect("load local membership")
        .expect("local membership");
    let local_membership: Membership =
        serde_json::from_str(&local_raw).expect("local membership json");
    assert!(local_membership.has_role(&local_node.node_id(), Role::Proposer));
    assert!(local_membership.has_role(&local_node.node_id(), Role::Verifier));
    assert!(local_membership.has_role(&local_node.node_id(), Role::Committer));
    assert!(local_membership.has_role(&local_node.node_id(), Role::Finalizer));

    let lan_dir = temp_startup_dir("lan-no-default-roles");
    let lan_node = crate::control::open_node_in_mode(
        &lan_dir,
        &lan_dir.join("node.state"),
        crate::control::NodeMode::Lan,
    )
    .expect("open lan node");
    let lan_raw = lan_node
        .store
        .load_membership()
        .expect("load lan membership")
        .expect("lan membership");
    let lan_membership: Membership = serde_json::from_str(&lan_raw).expect("lan membership json");
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Proposer));
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Verifier));
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Committer));
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Finalizer));
}

#[test]
fn network_config_ignores_legacy_bootstrap_peers_env() {
    let _lock = lock_env_test_mutex();
    let _bootstrap = EnvVarGuard::set(
        "WATTSWARM_P2P_BOOTSTRAP_PEERS",
        Some("node-a@127.0.0.1:4001"),
    );
    let config = network_config_from_env();
    assert!(config.bootstrap_peers.is_empty());
}

#[test]
fn network_config_does_not_read_bootstrap_contacts_as_legacy_peers() {
    let _lock = lock_env_test_mutex();
    let _bootstrap = EnvVarGuard::set("WATTSWARM_P2P_BOOTSTRAP_PEERS", None);
    let dir = temp_startup_dir("startup-bootstrap");
    fs::write(
        dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "bootstrap_contacts": [
                "{\"transport\":\"iroh_direct\",\"peer_id\":\"node-a\",\"metadata\":{\"route\":\"iroh_direct\",\"generated_at\":1,\"endpoint_id\":\"node-a\",\"alpn\":\"/wattswarm/iroh/1\",\"listen_addrs\":[\"127.0.0.1:4002\"],\"capabilities\":{\"supports_iroh_direct\":true,\"supports_streaming\":true,\"max_recommended_inline_bytes\":16384,\"preferred_data_route\":\"iroh_direct\"}},\"extra\":{\"endpoint_id\":\"node-a\",\"alpn\":\"/wattswarm/iroh/1\",\"direct_addrs\":[\"127.0.0.1:4002\"],\"relay_urls\":[]}}"
            ]
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");

    let config = network_config_from_state_dir(&dir);
    assert!(config.bootstrap_peers.is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn global_publish_rate_guard_limits_only_high_frequency_global_events() {
    let mut guard = GlobalPublishRateGuard::new(Instant::now());
    let event = build_event_for_external(
        &NodeIdentity::random(),
        1,
        10,
        crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
            task_id: "task-rate-limit".to_owned(),
            role: crate::types::ClaimRole::Propose,
            claimer_node_id: "node-a".to_owned(),
            execution_id: "exec-1".to_owned(),
            lease_until: 20,
        }),
    )
    .expect("event");

    for _ in 0..GLOBAL_HIGH_FREQUENCY_LIMIT {
        assert!(guard.allow(&SwarmScope::Global, &event));
    }
    assert!(!guard.allow(&SwarmScope::Global, &event));
    assert!(guard.allow(&SwarmScope::Node("lab-1".to_owned()), &event));
}

#[test]
fn smarter_backfill_prefers_peer_with_known_scope_activity() {
    let dir_a = temp_startup_dir("backfill-peer-a");
    let dir_b = temp_startup_dir("backfill-peer-b");
    std::fs::write(dir_a.join("node_seed.hex"), hex::encode([41_u8; 32])).expect("write seed a");
    std::fs::write(dir_b.join("node_seed.hex"), hex::encode([42_u8; 32])).expect("write seed b");
    let node_a = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_a)
            .expect("node a endpoint")
            .to_string(),
    )
    .expect("node a id");
    let node_b = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_b)
            .expect("node b endpoint")
            .to_string(),
    )
    .expect("node b id");
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let other_scope = SwarmScope::Node("lab-2".to_owned());
    let now = Instant::now();

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let contact_a = export_local_contact_material_for_network_peer_id(
        &dir_a,
        node_a.as_str(),
        observed_at_ms(),
    )
    .expect("contact a");
    let contact_b = export_local_contact_material_for_network_peer_id(
        &dir_b,
        node_b.as_str(),
        observed_at_ms(),
    )
    .expect("contact b");
    service
        .runtime
        .upsert_remote_contact_material(node_a.to_string(), contact_a)
        .expect("upsert contact a");
    service
        .runtime
        .upsert_remote_contact_material(node_b.to_string(), contact_b)
        .expect("upsert contact b");
    service.connected_peers.insert(node_a.clone());
    service.connected_peers.insert(node_b.clone());

    let mut state_a = PeerSyncState::new(now - Duration::from_secs(30));
    state_a.known_scopes.insert(other_scope);
    let mut state_b = PeerSyncState::new(now - Duration::from_secs(30));
    state_b.known_scopes.insert(target_scope.clone());
    service.peer_sync_state.insert(node_a, state_a);
    service.peer_sync_state.insert(node_b.clone(), state_b);

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&target_scope, now),
        Some(node_b)
    );
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_a);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_b);
    let _ = std::fs::remove_dir_all(dir_a);
    let _ = std::fs::remove_dir_all(dir_b);
}

#[test]
fn peer_backfill_cursor_is_tracked_per_scope_and_feed() {
    let scope = SwarmScope::Group("crew-7".to_owned());
    let mut state = PeerSyncState::new(Instant::now());

    assert_eq!(state.backfill_cursor(&scope, None), 0);
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 0);

    state.record_backfill_cursor(&scope, None, 5);
    state.record_backfill_cursor(&scope, Some("crew.chat"), 9);
    state.record_backfill_cursor(&scope, None, 3);
    state.reset_backfill_cursor(&scope, Some("market.chat"));

    assert_eq!(state.backfill_cursor(&scope, None), 5);
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 9);
    assert_eq!(state.backfill_cursor(&scope, Some("market.chat")), 0);
}

#[test]
fn peer_sync_state_persists_scope_cursor_and_remote_heads() {
    let dir = temp_startup_dir("peer-sync-state-persist");
    let peer = random_network_node_id();
    let scope = SwarmScope::Group("crew-7".to_owned());
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("store.state"));

    service.record_peer_scope_activity(peer.clone(), &scope);
    service.record_peer_backfill_cursor(peer.clone(), &scope, Some("crew.chat"), 42);
    service.record_peer_remote_head_event_ids(
        peer.clone(),
        &scope,
        Some("crew.chat"),
        &["evt-head".to_owned()],
    );

    let mut reloaded = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    reloaded.set_state_dir(dir.clone(), dir.join("store.state"));
    let state = reloaded
        .peer_sync_state
        .get(&peer)
        .expect("reloaded peer sync state");

    assert!(state.known_scopes.contains(&scope));
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 42);
    assert_eq!(
        state
            .remote_head_event_ids
            .get(&BackfillLaneKey::new(&scope, Some("crew.chat"))),
        Some(&vec!["evt-head".to_owned()])
    );
    assert!(!dir.join("network_peer_sync_state.json").exists());
    let persisted = crate::control::load_network_peer_sync_state_records_state(&dir)
        .expect("load peer sync DB rows");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].network_peer_id, peer.to_string());
    assert_eq!(
        serde_json::from_str::<Vec<SwarmScope>>(&persisted[0].known_scopes_json)
            .expect("known scopes JSON"),
        vec![scope]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn peer_sync_state_migrates_legacy_json_to_db() {
    let dir = temp_startup_dir("peer-sync-state-legacy-migrate");
    let peer = random_network_node_id();
    let scope = SwarmScope::Group("crew-7".to_owned());
    std::fs::write(
        legacy_peer_sync_state_path(&dir),
        serde_json::to_vec_pretty(&vec![LegacyPeerSyncStateRecord {
            peer_id: peer.to_string(),
            known_scopes: vec![scope.clone()],
            backfill_cursors: vec![PersistedBackfillCursorRecord {
                lane: BackfillLaneKey::new(&scope, Some("crew.chat")),
                cursor: 51,
            }],
            remote_heads: vec![PersistedBackfillRemoteHeadRecord {
                lane: BackfillLaneKey::new(&scope, Some("crew.chat")),
                head_event_ids: vec!["evt-head".to_owned()],
            }],
            backfill_successes: 2,
            backfill_failures: 1,
        }])
        .expect("legacy peer sync JSON"),
    )
    .expect("write legacy peer sync JSON");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("store.state"));

    assert!(!legacy_peer_sync_state_path(&dir).exists());
    let state = service
        .peer_sync_state
        .get(&peer)
        .expect("migrated peer sync state");
    assert!(state.known_scopes.contains(&scope));
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 51);
    assert_eq!(state.backfill_successes, 2);
    assert_eq!(state.backfill_failures, 1);
    let persisted = crate::control::load_network_peer_sync_state_records_state(&dir)
        .expect("load migrated peer sync DB rows");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].network_peer_id, peer.to_string());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn connection_closed_with_remaining_established_keeps_peer_state() {
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(peer.clone());
    service
        .peer_sync_state
        .insert(peer.clone(), PeerSyncState::new(Instant::now()));

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer: peer.clone(),
                remaining_established: 1,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    assert!(service.connected_peers.contains(&peer));
    assert!(service.peer_sync_state.contains_key(&peer));
}

#[test]
fn connection_closed_with_zero_remaining_preserves_peer_sync_state() {
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(peer.clone());
    service
        .peer_sync_state
        .insert(peer.clone(), PeerSyncState::new(Instant::now()));

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer: peer.clone(),
                remaining_established: 0,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::Disconnected { peer: seen } if seen == peer));
    assert!(!service.connected_peers.contains(&peer));
    assert!(service.peer_sync_state.contains_key(&peer));
    assert_eq!(service.peer_sync_state[&peer].inflight_backfills, 0);
}

#[test]
fn peer_discovered_event_persists_wan_source_into_local_registry() {
    let dir = temp_startup_dir("peer-discovered-persist");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    let peer = random_network_node_id();
    let address = "/ip4/203.0.113.10/tcp/4001"
        .parse::<NetworkAddress>()
        .expect("addr");

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::PeerDiscovered {
                peer: peer.clone(),
                address: address.clone(),
                source: PeerDiscoverySourceKind::Bootstrap,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    let rows = crate::control::load_discovered_peer_records_state(&dir).expect("load rows");
    assert_eq!(
        rows,
        vec![crate::control::DiscoveredPeerRecord {
            node_id: peer.to_string(),
            source_kind: "bootstrap".to_owned(),
        }]
    );

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn peer_identified_event_persists_peer_metadata_locally() {
    let dir = temp_startup_dir("peer-identified-metadata");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    let peer = random_network_node_id();

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::PeerMetadataObserved {
                peer: peer.clone(),
                metadata: crate::network_p2p::PeerMetadata {
                    network_id: "mainnet:watt-galaxy".to_owned(),
                    params_version: 7,
                    params_hash: "params-abc".to_owned(),
                    agent_version_raw: "wattswarm-network-p2p|mainnet:watt-galaxy|7|params-abc"
                        .to_owned(),
                    agent_version_prefix: "wattswarm-network-p2p".to_owned(),
                    protocol_version: "wattswarm/1.0.0".to_owned(),
                    observed_addr: "/ip4/198.51.100.2/tcp/4001".to_owned(),
                    listen_addrs: vec!["/ip4/203.0.113.10/tcp/4001".to_owned()],
                    protocols: vec!["/meshsub/1.1.0".to_owned()],
                },
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    let rows = crate::control::load_peer_metadata_records_state(&dir).expect("load rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, peer.to_string());
    assert_eq!(rows[0].network_id.as_deref(), Some("mainnet:watt-galaxy"));
    assert_eq!(rows[0].params_version, Some(7));
    assert_eq!(rows[0].params_hash.as_deref(), Some("params-abc"));
    assert_eq!(rows[0].handshake_status, "identified");
    assert_eq!(rows[0].listen_addrs, vec!["/ip4/203.0.113.10/tcp/4001"]);
    assert_eq!(rows[0].protocols, vec!["/meshsub/1.1.0"]);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn build_contact_material_accepts_iroh_endpoint_network_peer_id() {
    let dir = temp_startup_dir("iroh-contact-material");
    crate::control::local_node_id(&dir).expect("create local identity");
    let endpoint_id = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
        .expect("endpoint id")
        .to_string();

    let contact = build_contact_material(&dir, &endpoint_id).expect("contact material");
    let material: serde_json::Value =
        serde_json::from_str(&contact.material_json).expect("contact material json");

    assert_eq!(material["peer_id"], endpoint_id);
    assert_eq!(material["transports"][0]["transport"], "iroh_direct");
    assert_eq!(
        material["transports"][0]["metadata"]["endpoint_id"],
        endpoint_id
    );
    assert_eq!(
        material["recommended_routes"]["topic_sync"],
        serde_json::json!("iroh_direct")
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn set_state_dir_loads_persisted_iroh_contact_material_into_runtime() {
    let local_dir = temp_startup_dir("iroh-contact-load-local");
    let remote_dir = temp_startup_dir("iroh-contact-load-remote");
    let local_seed = [81u8; 32];
    let remote_seed = [82u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    crate::control::save_peer_metadata_record_state(
        &local_dir,
        &crate::control::PeerMetadataRecord {
            node_id: remote_endpoint.clone(),
            network_id: Some("mainnet".to_owned()),
            params_version: Some(1),
            params_hash: Some("params".to_owned()),
            agent_version_raw: None,
            agent_version_prefix: None,
            protocol_version: None,
            observed_addr: None,
            listen_addrs: Vec::new(),
            protocols: Vec::new(),
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(json!({
                "node_id": remote_endpoint,
                "peer_id": remote_contact.peer_id,
                "transports": [remote_contact],
            })),
            contact_material_signature: None,
            contact_material_updated_at: Some(observed_at_ms()),
            first_identified_at: observed_at_ms(),
            last_identified_at: observed_at_ms(),
        },
    )
    .expect("save peer metadata");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    assert_eq!(service.known_remote_contact_count(), 1);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn set_state_dir_loads_startup_iroh_bootstrap_contacts_into_runtime() {
    let local_dir = temp_startup_dir("iroh-startup-contact-local");
    let remote_dir = temp_startup_dir("iroh-startup-contact-remote");
    let local_seed = [91u8; 32];
    let remote_seed = [92u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    std::fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "bootstrap_contacts": [serde_json::to_string(&remote_contact).expect("contact json")]
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    assert_eq!(service.known_remote_contact_count(), 1);
    let rows =
        crate::control::load_peer_metadata_records_state(&local_dir).expect("peer metadata rows");
    assert!(rows.iter().any(|row| {
        row.node_id == remote_endpoint && row.handshake_status == "startup_contact"
    }));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn nearby_discovery_gossip_accepts_peer_inside_radius() {
    let _lock = lock_env_test_mutex();
    let _enabled = EnvVarGuard::set(ENV_NEARBY_DISCOVERY_ENABLED, None);
    let _radius = EnvVarGuard::set(ENV_NEARBY_DISCOVERY_RADIUS_KM, None);
    let _interval = EnvVarGuard::set(ENV_NEARBY_DISCOVERY_INTERVAL_MS, None);
    let _ttl = EnvVarGuard::set(ENV_NEARBY_DISCOVERY_TTL_MS, None);
    let local_dir = temp_startup_dir("nearby-discovery-local");
    let remote_dir = temp_startup_dir("nearby-discovery-remote");
    let local_seed = [93u8; 32];
    let remote_seed = [94u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    std::fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "latitude": 0.0,
            "longitude": 0.0,
            "nearby_radius_km": 20.0
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    crate::control::local_node_id(&remote_dir).expect("create remote identity");
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_contact =
        build_contact_material(&remote_dir, &remote_endpoint).expect("remote contact material");
    let discovery = PeerDiscoveryAnnouncement {
        scope: SwarmScope::Global,
        network_id: DEFAULT_NETWORK_CONTEXT_ID.to_owned(),
        source_node_id: remote_endpoint.clone(),
        latitude: 0.1,
        longitude: 0.1,
        radius_km: 20.0,
        contact_material: remote_contact,
        updated_at: observed_at_ms(),
        ttl_ms: DEFAULT_NEARBY_DISCOVERY_TTL_MS,
    };
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    let remote_peer = remote_endpoint
        .parse::<NetworkNodeId>()
        .expect("remote endpoint node id");

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::Gossip {
                propagation_source: remote_peer,
                message: GossipMessage::Discovery(discovery),
            },
        )
        .expect("process discovery");

    assert!(
        matches!(tick, NetworkBridgeTick::TransportNotice { detail } if detail.contains("nearby_discovery_accepted"))
    );
    assert!(node.peers().contains(&remote_endpoint));
    assert_eq!(service.known_remote_contact_count(), 1);
    let discovered =
        crate::control::load_discovered_peer_records_state(&local_dir).expect("discovered peers");
    assert_eq!(discovered[0].node_id, remote_endpoint);
    assert_eq!(discovered[0].source_kind, "nearby");
    let metadata =
        crate::control::load_peer_metadata_records_state(&local_dir).expect("peer metadata");
    assert_eq!(
        metadata[0].network_id.as_deref(),
        Some(DEFAULT_NETWORK_CONTEXT_ID)
    );
    assert_eq!(metadata[0].handshake_status, "nearby_discovery");

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn nearby_discovery_rejects_peer_outside_radius() {
    let settings = NearbyDiscoverySettings {
        enabled: true,
        latitude: Some(0.0),
        longitude: Some(0.0),
        radius_km: 5.0,
        interval: DEFAULT_NEARBY_DISCOVERY_INTERVAL,
        ttl_ms: DEFAULT_NEARBY_DISCOVERY_TTL_MS,
    };
    let announcement = PeerDiscoveryAnnouncement {
        scope: SwarmScope::Global,
        network_id: DEFAULT_NETWORK_CONTEXT_ID.to_owned(),
        source_node_id: "peer-a".to_owned(),
        latitude: 1.0,
        longitude: 1.0,
        radius_km: 5.0,
        contact_material: RawContactMaterial {
            material_json: "{}".to_owned(),
            signature: None,
            generated_at: observed_at_ms(),
        },
        updated_at: observed_at_ms(),
        ttl_ms: DEFAULT_NEARBY_DISCOVERY_TTL_MS,
    };

    assert!(
        nearby_discovery_is_eligible(
            &settings,
            "local-peer",
            DEFAULT_NETWORK_CONTEXT_ID,
            &announcement,
            observed_at_ms(),
        )
        .is_none()
    );
}

#[test]
fn scopes_to_request_for_peer_falls_back_to_all_scopes_until_peer_is_profiled() {
    let peer = random_network_node_id();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![SwarmScope::Global, target_scope]
    );
}

#[test]
fn local_scope_aliases_map_to_node_scope() {
    let policy_hash = "policy-hash-alias".to_owned();
    let mut contract = sample_contract("task-local-alias", policy_hash);
    contract.inputs = json!({"swarm_scope":" local:lab-1 "});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-1".to_owned())
    );

    contract.inputs = json!({"swarm_scope":{"kind":"local","id":"lab-2"}});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-2".to_owned())
    );

    contract.inputs = json!({});
    contract.task_type = "local:lab-3:swarm".to_owned();
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-3".to_owned())
    );
}

#[test]
fn parse_scope_hint_string_preserves_task_type_style_prefix_fallback() {
    assert_eq!(
        scope::parse_scope_hint_string("region:sol-1:swarm"),
        Some(SwarmScope::Region("sol-1".to_owned()))
    );
    assert_eq!(
        scope::parse_scope_hint_string(" local:lab-7:worker "),
        Some(SwarmScope::Node("lab-7".to_owned()))
    );
    assert_eq!(
        scope::parse_scope_hint_string(" group:crew-7:worker "),
        Some(SwarmScope::Group("crew-7".to_owned()))
    );
}

#[test]
fn contract_scope_object_uses_shared_canonical_scope_rules() {
    let mut contract = sample_contract("task-object-scope", "policy-hash-object".to_owned());
    contract.inputs = json!({"swarm_scope":{"kind":" local ","id":" lab-8 "}});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-8".to_owned())
    );
}

#[test]
fn backfill_response_filters_by_scope() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut global_contract = sample_contract("task-global-backfill", policy_hash.clone());
    global_contract.inputs = json!({"prompt":"global"});
    node.submit_task(global_contract, 1, 10)
        .expect("submit global");

    let mut region_contract = sample_contract("task-region-backfill", policy_hash);
    region_contract.task_type = "region:sol-1:swarm".to_owned();
    region_contract.inputs = json!({"prompt":"region", "swarm_scope":"region:sol-1"});
    node.submit_task(region_contract, 1, 11)
        .expect("submit region");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Region("sol-1".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert_eq!(response.events.len(), 1);
    assert_eq!(
        response.events[0].scope,
        SwarmScope::Region("sol-1".to_owned())
    );
    assert_eq!(
        response.events[0].event.task_id.as_deref(),
        Some("task-region-backfill")
    );
}

#[test]
fn backfill_response_skips_network_substrate_events_for_other_networks() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("node");

    let mismatched_event = build_event_for_external(
        &identity,
        1,
        100,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "mainnet:other".to_owned(),
                subscriber_node_id: node_id,
                feed_key: "market.alpha".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
    )
    .expect("event");
    node.store
        .append_event(&mismatched_event)
        .expect("append event");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Region("sol-1".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(response.events.is_empty());
}

#[test]
fn global_backfill_skips_task_process_layer_events() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-global-process-backfill", policy_hash);
    contract.inputs = json!({"prompt":"backfill without process"});
    node.submit_task(contract, 1, 100).expect("submit task");
    node.claim_task(
        "task-global-process-backfill",
        crate::types::ClaimRole::Propose,
        "exec-backfill-1",
        500,
        1,
        101,
    )
    .expect("claim task");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 16,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(
        response.events.iter().any(|envelope| matches!(
            envelope.event.payload,
            crate::types::EventPayload::TaskCreated(_)
        )),
        "global backfill should still include task detail events"
    );
    assert!(
        response.events.iter().all(|envelope| {
            !matches!(
                envelope.event.payload,
                crate::types::EventPayload::TaskClaimed(_)
                    | crate::types::EventPayload::TaskClaimRenewed(_)
                    | crate::types::EventPayload::TaskClaimReleased(_)
                    | crate::types::EventPayload::CandidateProposed(_)
                    | crate::types::EventPayload::EvidenceAdded(_)
                    | crate::types::EventPayload::EvidenceAvailable(_)
                    | crate::types::EventPayload::VerifierResultSubmitted(_)
                    | crate::types::EventPayload::VoteCommit(_)
                    | crate::types::EventPayload::VoteReveal(_)
                    | crate::types::EventPayload::DecisionCommitted(_)
                    | crate::types::EventPayload::TaskError(_)
                    | crate::types::EventPayload::TaskRetryScheduled(_)
            )
        }),
        "global backfill should suppress task process traffic"
    );
}

#[test]
fn summary_publish_is_suppressed_when_backlog_is_high() {
    assert!(should_publish_summaries(
        SUMMARY_BACKPRESSURE_HIGH_WATERMARK,
        0
    ));
    assert!(!should_publish_summaries(
        SUMMARY_BACKPRESSURE_HIGH_WATERMARK + 1,
        0
    ));
}

#[test]
fn apply_summary_announcement_imports_knowledge_and_reputation() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Region("sol-1".to_owned()),
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-a".to_owned(),
                task_type: "summary-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-knowledge-summary".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-1".to_owned(),
                    winning_candidate_hash: "candidate-hash-1".to_owned(),
                    output_digest: "output-digest-1".to_owned(),
                    result_summary: json!({"answer":"summary-import"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"ok"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "summary-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 100,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply knowledge summary");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-reputation-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::ReputationSummaryBundle {
                source_node_id: "node-a".to_owned(),
                entries: vec![crate::storage::ReputationSnapshotRow {
                    runtime_id: "runtime-a".to_owned(),
                    profile_id: "model-a".to_owned(),
                    stability_reputation: 10,
                    quality_reputation: 20,
                    last_updated_at: 101,
                }],
            })
            .expect("reputation payload"),
        },
    )
    .expect("apply reputation summary");

    let hits = node
        .store
        .list_decision_memory_hits_by_task_type("summary-type", 8)
        .expect("list hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].result_summary["answer"], json!("summary-import"));

    let reputation = node
        .store
        .get_reputation_snapshot("runtime-a", "model-a")
        .expect("get reputation")
        .expect("reputation row");
    assert_eq!(reputation.stability_reputation, 10);
    assert_eq!(reputation.quality_reputation, 20);
}

#[test]
fn subnet_summary_is_mirrored_into_parent_network_store() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let base_store = PgStore::open_in_memory().expect("store");
    let mainnet = base_store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = base_store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let subnet_store = base_store.for_org(&subnet.org.org_id);
    let mut node = Node::new(identity, subnet_store, membership).expect("subnet node");
    node.store
        .put_decision_memory(
            "task-parent-uplink",
            1,
            "commit-parent-uplink",
            100,
            "candidate-parent-uplink",
            "digest-parent-uplink",
            &json!({"answer":"compress-me"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"ok"}),
            "policy-snap",
            "uplink-type",
            "input-digest",
            "schema-digest",
            "vp.schema_only.v1",
            "params-digest",
        )
        .expect("put decision memory");

    let summary = build_knowledge_summary_for_task_type(&node, &SwarmScope::Global, "uplink-type")
        .expect("build summary")
        .expect("summary exists");
    assert!(
        mirror_summary_to_parent_network(&node, &summary).expect("mirror summary"),
        "subnet summary should mirror to parent network"
    );

    let parent_store = base_store.for_org(&mainnet.org.org_id);
    let hits = parent_store
        .list_decision_memory_hits_by_task_type("uplink-type", 8)
        .expect("list mirrored hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].result_summary["answer"], json!("compress-me"));

    let revoke_event = node
        .revoke_summary(
            &summary.summary_id,
            KNOWLEDGE_SUMMARY_KIND,
            "bad subnet summary",
            1,
            200,
        )
        .expect("revoke summary");
    assert!(
        mirror_summary_controls_to_parent_network(&node, &revoke_event)
            .expect("mirror revoke to parent"),
        "summary revoke should mirror to parent network"
    );
    assert!(
        parent_store
            .list_decision_memory_hits_by_task_type("uplink-type", 8)
            .expect("list mirrored hits after revoke")
            .is_empty()
    );
}

#[test]
fn task_outcome_summary_imports_compressed_result_facts() {
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
    let mut contract = sample_contract("task-outcome-summary", policy_hash);
    contract.task_type = "cross-layer-type".to_owned();
    node.store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-outcome".to_owned(),
        execution_id: "exec-outcome".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: "artifact://reference/cand-outcome".to_owned(),
            digest: "sha256:cand-outcome".to_owned(),
            size_bytes: 90,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        },
        output: json!({
            "answer": "compressed answer",
            "decision": "approve",
            "confidence": 0.98,
            "check_summary": "ok",
            "raw": {"large":"payload"}
        }),
        evidence_inline: vec![],
        evidence_refs: vec![crate::types::ArtifactRef {
            uri: "ipfs://evidence-1".to_owned(),
            digest: "sha256:evidence-1".to_owned(),
            size_bytes: 32,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        }],
    };
    node.store
        .put_candidate(&contract.task_id, &node_id, &candidate)
        .expect("put candidate");
    let winning_candidate_hash = crate::crypto::candidate_hash(&candidate).expect("candidate hash");
    let finalized = build_event_for_external(
        &identity,
        1,
        100,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: contract.task_id.clone(),
            epoch: 1,
            candidate_id: candidate.candidate_id.clone(),
            winning_candidate_hash,
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: vec![crate::types::SignatureEnvelope {
                    signer_node_id: node_id.clone(),
                    signature_hex: "sig-1".to_owned(),
                }],
            },
        }),
    )
    .expect("finalized event");

    let summary = summary::task_outcome_summary_for_event(
        &node,
        &finalized,
        &SwarmScope::Region("sol-1".to_owned()),
    )
    .expect("build outcome summary")
    .expect("outcome summary exists");
    apply_summary_announcement(&mut node, &summary).expect("apply outcome summary");

    let outcomes = node
        .store
        .list_imported_task_outcomes_by_task_type("cross-layer-type", 8)
        .expect("list imported outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].task_id, "task-outcome-summary");
    assert_eq!(outcomes[0].candidate_id, "cand-outcome");
    assert_eq!(
        outcomes[0].result_summary["answer"],
        json!("compressed answer")
    );
    assert_eq!(outcomes[0].result_summary["check_summary"], json!("ok"));
    assert_eq!(outcomes[0].evidence_digest_count, 1);
    assert_eq!(outcomes[0].scope_hint, "region:sol-1");
}

#[test]
fn subnet_outcome_summary_and_checkpoint_anchor_mirror_to_parent_network() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let base_store = PgStore::open_in_memory().expect("store");
    let mainnet = base_store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = base_store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let subnet_store = base_store.for_org(&subnet.org.org_id);
    let mut node = Node::new(identity.clone(), subnet_store, membership).expect("subnet node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-parent-outcome", policy_hash);
    contract.task_type = "parent-outcome-type".to_owned();
    node.store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-parent-outcome".to_owned(),
        execution_id: "exec-parent-outcome".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: "artifact://reference/cand-parent-outcome".to_owned(),
            digest: "sha256:cand-parent-outcome".to_owned(),
            size_bytes: 64,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        },
        output: json!({"answer":"uplinked","decision":"approve","check_summary":"compressed"}),
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.store
        .put_candidate(&contract.task_id, &node_id, &candidate)
        .expect("put candidate");
    let winning_candidate_hash = crate::crypto::candidate_hash(&candidate).expect("candidate hash");
    let finalized = build_event_for_external(
        &identity,
        1,
        120,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: contract.task_id.clone(),
            epoch: 1,
            candidate_id: candidate.candidate_id.clone(),
            winning_candidate_hash,
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: vec![crate::types::SignatureEnvelope {
                    signer_node_id: node_id.clone(),
                    signature_hex: "sig-parent".to_owned(),
                }],
            },
        }),
    )
    .expect("finalized event");
    let scope = SwarmScope::Global;
    let summary = summary::task_outcome_summary_for_event(&node, &finalized, &scope)
        .expect("build outcome summary")
        .expect("outcome summary exists");
    let checkpoint = announcements::checkpoint_announcement_for_event(&node, &finalized, &scope)
        .expect("build checkpoint announcement")
        .expect("checkpoint announcement exists");

    assert!(
        mirror_summary_to_parent_network(&node, &summary).expect("mirror outcome summary"),
        "subnet outcome summary should mirror to parent network"
    );
    assert!(
        announcements::mirror_checkpoint_to_parent_network(&node, &checkpoint)
            .expect("mirror checkpoint"),
        "subnet checkpoint anchor should mirror to parent network"
    );

    let parent_store = base_store.for_org(&mainnet.org.org_id);
    let outcomes = parent_store
        .list_imported_task_outcomes_by_task_type("parent-outcome-type", 8)
        .expect("list mirrored outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].result_summary["answer"], json!("uplinked"));
    assert_eq!(outcomes[0].checkpoint_id, checkpoint.checkpoint_id);
    let mirrored_checkpoint = parent_store
        .get_checkpoint_announcement("global", &checkpoint.checkpoint_id)
        .expect("load mirrored checkpoint")
        .expect("mirrored checkpoint exists");
    assert_eq!(mirrored_checkpoint.artifact_path, checkpoint.artifact_path);

    let revoke_event = node
        .revoke_summary(
            &summary.summary_id,
            TASK_OUTCOME_SUMMARY_KIND,
            "bad outcome summary",
            1,
            200,
        )
        .expect("revoke outcome summary");
    assert!(
        mirror_summary_controls_to_parent_network(&node, &revoke_event)
            .expect("mirror outcome revoke to parent"),
        "outcome summary revoke should mirror to parent network"
    );
    assert!(
        parent_store
            .list_imported_task_outcomes_by_task_type("parent-outcome-type", 8)
            .expect("list mirrored outcomes after revoke")
            .is_empty()
    );
}

#[test]
fn knowledge_summary_builder_respects_protocol_limit() {
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    for task_id in ["task-dm-1", "task-dm-2"] {
        node.store
            .put_decision_memory(
                task_id,
                1,
                &format!("commit-{task_id}"),
                100,
                &format!("candidate-{task_id}"),
                &format!("digest-{task_id}"),
                &json!({"quorum":"ok"}),
                &json!({"answer": task_id}),
                &[100],
                &json!({"detail":"ok"}),
                "policy-snap",
                "limit-type",
                "input-digest",
                "schema-digest",
                "vp.schema_only.v1",
                "params-digest",
            )
            .expect("put decision memory");
    }

    let summary = summary::build_knowledge_summary_for_task_type_with_limit(
        &node,
        &SwarmScope::Global,
        "limit-type",
        1,
    )
    .expect("build summary")
    .expect("summary exists");
    let payload: summary::KnowledgeSummaryBundle =
        serde_json::from_value(summary.payload).expect("decode payload");

    assert_eq!(payload.decisions.len(), 1);
    assert!(matches!(
        payload.decisions[0].task_id.as_str(),
        "task-dm-1" | "task-dm-2"
    ));
}

#[test]
fn service_initializes_summary_limits_from_protocol_params() {
    let params = NetworkProtocolParams {
        summary_reputation_limit: 1,
        summary_decision_memory_limit: 2,
        ..NetworkProtocolParams::default()
    };
    let service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &params,
    )
    .expect("service");
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    node.store
        .put_reputation_snapshot("runtime-a", "model-a", 10, 20, 100)
        .expect("put reputation a");
    node.store
        .put_reputation_snapshot("runtime-b", "model-b", 30, 40, 200)
        .expect("put reputation b");

    assert_eq!(service.summary_reputation_limit, 1);
    assert_eq!(service.summary_decision_memory_limit, 2);
    assert_eq!(
        node.store
            .list_local_reputation_snapshots(service.summary_reputation_limit)
            .expect("list local reputation")
            .len(),
        1
    );
}

#[test]
fn observability_snapshot_reports_network_and_sync_health() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let store = PgStore::open_in_memory().expect("store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let node = Node::new(
        identity,
        store.for_org(subnet.org.org_id.clone()),
        membership,
    )
    .expect("subnet node");

    node.store
        .put_imported_decision_memory(
            "summary-knowledge",
            "peer-a",
            &DecisionMemoryHitRow {
                task_id: "task-observe".to_owned(),
                epoch: 1,
                final_commit_hash: "commit-hash".to_owned(),
                winning_candidate_hash: "candidate-hash".to_owned(),
                output_digest: "output-digest".to_owned(),
                result_summary: json!({"answer":"ok"}),
                quorum_result: json!({"quorum":"pass"}),
                reason_codes: vec![100],
                reason_details: json!({"detail":"good"}),
                policy_snapshot_digest: "policy-snapshot".to_owned(),
                input_digest: "input-digest".to_owned(),
                output_schema_digest: "schema-digest".to_owned(),
                policy_id: "vp.schema_only.v1".to_owned(),
                task_type: "resume_review".to_owned(),
                policy_params_digest: "params-digest".to_owned(),
                deprecated_as_exact: false,
                finalized_at: 10,
                confidence_hint: 0.9,
            },
        )
        .expect("import decision memory");
    node.store
        .put_imported_reputation_snapshot(
            "summary-reputation",
            "peer-a",
            &ReputationSnapshotRow {
                runtime_id: "stub".to_owned(),
                profile_id: "p1".to_owned(),
                stability_reputation: 5,
                quality_reputation: 7,
                last_updated_at: 11,
            },
        )
        .expect("import reputation");
    node.store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-outcome".to_owned(),
            source_node_id: "peer-a".to_owned(),
            scope_hint: "global".to_owned(),
            task_id: "task-observe".to_owned(),
            task_type: "resume_review".to_owned(),
            candidate_id: "cand-1".to_owned(),
            output_digest: "digest-1".to_owned(),
            result_summary: json!({"answer":"ok"}),
            evidence_digest_count: 1,
            checkpoint_id: "cp-1".to_owned(),
            proof_artifact_path: "ipfs://cp-1".to_owned(),
            finalized_at: 12,
            revoked: false,
        })
        .expect("import task outcome");
    node.store
        .put_checkpoint_announcement("global", "cp-local", "ipfs://cp-local", 13)
        .expect("checkpoint");
    node.store
        .upsert_execution_set_member(
            "task-observe",
            "exec-1",
            "peer-a",
            "worker",
            "region:sol-1",
            "confirmed",
            Some("peer-a"),
            14,
        )
        .expect("execution set");

    let parent_store = store.for_org(mainnet.org.org_id.clone());
    parent_store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-parent".to_owned(),
            source_node_id: node.node_id(),
            scope_hint: "global".to_owned(),
            task_id: "task-parent".to_owned(),
            task_type: "resume_review".to_owned(),
            candidate_id: "cand-parent".to_owned(),
            output_digest: "digest-parent".to_owned(),
            result_summary: json!({"answer":"uplink"}),
            evidence_digest_count: 0,
            checkpoint_id: "cp-parent".to_owned(),
            proof_artifact_path: "ipfs://cp-parent".to_owned(),
            finalized_at: 15,
            revoked: false,
        })
        .expect("parent outcome");
    parent_store
        .put_checkpoint_announcement("global", "cp-parent", "ipfs://cp-parent", 16)
        .expect("parent checkpoint");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    let peer = random_network_node_id();
    service.connected_peers.insert(peer.clone());
    let mut peer_state = PeerSyncState::new(Instant::now());
    peer_state.known_scopes.insert(SwarmScope::Global);
    peer_state
        .known_scopes
        .insert(SwarmScope::Region("sol-1".to_owned()));
    peer_state.inflight_backfills = 1;
    peer_state.next_retry_at = Instant::now() + Duration::from_secs(5);
    service.peer_sync_state.insert(peer, peer_state);
    service.scope_traffic.insert(
        SwarmScope::Global,
        ScopeTrafficStats {
            published_events: 2,
            ingested_events: 3,
            summaries_applied: 1,
            rules_applied: 0,
            checkpoints_applied: 1,
            backfills_applied: 1,
            backfill_events_applied: 4,
        },
    );

    let snapshot = service
        .observability_snapshot(&node)
        .expect("observability snapshot");

    assert_eq!(snapshot.connected_peer_count, 1);
    assert_eq!(snapshot.p2p_foundation, "iroh");
    assert!(snapshot.local_iroh_endpoint_id.is_some());
    assert!(!snapshot.subscribed_iroh_gossip_topics.is_empty());
    assert_eq!(snapshot.known_iroh_contacts, 0);
    assert!(!snapshot.legacy_transport_active);
    assert_eq!(snapshot.nat_status, "iroh-direct");
    assert_eq!(snapshot.nat_public_address, None);
    assert_eq!(snapshot.nat_confidence, 0);
    assert!(snapshot.relay_reservations.is_empty());
    assert_eq!(snapshot.peer_health.len(), 1);
    assert!(snapshot.peer_health[0].connected);
    assert!(!snapshot.peer_health[0].blacklisted);
    assert_eq!(snapshot.peer_health[0].score, 0);
    assert_eq!(snapshot.peer_health[0].reputation_tier, "healthy");
    assert!(!snapshot.peer_health[0].quarantined);
    assert_eq!(snapshot.peer_health[0].quarantine_remaining_ms, 0);
    assert_eq!(snapshot.peer_health[0].ban_remaining_ms, 0);
    assert_eq!(snapshot.peer_health[0].throttle_factor_percent, 100);
    assert_eq!(
        snapshot.peer_health[0].known_scopes,
        vec!["global".to_owned(), "region:sol-1".to_owned()]
    );
    assert_eq!(snapshot.scope_traffic.len(), 1);
    assert_eq!(snapshot.scope_traffic[0].scope, "global");
    assert_eq!(snapshot.scope_traffic[0].stats.published_events, 2);
    assert_eq!(snapshot.summary_health.imported_decision_memory_rows, 1);
    assert_eq!(snapshot.summary_health.imported_reputation_rows, 1);
    assert_eq!(snapshot.summary_health.imported_task_outcome_rows, 1);
    assert_eq!(snapshot.summary_health.checkpoint_rows, 1);
    assert_eq!(
        snapshot.subnet_sync_health.network_kind,
        NetworkKind::Subnet.as_str()
    );
    assert_eq!(
        snapshot.subnet_sync_health.parent_network_id.as_deref(),
        Some(mainnet.network.network_id.as_str())
    );
    assert!(snapshot.subnet_sync_health.parent_uplink_available);
    assert_eq!(
        snapshot
            .subnet_sync_health
            .parent_imported_task_outcome_rows,
        Some(1)
    );
    assert_eq!(snapshot.subnet_sync_health.parent_checkpoint_rows, Some(1));
    assert_eq!(snapshot.execution_set_health.execution_set_count, 1);
    assert_eq!(snapshot.execution_set_health.execution_set_member_count, 1);
}

#[test]
fn summary_revocation_and_penalty_remove_imported_state() {
    let mut node =
        Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-2".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                task_type: "revoked-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-revoked-summary".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-revoked".to_owned(),
                    winning_candidate_hash: "candidate-revoked".to_owned(),
                    output_digest: "output-revoked".to_owned(),
                    result_summary: json!({"answer":"bad summary"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"bad"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 100,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply knowledge summary");
    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-reputation-2".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::ReputationSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                entries: vec![crate::storage::ReputationSnapshotRow {
                    runtime_id: "runtime-bad".to_owned(),
                    profile_id: "model-bad".to_owned(),
                    stability_reputation: 999,
                    quality_reputation: 888,
                    last_updated_at: 101,
                }],
            })
            .expect("reputation payload"),
        },
    )
    .expect("apply reputation summary");

    assert_eq!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions")
            .len(),
        1
    );
    assert!(
        node.store
            .get_reputation_snapshot("runtime-bad", "model-bad")
            .expect("get reputation")
            .is_some()
    );

    node.revoke_summary(
        "summary-knowledge-2",
        KNOWLEDGE_SUMMARY_KIND,
        "bad knowledge",
        1,
        200,
    )
    .expect("revoke summary");
    assert!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions after revoke")
            .is_empty()
    );

    node.penalize_node(
        "node-bad",
        "malicious source",
        Vec::new(),
        Vec::new(),
        1,
        201,
    )
    .expect("penalize node");
    assert!(
        node.store
            .get_reputation_snapshot("runtime-bad", "model-bad")
            .expect("get reputation after penalty")
            .is_none()
    );

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-3".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                task_type: "revoked-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-revoked-summary-2".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-revoked-2".to_owned(),
                    winning_candidate_hash: "candidate-revoked-2".to_owned(),
                    output_digest: "output-revoked-2".to_owned(),
                    result_summary: json!({"answer":"should be ignored"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"ignored"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 102,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply blocked summary");
    assert!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions after penalty")
            .is_empty()
    );
}

#[test]
fn revoked_event_rebuild_removes_previous_projection_state() {
    let mut node =
        Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-revoked-event", policy_hash);
    contract.inputs = json!({"prompt":"revoke me"});
    let created = node.submit_task(contract, 1, 100).expect("submit task");
    assert!(
        node.task_view("task-revoked-event")
            .expect("task view")
            .is_some()
    );

    node.revoke_event(&created.event_id, "malicious task", 1, 101)
        .expect("revoke event");

    assert!(
        node.task_view("task-revoked-event")
            .expect("task view after revoke")
            .is_none()
    );
}

#[test]
fn publish_pending_global_events_publishes_local_rows_and_skips_remote_rows() {
    let local = NodeIdentity::random();
    let local_node_id = local.node_id();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local_node_id.clone(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut local_contract = sample_contract("task-publish-local", policy_hash.clone());
    local_contract.inputs = json!({"prompt":"publish me"});
    node.submit_task(local_contract, 1, 100)
        .expect("local task");

    let remote_event = build_event_for_external(
        &remote,
        1,
        101,
        crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
            checkpoint_id: "cp-remote".to_owned(),
            up_to_seq: 1,
        }),
    )
    .expect("remote event");
    node.ingest_remote(remote_event).expect("ingest remote");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("network service");
    let mut peer_node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("peer node");
    let mut peer_service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("peer network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("peer network service");

    let listen_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < listen_deadline {
        let _ = service.try_tick(&mut node).expect("service tick");
        let _ = peer_service
            .try_tick(&mut peer_node)
            .expect("peer service tick");
        if !service.listen_addrs().is_empty() && !peer_service.listen_addrs().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!service.listen_addrs().is_empty());
    assert!(!peer_service.listen_addrs().is_empty());
    let peer_addr = NetworkAddress::new(format!(
        "{}@{}",
        peer_service.local_peer_id(),
        peer_service.listen_addrs()[0]
    ))
    .expect("peer dial addr");
    let service_addr = NetworkAddress::new(format!(
        "{}@{}",
        service.local_peer_id(),
        service.listen_addrs()[0]
    ))
    .expect("service dial addr");
    service.dial(peer_addr).expect("dial peer");
    peer_service
        .dial(service_addr)
        .expect("dial reciprocal peer");
    let mut service_connected = false;
    let mut peer_connected = false;
    let connect_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < connect_deadline {
        let tick = service.try_tick(&mut node).expect("service tick");
        if matches!(tick, Some(NetworkBridgeTick::Connected { .. })) {
            service_connected = true;
        }
        let peer_tick = peer_service
            .try_tick(&mut peer_node)
            .expect("peer service tick");
        if matches!(peer_tick, Some(NetworkBridgeTick::Connected { .. })) {
            peer_connected = true;
        }
        if service_connected && peer_connected {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(service_connected && peer_connected);

    let mut last = 0;
    let publish_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < publish_deadline {
        last = publish_pending_global_events(&mut service, &node, &local_node_id, 0)
            .expect("publish pending");
        if last == 2 {
            break;
        }
        let _ = service.try_tick(&mut node).expect("service tick");
        let _ = peer_service
            .try_tick(&mut peer_node)
            .expect("peer service tick");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(last, 2);
}

#[test]
fn latest_connected_peer_ids_uses_runtime_observability_snapshot() {
    let dir = std::env::temp_dir().join(format!(
        "wattswarm-network-bridge-observability-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");

    store_latest_network_observability_snapshot(
        &dir,
        NetworkBridgeObservabilitySnapshot {
            local_network_peer_id: "local-peer".to_owned(),
            local_endpoint_addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_owned()],
            p2p_foundation: "iroh".to_owned(),
            local_iroh_endpoint_id: Some("iroh-endpoint".to_owned()),
            subscribed_iroh_gossip_topics: vec!["global:task:abc123".to_owned()],
            known_iroh_contacts: 1,
            legacy_transport_active: false,
            subscribed_scopes: vec!["global".to_owned()],
            connected_peer_count: 1,
            nat_status: "unknown".to_owned(),
            nat_public_address: None,
            nat_confidence: 0,
            relay_reservations: Vec::new(),
            peer_health: vec![
                NetworkBridgePeerHealth {
                    network_peer_id: "peer-connected".to_owned(),
                    connected: true,
                    score: 0,
                    blacklisted: false,
                    reputation_tier: "healthy".to_owned(),
                    quarantined: false,
                    quarantine_remaining_ms: 0,
                    ban_remaining_ms: 0,
                    throttle_factor_percent: 100,
                    known_scopes: vec!["global".to_owned()],
                    inflight_backfills: 0,
                    next_retry_in_ms: 0,
                },
                NetworkBridgePeerHealth {
                    network_peer_id: "peer-disconnected".to_owned(),
                    connected: false,
                    score: 0,
                    blacklisted: false,
                    reputation_tier: "healthy".to_owned(),
                    quarantined: false,
                    quarantine_remaining_ms: 0,
                    ban_remaining_ms: 0,
                    throttle_factor_percent: 100,
                    known_scopes: vec!["global".to_owned()],
                    inflight_backfills: 0,
                    next_retry_in_ms: 0,
                },
            ],
            scope_traffic: Vec::new(),
            dropped_malformed_gossip: 0,
            invalid_control_payloads: 0,
            dial_failures: 0,
            response_validation_failures: 0,
            retry_suppressed_dials: 0,
            summary_health: NetworkBridgeSummaryHealth {
                imported_decision_memory_rows: 0,
                imported_reputation_rows: 0,
                imported_task_outcome_rows: 0,
                checkpoint_rows: 0,
            },
            subnet_sync_health: NetworkBridgeSubnetSyncHealth {
                network_id: "mainnet:watt-etheria".to_owned(),
                network_kind: "mainnet".to_owned(),
                parent_network_id: None,
                parent_uplink_available: false,
                parent_imported_task_outcome_rows: None,
                parent_checkpoint_rows: None,
            },
            execution_set_health: NetworkBridgeExecutionSetHealth {
                execution_set_count: 0,
                execution_set_member_count: 0,
            },
        },
    );

    assert_eq!(
        latest_connected_peer_ids(&dir),
        Some(vec!["peer-connected".to_owned()])
    );

    clear_latest_network_observability_snapshot(&dir);
    assert_eq!(latest_connected_peer_ids(&dir), None);
    let _ = std::fs::remove_dir_all(dir);
}
