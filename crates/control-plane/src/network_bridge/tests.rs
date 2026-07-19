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
use serde_json::{Map, Value, json};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;
use wattswarm_network_discovery::{
    DEFAULT_RECORD_TTL_MS, DiscoveryNodeRecordBody, DiscoveryTopicProvider,
    DiscoveryTopicProviderCapabilities,
};
use wattswarm_protocol::types::SourceAgentCard;

mod discovery_scope;
mod event_routing;
mod gossip_backfill;
mod governance;
mod network_substrate;
mod peer_sync;
mod subscriptions;
mod summary_observability;

fn env_test_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

pub(super) fn lock_env_test_mutex() -> std::sync::MutexGuard<'static, ()> {
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

fn test_network_node(config: NetworkP2pConfig) -> Result<NetworkP2pNode> {
    let seed: [u8; 32] = rand::random();
    let state_dir = temp_startup_dir("iroh-node");
    ensure_test_relay_urls(&state_dir);
    NetworkP2pNode::from_iroh_state_dir(config, state_dir, seed)
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

fn sample_protocol_agent_envelope(capability: &str) -> wattswarm_protocol::types::AgentEnvelope {
    wattswarm_protocol::types::AgentEnvelope {
        protocol: "google_a2a".to_owned(),
        source_agent_id: Some("agent-source".to_owned()),
        target_agent_id: Some("agent-target".to_owned()),
        source_node_id: Some("node-source".to_owned()),
        target_node_id: Some("node-target".to_owned()),
        capability: Some(capability.to_owned()),
        message_json: json!({
            "message_id": "msg-1",
            "body": "diagnostic envelope"
        })
        .to_string(),
        extensions_json: None,
        signature: Some("sig".to_owned()),
        ..wattswarm_protocol::types::AgentEnvelope::default()
    }
}

fn diagnostic_event_from_payload(payload: crate::types::EventPayload) -> crate::types::Event {
    let event_kind = payload.kind();
    crate::types::Event {
        event_id: format!("evt-{event_kind:?}"),
        protocol_version: "1".to_owned(),
        event_kind,
        task_id: payload.task_id().map(ToOwned::to_owned),
        swarm_scope: "global".to_owned(),
        epoch: 1,
        author_node_id: "node-source".to_owned(),
        created_at: 100,
        payload,
        signature_hex: "sig".to_owned(),
    }
}

fn diagnostic_agent_envelope(details: &Map<String, Value>) -> &Value {
    details
        .get("agent_envelope")
        .expect("diagnostic agent envelope")
}

#[test]
fn run_prefixed_task_claim_is_logged_for_stigmergy_intake() {
    let state_dir = temp_startup_dir("run-prefixed-stigmergy-claim");
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let event = diagnostic_event_from_payload(crate::types::EventPayload::TaskClaimed(
        crate::types::ClaimPayload {
            task_id: "run-open-stigmergy-task".to_owned(),
            role: ClaimRole::Propose,
            claimer_node_id: "node-source".to_owned(),
            execution_id: "exec-run-open".to_owned(),
            lease_until: 200,
            agent_envelope: None,
        },
    ));

    log_run_queue_events_if_applicable(&node, &state_dir, &event);

    let content = fs::read_to_string(state_dir.join("pending_stigmergy_contributions.jsonl"))
        .expect("pending stigmergy file");
    let entry: Value = serde_json::from_str(content.lines().next().expect("jsonl line"))
        .expect("pending stigmergy entry");
    assert_eq!(entry["kind"].as_str(), Some("claim"));
    assert_eq!(entry["task_id"].as_str(), Some("run-open-stigmergy-task"));
}

#[test]
fn run_prefixed_candidate_is_logged_for_stigmergy_and_run_queue_intake() {
    let state_dir = temp_startup_dir("run-prefixed-stigmergy-candidate");
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let event = diagnostic_event_from_payload(crate::types::EventPayload::CandidateProposed(
        crate::types::CandidateProposedPayload {
            task_id: "run-open-stigmergy-task".to_owned(),
            candidate: crate::types::Candidate {
                candidate_id: "cand-run-open".to_owned(),
                execution_id: "exec-run-open".to_owned(),
                output_ref: sample_topic_content_ref("sha256:candidate", "node-source"),
                output: json!({"decision": "support"}),
                evidence_inline: Vec::new(),
                evidence_refs: Vec::new(),
            },
            agent_envelope: None,
        },
    ));

    log_run_queue_events_if_applicable(&node, &state_dir, &event);

    let stigmergy_content =
        fs::read_to_string(state_dir.join("pending_stigmergy_contributions.jsonl"))
            .expect("pending stigmergy file");
    let stigmergy_entry: Value =
        serde_json::from_str(stigmergy_content.lines().next().expect("jsonl line"))
            .expect("pending stigmergy entry");
    assert_eq!(stigmergy_entry["kind"].as_str(), Some("candidate"));
    assert_eq!(
        stigmergy_entry["task_id"].as_str(),
        Some("run-open-stigmergy-task")
    );

    let run_queue_content = fs::read_to_string(state_dir.join("pending_run_queue_results.jsonl"))
        .expect("pending run queue file");
    let run_queue_entry: Value =
        serde_json::from_str(run_queue_content.lines().next().expect("jsonl line"))
            .expect("pending run queue entry");
    assert_eq!(
        run_queue_entry["task_id"].as_str(),
        Some("run-open-stigmergy-task")
    );
}

#[test]
fn event_diagnostics_include_agent_envelope_for_all_network_payloads() {
    let envelope = sample_protocol_agent_envelope("network.event");
    let content_ref = sample_topic_content_ref("sha256:topic", "node-source");
    let payloads = vec![
        crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
            task_id: "task-1".to_owned(),
            role: crate::types::ClaimRole::Propose,
            claimer_node_id: "node-source".to_owned(),
            execution_id: "exec-1".to_owned(),
            lease_until: 200,
            agent_envelope: Some(envelope.clone()),
        }),
        crate::types::EventPayload::CandidateProposed(crate::types::CandidateProposedPayload {
            task_id: "task-1".to_owned(),
            candidate: crate::types::Candidate {
                candidate_id: "candidate-1".to_owned(),
                execution_id: "exec-1".to_owned(),
                output_ref: content_ref.clone(),
                output: json!({"ok": true}),
                evidence_inline: Vec::new(),
                evidence_refs: Vec::new(),
            },
            agent_envelope: Some(envelope.clone()),
        }),
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: "task-1".to_owned(),
            epoch: 1,
            candidate_id: "candidate-1".to_owned(),
            winning_candidate_hash: "hash".to_owned(),
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: Vec::new(),
            },
            agent_envelope: Some(envelope.clone()),
        }),
        crate::types::EventPayload::TaskError(crate::types::TaskErrorPayload {
            task_id: "task-1".to_owned(),
            reason: crate::types::TaskErrorReason::Other,
            reason_codes: Vec::new(),
            custom_reason_namespace: None,
            custom_reason_code: None,
            custom_reason_message: None,
            message: "failed".to_owned(),
            agent_envelope: Some(envelope.clone()),
        }),
        crate::types::EventPayload::TaskRetryScheduled(crate::types::TaskRetryScheduledPayload {
            task_id: "task-1".to_owned(),
            attempt: 2,
            run_at: 300,
            agent_envelope: Some(envelope.clone()),
        }),
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "mainnet:watt-etheria".to_owned(),
                subscriber_node_id: "node-source".to_owned(),
                feed_key: "germany-economy".to_owned(),
                scope_hint: "group:germany-economy".to_owned(),
                gossip_kinds: vec!["messages".to_owned()],
                provider_capabilities: None,
                agent_envelope: Some(envelope.clone()),
                active: true,
            },
        ),
        crate::types::EventPayload::TaskAnnounced(crate::types::TaskAnnouncedPayload {
            network_id: "mainnet:watt-etheria".to_owned(),
            task_id: "task-1".to_owned(),
            announcement_id: "ann-1".to_owned(),
            feed_key: "tasks".to_owned(),
            scope_hint: "global".to_owned(),
            summary: json!({"title": "task"}),
            detail_ref: None,
            agent_envelope: Some(envelope.clone()),
        }),
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "mainnet:watt-etheria".to_owned(),
            feed_key: "germany-economy".to_owned(),
            scope_hint: "group:germany-economy".to_owned(),
            content_ref,
            local_content_cache: Some(json!({"text": "hello"})),
            reply_to_message_id: None,
            agent_envelope: Some(envelope.clone()),
        }),
    ];

    for payload in payloads {
        let details = event_diagnostic_details(&diagnostic_event_from_payload(payload));
        let diagnostic_envelope = diagnostic_agent_envelope(&details);
        assert_eq!(
            diagnostic_envelope["source_agent_id"],
            json!("agent-source")
        );
        assert_eq!(diagnostic_envelope["capability"], json!("network.event"));
    }
}

#[test]
fn topic_message_diagnostics_include_embedded_dm_agent_envelope() {
    let embedded_envelope = json!({
        "protocol": "google_a2a",
        "source_agent_id": "agent-dm-source",
        "target_agent_id": "agent-dm-target",
        "source_node_id": "node-source",
        "target_node_id": "node-target",
        "capability": "social.dm.send",
        "message": {"content": "hello"}
    });
    let event = diagnostic_event_from_payload(crate::types::EventPayload::TopicMessagePosted(
        crate::types::TopicMessagePostedPayload {
            network_id: "mainnet:watt-etheria".to_owned(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: "node:node-target".to_owned(),
            content_ref: sample_topic_content_ref("sha256:dm", "node-source"),
            local_content_cache: Some(json!({
                "kind": "direct_message",
                "agent_envelope": embedded_envelope,
            })),
            reply_to_message_id: None,
            agent_envelope: None,
        },
    ));

    let details = event_diagnostic_details(&event);

    assert_eq!(
        diagnostic_agent_envelope(&details)["source_agent_id"],
        json!("agent-dm-source")
    );
}

#[test]
fn summary_diagnostics_include_agent_envelope_from_payload() {
    let envelope = json!({
        "protocol": "google_a2a",
        "source_agent_id": "agent-payment-source",
        "target_agent_id": "agent-payment-target",
        "capability": "payment.agent_message",
        "message": {"payment_id": "pay-1"}
    });
    let summary = SummaryAnnouncement {
        summary_id: "payment:pay-1".to_owned(),
        source_node_id: "node-source".to_owned(),
        scope: SwarmScope::Node("node-target".to_owned()),
        summary_kind: AGENT_PAYMENT_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: json!({
            "message_kind": "payment_request",
            "agent_envelope": envelope,
        }),
    };

    let details = summary_diagnostic_details(&summary);

    assert_eq!(
        diagnostic_agent_envelope(&details)["source_agent_id"],
        json!("agent-payment-source")
    );
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

fn signed_discovery_record_for_test(
    state_dir: &Path,
    seed: [u8; 32],
    network_id: &str,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
) -> SignedDiscoveryNodeRecord {
    let now = observed_at_ms();
    signed_discovery_record_for_test_at(
        state_dir,
        seed,
        network_id,
        latitude,
        longitude,
        radius_km,
        now,
        DEFAULT_RECORD_TTL_MS,
    )
}

fn signed_discovery_record_for_test_at(
    state_dir: &Path,
    seed: [u8; 32],
    network_id: &str,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
    updated_at_ms: u64,
    ttl_ms: u64,
) -> SignedDiscoveryNodeRecord {
    let identity = NodeIdentity::from_seed(seed);
    ensure_test_relay_urls(state_dir);
    let peer_id = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(state_dir)
        .expect("endpoint id")
        .to_string();
    let contact =
        export_local_contact_material_for_network_peer_id(state_dir, &peer_id, updated_at_ms)
            .expect("contact");
    let mut body = DiscoveryNodeRecordBody::new(
        network_id,
        identity.node_id(),
        identity.node_id(),
        updated_at_ms,
        updated_at_ms,
    );
    body.ttl_ms = ttl_ms;
    body.geo = Some(DiscoveryGeo {
        latitude,
        longitude,
        radius_km,
    });
    body.transport_contact = Some(contact);
    SignedDiscoveryNodeRecord::sign(body, &identity).expect("signed discovery record")
}

fn ensure_test_relay_urls(state_dir: &Path) {
    let path = state_dir.join("startup_config.json");
    let mut value = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .unwrap_or_else(|| json!({}));
    if value.get("relay_urls").is_some() {
        return;
    }
    value["relay_urls"] = json!(["https://relay.example.invalid/"]);
    fs::write(
        path,
        serde_json::to_vec(&value).expect("startup config json"),
    )
    .expect("write startup config relay urls");
}
