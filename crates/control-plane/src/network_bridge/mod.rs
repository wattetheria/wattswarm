mod announcements;
mod backfill;
mod diagnostics;
mod publish;
mod scope;
mod service_dispatch;
mod service_ops;
mod service_runtime;
mod summary;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result, anyhow, bail};

use crate::constants::BACKFILL_BATCH_EVENTS;
use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, ContactMaterialRequestId, EventEnvelope, GossipKind,
    GossipMessage, Multiaddr, NetworkP2pConfig, NetworkP2pNode, NetworkRuntime,
    NetworkRuntimeEvent, PeerDirectMessageRequest, PeerDirectMessageRequestId,
    PeerDirectMessageResponse, PeerHandshakeMetadata, PeerId, PeerRelationshipRequest,
    PeerRelationshipRequestId, PeerRelationshipResponse, RawAgentEnvelope, RawContactMaterial,
    RawContactMaterialRequest, RawContactMaterialResponse, RawPeerDirectMessageKind,
    RawPeerRelationshipAction, SummaryAnnouncement, SwarmScope,
};
use crate::node::Node;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use uuid::Uuid;
use wattswarm_network_transport_core::{
    TransferIntent, TransferKind, TransportRoute as DataTransportRoute, TransportRouter,
};
use wattswarm_network_transport_iroh::export_local_contact_material;
use wattswarm_protocol::types::NetworkProtocolParams;

pub use announcements::{apply_checkpoint_announcement, apply_rule_announcement};
pub use backfill::{
    backfill_response_for_request, dial_bootstrap_peer_endpoints, dial_discovered_peer_endpoints,
    ingest_backfill_response,
};
pub use diagnostics::{
    DiagnosticEntry, DiagnosticFilter, list_diagnostics as list_network_diagnostics,
};
pub use publish::{publish_pending_global_events, publish_pending_scoped_updates};
pub use summary::{
    apply_summary_announcement, build_knowledge_summary_for_task_type,
    build_reputation_summary_for_runtime,
};

use backfill::{latest_scoped_event_seq, should_publish_summaries, should_sync_event};
use publish::GlobalPublishRateGuard;
use scope::{
    dynamic_subscription_scope_kinds_for_node, event_scope, merge_scopes,
    node_has_active_subscription_scope_kinds,
};
use summary::{
    knowledge_summary_for_event, mirror_summary_controls_to_parent_network,
    mirror_summary_to_parent_network, reputation_summary_for_event, task_outcome_summary_for_event,
};

const ENV_P2P_REGION_IDS: &str = "WATTSWARM_P2P_REGION_IDS";
const ENV_P2P_LOCAL_IDS: &str = "WATTSWARM_P2P_LOCAL_IDS";
const ENV_P2P_NODE_IDS: &str = "WATTSWARM_P2P_NODE_IDS";
const KNOWLEDGE_SUMMARY_KIND: &str = "knowledge_task_type_v1";
const REPUTATION_SUMMARY_KIND: &str = "reputation_runtime_profile_v1";
const TASK_OUTCOME_SUMMARY_KIND: &str = "task_outcome_v1";
const AGENT_PAYMENT_SUMMARY_KIND: &str = "agent_payment_session_v1";
const CORE_AGENT_EXECUTOR_NAME: &str = "core-agent";
const MAX_INFLIGHT_BACKFILLS_PER_PEER: usize = 1;
const SUMMARY_BACKPRESSURE_HIGH_WATERMARK: u64 = 256;
const IDLE_NETWORK_SLEEP: Duration = Duration::from_millis(50);
const ANNOUNCED_PEER_TTL: Duration = Duration::from_secs(60 * 60);

const ENV_P2P_ENABLED: &str = "WATTSWARM_P2P_ENABLED";
const ENV_P2P_MDNS: &str = "WATTSWARM_P2P_MDNS";
const ENV_P2P_PORT: &str = "WATTSWARM_P2P_PORT";
const ENV_P2P_LISTEN_ADDRS: &str = "WATTSWARM_P2P_LISTEN_ADDRS";
const ENV_P2P_BOOTSTRAP_PEERS: &str = "WATTSWARM_P2P_BOOTSTRAP_PEERS";
const STARTUP_CONFIG_FILE: &str = "startup_config.json";
const DEFAULT_P2P_PORT: u16 = 4001;
const GLOBAL_HIGH_FREQUENCY_WINDOW: Duration = Duration::from_secs(5);
const GLOBAL_HIGH_FREQUENCY_LIMIT: usize = 32;
const DEFAULT_NETWORK_CONTEXT_ID: &str = "default";
static STARTED_NETWORK_SERVICES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
static LATEST_NETWORK_OBSERVABILITY_SNAPSHOTS: OnceLock<
    Mutex<HashMap<PathBuf, NetworkBridgeObservabilitySnapshot>>,
> = OnceLock::new();

fn started_network_services() -> &'static Mutex<HashSet<PathBuf>> {
    STARTED_NETWORK_SERVICES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn record_peer_announcement(
    announced_peers: &mut HashMap<String, Instant>,
    peer: &str,
    now: Instant,
) -> bool {
    announced_peers.retain(|_, announced_at| {
        now.saturating_duration_since(*announced_at) <= ANNOUNCED_PEER_TTL
    });
    announced_peers.insert(peer.to_owned(), now).is_none()
}

fn latest_network_observability_snapshots()
-> &'static Mutex<HashMap<PathBuf, NetworkBridgeObservabilitySnapshot>> {
    LATEST_NETWORK_OBSERVABILITY_SNAPSHOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn store_latest_network_observability_snapshot(
    state_dir: &Path,
    snapshot: NetworkBridgeObservabilitySnapshot,
) {
    let mut snapshots = latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    snapshots.insert(state_dir.to_path_buf(), snapshot);
}

fn clear_latest_network_observability_snapshot(state_dir: &Path) {
    let mut snapshots = latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    snapshots.remove(state_dir);
}

pub fn latest_connected_peer_ids(state_dir: &Path) -> Option<Vec<String>> {
    let snapshots = latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let snapshot = snapshots.get(state_dir)?;
    let mut peers = snapshot
        .peer_health
        .iter()
        .filter(|entry| entry.connected)
        .map(|entry| entry.peer.clone())
        .collect::<Vec<_>>();
    peers.sort();
    peers.dedup();
    Some(peers)
}

pub fn latest_network_observability_snapshot(
    state_dir: &Path,
) -> Option<NetworkBridgeObservabilitySnapshot> {
    latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(state_dir)
        .cloned()
}

pub fn network_service_started(state_dir: &Path) -> bool {
    started_network_services()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(state_dir)
}

fn current_network_context_id(node: &Node) -> String {
    if node.store.is_org_configured()
        && let Ok(topology) = node
            .store
            .load_network_topology_for_org(node.store.org_id())
    {
        return topology.network.network_id;
    }
    node.store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| DEFAULT_NETWORK_CONTEXT_ID.to_owned())
}

fn network_id_for_network_substrate_event(event: &crate::types::Event) -> Option<&str> {
    match &event.payload {
        crate::types::EventPayload::FeedSubscriptionUpdated(payload) => Some(&payload.network_id),
        crate::types::EventPayload::TaskAnnounced(payload) => Some(&payload.network_id),
        crate::types::EventPayload::ExecutionIntentDeclared(payload) => Some(&payload.network_id),
        crate::types::EventPayload::ExecutionSetConfirmed(payload) => Some(&payload.network_id),
        crate::types::EventPayload::TopicMessagePosted(payload) => Some(&payload.network_id),
        _ => None,
    }
}

fn observed_at_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn scope_hint_label(scope: &SwarmScope) -> String {
    match scope {
        SwarmScope::Global => "global".to_owned(),
        SwarmScope::Region(id) => format!("region:{id}"),
        SwarmScope::Node(id) => format!("node:{id}"),
        SwarmScope::Group(id) => format!("group:{id}"),
    }
}

fn feed_subscription_gossip_kinds(raw_kinds: &[String]) -> Vec<GossipKind> {
    if raw_kinds.is_empty() {
        return GossipKind::ALL.to_vec();
    }
    let mut kinds = Vec::new();
    for raw in raw_kinds {
        let kind = match raw.trim() {
            "events" => GossipKind::Events,
            "messages" => GossipKind::Messages,
            "rules" => GossipKind::Rules,
            "checkpoints" => GossipKind::Checkpoints,
            "summaries" => GossipKind::Summaries,
            _ => continue,
        };
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    kinds
}

fn wire_peer_relationship_action(
    action: crate::control::PeerRelationshipAction,
) -> RawPeerRelationshipAction {
    match action {
        crate::control::PeerRelationshipAction::Request => RawPeerRelationshipAction::Request,
        crate::control::PeerRelationshipAction::Accept => RawPeerRelationshipAction::Accept,
        crate::control::PeerRelationshipAction::Reject => RawPeerRelationshipAction::Reject,
        crate::control::PeerRelationshipAction::Cancel => RawPeerRelationshipAction::Cancel,
        crate::control::PeerRelationshipAction::Remove => RawPeerRelationshipAction::Remove,
        crate::control::PeerRelationshipAction::Block => RawPeerRelationshipAction::Block,
        crate::control::PeerRelationshipAction::Unblock => RawPeerRelationshipAction::Unblock,
    }
}

fn control_peer_relationship_action(
    action: RawPeerRelationshipAction,
) -> crate::control::PeerRelationshipAction {
    match action {
        RawPeerRelationshipAction::Request => crate::control::PeerRelationshipAction::Request,
        RawPeerRelationshipAction::Accept => crate::control::PeerRelationshipAction::Accept,
        RawPeerRelationshipAction::Reject => crate::control::PeerRelationshipAction::Reject,
        RawPeerRelationshipAction::Cancel => crate::control::PeerRelationshipAction::Cancel,
        RawPeerRelationshipAction::Remove => crate::control::PeerRelationshipAction::Remove,
        RawPeerRelationshipAction::Block => crate::control::PeerRelationshipAction::Block,
        RawPeerRelationshipAction::Unblock => crate::control::PeerRelationshipAction::Unblock,
    }
}

fn maybe_record_topic_cursor(
    node: &Node,
    subscriber_node_id: &str,
    feed_key: &str,
    scope: &SwarmScope,
    last_event_seq: u64,
    updated_at: u64,
) -> Result<()> {
    let Some(subscription) = node
        .store
        .get_feed_subscription(subscriber_node_id, feed_key)?
    else {
        return Ok(());
    };
    if !subscription.active || subscription.scope_hint != scope_hint_label(scope) {
        return Ok(());
    }
    node.store.upsert_topic_cursor(
        subscriber_node_id,
        feed_key,
        &subscription.scope_hint,
        last_event_seq,
        updated_at,
    )
}

fn maybe_record_topic_cursor_for_event_id(
    node: &Node,
    subscriber_node_id: &str,
    feed_key: &str,
    scope: &SwarmScope,
    event_id: &str,
    updated_at: u64,
) -> Result<()> {
    let Some(last_event_seq) = node.store.event_seq_for_event_id(event_id)? else {
        return Ok(());
    };
    maybe_record_topic_cursor(
        node,
        subscriber_node_id,
        feed_key,
        scope,
        last_event_seq,
        updated_at,
    )
}

fn maybe_record_topic_cursor_for_response(
    node: &Node,
    subscriber_node_id: &str,
    response: &crate::network_p2p::BackfillResponse,
    updated_at: u64,
) -> Result<()> {
    let Some(feed_key) = &response.feed_key else {
        return Ok(());
    };
    maybe_record_topic_cursor(
        node,
        subscriber_node_id,
        feed_key,
        &response.scope,
        response.next_from_event_seq,
        updated_at,
    )
}

pub(super) fn parent_uplink_store(node: &Node) -> Result<Option<crate::storage::PgStore>> {
    if !node.store.is_org_configured() {
        return Ok(None);
    }
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())?;
    if !topology.network.is_subnet() {
        return Ok(None);
    }
    let Some(parent_topology) = node
        .store
        .load_parent_network_topology_for_org(node.store.org_id())?
    else {
        return Ok(None);
    };
    if !node
        .store
        .node_has_network_membership(&node.node_id(), &parent_topology.network.network_id)?
    {
        return Ok(None);
    }
    Ok(Some(node.store.for_org(parent_topology.org.org_id)))
}

#[derive(Debug, Clone)]
struct PeerSyncState {
    inflight_backfills: usize,
    last_backfill_request_at: Option<Instant>,
    next_retry_at: Instant,
    known_scopes: HashSet<SwarmScope>,
}

impl PeerSyncState {
    fn new(now: Instant) -> Self {
        Self {
            inflight_backfills: 0,
            last_backfill_request_at: None,
            next_retry_at: now,
            known_scopes: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct PendingPeerRelationshipRequest {
    peer: PeerId,
    remote_node_id: String,
    action: crate::control::PeerRelationshipAction,
}

#[derive(Debug, Clone)]
struct PendingPeerDirectMessageRequest {
    peer: PeerId,
    remote_node_id: String,
    thread_id: String,
    message_id: String,
    kind: crate::control::PeerDmMessageKind,
    a2a_protocol: String,
}

#[derive(Debug, Clone)]
struct PendingContactMaterialRequest {
    peer: PeerId,
    remote_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PendingNetworkCommand {
    PeerRelationship {
        remote_node_id: String,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: RawAgentEnvelope,
    },
    PeerDirectMessage {
        remote_node_id: String,
        agent_envelope: RawAgentEnvelope,
        content: Value,
    },
    AgentPayment {
        remote_node_id: String,
        message_kind: String,
        payment: Value,
    },
}

fn pending_network_commands_path(state_dir: &Path) -> PathBuf {
    state_dir.join("pending_network_commands.jsonl")
}

fn enqueue_pending_network_command(
    state_dir: &Path,
    command: &PendingNetworkCommand,
) -> Result<()> {
    let path = pending_network_commands_path(state_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{}", serde_json::to_string(command)?)?;
    Ok(())
}

pub fn enqueue_peer_relationship_action_command(
    state_dir: &Path,
    remote_node_id: &str,
    action: crate::control::PeerRelationshipAction,
    agent_envelope: RawAgentEnvelope,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::PeerRelationship {
            remote_node_id: remote_node_id.trim().to_owned(),
            action,
            agent_envelope,
        },
    )
}

pub fn enqueue_peer_direct_message_command(
    state_dir: &Path,
    remote_node_id: &str,
    agent_envelope: RawAgentEnvelope,
    content: Value,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::PeerDirectMessage {
            remote_node_id: remote_node_id.trim().to_owned(),
            agent_envelope,
            content,
        },
    )
}

pub fn enqueue_agent_payment_command(
    state_dir: &Path,
    remote_node_id: &str,
    message_kind: &str,
    payment: Value,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::AgentPayment {
            remote_node_id: remote_node_id.trim().to_owned(),
            message_kind: message_kind.trim().to_owned(),
            payment,
        },
    )
}

fn raw_agent_envelope_to_control_record(
    envelope: &RawAgentEnvelope,
) -> crate::control::AgentInteractionEnvelope {
    crate::control::AgentInteractionEnvelope {
        protocol: envelope.protocol.clone(),
        source_agent_id: envelope.source_agent_id.clone(),
        target_agent_id: envelope.target_agent_id.clone(),
        capability: envelope.capability.clone(),
        message: serde_json::from_str(&envelope.message_json).unwrap_or_else(|_| json!({})),
        extensions: envelope
            .extensions_json
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok()),
        signature: envelope.signature.clone(),
    }
}

fn build_agent_payment_summary(
    remote_node_id: &str,
    message_kind: &str,
    payment: Value,
) -> SummaryAnnouncement {
    let remote_node_id = remote_node_id.trim();
    let payment_id = payment
        .get("payment_id")
        .and_then(Value::as_str)
        .unwrap_or("payment");
    SummaryAnnouncement {
        summary_id: format!("payment:{payment_id}:{}", Uuid::new_v4()),
        source_node_id: String::new(),
        scope: SwarmScope::Node(remote_node_id.to_owned()),
        summary_kind: AGENT_PAYMENT_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: json!({
            "message_kind": message_kind,
            "payment": payment,
        }),
    }
}

fn agent_event_status_label(status: wattswarm_protocol::types::AgentEventStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "pending".to_owned())
}

fn load_commit_plane_token(token_file: &str) -> Result<String> {
    let token = std::fs::read_to_string(token_file)
        .with_context(|| format!("read Wattetheria control token file: {token_file}"))?;
    let token = token.trim().to_owned();
    if token.is_empty() {
        bail!("Wattetheria control token file is empty: {token_file}");
    }
    Ok(token)
}

fn route_agent_decision_to_wattetheria(
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
    entry: &crate::control::ExecutorRegistryEntry,
) -> Result<(i64, String)> {
    let commit_plane_endpoint = entry
        .commit_plane_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("core-agent executor missing commit_plane_endpoint"))?;
    let commit_plane_token_file = entry
        .commit_plane_token_file
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("core-agent executor missing commit_plane_token_file"))?;
    let token = load_commit_plane_token(commit_plane_token_file)?;
    let client = reqwest::blocking::Client::new();
    let response = client
        .post(format!(
            "{}/v1/agent-actions/commit",
            commit_plane_endpoint.trim_end_matches('/')
        ))
        .header("authorization", format!("Bearer {token}"))
        .header("x-agent-event-id", &event.event_id)
        .header("x-agent-decision-id", &decision.decision_id)
        .json(&json!({
            "event": event,
            "decision": decision,
        }))
        .send()
        .context("POST Wattetheria agent action commit")?;
    let status_code = response.status().as_u16() as i64;
    let body = response.text().unwrap_or_default();
    if status_code < 200 || status_code >= 300 {
        bail!("Wattetheria agent action commit returned {status_code}: {body}");
    }
    Ok((status_code, body))
}

fn required_event_payload_string(
    payload: &Value,
    key: &str,
    event_type: &wattswarm_protocol::types::AgentEventType,
) -> Result<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("agent event {:?} missing {}", event_type, key))
}

fn route_topic_message_reply_to_wattswarm(
    state_dir: &Path,
    db_path: &Path,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    match decision.action.as_str() {
        "ignore" => {
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "ignored",
                    "store": "wattswarm",
                    "event_id": event.event_id,
                })
                .to_string(),
            ));
        }
        "reply" => {}
        action => {
            bail!(
                "unsupported wattswarm_direct topic action {action} for event_type={:?}",
                event.event_type
            );
        }
    }

    let feed_key = required_event_payload_string(&event.payload, "feed_key", &event.event_type)?;
    let scope_hint =
        required_event_payload_string(&event.payload, "scope_hint", &event.event_type)?;
    let content = decision
        .payload
        .get("content")
        .cloned()
        .ok_or_else(|| anyhow!("topic reply decision requires payload.content"))?;
    let reply_to_message_id = decision
        .payload
        .get("reply_to_message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            event
                .payload
                .get("message_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });

    let mut node = crate::control::open_node(state_dir, db_path)?;
    let network_id = node
        .store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| DEFAULT_NETWORK_CONTEXT_ID.to_owned());
    let created_at = observed_at_ms();
    let emitted = crate::control::emit_topic_message_with_content(
        &mut node,
        state_dir,
        &network_id,
        &feed_key,
        &scope_hint,
        content,
        reply_to_message_id,
        created_at,
    )?;
    let _ = crate::topic_interpretation::process_topic_interpretation_for_topic(
        &mut node,
        state_dir,
        &feed_key,
        &scope_hint,
    );
    let _ = crate::topic_consensus::process_structured_topic_consensus_for_topic(
        &mut node,
        state_dir,
        &feed_key,
        &scope_hint,
    );

    Ok((
        201,
        json!({
            "ok": true,
            "status": "posted",
            "store": "wattswarm",
            "message_id": emitted.event_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
        })
        .to_string(),
    ))
}

fn route_task_result_to_wattswarm(
    state_dir: &Path,
    db_path: &Path,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    let task_id = required_event_payload_string(&event.payload, "task_id", &event.event_type)?;
    let event_kind =
        required_event_payload_string(&event.payload, "event_kind", &event.event_type)?;
    let mut node = crate::control::open_node(state_dir, db_path)?;
    let task = node
        .task_view(&task_id)?
        .ok_or_else(|| anyhow!("task not found for task_result_received: {task_id}"))?;
    let now = observed_at_ms();

    match decision.action.as_str() {
        "accept_result" => {
            if event_kind == "candidate_proposed" {
                let candidate_id = required_event_payload_string(
                    &event.payload,
                    "candidate_id",
                    &event.event_type,
                )?;
                let result =
                    crate::control::accept_task_result_locally(&mut node, &task_id, &candidate_id)?;
                return Ok((200, result.to_string()));
            }
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "observed",
                    "store": "wattswarm",
                    "task_id": task_id,
                    "event_kind": event_kind,
                })
                .to_string(),
            ));
        }
        "reject_result" | "request_retry" => {
            let next_attempt = task.retry_attempt.saturating_add(1);
            let run_at = now.saturating_add(task.contract.acceptance.vote.reveal_deadline_ms);
            node.schedule_retry(&task_id, next_attempt, run_at, task.epoch, now)?;
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "retry_scheduled",
                    "store": "wattswarm",
                    "task_id": task_id,
                    "attempt": next_attempt,
                    "run_at": run_at,
                })
                .to_string(),
            ));
        }
        "inspect_task" => {
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "observed",
                    "store": "wattswarm",
                    "task_id": task_id,
                })
                .to_string(),
            ));
        }
        action => {
            bail!(
                "unsupported wattswarm_direct task result action {action} for event_type={:?}",
                event.event_type
            );
        }
    }
}

fn route_task_claim_to_wattswarm(
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    let task_id = required_event_payload_string(&event.payload, "task_id", &event.event_type)?;
    match decision.action.as_str() {
        "decide_claim" | "inspect_task" => Ok((
            200,
            json!({
                "ok": true,
                "status": "recorded",
                "store": "wattswarm",
                "task_id": task_id,
                "detail": "task claim lifecycle is already authoritative in wattswarm; no secondary semantic commit is applied here"
            })
            .to_string(),
        )),
        action => bail!(
            "unsupported wattswarm_direct task claim action {action} for event_type={:?}",
            event.event_type
        ),
    }
}

fn route_agent_decision_to_wattswarm(
    state_dir: &Path,
    db_path: Option<&Path>,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    match event.event_type {
        wattswarm_protocol::types::AgentEventType::TopicMessageRequiresReply => {
            let db_path = db_path.ok_or_else(|| {
                anyhow!("wattswarm_direct topic routing requires a configured db_path")
            })?;
            route_topic_message_reply_to_wattswarm(state_dir, db_path, event, decision)
        }
        wattswarm_protocol::types::AgentEventType::TaskResultReceived => {
            let db_path = db_path.ok_or_else(|| {
                anyhow!("wattswarm_direct task result routing requires a configured db_path")
            })?;
            route_task_result_to_wattswarm(state_dir, db_path, event, decision)
        }
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived => {
            route_task_claim_to_wattswarm(event, decision)
        }
        _ => bail!(
            "wattswarm_direct decision routing is not supported yet for event_type={:?}",
            event.event_type
        ),
    }
}

fn route_agent_decision(
    state_dir: &Path,
    db_path: Option<&Path>,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
    entry: &crate::control::ExecutorRegistryEntry,
) -> Result<Option<(String, i64, String)>> {
    match decision.route {
        wattswarm_protocol::types::AgentDecisionRoute::Noop => Ok(None),
        wattswarm_protocol::types::AgentDecisionRoute::WattetheriaCommit => {
            let (status_code, body) = route_agent_decision_to_wattetheria(event, decision, entry)?;
            Ok(Some((
                entry
                    .commit_plane_endpoint
                    .clone()
                    .unwrap_or_else(|| String::from("wattetheria")),
                status_code,
                body,
            )))
        }
        wattswarm_protocol::types::AgentDecisionRoute::WattswarmDirect => {
            let (status_code, body) =
                route_agent_decision_to_wattswarm(state_dir, db_path, event, decision)?;
            Ok(Some(("wattswarm:direct".to_owned(), status_code, body)))
        }
    }
}

fn topic_message_kind(content: &Value) -> Option<&str> {
    content
        .as_object()
        .and_then(|object| object.get("kind"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn topic_message_requires_reply(content: &Value) -> bool {
    !matches!(
        topic_message_kind(content),
        Some("proposal" | "stance" | "interpreted_stance" | "consensus_result")
    )
}

fn task_claim_agent_event(
    node: &Node,
    event: &crate::types::Event,
    payload: &crate::types::ClaimPayload,
) -> Result<wattswarm_protocol::types::AgentEvent> {
    let task_inputs = node
        .task_view(&payload.task_id)?
        .map(|task| task.contract.inputs)
        .unwrap_or(Value::Null);
    Ok(build_agent_event(
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
        Some(event.author_node_id.clone()),
        None,
        json!({
            "event_id": &event.event_id,
            "event_kind": "task_claimed",
            "task_id": &payload.task_id,
            "claimer_node_id": &payload.claimer_node_id,
            "execution_id": &payload.execution_id,
            "role": payload.role,
            "lease_until": payload.lease_until,
            "task_inputs": task_inputs,
            "created_at": event.created_at,
        }),
        false,
        vec!["inspect_task".to_owned(), "decide_claim".to_owned()],
        Some(payload.task_id.clone()),
        Some(format!(
            "task_claim:{}:{}",
            payload.task_id, payload.execution_id
        )),
    ))
}

fn task_result_agent_event(
    node: &Node,
    event: &crate::types::Event,
) -> Result<Option<wattswarm_protocol::types::AgentEvent>> {
    match &event.payload {
        crate::types::EventPayload::CandidateProposed(payload) => {
            let candidate_output = node
                .store
                .get_candidate_by_id(&payload.task_id, &payload.candidate.candidate_id)?
                .map(|candidate| candidate.output)
                .unwrap_or(Value::Null);
            Ok(Some(build_agent_event(
                wattswarm_protocol::types::AgentEventType::TaskResultReceived,
                wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
                Some(event.author_node_id.clone()),
                None,
                json!({
                    "event_id": &event.event_id,
                    "event_kind": "candidate_proposed",
                    "task_id": &payload.task_id,
                    "candidate_id": &payload.candidate.candidate_id,
                    "execution_id": &payload.candidate.execution_id,
                    "candidate_hash": &payload.candidate.output_ref.digest,
                    "candidate_output": candidate_output,
                    "created_at": event.created_at,
                }),
                false,
                vec![
                    "inspect_task".to_owned(),
                    "accept_result".to_owned(),
                    "reject_result".to_owned(),
                    "request_retry".to_owned(),
                ],
                Some(payload.task_id.clone()),
                Some(format!(
                    "task_result:{}:{}",
                    payload.task_id, payload.candidate.candidate_id
                )),
            )))
        }
        crate::types::EventPayload::DecisionFinalized(payload) => Ok(Some(build_agent_event(
            wattswarm_protocol::types::AgentEventType::TaskResultReceived,
            wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
            Some(event.author_node_id.clone()),
            None,
            json!({
                "event_id": &event.event_id,
                "event_kind": "decision_finalized",
                "task_id": &payload.task_id,
                "candidate_id": &payload.candidate_id,
                "winning_candidate_hash": &payload.winning_candidate_hash,
                "finality_proof": &payload.finality_proof,
                "created_at": event.created_at,
            }),
            false,
            vec!["inspect_task".to_owned(), "accept_result".to_owned()],
            Some(payload.task_id.clone()),
            Some(format!(
                "task_result:{}:finalized:{}",
                payload.task_id, payload.candidate_id
            )),
        ))),
        crate::types::EventPayload::TaskError(payload) => Ok(Some(build_agent_event(
            wattswarm_protocol::types::AgentEventType::TaskResultReceived,
            wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
            Some(event.author_node_id.clone()),
            None,
            json!({
                "event_id": &event.event_id,
                "event_kind": "task_error",
                "task_id": &payload.task_id,
                "reason": payload.reason,
                "reason_codes": &payload.reason_codes,
                "message": &payload.message,
                "custom_reason_namespace": &payload.custom_reason_namespace,
                "custom_reason_code": &payload.custom_reason_code,
                "custom_reason_message": &payload.custom_reason_message,
                "created_at": event.created_at,
            }),
            false,
            vec!["inspect_task".to_owned(), "request_retry".to_owned()],
            Some(payload.task_id.clone()),
            Some(format!(
                "task_result:{}:error:{}",
                payload.task_id, event.event_id
            )),
        ))),
        crate::types::EventPayload::TaskRetryScheduled(payload) => Ok(Some(build_agent_event(
            wattswarm_protocol::types::AgentEventType::TaskResultReceived,
            wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
            Some(event.author_node_id.clone()),
            None,
            json!({
                "event_id": &event.event_id,
                "event_kind": "task_retry_scheduled",
                "task_id": &payload.task_id,
                "attempt": payload.attempt,
                "run_at": payload.run_at,
                "created_at": event.created_at,
            }),
            false,
            vec!["inspect_task".to_owned()],
            Some(payload.task_id.clone()),
            Some(format!(
                "task_result:{}:retry:{}",
                payload.task_id, payload.attempt
            )),
        ))),
        _ => Ok(None),
    }
}

fn topic_message_agent_event(
    node: &Node,
    event: &crate::types::Event,
    payload: &crate::types::TopicMessagePostedPayload,
) -> Result<Option<wattswarm_protocol::types::AgentEvent>> {
    let Some(topic_message) = node.store.get_topic_message(&event.event_id)? else {
        return Ok(None);
    };
    if !topic_message_requires_reply(&topic_message.content) {
        return Ok(None);
    }
    Ok(Some(build_agent_event(
        wattswarm_protocol::types::AgentEventType::TopicMessageRequiresReply,
        wattswarm_protocol::types::AgentEventSourceKind::TopicMessage,
        Some(event.author_node_id.clone()),
        None,
        json!({
            "message_id": &event.event_id,
            "network_id": &payload.network_id,
            "feed_key": &payload.feed_key,
            "scope_hint": &payload.scope_hint,
            "author_node_id": &event.author_node_id,
            "content": topic_message.content,
            "reply_to_message_id": &payload.reply_to_message_id,
            "created_at": event.created_at,
        }),
        false,
        vec!["reply".to_owned(), "ignore".to_owned()],
        Some(payload.feed_key.clone()),
        Some(format!("topic_message:{}", event.event_id)),
    )))
}

fn build_agent_event(
    event_type: wattswarm_protocol::types::AgentEventType,
    source_kind: wattswarm_protocol::types::AgentEventSourceKind,
    source_node_id: Option<String>,
    target_agent_id: Option<String>,
    payload: Value,
    requires_commit: bool,
    allowed_actions: Vec<String>,
    correlation_id: Option<String>,
    dedupe_key: Option<String>,
) -> wattswarm_protocol::types::AgentEvent {
    wattswarm_protocol::types::AgentEvent {
        event_id: Uuid::new_v4().to_string(),
        event_type,
        source_kind,
        source_node_id,
        target_agent_id,
        target_executor: Some(CORE_AGENT_EXECUTOR_NAME.to_owned()),
        payload,
        requires_commit,
        allowed_actions,
        correlation_id,
        dedupe_key,
        created_at: observed_at_ms(),
    }
}

fn deliver_agent_event_to_local_executor(
    state_dir: &Path,
    db_path: Option<&Path>,
    event: &wattswarm_protocol::types::AgentEvent,
) -> Result<()> {
    if let Some(dedupe_key) = event.dedupe_key.as_deref()
        && crate::control::find_agent_event_record_by_dedupe_key(state_dir, dedupe_key)?.is_some()
    {
        diagnostics::record_diagnostic(
            Some(state_dir),
            diagnostics::DiagnosticEvent::new(
                "info",
                "agent_event",
                "delivery.dedupe",
                "skipped",
                format!("agent event dedupe skipped: {:?}", event.event_type),
            )
            .event_id(event.event_id.clone())
            .object("agent_event", event.correlation_id.clone())
            .source_node_id(event.source_node_id.clone())
            .details(json!({
                "event_type": format!("{:?}", event.event_type),
                "dedupe_key": dedupe_key,
            })),
        );
        return Ok(());
    }

    let now = observed_at_ms();
    crate::control::save_agent_event_record_state(
        state_dir,
        event,
        wattswarm_protocol::types::AgentEventStatus::Pending,
        now,
    )?;
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new(
            "info",
            "agent_event",
            "delivery.queued",
            "pending",
            format!(
                "agent event queued for local executor: {:?}",
                event.event_type
            ),
        )
        .event_id(event.event_id.clone())
        .object("agent_event", event.correlation_id.clone())
        .source_node_id(event.source_node_id.clone())
        .details(json!({
            "event_type": format!("{:?}", event.event_type),
            "target_agent_id": event.target_agent_id,
            "target_executor": event.target_executor,
            "allowed_actions": event.allowed_actions,
            "requires_commit": event.requires_commit,
        })),
    );

    let registry = crate::control::load_executor_registry_state(state_dir)?;
    let Some(entry) = registry
        .entries
        .into_iter()
        .find(|candidate| candidate.name == CORE_AGENT_EXECUTOR_NAME)
    else {
        crate::control::save_agent_event_record_state(
            state_dir,
            event,
            wattswarm_protocol::types::AgentEventStatus::Failed,
            observed_at_ms(),
        )?;
        crate::control::append_agent_event_delivery_record_state(
            state_dir,
            &crate::storage::LocalAgentEventDeliveryRow {
                delivery_id: Uuid::new_v4().to_string(),
                event_id: event.event_id.clone(),
                attempt_no: 1,
                endpoint_url: String::new(),
                delivery_status: "failed".to_owned(),
                response_code: None,
                response_body: None,
                error_text: Some("core-agent executor is not registered".to_owned()),
                next_retry_at: None,
                created_at: observed_at_ms(),
            },
        )?;
        diagnostics::record_diagnostic(
            Some(state_dir),
            diagnostics::DiagnosticEvent::new(
                "error",
                "agent_event",
                "delivery.executor",
                "failed",
                "core-agent executor is not registered",
            )
            .event_id(event.event_id.clone())
            .object("agent_event", event.correlation_id.clone())
            .source_node_id(event.source_node_id.clone())
            .details(json!({
                "event_type": format!("{:?}", event.event_type),
            })),
        );
        return Ok(());
    };

    let callback_base_url = entry
        .agent_event_callback_base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| entry.base_url.trim_end_matches('/'));
    let endpoint_url = format!("{}/agent-events", callback_base_url.trim_end_matches('/'));
    let client = reqwest::blocking::Client::new();
    let delivery_started_at = observed_at_ms();
    let callback_request = wattswarm_protocol::types::AgentEventCallbackRequest {
        event: event.clone(),
    };
    let result = client.post(&endpoint_url).json(&callback_request).send();
    match result {
        Ok(response) => {
            let response_code = response.status().as_u16() as i64;
            let body = response.text().unwrap_or_default();
            let parsed = serde_json::from_str::<
                wattswarm_protocol::types::AgentEventCallbackResponse,
            >(&body)
            .ok();
            let status = if response_code >= 200 && response_code < 300 {
                if parsed.as_ref().is_some_and(|ack| ack.ok) {
                    wattswarm_protocol::types::AgentEventStatus::Acked
                } else {
                    wattswarm_protocol::types::AgentEventStatus::Delivered
                }
            } else {
                wattswarm_protocol::types::AgentEventStatus::Failed
            };
            crate::control::save_agent_event_record_state(
                state_dir,
                event,
                status.clone(),
                observed_at_ms(),
            )?;
            crate::control::append_agent_event_delivery_record_state(
                state_dir,
                &crate::storage::LocalAgentEventDeliveryRow {
                    delivery_id: Uuid::new_v4().to_string(),
                    event_id: event.event_id.clone(),
                    attempt_no: 1,
                    endpoint_url: endpoint_url.clone(),
                    delivery_status: agent_event_status_label(status),
                    response_code: Some(response_code),
                    response_body: Some(body),
                    error_text: None,
                    next_retry_at: None,
                    created_at: delivery_started_at,
                },
            )?;
            diagnostics::record_diagnostic(
                Some(state_dir),
                diagnostics::DiagnosticEvent::new(
                    if response_code >= 200 && response_code < 300 {
                        "info"
                    } else {
                        "warn"
                    },
                    "agent_event",
                    "delivery.callback",
                    if response_code >= 200 && response_code < 300 {
                        "delivered"
                    } else {
                        "failed"
                    },
                    format!("agent event callback returned {response_code}"),
                )
                .event_id(event.event_id.clone())
                .object("agent_event", event.correlation_id.clone())
                .source_node_id(event.source_node_id.clone())
                .details(json!({
                    "event_type": format!("{:?}", event.event_type),
                    "endpoint_url": endpoint_url,
                    "response_code": response_code,
                })),
            );
            if let Some(decision) = parsed.and_then(|ack| ack.decision) {
                match route_agent_decision(state_dir, db_path, event, &decision, &entry) {
                    Ok(commit_result) => {
                        crate::control::save_agent_event_record_state(
                            state_dir,
                            event,
                            wattswarm_protocol::types::AgentEventStatus::Completed,
                            observed_at_ms(),
                        )?;
                        if let Some((endpoint_url, commit_code, commit_body)) = commit_result {
                            crate::control::append_agent_event_delivery_record_state(
                                state_dir,
                                &crate::storage::LocalAgentEventDeliveryRow {
                                    delivery_id: Uuid::new_v4().to_string(),
                                    event_id: event.event_id.clone(),
                                    attempt_no: 2,
                                    endpoint_url,
                                    delivery_status: "completed".to_owned(),
                                    response_code: Some(commit_code),
                                    response_body: Some(commit_body),
                                    error_text: None,
                                    next_retry_at: None,
                                    created_at: observed_at_ms(),
                                },
                            )?;
                        }
                        diagnostics::record_diagnostic(
                            Some(state_dir),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "agent_event",
                                "decision.route",
                                "completed",
                                format!("agent decision routed: {:?}", decision.route),
                            )
                            .event_id(event.event_id.clone())
                            .object("agent_event", event.correlation_id.clone())
                            .source_node_id(event.source_node_id.clone())
                            .details(json!({
                                "event_type": format!("{:?}", event.event_type),
                                "decision_id": decision.decision_id,
                                "action": decision.action,
                                "route": format!("{:?}", decision.route),
                            })),
                        );
                    }
                    Err(error) => {
                        crate::control::save_agent_event_record_state(
                            state_dir,
                            event,
                            wattswarm_protocol::types::AgentEventStatus::Failed,
                            observed_at_ms(),
                        )?;
                        crate::control::append_agent_event_delivery_record_state(
                            state_dir,
                            &crate::storage::LocalAgentEventDeliveryRow {
                                delivery_id: Uuid::new_v4().to_string(),
                                event_id: event.event_id.clone(),
                                attempt_no: 2,
                                endpoint_url: entry
                                    .commit_plane_endpoint
                                    .clone()
                                    .unwrap_or_else(String::new),
                                delivery_status: "failed".to_owned(),
                                response_code: None,
                                response_body: None,
                                error_text: Some(error.to_string()),
                                next_retry_at: None,
                                created_at: observed_at_ms(),
                            },
                        )?;
                        diagnostics::record_diagnostic(
                            Some(state_dir),
                            diagnostics::DiagnosticEvent::new(
                                "error",
                                "agent_event",
                                "decision.route",
                                "failed",
                                format!("agent decision routing failed: {error}"),
                            )
                            .event_id(event.event_id.clone())
                            .object("agent_event", event.correlation_id.clone())
                            .source_node_id(event.source_node_id.clone())
                            .details(json!({
                                "event_type": format!("{:?}", event.event_type),
                                "decision_id": decision.decision_id,
                                "action": decision.action,
                                "route": format!("{:?}", decision.route),
                            })),
                        );
                    }
                }
            }
        }
        Err(error) => {
            crate::control::save_agent_event_record_state(
                state_dir,
                event,
                wattswarm_protocol::types::AgentEventStatus::Failed,
                observed_at_ms(),
            )?;
            crate::control::append_agent_event_delivery_record_state(
                state_dir,
                &crate::storage::LocalAgentEventDeliveryRow {
                    delivery_id: Uuid::new_v4().to_string(),
                    event_id: event.event_id.clone(),
                    attempt_no: 1,
                    endpoint_url: endpoint_url.clone(),
                    delivery_status: "failed".to_owned(),
                    response_code: None,
                    response_body: None,
                    error_text: Some(error.to_string()),
                    next_retry_at: None,
                    created_at: delivery_started_at,
                },
            )?;
            diagnostics::record_diagnostic(
                Some(state_dir),
                diagnostics::DiagnosticEvent::new(
                    "error",
                    "agent_event",
                    "delivery.callback",
                    "failed",
                    format!("agent event callback failed: {error}"),
                )
                .event_id(event.event_id.clone())
                .object("agent_event", event.correlation_id.clone())
                .source_node_id(event.source_node_id.clone())
                .details(json!({
                    "event_type": format!("{:?}", event.event_type),
                    "endpoint_url": endpoint_url,
                })),
            );
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct UnsignedAgentEnvelope<'a> {
    protocol: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_agent_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_agent_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability: Option<&'a String>,
    message_json: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    extensions_json: Option<&'a String>,
}

fn verify_agent_envelope_signature(envelope: &RawAgentEnvelope) -> Result<()> {
    let Some(signature) = envelope.signature.as_deref() else {
        return Ok(());
    };
    let signer_ref = envelope.source_agent_id.as_deref().ok_or_else(|| {
        anyhow!("agent envelope source_agent_id is required when signature is set")
    })?;
    let unsigned = UnsignedAgentEnvelope {
        protocol: &envelope.protocol,
        source_agent_id: envelope.source_agent_id.as_ref(),
        target_agent_id: envelope.target_agent_id.as_ref(),
        capability: envelope.capability.as_ref(),
        message_json: &envelope.message_json,
        extensions_json: envelope.extensions_json.as_ref(),
    };
    crate::crypto::verify_signature_ref(
        signer_ref,
        serde_jcs::to_string(&unsigned)?.as_bytes(),
        signature,
    )
    .context("verify agent envelope signature")
}

fn attach_agent_envelope_to_relationship(
    state_dir: &Path,
    remote_node_id: &str,
    envelope: &RawAgentEnvelope,
) -> Result<()> {
    let Some(mut record) = crate::control::load_peer_relationship_records_state(state_dir)?
        .into_iter()
        .find(|entry| entry.remote_node_id == remote_node_id)
    else {
        return Ok(());
    };
    record.agent_envelope = Some(raw_agent_envelope_to_control_record(envelope));
    crate::control::save_peer_relationship_record_state(state_dir, &record)
}

fn default_agent_envelope(
    local_node_id: &str,
    remote_node_id: &str,
    capability: &str,
    payload: Value,
) -> RawAgentEnvelope {
    RawAgentEnvelope {
        protocol: "google_a2a".to_owned(),
        source_agent_id: Some(local_node_id.to_owned()),
        target_agent_id: Some(remote_node_id.to_owned()),
        capability: Some(capability.to_owned()),
        message_json: serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned()),
        extensions_json: None,
        signature: None,
    }
}

fn peer_dm_content_from_control_envelope(
    envelope: &crate::control::AgentInteractionEnvelope,
) -> Value {
    crate::control::peer_dm_content_from_envelope(envelope)
}

fn peer_dm_thread_id(local_node_id: &str, remote_node_id: &str) -> String {
    let mut members = [local_node_id.to_owned(), remote_node_id.to_owned()];
    members.sort();
    format!("dm:{}:{}", members[0], members[1])
}

fn build_contact_material(state_dir: &Path, local_peer_id: &str) -> Result<RawContactMaterial> {
    let generated_at = observed_at_ms();
    let identity = crate::control::load_local_identity(state_dir)?;
    let peer_id = local_peer_id
        .parse::<PeerId>()
        .map_err(|err| anyhow!("parse local_peer_id as peer id: {err}"))?;
    let iroh_contact = export_local_contact_material(state_dir, &peer_id, generated_at)?;
    let material = json!({
        "node_id": identity.node_id(),
        "peer_id": local_peer_id,
        "listen_addrs": iroh_contact.metadata.listen_addrs.clone(),
        "generated_at": generated_at,
        "transports": [iroh_contact],
        "recommended_routes": crate::control::recommended_data_routes(Some(&iroh_contact.metadata.capabilities)),
    });
    let signature = identity.sign_bytes(&serde_json::to_vec(&material)?);
    Ok(RawContactMaterial {
        material_json: serde_json::to_string(&material)?,
        signature: Some(signature),
        generated_at,
    })
}

fn upsert_contact_material_for_peer(
    state_dir: &Path,
    remote_node_id: &str,
    contact_material: &RawContactMaterial,
) -> Result<()> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.node_id == remote_node_id);
    let record = crate::control::PeerMetadataRecord {
        node_id: remote_node_id.to_owned(),
        network_id: existing.as_ref().and_then(|entry| entry.network_id.clone()),
        params_version: existing.as_ref().and_then(|entry| entry.params_version),
        params_hash: existing
            .as_ref()
            .and_then(|entry| entry.params_hash.clone()),
        agent_version_raw: existing
            .as_ref()
            .and_then(|entry| entry.agent_version_raw.clone()),
        agent_version_prefix: existing
            .as_ref()
            .and_then(|entry| entry.agent_version_prefix.clone()),
        protocol_version: existing
            .as_ref()
            .and_then(|entry| entry.protocol_version.clone()),
        observed_addr: existing
            .as_ref()
            .and_then(|entry| entry.observed_addr.clone()),
        listen_addrs: existing
            .as_ref()
            .map_or_else(Vec::new, |entry| entry.listen_addrs.clone()),
        protocols: existing
            .as_ref()
            .map_or_else(Vec::new, |entry| entry.protocols.clone()),
        handshake_status: existing.as_ref().map_or_else(
            || "contact_material".to_owned(),
            |entry| entry.handshake_status.clone(),
        ),
        last_error: None,
        contact_material: serde_json::from_str(&contact_material.material_json).ok(),
        contact_material_signature: contact_material.signature.clone(),
        contact_material_updated_at: Some(contact_material.generated_at),
        first_identified_at: existing
            .as_ref()
            .map_or(now, |entry| entry.first_identified_at),
        last_identified_at: existing
            .as_ref()
            .map_or(now, |entry| entry.last_identified_at),
    };
    crate::control::save_peer_metadata_record_state(state_dir, &record)
}

fn relationship_state_for(
    state_dir: &Path,
    remote_node_id: &str,
) -> Result<Option<crate::control::PeerRelationshipState>> {
    Ok(
        crate::control::load_peer_relationship_records_state(state_dir)?
            .into_iter()
            .find(|record| record.remote_node_id == remote_node_id)
            .map(|record| record.relationship_state),
    )
}

fn recommended_backfill_route_for_peer(
    state_dir: &Path,
    peer: &PeerId,
) -> Result<DataTransportRoute> {
    let peer_id = peer.to_string();
    let record =
        crate::control::load_peer_metadata_record_for_transport_peer_id_state(state_dir, &peer_id)?;
    Ok(TransportRouter::select(
        &TransferIntent {
            kind: TransferKind::BackfillChunk,
            payload_bytes: 64 * 1024,
            requires_streaming: true,
        },
        record
            .as_ref()
            .and_then(|entry| entry.transport_capabilities())
            .as_ref(),
    ))
}

fn candidate_peer_addrs(state_dir: &Path, remote_node_id: &str) -> Result<Vec<Multiaddr>> {
    let mut addrs = Vec::new();
    for record in crate::control::load_discovered_peer_records_state(state_dir)? {
        if record.node_id != remote_node_id {
            continue;
        }
        let Some(listen_addr) = record.listen_addr else {
            continue;
        };
        if let Ok(addr) = listen_addr.parse::<Multiaddr>() {
            addrs.push(addr);
        }
    }
    for record in crate::control::load_peer_metadata_records_state(state_dir)? {
        if record.node_id != remote_node_id {
            continue;
        }
        if let Some(observed_addr) = record.observed_addr
            && let Ok(addr) = observed_addr.parse::<Multiaddr>()
        {
            addrs.push(addr);
        }
        for listen_addr in record.listen_addrs {
            if let Ok(addr) = listen_addr.parse::<Multiaddr>() {
                addrs.push(addr);
            }
        }
    }
    addrs.sort();
    addrs.dedup();
    Ok(addrs)
}

fn upsert_dm_thread(
    state_dir: &Path,
    remote_node_id: &str,
    thread_id: &str,
    session_state: crate::control::PeerDmSessionState,
    relationship_established_at: Option<u64>,
    last_message_at: Option<u64>,
) -> Result<crate::control::PeerDmThreadRecord> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_dm_thread_records_state(state_dir)?
        .into_iter()
        .find(|record| record.thread_id == thread_id);
    let session_rank = |state: crate::control::PeerDmSessionState| match state {
        crate::control::PeerDmSessionState::Established => 0_u8,
        crate::control::PeerDmSessionState::SessionPending => 1_u8,
        crate::control::PeerDmSessionState::Ready => 2_u8,
        crate::control::PeerDmSessionState::Blocked => 3_u8,
    };
    let merged_session_state = existing
        .as_ref()
        .map(|record| record.session_state)
        .map(|current| {
            if session_rank(session_state) >= session_rank(current) {
                session_state
            } else {
                current
            }
        })
        .unwrap_or(session_state);
    let record = crate::control::PeerDmThreadRecord {
        remote_node_id: remote_node_id.to_owned(),
        thread_id: thread_id.to_owned(),
        thread_kind: crate::control::PeerDmThreadKind::Direct,
        session_state: merged_session_state,
        relationship_established_at: relationship_established_at.or_else(|| {
            existing
                .as_ref()
                .and_then(|record| record.relationship_established_at)
        }),
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        updated_at: now,
        last_message_at: last_message_at
            .or_else(|| existing.as_ref().and_then(|record| record.last_message_at)),
    };
    crate::control::save_peer_dm_thread_record_state(state_dir, &record)?;
    Ok(record)
}

fn save_dm_message(
    state_dir: &Path,
    remote_node_id: &str,
    thread_id: &str,
    message_id: &str,
    message_kind: crate::control::PeerDmMessageKind,
    direction: crate::control::PeerDmDirection,
    delivery_state: crate::control::PeerDmDeliveryState,
    a2a_protocol: &str,
    agent_envelope: Option<&RawAgentEnvelope>,
    content: Value,
    acknowledged_at: Option<u64>,
) -> Result<crate::control::PeerDmMessageRecord> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_dm_message_records_state(state_dir, thread_id)?
        .into_iter()
        .find(|record| record.message_id == message_id);
    let agent_envelope = agent_envelope
        .map(raw_agent_envelope_to_control_record)
        .or_else(|| {
            existing
                .as_ref()
                .and_then(|record| record.agent_envelope.clone())
        })
        .or_else(|| {
            Some(crate::control::synthesize_peer_dm_envelope(
                a2a_protocol,
                &content,
            ))
        });
    let record = crate::control::PeerDmMessageRecord {
        thread_id: thread_id.to_owned(),
        message_id: message_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        message_kind,
        direction,
        delivery_state,
        a2a_protocol: a2a_protocol.to_owned(),
        content: agent_envelope
            .as_ref()
            .map(peer_dm_content_from_control_envelope)
            .or_else(|| existing.as_ref().map(|record| record.content.clone()))
            .unwrap_or(content),
        agent_envelope,
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        acknowledged_at: acknowledged_at
            .or_else(|| existing.as_ref().and_then(|record| record.acknowledged_at)),
    };
    crate::control::save_peer_dm_message_record_state(state_dir, &record)?;
    Ok(record)
}

fn save_agent_payment_summary(
    state_dir: &Path,
    remote_node_id: &str,
    summary: &SummaryAnnouncement,
) -> Result<crate::control::AgentPaymentRecord> {
    let message_kind = summary
        .payload
        .get("message_kind")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("agent payment summary missing message_kind"))?;
    let payment = summary
        .payload
        .get("payment")
        .cloned()
        .ok_or_else(|| anyhow!("agent payment summary missing payment payload"))?;
    let payment_id = payment
        .get("payment_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("agent payment summary missing payment_id"))?;
    let record = crate::control::AgentPaymentRecord {
        payment_id: payment_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        summary_id: summary.summary_id.clone(),
        message_kind: message_kind.to_owned(),
        payment,
        updated_at: observed_at_ms(),
    };
    crate::control::save_agent_payment_record_state(state_dir, &record)?;
    let event_type = if message_kind == "payment_request" {
        wattswarm_protocol::types::AgentEventType::PaymentRequest
    } else {
        wattswarm_protocol::types::AgentEventType::PaymentUpdate
    };
    let allowed_actions = payment_allowed_actions(message_kind);
    let event = build_agent_event(
        event_type,
        wattswarm_protocol::types::AgentEventSourceKind::PaymentSummary,
        Some(remote_node_id.to_owned()),
        None,
        json!({
            "summary_id": summary.summary_id,
            "message_kind": message_kind,
            "payment": record.payment,
        }),
        true,
        allowed_actions,
        Some(record.payment_id.clone()),
        Some(format!("payment:{}:{}", record.payment_id, message_kind)),
    );
    deliver_agent_event_to_local_executor(state_dir, None, &event)?;
    Ok(record)
}

fn payment_allowed_actions(message_kind: &str) -> Vec<String> {
    match message_kind {
        "payment_request" => vec![
            "authorize".to_owned(),
            "reject".to_owned(),
            "cancel".to_owned(),
        ],
        "payment_authorized" => vec!["submit".to_owned(), "cancel".to_owned()],
        "payment_submitted" => vec!["settle".to_owned()],
        "payment_settled" | "payment_rejected" | "payment_cancelled" => Vec::new(),
        _ => Vec::new(),
    }
}

fn process_pending_network_commands(
    service: &mut NetworkBridgeService,
    state_dir: &Path,
) -> Result<u64> {
    let pending_path = pending_network_commands_path(state_dir);
    if !pending_path.exists() {
        return Ok(0);
    }
    let content = fs::read_to_string(&pending_path)?;
    if content.trim().is_empty() {
        return Ok(0);
    }
    fs::write(&pending_path, "")?;

    let mut processed = 0_u64;
    let mut failed = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let command: PendingNetworkCommand = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let result = match command {
            PendingNetworkCommand::PeerRelationship {
                remote_node_id,
                action,
                agent_envelope,
            } => service
                .send_peer_relationship_action(&remote_node_id, action, Some(agent_envelope))
                .map(|_| ()),
            PendingNetworkCommand::PeerDirectMessage {
                remote_node_id,
                agent_envelope,
                content,
            } => service
                .send_peer_direct_message(&remote_node_id, Some(agent_envelope), content)
                .map(|_| ()),
            PendingNetworkCommand::AgentPayment {
                remote_node_id,
                message_kind,
                payment,
            } => {
                let mut summary =
                    build_agent_payment_summary(&remote_node_id, &message_kind, payment);
                summary.source_node_id = service.local_peer_id().to_string();
                service.publish_summary(summary).map(|_| ())
            }
        };
        match result {
            Ok(()) => processed += 1,
            Err(err) => {
                eprintln!("network_bridge: failed to process queued network command: {err:#}");
                failed.push(line.to_owned());
            }
        }
    }
    if !failed.is_empty() {
        let mut retry = failed.join("\n");
        retry.push('\n');
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&pending_path)
            .and_then(|mut file| file.write_all(retry.as_bytes()));
    }
    Ok(processed)
}

fn record_data_plane_status(
    state_dir: &Path,
    object_kind: &str,
    object_id: &str,
    remote_node_id: Option<&str>,
    route: &str,
    status: &str,
    detail: Option<&str>,
) -> Result<()> {
    crate::control::save_data_plane_status_record_state(
        state_dir,
        object_kind,
        object_id,
        remote_node_id,
        route,
        status,
        detail,
        observed_at_ms(),
    )
}

pub fn ingest_event_envelope(
    node: &mut Node,
    envelope: &EventEnvelope,
) -> Result<crate::types::Event> {
    let event = envelope.event.clone();
    node.ingest_remote(event.clone())?;
    Ok(event)
}

/// Log run-queue-relevant gossip events to JSONL files for async processing.
///
/// - `TaskAnnounced` with feed_key `venue.run_queue` → `pending_bridge_tasks.jsonl`
///   (executor side: this node should pick up and execute the task)
/// - `CandidateProposed` for `run-*` tasks → `pending_run_queue_results.jsonl`
///   (coordinator side: write remote result back to run_steps)
pub fn log_run_queue_events_if_applicable(
    node: &Node,
    state_dir: &Path,
    event: &crate::types::Event,
) {
    if event.author_node_id == node.node_id() {
        return;
    }

    match &event.payload {
        crate::types::EventPayload::TaskAnnounced(payload) => {
            if payload.feed_key != "venue.run_queue" {
                return;
            }
            let executor = payload
                .summary
                .get("executor")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let executor = crate::control::normalize_executor_name(executor);
            let profile = payload
                .summary
                .get("profile")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("default");
            if executor.is_empty() {
                return;
            }
            // Check that we have a matching local executor before queueing.
            if let Ok(reg) = crate::control::load_executor_registry_state(state_dir) {
                let has_local = reg
                    .entries
                    .iter()
                    .any(|e| !e.is_remote() && e.name == executor);
                if !has_local {
                    return;
                }
            }
            let entry = serde_json::json!({
                "task_id": payload.task_id,
                "executor": executor,
                "profile": profile,
                "queued_at": chrono::Utc::now().timestamp_millis(),
            });
            append_jsonl(state_dir, "pending_bridge_tasks.jsonl", &entry);
        }
        crate::types::EventPayload::CandidateProposed(payload) => {
            if !payload.task_id.starts_with("run-") {
                return;
            }
            let candidate_output = node
                .store
                .get_candidate_by_id(&payload.task_id, &payload.candidate.candidate_id)
                .ok()
                .flatten()
                .map(|candidate| candidate.output)
                .unwrap_or(Value::Null);
            let entry = serde_json::json!({
                "task_id": payload.task_id,
                "candidate_id": payload.candidate.candidate_id,
                "candidate_output": candidate_output,
                "author_node_id": event.author_node_id,
                "execution_id": payload.candidate.execution_id,
                "received_at": chrono::Utc::now().timestamp_millis(),
            });
            append_jsonl(state_dir, "pending_run_queue_results.jsonl", &entry);
        }
        _ => {}
    }
}

fn append_jsonl(state_dir: &Path, filename: &str, entry: &serde_json::Value) {
    if let Ok(mut line) = serde_json::to_string(entry) {
        line.push('\n');
        let path = state_dir.join(filename);
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
    }
}

pub fn event_envelope_from_gossip(message: &GossipMessage) -> Result<&EventEnvelope> {
    match message {
        GossipMessage::Event(envelope) => Ok(envelope),
        _ => bail!("gossip message is not an event payload"),
    }
}

pub fn summary_announcement_from_gossip(message: &GossipMessage) -> Result<&SummaryAnnouncement> {
    match message {
        GossipMessage::Summary(summary) => Ok(summary),
        _ => bail!("gossip message is not a summary payload"),
    }
}

pub fn event_gossip(envelope: EventEnvelope) -> GossipMessage {
    GossipMessage::Event(envelope)
}

pub fn chat_gossip(envelope: EventEnvelope) -> GossipMessage {
    GossipMessage::Chat(envelope)
}

pub fn global_event_gossip(envelope: EventEnvelope) -> Result<GossipMessage> {
    if envelope.scope != SwarmScope::Global {
        bail!("global event gossip requires global scope");
    }
    Ok(event_gossip(envelope))
}

fn parse_bool_env_with_default(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn parse_listen_addrs_env(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_scope_id_env(raw: &str, kind: fn(String) -> SwarmScope) -> Vec<SwarmScope> {
    raw.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| kind(segment.to_owned()))
        .collect()
}

pub fn configured_network_scopes_from_env() -> Vec<SwarmScope> {
    let mut scopes = vec![SwarmScope::Global];
    if let Ok(raw) = env::var(ENV_P2P_REGION_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Region) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
    }
    if let Ok(raw) = env::var(ENV_P2P_NODE_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Node) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
    }
    if let Ok(raw) = env::var(ENV_P2P_LOCAL_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Node) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
    }
    scopes
}

pub fn network_enabled_from_env() -> bool {
    parse_bool_env_with_default(ENV_P2P_ENABLED, true)
}

pub fn network_config_from_env() -> NetworkP2pConfig {
    let listen_addrs = env::var(ENV_P2P_LISTEN_ADDRS)
        .ok()
        .map(|raw| parse_listen_addrs_env(&raw))
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            let port = env::var(ENV_P2P_PORT)
                .ok()
                .and_then(|raw| raw.parse::<u16>().ok())
                .unwrap_or(DEFAULT_P2P_PORT);
            vec![format!("/ip4/0.0.0.0/tcp/{port}")]
        });
    let bootstrap_peers = env::var(ENV_P2P_BOOTSTRAP_PEERS)
        .ok()
        .map(|raw| parse_listen_addrs_env(&raw))
        .unwrap_or_default();

    NetworkP2pConfig {
        listen_addrs,
        bootstrap_peers,
        enable_mdns: parse_bool_env_with_default(ENV_P2P_MDNS, true),
        ..NetworkP2pConfig::default()
    }
}

pub fn network_config_from_state_dir(state_dir: &Path) -> NetworkP2pConfig {
    let mut config = network_config_from_env();
    if config.bootstrap_peers.is_empty() {
        config.bootstrap_peers = load_bootstrap_peers_from_startup_config(state_dir);
    }
    config
}

fn network_node_from_state_dir(
    state_dir: &Path,
    config: NetworkP2pConfig,
) -> Result<NetworkP2pNode> {
    let identity = crate::control::load_local_identity(state_dir)?;
    NetworkP2pNode::from_ed25519_secret_bytes(config, identity.secret_bytes())
}

pub fn maybe_start_background_network_service(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> Result<bool> {
    maybe_start_background_network_service_with_hook(state_dir, db_path, None)
}

/// Start the background network service with an optional per-tick hook.
/// The hook is invoked once per tick with mutable Node access and state_dir,
/// enabling external modules (e.g. run-queue bridge) to process pending work.
pub fn maybe_start_background_network_service_with_hook(
    state_dir: PathBuf,
    db_path: PathBuf,
    post_tick_hook: Option<PostTickHook>,
) -> Result<bool> {
    if !network_enabled_from_env() {
        return Ok(false);
    }
    let Some(mode) = crate::control::configured_node_mode(&state_dir)? else {
        eprintln!("wattswarm p2p network deferred (node mode not configured yet)");
        return Ok(false);
    };
    if matches!(mode, crate::control::NodeMode::Local) {
        return Ok(false);
    }

    let config = network_config_from_state_dir(&state_dir);
    let scopes = configured_network_scopes_from_env();
    config.validate()?;
    {
        let mut started = started_network_services()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if started.contains(&state_dir) {
            return Ok(true);
        }
        started.insert(state_dir.clone());
    }
    let state_dir_for_registry = state_dir.clone();
    thread::spawn(move || {
        if let Err(err) = run_background_network_service_with_hook(
            &state_dir,
            &db_path,
            config,
            scopes,
            post_tick_hook,
        ) {
            eprintln!("network bridge stopped: {err}");
        }
        clear_latest_network_observability_snapshot(&state_dir_for_registry);
        let mut started = started_network_services()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        started.remove(&state_dir_for_registry);
    });
    Ok(true)
}

/// Callback invoked once per tick of the background network service.
/// Receives mutable Node access and the state_dir path.
pub type PostTickHook = Box<dyn Fn(&mut Node, &Path) + Send + 'static>;

fn run_background_network_service_with_hook(
    state_dir: &Path,
    db_path: &Path,
    config: NetworkP2pConfig,
    configured_scopes: Vec<SwarmScope>,
    post_tick_hook: Option<PostTickHook>,
) -> Result<()> {
    let bootstrap_peers = config.bootstrap_peers.clone();
    let mut node = crate::control::open_configured_node(state_dir, db_path)?;
    let node_id = node.node_id();
    let scopes = merge_scopes(configured_scopes);
    let dynamic_subscriptions = dynamic_subscription_scope_kinds_for_node(&node, &node_id)?;
    let verified_protocol_params = node.store.load_verified_network_protocol_params()?;
    let protocol_params = verified_protocol_params.params().clone();
    let mut config = config.apply_protocol_params(&protocol_params);
    let handshake_network_id = verified_protocol_params.network_id.clone();
    config.namespace.network_id = handshake_network_id.clone();
    let handshake_params_version = verified_protocol_params.signed.version;
    let handshake_params_hash = verified_protocol_params.params_hash().to_owned();
    config.identify_agent_version =
        crate::network_p2p::encode_wattswarm_agent_version(&PeerHandshakeMetadata {
            network_id: handshake_network_id,
            params_version: handshake_params_version,
            params_hash: handshake_params_hash,
        });
    config.validate()?;
    let mut service = NetworkBridgeService::new(
        network_node_from_state_dir(state_dir, config)?,
        &scopes,
        &protocol_params,
    )?;
    for (scope, gossip_kinds) in dynamic_subscriptions {
        service.subscribe_scope_kinds(&scope, &gossip_kinds)?;
    }
    service.set_state_dir(state_dir.to_path_buf(), db_path.to_path_buf());
    store_latest_network_observability_snapshot(state_dir, service.observability_snapshot(&node)?);
    let mut announced_listen = false;
    let mut announced_peers: HashMap<String, Instant> = HashMap::new();
    let mut last_published_seq = node.head_seq()?;
    let mut next_dial_attempt_at = HashMap::new();

    loop {
        let mut did_work = false;
        loop {
            match service.try_tick(&mut node) {
                Ok(Some(NetworkBridgeTick::Listening { address })) => {
                    did_work = true;
                    if !announced_listen {
                        crate::udp_announce::announce_startup(
                            "p2p-startup",
                            Some(&address.to_string()),
                            Some(&node_id),
                        );
                        announced_listen = true;
                    }
                }
                Ok(Some(NetworkBridgeTick::Connected { peer })) => {
                    did_work = true;
                    let peer_str = peer.to_string();
                    if record_peer_announcement(&mut announced_peers, &peer_str, Instant::now()) {
                        eprintln!("p2p peer connected: {peer}");
                    }
                }
                Ok(Some(_)) => {
                    did_work = true;
                }
                Ok(None) => break,
                Err(err) => {
                    eprintln!("network bridge tick failed: {err}");
                    thread::sleep(Duration::from_millis(250));
                    break;
                }
            }
        }
        if dial_discovered_peer_endpoints(
            &mut service,
            state_dir,
            &node_id,
            &mut next_dial_attempt_at,
        )? > 0
        {
            did_work = true;
        }
        if dial_bootstrap_peer_endpoints(&mut service, &bootstrap_peers, &mut next_dial_attempt_at)?
            > 0
        {
            did_work = true;
        }
        let processed_pending_commands =
            match process_pending_network_commands(&mut service, state_dir) {
                Ok(count) => count,
                Err(err) => {
                    eprintln!("network bridge pending command processing failed: {err}");
                    0
                }
            };
        if processed_pending_commands > 0 {
            did_work = true;
        }
        let new_last_published_seq =
            publish_pending_scoped_updates(&mut service, &node, &node_id, last_published_seq)?;
        if new_last_published_seq != last_published_seq {
            did_work = true;
            last_published_seq = new_last_published_seq;
        }
        if service.run_anti_entropy(&node)? > 0 {
            did_work = true;
        }
        match service.observability_snapshot(&node) {
            Ok(snapshot) => store_latest_network_observability_snapshot(state_dir, snapshot),
            Err(err) => eprintln!("network bridge observability snapshot failed: {err}"),
        }
        if let Some(hook) = &post_tick_hook {
            hook(&mut node, state_dir);
        }
        if !did_work {
            thread::sleep(IDLE_NETWORK_SLEEP);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBridgeTick {
    Listening {
        address: Multiaddr,
    },
    TransportNotice {
        detail: String,
    },
    Connected {
        peer: PeerId,
    },
    Disconnected {
        peer: PeerId,
    },
    EventIngested {
        peer: PeerId,
        event_id: String,
    },
    SummaryApplied {
        peer: PeerId,
        summary_kind: String,
    },
    RuleApplied {
        peer: PeerId,
        rule_set: String,
        rule_version: u64,
    },
    CheckpointApplied {
        peer: PeerId,
        checkpoint_id: String,
    },
    GossipIgnored {
        peer: PeerId,
        message_kind: String,
    },
    BackfillServed {
        peer: PeerId,
        events: usize,
    },
    BackfillApplied {
        peer: PeerId,
        request_id: BackfillRequestId,
        events: usize,
    },
    BackfillFailed {
        peer: PeerId,
        request_id: BackfillRequestId,
        error: String,
    },
    PeerRelationshipUpdated {
        peer: PeerId,
        action: crate::control::PeerRelationshipAction,
        relationship_state: crate::control::PeerRelationshipState,
        initiated_by: crate::control::PeerRelationshipInitiator,
    },
    PeerRelationshipFailed {
        peer: PeerId,
        action: crate::control::PeerRelationshipAction,
        error: String,
    },
    PeerDirectMessageUpdated {
        peer: PeerId,
        kind: crate::control::PeerDmMessageKind,
        delivery_state: crate::control::PeerDmDeliveryState,
    },
    PeerDirectMessageFailed {
        peer: PeerId,
        kind: crate::control::PeerDmMessageKind,
        error: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScopeTrafficStats {
    pub published_events: u64,
    pub ingested_events: u64,
    pub summaries_applied: u64,
    pub rules_applied: u64,
    pub checkpoints_applied: u64,
    pub backfills_applied: u64,
    pub backfill_events_applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkBridgePeerHealth {
    pub peer: String,
    pub connected: bool,
    pub score: i64,
    pub blacklisted: bool,
    pub reputation_tier: String,
    pub quarantined: bool,
    pub quarantine_remaining_ms: u64,
    pub ban_remaining_ms: u64,
    pub throttle_factor_percent: u32,
    pub known_scopes: Vec<String>,
    pub inflight_backfills: usize,
    pub next_retry_in_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkBridgeScopeTraffic {
    pub scope: String,
    pub stats: ScopeTrafficStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkBridgeSummaryHealth {
    pub imported_decision_memory_rows: u64,
    pub imported_reputation_rows: u64,
    pub imported_task_outcome_rows: u64,
    pub checkpoint_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkBridgeSubnetSyncHealth {
    pub network_id: String,
    pub network_kind: String,
    pub parent_network_id: Option<String>,
    pub parent_uplink_available: bool,
    pub parent_imported_task_outcome_rows: Option<u64>,
    pub parent_checkpoint_rows: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkBridgeExecutionSetHealth {
    pub execution_set_count: u64,
    pub execution_set_member_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkBridgeObservabilitySnapshot {
    pub local_peer_id: String,
    pub listen_addrs: Vec<String>,
    pub subscribed_scopes: Vec<String>,
    pub connected_peer_count: usize,
    pub nat_status: String,
    pub nat_public_address: Option<String>,
    pub nat_confidence: u32,
    pub relay_reservations: Vec<String>,
    pub peer_health: Vec<NetworkBridgePeerHealth>,
    pub scope_traffic: Vec<NetworkBridgeScopeTraffic>,
    pub summary_health: NetworkBridgeSummaryHealth,
    pub subnet_sync_health: NetworkBridgeSubnetSyncHealth,
    pub execution_set_health: NetworkBridgeExecutionSetHealth,
}

pub struct NetworkBridgeService {
    runtime: NetworkRuntime,
    tokio_runtime: Runtime,
    subscribed_scopes: Vec<SwarmScope>,
    subscribed_scope_kinds: HashMap<SwarmScope, HashSet<GossipKind>>,
    pinned_scopes: Vec<SwarmScope>,
    peer_sync_state: HashMap<PeerId, PeerSyncState>,
    connected_peers: HashSet<PeerId>,
    global_publish_rate_guard: GlobalPublishRateGuard,
    anti_entropy_interval: Duration,
    backfill_retry_after: Duration,
    summary_reputation_limit: usize,
    summary_decision_memory_limit: u32,
    scope_traffic: HashMap<SwarmScope, ScopeTrafficStats>,
    pending_contact_material_requests:
        HashMap<ContactMaterialRequestId, PendingContactMaterialRequest>,
    pending_relationship_requests:
        HashMap<PeerRelationshipRequestId, PendingPeerRelationshipRequest>,
    pending_dm_requests: HashMap<PeerDirectMessageRequestId, PendingPeerDirectMessageRequest>,
    /// Optional state_dir for run-queue bridge hooks.
    state_dir: Option<PathBuf>,
    db_path: Option<PathBuf>,
}

impl NetworkBridgeService {
    pub fn new(
        node: NetworkP2pNode,
        scopes: &[SwarmScope],
        protocol_params: &NetworkProtocolParams,
    ) -> Result<Self> {
        let tokio_runtime = Runtime::new()?;
        let mut runtime = tokio_runtime.block_on(async { NetworkRuntime::new(node) })?;
        let mut subscribed_scopes = Vec::new();
        let mut subscribed_scope_kinds = HashMap::new();
        for scope in scopes {
            runtime.subscribe_scope(scope)?;
            if !subscribed_scopes.contains(scope) {
                subscribed_scopes.push(scope.clone());
            }
            subscribed_scope_kinds.insert(scope.clone(), GossipKind::ALL.into_iter().collect());
        }
        if subscribed_scopes.is_empty() {
            runtime.subscribe_scope(&SwarmScope::Global)?;
            subscribed_scopes.push(SwarmScope::Global);
            subscribed_scope_kinds
                .insert(SwarmScope::Global, GossipKind::ALL.into_iter().collect());
        }
        Ok(Self {
            runtime,
            tokio_runtime,
            pinned_scopes: subscribed_scopes.clone(),
            subscribed_scopes,
            subscribed_scope_kinds,
            peer_sync_state: HashMap::new(),
            connected_peers: HashSet::new(),
            global_publish_rate_guard: GlobalPublishRateGuard::new(Instant::now()),
            anti_entropy_interval: Duration::from_secs(protocol_params.anti_entropy_interval_secs),
            backfill_retry_after: Duration::from_secs(protocol_params.backfill_retry_after_secs),
            summary_reputation_limit: protocol_params.summary_reputation_limit,
            summary_decision_memory_limit: protocol_params.summary_decision_memory_limit,
            scope_traffic: HashMap::new(),
            pending_contact_material_requests: HashMap::new(),
            pending_relationship_requests: HashMap::new(),
            pending_dm_requests: HashMap::new(),
            state_dir: None,
            db_path: None,
        })
    }

    /// Set the local persistence paths for run-queue and agent-event hooks.
    pub fn set_state_dir(&mut self, state_dir: PathBuf, db_path: PathBuf) {
        self.state_dir = Some(state_dir);
        self.db_path = Some(db_path);
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.runtime.local_peer_id()
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        self.runtime.listen_addrs()
    }

    pub fn subscribed_scopes(&self) -> &[SwarmScope] {
        &self.subscribed_scopes
    }

    #[cfg(test)]
    pub fn subscribed_gossip_kinds(&self, scope: &SwarmScope) -> Vec<GossipKind> {
        self.subscribed_scope_kinds
            .get(scope)
            .map(|kinds| kinds.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.subscribe_scope_kinds(scope, &GossipKind::ALL)
    }

    pub fn subscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        if kinds.is_empty() {
            return Ok(());
        }
        let _guard = self.tokio_runtime.enter();
        self.runtime.subscribe_scope_kinds(scope, kinds)?;
        if !self.subscribed_scopes.contains(scope) {
            self.subscribed_scopes.push(scope.clone());
        }
        let subscribed = self
            .subscribed_scope_kinds
            .entry(scope.clone())
            .or_default();
        subscribed.extend(kinds.iter().copied());
        Ok(())
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.unsubscribe_scope_kinds(scope, &GossipKind::ALL)
    }

    pub fn unsubscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        if *scope == SwarmScope::Global
            || self.pinned_scopes.contains(scope)
            || !self.subscribed_scopes.contains(scope)
            || kinds.is_empty()
        {
            return Ok(());
        }
        let _guard = self.tokio_runtime.enter();
        self.runtime.unsubscribe_scope_kinds(scope, kinds)?;
        if let Some(subscribed) = self.subscribed_scope_kinds.get_mut(scope) {
            for kind in kinds {
                subscribed.remove(kind);
            }
            if subscribed.is_empty() {
                self.subscribed_scope_kinds.remove(scope);
                self.subscribed_scopes.retain(|existing| existing != scope);
            }
        }
        Ok(())
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.dial(addr)
    }
}

#[derive(Debug, Deserialize, Default)]
struct StartupBootstrapConfig {
    #[serde(default)]
    bootstrap_peers: Vec<String>,
}

fn load_bootstrap_peers_from_startup_config(state_dir: &Path) -> Vec<String> {
    let path = state_dir.join(STARTUP_CONFIG_FILE);
    let Ok(bytes) = fs::read(&path) else {
        return Vec::new();
    };
    match serde_json::from_slice::<StartupBootstrapConfig>(&bytes) {
        Ok(config) => config
            .bootstrap_peers
            .into_iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect(),
        Err(error) => {
            eprintln!(
                "failed to parse startup bootstrap peers from {}: {error}",
                path.display()
            );
            Vec::new()
        }
    }
}
