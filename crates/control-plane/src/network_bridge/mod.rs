mod announcements;
mod backfill;
mod publish;
mod scope;
mod summary;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result, anyhow, bail};

use crate::constants::BACKFILL_BATCH_EVENTS;
use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, ContactMaterialRequestId, EventEnvelope, GossipMessage,
    Multiaddr, NetworkP2pConfig, NetworkP2pNode, NetworkRuntime, NetworkRuntimeEvent,
    PeerDirectMessageRequest, PeerDirectMessageRequestId, PeerDirectMessageResponse,
    PeerHandshakeMetadata, PeerId, PeerRelationshipRequest, PeerRelationshipRequestId,
    PeerRelationshipResponse, RawAgentEnvelope, RawContactMaterial, RawContactMaterialRequest,
    RawContactMaterialResponse, RawPeerDirectMessageKind, RawPeerRelationshipAction,
    SummaryAnnouncement, SwarmScope,
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
pub use publish::{publish_pending_global_events, publish_pending_scoped_updates};
pub use summary::{
    apply_summary_announcement, build_knowledge_summary_for_task_type,
    build_reputation_summary_for_runtime,
};

use backfill::{latest_scoped_event_seq, should_publish_summaries, should_sync_event};
use publish::GlobalPublishRateGuard;
use scope::{
    dynamic_subscription_scopes_for_node, event_scope, merge_scopes,
    node_has_active_subscription_scope,
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
const MAX_INFLIGHT_BACKFILLS_PER_PEER: usize = 1;
const SUMMARY_BACKPRESSURE_HIGH_WATERMARK: u64 = 256;
const IDLE_NETWORK_SLEEP: Duration = Duration::from_millis(50);

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
    encrypted_body: Option<String>,
    content_encoding: Option<String>,
    acknowledged_at: Option<u64>,
) -> Result<crate::control::PeerDmMessageRecord> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_dm_message_records_state(state_dir, thread_id)?
        .into_iter()
        .find(|record| record.message_id == message_id);
    let record = crate::control::PeerDmMessageRecord {
        thread_id: thread_id.to_owned(),
        message_id: message_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        message_kind,
        direction,
        delivery_state,
        a2a_protocol: a2a_protocol.to_owned(),
        agent_envelope: agent_envelope.map(raw_agent_envelope_to_control_record),
        content: existing
            .as_ref()
            .map(|record| record.content.clone())
            .unwrap_or(content),
        encrypted_body: existing
            .as_ref()
            .and_then(|record| record.encrypted_body.clone())
            .or(encrypted_body),
        content_encoding: existing
            .as_ref()
            .and_then(|record| record.content_encoding.clone())
            .or(content_encoding),
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        acknowledged_at: acknowledged_at
            .or_else(|| existing.as_ref().and_then(|record| record.acknowledged_at)),
    };
    crate::control::save_peer_dm_message_record_state(state_dir, &record)?;
    Ok(record)
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
    let scopes = merge_scopes(
        configured_scopes
            .into_iter()
            .chain(dynamic_subscription_scopes_for_node(&node, &node_id)?),
    );
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
    service.set_state_dir(state_dir.to_path_buf());
    store_latest_network_observability_snapshot(state_dir, service.observability_snapshot(&node)?);
    let mut announced_listen = false;
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
                    eprintln!("p2p peer connected: {peer}");
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeTrafficStats {
    pub published_events: u64,
    pub ingested_events: u64,
    pub summaries_applied: u64,
    pub rules_applied: u64,
    pub checkpoints_applied: u64,
    pub backfills_applied: u64,
    pub backfill_events_applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkBridgeScopeTraffic {
    pub scope: String,
    pub stats: ScopeTrafficStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkBridgeSummaryHealth {
    pub imported_decision_memory_rows: u64,
    pub imported_reputation_rows: u64,
    pub imported_task_outcome_rows: u64,
    pub checkpoint_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkBridgeSubnetSyncHealth {
    pub network_id: String,
    pub network_kind: String,
    pub parent_network_id: Option<String>,
    pub parent_uplink_available: bool,
    pub parent_imported_task_outcome_rows: Option<u64>,
    pub parent_checkpoint_rows: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkBridgeExecutionSetHealth {
    pub execution_set_count: u64,
    pub execution_set_member_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
        for scope in scopes {
            runtime.subscribe_scope(scope)?;
            if !subscribed_scopes.contains(scope) {
                subscribed_scopes.push(scope.clone());
            }
        }
        if subscribed_scopes.is_empty() {
            runtime.subscribe_scope(&SwarmScope::Global)?;
            subscribed_scopes.push(SwarmScope::Global);
        }
        Ok(Self {
            runtime,
            tokio_runtime,
            pinned_scopes: subscribed_scopes.clone(),
            subscribed_scopes,
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
        })
    }

    /// Set the state_dir for run-queue bridge hooks.
    pub fn set_state_dir(&mut self, state_dir: PathBuf) {
        self.state_dir = Some(state_dir);
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

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.subscribe_scope(scope)?;
        if !self.subscribed_scopes.contains(scope) {
            self.subscribed_scopes.push(scope.clone());
        }
        Ok(())
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        if *scope == SwarmScope::Global
            || self.pinned_scopes.contains(scope)
            || !self.subscribed_scopes.contains(scope)
        {
            return Ok(());
        }
        let _guard = self.tokio_runtime.enter();
        self.runtime.unsubscribe_scope(scope)?;
        self.subscribed_scopes.retain(|existing| existing != scope);
        Ok(())
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.dial(addr)
    }

    pub fn send_peer_relationship_action(
        &mut self,
        remote_node_id: &str,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: Option<RawAgentEnvelope>,
    ) -> Result<PeerRelationshipRequestId> {
        let Some(state_dir) = &self.state_dir else {
            bail!("peer relationship actions require state_dir to be configured");
        };
        let remote_node_id = remote_node_id.trim();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let peer = remote_node_id
            .parse::<PeerId>()
            .map_err(|err| anyhow!("parse remote_node_id as peer id: {err}"))?;
        if !self.connected_peers.contains(&peer) {
            bail!("peer relationship actions require a connected peer");
        }
        let relationship_record = crate::control::apply_peer_relationship_action_state(
            state_dir,
            remote_node_id,
            action,
            crate::control::PeerRelationshipInitiator::Local,
        )?;
        let local_node_id = self.local_peer_id().to_string();
        let capability = match action {
            crate::control::PeerRelationshipAction::Request => "peer.relationship.request",
            crate::control::PeerRelationshipAction::Accept => "peer.relationship.accept",
            crate::control::PeerRelationshipAction::Reject => "peer.relationship.reject",
            crate::control::PeerRelationshipAction::Cancel => "peer.relationship.cancel",
            crate::control::PeerRelationshipAction::Remove => "peer.relationship.remove",
            crate::control::PeerRelationshipAction::Block => "peer.relationship.block",
            crate::control::PeerRelationshipAction::Unblock => "peer.relationship.unblock",
        };
        let envelope = agent_envelope.unwrap_or_else(|| {
            default_agent_envelope(
                &local_node_id,
                remote_node_id,
                capability,
                json!({
                    "action": wire_peer_relationship_action(action),
                    "remote_node_id": remote_node_id,
                }),
            )
        });
        if action == crate::control::PeerRelationshipAction::Accept
            && relationship_record.relationship_state
                == crate::control::PeerRelationshipState::Accepted
        {
            self.finalize_dm_session_from_relationship(
                state_dir,
                remote_node_id,
                crate::control::PeerDmDirection::Outbound,
                &envelope.protocol,
                relationship_record.updated_at,
            )?;
        }
        let _guard = self.tokio_runtime.enter();
        let request_id = self.runtime.send_peer_relationship_request(
            &peer,
            PeerRelationshipRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.to_owned(),
                action: wire_peer_relationship_action(action),
                agent_envelope: Some(envelope),
            },
        )?;
        self.pending_relationship_requests.insert(
            request_id,
            PendingPeerRelationshipRequest {
                peer,
                remote_node_id: remote_node_id.to_owned(),
                action,
            },
        );
        Ok(request_id)
    }

    fn request_peer_contact_material(
        &mut self,
        remote_node_id: &str,
    ) -> Result<ContactMaterialRequestId> {
        let Some(state_dir) = self.state_dir.clone() else {
            bail!("contact material requests require state_dir to be configured");
        };
        let peer = remote_node_id
            .parse::<PeerId>()
            .map_err(|err| anyhow!("parse remote_node_id as peer id: {err}"))?;
        self.ensure_peer_connected(&state_dir, &peer, remote_node_id)?;
        if !self.connected_peers.contains(&peer) {
            bail!("contact material requests require a connected peer");
        }
        let local_node_id = self.local_peer_id().to_string();
        let request_id = self.runtime.send_contact_material_request(
            &peer,
            RawContactMaterialRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.to_owned(),
            },
        )?;
        self.pending_contact_material_requests.insert(
            request_id,
            PendingContactMaterialRequest {
                peer,
                remote_node_id: remote_node_id.to_owned(),
            },
        );
        Ok(request_id)
    }

    pub fn send_peer_direct_message(
        &mut self,
        remote_node_id: &str,
        agent_envelope: Option<RawAgentEnvelope>,
        content: Value,
    ) -> Result<PeerDirectMessageRequestId> {
        let Some(state_dir) = self.state_dir.clone() else {
            bail!("peer direct messages require state_dir to be configured");
        };
        if relationship_state_for(&state_dir, remote_node_id)?
            != Some(crate::control::PeerRelationshipState::Accepted)
        {
            bail!("peer direct messages require an accepted relationship");
        }
        let remote_node_id = remote_node_id.trim();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let peer = remote_node_id
            .parse::<PeerId>()
            .map_err(|err| anyhow!("parse remote_node_id as peer id: {err}"))?;
        if !self.connected_peers.contains(&peer) {
            self.ensure_peer_connected(&state_dir, &peer, remote_node_id)?;
        }
        if !self.connected_peers.contains(&peer) {
            bail!("peer direct messages require a connected peer");
        }
        let local_node_id = self.local_peer_id().to_string();
        let thread_id = peer_dm_thread_id(&local_node_id, remote_node_id);
        let thread = crate::control::load_peer_dm_thread_records_state(&state_dir)?
            .into_iter()
            .find(|record| record.thread_id == thread_id)
            .ok_or_else(|| anyhow!("peer direct messages require an established session"))?;
        if thread.session_state != crate::control::PeerDmSessionState::Ready {
            bail!("peer direct messages require a ready session");
        }
        let message_id = Uuid::new_v4().to_string();
        let content_ref = crate::control::materialize_json_content_artifact(
            &state_dir,
            wattswarm_artifact_store::ArtifactKind::DirectMessage,
            &local_node_id,
            &content,
            observed_at_ms(),
        )?;
        let envelope = agent_envelope.unwrap_or_else(|| {
            default_agent_envelope(
                &local_node_id,
                remote_node_id,
                "peer.dm.message",
                content.clone(),
            )
        });
        let a2a_protocol = envelope.protocol.clone();
        save_dm_message(
            &state_dir,
            remote_node_id,
            &thread_id,
            &message_id,
            crate::control::PeerDmMessageKind::Message,
            crate::control::PeerDmDirection::Outbound,
            crate::control::PeerDmDeliveryState::Pending,
            &envelope.protocol,
            Some(&envelope),
            content.clone(),
            None,
            None,
            None,
        )?;
        record_data_plane_status(
            &state_dir,
            "dm_message",
            &message_id,
            Some(remote_node_id),
            "iroh_direct",
            "content_materialized",
            None,
        )?;
        let _guard = self.tokio_runtime.enter();
        let request_id = self.runtime.send_peer_direct_message_request(
            &peer,
            PeerDirectMessageRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.to_owned(),
                thread_id: thread_id.clone(),
                message_id: message_id.clone(),
                kind: RawPeerDirectMessageKind::Message,
                agent_envelope: Some(envelope),
                contact_material: None,
                content_ref: Some(content_ref),
                encrypted_body: None,
                content_encoding: None,
                control_json: None,
            },
        )?;
        record_data_plane_status(
            &state_dir,
            "dm_message",
            &message_id,
            Some(remote_node_id),
            "libp2p_control",
            "control_sent",
            None,
        )?;
        self.pending_dm_requests.insert(
            request_id,
            PendingPeerDirectMessageRequest {
                peer,
                remote_node_id: remote_node_id.to_owned(),
                thread_id,
                message_id,
                kind: crate::control::PeerDmMessageKind::Message,
                a2a_protocol,
            },
        );
        Ok(request_id)
    }

    fn maybe_sync_topic_message_content(
        &mut self,
        node: &mut Node,
        event: &crate::types::Event,
        content_source_node_id: Option<&str>,
    ) -> Result<()> {
        let Some(state_dir) = self.state_dir.as_ref() else {
            return Ok(());
        };
        let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload else {
            return Ok(());
        };
        let remote_node_id = content_source_node_id.unwrap_or(&payload.content_ref.producer);
        record_data_plane_status(
            state_dir,
            "topic_message",
            &event.event_id,
            Some(remote_node_id),
            "libp2p_control",
            "control_received",
            None,
        )?;
        let content = match crate::control::fetch_json_content_artifact_via_iroh_with_local_peer_id(
            state_dir,
            &self.local_peer_id(),
            remote_node_id,
            wattswarm_artifact_store::ArtifactKind::TopicMessage,
            &payload.content_ref,
        ) {
            Ok(content) => content,
            Err(err) => {
                let _ = record_data_plane_status(
                    state_dir,
                    "topic_message",
                    &event.event_id,
                    Some(remote_node_id),
                    "iroh_direct",
                    "hydrate_failed",
                    Some(&err.to_string()),
                );
                return Err(err);
            }
        };
        node.store
            .update_topic_message_content(&event.event_id, &content, observed_at_ms())?;
        record_data_plane_status(
            state_dir,
            "topic_message",
            &event.event_id,
            Some(remote_node_id),
            "iroh_direct",
            "content_hydrated",
            None,
        )?;
        Ok(())
    }

    fn maybe_sync_candidate_output(
        &mut self,
        node: &mut Node,
        event: &crate::types::Event,
    ) -> Result<()> {
        let Some(state_dir) = self.state_dir.as_ref() else {
            return Ok(());
        };
        let crate::types::EventPayload::CandidateProposed(payload) = &event.payload else {
            return Ok(());
        };
        if payload.candidate.has_resolved_output()
            || node
                .store
                .get_candidate_by_id(&payload.task_id, &payload.candidate.candidate_id)?
                .is_some_and(|candidate| candidate.has_resolved_output())
        {
            return Ok(());
        }
        record_data_plane_status(
            state_dir,
            "candidate_output",
            &payload.candidate.candidate_id,
            Some(&payload.candidate.output_ref.producer),
            "libp2p_control",
            "control_received",
            None,
        )?;
        let content = match crate::control::fetch_json_content_artifact_via_iroh_with_local_peer_id(
            state_dir,
            &self.local_peer_id(),
            &payload.candidate.output_ref.producer,
            wattswarm_artifact_store::ArtifactKind::Reference,
            &payload.candidate.output_ref,
        ) {
            Ok(content) => content,
            Err(err) => {
                let _ = record_data_plane_status(
                    state_dir,
                    "candidate_output",
                    &payload.candidate.candidate_id,
                    Some(&payload.candidate.output_ref.producer),
                    "iroh_direct",
                    "hydrate_failed",
                    Some(&err.to_string()),
                );
                return Err(err);
            }
        };
        node.store.update_candidate_output(
            &payload.task_id,
            &payload.candidate.candidate_id,
            &content,
            observed_at_ms(),
        )?;
        record_data_plane_status(
            state_dir,
            "candidate_output",
            &payload.candidate.candidate_id,
            Some(&payload.candidate.output_ref.producer),
            "iroh_direct",
            "content_hydrated",
            None,
        )?;
        Ok(())
    }

    fn record_dm_session_ready(
        &self,
        state_dir: &Path,
        remote_node_id: &str,
        thread_id: &str,
        direction: crate::control::PeerDmDirection,
        a2a_protocol: &str,
        acknowledged_at: Option<u64>,
    ) -> Result<()> {
        save_dm_message(
            state_dir,
            remote_node_id,
            thread_id,
            &format!("session-init:{thread_id}"),
            crate::control::PeerDmMessageKind::SessionInit,
            direction,
            crate::control::PeerDmDeliveryState::Delivered,
            a2a_protocol,
            None,
            json!({
                "thread_id": thread_id,
                "session_state": "ready",
                "synthetic": true,
            }),
            None,
            None,
            acknowledged_at,
        )?;
        Ok(())
    }

    fn finalize_dm_session_from_relationship(
        &self,
        state_dir: &Path,
        remote_node_id: &str,
        direction: crate::control::PeerDmDirection,
        a2a_protocol: &str,
        established_at: u64,
    ) -> Result<()> {
        let local_node_id = self.local_peer_id().to_string();
        let thread_id = peer_dm_thread_id(&local_node_id, remote_node_id);
        upsert_dm_thread(
            state_dir,
            remote_node_id,
            &thread_id,
            crate::control::PeerDmSessionState::Ready,
            Some(established_at),
            Some(established_at),
        )?;
        save_dm_message(
            state_dir,
            remote_node_id,
            &thread_id,
            &format!("relationship-established:{thread_id}"),
            crate::control::PeerDmMessageKind::RelationshipEstablished,
            direction,
            crate::control::PeerDmDeliveryState::Delivered,
            a2a_protocol,
            None,
            json!({
                "relationship_state": "accepted",
                "thread_id": thread_id,
                "established_at": established_at,
                "synthetic": true,
            }),
            None,
            None,
            Some(established_at),
        )?;
        self.record_dm_session_ready(
            state_dir,
            remote_node_id,
            &thread_id,
            direction,
            a2a_protocol,
            Some(established_at),
        )?;
        Ok(())
    }

    fn ensure_peer_connected(
        &mut self,
        state_dir: &Path,
        peer: &PeerId,
        remote_node_id: &str,
    ) -> Result<()> {
        if self.connected_peers.contains(peer) {
            return Ok(());
        }
        for addr in candidate_peer_addrs(state_dir, remote_node_id)? {
            match self.dial(addr.clone()) {
                Ok(()) => {}
                Err(err)
                    if err
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate connection") => {}
                Err(err) => {
                    eprintln!("peer reconnect dial failed for {remote_node_id} via {addr}: {err}");
                }
            }
        }
        Ok(())
    }

    pub fn publish_event_for_scope(
        &mut self,
        scope: &SwarmScope,
        event: crate::types::Event,
    ) -> Result<()> {
        if !self.global_publish_rate_guard.allow(scope, &event) {
            bail!("GlobalTopicRateLimited");
        }
        self.runtime.publish_gossip(&event_gossip(EventEnvelope {
            scope: scope.clone(),
            event,
            content_source_node_id: None,
        }))
    }

    pub fn publish_chat_for_scope(
        &mut self,
        scope: &SwarmScope,
        event: crate::types::Event,
    ) -> Result<()> {
        if !self.global_publish_rate_guard.allow(scope, &event) {
            bail!("GlobalTopicRateLimited");
        }
        self.runtime.publish_gossip(&chat_gossip(EventEnvelope {
            scope: scope.clone(),
            event,
            content_source_node_id: Some(self.local_peer_id().to_string()),
        }))
    }

    pub fn publish_global_event(&mut self, event: crate::types::Event) -> Result<()> {
        self.publish_event_for_scope(&SwarmScope::Global, event)
    }

    pub fn publish_summary(&mut self, summary: SummaryAnnouncement) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Summary(summary))
    }

    pub fn publish_checkpoint(
        &mut self,
        checkpoint: crate::network_p2p::CheckpointAnnouncement,
    ) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Checkpoint(checkpoint))
    }

    pub fn observability_snapshot(
        &self,
        node: &Node,
    ) -> Result<NetworkBridgeObservabilitySnapshot> {
        let runtime = self.runtime.observability_snapshot();
        let topology = node
            .store
            .load_network_topology_for_org(node.store.org_id())?;
        let parent_uplink = parent_uplink_store(node)?;
        let parent_imported_task_outcome_rows = parent_uplink
            .as_ref()
            .map(|store| store.count_imported_task_outcomes())
            .transpose()?;
        let parent_checkpoint_rows = parent_uplink
            .as_ref()
            .map(|store| store.count_checkpoint_announcements())
            .transpose()?;

        let traffic_scores = runtime
            .peer_health
            .iter()
            .map(|entry| (entry.peer.clone(), entry))
            .collect::<HashMap<_, _>>();
        let mut peer_ids = self
            .peer_sync_state
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        for entry in &runtime.peer_health {
            if !peer_ids.iter().any(|peer| peer == &entry.peer) {
                peer_ids.push(entry.peer.clone());
            }
        }
        peer_ids.sort();
        peer_ids.dedup();

        let now = Instant::now();
        let mut peer_health = Vec::with_capacity(peer_ids.len());
        for peer in peer_ids {
            let traffic = traffic_scores.get(&peer);
            let sync = self
                .peer_sync_state
                .iter()
                .find(|(peer_id, _)| peer_id.to_string() == peer)
                .map(|(_, state)| state);
            let mut known_scopes = sync
                .map(|state| {
                    let mut scopes = state
                        .known_scopes
                        .iter()
                        .map(scope_hint_label)
                        .collect::<Vec<_>>();
                    scopes.sort();
                    scopes
                })
                .unwrap_or_default();
            known_scopes.sort();
            peer_health.push(NetworkBridgePeerHealth {
                peer: peer.clone(),
                connected: self
                    .connected_peers
                    .iter()
                    .any(|peer_id| peer_id.to_string() == peer),
                score: traffic.map_or(0, |entry| entry.score),
                blacklisted: traffic.is_some_and(|entry| entry.blacklisted),
                reputation_tier: traffic
                    .map(|entry| entry.reputation_tier.clone())
                    .unwrap_or_else(|| "healthy".to_owned()),
                quarantined: traffic.is_some_and(|entry| entry.quarantined),
                quarantine_remaining_ms: traffic.map_or(0, |entry| entry.quarantine_remaining_ms),
                ban_remaining_ms: traffic.map_or(0, |entry| entry.ban_remaining_ms),
                throttle_factor_percent: traffic.map_or(100, |entry| entry.throttle_factor_percent),
                known_scopes,
                inflight_backfills: sync.map_or(0, |state| state.inflight_backfills),
                next_retry_in_ms: sync.map_or(0, |state| {
                    state
                        .next_retry_at
                        .saturating_duration_since(now)
                        .as_millis() as u64
                }),
            });
        }

        let mut scope_traffic = self
            .scope_traffic
            .iter()
            .map(|(scope, stats)| NetworkBridgeScopeTraffic {
                scope: scope_hint_label(scope),
                stats: stats.clone(),
            })
            .collect::<Vec<_>>();
        scope_traffic.sort_by(|left, right| left.scope.cmp(&right.scope));

        Ok(NetworkBridgeObservabilitySnapshot {
            local_peer_id: self.local_peer_id().to_string(),
            listen_addrs: self
                .listen_addrs()
                .iter()
                .map(ToString::to_string)
                .collect(),
            subscribed_scopes: self
                .subscribed_scopes
                .iter()
                .map(scope_hint_label)
                .collect(),
            connected_peer_count: self.connected_peers.len(),
            nat_status: runtime.nat_status,
            nat_public_address: runtime.nat_public_address,
            nat_confidence: runtime.nat_confidence,
            relay_reservations: runtime.relay_reservations,
            peer_health,
            scope_traffic,
            summary_health: NetworkBridgeSummaryHealth {
                imported_decision_memory_rows: node.store.count_imported_decision_memory()?,
                imported_reputation_rows: node.store.count_imported_reputation_snapshots()?,
                imported_task_outcome_rows: node.store.count_imported_task_outcomes()?,
                checkpoint_rows: node.store.count_checkpoint_announcements()?,
            },
            subnet_sync_health: NetworkBridgeSubnetSyncHealth {
                network_id: topology.network.network_id,
                network_kind: topology.network.network_kind.as_str().to_owned(),
                parent_network_id: topology.network.parent_network_id,
                parent_uplink_available: parent_uplink.is_some(),
                parent_imported_task_outcome_rows,
                parent_checkpoint_rows,
            },
            execution_set_health: NetworkBridgeExecutionSetHealth {
                execution_set_count: node.store.count_distinct_execution_sets()?,
                execution_set_member_count: node.store.count_execution_set_members()?,
            },
        })
    }

    pub fn request_global_backfill(
        &mut self,
        peer: &PeerId,
        from_event_seq: u64,
        limit: usize,
    ) -> Result<BackfillRequestId> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.send_backfill_request(
            peer,
            BackfillRequest {
                scope: SwarmScope::Global,
                from_event_seq,
                limit,
                feed_key: None,
            },
        )
    }

    pub fn tick(&mut self, node: &mut Node) -> Result<NetworkBridgeTick> {
        let event = self.tokio_runtime.block_on(self.runtime.next_event());
        self.handle_runtime_event(node, event)
    }

    fn request_backfill_for_peer(&mut self, peer: &PeerId, node: &Node) -> Result<bool> {
        let scopes = self.scopes_to_request_for_peer(peer);
        self.request_backfill_scopes_for_peer(peer, node, &scopes)
    }

    fn request_backfill_scopes_for_peer(
        &mut self,
        peer: &PeerId,
        node: &Node,
        scopes: &[SwarmScope],
    ) -> Result<bool> {
        let now = Instant::now();
        let state = self
            .peer_sync_state
            .entry(*peer)
            .or_insert_with(|| PeerSyncState::new(now));
        if state.inflight_backfills >= MAX_INFLIGHT_BACKFILLS_PER_PEER || state.next_retry_at > now
        {
            return Ok(false);
        }
        if scopes.is_empty() {
            return Ok(false);
        }
        let local_node_id = node.node_id();
        let active_subscriptions = node.store.list_active_feed_subscriptions(&local_node_id)?;
        let mut requests_sent = 0usize;
        let selected_route = self
            .state_dir
            .as_ref()
            .map(|state_dir| recommended_backfill_route_for_peer(state_dir, peer))
            .transpose()?
            .unwrap_or(DataTransportRoute::Libp2pControl);
        if let Some(state_dir) = &self.state_dir {
            append_jsonl(
                state_dir,
                "transport_decisions.jsonl",
                &json!({
                    "kind": TransferKind::BackfillChunk.as_str(),
                    "peer_id": peer.to_string(),
                    "route": selected_route.as_str(),
                    "scopes": scopes.iter().map(scope_hint_label).collect::<Vec<_>>(),
                    "decided_at": observed_at_ms(),
                }),
            );
        }
        for scope in scopes.iter().cloned() {
            let scope_hint = scope_hint_label(&scope);
            let from_event_seq = latest_scoped_event_seq(node, &scope)?;
            let _guard = self.tokio_runtime.enter();
            let _ = self.runtime.send_backfill_request(
                peer,
                BackfillRequest {
                    scope: scope.clone(),
                    from_event_seq,
                    limit: BACKFILL_BATCH_EVENTS,
                    feed_key: None,
                },
            )?;
            requests_sent += 1;
            for subscription in active_subscriptions
                .iter()
                .filter(|subscription| subscription.scope_hint == scope_hint)
            {
                let from_event_seq = node
                    .store
                    .get_topic_cursor(&local_node_id, &subscription.feed_key)?
                    .map_or(0, |cursor| cursor.last_event_seq);
                let _ = self.runtime.send_backfill_request(
                    peer,
                    BackfillRequest {
                        scope: scope.clone(),
                        from_event_seq,
                        limit: BACKFILL_BATCH_EVENTS,
                        feed_key: Some(subscription.feed_key.clone()),
                    },
                )?;
                requests_sent += 1;
            }
        }
        state.inflight_backfills += requests_sent;
        state.last_backfill_request_at = Some(now);
        state.next_retry_at = now + self.backfill_retry_after;
        Ok(requests_sent > 0)
    }

    pub fn run_anti_entropy(&mut self, node: &Node) -> Result<usize> {
        let now = Instant::now();
        let mut requested = 0usize;
        let mut scopes = self.subscribed_scopes.clone();
        scopes.sort_by_key(|scope| matches!(scope, SwarmScope::Global));
        let mut requests_by_peer: HashMap<PeerId, Vec<SwarmScope>> = HashMap::new();
        for scope in scopes {
            let Some(peer) = self.preferred_backfill_peer_for_scope(&scope, now) else {
                continue;
            };
            requests_by_peer.entry(peer).or_default().push(scope);
        }
        for (peer, scopes) in requests_by_peer {
            if self.request_backfill_scopes_for_peer(&peer, node, &scopes)? {
                requested += 1;
            }
        }
        Ok(requested)
    }

    fn scopes_to_request_for_peer(&self, peer: &PeerId) -> Vec<SwarmScope> {
        let Some(state) = self.peer_sync_state.get(peer) else {
            return self.subscribed_scopes.clone();
        };
        if state.known_scopes.is_empty() {
            return self.subscribed_scopes.clone();
        }
        self.subscribed_scopes
            .iter()
            .filter(|scope| state.known_scopes.contains(*scope))
            .cloned()
            .collect()
    }

    fn preferred_backfill_peer_for_scope(
        &self,
        scope: &SwarmScope,
        now: Instant,
    ) -> Option<PeerId> {
        let peers = self.connected_peers.iter().copied().collect::<Vec<_>>();
        self.best_peer_for_scope(peers.iter().copied(), scope, now, true)
            .or_else(|| self.best_peer_for_scope(peers.into_iter(), scope, now, false))
    }

    fn best_peer_for_scope<I>(
        &self,
        peers: I,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> Option<PeerId>
    where
        I: Iterator<Item = PeerId>,
    {
        let runtime = self.runtime.observability_snapshot();
        let peer_scores = runtime
            .peer_health
            .into_iter()
            .map(|entry| (entry.peer.clone(), entry))
            .collect::<HashMap<_, _>>();
        peers
            .filter(|peer| self.peer_is_eligible_for_scope(peer, scope, now, require_known_scope))
            .max_by(|left, right| {
                let left_key = peer_scores
                    .get(&left.to_string())
                    .map_or((0_i64, 100_u32), |entry| {
                        (entry.score, entry.throttle_factor_percent)
                    });
                let right_key = peer_scores
                    .get(&right.to_string())
                    .map_or((0_i64, 100_u32), |entry| {
                        (entry.score, entry.throttle_factor_percent)
                    });
                left_key
                    .cmp(&right_key)
                    .then_with(|| left.to_string().cmp(&right.to_string()))
            })
    }

    fn peer_is_eligible_for_scope(
        &self,
        peer: &PeerId,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> bool {
        if !self.runtime.allows_outbound_backfill_to(peer) {
            return false;
        }
        match self.peer_sync_state.get(peer) {
            Some(state) => {
                if require_known_scope && !state.known_scopes.contains(scope) {
                    return false;
                }
                state.inflight_backfills < MAX_INFLIGHT_BACKFILLS_PER_PEER
                    && state.next_retry_at <= now
                    && state.last_backfill_request_at.map_or(true, |last| {
                        now.duration_since(last) >= self.anti_entropy_interval
                    })
            }
            None => !require_known_scope,
        }
    }

    fn record_peer_scope_activity(&mut self, peer: PeerId, scope: &SwarmScope) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .known_scopes
            .insert(scope.clone());
    }

    fn scope_traffic_mut(&mut self, scope: &SwarmScope) -> &mut ScopeTrafficStats {
        self.scope_traffic.entry(scope.clone()).or_default()
    }

    pub(crate) fn record_scope_event_published(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).published_events += 1;
    }

    fn record_scope_event_ingested(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).ingested_events += 1;
    }

    fn record_scope_summary_applied(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).summaries_applied += 1;
    }

    fn record_scope_rule_applied(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).rules_applied += 1;
    }

    fn record_scope_checkpoint_applied(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).checkpoints_applied += 1;
    }

    fn record_scope_backfill_applied(&mut self, scope: &SwarmScope, events: usize) {
        let stats = self.scope_traffic_mut(scope);
        stats.backfills_applied += 1;
        stats.backfill_events_applied += events as u64;
    }

    fn mark_peer_connected(&mut self, peer: PeerId) {
        self.connected_peers.insert(peer);
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()));
    }

    fn mark_peer_disconnected(&mut self, peer: PeerId) {
        self.connected_peers.remove(&peer);
        self.peer_sync_state.remove(&peer);
        self.pending_relationship_requests
            .retain(|_, pending| pending.peer != peer);
        self.pending_dm_requests
            .retain(|_, pending| pending.peer != peer);
    }

    fn mark_backfill_completed(&mut self, peer: PeerId) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfills = state.inflight_backfills.saturating_sub(1);
            state.next_retry_at = Instant::now() + self.anti_entropy_interval;
        }
    }

    fn mark_backfill_failed(&mut self, peer: PeerId) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfills = state.inflight_backfills.saturating_sub(1);
            state.next_retry_at = Instant::now() + self.backfill_retry_after;
        }
    }

    pub fn try_tick(&mut self, node: &mut Node) -> Result<Option<NetworkBridgeTick>> {
        let event = {
            let _guard = self.tokio_runtime.enter();
            self.runtime.try_next_event()?
        };
        let Some(event) = event else {
            return Ok(None);
        };
        Ok(Some(self.process_runtime_event(node, event)?))
    }

    fn handle_runtime_event(
        &mut self,
        node: &mut Node,
        event: Result<NetworkRuntimeEvent>,
    ) -> Result<NetworkBridgeTick> {
        self.process_runtime_event(node, event?)
    }

    fn process_runtime_event(
        &mut self,
        node: &mut Node,
        event: NetworkRuntimeEvent,
    ) -> Result<NetworkBridgeTick> {
        match event {
            NetworkRuntimeEvent::PeerDiscovered {
                peer,
                address,
                source,
            } => {
                if let Some(state_dir) = &self.state_dir {
                    let _ = crate::control::add_discovered_peer_endpoint_with_source(
                        state_dir,
                        &peer.to_string(),
                        Some(&address.to_string()),
                        source.as_str(),
                    );
                }
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "peer_discovered peer={peer} source={} address={address}",
                        source.as_str()
                    ),
                })
            }
            NetworkRuntimeEvent::PeerIdentified { peer, metadata } => {
                let mut missing_contact_material = false;
                if let Some(state_dir) = &self.state_dir {
                    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    let existing = crate::control::load_peer_metadata_records_state(state_dir)
                        .ok()
                        .and_then(|records| {
                            records
                                .into_iter()
                                .find(|record| record.node_id == peer.to_string())
                        });
                    let record = crate::control::PeerMetadataRecord {
                        node_id: peer.to_string(),
                        network_id: Some(metadata.network_id.clone()),
                        params_version: Some(metadata.params_version),
                        params_hash: Some(metadata.params_hash.clone()),
                        agent_version_raw: Some(metadata.agent_version_raw.clone()),
                        agent_version_prefix: Some(metadata.agent_version_prefix.clone()),
                        protocol_version: Some(metadata.protocol_version.clone()),
                        observed_addr: Some(metadata.observed_addr.clone()),
                        listen_addrs: metadata.listen_addrs.clone(),
                        protocols: metadata.protocols.clone(),
                        handshake_status: "identified".to_owned(),
                        last_error: None,
                        contact_material: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material.clone()),
                        contact_material_signature: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_signature.clone()),
                        contact_material_updated_at: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_updated_at),
                        first_identified_at: existing
                            .as_ref()
                            .map_or(now, |entry| entry.first_identified_at),
                        last_identified_at: now,
                    };
                    missing_contact_material = record.contact_material.is_none();
                    let _ = crate::control::save_peer_metadata_record_state(state_dir, &record);
                }
                if missing_contact_material
                    && let Err(err) = self.request_peer_contact_material(&peer.to_string())
                {
                    eprintln!("contact material request failed for {peer}: {err}");
                }
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "peer_identified peer={peer} network_id={} params_version={}",
                        metadata.network_id, metadata.params_version
                    ),
                })
            }
            NetworkRuntimeEvent::NewListenAddr { address } => {
                Ok(NetworkBridgeTick::Listening { address })
            }
            NetworkRuntimeEvent::NatStatusChanged {
                old,
                new,
                public_address,
                confidence,
            } => Ok(NetworkBridgeTick::TransportNotice {
                detail: format!(
                    "nat_status_changed old={old} new={new} public_address={} confidence={confidence}",
                    public_address
                        .map(|addr| addr.to_string())
                        .unwrap_or_else(|| "none".to_owned())
                ),
            }),
            NetworkRuntimeEvent::RelayReservationAccepted {
                relay_peer,
                renewal,
            } => Ok(NetworkBridgeTick::TransportNotice {
                detail: format!(
                    "relay_reservation_accepted relay_peer={relay_peer} renewal={renewal}"
                ),
            }),
            NetworkRuntimeEvent::RelayCircuitEstablished { relay_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("relay_circuit_established relay_peer={relay_peer}"),
                })
            }
            NetworkRuntimeEvent::RelayInboundCircuitEstablished { source_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("relay_inbound_circuit_established source_peer={source_peer}"),
                })
            }
            NetworkRuntimeEvent::DcutrConnectionUpgradeSucceeded { remote_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("dcutr_upgrade_succeeded remote_peer={remote_peer}"),
                })
            }
            NetworkRuntimeEvent::DcutrConnectionUpgradeFailed { remote_peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("dcutr_upgrade_failed remote_peer={remote_peer} error={error}"),
                })
            }
            NetworkRuntimeEvent::ConnectionEstablished { peer, remote_addr } => {
                self.mark_peer_connected(peer);
                if let Some(state_dir) = &self.state_dir {
                    let remote_addr_text = remote_addr.to_string();
                    let _ = crate::control::add_discovered_peer_endpoint_with_source(
                        state_dir,
                        &peer.to_string(),
                        Some(remote_addr_text.as_str()),
                        "connected",
                    );
                }
                let _ = self.request_backfill_for_peer(&peer, node)?;
                if !node.store.is_node_penalized(&node.node_id())? {
                    for entry in node
                        .store
                        .list_local_reputation_snapshots(self.summary_reputation_limit)?
                    {
                        if let Some(summary) = build_reputation_summary_for_runtime(
                            node,
                            &entry.runtime_id,
                            &entry.profile_id,
                        )? {
                            let _ = self.publish_summary(summary);
                        }
                    }
                }
                Ok(NetworkBridgeTick::Connected { peer })
            }
            NetworkRuntimeEvent::PeerHandshakeRejected { peer, detail } => {
                self.mark_peer_disconnected(peer);
                if let Some(state_dir) = &self.state_dir {
                    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    let existing = crate::control::load_peer_metadata_records_state(state_dir)
                        .ok()
                        .and_then(|records| {
                            records
                                .into_iter()
                                .find(|record| record.node_id == peer.to_string())
                        });
                    let record = crate::control::PeerMetadataRecord {
                        node_id: peer.to_string(),
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
                        handshake_status: "rejected".to_owned(),
                        last_error: Some(detail.clone()),
                        contact_material: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material.clone()),
                        contact_material_signature: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_signature.clone()),
                        contact_material_updated_at: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_updated_at),
                        first_identified_at: existing
                            .as_ref()
                            .map_or(now, |entry| entry.first_identified_at),
                        last_identified_at: now,
                    };
                    let _ = crate::control::save_peer_metadata_record_state(state_dir, &record);
                }
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("peer_handshake_rejected peer={peer} {detail}"),
                })
            }
            NetworkRuntimeEvent::ConnectionClosed {
                peer,
                remaining_established,
            } => {
                if remaining_established == 0 {
                    self.mark_peer_disconnected(peer);
                    Ok(NetworkBridgeTick::Disconnected { peer })
                } else {
                    Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "connection_closed peer={peer} remaining_established={remaining_established}"
                        ),
                    })
                }
            }
            NetworkRuntimeEvent::Gossip {
                propagation_source,
                message,
            } => match message {
                GossipMessage::Event(envelope) => {
                    self.record_peer_scope_activity(propagation_source, &envelope.scope);
                    let ingested_event = ingest_event_envelope(node, &envelope)?;
                    if let Err(err) = self.maybe_sync_candidate_output(node, &ingested_event) {
                        eprintln!(
                            "candidate output sync failed for {}: {err}",
                            ingested_event.event_id
                        );
                    }
                    if let Some(state_dir) = &self.state_dir {
                        log_run_queue_events_if_applicable(node, state_dir, &ingested_event);
                    }
                    self.record_scope_event_ingested(&envelope.scope);
                    Ok(NetworkBridgeTick::EventIngested {
                        peer: propagation_source,
                        event_id: ingested_event.event_id.clone(),
                    })
                }
                GossipMessage::Chat(envelope) => {
                    self.record_peer_scope_activity(propagation_source, &envelope.scope);
                    let ingested_event = ingest_event_envelope(node, &envelope)?;
                    if let Err(err) = self.maybe_sync_candidate_output(node, &ingested_event) {
                        eprintln!(
                            "candidate output sync failed for {}: {err}",
                            ingested_event.event_id
                        );
                    }
                    if let Err(err) = self.maybe_sync_topic_message_content(
                        node,
                        &ingested_event,
                        envelope.content_source_node_id.as_deref(),
                    ) {
                        eprintln!(
                            "topic content sync failed for {}: {err}",
                            ingested_event.event_id
                        );
                    }
                    if let crate::types::EventPayload::TopicMessagePosted(payload) =
                        &ingested_event.payload
                    {
                        maybe_record_topic_cursor_for_event_id(
                            node,
                            &node.node_id(),
                            &payload.feed_key,
                            &envelope.scope,
                            &ingested_event.event_id,
                            ingested_event.created_at,
                        )?;
                    }
                    self.record_scope_event_ingested(&envelope.scope);
                    Ok(NetworkBridgeTick::EventIngested {
                        peer: propagation_source,
                        event_id: ingested_event.event_id.clone(),
                    })
                }
                GossipMessage::Summary(summary) => {
                    self.record_peer_scope_activity(propagation_source, &summary.scope);
                    apply_summary_announcement(node, &summary)?;
                    self.record_scope_summary_applied(&summary.scope);
                    Ok(NetworkBridgeTick::SummaryApplied {
                        peer: propagation_source,
                        summary_kind: summary.summary_kind,
                    })
                }
                GossipMessage::Rule(rule) => {
                    self.record_peer_scope_activity(propagation_source, &rule.scope);
                    apply_rule_announcement(node, &rule)?;
                    self.record_scope_rule_applied(&rule.scope);
                    Ok(NetworkBridgeTick::RuleApplied {
                        peer: propagation_source,
                        rule_set: rule.rule_set,
                        rule_version: rule.rule_version,
                    })
                }
                GossipMessage::Checkpoint(checkpoint) => {
                    self.record_peer_scope_activity(propagation_source, &checkpoint.scope);
                    apply_checkpoint_announcement(node, &checkpoint)?;
                    if let Some(state_dir) = &self.state_dir {
                        let _ = crate::control::save_data_source_binding_record_state(
                            state_dir,
                            &crate::control::DataSourceBindingRecord {
                                binding_kind: crate::control::DataSourceBindingKind::Checkpoint,
                                binding_scope: Some(checkpoint.scope.label()?),
                                binding_key: checkpoint.checkpoint_id.clone(),
                                source_node_id: propagation_source.to_string(),
                                source_uri: Some(checkpoint.artifact_path.clone()),
                                updated_at: observed_at_ms(),
                            },
                        );
                    }
                    self.record_scope_checkpoint_applied(&checkpoint.scope);
                    Ok(NetworkBridgeTick::CheckpointApplied {
                        peer: propagation_source,
                        checkpoint_id: checkpoint.checkpoint_id,
                    })
                }
            },
            NetworkRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            } => {
                let response = backfill_response_for_request(
                    node,
                    &self.local_peer_id().to_string(),
                    &request,
                    self.runtime.config().max_backfill_events,
                    self.runtime.config().max_backfill_events_hard_limit,
                )?;
                let events = response.events.len();
                match self.runtime.send_backfill_response(channel, response) {
                    Ok(()) => Ok(NetworkBridgeTick::BackfillServed { peer, events }),
                    Err(error)
                        if error
                            .to_string()
                            .contains("backfill response channel closed") =>
                    {
                        Ok(NetworkBridgeTick::TransportNotice {
                            detail: format!("backfill_response_dropped peer={peer} reason={error}"),
                        })
                    }
                    Err(error) => Err(error),
                }
            }
            NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response,
            } => {
                self.record_peer_scope_activity(peer, &response.scope);
                self.mark_backfill_completed(peer);
                let events = ingest_backfill_response(node, &response)?;
                for envelope in &response.events {
                    if let Err(err) = self.maybe_sync_candidate_output(node, &envelope.event) {
                        eprintln!(
                            "candidate output sync failed for {}: {err}",
                            envelope.event.event_id
                        );
                    }
                    if let Err(err) = self.maybe_sync_topic_message_content(
                        node,
                        &envelope.event,
                        envelope.content_source_node_id.as_deref(),
                    ) {
                        eprintln!(
                            "topic content sync failed for {}: {err}",
                            envelope.event.event_id
                        );
                    }
                }
                maybe_record_topic_cursor_for_response(
                    node,
                    &node.node_id(),
                    &response,
                    observed_at_ms(),
                )?;
                self.record_scope_backfill_applied(&response.scope, events);
                Ok(NetworkBridgeTick::BackfillApplied {
                    peer,
                    request_id,
                    events,
                })
            }
            NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                self.mark_backfill_failed(peer);
                Ok(NetworkBridgeTick::BackfillFailed {
                    peer,
                    request_id,
                    error,
                })
            }
            NetworkRuntimeEvent::BackfillInboundFailure { peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("backfill_inbound_failure peer={peer} error={error}"),
                })
            }
            NetworkRuntimeEvent::ContactMaterialRequest {
                peer,
                request,
                channel,
            } => {
                let local_node_id = self.local_peer_id().to_string();
                let now = observed_at_ms();
                let response = if request.source_node_id != peer.to_string() {
                    RawContactMaterialResponse {
                        source_node_id: local_node_id,
                        target_node_id: request.source_node_id,
                        applied: false,
                        contact_material: None,
                        detail: Some("contact material request source_node_id mismatch".to_owned()),
                        updated_at: now,
                    }
                } else if let Some(state_dir) = self.state_dir.as_ref() {
                    RawContactMaterialResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id,
                        applied: true,
                        contact_material: Some(build_contact_material(
                            state_dir,
                            &self.local_peer_id().to_string(),
                        )?),
                        detail: None,
                        updated_at: now,
                    }
                } else {
                    RawContactMaterialResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id,
                        applied: false,
                        contact_material: None,
                        detail: Some("state_dir is not configured".to_owned()),
                        updated_at: now,
                    }
                };
                self.runtime
                    .send_contact_material_response(channel, response)?;
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("contact_material_served peer={peer}"),
                })
            }
            NetworkRuntimeEvent::ContactMaterialResponse {
                peer,
                request_id,
                response,
            } => {
                let Some(pending) = self.pending_contact_material_requests.remove(&request_id)
                else {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "contact_material_response_without_pending peer={peer} request_id={request_id:?}"
                        ),
                    });
                };
                if pending.peer != peer
                    || response.source_node_id != pending.remote_node_id
                    || response.target_node_id != self.local_peer_id().to_string()
                {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "contact_material_response_mismatch peer={peer} remote={} source={} target={}",
                            pending.remote_node_id,
                            response.source_node_id,
                            response.target_node_id
                        ),
                    });
                }
                if !response.applied {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "contact_material_rejected peer={peer} detail={}",
                            response.detail.unwrap_or_else(|| "unknown".to_owned())
                        ),
                    });
                }
                if let (Some(state_dir), Some(contact_material)) =
                    (self.state_dir.as_ref(), response.contact_material.as_ref())
                {
                    upsert_contact_material_for_peer(
                        state_dir,
                        &pending.remote_node_id,
                        contact_material,
                    )?;
                }
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("contact_material_updated peer={peer}"),
                })
            }
            NetworkRuntimeEvent::ContactMaterialOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                self.pending_contact_material_requests.remove(&request_id);
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "contact_material_outbound_failure peer={peer} request_id={request_id:?} error={error}"
                    ),
                })
            }
            NetworkRuntimeEvent::ContactMaterialInboundFailure { peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("contact_material_inbound_failure peer={peer} error={error}"),
                })
            }
            NetworkRuntimeEvent::PeerRelationshipRequest {
                peer,
                request,
                channel,
            } => {
                let action = control_peer_relationship_action(request.action);
                let local_node_id = self.local_peer_id().to_string();
                let now = observed_at_ms();
                let (response, tick) = if request.source_node_id != peer.to_string() {
                    let error = format!(
                        "peer relationship request source_node_id mismatch: payload={} transport={peer}",
                        request.source_node_id
                    );
                    (
                        PeerRelationshipResponse {
                            source_node_id: local_node_id.clone(),
                            target_node_id: request.source_node_id.clone(),
                            action: request.action,
                            applied: false,
                            agent_envelope: Some(default_agent_envelope(
                                &local_node_id,
                                &request.source_node_id,
                                "peer.relationship.response",
                                json!({
                                    "action": request.action,
                                    "applied": false,
                                    "detail": error,
                                }),
                            )),
                            relationship_state: None,
                            detail: Some(error.clone()),
                            updated_at: now,
                        },
                        NetworkBridgeTick::PeerRelationshipFailed {
                            peer,
                            action,
                            error,
                        },
                    )
                } else if request.target_node_id != local_node_id {
                    let error = format!(
                        "peer relationship request target_node_id mismatch: payload={} local={local_node_id}",
                        request.target_node_id
                    );
                    (
                        PeerRelationshipResponse {
                            source_node_id: local_node_id.clone(),
                            target_node_id: request.source_node_id.clone(),
                            action: request.action,
                            applied: false,
                            agent_envelope: Some(default_agent_envelope(
                                &local_node_id,
                                &request.source_node_id,
                                "peer.relationship.response",
                                json!({
                                    "action": request.action,
                                    "applied": false,
                                    "detail": error,
                                }),
                            )),
                            relationship_state: None,
                            detail: Some(error.clone()),
                            updated_at: now,
                        },
                        NetworkBridgeTick::PeerRelationshipFailed {
                            peer,
                            action,
                            error,
                        },
                    )
                } else if let Some(state_dir) = self.state_dir.clone() {
                    if let Some(agent_envelope) = request.agent_envelope.as_ref() {
                        verify_agent_envelope_signature(agent_envelope)?;
                    }
                    match crate::control::apply_peer_relationship_action_state(
                        &state_dir,
                        &request.source_node_id,
                        action,
                        crate::control::PeerRelationshipInitiator::Remote,
                    ) {
                        Ok(record) => {
                            if let Some(agent_envelope) = request.agent_envelope.as_ref() {
                                attach_agent_envelope_to_relationship(
                                    &state_dir,
                                    &request.source_node_id,
                                    agent_envelope,
                                )?;
                            }
                            if action == crate::control::PeerRelationshipAction::Accept
                                && record.relationship_state
                                    == crate::control::PeerRelationshipState::Accepted
                            {
                                let a2a_protocol = request
                                    .agent_envelope
                                    .as_ref()
                                    .map(|envelope| envelope.protocol.clone())
                                    .unwrap_or_else(|| "google_a2a".to_owned());
                                self.finalize_dm_session_from_relationship(
                                    &state_dir,
                                    &request.source_node_id,
                                    crate::control::PeerDmDirection::Inbound,
                                    &a2a_protocol,
                                    record.updated_at,
                                )?;
                            }
                            (
                                PeerRelationshipResponse {
                                    source_node_id: local_node_id.clone(),
                                    target_node_id: request.source_node_id.clone(),
                                    action: request.action,
                                    applied: true,
                                    agent_envelope: Some(default_agent_envelope(
                                        &local_node_id,
                                        &request.source_node_id,
                                        "peer.relationship.response",
                                        json!({
                                            "action": request.action,
                                            "applied": true,
                                            "relationship_state": record.relationship_state.as_str(),
                                        }),
                                    )),
                                    relationship_state: Some(
                                        record.relationship_state.as_str().to_owned(),
                                    ),
                                    detail: None,
                                    updated_at: record.updated_at,
                                },
                                NetworkBridgeTick::PeerRelationshipUpdated {
                                    peer,
                                    action,
                                    relationship_state: record.relationship_state,
                                    initiated_by: crate::control::PeerRelationshipInitiator::Remote,
                                },
                            )
                        }
                        Err(error) => {
                            let error = error.to_string();
                            (
                                PeerRelationshipResponse {
                                    source_node_id: local_node_id.clone(),
                                    target_node_id: request.source_node_id.clone(),
                                    action: request.action,
                                    applied: false,
                                    agent_envelope: Some(default_agent_envelope(
                                        &local_node_id,
                                        &request.source_node_id,
                                        "peer.relationship.response",
                                        json!({
                                            "action": request.action,
                                            "applied": false,
                                            "detail": error,
                                        }),
                                    )),
                                    relationship_state: None,
                                    detail: Some(error.clone()),
                                    updated_at: now,
                                },
                                NetworkBridgeTick::PeerRelationshipFailed {
                                    peer,
                                    action,
                                    error,
                                },
                            )
                        }
                    }
                } else {
                    let error =
                        "peer relationship request received before state_dir was configured"
                            .to_owned();
                    (
                        PeerRelationshipResponse {
                            source_node_id: local_node_id.clone(),
                            target_node_id: request.source_node_id.clone(),
                            action: request.action,
                            applied: false,
                            agent_envelope: Some(default_agent_envelope(
                                &local_node_id,
                                &request.source_node_id,
                                "peer.relationship.response",
                                json!({
                                    "action": request.action,
                                    "applied": false,
                                    "detail": error,
                                }),
                            )),
                            relationship_state: None,
                            detail: Some(error.clone()),
                            updated_at: now,
                        },
                        NetworkBridgeTick::PeerRelationshipFailed {
                            peer,
                            action,
                            error,
                        },
                    )
                };
                match self
                    .runtime
                    .send_peer_relationship_response(channel, response)
                {
                    Ok(()) => Ok(tick),
                    Err(error)
                        if error
                            .to_string()
                            .contains("peer relationship response channel closed") =>
                    {
                        Ok(NetworkBridgeTick::TransportNotice {
                            detail: format!(
                                "peer_relationship_response_dropped peer={peer} reason={error}"
                            ),
                        })
                    }
                    Err(error) => Err(error),
                }
            }
            NetworkRuntimeEvent::PeerRelationshipResponse {
                peer,
                request_id,
                response,
            } => {
                let Some(pending) = self.pending_relationship_requests.remove(&request_id) else {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "peer_relationship_response_without_pending peer={peer} request_id={request_id:?}"
                        ),
                    });
                };
                let action = pending.action;
                let expected_action = wire_peer_relationship_action(action);
                let local_node_id = self.local_peer_id().to_string();
                if pending.peer != peer {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: format!(
                            "peer relationship response peer mismatch: expected={} actual={peer}",
                            pending.peer
                        ),
                    });
                }
                if response.source_node_id != pending.remote_node_id
                    || response.target_node_id != local_node_id
                    || response.action != expected_action
                {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: format!(
                            "peer relationship response payload mismatch source={} target={} action={:?}",
                            response.source_node_id, response.target_node_id, response.action
                        ),
                    });
                }
                if !response.applied {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: response
                            .detail
                            .unwrap_or_else(|| "peer relationship action rejected".to_owned()),
                    });
                }
                let Some(state_dir) = self.state_dir.clone() else {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error:
                            "peer relationship response received before state_dir was configured"
                                .to_owned(),
                    });
                };
                let local_state = crate::control::load_peer_relationship_records_state(&state_dir)?
                    .into_iter()
                    .find(|record| record.remote_node_id == pending.remote_node_id)
                    .map(|record| record.relationship_state);
                match local_state {
                    Some(relationship_state) => {
                        if action == crate::control::PeerRelationshipAction::Accept
                            && relationship_state == crate::control::PeerRelationshipState::Accepted
                        {
                            let a2a_protocol = response
                                .agent_envelope
                                .as_ref()
                                .map(|envelope| envelope.protocol.clone())
                                .unwrap_or_else(|| "google_a2a".to_owned());
                            self.finalize_dm_session_from_relationship(
                                &state_dir,
                                &pending.remote_node_id,
                                crate::control::PeerDmDirection::Outbound,
                                &a2a_protocol,
                                response.updated_at,
                            )?;
                        }
                        Ok(NetworkBridgeTick::PeerRelationshipUpdated {
                            peer,
                            action,
                            relationship_state,
                            initiated_by: crate::control::PeerRelationshipInitiator::Local,
                        })
                    }
                    None => Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: "peer relationship response applied without a local state record"
                            .to_owned(),
                    }),
                }
            }
            NetworkRuntimeEvent::PeerDirectMessageRequest {
                peer,
                request,
                channel,
            } => {
                let Some(state_dir) = self.state_dir.clone() else {
                    let response = PeerDirectMessageResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id.clone(),
                        thread_id: request.thread_id,
                        message_id: request.message_id,
                        kind: request.kind,
                        applied: false,
                        delivery_state: "rejected".to_owned(),
                        contact_material: None,
                        detail: Some(
                            "peer direct message request received before state_dir was configured"
                                .to_owned(),
                        ),
                        updated_at: observed_at_ms(),
                    };
                    if let Err(err) = self
                        .runtime
                        .send_peer_direct_message_response(channel, response)
                    {
                        if err.to_string() == "peer direct message response channel closed" {
                            return Ok(NetworkBridgeTick::TransportNotice {
                                detail: format!(
                                    "peer_direct_message_response_closed peer={peer} phase=state_dir_missing kind={}",
                                    format!("{:?}", request.kind)
                                ),
                            });
                        }
                        return Err(err);
                    }
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: crate::control::PeerDmMessageKind::Message,
                        error:
                            "peer direct message request received before state_dir was configured"
                                .to_owned(),
                    });
                };
                if request.source_node_id != peer.to_string()
                    || request.target_node_id != self.local_peer_id().to_string()
                {
                    let response = PeerDirectMessageResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id.clone(),
                        thread_id: request.thread_id,
                        message_id: request.message_id,
                        kind: request.kind,
                        applied: false,
                        delivery_state: "rejected".to_owned(),
                        contact_material: None,
                        detail: Some("peer direct message payload mismatch".to_owned()),
                        updated_at: observed_at_ms(),
                    };
                    if let Err(err) = self
                        .runtime
                        .send_peer_direct_message_response(channel, response)
                    {
                        if err.to_string() == "peer direct message response channel closed" {
                            return Ok(NetworkBridgeTick::TransportNotice {
                                detail: format!(
                                    "peer_direct_message_response_closed peer={peer} phase=payload_mismatch kind={}",
                                    format!("{:?}", request.kind)
                                ),
                            });
                        }
                        return Err(err);
                    }
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: crate::control::PeerDmMessageKind::Message,
                        error: "peer direct message payload mismatch".to_owned(),
                    });
                }
                if relationship_state_for(&state_dir, &request.source_node_id)?
                    != Some(crate::control::PeerRelationshipState::Accepted)
                {
                    let response = PeerDirectMessageResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id.clone(),
                        thread_id: request.thread_id,
                        message_id: request.message_id,
                        kind: request.kind,
                        applied: false,
                        delivery_state: "rejected".to_owned(),
                        contact_material: None,
                        detail: Some(
                            "peer direct messages require an accepted relationship".to_owned(),
                        ),
                        updated_at: observed_at_ms(),
                    };
                    if let Err(err) = self
                        .runtime
                        .send_peer_direct_message_response(channel, response)
                    {
                        if err.to_string() == "peer direct message response channel closed" {
                            return Ok(NetworkBridgeTick::TransportNotice {
                                detail: format!(
                                    "peer_direct_message_response_closed peer={peer} phase=relationship_not_accepted kind={}",
                                    format!("{:?}", request.kind)
                                ),
                            });
                        }
                        return Err(err);
                    }
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: match request.kind {
                            RawPeerDirectMessageKind::RelationshipEstablished => {
                                crate::control::PeerDmMessageKind::RelationshipEstablished
                            }
                            RawPeerDirectMessageKind::SessionInit => {
                                crate::control::PeerDmMessageKind::SessionInit
                            }
                            RawPeerDirectMessageKind::Message => {
                                crate::control::PeerDmMessageKind::Message
                            }
                        },
                        error: "peer direct messages require an accepted relationship".to_owned(),
                    });
                }

                let kind = match request.kind {
                    RawPeerDirectMessageKind::RelationshipEstablished => {
                        crate::control::PeerDmMessageKind::RelationshipEstablished
                    }
                    RawPeerDirectMessageKind::SessionInit => {
                        crate::control::PeerDmMessageKind::SessionInit
                    }
                    RawPeerDirectMessageKind::Message => crate::control::PeerDmMessageKind::Message,
                };
                let now = observed_at_ms();
                if let Some(agent_envelope) = request.agent_envelope.as_ref() {
                    verify_agent_envelope_signature(agent_envelope)?;
                }
                if let Some(contact_material) = &request.contact_material {
                    upsert_contact_material_for_peer(
                        &state_dir,
                        &request.source_node_id,
                        contact_material,
                    )?;
                }
                let next_session_state = match request.kind {
                    RawPeerDirectMessageKind::RelationshipEstablished => {
                        crate::control::PeerDmSessionState::Ready
                    }
                    RawPeerDirectMessageKind::SessionInit => {
                        crate::control::PeerDmSessionState::Ready
                    }
                    RawPeerDirectMessageKind::Message => crate::control::PeerDmSessionState::Ready,
                };
                upsert_dm_thread(
                    &state_dir,
                    &request.source_node_id,
                    &request.thread_id,
                    next_session_state,
                    if matches!(
                        request.kind,
                        RawPeerDirectMessageKind::RelationshipEstablished
                    ) {
                        Some(now)
                    } else {
                        None
                    },
                    Some(now),
                )?;
                let a2a_protocol = request
                    .agent_envelope
                    .as_ref()
                    .map(|envelope| envelope.protocol.clone())
                    .unwrap_or_else(|| "google_a2a".to_owned());
                let content = match request.kind {
                    RawPeerDirectMessageKind::Message => {
                        let content_ref = request
                            .content_ref
                            .clone()
                            .ok_or_else(|| anyhow!("peer direct message content_ref missing"))?;
                        record_data_plane_status(
                            &state_dir,
                            "dm_message",
                            &request.message_id,
                            Some(&request.source_node_id),
                            "libp2p_control",
                            "control_received",
                            None,
                        )?;
                        match crate::control::fetch_json_content_artifact_via_iroh_with_local_peer_id(
                            &state_dir,
                            &self.local_peer_id(),
                            &request.source_node_id,
                            wattswarm_artifact_store::ArtifactKind::DirectMessage,
                            &content_ref,
                        ) {
                            Ok(content) => {
                                record_data_plane_status(
                                    &state_dir,
                                    "dm_message",
                                    &request.message_id,
                                    Some(&request.source_node_id),
                                    "iroh_direct",
                                    "content_hydrated",
                                    None,
                                )?;
                                content
                            }
                            Err(err) => {
                                let _ = record_data_plane_status(
                                    &state_dir,
                                    "dm_message",
                                    &request.message_id,
                                    Some(&request.source_node_id),
                                    "iroh_direct",
                                    "hydrate_failed",
                                    Some(&err.to_string()),
                                );
                                return Err(err);
                            }
                        }
                    }
                    _ => request
                        .control_json
                        .as_deref()
                        .map(|raw| serde_json::from_str::<Value>(raw).unwrap_or_else(|_| json!({})))
                        .unwrap_or_else(|| json!({})),
                };
                save_dm_message(
                    &state_dir,
                    &request.source_node_id,
                    &request.thread_id,
                    &request.message_id,
                    kind,
                    crate::control::PeerDmDirection::Inbound,
                    crate::control::PeerDmDeliveryState::Delivered,
                    &a2a_protocol,
                    request.agent_envelope.as_ref(),
                    content,
                    request.encrypted_body.clone(),
                    request.content_encoding.clone(),
                    None,
                )?;
                let response_contact_material = if matches!(
                    request.kind,
                    RawPeerDirectMessageKind::RelationshipEstablished
                ) {
                    Some(build_contact_material(
                        &state_dir,
                        &self.local_peer_id().to_string(),
                    )?)
                } else {
                    None
                };
                let response = PeerDirectMessageResponse {
                    source_node_id: self.local_peer_id().to_string(),
                    target_node_id: request.source_node_id.clone(),
                    thread_id: request.thread_id.clone(),
                    message_id: request.message_id.clone(),
                    kind: request.kind,
                    applied: true,
                    delivery_state: "delivered".to_owned(),
                    contact_material: response_contact_material,
                    detail: None,
                    updated_at: now,
                };
                if let Err(err) = self
                    .runtime
                    .send_peer_direct_message_response(channel, response)
                {
                    if err.to_string() == "peer direct message response channel closed" {
                        return Ok(NetworkBridgeTick::TransportNotice {
                            detail: format!(
                                "peer_direct_message_response_closed peer={peer} phase=applied kind={}",
                                format!("{:?}", request.kind)
                            ),
                        });
                    }
                    return Err(err);
                }
                if matches!(
                    request.kind,
                    RawPeerDirectMessageKind::RelationshipEstablished
                ) {
                    self.record_dm_session_ready(
                        &state_dir,
                        &request.source_node_id,
                        &request.thread_id,
                        crate::control::PeerDmDirection::Inbound,
                        &a2a_protocol,
                        Some(now),
                    )?;
                }
                Ok(NetworkBridgeTick::PeerDirectMessageUpdated {
                    peer,
                    kind,
                    delivery_state: crate::control::PeerDmDeliveryState::Delivered,
                })
            }
            NetworkRuntimeEvent::PeerDirectMessageResponse {
                peer,
                request_id,
                response,
            } => {
                let Some(pending) = self.pending_dm_requests.remove(&request_id) else {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "peer_direct_message_response_without_pending peer={peer} request_id={request_id:?}"
                        ),
                    });
                };
                if pending.peer != peer
                    || response.source_node_id != pending.remote_node_id
                    || response.target_node_id != self.local_peer_id().to_string()
                    || response.thread_id != pending.thread_id
                    || response.message_id != pending.message_id
                {
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: pending.kind,
                        error: "peer direct message response payload mismatch".to_owned(),
                    });
                }
                let Some(state_dir) = self.state_dir.clone() else {
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: pending.kind,
                        error:
                            "peer direct message response received before state_dir was configured"
                                .to_owned(),
                    });
                };
                if !response.applied {
                    save_dm_message(
                        &state_dir,
                        &pending.remote_node_id,
                        &pending.thread_id,
                        &pending.message_id,
                        pending.kind,
                        crate::control::PeerDmDirection::Outbound,
                        crate::control::PeerDmDeliveryState::Rejected,
                        &pending.a2a_protocol,
                        None,
                        json!({}),
                        None,
                        None,
                        None,
                    )?;
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: pending.kind,
                        error: response
                            .detail
                            .unwrap_or_else(|| "peer direct message rejected".to_owned()),
                    });
                }
                if let Some(contact_material) = &response.contact_material {
                    upsert_contact_material_for_peer(
                        &state_dir,
                        &pending.remote_node_id,
                        contact_material,
                    )?;
                }
                record_data_plane_status(
                    &state_dir,
                    "dm_message",
                    &pending.message_id,
                    Some(&pending.remote_node_id),
                    "libp2p_control",
                    "control_acknowledged",
                    None,
                )?;
                save_dm_message(
                    &state_dir,
                    &pending.remote_node_id,
                    &pending.thread_id,
                    &pending.message_id,
                    pending.kind,
                    crate::control::PeerDmDirection::Outbound,
                    crate::control::PeerDmDeliveryState::Delivered,
                    &pending.a2a_protocol,
                    None,
                    json!({}),
                    None,
                    None,
                    Some(response.updated_at),
                )?;
                let session_state = match pending.kind {
                    crate::control::PeerDmMessageKind::RelationshipEstablished => {
                        crate::control::PeerDmSessionState::Ready
                    }
                    crate::control::PeerDmMessageKind::SessionInit
                    | crate::control::PeerDmMessageKind::Message => {
                        crate::control::PeerDmSessionState::Ready
                    }
                };
                upsert_dm_thread(
                    &state_dir,
                    &pending.remote_node_id,
                    &pending.thread_id,
                    session_state,
                    if pending.kind == crate::control::PeerDmMessageKind::RelationshipEstablished {
                        Some(response.updated_at)
                    } else {
                        None
                    },
                    Some(response.updated_at),
                )?;
                if pending.kind == crate::control::PeerDmMessageKind::RelationshipEstablished {
                    self.record_dm_session_ready(
                        &state_dir,
                        &pending.remote_node_id,
                        &pending.thread_id,
                        crate::control::PeerDmDirection::Outbound,
                        &pending.a2a_protocol,
                        Some(response.updated_at),
                    )?;
                }
                Ok(NetworkBridgeTick::PeerDirectMessageUpdated {
                    peer,
                    kind: pending.kind,
                    delivery_state: crate::control::PeerDmDeliveryState::Delivered,
                })
            }
            NetworkRuntimeEvent::PeerDirectMessageOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                let Some(pending) = self.pending_dm_requests.remove(&request_id) else {
                    return Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                        peer,
                        kind: crate::control::PeerDmMessageKind::Message,
                        error,
                    });
                };
                if let Some(state_dir) = &self.state_dir {
                    let _ = save_dm_message(
                        state_dir,
                        &pending.remote_node_id,
                        &pending.thread_id,
                        &pending.message_id,
                        pending.kind,
                        crate::control::PeerDmDirection::Outbound,
                        crate::control::PeerDmDeliveryState::Rejected,
                        &pending.a2a_protocol,
                        None,
                        json!({}),
                        None,
                        None,
                        None,
                    );
                }
                Ok(NetworkBridgeTick::PeerDirectMessageFailed {
                    peer,
                    kind: pending.kind,
                    error,
                })
            }
            NetworkRuntimeEvent::PeerDirectMessageInboundFailure { peer, error } => {
                if error == "Timeout while receiving request or sending response" {
                    Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!("peer_direct_message_inbound_timeout peer={peer}"),
                    })
                } else {
                    Err(anyhow!(
                        "peer direct message inbound failure from {peer}: {error}"
                    ))
                }
            }
            NetworkRuntimeEvent::PeerRelationshipOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                let action = self
                    .pending_relationship_requests
                    .remove(&request_id)
                    .map(|pending| pending.action)
                    .unwrap_or(crate::control::PeerRelationshipAction::Request);
                Ok(NetworkBridgeTick::PeerRelationshipFailed {
                    peer,
                    action,
                    error,
                })
            }
            NetworkRuntimeEvent::PeerRelationshipInboundFailure { peer, error } => Err(anyhow!(
                "peer relationship inbound failure from {peer}: {error}"
            ))
            .or_else(|error| {
                let error_text = error.to_string();
                if error_text.contains("peer relationship inbound failure")
                    && error_text.contains("Timeout while receiving request or sending response")
                {
                    Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "peer_relationship_inbound_timeout peer={peer} reason={error_text}"
                        ),
                    })
                } else {
                    Err(error)
                }
            }),
        }
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
