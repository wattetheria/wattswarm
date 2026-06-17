mod agent_delivery;
mod announcements;
mod backfill;
mod bootstrap_contact;
mod diagnostics;
mod discovery_bootnode;
mod event_relevance;
mod peer_interactions;
mod publish;
mod scope;
mod service_dispatch;
mod service_loop;
mod service_ops;
mod service_runtime;
mod summary;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result, anyhow, bail};

use crate::constants::BACKFILL_BATCH_EVENTS;
use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, BackfillResponse, ContactMaterialRequestId, EventEnvelope,
    GossipKind, GossipMessage, NetworkAddress, NetworkNodeId, NetworkP2pConfig, NetworkP2pNode,
    NetworkRuntime, NetworkRuntimeEvent, PeerHandshakeMetadata, PeerRelationshipRequest,
    PeerRelationshipRequestId, PeerRelationshipResponse, RawAgentEnvelope, RawContactMaterial,
    RawContactMaterialRequest, RawContactMaterialResponse, RawPeerRelationshipAction,
    RawSourceAgentCard, SummaryAnnouncement, SwarmScope,
};
use crate::node::Node;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use uuid::Uuid;
use wattswarm_network_discovery::{DiscoveryGeo, SignedDiscoveryNodeRecord};
use wattswarm_network_transport_core::{
    PeerTransportCapabilities, TransferIntent, TransferKind, TransportContactMaterial,
    TransportMetadata, TransportRoute as DataTransportRoute, TransportRouter,
};
use wattswarm_network_transport_iroh::{
    export_local_contact_material_for_network_peer_id, local_endpoint_id_from_state_dir,
};
use wattswarm_protocol::types::NetworkProtocolParams;

pub use announcements::{
    apply_checkpoint_announcement, apply_rule_announcement, sign_rule_announcement,
};
pub use backfill::{backfill_response_for_request, ingest_backfill_response};
pub use diagnostics::{
    DiagnosticEntry, DiagnosticFilter, list_diagnostics as list_network_diagnostics,
};
pub use peer_interactions::{
    default_agent_envelope, enqueue_agent_payment_command,
    enqueue_peer_relationship_action_command, verified_agent_context_for_source,
};
pub use publish::{publish_pending_global_events, publish_pending_scoped_updates};
pub use service_loop::{
    PostTickHook, configured_network_scopes_from_env, latest_connected_peer_ids,
    latest_network_observability_snapshot, maybe_start_background_network_service,
    maybe_start_background_network_service_with_hook, network_config_from_env,
    network_config_from_state_dir, network_enabled_from_env, network_service_started,
    network_service_status,
};
pub use summary::{
    apply_summary_announcement, build_knowledge_summary_for_task_type,
    build_reputation_summary_for_runtime,
};

#[cfg(test)]
use agent_delivery::{build_agent_event, topic_message_requires_reply};
use agent_delivery::{
    build_agent_event_with_agent_envelope, deliver_agent_event_to_local_executor,
    task_claim_agent_event, task_claim_decision_agent_event, task_completion_decision_agent_event,
    task_result_agent_event, task_settled_agent_event, topic_message_agent_event,
};
use backfill::{
    BACKFILL_KNOWN_EVENT_IDS_LIMIT, ingest_backfill_response_events,
    recent_backfill_lane_event_ids, should_publish_summaries, should_sync_event,
};
use bootstrap_contact::{
    build_contact_material, candidate_peer_addrs, iroh_contact_network_peer_id,
    raw_contact_material_private_message_key_len, upsert_contact_material_for_peer,
};
pub use bootstrap_contact::{
    export_local_bootstrap_contact, export_local_bootstrap_contact_json, validate_bootstrap_contact,
};
use discovery_bootnode::{
    apply_discovery_bootnode_record, discovery_bootnode_settings_from_state_dir,
    query_discovery_bootnodes_for_candidate_records,
};
#[cfg(test)]
use discovery_bootnode::{
    discovery_bootnode_failure_log_decision, reset_discovery_bootnode_failure_log_state,
};
use event_relevance::EventRelevanceFilter;
#[cfg(test)]
use peer_interactions::payment_allowed_actions;
use peer_interactions::{
    PendingContactMaterialRequest, PendingPeerRelationshipRequest,
    attach_agent_envelope_to_relationship, control_peer_relationship_action,
    optional_verified_agent_context_for_protocol_source, payload_with_verified_agent_context,
    peer_dm_thread_id, process_pending_network_commands, raw_agent_envelope_to_protocol,
    record_peer_relationship_action_command_failure, remove_peer_relationship_action_command,
    save_agent_payment_event, save_agent_payment_summary, save_dm_message,
    save_inbound_private_dm_topic_message, upsert_dm_thread,
    verify_agent_envelope_signature_for_source, verify_protocol_agent_envelope_for_source,
    wire_peer_relationship_action,
};
use publish::GlobalPublishRateGuard;
use scope::{
    dynamic_subscription_scope_kinds_for_node, event_matches_signed_scope, event_transport_route,
    feed_subscription_target_scope, merge_scopes, node_has_active_subscription_scope_kinds,
};
#[cfg(test)]
use service_loop::{
    clear_latest_network_observability_snapshot, store_latest_network_observability_snapshot,
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
const MAX_INFLIGHT_BACKFILLS_PER_PEER: usize = 8;
const BACKFILL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const BACKFILL_LANE_COOLDOWN_INITIAL: Duration = BACKFILL_REQUEST_TIMEOUT;
const BACKFILL_LANE_COOLDOWN_MAX: Duration = Duration::from_secs(300);
const BACKFILL_PEER_COOLDOWN_INITIAL: Duration = BACKFILL_REQUEST_TIMEOUT;
const BACKFILL_PEER_COOLDOWN_MAX: Duration = Duration::from_secs(300);
const PEER_LAST_SEEN_TTL: Duration = Duration::from_secs(180);
const SUMMARY_BACKPRESSURE_HIGH_WATERMARK: u64 = 256;
const IDLE_NETWORK_SLEEP: Duration = Duration::from_millis(50);
const ANNOUNCED_PEER_TTL: Duration = Duration::from_secs(60 * 60);
const PEER_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const PEER_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(60);
const PEER_RECONNECT_MAX_ATTEMPTS: u32 = 10;

const ENV_P2P_ENABLED: &str = "WATTSWARM_P2P_ENABLED";
const ENV_P2P_LOCAL_DISCOVERY: &str = "WATTSWARM_P2P_MDNS";
const ENV_P2P_PORT: &str = "WATTSWARM_P2P_PORT";
const ENV_P2P_LISTEN_ADDRS: &str = "WATTSWARM_P2P_LISTEN_ADDRS";
const STARTUP_CONFIG_FILE: &str = "startup_config.json";
const DEFAULT_P2P_PORT: u16 = 4001;
const DEFAULT_DISCOVERY_GEO_RADIUS_KM: f64 = 1000.0;
const DEFAULT_DISCOVERY_BOOTNODE_QUERY_INTERVAL: Duration = Duration::from_secs(30);
const DISCOVERY_BOOTNODE_QUERY_LIMIT: usize = 50;
const DISCOVERY_BOOTNODE_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_BOOTNODE_FAILURE_LOG_EVERY: u64 = 10;
const DISCOVERY_NODE_CAPABILITY: &str = "wattswarm.node";
const GLOBAL_HIGH_FREQUENCY_WINDOW: Duration = Duration::from_secs(5);
const GLOBAL_HIGH_FREQUENCY_LIMIT: usize = 32;
const DEFAULT_NETWORK_CONTEXT_ID: &str = "default";
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
        crate::types::EventPayload::AgentPaymentPosted(payload) => Some(&payload.network_id),
        _ => None,
    }
}

fn event_diagnostic_details(event: &crate::types::Event) -> Map<String, Value> {
    let mut details = Map::new();
    details.insert(
        "event_kind".to_owned(),
        json!(format!("{:?}", event.event_kind)),
    );
    details.insert("author_node_id".to_owned(), json!(event.author_node_id));
    details.insert("created_at".to_owned(), json!(event.created_at));
    details.insert(
        "payload_kind".to_owned(),
        json!(format!("{:?}", event.payload.kind())),
    );
    if let Some(agent_envelope) = event_payload_agent_envelope(&event.payload) {
        details.insert("agent_envelope".to_owned(), agent_envelope);
    }
    if let crate::types::EventPayload::FeedSubscriptionUpdated(payload) = &event.payload {
        details.insert("target_scope_hint".to_owned(), json!(payload.scope_hint));
        details.insert("feed_key".to_owned(), json!(payload.feed_key));
        details.insert(
            "subscriber_node_id".to_owned(),
            json!(payload.subscriber_node_id),
        );
        details.insert("gossip_kinds".to_owned(), json!(payload.gossip_kinds));
        details.insert("active".to_owned(), json!(payload.active));
    }
    details
}

fn event_payload_agent_envelope(payload: &crate::types::EventPayload) -> Option<Value> {
    match payload {
        crate::types::EventPayload::TaskClaimed(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskClaimDecided(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskCompleted(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskCompletionDecided(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskSettled(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::CandidateProposed(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::DecisionFinalized(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskError(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskRetryScheduled(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::FeedSubscriptionUpdated(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TaskAnnounced(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope)),
        crate::types::EventPayload::TopicMessagePosted(payload) => payload
            .agent_envelope
            .as_ref()
            .map(|envelope| json!(envelope))
            .or_else(|| {
                payload
                    .local_content_cache
                    .as_ref()
                    .and_then(|content| content.get("agent_envelope").cloned())
            }),
        crate::types::EventPayload::AgentPaymentPosted(payload) => {
            Some(json!(payload.agent_envelope))
        }
        _ => None,
    }
}

fn summary_diagnostic_details(summary: &SummaryAnnouncement) -> Map<String, Value> {
    let mut details = Map::new();
    details.insert("summary_id".to_owned(), json!(summary.summary_id));
    details.insert("source_node_id".to_owned(), json!(summary.source_node_id));
    details.insert("summary_kind".to_owned(), json!(summary.summary_kind));
    details.insert("scope".to_owned(), json!(summary.scope));
    if let Some(artifact_path) = &summary.artifact_path {
        details.insert("artifact_path".to_owned(), json!(artifact_path));
    }
    if let Some(agent_envelope) = summary.payload.get("agent_envelope") {
        details.insert("agent_envelope".to_owned(), agent_envelope.clone());
    }
    details
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

fn maybe_record_topic_cursor(
    node: &Node,
    subscriber_node_id: &str,
    feed_key: &str,
    scope: &SwarmScope,
    last_event_seq: u64,
    updated_at: u64,
) -> Result<()> {
    let network_id = current_network_context_id(node);
    let Some(subscription) =
        node.store
            .get_feed_subscription(&network_id, subscriber_node_id, feed_key)?
    else {
        return Ok(());
    };
    if !subscription.active || subscription.scope_hint != scope_hint_label(scope) {
        return Ok(());
    }
    node.store.upsert_topic_cursor(
        &network_id,
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
    let Some(last_event_id) = response
        .events
        .iter()
        .rev()
        .find(|envelope| {
            envelope.scope == response.scope
                && matches!(
                    &envelope.event.payload,
                    crate::types::EventPayload::TopicMessagePosted(payload)
                        if payload.feed_key == *feed_key
                )
        })
        .map(|envelope| envelope.event.event_id.as_str())
    else {
        return Ok(());
    };
    let Some(last_event_seq) = node.store.event_seq_for_event_id(last_event_id)? else {
        return Ok(());
    };
    maybe_record_topic_cursor(
        node,
        subscriber_node_id,
        feed_key,
        &response.scope,
        last_event_seq,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct BackfillLaneKey {
    scope: SwarmScope,
    feed_key: Option<String>,
}

impl BackfillLaneKey {
    fn new(scope: &SwarmScope, feed_key: Option<&str>) -> Self {
        Self {
            scope: scope.clone(),
            feed_key: feed_key.map(ToOwned::to_owned),
        }
    }
}

#[derive(Debug, Clone)]
struct PeerSyncState {
    last_seen_at: Instant,
    last_backfill_request_at: Option<Instant>,
    next_retry_at: Instant,
    known_scopes: HashSet<SwarmScope>,
    backfill_cursors: HashMap<BackfillLaneKey, u64>,
    remote_head_event_ids: HashMap<BackfillLaneKey, Vec<String>>,
    backfill_lane_health: HashMap<BackfillLaneKey, BackfillCooldownHealth>,
    backfill_peer_health: BackfillCooldownHealth,
    inflight_backfill_requests: HashMap<BackfillRequestId, PendingBackfillRequest>,
    backfill_successes: u64,
    backfill_failures: u64,
}

#[derive(Debug, Clone)]
struct BackfillCooldownHealth {
    cooldown_until: Option<Instant>,
    cooldown: Duration,
    recovery_probe_inflight: bool,
}

impl BackfillCooldownHealth {
    fn new() -> Self {
        Self {
            cooldown_until: None,
            cooldown: Duration::ZERO,
            recovery_probe_inflight: false,
        }
    }

    fn can_request_at(&self, now: Instant) -> bool {
        if self.cooldown_until.is_some_and(|until| until > now) {
            return false;
        }
        !self.recovery_probe_inflight
    }

    fn claim_request_at(&mut self, now: Instant) {
        if self.cooldown_until.is_some_and(|until| until <= now) {
            self.recovery_probe_inflight = true;
        }
    }

    fn record_timeout_at(
        &mut self,
        now: Instant,
        initial: Duration,
        max: Duration,
    ) -> BackfillCooldownEvent {
        let was_recovery_probe = self.recovery_probe_inflight;
        let previous = self.cooldown;
        let cooldown = if previous.is_zero() {
            initial
        } else {
            previous.checked_mul(2).unwrap_or(max).min(max)
        };
        self.cooldown = cooldown;
        self.cooldown_until = Some(now + cooldown);
        self.recovery_probe_inflight = false;
        BackfillCooldownEvent {
            cooldown,
            was_recovery_probe,
        }
    }

    fn clear_recovery_probe(&mut self) {
        self.recovery_probe_inflight = false;
    }

    fn clear(&mut self) {
        self.cooldown_until = None;
        self.cooldown = Duration::ZERO;
        self.recovery_probe_inflight = false;
    }
}

#[derive(Debug, Clone, Copy)]
struct BackfillCooldownEvent {
    cooldown: Duration,
    was_recovery_probe: bool,
}

#[derive(Debug, Clone)]
struct PendingBackfillRequest {
    scope: SwarmScope,
    feed_key: Option<String>,
    sent_at: Instant,
}

impl PendingBackfillRequest {
    fn lane_key(&self) -> BackfillLaneKey {
        BackfillLaneKey::new(&self.scope, self.feed_key.as_deref())
    }
}

#[derive(Debug, Clone)]
struct PeerReconnectState {
    attempts: u32,
    next_attempt_at: Instant,
}

impl PeerReconnectState {
    fn ready_now() -> Self {
        Self {
            attempts: 0,
            next_attempt_at: Instant::now(),
        }
    }

    fn schedule_next_attempt(&mut self, now: Instant) {
        self.attempts = self.attempts.saturating_add(1);
        let delay_secs = PEER_RECONNECT_INITIAL_DELAY
            .as_secs()
            .saturating_mul(2_u64.saturating_pow(self.attempts.saturating_sub(1)))
            .min(PEER_RECONNECT_MAX_DELAY.as_secs());
        self.next_attempt_at = now + Duration::from_secs(delay_secs);
    }

    fn should_abandon(&self) -> bool {
        self.attempts >= PEER_RECONNECT_MAX_ATTEMPTS
    }
}

impl PeerSyncState {
    fn new(now: Instant) -> Self {
        Self {
            last_seen_at: now,
            last_backfill_request_at: None,
            next_retry_at: now,
            known_scopes: HashSet::new(),
            backfill_cursors: HashMap::new(),
            remote_head_event_ids: HashMap::new(),
            backfill_lane_health: HashMap::new(),
            backfill_peer_health: BackfillCooldownHealth::new(),
            inflight_backfill_requests: HashMap::new(),
            backfill_successes: 0,
            backfill_failures: 0,
        }
    }

    fn record_seen_at(&mut self, now: Instant) {
        self.last_seen_at = now;
    }

    fn recently_seen_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_seen_at) <= PEER_LAST_SEEN_TTL
    }

    fn inflight_backfills(&self) -> usize {
        self.inflight_backfill_requests.len()
    }

    fn record_pending_backfill(
        &mut self,
        request_id: BackfillRequestId,
        scope: SwarmScope,
        feed_key: Option<String>,
        sent_at: Instant,
    ) {
        self.backfill_peer_health.claim_request_at(sent_at);
        let lane = BackfillLaneKey::new(&scope, feed_key.as_deref());
        if let Some(health) = self.backfill_lane_health.get_mut(&lane) {
            health.claim_request_at(sent_at);
        }
        self.inflight_backfill_requests.insert(
            request_id,
            PendingBackfillRequest {
                scope,
                feed_key,
                sent_at,
            },
        );
    }

    fn backfill_peer_available_at(&self, now: Instant) -> bool {
        self.backfill_peer_health.can_request_at(now)
    }

    fn backfill_lane_available_at(
        &self,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        now: Instant,
    ) -> bool {
        self.backfill_lane_health
            .get(&BackfillLaneKey::new(scope, feed_key))
            .is_none_or(|health| health.can_request_at(now))
    }

    fn record_backfill_request_completed(&mut self, request_id: BackfillRequestId) -> bool {
        let Some(pending) = self.inflight_backfill_requests.remove(&request_id) else {
            return false;
        };
        self.backfill_lane_health.remove(&pending.lane_key());
        self.backfill_peer_health.clear();
        true
    }

    fn record_backfill_request_failed_at(
        &mut self,
        request_id: BackfillRequestId,
        now: Instant,
    ) -> Option<BackfillCooldownEvent> {
        let pending = self.inflight_backfill_requests.remove(&request_id)?;
        let lane = pending.lane_key();
        Some(
            self.backfill_lane_health
                .entry(lane)
                .or_insert_with(BackfillCooldownHealth::new)
                .record_timeout_at(
                    now,
                    BACKFILL_LANE_COOLDOWN_INITIAL,
                    BACKFILL_LANE_COOLDOWN_MAX,
                ),
        )
    }

    fn record_peer_backfill_timeout_at(&mut self, now: Instant) -> BackfillCooldownEvent {
        self.backfill_peer_health.record_timeout_at(
            now,
            BACKFILL_PEER_COOLDOWN_INITIAL,
            BACKFILL_PEER_COOLDOWN_MAX,
        )
    }

    fn clear_backfill_recovery_probes(&mut self) {
        self.backfill_peer_health.clear_recovery_probe();
        for health in self.backfill_lane_health.values_mut() {
            health.clear_recovery_probe();
        }
    }

    fn backfill_cursor(&self, scope: &SwarmScope, feed_key: Option<&str>) -> u64 {
        self.backfill_cursors
            .get(&BackfillLaneKey::new(scope, feed_key))
            .copied()
            .unwrap_or(0)
    }

    fn record_backfill_cursor(
        &mut self,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        next_from_event_seq: u64,
    ) {
        let key = BackfillLaneKey::new(scope, feed_key);
        let cursor = self.backfill_cursors.entry(key).or_insert(0);
        *cursor = (*cursor).max(next_from_event_seq);
    }

    fn reset_backfill_cursor(&mut self, scope: &SwarmScope, feed_key: Option<&str>) {
        self.backfill_cursors
            .insert(BackfillLaneKey::new(scope, feed_key), 0);
    }

    fn record_remote_head_event_ids(
        &mut self,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        head_event_ids: &[String],
    ) {
        self.remote_head_event_ids.insert(
            BackfillLaneKey::new(scope, feed_key),
            head_event_ids.to_vec(),
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedBackfillCursorRecord {
    lane: BackfillLaneKey,
    cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedBackfillRemoteHeadRecord {
    lane: BackfillLaneKey,
    head_event_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyPeerSyncStateRecord {
    peer_id: String,
    #[serde(default)]
    known_scopes: Vec<SwarmScope>,
    #[serde(default)]
    backfill_cursors: Vec<PersistedBackfillCursorRecord>,
    #[serde(default)]
    remote_heads: Vec<PersistedBackfillRemoteHeadRecord>,
    #[serde(default)]
    backfill_successes: u64,
    #[serde(default)]
    backfill_failures: u64,
}

fn legacy_peer_sync_state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("network_peer_sync_state.json")
}

fn recommended_backfill_route_for_peer(
    state_dir: &Path,
    peer: &NetworkNodeId,
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
    if !event_matches_signed_scope(&envelope.event, &envelope.scope) {
        return Err(anyhow!("event envelope scope does not match signed scope"));
    }
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
/// - `TaskClaimed`, `TaskCompleted`, and `CandidateProposed` for any task ID →
///   `pending_stigmergy_contributions.jsonl` (open stigmergy runs are matched
///   later by `runs.market_task_id`, not by a task ID prefix)
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
        crate::types::EventPayload::TaskClaimed(payload) => {
            let agent_id = payload
                .agent_envelope
                .as_ref()
                .and_then(|envelope| envelope.source_agent_id.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(&payload.claimer_node_id);
            let entry = serde_json::json!({
                "kind": "claim",
                "task_id": payload.task_id,
                "agent_id": agent_id,
                "author_node_id": event.author_node_id,
                "claimer_node_id": payload.claimer_node_id,
                "execution_id": payload.execution_id,
                "received_at": chrono::Utc::now().timestamp_millis(),
            });
            append_jsonl(state_dir, "pending_stigmergy_contributions.jsonl", &entry);
        }
        crate::types::EventPayload::TaskCompleted(payload) => {
            let agent_id = payload
                .agent_envelope
                .as_ref()
                .and_then(|envelope| envelope.source_agent_id.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(&payload.completed_by_node_id);
            let entry = serde_json::json!({
                "kind": "completion",
                "task_id": payload.task_id,
                "agent_id": agent_id,
                "author_node_id": event.author_node_id,
                "completed_by_node_id": payload.completed_by_node_id,
                "execution_id": payload.execution_id,
                "output": payload.output,
                "received_at": chrono::Utc::now().timestamp_millis(),
            });
            append_jsonl(state_dir, "pending_stigmergy_contributions.jsonl", &entry);
        }
        crate::types::EventPayload::CandidateProposed(payload) => {
            let candidate_output = node
                .store
                .get_candidate_by_id(&payload.task_id, &payload.candidate.candidate_id)
                .ok()
                .flatten()
                .map(|candidate| candidate.output)
                .unwrap_or(Value::Null);
            let agent_id = payload
                .agent_envelope
                .as_ref()
                .and_then(|envelope| envelope.source_agent_id.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(&event.author_node_id);
            let stigmergy_entry = serde_json::json!({
                "kind": "candidate",
                "task_id": payload.task_id,
                "agent_id": agent_id,
                "candidate_id": payload.candidate.candidate_id,
                "candidate_output": candidate_output.clone(),
                "author_node_id": event.author_node_id,
                "execution_id": payload.candidate.execution_id,
                "received_at": chrono::Utc::now().timestamp_millis(),
            });
            append_jsonl(
                state_dir,
                "pending_stigmergy_contributions.jsonl",
                &stigmergy_entry,
            );
            if !payload.task_id.starts_with("run-") {
                return;
            }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBridgeTick {
    Listening {
        address: NetworkAddress,
    },
    TransportNotice {
        detail: String,
    },
    Connected {
        peer: NetworkNodeId,
    },
    Disconnected {
        peer: NetworkNodeId,
    },
    EventIngested {
        peer: NetworkNodeId,
        event_id: String,
    },
    SummaryApplied {
        peer: NetworkNodeId,
        summary_kind: String,
    },
    RuleApplied {
        peer: NetworkNodeId,
        rule_set: String,
        rule_version: u64,
    },
    CheckpointApplied {
        peer: NetworkNodeId,
        checkpoint_id: String,
    },
    GossipIgnored {
        peer: NetworkNodeId,
        message_kind: String,
    },
    BackfillServed {
        peer: NetworkNodeId,
        events: usize,
    },
    BackfillApplied {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        events: usize,
    },
    BackfillFailed {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        error: String,
    },
    PeerRelationshipUpdated {
        peer: NetworkNodeId,
        action: crate::control::PeerRelationshipAction,
        relationship_state: crate::control::PeerRelationshipState,
        initiated_by: crate::control::PeerRelationshipInitiator,
    },
    PeerRelationshipFailed {
        peer: NetworkNodeId,
        action: crate::control::PeerRelationshipAction,
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
    pub network_peer_id: String,
    pub connected: bool,
    pub recently_seen: bool,
    pub stale: bool,
    pub last_seen_age_ms: Option<u64>,
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
    pub local_network_peer_id: String,
    pub local_endpoint_addrs: Vec<String>,
    pub p2p_foundation: String,
    pub local_iroh_endpoint_id: Option<String>,
    pub subscribed_iroh_gossip_topics: Vec<String>,
    pub known_iroh_contacts: usize,
    pub legacy_transport_active: bool,
    pub subscribed_scopes: Vec<String>,
    pub connected_peer_count: usize,
    pub nat_status: String,
    pub nat_public_address: Option<String>,
    pub nat_confidence: u32,
    pub relay_reservations: Vec<String>,
    pub peer_health: Vec<NetworkBridgePeerHealth>,
    pub scope_traffic: Vec<NetworkBridgeScopeTraffic>,
    pub dropped_malformed_gossip: u64,
    pub invalid_control_payloads: u64,
    pub dial_failures: u64,
    pub response_validation_failures: u64,
    pub retry_suppressed_dials: u64,
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
    peer_sync_state: HashMap<NetworkNodeId, PeerSyncState>,
    connected_peers: HashSet<NetworkNodeId>,
    global_backfill_providers: HashSet<NetworkNodeId>,
    known_peer_addrs: HashMap<NetworkNodeId, NetworkAddress>,
    peer_reconnect_state: HashMap<NetworkNodeId, PeerReconnectState>,
    abandoned_reconnect_peers: HashSet<NetworkNodeId>,
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
        let global_backfill_providers = node
            .config()
            .parse_bootstrap_peers()?
            .into_iter()
            .map(|(peer, _)| peer)
            .collect::<HashSet<_>>();
        let mut runtime = NetworkRuntime::new(node)?;
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
        let tokio_runtime = Runtime::new()?;
        Ok(Self {
            runtime,
            tokio_runtime,
            pinned_scopes: subscribed_scopes.clone(),
            subscribed_scopes,
            subscribed_scope_kinds,
            peer_sync_state: HashMap::new(),
            connected_peers: HashSet::new(),
            global_backfill_providers,
            known_peer_addrs: HashMap::new(),
            peer_reconnect_state: HashMap::new(),
            abandoned_reconnect_peers: HashSet::new(),
            global_publish_rate_guard: GlobalPublishRateGuard::new(Instant::now()),
            anti_entropy_interval: Duration::from_secs(protocol_params.anti_entropy_interval_secs),
            backfill_retry_after: Duration::from_secs(protocol_params.backfill_retry_after_secs),
            summary_reputation_limit: protocol_params.summary_reputation_limit,
            summary_decision_memory_limit: protocol_params.summary_decision_memory_limit,
            scope_traffic: HashMap::new(),
            pending_contact_material_requests: HashMap::new(),
            pending_relationship_requests: HashMap::new(),
            state_dir: None,
            db_path: None,
        })
    }

    /// Set the local persistence paths for run-queue and agent-event hooks.
    pub fn set_state_dir(&mut self, state_dir: PathBuf, db_path: PathBuf) {
        if let Err(err) = self.load_peer_sync_state(&state_dir) {
            eprintln!(
                "peer sync state load failed for {}: {err}",
                state_dir.display()
            );
        }
        if let Err(err) = self.load_startup_bootstrap_contacts(&state_dir) {
            eprintln!(
                "startup bootstrap contact load failed for {}: {err:#}",
                state_dir.display()
            );
        }
        if let Err(err) = self.load_iroh_contact_material(&state_dir) {
            eprintln!(
                "iroh contact material load failed for {}: {err}",
                state_dir.display()
            );
        }
        self.state_dir = Some(state_dir);
        self.db_path = Some(db_path);
    }

    fn load_peer_sync_state(&mut self, state_dir: &Path) -> Result<()> {
        let mut records = crate::control::load_network_peer_sync_state_records_state(state_dir)?;
        if records.is_empty() {
            records = migrate_legacy_peer_sync_state_records(state_dir)?;
        }
        let now = Instant::now();
        for record in records {
            let peer = match record.network_peer_id.parse::<NetworkNodeId>() {
                Ok(peer) => peer,
                Err(_) => continue,
            };
            let mut state = PeerSyncState::new(now);
            state.known_scopes = serde_json::from_str::<Vec<SwarmScope>>(&record.known_scopes_json)
                .unwrap_or_default()
                .into_iter()
                .collect();
            for cursor in serde_json::from_str::<Vec<PersistedBackfillCursorRecord>>(
                &record.backfill_cursors_json,
            )
            .unwrap_or_default()
            {
                state.backfill_cursors.insert(cursor.lane, cursor.cursor);
            }
            for head in serde_json::from_str::<Vec<PersistedBackfillRemoteHeadRecord>>(
                &record.remote_heads_json,
            )
            .unwrap_or_default()
            {
                state
                    .remote_head_event_ids
                    .insert(head.lane, head.head_event_ids);
            }
            state.backfill_successes = record.backfill_successes;
            state.backfill_failures = record.backfill_failures;
            self.peer_sync_state.insert(peer, state);
        }
        Ok(())
    }

    fn persist_peer_sync_state(&self) {
        let Some(state_dir) = &self.state_dir else {
            return;
        };
        let updated_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let mut records = self
            .peer_sync_state
            .iter()
            .filter_map(|(peer, state)| peer_sync_state_record(peer, state, updated_at))
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.network_peer_id.cmp(&right.network_peer_id));
        for record in records {
            if let Err(err) =
                crate::control::save_network_peer_sync_state_record_state(state_dir, &record)
            {
                eprintln!(
                    "peer sync state DB write failed for {}: {err}",
                    record.network_peer_id
                );
            }
        }
    }

    pub fn local_peer_id(&self) -> NetworkNodeId {
        self.runtime.local_peer_id()
    }

    pub fn listen_addrs(&self) -> &[NetworkAddress] {
        self.runtime.listen_addrs()
    }

    pub fn upsert_remote_contact_material(
        &mut self,
        remote_network_peer_id: impl Into<String>,
        contact: TransportContactMaterial,
    ) -> Result<bool> {
        let remote_network_peer_id = remote_network_peer_id.into();
        let peer = NetworkNodeId::new(remote_network_peer_id.clone())?;
        let updated = self
            .runtime
            .upsert_remote_contact_material(remote_network_peer_id, contact.clone())?;
        self.remember_peer_address_from_contact(peer.as_str(), &contact);
        self.schedule_peer_reconnect(peer);
        Ok(updated)
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

    #[cfg(test)]
    pub fn known_remote_contact_count(&self) -> usize {
        self.runtime.known_remote_contact_count()
    }

    #[cfg(test)]
    pub fn reconnect_attempts_for_peer(&self, peer: &NetworkNodeId) -> Option<u32> {
        self.peer_reconnect_state
            .get(peer)
            .map(|state| state.attempts)
    }

    #[cfg(test)]
    pub fn reconnect_abandoned_for_peer(&self, peer: &NetworkNodeId) -> bool {
        self.abandoned_reconnect_peers.contains(peer)
    }

    #[cfg(test)]
    pub fn pending_contact_material_request_count_for_peer(&self, peer: &NetworkNodeId) -> usize {
        self.pending_contact_material_requests
            .values()
            .filter(|pending| &pending.peer == peer)
            .count()
    }

    #[cfg(test)]
    pub fn force_reconnect_due_for_peer(&mut self, peer: &NetworkNodeId) {
        if let Some(state) = self.peer_reconnect_state.get_mut(peer) {
            state.next_attempt_at = Instant::now();
        }
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
        if let Some(subscribed) = self.subscribed_scope_kinds.get_mut(scope) {
            for kind in kinds {
                subscribed.remove(kind);
            }
            if subscribed.is_empty() {
                self.subscribed_scope_kinds.remove(scope);
                self.subscribed_scopes.retain(|existing| existing != scope);
            }
        }
        self.runtime.unsubscribe_scope_kinds(scope, kinds)?;
        Ok(())
    }

    pub fn dial(&mut self, addr: NetworkAddress) -> Result<()> {
        self.runtime.dial(addr)
    }
}

fn peer_sync_state_record(
    peer: &NetworkNodeId,
    state: &PeerSyncState,
    updated_at: u64,
) -> Option<crate::control::NetworkPeerSyncStateRecord> {
    let mut known_scopes = state.known_scopes.iter().cloned().collect::<Vec<_>>();
    known_scopes.sort_by_key(scope_hint_label);
    let mut backfill_cursors = state
        .backfill_cursors
        .iter()
        .map(|(lane, cursor)| PersistedBackfillCursorRecord {
            lane: lane.clone(),
            cursor: *cursor,
        })
        .collect::<Vec<_>>();
    backfill_cursors.sort_by_key(|entry| {
        (
            scope_hint_label(&entry.lane.scope),
            entry.lane.feed_key.clone(),
        )
    });
    let mut remote_heads = state
        .remote_head_event_ids
        .iter()
        .map(|(lane, head_event_ids)| PersistedBackfillRemoteHeadRecord {
            lane: lane.clone(),
            head_event_ids: head_event_ids.clone(),
        })
        .collect::<Vec<_>>();
    remote_heads.sort_by_key(|entry| {
        (
            scope_hint_label(&entry.lane.scope),
            entry.lane.feed_key.clone(),
        )
    });
    let known_scopes_json = match serde_json::to_string(&known_scopes) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("peer sync known scopes encode failed for {peer}: {err}");
            return None;
        }
    };
    let backfill_cursors_json = match serde_json::to_string(&backfill_cursors) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("peer sync cursors encode failed for {peer}: {err}");
            return None;
        }
    };
    let remote_heads_json = match serde_json::to_string(&remote_heads) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("peer sync remote heads encode failed for {peer}: {err}");
            return None;
        }
    };
    Some(crate::control::NetworkPeerSyncStateRecord {
        network_peer_id: peer.to_string(),
        known_scopes_json,
        backfill_cursors_json,
        remote_heads_json,
        backfill_successes: state.backfill_successes,
        backfill_failures: state.backfill_failures,
        updated_at,
    })
}

fn migrate_legacy_peer_sync_state_records(
    state_dir: &Path,
) -> Result<Vec<crate::control::NetworkPeerSyncStateRecord>> {
    let path = legacy_peer_sync_state_path(state_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("read legacy peer sync state from {}", path.display()))?;
    if raw.trim().is_empty() {
        let _ = fs::remove_file(&path);
        return Ok(Vec::new());
    }
    let legacy: Vec<LegacyPeerSyncStateRecord> = serde_json::from_str(&raw)
        .with_context(|| format!("parse legacy peer sync state from {}", path.display()))?;
    let updated_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let mut migrated = Vec::new();
    for record in legacy {
        let Ok(peer) = record.peer_id.parse::<NetworkNodeId>() else {
            continue;
        };
        let mut state = PeerSyncState::new(Instant::now());
        state.known_scopes = record.known_scopes.into_iter().collect();
        for cursor in record.backfill_cursors {
            state.backfill_cursors.insert(cursor.lane, cursor.cursor);
        }
        for head in record.remote_heads {
            state
                .remote_head_event_ids
                .insert(head.lane, head.head_event_ids);
        }
        state.backfill_successes = record.backfill_successes;
        state.backfill_failures = record.backfill_failures;
        let Some(row) = peer_sync_state_record(&peer, &state, updated_at) else {
            continue;
        };
        crate::control::save_network_peer_sync_state_record_state(state_dir, &row)?;
        migrated.push(row);
    }
    if !migrated.is_empty() {
        let _ = fs::remove_file(&path);
    }
    Ok(migrated)
}
