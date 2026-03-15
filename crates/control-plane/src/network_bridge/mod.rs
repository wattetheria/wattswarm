mod announcements;
mod backfill;
mod publish;
mod scope;
mod summary;

#[cfg(test)]
mod tests;

use anyhow::{Result, anyhow, bail};

use crate::constants::BACKFILL_BATCH_EVENTS;
use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, EventEnvelope, GossipMessage, Multiaddr, NetworkP2pConfig,
    NetworkP2pNode, NetworkRuntime, NetworkRuntimeEvent, PeerHandshakeMetadata, PeerId,
    SummaryAnnouncement, SwarmScope,
};
use crate::node::Node;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
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
const DEFAULT_P2P_PORT: u16 = 4001;
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
    }
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

pub fn ingest_event_envelope(node: &mut Node, envelope: &EventEnvelope) -> Result<()> {
    node.ingest_remote(envelope.event.clone())
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

pub fn maybe_start_background_network_service(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> Result<bool> {
    if !network_enabled_from_env() {
        return Ok(false);
    }

    let config = network_config_from_env();
    let scopes = configured_network_scopes_from_env();
    config.validate()?;
    thread::spawn(move || {
        if let Err(err) = run_background_network_service(&state_dir, &db_path, config, scopes) {
            eprintln!("network bridge stopped: {err}");
        }
    });
    Ok(true)
}

fn run_background_network_service(
    state_dir: &Path,
    db_path: &Path,
    config: NetworkP2pConfig,
    configured_scopes: Vec<SwarmScope>,
) -> Result<()> {
    let bootstrap_peers = config.bootstrap_peers.clone();
    let mut node = crate::control::open_node(state_dir, db_path)?;
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
    config.identify_agent_version = PeerHandshakeMetadata {
        network_id: handshake_network_id,
        params_version: handshake_params_version,
        params_hash: handshake_params_hash,
    }
    .encode_agent_version();
    config.validate()?;
    let mut service =
        NetworkBridgeService::new(NetworkP2pNode::generate(config)?, &scopes, &protocol_params)?;
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
        let new_last_published_seq =
            publish_pending_scoped_updates(&mut service, &node, &node_id, last_published_seq)?;
        if new_last_published_seq != last_published_seq {
            did_work = true;
            last_published_seq = new_last_published_seq;
        }
        if service.run_anti_entropy(&node)? > 0 {
            did_work = true;
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
        })
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
        let mut requests_sent = 0usize;
        for scope in scopes.iter().cloned() {
            let from_event_seq = latest_scoped_event_seq(node, &scope)?;
            let _guard = self.tokio_runtime.enter();
            let _ = self.runtime.send_backfill_request(
                peer,
                BackfillRequest {
                    scope,
                    from_event_seq,
                    limit: BACKFILL_BATCH_EVENTS,
                },
            )?;
            requests_sent += 1;
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
        peers
            .iter()
            .copied()
            .find(|peer| self.peer_is_eligible_for_scope(peer, scope, now, true))
            .or_else(|| {
                peers
                    .into_iter()
                    .find(|peer| self.peer_is_eligible_for_scope(peer, scope, now, false))
            })
    }

    fn peer_is_eligible_for_scope(
        &self,
        peer: &PeerId,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> bool {
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
            NetworkRuntimeEvent::ConnectionEstablished { peer } => {
                self.mark_peer_connected(peer);
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
                    ingest_event_envelope(node, &envelope)?;
                    self.record_scope_event_ingested(&envelope.scope);
                    Ok(NetworkBridgeTick::EventIngested {
                        peer: propagation_source,
                        event_id: envelope.event.event_id.clone(),
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
                    &request,
                    self.runtime.config().max_backfill_events,
                    self.runtime.config().max_backfill_events_hard_limit,
                )?;
                let events = response.events.len();
                self.runtime.send_backfill_response(channel, response)?;
                Ok(NetworkBridgeTick::BackfillServed { peer, events })
            }
            NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response,
            } => {
                self.record_peer_scope_activity(peer, &response.scope);
                self.mark_backfill_completed(peer);
                let events = ingest_backfill_response(node, &response)?;
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
                Err(anyhow!("backfill inbound failure from {peer}: {error}"))
            }
        }
    }
}
