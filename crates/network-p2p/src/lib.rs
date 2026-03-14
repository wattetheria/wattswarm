use anyhow::{Result, anyhow, bail};
use libp2p::connection_limits;
use libp2p::futures::{FutureExt, StreamExt};
use libp2p::gossipsub::{
    Behaviour as Gossipsub, ConfigBuilder as GossipsubConfigBuilder, Event as GossipsubEvent,
};
use libp2p::gossipsub::{IdentTopic, MessageAuthenticity, ValidationMode};
use libp2p::identify;
use libp2p::kad::{self, store::MemoryStore};
use libp2p::mdns;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{self, Message as RequestResponseMessage, ProtocolSupport};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent, behaviour::toggle::Toggle};
pub use libp2p::{Multiaddr, PeerId};
use libp2p::{
    StreamProtocol, Swarm, SwarmBuilder, autonat, dcutr, identity, noise, relay, tcp, yamux,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};
use wattswarm_protocol::types::Event;

const DEFAULT_NAMESPACE: &str = "wattswarm";
const DEFAULT_PROTOCOL_VERSION: &str = "/wattswarm/0.1.0";
// Backfill defaults are governance-controlled via NetworkProtocolParams;
// kept here only as fallbacks for NetworkP2pConfig::default().
const DEFAULT_MAX_BACKFILL_EVENTS_FALLBACK: usize = 512;
const MAX_BACKFILL_EVENTS_HARD_LIMIT_FALLBACK: usize = 8_192;
const BACKFILL_PROTOCOL: StreamProtocol = StreamProtocol::new("/wattswarm/backfill/1");
const KADEMLIA_PROTOCOL: StreamProtocol = StreamProtocol::new("/wattswarm/kad/1");

// Connection limits guard against resource exhaustion from too many peers.
const MAX_ESTABLISHED_INCOMING: u32 = 512;
const MAX_ESTABLISHED_OUTGOING: u32 = 256;
const MAX_ESTABLISHED_PER_PEER: u32 = 2;
const MAX_PENDING_INCOMING: u32 = 64;
const MAX_PENDING_OUTGOING: u32 = 64;

// Gossipsub mesh defaults — governance-controlled via NetworkProtocolParams;
// kept here only as fallbacks for NetworkP2pConfig::default().
const GOSSIPSUB_D_FALLBACK: usize = 6;
const GOSSIPSUB_D_LOW_FALLBACK: usize = 4;
const GOSSIPSUB_D_HIGH_FALLBACK: usize = 12;
const GOSSIPSUB_HEARTBEAT_MS_FALLBACK: u64 = 1_000;
const GOSSIPSUB_MAX_TRANSMIT_SIZE_FALLBACK: usize = 512 * 1024;
const TRAFFIC_WINDOW: Duration = Duration::from_secs(60);
const TRAFFIC_DEDUPE_TTL: Duration = Duration::from_secs(120);
const PER_PEER_MSGS_PER_WINDOW: usize = 1_200;
const PER_PEER_TOPIC_MSGS_PER_WINDOW: usize = 600;
const PER_TOPIC_PUBLISHES_PER_WINDOW: usize = 600;
const PER_PEER_BACKFILL_REQUESTS_PER_WINDOW: usize = 60;
const TRAFFIC_BLACKLIST_SCORE: i64 = -8;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmScope {
    Global,
    Region(String),
    Node(String),
}

impl SwarmScope {
    pub fn label(&self) -> Result<String> {
        match self {
            Self::Global => Ok("global".to_owned()),
            Self::Region(region_id) => Ok(format!("region.{}", sanitize_segment(region_id)?)),
            Self::Node(node_id) => Ok(format!("node.{}", sanitize_segment(node_id)?)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicKind {
    Events,
    Rules,
    Checkpoints,
    Summaries,
}

impl TopicKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Rules => "rules",
            Self::Checkpoints => "checkpoints",
            Self::Summaries => "summaries",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicNamespace {
    pub network: String,
}

impl Default for TopicNamespace {
    fn default() -> Self {
        Self {
            network: DEFAULT_NAMESPACE.to_owned(),
        }
    }
}

impl TopicNamespace {
    pub fn topic_name(&self, scope: &SwarmScope, kind: TopicKind) -> Result<String> {
        Ok(format!(
            "{}.{}.{}",
            sanitize_segment(&self.network)?,
            scope.label()?,
            kind.as_str()
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCatalog {
    pub events: String,
    pub rules: String,
    pub checkpoints: String,
    pub summaries: String,
}

impl TopicCatalog {
    pub fn new(namespace: &TopicNamespace, scope: &SwarmScope) -> Result<Self> {
        Ok(Self {
            events: namespace.topic_name(scope, TopicKind::Events)?,
            rules: namespace.topic_name(scope, TopicKind::Rules)?,
            checkpoints: namespace.topic_name(scope, TopicKind::Checkpoints)?,
            summaries: namespace.topic_name(scope, TopicKind::Summaries)?,
        })
    }

    pub fn as_ident_topics(&self) -> [IdentTopic; 4] {
        [
            IdentTopic::new(self.events.clone()),
            IdentTopic::new(self.rules.clone()),
            IdentTopic::new(self.checkpoints.clone()),
            IdentTopic::new(self.summaries.clone()),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillRequest {
    pub scope: SwarmScope,
    pub from_event_seq: u64,
    pub limit: usize,
}

impl BackfillRequest {
    pub fn validate(&self, max_limit: usize, hard_limit: usize) -> Result<()> {
        if self.limit == 0 {
            bail!("backfill limit must be > 0");
        }
        if self.limit > max_limit {
            bail!("backfill limit exceeds configured max");
        }
        if self.limit > hard_limit {
            bail!("backfill limit exceeds hard safety limit");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub scope: SwarmScope,
    pub event: Event,
}

impl EventEnvelope {
    pub fn encode_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackfillResponse {
    pub scope: SwarmScope,
    pub next_from_event_seq: u64,
    pub events: Vec<EventEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleAnnouncement {
    pub scope: SwarmScope,
    pub rule_set: String,
    pub rule_version: u64,
    pub activation_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointAnnouncement {
    pub scope: SwarmScope,
    pub checkpoint_id: String,
    pub artifact_path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryAnnouncement {
    pub summary_id: String,
    pub source_node_id: String,
    pub scope: SwarmScope,
    pub summary_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum GossipMessage {
    Event(EventEnvelope),
    Rule(RuleAnnouncement),
    Checkpoint(CheckpointAnnouncement),
    Summary(SummaryAnnouncement),
}

impl GossipMessage {
    pub fn kind(&self) -> TopicKind {
        match self {
            Self::Event(_) => TopicKind::Events,
            Self::Rule(_) => TopicKind::Rules,
            Self::Checkpoint(_) => TopicKind::Checkpoints,
            Self::Summary(_) => TopicKind::Summaries,
        }
    }

    pub fn scope(&self) -> &SwarmScope {
        match self {
            Self::Event(payload) => &payload.scope,
            Self::Rule(payload) => &payload.scope,
            Self::Checkpoint(payload) => &payload.scope,
            Self::Summary(payload) => &payload.scope,
        }
    }

    pub fn encode_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkP2pConfig {
    // ── Node-local settings ──────────────────────────────────────────
    pub namespace: TopicNamespace,
    pub protocol_version: String,
    pub listen_addrs: Vec<String>,
    pub bootstrap_peers: Vec<String>,
    pub enable_mdns: bool,

    // ── Governance-controlled (populated from NetworkProtocolParams) ─
    pub gossipsub_d: usize,
    pub gossipsub_d_low: usize,
    pub gossipsub_d_high: usize,
    pub gossipsub_heartbeat_ms: u64,
    pub gossipsub_max_transmit_size: usize,
    pub max_backfill_events: usize,
    pub max_backfill_events_hard_limit: usize,
}

impl Default for NetworkP2pConfig {
    fn default() -> Self {
        Self {
            namespace: TopicNamespace::default(),
            protocol_version: DEFAULT_PROTOCOL_VERSION.to_owned(),
            listen_addrs: vec!["/ip4/0.0.0.0/tcp/0".to_owned()],
            bootstrap_peers: Vec::new(),
            enable_mdns: true,
            gossipsub_d: GOSSIPSUB_D_FALLBACK,
            gossipsub_d_low: GOSSIPSUB_D_LOW_FALLBACK,
            gossipsub_d_high: GOSSIPSUB_D_HIGH_FALLBACK,
            gossipsub_heartbeat_ms: GOSSIPSUB_HEARTBEAT_MS_FALLBACK,
            gossipsub_max_transmit_size: GOSSIPSUB_MAX_TRANSMIT_SIZE_FALLBACK,
            max_backfill_events: DEFAULT_MAX_BACKFILL_EVENTS_FALLBACK,
            max_backfill_events_hard_limit: MAX_BACKFILL_EVENTS_HARD_LIMIT_FALLBACK,
        }
    }
}

impl NetworkP2pConfig {
    /// Populate governance-controlled fields from `NetworkProtocolParams`.
    pub fn apply_protocol_params(
        mut self,
        params: &wattswarm_protocol::types::NetworkProtocolParams,
    ) -> Self {
        self.gossipsub_d = params.gossipsub_d;
        self.gossipsub_d_low = params.gossipsub_d_low;
        self.gossipsub_d_high = params.gossipsub_d_high;
        self.gossipsub_heartbeat_ms = params.gossipsub_heartbeat_ms;
        self.gossipsub_max_transmit_size = params.gossipsub_max_transmit_size;
        self.max_backfill_events = params.default_max_backfill_events;
        self.max_backfill_events_hard_limit = params.max_backfill_events_hard_limit;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.listen_addrs.is_empty() {
            bail!("at least one listen address is required");
        }
        if self.protocol_version.trim().is_empty() {
            bail!("protocol_version is required");
        }
        if self.gossipsub_d == 0 || self.gossipsub_d_low == 0 || self.gossipsub_d_high == 0 {
            bail!("gossipsub mesh parameters must be > 0");
        }
        if self.gossipsub_d_low > self.gossipsub_d || self.gossipsub_d > self.gossipsub_d_high {
            bail!("gossipsub mesh parameters must satisfy d_low <= d <= d_high");
        }
        if self.gossipsub_heartbeat_ms == 0 {
            bail!("gossipsub heartbeat must be > 0");
        }
        if self.gossipsub_max_transmit_size == 0 {
            bail!("gossipsub max transmit size must be > 0");
        }
        if self.max_backfill_events == 0 {
            bail!("max_backfill_events must be > 0");
        }
        if self.max_backfill_events_hard_limit == 0 {
            bail!("max_backfill_events_hard_limit must be > 0");
        }
        if self.max_backfill_events > self.max_backfill_events_hard_limit {
            bail!("max_backfill_events exceeds hard safety limit");
        }
        Ok(())
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<Multiaddr>> {
        self.listen_addrs
            .iter()
            .map(|addr| {
                addr.parse::<Multiaddr>()
                    .map_err(|err| anyhow!("invalid listen address '{}': {err}", addr))
            })
            .collect()
    }

    pub fn parse_bootstrap_peers(&self) -> Result<Vec<(PeerId, Multiaddr)>> {
        self.bootstrap_peers
            .iter()
            .map(|addr| parse_peer_multiaddr(addr))
            .collect()
    }

    pub fn topic_catalog(&self, scope: &SwarmScope) -> Result<TopicCatalog> {
        TopicCatalog::new(&self.namespace, scope)
    }
}

pub type BackfillRequestId = request_response::OutboundRequestId;
pub type BackfillResponseChannel = request_response::ResponseChannel<BackfillResponse>;

#[derive(Debug)]
pub enum WattSwarmBehaviourEvent {
    Gossipsub(GossipsubEvent),
    Identify(identify::Event),
    Kademlia(kad::Event),
    Relay(relay::client::Event),
    Autonat(autonat::Event),
    Dcutr(dcutr::Event),
    RequestResponse(request_response::Event<BackfillRequest, BackfillResponse>),
    Mdns(mdns::Event),
}

impl From<GossipsubEvent> for WattSwarmBehaviourEvent {
    fn from(value: GossipsubEvent) -> Self {
        Self::Gossipsub(value)
    }
}

impl From<identify::Event> for WattSwarmBehaviourEvent {
    fn from(value: identify::Event) -> Self {
        Self::Identify(value)
    }
}

impl From<kad::Event> for WattSwarmBehaviourEvent {
    fn from(value: kad::Event) -> Self {
        Self::Kademlia(value)
    }
}

impl From<relay::client::Event> for WattSwarmBehaviourEvent {
    fn from(value: relay::client::Event) -> Self {
        Self::Relay(value)
    }
}

impl From<autonat::Event> for WattSwarmBehaviourEvent {
    fn from(value: autonat::Event) -> Self {
        Self::Autonat(value)
    }
}

impl From<dcutr::Event> for WattSwarmBehaviourEvent {
    fn from(value: dcutr::Event) -> Self {
        Self::Dcutr(value)
    }
}

impl From<request_response::Event<BackfillRequest, BackfillResponse>> for WattSwarmBehaviourEvent {
    fn from(value: request_response::Event<BackfillRequest, BackfillResponse>) -> Self {
        Self::RequestResponse(value)
    }
}

impl From<mdns::Event> for WattSwarmBehaviourEvent {
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

// connection_limits::Behaviour emits Infallible (never fires); required by derive macro.
impl From<std::convert::Infallible> for WattSwarmBehaviourEvent {
    fn from(v: std::convert::Infallible) -> Self {
        match v {}
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "WattSwarmBehaviourEvent")]
pub struct WattSwarmBehaviour {
    pub gossipsub: Gossipsub,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub relay_client: relay::client::Behaviour,
    pub autonat: autonat::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub request_response: request_response::cbor::Behaviour<BackfillRequest, BackfillResponse>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub connection_limits: connection_limits::Behaviour,
}

#[derive(Debug)]
pub enum NetworkRuntimeEvent {
    NewListenAddr {
        address: Multiaddr,
    },
    ConnectionEstablished {
        peer: PeerId,
    },
    ConnectionClosed {
        peer: PeerId,
    },
    Gossip {
        propagation_source: PeerId,
        message: GossipMessage,
    },
    BackfillRequest {
        peer: PeerId,
        request: BackfillRequest,
        channel: BackfillResponseChannel,
    },
    BackfillResponse {
        peer: PeerId,
        request_id: BackfillRequestId,
        response: BackfillResponse,
    },
    BackfillOutboundFailure {
        peer: PeerId,
        request_id: BackfillRequestId,
        error: String,
    },
    BackfillInboundFailure {
        peer: PeerId,
        error: String,
    },
    NatStatusChanged {
        old: String,
        new: String,
        public_address: Option<Multiaddr>,
        confidence: usize,
    },
    RelayReservationAccepted {
        relay_peer: PeerId,
        renewal: bool,
    },
    RelayCircuitEstablished {
        relay_peer: PeerId,
    },
    RelayInboundCircuitEstablished {
        source_peer: PeerId,
    },
    DcutrConnectionUpgradeSucceeded {
        remote_peer: PeerId,
    },
    DcutrConnectionUpgradeFailed {
        remote_peer: PeerId,
        error: String,
    },
}

pub struct NetworkP2pNode {
    config: NetworkP2pConfig,
    local_key: identity::Keypair,
    local_peer_id: PeerId,
}

struct BehaviourSeedParts {
    gossipsub: Gossipsub,
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
    autonat: autonat::Behaviour,
    request_response: request_response::cbor::Behaviour<BackfillRequest, BackfillResponse>,
    mdns: Toggle<mdns::tokio::Behaviour>,
    connection_limits: connection_limits::Behaviour,
}

impl NetworkP2pNode {
    pub fn new(config: NetworkP2pConfig, local_key: identity::Keypair) -> Result<Self> {
        config.validate()?;
        let local_peer_id = local_key.public().to_peer_id();
        Ok(Self {
            config,
            local_key,
            local_peer_id,
        })
    }

    pub fn generate(config: NetworkP2pConfig) -> Result<Self> {
        Self::new(config, identity::Keypair::generate_ed25519())
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<Multiaddr>> {
        self.config.parse_listen_addrs()
    }

    fn build_behaviour_seed_parts(
        config: &NetworkP2pConfig,
        local_key: &identity::Keypair,
        local_peer_id: PeerId,
    ) -> Result<BehaviourSeedParts> {
        let gossipsub_config = GossipsubConfigBuilder::default()
            .validation_mode(ValidationMode::Strict)
            .mesh_n(config.gossipsub_d)
            .mesh_n_low(config.gossipsub_d_low)
            .mesh_n_high(config.gossipsub_d_high)
            .heartbeat_interval(Duration::from_millis(config.gossipsub_heartbeat_ms))
            .max_transmit_size(config.gossipsub_max_transmit_size)
            .build()
            .map_err(|err| anyhow!(err))?;
        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )
        .map_err(|err| anyhow!(err))?;

        let identify = identify::Behaviour::new(
            identify::Config::new(config.protocol_version.clone(), local_key.public())
                .with_agent_version("wattswarm-network-p2p".to_owned()),
        );

        let request_response = request_response::cbor::Behaviour::new(
            [(BACKFILL_PROTOCOL, ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let mut kad_config = kad::Config::new(KADEMLIA_PROTOCOL);
        kad_config.set_periodic_bootstrap_interval(Some(Duration::from_secs(300)));
        let mut kademlia =
            kad::Behaviour::with_config(local_peer_id, MemoryStore::new(local_peer_id), kad_config);
        let mut autonat = autonat::Behaviour::new(local_peer_id, autonat::Config::default());
        for (peer, addr) in config.parse_bootstrap_peers()? {
            kademlia.add_address(&peer, addr.clone());
            autonat.add_server(peer, Some(addr));
        }

        let mdns = Toggle::from(if config.enable_mdns {
            Some(mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                local_peer_id,
            )?)
        } else {
            None
        });

        let connection_limits = connection_limits::Behaviour::new(
            connection_limits::ConnectionLimits::default()
                .with_max_established_incoming(Some(MAX_ESTABLISHED_INCOMING))
                .with_max_established_outgoing(Some(MAX_ESTABLISHED_OUTGOING))
                .with_max_established_per_peer(Some(MAX_ESTABLISHED_PER_PEER))
                .with_max_pending_incoming(Some(MAX_PENDING_INCOMING))
                .with_max_pending_outgoing(Some(MAX_PENDING_OUTGOING)),
        );

        Ok(BehaviourSeedParts {
            gossipsub,
            identify,
            kademlia,
            autonat,
            request_response,
            mdns,
            connection_limits,
        })
    }

    pub fn build_behaviour(&self) -> Result<WattSwarmBehaviour> {
        let (_, relay_client) = relay::client::new(self.local_peer_id);
        let parts =
            Self::build_behaviour_seed_parts(&self.config, &self.local_key, self.local_peer_id)?;
        Ok(WattSwarmBehaviour {
            gossipsub: parts.gossipsub,
            identify: parts.identify,
            kademlia: parts.kademlia,
            relay_client,
            autonat: parts.autonat,
            dcutr: dcutr::Behaviour::new(self.local_peer_id),
            request_response: parts.request_response,
            mdns: parts.mdns,
            connection_limits: parts.connection_limits,
        })
    }

    pub fn build_swarm(&self) -> Result<Swarm<WattSwarmBehaviour>> {
        let config = self.config.clone();
        let local_peer_id = self.local_peer_id;
        let parts = Self::build_behaviour_seed_parts(&config, &self.local_key, local_peer_id)?;
        Ok(SwarmBuilder::with_existing_identity(self.local_key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(move |_, relay_client| WattSwarmBehaviour {
                gossipsub: parts.gossipsub,
                identify: parts.identify,
                kademlia: parts.kademlia,
                relay_client,
                autonat: parts.autonat,
                dcutr: dcutr::Behaviour::new(local_peer_id),
                request_response: parts.request_response,
                mdns: parts.mdns,
                connection_limits: parts.connection_limits,
            })?
            .build())
    }
}

pub struct NetworkRuntime {
    config: NetworkP2pConfig,
    local_peer_id: PeerId,
    swarm: Swarm<WattSwarmBehaviour>,
    listen_addrs: Vec<Multiaddr>,
    subscribed_topics: HashSet<String>,
    traffic_guard: TrafficGuard,
    relay_reservations: HashSet<String>,
}

#[derive(Debug)]
struct TrafficGuard {
    seen_messages: HashMap<u64, Instant>,
    peer_windows: HashMap<PeerId, Vec<Instant>>,
    peer_topic_windows: HashMap<(PeerId, String), Vec<Instant>>,
    local_publish_windows: HashMap<String, Vec<Instant>>,
    backfill_request_windows: HashMap<PeerId, Vec<Instant>>,
    peer_scores: HashMap<PeerId, i64>,
    blacklisted_peers: HashSet<PeerId>,
}

impl TrafficGuard {
    fn new() -> Self {
        Self {
            seen_messages: HashMap::new(),
            peer_windows: HashMap::new(),
            peer_topic_windows: HashMap::new(),
            local_publish_windows: HashMap::new(),
            backfill_request_windows: HashMap::new(),
            peer_scores: HashMap::new(),
            blacklisted_peers: HashSet::new(),
        }
    }

    fn allow_local_publish(&mut self, topic: &str, now: Instant) -> bool {
        let window = self
            .local_publish_windows
            .entry(topic.to_owned())
            .or_default();
        prune_window(window, now, TRAFFIC_WINDOW);
        if window.len() >= PER_TOPIC_PUBLISHES_PER_WINDOW {
            return false;
        }
        window.push(now);
        true
    }

    fn allow_inbound_gossip(
        &mut self,
        source_peer: PeerId,
        topic: &str,
        data: &[u8],
        now: Instant,
    ) -> bool {
        if self.blacklisted_peers.contains(&source_peer) {
            return false;
        }

        let digest = traffic_digest(data);
        self.gc_seen(now);
        if self
            .seen_messages
            .get(&digest)
            .is_some_and(|seen_at| now.duration_since(*seen_at) <= TRAFFIC_DEDUPE_TTL)
        {
            self.penalize(source_peer, 1);
            return false;
        }
        self.seen_messages.insert(digest, now);

        let peer_window = self.peer_windows.entry(source_peer).or_default();
        prune_window(peer_window, now, TRAFFIC_WINDOW);
        if peer_window.len() >= PER_PEER_MSGS_PER_WINDOW {
            self.penalize(source_peer, 3);
            return false;
        }
        peer_window.push(now);

        let topic_window = self
            .peer_topic_windows
            .entry((source_peer, topic.to_owned()))
            .or_default();
        prune_window(topic_window, now, TRAFFIC_WINDOW);
        if topic_window.len() >= PER_PEER_TOPIC_MSGS_PER_WINDOW {
            self.penalize(source_peer, 2);
            return false;
        }
        topic_window.push(now);

        self.reward(source_peer, 1);
        true
    }

    fn allow_backfill_request(&mut self, peer: PeerId, now: Instant) -> bool {
        if self.blacklisted_peers.contains(&peer) {
            return false;
        }
        let window = self.backfill_request_windows.entry(peer).or_default();
        prune_window(window, now, TRAFFIC_WINDOW);
        if window.len() >= PER_PEER_BACKFILL_REQUESTS_PER_WINDOW {
            self.penalize(peer, 2);
            return false;
        }
        window.push(now);
        true
    }

    fn penalize(&mut self, peer: PeerId, amount: i64) {
        let score = self.peer_scores.entry(peer).or_insert(0);
        *score -= amount;
        if *score <= TRAFFIC_BLACKLIST_SCORE {
            self.blacklisted_peers.insert(peer);
        }
    }

    fn reward(&mut self, peer: PeerId, amount: i64) {
        let score = self.peer_scores.entry(peer).or_insert(0);
        *score = (*score + amount).min(100);
    }

    fn gc_seen(&mut self, now: Instant) {
        self.seen_messages
            .retain(|_, seen_at| now.duration_since(*seen_at) <= TRAFFIC_DEDUPE_TTL);
    }
}

impl NetworkRuntime {
    pub fn new(node: NetworkP2pNode) -> Result<Self> {
        let local_peer_id = node.local_peer_id();
        let config = node.config.clone();
        let swarm = node.build_swarm()?;
        let mut this = Self {
            config,
            local_peer_id,
            swarm,
            listen_addrs: Vec::new(),
            subscribed_topics: HashSet::new(),
            traffic_guard: TrafficGuard::new(),
            relay_reservations: HashSet::new(),
        };
        this.listen_on_configured_addrs()?;
        this.seed_bootstrap_peers()?;
        Ok(this)
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        &self.listen_addrs
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        for topic in self.config.topic_catalog(scope)?.as_ident_topics() {
            let topic_name = topic.to_string();
            if self.subscribed_topics.contains(&topic_name) {
                continue;
            }
            self.swarm
                .behaviour_mut()
                .gossipsub
                .subscribe(&topic)
                .map_err(|err| anyhow!(err))?;
            self.subscribed_topics.insert(topic_name);
        }
        Ok(())
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        for topic in self.config.topic_catalog(scope)?.as_ident_topics() {
            let topic_name = topic.to_string();
            if !self.subscribed_topics.contains(&topic_name) {
                continue;
            }
            let _ = self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic);
            self.subscribed_topics.remove(&topic_name);
        }
        Ok(())
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        self.swarm.dial(addr).map_err(|err| anyhow!(err))?;
        Ok(())
    }

    pub fn publish_gossip(&mut self, message: &GossipMessage) -> Result<()> {
        let topic_name = self
            .config
            .namespace
            .topic_name(message.scope(), message.kind())?;
        if !self
            .traffic_guard
            .allow_local_publish(&topic_name, Instant::now())
        {
            bail!("LocalTopicRateLimited");
        }
        let topic = IdentTopic::new(topic_name);
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, message.encode_json()?)
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }

    pub fn send_backfill_request(
        &mut self,
        peer: &PeerId,
        request: BackfillRequest,
    ) -> Result<BackfillRequestId> {
        request.validate(
            self.config.max_backfill_events,
            self.config.max_backfill_events_hard_limit,
        )?;
        Ok(self
            .swarm
            .behaviour_mut()
            .request_response
            .send_request(peer, request))
    }

    pub fn send_backfill_response(
        &mut self,
        channel: BackfillResponseChannel,
        response: BackfillResponse,
    ) -> Result<()> {
        self.swarm
            .behaviour_mut()
            .request_response
            .send_response(channel, response)
            .map_err(|_| anyhow!("backfill response channel closed"))?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> Result<NetworkRuntimeEvent> {
        loop {
            let swarm_event = self.swarm.select_next_some().await;
            if let Some(event) = self.handle_swarm_event(swarm_event)? {
                return Ok(event);
            }
        }
    }

    pub fn try_next_event(&mut self) -> Result<Option<NetworkRuntimeEvent>> {
        let Some(event) = self.swarm.select_next_some().now_or_never() else {
            return Ok(None);
        };
        self.handle_swarm_event(event)
    }

    fn listen_on_configured_addrs(&mut self) -> Result<()> {
        for addr in self.config.parse_listen_addrs()? {
            self.swarm.listen_on(addr).map_err(|err| anyhow!(err))?;
        }
        Ok(())
    }

    fn seed_bootstrap_peers(&mut self) -> Result<()> {
        for (peer, addr) in self.config.parse_bootstrap_peers()? {
            self.register_peer_address(peer, addr.clone());
            match self.swarm.dial(addr.clone()) {
                Ok(()) => {}
                Err(err)
                    if err
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate connection") => {}
                Err(err) => return Err(anyhow!(err)),
            }
            self.ensure_relay_reservation(peer, addr)?;
        }
        self.trigger_kademlia_refresh();
        Ok(())
    }

    fn register_peer_address(&mut self, peer: PeerId, addr: Multiaddr) {
        self.swarm.add_peer_address(peer, addr.clone());
        self.swarm
            .behaviour_mut()
            .gossipsub
            .add_explicit_peer(&peer);
        self.swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer, addr.clone());
        self.swarm
            .behaviour_mut()
            .autonat
            .add_server(peer, Some(addr));
    }

    fn ensure_relay_reservation(
        &mut self,
        relay_peer: PeerId,
        relay_addr: Multiaddr,
    ) -> Result<()> {
        let listen_addr = relay_reservation_addr(relay_peer, &relay_addr);
        let key = listen_addr.to_string();
        if self.relay_reservations.contains(&key) {
            return Ok(());
        }
        self.swarm
            .listen_on(listen_addr)
            .map_err(|err| anyhow!(err))?;
        self.relay_reservations.insert(key);
        Ok(())
    }

    fn maybe_dial_peer(&mut self, peer: PeerId, addr: Multiaddr) {
        if peer == self.local_peer_id {
            return;
        }
        self.register_peer_address(peer, addr.clone());
        match self.swarm.dial(addr) {
            Ok(()) => {}
            Err(err)
                if err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("duplicate connection") => {}
            Err(_) => {}
        }
    }

    fn trigger_kademlia_refresh(&mut self) {
        let _ = self.swarm.behaviour_mut().kademlia.bootstrap();
        let _ = self
            .swarm
            .behaviour_mut()
            .kademlia
            .get_closest_peers(self.local_peer_id);
    }

    fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<WattSwarmBehaviourEvent>,
    ) -> Result<Option<NetworkRuntimeEvent>> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                if !self.listen_addrs.iter().any(|seen| seen == &address) {
                    self.listen_addrs.push(address.clone());
                }
                Ok(Some(NetworkRuntimeEvent::NewListenAddr { address }))
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                self.trigger_kademlia_refresh();
                Ok(Some(NetworkRuntimeEvent::ConnectionEstablished {
                    peer: peer_id,
                }))
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                Ok(Some(NetworkRuntimeEvent::ConnectionClosed {
                    peer: peer_id,
                }))
            }
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Gossipsub(
                GossipsubEvent::Message {
                    propagation_source,
                    message,
                    ..
                },
            )) => {
                let topic = message.topic.to_string();
                if !self.traffic_guard.allow_inbound_gossip(
                    propagation_source,
                    &topic,
                    &message.data,
                    Instant::now(),
                ) {
                    return Ok(None);
                }
                Ok(Some(NetworkRuntimeEvent::Gossip {
                    propagation_source,
                    message: GossipMessage::decode_json(&message.data)?,
                }))
            }
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::RequestResponse(
                request_response::Event::Message { peer, message, .. },
            )) => match message {
                RequestResponseMessage::Request {
                    request, channel, ..
                } => {
                    if !self
                        .traffic_guard
                        .allow_backfill_request(peer, Instant::now())
                    {
                        return Ok(None);
                    }
                    Ok(Some(NetworkRuntimeEvent::BackfillRequest {
                        peer,
                        request,
                        channel,
                    }))
                }
                RequestResponseMessage::Response {
                    request_id,
                    response,
                } => Ok(Some(NetworkRuntimeEvent::BackfillResponse {
                    peer,
                    request_id,
                    response,
                })),
            },
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure {
                    peer,
                    request_id,
                    error,
                    ..
                },
            )) => Ok(Some(NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error: error.to_string(),
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::RequestResponse(
                request_response::Event::InboundFailure { peer, error, .. },
            )) => Ok(Some(NetworkRuntimeEvent::BackfillInboundFailure {
                peer,
                error: error.to_string(),
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Mdns(mdns::Event::Discovered(
                discovered,
            ))) => {
                for (peer, addr) in discovered {
                    self.maybe_dial_peer(peer, addr);
                }
                Ok(None)
            }
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Mdns(mdns::Event::Expired(expired))) => {
                for (peer, _) in expired {
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer);
                }
                Ok(None)
            }
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Identify(
                identify::Event::Received { peer_id, info, .. },
            )) => {
                for addr in info.listen_addrs {
                    self.register_peer_address(peer_id, addr);
                }
                self.trigger_kademlia_refresh();
                Ok(None)
            }
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Relay(
                relay::client::Event::ReservationReqAccepted {
                    relay_peer_id,
                    renewal,
                    ..
                },
            )) => Ok(Some(NetworkRuntimeEvent::RelayReservationAccepted {
                relay_peer: relay_peer_id,
                renewal,
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Relay(
                relay::client::Event::OutboundCircuitEstablished { relay_peer_id, .. },
            )) => Ok(Some(NetworkRuntimeEvent::RelayCircuitEstablished {
                relay_peer: relay_peer_id,
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Relay(
                relay::client::Event::InboundCircuitEstablished { src_peer_id, .. },
            )) => Ok(Some(NetworkRuntimeEvent::RelayInboundCircuitEstablished {
                source_peer: src_peer_id,
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Autonat(
                autonat::Event::StatusChanged { old, new },
            )) => Ok(Some(NetworkRuntimeEvent::NatStatusChanged {
                old: nat_status_label(&old),
                new: nat_status_label(&new),
                public_address: self.swarm.behaviour().autonat.public_address().cloned(),
                confidence: self.swarm.behaviour().autonat.confidence(),
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Dcutr(dcutr::Event {
                remote_peer_id,
                result,
            })) => match result {
                Ok(_) => Ok(Some(NetworkRuntimeEvent::DcutrConnectionUpgradeSucceeded {
                    remote_peer: remote_peer_id,
                })),
                Err(err) => Ok(Some(NetworkRuntimeEvent::DcutrConnectionUpgradeFailed {
                    remote_peer: remote_peer_id,
                    error: err.to_string(),
                })),
            },
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Kademlia(
                kad::Event::RoutingUpdated {
                    peer, addresses, ..
                },
            )) => {
                for addr in addresses.into_vec() {
                    self.maybe_dial_peer(peer, addr);
                }
                Ok(None)
            }
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                    ..
                },
            )) => {
                for peer in ok.peers {
                    for addr in peer.addrs {
                        self.maybe_dial_peer(peer.peer_id, addr);
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

fn parse_peer_multiaddr(raw: &str) -> Result<(PeerId, Multiaddr)> {
    let mut addr = raw
        .parse::<Multiaddr>()
        .map_err(|err| anyhow!("invalid bootstrap peer address '{}': {err}", raw))?;
    let Some(Protocol::P2p(peer)) = addr.pop() else {
        bail!("bootstrap peer address must end with /p2p/<peer_id>: {raw}");
    };
    Ok((peer, addr))
}

fn prune_window(entries: &mut Vec<Instant>, now: Instant, window: Duration) {
    entries.retain(|ts| now.duration_since(*ts) <= window);
}

fn traffic_digest(data: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

fn nat_status_label(status: &autonat::NatStatus) -> String {
    match status {
        autonat::NatStatus::Public(_) => "public".to_owned(),
        autonat::NatStatus::Private => "private".to_owned(),
        autonat::NatStatus::Unknown => "unknown".to_owned(),
    }
}

fn relay_reservation_addr(relay_peer: PeerId, relay_addr: &Multiaddr) -> Multiaddr {
    relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer))
        .with(Protocol::P2pCircuit)
}

fn sanitize_segment(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("network segment cannot be empty");
    }

    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> Event {
        Event {
            event_id: "evt-1".to_owned(),
            protocol_version: "0.1.0".to_owned(),
            event_kind: wattswarm_protocol::types::EventKind::CheckpointCreated,
            task_id: None,
            epoch: 1,
            author_node_id: "node-1".to_owned(),
            created_at: 42,
            payload: wattswarm_protocol::types::EventPayload::CheckpointCreated(
                wattswarm_protocol::types::CheckpointCreatedPayload {
                    checkpoint_id: "cp-1".to_owned(),
                    up_to_seq: 10,
                },
            ),
            signature_hex: "sig".to_owned(),
        }
    }

    #[test]
    fn topic_catalog_uses_namespace_scope_and_kind() {
        let namespace = TopicNamespace {
            network: "WattSwarm Main".to_owned(),
        };
        let catalog = TopicCatalog::new(&namespace, &SwarmScope::Region("sol-1/alpha".to_owned()))
            .expect("catalog");
        assert_eq!(catalog.events, "wattswarm-main.region.sol-1-alpha.events");
        assert_eq!(catalog.rules, "wattswarm-main.region.sol-1-alpha.rules");
    }

    #[test]
    fn backfill_request_enforces_limits() {
        let req = BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 42,
            limit: 128,
        };
        req.validate(256, 512).expect("valid");
        assert!(req.validate(64, 512).is_err());
        assert!(req.validate(256, 96).is_err());
    }

    #[test]
    fn event_and_gossip_messages_roundtrip_as_json() {
        let message = GossipMessage::Event(EventEnvelope {
            scope: SwarmScope::Global,
            event: sample_event(),
        });
        let bytes = message.encode_json().expect("encode");
        let decoded = GossipMessage::decode_json(&bytes).expect("decode");
        assert_eq!(decoded, message);
        assert_eq!(decoded.kind(), TopicKind::Events);
    }

    #[test]
    fn summary_messages_roundtrip_as_json() {
        let message = GossipMessage::Summary(SummaryAnnouncement {
            summary_id: "summary-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Region("sol-1".to_owned()),
            summary_kind: "knowledge_task_type_v1".to_owned(),
            artifact_path: Some("summaries/sol-1/knowledge.json".to_owned()),
            payload: serde_json::json!({"task_type":"swarm","rows":[{"task_id":"task-1"}]}),
        });
        let bytes = message.encode_json().expect("encode");
        let decoded = GossipMessage::decode_json(&bytes).expect("decode");
        assert_eq!(decoded, message);
        assert_eq!(decoded.kind(), TopicKind::Summaries);
    }

    #[test]
    fn default_config_is_valid_and_serializable() {
        let config = NetworkP2pConfig::default();
        config.validate().expect("config valid");

        let json = serde_json::to_value(&config).expect("serialize config");
        assert_eq!(json["namespace"]["network"], "wattswarm");
        assert_eq!(json["max_backfill_events"], 512);
        assert_eq!(json["protocol_version"], "/wattswarm/0.1.0");
    }

    #[test]
    fn apply_protocol_params_overrides_governed_runtime_fields() {
        let params = wattswarm_protocol::types::NetworkProtocolParams {
            gossipsub_d: 9,
            gossipsub_d_low: 7,
            gossipsub_d_high: 15,
            gossipsub_heartbeat_ms: 2_500,
            gossipsub_max_transmit_size: 1024 * 1024,
            default_max_backfill_events: 1024,
            max_backfill_events_hard_limit: 16_384,
            ..wattswarm_protocol::types::NetworkProtocolParams::default()
        };

        let config = NetworkP2pConfig::default().apply_protocol_params(&params);

        assert_eq!(config.gossipsub_d, 9);
        assert_eq!(config.gossipsub_d_low, 7);
        assert_eq!(config.gossipsub_d_high, 15);
        assert_eq!(config.gossipsub_heartbeat_ms, 2_500);
        assert_eq!(config.gossipsub_max_transmit_size, 1024 * 1024);
        assert_eq!(config.max_backfill_events, 1024);
        assert_eq!(config.max_backfill_events_hard_limit, 16_384);
    }

    #[test]
    fn config_validation_rejects_invalid_governed_mesh_and_backfill_limits() {
        let invalid_mesh = NetworkP2pConfig {
            gossipsub_d: 4,
            gossipsub_d_low: 5,
            gossipsub_d_high: 8,
            ..NetworkP2pConfig::default()
        };
        assert!(invalid_mesh.validate().is_err());

        let invalid_backfill = NetworkP2pConfig {
            max_backfill_events: 1024,
            max_backfill_events_hard_limit: 512,
            ..NetworkP2pConfig::default()
        };
        assert!(invalid_backfill.validate().is_err());
    }

    #[test]
    fn config_parses_multiaddrs() {
        let config = NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_owned()],
            ..NetworkP2pConfig::default()
        };
        let addrs = config.parse_listen_addrs().expect("multiaddr parse");
        assert_eq!(addrs.len(), 1);
    }

    #[test]
    fn config_parses_bootstrap_peers_with_peer_ids() {
        let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        let config = NetworkP2pConfig {
            bootstrap_peers: vec![format!("/ip4/127.0.0.1/tcp/4001/p2p/{peer}")],
            ..NetworkP2pConfig::default()
        };
        let peers = config.parse_bootstrap_peers().expect("bootstrap peers");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].0, peer);
        assert_eq!(peers[0].1.to_string(), "/ip4/127.0.0.1/tcp/4001");
    }

    #[test]
    fn traffic_guard_rejects_duplicate_gossip_and_backfill_spam() {
        let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        let mut guard = TrafficGuard::new();
        let now = Instant::now();
        let payload = br#"{"kind":"event","n":1}"#;

        assert!(guard.allow_inbound_gossip(peer, "wattswarm.global.events", payload, now));
        assert!(!guard.allow_inbound_gossip(
            peer,
            "wattswarm.global.events",
            payload,
            now + Duration::from_secs(1),
        ));

        let mut later = now;
        for _ in 0..PER_PEER_BACKFILL_REQUESTS_PER_WINDOW {
            later += Duration::from_millis(1);
            assert!(guard.allow_backfill_request(peer, later));
        }
        later += Duration::from_millis(1);
        assert!(!guard.allow_backfill_request(peer, later));
    }

    #[test]
    fn relay_reservation_addr_uses_peer_and_circuit_protocols() {
        let relay_peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        let relay_addr = "/ip4/127.0.0.1/tcp/4001"
            .parse::<Multiaddr>()
            .expect("relay addr");
        let reserved = relay_reservation_addr(relay_peer, &relay_addr);
        assert_eq!(
            reserved.to_string(),
            format!("/ip4/127.0.0.1/tcp/4001/p2p/{relay_peer}/p2p-circuit")
        );
    }

    #[tokio::test]
    async fn runtime_seeds_relay_reservations_from_bootstrap_peers() {
        let relay_peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        let node = NetworkP2pNode::generate(NetworkP2pConfig {
            bootstrap_peers: vec![format!("/ip4/127.0.0.1/tcp/4001/p2p/{relay_peer}")],
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("node");
        let runtime = NetworkRuntime::new(node).expect("runtime");
        assert!(
            runtime.relay_reservations.contains(&format!(
                "/ip4/127.0.0.1/tcp/4001/p2p/{relay_peer}/p2p-circuit"
            )),
            "bootstrap peers should seed relay listen reservations"
        );
    }

    #[tokio::test]
    async fn node_builds_libp2p_behaviour() {
        let node = NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node");
        let behaviour = node.build_behaviour().expect("behaviour");
        let topics = node
            .config()
            .topic_catalog(&SwarmScope::Global)
            .expect("topic catalog")
            .as_ident_topics();

        let mut gossipsub = behaviour.gossipsub;
        for topic in topics {
            assert!(gossipsub.subscribe(&topic).expect("subscribe"));
        }
    }

    #[tokio::test]
    async fn runtime_tracks_listen_addresses_and_subscriptions() {
        let node = NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node");
        let mut runtime = NetworkRuntime::new(node).expect("runtime");
        runtime
            .subscribe_scope(&SwarmScope::Global)
            .expect("subscribe scope");

        let mut saw_addr = false;
        for _ in 0..128 {
            if let Some(NetworkRuntimeEvent::NewListenAddr { .. }) =
                runtime.try_next_event().expect("poll event")
            {
                saw_addr = true;
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(saw_addr);
        assert!(!runtime.listen_addrs().is_empty());
    }

    #[test]
    fn mdns_flag_controls_behaviour_construction() {
        let node = NetworkP2pNode::generate(NetworkP2pConfig {
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("node");
        let behaviour = node.build_behaviour().expect("behaviour");
        assert!(!behaviour.mdns.is_enabled());
    }
}
