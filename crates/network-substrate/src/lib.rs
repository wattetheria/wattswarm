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
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

const DEFAULT_NAMESPACE: &str = "wattswarm";
const DEFAULT_PROTOCOL_VERSION: &str = "/wattswarm/0.1.0";
const DEFAULT_IDENTIFY_AGENT_NAME: &str = "wattswarm-network-substrate";
const DEFAULT_MAX_BACKFILL_EVENTS_FALLBACK: usize = 512;
const MAX_BACKFILL_EVENTS_HARD_LIMIT_FALLBACK: usize = 8_192;
const BACKFILL_PROTOCOL: StreamProtocol = StreamProtocol::new("/wattswarm/backfill/1");
const KADEMLIA_PROTOCOL: StreamProtocol = StreamProtocol::new("/wattswarm/kad/1");

const MAX_ESTABLISHED_INCOMING: u32 = 512;
const MAX_ESTABLISHED_OUTGOING: u32 = 256;
const MAX_ESTABLISHED_PER_PEER_FALLBACK: u32 = 2;
const MAX_PENDING_INCOMING: u32 = 64;
const MAX_PENDING_OUTGOING: u32 = 64;

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
const TRAFFIC_THROTTLED_SCORE: i64 = -4;
const TRAFFIC_QUARANTINE_SCORE: i64 = -8;
const TRAFFIC_BAN_SCORE: i64 = -16;
const TRAFFIC_QUARANTINE_DURATION: Duration = Duration::from_secs(90);
const TRAFFIC_BAN_DURATION: Duration = Duration::from_secs(5 * 60);
const TRAFFIC_FAILURE_PENALTY: i64 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmScope {
    Global,
    Region(String),
    Node(String),
    Group(String),
}

impl SwarmScope {
    pub fn label(&self) -> Result<String> {
        match self {
            Self::Global => Ok("global".to_owned()),
            Self::Region(region_id) => Ok(format!("region.{}", sanitize_segment(region_id)?)),
            Self::Node(node_id) => Ok(format!("node.{}", sanitize_segment(node_id)?)),
            Self::Group(group_id) => Ok(format!("group.{}", sanitize_segment(group_id)?)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicKind {
    Events,
    Messages,
    Rules,
    Checkpoints,
    Summaries,
}

impl TopicKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Messages => "messages",
            Self::Rules => "rules",
            Self::Checkpoints => "checkpoints",
            Self::Summaries => "summaries",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicNamespace {
    pub network: String,
    #[serde(default = "default_network_context_id")]
    pub network_id: String,
}

impl Default for TopicNamespace {
    fn default() -> Self {
        Self {
            network: DEFAULT_NAMESPACE.to_owned(),
            network_id: default_network_context_id(),
        }
    }
}

impl TopicNamespace {
    pub fn topic_name(&self, scope: &SwarmScope, kind: TopicKind) -> Result<String> {
        Ok(format!(
            "{}.{}.{}.{}",
            sanitize_segment(&self.network)?,
            sanitize_segment(&self.network_id)?,
            scope.label()?,
            kind.as_str()
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCatalog {
    pub events: String,
    pub messages: String,
    pub rules: String,
    pub checkpoints: String,
    pub summaries: String,
}

impl TopicCatalog {
    pub fn new(namespace: &TopicNamespace, scope: &SwarmScope) -> Result<Self> {
        Ok(Self {
            events: namespace.topic_name(scope, TopicKind::Events)?,
            messages: namespace.topic_name(scope, TopicKind::Messages)?,
            rules: namespace.topic_name(scope, TopicKind::Rules)?,
            checkpoints: namespace.topic_name(scope, TopicKind::Checkpoints)?,
            summaries: namespace.topic_name(scope, TopicKind::Summaries)?,
        })
    }

    pub fn as_ident_topics(&self) -> [IdentTopic; 5] {
        [
            IdentTopic::new(self.events.clone()),
            IdentTopic::new(self.messages.clone()),
            IdentTopic::new(self.rules.clone()),
            IdentTopic::new(self.checkpoints.clone()),
            IdentTopic::new(self.summaries.clone()),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerHandshakeMetadata {
    pub network_id: String,
    pub params_version: u64,
    pub params_hash: String,
}

impl Default for PeerHandshakeMetadata {
    fn default() -> Self {
        Self {
            network_id: "default".to_owned(),
            params_version: 0,
            params_hash: "default".to_owned(),
        }
    }
}

impl PeerHandshakeMetadata {
    pub fn encode_agent_version(&self) -> String {
        self.encode_agent_version_with_prefix(DEFAULT_IDENTIFY_AGENT_NAME)
    }

    pub fn encode_agent_version_with_prefix(&self, prefix: &str) -> String {
        format!(
            "{}|{}|{}|{}",
            prefix, self.network_id, self.params_version, self.params_hash
        )
    }

    pub fn decode_agent_version(raw: &str) -> Result<Self> {
        Self::decode_agent_version_with_prefix(raw, DEFAULT_IDENTIFY_AGENT_NAME)
    }

    pub fn decode_agent_version_parts(raw: &str) -> Result<(String, Self)> {
        let mut parts = raw.splitn(4, '|');
        let Some(prefix) = parts.next() else {
            bail!("identify agent version is empty");
        };
        let Some(network_id) = parts.next() else {
            bail!("identify agent version missing network_id");
        };
        let Some(params_version) = parts.next() else {
            bail!("identify agent version missing params_version");
        };
        let Some(params_hash) = parts.next() else {
            bail!("identify agent version missing params_hash");
        };
        Ok((
            prefix.to_owned(),
            Self {
                network_id: network_id.to_owned(),
                params_version: params_version
                    .parse::<u64>()
                    .map_err(|err| anyhow!("invalid params_version: {err}"))?,
                params_hash: params_hash.to_owned(),
            },
        ))
    }

    pub fn decode_agent_version_with_prefix(raw: &str, expected_prefix: &str) -> Result<Self> {
        let (prefix, metadata) = Self::decode_agent_version_parts(raw)?;
        if prefix != expected_prefix {
            bail!("unexpected identify agent prefix");
        }
        Ok(metadata)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackfillRequest {
    pub scope: SwarmScope,
    pub from_event_seq: u64,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
}

impl RawBackfillRequest {
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
        if let Some(feed_key) = &self.feed_key
            && feed_key.trim().is_empty()
        {
            bail!("backfill feed_key must not be empty");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackfillResponse {
    pub scope: SwarmScope,
    pub next_from_event_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
    pub items: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawGossipMessage {
    pub scope: SwarmScope,
    pub kind: TopicKind,
    pub payload: Vec<u8>,
}

impl RawGossipMessage {
    pub fn encode_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubstrateConfig {
    pub namespace: TopicNamespace,
    pub protocol_version: String,
    pub identify_agent_version: String,
    pub listen_addrs: Vec<String>,
    pub bootstrap_peers: Vec<String>,
    pub enable_mdns: bool,
    pub max_established_per_peer: u32,
    pub gossipsub_d: usize,
    pub gossipsub_d_low: usize,
    pub gossipsub_d_high: usize,
    pub gossipsub_heartbeat_ms: u64,
    pub gossipsub_max_transmit_size: usize,
    pub max_backfill_events: usize,
    pub max_backfill_events_hard_limit: usize,
}

impl Default for SubstrateConfig {
    fn default() -> Self {
        Self {
            namespace: TopicNamespace::default(),
            protocol_version: DEFAULT_PROTOCOL_VERSION.to_owned(),
            identify_agent_version: PeerHandshakeMetadata::default().encode_agent_version(),
            listen_addrs: vec!["/ip4/0.0.0.0/tcp/0".to_owned()],
            bootstrap_peers: Vec::new(),
            enable_mdns: true,
            max_established_per_peer: MAX_ESTABLISHED_PER_PEER_FALLBACK,
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

impl SubstrateConfig {
    pub fn validate(&self) -> Result<()> {
        if self.listen_addrs.is_empty() {
            bail!("at least one listen address is required");
        }
        if self.namespace.network.trim().is_empty() {
            bail!("namespace network is required");
        }
        if self.namespace.network_id.trim().is_empty() {
            bail!("namespace network_id is required");
        }
        if self.protocol_version.trim().is_empty() {
            bail!("protocol_version is required");
        }
        if self.identify_agent_version.trim().is_empty() {
            bail!("identify_agent_version is required");
        }
        if self.max_established_per_peer == 0 {
            bail!("max_established_per_peer must be > 0");
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
pub type BackfillResponseChannel = request_response::ResponseChannel<RawBackfillResponse>;

#[derive(Debug)]
pub enum SubstrateBehaviourEvent {
    Gossipsub(GossipsubEvent),
    Identify(identify::Event),
    Kademlia(kad::Event),
    Relay(relay::client::Event),
    Autonat(autonat::Event),
    Dcutr(dcutr::Event),
    RequestResponse(request_response::Event<RawBackfillRequest, RawBackfillResponse>),
    Mdns(mdns::Event),
}

impl From<GossipsubEvent> for SubstrateBehaviourEvent {
    fn from(value: GossipsubEvent) -> Self {
        Self::Gossipsub(value)
    }
}

impl From<identify::Event> for SubstrateBehaviourEvent {
    fn from(value: identify::Event) -> Self {
        Self::Identify(value)
    }
}

impl From<kad::Event> for SubstrateBehaviourEvent {
    fn from(value: kad::Event) -> Self {
        Self::Kademlia(value)
    }
}

impl From<relay::client::Event> for SubstrateBehaviourEvent {
    fn from(value: relay::client::Event) -> Self {
        Self::Relay(value)
    }
}

impl From<autonat::Event> for SubstrateBehaviourEvent {
    fn from(value: autonat::Event) -> Self {
        Self::Autonat(value)
    }
}

impl From<dcutr::Event> for SubstrateBehaviourEvent {
    fn from(value: dcutr::Event) -> Self {
        Self::Dcutr(value)
    }
}

impl From<request_response::Event<RawBackfillRequest, RawBackfillResponse>>
    for SubstrateBehaviourEvent
{
    fn from(value: request_response::Event<RawBackfillRequest, RawBackfillResponse>) -> Self {
        Self::RequestResponse(value)
    }
}

impl From<mdns::Event> for SubstrateBehaviourEvent {
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

impl From<std::convert::Infallible> for SubstrateBehaviourEvent {
    fn from(v: std::convert::Infallible) -> Self {
        match v {}
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "SubstrateBehaviourEvent")]
pub struct SubstrateBehaviour {
    pub connection_limits: connection_limits::Behaviour,
    pub gossipsub: Gossipsub,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub relay_client: relay::client::Behaviour,
    pub autonat: autonat::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub request_response:
        request_response::cbor::Behaviour<RawBackfillRequest, RawBackfillResponse>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
}

#[derive(Debug)]
pub enum SubstrateRuntimeEvent {
    NewListenAddr {
        address: Multiaddr,
    },
    ConnectionEstablished {
        peer: PeerId,
    },
    ConnectionClosed {
        peer: PeerId,
        remaining_established: u32,
    },
    PeerHandshakeRejected {
        peer: PeerId,
        detail: String,
    },
    Gossip {
        propagation_source: PeerId,
        message: RawGossipMessage,
    },
    BackfillRequest {
        peer: PeerId,
        request: RawBackfillRequest,
        channel: BackfillResponseChannel,
    },
    BackfillResponse {
        peer: PeerId,
        request_id: BackfillRequestId,
        response: RawBackfillResponse,
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

pub struct SubstrateNode {
    config: SubstrateConfig,
    local_key: identity::Keypair,
    local_peer_id: PeerId,
}

struct BehaviourSeedParts {
    gossipsub: Gossipsub,
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
    autonat: autonat::Behaviour,
    request_response: request_response::cbor::Behaviour<RawBackfillRequest, RawBackfillResponse>,
    mdns: Toggle<mdns::tokio::Behaviour>,
    connection_limits: connection_limits::Behaviour,
}

impl SubstrateNode {
    pub fn new(config: SubstrateConfig, local_key: identity::Keypair) -> Result<Self> {
        config.validate()?;
        let local_peer_id = local_key.public().to_peer_id();
        Ok(Self {
            config,
            local_key,
            local_peer_id,
        })
    }

    pub fn generate(config: SubstrateConfig) -> Result<Self> {
        Self::new(config, identity::Keypair::generate_ed25519())
    }

    pub fn config(&self) -> &SubstrateConfig {
        &self.config
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<Multiaddr>> {
        self.config.parse_listen_addrs()
    }

    fn build_behaviour_seed_parts(
        config: &SubstrateConfig,
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
                .with_agent_version(config.identify_agent_version.clone()),
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
                .with_max_established_per_peer(Some(config.max_established_per_peer))
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

    pub fn build_behaviour(&self) -> Result<SubstrateBehaviour> {
        let (_, relay_client) = relay::client::new(self.local_peer_id);
        let parts =
            Self::build_behaviour_seed_parts(&self.config, &self.local_key, self.local_peer_id)?;
        Ok(SubstrateBehaviour {
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

    pub fn build_swarm(&self) -> Result<Swarm<SubstrateBehaviour>> {
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
            .with_behaviour(move |_, relay_client| SubstrateBehaviour {
                connection_limits: parts.connection_limits,
                gossipsub: parts.gossipsub,
                identify: parts.identify,
                kademlia: parts.kademlia,
                relay_client,
                autonat: parts.autonat,
                dcutr: dcutr::Behaviour::new(local_peer_id),
                request_response: parts.request_response,
                mdns: parts.mdns,
            })?
            .build())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrafficGuardPeerHealth {
    pub peer: String,
    pub score: i64,
    pub blacklisted: bool,
    pub reputation_tier: String,
    pub quarantined: bool,
    pub quarantine_remaining_ms: u64,
    pub ban_remaining_ms: u64,
    pub throttle_factor_percent: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkRuntimeObservabilitySnapshot {
    pub nat_status: String,
    pub nat_public_address: Option<String>,
    pub nat_confidence: u32,
    pub relay_reservations: Vec<String>,
    pub peer_health: Vec<TrafficGuardPeerHealth>,
}

#[derive(Debug)]
struct TrafficGuard {
    seen_messages: HashMap<u64, Instant>,
    peer_windows: HashMap<PeerId, Vec<Instant>>,
    peer_topic_windows: HashMap<(PeerId, String), Vec<Instant>>,
    local_publish_windows: HashMap<String, Vec<Instant>>,
    backfill_request_windows: HashMap<PeerId, Vec<Instant>>,
    peer_scores: HashMap<PeerId, i64>,
    quarantined_peers: HashMap<PeerId, Instant>,
    banned_peers: HashMap<PeerId, Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrafficPolicyTier {
    Healthy,
    Throttled,
    Quarantined,
    Banned,
}

impl TrafficPolicyTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Throttled => "throttled",
            Self::Quarantined => "quarantined",
            Self::Banned => "banned",
        }
    }

    fn throttle_factor_percent(self) -> u32 {
        match self {
            Self::Healthy => 100,
            Self::Throttled => 50,
            Self::Quarantined | Self::Banned => 0,
        }
    }
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
            quarantined_peers: HashMap::new(),
            banned_peers: HashMap::new(),
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
        self.gc_peer_actions(now);
        let policy = self.policy_for_peer(source_peer, now);
        if matches!(
            policy,
            TrafficPolicyTier::Quarantined | TrafficPolicyTier::Banned
        ) {
            return false;
        }

        let digest = traffic_digest(data);
        self.gc_seen(now);
        if self
            .seen_messages
            .get(&digest)
            .is_some_and(|seen_at| now.duration_since(*seen_at) <= TRAFFIC_DEDUPE_TTL)
        {
            self.penalize(source_peer, 1, now);
            return false;
        }
        self.seen_messages.insert(digest, now);

        let peer_limit = throttled_limit(PER_PEER_MSGS_PER_WINDOW, policy);
        let peer_window = self.peer_windows.entry(source_peer).or_default();
        prune_window(peer_window, now, TRAFFIC_WINDOW);
        if peer_window.len() >= peer_limit {
            self.penalize(source_peer, 3, now);
            return false;
        }
        peer_window.push(now);

        let topic_limit = throttled_limit(PER_PEER_TOPIC_MSGS_PER_WINDOW, policy);
        let topic_window = self
            .peer_topic_windows
            .entry((source_peer, topic.to_owned()))
            .or_default();
        prune_window(topic_window, now, TRAFFIC_WINDOW);
        if topic_window.len() >= topic_limit {
            self.penalize(source_peer, 2, now);
            return false;
        }
        topic_window.push(now);

        self.reward(source_peer, 1, now);
        true
    }

    fn allow_backfill_request(&mut self, peer: PeerId, now: Instant) -> bool {
        self.gc_peer_actions(now);
        let policy = self.policy_for_peer(peer, now);
        if matches!(
            policy,
            TrafficPolicyTier::Quarantined | TrafficPolicyTier::Banned
        ) {
            return false;
        }
        let backfill_limit = throttled_limit(PER_PEER_BACKFILL_REQUESTS_PER_WINDOW, policy);
        let window = self.backfill_request_windows.entry(peer).or_default();
        prune_window(window, now, TRAFFIC_WINDOW);
        if window.len() >= backfill_limit {
            self.penalize(peer, 2, now);
            return false;
        }
        window.push(now);
        true
    }

    fn penalize(&mut self, peer: PeerId, amount: i64, now: Instant) {
        let score = self.peer_scores.entry(peer).or_insert(0);
        *score -= amount;
        if *score <= TRAFFIC_BAN_SCORE {
            self.banned_peers.insert(peer, now + TRAFFIC_BAN_DURATION);
            self.quarantined_peers.remove(&peer);
        } else if *score <= TRAFFIC_QUARANTINE_SCORE {
            let until = now + TRAFFIC_QUARANTINE_DURATION;
            self.quarantined_peers
                .entry(peer)
                .and_modify(|existing| {
                    if *existing < until {
                        *existing = until;
                    }
                })
                .or_insert(until);
        }
    }

    fn reward(&mut self, peer: PeerId, amount: i64, now: Instant) {
        self.gc_peer_actions(now);
        let score = self.peer_scores.entry(peer).or_insert(0);
        *score = (*score + amount).min(100);
        if *score > TRAFFIC_QUARANTINE_SCORE {
            self.quarantined_peers.remove(&peer);
        }
    }

    fn gc_seen(&mut self, now: Instant) {
        self.seen_messages
            .retain(|_, seen_at| now.duration_since(*seen_at) <= TRAFFIC_DEDUPE_TTL);
    }

    fn gc_peer_actions(&mut self, now: Instant) {
        self.quarantined_peers.retain(|_, until| *until > now);
        self.banned_peers.retain(|_, until| *until > now);
    }

    fn policy_for_peer(&self, peer: PeerId, now: Instant) -> TrafficPolicyTier {
        if self
            .banned_peers
            .get(&peer)
            .is_some_and(|until| *until > now)
        {
            return TrafficPolicyTier::Banned;
        }
        if self
            .quarantined_peers
            .get(&peer)
            .is_some_and(|until| *until > now)
        {
            return TrafficPolicyTier::Quarantined;
        }
        let score = self.peer_scores.get(&peer).copied().unwrap_or(0);
        if score <= TRAFFIC_THROTTLED_SCORE {
            TrafficPolicyTier::Throttled
        } else {
            TrafficPolicyTier::Healthy
        }
    }

    fn quarantine_remaining_ms(&self, peer: PeerId, now: Instant) -> u64 {
        self.quarantined_peers
            .get(&peer)
            .map(|until| until.saturating_duration_since(now).as_millis() as u64)
            .unwrap_or(0)
    }

    fn ban_remaining_ms(&self, peer: PeerId, now: Instant) -> u64 {
        self.banned_peers
            .get(&peer)
            .map(|until| until.saturating_duration_since(now).as_millis() as u64)
            .unwrap_or(0)
    }

    fn note_backfill_failure(&mut self, peer: PeerId, now: Instant) {
        self.penalize(peer, TRAFFIC_FAILURE_PENALTY, now);
    }

    fn note_handshake_rejection(&mut self, peer: PeerId, now: Instant) {
        self.penalize(peer, TRAFFIC_QUARANTINE_SCORE.abs(), now);
    }

    fn allows_outbound_backfill(&self, peer: PeerId, now: Instant) -> bool {
        !matches!(
            self.policy_for_peer(peer, now),
            TrafficPolicyTier::Quarantined | TrafficPolicyTier::Banned
        )
    }
}

pub struct SubstrateRuntime {
    config: SubstrateConfig,
    local_peer_id: PeerId,
    swarm: Swarm<SubstrateBehaviour>,
    listen_addrs: Vec<Multiaddr>,
    subscribed_topics: HashSet<String>,
    traffic_guard: TrafficGuard,
    relay_reservations: HashSet<String>,
    expected_agent_version_prefix: String,
    expected_peer_handshake: PeerHandshakeMetadata,
    nat_status: String,
    nat_public_address: Option<Multiaddr>,
    nat_confidence: u32,
}

impl SubstrateRuntime {
    pub fn new(node: SubstrateNode) -> Result<Self> {
        let local_peer_id = node.local_peer_id();
        let config = node.config.clone();
        let (expected_agent_version_prefix, expected_peer_handshake) =
            PeerHandshakeMetadata::decode_agent_version_parts(&config.identify_agent_version)?;
        let swarm = node.build_swarm()?;
        let mut this = Self {
            config,
            local_peer_id,
            swarm,
            listen_addrs: Vec::new(),
            subscribed_topics: HashSet::new(),
            traffic_guard: TrafficGuard::new(),
            relay_reservations: HashSet::new(),
            expected_agent_version_prefix,
            expected_peer_handshake,
            nat_status: "unknown".to_owned(),
            nat_public_address: None,
            nat_confidence: 0,
        };
        this.listen_on_configured_addrs()?;
        this.seed_bootstrap_peers()?;
        Ok(this)
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn config(&self) -> &SubstrateConfig {
        &self.config
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        &self.listen_addrs
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        let now = Instant::now();
        let mut peer_health = self
            .traffic_guard
            .peer_scores
            .iter()
            .map(|(peer, score)| {
                let policy = self.traffic_guard.policy_for_peer(*peer, now);
                TrafficGuardPeerHealth {
                    peer: peer.to_string(),
                    score: *score,
                    blacklisted: matches!(policy, TrafficPolicyTier::Banned),
                    reputation_tier: policy.as_str().to_owned(),
                    quarantined: matches!(policy, TrafficPolicyTier::Quarantined),
                    quarantine_remaining_ms: self.traffic_guard.quarantine_remaining_ms(*peer, now),
                    ban_remaining_ms: self.traffic_guard.ban_remaining_ms(*peer, now),
                    throttle_factor_percent: policy.throttle_factor_percent(),
                }
            })
            .collect::<Vec<_>>();
        peer_health.sort_by(|left, right| left.peer.cmp(&right.peer));

        let mut relay_reservations = self.relay_reservations.iter().cloned().collect::<Vec<_>>();
        relay_reservations.sort();

        NetworkRuntimeObservabilitySnapshot {
            nat_status: self.nat_status.clone(),
            nat_public_address: self.nat_public_address.as_ref().map(ToString::to_string),
            nat_confidence: self.nat_confidence,
            relay_reservations,
            peer_health,
        }
    }

    pub fn allows_outbound_backfill_to(&self, peer: &PeerId) -> bool {
        self.traffic_guard
            .allows_outbound_backfill(*peer, Instant::now())
    }

    pub fn validate_identify_info(&self, info: &identify::Info) -> Result<Vec<Multiaddr>> {
        if info.protocol_version != self.config.protocol_version {
            bail!(
                "identify protocol_version mismatch expected={} got={}",
                self.config.protocol_version,
                info.protocol_version
            );
        }
        let (remote_prefix, remote) =
            PeerHandshakeMetadata::decode_agent_version_parts(&info.agent_version)?;
        if remote_prefix != self.expected_agent_version_prefix {
            bail!(
                "identify agent prefix mismatch expected={} got={}",
                self.expected_agent_version_prefix,
                remote_prefix
            );
        }
        if remote.network_id != self.expected_peer_handshake.network_id {
            bail!(
                "identify network_id mismatch expected={} got={}",
                self.expected_peer_handshake.network_id,
                remote.network_id
            );
        }
        if remote.params_version != self.expected_peer_handshake.params_version {
            bail!(
                "identify params_version mismatch expected={} got={}",
                self.expected_peer_handshake.params_version,
                remote.params_version
            );
        }
        if remote.params_hash != self.expected_peer_handshake.params_hash {
            bail!(
                "identify params_hash mismatch expected={} got={}",
                self.expected_peer_handshake.params_hash,
                remote.params_hash
            );
        }
        Ok(info.listen_addrs.clone())
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

    pub fn publish(&mut self, scope: &SwarmScope, kind: TopicKind, payload: &[u8]) -> Result<()> {
        let topic_name = self.config.namespace.topic_name(scope, kind)?;
        if !self
            .traffic_guard
            .allow_local_publish(&topic_name, Instant::now())
        {
            bail!("LocalTopicRateLimited");
        }
        let topic = IdentTopic::new(topic_name);
        let envelope = RawGossipMessage {
            scope: scope.clone(),
            kind,
            payload: payload.to_vec(),
        };
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, envelope.encode_json()?)
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }

    pub fn send_backfill_request(
        &mut self,
        peer: &PeerId,
        request: RawBackfillRequest,
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
        response: RawBackfillResponse,
    ) -> Result<()> {
        self.swarm
            .behaviour_mut()
            .request_response
            .send_response(channel, response)
            .map_err(|_| anyhow!("backfill response channel closed"))?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> Result<SubstrateRuntimeEvent> {
        loop {
            let swarm_event = self.swarm.select_next_some().await;
            if let Some(event) = self.handle_swarm_event(swarm_event)? {
                return Ok(event);
            }
        }
    }

    pub fn try_next_event(&mut self) -> Result<Option<SubstrateRuntimeEvent>> {
        loop {
            let Some(event) = self.swarm.select_next_some().now_or_never() else {
                return Ok(None);
            };
            if let Some(runtime_event) = self.handle_swarm_event(event)? {
                return Ok(Some(runtime_event));
            }
        }
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
        if self.swarm.is_connected(&peer) {
            self.register_peer_address(peer, addr);
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
        event: SwarmEvent<SubstrateBehaviourEvent>,
    ) -> Result<Option<SubstrateRuntimeEvent>> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                if !self.listen_addrs.iter().any(|seen| seen == &address) {
                    self.listen_addrs.push(address.clone());
                }
                Ok(Some(SubstrateRuntimeEvent::NewListenAddr { address }))
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                self.trigger_kademlia_refresh();
                Ok(Some(SubstrateRuntimeEvent::ConnectionEstablished {
                    peer: peer_id,
                }))
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => Ok(Some(SubstrateRuntimeEvent::ConnectionClosed {
                peer: peer_id,
                remaining_established: num_established,
            })),
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Gossipsub(
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
                Ok(Some(SubstrateRuntimeEvent::Gossip {
                    propagation_source,
                    message: RawGossipMessage::decode_json(&message.data)?,
                }))
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::RequestResponse(
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
                    Ok(Some(SubstrateRuntimeEvent::BackfillRequest {
                        peer,
                        request,
                        channel,
                    }))
                }
                RequestResponseMessage::Response {
                    request_id,
                    response,
                } => Ok(Some(SubstrateRuntimeEvent::BackfillResponse {
                    peer,
                    request_id,
                    response,
                })),
            },
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure {
                    peer,
                    request_id,
                    error,
                    ..
                },
            )) => {
                self.traffic_guard
                    .note_backfill_failure(peer, Instant::now());
                Ok(Some(SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer,
                    request_id,
                    error: error.to_string(),
                }))
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::RequestResponse(
                request_response::Event::InboundFailure { peer, error, .. },
            )) => {
                self.traffic_guard
                    .note_backfill_failure(peer, Instant::now());
                Ok(Some(SubstrateRuntimeEvent::BackfillInboundFailure {
                    peer,
                    error: error.to_string(),
                }))
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Mdns(mdns::Event::Discovered(
                discovered,
            ))) => {
                for (peer, addr) in discovered {
                    self.maybe_dial_peer(peer, addr);
                }
                Ok(None)
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Mdns(mdns::Event::Expired(expired))) => {
                for (peer, _) in expired {
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer);
                }
                Ok(None)
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Identify(
                identify::Event::Received { peer_id, info, .. },
            )) => match self.validate_identify_info(&info) {
                Ok(listen_addrs) => {
                    for addr in listen_addrs {
                        self.register_peer_address(peer_id, addr);
                    }
                    self.trigger_kademlia_refresh();
                    Ok(None)
                }
                Err(err) => {
                    self.traffic_guard
                        .note_handshake_rejection(peer_id, Instant::now());
                    let detail = err.to_string();
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                    Ok(Some(SubstrateRuntimeEvent::PeerHandshakeRejected {
                        peer: peer_id,
                        detail,
                    }))
                }
            },
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Relay(
                relay::client::Event::ReservationReqAccepted {
                    relay_peer_id,
                    renewal,
                    ..
                },
            )) => Ok(Some(SubstrateRuntimeEvent::RelayReservationAccepted {
                relay_peer: relay_peer_id,
                renewal,
            })),
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Relay(
                relay::client::Event::OutboundCircuitEstablished { relay_peer_id, .. },
            )) => Ok(Some(SubstrateRuntimeEvent::RelayCircuitEstablished {
                relay_peer: relay_peer_id,
            })),
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Relay(
                relay::client::Event::InboundCircuitEstablished { src_peer_id, .. },
            )) => Ok(Some(
                SubstrateRuntimeEvent::RelayInboundCircuitEstablished {
                    source_peer: src_peer_id,
                },
            )),
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Autonat(
                autonat::Event::StatusChanged { old, new },
            )) => {
                let old_label = nat_status_label(&old);
                let new_label = nat_status_label(&new);
                let public_address = self.swarm.behaviour().autonat.public_address().cloned();
                let confidence = self.swarm.behaviour().autonat.confidence();
                self.nat_status = new_label.clone();
                self.nat_public_address = public_address.clone();
                self.nat_confidence = confidence as u32;
                Ok(Some(SubstrateRuntimeEvent::NatStatusChanged {
                    old: old_label,
                    new: new_label,
                    public_address,
                    confidence,
                }))
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Dcutr(dcutr::Event {
                remote_peer_id,
                result,
            })) => match result {
                Ok(_) => Ok(Some(
                    SubstrateRuntimeEvent::DcutrConnectionUpgradeSucceeded {
                        remote_peer: remote_peer_id,
                    },
                )),
                Err(err) => Ok(Some(SubstrateRuntimeEvent::DcutrConnectionUpgradeFailed {
                    remote_peer: remote_peer_id,
                    error: err.to_string(),
                })),
            },
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Kademlia(
                kad::Event::RoutingUpdated {
                    peer, addresses, ..
                },
            )) => {
                for addr in addresses.into_vec() {
                    self.maybe_dial_peer(peer, addr);
                }
                Ok(None)
            }
            SwarmEvent::Behaviour(SubstrateBehaviourEvent::Kademlia(
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

fn throttled_limit(base: usize, policy: TrafficPolicyTier) -> usize {
    let factor = policy.throttle_factor_percent() as usize;
    ((base.saturating_mul(factor)).max(100)) / 100
}

fn nat_status_label(status: &autonat::NatStatus) -> String {
    match status {
        autonat::NatStatus::Public(_) => "public".to_owned(),
        autonat::NatStatus::Private => "private".to_owned(),
        autonat::NatStatus::Unknown => "unknown".to_owned(),
    }
}

pub fn relay_reservation_addr(relay_peer: PeerId, relay_addr: &Multiaddr) -> Multiaddr {
    relay_addr
        .clone()
        .with(Protocol::P2p(relay_peer))
        .with(Protocol::P2pCircuit)
}

pub fn sanitize_segment(raw: &str) -> Result<String> {
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

fn default_network_context_id() -> String {
    "default".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_catalog_uses_namespace_scope_and_kind() {
        let namespace = TopicNamespace {
            network: "swarmnet".to_owned(),
            network_id: "main/alpha".to_owned(),
        };
        let catalog =
            TopicCatalog::new(&namespace, &SwarmScope::Region("sol-1/alpha".to_owned())).unwrap();
        assert_eq!(
            catalog.events,
            "swarmnet.main-alpha.region.sol-1-alpha.events"
        );
        assert_eq!(
            catalog.checkpoints,
            "swarmnet.main-alpha.region.sol-1-alpha.checkpoints"
        );
    }

    #[test]
    fn backfill_request_validate_enforces_bounds() {
        let req = RawBackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 10,
            limit: 5,
            feed_key: Some("feed".to_owned()),
        };
        req.validate(10, 20).unwrap();
        assert!(
            RawBackfillRequest {
                limit: 0,
                ..req.clone()
            }
            .validate(10, 20)
            .is_err()
        );
        assert!(
            RawBackfillRequest { limit: 25, ..req }
                .validate(10, 20)
                .is_err()
        );
    }

    #[test]
    fn peer_handshake_roundtrip() {
        let handshake = PeerHandshakeMetadata {
            network_id: "mainnet".to_owned(),
            params_version: 7,
            params_hash: "abc123".to_owned(),
        };
        let encoded = handshake.encode_agent_version();
        let decoded = PeerHandshakeMetadata::decode_agent_version(&encoded).unwrap();
        assert_eq!(decoded, handshake);
    }

    #[test]
    fn config_parses_multiaddrs() {
        let config = SubstrateConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/4101".to_owned()],
            bootstrap_peers: vec![
                "/ip4/127.0.0.1/tcp/4102/p2p/12D3KooWLUtmMxdKgf6D9A7feN4zkRgZNpmjQe7YQDdyCjTiMQuH"
                    .to_owned(),
            ],
            ..SubstrateConfig::default()
        };
        assert_eq!(config.parse_listen_addrs().unwrap().len(), 1);
        assert_eq!(config.parse_bootstrap_peers().unwrap().len(), 1);
    }
}
