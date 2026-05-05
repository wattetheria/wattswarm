use anyhow::{Result, anyhow};
use libp2p::identify;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, path::PathBuf};
use wattswarm_network_substrate::{
    self as substrate, RawBackfillResponse, RawGossipMessage, SubstrateConfig, SubstrateNode,
    SubstrateRuntime, SubstrateRuntimeEvent,
};
use wattswarm_protocol::types::{Event, NetworkProtocolParams};
#[cfg(test)]
use wattswarm_protocol::types::{EventKind, EventPayload};

#[cfg(feature = "iroh-runtime")]
pub mod iroh_runtime;

pub use substrate::{
    BackfillResponseChannel, ContactMaterialResponseChannel, GossipKind, Multiaddr,
    NetworkRuntimeObservabilitySnapshot, PeerDirectMessageResponseChannel, PeerDiscoverySourceKind,
    PeerHandshakeMetadata, PeerId, PeerIdentificationMetadata, PeerRelationshipResponseChannel,
    RawAgentEnvelope, RawContactMaterial, RawContactMaterialRequest, RawContactMaterialResponse,
    RawPeerDirectMessageKind, RawPeerRelationshipAction, SwarmScope, TopicCatalog, TopicNamespace,
    TrafficGuardPeerHealth, relay_reservation_addr, sanitize_segment,
};

pub type BackfillRequest = substrate::RawBackfillRequest;
pub type ContactMaterialRequest = substrate::RawContactMaterialRequest;
pub type ContactMaterialResponse = substrate::RawContactMaterialResponse;
pub type PeerDirectMessageRequest = substrate::RawPeerDirectMessageRequest;
pub type PeerDirectMessageResponse = substrate::RawPeerDirectMessageResponse;
pub type PeerRelationshipRequest = substrate::RawPeerRelationshipRequest;
pub type PeerRelationshipResponse = substrate::RawPeerRelationshipResponse;
pub type WattSwarmBehaviour = substrate::SubstrateBehaviour;
pub type WattSwarmBehaviourEvent = substrate::SubstrateBehaviourEvent;

macro_rules! define_outbound_request_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        pub struct $name(u64);

        impl $name {
            pub(crate) fn new(value: u64) -> Self {
                Self(value)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.0)
            }
        }
    };
}

define_outbound_request_id!(BackfillRequestId);
define_outbound_request_id!(ContactMaterialRequestId);
define_outbound_request_id!(PeerRelationshipRequestId);
define_outbound_request_id!(PeerDirectMessageRequestId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InboundRequestId(u64);

impl std::fmt::Display for InboundRequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

pub const WATTSWARM_IDENTIFY_AGENT_PREFIX: &str = "wattswarm-network-p2p";

pub fn encode_wattswarm_agent_version(metadata: &PeerHandshakeMetadata) -> String {
    metadata.encode_agent_version_with_prefix(WATTSWARM_IDENTIFY_AGENT_PREFIX)
}

pub fn peer_id_from_ed25519_public_key(public_key_32: [u8; 32]) -> Result<PeerId> {
    let public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(&public_key_32)
        .map_err(|err| anyhow!("parse libp2p ed25519 public key: {err}"))?;
    let public_key = libp2p::identity::PublicKey::from(public_key);
    Ok(PeerId::from_public_key(&public_key))
}

pub fn keypair_from_ed25519_secret_bytes(
    secret_key_32: [u8; 32],
) -> Result<libp2p::identity::Keypair> {
    libp2p::identity::Keypair::ed25519_from_bytes(secret_key_32)
        .map_err(|err| anyhow!("parse libp2p ed25519 secret key: {err}"))
}

pub fn bootstrap_http_base_url(raw_addr: &str, port: u16) -> Result<String> {
    use libp2p::multiaddr::Protocol;

    let addr = raw_addr.parse::<Multiaddr>()?;
    let mut host = None;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(ip) => {
                host = Some(ip.to_string());
                break;
            }
            Protocol::Ip6(ip) => {
                host = Some(format!("[{ip}]"));
                break;
            }
            Protocol::Dns(name)
            | Protocol::Dns4(name)
            | Protocol::Dns6(name)
            | Protocol::Dnsaddr(name) => {
                host = Some(name.to_string());
                break;
            }
            _ => {}
        }
    }
    let host = host.ok_or_else(|| anyhow!("bootstrap multiaddr missing routable host"))?;
    Ok(format!("http://{host}:{port}"))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub scope: SwarmScope,
    pub event: Event,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_source_node_id: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub head_event_ids: Vec<String>,
    pub events: Vec<EventEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleAnnouncement {
    pub scope: SwarmScope,
    pub rule_set: String,
    pub rule_version: u64,
    pub activation_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signer_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signature_hex: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointAnnouncement {
    pub scope: SwarmScope,
    pub checkpoint_id: String,
    pub artifact_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signer_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signature_hex: Option<String>,
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
    Chat(EventEnvelope),
    Rule(RuleAnnouncement),
    Checkpoint(CheckpointAnnouncement),
    Summary(SummaryAnnouncement),
}

impl GossipMessage {
    pub fn kind(&self) -> GossipKind {
        match self {
            Self::Event(_) => GossipKind::Events,
            Self::Chat(_) => GossipKind::Messages,
            Self::Rule(_) => GossipKind::Rules,
            Self::Checkpoint(_) => GossipKind::Checkpoints,
            Self::Summary(_) => GossipKind::Summaries,
        }
    }

    pub fn scope(&self) -> &SwarmScope {
        match self {
            Self::Event(payload) => &payload.scope,
            Self::Chat(payload) => &payload.scope,
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

impl Default for NetworkP2pConfig {
    fn default() -> Self {
        let mut config = Self::from_substrate(SubstrateConfig::default());
        config.identify_agent_version =
            encode_wattswarm_agent_version(&PeerHandshakeMetadata::default());
        config
    }
}

impl NetworkP2pConfig {
    fn from_substrate(config: SubstrateConfig) -> Self {
        Self {
            namespace: config.namespace,
            protocol_version: config.protocol_version,
            identify_agent_version: config.identify_agent_version,
            listen_addrs: config.listen_addrs,
            bootstrap_peers: config.bootstrap_peers,
            enable_mdns: config.enable_mdns,
            max_established_per_peer: config.max_established_per_peer,
            gossipsub_d: config.gossipsub_d,
            gossipsub_d_low: config.gossipsub_d_low,
            gossipsub_d_high: config.gossipsub_d_high,
            gossipsub_heartbeat_ms: config.gossipsub_heartbeat_ms,
            gossipsub_max_transmit_size: config.gossipsub_max_transmit_size,
            max_backfill_events: config.max_backfill_events,
            max_backfill_events_hard_limit: config.max_backfill_events_hard_limit,
        }
    }

    fn as_substrate(&self) -> SubstrateConfig {
        SubstrateConfig {
            namespace: self.namespace.clone(),
            protocol_version: self.protocol_version.clone(),
            identify_agent_version: self.identify_agent_version.clone(),
            listen_addrs: self.listen_addrs.clone(),
            bootstrap_peers: self.bootstrap_peers.clone(),
            enable_mdns: self.enable_mdns,
            max_established_per_peer: self.max_established_per_peer,
            gossipsub_d: self.gossipsub_d,
            gossipsub_d_low: self.gossipsub_d_low,
            gossipsub_d_high: self.gossipsub_d_high,
            gossipsub_heartbeat_ms: self.gossipsub_heartbeat_ms,
            gossipsub_max_transmit_size: self.gossipsub_max_transmit_size,
            max_backfill_events: self.max_backfill_events,
            max_backfill_events_hard_limit: self.max_backfill_events_hard_limit,
        }
    }

    pub fn apply_protocol_params(mut self, params: &NetworkProtocolParams) -> Self {
        let network_id = self.namespace.network_id.clone();
        self.namespace = TopicNamespace {
            network: params.namespace_network.clone(),
            network_id,
        };
        self.protocol_version = params.protocol_version.clone();
        self.max_established_per_peer = params.max_established_per_peer;
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
        self.as_substrate().validate()
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<Multiaddr>> {
        self.as_substrate().parse_listen_addrs()
    }

    pub fn parse_bootstrap_peers(&self) -> Result<Vec<(PeerId, Multiaddr)>> {
        self.as_substrate().parse_bootstrap_peers()
    }

    pub fn topic_catalog(&self, scope: &SwarmScope) -> Result<TopicCatalog> {
        self.as_substrate().topic_catalog(scope)
    }
}

pub struct NetworkP2pNode {
    config: NetworkP2pConfig,
    inner: SubstrateNode,
    iroh_state_dir: Option<PathBuf>,
}

impl NetworkP2pNode {
    pub fn new(config: NetworkP2pConfig, local_key: libp2p::identity::Keypair) -> Result<Self> {
        let inner = SubstrateNode::new(config.as_substrate(), local_key)?;
        Ok(Self {
            config,
            inner,
            iroh_state_dir: None,
        })
    }

    pub fn from_ed25519_secret_bytes(
        config: NetworkP2pConfig,
        secret_key_32: [u8; 32],
    ) -> Result<Self> {
        Self::new(config, keypair_from_ed25519_secret_bytes(secret_key_32)?)
    }

    pub fn generate(config: NetworkP2pConfig) -> Result<Self> {
        let inner = SubstrateNode::generate(config.as_substrate())?;
        Ok(Self {
            config,
            inner,
            iroh_state_dir: None,
        })
    }

    #[cfg(feature = "iroh-runtime")]
    pub fn from_iroh_state_dir(
        config: NetworkP2pConfig,
        state_dir: impl Into<PathBuf>,
        secret_key_32: [u8; 32],
    ) -> Result<Self> {
        let mut node = Self::from_ed25519_secret_bytes(config, secret_key_32)?;
        node.iroh_state_dir = Some(state_dir.into());
        Ok(node)
    }

    pub fn is_iroh_backed(&self) -> bool {
        self.iroh_state_dir.is_some()
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.inner.local_peer_id()
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<Multiaddr>> {
        self.config.parse_listen_addrs()
    }

    pub fn build_behaviour(&self) -> Result<WattSwarmBehaviour> {
        self.inner.build_behaviour()
    }

    pub fn build_swarm(&self) -> Result<libp2p::Swarm<WattSwarmBehaviour>> {
        self.inner.build_swarm()
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkRuntimeEvent {
    PeerDiscovered {
        peer: PeerId,
        address: Multiaddr,
        source: PeerDiscoverySourceKind,
    },
    PeerIdentified {
        peer: PeerId,
        metadata: PeerIdentificationMetadata,
    },
    NewListenAddr {
        address: Multiaddr,
    },
    ConnectionEstablished {
        peer: PeerId,
        remote_addr: Multiaddr,
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
        message: GossipMessage,
    },
    BackfillRequest {
        peer: PeerId,
        request: BackfillRequest,
        request_id: InboundRequestId,
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
    ContactMaterialRequest {
        peer: PeerId,
        request: ContactMaterialRequest,
        request_id: InboundRequestId,
    },
    ContactMaterialResponse {
        peer: PeerId,
        request_id: ContactMaterialRequestId,
        response: ContactMaterialResponse,
    },
    ContactMaterialOutboundFailure {
        peer: PeerId,
        request_id: ContactMaterialRequestId,
        error: String,
    },
    ContactMaterialInboundFailure {
        peer: PeerId,
        error: String,
    },
    PeerRelationshipRequest {
        peer: PeerId,
        request: PeerRelationshipRequest,
        request_id: InboundRequestId,
    },
    PeerRelationshipResponse {
        peer: PeerId,
        request_id: PeerRelationshipRequestId,
        response: PeerRelationshipResponse,
    },
    PeerRelationshipOutboundFailure {
        peer: PeerId,
        request_id: PeerRelationshipRequestId,
        error: String,
    },
    PeerRelationshipInboundFailure {
        peer: PeerId,
        error: String,
    },
    PeerDirectMessageRequest {
        peer: PeerId,
        request: PeerDirectMessageRequest,
        request_id: InboundRequestId,
    },
    PeerDirectMessageResponse {
        peer: PeerId,
        request_id: PeerDirectMessageRequestId,
        response: PeerDirectMessageResponse,
    },
    PeerDirectMessageOutboundFailure {
        peer: PeerId,
        request_id: PeerDirectMessageRequestId,
        error: String,
    },
    PeerDirectMessageInboundFailure {
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

pub struct NetworkRuntime {
    config: NetworkP2pConfig,
    backend: NetworkRuntimeBackend,
    local_peer_id: PeerId,
    iroh_listen_addrs: Vec<Multiaddr>,
    next_inbound_request_id: u64,
    next_outbound_request_id: u64,
    backfill_response_channels: HashMap<InboundRequestId, BackfillResponseChannel>,
    contact_material_response_channels: HashMap<InboundRequestId, ContactMaterialResponseChannel>,
    peer_relationship_response_channels: HashMap<InboundRequestId, PeerRelationshipResponseChannel>,
    peer_direct_message_response_channels:
        HashMap<InboundRequestId, PeerDirectMessageResponseChannel>,
    legacy_backfill_request_ids: HashMap<substrate::BackfillRequestId, BackfillRequestId>,
    legacy_contact_material_request_ids:
        HashMap<substrate::ContactMaterialRequestId, ContactMaterialRequestId>,
    legacy_peer_relationship_request_ids:
        HashMap<substrate::PeerRelationshipRequestId, PeerRelationshipRequestId>,
    legacy_peer_direct_message_request_ids:
        HashMap<substrate::PeerDirectMessageRequestId, PeerDirectMessageRequestId>,
}

#[allow(clippy::large_enum_variant)]
enum NetworkRuntimeBackend {
    Legacy(SubstrateRuntime),
    #[cfg(feature = "iroh-runtime")]
    Iroh(iroh_runtime::IrohNetworkRuntime),
}

impl NetworkRuntime {
    pub fn new(node: NetworkP2pNode) -> Result<Self> {
        let config = node.config.clone();
        let local_peer_id = node.local_peer_id();
        let backend = match node.iroh_state_dir {
            #[cfg(feature = "iroh-runtime")]
            Some(state_dir) => {
                let endpoint_id =
                    wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&state_dir)?
                        .to_string();
                NetworkRuntimeBackend::Iroh(iroh_runtime::IrohNetworkRuntime::new(
                    iroh_runtime::IrohRuntimeConfig {
                        network_id: config.namespace.network_id.clone(),
                        state_dir,
                        local_network_peer_id: endpoint_id,
                    },
                )?)
            }
            #[cfg(not(feature = "iroh-runtime"))]
            Some(_) => {
                return Err(anyhow!(
                    "iroh-backed NetworkP2pNode requires network-p2p iroh-runtime feature"
                ));
            }
            None => NetworkRuntimeBackend::Legacy(SubstrateRuntime::new(node.inner)?),
        };
        Ok(Self {
            config,
            backend,
            local_peer_id,
            iroh_listen_addrs: Vec::new(),
            next_inbound_request_id: 1,
            next_outbound_request_id: 1,
            backfill_response_channels: HashMap::new(),
            contact_material_response_channels: HashMap::new(),
            peer_relationship_response_channels: HashMap::new(),
            peer_direct_message_response_channels: HashMap::new(),
            legacy_backfill_request_ids: HashMap::new(),
            legacy_contact_material_request_ids: HashMap::new(),
            legacy_peer_relationship_request_ids: HashMap::new(),
            legacy_peer_direct_message_request_ids: HashMap::new(),
        })
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn is_iroh_backed(&self) -> bool {
        #[cfg(feature = "iroh-runtime")]
        {
            matches!(self.backend, NetworkRuntimeBackend::Iroh(_))
        }
        #[cfg(not(feature = "iroh-runtime"))]
        {
            false
        }
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        match &self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.listen_addrs(),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(_) => &self.iroh_listen_addrs,
        }
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        match &self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.observability_snapshot(),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => iroh_observability_snapshot(inner),
        }
    }

    pub fn known_remote_contact_count(&self) -> usize {
        match &self.backend {
            NetworkRuntimeBackend::Legacy(_) => 0,
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => {
                inner.observability_snapshot().known_remote_contacts
            }
        }
    }

    pub fn allows_outbound_backfill_to(&self, peer: &PeerId) -> bool {
        match &self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.allows_outbound_backfill_to(peer),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(_) => true,
        }
    }

    #[cfg(feature = "iroh-runtime")]
    pub fn upsert_remote_contact_material(
        &mut self,
        remote_network_peer_id: impl Into<String>,
        contact: wattswarm_network_transport_core::TransportContactMaterial,
    ) -> Result<bool> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(_) => Ok(false),
            NetworkRuntimeBackend::Iroh(inner) => {
                inner.upsert_remote_contact_material(remote_network_peer_id, contact)
            }
        }
    }

    pub fn validate_identify_info(
        &self,
        info: &identify::Info,
    ) -> Result<(Vec<Multiaddr>, PeerIdentificationMetadata)> {
        match &self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.validate_identify_info(info),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(_) => {
                Err(anyhow!("libp2p identify is not active for iroh runtime"))
            }
        }
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.subscribe_scope(scope),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.subscribe_scope(scope),
        }
    }

    pub fn subscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.subscribe_scope_kinds(scope, kinds),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.subscribe_scope_kinds(scope, kinds),
        }
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.unsubscribe_scope(scope),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.unsubscribe_scope(scope),
        }
    }

    pub fn unsubscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.unsubscribe_scope_kinds(scope, kinds),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.unsubscribe_scope_kinds(scope, kinds),
        }
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.dial(addr),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(_) => Err(anyhow!(
                "libp2p multiaddr dial is not active for iroh runtime; use iroh contact material"
            )),
        }
    }

    pub fn publish_gossip(&mut self, message: &GossipMessage) -> Result<()> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => inner.publish(
                message.scope(),
                message.kind(),
                &encode_overlay_payload(message)?,
            ),
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.publish_gossip(message),
        }
    }

    pub fn send_backfill_request(
        &mut self,
        peer: &PeerId,
        request: BackfillRequest,
    ) -> Result<BackfillRequestId> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => {
                let substrate_id = inner.send_backfill_request(peer, request)?;
                let request_id = self.reserve_backfill_request_id();
                self.legacy_backfill_request_ids
                    .insert(substrate_id, request_id);
                Ok(request_id)
            }
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => {
                inner.send_backfill_request(&peer.to_string(), request)
            }
        }
    }

    pub fn send_backfill_response(
        &mut self,
        request_id: InboundRequestId,
        response: BackfillResponse,
    ) -> Result<()> {
        let NetworkRuntimeBackend::Legacy(inner) = &mut self.backend else {
            #[cfg(feature = "iroh-runtime")]
            return match &mut self.backend {
                NetworkRuntimeBackend::Iroh(inner) => {
                    inner.send_backfill_response(request_id, response)
                }
                NetworkRuntimeBackend::Legacy(_) => unreachable!(),
            };
            #[cfg(not(feature = "iroh-runtime"))]
            unreachable!();
        };
        let channel = self
            .backfill_response_channels
            .remove(&request_id)
            .ok_or_else(|| anyhow!("unknown backfill response request id {request_id}"))?;
        let items = response
            .events
            .iter()
            .map(EventEnvelope::encode_json)
            .collect::<Result<Vec<_>>>()?;
        inner.send_backfill_response(
            channel,
            RawBackfillResponse {
                scope: response.scope,
                next_from_event_seq: response.next_from_event_seq,
                feed_key: response.feed_key,
                head_event_ids: response.head_event_ids,
                items,
            },
        )
    }

    pub fn send_contact_material_request(
        &mut self,
        peer: &PeerId,
        request: ContactMaterialRequest,
    ) -> Result<ContactMaterialRequestId> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => {
                let substrate_id = inner.send_contact_material_request(peer, request)?;
                let request_id = self.reserve_contact_material_request_id();
                self.legacy_contact_material_request_ids
                    .insert(substrate_id, request_id);
                Ok(request_id)
            }
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => {
                inner.send_contact_material_request(&peer.to_string(), request)
            }
        }
    }

    pub fn send_contact_material_response(
        &mut self,
        request_id: InboundRequestId,
        response: ContactMaterialResponse,
    ) -> Result<()> {
        let NetworkRuntimeBackend::Legacy(inner) = &mut self.backend else {
            #[cfg(feature = "iroh-runtime")]
            return match &mut self.backend {
                NetworkRuntimeBackend::Iroh(inner) => {
                    inner.send_contact_material_response(request_id, response)
                }
                NetworkRuntimeBackend::Legacy(_) => unreachable!(),
            };
            #[cfg(not(feature = "iroh-runtime"))]
            unreachable!();
        };
        let channel = self
            .contact_material_response_channels
            .remove(&request_id)
            .ok_or_else(|| anyhow!("unknown contact material response request id {request_id}"))?;
        inner.send_contact_material_response(channel, response)
    }

    pub fn send_peer_relationship_request(
        &mut self,
        peer: &PeerId,
        request: PeerRelationshipRequest,
    ) -> Result<PeerRelationshipRequestId> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => {
                let substrate_id = inner.send_peer_relationship_request(peer, request)?;
                let request_id = self.reserve_peer_relationship_request_id();
                self.legacy_peer_relationship_request_ids
                    .insert(substrate_id, request_id);
                Ok(request_id)
            }
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => {
                inner.send_peer_relationship_request(&peer.to_string(), request)
            }
        }
    }

    pub fn send_peer_relationship_response(
        &mut self,
        request_id: InboundRequestId,
        response: PeerRelationshipResponse,
    ) -> Result<()> {
        let NetworkRuntimeBackend::Legacy(inner) = &mut self.backend else {
            #[cfg(feature = "iroh-runtime")]
            return match &mut self.backend {
                NetworkRuntimeBackend::Iroh(inner) => {
                    inner.send_peer_relationship_response(request_id, response)
                }
                NetworkRuntimeBackend::Legacy(_) => unreachable!(),
            };
            #[cfg(not(feature = "iroh-runtime"))]
            unreachable!();
        };
        let channel = self
            .peer_relationship_response_channels
            .remove(&request_id)
            .ok_or_else(|| anyhow!("unknown peer relationship response request id {request_id}"))?;
        inner.send_peer_relationship_response(channel, response)
    }

    pub fn send_peer_direct_message_request(
        &mut self,
        peer: &PeerId,
        request: PeerDirectMessageRequest,
    ) -> Result<PeerDirectMessageRequestId> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => {
                let substrate_id = inner.send_peer_direct_message_request(peer, request)?;
                let request_id = self.reserve_peer_direct_message_request_id();
                self.legacy_peer_direct_message_request_ids
                    .insert(substrate_id, request_id);
                Ok(request_id)
            }
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => {
                inner.send_peer_direct_message_request(&peer.to_string(), request)
            }
        }
    }

    pub fn send_peer_direct_message_response(
        &mut self,
        request_id: InboundRequestId,
        response: PeerDirectMessageResponse,
    ) -> Result<()> {
        let NetworkRuntimeBackend::Legacy(inner) = &mut self.backend else {
            #[cfg(feature = "iroh-runtime")]
            return match &mut self.backend {
                NetworkRuntimeBackend::Iroh(inner) => {
                    inner.send_peer_direct_message_response(request_id, response)
                }
                NetworkRuntimeBackend::Legacy(_) => unreachable!(),
            };
            #[cfg(not(feature = "iroh-runtime"))]
            unreachable!();
        };
        let channel = self
            .peer_direct_message_response_channels
            .remove(&request_id)
            .ok_or_else(|| {
                anyhow!("unknown peer direct message response request id {request_id}")
            })?;
        inner.send_peer_direct_message_response(channel, response)
    }

    pub async fn next_event(&mut self) -> Result<NetworkRuntimeEvent> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => {
                let event = inner.next_event().await?;
                self.map_runtime_event(event)
            }
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.next_event().await,
        }
    }

    pub fn try_next_event(&mut self) -> Result<Option<NetworkRuntimeEvent>> {
        match &mut self.backend {
            NetworkRuntimeBackend::Legacy(inner) => match inner.try_next_event()? {
                Some(event) => Ok(Some(self.map_runtime_event(event)?)),
                None => Ok(None),
            },
            #[cfg(feature = "iroh-runtime")]
            NetworkRuntimeBackend::Iroh(inner) => inner.try_next_event(),
        }
    }

    fn reserve_inbound_request_id(&mut self) -> InboundRequestId {
        let request_id = InboundRequestId(self.next_inbound_request_id);
        self.next_inbound_request_id = self.next_inbound_request_id.saturating_add(1).max(1);
        request_id
    }

    fn reserve_outbound_request_number(&mut self) -> u64 {
        let request_id = self.next_outbound_request_id;
        self.next_outbound_request_id = self.next_outbound_request_id.saturating_add(1).max(1);
        request_id
    }

    fn reserve_backfill_request_id(&mut self) -> BackfillRequestId {
        BackfillRequestId::new(self.reserve_outbound_request_number())
    }

    fn reserve_contact_material_request_id(&mut self) -> ContactMaterialRequestId {
        ContactMaterialRequestId::new(self.reserve_outbound_request_number())
    }

    fn reserve_peer_relationship_request_id(&mut self) -> PeerRelationshipRequestId {
        PeerRelationshipRequestId::new(self.reserve_outbound_request_number())
    }

    fn reserve_peer_direct_message_request_id(&mut self) -> PeerDirectMessageRequestId {
        PeerDirectMessageRequestId::new(self.reserve_outbound_request_number())
    }

    fn backfill_request_id_for(
        &mut self,
        substrate_id: substrate::BackfillRequestId,
    ) -> BackfillRequestId {
        self.legacy_backfill_request_ids
            .remove(&substrate_id)
            .unwrap_or_else(|| self.reserve_backfill_request_id())
    }

    fn contact_material_request_id_for(
        &mut self,
        substrate_id: substrate::ContactMaterialRequestId,
    ) -> ContactMaterialRequestId {
        self.legacy_contact_material_request_ids
            .remove(&substrate_id)
            .unwrap_or_else(|| self.reserve_contact_material_request_id())
    }

    fn peer_relationship_request_id_for(
        &mut self,
        substrate_id: substrate::PeerRelationshipRequestId,
    ) -> PeerRelationshipRequestId {
        self.legacy_peer_relationship_request_ids
            .remove(&substrate_id)
            .unwrap_or_else(|| self.reserve_peer_relationship_request_id())
    }

    fn peer_direct_message_request_id_for(
        &mut self,
        substrate_id: substrate::PeerDirectMessageRequestId,
    ) -> PeerDirectMessageRequestId {
        self.legacy_peer_direct_message_request_ids
            .remove(&substrate_id)
            .unwrap_or_else(|| self.reserve_peer_direct_message_request_id())
    }

    fn map_runtime_event(&mut self, event: SubstrateRuntimeEvent) -> Result<NetworkRuntimeEvent> {
        Ok(match event {
            SubstrateRuntimeEvent::PeerDiscovered {
                peer,
                address,
                source,
            } => NetworkRuntimeEvent::PeerDiscovered {
                peer,
                address,
                source,
            },
            SubstrateRuntimeEvent::PeerIdentified { peer, metadata } => {
                NetworkRuntimeEvent::PeerIdentified { peer, metadata }
            }
            SubstrateRuntimeEvent::NewListenAddr { address } => {
                NetworkRuntimeEvent::NewListenAddr { address }
            }
            SubstrateRuntimeEvent::ConnectionEstablished { peer, remote_addr } => {
                NetworkRuntimeEvent::ConnectionEstablished { peer, remote_addr }
            }
            SubstrateRuntimeEvent::ConnectionClosed {
                peer,
                remaining_established,
            } => NetworkRuntimeEvent::ConnectionClosed {
                peer,
                remaining_established,
            },
            SubstrateRuntimeEvent::PeerHandshakeRejected { peer, detail } => {
                NetworkRuntimeEvent::PeerHandshakeRejected { peer, detail }
            }
            SubstrateRuntimeEvent::Gossip {
                propagation_source,
                message,
            } => NetworkRuntimeEvent::Gossip {
                propagation_source,
                message: decode_overlay_gossip(message)?,
            },
            SubstrateRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            } => {
                let request_id = self.reserve_inbound_request_id();
                self.backfill_response_channels.insert(request_id, channel);
                NetworkRuntimeEvent::BackfillRequest {
                    peer,
                    request,
                    request_id,
                }
            }
            SubstrateRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response,
            } => NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id: self.backfill_request_id_for(request_id),
                response: decode_backfill_response(response)?,
            },
            SubstrateRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id: self.backfill_request_id_for(request_id),
                error,
            },
            SubstrateRuntimeEvent::BackfillInboundFailure { peer, error } => {
                NetworkRuntimeEvent::BackfillInboundFailure { peer, error }
            }
            SubstrateRuntimeEvent::ContactMaterialRequest {
                peer,
                request,
                channel,
            } => {
                let request_id = self.reserve_inbound_request_id();
                self.contact_material_response_channels
                    .insert(request_id, channel);
                NetworkRuntimeEvent::ContactMaterialRequest {
                    peer,
                    request,
                    request_id,
                }
            }
            SubstrateRuntimeEvent::ContactMaterialResponse {
                peer,
                request_id,
                response,
            } => NetworkRuntimeEvent::ContactMaterialResponse {
                peer,
                request_id: self.contact_material_request_id_for(request_id),
                response,
            },
            SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::ContactMaterialOutboundFailure {
                peer,
                request_id: self.contact_material_request_id_for(request_id),
                error,
            },
            SubstrateRuntimeEvent::ContactMaterialInboundFailure { peer, error } => {
                NetworkRuntimeEvent::ContactMaterialInboundFailure { peer, error }
            }
            SubstrateRuntimeEvent::PeerRelationshipRequest {
                peer,
                request,
                channel,
            } => {
                let request_id = self.reserve_inbound_request_id();
                self.peer_relationship_response_channels
                    .insert(request_id, channel);
                NetworkRuntimeEvent::PeerRelationshipRequest {
                    peer,
                    request,
                    request_id,
                }
            }
            SubstrateRuntimeEvent::PeerRelationshipResponse {
                peer,
                request_id,
                response,
            } => NetworkRuntimeEvent::PeerRelationshipResponse {
                peer,
                request_id: self.peer_relationship_request_id_for(request_id),
                response,
            },
            SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::PeerRelationshipOutboundFailure {
                peer,
                request_id: self.peer_relationship_request_id_for(request_id),
                error,
            },
            SubstrateRuntimeEvent::PeerRelationshipInboundFailure { peer, error } => {
                NetworkRuntimeEvent::PeerRelationshipInboundFailure { peer, error }
            }
            SubstrateRuntimeEvent::PeerDirectMessageRequest {
                peer,
                request,
                channel,
            } => {
                let request_id = self.reserve_inbound_request_id();
                self.peer_direct_message_response_channels
                    .insert(request_id, channel);
                NetworkRuntimeEvent::PeerDirectMessageRequest {
                    peer,
                    request,
                    request_id,
                }
            }
            SubstrateRuntimeEvent::PeerDirectMessageResponse {
                peer,
                request_id,
                response,
            } => NetworkRuntimeEvent::PeerDirectMessageResponse {
                peer,
                request_id: self.peer_direct_message_request_id_for(request_id),
                response,
            },
            SubstrateRuntimeEvent::PeerDirectMessageOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::PeerDirectMessageOutboundFailure {
                peer,
                request_id: self.peer_direct_message_request_id_for(request_id),
                error,
            },
            SubstrateRuntimeEvent::PeerDirectMessageInboundFailure { peer, error } => {
                NetworkRuntimeEvent::PeerDirectMessageInboundFailure { peer, error }
            }
            SubstrateRuntimeEvent::NatStatusChanged {
                old,
                new,
                public_address,
                confidence,
            } => NetworkRuntimeEvent::NatStatusChanged {
                old,
                new,
                public_address,
                confidence,
            },
            SubstrateRuntimeEvent::RelayReservationAccepted {
                relay_peer,
                renewal,
            } => NetworkRuntimeEvent::RelayReservationAccepted {
                relay_peer,
                renewal,
            },
            SubstrateRuntimeEvent::RelayCircuitEstablished { relay_peer } => {
                NetworkRuntimeEvent::RelayCircuitEstablished { relay_peer }
            }
            SubstrateRuntimeEvent::RelayInboundCircuitEstablished { source_peer } => {
                NetworkRuntimeEvent::RelayInboundCircuitEstablished { source_peer }
            }
            SubstrateRuntimeEvent::DcutrConnectionUpgradeSucceeded { remote_peer } => {
                NetworkRuntimeEvent::DcutrConnectionUpgradeSucceeded { remote_peer }
            }
            SubstrateRuntimeEvent::DcutrConnectionUpgradeFailed { remote_peer, error } => {
                NetworkRuntimeEvent::DcutrConnectionUpgradeFailed { remote_peer, error }
            }
        })
    }
}

pub(crate) fn decode_overlay_gossip(message: RawGossipMessage) -> Result<GossipMessage> {
    let payload = &message.payload;
    match message.kind {
        GossipKind::Events => Ok(GossipMessage::Event(EventEnvelope::decode_json(payload)?)),
        GossipKind::Messages => Ok(GossipMessage::Chat(EventEnvelope::decode_json(payload)?)),
        GossipKind::Rules => Ok(GossipMessage::Rule(serde_json::from_slice(payload)?)),
        GossipKind::Checkpoints => Ok(GossipMessage::Checkpoint(serde_json::from_slice(payload)?)),
        GossipKind::Summaries => Ok(GossipMessage::Summary(serde_json::from_slice(payload)?)),
    }
}

pub(crate) fn encode_overlay_payload(message: &GossipMessage) -> Result<Vec<u8>> {
    match message {
        GossipMessage::Event(envelope) | GossipMessage::Chat(envelope) => envelope.encode_json(),
        GossipMessage::Rule(rule) => Ok(serde_json::to_vec(rule)?),
        GossipMessage::Checkpoint(checkpoint) => Ok(serde_json::to_vec(checkpoint)?),
        GossipMessage::Summary(summary) => Ok(serde_json::to_vec(summary)?),
    }
}

fn decode_backfill_response(response: RawBackfillResponse) -> Result<BackfillResponse> {
    let events = response
        .items
        .iter()
        .map(|raw| EventEnvelope::decode_json(raw))
        .collect::<Result<Vec<_>>>()?;
    Ok(BackfillResponse {
        scope: response.scope,
        next_from_event_seq: response.next_from_event_seq,
        feed_key: response.feed_key,
        head_event_ids: response.head_event_ids,
        events,
    })
}

#[cfg(feature = "iroh-runtime")]
fn iroh_observability_snapshot(
    runtime: &iroh_runtime::IrohNetworkRuntime,
) -> NetworkRuntimeObservabilitySnapshot {
    let snapshot = runtime.observability_snapshot();
    let subscribed_iroh_gossip_topics = snapshot
        .subscribed_topics
        .iter()
        .map(|subscription| {
            format!(
                "{:?}:{}:{}",
                subscription.scope,
                subscription.kind.as_str(),
                subscription.topic_id_hex
            )
        })
        .collect();
    let peer_health = snapshot
        .peer_quality
        .iter()
        .map(|entry| {
            let score = -i64::from(entry.consecutive_failures);
            let throttled = entry.retry_after_ms.is_some();
            TrafficGuardPeerHealth {
                peer: entry.network_peer_id.clone(),
                score,
                blacklisted: false,
                reputation_tier: if throttled { "backoff" } else { "healthy" }.to_owned(),
                quarantined: throttled,
                quarantine_remaining_ms: entry.retry_after_ms.unwrap_or_default(),
                ban_remaining_ms: 0,
                throttle_factor_percent: if throttled { 0 } else { 100 },
            }
        })
        .collect();
    NetworkRuntimeObservabilitySnapshot {
        p2p_foundation: "iroh".to_owned(),
        local_iroh_endpoint_id: Some(snapshot.local_endpoint_id),
        subscribed_iroh_gossip_topics,
        known_iroh_contacts: snapshot.known_remote_contacts,
        legacy_libp2p_active: false,
        nat_status: "iroh-direct".to_owned(),
        nat_public_address: None,
        nat_confidence: 0,
        relay_reservations: Vec::new(),
        peer_health,
        dropped_malformed_gossip: snapshot.counters.malformed_gossip_notifications,
        invalid_control_payloads: snapshot.counters.invalid_control_payloads,
        dial_failures: snapshot.counters.stream_request_failures,
        response_validation_failures: 0,
        retry_suppressed_dials: snapshot.counters.retry_suppressed_connects,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> Event {
        Event {
            event_id: "evt-1".to_owned(),
            protocol_version: "1".to_owned(),
            event_kind: EventKind::CheckpointCreated,
            task_id: Some("task-1".to_owned()),
            epoch: 0,
            author_node_id: "node-1".to_owned(),
            created_at: 1,
            payload: EventPayload::CheckpointCreated(
                wattswarm_protocol::types::CheckpointCreatedPayload {
                    checkpoint_id: "cp-1".to_owned(),
                    up_to_seq: 1,
                },
            ),
            signature_hex: "sig".to_owned(),
        }
    }

    #[test]
    fn gossip_event_roundtrip() {
        let message = GossipMessage::Event(EventEnvelope {
            scope: SwarmScope::Global,
            event: sample_event(),
            content_source_node_id: None,
        });
        let encoded = message.encode_json().unwrap();
        let decoded = GossipMessage::decode_json(&encoded).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn config_apply_protocol_params_updates_governed_fields() {
        let config = NetworkP2pConfig::default().apply_protocol_params(&NetworkProtocolParams {
            namespace_network: "swarmnet".to_owned(),
            protocol_version: "/swarmnet/1".to_owned(),
            max_established_per_peer: 4,
            gossipsub_d: 9,
            gossipsub_d_low: 7,
            gossipsub_d_high: 12,
            gossipsub_heartbeat_ms: 2500,
            gossipsub_max_transmit_size: 1024 * 1024,
            default_max_backfill_events: 1024,
            max_backfill_events_hard_limit: 4096,
            ..NetworkProtocolParams::default()
        });
        assert_eq!(config.namespace.network, "swarmnet");
        assert_eq!(config.protocol_version, "/swarmnet/1");
        assert_eq!(config.max_backfill_events, 1024);
    }

    #[test]
    fn raw_gossip_maps_back_to_overlay_message() {
        let overlay = GossipMessage::Summary(SummaryAnnouncement {
            summary_id: "sum-1".to_owned(),
            source_node_id: "node-1".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: "knowledge".to_owned(),
            artifact_path: None,
            payload: serde_json::json!({"ok": true}),
        });
        let raw = RawGossipMessage {
            scope: overlay.scope().clone(),
            kind: overlay.kind(),
            payload: encode_overlay_payload(&overlay).unwrap(),
        };
        let decoded = decode_overlay_gossip(raw).unwrap();
        assert_eq!(decoded, overlay);
    }

    #[test]
    fn raw_backfill_response_maps_back_to_overlay_events() {
        let envelope = EventEnvelope {
            scope: SwarmScope::Global,
            event: sample_event(),
            content_source_node_id: None,
        };
        let response = decode_backfill_response(RawBackfillResponse {
            scope: SwarmScope::Global,
            next_from_event_seq: 2,
            feed_key: None,
            head_event_ids: Vec::new(),
            items: vec![envelope.encode_json().unwrap()],
        })
        .unwrap();
        assert_eq!(response.events, vec![envelope]);
    }

    #[test]
    fn inbound_response_request_ids_are_runtime_local_and_monotonic() {
        let tokio_runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let _guard = tokio_runtime.enter();
        let node = NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node");
        let mut runtime = NetworkRuntime::new(node).expect("network runtime");

        assert_eq!(runtime.reserve_inbound_request_id(), InboundRequestId(1));
        assert_eq!(runtime.reserve_inbound_request_id(), InboundRequestId(2));
        assert_eq!(runtime.reserve_inbound_request_id().to_string(), "3");
    }

    #[test]
    fn seeded_network_node_derives_expected_peer_id() {
        let seed = [9_u8; 32];
        let node = NetworkP2pNode::from_ed25519_secret_bytes(NetworkP2pConfig::default(), seed)
            .expect("seeded node");
        let expected = keypair_from_ed25519_secret_bytes(seed)
            .expect("seeded keypair")
            .public()
            .to_peer_id();
        assert_eq!(node.local_peer_id(), expected);
    }

    #[cfg(feature = "iroh-runtime")]
    #[test]
    fn state_dir_node_starts_iroh_runtime_backend_without_libp2p_listener() {
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = [77u8; 32];
        std::fs::write(dir.path().join("node_seed.hex"), hex::encode(secret))
            .expect("write node seed");
        let node = NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            dir.path().to_path_buf(),
            secret,
        )
        .expect("iroh node");
        assert!(node.is_iroh_backed());

        let runtime = NetworkRuntime::new(node).expect("iroh runtime");
        assert!(runtime.is_iroh_backed());
        assert!(runtime.listen_addrs().is_empty());
        assert_eq!(runtime.observability_snapshot().nat_status, "iroh-direct");

        wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(dir.path());
    }
}
