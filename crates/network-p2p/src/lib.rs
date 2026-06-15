use anyhow::{Result, anyhow};
use iroh::EndpointId;
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

pub use substrate::{
    BackfillResponseChannel, ContactMaterialResponseChannel, GossipKind, NetworkAddress,
    NetworkNodeId, NetworkRuntimeObservabilitySnapshot, PeerDiscoverySourceKind,
    PeerHandshakeMetadata, PeerMetadata, PeerRelationshipResponseChannel, RawAgentEnvelope,
    RawContactMaterial, RawContactMaterialRequest, RawContactMaterialResponse,
    RawPeerRelationshipAction, RawSourceAgentCard, SwarmScope, TopicCatalog, TopicNamespace,
    TrafficGuardPeerHealth, sanitize_segment,
};

pub type BackfillRequest = substrate::RawBackfillRequest;
pub type ContactMaterialRequest = substrate::RawContactMaterialRequest;
pub type ContactMaterialResponse = substrate::RawContactMaterialResponse;
pub type PeerRelationshipRequest = substrate::RawPeerRelationshipRequest;
pub type PeerRelationshipResponse = substrate::RawPeerRelationshipResponse;
pub type BackfillRequestId = substrate::BackfillRequestId;
pub type ContactMaterialRequestId = substrate::ContactMaterialRequestId;
pub type PeerRelationshipRequestId = substrate::PeerRelationshipRequestId;

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

pub fn network_peer_id_from_ed25519_public_key(public_key_32: [u8; 32]) -> Result<NetworkNodeId> {
    let endpoint_id = EndpointId::from_bytes(&public_key_32)
        .map_err(|err| anyhow!("parse iroh endpoint id from public key: {err}"))?;
    Ok(NetworkNodeId::from_endpoint_id(endpoint_id))
}

pub fn bootstrap_http_base_url(raw_addr: &str, port: u16) -> Result<String> {
    let host = raw_addr
        .rsplit_once('@')
        .map(|(_, addr)| addr)
        .unwrap_or(raw_addr)
        .split(':')
        .next()
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| anyhow!("bootstrap address missing routable host"))?;
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
    pub enable_local_discovery: bool,
    pub max_established_per_peer: u32,
    pub gossip_mesh_d: usize,
    pub gossip_mesh_d_low: usize,
    pub gossip_mesh_d_high: usize,
    pub gossip_mesh_heartbeat_ms: u64,
    pub gossip_mesh_max_transmit_size: usize,
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
            identify_agent_version: encode_wattswarm_agent_version(
                &PeerHandshakeMetadata::default(),
            ),
            listen_addrs: config.listen_addrs,
            bootstrap_peers: config.bootstrap_peers,
            enable_local_discovery: false,
            max_established_per_peer: config.max_established_per_peer,
            gossip_mesh_d: config.gossip_mesh_d,
            gossip_mesh_d_low: config.gossip_mesh_d_low,
            gossip_mesh_d_high: config.gossip_mesh_d_high,
            gossip_mesh_heartbeat_ms: config.gossip_mesh_heartbeat_ms,
            gossip_mesh_max_transmit_size: config.gossip_mesh_max_transmit_size,
            max_backfill_events: config.max_backfill_events,
            max_backfill_events_hard_limit: config.max_backfill_events_hard_limit,
        }
    }

    fn as_substrate(&self) -> SubstrateConfig {
        SubstrateConfig {
            namespace: self.namespace.clone(),
            protocol_version: self.protocol_version.clone(),
            listen_addrs: self.listen_addrs.clone(),
            bootstrap_peers: self.bootstrap_peers.clone(),
            max_established_per_peer: self.max_established_per_peer,
            gossip_mesh_d: self.gossip_mesh_d,
            gossip_mesh_d_low: self.gossip_mesh_d_low,
            gossip_mesh_d_high: self.gossip_mesh_d_high,
            gossip_mesh_heartbeat_ms: self.gossip_mesh_heartbeat_ms,
            gossip_mesh_max_transmit_size: self.gossip_mesh_max_transmit_size,
            max_backfill_events: self.max_backfill_events,
            max_backfill_events_hard_limit: self.max_backfill_events_hard_limit,
            control_request_timeout_ms: 30_000,
        }
    }

    pub fn apply_protocol_params(mut self, params: &NetworkProtocolParams) -> Self {
        let network_id = self.namespace.network_id.clone();
        self.namespace = TopicNamespace {
            network: params.namespace_network.clone(),
            network_id,
        };
        self.protocol_version = params.protocol_version.clone();
        self.gossip_mesh_d = params.gossip_mesh_d;
        self.gossip_mesh_d_low = params.gossip_mesh_d_low;
        self.gossip_mesh_d_high = params.gossip_mesh_d_high;
        self.gossip_mesh_heartbeat_ms = params.gossip_mesh_heartbeat_ms;
        self.gossip_mesh_max_transmit_size = params.gossip_mesh_max_transmit_size;
        self.max_backfill_events = params.default_max_backfill_events;
        self.max_backfill_events_hard_limit = params.max_backfill_events_hard_limit;
        self
    }

    pub fn validate(&self) -> Result<()> {
        self.as_substrate().validate()
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<NetworkAddress>> {
        self.as_substrate().parse_listen_addrs()
    }

    pub fn parse_bootstrap_peers(&self) -> Result<Vec<(NetworkNodeId, NetworkAddress)>> {
        self.as_substrate().parse_bootstrap_peers()
    }

    pub fn topic_catalog(&self, scope: &SwarmScope) -> Result<TopicCatalog> {
        self.as_substrate().topic_catalog(scope)
    }
}

pub struct NetworkP2pNode {
    config: NetworkP2pConfig,
    inner: SubstrateNode,
}

impl NetworkP2pNode {
    pub fn from_ed25519_secret_bytes(
        config: NetworkP2pConfig,
        secret_key_32: [u8; 32],
    ) -> Result<Self> {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-iroh-node-{}",
            hex::encode(secret_key_32)
        ));
        Self::from_iroh_state_dir(config, state_dir, secret_key_32)
    }

    pub fn generate(config: NetworkP2pConfig) -> Result<Self> {
        let seed: [u8; 32] = rand::random();
        Self::from_ed25519_secret_bytes(config, seed)
    }

    pub fn from_iroh_state_dir(
        config: NetworkP2pConfig,
        state_dir: impl Into<PathBuf>,
        secret_key_32: [u8; 32],
    ) -> Result<Self> {
        let inner =
            SubstrateNode::from_seed_bytes(config.as_substrate(), state_dir, secret_key_32)?;
        Ok(Self { config, inner })
    }

    pub fn is_iroh_backed(&self) -> bool {
        true
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn local_peer_id(&self) -> NetworkNodeId {
        self.inner.local_peer_id()
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<NetworkAddress>> {
        self.config.parse_listen_addrs()
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkRuntimeEvent {
    PeerDiscovered {
        peer: NetworkNodeId,
        address: NetworkAddress,
        source: PeerDiscoverySourceKind,
    },
    PeerMetadataObserved {
        peer: NetworkNodeId,
        metadata: PeerMetadata,
    },
    NewListenAddr {
        address: NetworkAddress,
    },
    ConnectionEstablished {
        peer: NetworkNodeId,
        remote_addr: NetworkAddress,
    },
    GossipNeighborUp {
        peer: NetworkNodeId,
        scope: SwarmScope,
        kind: GossipKind,
    },
    ConnectionClosed {
        peer: NetworkNodeId,
        remaining_established: u32,
    },
    PeerHandshakeRejected {
        peer: NetworkNodeId,
        detail: String,
    },
    Gossip {
        propagation_source: NetworkNodeId,
        message: GossipMessage,
    },
    BackfillRequest {
        peer: NetworkNodeId,
        request: BackfillRequest,
        request_id: InboundRequestId,
    },
    BackfillResponse {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        response: BackfillResponse,
    },
    BackfillOutboundFailure {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        error: String,
    },
    BackfillInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
    ContactMaterialRequest {
        peer: NetworkNodeId,
        request: ContactMaterialRequest,
        request_id: InboundRequestId,
    },
    ContactMaterialResponse {
        peer: NetworkNodeId,
        request_id: ContactMaterialRequestId,
        response: ContactMaterialResponse,
    },
    ContactMaterialOutboundFailure {
        peer: NetworkNodeId,
        request_id: ContactMaterialRequestId,
        error: String,
    },
    ContactMaterialInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
    PeerRelationshipRequest {
        peer: NetworkNodeId,
        request: PeerRelationshipRequest,
        request_id: InboundRequestId,
    },
    PeerRelationshipResponse {
        peer: NetworkNodeId,
        request_id: PeerRelationshipRequestId,
        response: PeerRelationshipResponse,
    },
    PeerRelationshipOutboundFailure {
        peer: NetworkNodeId,
        request_id: PeerRelationshipRequestId,
        error: String,
    },
    PeerRelationshipInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
    NatStatusChanged {
        old: String,
        new: String,
        public_address: Option<NetworkAddress>,
        confidence: usize,
    },
    RelayReservationAccepted {
        relay_peer: NetworkNodeId,
        renewal: bool,
    },
    RelayCircuitEstablished {
        relay_peer: NetworkNodeId,
    },
    RelayInboundCircuitEstablished {
        source_peer: NetworkNodeId,
    },
    DirectUpgradeConnectionUpgradeSucceeded {
        remote_peer: NetworkNodeId,
    },
    DirectUpgradeConnectionUpgradeFailed {
        remote_peer: NetworkNodeId,
        error: String,
    },
}

pub struct NetworkRuntime {
    config: NetworkP2pConfig,
    inner: SubstrateRuntime,
    local_peer_id: NetworkNodeId,
    next_inbound_request_id: u64,
    backfill_response_channels: HashMap<InboundRequestId, BackfillResponseChannel>,
    contact_material_response_channels: HashMap<InboundRequestId, ContactMaterialResponseChannel>,
    peer_relationship_response_channels: HashMap<InboundRequestId, PeerRelationshipResponseChannel>,
}

impl NetworkRuntime {
    pub fn new(node: NetworkP2pNode) -> Result<Self> {
        let config = node.config.clone();
        let local_peer_id = node.local_peer_id();
        let inner = SubstrateRuntime::new(node.inner)?;
        Ok(Self {
            config,
            inner,
            local_peer_id,
            next_inbound_request_id: 1,
            backfill_response_channels: HashMap::new(),
            contact_material_response_channels: HashMap::new(),
            peer_relationship_response_channels: HashMap::new(),
        })
    }

    pub fn local_peer_id(&self) -> NetworkNodeId {
        self.local_peer_id.clone()
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn is_iroh_backed(&self) -> bool {
        true
    }

    pub fn listen_addrs(&self) -> &[NetworkAddress] {
        self.inner.listen_addrs()
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        self.inner.observability_snapshot()
    }

    pub fn known_remote_contact_count(&self) -> usize {
        self.inner.observability_snapshot().known_iroh_contacts
    }

    pub fn remote_contact_peer_ids(&self) -> Vec<NetworkNodeId> {
        self.inner.remote_contact_peer_ids()
    }

    pub fn allows_outbound_backfill_to(&self, peer: &NetworkNodeId) -> bool {
        self.inner.allows_outbound_backfill_to(peer)
    }

    pub fn rejoin_gossip_with_remote_contact(&mut self, peer: &NetworkNodeId) -> Result<bool> {
        self.inner.rejoin_gossip_with_remote_contact(peer)
    }

    pub fn upsert_remote_contact_material(
        &mut self,
        remote_network_peer_id: impl Into<String>,
        contact: wattswarm_network_transport_core::TransportContactMaterial,
    ) -> Result<bool> {
        self.inner
            .upsert_remote_contact_material(remote_network_peer_id, contact)
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.inner.subscribe_scope(scope)
    }

    pub fn subscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        self.inner.subscribe_scope_kinds(scope, kinds)
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.inner.unsubscribe_scope(scope)
    }

    pub fn unsubscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        self.inner.unsubscribe_scope_kinds(scope, kinds)
    }

    pub fn dial(&mut self, addr: NetworkAddress) -> Result<()> {
        self.inner.dial(addr)
    }

    pub fn publish_gossip(&mut self, message: &GossipMessage) -> Result<()> {
        self.inner.publish(
            message.scope(),
            message.kind(),
            &encode_overlay_payload(message)?,
        )
    }

    pub fn send_backfill_request(
        &mut self,
        peer: &NetworkNodeId,
        request: BackfillRequest,
    ) -> Result<BackfillRequestId> {
        self.inner.send_backfill_request(peer, request)
    }

    pub fn send_backfill_response(
        &mut self,
        request_id: InboundRequestId,
        response: BackfillResponse,
    ) -> Result<()> {
        let channel = self
            .backfill_response_channels
            .remove(&request_id)
            .ok_or_else(|| anyhow!("unknown backfill response request id {request_id}"))?;
        let items = response
            .events
            .iter()
            .map(EventEnvelope::encode_json)
            .collect::<Result<Vec<_>>>()?;
        self.inner.send_backfill_response(
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
        peer: &NetworkNodeId,
        request: ContactMaterialRequest,
    ) -> Result<ContactMaterialRequestId> {
        self.inner.send_contact_material_request(peer, request)
    }

    pub fn send_contact_material_response(
        &mut self,
        request_id: InboundRequestId,
        response: ContactMaterialResponse,
    ) -> Result<()> {
        let channel = self
            .contact_material_response_channels
            .remove(&request_id)
            .ok_or_else(|| anyhow!("unknown contact material response request id {request_id}"))?;
        self.inner.send_contact_material_response(channel, response)
    }

    pub fn send_peer_relationship_request(
        &mut self,
        peer: &NetworkNodeId,
        request: PeerRelationshipRequest,
    ) -> Result<PeerRelationshipRequestId> {
        self.inner.send_peer_relationship_request(peer, request)
    }

    pub fn send_peer_relationship_response(
        &mut self,
        request_id: InboundRequestId,
        response: PeerRelationshipResponse,
    ) -> Result<()> {
        let channel = self
            .peer_relationship_response_channels
            .remove(&request_id)
            .ok_or_else(|| anyhow!("unknown peer relationship response request id {request_id}"))?;
        self.inner
            .send_peer_relationship_response(channel, response)
    }

    pub async fn next_event(&mut self) -> Result<NetworkRuntimeEvent> {
        let event = self.inner.next_event().await?;
        self.map_runtime_event(event)
    }

    pub fn try_next_event(&mut self) -> Result<Option<NetworkRuntimeEvent>> {
        match self.inner.try_next_event()? {
            Some(event) => Ok(Some(self.map_runtime_event(event)?)),
            None => Ok(None),
        }
    }

    fn reserve_inbound_request_id(&mut self) -> InboundRequestId {
        let request_id = InboundRequestId(self.next_inbound_request_id);
        self.next_inbound_request_id = self.next_inbound_request_id.saturating_add(1).max(1);
        request_id
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
            SubstrateRuntimeEvent::PeerMetadataObserved { peer, metadata } => {
                NetworkRuntimeEvent::PeerMetadataObserved { peer, metadata }
            }
            SubstrateRuntimeEvent::NewListenAddr { address } => {
                NetworkRuntimeEvent::NewListenAddr { address }
            }
            SubstrateRuntimeEvent::ConnectionEstablished { peer, remote_addr } => {
                NetworkRuntimeEvent::ConnectionEstablished { peer, remote_addr }
            }
            SubstrateRuntimeEvent::GossipNeighborUp { peer, scope, kind } => {
                NetworkRuntimeEvent::GossipNeighborUp { peer, scope, kind }
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
                request_id,
                response: decode_backfill_response(response)?,
            },
            SubstrateRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
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
                request_id,
                response,
            },
            SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::ContactMaterialOutboundFailure {
                peer,
                request_id,
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
                request_id,
                response,
            },
            SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                peer,
                request_id,
                error,
            } => NetworkRuntimeEvent::PeerRelationshipOutboundFailure {
                peer,
                request_id,
                error,
            },
            SubstrateRuntimeEvent::PeerRelationshipInboundFailure { peer, error } => {
                NetworkRuntimeEvent::PeerRelationshipInboundFailure { peer, error }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn sample_event() -> Event {
        Event {
            event_id: "evt-1".to_owned(),
            protocol_version: "1".to_owned(),
            event_kind: EventKind::CheckpointCreated,
            task_id: Some("task-1".to_owned()),
            swarm_scope: "global".to_owned(),
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

    fn test_iroh_node(
        config: NetworkP2pConfig,
        seed: [u8; 32],
    ) -> (tempfile::TempDir, NetworkP2pNode) {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("node_seed.hex"), hex::encode(seed))
            .expect("write node seed");
        std::fs::write(
            dir.path().join("startup_config.json"),
            serde_json::to_vec(
                &serde_json::json!({"relay_urls":["https://relay.example.invalid/"]}),
            )
            .expect("startup config json"),
        )
        .expect("write startup config");
        let node = NetworkP2pNode::from_iroh_state_dir(config, dir.path().to_path_buf(), seed)
            .expect("iroh node");
        (dir, node)
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
            gossip_mesh_d: 9,
            gossip_mesh_d_low: 7,
            gossip_mesh_d_high: 12,
            gossip_mesh_heartbeat_ms: 2500,
            gossip_mesh_max_transmit_size: 1024 * 1024,
            default_max_backfill_events: 1024,
            max_backfill_events_hard_limit: 4096,
            ..NetworkProtocolParams::default()
        });
        assert_eq!(config.namespace.network, "swarmnet");
        assert_eq!(config.protocol_version, "/swarmnet/1");
        assert_eq!(config.max_established_per_peer, 2);
        assert_eq!(config.gossip_mesh_d, 9);
        assert_eq!(config.gossip_mesh_d_low, 7);
        assert_eq!(config.gossip_mesh_d_high, 12);
        assert_eq!(config.gossip_mesh_heartbeat_ms, 2500);
        assert_eq!(config.gossip_mesh_max_transmit_size, 1024 * 1024);
        assert_eq!(config.max_backfill_events, 1024);
        assert_eq!(config.max_backfill_events_hard_limit, 4096);

        let substrate = config.as_substrate();
        assert_eq!(substrate.max_established_per_peer, 2);
        assert_eq!(substrate.gossip_mesh_d, 9);
        assert_eq!(substrate.gossip_mesh_d_low, 7);
        assert_eq!(substrate.gossip_mesh_d_high, 12);
        assert_eq!(substrate.gossip_mesh_heartbeat_ms, 2500);
        assert_eq!(substrate.gossip_mesh_max_transmit_size, 1024 * 1024);
        assert_eq!(substrate.max_backfill_events, 1024);
        assert_eq!(substrate.max_backfill_events_hard_limit, 4096);
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
        let (_dir, node) = test_iroh_node(
            NetworkP2pConfig {
                listen_addrs: vec!["127.0.0.1:0".to_owned()],
                enable_local_discovery: false,
                ..NetworkP2pConfig::default()
            },
            [151u8; 32],
        );
        let mut runtime = NetworkRuntime::new(node).expect("network runtime");

        assert_eq!(runtime.reserve_inbound_request_id(), InboundRequestId(1));
        assert_eq!(runtime.reserve_inbound_request_id(), InboundRequestId(2));
        assert_eq!(runtime.reserve_inbound_request_id().to_string(), "3");
    }

    #[test]
    fn dialed_iroh_peers_join_existing_gossip_subscriptions() {
        let (_dir_a, node_a) = test_iroh_node(
            NetworkP2pConfig {
                listen_addrs: vec!["127.0.0.1:0".to_owned()],
                enable_local_discovery: false,
                ..NetworkP2pConfig::default()
            },
            [152u8; 32],
        );
        let (_dir_b, node_b) = test_iroh_node(
            NetworkP2pConfig {
                listen_addrs: vec!["127.0.0.1:0".to_owned()],
                enable_local_discovery: false,
                ..NetworkP2pConfig::default()
            },
            [153u8; 32],
        );
        let mut runtime_a = NetworkRuntime::new(node_a).expect("runtime a");
        let mut runtime_b = NetworkRuntime::new(node_b).expect("runtime b");
        runtime_a
            .subscribe_scope(&SwarmScope::Global)
            .expect("subscribe a");
        runtime_b
            .subscribe_scope(&SwarmScope::Global)
            .expect("subscribe b");

        if runtime_a.listen_addrs().is_empty() || runtime_b.listen_addrs().is_empty() {
            return;
        }
        let addr_a = format!(
            "{}@{}",
            runtime_a.local_peer_id(),
            runtime_a.listen_addrs()[0]
        )
        .parse()
        .expect("addr a");
        let addr_b = format!(
            "{}@{}",
            runtime_b.local_peer_id(),
            runtime_b.listen_addrs()[0]
        )
        .parse()
        .expect("addr b");
        let peer_b = runtime_b.local_peer_id();
        runtime_a.dial(addr_b).expect("dial b");
        runtime_b.dial(addr_a).expect("dial a");

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut connected = false;
        while Instant::now() < deadline {
            if matches!(
                runtime_a.try_next_event().expect("runtime a event"),
                Some(NetworkRuntimeEvent::ConnectionEstablished { peer, .. }) if peer == peer_b
            ) {
                connected = true;
                break;
            }
            let _ = runtime_b.try_next_event();
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            connected,
            "dialed Iroh peer should become a gossip neighbor"
        );

        let message = GossipMessage::Event(EventEnvelope {
            scope: SwarmScope::Global,
            event: sample_event(),
            content_source_node_id: None,
        });
        runtime_a.publish_gossip(&message).expect("publish gossip");

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut received = false;
        while Instant::now() < deadline {
            if matches!(
                runtime_b.try_next_event().expect("runtime b event"),
                Some(NetworkRuntimeEvent::Gossip { message: seen, .. }) if seen == message
            ) {
                received = true;
                break;
            }
            let _ = runtime_a.try_next_event();
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(received, "dialed Iroh peer should receive gossip");
    }

    #[test]
    fn seeded_network_node_derives_expected_peer_id() {
        let seed = [9_u8; 32];
        let node = NetworkP2pNode::from_ed25519_secret_bytes(NetworkP2pConfig::default(), seed)
            .expect("seeded node");
        let expected = {
            let secret = iroh::SecretKey::from_bytes(&seed);
            NetworkNodeId::from_endpoint_id(
                EndpointId::from_bytes(secret.public().as_bytes()).expect("endpoint id"),
            )
        };
        assert_eq!(node.local_peer_id(), expected);
    }

    #[cfg(feature = "iroh-runtime")]
    #[test]
    fn state_dir_node_starts_iroh_runtime_backend_without_secondary_listener() {
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = [77u8; 32];
        std::fs::write(dir.path().join("node_seed.hex"), hex::encode(secret))
            .expect("write node seed");
        std::fs::write(
            dir.path().join("startup_config.json"),
            serde_json::to_vec(
                &serde_json::json!({"relay_urls":["https://relay.example.invalid/"]}),
            )
            .expect("startup config json"),
        )
        .expect("write startup config");
        let node = NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            dir.path().to_path_buf(),
            secret,
        )
        .expect("iroh node");
        assert!(node.is_iroh_backed());

        let runtime = NetworkRuntime::new(node).expect("iroh runtime");
        assert!(runtime.is_iroh_backed());
        let observability = runtime.observability_snapshot();
        assert_eq!(observability.nat_status, "iroh-direct");

        wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(dir.path());
    }
}
