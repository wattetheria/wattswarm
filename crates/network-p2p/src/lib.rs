use anyhow::{Result, anyhow};
use libp2p::identify;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wattswarm_network_substrate::{
    self as substrate, RawBackfillResponse, RawGossipMessage, SubstrateConfig, SubstrateNode,
    SubstrateRuntime, SubstrateRuntimeEvent,
};
use wattswarm_protocol::types::{Event, NetworkProtocolParams};
#[cfg(test)]
use wattswarm_protocol::types::{EventKind, EventPayload};

pub use substrate::{
    BackfillRequestId, BackfillResponseChannel, Multiaddr, NetworkRuntimeObservabilitySnapshot,
    PeerHandshakeMetadata, PeerId, SwarmScope, TopicCatalog, TopicKind, TopicNamespace,
    TrafficGuardPeerHealth, relay_reservation_addr, sanitize_segment,
};

pub type BackfillRequest = substrate::RawBackfillRequest;
pub type WattSwarmBehaviour = substrate::SubstrateBehaviour;
pub type WattSwarmBehaviourEvent = substrate::SubstrateBehaviourEvent;

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
    Chat(EventEnvelope),
    Rule(RuleAnnouncement),
    Checkpoint(CheckpointAnnouncement),
    Summary(SummaryAnnouncement),
}

impl GossipMessage {
    pub fn kind(&self) -> TopicKind {
        match self {
            Self::Event(_) => TopicKind::Events,
            Self::Chat(_) => TopicKind::Messages,
            Self::Rule(_) => TopicKind::Rules,
            Self::Checkpoint(_) => TopicKind::Checkpoints,
            Self::Summary(_) => TopicKind::Summaries,
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
}

impl NetworkP2pNode {
    pub fn new(config: NetworkP2pConfig, local_key: libp2p::identity::Keypair) -> Result<Self> {
        let inner = SubstrateNode::new(config.as_substrate(), local_key)?;
        Ok(Self { config, inner })
    }

    pub fn generate(config: NetworkP2pConfig) -> Result<Self> {
        let inner = SubstrateNode::generate(config.as_substrate())?;
        Ok(Self { config, inner })
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
pub enum NetworkRuntimeEvent {
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

pub struct NetworkRuntime {
    config: NetworkP2pConfig,
    inner: SubstrateRuntime,
}

impl NetworkRuntime {
    pub fn new(node: NetworkP2pNode) -> Result<Self> {
        let config = node.config.clone();
        let inner = SubstrateRuntime::new(node.inner)?;
        Ok(Self { config, inner })
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.inner.local_peer_id()
    }

    pub fn config(&self) -> &NetworkP2pConfig {
        &self.config
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        self.inner.listen_addrs()
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        self.inner.observability_snapshot()
    }

    pub fn allows_outbound_backfill_to(&self, peer: &PeerId) -> bool {
        self.inner.allows_outbound_backfill_to(peer)
    }

    pub fn validate_identify_info(&self, info: &identify::Info) -> Result<Vec<Multiaddr>> {
        self.inner.validate_identify_info(info)
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.inner.subscribe_scope(scope)
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.inner.unsubscribe_scope(scope)
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
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
        peer: &PeerId,
        request: BackfillRequest,
    ) -> Result<BackfillRequestId> {
        self.inner.send_backfill_request(peer, request)
    }

    pub fn send_backfill_response(
        &mut self,
        channel: BackfillResponseChannel,
        response: BackfillResponse,
    ) -> Result<()> {
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
                items,
            },
        )
    }

    pub async fn next_event(&mut self) -> Result<NetworkRuntimeEvent> {
        Self::map_runtime_event(self.inner.next_event().await?)
    }

    pub fn try_next_event(&mut self) -> Result<Option<NetworkRuntimeEvent>> {
        self.inner
            .try_next_event()?
            .map(Self::map_runtime_event)
            .transpose()
    }

    fn map_runtime_event(event: SubstrateRuntimeEvent) -> Result<NetworkRuntimeEvent> {
        Ok(match event {
            SubstrateRuntimeEvent::NewListenAddr { address } => {
                NetworkRuntimeEvent::NewListenAddr { address }
            }
            SubstrateRuntimeEvent::ConnectionEstablished { peer } => {
                NetworkRuntimeEvent::ConnectionEstablished { peer }
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
            } => NetworkRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            },
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

fn decode_overlay_gossip(message: RawGossipMessage) -> Result<GossipMessage> {
    let payload = &message.payload;
    match message.kind {
        TopicKind::Events => Ok(GossipMessage::Event(EventEnvelope::decode_json(payload)?)),
        TopicKind::Messages => Ok(GossipMessage::Chat(EventEnvelope::decode_json(payload)?)),
        TopicKind::Rules => Ok(GossipMessage::Rule(serde_json::from_slice(payload)?)),
        TopicKind::Checkpoints => Ok(GossipMessage::Checkpoint(serde_json::from_slice(payload)?)),
        TopicKind::Summaries => Ok(GossipMessage::Summary(serde_json::from_slice(payload)?)),
    }
}

fn encode_overlay_payload(message: &GossipMessage) -> Result<Vec<u8>> {
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
        events,
    })
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
        };
        let response = decode_backfill_response(RawBackfillResponse {
            scope: SwarmScope::Global,
            next_from_event_seq: 2,
            feed_key: None,
            items: vec![envelope.encode_json().unwrap()],
        })
        .unwrap();
        assert_eq!(response.events, vec![envelope]);
    }
}
