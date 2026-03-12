use anyhow::{Result, anyhow, bail};
use libp2p::futures::{FutureExt, StreamExt};
use libp2p::gossipsub::{
    Behaviour as Gossipsub, ConfigBuilder as GossipsubConfigBuilder, Event as GossipsubEvent,
};
use libp2p::gossipsub::{IdentTopic, MessageAuthenticity, ValidationMode};
use libp2p::identify;
use libp2p::mdns;
use libp2p::request_response::{self, Message as RequestResponseMessage, ProtocolSupport};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent, behaviour::toggle::Toggle};
pub use libp2p::{Multiaddr, PeerId};
use libp2p::{StreamProtocol, Swarm, SwarmBuilder, identity, noise, tcp, yamux};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use wattswarm_protocol::types::Event;

const DEFAULT_NAMESPACE: &str = "wattswarm";
const DEFAULT_PROTOCOL_VERSION: &str = "/wattswarm/0.1.0";
const DEFAULT_MAX_BACKFILL_EVENTS: usize = 512;
const MAX_BACKFILL_EVENTS_HARD_LIMIT: usize = 8_192;
const BACKFILL_PROTOCOL: StreamProtocol = StreamProtocol::new("/wattswarm/backfill/1");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmScope {
    Global,
    Region(String),
    Local(String),
}

impl SwarmScope {
    pub fn label(&self) -> Result<String> {
        match self {
            Self::Global => Ok("global".to_owned()),
            Self::Region(region_id) => Ok(format!("region.{}", sanitize_segment(region_id)?)),
            Self::Local(local_id) => Ok(format!("local.{}", sanitize_segment(local_id)?)),
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
    pub fn validate(&self, max_limit: usize) -> Result<()> {
        if self.limit == 0 {
            bail!("backfill limit must be > 0");
        }
        if self.limit > max_limit {
            bail!("backfill limit exceeds configured max");
        }
        if self.limit > MAX_BACKFILL_EVENTS_HARD_LIMIT {
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
    pub namespace: TopicNamespace,
    pub protocol_version: String,
    pub listen_addrs: Vec<String>,
    pub max_backfill_events: usize,
    pub enable_mdns: bool,
}

impl Default for NetworkP2pConfig {
    fn default() -> Self {
        Self {
            namespace: TopicNamespace::default(),
            protocol_version: DEFAULT_PROTOCOL_VERSION.to_owned(),
            listen_addrs: vec!["/ip4/0.0.0.0/tcp/0".to_owned()],
            max_backfill_events: DEFAULT_MAX_BACKFILL_EVENTS,
            enable_mdns: true,
        }
    }
}

impl NetworkP2pConfig {
    pub fn validate(&self) -> Result<()> {
        if self.listen_addrs.is_empty() {
            bail!("at least one listen address is required");
        }
        if self.protocol_version.trim().is_empty() {
            bail!("protocol_version is required");
        }
        if self.max_backfill_events == 0 {
            bail!("max_backfill_events must be > 0");
        }
        if self.max_backfill_events > MAX_BACKFILL_EVENTS_HARD_LIMIT {
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

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "WattSwarmBehaviourEvent")]
pub struct WattSwarmBehaviour {
    pub gossipsub: Gossipsub,
    pub identify: identify::Behaviour,
    pub request_response: request_response::cbor::Behaviour<BackfillRequest, BackfillResponse>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
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
}

pub struct NetworkP2pNode {
    config: NetworkP2pConfig,
    local_key: identity::Keypair,
    local_peer_id: PeerId,
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

    pub fn build_behaviour(&self) -> Result<WattSwarmBehaviour> {
        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(self.local_key.clone()),
            GossipsubConfigBuilder::default()
                .validation_mode(ValidationMode::Strict)
                .build()
                .map_err(|err| anyhow!(err))?,
        )
        .map_err(|err| anyhow!(err))?;

        let identify = identify::Behaviour::new(
            identify::Config::new(
                self.config.protocol_version.clone(),
                self.local_key.public(),
            )
            .with_agent_version("wattswarm-network-p2p".to_owned()),
        );

        let request_response = request_response::cbor::Behaviour::new(
            [(BACKFILL_PROTOCOL, ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let mdns = Toggle::from(if self.config.enable_mdns {
            Some(mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                self.local_peer_id,
            )?)
        } else {
            None
        });

        Ok(WattSwarmBehaviour {
            gossipsub,
            identify,
            request_response,
            mdns,
        })
    }

    pub fn build_swarm(&self) -> Result<Swarm<WattSwarmBehaviour>> {
        let behaviour = self.build_behaviour()?;
        Ok(SwarmBuilder::with_existing_identity(self.local_key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(move |_| behaviour)?
            .build())
    }
}

pub struct NetworkRuntime {
    config: NetworkP2pConfig,
    local_peer_id: PeerId,
    swarm: Swarm<WattSwarmBehaviour>,
    listen_addrs: Vec<Multiaddr>,
    subscribed_topics: HashSet<String>,
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
        };
        this.listen_on_configured_addrs()?;
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

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        self.swarm.dial(addr).map_err(|err| anyhow!(err))?;
        Ok(())
    }

    pub fn publish_gossip(&mut self, message: &GossipMessage) -> Result<()> {
        let topic = IdentTopic::new(
            self.config
                .namespace
                .topic_name(message.scope(), message.kind())?,
        );
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
        request.validate(self.config.max_backfill_events)?;
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
            )) => Ok(Some(NetworkRuntimeEvent::Gossip {
                propagation_source,
                message: GossipMessage::decode_json(&message.data)?,
            })),
            SwarmEvent::Behaviour(WattSwarmBehaviourEvent::RequestResponse(
                request_response::Event::Message { peer, message, .. },
            )) => match message {
                RequestResponseMessage::Request {
                    request, channel, ..
                } => Ok(Some(NetworkRuntimeEvent::BackfillRequest {
                    peer,
                    request,
                    channel,
                })),
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
                    self.swarm.add_peer_address(peer, addr);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer);
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
            _ => Ok(None),
        }
    }
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
        req.validate(256).expect("valid");
        assert!(req.validate(64).is_err());
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
    fn config_parses_multiaddrs() {
        let config = NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_owned()],
            ..NetworkP2pConfig::default()
        };
        let addrs = config.parse_listen_addrs().expect("multiaddr parse");
        assert_eq!(addrs.len(), 1);
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
