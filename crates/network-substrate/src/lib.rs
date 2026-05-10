use anyhow::{Context, Result, anyhow, bail};
use iroh::EndpointId;
use iroh_gossip::api::{Event as IrohGossipEvent, GossipSender};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::{Runtime, RuntimeFlavor};
use wattswarm_network_transport_core::{
    PeerTransportCapabilities, TransportContactMaterial, TransportMetadata, TransportRoute,
};
use wattswarm_network_transport_iroh::{
    DEFAULT_IROH_CONTROL_ALPN, IrohControlStreamRequest, IrohControlStreamResponse,
    derive_gossip_topic_id, export_local_contact_material_for_network_peer_id,
    install_local_contact_material_control_handler_for_network_peer_id,
    local_endpoint_id_from_state_dir, local_gossip_for_network_peer_id,
    register_remote_contact_material_for_network_peer_id,
    send_control_stream_request_for_network_peer_id,
    set_local_control_stream_handler_for_network_peer_id, shutdown_local_iroh_data_plane,
};
use wattswarm_protocol::types::ArtifactRef;

const DEFAULT_NAMESPACE: &str = "wattswarm";
const DEFAULT_IDENTIFY_AGENT_NAME: &str = "wattswarm-network-substrate";
const MAX_BACKFILL_KNOWN_EVENT_IDS: usize = 256;
const MAX_BACKFILL_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_AGENT_ENVELOPE_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTACT_MATERIAL_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTROL_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTROL_DETAIL_BYTES: usize = 8 * 1024;
const DEFAULT_SUBSTRATE_CONTROL_REQUEST_TIMEOUT_MS: u64 = 30_000;
const IROH_CONTROL_KIND_BACKFILL: &str = "backfill.v1";
const IROH_CONTROL_KIND_CONTACT_MATERIAL: &str = "contact_material.v1";
const IROH_CONTROL_KIND_PEER_RELATIONSHIP: &str = "peer_relationship.v1";
const IROH_CONTROL_KIND_PEER_DIRECT_MESSAGE: &str = "peer_direct_message.v1";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NetworkNodeId(String);

impl NetworkNodeId {
    pub fn new(raw: impl Into<String>) -> Result<Self> {
        Ok(Self(parse_iroh_endpoint_id_string(raw, "network node id")?))
    }

    pub fn from_endpoint_id(endpoint_id: EndpointId) -> Self {
        Self(endpoint_id.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub fn random() -> Self {
        static NEXT_TEST_NODE_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let now = unix_timestamp_millis();
        let sequence = NEXT_TEST_NODE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut seed = [0_u8; 32];
        seed[..8].copy_from_slice(&now.to_le_bytes());
        seed[8..16].copy_from_slice(&u64::from(std::process::id()).to_le_bytes());
        seed[16..24].copy_from_slice(&sequence.to_le_bytes());
        let secret_key = iroh::SecretKey::from_bytes(&seed);
        let endpoint_id = EndpointId::from_bytes(secret_key.public().as_bytes())
            .expect("iroh secret public key must be a valid endpoint id");
        Self::from_endpoint_id(endpoint_id)
    }
}

impl fmt::Display for NetworkNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for NetworkNodeId {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        Self::new(raw.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NetworkAddress(String);

impl NetworkAddress {
    pub fn new(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        if raw.trim().is_empty() {
            bail!("network address cannot be empty");
        }
        if raw.chars().any(char::is_whitespace) {
            bail!("network address cannot contain whitespace");
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NetworkAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for NetworkAddress {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        Self::new(raw.to_owned())
    }
}

macro_rules! define_request_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0)
            }
        }
    };
}

define_request_id!(BackfillRequestId);
define_request_id!(ContactMaterialRequestId);
define_request_id!(PeerRelationshipRequestId);
define_request_id!(PeerDirectMessageRequestId);

pub type BackfillResponseChannel = Sender<RawBackfillResponse>;
pub type ContactMaterialResponseChannel = Sender<RawContactMaterialResponse>;
pub type PeerRelationshipResponseChannel = Sender<RawPeerRelationshipResponse>;
pub type PeerDirectMessageResponseChannel = Sender<RawPeerDirectMessageResponse>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GossipKind {
    Events,
    Messages,
    Rules,
    Checkpoints,
    Summaries,
    Discovery,
}

impl GossipKind {
    pub const ALL: [Self; 6] = [
        Self::Events,
        Self::Messages,
        Self::Rules,
        Self::Checkpoints,
        Self::Summaries,
        Self::Discovery,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Messages => "messages",
            Self::Rules => "rules",
            Self::Checkpoints => "checkpoints",
            Self::Summaries => "summaries",
            Self::Discovery => "discovery",
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
    pub fn topic_name(&self, scope: &SwarmScope, kind: GossipKind) -> Result<String> {
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
    pub discovery: String,
}

impl TopicCatalog {
    pub fn new(namespace: &TopicNamespace, scope: &SwarmScope) -> Result<Self> {
        Ok(Self {
            events: namespace.topic_name(scope, GossipKind::Events)?,
            messages: namespace.topic_name(scope, GossipKind::Messages)?,
            rules: namespace.topic_name(scope, GossipKind::Rules)?,
            checkpoints: namespace.topic_name(scope, GossipKind::Checkpoints)?,
            summaries: namespace.topic_name(scope, GossipKind::Summaries)?,
            discovery: namespace.topic_name(scope, GossipKind::Discovery)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubstrateConfig {
    pub namespace: TopicNamespace,
    pub protocol_version: String,
    #[serde(default)]
    pub listen_addrs: Vec<String>,
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    pub max_backfill_events: usize,
    pub max_backfill_events_hard_limit: usize,
    pub control_request_timeout_ms: u64,
}

impl Default for SubstrateConfig {
    fn default() -> Self {
        Self {
            namespace: TopicNamespace::default(),
            protocol_version: "wattswarm/iroh/1".to_owned(),
            listen_addrs: Vec::new(),
            bootstrap_peers: Vec::new(),
            max_backfill_events: 512,
            max_backfill_events_hard_limit: 8192,
            control_request_timeout_ms: DEFAULT_SUBSTRATE_CONTROL_REQUEST_TIMEOUT_MS,
        }
    }
}

impl SubstrateConfig {
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version.trim().is_empty() {
            bail!("network protocol version cannot be empty");
        }
        if self.max_backfill_events == 0 {
            bail!("max_backfill_events must be > 0");
        }
        if self.max_backfill_events > self.max_backfill_events_hard_limit {
            bail!("max_backfill_events exceeds hard limit");
        }
        for addr in &self.listen_addrs {
            NetworkAddress::new(addr.clone())?;
        }
        for peer in &self.bootstrap_peers {
            Self::parse_bootstrap_peer(peer)?;
        }
        Ok(())
    }

    pub fn parse_listen_addrs(&self) -> Result<Vec<NetworkAddress>> {
        self.listen_addrs
            .iter()
            .map(|addr| NetworkAddress::new(addr.clone()))
            .collect()
    }

    pub fn parse_bootstrap_peers(&self) -> Result<Vec<(NetworkNodeId, NetworkAddress)>> {
        self.bootstrap_peers
            .iter()
            .map(|peer| Self::parse_bootstrap_peer(peer))
            .collect()
    }

    pub fn topic_catalog(&self, scope: &SwarmScope) -> Result<TopicCatalog> {
        TopicCatalog::new(&self.namespace, scope)
    }

    fn parse_bootstrap_peer(raw: &str) -> Result<(NetworkNodeId, NetworkAddress)> {
        let (peer, addr) = raw
            .split_once('@')
            .ok_or_else(|| anyhow!("bootstrap peer must use <iroh_node_id>@<address>"))?;
        Ok((
            NetworkNodeId::new(peer.to_owned())?,
            NetworkAddress::new(addr.to_owned())?,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct SubstrateNode {
    config: SubstrateConfig,
    local_peer_id: NetworkNodeId,
    state_dir: PathBuf,
}

impl SubstrateNode {
    pub fn from_state_dir(config: SubstrateConfig, state_dir: impl Into<PathBuf>) -> Result<Self> {
        config.validate()?;
        let state_dir = state_dir.into();
        let endpoint_id = local_endpoint_id_from_state_dir(&state_dir)?;
        Ok(Self {
            config,
            local_peer_id: NetworkNodeId::from_endpoint_id(endpoint_id),
            state_dir,
        })
    }

    pub fn from_seed_bytes(
        config: SubstrateConfig,
        state_dir: impl Into<PathBuf>,
        secret_key_32: [u8; 32],
    ) -> Result<Self> {
        let state_dir = state_dir.into();
        std::fs::create_dir_all(&state_dir)?;
        std::fs::write(state_dir.join("node_seed.hex"), hex::encode(secret_key_32))?;
        Self::from_state_dir(config, state_dir)
    }

    pub fn local_peer_id(&self) -> NetworkNodeId {
        self.local_peer_id.clone()
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn config(&self) -> &SubstrateConfig {
        &self.config
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
pub struct PeerMetadata {
    pub network_id: String,
    pub params_version: u64,
    pub params_hash: String,
    pub agent_version_raw: String,
    pub agent_version_prefix: String,
    pub protocol_version: String,
    pub observed_addr: String,
    pub listen_addrs: Vec<String>,
    pub protocols: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackfillRequest {
    pub scope: SwarmScope,
    pub from_event_seq: u64,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub known_event_ids: Vec<String>,
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
        if self.known_event_ids.len() > MAX_BACKFILL_KNOWN_EVENT_IDS {
            bail!("backfill known_event_ids exceeds configured max");
        }
        if self
            .known_event_ids
            .iter()
            .any(|event_id| event_id.trim().is_empty())
        {
            bail!("backfill known_event_ids must not contain empty event ids");
        }
        Ok(())
    }
}

trait InboundControlPeer {
    fn inbound_peer(&self, fallback: &NetworkNodeId) -> Result<NetworkNodeId>;
}

impl InboundControlPeer for RawBackfillRequest {
    fn inbound_peer(&self, fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        Ok(fallback.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackfillResponse {
    pub scope: SwarmScope,
    pub next_from_event_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub head_event_ids: Vec<String>,
    pub items: Vec<Vec<u8>>,
}

impl RawBackfillResponse {
    pub fn validate(&self, max_limit: usize, hard_limit: usize) -> Result<()> {
        if self.items.len() > max_limit {
            bail!("backfill response items exceeds configured max");
        }
        if self.items.len() > hard_limit {
            bail!("backfill response items exceeds hard safety limit");
        }
        let response_bytes = self
            .items
            .iter()
            .try_fold(0usize, |total, item| total.checked_add(item.len()))
            .ok_or_else(|| anyhow!("backfill response byte count overflow"))?;
        if response_bytes > MAX_BACKFILL_RESPONSE_BYTES {
            bail!("backfill response bytes exceeds configured max");
        }
        if let Some(feed_key) = &self.feed_key
            && feed_key.trim().is_empty()
        {
            bail!("backfill response feed_key must not be empty");
        }
        if self.head_event_ids.len() > MAX_BACKFILL_KNOWN_EVENT_IDS {
            bail!("backfill response head_event_ids exceeds configured max");
        }
        if self
            .head_event_ids
            .iter()
            .any(|event_id| event_id.trim().is_empty())
        {
            bail!("backfill response head_event_ids must not contain empty event ids");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawPeerRelationshipAction {
    Request,
    Accept,
    Reject,
    Cancel,
    Remove,
    Block,
    Unblock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerRelationshipRequest {
    pub source_node_id: String,
    pub target_node_id: String,
    pub action: RawPeerRelationshipAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<RawAgentEnvelope>,
}

impl RawPeerRelationshipRequest {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer relationship request source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer relationship request target_node_id is required");
        }
        if let Some(envelope) = &self.agent_envelope {
            envelope.validate()?;
        }
        Ok(())
    }
}

impl InboundControlPeer for RawPeerRelationshipRequest {
    fn inbound_peer(&self, _fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        NetworkNodeId::new(self.source_node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerRelationshipResponse {
    pub source_node_id: String,
    pub target_node_id: String,
    pub action: RawPeerRelationshipAction,
    pub applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<RawAgentEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub updated_at: u64,
}

impl RawPeerRelationshipResponse {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer relationship response source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer relationship response target_node_id is required");
        }
        if let Some(envelope) = &self.agent_envelope {
            envelope.validate()?;
        }
        if let Some(detail) = &self.detail {
            validate_max_bytes(
                "peer relationship response detail",
                detail,
                MAX_CONTROL_DETAIL_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawAgentEnvelope {
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(default)]
    pub message_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl RawAgentEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.protocol.trim().is_empty() {
            bail!("agent envelope protocol is required");
        }
        validate_max_bytes(
            "agent envelope message_json",
            &self.message_json,
            MAX_AGENT_ENVELOPE_JSON_BYTES,
        )?;
        if let Some(extensions_json) = &self.extensions_json {
            validate_max_bytes(
                "agent envelope extensions_json",
                extensions_json,
                MAX_AGENT_ENVELOPE_JSON_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawContactMaterial {
    pub material_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub generated_at: u64,
}

impl RawContactMaterial {
    pub fn validate(&self) -> Result<()> {
        validate_max_bytes(
            "contact material",
            &self.material_json,
            MAX_CONTACT_MATERIAL_JSON_BYTES,
        )?;
        if self.generated_at == 0 {
            bail!("contact material generated_at is required");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawContactMaterialRequest {
    pub source_node_id: String,
    pub target_node_id: String,
}

impl RawContactMaterialRequest {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("contact material request source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("contact material request target_node_id is required");
        }
        Ok(())
    }
}

impl InboundControlPeer for RawContactMaterialRequest {
    fn inbound_peer(&self, _fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        NetworkNodeId::new(self.source_node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawContactMaterialResponse {
    pub source_node_id: String,
    pub target_node_id: String,
    pub applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<RawContactMaterial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub updated_at: u64,
}

impl RawContactMaterialResponse {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("contact material response source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("contact material response target_node_id is required");
        }
        if let Some(material) = &self.contact_material {
            material.validate()?;
        }
        if let Some(detail) = &self.detail {
            validate_max_bytes(
                "contact material response detail",
                detail,
                MAX_CONTROL_DETAIL_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawPeerDirectMessageKind {
    RelationshipEstablished,
    SessionInit,
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerDirectMessageRequest {
    pub source_node_id: String,
    pub target_node_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub kind: RawPeerDirectMessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<RawAgentEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<RawContactMaterial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_json: Option<String>,
}

impl RawPeerDirectMessageRequest {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer direct message source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer direct message target_node_id is required");
        }
        if self.thread_id.trim().is_empty() {
            bail!("peer direct message thread_id is required");
        }
        if self.message_id.trim().is_empty() {
            bail!("peer direct message message_id is required");
        }
        if let Some(envelope) = &self.agent_envelope {
            envelope.validate()?;
        }
        if let Some(material) = &self.contact_material {
            material.validate()?;
        }
        match self.kind {
            RawPeerDirectMessageKind::Message => {
                let Some(content_ref) = &self.content_ref else {
                    bail!("peer direct message content_ref is required for message kind");
                };
                if content_ref.uri.trim().is_empty()
                    || content_ref.digest.trim().is_empty()
                    || content_ref.mime.trim().is_empty()
                    || content_ref.producer.trim().is_empty()
                    || content_ref.size_bytes == 0
                {
                    bail!("peer direct message content_ref is invalid");
                }
            }
            RawPeerDirectMessageKind::RelationshipEstablished
            | RawPeerDirectMessageKind::SessionInit => {
                let control_json = self.control_json.as_deref().unwrap_or("");
                if control_json.trim().is_empty() {
                    bail!("peer direct message control_json is required for control kinds");
                }
                validate_max_bytes(
                    "peer direct message control_json",
                    control_json,
                    MAX_CONTROL_JSON_BYTES,
                )?;
            }
        }
        Ok(())
    }
}

impl InboundControlPeer for RawPeerDirectMessageRequest {
    fn inbound_peer(&self, _fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        NetworkNodeId::new(self.source_node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerDirectMessageResponse {
    pub source_node_id: String,
    pub target_node_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub kind: RawPeerDirectMessageKind,
    pub applied: bool,
    pub delivery_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<RawContactMaterial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub updated_at: u64,
}

impl RawPeerDirectMessageResponse {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer direct message response source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer direct message response target_node_id is required");
        }
        if self.thread_id.trim().is_empty() {
            bail!("peer direct message response thread_id is required");
        }
        if self.message_id.trim().is_empty() {
            bail!("peer direct message response message_id is required");
        }
        if self.delivery_state.trim().is_empty() {
            bail!("peer direct message response delivery_state is required");
        }
        if let Some(material) = &self.contact_material {
            material.validate()?;
        }
        if let Some(detail) = &self.detail {
            validate_max_bytes(
                "peer direct message response detail",
                detail,
                MAX_CONTROL_DETAIL_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawGossipMessage {
    pub scope: SwarmScope,
    pub kind: GossipKind,
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
#[allow(clippy::large_enum_variant)]
pub enum RawControlRequest {
    Backfill(RawBackfillRequest),
    ContactMaterial(RawContactMaterialRequest),
    PeerRelationship(RawPeerRelationshipRequest),
    PeerDirectMessage(RawPeerDirectMessageRequest),
}

impl RawControlRequest {
    pub fn validate(&self, max_backfill_events: usize, backfill_hard_limit: usize) -> Result<()> {
        match self {
            Self::Backfill(request) => request.validate(max_backfill_events, backfill_hard_limit),
            Self::ContactMaterial(request) => request.validate(),
            Self::PeerRelationship(request) => request.validate(),
            Self::PeerDirectMessage(request) => request.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RawControlResponse {
    Backfill(RawBackfillResponse),
    ContactMaterial(RawContactMaterialResponse),
    PeerRelationship(RawPeerRelationshipResponse),
    PeerDirectMessage(RawPeerDirectMessageResponse),
}

impl RawControlResponse {
    pub fn validate(&self, max_backfill_events: usize, backfill_hard_limit: usize) -> Result<()> {
        match self {
            Self::Backfill(response) => response.validate(max_backfill_events, backfill_hard_limit),
            Self::ContactMaterial(response) => response.validate(),
            Self::PeerRelationship(response) => response.validate(),
            Self::PeerDirectMessage(response) => response.validate(),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SubstrateRuntimeEvent {
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
        message: RawGossipMessage,
    },
    BackfillRequest {
        peer: NetworkNodeId,
        request: RawBackfillRequest,
        channel: BackfillResponseChannel,
    },
    BackfillResponse {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        response: RawBackfillResponse,
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
        request: RawContactMaterialRequest,
        channel: ContactMaterialResponseChannel,
    },
    ContactMaterialResponse {
        peer: NetworkNodeId,
        request_id: ContactMaterialRequestId,
        response: RawContactMaterialResponse,
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
        request: RawPeerRelationshipRequest,
        channel: PeerRelationshipResponseChannel,
    },
    PeerRelationshipResponse {
        peer: NetworkNodeId,
        request_id: PeerRelationshipRequestId,
        response: RawPeerRelationshipResponse,
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
    PeerDirectMessageRequest {
        peer: NetworkNodeId,
        request: RawPeerDirectMessageRequest,
        channel: PeerDirectMessageResponseChannel,
    },
    PeerDirectMessageResponse {
        peer: NetworkNodeId,
        request_id: PeerDirectMessageRequestId,
        response: RawPeerDirectMessageResponse,
    },
    PeerDirectMessageOutboundFailure {
        peer: NetworkNodeId,
        request_id: PeerDirectMessageRequestId,
        error: String,
    },
    PeerDirectMessageInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IrohGossipSubscription {
    pub scope: SwarmScope,
    pub kind: GossipKind,
    pub topic_id_hex: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohRuntimeCounters {
    pub malformed_gossip_notifications: u64,
    pub scope_kind_mismatch_drops: u64,
    pub stale_contact_material_drops: u64,
    pub invalid_control_payloads: u64,
    pub stream_request_attempts: u64,
    pub stream_request_successes: u64,
    pub stream_request_failures: u64,
    pub retry_suppressed_connects: u64,
    pub request_limit_drops: u64,
}

struct IrohGossipTopicHandle {
    sender: GossipSender,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
enum IrohGossipInbound {
    Message {
        subscription: IrohGossipSubscription,
        delivered_from: String,
        bytes: Vec<u8>,
    },
    NeighborUp {
        peer: String,
    },
    NeighborDown {
        peer: String,
    },
    Malformed {
        remote_network_peer_id: String,
        error: String,
    },
    Lagged,
}

pub struct SubstrateRuntime {
    config: SubstrateConfig,
    runtime: Option<Runtime>,
    state_dir: PathBuf,
    local_peer_id: NetworkNodeId,
    local_endpoint_id: String,
    local_listen_addrs: Vec<NetworkAddress>,
    subscriptions: HashSet<IrohGossipSubscription>,
    gossip_topics: HashMap<IrohGossipSubscription, IrohGossipTopicHandle>,
    gossip_tx: Sender<IrohGossipInbound>,
    gossip_rx: Receiver<IrohGossipInbound>,
    control_tx: Sender<SubstrateRuntimeEvent>,
    control_rx: Receiver<SubstrateRuntimeEvent>,
    pending_events: VecDeque<SubstrateRuntimeEvent>,
    remote_contacts: HashMap<String, TransportContactMaterial>,
    counters: IrohRuntimeCounters,
    next_request_id: u64,
}

impl SubstrateRuntime {
    pub fn new(node: SubstrateNode) -> Result<Self> {
        let local_contact = export_local_contact_material_for_network_peer_id(
            node.state_dir(),
            node.local_peer_id.as_str(),
            unix_timestamp_millis() / 1000,
        )?;
        install_local_contact_material_control_handler_for_network_peer_id(
            node.state_dir(),
            node.local_peer_id.as_str(),
        )?;
        let runtime = Runtime::new()?;
        let (gossip_tx, gossip_rx) = mpsc::channel();
        let (control_tx, control_rx) = mpsc::channel();
        let mut this = Self {
            local_endpoint_id: local_endpoint_id_from_state_dir(node.state_dir())?.to_string(),
            local_listen_addrs: local_contact
                .metadata
                .listen_addrs
                .iter()
                .filter_map(|addr| NetworkAddress::new(addr.clone()).ok())
                .collect(),
            config: node.config,
            runtime: Some(runtime),
            state_dir: node.state_dir,
            local_peer_id: node.local_peer_id,
            subscriptions: HashSet::new(),
            gossip_topics: HashMap::new(),
            gossip_tx,
            gossip_rx,
            control_tx,
            control_rx,
            pending_events: VecDeque::new(),
            remote_contacts: HashMap::new(),
            counters: IrohRuntimeCounters::default(),
            next_request_id: 1,
        };
        this.install_control_handlers()?;
        for (peer, address) in this.config.parse_bootstrap_peers()? {
            let dial_address = NetworkAddress::new(format!("{peer}@{address}"))?;
            this.dial(dial_address)?;
        }
        Ok(this)
    }

    pub fn local_peer_id(&self) -> NetworkNodeId {
        self.local_peer_id.clone()
    }

    pub fn listen_addrs(&self) -> &[NetworkAddress] {
        &self.local_listen_addrs
    }

    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("substrate runtime must be available until drop")
    }

    fn block_on<T: Send>(&self, future: impl Future<Output = T> + Send) -> T {
        let runtime = self.runtime();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| runtime.block_on(future))
            }
            Ok(_) => std::thread::scope(|scope| {
                scope
                    .spawn(move || runtime.block_on(future))
                    .join()
                    .expect("join substrate runtime blocking operation")
            }),
            Err(_) => runtime.block_on(future),
        }
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        let mut subscribed_iroh_gossip_topics = self
            .subscriptions
            .iter()
            .map(|subscription| {
                format!(
                    "{:?}:{}:{}",
                    subscription.scope,
                    subscription.kind.as_str(),
                    subscription.topic_id_hex
                )
            })
            .collect::<Vec<_>>();
        subscribed_iroh_gossip_topics.sort();
        NetworkRuntimeObservabilitySnapshot {
            p2p_foundation: "iroh".to_owned(),
            local_iroh_endpoint_id: Some(self.local_endpoint_id.clone()),
            subscribed_iroh_gossip_topics,
            known_iroh_contacts: self.remote_contacts.len(),
            legacy_transport_active: false,
            nat_status: "iroh-direct".to_owned(),
            nat_public_address: None,
            nat_confidence: 0,
            relay_reservations: Vec::new(),
            peer_health: Vec::new(),
            dropped_malformed_gossip: self.counters.malformed_gossip_notifications,
            invalid_control_payloads: self.counters.invalid_control_payloads,
            dial_failures: self.counters.stream_request_failures,
            response_validation_failures: 0,
            retry_suppressed_dials: self.counters.retry_suppressed_connects,
        }
    }

    pub fn upsert_remote_contact_material(
        &mut self,
        remote_network_peer_id: impl Into<String>,
        mut contact: TransportContactMaterial,
    ) -> Result<bool> {
        let remote_network_peer_id = NetworkNodeId::new(remote_network_peer_id.into())?.to_string();
        let contact_endpoint = contact_endpoint_id(&contact)?;
        let contact_endpoint = contact_endpoint.to_string();
        if contact.peer_id != contact_endpoint {
            bail!(
                "iroh contact material peer_id {} must match endpoint_id {}",
                contact.peer_id,
                contact_endpoint
            );
        }
        if remote_network_peer_id != contact_endpoint {
            bail!(
                "remote network node id {remote_network_peer_id} must match iroh endpoint_id {contact_endpoint}"
            );
        }
        contact.metadata.endpoint_id = Some(contact_endpoint.clone());
        if self
            .remote_contacts
            .get(&remote_network_peer_id)
            .is_some_and(|existing| existing.metadata.generated_at > contact.metadata.generated_at)
        {
            self.counters.stale_contact_material_drops =
                self.counters.stale_contact_material_drops.saturating_add(1);
            return Ok(false);
        }
        register_remote_contact_material_for_network_peer_id(
            &self.state_dir,
            self.local_peer_id.as_str(),
            &contact,
        )?;
        self.remote_contacts.insert(remote_network_peer_id, contact);
        self.join_gossip_topics_with_bootstrap_contacts()?;
        Ok(true)
    }

    pub fn allows_outbound_backfill_to(&self, peer: &NetworkNodeId) -> bool {
        self.remote_contacts.contains_key(peer.as_str())
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.subscribe_scope_kinds(scope, &GossipKind::ALL)
    }

    pub fn subscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        for kind in kinds {
            let subscription = self.subscription_for(scope, *kind)?;
            self.ensure_gossip_topic(subscription)?;
        }
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
        for kind in kinds {
            let subscription = self.subscription_for(scope, *kind)?;
            self.subscriptions.remove(&subscription);
            if let Some(handle) = self.gossip_topics.remove(&subscription) {
                handle.task.abort();
            }
        }
        Ok(())
    }

    pub fn publish(&mut self, scope: &SwarmScope, kind: GossipKind, payload: &[u8]) -> Result<()> {
        let message = RawGossipMessage {
            scope: scope.clone(),
            kind,
            payload: payload.to_vec(),
        };
        let bytes = message.encode_json()?;
        let subscription = self.subscription_for(scope, kind)?;
        self.ensure_gossip_topic(subscription.clone())?;
        let topic = self
            .gossip_topics
            .get(&subscription)
            .ok_or_else(|| anyhow!("missing iroh gossip topic {}", subscription.topic_id_hex))?;
        self.block_on(topic.sender.broadcast(bytes.into()))
            .map_err(|err| anyhow!("publish iroh gossip notification: {err}"))?;
        Ok(())
    }

    pub fn dial(&mut self, addr: NetworkAddress) -> Result<()> {
        let Some((raw_peer, raw_direct_addr)) = addr.as_str().split_once('@') else {
            return Ok(());
        };
        let peer = NetworkNodeId::new(raw_peer.to_owned())?;
        EndpointId::from_str(raw_peer).context("parse iroh endpoint id from dial target")?;
        let direct_addr = NetworkAddress::new(raw_direct_addr.to_owned())?;
        let contact = TransportContactMaterial {
            transport: TransportRoute::IrohDirect.as_str().to_owned(),
            peer_id: peer.to_string(),
            metadata: TransportMetadata {
                route: TransportRoute::IrohDirect,
                generated_at: unix_timestamp_millis(),
                endpoint_id: Some(peer.to_string()),
                alpn: Some(DEFAULT_IROH_CONTROL_ALPN.to_owned()),
                listen_addrs: vec![direct_addr.to_string()],
                capabilities: PeerTransportCapabilities::iroh_direct_default(),
            },
            extra: serde_json::json!({
                "endpoint_id": peer.to_string(),
                "alpn": DEFAULT_IROH_CONTROL_ALPN,
                "direct_addrs": [direct_addr.to_string()],
                "relay_urls": [],
            }),
        };
        self.upsert_remote_contact_material(peer.to_string(), contact)?;
        self.pending_events
            .push_back(SubstrateRuntimeEvent::PeerDiscovered {
                peer: peer.clone(),
                address: direct_addr.clone(),
                source: PeerDiscoverySourceKind::Bootstrap,
            });
        Ok(())
    }

    fn join_gossip_topics_with_bootstrap_contacts(&self) -> Result<()> {
        if self.gossip_topics.is_empty() {
            return Ok(());
        }
        let bootstrap = self
            .remote_contacts
            .values()
            .map(contact_endpoint_id)
            .collect::<Result<Vec<_>>>()?;
        if bootstrap.is_empty() {
            return Ok(());
        }
        for topic in self.gossip_topics.values() {
            self.block_on(topic.sender.join_peers(bootstrap.clone()))
                .map_err(|err| anyhow!("join iroh gossip peers: {err}"))?;
        }
        Ok(())
    }

    pub fn send_backfill_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawBackfillRequest,
    ) -> Result<BackfillRequestId> {
        let request_id = BackfillRequestId::new(self.reserve_request_number());
        let (kind, payload) = match encode_raw_control_request(RawControlRequest::Backfill(request))
            .and_then(|(kind, payload)| {
                RawControlRequest::Backfill(serde_json::from_slice(&payload)?).validate(
                    self.config.max_backfill_events,
                    self.config.max_backfill_events_hard_limit,
                )?;
                Ok((kind, payload))
            }) {
            Ok(encoded) => encoded,
            Err(err) => {
                self.pending_events
                    .push_back(SubstrateRuntimeEvent::BackfillOutboundFailure {
                        peer: peer.clone(),
                        request_id,
                        error: err.to_string(),
                    });
                return Ok(request_id);
            }
        };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events
                .push_back(SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                });
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::Backfill(response)) => {
                    SubstrateRuntimeEvent::BackfillResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected backfill control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    pub fn send_backfill_response(
        &mut self,
        channel: BackfillResponseChannel,
        response: RawBackfillResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("backfill response channel closed"))
    }

    pub fn send_contact_material_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawContactMaterialRequest,
    ) -> Result<ContactMaterialRequestId> {
        let request_id = ContactMaterialRequestId::new(self.reserve_request_number());
        let (kind, payload) =
            match encode_raw_control_request(RawControlRequest::ContactMaterial(request)) {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.pending_events.push_back(
                        SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                            peer: peer.clone(),
                            request_id,
                            error: err.to_string(),
                        },
                    );
                    return Ok(request_id);
                }
            };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events
                .push_back(SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                });
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::ContactMaterial(response)) => {
                    SubstrateRuntimeEvent::ContactMaterialResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected contact material control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    pub fn send_contact_material_response(
        &mut self,
        channel: ContactMaterialResponseChannel,
        response: RawContactMaterialResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("contact material response channel closed"))
    }

    pub fn send_peer_relationship_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawPeerRelationshipRequest,
    ) -> Result<PeerRelationshipRequestId> {
        let request_id = PeerRelationshipRequestId::new(self.reserve_request_number());
        let (kind, payload) =
            match encode_raw_control_request(RawControlRequest::PeerRelationship(request)) {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.pending_events.push_back(
                        SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                            peer: peer.clone(),
                            request_id,
                            error: err.to_string(),
                        },
                    );
                    return Ok(request_id);
                }
            };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events
                .push_back(SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                });
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::PeerRelationship(response)) => {
                    SubstrateRuntimeEvent::PeerRelationshipResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected peer relationship control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    pub fn send_peer_relationship_response(
        &mut self,
        channel: PeerRelationshipResponseChannel,
        response: RawPeerRelationshipResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("peer relationship response channel closed"))
    }

    pub fn send_peer_direct_message_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawPeerDirectMessageRequest,
    ) -> Result<PeerDirectMessageRequestId> {
        let request_id = PeerDirectMessageRequestId::new(self.reserve_request_number());
        let (kind, payload) =
            match encode_raw_control_request(RawControlRequest::PeerDirectMessage(request)) {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.pending_events.push_back(
                        SubstrateRuntimeEvent::PeerDirectMessageOutboundFailure {
                            peer: peer.clone(),
                            request_id,
                            error: err.to_string(),
                        },
                    );
                    return Ok(request_id);
                }
            };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events.push_back(
                SubstrateRuntimeEvent::PeerDirectMessageOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                },
            );
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::PeerDirectMessage(response)) => {
                    SubstrateRuntimeEvent::PeerDirectMessageResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::PeerDirectMessageOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected peer direct message control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::PeerDirectMessageOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    fn spawn_control_request(
        &self,
        peer: &NetworkNodeId,
        kind: String,
        payload: Vec<u8>,
        remote_contact: TransportContactMaterial,
        build_event: impl FnOnce(
            NetworkNodeId,
            std::result::Result<RawControlResponse, anyhow::Error>,
        ) -> SubstrateRuntimeEvent
        + Send
        + 'static,
    ) {
        let state_dir = self.state_dir.clone();
        let local_peer_id = self.local_peer_id.clone();
        let peer = peer.clone();
        let control_tx = self.control_tx.clone();
        std::thread::spawn(move || {
            let response = send_control_stream_request_for_network_peer_id(
                &state_dir,
                local_peer_id.as_str(),
                &remote_contact,
                &IrohControlStreamRequest {
                    kind: kind.clone(),
                    payload,
                },
            )
            .and_then(|response| decode_raw_control_response(&kind, response));
            let _ = control_tx.send(build_event(peer, response));
        });
    }

    pub fn send_peer_direct_message_response(
        &mut self,
        channel: PeerDirectMessageResponseChannel,
        response: RawPeerDirectMessageResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("peer direct message response channel closed"))
    }

    pub async fn next_event(&mut self) -> Result<SubstrateRuntimeEvent> {
        loop {
            if let Some(event) = self.try_next_event()? {
                return Ok(event);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub fn try_next_event(&mut self) -> Result<Option<SubstrateRuntimeEvent>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }
        while let Ok(event) = self.control_rx.try_recv() {
            self.pending_events.push_back(event);
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
        }
        while let Ok(inbound) = self.gossip_rx.try_recv() {
            match inbound {
                IrohGossipInbound::Message {
                    subscription,
                    delivered_from,
                    bytes,
                } => {
                    let message = RawGossipMessage::decode_json(&bytes)?;
                    if message.scope != subscription.scope || message.kind != subscription.kind {
                        self.counters.scope_kind_mismatch_drops =
                            self.counters.scope_kind_mismatch_drops.saturating_add(1);
                        continue;
                    }
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::Gossip {
                            propagation_source: NetworkNodeId::new(delivered_from)?,
                            message,
                        });
                }
                IrohGossipInbound::NeighborUp { peer } => {
                    let remote_addr = self
                        .remote_contacts
                        .get(peer.as_str())
                        .and_then(|contact| contact.metadata.listen_addrs.first())
                        .and_then(|addr| NetworkAddress::new(addr.clone()).ok())
                        .unwrap_or_else(|| {
                            NetworkAddress::new(peer.clone())
                                .expect("iroh node id is a network address")
                        });
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::ConnectionEstablished {
                            peer: NetworkNodeId::new(peer)?,
                            remote_addr,
                        });
                }
                IrohGossipInbound::NeighborDown { peer } => {
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::ConnectionClosed {
                            peer: NetworkNodeId::new(peer)?,
                            remaining_established: 0,
                        });
                }
                IrohGossipInbound::Malformed {
                    remote_network_peer_id,
                    error,
                } => {
                    self.counters.invalid_control_payloads =
                        self.counters.invalid_control_payloads.saturating_add(1);
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::PeerHandshakeRejected {
                            peer: NetworkNodeId::new(remote_network_peer_id)?,
                            detail: error,
                        });
                }
                IrohGossipInbound::Lagged => {
                    self.counters.malformed_gossip_notifications = self
                        .counters
                        .malformed_gossip_notifications
                        .saturating_add(1);
                }
            }
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    fn install_control_handlers(&mut self) -> Result<()> {
        self.install_typed_control_handler::<RawBackfillRequest, RawBackfillResponse>(
            IROH_CONTROL_KIND_BACKFILL,
            |peer, request, channel| SubstrateRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            },
        )?;
        self.install_typed_control_handler::<RawContactMaterialRequest, RawContactMaterialResponse>(
            IROH_CONTROL_KIND_CONTACT_MATERIAL,
            |peer, request, channel| SubstrateRuntimeEvent::ContactMaterialRequest {
                peer,
                request,
                channel,
            },
        )?;
        self.install_typed_control_handler::<RawPeerRelationshipRequest, RawPeerRelationshipResponse>(
            IROH_CONTROL_KIND_PEER_RELATIONSHIP,
            |peer, request, channel| SubstrateRuntimeEvent::PeerRelationshipRequest {
                peer,
                request,
                channel,
            },
        )?;
        self.install_typed_control_handler::<RawPeerDirectMessageRequest, RawPeerDirectMessageResponse>(
            IROH_CONTROL_KIND_PEER_DIRECT_MESSAGE,
            |peer, request, channel| SubstrateRuntimeEvent::PeerDirectMessageRequest {
                peer,
                request,
                channel,
            },
        )?;
        Ok(())
    }

    fn install_typed_control_handler<Req, Resp>(
        &self,
        kind: &'static str,
        build_event: impl Fn(NetworkNodeId, Req, Sender<Resp>) -> SubstrateRuntimeEvent
        + Send
        + Sync
        + 'static,
    ) -> Result<()>
    where
        Req: for<'de> Deserialize<'de> + Send + 'static,
        Resp: Serialize + Send + 'static,
        Req: InboundControlPeer,
    {
        let pending_tx = self.control_tx.clone();
        let local_peer_id = self.local_peer_id.clone();
        let timeout = Duration::from_millis(self.config.control_request_timeout_ms);
        set_local_control_stream_handler_for_network_peer_id(
            &self.state_dir,
            self.local_peer_id.as_str(),
            kind,
            Some(move |request: IrohControlStreamRequest| {
                if request.kind != kind {
                    return IrohControlStreamResponse {
                        ok: false,
                        error: Some(format!(
                            "unexpected iroh control request kind {}",
                            request.kind
                        )),
                        payload: Vec::new(),
                    };
                }
                let decoded = match serde_json::from_slice::<Req>(&request.payload) {
                    Ok(decoded) => decoded,
                    Err(err) => {
                        return IrohControlStreamResponse {
                            ok: false,
                            error: Some(format!("decode iroh control request: {err}")),
                            payload: Vec::new(),
                        };
                    }
                };
                let peer = match decoded.inbound_peer(&local_peer_id) {
                    Ok(peer) => peer,
                    Err(err) => {
                        return IrohControlStreamResponse {
                            ok: false,
                            error: Some(format!("resolve iroh control peer: {err}")),
                            payload: Vec::new(),
                        };
                    }
                };
                let (response_tx, response_rx) = mpsc::channel::<Resp>();
                let event = build_event(peer, decoded, response_tx);
                if pending_tx.send(event).is_err() {
                    return IrohControlStreamResponse {
                        ok: false,
                        error: Some("control dispatch channel unavailable".to_owned()),
                        payload: Vec::new(),
                    };
                }
                match response_rx.recv_timeout(timeout) {
                    Ok(response) => match serde_json::to_vec(&response) {
                        Ok(payload) => IrohControlStreamResponse {
                            ok: true,
                            error: None,
                            payload,
                        },
                        Err(err) => IrohControlStreamResponse {
                            ok: false,
                            error: Some(format!("encode iroh control response: {err}")),
                            payload: Vec::new(),
                        },
                    },
                    Err(err) => IrohControlStreamResponse {
                        ok: false,
                        error: Some(format!("wait for iroh control response: {err}")),
                        payload: Vec::new(),
                    },
                }
            }),
        )
    }

    fn subscription_for(
        &self,
        scope: &SwarmScope,
        kind: GossipKind,
    ) -> Result<IrohGossipSubscription> {
        let scope_hint = scope.label()?;
        let topic_id = derive_gossip_topic_id(
            &self.config.namespace.network_id,
            &scope_hint,
            kind.as_str(),
        );
        Ok(IrohGossipSubscription {
            scope: scope.clone(),
            kind,
            topic_id_hex: hex::encode(topic_id.as_bytes()),
        })
    }

    fn ensure_gossip_topic(&mut self, subscription: IrohGossipSubscription) -> Result<()> {
        self.subscriptions.insert(subscription.clone());
        if self.gossip_topics.contains_key(&subscription) {
            return Ok(());
        }
        let topic_id_bytes =
            hex::decode(&subscription.topic_id_hex).map_err(|err| anyhow!("{err}"))?;
        let topic_id_bytes: [u8; 32] = topic_id_bytes
            .try_into()
            .map_err(|_| anyhow!("iroh gossip topic id must be 32 bytes"))?;
        let topic_id = iroh_gossip::TopicId::from_bytes(topic_id_bytes);
        let bootstrap = self
            .remote_contacts
            .values()
            .map(contact_endpoint_id)
            .collect::<Result<Vec<_>>>()?;
        let gossip =
            local_gossip_for_network_peer_id(&self.state_dir, self.local_peer_id.as_str())?;
        let topic = self
            .block_on(gossip.subscribe(topic_id, bootstrap))
            .map_err(|err| anyhow!("subscribe iroh gossip topic: {err}"))?;
        let (sender, mut receiver) = topic.split();
        let inbound_tx = self.gossip_tx.clone();
        let task_subscription = subscription.clone();
        let task = self.runtime().spawn(async move {
            while let Some(event) = receiver.next().await {
                match event {
                    Ok(IrohGossipEvent::Received(message)) => {
                        let delivered_from = message.delivered_from.to_string();
                        if inbound_tx
                            .send(IrohGossipInbound::Message {
                                subscription: task_subscription.clone(),
                                delivered_from,
                                bytes: message.content.to_vec(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(IrohGossipEvent::Lagged) => {
                        if inbound_tx.send(IrohGossipInbound::Lagged).is_err() {
                            break;
                        }
                    }
                    Ok(IrohGossipEvent::NeighborUp(peer)) => {
                        if inbound_tx
                            .send(IrohGossipInbound::NeighborUp {
                                peer: peer.to_string(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(IrohGossipEvent::NeighborDown(peer)) => {
                        if inbound_tx
                            .send(IrohGossipInbound::NeighborDown {
                                peer: peer.to_string(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = inbound_tx.send(IrohGossipInbound::Malformed {
                            remote_network_peer_id: task_subscription.topic_id_hex.clone(),
                            error: err.to_string(),
                        });
                        break;
                    }
                }
            }
        });
        self.gossip_topics
            .insert(subscription, IrohGossipTopicHandle { sender, task });
        Ok(())
    }

    fn reserve_request_number(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1).max(1);
        request_id
    }
}

impl Drop for SubstrateRuntime {
    fn drop(&mut self) {
        for (_, topic) in self.gossip_topics.drain() {
            topic.task.abort();
        }
        shutdown_local_iroh_data_plane(&self.state_dir);
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| drop(runtime));
            }
            Ok(_) => {
                std::thread::scope(|scope| {
                    scope
                        .spawn(move || drop(runtime))
                        .join()
                        .expect("join substrate runtime drop thread");
                });
            }
            Err(_) => drop(runtime),
        }
    }
}

fn contact_endpoint_id(contact: &TransportContactMaterial) -> Result<EndpointId> {
    let metadata_endpoint = contact.metadata.endpoint_id.as_deref().map(str::trim);
    let extra_endpoint = contact
        .extra
        .get("endpoint_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim);
    let endpoint_id = extra_endpoint
        .or(metadata_endpoint)
        .unwrap_or(contact.peer_id.as_str())
        .trim();
    if endpoint_id.is_empty() {
        bail!("iroh contact material endpoint_id cannot be empty");
    }
    if let Some(metadata_endpoint) = metadata_endpoint
        && metadata_endpoint != endpoint_id
    {
        bail!(
            "iroh contact metadata endpoint_id {metadata_endpoint} does not match endpoint_id {endpoint_id}"
        );
    }
    EndpointId::from_str(endpoint_id).with_context(|| {
        format!("parse iroh endpoint id from contact material endpoint_id {endpoint_id}")
    })
}

fn parse_iroh_endpoint_id_string(raw: impl Into<String>, label: &str) -> Result<String> {
    let raw = raw.into();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{label} cannot be empty");
    }
    EndpointId::from_str(trimmed)
        .with_context(|| format!("{label} must be an iroh NodeId / EndpointId"))?;
    Ok(trimmed.to_owned())
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

fn encode_raw_control_request(request: RawControlRequest) -> Result<(String, Vec<u8>)> {
    match request {
        RawControlRequest::Backfill(request) => Ok((
            IROH_CONTROL_KIND_BACKFILL.to_owned(),
            serde_json::to_vec(&request)?,
        )),
        RawControlRequest::ContactMaterial(request) => Ok((
            IROH_CONTROL_KIND_CONTACT_MATERIAL.to_owned(),
            serde_json::to_vec(&request)?,
        )),
        RawControlRequest::PeerRelationship(request) => Ok((
            IROH_CONTROL_KIND_PEER_RELATIONSHIP.to_owned(),
            serde_json::to_vec(&request)?,
        )),
        RawControlRequest::PeerDirectMessage(request) => Ok((
            IROH_CONTROL_KIND_PEER_DIRECT_MESSAGE.to_owned(),
            serde_json::to_vec(&request)?,
        )),
    }
}

fn decode_raw_control_response(
    kind: &str,
    response: IrohControlStreamResponse,
) -> Result<RawControlResponse> {
    if !response.ok {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "remote iroh control request failed".to_owned())
        );
    }
    Ok(match kind {
        IROH_CONTROL_KIND_BACKFILL => {
            RawControlResponse::Backfill(serde_json::from_slice(&response.payload)?)
        }
        IROH_CONTROL_KIND_CONTACT_MATERIAL => {
            RawControlResponse::ContactMaterial(serde_json::from_slice(&response.payload)?)
        }
        IROH_CONTROL_KIND_PEER_RELATIONSHIP => {
            RawControlResponse::PeerRelationship(serde_json::from_slice(&response.payload)?)
        }
        IROH_CONTROL_KIND_PEER_DIRECT_MESSAGE => {
            RawControlResponse::PeerDirectMessage(serde_json::from_slice(&response.payload)?)
        }
        other => bail!("unexpected iroh control response kind {other}"),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerDiscoverySourceKind {
    Udp,
    LocalDiscovery,
    Bootstrap,
    Identify,
    BootstrapIndex,
    Unknown,
}

impl PeerDiscoverySourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::LocalDiscovery => "local_discovery",
            Self::Bootstrap => "bootstrap",
            Self::Identify => "identify",
            Self::BootstrapIndex => "bootstrap_index",
            Self::Unknown => "unknown",
        }
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
    pub p2p_foundation: String,
    pub local_iroh_endpoint_id: Option<String>,
    pub subscribed_iroh_gossip_topics: Vec<String>,
    pub known_iroh_contacts: usize,
    pub legacy_transport_active: bool,
    pub nat_status: String,
    pub nat_public_address: Option<String>,
    pub nat_confidence: u32,
    pub relay_reservations: Vec<String>,
    pub peer_health: Vec<TrafficGuardPeerHealth>,
    pub dropped_malformed_gossip: u64,
    pub invalid_control_payloads: u64,
    pub dial_failures: u64,
    pub response_validation_failures: u64,
    pub retry_suppressed_dials: u64,
}

fn validate_max_bytes(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.len() > max_bytes {
        bail!("{label} exceeds configured max bytes");
    }
    Ok(())
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
        assert_eq!(
            catalog.discovery,
            "swarmnet.main-alpha.region.sol-1-alpha.discovery"
        );
    }

    #[test]
    fn backfill_request_validate_enforces_bounds() {
        let req = RawBackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 10,
            limit: 5,
            feed_key: Some("feed".to_owned()),
            known_event_ids: Vec::new(),
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
            RawBackfillRequest {
                limit: 25,
                ..req.clone()
            }
            .validate(10, 20)
            .is_err()
        );
        assert!(
            RawBackfillRequest {
                limit: 5,
                known_event_ids: vec!["event".to_owned(); MAX_BACKFILL_KNOWN_EVENT_IDS + 1],
                ..req
            }
            .validate(10, 20)
            .is_err()
        );
    }

    #[test]
    fn backfill_response_validate_enforces_bounds() {
        let response = RawBackfillResponse {
            scope: SwarmScope::Global,
            next_from_event_seq: 10,
            feed_key: Some("feed".to_owned()),
            head_event_ids: vec!["evt-1".to_owned()],
            items: vec![b"event".to_vec()],
        };
        response.validate(10, 20).unwrap();
        assert!(
            RawBackfillResponse {
                items: vec![b"event".to_vec(); 25],
                ..response.clone()
            }
            .validate(10, 20)
            .is_err()
        );
        assert!(
            RawBackfillResponse {
                items: vec![vec![b'x'; MAX_BACKFILL_RESPONSE_BYTES + 1]],
                ..response.clone()
            }
            .validate(10, 20)
            .is_err()
        );
        assert!(
            RawBackfillResponse {
                head_event_ids: vec!["event".to_owned(); MAX_BACKFILL_KNOWN_EVENT_IDS + 1],
                ..response.clone()
            }
            .validate(10, 20)
            .is_err()
        );
    }

    #[test]
    fn raw_control_request_and_response_validate_payloads() {
        let invalid_request = RawControlRequest::PeerDirectMessage(RawPeerDirectMessageRequest {
            source_node_id: "node-a".to_owned(),
            target_node_id: "node-b".to_owned(),
            thread_id: "thread-1".to_owned(),
            message_id: "message-1".to_owned(),
            kind: RawPeerDirectMessageKind::Message,
            agent_envelope: None,
            contact_material: None,
            content_ref: None,
            control_json: None,
        });
        assert!(invalid_request.validate(10, 20).is_err());

        let invalid_response = RawControlResponse::Backfill(RawBackfillResponse {
            scope: SwarmScope::Global,
            next_from_event_seq: 1,
            feed_key: None,
            head_event_ids: Vec::new(),
            items: vec![b"event".to_vec(); 25],
        });
        assert!(invalid_response.validate(10, 20).is_err());
    }

    #[test]
    fn control_payload_validation_rejects_oversized_json() {
        let oversized = "x".repeat(MAX_CONTROL_JSON_BYTES + 1);
        let request = RawPeerDirectMessageRequest {
            source_node_id: "node-a".to_owned(),
            target_node_id: "node-b".to_owned(),
            thread_id: "thread-1".to_owned(),
            message_id: "message-1".to_owned(),
            kind: RawPeerDirectMessageKind::SessionInit,
            agent_envelope: None,
            contact_material: None,
            content_ref: None,
            control_json: Some(oversized),
        };
        assert!(request.validate().is_err());
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
    fn peer_relationship_wire_supports_agent_envelope_roundtrip() {
        let envelope = RawAgentEnvelope {
            protocol: "google_a2a".to_owned(),
            source_agent_id: Some("agent-a".to_owned()),
            target_agent_id: Some("agent-b".to_owned()),
            capability: Some("peer.relationship.request".to_owned()),
            message_json: "{\"intent\":\"friend_request\"}".to_owned(),
            extensions_json: Some("{\"reason\":\"collaboration\"}".to_owned()),
            signature: Some("sig-123".to_owned()),
        };
        let request = RawPeerRelationshipRequest {
            source_node_id: "node-a".to_owned(),
            target_node_id: "node-b".to_owned(),
            action: RawPeerRelationshipAction::Request,
            agent_envelope: Some(envelope.clone()),
        };
        let response = RawPeerRelationshipResponse {
            source_node_id: "node-b".to_owned(),
            target_node_id: "node-a".to_owned(),
            action: RawPeerRelationshipAction::Accept,
            applied: true,
            agent_envelope: Some(envelope),
            relationship_state: Some("accepted".to_owned()),
            detail: None,
            updated_at: 42,
        };

        let request_roundtrip: RawPeerRelationshipRequest =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        let response_roundtrip: RawPeerRelationshipResponse =
            serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();

        assert_eq!(
            request_roundtrip
                .agent_envelope
                .as_ref()
                .and_then(|entry| entry.source_agent_id.as_deref()),
            Some("agent-a")
        );
        assert_eq!(
            response_roundtrip
                .agent_envelope
                .as_ref()
                .and_then(|entry| entry.capability.as_deref()),
            Some("peer.relationship.request")
        );
    }
}
