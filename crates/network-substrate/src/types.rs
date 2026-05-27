use super::*;

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

pub type BackfillResponseChannel = Sender<RawBackfillResponse>;
pub type ContactMaterialResponseChannel = Sender<RawContactMaterialResponse>;
pub type PeerRelationshipResponseChannel = Sender<RawPeerRelationshipResponse>;

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
}

impl GossipKind {
    pub const ALL: [Self; 5] = [
        Self::Events,
        Self::Messages,
        Self::Rules,
        Self::Checkpoints,
        Self::Summaries,
    ];

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
}

impl TopicCatalog {
    pub fn new(namespace: &TopicNamespace, scope: &SwarmScope) -> Result<Self> {
        Ok(Self {
            events: namespace.topic_name(scope, GossipKind::Events)?,
            messages: namespace.topic_name(scope, GossipKind::Messages)?,
            rules: namespace.topic_name(scope, GossipKind::Rules)?,
            checkpoints: namespace.topic_name(scope, GossipKind::Checkpoints)?,
            summaries: namespace.topic_name(scope, GossipKind::Summaries)?,
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
    pub(super) config: SubstrateConfig,
    pub(super) local_peer_id: NetworkNodeId,
    pub(super) state_dir: PathBuf,
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
