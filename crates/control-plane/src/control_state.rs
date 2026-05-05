use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub running: bool,
    #[serde(default = "default_node_mode")]
    pub mode: NodeMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    Local,
    Lan,
    Network,
}

fn default_node_mode() -> NodeMode {
    NodeMode::Local
}

impl Default for NodeMode {
    fn default() -> Self {
        Self::Local
    }
}

impl NodeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Lan => "lan",
            Self::Network => "network",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "lan" => Ok(Self::Lan),
            "network" => Ok(Self::Network),
            other => Err(anyhow!(
                "unsupported node mode '{other}'; expected one of: local, lan, network"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutorKind {
    Local,
    Remote,
}

impl Default for ExecutorKind {
    fn default() -> Self {
        Self::Local
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorRegistryEntry {
    pub name: String,
    pub base_url: String,
    /// Optional dedicated callback base URL for structured agent events.
    /// When unset, the runtime `base_url` is used as the callback target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_event_callback_base_url: Option<String>,
    /// Executor kind: `local` (default) or `remote`.
    /// Remote executors are dispatched via the network coordination protocol
    /// instead of direct HTTP calls.
    #[serde(default)]
    pub kind: ExecutorKind,
    /// For remote executors: the target node_id that should execute this task.
    /// If empty, the task is announced to the network scope and any eligible node may pick it up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    /// For remote executors: the network scope hint for task announcement routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_hint: Option<String>,
    /// Optional same-node Wattetheria commit-plane endpoint for structured agent decisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_plane_endpoint: Option<String>,
    /// Optional local token file path used to authenticate commit-plane requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_plane_token_file: Option<String>,
}

impl ExecutorRegistryEntry {
    pub fn is_remote(&self) -> bool {
        self.kind == ExecutorKind::Remote
    }
}

pub const CORE_AGENT_EXECUTOR_NAME: &str = "core-agent";
const LEGACY_RT_EXECUTOR_NAME: &str = "rt";

pub fn normalize_executor_name(executor: &str) -> &str {
    if executor.trim() == LEGACY_RT_EXECUTOR_NAME {
        CORE_AGENT_EXECUTOR_NAME
    } else {
        executor
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutorRegistry {
    pub entries: Vec<ExecutorRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DiscoveredPeersRegistry {
    pub peers: Vec<DiscoveredPeerRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DiscoveredPeerRecord {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    #[serde(default = "default_discovered_peer_source_kind")]
    pub source_kind: String,
}

fn default_discovered_peer_source_kind() -> String {
    "unknown".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerMetadataRecord {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version_raw: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_addr: Option<String>,
    #[serde(default)]
    pub listen_addrs: Vec<String>,
    #[serde(default)]
    pub protocols: Vec<String>,
    pub handshake_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material_signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material_updated_at: Option<u64>,
    pub first_identified_at: u64,
    pub last_identified_at: u64,
}

impl PeerMetadataRecord {
    pub fn contact_material_transport_peer_id(&self) -> Option<String> {
        self.contact_material
            .as_ref()
            .and_then(|material| material.get("peer_id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    }

    pub fn transport_capabilities(&self) -> Option<PeerTransportCapabilities> {
        let material = self.contact_material.as_ref()?;
        let transports = material.get("transports")?.as_array()?;
        transports.iter().find_map(|entry| {
            serde_json::from_value::<TransportContactMaterial>(entry.clone())
                .ok()
                .map(|contact| contact.metadata.capabilities)
        })
    }

    pub fn transport_contact_materials(&self) -> Vec<TransportContactMaterial> {
        let Some(material) = self.contact_material.as_ref() else {
            return Vec::new();
        };
        let Some(transports) = material.get("transports").and_then(Value::as_array) else {
            return Vec::new();
        };
        transports
            .iter()
            .filter_map(|entry| {
                serde_json::from_value::<TransportContactMaterial>(entry.clone()).ok()
            })
            .collect()
    }

    pub fn advertised_transports(&self) -> Vec<String> {
        let Some(material) = &self.contact_material else {
            return Vec::new();
        };
        let Some(transports) = material.get("transports").and_then(Value::as_array) else {
            return Vec::new();
        };
        transports
            .iter()
            .filter_map(|entry| entry.get("transport").and_then(Value::as_str))
            .map(str::to_owned)
            .collect()
    }

    pub fn recommended_data_routes(&self) -> BTreeMap<String, String> {
        recommended_data_routes(self.transport_capabilities().as_ref())
    }

    pub fn transport_contact_material(
        &self,
        route: DataTransportRoute,
    ) -> Option<TransportContactMaterial> {
        let material = self.contact_material.as_ref()?;
        let transports = material.get("transports")?.as_array()?;
        transports.iter().find_map(|entry| {
            let contact = serde_json::from_value::<TransportContactMaterial>(entry.clone()).ok()?;
            if contact.metadata.route == route {
                Some(contact)
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceBindingKind {
    TaskDetail,
    Evidence,
    Checkpoint,
    Snapshot,
}

impl DataSourceBindingKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TaskDetail => "task_detail",
            Self::Evidence => "evidence",
            Self::Checkpoint => "checkpoint",
            Self::Snapshot => "snapshot",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "task_detail" => Ok(Self::TaskDetail),
            "evidence" => Ok(Self::Evidence),
            "checkpoint" => Ok(Self::Checkpoint),
            "snapshot" => Ok(Self::Snapshot),
            other => Err(anyhow!("unsupported data source binding kind '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataSourceBindingRecord {
    pub binding_kind: DataSourceBindingKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_scope: Option<String>,
    pub binding_key: String,
    pub source_node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkPeerSyncStateRecord {
    pub network_peer_id: String,
    #[serde(default = "default_json_array_string")]
    pub known_scopes_json: String,
    #[serde(default = "default_json_array_string")]
    pub backfill_cursors_json: String,
    #[serde(default = "default_json_array_string")]
    pub remote_heads_json: String,
    #[serde(default)]
    pub backfill_successes: u64,
    #[serde(default)]
    pub backfill_failures: u64,
    pub updated_at: u64,
}

fn default_json_array_string() -> String {
    "[]".to_owned()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerRelationshipState {
    None,
    Requested,
    Accepted,
    Rejected,
    Blocked,
}

impl PeerRelationshipState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Requested => "requested",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerRelationshipAction {
    Request,
    Accept,
    Reject,
    Cancel,
    Remove,
    Block,
    Unblock,
}

impl PeerRelationshipAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Accept => "accept",
            Self::Reject => "reject",
            Self::Cancel => "cancel",
            Self::Remove => "remove",
            Self::Block => "block",
            Self::Unblock => "unblock",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerRelationshipInitiator {
    Local,
    Remote,
    System,
}

impl PeerRelationshipInitiator {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::System => "system",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerRelationshipRecord {
    pub remote_node_id: String,
    pub relationship_state: PeerRelationshipState,
    pub last_action: PeerRelationshipAction,
    pub initiated_by: PeerRelationshipInitiator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<AgentInteractionEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responded_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleared_at: Option<u64>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AgentInteractionEnvelope {
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(default)]
    pub message: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerDmThreadKind {
    Direct,
}

impl PeerDmThreadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerDmSessionState {
    Established,
    SessionPending,
    Ready,
    Blocked,
}

impl PeerDmSessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Established => "established",
            Self::SessionPending => "session_pending",
            Self::Ready => "ready",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerDmMessageKind {
    RelationshipEstablished,
    SessionInit,
    Message,
}

impl PeerDmMessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelationshipEstablished => "relationship_established",
            Self::SessionInit => "session_init",
            Self::Message => "message",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerDmDirection {
    Inbound,
    Outbound,
}

impl PeerDmDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerDmDeliveryState {
    Pending,
    Delivered,
    Rejected,
}

impl PeerDmDeliveryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerDmThreadRecord {
    pub remote_node_id: String,
    pub thread_id: String,
    pub thread_kind: PeerDmThreadKind,
    pub session_state: PeerDmSessionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_established_at: Option<u64>,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerDmMessageRecord {
    pub thread_id: String,
    pub message_id: String,
    pub remote_node_id: String,
    pub message_kind: PeerDmMessageKind,
    pub direction: PeerDmDirection,
    pub delivery_state: PeerDmDeliveryState,
    pub a2a_protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<AgentInteractionEnvelope>,
    #[serde(default)]
    pub content: Value,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentPaymentRecord {
    pub payment_id: String,
    pub remote_node_id: String,
    pub summary_id: String,
    pub message_kind: String,
    pub payment: Value,
    pub updated_at: u64,
}

pub(crate) fn peer_dm_content_from_envelope(envelope: &AgentInteractionEnvelope) -> Value {
    envelope
        .message
        .get("content")
        .cloned()
        .unwrap_or_else(|| envelope.message.clone())
}

pub(crate) fn synthesize_peer_dm_envelope(
    a2a_protocol: &str,
    content: &Value,
) -> AgentInteractionEnvelope {
    AgentInteractionEnvelope {
        protocol: a2a_protocol.to_owned(),
        message: json!({
            "content": content.clone(),
        }),
        ..AgentInteractionEnvelope::default()
    }
}

fn parse_json_string_list(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn peer_relationship_state_from_str(value: &str) -> PeerRelationshipState {
    match value.trim().to_ascii_lowercase().as_str() {
        "requested" => PeerRelationshipState::Requested,
        "accepted" => PeerRelationshipState::Accepted,
        "rejected" => PeerRelationshipState::Rejected,
        "blocked" => PeerRelationshipState::Blocked,
        _ => PeerRelationshipState::None,
    }
}

fn peer_relationship_action_from_str(value: &str) -> PeerRelationshipAction {
    match value.trim().to_ascii_lowercase().as_str() {
        "request" => PeerRelationshipAction::Request,
        "accept" => PeerRelationshipAction::Accept,
        "reject" => PeerRelationshipAction::Reject,
        "cancel" => PeerRelationshipAction::Cancel,
        "remove" => PeerRelationshipAction::Remove,
        "block" => PeerRelationshipAction::Block,
        "unblock" => PeerRelationshipAction::Unblock,
        _ => PeerRelationshipAction::Remove,
    }
}

fn peer_relationship_initiator_from_str(value: &str) -> PeerRelationshipInitiator {
    match value.trim().to_ascii_lowercase().as_str() {
        "remote" => PeerRelationshipInitiator::Remote,
        "system" => PeerRelationshipInitiator::System,
        _ => PeerRelationshipInitiator::Local,
    }
}

fn peer_dm_thread_kind_from_str(value: &str) -> PeerDmThreadKind {
    match value.trim().to_ascii_lowercase().as_str() {
        "direct" => PeerDmThreadKind::Direct,
        _ => PeerDmThreadKind::Direct,
    }
}

fn peer_dm_session_state_from_str(value: &str) -> PeerDmSessionState {
    match value.trim().to_ascii_lowercase().as_str() {
        "established" => PeerDmSessionState::Established,
        "session_pending" => PeerDmSessionState::SessionPending,
        "ready" => PeerDmSessionState::Ready,
        "blocked" => PeerDmSessionState::Blocked,
        _ => PeerDmSessionState::Established,
    }
}

fn peer_dm_message_kind_from_str(value: &str) -> PeerDmMessageKind {
    match value.trim().to_ascii_lowercase().as_str() {
        "relationship_established" => PeerDmMessageKind::RelationshipEstablished,
        "session_init" => PeerDmMessageKind::SessionInit,
        "message" => PeerDmMessageKind::Message,
        _ => PeerDmMessageKind::Message,
    }
}

fn peer_dm_direction_from_str(value: &str) -> PeerDmDirection {
    match value.trim().to_ascii_lowercase().as_str() {
        "inbound" => PeerDmDirection::Inbound,
        _ => PeerDmDirection::Outbound,
    }
}

fn peer_dm_delivery_state_from_str(value: &str) -> PeerDmDeliveryState {
    match value.trim().to_ascii_lowercase().as_str() {
        "delivered" => PeerDmDeliveryState::Delivered,
        "rejected" => PeerDmDeliveryState::Rejected,
        _ => PeerDmDeliveryState::Pending,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryNetworkInstance {
    pub topology: crate::types::NetworkTopology,
    pub is_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryFeedSource {
    pub feed_key: String,
    pub scope_hint: String,
    pub subscriber_count: u32,
    pub latest_announcement_id: Option<String>,
    pub latest_task_id: Option<String>,
    pub latest_source_node_id: Option<String>,
    pub latest_announced_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirectorySyncEndpoint {
    pub network_id: String,
    pub node_id: String,
    pub listen_addr: String,
    pub source_kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transports: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub recommended_routes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkDirectorySnapshot {
    pub current_topology: crate::types::NetworkTopology,
    pub networks: Vec<DirectoryNetworkInstance>,
    pub active_dissemination_domains: Vec<String>,
    pub feeds: Vec<DirectoryFeedSource>,
    pub sync_endpoints: Vec<DirectorySyncEndpoint>,
}

#[derive(Debug, Clone)]
pub struct RealTaskRunRequest {
    pub executor: String,
    pub profile: String,
    pub task_id: Option<String>,
    pub task_file: Option<PathBuf>,
    pub task_contract: Option<TaskContract>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTaskBridgeRequest {
    pub executor: String,
    pub profile: String,
    pub task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RemoteTaskBridgeRegistry {
    pub entries: Vec<RemoteTaskBridgeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteTaskBridgeRecord {
    pub task_id: String,
    pub announcement_id: String,
    pub network_id: String,
    pub source_node_id: String,
    pub source_scope_hint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_ref_digest: Option<String>,
    pub executor: String,
    pub profile: String,
    pub candidate_id: String,
    pub terminal_state: String,
    pub bridged_at: u64,
}

#[derive(Clone)]
pub(crate) struct PreparedRuntime {
    pub(crate) runtime: HttpRuntimeClient,
    pub(crate) capabilities: RuntimeCapabilities,
}

pub(crate) const ENV_NETWORK_BOOTSTRAP_HTTP_URLS: &str = "WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS";
pub(crate) const DEFAULT_BOOTSTRAP_HTTP_PORT: u16 = 7788;
pub(crate) const NETWORK_BOOTSTRAP_ROUTE: &str = "/api/network/bootstrap";

#[derive(Debug, Deserialize)]
pub(crate) struct NetworkBootstrapBundleResponse {
    pub(crate) ok: bool,
    pub(crate) bundle: NetworkBootstrapBundle,
}

pub fn local_node_id(state_dir: &Path) -> Result<String> {
    Ok(load_or_create_identity(&state_dir.join("node_seed.hex"))?.node_id())
}

pub(crate) fn load_local_identity(state_dir: &Path) -> Result<NodeIdentity> {
    load_or_create_identity(&state_dir.join("node_seed.hex"))
}

pub fn local_peer_id(state_dir: &Path) -> Result<String> {
    let seed_file = state_dir.join("node_seed.hex");
    let hex_seed = fs::read_to_string(&seed_file)
        .with_context(|| format!("read node identity seed from {}", seed_file.display()))?;
    let bytes = hex::decode(hex_seed.trim())?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("seed must be 32 bytes"))?;
    let identity = NodeIdentity::from_seed(arr);
    Ok(
        crate::network_p2p::peer_id_from_ed25519_public_key(identity.verifying_key().to_bytes())?
            .to_string(),
    )
}

pub fn resolve_node_mode(state_dir: &Path) -> Result<NodeMode> {
    Ok(configured_node_mode(state_dir)?.unwrap_or(NodeMode::Local))
}

pub fn require_configured_node_mode(state_dir: &Path) -> Result<NodeMode> {
    configured_node_mode(state_dir)?.ok_or_else(|| {
        anyhow!(
            "node mode is not configured yet; save startup config or set WATTSWARM_NODE_MODE first"
        )
    })
}

pub fn configured_node_mode(state_dir: &Path) -> Result<Option<NodeMode>> {
    let state_path = node_state_path(state_dir);
    if state_path.exists() {
        let state: NodeState = serde_json::from_slice(&fs::read(&state_path)?)?;
        return Ok(Some(state.mode));
    }
    if let Ok(value) = env::var("WATTSWARM_NODE_MODE") {
        return NodeMode::parse(&value).map(Some);
    }
    configured_node_mode_from_startup_config(state_dir)
}

fn configured_node_mode_from_startup_config(state_dir: &Path) -> Result<Option<NodeMode>> {
    let path = state_dir.join("startup_config.json");
    let Ok(bytes) = fs::read(&path) else {
        return Ok(None);
    };
    #[derive(Deserialize)]
    struct StartupNetworkMode {
        #[serde(default)]
        network_mode: String,
    }
    let config: StartupNetworkMode = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse startup config at {}", path.display()))?;
    if config.network_mode.trim().is_empty() {
        return Ok(None);
    }
    match config.network_mode.trim().to_ascii_lowercase().as_str() {
        "wan" => Ok(Some(NodeMode::Network)),
        "lan" => Ok(Some(NodeMode::Lan)),
        "local" => Ok(Some(NodeMode::Local)),
        other => Err(anyhow!(
            "unsupported startup-config node mode '{other}'; expected one of: local, lan, wan"
        )),
    }
}

pub fn write_node_state(state_dir: &Path, running: bool, mode: NodeMode) -> Result<()> {
    fs::create_dir_all(state_dir)?;
    fs::write(
        node_state_path(state_dir),
        serde_json::to_vec_pretty(&NodeState { running, mode })?,
    )?;
    Ok(())
}

pub fn open_node(state_dir: &Path, db_path: &Path) -> Result<Node> {
    let mode = resolve_node_mode(state_dir)?;
    open_node_in_mode(state_dir, db_path, mode)
}

pub fn open_configured_node(state_dir: &Path, db_path: &Path) -> Result<Node> {
    let mode = require_configured_node_mode(state_dir)?;
    open_node_in_mode(state_dir, db_path, mode)
}

fn open_node_for_topology(
    state_dir: &Path,
    topology: crate::types::NetworkTopology,
    identity: NodeIdentity,
    store: PgStore,
) -> Result<Node> {
    let _ = open_local_artifact_store(state_dir)?;
    store
        .ensure_bootstrap_signed_network_protocol_params(&topology.network.network_id, &identity)?;
    let mut membership = Membership::new();
    if topology.network.network_kind == crate::types::NetworkKind::Local {
        for role in [
            Role::Proposer,
            Role::Verifier,
            Role::Committer,
            Role::Finalizer,
        ] {
            membership.grant(&identity.node_id(), role);
        }
    }

    let self_node_id = identity.node_id();
    let bound_store = store.for_org(&topology.org.org_id);
    bound_store
        .load_verified_network_protocol_params()
        .with_context(|| {
            format!(
                "load verified network params for org {}",
                topology.org.org_id
            )
        })?;
    let mut node = Node::new(identity, bound_store, membership)?;
    let replay_on_open = env::var("WATTSWARM_REPLAY_ON_OPEN").ok().is_some_and(|v| {
        let t = v.trim().to_ascii_lowercase();
        t == "1" || t == "true" || t == "yes"
    });
    if replay_on_open {
        node.replay_rebuild_projection()?;
    }
    if let Ok(peers) = load_discovered_peers_state(state_dir) {
        for peer_id in peers {
            if peer_id != self_node_id {
                node.discover_peer(peer_id);
            }
        }
    }
    Ok(node)
}

pub fn open_node_in_mode(state_dir: &Path, db_path: &Path, mode: NodeMode) -> Result<Node> {
    let identity = load_or_create_identity(&state_dir.join("node_seed.hex"))?;
    let self_node_id = identity.node_id();
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let store = PgStore::open(db_path)?;
    if mode == NodeMode::Network {
        maybe_sync_network_bootstrap_bundle(state_dir, &store, &self_node_id, now)?;
    }
    let topology = match mode {
        NodeMode::Local => {
            store.ensure_local_bootstrap_network_topology(&self_node_id, &self_node_id, now)?
        }
        NodeMode::Lan => {
            store.ensure_lan_bootstrap_network_topology(&self_node_id, &self_node_id, now)?
        }
        NodeMode::Network => store.resolve_network_bootstrap_topology_descriptor(
            &self_node_id,
            &self_node_id,
            now,
        )?,
    };
    open_node_for_topology(state_dir, topology, identity, store)
}

pub fn open_node_on_network_id(state_dir: &Path, db_path: &Path, network_id: &str) -> Result<Node> {
    let identity = load_or_create_identity(&state_dir.join("node_seed.hex"))?;
    let self_node_id = identity.node_id();
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let store = PgStore::open(db_path)?;
    let topology =
        store.join_node_to_network_topology(network_id, &self_node_id, &self_node_id, now)?;
    open_node_for_topology(state_dir, topology, identity, store)
}

pub fn load_discovered_peers(path: &Path) -> Result<Vec<String>> {
    Ok(load_discovered_peer_records(path)?
        .into_iter()
        .map(|record| record.node_id)
        .collect())
}

pub fn load_discovered_peer_records(path: &Path) -> Result<Vec<DiscoveredPeerRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read(path)?;
    if let Ok(reg) = serde_json::from_slice::<DiscoveredPeersRegistry>(&raw) {
        return Ok(reg.peers);
    }
    if let Ok(v) = serde_json::from_slice::<Vec<String>>(&raw) {
        return Ok(v
            .into_iter()
            .map(|node_id| DiscoveredPeerRecord {
                node_id,
                listen_addr: None,
                source_kind: default_discovered_peer_source_kind(),
            })
            .collect());
    }
    Err(anyhow!(
        "parse discovered peers registry from {}",
        path.display()
    ))
}

pub fn save_discovered_peers(path: &Path, peers: &[String]) -> Result<()> {
    let records = peers
        .iter()
        .cloned()
        .map(|node_id| DiscoveredPeerRecord {
            node_id,
            listen_addr: None,
            source_kind: default_discovered_peer_source_kind(),
        })
        .collect::<Vec<_>>();
    save_discovered_peer_records(path, &records)
}

pub fn save_discovered_peer_records(path: &Path, peers: &[DiscoveredPeerRecord]) -> Result<()> {
    let reg = DiscoveredPeersRegistry {
        peers: peers.to_vec(),
    };
    fs::write(path, serde_json::to_vec_pretty(&reg)?)?;
    Ok(())
}

pub fn load_discovered_peers_state(state_dir: &Path) -> Result<Vec<String>> {
    Ok(load_discovered_peer_records_state(state_dir)?
        .into_iter()
        .map(|record| record.node_id)
        .collect())
}

pub fn load_discovered_peer_records_state(state_dir: &Path) -> Result<Vec<DiscoveredPeerRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    let rows = store.list_local_discovered_peers(&scope_id)?;
    if !rows.is_empty() {
        return Ok(rows
            .into_iter()
            .map(|row| DiscoveredPeerRecord {
                node_id: row.node_id,
                listen_addr: row.listen_addr,
                source_kind: row.source_kind,
            })
            .collect());
    }
    let path = discovered_peers_path(state_dir);
    let legacy = load_discovered_peer_records(&path)?;
    if !legacy.is_empty() {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        store.replace_local_discovered_peers(
            &scope_id,
            &legacy
                .iter()
                .map(|record| crate::storage::LocalDiscoveredPeerRow {
                    node_id: record.node_id.clone(),
                    listen_addr: record.listen_addr.clone(),
                    source_kind: record.source_kind.clone(),
                    discovered_at: now,
                    updated_at: now,
                })
                .collect::<Vec<_>>(),
            now,
        )?;
    }
    Ok(legacy)
}

pub fn save_discovered_peer_records_state(
    state_dir: &Path,
    peers: &[DiscoveredPeerRecord],
) -> Result<()> {
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.replace_local_discovered_peers(
        &scope_id,
        &peers
            .iter()
            .map(|record| crate::storage::LocalDiscoveredPeerRow {
                node_id: record.node_id.clone(),
                listen_addr: record.listen_addr.clone(),
                source_kind: record.source_kind.clone(),
                discovered_at: now,
                updated_at: now,
            })
            .collect::<Vec<_>>(),
        now,
    )
}

pub fn load_peer_metadata_records_state(state_dir: &Path) -> Result<Vec<PeerMetadataRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .list_local_peer_metadata(&scope_id)?
        .into_iter()
        .map(|row| PeerMetadataRecord {
            node_id: row.node_id,
            network_id: row.network_id,
            params_version: row.params_version,
            params_hash: row.params_hash,
            agent_version_raw: row.agent_version_raw,
            agent_version_prefix: row.agent_version_prefix,
            protocol_version: row.protocol_version,
            observed_addr: row.observed_addr,
            listen_addrs: parse_json_string_list(&row.listen_addrs_json),
            protocols: parse_json_string_list(&row.protocols_json),
            handshake_status: row.handshake_status,
            last_error: row.last_error,
            contact_material: row
                .contact_material_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            contact_material_signature: row.contact_material_signature,
            contact_material_updated_at: row.contact_material_updated_at,
            first_identified_at: row.first_identified_at,
            last_identified_at: row.last_identified_at,
        })
        .collect())
}

pub fn save_peer_metadata_record_state(
    state_dir: &Path,
    record: &PeerMetadataRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_peer_metadata(
        &scope_id,
        &crate::storage::LocalPeerMetadataRow {
            node_id: record.node_id.clone(),
            network_id: record.network_id.clone(),
            params_version: record.params_version,
            params_hash: record.params_hash.clone(),
            agent_version_raw: record.agent_version_raw.clone(),
            agent_version_prefix: record.agent_version_prefix.clone(),
            protocol_version: record.protocol_version.clone(),
            observed_addr: record.observed_addr.clone(),
            listen_addrs_json: serde_json::to_string(&record.listen_addrs)?,
            protocols_json: serde_json::to_string(&record.protocols)?,
            handshake_status: record.handshake_status.clone(),
            last_error: record.last_error.clone(),
            contact_material_json: record
                .contact_material
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            contact_material_signature: record.contact_material_signature.clone(),
            contact_material_updated_at: record.contact_material_updated_at,
            first_identified_at: record.first_identified_at,
            last_identified_at: record.last_identified_at,
        },
    )
}

pub fn load_network_peer_sync_state_records_state(
    state_dir: &Path,
) -> Result<Vec<NetworkPeerSyncStateRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .list_local_network_peer_sync_states(&scope_id)?
        .into_iter()
        .map(|row| NetworkPeerSyncStateRecord {
            network_peer_id: row.network_peer_id,
            known_scopes_json: row.known_scopes_json,
            backfill_cursors_json: row.backfill_cursors_json,
            remote_heads_json: row.remote_heads_json,
            backfill_successes: row.backfill_successes,
            backfill_failures: row.backfill_failures,
            updated_at: row.updated_at,
        })
        .collect())
}

pub fn save_network_peer_sync_state_record_state(
    state_dir: &Path,
    record: &NetworkPeerSyncStateRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_network_peer_sync_state(
        &scope_id,
        &crate::storage::LocalNetworkPeerSyncStateRow {
            network_peer_id: record.network_peer_id.clone(),
            known_scopes_json: record.known_scopes_json.clone(),
            backfill_cursors_json: record.backfill_cursors_json.clone(),
            remote_heads_json: record.remote_heads_json.clone(),
            backfill_successes: record.backfill_successes,
            backfill_failures: record.backfill_failures,
            updated_at: record.updated_at,
        },
    )
}

pub fn load_data_source_binding_records_state(
    state_dir: &Path,
) -> Result<Vec<DataSourceBindingRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .list_local_data_source_bindings(&scope_id)?
        .into_iter()
        .map(|row| -> Result<DataSourceBindingRecord> {
            let binding_kind = DataSourceBindingKind::parse(&row.binding_kind)?;
            Ok(DataSourceBindingRecord {
                binding_kind,
                binding_scope: (!row.binding_scope.is_empty()).then_some(row.binding_scope),
                binding_key: row.binding_key,
                source_node_id: row.source_node_id,
                source_uri: row.source_uri,
                updated_at: row.updated_at,
            })
        })
        .collect::<Result<Vec<_>>>()?)
}

pub fn load_data_source_binding_record_state(
    state_dir: &Path,
    binding_kind: DataSourceBindingKind,
    binding_scope: Option<&str>,
    binding_key: &str,
) -> Result<Option<DataSourceBindingRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .get_local_data_source_binding(
            &scope_id,
            binding_kind.as_str(),
            binding_scope.unwrap_or(""),
            binding_key,
        )?
        .map(|row| -> Result<DataSourceBindingRecord> {
            let binding_kind = DataSourceBindingKind::parse(&row.binding_kind)?;
            Ok(DataSourceBindingRecord {
                binding_kind,
                binding_scope: (!row.binding_scope.is_empty()).then_some(row.binding_scope),
                binding_key: row.binding_key,
                source_node_id: row.source_node_id,
                source_uri: row.source_uri,
                updated_at: row.updated_at,
            })
        })
        .transpose()?)
}

pub fn save_data_source_binding_record_state(
    state_dir: &Path,
    record: &DataSourceBindingRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_data_source_binding(
        &scope_id,
        &crate::storage::LocalDataSourceBindingRow {
            binding_kind: record.binding_kind.as_str().to_owned(),
            binding_scope: record.binding_scope.clone().unwrap_or_default(),
            binding_key: record.binding_key.clone(),
            source_node_id: record.source_node_id.clone(),
            source_uri: record.source_uri.clone(),
            updated_at: record.updated_at,
        },
    )
}

pub fn load_peer_relationship_records_state(
    state_dir: &Path,
) -> Result<Vec<PeerRelationshipRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .list_local_peer_relationships(&scope_id)?
        .into_iter()
        .map(|row| PeerRelationshipRecord {
            remote_node_id: row.remote_node_id,
            relationship_state: peer_relationship_state_from_str(&row.relationship_state),
            last_action: peer_relationship_action_from_str(&row.last_action),
            initiated_by: peer_relationship_initiator_from_str(&row.initiated_by),
            agent_envelope: row
                .agent_envelope_json
                .as_deref()
                .and_then(|value| serde_json::from_str(value).ok()),
            requested_at: row.requested_at,
            responded_at: row.responded_at,
            blocked_at: row.blocked_at,
            cleared_at: row.cleared_at,
            updated_at: row.updated_at,
        })
        .collect())
}

pub fn save_peer_relationship_record_state(
    state_dir: &Path,
    record: &PeerRelationshipRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_peer_relationship(
        &scope_id,
        &crate::storage::LocalPeerRelationshipRow {
            remote_node_id: record.remote_node_id.clone(),
            relationship_state: record.relationship_state.as_str().to_owned(),
            last_action: record.last_action.as_str().to_owned(),
            initiated_by: record.initiated_by.as_str().to_owned(),
            agent_envelope_json: record
                .agent_envelope
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            requested_at: record.requested_at,
            responded_at: record.responded_at,
            blocked_at: record.blocked_at,
            cleared_at: record.cleared_at,
            updated_at: record.updated_at,
        },
    )
}

pub fn load_peer_dm_thread_records_state(state_dir: &Path) -> Result<Vec<PeerDmThreadRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .list_local_peer_dm_threads(&scope_id)?
        .into_iter()
        .map(|row| PeerDmThreadRecord {
            remote_node_id: row.remote_node_id,
            thread_id: row.thread_id,
            thread_kind: peer_dm_thread_kind_from_str(&row.thread_kind),
            session_state: peer_dm_session_state_from_str(&row.session_state),
            relationship_established_at: row.relationship_established_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_message_at: row.last_message_at,
        })
        .collect())
}

pub fn save_peer_dm_thread_record_state(
    state_dir: &Path,
    record: &PeerDmThreadRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_peer_dm_thread(
        &scope_id,
        &crate::storage::LocalPeerDmThreadRow {
            remote_node_id: record.remote_node_id.clone(),
            thread_id: record.thread_id.clone(),
            thread_kind: record.thread_kind.as_str().to_owned(),
            session_state: record.session_state.as_str().to_owned(),
            relationship_established_at: record.relationship_established_at,
            created_at: record.created_at,
            updated_at: record.updated_at,
            last_message_at: record.last_message_at,
        },
    )
}

pub fn load_peer_dm_message_records_state(
    state_dir: &Path,
    thread_id: &str,
) -> Result<Vec<PeerDmMessageRecord>> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    Ok(store
        .list_local_peer_dm_messages(&scope_id, thread_id)?
        .into_iter()
        .map(|row| {
            let agent_envelope = row
                .agent_envelope_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<AgentInteractionEnvelope>(value).ok());
            PeerDmMessageRecord {
                thread_id: row.thread_id,
                message_id: row.message_id,
                remote_node_id: row.remote_node_id,
                message_kind: peer_dm_message_kind_from_str(&row.message_kind),
                direction: peer_dm_direction_from_str(&row.direction),
                delivery_state: peer_dm_delivery_state_from_str(&row.delivery_state),
                a2a_protocol: row.a2a_protocol,
                content: agent_envelope
                    .as_ref()
                    .map(peer_dm_content_from_envelope)
                    .unwrap_or_else(|| json!({})),
                agent_envelope,
                created_at: row.created_at,
                acknowledged_at: row.acknowledged_at,
            }
        })
        .collect())
}

pub fn save_peer_dm_message_record_state(
    state_dir: &Path,
    record: &PeerDmMessageRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_peer_dm_message(
        &scope_id,
        &crate::storage::LocalPeerDmMessageRow {
            thread_id: record.thread_id.clone(),
            message_id: record.message_id.clone(),
            remote_node_id: record.remote_node_id.clone(),
            message_kind: record.message_kind.as_str().to_owned(),
            direction: record.direction.as_str().to_owned(),
            delivery_state: record.delivery_state.as_str().to_owned(),
            a2a_protocol: record.a2a_protocol.clone(),
            agent_envelope_json: record
                .agent_envelope
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            created_at: record.created_at,
            acknowledged_at: record.acknowledged_at,
        },
    )
}

pub fn save_data_plane_status_record_state(
    state_dir: &Path,
    object_kind: &str,
    object_id: &str,
    remote_node_id: Option<&str>,
    route: &str,
    status: &str,
    detail: Option<&str>,
    updated_at: u64,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_data_plane_status(
        &scope_id,
        &crate::storage::LocalDataPlaneStatusRow {
            object_kind: object_kind.to_owned(),
            object_id: object_id.to_owned(),
            remote_node_id: remote_node_id.map(ToOwned::to_owned),
            route: route.to_owned(),
            status: status.to_owned(),
            detail: detail.map(ToOwned::to_owned),
            updated_at,
        },
    )
}

pub fn load_data_plane_status_records_state(
    state_dir: &Path,
) -> Result<Vec<crate::storage::LocalDataPlaneStatusRow>> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.list_local_data_plane_statuses(&scope_id)
}

pub fn load_agent_payment_records_state(state_dir: &Path) -> Result<Vec<AgentPaymentRecord>> {
    let scope_id = local_control_scope_id(state_dir);
    Ok(local_control_store(state_dir)?
        .list_local_agent_payments(&scope_id)?
        .into_iter()
        .filter_map(|row| {
            serde_json::from_str::<Value>(&row.payment_json)
                .ok()
                .map(|payment| AgentPaymentRecord {
                    payment_id: row.payment_id,
                    remote_node_id: row.remote_node_id,
                    summary_id: row.summary_id,
                    message_kind: row.message_kind,
                    payment,
                    updated_at: row.updated_at,
                })
        })
        .collect())
}

pub fn save_agent_payment_record_state(
    state_dir: &Path,
    record: &AgentPaymentRecord,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_agent_payment(
        &scope_id,
        &crate::storage::LocalAgentPaymentRow {
            payment_id: record.payment_id.clone(),
            remote_node_id: record.remote_node_id.clone(),
            summary_id: record.summary_id.clone(),
            message_kind: record.message_kind.clone(),
            payment_json: serde_json::to_string(&record.payment)?,
            updated_at: record.updated_at,
        },
    )
}

pub fn load_agent_event_records_state(
    state_dir: &Path,
) -> Result<Vec<wattswarm_protocol::types::AgentEvent>> {
    let scope_id = local_control_scope_id(state_dir);
    Ok(local_control_store(state_dir)?
        .list_local_agent_events(&scope_id)?
        .into_iter()
        .filter_map(|row| {
            let payload = serde_json::from_str::<Value>(&row.payload_json).ok()?;
            let allowed_actions =
                serde_json::from_str::<Vec<String>>(&row.allowed_actions_json).ok()?;
            Some(wattswarm_protocol::types::AgentEvent {
                event_id: row.event_id,
                event_type: serde_json::from_value(Value::String(row.event_type)).ok()?,
                source_kind: serde_json::from_value(Value::String(row.source_kind)).ok()?,
                source_node_id: row.source_node_id,
                target_agent_id: row.target_agent_id,
                target_executor: row.target_executor,
                payload,
                requires_commit: row.requires_commit,
                allowed_actions,
                correlation_id: row.correlation_id,
                dedupe_key: row.dedupe_key,
                created_at: row.created_at,
            })
        })
        .collect())
}

pub fn save_agent_event_record_state(
    state_dir: &Path,
    event: &wattswarm_protocol::types::AgentEvent,
    status: wattswarm_protocol::types::AgentEventStatus,
    updated_at: u64,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_agent_event(
        &scope_id,
        &crate::storage::LocalAgentEventRow {
            event_id: event.event_id.clone(),
            event_type: serde_json::to_value(&event.event_type)?
                .as_str()
                .unwrap_or("friend_request")
                .to_owned(),
            source_kind: serde_json::to_value(&event.source_kind)?
                .as_str()
                .unwrap_or("peer_relationship")
                .to_owned(),
            source_node_id: event.source_node_id.clone(),
            target_agent_id: event.target_agent_id.clone(),
            target_executor: event.target_executor.clone(),
            payload_json: serde_json::to_string(&event.payload)?,
            allowed_actions_json: serde_json::to_string(&event.allowed_actions)?,
            requires_commit: event.requires_commit,
            status: serde_json::to_value(&status)?
                .as_str()
                .unwrap_or("pending")
                .to_owned(),
            dedupe_key: event.dedupe_key.clone(),
            correlation_id: event.correlation_id.clone(),
            created_at: event.created_at,
            updated_at,
        },
    )
}

pub fn find_agent_event_record_by_dedupe_key(
    state_dir: &Path,
    dedupe_key: &str,
) -> Result<Option<wattswarm_protocol::types::AgentEvent>> {
    let scope_id = local_control_scope_id(state_dir);
    let Some(row) = local_control_store(state_dir)?
        .find_local_agent_event_by_dedupe_key(&scope_id, dedupe_key)?
    else {
        return Ok(None);
    };
    Ok(Some(wattswarm_protocol::types::AgentEvent {
        event_id: row.event_id,
        event_type: serde_json::from_value(Value::String(row.event_type))?,
        source_kind: serde_json::from_value(Value::String(row.source_kind))?,
        source_node_id: row.source_node_id,
        target_agent_id: row.target_agent_id,
        target_executor: row.target_executor,
        payload: serde_json::from_str(&row.payload_json)?,
        allowed_actions: serde_json::from_str(&row.allowed_actions_json)?,
        requires_commit: row.requires_commit,
        correlation_id: row.correlation_id,
        dedupe_key: row.dedupe_key,
        created_at: row.created_at,
    }))
}

pub fn append_agent_event_delivery_record_state(
    state_dir: &Path,
    row: &crate::storage::LocalAgentEventDeliveryRow,
) -> Result<()> {
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.append_local_agent_event_delivery(&scope_id, row)
}

pub fn apply_peer_relationship_action_state(
    state_dir: &Path,
    remote_node_id: &str,
    action: PeerRelationshipAction,
    initiated_by: PeerRelationshipInitiator,
) -> Result<PeerRelationshipRecord> {
    let remote_node_id = remote_node_id.trim();
    if remote_node_id.is_empty() {
        return Err(anyhow!("remote_node_id is required"));
    }
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let existing = load_peer_relationship_records_state(state_dir)?
        .into_iter()
        .find(|record| record.remote_node_id == remote_node_id);
    let mut record = existing.unwrap_or(PeerRelationshipRecord {
        remote_node_id: remote_node_id.to_owned(),
        relationship_state: PeerRelationshipState::None,
        last_action: PeerRelationshipAction::Remove,
        initiated_by,
        agent_envelope: None,
        requested_at: None,
        responded_at: None,
        blocked_at: None,
        cleared_at: None,
        updated_at: now,
    });
    record.initiated_by = initiated_by;
    record.last_action = action;
    record.updated_at = now;
    match action {
        PeerRelationshipAction::Request => {
            if record.relationship_state == PeerRelationshipState::Blocked {
                return Err(anyhow!(
                    "cannot request relationship while remote_node_id={} is blocked",
                    remote_node_id
                ));
            }
            record.relationship_state = PeerRelationshipState::Requested;
            record.requested_at = Some(now);
            record.responded_at = None;
            record.blocked_at = None;
            record.cleared_at = None;
        }
        PeerRelationshipAction::Accept => {
            if record.relationship_state != PeerRelationshipState::Requested {
                return Err(anyhow!(
                    "cannot accept relationship for remote_node_id={} without a pending request",
                    remote_node_id
                ));
            }
            record.relationship_state = PeerRelationshipState::Accepted;
            record.responded_at = Some(now);
            record.blocked_at = None;
            record.cleared_at = None;
        }
        PeerRelationshipAction::Reject => {
            if record.relationship_state != PeerRelationshipState::Requested {
                return Err(anyhow!(
                    "cannot reject relationship for remote_node_id={} without a pending request",
                    remote_node_id
                ));
            }
            record.relationship_state = PeerRelationshipState::Rejected;
            record.responded_at = Some(now);
            record.blocked_at = None;
            record.cleared_at = None;
        }
        PeerRelationshipAction::Cancel | PeerRelationshipAction::Remove => {
            let can_clear = matches!(
                (action, record.relationship_state),
                (
                    PeerRelationshipAction::Cancel,
                    PeerRelationshipState::Requested
                ) | (
                    PeerRelationshipAction::Remove,
                    PeerRelationshipState::Accepted | PeerRelationshipState::Rejected
                )
            );
            if !can_clear {
                return Err(anyhow!(
                    "cannot {} relationship for remote_node_id={} from state={}",
                    action.as_str(),
                    remote_node_id,
                    record.relationship_state.as_str()
                ));
            }
            record.relationship_state = PeerRelationshipState::None;
            record.cleared_at = Some(now);
            record.blocked_at = None;
        }
        PeerRelationshipAction::Block => {
            if record.relationship_state == PeerRelationshipState::Blocked {
                return Err(anyhow!(
                    "remote_node_id={} is already blocked",
                    remote_node_id
                ));
            }
            record.relationship_state = PeerRelationshipState::Blocked;
            record.blocked_at = Some(now);
            record.cleared_at = None;
        }
        PeerRelationshipAction::Unblock => {
            if record.relationship_state != PeerRelationshipState::Blocked {
                return Err(anyhow!(
                    "cannot unblock remote_node_id={} because it is not blocked",
                    remote_node_id
                ));
            }
            record.relationship_state = PeerRelationshipState::None;
            record.blocked_at = None;
            record.cleared_at = Some(now);
        }
    }
    save_peer_relationship_record_state(state_dir, &record)?;
    Ok(record)
}

pub fn add_discovered_peer(state_dir: &Path, peer_node_id: &str) -> Result<bool> {
    add_discovered_peer_endpoint_with_source(state_dir, peer_node_id, None, "unknown")
}

pub fn add_discovered_peer_endpoint(
    state_dir: &Path,
    peer_node_id: &str,
    listen_addr: Option<&str>,
) -> Result<bool> {
    add_discovered_peer_endpoint_with_source(state_dir, peer_node_id, listen_addr, "udp")
}

pub fn add_discovered_peer_endpoint_with_source(
    state_dir: &Path,
    peer_node_id: &str,
    listen_addr: Option<&str>,
    source_kind: &str,
) -> Result<bool> {
    let peer = peer_node_id.trim();
    if peer.is_empty() {
        return Ok(false);
    }
    fs::create_dir_all(state_dir)?;
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.upsert_local_discovered_peer(
        &scope_id,
        peer,
        listen_addr,
        source_kind,
        now,
    )
}

pub fn load_executor_registry(path: &Path) -> Result<ExecutorRegistry> {
    if !path.exists() {
        return Ok(ExecutorRegistry::default());
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn save_executor_registry(path: &Path, reg: &ExecutorRegistry) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(reg)?)?;
    Ok(())
}

pub fn load_executor_registry_state(state_dir: &Path) -> Result<ExecutorRegistry> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    let entries = store.list_local_executors(&scope_id)?;
    if !entries.is_empty() {
        return Ok(ExecutorRegistry {
            entries: entries
                .into_iter()
                .map(|entry| ExecutorRegistryEntry {
                    name: entry.name,
                    base_url: entry.base_url,
                    agent_event_callback_base_url: entry.agent_event_callback_base_url,
                    kind: if entry.kind.eq_ignore_ascii_case("remote") {
                        ExecutorKind::Remote
                    } else {
                        ExecutorKind::Local
                    },
                    target_node_id: entry.target_node_id,
                    scope_hint: entry.scope_hint,
                    commit_plane_endpoint: entry.commit_plane_endpoint,
                    commit_plane_token_file: entry.commit_plane_token_file,
                })
                .collect(),
        });
    }
    let path = executor_registry_path(state_dir);
    let legacy = load_executor_registry(&path)?;
    if !legacy.entries.is_empty() {
        save_executor_registry_state(state_dir, &legacy)?;
    }
    Ok(legacy)
}

pub fn save_executor_registry_state(state_dir: &Path, reg: &ExecutorRegistry) -> Result<()> {
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let scope_id = local_control_scope_id(state_dir);
    local_control_store(state_dir)?.replace_local_executors(
        &scope_id,
        &reg.entries
            .iter()
            .map(|entry| crate::storage::LocalExecutorEntryRow {
                name: entry.name.clone(),
                base_url: entry.base_url.clone(),
                agent_event_callback_base_url: entry.agent_event_callback_base_url.clone(),
                kind: match entry.kind {
                    ExecutorKind::Local => "local".to_owned(),
                    ExecutorKind::Remote => "remote".to_owned(),
                },
                target_node_id: entry.target_node_id.clone(),
                scope_hint: entry.scope_hint.clone(),
                commit_plane_endpoint: entry.commit_plane_endpoint.clone(),
                commit_plane_token_file: entry.commit_plane_token_file.clone(),
                updated_at: now,
            })
            .collect::<Vec<_>>(),
        now,
    )
}
