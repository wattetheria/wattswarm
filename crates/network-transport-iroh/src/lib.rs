use anyhow::{Context, Result, anyhow, bail};
use hex::decode;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey, TransportAddr,
    address_lookup::memory::MemoryLookup,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{
    BlobFormat, BlobsProtocol, Hash as IrohBlobHash, HashAndFormat, store::fs::FsStore,
};
use iroh_gossip::TopicId as IrohGossipTopicId;
use iroh_gossip::{
    Gossip,
    proto::{HyparviewConfig, PlumtreeConfig},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime, RuntimeFlavor};
use wattswarm_artifact_store::{ArtifactKind, ArtifactStore};
use wattswarm_network_transport_core::{
    DirectDataFetchRequest, DirectDataFetchResponse, DirectDataTransportAdapter,
    PeerTransportCapabilities, TransportContactMaterial, TransportError, TransportMetadata,
    TransportRoute,
};

pub const DEFAULT_IROH_ALPN: &str = "/wattswarm/iroh/1";
pub const DEFAULT_IROH_CONTROL_ALPN: &str = "/wattswarm/iroh-control/1";
pub const IROH_CONTROL_KIND_CONTACT_MATERIAL: &str = "contact_material.v1";
pub const WATTSWARM_IROH_GOSSIP_TOPIC_PREFIX: &str = "wattswarm:iroh-gossip-topic:v1";
pub const ENV_IROH_RELAY_URLS: &str = "WATTSWARM_IROH_RELAY_URLS";
pub const ENV_IROH_BIND_ADDR: &str = "WATTSWARM_IROH_BIND_ADDR";
pub const ENV_IROH_PUBLISH_DIRECT_ADDRS: &str = "WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS";
pub const ENV_IROH_DATA_PLANE_START_TIMEOUT_MS: &str = "WATTSWARM_IROH_DATA_PLANE_START_TIMEOUT_MS";
const MAX_FETCH_REQUEST_BYTES: usize = 64 * 1024;
const MAX_FETCH_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONTROL_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_CONTROL_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_GOSSIP_MESSAGE_BYTES: usize = 512 * 1024;
const DEFAULT_GOSSIP_ACTIVE_VIEW_CAPACITY: usize = 6;
const DEFAULT_GOSSIP_PASSIVE_VIEW_CAPACITY: usize = 12;
const DEFAULT_GOSSIP_SHUFFLE_ACTIVE_VIEW_COUNT: usize = 4;
const DEFAULT_GOSSIP_MAINTENANCE_INTERVAL_MS: u64 = 1_000;
const DEFAULT_IROH_DATA_PLANE_START_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_IROH_CONTROL_STREAM_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrohGossipRuntimeConfig {
    pub max_message_size: usize,
    pub active_view_capacity: usize,
    pub passive_view_capacity: usize,
    pub shuffle_active_view_count: usize,
    pub maintenance_interval: Duration,
}

impl Default for IrohGossipRuntimeConfig {
    fn default() -> Self {
        Self {
            max_message_size: MAX_GOSSIP_MESSAGE_BYTES,
            active_view_capacity: DEFAULT_GOSSIP_ACTIVE_VIEW_CAPACITY,
            passive_view_capacity: DEFAULT_GOSSIP_PASSIVE_VIEW_CAPACITY,
            shuffle_active_view_count: DEFAULT_GOSSIP_SHUFFLE_ACTIVE_VIEW_COUNT,
            maintenance_interval: Duration::from_millis(DEFAULT_GOSSIP_MAINTENANCE_INTERVAL_MS),
        }
    }
}

impl IrohGossipRuntimeConfig {
    fn membership_config(&self) -> HyparviewConfig {
        let mut config = HyparviewConfig::default();
        config.active_view_capacity = self.active_view_capacity;
        config.passive_view_capacity = self.passive_view_capacity;
        config.shuffle_active_view_count = self.shuffle_active_view_count;
        config.shuffle_interval = self.maintenance_interval;
        config
    }

    fn broadcast_config(&self) -> PlumtreeConfig {
        let mut config = PlumtreeConfig::default();
        config.cache_evict_interval = self.maintenance_interval;
        config
    }
}

pub fn derive_gossip_topic_id(
    network_id: &str,
    scope_hint: &str,
    gossip_kind: &str,
) -> IrohGossipTopicId {
    let material = format!(
        "{WATTSWARM_IROH_GOSSIP_TOPIC_PREFIX}\0{}\0{}\0{}",
        network_id.trim(),
        scope_hint.trim(),
        gossip_kind.trim()
    );
    IrohGossipTopicId::from_bytes(*IrohBlobHash::new(material).as_bytes())
}

pub fn blob_hash_for_bytes(bytes: impl AsRef<[u8]>) -> IrohBlobHash {
    IrohBlobHash::new(bytes)
}

fn parse_relay_urls(raw: &str) -> Result<Vec<RelayUrl>> {
    let mut urls = Vec::new();
    for value in raw
        .split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let url =
            RelayUrl::from_str(value).with_context(|| format!("parse iroh relay url {value}"))?;
        if !urls.contains(&url) {
            urls.push(url);
        }
    }
    Ok(urls)
}

fn parse_optional_socket_addr_env(key: &str, raw: Option<&str>) -> Result<Option<SocketAddr>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<SocketAddr>()
        .map(Some)
        .with_context(|| format!("{key} must be a socket address, got {value}"))
}

fn parse_direct_addr_publish_env(raw: Option<&str>) -> Result<Vec<String>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off" | "1" | "true" | "yes" | "on"
        )
    {
        return Ok(Vec::new());
    }
    let mut addrs = Vec::new();
    for value in trimmed
        .split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let addr = value
            .parse::<SocketAddr>()
            .with_context(|| {
                format!(
                    "{ENV_IROH_PUBLISH_DIRECT_ADDRS} must be false or a list of socket addresses, got {value}"
                )
            })?;
        let normalized = addr.to_string();
        if !addrs.contains(&normalized) {
            addrs.push(normalized);
        }
    }
    Ok(addrs)
}

fn parse_positive_u64_env(key: &str, raw: Option<&str>, default: u64) -> Result<u64> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Ok(default);
    }
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{key} must be an integer value, got {value}"))?;
    if parsed == 0 {
        bail!("{key} must be greater than zero");
    }
    Ok(parsed)
}

fn iroh_data_plane_start_timeout() -> Result<Duration> {
    parse_positive_u64_env(
        ENV_IROH_DATA_PLANE_START_TIMEOUT_MS,
        std::env::var(ENV_IROH_DATA_PLANE_START_TIMEOUT_MS)
            .ok()
            .as_deref(),
        DEFAULT_IROH_DATA_PLANE_START_TIMEOUT_MS,
    )
    .map(Duration::from_millis)
}

fn normalize_public_relay_url(url: &RelayUrl) -> String {
    url.to_string().trim_end_matches('/').to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohBlobReference {
    pub hash: String,
    pub format: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohControlStreamRequest {
    pub kind: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohControlStreamResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub payload: Vec<u8>,
}

type ControlStreamHandler =
    Arc<dyn Fn(String, IrohControlStreamRequest) -> IrohControlStreamResponse + Send + Sync>;
type ControlStreamHandlers = Arc<Mutex<HashMap<String, ControlStreamHandler>>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohTransportConfig {
    pub alpn: String,
}

impl Default for IrohTransportConfig {
    fn default() -> Self {
        Self {
            alpn: DEFAULT_IROH_ALPN.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohTransportMaterial {
    pub endpoint_id: String,
    pub alpn: String,
    #[serde(default)]
    pub direct_addrs: Vec<String>,
    #[serde(default)]
    pub relay_urls: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IrohTransportAdapter {
    network_peer_id: String,
    endpoint_id: EndpointId,
    config: IrohTransportConfig,
    capabilities: PeerTransportCapabilities,
}

impl IrohTransportAdapter {
    pub fn from_seed_bytes(secret_key_32: [u8; 32]) -> Result<Self> {
        let secret_key = SecretKey::from_bytes(&secret_key_32);
        Ok(Self::from_secret_key(&secret_key))
    }

    pub fn from_secret_key(secret_key: &SecretKey) -> Self {
        let endpoint_id = endpoint_id_from_secret_key(secret_key);
        Self::from_endpoint_id(endpoint_id, endpoint_id.to_string())
    }

    pub fn from_endpoint_id(endpoint_id: EndpointId, network_peer_id: impl Into<String>) -> Self {
        Self {
            network_peer_id: network_peer_id.into(),
            endpoint_id,
            config: IrohTransportConfig::default(),
            capabilities: PeerTransportCapabilities::iroh_direct_default(),
        }
    }

    pub fn network_peer_id(&self) -> &str {
        &self.network_peer_id
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint_id
    }

    pub fn material_payload(
        &self,
        direct_addrs: &[String],
        relay_urls: &[String],
        generated_at: u64,
    ) -> TransportContactMaterial {
        TransportContactMaterial {
            transport: TransportRoute::IrohDirect.as_str().to_owned(),
            peer_id: self.network_peer_id.clone(),
            metadata: TransportMetadata {
                route: TransportRoute::IrohDirect,
                generated_at,
                endpoint_id: Some(self.endpoint_id.to_string()),
                alpn: Some(self.config.alpn.clone()),
                listen_addrs: direct_addrs.to_vec(),
                capabilities: self.capabilities.clone(),
            },
            extra: json!(IrohTransportMaterial {
                endpoint_id: self.endpoint_id.to_string(),
                alpn: self.config.alpn.clone(),
                direct_addrs: direct_addrs.to_vec(),
                relay_urls: relay_urls.to_vec(),
            }),
        }
    }
}

impl DirectDataTransportAdapter for IrohTransportAdapter {
    fn route(&self) -> TransportRoute {
        TransportRoute::IrohDirect
    }

    fn capabilities(&self) -> &PeerTransportCapabilities {
        &self.capabilities
    }

    fn export_contact_material(
        &self,
        listen_addrs: &[String],
        generated_at: u64,
    ) -> Result<TransportContactMaterial, TransportError> {
        Ok(self.material_payload(listen_addrs, &[], generated_at))
    }
}

struct IrohDataPlaneService {
    runtime: Runtime,
    endpoint: Endpoint,
    router: Router,
    gossip: Gossip,
    gossip_config: IrohGossipRuntimeConfig,
    blob_store: FsStore,
    adapter: IrohTransportAdapter,
    endpoint_options: IrohEndpointOptions,
    control_handlers: ControlStreamHandlers,
    op_lock: Mutex<()>,
    // Sentinel flock held for the lifetime of the service. Drop releases the
    // OS-level lock so a subsequent process can acquire it. Held to prevent two
    // wattswarm processes from sharing one state_dir (which would otherwise
    // deadlock on the iroh-blobs redb file lock with a misleading 120s timeout).
    _data_plane_lock: fs::File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IrohEndpointOptions {
    relay_urls: Vec<RelayUrl>,
    published_relay_urls: Vec<String>,
    bind_addr: Option<SocketAddr>,
    published_direct_addrs: Vec<String>,
}

/// Relay URLs persisted by the control plane in `startup_config.json`,
/// refreshed from the network join manifest on node startup. A non-empty
/// persisted list takes priority over `WATTSWARM_IROH_RELAY_URLS`; the env
/// variable remains the fallback seed for nodes that have not synced a
/// join manifest yet (for example the genesis node itself).
fn startup_config_relay_urls(state_dir: &Path) -> Option<String> {
    #[derive(Deserialize)]
    struct StartupConfigRelayUrls {
        #[serde(default)]
        relay_urls: Vec<String>,
    }
    let bytes = fs::read(state_dir.join("startup_config.json")).ok()?;
    let config: StartupConfigRelayUrls = serde_json::from_slice(&bytes).ok()?;
    let urls: Vec<String> = config
        .relay_urls
        .iter()
        .map(|url| url.trim().to_owned())
        .filter(|url| !url.is_empty())
        .collect();
    if urls.is_empty() {
        None
    } else {
        Some(urls.join(","))
    }
}

impl IrohEndpointOptions {
    fn resolve(state_dir: &Path) -> Result<Self> {
        let relay_urls_raw = startup_config_relay_urls(state_dir)
            .or_else(|| std::env::var(ENV_IROH_RELAY_URLS).ok());
        Self::from_raw_env(
            relay_urls_raw.as_deref(),
            std::env::var(ENV_IROH_BIND_ADDR).ok().as_deref(),
            std::env::var(ENV_IROH_PUBLISH_DIRECT_ADDRS).ok().as_deref(),
        )
    }

    fn from_raw_env(
        relay_urls: Option<&str>,
        bind_addr: Option<&str>,
        publish_direct_addrs: Option<&str>,
    ) -> Result<Self> {
        let relay_urls = parse_relay_urls(relay_urls.unwrap_or_default())?;
        Ok(Self {
            published_relay_urls: relay_urls.iter().map(normalize_public_relay_url).collect(),
            relay_urls,
            bind_addr: parse_optional_socket_addr_env(ENV_IROH_BIND_ADDR, bind_addr)?,
            published_direct_addrs: parse_direct_addr_publish_env(publish_direct_addrs)?,
        })
    }

    fn published_direct_addrs(&self, _observed_direct_addrs: Vec<String>) -> Vec<String> {
        self.published_direct_addrs.clone()
    }

    fn published_relay_urls(&self, observed_relay_urls: Vec<String>) -> Vec<String> {
        let mut relay_urls = self.published_relay_urls.clone();
        for url in observed_relay_urls {
            let normalized = url.trim().trim_end_matches('/').to_owned();
            if !normalized.is_empty() && !relay_urls.contains(&normalized) {
                relay_urls.push(normalized);
            }
        }
        relay_urls
    }
}

#[derive(Debug, Clone)]
struct DirectFetchProtocol {
    state_dir: PathBuf,
}

impl ProtocolHandler for DirectFetchProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        serve_incoming_fetch_request(self.state_dir.clone(), connection)
            .await
            .map_err(|err| AcceptError::from_boxed(err.into()))
    }
}

#[derive(Clone)]
struct ControlStreamProtocol {
    handlers: ControlStreamHandlers,
}

impl std::fmt::Debug for ControlStreamProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ControlStreamProtocol").finish()
    }
}

impl ProtocolHandler for ControlStreamProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        serve_incoming_control_stream_request(self.handlers.clone(), connection)
            .await
            .map_err(|err| AcceptError::from_boxed(err.into()))
    }
}

static LOCAL_IROH_DATA_PLANES: OnceLock<Mutex<HashMap<PathBuf, Arc<IrohDataPlaneService>>>> =
    OnceLock::new();

fn local_iroh_data_planes() -> &'static Mutex<HashMap<PathBuf, Arc<IrohDataPlaneService>>> {
    LOCAL_IROH_DATA_PLANES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn artifact_store_path(state_dir: &Path) -> PathBuf {
    state_dir.join("artifacts")
}

fn iroh_blob_store_path(state_dir: &Path) -> PathBuf {
    state_dir.join("iroh-blobs")
}

fn iroh_data_plane_lock_path(state_dir: &Path) -> PathBuf {
    iroh_blob_store_path(state_dir).join(".wattswarm-data-plane.lock")
}

/// Acquire an exclusive advisory flock on the data-plane sentinel file.
///
/// The returned [`fs::File`] must be kept alive for the lifetime of the iroh
/// data plane; dropping it (or process exit) releases the lock.
///
/// Fails fast with a descriptive error when another process already holds the
/// lock, instead of letting `iroh-blobs` deadlock on its internal redb file
/// lock and surface as a misleading 120s "initialize iroh router timed out".
fn acquire_data_plane_sentinel_lock(state_dir: &Path) -> Result<fs::File> {
    let blob_dir = iroh_blob_store_path(state_dir);
    fs::create_dir_all(&blob_dir)
        .with_context(|| format!("create iroh data plane directory {}", blob_dir.display()))?;
    let lock_path = iroh_data_plane_lock_path(state_dir);
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("open iroh data plane lock file {}", lock_path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => bail!(
            "iroh data plane already locked at {} — another wattswarm process is using state_dir {}. \
             On Linux, run `cat /proc/locks | grep $(stat -c %i {})` to find the holding PID. \
             Common cause: a sibling container (e.g. wattswarm-worker) mounts the same state volume; \
             give it its own state_dir or disable its P2P stack with WATTSWARM_P2P_ENABLED=false.",
            lock_path.display(),
            state_dir.display(),
            lock_path.display()
        ),
        Err(std::fs::TryLockError::Error(err)) => Err(err).with_context(|| {
            format!(
                "acquire exclusive flock on iroh data plane lock file {}",
                lock_path.display()
            )
        }),
    }
}

fn block_on_iroh_data_plane_start<T: Send>(
    runtime: &Runtime,
    operation: &'static str,
    future: impl Future<Output = Result<T>> + Send,
) -> Result<T> {
    let timeout = iroh_data_plane_start_timeout()?;
    let result = std::thread::scope(|scope| {
        scope
            .spawn(move || {
                runtime.block_on(async move { tokio::time::timeout(timeout, future).await })
            })
            .join()
            .expect("join iroh data plane startup operation")
    });
    result
        .with_context(|| format!("{operation} timed out after {}ms", timeout.as_millis()))?
        .with_context(|| format!("{operation} failed"))
}

fn open_artifact_store(state_dir: &Path) -> Result<ArtifactStore> {
    let store = ArtifactStore::new(artifact_store_path(state_dir));
    store.ensure_layout()?;
    Ok(store)
}

fn load_secret_key_from_state_dir(state_dir: &Path) -> Result<SecretKey> {
    let seed_file = state_dir.join("node_seed.hex");
    let hex_seed = fs::read_to_string(&seed_file)
        .with_context(|| format!("read node identity seed from {}", seed_file.display()))?;
    let bytes = decode(hex_seed.trim())?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("seed must be 32 bytes"))?;
    Ok(SecretKey::from_bytes(&arr))
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

async fn serve_incoming_fetch_request(state_dir: PathBuf, connection: Connection) -> Result<()> {
    async {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let request_bytes = recv.read_to_end(MAX_FETCH_REQUEST_BYTES).await?;
        let request: DirectDataFetchRequest = serde_json::from_slice(&request_bytes)?;
        let response = serve_fetch_request(&state_dir, &request)
            .with_context(|| format!("serve iroh fetch request {:?}", request.object_kind));
        let payload = match response {
            Ok(payload) => DirectDataFetchResponse {
                ok: true,
                content_type: payload.content_type,
                error: None,
                bytes: payload.bytes,
            },
            Err(err) => DirectDataFetchResponse {
                ok: false,
                content_type: None,
                error: Some(err.to_string()),
                bytes: Vec::new(),
            },
        };
        let bytes = serde_json::to_vec(&payload)?;
        send.write_all(&bytes).await?;
        send.finish()?;
        connection.closed().await;
        Result::<(), anyhow::Error>::Ok(())
    }
    .await
    .with_context(|| format!("iroh data fetch handler failed for {}", state_dir.display()))
}

async fn serve_incoming_control_stream_request(
    handlers: ControlStreamHandlers,
    connection: Connection,
) -> Result<()> {
    async {
        let remote_peer_id = connection.remote_id().to_string();
        let (mut send, mut recv) = connection.accept_bi().await?;
        let request_bytes = recv.read_to_end(MAX_CONTROL_REQUEST_BYTES).await?;
        let request: IrohControlStreamRequest = serde_json::from_slice(&request_bytes)?;
        let request_kind = request.kind.clone();
        let response = match handlers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&request_kind)
            .cloned()
        {
            Some(handler) => handler(remote_peer_id, request),
            None => IrohControlStreamResponse {
                ok: false,
                error: Some(format!(
                    "no iroh control stream handler registered for {request_kind}"
                )),
                payload: Vec::new(),
            },
        };
        let response_bytes = serde_json::to_vec(&response)?;
        if response_bytes.len() > MAX_CONTROL_RESPONSE_BYTES {
            bail!("iroh control stream response exceeds max response bytes");
        }
        send.write_all(&response_bytes).await?;
        send.finish()?;
        connection.closed().await;
        Result::<(), anyhow::Error>::Ok(())
    }
    .await
    .context("serve iroh control stream request")
}

#[derive(Debug)]
struct ServedFetchPayload {
    content_type: Option<String>,
    bytes: Vec<u8>,
}

fn serve_fetch_request(
    state_dir: &Path,
    request: &DirectDataFetchRequest,
) -> Result<ServedFetchPayload> {
    let artifact_store = open_artifact_store(state_dir)?;
    match request.object_kind {
        wattswarm_network_transport_core::DirectDataObjectKind::ReferenceArtifact => {
            let bytes = artifact_store.read_validated_bytes(
                ArtifactKind::Reference,
                &request.object_id,
                None,
                request.expected_digest.as_deref(),
                request.expected_size,
            )?;
            Ok(ServedFetchPayload {
                content_type: Some("application/octet-stream".to_owned()),
                bytes,
            })
        }
        wattswarm_network_transport_core::DirectDataObjectKind::EvidenceArtifact => {
            let bytes = artifact_store.read_validated_bytes(
                ArtifactKind::Evidence,
                &request.object_id,
                None,
                request.expected_digest.as_deref(),
                request.expected_size,
            )?;
            Ok(ServedFetchPayload {
                content_type: Some("application/octet-stream".to_owned()),
                bytes,
            })
        }
        wattswarm_network_transport_core::DirectDataObjectKind::TopicMessageJson => {
            let bytes = artifact_store.read_validated_bytes(
                ArtifactKind::TopicMessage,
                &request.object_id,
                None,
                request.expected_digest.as_deref(),
                request.expected_size,
            )?;
            Ok(ServedFetchPayload {
                content_type: Some("application/json".to_owned()),
                bytes,
            })
        }
        wattswarm_network_transport_core::DirectDataObjectKind::DirectMessageJson => {
            let bytes = artifact_store.read_validated_bytes(
                ArtifactKind::DirectMessage,
                &request.object_id,
                None,
                request.expected_digest.as_deref(),
                request.expected_size,
            )?;
            Ok(ServedFetchPayload {
                content_type: Some("application/json".to_owned()),
                bytes,
            })
        }
        wattswarm_network_transport_core::DirectDataObjectKind::CheckpointJson => {
            let path = artifact_store.checkpoint_path(&request.object_id)?;
            let bytes = artifact_store.read_bytes(&path)?;
            Ok(ServedFetchPayload {
                content_type: Some("application/json".to_owned()),
                bytes,
            })
        }
        wattswarm_network_transport_core::DirectDataObjectKind::SnapshotJson => {
            let scope = request
                .scope
                .as_deref()
                .ok_or_else(|| anyhow!("snapshot fetch requires scope"))?;
            let path = artifact_store.snapshot_path(scope, &request.object_id)?;
            let bytes = artifact_store.read_bytes(&path)?;
            Ok(ServedFetchPayload {
                content_type: Some("application/json".to_owned()),
                bytes,
            })
        }
    }
}

impl IrohDataPlaneService {
    fn new(
        state_dir: &Path,
        network_peer_id: &str,
        gossip_config: IrohGossipRuntimeConfig,
    ) -> Result<Arc<Self>> {
        let secret_key = load_secret_key_from_state_dir(state_dir)?;
        let endpoint_id = endpoint_id_from_secret_key(&secret_key);
        if !network_peer_id_matches_endpoint_id(network_peer_id, endpoint_id) {
            bail!(
                "iroh network peer id {network_peer_id} does not match node seed endpoint id {endpoint_id}"
            );
        }
        // Fail fast if another wattswarm process is already using this state_dir.
        // Must happen before any iroh init so we don't deadlock on iroh-blobs's
        // internal redb file lock and emit a misleading "router timed out" error.
        let data_plane_lock = acquire_data_plane_sentinel_lock(state_dir)?;
        let adapter = IrohTransportAdapter::from_endpoint_id(endpoint_id, network_peer_id);
        let endpoint_options = IrohEndpointOptions::resolve(state_dir)?;
        let runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let endpoint_future = || async {
            let mut builder = Endpoint::builder(presets::N0).secret_key(secret_key);
            if let Some(bind_addr) = endpoint_options.bind_addr {
                builder = builder.clear_ip_transports().bind_addr(bind_addr)?;
            }
            if !endpoint_options.relay_urls.is_empty() {
                builder =
                    builder.relay_mode(RelayMode::custom(endpoint_options.relay_urls.clone()));
            }
            Ok(builder.bind().await?)
        };
        let endpoint = block_on_iroh_data_plane_start(
            &runtime,
            "initialize iroh endpoint",
            endpoint_future(),
        )?;
        let control_handlers = Arc::new(Mutex::new(HashMap::new()));
        let router_future = || async {
            let blob_store = FsStore::load(iroh_blob_store_path(state_dir)).await?;
            let blobs = BlobsProtocol::new(&blob_store, None);
            let gossip = Gossip::builder()
                .max_message_size(gossip_config.max_message_size)
                .membership_config(gossip_config.membership_config())
                .broadcast_config(gossip_config.broadcast_config())
                .spawn(endpoint.clone());
            let router = Router::builder(endpoint.clone())
                .accept(
                    adapter.config.alpn.as_bytes(),
                    DirectFetchProtocol {
                        state_dir: state_dir.to_path_buf(),
                    },
                )
                .accept(
                    DEFAULT_IROH_CONTROL_ALPN.as_bytes(),
                    ControlStreamProtocol {
                        handlers: control_handlers.clone(),
                    },
                )
                .accept(iroh_gossip::ALPN, gossip.clone())
                .accept(iroh_blobs::ALPN, blobs)
                .spawn();
            Result::<(Gossip, FsStore, Router)>::Ok((gossip, blob_store, router))
        };
        let (gossip, blob_store, router) =
            block_on_iroh_data_plane_start(&runtime, "initialize iroh router", router_future())?;
        Ok(Arc::new(Self {
            runtime,
            endpoint,
            router,
            gossip,
            gossip_config,
            blob_store,
            adapter,
            endpoint_options,
            control_handlers,
            op_lock: Mutex::new(()),
            _data_plane_lock: data_plane_lock,
        }))
    }

    fn block_on<T: Send>(&self, future: impl Future<Output = T> + Send) -> T {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| self.runtime.block_on(future))
            }
            Ok(_) => std::thread::scope(|scope| {
                scope
                    .spawn(|| self.runtime.block_on(future))
                    .join()
                    .expect("join iroh runtime blocking operation")
            }),
            Err(_) => self.runtime.block_on(future),
        }
    }

    fn export_contact_material(&self, generated_at: u64) -> Result<TransportContactMaterial> {
        let addr = self.endpoint.addr();
        let direct_addrs = self
            .endpoint_options
            .published_direct_addrs(addr.ip_addrs().map(|value| value.to_string()).collect());
        let relay_urls = self.endpoint_options.published_relay_urls(
            addr.relay_urls()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
        );
        if direct_addrs.is_empty() && relay_urls.is_empty() {
            bail!("iroh contact material has no public direct addresses or relay urls");
        }
        Ok(self
            .adapter
            .material_payload(&direct_addrs, &relay_urls, generated_at))
    }

    fn fetch(
        &self,
        remote: &TransportContactMaterial,
        request: &DirectDataFetchRequest,
    ) -> Result<DirectDataFetchResponse> {
        let _guard = self
            .op_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let endpoint_addr = endpoint_addr_from_contact_material(remote)?;
        let alpn = remote
            .metadata
            .alpn
            .clone()
            .unwrap_or_else(|| self.adapter.config.alpn.clone());
        let request_bytes = serde_json::to_vec(request)?;
        self.block_on(async {
            let connection = self
                .endpoint
                .connect(endpoint_addr, alpn.as_bytes())
                .await
                .context("connect iroh endpoint")?;
            let (mut send, mut recv) = connection.open_bi().await.context("open bi stream")?;
            send.write_all(&request_bytes)
                .await
                .context("write fetch request")?;
            send.finish().context("finish request stream")?;
            let response_bytes = recv
                .read_to_end(MAX_FETCH_RESPONSE_BYTES)
                .await
                .context("read fetch response")?;
            let response: DirectDataFetchResponse =
                serde_json::from_slice(&response_bytes).context("decode fetch response")?;
            if !response.ok {
                bail!(
                    "{}",
                    response
                        .error
                        .unwrap_or_else(|| "remote iroh fetch failed".to_owned())
                );
            }
            Ok(response)
        })
    }

    fn send_control_stream_request(
        &self,
        remote: &TransportContactMaterial,
        request: &IrohControlStreamRequest,
        timeout: Duration,
    ) -> Result<IrohControlStreamResponse> {
        let _guard = self
            .op_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let endpoint_addr = endpoint_addr_from_contact_material(remote)?;
        let request_bytes = serde_json::to_vec(request)?;
        if request_bytes.len() > MAX_CONTROL_REQUEST_BYTES {
            bail!("iroh control stream request exceeds max request bytes");
        }
        self.block_on(async {
            tokio::time::timeout(timeout, async {
                let connection = self
                    .endpoint
                    .connect(endpoint_addr, DEFAULT_IROH_CONTROL_ALPN.as_bytes())
                    .await
                    .context("connect iroh control stream endpoint")?;
                let (mut send, mut recv) = connection
                    .open_bi()
                    .await
                    .context("open iroh control stream")?;
                send.write_all(&request_bytes)
                    .await
                    .context("write iroh control stream request")?;
                send.finish().context("finish control stream request")?;
                let response_bytes = recv
                    .read_to_end(MAX_CONTROL_RESPONSE_BYTES)
                    .await
                    .context("read iroh control stream response")?;
                let response: IrohControlStreamResponse =
                    serde_json::from_slice(&response_bytes)
                        .context("decode iroh control stream response")?;
                Ok(response)
            })
            .await
            .with_context(|| {
                format!(
                    "iroh control stream request timed out after {}ms",
                    timeout.as_millis()
                )
            })?
        })
    }

    fn set_control_stream_handler(&self, kind: &str, handler: Option<ControlStreamHandler>) {
        let mut handlers = self
            .control_handlers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match handler {
            Some(handler) => {
                handlers.insert(kind.to_owned(), handler);
            }
            None => {
                handlers.remove(kind);
            }
        }
    }

    fn put_blob_bytes(&self, bytes: Vec<u8>) -> Result<IrohBlobReference> {
        let size = bytes.len() as u64;
        let info = self
            .block_on(async { self.blob_store.add_slice(bytes).await })
            .context("store bytes in iroh-blobs")?;
        Ok(IrohBlobReference {
            hash: info.hash.to_hex(),
            format: iroh_blob_format_label(info.format).to_owned(),
            size,
        })
    }

    fn read_blob_bytes(&self, reference: &IrohBlobReference) -> Result<Vec<u8>> {
        let hash = parse_iroh_blob_hash(&reference.hash)?;
        let bytes = self
            .block_on(self.blob_store.get_bytes(hash))
            .context("read bytes from iroh-blobs")?
            .to_vec();
        validate_iroh_blob_bytes(reference, &bytes)?;
        Ok(bytes)
    }

    fn fetch_blob_bytes(
        &self,
        remote: &TransportContactMaterial,
        reference: &IrohBlobReference,
    ) -> Result<Vec<u8>> {
        let endpoint_addr = endpoint_addr_from_contact_material(remote)?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(endpoint_addr.clone());
        self.endpoint
            .address_lookup()
            .context("load local iroh address lookup")?
            .add(lookup);
        let hash = parse_iroh_blob_hash(&reference.hash)?;
        let format = parse_iroh_blob_format(&reference.format)?;
        let request = HashAndFormat::new(hash, format);
        self.block_on(async {
            self.blob_store
                .downloader(&self.endpoint)
                .download(request, vec![endpoint_addr.id])
                .await
        })
        .context("download iroh blob from remote")?;
        self.read_blob_bytes(reference)
    }

    fn shutdown(&self) {
        let _ = self.block_on(self.router.shutdown());
    }
}

fn iroh_blob_format_label(format: BlobFormat) -> &'static str {
    match format {
        BlobFormat::Raw => "raw",
        BlobFormat::HashSeq => "hash_seq",
    }
}

fn parse_iroh_blob_format(raw: &str) -> Result<BlobFormat> {
    match raw {
        "raw" => Ok(BlobFormat::Raw),
        "hash_seq" => Ok(BlobFormat::HashSeq),
        other => bail!("unsupported iroh blob format {other}"),
    }
}

fn parse_iroh_blob_hash(raw: &str) -> Result<IrohBlobHash> {
    IrohBlobHash::from_str(raw).with_context(|| format!("parse iroh blob hash {raw}"))
}

fn validate_iroh_blob_bytes(reference: &IrohBlobReference, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != reference.size {
        bail!(
            "iroh blob size mismatch for {}: expected {}, got {}",
            reference.hash,
            reference.size,
            bytes.len()
        );
    }
    let actual = blob_hash_for_bytes(bytes).to_hex();
    if actual != reference.hash {
        bail!(
            "iroh blob hash mismatch: expected {}, got {}",
            reference.hash,
            actual
        );
    }
    Ok(())
}

fn endpoint_addr_from_contact_material(contact: &TransportContactMaterial) -> Result<EndpointAddr> {
    let extra: IrohTransportMaterial =
        serde_json::from_value(contact.extra.clone()).context("decode iroh contact material")?;
    let endpoint_id = EndpointId::from_str(&extra.endpoint_id).context("parse iroh endpoint id")?;
    let addrs = extra
        .direct_addrs
        .into_iter()
        .map(|raw| {
            let addr = raw
                .parse::<SocketAddr>()
                .with_context(|| format!("parse iroh direct addr {raw}"))?;
            Ok(TransportAddr::Ip(addr))
        })
        .chain(extra.relay_urls.into_iter().map(|raw| {
            let url = iroh::RelayUrl::from_str(&raw)
                .with_context(|| format!("parse iroh relay url {raw}"))?;
            Ok(TransportAddr::Relay(url))
        }))
        .collect::<Result<Vec<_>>>()?;
    if addrs.is_empty() {
        bail!("iroh contact material missing transport addresses");
    }
    Ok(EndpointAddr::from_parts(endpoint_id, addrs))
}

fn ensure_local_iroh_data_plane_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
) -> Result<Arc<IrohDataPlaneService>> {
    ensure_local_iroh_data_plane_for_network_peer_id_with_gossip_config(
        state_dir,
        network_peer_id,
        IrohGossipRuntimeConfig::default(),
        false,
    )
}

fn ensure_local_iroh_data_plane_for_network_peer_id_with_gossip_config(
    state_dir: &Path,
    network_peer_id: &str,
    gossip_config: IrohGossipRuntimeConfig,
    require_config_match: bool,
) -> Result<Arc<IrohDataPlaneService>> {
    let mut services = local_iroh_data_planes()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = services.get(state_dir) {
        if existing.adapter.network_peer_id() != network_peer_id {
            bail!(
                "iroh data plane already exists for {} with a different network peer id",
                state_dir.display()
            );
        }
        if require_config_match && existing.gossip_config != gossip_config {
            bail!(
                "iroh data plane already exists for {} with different gossip runtime config",
                state_dir.display()
            );
        }
        return Ok(existing.clone());
    }
    let service = IrohDataPlaneService::new(state_dir, network_peer_id, gossip_config)?;
    services.insert(state_dir.to_path_buf(), service.clone());
    Ok(service)
}

pub fn export_local_contact_material_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    generated_at: u64,
) -> Result<TransportContactMaterial> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .export_contact_material(generated_at)
}

pub fn export_local_contact_material_for_network_peer_id_with_gossip_config(
    state_dir: &Path,
    network_peer_id: &str,
    generated_at: u64,
    gossip_config: IrohGossipRuntimeConfig,
) -> Result<TransportContactMaterial> {
    ensure_local_iroh_data_plane_for_network_peer_id_with_gossip_config(
        state_dir,
        network_peer_id,
        gossip_config,
        true,
    )?
    .export_contact_material(generated_at)
}

pub fn fetch_direct_data_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
    request: &DirectDataFetchRequest,
) -> Result<DirectDataFetchResponse> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .fetch(remote_contact, request)
}

pub fn send_control_stream_request_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
    request: &IrohControlStreamRequest,
) -> Result<IrohControlStreamResponse> {
    send_control_stream_request_for_network_peer_id_with_timeout(
        state_dir,
        network_peer_id,
        remote_contact,
        request,
        Duration::from_millis(DEFAULT_IROH_CONTROL_STREAM_TIMEOUT_MS),
    )
}

pub fn send_control_stream_request_for_network_peer_id_with_timeout(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
    request: &IrohControlStreamRequest,
    timeout: Duration,
) -> Result<IrohControlStreamResponse> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .send_control_stream_request(remote_contact, request, timeout)
}

pub fn set_local_control_stream_handler_for_network_peer_id<H>(
    state_dir: &Path,
    network_peer_id: &str,
    kind: &str,
    handler: Option<H>,
) -> Result<()>
where
    H: Fn(String, IrohControlStreamRequest) -> IrohControlStreamResponse + Send + Sync + 'static,
{
    let handler = handler.map(|handler| Arc::new(handler) as ControlStreamHandler);
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .set_control_stream_handler(kind, handler);
    Ok(())
}

pub fn install_local_contact_material_control_handler_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
) -> Result<()> {
    let state_dir = state_dir.to_path_buf();
    let network_peer_id = network_peer_id.to_owned();
    set_local_control_stream_handler_for_network_peer_id(
        &state_dir.clone(),
        &network_peer_id.clone(),
        IROH_CONTROL_KIND_CONTACT_MATERIAL,
        Some(
            move |_remote_peer_id: String, request: IrohControlStreamRequest| match request
                .kind
                .as_str()
            {
                IROH_CONTROL_KIND_CONTACT_MATERIAL => {
                    match export_local_contact_material_for_network_peer_id(
                        &state_dir,
                        &network_peer_id,
                        unix_timestamp_secs(),
                    )
                    .and_then(|material| Ok(serde_json::to_vec(&material)?))
                    {
                        Ok(payload) => IrohControlStreamResponse {
                            ok: true,
                            error: None,
                            payload,
                        },
                        Err(err) => IrohControlStreamResponse {
                            ok: false,
                            error: Some(err.to_string()),
                            payload: Vec::new(),
                        },
                    }
                }
                _ => IrohControlStreamResponse {
                    ok: false,
                    error: Some(format!("unsupported iroh control kind {}", request.kind)),
                    payload: Vec::new(),
                },
            },
        ),
    )
}

pub fn request_contact_material_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
) -> Result<TransportContactMaterial> {
    let response = send_control_stream_request_for_network_peer_id(
        state_dir,
        network_peer_id,
        remote_contact,
        &IrohControlStreamRequest {
            kind: IROH_CONTROL_KIND_CONTACT_MATERIAL.to_owned(),
            payload: Vec::new(),
        },
    )?;
    if !response.ok {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "remote contact material request failed".to_owned())
        );
    }
    Ok(serde_json::from_slice(&response.payload)?)
}

pub fn local_gossip_for_network_peer_id(state_dir: &Path, network_peer_id: &str) -> Result<Gossip> {
    Ok(
        ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
            .gossip
            .clone(),
    )
}

pub fn register_remote_contact_material_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
) -> Result<()> {
    let endpoint_addr = endpoint_addr_from_contact_material(remote_contact)?;
    let lookup = MemoryLookup::new();
    lookup.add_endpoint_info(endpoint_addr);
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .endpoint
        .address_lookup()
        .context("load local iroh address lookup")?
        .add(lookup);
    Ok(())
}

pub fn put_local_blob_bytes_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    bytes: impl Into<Vec<u8>>,
) -> Result<IrohBlobReference> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .put_blob_bytes(bytes.into())
}

pub fn read_local_blob_bytes_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    reference: &IrohBlobReference,
) -> Result<Vec<u8>> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .read_blob_bytes(reference)
}

pub fn fetch_remote_blob_bytes_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
    reference: &IrohBlobReference,
) -> Result<Vec<u8>> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?
        .fetch_blob_bytes(remote_contact, reference)
}

pub fn shutdown_local_iroh_data_plane(state_dir: &Path) {
    let service = {
        let mut services = local_iroh_data_planes()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        services.remove(state_dir)
    };
    if let Some(service) = service {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| {
                    service.shutdown();
                    drop(service);
                });
            }
            Ok(_) => {
                std::thread::scope(|scope| {
                    scope.spawn(|| {
                        service.shutdown();
                        drop(service);
                    });
                });
            }
            Err(_) => {
                service.shutdown();
                drop(service);
            }
        }
    }
}

pub fn endpoint_id_from_secret_key(secret_key: &SecretKey) -> EndpointId {
    EndpointId::from_bytes(secret_key.public().as_bytes())
        .expect("iroh secret public key must be a valid endpoint id")
}

pub fn local_endpoint_id_from_state_dir(state_dir: &Path) -> Result<EndpointId> {
    Ok(endpoint_id_from_secret_key(
        &load_secret_key_from_state_dir(state_dir)?,
    ))
}

fn network_peer_id_matches_endpoint_id(network_peer_id: &str, endpoint_id: EndpointId) -> bool {
    network_peer_id == endpoint_id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_gossip::api::Event;
    use n0_future::StreamExt;
    use tempfile::tempdir;
    use tokio::time::{Duration, timeout};

    fn seed_state_dir(dir: &Path, seed: [u8; 32]) {
        fs::write(dir.join("node_seed.hex"), hex::encode(seed)).expect("write node seed");
    }

    #[test]
    fn exports_iroh_contact_material_with_capabilities() {
        let adapter = IrohTransportAdapter::from_seed_bytes([5u8; 32]).expect("adapter");
        let material = adapter.material_payload(
            &["127.0.0.1:7777".to_owned()],
            &["https://relay.example".to_owned()],
            42,
        );
        assert_eq!(material.transport, "iroh_direct");
        assert_eq!(material.peer_id, adapter.network_peer_id());
        assert_eq!(material.metadata.generated_at, 42);
        assert!(material.metadata.capabilities.supports_iroh_direct);
        assert!(material.metadata.endpoint_id.is_some());
        let extra: IrohTransportMaterial =
            serde_json::from_value(material.extra.clone()).expect("extra");
        assert_eq!(extra.direct_addrs, vec!["127.0.0.1:7777".to_owned()]);
    }

    #[test]
    fn local_data_plane_applies_gossip_runtime_config() {
        let dir = tempdir().expect("tempdir");
        seed_state_dir(dir.path(), [31u8; 32]);
        let endpoint_id = local_endpoint_id_from_state_dir(dir.path()).expect("endpoint id");
        let config = IrohGossipRuntimeConfig {
            max_message_size: 1024,
            active_view_capacity: 3,
            passive_view_capacity: 8,
            shuffle_active_view_count: 2,
            maintenance_interval: Duration::from_millis(250),
        };

        let service = ensure_local_iroh_data_plane_for_network_peer_id_with_gossip_config(
            dir.path(),
            &endpoint_id.to_string(),
            config.clone(),
            true,
        )
        .expect("data plane");

        assert_eq!(service.gossip.max_message_size(), 1024);
        assert_eq!(service.gossip_config, config);
        shutdown_local_iroh_data_plane(dir.path());
    }

    #[test]
    fn iroh_endpoint_options_parse_custom_relay_and_publish_flag() {
        let options = IrohEndpointOptions::from_raw_env(
            Some("https://relay.wattetheria.com/, https://relay.wattetheria.com"),
            None,
            Some("false"),
        )
        .expect("endpoint options");

        assert_eq!(
            options.published_relay_urls,
            vec!["https://relay.wattetheria.com".to_owned()]
        );
        assert_eq!(
            options.published_direct_addrs(vec!["10.0.0.1:1234".to_owned()]),
            Vec::<String>::new()
        );
        assert!(options.published_direct_addrs.is_empty());
    }

    #[test]
    fn startup_config_relay_urls_prefers_persisted_non_empty_list() {
        let dir = tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("startup_config.json"),
            r#"{"network_mode":"wan","relay_urls":[" https://relay.wattetheria.com/ ", "", "https://relay2.wattetheria.com"]}"#,
        )
        .expect("write startup config");

        assert_eq!(
            startup_config_relay_urls(dir.path()),
            Some("https://relay.wattetheria.com/,https://relay2.wattetheria.com".to_owned())
        );
    }

    #[test]
    fn startup_config_relay_urls_ignores_missing_empty_or_invalid_config() {
        let dir = tempdir().expect("tempdir");
        assert_eq!(startup_config_relay_urls(dir.path()), None);

        let path = dir.path().join("startup_config.json");
        std::fs::write(&path, r#"{"network_mode":"wan"}"#).expect("write config");
        assert_eq!(startup_config_relay_urls(dir.path()), None);

        std::fs::write(&path, r#"{"relay_urls":[]}"#).expect("write config");
        assert_eq!(startup_config_relay_urls(dir.path()), None);

        std::fs::write(&path, r#"{"relay_urls":["  "]}"#).expect("write config");
        assert_eq!(startup_config_relay_urls(dir.path()), None);

        std::fs::write(&path, "not-json").expect("write config");
        assert_eq!(startup_config_relay_urls(dir.path()), None);
    }

    #[test]
    fn iroh_endpoint_options_resolve_uses_startup_config_relay_urls() {
        let dir = tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("startup_config.json"),
            r#"{"relay_urls":["https://relay2.wattetheria.com/"]}"#,
        )
        .expect("write startup config");

        let options = IrohEndpointOptions::resolve(dir.path()).expect("resolve options");
        assert_eq!(
            options.published_relay_urls,
            vec!["https://relay2.wattetheria.com".to_owned()]
        );
    }

    #[test]
    fn iroh_endpoint_options_hide_direct_addrs_by_default() {
        let options =
            IrohEndpointOptions::from_raw_env(None, None, None).expect("endpoint options");

        assert_eq!(
            options.published_direct_addrs(vec!["10.0.0.1:1234".to_owned()]),
            Vec::<String>::new()
        );
        assert!(options.published_direct_addrs.is_empty());
    }

    #[test]
    fn iroh_endpoint_options_parse_fixed_bind_addr() {
        let options = IrohEndpointOptions::from_raw_env(None, Some("0.0.0.0:4002"), Some("true"))
            .expect("endpoint options");

        assert_eq!(
            options.bind_addr,
            Some("0.0.0.0:4002".parse().expect("socket addr"))
        );
        assert!(options.published_direct_addrs.is_empty());
    }

    #[test]
    fn iroh_endpoint_options_publish_direct_addrs_uses_explicit_addrs() {
        let options = IrohEndpointOptions::from_raw_env(
            None,
            None,
            Some("203.0.113.10:4002, 203.0.113.10:4002\n192.168.1.20:4002"),
        )
        .expect("endpoint options");

        assert_eq!(
            options.published_direct_addrs(vec!["172.20.0.4:4002".to_owned()]),
            vec![
                "203.0.113.10:4002".to_owned(),
                "192.168.1.20:4002".to_owned()
            ]
        );
    }

    #[test]
    fn iroh_data_plane_start_timeout_parses_override() {
        assert_eq!(
            parse_positive_u64_env(ENV_IROH_DATA_PLANE_START_TIMEOUT_MS, None, 120_000)
                .expect("default timeout"),
            120_000
        );
        assert_eq!(
            parse_positive_u64_env(ENV_IROH_DATA_PLANE_START_TIMEOUT_MS, Some("45000"), 120_000)
                .expect("custom timeout"),
            45_000
        );
        assert!(
            parse_positive_u64_env(ENV_IROH_DATA_PLANE_START_TIMEOUT_MS, Some("0"), 120_000)
                .is_err()
        );
        assert!(
            parse_positive_u64_env(ENV_IROH_DATA_PLANE_START_TIMEOUT_MS, Some("slow"), 120_000)
                .is_err()
        );
    }

    #[test]
    fn data_plane_sentinel_lock_rejects_concurrent_acquisition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = acquire_data_plane_sentinel_lock(dir.path()).expect("first lock acquired");
        let err = acquire_data_plane_sentinel_lock(dir.path())
            .expect_err("second lock acquisition must fail while the first is held");
        let message = format!("{err:#}");
        assert!(
            message.contains("iroh data plane already locked"),
            "expected fail-fast message, got: {message}"
        );
        drop(first);
        let _retry = acquire_data_plane_sentinel_lock(dir.path())
            .expect("lock acquisition succeeds after the first holder is dropped");
    }

    #[test]
    fn relay_only_contact_material_can_be_dial_target() {
        let adapter = IrohTransportAdapter::from_seed_bytes([6u8; 32]).expect("adapter");
        let material =
            adapter.material_payload(&[], &["https://relay.wattetheria.com".to_owned()], 42);

        let extra: IrohTransportMaterial =
            serde_json::from_value(material.extra.clone()).expect("extra");
        assert!(extra.direct_addrs.is_empty());
        assert_eq!(
            extra.relay_urls,
            vec!["https://relay.wattetheria.com".to_owned()]
        );
        endpoint_addr_from_contact_material(&material).expect("relay-only endpoint addr");
    }

    #[test]
    fn exports_contact_material_with_iroh_endpoint_identity() {
        let dir = tempdir().expect("tempdir");
        seed_state_dir(dir.path(), [11u8; 32]);
        let endpoint_id = local_endpoint_id_from_state_dir(dir.path()).expect("endpoint id");
        let endpoint_peer_id = endpoint_id.to_string();

        let first =
            export_local_contact_material_for_network_peer_id(dir.path(), &endpoint_peer_id, 100)
                .expect("first contact material");
        let second =
            export_local_contact_material_for_network_peer_id(dir.path(), &endpoint_peer_id, 101)
                .expect("second contact material");

        assert_eq!(first.peer_id, endpoint_peer_id);
        assert_eq!(second.peer_id, endpoint_peer_id);
        assert_eq!(first.metadata.endpoint_id, Some(endpoint_id.to_string()));
        assert_eq!(second.metadata.endpoint_id, Some(endpoint_id.to_string()));

        shutdown_local_iroh_data_plane(dir.path());
    }

    #[test]
    fn existing_iroh_endpoint_rejects_different_network_peer_id() {
        let dir = tempdir().expect("tempdir");
        seed_state_dir(dir.path(), [13u8; 32]);
        let endpoint_id = local_endpoint_id_from_state_dir(dir.path()).expect("endpoint id");
        export_local_contact_material_for_network_peer_id(dir.path(), &endpoint_id.to_string(), 1)
            .expect("contact material");

        let err = export_local_contact_material_for_network_peer_id(dir.path(), "other-peer", 2)
            .expect_err("different peer id should fail");
        assert!(
            err.to_string().contains("different network peer id"),
            "{err}"
        );

        shutdown_local_iroh_data_plane(dir.path());
    }

    #[test]
    fn gossip_topic_id_derivation_is_stable_and_scoped() {
        let first = derive_gossip_topic_id("mainnet", "group:alpha", "messages");
        let second = derive_gossip_topic_id("mainnet", "group:alpha", "messages");
        let different_scope = derive_gossip_topic_id("mainnet", "group:beta", "messages");
        let different_kind = derive_gossip_topic_id("mainnet", "group:alpha", "events");

        assert_eq!(first, second);
        assert_ne!(first, different_scope);
        assert_ne!(first, different_kind);
        assert_eq!(first.as_bytes().len(), 32);
    }

    #[test]
    fn blob_hash_for_bytes_uses_iroh_blobs_hashing() {
        let hash = blob_hash_for_bytes(b"wattswarm artifact");
        assert_eq!(hash, IrohBlobHash::new(b"wattswarm artifact"));
        assert_eq!(hash.as_bytes().len(), 32);
    }

    #[test]
    fn local_iroh_blob_store_validates_hash_and_size() {
        let dir = tempdir().expect("tempdir");
        seed_state_dir(dir.path(), [17u8; 32]);
        let endpoint_id = local_endpoint_id_from_state_dir(dir.path()).expect("endpoint id");
        let peer_id = endpoint_id.to_string();

        let reference =
            put_local_blob_bytes_for_network_peer_id(dir.path(), &peer_id, b"content-addressed")
                .expect("put blob");
        assert_eq!(reference.format, "raw");
        assert_eq!(reference.size, 17);
        assert_eq!(
            reference.hash,
            blob_hash_for_bytes(b"content-addressed").to_hex()
        );

        let bytes = read_local_blob_bytes_for_network_peer_id(dir.path(), &peer_id, &reference)
            .expect("read blob");
        assert_eq!(bytes, b"content-addressed");

        let mut wrong_size = reference.clone();
        wrong_size.size += 1;
        let err = read_local_blob_bytes_for_network_peer_id(dir.path(), &peer_id, &wrong_size)
            .expect_err("wrong size should fail");
        assert!(err.to_string().contains("size mismatch"), "{err}");

        shutdown_local_iroh_data_plane(dir.path());
    }

    #[test]
    fn two_nodes_fetch_blob_over_iroh_blobs_protocol() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [18u8; 32]);
        seed_state_dir(dir_b.path(), [19u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        let reference =
            put_local_blob_bytes_for_network_peer_id(dir_b.path(), &peer_b, b"remote blob")
                .expect("put remote blob");

        let bytes = fetch_remote_blob_bytes_for_network_peer_id(
            dir_a.path(),
            &peer_a,
            &contact_b,
            &reference,
        )
        .expect("fetch remote blob");
        assert_eq!(bytes, b"remote blob");

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn two_nodes_exchange_iroh_gossip_notification_on_shared_topic() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [21u8; 32]);
        seed_state_dir(dir_b.path(), [22u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let contact_a = export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        register_remote_contact_material_for_network_peer_id(dir_a.path(), &peer_a, &contact_b)
            .expect("node a learns node b");
        register_remote_contact_material_for_network_peer_id(dir_b.path(), &peer_b, &contact_a)
            .expect("node b learns node a");

        let gossip_a = local_gossip_for_network_peer_id(dir_a.path(), &peer_a).expect("gossip a");
        let gossip_b = local_gossip_for_network_peer_id(dir_b.path(), &peer_b).expect("gossip b");
        let topic_id = derive_gossip_topic_id("mainnet", "group.alpha", "messages");
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

        runtime
            .block_on(async {
                let mut topic_a = gossip_a.subscribe(topic_id, vec![]).await?;
                let mut topic_b = gossip_b.subscribe(topic_id, vec![endpoint_a]).await?;
                timeout(Duration::from_secs(5), topic_a.joined())
                    .await
                    .context("wait node a gossip join")??;
                timeout(Duration::from_secs(5), topic_b.joined())
                    .await
                    .context("wait node b gossip join")??;

                topic_b
                    .broadcast(b"hello wattswarm".to_vec().into())
                    .await?;
                let received = timeout(Duration::from_secs(5), async {
                    loop {
                        let event = topic_a
                            .next()
                            .await
                            .ok_or_else(|| anyhow!("node a gossip stream closed"))??;
                        if let Event::Received(message) = event {
                            return Result::<Vec<u8>>::Ok(message.content.to_vec());
                        }
                    }
                })
                .await
                .context("wait gossip notification")??;
                assert_eq!(received, b"hello wattswarm");
                Result::<()>::Ok(())
            })
            .expect("exchange gossip notification");

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn two_nodes_exchange_iroh_control_stream_request_response() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [31u8; 32]);
        seed_state_dir(dir_b.path(), [32u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        set_local_control_stream_handler_for_network_peer_id(
            dir_b.path(),
            &peer_b,
            "echo.v1",
            Some(
                |_remote_peer_id: String, request: IrohControlStreamRequest| {
                    IrohControlStreamResponse {
                        ok: true,
                        error: None,
                        payload: format!("{}:", request.kind)
                            .into_bytes()
                            .into_iter()
                            .chain(request.payload)
                            .collect(),
                    }
                },
            ),
        )
        .expect("set control handler");

        let response = send_control_stream_request_for_network_peer_id(
            dir_a.path(),
            &peer_a,
            &contact_b,
            &IrohControlStreamRequest {
                kind: "echo.v1".to_owned(),
                payload: b"page-1".to_vec(),
            },
        )
        .expect("control stream response");

        assert!(response.ok);
        assert_eq!(response.payload, b"echo.v1:page-1");

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn two_nodes_request_contact_material_over_iroh_control_stream() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [41u8; 32]);
        seed_state_dir(dir_b.path(), [42u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let bootstrap_contact_b =
            export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
                .expect("bootstrap contact b");
        install_local_contact_material_control_handler_for_network_peer_id(dir_b.path(), &peer_b)
            .expect("install contact handler");

        let requested_contact = request_contact_material_for_network_peer_id(
            dir_a.path(),
            &peer_a,
            &bootstrap_contact_b,
        )
        .expect("requested contact material");

        assert_eq!(requested_contact.peer_id, peer_b);
        assert_eq!(
            requested_contact.metadata.endpoint_id,
            Some(endpoint_b.to_string())
        );

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn local_service_fetches_reference_bytes_over_iroh() {
        let local_dir = tempdir().expect("local tempdir");
        let remote_dir = tempdir().expect("remote tempdir");
        seed_state_dir(local_dir.path(), [7u8; 32]);
        seed_state_dir(remote_dir.path(), [9u8; 32]);

        let local_peer_id = local_endpoint_id_from_state_dir(local_dir.path())
            .expect("local endpoint")
            .to_string();
        let remote_peer_id = local_endpoint_id_from_state_dir(remote_dir.path())
            .expect("remote endpoint")
            .to_string();

        let remote_store = open_artifact_store(remote_dir.path()).expect("artifact store");
        let bytes = br#"{"hello":"iroh"}"#;
        let digest = format!("sha256:{}", wattswarm_crypto::sha256_hex(bytes));
        remote_store
            .write_validated_bytes(
                ArtifactKind::Reference,
                &digest,
                None,
                bytes,
                Some(&digest),
                Some(bytes.len() as u64),
            )
            .expect("write remote artifact");

        let remote_contact = export_local_contact_material_for_network_peer_id(
            remote_dir.path(),
            &remote_peer_id,
            11,
        )
        .expect("remote contact material");
        let response = fetch_direct_data_for_network_peer_id(
            local_dir.path(),
            &local_peer_id,
            &remote_contact,
            &DirectDataFetchRequest {
                object_kind:
                    wattswarm_network_transport_core::DirectDataObjectKind::ReferenceArtifact,
                object_id: digest.clone(),
                scope: None,
                source_uri: Some("ipfs://remote-reference".to_owned()),
                expected_digest: Some(digest),
                expected_size: Some(bytes.len() as u64),
            },
        )
        .expect("fetch direct data");
        assert!(response.ok);
        assert_eq!(response.bytes, bytes);

        shutdown_local_iroh_data_plane(local_dir.path());
        shutdown_local_iroh_data_plane(remote_dir.path());
    }
}
