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
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
const ENV_NETWORK_DEBUG_DIAGNOSTICS: &str = "WATTSWARM_NETWORK_DEBUG_DIAGNOSTICS";
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
const IROH_ONLINE_WAIT_TIMEOUT_MS: u64 = 10_000;
const CONTROL_STREAM_RECOVERY_TIMEOUT_THRESHOLD: u32 = 3;
const CONTROL_STREAM_RECOVERY_COOLDOWN: Duration = Duration::from_secs(60);
const PUBLIC_IP_RESOLVER_TIMEOUT_MS: u64 = 2_000;
const PUBLIC_IP_CACHE_TTL_SECS: u64 = 300;
const DIAGNOSTIC_LOG_RELATIVE_PATH: &str = "diagnostics/wattswarm_node.jsonl";
const IROH_CONTACT_MATERIAL_EXPORT_PHASE: &str = "iroh.contact_material.export";
const DEFAULT_PUBLIC_IP_RESOLVER_URLS: &[&str] = &[
    "https://api.ipify.org",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
];
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohRouteProbeResult {
    pub peer_id: String,
    pub endpoint_id: String,
    pub selected_route: String,
    pub path_count: usize,
    pub stage: String,
    pub runtime: String,
    pub direct_capable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
}

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

fn observed_direct_addrs_for_publish(
    observed_direct_addrs: Vec<String>,
    bind_port: Option<u16>,
) -> Vec<String> {
    let mut published = Vec::new();
    for raw in observed_direct_addrs {
        // A relay-observed global address carries the NAT flow port, which is
        // not cold-dialable; the deployment's inbound mapping listens on the
        // local bind port, so global addresses are republished on that port.
        let published_addr = match (
            raw.parse::<SocketAddr>(),
            bind_port.filter(|port| *port != 0),
        ) {
            (Ok(addr), Some(port)) if ip_addr_is_global(&addr.ip()) => {
                SocketAddr::new(addr.ip(), port).to_string()
            }
            (Ok(addr), _) => addr.to_string(),
            (Err(_), _) => raw,
        };
        if !published.contains(&published_addr) {
            published.push(published_addr);
        }
    }
    published
}

fn observed_direct_addrs_for_publish_with_public_ip_fallback(
    observed_direct_addrs: Vec<String>,
    bind_addr: Option<SocketAddr>,
    public_ip: Option<IpAddr>,
) -> Vec<String> {
    let bind_port = bind_addr.map(|addr| addr.port()).filter(|port| *port != 0);
    let publish_loopback = bind_addr.is_some_and(|addr| addr.ip().is_loopback());
    let mut published = observed_direct_addrs_for_publish(observed_direct_addrs, bind_port)
        .into_iter()
        .filter(|raw| {
            raw.parse::<SocketAddr>().is_ok_and(|addr| {
                ip_addr_is_global(&addr.ip()) || (publish_loopback && addr.ip().is_loopback())
            })
        })
        .collect::<Vec<_>>();
    let Some(port) = bind_port else {
        return published;
    };
    if !published.is_empty() {
        return published;
    }
    let Some(public_ip) = public_ip.filter(ip_addr_is_global) else {
        return published;
    };
    let published_addr = SocketAddr::new(public_ip, port).to_string();
    if !published.contains(&published_addr) {
        published.push(published_addr);
    }
    published
}

fn ip_addr_is_global(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            let is_this_network = a == 0;
            let is_cgnat = a == 100 && (b & 0b1100_0000) == 64;
            let is_ietf_protocol_assignment = a == 192 && b == 0 && c == 0;
            let is_benchmarking = a == 198 && (b == 18 || b == 19);
            let is_deprecated_6to4_relay_anycast = a == 192 && b == 88 && c == 99;
            let is_reserved = a >= 240;
            let is_non_global = v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_multicast()
                || is_this_network
                || is_cgnat
                || is_ietf_protocol_assignment
                || is_benchmarking
                || is_deprecated_6to4_relay_anycast
                || is_reserved;
            !is_non_global
        }
        IpAddr::V6(v6) => {
            let segments = v6.segments();
            let first = segments[0];
            let is_global_unicast = (first & 0xe000) == 0x2000;
            let is_documentation = first == 0x2001 && segments[1] == 0x0db8;
            let is_benchmarking = first == 0x2001 && segments[1] == 0x0002;
            let is_orchid_v2 = first == 0x2001 && (segments[1] & 0xfff0) == 0x0020;
            is_global_unicast && !(is_documentation || is_benchmarking || is_orchid_v2)
        }
    }
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
    state_dir: PathBuf,
    runtime: Runtime,
    endpoint: Endpoint,
    router: Router,
    gossip: Gossip,
    gossip_config: IrohGossipRuntimeConfig,
    blob_store: FsStore,
    adapter: IrohTransportAdapter,
    endpoint_options: IrohEndpointOptions,
    control_handlers: ControlStreamHandlers,
    fetch_lock: Mutex<()>,
    endpoint_online_awaited: AtomicBool,
    control_stream_recovery: Mutex<ControlStreamRecoveryState>,
    public_ip_cache: Mutex<Option<PublicIpCacheEntry>>,
    contact_material_diagnostic_key: Mutex<Option<String>>,
    // Sentinel flock held for the lifetime of the service. Drop releases the
    // OS-level lock so a subsequent process can acquire it. Held to prevent two
    // wattswarm processes from sharing one state_dir (which would otherwise
    // deadlock on the iroh-blobs redb file lock with a misleading 120s timeout).
    _data_plane_lock: fs::File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IrohEndpointOptions {
    relay_urls: Vec<RelayUrl>,
    bind_addr: Option<SocketAddr>,
    publish_observed_direct_addrs: bool,
    published_direct_addrs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicIpResolverResult {
    ip: IpAddr,
    source: String,
}

#[derive(Debug, Clone)]
struct PublicIpCacheEntry {
    resolved_at_secs: u64,
    result: PublicIpResolverResult,
}

#[derive(Debug, Default)]
struct ControlStreamRecoveryState {
    consecutive_timeouts: u32,
    last_network_change_at: Option<Instant>,
}

impl ControlStreamRecoveryState {
    fn record_outcome(&mut self, timed_out: bool, now: Instant) -> bool {
        if !timed_out {
            self.consecutive_timeouts = 0;
            return false;
        }
        self.consecutive_timeouts = self.consecutive_timeouts.saturating_add(1);
        if self.consecutive_timeouts < CONTROL_STREAM_RECOVERY_TIMEOUT_THRESHOLD {
            return false;
        }
        if self.last_network_change_at.is_some_and(|last| {
            now.saturating_duration_since(last) < CONTROL_STREAM_RECOVERY_COOLDOWN
        }) {
            return false;
        }
        self.consecutive_timeouts = 0;
        self.last_network_change_at = Some(now);
        true
    }
}

impl PublicIpCacheEntry {
    fn is_fresh(&self, now_secs: u64) -> bool {
        now_secs.saturating_sub(self.resolved_at_secs) <= PUBLIC_IP_CACHE_TTL_SECS
    }
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
        let publish_observed_direct_addrs = publish_direct_addrs
            .map(str::trim)
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        Ok(Self {
            relay_urls,
            bind_addr: parse_optional_socket_addr_env(ENV_IROH_BIND_ADDR, bind_addr)?,
            publish_observed_direct_addrs,
            published_direct_addrs: parse_direct_addr_publish_env(publish_direct_addrs)?,
        })
    }

    fn published_direct_addrs(
        &self,
        observed_direct_addrs: Vec<String>,
        fallback_public_ip: Option<IpAddr>,
    ) -> Vec<String> {
        if self.publish_observed_direct_addrs {
            return observed_direct_addrs_for_publish_with_public_ip_fallback(
                observed_direct_addrs,
                self.bind_addr,
                fallback_public_ip,
            );
        }
        self.published_direct_addrs.clone()
    }

    fn published_relay_urls(&self, observed_relay_urls: Vec<String>) -> Vec<String> {
        observed_relay_urls
            .into_iter()
            .find_map(|url| {
                let normalized = url.trim().trim_end_matches('/').to_owned();
                (!normalized.is_empty()).then_some(normalized)
            })
            .into_iter()
            .collect()
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

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

fn parse_public_ip_response(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim();
    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return ip_addr_is_global(&ip).then_some(ip);
    }
    for line in trimmed.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.trim() != "ip" {
            continue;
        }
        let Ok(ip) = value.trim().parse::<IpAddr>() else {
            continue;
        };
        if ip_addr_is_global(&ip) {
            return Some(ip);
        }
    }
    None
}

fn append_iroh_contact_material_diagnostic(
    state_dir: &Path,
    entry: &serde_json::Value,
) -> Result<()> {
    let path = state_dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("create Wattswarm diagnostics directory")?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open Wattswarm diagnostics log {}", path.display()))?;
    file.write_all(serde_json::to_string(entry)?.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

async fn resolve_public_ip_from_default_endpoints() -> Result<Option<PublicIpResolverResult>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(PUBLIC_IP_RESOLVER_TIMEOUT_MS))
        .build()
        .context("build public IP resolver client")?;
    let mut errors = Vec::new();
    for source in DEFAULT_PUBLIC_IP_RESOLVER_URLS {
        let response = match client.get(*source).send().await {
            Ok(response) => response,
            Err(error) => {
                errors.push(format!("{source}: {error}"));
                continue;
            }
        };
        let status = response.status();
        if !status.is_success() {
            errors.push(format!("{source}: HTTP {status}"));
            continue;
        }
        let body = match response.text().await {
            Ok(body) => body,
            Err(error) => {
                errors.push(format!("{source}: {error}"));
                continue;
            }
        };
        if let Some(ip) = parse_public_ip_response(&body) {
            return Ok(Some(PublicIpResolverResult {
                ip,
                source: (*source).to_owned(),
            }));
        }
        errors.push(format!(
            "{source}: response did not contain a global IP address"
        ));
    }
    if errors.is_empty() {
        Ok(None)
    } else {
        bail!("public IP resolver failed: {}", errors.join("; "))
    }
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
            state_dir: state_dir.to_path_buf(),
            runtime,
            endpoint,
            router,
            gossip,
            gossip_config,
            blob_store,
            adapter,
            endpoint_options,
            control_handlers,
            fetch_lock: Mutex::new(()),
            endpoint_online_awaited: AtomicBool::new(false),
            control_stream_recovery: Mutex::new(ControlStreamRecoveryState::default()),
            public_ip_cache: Mutex::new(None),
            contact_material_diagnostic_key: Mutex::new(None),
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

    /// `Endpoint::addr()` is only complete after the endpoint has been online.
    /// The wait is bounded so an unreachable relay cannot stall exports, and
    /// awaited only once because online state is a latch.
    fn await_endpoint_online_once(&self) -> &'static str {
        if self.endpoint_online_awaited.swap(true, Ordering::SeqCst) {
            return "already_awaited";
        }
        match self.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(IROH_ONLINE_WAIT_TIMEOUT_MS),
                self.endpoint.online(),
            )
            .await
        }) {
            Ok(()) => "online",
            Err(_) => "timeout",
        }
    }

    fn resolve_public_ip_for_publish(
        &self,
        online_wait_status: &str,
    ) -> Option<PublicIpResolverResult> {
        let now_secs = unix_timestamp_secs();
        {
            let cache = self
                .public_ip_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = cache.as_ref().filter(|entry| entry.is_fresh(now_secs)) {
                return Some(entry.result.clone());
            }
        }
        let resolved = match self.block_on(resolve_public_ip_from_default_endpoints()) {
            Ok(Some(result)) => result,
            Ok(None) => return None,
            Err(error) => {
                self.record_contact_material_export_diagnostic(
                    "warn",
                    "resolver_failed",
                    "public IP resolver failed",
                    online_wait_status,
                    &[],
                    &[],
                    &[],
                    None,
                    Some(error.to_string()),
                );
                return None;
            }
        };
        let mut cache = self
            .public_ip_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *cache = Some(PublicIpCacheEntry {
            resolved_at_secs: now_secs,
            result: resolved.clone(),
        });
        Some(resolved)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_contact_material_export_diagnostic(
        &self,
        level: &str,
        status: &str,
        message: &str,
        online_wait_status: &str,
        raw_ip_addrs: &[String],
        published_direct_addrs: &[String],
        relay_urls: &[String],
        public_ip: Option<&PublicIpResolverResult>,
        resolver_error: Option<String>,
    ) {
        let diagnostic_key = format!(
            "{}|{}|{}|{}|{}|{}|{:?}|{}",
            level,
            status,
            online_wait_status,
            raw_ip_addrs.join(","),
            published_direct_addrs.join(","),
            relay_urls.join(","),
            public_ip,
            resolver_error.as_deref().unwrap_or_default()
        );
        {
            let mut last_key = self
                .contact_material_diagnostic_key
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if last_key.as_deref() == Some(diagnostic_key.as_str()) {
                return;
            }
            *last_key = Some(diagnostic_key);
        }
        let details = json!({
            "publish_observed_direct_addrs": self.endpoint_options.publish_observed_direct_addrs,
            "bind_addr": self.endpoint_options.bind_addr.map(|addr| addr.to_string()),
            "online_wait_status": online_wait_status,
            "raw_ip_addrs": raw_ip_addrs,
            "published_direct_addrs": published_direct_addrs,
            "relay_urls": relay_urls,
            "public_ip_resolver": public_ip.map(|result| json!({
                "ip": result.ip.to_string(),
                "source": result.source.clone(),
            })),
            "resolver_error": resolver_error,
            "contains_global_direct_addr": published_direct_addrs.iter().any(|raw| {
                raw.parse::<SocketAddr>()
                    .is_ok_and(|addr| ip_addr_is_global(&addr.ip()))
            }),
        });
        let entry = json!({
            "id": format!("iroh-contact-material-{}", unix_timestamp_millis()),
            "timestamp_ms": unix_timestamp_millis(),
            "level": level,
            "component": "wattswarm.network_transport_iroh",
            "category": "transport",
            "phase": IROH_CONTACT_MATERIAL_EXPORT_PHASE,
            "status": status,
            "message": message,
            "object_kind": "peer",
            "object_id": self.adapter.network_peer_id(),
            "source_node_id": self.adapter.network_peer_id(),
            "details": details,
        });
        let append_result = append_iroh_contact_material_diagnostic(&self.state_dir, &entry);
        if let Err(error) = append_result {
            eprintln!("wattswarm iroh diagnostic append failed: {error:#}");
        }
    }

    fn export_contact_material(&self, generated_at: u64) -> Result<TransportContactMaterial> {
        let mut online_wait_status = "not_awaited";
        if self.endpoint_options.publish_observed_direct_addrs {
            online_wait_status = self.await_endpoint_online_once();
        }
        let addr = self.endpoint.addr();
        let raw_ip_addrs = addr
            .ip_addrs()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let public_ip = if self.endpoint_options.publish_observed_direct_addrs
            && self.endpoint_options.bind_addr.is_some()
            && !raw_ip_addrs.iter().any(|raw| {
                raw.parse::<SocketAddr>()
                    .is_ok_and(|addr| ip_addr_is_global(&addr.ip()))
            }) {
            self.resolve_public_ip_for_publish(online_wait_status)
        } else {
            None
        };
        let direct_addrs = self.endpoint_options.published_direct_addrs(
            raw_ip_addrs.clone(),
            public_ip.as_ref().map(|result| result.ip),
        );
        let relay_urls = self.endpoint_options.published_relay_urls(
            addr.relay_urls()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
        );
        let contains_global_direct_addr = direct_addrs.iter().any(|raw| {
            raw.parse::<SocketAddr>()
                .is_ok_and(|addr| ip_addr_is_global(&addr.ip()))
        });
        self.record_contact_material_export_diagnostic(
            if contains_global_direct_addr {
                "info"
            } else {
                "warn"
            },
            if contains_global_direct_addr {
                "published_global_direct_addr"
            } else {
                "no_global_direct_addr"
            },
            if contains_global_direct_addr {
                "iroh contact material published a global direct address"
            } else {
                "iroh contact material did not publish a global direct address"
            },
            online_wait_status,
            &raw_ip_addrs,
            &direct_addrs,
            &relay_urls,
            public_ip.as_ref(),
            None,
        );
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
            .fetch_lock
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
        let endpoint_addr = endpoint_addr_from_contact_material(remote)?;
        let request_bytes = serde_json::to_vec(request)?;
        if request_bytes.len() > MAX_CONTROL_REQUEST_BYTES {
            bail!("iroh control stream request exceeds max request bytes");
        }
        if !network_debug_diagnostics_enabled() {
            let result = self.block_on(async {
                match tokio::time::timeout(timeout, async {
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
                    serde_json::from_slice(&response_bytes)
                        .context("decode iroh control stream response")
                })
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(ControlStreamTimeout::request(timeout).into()),
                }
            });
            return self.finish_control_stream_request(result);
        }

        let contact_summary = control_stream_contact_summary(remote)?;
        let result = self.block_on(async {
            let deadline = tokio::time::Instant::now() + timeout;
            let connection =
                await_control_stream_stage(deadline, "connect", timeout, &contact_summary, async {
                    self.endpoint
                        .connect(endpoint_addr, DEFAULT_IROH_CONTROL_ALPN.as_bytes())
                        .await
                        .context("connect iroh control stream endpoint")
                })
                .await?;
            let (mut send, mut recv) =
                await_control_stream_stage(deadline, "open_bi", timeout, &contact_summary, async {
                    connection
                        .open_bi()
                        .await
                        .context("open iroh control stream")
                })
                .await?;
            await_control_stream_stage(deadline, "write", timeout, &contact_summary, async {
                send.write_all(&request_bytes)
                    .await
                    .context("write iroh control stream request")?;
                send.finish().context("finish control stream request")
            })
            .await?;
            let response_bytes =
                await_control_stream_stage(deadline, "read", timeout, &contact_summary, async {
                    recv.read_to_end(MAX_CONTROL_RESPONSE_BYTES)
                        .await
                        .context("read iroh control stream response")
                })
                .await?;
            serde_json::from_slice(&response_bytes).with_context(|| {
                format!("decode iroh control stream response stage=read contact={contact_summary}")
            })
        });
        self.finish_control_stream_request(result)
    }

    fn finish_control_stream_request(
        &self,
        result: Result<IrohControlStreamResponse>,
    ) -> Result<IrohControlStreamResponse> {
        let timed_out = result.as_ref().err().is_some_and(|error| {
            error
                .chain()
                .find_map(|cause| cause.downcast_ref::<ControlStreamTimeout>())
                .is_some_and(ControlStreamTimeout::triggers_network_recovery)
        });
        let notify_network_change = self
            .control_stream_recovery
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .record_outcome(timed_out, Instant::now());
        if notify_network_change {
            self.block_on(self.endpoint.network_change());
        }
        result
    }

    fn probe_route(
        &self,
        remote: &TransportContactMaterial,
        hold: Duration,
        timeout: Duration,
    ) -> Result<IrohRouteProbeResult> {
        let endpoint_addr = endpoint_addr_from_contact_material(remote)?;
        let connection = self.block_on(async {
            tokio::time::timeout(timeout, async {
                self.endpoint
                    .connect(endpoint_addr, DEFAULT_IROH_CONTROL_ALPN.as_bytes())
                    .await
                    .context("connect iroh route probe endpoint")
            })
            .await
            .with_context(|| {
                format!("iroh route probe timed out after {}ms", timeout.as_millis())
            })?
        })?;
        if !hold.is_zero() {
            std::thread::sleep(hold);
        }
        Ok(iroh_route_probe_result(
            remote,
            &connection,
            "probe",
            "in_process",
        ))
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
    Ok(EndpointAddr::from_parts(endpoint_id, addrs))
}

fn network_debug_diagnostics_enabled() -> bool {
    std::env::var(ENV_NETWORK_DEBUG_DIAGNOSTICS)
        .ok()
        .is_some_and(|value| network_debug_diagnostics_value_enabled(&value))
}

fn network_debug_diagnostics_value_enabled(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "on" | "ON")
}

fn control_stream_contact_summary(contact: &TransportContactMaterial) -> Result<String> {
    let extra: IrohTransportMaterial =
        serde_json::from_value(contact.extra.clone()).context("decode iroh contact material")?;
    serde_json::to_string(&json!({
        "peer_id": contact.peer_id,
        "endpoint_id": extra.endpoint_id,
        "transport": contact.transport,
        "generated_at": contact.metadata.generated_at,
        "relay_urls": extra.relay_urls,
        "direct_addr_count": extra.direct_addrs.len(),
    }))
    .context("encode iroh control stream contact summary")
}

async fn await_control_stream_stage<T>(
    deadline: tokio::time::Instant,
    stage: &'static str,
    timeout: Duration,
    contact_summary: &str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout_at(deadline, future).await {
        Ok(result) => result.with_context(|| {
            format!("iroh control stream failed stage={stage} contact={contact_summary}")
        }),
        Err(_) => Err(ControlStreamTimeout::stage(stage, timeout, contact_summary).into()),
    }
}

#[derive(Debug)]
struct ControlStreamTimeout {
    stage: Option<&'static str>,
    timeout: Duration,
    contact_summary: Option<String>,
}

impl ControlStreamTimeout {
    fn request(timeout: Duration) -> Self {
        Self {
            stage: None,
            timeout,
            contact_summary: None,
        }
    }

    fn stage(stage: &'static str, timeout: Duration, contact_summary: &str) -> Self {
        Self {
            stage: Some(stage),
            timeout,
            contact_summary: Some(contact_summary.to_owned()),
        }
    }

    fn triggers_network_recovery(&self) -> bool {
        self.stage.is_none_or(|stage| stage == "connect")
    }
}

impl std::fmt::Display for ControlStreamTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.stage, self.contact_summary.as_deref()) {
            (Some(stage), Some(contact_summary)) => write!(
                formatter,
                "iroh control stream request timed out stage={stage} timeout_ms={} contact={contact_summary}",
                self.timeout.as_millis()
            ),
            _ => write!(
                formatter,
                "iroh control stream request timed out after {}ms",
                self.timeout.as_millis()
            ),
        }
    }
}

impl std::error::Error for ControlStreamTimeout {}

fn iroh_route_probe_result(
    remote: &TransportContactMaterial,
    connection: &Connection,
    stage: &str,
    runtime: &str,
) -> IrohRouteProbeResult {
    let connection_info = connection.to_info();
    let selected_path = connection_info.selected_path();
    let selected_route = selected_path
        .as_ref()
        .map(|path| {
            if path.is_ip() {
                "direct"
            } else if path.is_relay() {
                "relay"
            } else {
                "custom"
            }
        })
        .unwrap_or("unknown");
    let path_count = connection_info.paths().into_iter().count();
    IrohRouteProbeResult {
        peer_id: remote.peer_id.clone(),
        endpoint_id: remote
            .metadata
            .endpoint_id
            .clone()
            .unwrap_or_else(|| remote.peer_id.clone()),
        selected_route: selected_route.to_owned(),
        path_count,
        stage: stage.to_owned(),
        runtime: runtime.to_owned(),
        direct_capable: selected_route == "direct",
        fallback_reason: (selected_route != "direct")
            .then(|| format!("iroh selected {selected_route} route")),
    }
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

pub fn probe_iroh_route_endpoint_id_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    endpoint_target: &str,
    hold: Duration,
    timeout: Duration,
) -> Result<IrohRouteProbeResult> {
    let (endpoint_id, direct_addr) = parse_iroh_probe_target(endpoint_target)?;
    let direct_addrs = direct_addr
        .into_iter()
        .map(|addr| addr.to_string())
        .collect();
    let remote_contact =
        iroh_probe_contact_material(endpoint_id.as_str(), direct_addrs, Vec::new());
    probe_iroh_route_for_network_peer_id(state_dir, network_peer_id, &remote_contact, hold, timeout)
}

pub fn probe_iroh_route_for_network_peer_id(
    state_dir: &Path,
    network_peer_id: &str,
    remote_contact: &TransportContactMaterial,
    hold: Duration,
    timeout: Duration,
) -> Result<IrohRouteProbeResult> {
    ensure_local_iroh_data_plane_for_network_peer_id(state_dir, network_peer_id)?.probe_route(
        remote_contact,
        hold,
        timeout,
    )
}

fn parse_iroh_probe_target(raw: &str) -> Result<(String, Option<SocketAddr>)> {
    let trimmed = raw.trim();
    let (endpoint_id, direct_addr) = trimmed
        .split_once('@')
        .map_or((trimmed, None), |(endpoint_id, direct_addr)| {
            (endpoint_id.trim(), Some(direct_addr.trim()))
        });
    EndpointId::from_str(endpoint_id).context("parse iroh probe endpoint id")?;
    let direct_addr = direct_addr
        .map(|addr| {
            if addr.is_empty() {
                bail!("iroh probe direct addr is empty");
            }
            addr.parse::<SocketAddr>()
                .with_context(|| format!("parse iroh probe direct addr {addr}"))
        })
        .transpose()?;
    Ok((endpoint_id.to_owned(), direct_addr))
}

fn iroh_probe_contact_material(
    endpoint_id: &str,
    direct_addrs: Vec<String>,
    relay_urls: Vec<String>,
) -> TransportContactMaterial {
    TransportContactMaterial {
        transport: TransportRoute::IrohDirect.as_str().to_owned(),
        peer_id: endpoint_id.to_owned(),
        metadata: TransportMetadata {
            route: TransportRoute::IrohDirect,
            generated_at: unix_timestamp_secs(),
            endpoint_id: Some(endpoint_id.to_owned()),
            alpn: Some(DEFAULT_IROH_CONTROL_ALPN.to_owned()),
            listen_addrs: Vec::new(),
            capabilities: PeerTransportCapabilities::iroh_direct_default(),
        },
        extra: json!(IrohTransportMaterial {
            endpoint_id: endpoint_id.to_owned(),
            alpn: DEFAULT_IROH_CONTROL_ALPN.to_owned(),
            direct_addrs,
            relay_urls,
        }),
    }
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

/// Current home relay URLs observed on the local iroh endpoint.
///
/// Returns an empty list when no data plane is running for `state_dir` or the
/// endpoint has not attached to a relay yet. Read-only: never creates a data
/// plane.
pub fn local_iroh_home_relay_urls(state_dir: &Path) -> Vec<String> {
    let service = {
        let services = local_iroh_data_planes()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        services.get(state_dir).cloned()
    };
    let Some(service) = service else {
        return Vec::new();
    };
    service
        .endpoint
        .addr()
        .relay_urls()
        .map(normalize_public_relay_url)
        .collect()
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

    fn write_test_relay_urls(dir: &Path) {
        fs::write(
            dir.join("startup_config.json"),
            r#"{"relay_urls":["https://relay.example.invalid/"]}"#,
        )
        .expect("write startup config");
    }

    fn contact_has_direct_addr(contact: &TransportContactMaterial) -> bool {
        contact
            .extra
            .get("direct_addrs")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty())
    }

    #[test]
    fn control_stream_timeout_reports_stage_and_contact_summary() {
        let adapter = IrohTransportAdapter::from_seed_bytes([7_u8; 32]).expect("adapter");
        let contact = adapter.material_payload(
            &["203.0.113.7:4002".to_owned()],
            &[
                "https://relay.wattetheria.com".to_owned(),
                "https://relay2.wattetheria.com".to_owned(),
            ],
            42,
        );
        let summary = control_stream_contact_summary(&contact).expect("contact summary");
        let summary_json: serde_json::Value = serde_json::from_str(&summary).expect("summary json");
        assert_eq!(summary_json["peer_id"], contact.peer_id);
        assert_eq!(summary_json["endpoint_id"], contact.peer_id);
        assert_eq!(summary_json["generated_at"], 42);
        assert_eq!(summary_json["direct_addr_count"], 1);
        assert_eq!(
            summary_json["relay_urls"],
            json!([
                "https://relay.wattetheria.com",
                "https://relay2.wattetheria.com"
            ])
        );

        let runtime = RuntimeBuilder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        for stage in ["connect", "open_bi", "write", "read"] {
            let error = runtime
                .block_on(await_control_stream_stage(
                    tokio::time::Instant::now() + Duration::from_millis(1),
                    stage,
                    Duration::from_millis(30_000),
                    &summary,
                    std::future::pending::<Result<()>>(),
                ))
                .expect_err("stage must time out");
            let error = error.to_string();
            assert!(error.contains(&format!("stage={stage}")), "{error}");
            assert!(error.contains("timeout_ms=30000"), "{error}");
            assert!(error.contains("https://relay2.wattetheria.com"), "{error}");
            assert!(error.contains(&contact.peer_id), "{error}");
        }
    }

    #[test]
    fn control_stream_diagnostics_env_values_match_network_debug_contract() {
        for value in ["1", "true", "TRUE", "yes", "on", "ON"] {
            assert!(
                network_debug_diagnostics_value_enabled(value),
                "{value} must enable diagnostics"
            );
        }
        for value in ["", "0", "false", "FALSE", "no", "off", "debug"] {
            assert!(
                !network_debug_diagnostics_value_enabled(value),
                "{value} must not enable diagnostics"
            );
        }
    }

    #[test]
    fn control_stream_recovery_is_bounded_and_resets_after_success() {
        let start = Instant::now();
        let mut state = ControlStreamRecoveryState::default();

        assert!(!state.record_outcome(true, start));
        assert!(!state.record_outcome(true, start + Duration::from_secs(1)));
        assert!(state.record_outcome(true, start + Duration::from_secs(2)));

        assert!(!state.record_outcome(true, start + Duration::from_secs(3)));
        assert!(!state.record_outcome(true, start + Duration::from_secs(4)));
        assert!(!state.record_outcome(true, start + Duration::from_secs(5)));
        assert!(!state.record_outcome(false, start + Duration::from_secs(6)));

        assert!(!state.record_outcome(true, start + CONTROL_STREAM_RECOVERY_COOLDOWN));
        assert!(!state.record_outcome(
            true,
            start + CONTROL_STREAM_RECOVERY_COOLDOWN + Duration::from_secs(1)
        ));
        assert!(state.record_outcome(
            true,
            start + CONTROL_STREAM_RECOVERY_COOLDOWN + Duration::from_secs(2)
        ));
        assert!(ControlStreamTimeout::request(Duration::from_secs(1)).triggers_network_recovery());
        assert!(
            ControlStreamTimeout::stage("connect", Duration::from_secs(1), "{}")
                .triggers_network_recovery()
        );
        assert!(
            !ControlStreamTimeout::stage("read", Duration::from_secs(1), "{}")
                .triggers_network_recovery()
        );
    }

    #[test]
    fn iroh_probe_target_accepts_endpoint_id_with_direct_addr() {
        let (endpoint_id, direct_addr) = parse_iroh_probe_target(
            "83393ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956@18.206.214.6:4002",
        )
        .expect("probe target");

        assert_eq!(
            endpoint_id,
            "83393ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956"
        );
        assert_eq!(
            direct_addr,
            Some("18.206.214.6:4002".parse().expect("socket addr"))
        );
    }

    #[test]
    fn iroh_probe_contact_material_is_direct_only() {
        let contact = iroh_probe_contact_material(
            "83393ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956",
            vec!["18.206.214.6:4002".to_owned()],
            Vec::new(),
        );
        let extra: IrohTransportMaterial =
            serde_json::from_value(contact.extra).expect("iroh material");

        assert_eq!(extra.direct_addrs, vec!["18.206.214.6:4002"]);
        assert!(extra.relay_urls.is_empty());
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

        assert_eq!(options.relay_urls.len(), 1);
        assert_eq!(
            normalize_public_relay_url(&options.relay_urls[0]),
            "https://relay.wattetheria.com"
        );
        assert_eq!(
            options.published_direct_addrs(vec!["10.0.0.1:1234".to_owned()], None),
            Vec::<String>::new()
        );
        assert!(options.published_direct_addrs.is_empty());
    }

    #[test]
    fn published_relay_urls_use_only_the_observed_home_relay() {
        let options = IrohEndpointOptions::from_raw_env(
            Some("https://relay.wattetheria.com,https://relay2.wattetheria.com"),
            None,
            Some("false"),
        )
        .expect("endpoint options");

        assert_eq!(
            options.published_relay_urls(vec![
                "https://relay2.wattetheria.com/".to_owned(),
                "https://relay2.wattetheria.com".to_owned(),
                "https://relay.wattetheria.com".to_owned(),
            ]),
            vec!["https://relay2.wattetheria.com".to_owned()]
        );
        assert!(options.published_relay_urls(Vec::new()).is_empty());
        assert_eq!(options.relay_urls.len(), 2);
    }

    #[test]
    fn local_iroh_home_relay_urls_is_empty_without_data_plane() {
        let dir = tempdir().expect("tempdir");
        assert!(local_iroh_home_relay_urls(dir.path()).is_empty());
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
        assert_eq!(options.relay_urls.len(), 1);
        assert_eq!(
            normalize_public_relay_url(&options.relay_urls[0]),
            "https://relay2.wattetheria.com"
        );
    }

    #[test]
    fn iroh_endpoint_options_hide_direct_addrs_by_default() {
        let options =
            IrohEndpointOptions::from_raw_env(None, None, None).expect("endpoint options");

        assert_eq!(
            options.published_direct_addrs(vec!["10.0.0.1:1234".to_owned()], None),
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
        assert_eq!(
            options.published_direct_addrs(vec!["127.0.0.1:4002".to_owned()], None),
            Vec::<String>::new()
        );
    }

    #[test]
    fn observed_publish_rewrites_global_addr_port_to_bind_port() {
        assert_eq!(
            observed_direct_addrs_for_publish(
                vec![
                    "172.19.0.3:4002".to_owned(),
                    "18.206.214.6:54321".to_owned()
                ],
                Some(4002),
            ),
            vec!["172.19.0.3:4002".to_owned(), "18.206.214.6:4002".to_owned()]
        );
    }

    #[test]
    fn observed_publish_keeps_ports_without_bind_port() {
        assert_eq!(
            observed_direct_addrs_for_publish(
                vec![
                    "172.19.0.3:4002".to_owned(),
                    "18.206.214.6:54321".to_owned()
                ],
                None,
            ),
            vec![
                "172.19.0.3:4002".to_owned(),
                "18.206.214.6:54321".to_owned()
            ]
        );
    }

    #[test]
    fn observed_publish_dedupes_rewritten_global_addrs() {
        assert_eq!(
            observed_direct_addrs_for_publish(
                vec![
                    "18.206.214.6:54321".to_owned(),
                    "18.206.214.6:54999".to_owned()
                ],
                Some(4002),
            ),
            vec!["18.206.214.6:4002".to_owned()]
        );
    }

    #[test]
    fn observed_publish_never_rewrites_non_global_ranges() {
        let non_global = vec![
            "0.1.2.3:5000".to_owned(),
            "127.0.0.1:5000".to_owned(),
            "10.1.2.3:5000".to_owned(),
            "192.168.1.20:5000".to_owned(),
            "169.254.1.1:5000".to_owned(),
            "100.64.0.1:5000".to_owned(),
            "192.0.0.8:5000".to_owned(),
            "192.88.99.1:5000".to_owned(),
            "198.18.0.1:5000".to_owned(),
            "198.19.255.255:5000".to_owned(),
            "224.0.0.1:5000".to_owned(),
            "240.0.0.1:5000".to_owned(),
            "[fc00::1]:5000".to_owned(),
            "[fe80::1]:5000".to_owned(),
            "[::1]:5000".to_owned(),
            "[2001:db8::1]:5000".to_owned(),
            "[2001:2::1]:5000".to_owned(),
            "[2001:20::1]:5000".to_owned(),
            "[4000::1]:5000".to_owned(),
            "[ff02::1]:5000".to_owned(),
        ];

        assert_eq!(
            observed_direct_addrs_for_publish(non_global.clone(), Some(4002)),
            non_global
        );
    }

    #[test]
    fn observed_publish_rewrites_global_ipv6_addr() {
        assert_eq!(
            observed_direct_addrs_for_publish(vec!["[2600::1]:53210".to_owned()], Some(4002)),
            vec!["[2600::1]:4002".to_owned()]
        );
    }

    #[test]
    fn observed_publish_adds_public_ip_fallback_when_observed_addrs_are_not_global() {
        assert_eq!(
            observed_direct_addrs_for_publish_with_public_ip_fallback(
                vec!["172.19.0.3:4002".to_owned(), "172.21.0.4:4002".to_owned()],
                Some("0.0.0.0:4002".parse().expect("bind addr")),
                Some("60.242.29.210".parse().expect("public ip")),
            ),
            vec!["60.242.29.210:4002".to_owned()]
        );
    }

    #[test]
    fn observed_publish_does_not_add_public_ip_fallback_when_global_observed_addr_exists() {
        assert_eq!(
            observed_direct_addrs_for_publish_with_public_ip_fallback(
                vec![
                    "172.19.0.3:4002".to_owned(),
                    "18.206.214.6:54321".to_owned()
                ],
                Some("0.0.0.0:4002".parse().expect("bind addr")),
                Some("60.242.29.210".parse().expect("public ip")),
            ),
            vec!["18.206.214.6:4002".to_owned()]
        );
    }

    #[test]
    fn observed_publish_allows_loopback_when_bound_to_loopback() {
        assert_eq!(
            observed_direct_addrs_for_publish_with_public_ip_fallback(
                vec!["127.0.0.1:54321".to_owned()],
                Some("127.0.0.1:0".parse().expect("bind addr")),
                Some("60.242.29.210".parse().expect("public ip")),
            ),
            vec!["127.0.0.1:54321".to_owned()]
        );
    }

    #[test]
    fn observed_publish_ignores_public_ip_fallback_without_bind_port() {
        assert_eq!(
            observed_direct_addrs_for_publish_with_public_ip_fallback(
                vec!["172.19.0.3:4002".to_owned()],
                None,
                Some("60.242.29.210".parse().expect("public ip")),
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn observed_publish_ignores_public_ip_fallback_with_ephemeral_bind_port() {
        assert_eq!(
            observed_direct_addrs_for_publish_with_public_ip_fallback(
                vec!["172.19.0.3:4002".to_owned()],
                Some("127.0.0.1:0".parse().expect("bind addr")),
                Some("60.242.29.210".parse().expect("public ip")),
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn parse_public_ip_response_accepts_raw_and_cdn_cgi_trace_formats() {
        assert_eq!(
            parse_public_ip_response("60.242.29.210\n"),
            Some("60.242.29.210".parse().expect("public ip"))
        );
        assert_eq!(
            parse_public_ip_response("fl=abc\nip=60.242.29.210\nuag=curl\n"),
            Some("60.242.29.210".parse().expect("public ip"))
        );
        assert_eq!(parse_public_ip_response("ip=172.19.0.3\n"), None);
    }

    #[test]
    fn append_iroh_contact_material_diagnostic_writes_jsonl_entry() {
        let dir = tempdir().expect("tempdir");
        let entry = json!({
            "id": "diag-1",
            "timestamp_ms": 1,
            "level": "info",
            "component": "wattswarm.network_transport_iroh",
            "category": "transport",
            "phase": IROH_CONTACT_MATERIAL_EXPORT_PHASE,
            "status": "published_global_direct_addr",
            "message": "test",
            "details": {
                "published_direct_addrs": ["60.242.29.210:4002"]
            }
        });

        append_iroh_contact_material_diagnostic(dir.path(), &entry).expect("append diagnostic");

        let raw = fs::read_to_string(dir.path().join(DIAGNOSTIC_LOG_RELATIVE_PATH))
            .expect("diagnostic log");
        let saved: serde_json::Value = serde_json::from_str(raw.trim()).expect("diagnostic json");
        assert_eq!(saved["phase"], IROH_CONTACT_MATERIAL_EXPORT_PHASE);
        assert_eq!(
            saved["details"]["published_direct_addrs"][0],
            "60.242.29.210:4002"
        );
    }

    #[test]
    fn iroh_endpoint_options_true_mode_republishes_global_addr_on_bind_port() {
        let options = IrohEndpointOptions::from_raw_env(None, Some("0.0.0.0:4002"), Some("true"))
            .expect("endpoint options");

        assert_eq!(
            options.published_direct_addrs(
                vec![
                    "172.19.0.3:4002".to_owned(),
                    "18.206.214.6:54321".to_owned(),
                ],
                None
            ),
            vec!["18.206.214.6:4002".to_owned()]
        );
    }

    #[test]
    fn iroh_endpoint_options_true_mode_uses_public_ip_fallback() {
        let options = IrohEndpointOptions::from_raw_env(None, Some("0.0.0.0:4002"), Some("true"))
            .expect("endpoint options");

        assert_eq!(
            options.published_direct_addrs(
                vec!["172.19.0.3:4002".to_owned(), "172.21.0.4:4002".to_owned()],
                Some("60.242.29.210".parse().expect("public ip")),
            ),
            vec!["60.242.29.210:4002".to_owned()]
        );
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
            options.published_direct_addrs(vec!["172.20.0.4:4002".to_owned()], None),
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
    fn endpoint_only_contact_material_can_be_dial_target() {
        let adapter = IrohTransportAdapter::from_seed_bytes([7u8; 32]).expect("adapter");
        let material = adapter.material_payload(&[], &[], 42);

        let endpoint_addr =
            endpoint_addr_from_contact_material(&material).expect("endpoint-only endpoint addr");
        assert_eq!(endpoint_addr.id, adapter.endpoint_id());
        assert!(endpoint_addr.addrs.is_empty());
    }

    #[test]
    fn exports_contact_material_with_iroh_endpoint_identity() {
        let dir = tempdir().expect("tempdir");
        seed_state_dir(dir.path(), [11u8; 32]);
        write_test_relay_urls(dir.path());
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
        write_test_relay_urls(dir.path());
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
        write_test_relay_urls(dir_a.path());
        write_test_relay_urls(dir_b.path());
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        if !contact_has_direct_addr(&contact_b) {
            shutdown_local_iroh_data_plane(dir_a.path());
            shutdown_local_iroh_data_plane(dir_b.path());
            return;
        }
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
        write_test_relay_urls(dir_a.path());
        write_test_relay_urls(dir_b.path());
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let contact_a = export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        if !contact_has_direct_addr(&contact_a) || !contact_has_direct_addr(&contact_b) {
            shutdown_local_iroh_data_plane(dir_a.path());
            shutdown_local_iroh_data_plane(dir_b.path());
            return;
        }
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
        write_test_relay_urls(dir_a.path());
        write_test_relay_urls(dir_b.path());
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        if !contact_has_direct_addr(&contact_b) {
            shutdown_local_iroh_data_plane(dir_a.path());
            shutdown_local_iroh_data_plane(dir_b.path());
            return;
        }
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
    fn slow_control_request_does_not_serialize_other_requests() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [33u8; 32]);
        seed_state_dir(dir_b.path(), [34u8; 32]);
        write_test_relay_urls(dir_a.path());
        write_test_relay_urls(dir_b.path());
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        if !contact_has_direct_addr(&contact_b) {
            shutdown_local_iroh_data_plane(dir_a.path());
            shutdown_local_iroh_data_plane(dir_b.path());
            return;
        }
        let (slow_entered_tx, slow_entered_rx) = std::sync::mpsc::channel();
        set_local_control_stream_handler_for_network_peer_id(
            dir_b.path(),
            &peer_b,
            "parallel.v1",
            Some(
                move |_remote_peer_id: String, request: IrohControlStreamRequest| {
                    if request.payload == b"slow" {
                        let _ = slow_entered_tx.send(());
                        std::thread::sleep(Duration::from_millis(750));
                    }
                    IrohControlStreamResponse {
                        ok: true,
                        error: None,
                        payload: request.payload,
                    }
                },
            ),
        )
        .expect("set control handler");

        let state_dir = dir_a.path().to_path_buf();
        let slow_peer = peer_a.clone();
        let slow_contact = contact_b.clone();
        let slow = std::thread::spawn(move || {
            send_control_stream_request_for_network_peer_id(
                &state_dir,
                &slow_peer,
                &slow_contact,
                &IrohControlStreamRequest {
                    kind: "parallel.v1".to_owned(),
                    payload: b"slow".to_vec(),
                },
            )
        });
        slow_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("slow request entered handler");

        let state_dir = dir_a.path().to_path_buf();
        let fast_peer = peer_a.clone();
        let fast_contact = contact_b.clone();
        let (fast_done_tx, fast_done_rx) = std::sync::mpsc::channel();
        let fast = std::thread::spawn(move || {
            let response = send_control_stream_request_for_network_peer_id(
                &state_dir,
                &fast_peer,
                &fast_contact,
                &IrohControlStreamRequest {
                    kind: "parallel.v1".to_owned(),
                    payload: b"fast".to_vec(),
                },
            );
            let _ = fast_done_tx.send(());
            response
        });

        fast_done_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("fast request must complete while slow request is active");
        assert_eq!(
            fast.join()
                .expect("fast thread")
                .expect("fast response")
                .payload,
            b"fast"
        );
        assert_eq!(
            slow.join()
                .expect("slow thread")
                .expect("slow response")
                .payload,
            b"slow"
        );

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn two_nodes_request_contact_material_over_iroh_control_stream() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [41u8; 32]);
        seed_state_dir(dir_b.path(), [42u8; 32]);
        write_test_relay_urls(dir_a.path());
        write_test_relay_urls(dir_b.path());
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let bootstrap_contact_b =
            export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
                .expect("bootstrap contact b");
        if !contact_has_direct_addr(&bootstrap_contact_b) {
            shutdown_local_iroh_data_plane(dir_a.path());
            shutdown_local_iroh_data_plane(dir_b.path());
            return;
        }
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
        write_test_relay_urls(local_dir.path());
        write_test_relay_urls(remote_dir.path());

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
        if !contact_has_direct_addr(&remote_contact) {
            shutdown_local_iroh_data_plane(local_dir.path());
            shutdown_local_iroh_data_plane(remote_dir.path());
            return;
        }
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
