use anyhow::{Context, Result, anyhow, bail};
use hex::decode;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr, endpoint::presets};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime, RuntimeFlavor};
use wattswarm_artifact_store::{ArtifactKind, ArtifactStore};
use wattswarm_network_transport_core::{
    DirectDataFetchRequest, DirectDataFetchResponse, DirectDataTransportAdapter,
    PeerTransportCapabilities, TransportContactMaterial, TransportError, TransportMetadata,
    TransportRoute,
};

pub const DEFAULT_IROH_ALPN: &str = "/wattswarm/iroh/1";
const MAX_FETCH_REQUEST_BYTES: usize = 64 * 1024;
const MAX_FETCH_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

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
    local_peer_id: PeerId,
    endpoint_id: EndpointId,
    config: IrohTransportConfig,
    capabilities: PeerTransportCapabilities,
}

impl IrohTransportAdapter {
    pub fn from_local_peer_id(peer_id: &PeerId) -> Result<Self> {
        let endpoint_id = peer_id_to_endpoint_id(peer_id)
            .ok_or_else(|| anyhow!("peer id is not ed25519-backed"))?;
        Ok(Self {
            local_peer_id: *peer_id,
            endpoint_id,
            config: IrohTransportConfig::default(),
            capabilities: PeerTransportCapabilities::iroh_direct_default(),
        })
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
            peer_id: self.local_peer_id.to_string(),
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

#[derive(Debug)]
struct IrohDataPlaneService {
    state_dir: PathBuf,
    runtime: Runtime,
    endpoint: Endpoint,
    adapter: IrohTransportAdapter,
    op_lock: Mutex<()>,
}

static LOCAL_IROH_DATA_PLANES: OnceLock<Mutex<HashMap<PathBuf, Arc<IrohDataPlaneService>>>> =
    OnceLock::new();

fn local_iroh_data_planes() -> &'static Mutex<HashMap<PathBuf, Arc<IrohDataPlaneService>>> {
    LOCAL_IROH_DATA_PLANES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn artifact_store_path(state_dir: &Path) -> PathBuf {
    state_dir.join("artifacts")
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

async fn handle_incoming_request(state_dir: PathBuf, connection: iroh::endpoint::Connection) {
    let response = match async {
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
    {
        Ok(()) => None,
        Err(err) => Some(err),
    };
    if let Some(err) = response {
        eprintln!(
            "iroh data fetch handler failed for {}: {err}",
            state_dir.display()
        );
    }
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
    fn new(state_dir: &Path, local_peer_id: &PeerId) -> Result<Arc<Self>> {
        let adapter = IrohTransportAdapter::from_local_peer_id(local_peer_id)?;
        let secret_key = load_secret_key_from_state_dir(state_dir)?;
        let runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let endpoint_future = || async {
            Endpoint::builder(presets::N0)
                .secret_key(secret_key)
                .alpns(vec![adapter.config.alpn.as_bytes().to_vec()])
                .bind()
                .await
        };
        let endpoint = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| runtime.block_on(endpoint_future()))?
            }
            Ok(_) => std::thread::scope(|scope| {
                scope
                    .spawn(|| runtime.block_on(endpoint_future()))
                    .join()
                    .expect("join iroh endpoint init thread")
            })?,
            Err(_) => runtime.block_on(endpoint_future())?,
        };
        let service = Arc::new(Self {
            state_dir: state_dir.to_path_buf(),
            runtime,
            endpoint,
            adapter,
            op_lock: Mutex::new(()),
        });
        service.spawn_accept_loop();
        Ok(service)
    }

    fn spawn_accept_loop(self: &Arc<Self>) {
        let endpoint = self.endpoint.clone();
        let state_dir = self.state_dir.clone();
        self.runtime.spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let state_dir = state_dir.clone();
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(connection) => handle_incoming_request(state_dir, connection).await,
                        Err(err) => eprintln!("iroh accept connection failed: {err}"),
                    }
                });
            }
        });
    }

    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn export_contact_material(&self, generated_at: u64) -> Result<TransportContactMaterial> {
        let addr = self.endpoint.addr();
        let direct_addrs = addr
            .ip_addrs()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let relay_urls = addr
            .relay_urls()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
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

    fn shutdown(&self) {
        self.block_on(self.endpoint.close());
    }
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

fn ensure_local_iroh_data_plane(
    state_dir: &Path,
    local_peer_id: &PeerId,
) -> Result<Arc<IrohDataPlaneService>> {
    let mut services = local_iroh_data_planes()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = services.get(state_dir) {
        if existing.adapter.local_peer_id != *local_peer_id {
            bail!(
                "iroh data plane already exists for {} with a different local peer id",
                state_dir.display()
            );
        }
        return Ok(existing.clone());
    }
    let service = IrohDataPlaneService::new(state_dir, local_peer_id)?;
    services.insert(state_dir.to_path_buf(), service.clone());
    Ok(service)
}

pub fn export_local_contact_material(
    state_dir: &Path,
    local_peer_id: &PeerId,
    generated_at: u64,
) -> Result<TransportContactMaterial> {
    ensure_local_iroh_data_plane(state_dir, local_peer_id)?.export_contact_material(generated_at)
}

pub fn fetch_direct_data(
    state_dir: &Path,
    local_peer_id: &PeerId,
    remote_contact: &TransportContactMaterial,
    request: &DirectDataFetchRequest,
) -> Result<DirectDataFetchResponse> {
    ensure_local_iroh_data_plane(state_dir, local_peer_id)?.fetch(remote_contact, request)
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

pub fn peer_id_to_endpoint_id(peer_id: &PeerId) -> Option<EndpointId> {
    let bytes = peer_id.to_bytes();
    if bytes.len() != 38 {
        return None;
    }
    let byte_array = <[u8; 32]>::try_from(&bytes[6..]).ok()?;
    EndpointId::from_bytes(&byte_array).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;
    use tempfile::tempdir;

    fn seed_state_dir(dir: &Path, seed: [u8; 32]) {
        fs::write(dir.join("node_seed.hex"), hex::encode(seed)).expect("write node seed");
    }

    #[test]
    fn converts_ed25519_peer_id_to_iroh_endpoint_id() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let endpoint_id = peer_id_to_endpoint_id(&peer_id).expect("ed25519 peer id");
        assert_eq!(endpoint_id.as_bytes(), &peer_id.to_bytes()[6..]);
    }

    #[test]
    fn exports_iroh_contact_material_with_capabilities() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let adapter = IrohTransportAdapter::from_local_peer_id(&peer_id).expect("adapter");
        let material = adapter.material_payload(
            &["127.0.0.1:7777".to_owned()],
            &["https://relay.example".to_owned()],
            42,
        );
        assert_eq!(material.transport, "iroh_direct");
        assert_eq!(material.peer_id, peer_id.to_string());
        assert_eq!(material.metadata.generated_at, 42);
        assert!(material.metadata.capabilities.supports_iroh_direct);
        assert!(material.metadata.endpoint_id.is_some());
        let extra: IrohTransportMaterial =
            serde_json::from_value(material.extra.clone()).expect("extra");
        assert_eq!(extra.direct_addrs, vec!["127.0.0.1:7777".to_owned()]);
    }

    #[test]
    fn local_service_fetches_reference_bytes_over_iroh() {
        let local_dir = tempdir().expect("local tempdir");
        let remote_dir = tempdir().expect("remote tempdir");
        seed_state_dir(local_dir.path(), [7u8; 32]);
        seed_state_dir(remote_dir.path(), [9u8; 32]);

        let local_peer_id = {
            let secret = SecretKey::from_bytes(&[7u8; 32]);
            PeerId::from_public_key(&libp2p::identity::PublicKey::from(
                libp2p::identity::ed25519::PublicKey::try_from_bytes(secret.public().as_bytes())
                    .expect("ed25519 public key"),
            ))
        };
        let remote_peer_id = {
            let secret = SecretKey::from_bytes(&[9u8; 32]);
            PeerId::from_public_key(&libp2p::identity::PublicKey::from(
                libp2p::identity::ed25519::PublicKey::try_from_bytes(secret.public().as_bytes())
                    .expect("ed25519 public key"),
            ))
        };

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

        let remote_contact = export_local_contact_material(remote_dir.path(), &remote_peer_id, 11)
            .expect("remote contact material");
        let response = fetch_direct_data(
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
