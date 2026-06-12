use serde_json::json;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;
use wattswarm_control_plane::artifact_store::{
    ArtifactAvailabilityStatus, ArtifactKind, ArtifactStore,
};
use wattswarm_control_plane::control::{
    DataSourceBindingKind, DataSourceBindingRecord, DiscoveredPeerRecord, ExecutorRegistry,
    ExecutorRegistryEntry, PeerMetadataRecord, RealTaskRunRequest, RemoteTaskBridgeRegistry,
    RemoteTaskBridgeRequest, add_discovered_peer, add_discovered_peer_endpoint,
    artifact_store_path, bridge_remote_task_into_local_execution, configured_node_mode,
    discovered_peers_path, executor_registry_path, fetch_checkpoint_artifact_json,
    fetch_evidence_artifact, fetch_snapshot_artifact_json, fetch_task_detail_artifact,
    list_artifacts_needing_repair, load_discovered_peer_records,
    load_discovered_peer_records_state, load_discovered_peers, load_discovered_peers_state,
    load_executor_registry, load_executor_registry_state, load_network_directory_snapshot,
    local_node_id, local_peer_id, materialize_checkpoint_artifact_json,
    materialize_evidence_artifact, materialize_snapshot_artifact_json,
    materialize_task_detail_artifact, open_node, open_node_on_network_id,
    recommended_transfer_route_for_remote_node, run_real_task_flow,
    save_data_source_binding_record_state, save_discovered_peers, save_executor_registry,
    save_executor_registry_state, save_peer_metadata_record_state,
};
use wattswarm_control_plane::crypto::{NodeIdentity, sha256_hex};
use wattswarm_control_plane::network_bridge::maybe_start_background_network_service;
use wattswarm_control_plane::storage::{local_control_scope_id, storage::pg::Connection};
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::types::{
    Membership, NetworkBootstrapBundle, NetworkJoinManifest, NetworkKind, NetworkProtocolParams,
    Role, SignedNetworkProtocolParamsEnvelope, TaskAnnouncedPayload, UnsignedEvent,
};
use wattswarm_control_plane::udp_announce::{announce_startup, maybe_start_listener};
use wattswarm_network_transport_core::{TransferIntent, TransferKind, TransportRoute};
use wattswarm_network_transport_iroh::{
    export_local_contact_material_for_network_peer_id, shutdown_local_iroh_data_plane,
};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const TEST_DB_LOCK_KEY: i64 = 1_987_654_321;

#[path = "control_and_udp_tests/artifacts.rs"]
mod artifacts;
#[path = "control_and_udp_tests/discovery_registry.rs"]
mod discovery_registry;
#[path = "control_and_udp_tests/executor_registry.rs"]
mod executor_registry;
#[path = "control_and_udp_tests/network_bootstrap.rs"]
mod network_bootstrap;
#[path = "control_and_udp_tests/node_mode.rs"]
mod node_mode;
#[path = "control_and_udp_tests/node_open.rs"]
mod node_open;
#[path = "control_and_udp_tests/real_task_flow.rs"]
mod real_task_flow;
#[path = "control_and_udp_tests/remote_bridge.rs"]
mod remote_bridge;
#[path = "control_and_udp_tests/udp.rs"]
mod udp;

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("wattswarm-{prefix}-{}", Uuid::new_v4().simple()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn cleanup_dir(path: &Path) {
    shutdown_local_iroh_data_plane(path);
    let _ = fs::remove_dir_all(path);
}

fn load_transport_decisions(state_dir: &Path) -> Vec<serde_json::Value> {
    let path = state_dir.join("transport_decisions.jsonl");
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("valid transport decision json"))
        .collect()
}

fn candidate_output_ref(
    candidate_id: &str,
    output: &serde_json::Value,
    created_at: u64,
    producer: &str,
) -> wattswarm_control_plane::types::ArtifactRef {
    let bytes = serde_json::to_vec(output).expect("serialize candidate output");
    let digest = sha256_hex(&bytes);
    wattswarm_control_plane::types::ArtifactRef {
        uri: format!("artifact://reference/{candidate_id}"),
        digest: format!("sha256:{digest}"),
        size_bytes: bytes.len() as u64,
        mime: "application/json".to_owned(),
        created_at,
        producer: producer.to_owned(),
    }
}

fn test_iroh_endpoint_id(seed: u8) -> String {
    wattswarm_control_plane::network_p2p::network_peer_id_from_ed25519_public_key(
        NodeIdentity::from_seed([seed; 32])
            .verifying_key()
            .to_bytes(),
    )
    .expect("valid test iroh endpoint id")
    .to_string()
}

fn test_iroh_contact_material(
    endpoint_id: &str,
    generated_at: u64,
    listen_addrs: &[&str],
) -> serde_json::Value {
    json!({
        "peer_id": endpoint_id,
        "transports": [{
            "transport": "iroh_direct",
            "peer_id": endpoint_id,
            "metadata": {
                "route": "iroh_direct",
                "generated_at": generated_at,
                "endpoint_id": endpoint_id,
                "alpn": "/wattswarm/iroh/1",
                "listen_addrs": listen_addrs,
                "capabilities": {
                    "supports_iroh_direct": true,
                    "supports_streaming": true,
                    "max_recommended_inline_bytes": 16384,
                    "preferred_data_route": "iroh_direct"
                }
            },
            "extra": {
                "endpoint_id": endpoint_id,
                "alpn": "/wattswarm/iroh/1",
                "direct_addrs": listen_addrs,
                "relay_urls": []
            }
        }]
    })
}

fn save_iroh_peer_metadata(state_dir: &Path, node_id: &str, updated_at: u64) {
    let peer_id = test_iroh_endpoint_id(updated_at as u8);
    save_peer_metadata_record_state(
        state_dir,
        &PeerMetadataRecord {
            node_id: node_id.to_owned(),
            network_id: Some("wan".to_owned()),
            params_version: Some(7),
            params_hash: Some("params-iroh".to_owned()),
            agent_version_raw: Some("wattswarm-network-p2p|wan|7|params-iroh".to_owned()),
            agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
            protocol_version: Some("wattswarm/1.0.0".to_owned()),
            observed_addr: Some(format!("{peer_id}@127.0.0.1:4001")),
            listen_addrs: vec!["127.0.0.1:4001".to_owned()],
            protocols: vec![],
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(test_iroh_contact_material(
                &peer_id,
                updated_at,
                &["127.0.0.1:4001"],
            )),
            contact_material_signature: Some(format!("sig-{node_id}")),
            contact_material_updated_at: Some(updated_at),
            first_identified_at: updated_at,
            last_identified_at: updated_at,
        },
    )
    .expect("save iroh peer metadata");
}

fn save_real_iroh_peer_metadata(
    state_dir: &Path,
    node_id: &str,
    remote_state_dir: &Path,
    updated_at: u64,
) {
    let remote_peer_id = local_peer_id(remote_state_dir).expect("remote peer id");
    let transport = export_local_contact_material_for_network_peer_id(
        remote_state_dir,
        &remote_peer_id,
        updated_at,
    )
    .expect("export remote iroh contact material");
    save_peer_metadata_record_state(
        state_dir,
        &PeerMetadataRecord {
            node_id: node_id.to_owned(),
            network_id: Some("wan".to_owned()),
            params_version: Some(7),
            params_hash: Some("params-iroh".to_owned()),
            agent_version_raw: Some("wattswarm-network-p2p|wan|7|params-iroh".to_owned()),
            agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
            protocol_version: Some("wattswarm/1.0.0".to_owned()),
            observed_addr: transport.metadata.listen_addrs.first().cloned(),
            listen_addrs: transport.metadata.listen_addrs.clone(),
            protocols: vec![],
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(json!({
                "peer_id": transport.peer_id,
                "transports": [transport],
            })),
            contact_material_signature: Some(format!("sig-{node_id}")),
            contact_material_updated_at: Some(updated_at),
            first_identified_at: updated_at,
            last_identified_at: updated_at,
        },
    )
    .expect("save real iroh peer metadata");
}

fn with_udp_env(enabled: &str, mode: &str, addr: &str, port: u16) {
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        std::env::set_var("WATTSWARM_UDP_ANNOUNCE_ENABLED", enabled);
        std::env::set_var("WATTSWARM_UDP_ANNOUNCE_MODE", mode);
        std::env::set_var("WATTSWARM_UDP_ANNOUNCE_ADDR", addr);
        std::env::set_var("WATTSWARM_UDP_ANNOUNCE_PORT", port.to_string());
    }
}

fn clear_udp_env() {
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        std::env::remove_var("WATTSWARM_UDP_ANNOUNCE_ENABLED");
        std::env::remove_var("WATTSWARM_UDP_ANNOUNCE_MODE");
        std::env::remove_var("WATTSWARM_UDP_ANNOUNCE_ADDR");
        std::env::remove_var("WATTSWARM_UDP_ANNOUNCE_PORT");
    }
}

fn clear_node_mode_env() -> Option<String> {
    let prev = std::env::var("WATTSWARM_NODE_MODE").ok();
    // SAFETY: tests serialize env mutations via ENV_LOCK when needed.
    unsafe {
        std::env::remove_var("WATTSWARM_NODE_MODE");
    }
    prev
}

fn restore_node_mode_env(prev: Option<String>) {
    // SAFETY: tests serialize env mutations via ENV_LOCK when needed.
    unsafe {
        if let Some(value) = prev {
            std::env::set_var("WATTSWARM_NODE_MODE", value);
        } else {
            std::env::remove_var("WATTSWARM_NODE_MODE");
        }
    }
}

struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: tests serialize env mutations via ENV_LOCK when needed.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }

    fn remove(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: tests serialize env mutations via ENV_LOCK when needed.
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: tests serialize env mutations via ENV_LOCK when needed.
        unsafe {
            if let Some(prev) = &self.prev {
                std::env::set_var(self.key, prev);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

struct DbTestLock {
    conn: Connection,
}

impl DbTestLock {
    fn acquire() -> Self {
        let conn = Connection::open("control-db-lock").expect("open db lock connection");
        conn.query_row(
            "SELECT pg_advisory_lock($1)",
            wattswarm_storage_core::params![TEST_DB_LOCK_KEY],
            |_| Ok(()),
        )
        .expect("acquire advisory lock");
        Self { conn }
    }
}

impl Drop for DbTestLock {
    fn drop(&mut self) {
        let _ = self.conn.query_row(
            "SELECT pg_advisory_unlock($1)",
            wattswarm_storage_core::params![TEST_DB_LOCK_KEY],
            |_| Ok(()),
        );
    }
}

fn reset_test_schema(schema: &str) {
    let prev_schema = std::env::var("WATTSWARM_PG_SCHEMA").ok();
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        std::env::remove_var("WATTSWARM_PG_SCHEMA");
    }
    let conn = Connection::open("schema-reset").expect("open pg connection");
    conn.execute_batch(&format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE;
         CREATE SCHEMA {schema};"
    ))
    .expect("reset test schema");
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        if let Some(value) = prev_schema {
            std::env::set_var("WATTSWARM_PG_SCHEMA", value);
        } else {
            std::env::remove_var("WATTSWARM_PG_SCHEMA");
        }
    }
}

struct RuntimeStub {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

struct BootstrapBundleStub {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl BootstrapBundleStub {
    fn start(bundle: NetworkBootstrapBundle) -> Self {
        Self::start_with_manifest(bundle, None)
    }

    fn start_with_manifest(
        bundle: NetworkBootstrapBundle,
        manifest: Option<NetworkJoinManifest>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind bootstrap bundle stub");
        listener
            .set_nonblocking(true)
            .expect("set bootstrap bundle stub nonblocking");
        let addr = listener
            .local_addr()
            .expect("bootstrap bundle stub local addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let body = serde_json::json!({
            "ok": true,
            "bundle": bundle,
        })
        .to_string();
        let manifest_body = manifest.map(|manifest| serde_json::to_string(&manifest).unwrap());

        let handle = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_http_request(&mut stream);
                        if request.is_empty() {
                            continue;
                        }
                        let request = String::from_utf8_lossy(&request);
                        let line = request.lines().next().unwrap_or_default();
                        let (status, payload) = if line.starts_with("GET /api/network/bootstrap ") {
                            (200, body.clone())
                        } else if line.starts_with("GET /.well-known/wattswarm/join.json ") {
                            match &manifest_body {
                                Some(manifest_body) => (200, manifest_body.clone()),
                                None => (404, "{}".to_owned()),
                            }
                        } else {
                            (404, "{}".to_owned())
                        };
                        let response = format!(
                            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            status,
                            match status {
                                200 => "OK",
                                404 => "Not Found",
                                _ => "Status",
                            },
                            payload.len(),
                            payload
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for BootstrapBundleStub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Minimal HTTP stub that serves only the network join manifest route.
struct JoinManifestStub {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl JoinManifestStub {
    fn start(manifest: NetworkJoinManifest) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind join manifest stub");
        listener
            .set_nonblocking(true)
            .expect("set join manifest stub nonblocking");
        let addr = listener
            .local_addr()
            .expect("join manifest stub local addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let manifest_body = serde_json::to_string(&manifest).expect("serialize join manifest");

        let handle = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_http_request(&mut stream);
                        if request.is_empty() {
                            continue;
                        }
                        let request = String::from_utf8_lossy(&request);
                        let line = request.lines().next().unwrap_or_default();
                        let (status, status_text, payload) =
                            if line.starts_with("GET /.well-known/wattswarm/join.json ") {
                                (200, "OK", manifest_body.clone())
                            } else {
                                (404, "Not Found", "{}".to_owned())
                            };
                        let response = format!(
                            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            status,
                            status_text,
                            payload.len(),
                            payload
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            stop,
            handle: Some(handle),
        }
    }

    fn manifest_url(&self) -> String {
        format!(
            "http://127.0.0.1:{}/.well-known/wattswarm/join.json",
            self.addr.port()
        )
    }
}

impl Drop for JoinManifestStub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl RuntimeStub {
    fn start(profiles: &[&str]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind runtime stub");
        listener
            .set_nonblocking(true)
            .expect("set runtime stub nonblocking");
        let addr = listener.local_addr().expect("runtime stub local addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let profile_values: Vec<String> = profiles.iter().map(|p| (*p).to_owned()).collect();

        let handle = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_http_request(&mut stream);
                        if request.is_empty() {
                            continue;
                        }
                        let req = String::from_utf8_lossy(&request);
                        let line = req.lines().next().unwrap_or_default();

                        let (status, body) = if line.starts_with("GET /health ") {
                            (200, "{}".to_owned())
                        } else if line.starts_with("GET /capabilities ") {
                            (
                                200,
                                serde_json::json!({
                                    "task_types": ["resume_review"],
                                    "profiles": profile_values.clone(),
                                    "provider_family": "stub",
                                    "model_id": "stub-1"
                                })
                                .to_string(),
                            )
                        } else if line.starts_with("POST /execute ") {
                            (
                                200,
                                serde_json::json!({
                                    "candidate_output": {
                                        "answer": "stub-answer",
                                        "confidence": 0.9,
                                        "check_summary": "ok"
                                    },
                                    "evidence_inline": [],
                                    "evidence_refs": []
                                })
                                .to_string(),
                            )
                        } else if line.starts_with("POST /verify ") {
                            (
                                200,
                                serde_json::json!({
                                    "verification_status": "passed",
                                    "passed": true,
                                    "score": 1.0,
                                    "reason_codes": [100],
                                    "verifier_result_hash": "vrh-stub",
                                    "provider_family": "stub",
                                    "model_id": "stub-1"
                                })
                                .to_string(),
                            )
                        } else {
                            (404, "{}".to_owned())
                        };

                        let response = format!(
                            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            status,
                            match status {
                                200 => "OK",
                                404 => "Not Found",
                                _ => "Status",
                            },
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            stop,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for RuntimeStub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn wait_for_stub_listener(stub: &RuntimeStub) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        match TcpStream::connect(stub.addr) {
            Ok(_) => return,
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    }
    panic!("runtime stub listener did not become ready in time");
}

fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(250)));
    let mut buf = [0_u8; 4096];
    let mut request = Vec::with_capacity(4096);

    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                if request.len() >= 64 * 1024 {
                    break;
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(_) => {
                request.clear();
                break;
            }
        }
    }

    request
}
