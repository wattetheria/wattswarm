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

#[test]
fn configured_node_mode_requires_explicit_selection() {
    let _env_lock = env_lock();
    let prev_mode = clear_node_mode_env();
    let state_dir = temp_test_dir("configured-node-mode");

    assert_eq!(
        configured_node_mode(&state_dir).expect("read configured mode"),
        None
    );

    fs::write(
        state_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "display_name": "Node Agent",
            "network_mode": "wan",
            "bootstrap_contacts": [],
            "core_agent": {
                "mode": "local_url",
                "base_url": "http://127.0.0.1:8787",
                "provider": "openai-compatible",
                "model": "",
                "api_key": ""
            }
        }))
        .expect("serialize startup config"),
    )
    .expect("write startup config");

    assert_eq!(
        configured_node_mode(&state_dir).expect("read configured startup mode"),
        Some(wattswarm_control_plane::control::NodeMode::Network)
    );

    cleanup_dir(&state_dir);
    restore_node_mode_env(prev_mode);
}

#[test]
fn background_network_service_is_deferred_until_mode_is_configured() {
    let _env_lock = env_lock();
    let prev_mode = clear_node_mode_env();
    let _p2p_enabled = EnvVarGuard::set("WATTSWARM_P2P_ENABLED", "true");
    let state_dir = temp_test_dir("network-service-deferred");
    let db_path = state_dir.join("wattswarm.db");

    let started = maybe_start_background_network_service(state_dir.clone(), db_path)
        .expect("attempt background network start");
    assert!(
        !started,
        "service should not start before node mode is configured"
    );

    cleanup_dir(&state_dir);
    restore_node_mode_env(prev_mode);
}

#[test]
fn discovered_peers_registry_roundtrip_and_legacy_parse() {
    let dir = temp_test_dir("peers-registry");
    let path = discovered_peers_path(&dir);

    let peers = vec!["node-b".to_owned(), "node-a".to_owned()];
    save_discovered_peers(&path, &peers).expect("save discovered peers");
    let loaded = load_discovered_peers(&path).expect("load discovered peers");
    assert_eq!(loaded, peers);
    let loaded_records = load_discovered_peer_records(&path).expect("load discovered peer records");
    assert_eq!(
        loaded_records,
        vec![
            DiscoveredPeerRecord {
                node_id: "node-b".to_owned(),
                source_kind: "unknown".to_owned(),
            },
            DiscoveredPeerRecord {
                node_id: "node-a".to_owned(),
                source_kind: "unknown".to_owned(),
            }
        ]
    );

    let legacy = vec!["legacy-a", "legacy-b"];
    fs::write(&path, serde_json::to_vec(&legacy).expect("legacy vec json")).expect("write legacy");
    let loaded_legacy = load_discovered_peers(&path).expect("load legacy vec");
    assert_eq!(
        loaded_legacy,
        vec!["legacy-a".to_owned(), "legacy-b".to_owned()]
    );

    fs::write(&path, b"{not-json").expect("write invalid json");
    let err = load_discovered_peers(&path).expect_err("invalid json should fail");
    assert!(err.to_string().contains("parse discovered peers registry"));

    cleanup_dir(&dir);
}

#[test]
fn add_discovered_peer_dedups_and_sorts() {
    let _env_guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("peers-add");

    assert!(add_discovered_peer(&dir, "node-c").expect("add node-c"));
    assert!(add_discovered_peer(&dir, "node-a").expect("add node-a"));
    assert!(add_discovered_peer(&dir, "node-b").expect("add node-b"));
    assert!(!add_discovered_peer(&dir, "node-a").expect("add duplicate"));
    assert!(
        add_discovered_peer_endpoint(&dir, "node-a", Some("/ip4/127.0.0.1/tcp/4001"))
            .expect("add node-a endpoint")
    );
    assert!(
        !add_discovered_peer_endpoint(&dir, "node-a", Some("/ip4/127.0.0.1/tcp/4001"))
            .expect("dedup node-a endpoint")
    );
    assert!(!add_discovered_peer(&dir, "   ").expect("add empty"));

    let peers = load_discovered_peers_state(&dir).expect("load peers");
    assert_eq!(peers, vec!["node-a", "node-b", "node-c"]);
    let records = load_discovered_peer_records_state(&dir).expect("load peer records");
    assert_eq!(
        records,
        vec![
            DiscoveredPeerRecord {
                node_id: "node-a".to_owned(),
                source_kind: "udp".to_owned(),
            },
            DiscoveredPeerRecord {
                node_id: "node-b".to_owned(),
                source_kind: "unknown".to_owned(),
            },
            DiscoveredPeerRecord {
                node_id: "node-c".to_owned(),
                source_kind: "unknown".to_owned(),
            }
        ]
    );

    let conn = Connection::open(dir.join("local-control.state")).expect("open local control db");
    let mut stmt = conn
        .prepare(
            "SELECT scope_id, node_id, source_kind
             FROM discovered_peers_local
             ORDER BY node_id ASC",
        )
        .expect("prepare discovered peers raw query");
    let raw_rows = stmt
        .query_map(wattswarm_storage_core::params![], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .expect("query discovered peers raw rows")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect discovered peers raw rows");
    assert_eq!(
        raw_rows,
        vec![
            (
                local_control_scope_id(&dir),
                "node-a".to_owned(),
                "udp".to_owned(),
            ),
            (
                local_control_scope_id(&dir),
                "node-b".to_owned(),
                "unknown".to_owned(),
            ),
            (
                local_control_scope_id(&dir),
                "node-c".to_owned(),
                "unknown".to_owned(),
            ),
        ]
    );

    cleanup_dir(&dir);
}

#[test]
fn network_directory_snapshot_lists_networks_feeds_domains_and_sync_endpoints() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("network-directory-snapshot");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let node = open_node(&state_dir, &db_path).expect("open node");
    let udp_endpoint = test_iroh_endpoint_id(102);
    add_discovered_peer_endpoint(&state_dir, "peer-udp", Some("127.0.0.1:4999"))
        .expect("save discovered peer");
    save_peer_metadata_record_state(
        &state_dir,
        &PeerMetadataRecord {
            node_id: "peer-udp".to_owned(),
            network_id: Some("local".to_owned()),
            params_version: Some(1),
            params_hash: Some("params-local".to_owned()),
            agent_version_raw: Some("wattswarm-network-p2p|local|1|params-local".to_owned()),
            agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
            protocol_version: Some("wattswarm/1.0.0".to_owned()),
            observed_addr: Some("127.0.0.1:4999".to_owned()),
            listen_addrs: vec!["127.0.0.1:4999".to_owned()],
            protocols: vec![],
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(test_iroh_contact_material(
                &udp_endpoint,
                42,
                &["127.0.0.1:4999"],
            )),
            contact_material_signature: Some("sig-peer-udp".to_owned()),
            contact_material_updated_at: Some(42),
            first_identified_at: 42,
            last_identified_at: 42,
        },
    )
    .expect("save peer metadata");
    node.store
        .upsert_feed_subscription(
            "default",
            "node-a",
            "feed-market",
            "region:sol-1",
            &[],
            true,
            10,
        )
        .expect("feed subscription");
    node.store
        .put_task_announcement(
            "task-dir-1",
            "announce-dir-1",
            "feed-market",
            "region:sol-1",
            &json!({"headline":"dir"}),
            None,
            "node-source",
            11,
        )
        .expect("task announcement");
    node.store
        .upsert_execution_set_member(
            "task-dir-1",
            "exec-dir-1",
            "node-worker",
            "worker",
            "node:lab-7",
            "active",
            Some("node-source"),
            12,
        )
        .expect("execution set");

    let snapshot =
        load_network_directory_snapshot(&node, &state_dir, 16).expect("directory snapshot");

    assert_eq!(
        snapshot.current_topology.network.network_kind,
        NetworkKind::Local
    );
    assert_eq!(snapshot.networks.len(), 1);
    assert!(snapshot.networks[0].is_current);
    assert_eq!(snapshot.feeds.len(), 1);
    assert_eq!(snapshot.feeds[0].feed_key, "feed-market");
    assert_eq!(snapshot.feeds[0].scope_hint, "region:sol-1");
    assert_eq!(snapshot.feeds[0].subscriber_count, 1);
    assert_eq!(
        snapshot.active_dissemination_domains,
        vec!["node:lab-7".to_owned(), "region:sol-1".to_owned()]
    );
    assert_eq!(snapshot.sync_endpoints.len(), 1);
    assert!(
        snapshot
            .sync_endpoints
            .iter()
            .any(|entry| entry.source_kind == "contact_material" && entry.node_id == "peer-udp")
    );
    let udp_entry = snapshot
        .sync_endpoints
        .iter()
        .find(|entry| entry.node_id == "peer-udp")
        .expect("udp sync endpoint");
    assert_eq!(udp_entry.transports, vec!["iroh_direct".to_owned()]);
    assert_eq!(
        udp_entry
            .recommended_routes
            .get("backfill_chunk")
            .map(String::as_str),
        Some("iroh_direct")
    );
    assert_eq!(
        udp_entry
            .recommended_routes
            .get("artifact_blob")
            .map(String::as_str),
        Some("iroh_direct")
    );
    assert_eq!(
        udp_entry
            .recommended_routes
            .get("checkpoint_snapshot")
            .map(String::as_str),
        Some("iroh_direct")
    );

    cleanup_dir(&dir);
}

#[test]
fn recommended_transfer_route_uses_peer_contact_material_capabilities() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("recommended-transfer-route");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let iroh_endpoint = test_iroh_endpoint_id(103);
    save_peer_metadata_record_state(
        &state_dir,
        &PeerMetadataRecord {
            node_id: "peer-iroh".to_owned(),
            network_id: Some("wan".to_owned()),
            params_version: Some(7),
            params_hash: Some("params-iroh".to_owned()),
            agent_version_raw: Some("wattswarm-network-p2p|wan|7|params-iroh".to_owned()),
            agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
            protocol_version: Some("wattswarm/1.0.0".to_owned()),
            observed_addr: Some("/ip4/198.51.100.7/tcp/4001".to_owned()),
            listen_addrs: vec![],
            protocols: vec![],
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(test_iroh_contact_material(
                &iroh_endpoint,
                7,
                &["127.0.0.1:4997"],
            )),
            contact_material_signature: None,
            contact_material_updated_at: Some(7),
            first_identified_at: 7,
            last_identified_at: 7,
        },
    )
    .expect("save peer metadata");

    let route = recommended_transfer_route_for_remote_node(
        &state_dir,
        "peer-iroh",
        &TransferIntent {
            kind: TransferKind::BackfillChunk,
            payload_bytes: 64 * 1024,
            requires_streaming: true,
        },
    )
    .expect("recommended route");
    assert_eq!(route, TransportRoute::IrohDirect);

    cleanup_dir(&dir);
}

#[test]
fn executor_registry_roundtrip_and_default_load() {
    let dir = temp_test_dir("executor-registry");
    let path = executor_registry_path(&dir);

    let default_loaded = load_executor_registry(&path).expect("load missing registry");
    assert!(default_loaded.entries.is_empty());

    let reg = ExecutorRegistry {
        entries: vec![ExecutorRegistryEntry {
            name: "rt".to_owned(),
            base_url: "http://127.0.0.1:7788".to_owned(),
            agent_event_callback_base_url: None,
            kind: Default::default(),
            target_node_id: None,
            scope_hint: None,
            commit_plane_endpoint: None,
            commit_plane_token_file: None,
        }],
    };
    save_executor_registry(&path, &reg).expect("save registry");
    let loaded = load_executor_registry(&path).expect("load registry");
    assert_eq!(loaded.entries.len(), 1);
    assert_eq!(loaded.entries[0].name, "rt");

    cleanup_dir(&dir);
}

#[test]
fn executor_registry_state_roundtrip_preserves_remote_metadata() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("executor-registry-state-roundtrip");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![
                ExecutorRegistryEntry {
                    name: "rt".to_owned(),
                    base_url: "http://127.0.0.1:7788".to_owned(),
                    agent_event_callback_base_url: None,
                    kind: wattswarm_control_plane::control::ExecutorKind::Remote,
                    target_node_id: Some("node-remote-a".to_owned()),
                    scope_hint: Some("group:alpha".to_owned()),
                    commit_plane_endpoint: None,
                    commit_plane_token_file: None,
                },
                ExecutorRegistryEntry {
                    name: "local-aux".to_owned(),
                    base_url: "http://127.0.0.1:8899".to_owned(),
                    agent_event_callback_base_url: None,
                    kind: wattswarm_control_plane::control::ExecutorKind::Local,
                    target_node_id: None,
                    scope_hint: None,
                    commit_plane_endpoint: None,
                    commit_plane_token_file: None,
                },
            ],
        },
    )
    .expect("save executor registry state");

    let loaded = load_executor_registry_state(&state_dir).expect("load executor registry state");
    assert_eq!(loaded.entries.len(), 2);

    let remote = loaded
        .entries
        .iter()
        .find(|entry| entry.name == "rt")
        .expect("remote executor present");
    assert_eq!(
        remote.kind,
        wattswarm_control_plane::control::ExecutorKind::Remote
    );
    assert_eq!(remote.target_node_id.as_deref(), Some("node-remote-a"));
    assert_eq!(remote.scope_hint.as_deref(), Some("group:alpha"));

    let local = loaded
        .entries
        .iter()
        .find(|entry| entry.name == "local-aux")
        .expect("local executor present");
    assert_eq!(
        local.kind,
        wattswarm_control_plane::control::ExecutorKind::Local
    );
    assert_eq!(local.target_node_id, None);
    assert_eq!(local.scope_hint, None);

    cleanup_dir(&dir);
}

#[test]
fn local_node_id_is_stable_for_same_state_dir() {
    let dir = temp_test_dir("local-node-id");

    let node_id_1 = local_node_id(&dir).expect("first node id");
    let node_id_2 = local_node_id(&dir).expect("second node id");
    assert_eq!(node_id_1, node_id_2);

    cleanup_dir(&dir);
}

#[test]
fn open_node_creates_local_artifact_store_layout() {
    let dir = temp_test_dir("artifact-layout");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let _node = open_node(&state_dir, &db_path).expect("open node");

    assert!(artifact_store_path(&state_dir).join("references").exists());
    assert!(artifact_store_path(&state_dir).join("evidence").exists());
    assert!(artifact_store_path(&state_dir).join("checkpoints").exists());
    assert!(artifact_store_path(&state_dir).join("snapshots").exists());
    assert!(
        artifact_store_path(&state_dir)
            .join("availability")
            .exists()
    );

    cleanup_dir(&dir);
}

#[test]
fn open_node_bootstraps_local_network_and_default_org() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("open-node-bootstrap");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let _node = open_node(&state_dir, &db_path).expect("open node");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = format!("local:{node_id}");
    let org_id = format!("{network_id}:bootstrap");

    let conn = Connection::open("bootstrap-verify").expect("open verification connection");
    let network_row = conn
        .query_row(
            "SELECT network_kind, genesis_node_id FROM network_registry WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .expect("network registry row");
    assert_eq!(network_row.0, "local");
    assert_eq!(network_row.1, node_id);

    let org_row = conn
        .query_row(
            "SELECT network_id, org_kind, is_default FROM org_registry WHERE org_id = $1",
            wattswarm_storage_core::params![&org_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            },
        )
        .expect("org registry row");
    assert_eq!(org_row.0, format!("local:{node_id}"));
    assert_eq!(org_row.1, "bootstrap");
    assert!(org_row.2);

    let node_row = conn
        .query_row(
            "SELECT public_key, home_network_id FROM node_registry WHERE node_id = $1",
            wattswarm_storage_core::params![&node_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .expect("node registry row");
    assert_eq!(node_row.0, node_id);
    assert_eq!(node_row.1, format!("local:{node_id}"));

    let signed_params: SignedNetworkProtocolParamsEnvelope = conn
        .query_row(
            "SELECT params_json FROM network_params WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| r.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).expect("signed network params"))
        .expect("network params row");
    assert_eq!(signed_params.network_id, network_id);
    assert_eq!(signed_params.version, 1);
    assert_eq!(signed_params.signed_by, node_id);

    cleanup_dir(&dir);
}

#[test]
fn open_node_bootstraps_lan_network_and_default_org() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "lan");
    let dir = temp_test_dir("open-node-lan-bootstrap");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let _node = open_node(&state_dir, &db_path).expect("open node");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = format!("lan:{node_id}");
    let org_id = format!("{network_id}:bootstrap");

    let conn = Connection::open("lan-bootstrap-verify").expect("open verification connection");
    let network_row = conn
        .query_row(
            "SELECT network_kind, genesis_node_id FROM network_registry WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .expect("network registry row");
    assert_eq!(network_row.0, "lan");
    assert_eq!(network_row.1, node_id);

    let org_row = conn
        .query_row(
            "SELECT network_id, org_kind, is_default FROM org_registry WHERE org_id = $1",
            wattswarm_storage_core::params![&org_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            },
        )
        .expect("org registry row");
    assert_eq!(org_row.0, network_id);
    assert_eq!(org_row.1, "bootstrap");
    assert!(org_row.2);

    let signed_params: SignedNetworkProtocolParamsEnvelope = conn
        .query_row(
            "SELECT params_json FROM network_params WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| r.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).expect("signed network params"))
        .expect("network params row");
    assert_eq!(signed_params.network_id, network_id);
    assert_eq!(signed_params.version, 1);
    assert_eq!(signed_params.signed_by, node_id);

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_uses_existing_network_topology_only() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = temp_test_dir("open-node-network-mode");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = "mainnet:watt-galaxy";
    let org_id = "mainnet:watt-galaxy:bootstrap";

    let _bootstrap_store = wattswarm_control_plane::storage::PgStore::open(&db_path)
        .expect("open store for bootstrap network setup");
    let conn = Connection::open("network-bootstrap-setup").expect("open setup connection");
    conn.execute(
        "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
         VALUES ($1, 'mainnet', NULL, 'Watt Galaxy', 'active', $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, &node_id, 1_700_000_000_000_i64],
    )
    .expect("insert network registry");
    conn.execute(
        "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
         VALUES ($1, 'manual_owner', 1, 1, '{}', TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, 1_700_000_000_000_i64],
    )
    .expect("insert network params");
    conn.execute(
        "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
         VALUES ($1, $2, 'bootstrap', 'Watt Galaxy Bootstrap', 'active', TRUE, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![org_id, network_id, 1_700_000_000_000_i64],
    )
    .expect("insert org registry");

    let node = open_node(&state_dir, &db_path).expect("open node in network mode");
    assert_eq!(node.store.org_id(), org_id);

    let conn = Connection::open("network-bootstrap-verify").expect("open verification connection");
    let local_network_count = conn
        .query_row(
            "SELECT COUNT(*) FROM network_registry WHERE network_id = $1",
            wattswarm_storage_core::params![format!("local:{node_id}")],
            |r| r.get::<_, i64>(0),
        )
        .expect("count local network row");
    assert_eq!(local_network_count, 0);

    let home_network = conn
        .query_row(
            "SELECT home_network_id FROM node_registry WHERE node_id = $1",
            wattswarm_storage_core::params![&node_id],
            |r| r.get::<_, String>(0),
        )
        .expect("node registry row");
    assert_eq!(home_network, network_id);

    let signed_params: SignedNetworkProtocolParamsEnvelope = conn
        .query_row(
            "SELECT params_json FROM network_params WHERE network_id = $1",
            wattswarm_storage_core::params![network_id],
            |r| r.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).expect("signed network params"))
        .expect("network params row");
    assert_eq!(signed_params.network_id, network_id);
    assert_eq!(signed_params.version, 1);
    assert_eq!(signed_params.signed_by, node_id);

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_rejects_unsigned_network_params() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = temp_test_dir("open-node-network-mode-unsigned-params");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let _node_id = local_node_id(&state_dir).expect("local node id");
    let genesis_identity = NodeIdentity::random();
    let network_id = "mainnet:watt-galaxy-unsigned";
    let org_id = "mainnet:watt-galaxy-unsigned:bootstrap";

    let _bootstrap_store = wattswarm_control_plane::storage::PgStore::open(&db_path)
        .expect("open store for unsigned bootstrap setup");
    let conn = Connection::open("network-bootstrap-unsigned-setup").expect("open setup connection");
    conn.execute(
        "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
         VALUES ($1, 'mainnet', NULL, 'Watt Galaxy', 'active', $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, &genesis_identity.node_id(), 1_700_000_000_000_i64],
    )
    .expect("insert network registry");
    conn.execute(
        "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
         VALUES ($1, 'manual_owner', 1, 1, '{}', TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, 1_700_000_000_000_i64],
    )
    .expect("insert unsigned network params");
    conn.execute(
        "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
         VALUES ($1, $2, 'bootstrap', 'Watt Galaxy Bootstrap', 'active', TRUE, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![org_id, network_id, 1_700_000_000_000_i64],
    )
    .expect("insert org registry");

    let err = match open_node(&state_dir, &db_path) {
        Ok(_) => panic!("unsigned network params must fail"),
        Err(err) => err,
    };
    assert!(!err.to_string().trim().is_empty());

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_auto_syncs_signed_bootstrap_bundle_from_remote() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let local_schema = format!("test_local_{}", Uuid::new_v4().simple());
    let remote_schema = format!("test_remote_{}", Uuid::new_v4().simple());
    reset_test_schema(&local_schema);
    reset_test_schema(&remote_schema);

    let remote_identity = NodeIdentity::random();
    let remote_node_id = remote_identity.node_id();
    let network_id = "mainnet:watt-etheria";
    let org_id = "mainnet:watt-etheria:bootstrap";
    let bundle = {
        let _remote_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &remote_schema);
        let remote_dir = temp_test_dir("remote-bootstrap-bundle");
        let remote_state_dir = remote_dir.join("state");
        fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
        let remote_db_path = remote_state_dir.join("remote.state");
        let remote_store = wattswarm_control_plane::storage::PgStore::open(&remote_db_path)
            .expect("open remote bootstrap store");
        remote_store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "Watt Etheria",
                &remote_node_id,
                &remote_node_id,
                1_700_000_000_000,
            )
            .expect("create remote mainnet bootstrap topology");
        let conn = Connection::open("remote-bootstrap-bundle-setup")
            .expect("open remote bootstrap bundle setup connection");
        conn.execute(
            "UPDATE org_registry SET name = 'Aether Genesis' WHERE org_id = $1",
            wattswarm_storage_core::params![org_id],
        )
        .expect("rename remote bootstrap org");
        remote_store
            .put_network_protocol_params(
                network_id,
                &remote_identity,
                &NetworkProtocolParams::default(),
            )
            .expect("sign remote network params");
        let bundle = remote_store
            .for_org(org_id)
            .load_network_bootstrap_bundle()
            .expect("load remote bootstrap bundle");
        cleanup_dir(&remote_dir);
        bundle
    };

    let bootstrap_stub = BootstrapBundleStub::start(bundle.clone());
    let _local_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &local_schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _bootstrap_url_guard = EnvVarGuard::set(
        "WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS",
        &format!("http://127.0.0.1:{}", bootstrap_stub.addr.port()),
    );
    let dir = temp_test_dir("open-node-network-mode-auto-sync");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create local state dir");
    let db_path = state_dir.join("local.state");
    let local_node_id = local_node_id(&state_dir).expect("local node id");

    let node = open_node(&state_dir, &db_path).expect("open node after bootstrap sync");
    assert_eq!(node.store.org_id(), org_id);
    assert_eq!(node.node_id(), local_node_id);

    let verified = node
        .store
        .load_verified_network_protocol_params()
        .expect("load synced network params");
    assert_eq!(verified.network_id, network_id);
    assert_eq!(verified.genesis_node_id, remote_node_id);
    assert_eq!(verified.signed.signed_by, remote_node_id);
    assert_eq!(verified.signed, bundle.signed_params);

    let conn = Connection::open("network-bootstrap-auto-sync-verify")
        .expect("open auto-sync verification connection");
    let imported_org: (String, bool) = conn
        .query_row(
            "SELECT name, is_default FROM org_registry WHERE org_id = $1",
            wattswarm_storage_core::params![org_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("load imported org");
    assert_eq!(imported_org.0, "Aether Genesis");
    assert!(imported_org.1);

    let home_network = conn
        .query_row(
            "SELECT home_network_id FROM node_registry WHERE node_id = $1",
            wattswarm_storage_core::params![&local_node_id],
            |r| r.get::<_, String>(0),
        )
        .expect("load local node registry row");
    assert_eq!(home_network, network_id);

    cleanup_dir(&dir);
}

#[test]
fn network_mode_loads_subnet_topology_as_network_subtype() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = temp_test_dir("open-node-subnet-network-mode");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = "subnet:alpha";
    let parent_network_id = "mainnet:watt-galaxy";
    let org_id = "subnet:alpha:bootstrap";

    let store = wattswarm_control_plane::storage::PgStore::open(&db_path).expect("open store");
    let conn = Connection::open("subnet-bootstrap-setup").expect("open setup connection");
    conn.execute(
        "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
         VALUES ($1, 'subnet', $2, 'Subnet Alpha', 'active', $3, TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, parent_network_id, &node_id, 1_700_000_000_000_i64],
    )
    .expect("insert subnet network registry");
    conn.execute(
        "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
         VALUES ($1, 'manual_owner', 1, 1, '{}', TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, 1_700_000_000_000_i64],
    )
    .expect("insert subnet network params");
    conn.execute(
        "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
         VALUES ($1, $2, 'bootstrap', 'Subnet Alpha Bootstrap', 'active', TRUE, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![org_id, network_id, 1_700_000_000_000_i64],
    )
    .expect("insert subnet org registry");

    let node = open_node(&state_dir, &db_path).expect("open node in subnet mode");
    assert_eq!(node.store.org_id(), org_id);

    let topology = store
        .load_network_topology_for_org(org_id)
        .expect("load subnet topology");
    assert_eq!(topology.network.network_kind, NetworkKind::Subnet);
    assert_eq!(
        topology.network.parent_network_id.as_deref(),
        Some(parent_network_id)
    );
    assert_eq!(topology.org.network_id, topology.network.network_id);

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_auto_syncs_signed_bundle_from_join_manifest() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let local_schema = format!("test_local_{}", Uuid::new_v4().simple());
    let remote_schema = format!("test_remote_{}", Uuid::new_v4().simple());
    reset_test_schema(&local_schema);
    reset_test_schema(&remote_schema);

    let remote_identity = NodeIdentity::random();
    let remote_node_id = remote_identity.node_id();
    let network_id = "mainnet:wattetheria";
    let org_id = "mainnet:wattetheria:bootstrap";
    let bundle = {
        let _remote_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &remote_schema);
        let remote_dir = temp_test_dir("remote-join-manifest-bundle");
        let remote_state_dir = remote_dir.join("state");
        fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
        let remote_db_path = remote_state_dir.join("remote.state");
        let remote_store = wattswarm_control_plane::storage::PgStore::open(&remote_db_path)
            .expect("open remote bootstrap store");
        remote_store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "Wattetheria",
                &remote_node_id,
                &remote_node_id,
                1_700_000_000_000,
            )
            .expect("create remote mainnet bootstrap topology");
        remote_store
            .put_network_protocol_params(
                network_id,
                &remote_identity,
                &NetworkProtocolParams::default(),
            )
            .expect("sign remote network params");
        let bundle = remote_store
            .for_org(org_id)
            .load_network_bootstrap_bundle()
            .expect("load remote bootstrap bundle");
        cleanup_dir(&remote_dir);
        bundle
    };
    let manifest = NetworkJoinManifest {
        network_id: network_id.to_owned(),
        genesis_node_id: remote_node_id.clone(),
        params_hash: bundle.signed_params.params_hash.clone(),
        bootstrap_urls: Vec::new(),
        bootstrap_contacts: vec!["iroh-bootstrap-contact-json".to_owned()],
        gateway_urls: vec!["https://gateway.wattetheria.com/".to_owned()],
        discovery_urls: vec![
            "https://bootstrap.wattetheria.com/api/network/discovery/".to_owned(),
            "https://bootstrap.wattetheria.com/api/network/discovery".to_owned(),
        ],
    };
    let bootstrap_stub = BootstrapBundleStub::start_with_manifest(bundle.clone(), Some(manifest));
    let _local_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &local_schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _bootstrap_url_guard = EnvVarGuard::remove("WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS");
    let _bootstrap_peers_guard = EnvVarGuard::remove("WATTSWARM_P2P_BOOTSTRAP_PEERS");
    let _manifest_guard = EnvVarGuard::set(
        "WATTSWARM_NETWORK_JOIN_MANIFEST_URLS",
        &format!(
            "http://127.0.0.1:{}/.well-known/wattswarm/join.json",
            bootstrap_stub.addr.port()
        ),
    );

    let dir = temp_test_dir("open-node-network-mode-join-manifest");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create local state dir");
    fs::write(
        state_dir.join("startup_config.json"),
        json!({
            "display_name": "Existing Node",
            "latitude": 37.0,
            "longitude": -122.0,
            "network_mode": "wan",
            "bootstrap_contacts": ["stale-bootstrap-contact"],
            "gateway_urls": ["https://old-gateway.wattetheria.com"]
        })
        .to_string(),
    )
    .expect("write existing startup config");
    let db_path = state_dir.join("local.state");
    let node = open_node(&state_dir, &db_path).expect("open node after join manifest sync");
    let verified = node
        .store
        .load_verified_network_protocol_params()
        .expect("load synced network params");
    assert_eq!(verified.network_id, network_id);
    assert_eq!(verified.genesis_node_id, remote_node_id);
    assert_eq!(verified.signed, bundle.signed_params);
    let startup_config: serde_json::Value =
        serde_json::from_slice(&fs::read(state_dir.join("startup_config.json")).unwrap()).unwrap();
    assert_eq!(startup_config["network_mode"].as_str(), Some("wan"));
    assert_eq!(
        startup_config["display_name"].as_str(),
        Some("Existing Node")
    );
    assert_eq!(startup_config["latitude"].as_f64(), Some(37.0));
    assert_eq!(startup_config["longitude"].as_f64(), Some(-122.0));
    assert_eq!(
        startup_config["bootstrap_contacts"]
            .as_array()
            .expect("bootstrap contacts")
            .len(),
        1
    );
    assert_eq!(
        startup_config["bootstrap_contacts"][0].as_str(),
        Some("iroh-bootstrap-contact-json")
    );
    assert_eq!(
        startup_config["gateway_urls"]
            .as_array()
            .expect("gateway urls")
            .len(),
        1
    );
    assert_eq!(
        startup_config["gateway_urls"][0].as_str(),
        Some("https://gateway.wattetheria.com")
    );
    assert_eq!(
        wattswarm_control_plane::load_discovery_bootnode_urls_state(&state_dir)
            .expect("load discovery urls"),
        vec!["https://bootstrap.wattetheria.com/api/network/discovery".to_owned()]
    );

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_rejects_join_manifest_params_hash_mismatch() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let local_schema = format!("test_local_{}", Uuid::new_v4().simple());
    let remote_schema = format!("test_remote_{}", Uuid::new_v4().simple());
    reset_test_schema(&local_schema);
    reset_test_schema(&remote_schema);

    let remote_identity = NodeIdentity::random();
    let remote_node_id = remote_identity.node_id();
    let network_id = "mainnet:wattetheria";
    let org_id = "mainnet:wattetheria:bootstrap";
    let bundle = {
        let _remote_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &remote_schema);
        let remote_dir = temp_test_dir("remote-join-mismatch-bundle");
        let remote_state_dir = remote_dir.join("state");
        fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
        let remote_db_path = remote_state_dir.join("remote.state");
        let remote_store = wattswarm_control_plane::storage::PgStore::open(&remote_db_path)
            .expect("open remote bootstrap store");
        remote_store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "Wattetheria",
                &remote_node_id,
                &remote_node_id,
                1_700_000_000_000,
            )
            .expect("create remote mainnet bootstrap topology");
        remote_store
            .put_network_protocol_params(
                network_id,
                &remote_identity,
                &NetworkProtocolParams::default(),
            )
            .expect("sign remote network params");
        let bundle = remote_store
            .for_org(org_id)
            .load_network_bootstrap_bundle()
            .expect("load remote bootstrap bundle");
        cleanup_dir(&remote_dir);
        bundle
    };
    let manifest = NetworkJoinManifest {
        network_id: network_id.to_owned(),
        genesis_node_id: remote_node_id,
        params_hash: "wrong-params-hash".to_owned(),
        bootstrap_urls: Vec::new(),
        bootstrap_contacts: Vec::new(),
        gateway_urls: Vec::new(),
        discovery_urls: Vec::new(),
    };
    let bootstrap_stub = BootstrapBundleStub::start_with_manifest(bundle, Some(manifest));
    let _local_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &local_schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _bootstrap_url_guard = EnvVarGuard::remove("WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS");
    let _bootstrap_peers_guard = EnvVarGuard::remove("WATTSWARM_P2P_BOOTSTRAP_PEERS");
    let _manifest_guard = EnvVarGuard::set(
        "WATTSWARM_NETWORK_JOIN_MANIFEST_URLS",
        &format!(
            "http://127.0.0.1:{}/.well-known/wattswarm/join.json",
            bootstrap_stub.addr.port()
        ),
    );

    let dir = temp_test_dir("open-node-network-mode-join-mismatch");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create local state dir");
    let db_path = state_dir.join("local.state");
    let err = match open_node(&state_dir, &db_path) {
        Ok(_) => panic!("mismatched params hash must fail"),
        Err(err) => err,
    };
    assert!(err.chain().any(|cause| {
        cause
            .to_string()
            .contains("join manifest params_hash mismatch")
    }));

    cleanup_dir(&dir);
}

#[test]
fn open_node_on_network_id_joins_and_binds_subnet_overlay() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("open-node-on-subnet");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let genesis_identity = NodeIdentity::random();
    let genesis_node_id = genesis_identity.node_id();
    let now = 1_700_000_000_000_u64;

    let store = wattswarm_control_plane::storage::PgStore::open(&db_path).expect("open store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &genesis_node_id,
            &genesis_node_id,
            now,
        )
        .expect("create mainnet topology");
    store
        .ensure_bootstrap_signed_network_protocol_params(
            &mainnet.network.network_id,
            &genesis_identity,
        )
        .expect("sign mainnet params");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &genesis_node_id,
            &genesis_node_id,
            now + 1,
        )
        .expect("create subnet topology");
    store
        .ensure_bootstrap_signed_network_protocol_params(
            &subnet.network.network_id,
            &genesis_identity,
        )
        .expect("sign subnet params");

    let node = open_node_on_network_id(&state_dir, &db_path, &subnet.network.network_id)
        .expect("open node on subnet");
    assert_eq!(node.store.org_id(), subnet.org.org_id);

    let opened_topology = store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load opened topology");
    assert_eq!(opened_topology.network.network_kind, NetworkKind::Subnet);
    assert_eq!(
        opened_topology.network.parent_network_id.as_deref(),
        Some(mainnet.network.network_id.as_str())
    );

    let joined_node_id = local_node_id(&state_dir).expect("joined node id");
    assert!(
        store
            .node_has_network_membership(&joined_node_id, &subnet.network.network_id)
            .expect("subnet membership")
    );
    assert!(
        store
            .node_has_network_membership(&joined_node_id, &mainnet.network.network_id)
            .expect("parent membership")
    );

    cleanup_dir(&dir);
}

#[test]
fn task_detail_and_evidence_artifacts_roundtrip_and_missing_refs_schedule_repair() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("artifact-detail-evidence");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let network_id = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology")
        .network
        .network_id;

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-artifact-detail", policy_hash);
    contract.inputs = json!({"prompt":"artifact detail"});
    node.submit_task(contract, 1, 10).expect("submit task");

    let detail_bytes = br#"{"task":"detail","v":1}"#;
    let detail_digest = format!("sha256:{}", sha256_hex(detail_bytes));
    node.emit_at(
        1,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(
            wattswarm_control_plane::types::TaskAnnouncedPayload {
                network_id,
                task_id: "task-artifact-detail".to_owned(),
                announcement_id: "announce-artifact-detail".to_owned(),
                feed_key: "feed.detail".to_owned(),
                scope_hint: "global".to_owned(),
                summary: json!({"headline":"detail"}),
                detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                    uri: "ipfs://task-artifact-detail".to_owned(),
                    digest: detail_digest.clone(),
                    size_bytes: detail_bytes.len() as u64,
                    mime: "application/json".to_owned(),
                    created_at: 11,
                    producer: node.node_id(),
                }),
            },
        ),
        11,
    )
    .expect("announce detail");

    let missing_detail = fetch_task_detail_artifact(&state_dir, &node, "task-artifact-detail", 20);
    assert!(missing_detail.is_err());

    let pending = list_artifacts_needing_repair(&state_dir, 20 + 30_000).expect("list repairs");
    assert!(pending.iter().any(|manifest| {
        manifest.artifact_kind == ArtifactKind::Reference
            && manifest.artifact_id == detail_digest
            && manifest.status == ArtifactAvailabilityStatus::Missing
    }));

    let detail_manifest = materialize_task_detail_artifact(
        &state_dir,
        &node,
        "task-artifact-detail",
        detail_bytes,
        21,
    )
    .expect("materialize task detail");
    assert_eq!(
        detail_manifest.status,
        ArtifactAvailabilityStatus::Available
    );

    let loaded_detail = fetch_task_detail_artifact(&state_dir, &node, "task-artifact-detail", 22)
        .expect("fetch task detail");
    assert_eq!(loaded_detail, detail_bytes);

    node.claim_task(
        "task-artifact-detail",
        wattswarm_control_plane::types::ClaimRole::Propose,
        "exec-artifact-detail",
        500,
        1,
        23,
    )
    .expect("claim task");
    let evidence_bytes = br#"{"evidence":"blob"}"#;
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_bytes));
    let candidate_output = json!({"answer":"ok"});
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-artifact-1".to_owned(),
        execution_id: "exec-artifact-detail".to_owned(),
        output_ref: candidate_output_ref("cand-artifact-1", &candidate_output, 24, &node.node_id()),
        output: candidate_output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.propose_candidate("task-artifact-detail", candidate, 1, 24)
        .expect("propose candidate");
    node.add_evidence(
        "task-artifact-detail",
        "cand-artifact-1",
        "exec-artifact-detail",
        vec![wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://evidence-artifact".to_owned(),
            digest: evidence_digest.clone(),
            size_bytes: evidence_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 24,
            producer: node.node_id(),
        }],
        1,
        25,
    )
    .expect("add evidence");

    materialize_evidence_artifact(
        &state_dir,
        &node,
        "task-artifact-detail",
        "cand-artifact-1",
        &evidence_digest,
        evidence_bytes,
        26,
    )
    .expect("materialize evidence");
    let loaded_evidence = fetch_evidence_artifact(
        &state_dir,
        &node,
        "task-artifact-detail",
        "cand-artifact-1",
        &evidence_digest,
        27,
    )
    .expect("fetch evidence");
    assert_eq!(loaded_evidence, evidence_bytes);

    cleanup_dir(&dir);
}

#[test]
fn checkpoint_and_snapshot_artifacts_roundtrip_and_record_missing_retries() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("artifact-checkpoint-snapshot");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node = open_node(&state_dir, &db_path).expect("open node");

    node.store
        .put_checkpoint_announcement("global", "cp-artifact-1", "ipfs://checkpoint-1", 30)
        .expect("put checkpoint announcement");

    let missing_checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-artifact-1",
        31,
    );
    assert!(missing_checkpoint.is_err());

    let checkpoint_manifest = materialize_checkpoint_artifact_json(
        &state_dir,
        &node,
        "global",
        "cp-artifact-1",
        &json!({"checkpoint":"cp-artifact-1","up_to_seq":42}),
        32,
    )
    .expect("materialize checkpoint");
    assert_eq!(checkpoint_manifest.artifact_kind, ArtifactKind::Checkpoint);

    let checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-artifact-1",
        33,
    )
    .expect("fetch checkpoint");
    assert_eq!(checkpoint["up_to_seq"], json!(42));

    let missing_snapshot =
        fetch_snapshot_artifact_json::<serde_json::Value>(&state_dir, "region:sol-1", "snap-1", 34);
    assert!(missing_snapshot.is_err());

    let pending = list_artifacts_needing_repair(&state_dir, 34 + 30_000).expect("list repairs");
    assert!(pending.iter().any(|manifest| {
        manifest.artifact_kind == ArtifactKind::Snapshot
            && manifest.artifact_id == "snap-1"
            && manifest.status == ArtifactAvailabilityStatus::Missing
    }));

    materialize_snapshot_artifact_json(
        &state_dir,
        "region:sol-1",
        "snap-1",
        Some("ipfs://snapshot-1"),
        &json!({"snapshot":"snap-1","epoch":7}),
        35,
    )
    .expect("materialize snapshot");
    let snapshot =
        fetch_snapshot_artifact_json::<serde_json::Value>(&state_dir, "region:sol-1", "snap-1", 36)
            .expect("fetch snapshot");
    assert_eq!(snapshot["epoch"], json!(7));

    cleanup_dir(&dir);
}

#[test]
fn missing_task_and_evidence_artifacts_record_remote_source_route() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("artifact-source-route");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-remote-artifact", policy_hash);
    contract.inputs = json!({"prompt":"remote artifact"});
    node.submit_task(contract, 1, 39).expect("submit task");

    save_iroh_peer_metadata(&state_dir, "node-remote", 40);

    let detail_bytes = br#"{"detail":"remote"}"#;
    let detail_digest = format!("sha256:{}", sha256_hex(detail_bytes));
    node.announce_task(
        "task-remote-artifact",
        "announce-remote-artifact",
        "feed-artifacts",
        "global",
        json!({"headline":"remote detail"}),
        Some(wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://task-remote-artifact".to_owned(),
            digest: detail_digest.clone(),
            size_bytes: detail_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 40,
            producer: "node-remote".to_owned(),
        }),
        1,
        40,
    )
    .expect("announce remote detail");

    let missing_detail = fetch_task_detail_artifact(&state_dir, &node, "task-remote-artifact", 41);
    assert!(missing_detail.is_err());

    node.claim_task(
        "task-remote-artifact",
        wattswarm_control_plane::types::ClaimRole::Propose,
        "exec-remote-evidence",
        500,
        1,
        42,
    )
    .expect("claim task");
    let evidence_bytes = br#"{"evidence":"remote"}"#;
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_bytes));
    let candidate_output = json!({"answer":"ok"});
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-remote-evidence".to_owned(),
        execution_id: "exec-remote-evidence".to_owned(),
        output_ref: candidate_output_ref(
            "cand-remote-evidence",
            &candidate_output,
            43,
            &node.node_id(),
        ),
        output: candidate_output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.propose_candidate("task-remote-artifact", candidate, 1, 43)
        .expect("propose candidate");
    node.add_evidence(
        "task-remote-artifact",
        "cand-remote-evidence",
        "exec-remote-evidence",
        vec![wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://evidence-remote-artifact".to_owned(),
            digest: evidence_digest.clone(),
            size_bytes: evidence_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 43,
            producer: "node-remote".to_owned(),
        }],
        1,
        44,
    )
    .expect("add remote evidence");

    let missing_evidence = fetch_evidence_artifact(
        &state_dir,
        &node,
        "task-remote-artifact",
        "cand-remote-evidence",
        &evidence_digest,
        45,
    );
    assert!(missing_evidence.is_err());

    let decisions = load_transport_decisions(&state_dir);
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("artifact_blob")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-remote")
            && entry["subject"]["artifact_kind"] == json!("task_detail")
    }));
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("evidence_blob")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-remote")
            && entry["subject"]["artifact_kind"] == json!("evidence")
    }));

    cleanup_dir(&dir);
}

#[test]
fn fetch_task_and_evidence_artifacts_over_iroh_when_remote_contact_material_is_available() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = temp_test_dir("artifact-source-iroh");
    let state_dir = dir.join("state");
    let remote_state_dir = dir.join("remote-state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
    let db_path = state_dir.join("test.state");
    let mut node = open_node(&state_dir, &db_path).expect("open node");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-remote-artifact-iroh", policy_hash);
    contract.inputs = json!({"prompt":"remote artifact over iroh"});
    node.submit_task(contract, 1, 60).expect("submit task");

    let _ = local_node_id(&remote_state_dir).expect("create remote node seed");
    save_real_iroh_peer_metadata(&state_dir, "node-remote", &remote_state_dir, 61);

    let remote_store = ArtifactStore::new(artifact_store_path(&remote_state_dir));
    remote_store.ensure_layout().expect("remote store layout");

    let detail_bytes = br#"{"detail":"remote-via-iroh"}"#;
    let detail_digest = format!("sha256:{}", sha256_hex(detail_bytes));
    remote_store
        .write_validated_bytes(
            ArtifactKind::Reference,
            &detail_digest,
            None,
            detail_bytes,
            Some(&detail_digest),
            Some(detail_bytes.len() as u64),
        )
        .expect("write remote detail");
    node.announce_task(
        "task-remote-artifact-iroh",
        "announce-remote-artifact-iroh",
        "feed-artifacts",
        "global",
        json!({"headline":"remote detail"}),
        Some(wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://task-remote-artifact-iroh".to_owned(),
            digest: detail_digest.clone(),
            size_bytes: detail_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 61,
            producer: "node-remote".to_owned(),
        }),
        1,
        61,
    )
    .expect("announce remote detail");

    let loaded_detail =
        fetch_task_detail_artifact(&state_dir, &node, "task-remote-artifact-iroh", 62)
            .expect("fetch detail over iroh");
    assert_eq!(loaded_detail, detail_bytes);

    node.claim_task(
        "task-remote-artifact-iroh",
        wattswarm_control_plane::types::ClaimRole::Propose,
        "exec-remote-evidence-iroh",
        500,
        1,
        63,
    )
    .expect("claim task");
    let candidate_output = json!({"answer":"ok"});
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-remote-evidence-iroh".to_owned(),
        execution_id: "exec-remote-evidence-iroh".to_owned(),
        output_ref: candidate_output_ref(
            "cand-remote-evidence-iroh",
            &candidate_output,
            64,
            &node.node_id(),
        ),
        output: candidate_output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.propose_candidate("task-remote-artifact-iroh", candidate, 1, 64)
        .expect("propose candidate");

    let evidence_bytes = br#"{"evidence":"remote-via-iroh"}"#;
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_bytes));
    remote_store
        .write_validated_bytes(
            ArtifactKind::Evidence,
            &evidence_digest,
            None,
            evidence_bytes,
            Some(&evidence_digest),
            Some(evidence_bytes.len() as u64),
        )
        .expect("write remote evidence");
    node.add_evidence(
        "task-remote-artifact-iroh",
        "cand-remote-evidence-iroh",
        "exec-remote-evidence-iroh",
        vec![wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://evidence-remote-artifact-iroh".to_owned(),
            digest: evidence_digest.clone(),
            size_bytes: evidence_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 65,
            producer: "node-remote".to_owned(),
        }],
        1,
        65,
    )
    .expect("add evidence");

    let loaded_evidence = fetch_evidence_artifact(
        &state_dir,
        &node,
        "task-remote-artifact-iroh",
        "cand-remote-evidence-iroh",
        &evidence_digest,
        66,
    )
    .expect("fetch evidence over iroh");
    assert_eq!(loaded_evidence, evidence_bytes);

    cleanup_dir(&dir);
}

#[test]
fn missing_checkpoint_and_snapshot_artifacts_use_saved_source_binding_for_route() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("checkpoint-snapshot-source-route");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node = open_node(&state_dir, &db_path).expect("open node");

    save_iroh_peer_metadata(&state_dir, "node-sync", 50);
    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Checkpoint,
            binding_scope: Some("global".to_owned()),
            binding_key: "cp-source-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://checkpoint-source-1".to_owned()),
            updated_at: 50,
        },
    )
    .expect("save checkpoint binding");
    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Snapshot,
            binding_scope: Some("region:sol-1".to_owned()),
            binding_key: "snap-source-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://snapshot-source-1".to_owned()),
            updated_at: 50,
        },
    )
    .expect("save snapshot binding");

    node.store
        .put_checkpoint_announcement("global", "cp-source-1", "ipfs://checkpoint-source-1", 51)
        .expect("put checkpoint announcement");

    let missing_checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-source-1",
        52,
    );
    assert!(missing_checkpoint.is_err());

    let missing_snapshot = fetch_snapshot_artifact_json::<serde_json::Value>(
        &state_dir,
        "region:sol-1",
        "snap-source-1",
        53,
    );
    assert!(missing_snapshot.is_err());

    let decisions = load_transport_decisions(&state_dir);
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("checkpoint_snapshot")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-sync")
            && entry["subject"]["artifact_kind"] == json!("checkpoint")
    }));
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("checkpoint_snapshot")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-sync")
            && entry["subject"]["artifact_kind"] == json!("snapshot")
    }));

    cleanup_dir(&dir);
}

#[test]
fn fetch_checkpoint_and_snapshot_artifacts_over_iroh_when_remote_contact_material_is_available() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = temp_test_dir("checkpoint-snapshot-iroh");
    let state_dir = dir.join("state");
    let remote_state_dir = dir.join("remote-state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
    let db_path = state_dir.join("test.state");
    let node = open_node(&state_dir, &db_path).expect("open node");

    let _ = local_node_id(&remote_state_dir).expect("create remote node seed");
    save_real_iroh_peer_metadata(&state_dir, "node-sync", &remote_state_dir, 80);

    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Checkpoint,
            binding_scope: Some("global".to_owned()),
            binding_key: "cp-iroh-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://checkpoint-iroh-1".to_owned()),
            updated_at: 80,
        },
    )
    .expect("save checkpoint binding");
    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Snapshot,
            binding_scope: Some("region:sol-1".to_owned()),
            binding_key: "snap-iroh-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://snapshot-iroh-1".to_owned()),
            updated_at: 80,
        },
    )
    .expect("save snapshot binding");

    node.store
        .put_checkpoint_announcement("global", "cp-iroh-1", "ipfs://checkpoint-iroh-1", 81)
        .expect("checkpoint announcement");

    let remote_store = ArtifactStore::new(artifact_store_path(&remote_state_dir));
    remote_store.ensure_layout().expect("remote store layout");
    remote_store
        .write_json(
            &remote_store
                .checkpoint_path("cp-iroh-1")
                .expect("checkpoint path"),
            &json!({"checkpoint":"cp-iroh-1","up_to_seq":77}),
        )
        .expect("write remote checkpoint");
    remote_store
        .write_json(
            &remote_store
                .snapshot_path("region:sol-1", "snap-iroh-1")
                .expect("snapshot path"),
            &json!({"snapshot":"snap-iroh-1","epoch":9}),
        )
        .expect("write remote snapshot");

    let checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-iroh-1",
        82,
    )
    .expect("fetch checkpoint over iroh");
    assert_eq!(checkpoint["up_to_seq"], json!(77));

    let snapshot = fetch_snapshot_artifact_json::<serde_json::Value>(
        &state_dir,
        "region:sol-1",
        "snap-iroh-1",
        83,
    )
    .expect("fetch snapshot over iroh");
    assert_eq!(snapshot["epoch"], json!(9));

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_returns_clear_error_when_executor_missing() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-missing-executor");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let req = RealTaskRunRequest {
        executor: "missing-executor".to_owned(),
        profile: "default".to_owned(),
        task_id: Some(format!("task-{}", Uuid::new_v4().simple())),
        task_file: None,
        task_contract: Some(sample_contract("task-inline", "policy-hash".to_owned())),
    };

    let err =
        run_real_task_flow(&mut node, &state_dir, req).expect_err("executor should be missing");
    assert!(
        err.to_string()
            .contains("executor not found: missing-executor")
    );

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_rejects_unsupported_profile() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-unsupported-profile");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["other-profile"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let req = RealTaskRunRequest {
        executor: "rt".to_owned(),
        profile: "default".to_owned(),
        task_id: Some(format!("task-{}", Uuid::new_v4().simple())),
        task_file: None,
        task_contract: Some(sample_contract("task-inline", "policy-hash".to_owned())),
    };

    let err = run_real_task_flow(&mut node, &state_dir, req)
        .expect_err("profile should be rejected before task run");
    assert!(err.to_string().contains("profile 'default' not supported"));

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_reports_task_file_parse_errors() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-task-file-parse");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let broken_task_file = dir.join("broken-task.json");
    fs::write(&broken_task_file, b"{not-json").expect("write broken task file");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let req = RealTaskRunRequest {
        executor: "rt".to_owned(),
        profile: "default".to_owned(),
        task_id: None,
        task_file: Some(broken_task_file.clone()),
        task_contract: None,
    };
    let err =
        run_real_task_flow(&mut node, &state_dir, req).expect_err("broken task json should fail");
    let err_msg = err.to_string();
    assert!(
        (err_msg.contains("parse task contract from") && err_msg.contains("broken-task.json"))
            || err_msg.contains("key must be a string")
            || err_msg.contains("expected value")
            || err_msg.contains("expected ident")
            || err_msg.contains("line 1 column")
    );

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_completes_with_stub_runtime() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("run-real-success");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("task-real-success-{}", Uuid::new_v4().simple());
    let req = RealTaskRunRequest {
        executor: "rt".to_owned(),
        profile: "default".to_owned(),
        task_id: Some(task_id.clone()),
        task_file: None,
        task_contract: Some(sample_contract(&task_id, policy_hash)),
    };

    let out = run_real_task_flow(&mut node, &state_dir, req).expect("run real task flow success");
    assert_eq!(out["task_id"], task_id);
    assert_eq!(out["profile"], "default");
    assert_eq!(out["terminal_state"], "Finalized");

    cleanup_dir(&dir);
}

#[test]
fn remote_task_bridge_materializes_executes_and_dedupes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("remote-task-bridge");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let remote = NodeIdentity::random();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&node.node_id(), role);
    }
    membership.grant(&remote.node_id(), Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("remote-bridge-task-{}", Uuid::new_v4().simple());
    let contract = sample_contract(&task_id, policy_hash);
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement_unsigned = UnsignedEvent::from_payload(
        "0.1.0".to_owned(),
        remote.node_id(),
        1,
        100,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(TaskAnnouncedPayload {
            network_id: topology.network.network_id.clone(),
            task_id: task_id.clone(),
            announcement_id: format!("ann-{}", Uuid::new_v4().simple()),
            feed_key: "remote-feed".to_owned(),
            scope_hint: "global".to_owned(),
            summary: json!({"title":"remote bridge task"}),
            detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                uri: "ipfs://remote-task-detail".to_owned(),
                digest: digest.clone(),
                size_bytes: contract_bytes.len() as u64,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: remote.node_id(),
            }),
        }),
    );
    let announcement = remote
        .sign_unsigned_event(&announcement_unsigned)
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");

    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");

    let out = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("bridge remote task");
    assert_eq!(out["task_id"], json!(task_id.clone()));
    assert_eq!(out["profile"], json!("default"));
    assert_eq!(out["terminal_state"], json!("Finalized"));
    assert_eq!(out["bridge"]["deduped"], json!(false));
    assert_eq!(out["bridge"]["source_node_id"], json!(remote.node_id()));

    let members = node
        .store
        .list_execution_set_members(&task_id, &format!("remote-bridge:{task_id}"))
        .expect("list execution set members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].participant_node_id, node.node_id());
    assert_eq!(members[0].status, "confirmed");

    let bridge_rows =
        wattswarm_control_plane::storage::PgStore::open(state_dir.join("local-control.state"))
            .expect("open local control store")
            .list_local_remote_task_bridges(&local_control_scope_id(&state_dir))
            .expect("list local remote task bridges");
    let registry = RemoteTaskBridgeRegistry {
        entries: bridge_rows
            .into_iter()
            .map(
                |entry| wattswarm_control_plane::control::RemoteTaskBridgeRecord {
                    task_id: entry.task_id,
                    announcement_id: entry.announcement_id,
                    network_id: entry.network_id,
                    source_node_id: entry.source_node_id,
                    source_scope_hint: entry.source_scope_hint,
                    detail_ref_digest: entry.detail_ref_digest,
                    executor: entry.executor,
                    profile: entry.profile,
                    candidate_id: entry.candidate_id,
                    terminal_state: entry.terminal_state,
                    bridged_at: entry.bridged_at,
                },
            )
            .collect(),
    };
    assert_eq!(registry.entries.len(), 1);
    assert_eq!(
        registry.entries[0].detail_ref_digest.as_deref(),
        Some(digest.as_str())
    );

    let deduped = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("dedupe bridged task");
    assert_eq!(deduped["bridge"]["deduped"], json!(true));
    assert_eq!(deduped["candidate_id"], out["candidate_id"]);

    let registry_after =
        wattswarm_control_plane::storage::PgStore::open(state_dir.join("local-control.state"))
            .expect("open local control store after")
            .list_local_remote_task_bridges(&local_control_scope_id(&state_dir))
            .expect("list local remote task bridges after");
    assert_eq!(registry_after.len(), 1);

    cleanup_dir(&dir);
}

#[test]
fn remote_task_bridge_rejects_node_scoped_tasks_for_other_nodes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("remote-task-bridge-scope-reject");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let remote = NodeIdentity::random();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&node.node_id(), role);
    }
    membership.grant(&remote.node_id(), Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("remote-bridge-node-scope-{}", Uuid::new_v4().simple());
    let contract = sample_contract(&task_id, policy_hash);
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement_unsigned = UnsignedEvent::from_payload(
        "0.1.0".to_owned(),
        remote.node_id(),
        1,
        100,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(TaskAnnouncedPayload {
            network_id: topology.network.network_id.clone(),
            task_id: task_id.clone(),
            announcement_id: format!("ann-{}", Uuid::new_v4().simple()),
            feed_key: "remote-feed".to_owned(),
            scope_hint: "node:not-this-node".to_owned(),
            summary: json!({"title":"node scoped bridge task"}),
            detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                uri: "ipfs://remote-task-detail".to_owned(),
                digest,
                size_bytes: contract_bytes.len() as u64,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: remote.node_id(),
            }),
        }),
    );
    let announcement = remote
        .sign_unsigned_event(&announcement_unsigned)
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");
    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");

    let err = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect_err("bridge should reject mismatched node scope");
    assert!(err.to_string().contains("not eligible for local node"));
    assert!(
        wattswarm_control_plane::storage::PgStore::open(state_dir.join("local-control.state"))
            .expect("open local control store")
            .list_local_remote_task_bridges(&local_control_scope_id(&state_dir))
            .expect("list local remote task bridges")
            .is_empty(),
        "rejected bridge should not persist dedupe registry rows"
    );

    cleanup_dir(&dir);
}

#[test]
fn remote_task_bridge_allows_group_scoped_tasks_for_target_nodes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("remote-task-bridge-group-scope-allow");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let remote = NodeIdentity::random();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&node.node_id(), role);
    }
    membership.grant(&remote.node_id(), Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("remote-bridge-group-scope-{}", Uuid::new_v4().simple());
    let mut contract = sample_contract(&task_id, policy_hash);
    contract.task_type = "group:crew-7:swarm".to_owned();
    contract.inputs = json!({
        "prompt":"group scoped bridge task",
        "swarm_scope":"group:crew-7",
        "swarm_route":{
            "group_id":"crew-7",
            "target_node_ids":[node.node_id()],
            "relation_tags":["crew"],
            "forward_budget":1
        }
    });
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement_unsigned = UnsignedEvent::from_payload(
        "0.1.0".to_owned(),
        remote.node_id(),
        1,
        100,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(TaskAnnouncedPayload {
            network_id: topology.network.network_id.clone(),
            task_id: task_id.clone(),
            announcement_id: format!("ann-{}", Uuid::new_v4().simple()),
            feed_key: "remote-feed".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            summary: json!({"title":"group scoped bridge task"}),
            detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                uri: "ipfs://remote-task-detail".to_owned(),
                digest,
                size_bytes: contract_bytes.len() as u64,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: remote.node_id(),
            }),
        }),
    );
    let announcement = remote
        .sign_unsigned_event(&announcement_unsigned)
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");
    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");

    let out = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("bridge should allow targeted group scope");

    assert_eq!(out["task_id"], json!(task_id));
    assert_eq!(out["bridge"]["source_scope_hint"], json!("group:crew-7"));

    cleanup_dir(&dir);
}

#[test]
fn udp_announce_startup_and_listener_paths_execute() {
    let _guard = env_lock();
    let dir = temp_test_dir("udp-announce");

    let payload = serde_json::json!({
        "kind": "wattswarm_udp_announce_v1",
        "node_id": "node-peer",
        "listen_addr": "/ip4/127.0.0.1/tcp/4001"
    })
    .to_string();

    let mut found = false;
    for _ in 0..5 {
        let probe = UdpSocket::bind("127.0.0.1:0").expect("bind probe udp socket");
        let port = probe.local_addr().expect("probe local addr").port();
        drop(probe);

        with_udp_env("1", "broadcast", "127.0.0.1", port);
        announce_startup("kernel", Some("127.0.0.1:7788"), Some("node-self"));
        maybe_start_listener(dir.clone(), "node-self".to_owned());

        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind sender socket");
        let _ = sender.send_to(payload.as_bytes(), ("127.0.0.1", port));

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(700) {
            if let Ok(records) = load_discovered_peer_records_state(&dir)
                && records
                    .iter()
                    .any(|record| record.node_id == "node-peer" && record.source_kind == "udp")
            {
                found = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        if found {
            break;
        }
    }
    assert!(found, "listener should persist discovered peer");

    clear_udp_env();
    cleanup_dir(&dir);
}

#[test]
fn udp_listener_noop_when_disabled() {
    let _guard = env_lock();
    let dir = temp_test_dir("udp-disabled");

    with_udp_env("0", "broadcast", "127.0.0.1", 37931);
    maybe_start_listener(dir.clone(), "node-self".to_owned());

    thread::sleep(Duration::from_millis(100));
    let path = discovered_peers_path(&dir);
    assert!(!path.exists());

    clear_udp_env();
    cleanup_dir(&dir);
}
