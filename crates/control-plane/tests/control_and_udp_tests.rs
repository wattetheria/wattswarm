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
use wattswarm_control_plane::artifact_store::{ArtifactAvailabilityStatus, ArtifactKind};
use wattswarm_control_plane::control::{
    DiscoveredPeerRecord, ExecutorRegistry, ExecutorRegistryEntry, RealTaskRunRequest,
    RemoteTaskBridgeRegistry, RemoteTaskBridgeRequest, add_discovered_peer,
    add_discovered_peer_endpoint, artifact_store_path, bridge_remote_task_into_local_execution,
    discovered_peers_path, executor_registry_path, fetch_checkpoint_artifact_json,
    fetch_evidence_artifact, fetch_snapshot_artifact_json, fetch_task_detail_artifact,
    list_artifacts_needing_repair, load_discovered_peer_records, load_discovered_peers,
    load_executor_registry, local_node_id, materialize_checkpoint_artifact_json,
    materialize_evidence_artifact, materialize_snapshot_artifact_json,
    materialize_task_detail_artifact, open_node, open_node_on_network_id,
    remote_task_bridge_registry_path, run_real_task_flow, save_discovered_peers,
    save_executor_registry,
};
use wattswarm_control_plane::crypto::{NodeIdentity, sha256_hex};
use wattswarm_control_plane::storage::storage::pg::Connection;
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::types::{
    Membership, NetworkKind, Role, SignedNetworkProtocolParamsEnvelope, TaskAnnouncedPayload,
    UnsignedEvent,
};
use wattswarm_control_plane::udp_announce::{announce_startup, maybe_start_listener};

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
    let _ = fs::remove_dir_all(path);
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
                listen_addr: None,
            },
            DiscoveredPeerRecord {
                node_id: "node-a".to_owned(),
                listen_addr: None,
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

    let peers = load_discovered_peers(&discovered_peers_path(&dir)).expect("load peers");
    assert_eq!(peers, vec!["node-a", "node-b", "node-c"]);
    let records =
        load_discovered_peer_records(&discovered_peers_path(&dir)).expect("load peer records");
    assert_eq!(
        records,
        vec![
            DiscoveredPeerRecord {
                node_id: "node-a".to_owned(),
                listen_addr: Some("/ip4/127.0.0.1/tcp/4001".to_owned()),
            },
            DiscoveredPeerRecord {
                node_id: "node-b".to_owned(),
                listen_addr: None,
            },
            DiscoveredPeerRecord {
                node_id: "node-c".to_owned(),
                listen_addr: None,
            }
        ]
    );

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
        }],
    };
    save_executor_registry(&path, &reg).expect("save registry");
    let loaded = load_executor_registry(&path).expect("load registry");
    assert_eq!(loaded.entries.len(), 1);
    assert_eq!(loaded.entries[0].name, "rt");

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
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-artifact-1".to_owned(),
        execution_id: "exec-artifact-detail".to_owned(),
        output: json!({"answer":"ok"}),
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

    save_executor_registry(
        &executor_registry_path(&state_dir),
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "rt".to_owned(),
                base_url: stub.base_url(),
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

    save_executor_registry(
        &executor_registry_path(&state_dir),
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "rt".to_owned(),
                base_url: stub.base_url(),
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

    save_executor_registry(
        &executor_registry_path(&state_dir),
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "rt".to_owned(),
                base_url: stub.base_url(),
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

    save_executor_registry(
        &executor_registry_path(&state_dir),
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "rt".to_owned(),
                base_url: stub.base_url(),
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
            executor: "rt".to_owned(),
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

    let registry: RemoteTaskBridgeRegistry = serde_json::from_slice(
        &fs::read(remote_task_bridge_registry_path(&state_dir)).expect("read registry"),
    )
    .expect("parse registry");
    assert_eq!(registry.entries.len(), 1);
    assert_eq!(
        registry.entries[0].detail_ref_digest.as_deref(),
        Some(digest.as_str())
    );

    let deduped = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "rt".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("dedupe bridged task");
    assert_eq!(deduped["bridge"]["deduped"], json!(true));
    assert_eq!(deduped["candidate_id"], out["candidate_id"]);

    let registry_after: RemoteTaskBridgeRegistry = serde_json::from_slice(
        &fs::read(remote_task_bridge_registry_path(&state_dir)).expect("read registry after"),
    )
    .expect("parse registry after");
    assert_eq!(registry_after.entries.len(), 1);

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

    save_executor_registry(
        &executor_registry_path(&state_dir),
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "rt".to_owned(),
                base_url: stub.base_url(),
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
            executor: "rt".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect_err("bridge should reject mismatched node scope");
    assert!(err.to_string().contains("not eligible for local node"));
    assert!(
        !remote_task_bridge_registry_path(&state_dir).exists(),
        "rejected bridge should not persist dedupe registry"
    );

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

    let peer_path = discovered_peers_path(&dir);
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
            if let Ok(records) = load_discovered_peer_records(&peer_path)
                && records.iter().any(|record| {
                    record.node_id == "node-peer"
                        && record.listen_addr.as_deref() == Some("/ip4/127.0.0.1/tcp/4001")
                })
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
