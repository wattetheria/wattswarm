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
use wattswarm_control_plane::control::{
    DiscoveredPeerRecord, ExecutorRegistry, ExecutorRegistryEntry, RealTaskRunRequest,
    add_discovered_peer, add_discovered_peer_endpoint, discovered_peers_path,
    executor_registry_path, load_discovered_peer_records, load_discovered_peers,
    load_executor_registry, local_node_id, open_node, run_real_task_flow, save_discovered_peers,
    save_executor_registry,
};
use wattswarm_control_plane::storage::storage::pg::Connection;
use wattswarm_control_plane::task_template::sample_contract;
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
                        let mut buf = [0_u8; 8192];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        if n == 0 {
                            continue;
                        }
                        let req = String::from_utf8_lossy(&buf[..n]);
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

    let bootstrap_store = wattswarm_control_plane::storage::PgStore::open(&db_path)
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
    drop(conn);
    drop(bootstrap_store);

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
