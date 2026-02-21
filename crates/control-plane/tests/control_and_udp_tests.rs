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
    ExecutorRegistry, ExecutorRegistryEntry, RealTaskRunRequest, add_discovered_peer,
    discovered_peers_path, executor_registry_path, load_discovered_peers, load_executor_registry,
    local_node_id, open_node, run_real_task_flow, save_discovered_peers, save_executor_registry,
};
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::udp_announce::{announce_startup, maybe_start_listener};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
        // SAFETY: tests serialize env mutations via ENV_LOCK.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: tests serialize env mutations via ENV_LOCK.
        unsafe {
            if let Some(prev) = &self.prev {
                std::env::set_var(self.key, prev);
            } else {
                std::env::remove_var(self.key);
            }
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
    assert!(!add_discovered_peer(&dir, "   ").expect("add empty"));

    let peers = load_discovered_peers(&discovered_peers_path(&dir)).expect("load peers");
    assert_eq!(peers, vec!["node-a", "node-b", "node-c"]);

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
fn run_real_task_flow_returns_clear_error_when_executor_missing() {
    let dir = temp_test_dir("run-real-missing-executor");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.db");

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
    let dir = temp_test_dir("run-real-unsupported-profile");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.db");
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
    let dir = temp_test_dir("run-real-task-file-parse");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.db");
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
    assert!(
        err.to_string().contains("parse task contract from")
            && err.to_string().contains("broken-task.json")
    );

    cleanup_dir(&dir);
}

#[test]
fn run_real_task_flow_completes_with_stub_runtime() {
    let _env_lock = env_lock();
    let _pg_iso = EnvVarGuard::set("WATTSWARM_PG_ISOLATE_BY_PATH", "1");

    let dir = temp_test_dir("run-real-success");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.db");
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
        "node_id": "node-peer"
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
            if let Ok(peers) = load_discovered_peers(&peer_path)
                && peers.iter().any(|p| p == "node-peer")
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
