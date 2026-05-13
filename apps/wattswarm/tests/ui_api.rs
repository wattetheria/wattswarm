use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tonic::Request as GrpcRequest;
use tower::ServiceExt;
use wattswarm::control::open_node;
use wattswarm::control::{
    ExecutorRegistry, ExecutorRegistryEntry, bridge_remote_task_into_local_execution,
    materialize_task_detail_artifact, save_executor_registry_state,
};
use wattswarm::crypto::NodeIdentity;
use wattswarm::node::build_event_for_external;
use wattswarm::run_control;
use wattswarm::run_queue::WorkerOptions;
use wattswarm::task_template::sample_contract;
use wattswarm::types::NetworkProtocolParams;
use wattswarm::types::{
    EventPayload, FeedSubscriptionUpdatedPayload, Membership, Role, TaskAnnouncedPayload,
    TopicMessagePostedPayload, UnsignedEvent,
};
use wattswarm::ui::{UiServerState, build_app};
use wattswarm::wattetheria_sync;
use wattswarm::wattetheria_sync::proto::wattetheria_sync_service_client::WattetheriaSyncServiceClient;
use wattswarm::wattetheria_sync::proto::{ProjectionStreamRequest, UpdateStartupGeoRequest};
use wattswarm_network_discovery::{
    DEFAULT_RECORD_TTL_MS, DiscoveryGeo, DiscoveryNodeRecordBody, DiscoveryRecordCapabilities,
    SignedDiscoveryNodeRecord,
};
use wattswarm_storage_core::storage::pg::Connection;
use wattswarm_storage_core::types::ArtifactRef;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const TEST_DB_LOCK_KEY: i64 = 1_987_654_321;
static TEST_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(1);

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn topic_content_ref(tag: &str, producer: &str, created_at: u64) -> ArtifactRef {
    ArtifactRef {
        uri: format!("artifact://topic-message/sha256:{tag}"),
        digest: format!("sha256:{tag}"),
        size_bytes: 128,
        mime: "application/json".to_owned(),
        created_at,
        producer: producer.to_owned(),
    }
}

fn next_test_schema(prefix: &str) -> String {
    let id = TEST_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sanitized = prefix
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{sanitized}_{}_{}", std::process::id(), id)
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

struct DbTestLock {
    conn: Connection,
}

#[derive(Clone)]
struct UiStubRuntimeConfig {
    health_body: String,
    capabilities_body: String,
    execute_body: String,
    execute_requests: Option<Arc<Mutex<Vec<Value>>>>,
}

struct UiStubRuntimeServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

struct DiscoveryRecordStubServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl UiStubRuntimeServer {
    fn start(cfg: UiStubRuntimeConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking listener");
        let addr = listener.local_addr().expect("listener local addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => handle_stub_conn(stream, &cfg),
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
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

impl Drop for UiStubRuntimeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl DiscoveryRecordStubServer {
    fn start() -> Self {
        Self::start_with_delay(Duration::ZERO)
    }

    fn start_with_delay(response_delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind discovery stub listener");
        listener
            .set_nonblocking(true)
            .expect("set discovery stub nonblocking");
        let addr = listener
            .local_addr()
            .expect("discovery listener local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        handle_discovery_stub_conn(stream, &request_log, response_delay)
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            requests,
            stop,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for DiscoveryRecordStubServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn wait_for_stub_listener(server: &UiStubRuntimeServer) {
    for _ in 0..50 {
        if TcpStream::connect(server.addr).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("stub listener did not become reachable in time");
}

fn reserve_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free local addr");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

fn write_stub_response(mut stream: TcpStream, status: u16, body: &str) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        status_text,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn read_stub_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = vec![0_u8; 8192];
    let mut offset = 0_usize;
    let mut content_length = None;
    loop {
        if offset == buf.len() {
            buf.resize(buf.len().saturating_mul(2), 0);
        }
        match stream.read(&mut buf[offset..]) {
            Ok(0) => {
                if offset == 0 {
                    return Ok(Vec::new());
                }
                break;
            }
            Ok(n) => {
                offset += n;
                let req = &buf[..offset];
                if content_length.is_none()
                    && let Some(headers_end) =
                        req.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let header_text = String::from_utf8_lossy(&req[..headers_end]);
                    content_length = header_text
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            if name.eq_ignore_ascii_case("content-length") {
                                value.trim().parse::<usize>().ok()
                            } else {
                                None
                            }
                        })
                        .or(Some(0));
                }
                if let Some(body_len) = content_length
                    && let Some(headers_end) =
                        req.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let expected_len = headers_end + 4 + body_len;
                    if offset >= expected_len {
                        break;
                    }
                }
            }
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                break;
            }
            Err(err) => return Err(err),
        }
    }
    buf.truncate(offset);
    Ok(buf)
}

fn handle_stub_conn(mut stream: TcpStream, cfg: &UiStubRuntimeConfig) {
    let Ok(req_bytes) = read_stub_request(&mut stream) else {
        return;
    };
    if req_bytes.is_empty() {
        return;
    }
    let req = String::from_utf8_lossy(&req_bytes);
    let line = req.lines().next().unwrap_or_default();
    if line.starts_with("GET /health ") {
        return write_stub_response(stream, 200, &cfg.health_body);
    }
    if line.starts_with("GET /capabilities ") {
        return write_stub_response(stream, 200, &cfg.capabilities_body);
    }
    if line.starts_with("POST /execute ") {
        let mut execute_body = cfg.execute_body.clone();
        if let Some(raw_body) = req.split("\r\n\r\n").nth(1)
            && let (Ok(request_json), Ok(mut response_json)) = (
                serde_json::from_str::<Value>(raw_body),
                serde_json::from_str::<Value>(&cfg.execute_body),
            )
        {
            if let Some(log) = &cfg.execute_requests
                && let Ok(mut requests) = log.lock()
            {
                requests.push(request_json.clone());
            }
            if let Some(source_message_id) = request_json
                .get("inputs")
                .and_then(|value| value.get("source_message"))
                .and_then(|value| value.get("message_id"))
                .and_then(Value::as_str)
            {
                if response_json
                    .get("candidate_output")
                    .and_then(|value| value.get("source_message_id"))
                    .and_then(Value::as_str)
                    == Some("placeholder-overridden-by-test")
                {
                    response_json["candidate_output"]["source_message_id"] =
                        json!(source_message_id);
                }
                if let Some(proposal_message_id) = request_json
                    .get("inputs")
                    .and_then(|value| value.get("candidate_proposals"))
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|value| value.get("proposal_message_id"))
                    .and_then(Value::as_str)
                {
                    if response_json
                        .get("candidate_output")
                        .and_then(|value| value.get("proposal_message_id"))
                        .and_then(Value::as_str)
                        == Some("placeholder-proposal-message-id")
                    {
                        response_json["candidate_output"]["proposal_message_id"] =
                            json!(proposal_message_id);
                    }
                }
                if let Some(first_evidence) = response_json
                    .get_mut("candidate_output")
                    .and_then(|value| value.get_mut("evidence"))
                    .and_then(Value::as_array_mut)
                    .and_then(|items| items.first_mut())
                {
                    if first_evidence.get("message_id").and_then(Value::as_str)
                        == Some("placeholder-overridden-by-test")
                    {
                        first_evidence["message_id"] = json!(source_message_id);
                    }
                }
                execute_body = response_json.to_string();
            }
        }
        return write_stub_response(stream, 200, &execute_body);
    }
    if line.starts_with("POST /verify ") {
        return write_stub_response(
            stream,
            200,
            "{\"passed\":true,\"score\":1.0,\"reason_codes\":[100],\"verifier_result_hash\":\"vr-stub\",\"provider_family\":\"stub\",\"model_id\":\"stub-1\"}",
        );
    }
    write_stub_response(stream, 404, "{}")
}

fn handle_discovery_stub_conn(
    mut stream: TcpStream,
    requests: &Arc<Mutex<Vec<String>>>,
    response_delay: Duration,
) {
    let Ok(req_bytes) = read_stub_request(&mut stream) else {
        return;
    };
    if req_bytes.is_empty() {
        return;
    }
    let req = String::from_utf8_lossy(&req_bytes);
    let line = req.lines().next().unwrap_or_default();
    if line.starts_with("POST /api/network/discovery/records ") {
        if let Some(raw_body) = req.split("\r\n\r\n").nth(1)
            && let Ok(mut requests) = requests.lock()
        {
            requests.push(raw_body.to_owned());
        }
        std::thread::sleep(response_delay);
        return write_stub_response(stream, 200, "{\"ok\":true}");
    }
    write_stub_response(stream, 404, "{}")
}

impl DbTestLock {
    fn acquire() -> Self {
        let conn = Connection::open("ui-db-lock").expect("open db lock connection");
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

fn reset_test_schema(schema: &str) -> String {
    let schema = next_test_schema(schema);
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
    .expect("reset ui test schema");
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        if let Some(value) = prev_schema {
            std::env::set_var("WATTSWARM_PG_SCHEMA", value);
        } else {
            std::env::remove_var("WATTSWARM_PG_SCHEMA");
        }
    }
    schema
}

fn count_projection_rows(table: &str) -> i64 {
    let conn = Connection::open("ui-count-projection-rows").expect("open pg connection");
    let schema = std::env::var("WATTSWARM_PG_SCHEMA").unwrap_or_else(|_| "public".to_owned());
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            wattswarm_storage_core::params![schema, table],
            |row| row.get::<_, i64>(0),
        )
        .expect("check projection table existence");
    if table_exists == 0 {
        return 0;
    }
    conn.query_row(
        &format!("SELECT COUNT(*) FROM {schema}.{table}"),
        wattswarm_storage_core::params![],
        |row| row.get::<_, i64>(0),
    )
    .expect("count projection rows")
}

async fn json_from(res: axum::response::Response) -> Value {
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn network_discovery_auto_announces_local_record_to_bootnode() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("ui_discovery_auto_announce");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = tempdir().expect("tempdir");
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    wattswarm::startup_config::save_startup_config(
        &wattswarm::startup_config::startup_config_path(&state_dir),
        &wattswarm::startup_config::StartupConfig {
            latitude: Some(37.0),
            longitude: Some(-122.0),
            network_mode: wattswarm::startup_config::NetworkMode::Local,
            ..Default::default()
        },
    )
    .expect("save startup config");
    let stub = DiscoveryRecordStubServer::start();
    wattswarm::control::save_discovery_bootnode_urls_state(&state_dir, &[stub.base_url()])
        .expect("save discovery bootnode urls");

    let report = wattswarm::ui::maybe_announce_local_record_to_discovery_bootnodes(
        &state_dir,
        &state_dir.join("local.state"),
    )
    .expect("announce local discovery record");

    assert_eq!(report.attempted, 1);
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.failed, 0);
    let requests = stub.requests.lock().expect("discovery requests");
    assert_eq!(requests.len(), 1);
    let record: SignedDiscoveryNodeRecord =
        serde_json::from_str(&requests[0]).expect("parse signed discovery record");
    record.verify().expect("verify discovery record");
    assert_eq!(record.body.geo.as_ref().map(|geo| geo.latitude), Some(37.0));
    assert_eq!(
        record.body.geo.as_ref().map(|geo| geo.longitude),
        Some(-122.0)
    );
    assert_eq!(
        record.body.geo.as_ref().map(|geo| geo.radius_km),
        Some(1000.0)
    );
    assert_eq!(record.body.ttl_ms, DEFAULT_RECORD_TTL_MS);
    assert!(record.body.capabilities.contains("wattswarm.node"));
    assert!(record.body.transport_contact.is_some());
}

#[test]
fn network_discovery_auto_announces_local_record_to_discovery_api_base_url() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("ui_discovery_auto_announce_base_url");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = tempdir().expect("tempdir");
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    wattswarm::startup_config::save_startup_config(
        &wattswarm::startup_config::startup_config_path(&state_dir),
        &wattswarm::startup_config::StartupConfig {
            latitude: Some(37.0),
            longitude: Some(-122.0),
            network_mode: wattswarm::startup_config::NetworkMode::Local,
            ..Default::default()
        },
    )
    .expect("save startup config");
    let stub = DiscoveryRecordStubServer::start();
    wattswarm::control::save_discovery_bootnode_urls_state(
        &state_dir,
        &[format!("{}/api/network/discovery", stub.base_url())],
    )
    .expect("save discovery bootnode urls");

    let report = wattswarm::ui::maybe_announce_local_record_to_discovery_bootnodes(
        &state_dir,
        &state_dir.join("local.state"),
    )
    .expect("announce local discovery record");

    assert_eq!(report.attempted, 1);
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.failed, 0);
    let requests = stub.requests.lock().expect("discovery requests");
    assert_eq!(requests.len(), 1);
}

#[test]
fn network_discovery_bootnode_announce_does_not_block_startup_path() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("ui_discovery_nonblocking_startup");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = tempdir().expect("tempdir");
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    wattswarm::startup_config::save_startup_config(
        &wattswarm::startup_config::startup_config_path(&state_dir),
        &wattswarm::startup_config::StartupConfig {
            latitude: Some(37.0),
            longitude: Some(-122.0),
            network_mode: wattswarm::startup_config::NetworkMode::Local,
            ..Default::default()
        },
    )
    .expect("save startup config");
    let stub = DiscoveryRecordStubServer::start_with_delay(Duration::from_millis(250));
    wattswarm::control::save_discovery_bootnode_urls_state(&state_dir, &[stub.base_url()])
        .expect("save discovery bootnode urls");

    let started = Instant::now();
    let handle = wattswarm::ui::spawn_discovery_bootnode_announce(
        state_dir.clone(),
        state_dir.join("local.state"),
    );
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "discovery announce must not block UI startup"
    );
    handle
        .join()
        .expect("discovery announce thread should finish");

    let requests = stub.requests.lock().expect("discovery requests");
    assert_eq!(requests.len(), 1);
}

fn sample_run_spec(run_id: &str) -> Value {
    json!({
        "run_id": run_id,
        "task_type": "resume_review",
        "shared_inputs": {
            "candidate": "Alex Example",
            "city": "San Francisco"
        },
        "agents": [
            {
                "agent_id": "CTO",
                "executor": "rt-ui",
                "profile": "default",
                "prompt": "review candidate from CTO perspective"
            },
            {
                "agent_id": "HR",
                "executor": "rt-ui",
                "profile": "default",
                "prompt": "review candidate from HR perspective"
            }
        ],
        "retry": {
            "max_attempts": 2,
            "backoff_ms": 250
        },
        "aggregation": {
            "mode": "all_done"
        }
    })
}

#[path = "ui_api/ui_api_agent_sync_tests.rs"]
mod ui_api_agent_sync_tests;
#[path = "ui_api/ui_api_basic_tests.rs"]
mod ui_api_basic_tests;
#[path = "ui_api/ui_api_runqueue_tests.rs"]
mod ui_api_runqueue_tests;
#[path = "ui_api/ui_api_startup_tests.rs"]
mod ui_api_startup_tests;
#[path = "ui_api/ui_api_tail_tests.rs"]
mod ui_api_tail_tests;
#[path = "ui_api/ui_api_topic_tests.rs"]
mod ui_api_topic_tests;
