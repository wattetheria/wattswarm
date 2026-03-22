use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;
use tower::ServiceExt;
use wattswarm::control::open_node;
use wattswarm::types::{EventPayload, FeedSubscriptionUpdatedPayload, TopicMessagePostedPayload};
use wattswarm::ui::{UiServerState, build_app};
use wattswarm_storage_core::storage::pg::Connection;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const TEST_DB_LOCK_KEY: i64 = 1_987_654_321;

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
}

struct UiStubRuntimeServer {
    addr: SocketAddr,
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

fn wait_for_stub_listener(server: &UiStubRuntimeServer) {
    for _ in 0..50 {
        if TcpStream::connect(server.addr).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("stub listener did not become reachable in time");
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

fn handle_stub_conn(mut stream: TcpStream, cfg: &UiStubRuntimeConfig) {
    let mut buf = [0_u8; 8192];
    let n = stream.read(&mut buf).unwrap_or(0);
    if n == 0 {
        return;
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let line = req.lines().next().unwrap_or_default();
    if line.starts_with("GET /health ") {
        return write_stub_response(stream, 200, &cfg.health_body);
    }
    if line.starts_with("GET /capabilities ") {
        return write_stub_response(stream, 200, &cfg.capabilities_body);
    }
    if line.starts_with("POST /execute ") {
        return write_stub_response(stream, 200, &cfg.execute_body);
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
    .expect("reset ui test schema");
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        if let Some(value) = prev_schema {
            std::env::set_var("WATTSWARM_PG_SCHEMA", value);
        } else {
            std::env::remove_var("WATTSWARM_PG_SCHEMA");
        }
    }
}

async fn json_from(res: axum::response::Response) -> Value {
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
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

#[test]
fn ui_supports_core_cli_operations() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let db_path = state_dir.join("ui.state");
        let app = build_app(UiServerState::new(state_dir.clone(), db_path));

        let up_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/node/up")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(up_res.status(), StatusCode::OK);

        let status_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/node/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let status_json = json_from(status_res).await;
        assert_eq!(status_json["running"].as_bool(), Some(true));

        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"rt-ui","base_url":"http://127.0.0.1:8787"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let list_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/executors/list")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_exec_res.status(), StatusCode::OK);
        let list_json = json_from(list_exec_res).await;
        assert_eq!(list_json["executors"][0]["name"].as_str(), Some("rt-ui"));

        let sample_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/sample?task_id=task-ui-e2e")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sample_res.status(), StatusCode::OK);
        let sample_json = json_from(sample_res).await;
        let contract = sample_json["contract"].clone();

        let submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/task/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"contract": contract})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_res.status(), StatusCode::OK);

        let watch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/task/watch/task-ui-e2e")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(watch_res.status(), StatusCode::OK);
        let watch_json = json_from(watch_res).await;
        assert_eq!(watch_json["task_id"].as_str(), Some("task-ui-e2e"));

        let by_task_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/knowledge/export")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"task_id":"task-ui-e2e"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(by_task_res.status(), StatusCode::OK);

        let swarm_page_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/swarm")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(swarm_page_res.status(), StatusCode::OK);

        let swarm_state_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/swarm/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(swarm_state_res.status(), StatusCode::OK);
        let swarm_state_json = json_from(swarm_state_res).await;
        assert!(swarm_state_json["state"]["signals"].is_array());
    });
}

#[test]
fn ui_exposes_topic_message_history_and_cursor_queries() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let subscriber_node_id = {
        let mut node = open_node(&state_dir, &db_path).expect("open node");
        let subscriber_node_id = node.node_id();
        let network_id = format!("local:{subscriber_node_id}");
        node.emit_at(
            1,
            EventPayload::FeedSubscriptionUpdated(FeedSubscriptionUpdatedPayload {
                network_id: network_id.clone(),
                subscriber_node_id: subscriber_node_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                active: true,
            }),
            100,
        )
        .expect("emit topic subscription");
        node.emit_at(
            1,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id: network_id.clone(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content: serde_json::json!({"text":"hello crew"}),
                reply_to_message_id: None,
            }),
            110,
        )
        .expect("emit first topic message");
        node.emit_at(
            1,
            EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                network_id,
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                content: serde_json::json!({"text":"second ping"}),
                reply_to_message_id: None,
            }),
            120,
        )
        .expect("emit second topic message");
        node.store
            .upsert_topic_cursor(&subscriber_node_id, "crew.chat", "group:crew-7", 22, 130)
            .expect("upsert topic cursor");
        subscriber_node_id
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let up_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/node/up")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(up_res.status(), StatusCode::OK);

        let first_page_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/messages?feed_key=crew.chat&scope_hint=group:crew-7&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_page_json = json_from(first_page_res).await;
        assert_eq!(
            first_page_json["ok"].as_bool(),
            Some(true),
            "topic messages first page failed: {first_page_json}"
        );
        assert_eq!(first_page_json["messages"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            first_page_json["messages"][0]["content"]["text"].as_str(),
            Some("second ping")
        );
        let before_created_at = first_page_json["next_anchor"]["before_created_at"]
            .as_u64()
            .expect("next anchor created_at");
        let before_message_id = first_page_json["next_anchor"]["before_message_id"]
            .as_str()
            .expect("next anchor message id");

        let second_page_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/topic/messages?feed_key=crew.chat&scope_hint=group:crew-7&limit=2&before_created_at={before_created_at}&before_message_id={before_message_id}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_page_json = json_from(second_page_res).await;
        assert_eq!(
            second_page_json["ok"].as_bool(),
            Some(true),
            "topic messages second page failed: {second_page_json}"
        );
        assert_eq!(second_page_json["messages"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            second_page_json["messages"][0]["content"]["text"].as_str(),
            Some("hello crew")
        );

        let cursor_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/cursor?feed_key=crew.chat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cursor_json = json_from(cursor_res).await;
        assert_eq!(
            cursor_json["ok"].as_bool(),
            Some(true),
            "topic cursor failed: {cursor_json}"
        );
        assert_eq!(
            cursor_json["subscriber_node_id"].as_str(),
            Some(subscriber_node_id.as_str())
        );
        assert_eq!(cursor_json["cursor"]["last_event_seq"].as_u64(), Some(22));
        assert_eq!(
            cursor_json["cursor"]["scope_hint"].as_str(),
            Some("group:crew-7")
        );
    });
}

#[test]
fn ui_accepts_topic_subscription_and_message_writes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let subscription_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/subscriptions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "active": true
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(subscription_res.status(), StatusCode::OK);

        let message_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "feed_key": "crew.chat",
                            "scope_hint": "group:crew-7",
                            "content": {"text": "hello from ui write api"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(message_res.status(), StatusCode::OK);

        let messages_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/messages?feed_key=crew.chat&scope_hint=group:crew-7&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let messages_json = json_from(messages_res).await;
        assert_eq!(messages_json["ok"].as_bool(), Some(true));
        assert_eq!(
            messages_json["messages"][0]["content"]["text"].as_str(),
            Some("hello from ui write api")
        );

        let cursor_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/topic/cursor?feed_key=crew.chat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cursor_json = json_from(cursor_res).await;
        assert_eq!(cursor_json["ok"].as_bool(), Some(true));
        assert!(cursor_json["subscriber_node_id"].as_str().is_some());
    });
}

#[test]
fn ui_exposes_run_queue_http_apis() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let run_id = "run-ui-http";
    let retry_run_id = "run-ui-http-retry";

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"rt-ui","base_url":"http://127.0.0.1:8787"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/run/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "kickoff": true,
                            "spec": sample_run_spec(run_id),
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_res.status(), StatusCode::OK);
        let submit_json = json_from(submit_res).await;
        assert_eq!(submit_json["run_id"].as_str(), Some(run_id));
        assert_eq!(submit_json["kicked_off"].as_bool(), Some(true));

        let watch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/watch/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(watch_res.status(), StatusCode::OK);
        let watch_json = json_from(watch_res).await;
        assert_eq!(watch_json["watch"]["run_id"].as_str(), Some(run_id));
        assert_eq!(watch_json["watch"]["status"].as_str(), Some("QUEUED"));
        assert_eq!(watch_json["watch"]["counts"]["queued"].as_i64(), Some(2));

        let result_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/result/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result_res.status(), StatusCode::OK);
        let result_json = json_from(result_res).await;
        assert_eq!(result_json["result"]["run_id"].as_str(), Some(run_id));
        assert_eq!(result_json["result"]["status"].as_str(), Some("QUEUED"));

        let events_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/events/{run_id}?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events_res.status(), StatusCode::OK);
        let events_json = json_from(events_res).await;
        let event_types: Vec<_> = events_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["event_type"].as_str())
            .collect();
        assert!(event_types.contains(&"RUN_CREATED"));
        assert!(event_types.contains(&"RUN_KICKOFF"));

        let cancel_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/run/cancel/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel_res.status(), StatusCode::OK);

        let cancelled_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/result/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cancelled_json = json_from(cancelled_res).await;
        assert_eq!(
            cancelled_json["result"]["status"].as_str(),
            Some("CANCELLED")
        );

        let retry_submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/run/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&sample_run_spec(retry_run_id)).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_submit_res.status(), StatusCode::OK);

        let retry_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/run/retry/{retry_run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_res.status(), StatusCode::OK);

        let retry_events_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/events/{retry_run_id}?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let retry_events_json = json_from(retry_events_res).await;
        let retry_event_types: Vec<_> = retry_events_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["event_type"].as_str())
            .collect();
        assert!(retry_event_types.contains(&"RUN_RETRY_REQUESTED"));
    });
}

#[test]
fn ui_exposes_egress_agent_config_and_google_a2a_card() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let get_default_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/egress-agent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_default_res.status(), StatusCode::OK);
        let get_default_json = json_from(get_default_res).await;
        assert_eq!(
            get_default_json["config"]["agent_id"].as_str(),
            Some("egress-agent")
        );
        assert_eq!(get_default_json["config"]["enabled"].as_bool(), Some(false));

        let disabled_well_known_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/.well-known/agent.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disabled_well_known_res.status(), StatusCode::BAD_REQUEST);

        let save_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/egress-agent")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "enabled": true,
                            "agent_id": "twilio-egress",
                            "display_name": "Twilio Bridge",
                            "description": "Bridges outbound SMS requests through a local Twilio adapter.",
                            "protocol": "google_a2a",
                            "mode": "direct_gateway",
                            "executor": "twilio-http",
                            "profile": "default",
                            "public_base_url": "https://node.example.com",
                            "publish_to_network": true,
                            "accept_inbound_invocations": true,
                            "skills": [
                                {
                                    "id": "send_sms",
                                    "name": "Send SMS",
                                    "description": "Sends outbound SMS via a local adapter.",
                                    "tags": ["twilio", "sms"]
                                }
                            ]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);
        let save_json = json_from(save_res).await;
        assert_eq!(save_json["config"]["enabled"].as_bool(), Some(true));
        assert_eq!(
            save_json["config"]["executor"].as_str(),
            Some("twilio-http")
        );

        let get_saved_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/egress-agent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let get_saved_json = json_from(get_saved_res).await;
        assert_eq!(
            get_saved_json["config"]["agent_id"].as_str(),
            Some("twilio-egress")
        );
        assert_eq!(
            get_saved_json["config"]["skills"][0]["id"].as_str(),
            Some("send_sms")
        );

        let card_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/a2a/google/agent-card")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(card_res.status(), StatusCode::OK);
        let card_json = json_from(card_res).await;
        assert_eq!(card_json["protocol"].as_str(), Some("google_a2a"));
        assert_eq!(card_json["name"].as_str(), Some("Twilio Bridge"));
        assert_eq!(
            card_json["url"].as_str(),
            Some("https://node.example.com/a2a/google")
        );
        assert_eq!(
            card_json["metadata"]["agent_id"].as_str(),
            Some("twilio-egress")
        );
        assert_eq!(
            card_json["skills"][0]["id"].as_str(),
            Some("send_sms")
        );

        let well_known_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/.well-known/agent.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(well_known_res.status(), StatusCode::OK);
        let well_known_json = json_from(well_known_res).await;
        assert_eq!(well_known_json, card_json);
    });
}

#[test]
fn ui_google_a2a_message_send_supports_direct_and_group_modes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path));
    let runtime_server = UiStubRuntimeServer::start(UiStubRuntimeConfig {
        health_body: "{}".to_owned(),
        capabilities_body: json!({
            "task_types": ["a2a_invoke"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
        execute_body: json!({
            "candidate_output": {"decision":"SENT","answer":"sent"},
            "evidence_inline": [],
            "evidence_refs": []
        })
        .to_string(),
    });
    wait_for_stub_listener(&runtime_server);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let disabled_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/a2a/google/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": "req-disabled",
                            "method": "message/send",
                            "params": {
                                "targetAgentId": "egress-agent",
                                "capability": "send_sms",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type":"data","data":{"to":"+61","body":"hello"}}]
                                }
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disabled_res.status(), StatusCode::BAD_REQUEST);

        let save_direct_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/egress-agent")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "enabled": true,
                            "agent_id": "twilio-egress",
                            "display_name": "Twilio Bridge",
                            "description": "Bridge agent",
                            "protocol": "google_a2a",
                            "mode": "direct_gateway",
                            "executor": "twilio-http",
                            "profile": "default",
                            "public_base_url": "https://node.example.com",
                            "publish_to_network": true,
                            "accept_inbound_invocations": true,
                            "skills": [{"id":"send_sms","name":"Send SMS"}]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_direct_res.status(), StatusCode::OK);

        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "name": "twilio-http",
                            "base_url": runtime_server.base_url()
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let direct_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/a2a/google/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": "req-direct",
                            "method": "message/send",
                            "params": {
                                "targetAgentId": "twilio-egress",
                                "capability": "send_sms",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type":"data","data":{"to":"+61400000000","body":"hello"}}]
                                },
                                "extensions": {
                                    "auth_proof": {"kind":"did"},
                                    "payment_proof": {"kind":"watt"},
                                    "signature": "sig-1"
                                }
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let direct_status = direct_res.status();
        let direct_bytes = to_bytes(direct_res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            direct_status,
            StatusCode::OK,
            "direct gateway body: {}",
            String::from_utf8_lossy(&direct_bytes)
        );
        let direct_json: Value = serde_json::from_slice(&direct_bytes).unwrap();
        assert_eq!(direct_json["jsonrpc"].as_str(), Some("2.0"));
        assert_eq!(direct_json["id"].as_str(), Some("req-direct"));
        assert_eq!(direct_json["result"]["status"].as_str(), Some("completed"));
        assert_eq!(
            direct_json["result"]["extensions"]["receipt"]["mode"].as_str(),
            Some("direct_gateway")
        );
        assert_eq!(
            direct_json["result"]["artifacts"][0]["candidate_output"]["decision"].as_str(),
            Some("SENT")
        );

        let save_group_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/egress-agent")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "enabled": true,
                            "agent_id": "team-egress",
                            "display_name": "Team Bridge",
                            "protocol": "google_a2a",
                            "mode": "group_representative",
                            "profile": "default",
                            "public_base_url": "https://node.example.com"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_group_res.status(), StatusCode::OK);

        let group_run_id = "run-a2a-group";
        let group_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/a2a/google/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": "req-group",
                            "method": "message/send",
                            "params": {
                                "targetAgentId": "team-egress",
                                "capability": "resume_review",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type":"data","data":{"resume":"alex"}}]
                                },
                                "extensions": {
                                    "kickoff": true,
                                    "run_spec": sample_run_spec(group_run_id)
                                }
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(group_res.status(), StatusCode::OK);
        let group_json = json_from(group_res).await;
        assert_eq!(group_json["id"].as_str(), Some("req-group"));
        assert_eq!(group_json["result"]["status"].as_str(), Some("accepted"));
        assert_eq!(
            group_json["result"]["extensions"]["receipt"]["mode"].as_str(),
            Some("group_representative")
        );
        assert_eq!(
            group_json["result"]["extensions"]["receipt"]["run_id"].as_str(),
            Some(group_run_id)
        );

        let run_watch_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/watch/{group_run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run_watch_res.status(), StatusCode::OK);
        let run_watch_json = json_from(run_watch_res).await;
        assert_eq!(
            run_watch_json["watch"]["run_id"].as_str(),
            Some(group_run_id)
        );
    });
}
