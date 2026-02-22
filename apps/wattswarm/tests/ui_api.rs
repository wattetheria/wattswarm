use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;
use tower::ServiceExt;
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
