use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;
use wattswarm::cli::sample_contract;
use wattswarm::policy::PolicyRegistry;
use wattswarm_storage_core::storage::pg::Connection;

fn cmd(schema: &str) -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin!("wattswarm"));
    cmd.env("WATTSWARM_PG_SCHEMA", schema);
    cmd
}

struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for_runtime(base_url: &str) {
    let client = reqwest::blocking::Client::new();
    for _ in 0..60 {
        if client
            .get(format!("{}/health", base_url))
            .send()
            .and_then(|r| r.error_for_status())
            .is_ok()
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("runtime did not become healthy: {}", base_url);
}

fn reset_test_schema(schema: &str) {
    let conn = Connection::open("cli-smoke-schema-reset").expect("open pg connection");
    conn.execute_batch(&format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE;
         CREATE SCHEMA {schema};"
    ))
    .expect("reset cli test schema");
}

fn drop_test_schema(schema: &str) {
    let conn = Connection::open("cli-smoke-schema-drop").expect("open pg connection");
    let _ = conn.execute_batch(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
}

struct CliSchemaGuard {
    name: String,
}

impl CliSchemaGuard {
    fn new() -> Self {
        let name = format!("test_cli_{}", Uuid::new_v4().simple());
        reset_test_schema(&name);
        Self { name }
    }

    fn as_str(&self) -> &str {
        &self.name
    }
}

impl Drop for CliSchemaGuard {
    fn drop(&mut self) {
        drop_test_schema(&self.name);
    }
}

#[test]
fn cli_node_lifecycle_and_log_head() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "up",
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "status",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"running\":true"))
        .stdout(predicate::str::contains("\"mode\":\"local\""))
        .stdout(predicate::str::contains("\"local_protocol_version\""));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "log",
            "head",
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "down",
        ])
        .assert()
        .success();
}

#[test]
fn cli_node_up_accepts_lan_mode() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "up",
            "--mode",
            "lan",
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "status",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"mode\":\"lan\""));
}

#[test]
fn cli_submit_watch_and_decision() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();

    let policy_hash = PolicyRegistry::with_builtin()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let task_id = format!("task-cli-{}", Uuid::new_v4().simple());
    let contract = sample_contract(&task_id, policy_hash);
    let task_file = dir.path().join("task.json");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(&task_file, serde_json::to_vec_pretty(&contract).unwrap()).unwrap();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "task",
            "submit",
            task_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "task",
            "watch",
            &task_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("task={task_id}")));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "task",
            "decision",
            &task_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("task={task_id}")));

    let out = dir.path().join("knowledge.json");
    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "knowledge",
            "export",
            "--task_id",
            &task_id,
            "--out",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert!(out.exists());
}

#[test]
fn cli_executors_add_and_list() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "executors",
            "add",
            "r1",
            "http://127.0.0.1:9999",
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "executors",
            "list",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("r1 http://127.0.0.1:9999"));
}

#[test]
fn cli_peers_and_log_replay_verify() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "up",
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "peers",
            "list",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("["));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "log",
            "replay",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("replayed"));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "log",
            "verify",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("verified"));
}

#[test]
fn cli_knowledge_export_requires_exactly_one_selector() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = "test.state";
    let out = dir.path().join("knowledge.json");
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "knowledge",
            "export",
            "--task_type",
            "resume_review",
            "--task_id",
            "task-1",
            "--out",
            out.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "knowledge export requires exactly one of --task_type or --task_id",
        ));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "knowledge",
            "export",
            "--out",
            out.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "knowledge export requires exactly one of --task_type or --task_id",
        ));
}

#[test]
fn cli_run_queue_lifecycle_smoke() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let run_id = format!("run-cli-{}", Uuid::new_v4().simple());
    let schema = CliSchemaGuard::new();
    let spec_file = dir.path().join("run.json");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(
        &spec_file,
        serde_json::to_vec_pretty(&serde_json::json!({
            "run_id": run_id,
            "task_type": "resume_review",
            "shared_inputs": {"city":"San Francisco"},
            "agents": [
                {
                    "agent_id": "a1",
                    "executor": "rt",
                    "profile": "default",
                    "prompt": "review"
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();

    cmd(schema.as_str())
        .args([
            "run",
            "--pg-url",
            "postgres://postgres:postgres@127.0.0.1:55432/wattswarm",
            "init",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("run queue schema initialized"));

    cmd(schema.as_str())
        .args(["run", "submit", spec_file.to_str().unwrap(), "--kickoff"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\": true"))
        .stdout(predicate::str::contains(&run_id));

    cmd(schema.as_str())
        .args(["run", "watch", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains(&run_id))
        .stdout(predicate::str::contains("\"status\":"));

    cmd(schema.as_str())
        .args(["run", "events", &run_id, "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("RUN_"));

    cmd(schema.as_str())
        .args(["run", "cancel", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancel requested"));

    cmd(schema.as_str())
        .args(["run", "result", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"CANCELL"));

    cmd(schema.as_str())
        .args(["run", "retry", "run-does-not-exist"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("run not found"));
}

#[test]
fn cli_run_real_flow_with_http_runtime() {
    if std::env::var("WATTSWARM_ENABLE_NETWORK_TESTS").as_deref() != Ok("1") {
        return;
    }

    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();

    let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(_) => return,
    };
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let base_url = format!("http://{}", addr);

    let mut runtime_cmd =
        std::process::Command::new(assert_cmd::cargo::cargo_bin!("wattswarm-runtime"));
    runtime_cmd
        .args(["--listen", &addr.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let runtime_child = runtime_cmd.spawn().unwrap();
    let _guard = ChildGuard(runtime_child);
    wait_for_runtime(&base_url);

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "executors",
            "add",
            "rt-real",
            &base_url,
        ])
        .assert()
        .success();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "task",
            "run-real",
            "--executor",
            "rt-real",
            "--profile",
            "default",
            "--task-id",
            "task-real-cli",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"task_id\": \"task-real-cli\""))
        .stdout(predicate::str::contains(
            "\"terminal_state\": \"Finalized\"",
        ));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "task",
            "decision",
            "task-real-cli",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("finalized=Some("));
}
