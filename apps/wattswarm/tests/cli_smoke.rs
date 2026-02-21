use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;
use wattswarm::cli::sample_contract;
use wattswarm::policy::PolicyRegistry;

fn cmd() -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin!("wattswarm"));
    cmd.env("WATTSWARM_PG_ISOLATE_BY_PATH", "1");
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

#[test]
fn cli_node_lifecycle_and_log_head() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let db = "test.db";

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "node",
            "up",
        ])
        .assert()
        .success();

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "node",
            "status",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"running\":true"))
        .stdout(predicate::str::contains("\"local_protocol_version\""));

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "log",
            "head",
        ])
        .assert()
        .success();

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "node",
            "down",
        ])
        .assert()
        .success();
}

#[test]
fn cli_submit_watch_and_decision() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let db = "test.db";

    let policy_hash = PolicyRegistry::with_builtin()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let task_id = format!("task-cli-{}", Uuid::new_v4().simple());
    let contract = sample_contract(&task_id, policy_hash);
    let task_file = dir.path().join("task.json");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(&task_file, serde_json::to_vec_pretty(&contract).unwrap()).unwrap();

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "task",
            "submit",
            task_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "task",
            "watch",
            &task_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("task={task_id}")));

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "task",
            "decision",
            &task_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("task={task_id}")));

    let out = dir.path().join("knowledge.json");
    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
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

    cmd()
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

    cmd()
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
fn cli_run_real_flow_with_http_runtime() {
    if std::env::var("WATTSWARM_ENABLE_NETWORK_TESTS").as_deref() != Ok("1") {
        return;
    }

    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let db = "test.db";

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

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "executors",
            "add",
            "rt-real",
            &base_url,
        ])
        .assert()
        .success();

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
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

    cmd()
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--db",
            db,
            "task",
            "decision",
            "task-real-cli",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("finalized=Some("));
}
