use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;
use wattswarm::cli::sample_contract;
use wattswarm::crypto::NodeIdentity;
use wattswarm::policy::PolicyRegistry;
use wattswarm::types::{Membership, Role};
use wattswarm::{control::NodeMode, storage::PgStore};
use wattswarm_storage_core::storage::pg::Connection;

fn cmd(schema: &str) -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin!("wattswarm"));
    cmd.env("WATTSWARM_PG_SCHEMA", schema);
    cmd
}

fn env_test_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

fn with_pg_schema<T>(schema: &str, f: impl FnOnce() -> T) -> T {
    let _lock = env_test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let prev = std::env::var("WATTSWARM_PG_SCHEMA").ok();
    unsafe {
        std::env::set_var("WATTSWARM_PG_SCHEMA", schema);
    }
    let out = f();
    unsafe {
        if let Some(prev) = prev {
            std::env::set_var("WATTSWARM_PG_SCHEMA", prev);
        } else {
            std::env::remove_var("WATTSWARM_PG_SCHEMA");
        }
    }
    out
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

fn write_identity_seed(state_dir: &std::path::Path, identity: &NodeIdentity) {
    std::fs::create_dir_all(state_dir).unwrap();
    let seed_hex = identity
        .secret_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::fs::write(state_dir.join("node_seed.hex"), seed_hex).unwrap();
}

fn setup_mainnet_cli_state(
    state_dir: &std::path::Path,
    store: &str,
    schema: &str,
    identity: &NodeIdentity,
    genesis_node_id: &str,
    network_id: &str,
) {
    write_identity_seed(state_dir, identity);
    let db_path = state_dir.join(store);
    with_pg_schema(schema, || {
        let store = PgStore::open(&db_path).unwrap();
        store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "CLI Governance Mainnet",
                genesis_node_id,
                genesis_node_id,
                100,
            )
            .unwrap();
    });
    wattswarm::control::write_node_state(state_dir, true, NodeMode::Network).unwrap();
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
fn cli_governance_commands_require_mainnet_genesis_and_emit_events() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("genesis-state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();
    let genesis = NodeIdentity::from_seed([7_u8; 32]);
    let genesis_node_id = genesis.node_id();
    let network_id = format!("mainnet-cli-{}", Uuid::new_v4().simple());
    setup_mainnet_cli_state(
        &state_dir,
        store,
        schema.as_str(),
        &genesis,
        &genesis_node_id,
        &network_id,
    );

    let mut membership = Membership::new();
    membership.grant(&genesis_node_id, Role::Finalizer);
    let membership_file = dir.path().join("membership.json");
    std::fs::write(
        &membership_file,
        serde_json::to_vec_pretty(&membership).unwrap(),
    )
    .unwrap();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "governance",
            "membership-update",
            membership_file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"action\": \"membership_update\"",
        ))
        .stdout(predicate::str::contains(
            "\"event_kind\": \"MembershipUpdated\"",
        ));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "governance",
            "revoke-event",
            "--event-id",
            "event-bad",
            "--reason",
            "bad event",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"action\": \"revoke_event\""))
        .stdout(predicate::str::contains("\"event_kind\": \"EventRevoked\""));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "governance",
            "revoke-summary",
            "--summary-id",
            "summary-bad",
            "--kind",
            "knowledge_task_type_v1",
            "--reason",
            "bad summary",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"action\": \"revoke_summary\""))
        .stdout(predicate::str::contains(
            "\"event_kind\": \"SummaryRevoked\"",
        ));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "governance",
            "penalize-node",
            "--node-id",
            "node-bad",
            "--reason",
            "bad node",
            "--revoke-event",
            "event-bad",
            "--revoke-summary",
            "summary-bad",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"action\": \"penalize_node\""))
        .stdout(predicate::str::contains(
            "\"event_kind\": \"NodePenalized\"",
        ));

    let non_genesis_dir = dir.path().join("non-genesis-state");
    let non_genesis = NodeIdentity::from_seed([8_u8; 32]);
    let non_genesis_store = "non-genesis.state";
    write_identity_seed(&non_genesis_dir, &non_genesis);
    wattswarm::control::write_node_state(&non_genesis_dir, true, NodeMode::Network).unwrap();
    cmd(schema.as_str())
        .args([
            "--state-dir",
            non_genesis_dir.to_str().unwrap(),
            "--store",
            non_genesis_store,
            "governance",
            "revoke-event",
            "--event-id",
            "event-bad",
            "--reason",
            "bad event",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "governance command requires mainnet genesis node",
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

    let mut submit = cmd(schema.as_str());
    submit.env("WATTSWARM_NODE_MODE", "local");
    submit
        .args(["run", "submit", spec_file.to_str().unwrap(), "--kickoff"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\": true"))
        .stdout(predicate::str::contains(&run_id));

    let mut watch = cmd(schema.as_str());
    watch.env("WATTSWARM_NODE_MODE", "local");
    watch
        .args(["run", "watch", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains(&run_id))
        .stdout(predicate::str::contains("\"status\":"));

    let mut events = cmd(schema.as_str());
    events.env("WATTSWARM_NODE_MODE", "local");
    events
        .args(["run", "events", &run_id, "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("RUN_"));

    let mut cancel = cmd(schema.as_str());
    cancel.env("WATTSWARM_NODE_MODE", "local");
    cancel
        .args(["run", "cancel", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancel requested"));

    let mut result = cmd(schema.as_str());
    result.env("WATTSWARM_NODE_MODE", "local");
    result
        .args(["run", "result", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"CANCELL"));

    let mut retry = cmd(schema.as_str());
    retry.env("WATTSWARM_NODE_MODE", "local");
    retry
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
