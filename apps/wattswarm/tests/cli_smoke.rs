use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use uuid::Uuid;
use wattswarm::cli::sample_contract;
use wattswarm::crypto::NodeIdentity;
use wattswarm::policy::PolicyRegistry;
use wattswarm::types::{
    AuthorityMember, AuthoritySet, Membership, Role, SignedNetworkAuthoritySetEnvelope,
    SignedNetworkProtocolParamsEnvelope,
};
use wattswarm::{control::NodeMode, storage::PgStore};
use wattswarm_storage_core::storage::pg::{BackendKind, Connection, configured_backend_kind};

fn cmd(schema: &str) -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin!("wattswarm"));
    cmd.env("WATTSWARM_PG_SCHEMA", schema);
    cmd
}

fn sqlite_cmd(state_dir: &std::path::Path) -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin!("wattswarm"));
    cmd.env("WATTSWARM_STORAGE_BACKEND", "sqlite").args([
        "--state-dir",
        state_dir.to_str().expect("utf-8 state dir"),
        "--store",
        "node.sqlite3",
    ]);
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
    if configured_backend_kind().expect("resolve storage backend") == BackendKind::Sqlite {
        return;
    }
    let conn = Connection::open("cli-smoke-schema-reset").expect("open pg connection");
    conn.execute_batch(&format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE;
         CREATE SCHEMA {schema};"
    ))
    .expect("reset cli test schema");
}

fn drop_test_schema(schema: &str) {
    if configured_backend_kind().expect("resolve storage backend") == BackendKind::Sqlite {
        return;
    }
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

fn load_signed_network_params(
    state_dir: &std::path::Path,
    store: &str,
    schema: &str,
    network_id: &str,
) -> SignedNetworkProtocolParamsEnvelope {
    let db_path = state_dir.join(store);
    with_pg_schema(schema, || {
        PgStore::open(&db_path)
            .unwrap()
            .for_org(wattswarm::storage::storage::bootstrap_org_id(network_id))
            .load_verified_network_protocol_params()
            .unwrap()
            .signed
    })
}

fn load_signed_authority_set(
    state_dir: &std::path::Path,
    store: &str,
    schema: &str,
    network_id: &str,
) -> SignedNetworkAuthoritySetEnvelope {
    let db_path = state_dir.join(store);
    with_pg_schema(schema, || {
        PgStore::open(&db_path)
            .unwrap()
            .load_latest_signed_network_authority_set(network_id)
            .unwrap()
    })
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
fn cli_node_exports_and_stores_bootstrap_contacts() {
    let dir = tempdir().unwrap();
    let genesis_state_dir = dir.path().join("genesis");
    let joining_state_dir = dir.path().join("joining");
    let schema = CliSchemaGuard::new();
    let bootstrap_addr = "127.0.0.1:7777";

    let output = cmd(schema.as_str())
        .env("WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS", bootstrap_addr)
        .args([
            "--state-dir",
            genesis_state_dir.to_str().unwrap(),
            "node",
            "export-contact",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let contact = String::from_utf8(output).unwrap();
    let contact = contact.trim();
    let (node_id, addr) = contact.split_once('@').unwrap();
    assert!(!node_id.is_empty());
    assert_eq!(addr, bootstrap_addr);

    let json_output = cmd(schema.as_str())
        .env("WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS", bootstrap_addr)
        .args([
            "--state-dir",
            genesis_state_dir.to_str().unwrap(),
            "node",
            "export-contact",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let contact_json: serde_json::Value =
        serde_json::from_str(String::from_utf8(json_output).unwrap().trim()).unwrap();
    assert_eq!(contact_json["transports"][0]["transport"], "iroh_direct");
    assert_eq!(
        contact_json["transports"][0]["metadata"]["endpoint_id"],
        node_id
    );

    cmd(schema.as_str())
        .args([
            "--state-dir",
            joining_state_dir.to_str().unwrap(),
            "node",
            "add-bootstrap-contact",
            contact,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("bootstrap contact added"));

    cmd(schema.as_str())
        .args([
            "--state-dir",
            joining_state_dir.to_str().unwrap(),
            "node",
            "bootstrap-contacts",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(contact));

    let saved: serde_json::Value = serde_json::from_slice(
        &std::fs::read(joining_state_dir.join("startup_config.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(saved["network_mode"], "wan");
    assert_eq!(saved["bootstrap_contacts"][0], contact);

    cmd(schema.as_str())
        .args([
            "--state-dir",
            joining_state_dir.to_str().unwrap(),
            "node",
            "add-bootstrap-contact",
            r#"{"not":"a contact"}"#,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "bootstrap contact must be exported by node export-contact",
        ));
}

#[test]
fn cli_node_iroh_probe_command_is_exposed() {
    let dir = tempdir().unwrap();
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            dir.path().to_str().unwrap(),
            "node",
            "iroh-probe",
            "--help",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("<ENDPOINT_ID>"))
        .stdout(predicate::str::contains("--direct-addr"))
        .stdout(predicate::str::contains("Usage: wattswarm node iroh-probe"))
        .stdout(predicate::str::contains("--hold-ms"))
        .stdout(predicate::str::contains("--timeout-ms"))
        .stdout(predicate::str::contains("--node-api-url"))
        .stdout(predicate::str::contains("--contact").not());
}

#[test]
fn cli_node_iroh_probe_rejects_non_http_node_api_url() {
    let dir = tempdir().unwrap();
    let schema = CliSchemaGuard::new();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            dir.path().to_str().unwrap(),
            "node",
            "iroh-probe",
            "9b196ed13c0ec849dd7b8bf5add0b07ad2c88c1bf5d79e8591f6681ab1803258",
            "--node-api-url",
            "ftp://127.0.0.1:7788",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "running node API URL must use http or https",
        ));
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
fn cli_node_sign_network_params_updates_signed_envelope() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("genesis-state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();
    let genesis = NodeIdentity::from_seed([9_u8; 32]);
    let genesis_node_id = genesis.node_id();
    let network_id = format!("mainnet-cli-params-{}", Uuid::new_v4().simple());
    setup_mainnet_cli_state(
        &state_dir,
        store,
        schema.as_str(),
        &genesis,
        &genesis_node_id,
        &network_id,
    );

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "sign-network-params",
            "--network-id",
            &network_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"version\": 1"))
        .stdout(predicate::str::contains(&genesis_node_id));

    let first = load_signed_network_params(&state_dir, store, schema.as_str(), &network_id);
    assert_eq!(first.version, 1);
    assert_eq!(first.prev_hash, None);
    assert_eq!(first.signed_by, genesis_node_id);

    let mut params = serde_json::to_value(first.params.clone())
        .unwrap()
        .as_object()
        .unwrap()
        .clone();
    params.insert("gossip_mesh_d".to_owned(), serde_json::json!(7));
    params.insert("future_window_size".to_owned(), serde_json::json!(42));
    params.insert("max_established_per_peer".to_owned(), serde_json::json!(3));
    let params_file = dir.path().join("network-params.json");
    std::fs::write(&params_file, serde_json::to_vec_pretty(&params).unwrap()).unwrap();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "sign-network-params",
            "--network-id",
            &network_id,
            "--params-file",
            params_file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"version\": 2"))
        .stdout(predicate::str::contains(&first.params_hash));

    let second = load_signed_network_params(&state_dir, store, schema.as_str(), &network_id);
    assert_eq!(second.version, 2);
    assert_eq!(
        second.prev_hash.as_deref(),
        Some(first.params_hash.as_str())
    );
    assert_eq!(second.params.gossip_mesh_d, 7);
    let second_kv = second.params_kv.as_ref().expect("kv payload");
    assert_eq!(
        second_kv.get("future_window_size"),
        Some(&serde_json::json!(42))
    );
    assert!(!second_kv.contains_key("max_established_per_peer"));
}

#[test]
fn cli_node_update_authority_set_requires_genesis_and_updates_signed_envelope() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("genesis-state");
    let store = "test.state";
    let schema = CliSchemaGuard::new();
    let genesis = NodeIdentity::from_seed([19_u8; 32]);
    let second_authority = NodeIdentity::from_seed([20_u8; 32]);
    let genesis_node_id = genesis.node_id();
    let network_id = format!("mainnet-cli-authority-{}", Uuid::new_v4().simple());
    setup_mainnet_cli_state(
        &state_dir,
        store,
        schema.as_str(),
        &genesis,
        &genesis_node_id,
        &network_id,
    );

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "update-authority-set",
            "--network-id",
            &network_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"authority_set_id\": 0"))
        .stdout(predicate::str::contains(&genesis_node_id));

    let first = load_signed_authority_set(&state_dir, store, schema.as_str(), &network_id);
    assert_eq!(first.authority_set.authority_set_id, 0);
    assert_eq!(first.authority_set.members[0].node_id, genesis_node_id);
    assert_eq!(first.signed_by, genesis_node_id);

    let next = AuthoritySet {
        authority_set_id: 1,
        members: vec![
            AuthorityMember {
                node_id: genesis_node_id.clone(),
                weight: 1,
            },
            AuthorityMember {
                node_id: second_authority.node_id(),
                weight: 1,
            },
        ],
        threshold_weight: 2,
    };
    let authority_file = dir.path().join("authority-set.json");
    std::fs::write(&authority_file, serde_json::to_vec_pretty(&next).unwrap()).unwrap();

    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            store,
            "node",
            "update-authority-set",
            "--network-id",
            &network_id,
            "--authority-set-file",
            authority_file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"authority_set_id\": 1"))
        .stdout(predicate::str::contains(&first.authority_set_hash));

    let second = load_signed_authority_set(&state_dir, store, schema.as_str(), &network_id);
    assert_eq!(second.authority_set, next);
    assert_eq!(
        second.prev_hash.as_deref(),
        Some(first.authority_set_hash.as_str())
    );
}

#[test]
fn cli_global_announcement_uses_genesis_and_existing_global_pipeline() {
    use serde_json::{Value, json};
    use std::path::Path;
    use std::time::Instant;
    use wattswarm::startup_config::{
        NetworkMode, StartupConfig, save_startup_config, startup_config_path,
    };
    use wattswarm::types::{EventKind, EventPayload, NetworkProtocolParams};

    fn wait_for(mut condition: impl FnMut() -> bool, message: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("{message}");
    }

    fn diagnostic_contains(state_dir: &Path, phase: &str, event_id: Option<&str>) -> bool {
        std::fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|row| row["phase"] == phase && event_id.is_none_or(|id| row["event_id"] == id))
    }

    fn start_ui(schema: &str, state_dir: &Path, listen: &str, iroh_bind: &str) -> ChildGuard {
        ChildGuard(
            cmd(schema)
                .env("WATTSWARM_P2P_ENABLED", "true")
                .env("WATTSWARM_P2P_MDNS", "false")
                .env("WATTSWARM_P2P_LISTEN_ADDRS", "127.0.0.1:0")
                .env("WATTSWARM_IROH_BIND_ADDR", iroh_bind)
                .env("WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS", iroh_bind)
                .env("WATTSWARM_NETWORK_DEBUG_DIAGNOSTICS", "true")
                .env("WATTSWARM_NETWORK_JOIN_MANIFEST_URLS", "http://127.0.0.1:9")
                .env(
                    "WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS",
                    "http://127.0.0.1:9",
                )
                .envs([
                    ("WATTSWARM_PUBLIC_GATEWAY_URLS", "http://127.0.0.1:9"),
                    ("WATTSWARM_PUBLIC_DISCOVERY_URLS", "http://127.0.0.1:9"),
                    ("WATTSWARM_PUBLIC_BOOTSTRAP_URLS", "http://127.0.0.1:9"),
                    ("WATTSWARM_IROH_RELAY_URLS", "http://127.0.0.1:9"),
                    ("HTTP_PROXY", "http://127.0.0.1:9"),
                    ("HTTPS_PROXY", "http://127.0.0.1:9"),
                    ("ALL_PROXY", "http://127.0.0.1:9"),
                    ("http_proxy", "http://127.0.0.1:9"),
                    ("https_proxy", "http://127.0.0.1:9"),
                    ("all_proxy", "http://127.0.0.1:9"),
                    ("NO_PROXY", "127.0.0.1,localhost,::1"),
                    ("no_proxy", "127.0.0.1,localhost,::1"),
                ])
                .env_remove("WATTSWARM_WATTETHERIA_SYNC_GRPC_LISTEN")
                .args([
                    "--state-dir",
                    state_dir.to_str().unwrap(),
                    "--store",
                    "test.state",
                    "ui",
                    "--listen",
                    listen,
                ])
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(state_dir.join("ui-stderr.log")).unwrap())
                .spawn()
                .unwrap(),
        )
    }

    let dir = tempdir().unwrap();
    let genesis_dir = dir.path().join("genesis");
    let receiver_dir = dir.path().join("receiver");
    let genesis_schema = CliSchemaGuard::new();
    let receiver_schema = CliSchemaGuard::new();
    let genesis = NodeIdentity::random();
    let receiver = NodeIdentity::random();
    let receipts = std::sync::Arc::new(Mutex::new(
        Vec::<(String, wattswarm::types::AgentEvent)>::new(),
    ));
    let callback_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let callback_url = format!("http://{}", callback_listener.local_addr().unwrap());
    callback_listener.set_nonblocking(true).unwrap();
    let callback_runtime = tokio::runtime::Runtime::new().unwrap();
    let callback_router = axum::Router::new().route(
        "/:node/agent-events",
        axum::routing::post({
            let receipts = receipts.clone();
            move |axum::extract::Path(node): axum::extract::Path<String>,
                      axum::Json(request): axum::Json<
                    wattswarm::types::AgentEventCallbackRequest,
                >| {
                    let receipts = receipts.clone();
                    async move {
                        receipts.lock().unwrap().push((node, request.event));
                        axum::Json(json!({"ok":true}))
                    }
                }
        }),
    );
    let callback_listener = {
        let _entered = callback_runtime.enter();
        tokio::net::TcpListener::from_std(callback_listener).unwrap()
    };
    callback_runtime.spawn(async move {
        axum::serve(callback_listener, callback_router)
            .await
            .unwrap()
    });
    let network_id = format!("mainnet-announcement-{}", Uuid::new_v4().simple());
    let params = NetworkProtocolParams {
        anti_entropy_interval_secs: 1,
        backfill_retry_after_secs: 1,
        ..NetworkProtocolParams::default()
    };
    let mut config = StartupConfig {
        network_mode: NetworkMode::Wan,
        relay_urls: vec!["http://127.0.0.1:9/".to_owned()],
        ..StartupConfig::default()
    };
    for (state_dir, schema, identity) in [
        (&genesis_dir, genesis_schema.as_str(), &genesis),
        (&receiver_dir, receiver_schema.as_str(), &receiver),
    ] {
        setup_mainnet_cli_state(
            state_dir,
            "test.state",
            schema,
            identity,
            &genesis.node_id(),
            &network_id,
        );
        with_pg_schema(schema, || {
            PgStore::open(state_dir.join("test.state"))
                .unwrap()
                .put_network_protocol_params(&network_id, &genesis, &params)
                .unwrap();
        });
        save_startup_config(&startup_config_path(state_dir), &config).unwrap();
        with_pg_schema(schema, || {
            wattswarm::control::save_executor_registry_state(
                state_dir,
                &wattswarm::control::ExecutorRegistry {
                    entries: vec![wattswarm::control::ExecutorRegistryEntry {
                        name: "core-agent".to_owned(),
                        base_url: format!(
                            "{callback_url}/{}",
                            state_dir.file_name().unwrap().to_str().unwrap()
                        ),
                        agent_event_callback_base_url: None,
                        kind: Default::default(),
                        target_node_id: None,
                        scope_hint: None,
                        commit_plane_endpoint: None,
                        commit_plane_token_file: None,
                    }],
                },
            )
            .unwrap();
        });
    }

    let content = json!({"opaque": {"title":"network announcement"}, "optional":null});
    let content_file = dir.path().join("content.json");
    std::fs::write(&content_file, serde_json::to_vec(&content).unwrap()).unwrap();
    let publish = |state_dir: &Path, schema: &str, file: &Path, id: &str| {
        cmd(schema)
            .env("WATTSWARM_NETWORK_JOIN_MANIFEST_URLS", "http://127.0.0.1:9")
            .env(
                "WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS",
                "http://127.0.0.1:9",
            )
            .envs([
                ("HTTP_PROXY", "http://127.0.0.1:9"),
                ("HTTPS_PROXY", "http://127.0.0.1:9"),
                ("ALL_PROXY", "http://127.0.0.1:9"),
                ("http_proxy", "http://127.0.0.1:9"),
                ("https_proxy", "http://127.0.0.1:9"),
                ("all_proxy", "http://127.0.0.1:9"),
                ("NO_PROXY", "127.0.0.1,localhost,::1"),
                ("no_proxy", "127.0.0.1,localhost,::1"),
            ])
            .args([
                "--state-dir",
                state_dir.to_str().unwrap(),
                "--store",
                "test.state",
                "governance",
                "global-announcement",
                file.to_str().unwrap(),
                "--announcement-id",
                id,
            ])
            .output()
            .unwrap()
    };
    let rejected = publish(
        &receiver_dir,
        receiver_schema.as_str(),
        &content_file,
        "unauthorized",
    );
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8(rejected.stderr)
            .unwrap()
            .contains("requires mainnet genesis node")
    );

    let free_listen = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string()
    };
    let genesis_listen = free_listen();
    let receiver_listen = free_listen();
    let free_iroh_bind = || {
        std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string()
    };
    let genesis_iroh_bind = free_iroh_bind();
    let receiver_iroh_bind = free_iroh_bind();
    let genesis_url = format!("http://{genesis_listen}");
    let receiver_url = format!("http://{receiver_listen}");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let activate = |url: &str, node_id: &str| {
        wait_for(
            || {
                client
                    .get(format!("{url}/api/node/status"))
                    .send()
                    .is_ok_and(|r| r.status().is_success())
            },
            "UI must be reachable",
        );
        let response = client.post(format!("{url}/api/network/permission/checkpoint")).json(&json!({
            "network_id": network_id, "node_id":node_id, "agent_did":"did:key:test-agent",
            "permission_status":"active", "network_status":"running", "revision":1,
            "credential_id":"test-credential", "credential_hash":"sha256:test-credential",
            "credential_trust_anchor": {
                "network_id":network_id, "trust_anchor_id":genesis.node_id(),
                "signature_algorithm":"ed25519", "public_key_encoding":"hex", "public_key":genesis.node_id()
            },
            "last_error":null, "updated_at_ms":100,
        })).send().unwrap().error_for_status().unwrap().json::<Value>().unwrap();
        assert_eq!(response["network_start_requested"], true);
    };
    let contact = |url: &str| {
        let mut material = None;
        wait_for(
            || {
                material = client
                    .get(format!("{url}/api/network/contact-material/local"))
                    .send()
                    .ok()
                    .and_then(|r| r.error_for_status().ok())
                    .and_then(|r| r.json::<Value>().ok())
                    .and_then(|v| {
                        v["contact_material"]["material_json"]
                            .as_str()
                            .map(str::to_owned)
                    });
                material.is_some()
            },
            "running node must export actual contact material",
        );
        material.unwrap()
    };

    let genesis_ui = start_ui(
        genesis_schema.as_str(),
        &genesis_dir,
        &genesis_listen,
        &genesis_iroh_bind,
    );
    activate(&genesis_url, &genesis.node_id());
    config.bootstrap_contacts = vec![contact(&genesis_url)];
    save_startup_config(&startup_config_path(&receiver_dir), &config).unwrap();
    let _receiver_ui = start_ui(
        receiver_schema.as_str(),
        &receiver_dir,
        &receiver_listen,
        &receiver_iroh_bind,
    );
    activate(&receiver_url, &receiver.node_id());
    wait_for(
        || diagnostic_contains(&genesis_dir, "connection.established", None),
        "genesis must connect before CLI publication",
    );
    thread::sleep(Duration::from_millis(500));

    let published = publish(
        &genesis_dir,
        genesis_schema.as_str(),
        &content_file,
        "live-announcement",
    );
    assert!(
        published.status.success(),
        "{}",
        String::from_utf8_lossy(&published.stderr)
    );
    let output: Value = serde_json::from_slice(&published.stdout).unwrap();
    assert_eq!(output["action"], "global_announcement");
    assert_eq!(output["author_node_id"], genesis.node_id());
    let event_id = output["event_id"].as_str().unwrap();
    wait_for(
        || diagnostic_contains(&receiver_dir, "event.ingest.live", Some(event_id)),
        "real Global gossip must deliver the CLI event",
    );
    let load = |state_dir: &Path, schema: &str, id: &str| {
        with_pg_schema(schema, || {
            PgStore::open(state_dir.join("test.state"))
                .unwrap()
                .for_org(wattswarm::storage::bootstrap_org_id(&network_id))
                .get_global_announcement(&network_id, id)
                .unwrap()
                .unwrap()
        })
    };
    let live = load(&genesis_dir, genesis_schema.as_str(), "live-announcement");
    assert_eq!(
        load(&receiver_dir, receiver_schema.as_str(), "live-announcement"),
        live
    );
    wattswarm::crypto::verify_event_signature(&live).unwrap();
    let EventPayload::EventAnnouncement(payload) = &live.payload else {
        panic!("announcement payload")
    };
    assert_eq!(payload.authority_node_id, genesis.node_id());
    assert_eq!(payload.network_id, network_id);
    assert_eq!(payload.content, content);
    wattswarm::crypto::verify_signature(
        &genesis.node_id(),
        &serde_jcs::to_vec(&payload.unsigned_payload()).unwrap(),
        &payload.authority_signature,
    )
    .unwrap();
    let page = client
        .get(format!("{receiver_url}/api/global/messages"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .unwrap();
    assert!(
        page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["message_id"] == event_id
                && message["content"]["payload"] == serde_json::to_value(payload).unwrap())
    );
    assert_eq!(
        client
            .post(format!("{genesis_url}/api/global/announcements"))
            .json(payload)
            .send()
            .unwrap()
            .status()
            .as_u16(),
        404
    );

    for _ in 0..3 {
        let repeated = publish(
            &genesis_dir,
            genesis_schema.as_str(),
            &content_file,
            "live-announcement",
        );
        assert!(repeated.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&repeated.stdout).unwrap()["event_id"],
            output["event_id"]
        );
        assert_eq!(
            load(&genesis_dir, genesis_schema.as_str(), "live-announcement"),
            live
        );
        with_pg_schema(genesis_schema.as_str(), || {
            assert_eq!(
                wattswarm::control::load_agent_event_records_state(&genesis_dir)
                    .unwrap()
                    .len(),
                1
            );
        });
        assert_eq!(
            receipts
                .lock()
                .unwrap()
                .iter()
                .filter(|(node, _)| node == "genesis")
                .count(),
            1
        );
    }
    let conflicting_file = dir.path().join("conflict.json");
    std::fs::write(&conflicting_file, b"{\"opaque\":\"different\"}").unwrap();
    let conflicting = publish(
        &genesis_dir,
        genesis_schema.as_str(),
        &conflicting_file,
        "live-announcement",
    );
    assert!(!conflicting.status.success());
    assert!(
        String::from_utf8(conflicting.stderr)
            .unwrap()
            .contains("already published with different content")
    );

    config.bootstrap_contacts = vec![contact(&receiver_url)];
    save_startup_config(&startup_config_path(&genesis_dir), &config).unwrap();
    drop(genesis_ui);
    let missed = publish(
        &genesis_dir,
        genesis_schema.as_str(),
        &content_file,
        "missed-announcement",
    );
    assert!(missed.status.success());
    let missed_output: Value = serde_json::from_slice(&missed.stdout).unwrap();
    let _restarted_genesis_ui = start_ui(
        genesis_schema.as_str(),
        &genesis_dir,
        &genesis_listen,
        &genesis_iroh_bind,
    );
    activate(&genesis_url, &genesis.node_id());
    let recovered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for(
            || {
                diagnostic_contains(
                    &receiver_dir,
                    "event.ingest.backfill",
                    missed_output["event_id"].as_str(),
                )
            },
            "offline CLI event must recover through existing Global backfill",
        );
    }));
    if let Err(error) = recovered {
        for state_dir in [&genesis_dir, &receiver_dir] {
            eprintln!(
                "{}\n{}",
                state_dir.display(),
                std::fs::read_to_string(state_dir.join("ui-stderr.log")).unwrap()
            );
            for line in std::fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
                .unwrap()
                .lines()
            {
                if line.contains(missed_output["event_id"].as_str().unwrap()) {
                    eprintln!("{line}");
                }
            }
        }
        std::panic::resume_unwind(error);
    }
    assert_eq!(
        load(
            &receiver_dir,
            receiver_schema.as_str(),
            "missed-announcement"
        ),
        load(&genesis_dir, genesis_schema.as_str(), "missed-announcement")
    );
    for (state_dir, schema) in [
        (&genesis_dir, genesis_schema.as_str()),
        (&receiver_dir, receiver_schema.as_str()),
    ] {
        with_pg_schema(schema, || {
            let mut node =
                wattswarm::control::open_node(state_dir, &state_dir.join("test.state")).unwrap();
            assert_eq!(
                node.store
                    .load_events_by_kinds(&[EventKind::EventAnnouncement])
                    .unwrap()
                    .len(),
                2
            );
            node.replay_rebuild_projection().unwrap();
            assert_eq!(
                node.store
                    .load_events_by_kinds(&[EventKind::EventAnnouncement])
                    .unwrap()
                    .len(),
                2
            );
            let notifications =
                wattswarm::control::load_agent_event_records_state(state_dir).unwrap();
            assert_eq!(notifications.len(), 2);
            assert!(notifications.iter().all(|event| event.event_type
                == wattswarm::types::AgentEventType::GlobalAnnouncementReceived
                && event.allowed_actions == ["ignore"]));
        });
    }
    let receipts = receipts.lock().unwrap();
    for node in ["genesis", "receiver"] {
        let delivered = receipts
            .iter()
            .filter(|(target, _)| target == node)
            .collect::<Vec<_>>();
        assert_eq!(delivered.len(), 2);
        assert!(delivered.iter().all(|(_, event)| event.event_type
            == wattswarm::types::AgentEventType::GlobalAnnouncementReceived));
    }
    println!(
        "GLOBAL_CLI_PUBLIC_EVIDENCE={}",
        serde_json::to_string(&serde_json::json!({
            "backend": std::env::var("WATTSWARM_STORAGE_BACKEND").unwrap_or_else(|_| "sqlite".into()),
            "genesis_public_key": genesis.node_id(),
            "receiver_public_key": receiver.node_id(),
            "cli_output": output,
            "signed_live_event": live,
            "global_get": page,
            "signed_backfilled_event": load(&receiver_dir, receiver_schema.as_str(), "missed-announcement"),
            "same_id_repeats": 3,
            "stored_events_per_node": 2,
            "stored_notifications_per_node": 2,
            "actual_http_callback_counts": {
                "genesis": receipts.iter().filter(|(node, _)| node == "genesis").count(),
                "receiver": receipts.iter().filter(|(node, _)| node == "receiver").count()
            },
            "non_genesis_rejected": true,
            "conflicting_content_rejected": true,
            "deleted_post_status": 404,
            "live_gossip_and_restart_backfill_verified": true
        }))
        .unwrap()
    );
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

    let network_ban_until = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64
        + 60_000)
        .to_string();

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
            "--network-ban",
            "--network-ban-until",
            network_ban_until.as_str(),
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
fn cli_old_subscription_schema_recovers_dm_scopes_on_restart() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let identity = NodeIdentity::random();
    write_identity_seed(&state_dir, &identity);
    wattswarm::control::write_node_state(&state_dir, true, NodeMode::Local).unwrap();
    let schema = CliSchemaGuard::new();
    let db_path = state_dir.join("test.state");
    let node_id = identity.node_id();
    let network_id = wattswarm::storage::local_network_id(&node_id);
    let org_id = wattswarm::storage::bootstrap_org_id(&network_id);
    with_pg_schema(schema.as_str(), || {
        let store = PgStore::open(&db_path).unwrap().for_org(&org_id);
        for (scope, time) in [("group:dm-a", 100), ("group:dm-b", 110)] {
            let event = wattswarm::node::build_event_for_external(
                &identity,
                1,
                time,
                wattswarm::types::EventPayload::FeedSubscriptionUpdated(
                    wattswarm::types::FeedSubscriptionUpdatedPayload {
                        network_id: network_id.clone(),
                        subscriber_node_id: node_id.clone(),
                        feed_key: "wattswarm.dm".to_owned(),
                        scope_hint: scope.to_owned(),
                        gossip_kinds: vec!["messages".to_owned()],
                        provider_capabilities: None,
                        agent_envelope: None,
                        active: true,
                    },
                ),
            )
            .unwrap();
            store.append_event(&event).unwrap();
        }
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "DROP TABLE feed_subscriptions;
            CREATE TABLE feed_subscriptions (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                network_id TEXT NOT NULL DEFAULT '',
                subscriber_node_id TEXT NOT NULL,
                feed_key TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                gossip_kinds_json TEXT NOT NULL DEFAULT '[]',
                provider_capabilities_json TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, network_id, subscriber_node_id, feed_key)
            );",
        )
        .unwrap();
        conn.execute("INSERT INTO feed_subscriptions(org_id, network_id, subscriber_node_id, feed_key, scope_hint, gossip_kinds_json, updated_at)
            VALUES ($1, $2, $3, 'wattswarm.dm', 'group:dm-b', '[\"messages\"]', NOW())",
            wattswarm_storage_core::params![org_id, network_id, node_id]).unwrap();
    });
    // A fresh process exercises migration rather than the in-process schema cache.
    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            "test.state",
            "log",
            "head",
        ])
        .assert()
        .success();
    with_pg_schema(schema.as_str(), || {
        let store = PgStore::open(&db_path).unwrap().for_org(&org_id);
        for scope in ["group:dm-a", "group:dm-b"] {
            assert!(
                store
                    .get_feed_subscription(&network_id, &node_id, "wattswarm.dm", scope)
                    .unwrap()
                    .unwrap()
                    .active
            );
        }
        assert!(store.feed_subscription_scope_recovery_completed().unwrap());
        store
            .upsert_feed_subscription(
                &network_id,
                &node_id,
                "wattswarm.dm",
                "group:dm-a",
                &["messages".to_owned()],
                false,
                120,
            )
            .unwrap();
    });
    cmd(schema.as_str())
        .args([
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--store",
            "test.state",
            "log",
            "head",
        ])
        .assert()
        .success();
    with_pg_schema(schema.as_str(), || {
        let store = PgStore::open(&db_path).unwrap().for_org(&org_id);
        assert!(
            !store
                .get_feed_subscription(&network_id, &node_id, "wattswarm.dm", "group:dm-a")
                .unwrap()
                .unwrap()
                .active
        );
        assert!(
            store
                .get_feed_subscription(&network_id, &node_id, "wattswarm.dm", "group:dm-b")
                .unwrap()
                .unwrap()
                .active
        );
    });
}

#[test]
fn cli_run_queue_lifecycle_smoke() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let state_dir = state_dir.to_str().unwrap();
    let store = "test.state";
    let run_id = format!("run-cli-{}", Uuid::new_v4().simple());
    let schema = CliSchemaGuard::new();
    let spec_file = dir.path().join("run.json");
    std::fs::create_dir_all(state_dir).unwrap();
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
            "--state-dir",
            state_dir,
            "--store",
            store,
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
        .args([
            "--state-dir",
            state_dir,
            "--store",
            store,
            "run",
            "submit",
            spec_file.to_str().unwrap(),
            "--kickoff",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\": true"))
        .stdout(predicate::str::contains(&run_id));

    let mut watch = cmd(schema.as_str());
    watch.env("WATTSWARM_NODE_MODE", "local");
    watch
        .args([
            "--state-dir",
            state_dir,
            "--store",
            store,
            "run",
            "watch",
            &run_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&run_id))
        .stdout(predicate::str::contains("\"status\":"));

    let mut events = cmd(schema.as_str());
    events.env("WATTSWARM_NODE_MODE", "local");
    events
        .args([
            "--state-dir",
            state_dir,
            "--store",
            store,
            "run",
            "events",
            &run_id,
            "--limit",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("RUN_"));

    let mut cancel = cmd(schema.as_str());
    cancel.env("WATTSWARM_NODE_MODE", "local");
    cancel
        .args([
            "--state-dir",
            state_dir,
            "--store",
            store,
            "run",
            "cancel",
            &run_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancel requested"));

    let mut result = cmd(schema.as_str());
    result.env("WATTSWARM_NODE_MODE", "local");
    result
        .args([
            "--state-dir",
            state_dir,
            "--store",
            store,
            "run",
            "result",
            &run_id,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"CANCELL"));

    let mut retry = cmd(schema.as_str());
    retry.env("WATTSWARM_NODE_MODE", "local");
    retry
        .args([
            "--state-dir",
            state_dir,
            "--store",
            store,
            "run",
            "retry",
            "run-does-not-exist",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("run not found"));
}

#[test]
fn cli_sqlite_node_and_run_queue_lifecycle_smoke() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let run_id = format!("run-sqlite-cli-{}", Uuid::new_v4().simple());
    let spec_file = dir.path().join("run.json");
    std::fs::write(
        &spec_file,
        serde_json::to_vec_pretty(&serde_json::json!({
            "run_id": run_id,
            "task_type": "sqlite_smoke",
            "shared_inputs": {"source":"cli"},
            "agents": [{
                "agent_id": "a1",
                "executor": "rt",
                "profile": "default",
                "prompt": "review"
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    sqlite_cmd(&state_dir)
        .args(["node", "up", "--mode", "local"])
        .assert()
        .success()
        .stdout(predicate::str::contains("node is up"));

    sqlite_cmd(&state_dir)
        .args(["run", "init"])
        .assert()
        .success()
        .stdout(predicate::str::contains("run queue schema initialized"));

    sqlite_cmd(&state_dir)
        .args(["run", "submit", spec_file.to_str().unwrap(), "--kickoff"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\": true"))
        .stdout(predicate::str::contains(&run_id));

    sqlite_cmd(&state_dir)
        .args(["run", "watch", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains(&run_id))
        .stdout(predicate::str::contains("\"status\": \"QUEUED\""));

    sqlite_cmd(&state_dir)
        .args(["run", "cancel", &run_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancel requested"));

    let unified_db = state_dir.join("wattswarm.db");
    assert!(unified_db.is_file());
    assert!(!state_dir.join("node.sqlite3").exists());
    assert!(!state_dir.join("local-control.state").exists());
    assert!(!state_dir.join("run-queue.sqlite3").exists());

    let conn = Connection::open_sqlite(&unified_db).expect("open unified sqlite database");
    let complete_table_count = conn
        .query_row(
            "SELECT COUNT(*)
             FROM sqlite_master
             WHERE type = 'table'
               AND name IN (
                   'events',
                   'executor_registry_local',
                   'runs',
                   'run_steps',
                   'run_events'
               )",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, i64>(0),
        )
        .expect("count unified storage tables");
    assert_eq!(complete_table_count, 5);
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
