use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;
use tempfile::tempdir;
use wattswarm_storage_core::storage::pg::{
    BackendKind, Connection, DatabaseClient, Error, ErrorCode, OptionalExtension, types::ValueRef,
};
use wattswarm_storage_core::storage::sqlite_layout::{
    LEGACY_LOCAL_CONTROL_SQLITE_FILE, LEGACY_MAIN_SQLITE_FILE, WATTSWARM_SQLITE_FILE,
};
use wattswarm_storage_core::storage::{
    CsMailboxDeliveryState, LocalAgentEventRow, LocalExecutorEntryRow, NetworkBackendStatusRow,
    PendingNetworkCommandInsert, PgStore,
};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        // SAFETY: this test binary serializes environment mutations with ENV_LOCK.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: this test binary serializes environment mutations with ENV_LOCK.
        unsafe {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

#[test]
fn sqlite_connection_round_trips_values_and_reopens() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("connection.sqlite3");

    {
        let conn = Connection::open_sqlite(&path).expect("open sqlite connection");
        assert_eq!(conn.backend_kind(), BackendKind::Sqlite);
        conn.execute_batch(
            "CREATE TABLE values_probe (
                id INTEGER PRIMARY KEY,
                score REAL NOT NULL,
                enabled INTEGER NOT NULL,
                label TEXT NOT NULL,
                payload BLOB NOT NULL
            );",
        )
        .expect("create values table");
        conn.execute(
            "INSERT INTO values_probe(id, score, enabled, label, payload)
             VALUES ($1, $2, $3, $4, $5)",
            wattswarm_storage_core::params![7_i64, 2.5_f64, true, "ready", vec![0_u8, 1, 2]],
        )
        .expect("insert sqlite row");
    }

    let conn = Connection::open_sqlite(&path).expect("reopen sqlite connection");
    conn.query_row(
        "SELECT id, score, enabled, label, payload FROM values_probe",
        wattswarm_storage_core::params![],
        |row| {
            assert_eq!(row.get::<usize, i64>(0)?, 7);
            assert!((row.get::<usize, f64>(1)? - 2.5).abs() < f64::EPSILON);
            assert!(row.get::<usize, bool>(2)?);
            assert_eq!(row.get::<usize, String>(3)?, "ready");
            assert!(matches!(row.get_ref(4)?, ValueRef::Blob(bytes) if bytes == vec![0, 1, 2]));
            Ok(())
        },
    )
    .expect("read reopened sqlite row");
}

#[test]
fn sqlite_constraint_errors_keep_the_shared_error_contract() {
    let conn = Connection::open_in_memory_sqlite().expect("open sqlite connection");
    conn.execute_batch("CREATE TABLE unique_probe (value TEXT UNIQUE NOT NULL);")
        .expect("create unique table");
    conn.execute(
        "INSERT INTO unique_probe(value) VALUES ($1)",
        wattswarm_storage_core::params!["same"],
    )
    .expect("insert first value");

    let error = conn
        .execute(
            "INSERT INTO unique_probe(value) VALUES ($1)",
            wattswarm_storage_core::params!["same"],
        )
        .expect_err("duplicate value must fail");
    assert!(matches!(
        error,
        Error::DbFailure(ref failure, _) if failure.code == ErrorCode::ConstraintViolation
    ));

    let missing = conn
        .query_row(
            "SELECT value FROM unique_probe WHERE value = $1",
            wattswarm_storage_core::params!["missing"],
            |row| row.get::<usize, String>(0),
        )
        .optional()
        .expect("optional query");
    assert_eq!(missing, None);
}

#[test]
fn sqlite_query_map_matches_the_shared_column_name_contract() {
    let conn = Connection::open_in_memory_sqlite().expect("open sqlite connection");
    conn.execute_batch(
        "CREATE TABLE map_probe (id INTEGER NOT NULL, label TEXT NOT NULL);
         INSERT INTO map_probe(id, label) VALUES (1, 'ready');",
    )
    .expect("create query map probe");

    let mut populated = conn
        .prepare("SELECT id, label FROM map_probe")
        .expect("prepare populated query");
    let rows: Vec<(i64, String)> = populated
        .query_map(wattswarm_storage_core::params![], |row| {
            Ok((row.get::<usize, i64>(0)?, row.get::<usize, String>(1)?))
        })
        .expect("query populated rows")
        .collect::<Result<_, _>>()
        .expect("collect populated rows");
    assert_eq!(rows, vec![(1, "ready".to_owned())]);
    assert_eq!(populated.column_names(), vec!["id", "label"]);

    let mut empty = conn
        .prepare("SELECT id, label FROM map_probe WHERE id < 0")
        .expect("prepare empty query");
    let rows: Vec<(i64, String)> = empty
        .query_map(wattswarm_storage_core::params![], |row| {
            Ok((row.get::<usize, i64>(0)?, row.get::<usize, String>(1)?))
        })
        .expect("query empty rows")
        .collect::<Result<_, _>>()
        .expect("collect empty rows");
    assert!(rows.is_empty());
    assert!(empty.column_names().is_empty());
}

#[test]
fn configured_sqlite_open_uses_the_canonical_runtime_database() {
    let _env_lock = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _backend = EnvVarGuard::set("WATTSWARM_STORAGE_BACKEND", "sqlite");
    let dir = tempdir().expect("temp dir");
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).expect("create state dir");
    let requested_path = state_dir.join("ui.state");
    let canonical_path = state_dir.join(WATTSWARM_SQLITE_FILE);

    let _store = PgStore::open(&requested_path).expect("initialize configured SQLite store");
    let conn = Connection::open(&requested_path).expect("open configured SQLite probe");
    let table_count = conn
        .query_row(
            "SELECT COUNT(*)
             FROM sqlite_master
             WHERE type = 'table'
               AND name IN ('org_registry', 'network_registry')",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, i64>(0),
        )
        .expect("query canonical runtime tables");

    assert_eq!(table_count, 2);
    assert!(canonical_path.is_file());
    assert!(!requested_path.exists());
}

#[test]
fn sqlite_rejects_postgres_constraint_ddl_instead_of_succeeding_silently() {
    let conn = Connection::open_in_memory_sqlite().expect("open SQLite connection");
    conn.execute_batch("CREATE TABLE constraint_probe (id INTEGER);")
        .expect("create constraint probe");

    let error = conn
        .execute_batch(
            "ALTER TABLE constraint_probe
             ADD CONSTRAINT constraint_probe_pkey PRIMARY KEY (id);",
        )
        .expect_err("unsupported constraint migration must fail");
    assert!(
        error
            .to_string()
            .contains("unsupported SQLite schema migration statement")
    );

    let execute_error = conn
        .execute(
            "ALTER TABLE constraint_probe
             DROP CONSTRAINT constraint_probe_pkey",
            wattswarm_storage_core::params![],
        )
        .expect_err("single-statement unsupported migration must fail");
    assert!(
        execute_error
            .to_string()
            .contains("unsupported SQLite schema migration statement")
    );
    conn.execute_batch("SELECT pg_advisory_lock(1);")
        .expect("advisory lock remains an explicit SQLite no-op");
    assert_eq!(
        conn.execute(
            "SELECT pg_advisory_lock($1)",
            wattswarm_storage_core::params![1_i64],
        )
        .expect("single advisory lock remains an explicit SQLite no-op"),
        0
    );

    let dir = tempdir().expect("temp dir");
    let mut client =
        DatabaseClient::open_sqlite(dir.path().join("ddl.sqlite3")).expect("open database client");
    client
        .batch_execute("CREATE TABLE transaction_constraint_probe (id INTEGER)")
        .expect("create database client probe");
    let client_error = client
        .execute(
            "ALTER TABLE transaction_constraint_probe
             ADD CONSTRAINT transaction_constraint_probe_pkey PRIMARY KEY (id)",
            &[],
        )
        .expect_err("database client must reject unsupported migration");
    assert!(
        client_error
            .to_string()
            .contains("unsupported SQLite schema migration statement")
    );
    let mut transaction = client.transaction().expect("open SQLite transaction");
    let transaction_error = transaction
        .execute(
            "ALTER TABLE transaction_constraint_probe
             ALTER COLUMN id SET NOT NULL",
            &[],
        )
        .expect_err("database transaction must reject unsupported migration");
    assert!(
        transaction_error
            .to_string()
            .contains("unsupported SQLite schema migration statement")
    );
}

#[test]
fn sqlite_store_rejects_an_existing_database_with_missing_primary_key() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("invalid-constraints.sqlite3");
    let conn = Connection::open_sqlite(&path).expect("open SQLite setup connection");
    conn.execute_batch(
        "CREATE TABLE network_registry (
             network_id TEXT NOT NULL,
             network_kind TEXT NOT NULL,
             parent_network_id TEXT,
             name TEXT NOT NULL,
             status TEXT NOT NULL,
             genesis_node_id TEXT NOT NULL,
             created_at INTEGER NOT NULL
         );",
    )
    .expect("create invalid legacy table");
    drop(conn);

    let error = PgStore::open_sqlite(&path)
        .err()
        .expect("missing primary key must reject schema initialization");
    assert!(
        error
            .to_string()
            .contains("SQLite schema constraint mismatch for network_registry")
    );
}

#[test]
fn sqlite_store_schema_and_projection_persist_across_reopen() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("node.sqlite3");
    let org_id = "local:sqlite-tests:bootstrap";

    {
        let store = PgStore::open_sqlite(&path)
            .expect("open sqlite store")
            .for_org(org_id);
        store
            .put_membership(r#"{"members":{"node-a":["proposer"]}}"#)
            .expect("store membership");
    }

    let store = PgStore::open_sqlite(&path)
        .expect("reopen sqlite store")
        .for_org(org_id);
    assert_eq!(
        store.load_membership().expect("load membership"),
        Some(r#"{"members":{"node-a":["proposer"]}}"#.to_owned())
    );

    let conn = Connection::open_sqlite(&path).expect("open sqlite probe");
    let required_tables = conn
        .query_row(
            "SELECT COUNT(*)
             FROM sqlite_master
             WHERE type = 'table'
               AND name IN ('events', 'membership_projection', 'network_registry')",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, i64>(0),
        )
        .expect("query required tables");
    assert_eq!(required_tables, 3);
    let foreign_keys = conn
        .query_row(
            "PRAGMA foreign_keys",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, i64>(0),
        )
        .expect("query foreign key pragma");
    assert_eq!(foreign_keys, 1);
}

#[test]
fn unified_sqlite_database_migrates_main_and_local_control_data_once() {
    let dir = tempdir().expect("temp dir");
    let state_dir = dir.path();
    let org_id = "local:sqlite-migration:bootstrap";
    let scope_id = state_dir.to_string_lossy().into_owned();

    {
        let legacy_main = PgStore::open_sqlite(state_dir.join(LEGACY_MAIN_SQLITE_FILE))
            .expect("open legacy main store")
            .for_org(org_id);
        legacy_main
            .put_membership(r#"{"members":{"node-a":["proposer"]}}"#)
            .expect("write legacy main data");
    }
    {
        let legacy_local = PgStore::open_sqlite(state_dir.join(LEGACY_LOCAL_CONTROL_SQLITE_FILE))
            .expect("open legacy local-control store");
        legacy_local
            .replace_local_executors(
                &scope_id,
                &[LocalExecutorEntryRow {
                    name: "core-agent".to_owned(),
                    base_url: "http://127.0.0.1:8787".to_owned(),
                    agent_event_callback_base_url: None,
                    kind: "local".to_owned(),
                    target_node_id: None,
                    scope_hint: None,
                    commit_plane_endpoint: None,
                    commit_plane_token_file: None,
                    updated_at: 1_700_000_000_000,
                }],
                1_700_000_000_000,
            )
            .expect("write legacy local-control data");
    }

    let unified_path = state_dir.join(WATTSWARM_SQLITE_FILE);
    let unified = PgStore::open_sqlite(&unified_path)
        .expect("open and migrate unified store")
        .for_org(org_id);
    assert_eq!(
        unified.load_membership().expect("read migrated membership"),
        Some(r#"{"members":{"node-a":["proposer"]}}"#.to_owned())
    );
    let executors = unified
        .list_local_executors(&scope_id)
        .expect("read migrated executors");
    assert_eq!(executors.len(), 1);
    assert_eq!(executors[0].name, "core-agent");

    PgStore::open_sqlite(&unified_path).expect("repeat idempotent migration");
    let conn = Connection::open_sqlite(&unified_path).expect("open migration probe");
    let migration_count = conn
        .query_row(
            "SELECT COUNT(*)
             FROM wattswarm_sqlite_migrations
             WHERE migration_key IN ('legacy-main-v1', 'legacy-local-control-v1')",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, i64>(0),
        )
        .expect("count completed migrations");
    assert_eq!(migration_count, 2);
    assert!(state_dir.join(LEGACY_MAIN_SQLITE_FILE).is_file());
    assert!(state_dir.join(LEGACY_LOCAL_CONTROL_SQLITE_FILE).is_file());
}

#[test]
fn unified_sqlite_schema_initialization_is_safe_across_concurrent_openers() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join(WATTSWARM_SQLITE_FILE);
    let start = Arc::new(Barrier::new(3));
    let handles = [(), ()].map(|()| {
        let path = path.clone();
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            PgStore::open_sqlite(path).expect("concurrent unified sqlite open")
        })
    });
    start.wait();
    for handle in handles {
        drop(handle.join().expect("schema initializer thread"));
    }

    let conn = Connection::open_sqlite(path).expect("open initialized schema");
    let table_count = conn
        .query_row(
            "SELECT COUNT(*)
             FROM sqlite_master
             WHERE type = 'table'
               AND name IN ('events', 'executor_registry_local', 'network_registry')",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, i64>(0),
        )
        .expect("count initialized tables");
    assert_eq!(table_count, 3);
    assert_eq!(
        conn.query_row(
            "PRAGMA integrity_check",
            wattswarm_storage_core::params![],
            |row| row.get::<usize, String>(0),
        )
        .expect("sqlite integrity check"),
        "ok"
    );
}

#[test]
fn sqlite_transport_queues_preserve_lease_and_retry_semantics() {
    let store = PgStore::open_in_memory_sqlite().expect("open sqlite store");
    let scope_id = "scope-transport";
    let now = 1_700_000_000_000;

    let command = PendingNetworkCommandInsert {
        command_id: "command-1".to_owned(),
        dedup_key: Some("announce:task-1".to_owned()),
        command_kind: "announce_task".to_owned(),
        payload_json: r#"{"task_id":"task-1"}"#.to_owned(),
        attempts: 0,
        next_retry_at: None,
        last_error: None,
        created_at: now,
    };
    assert!(
        store
            .enqueue_pending_network_command(scope_id, &command)
            .expect("enqueue command")
    );
    assert_eq!(
        store
            .pending_network_command_count(scope_id)
            .expect("count commands"),
        1
    );

    let first_claim = store
        .claim_due_pending_network_commands(scope_id, "worker-a", now, 100, 10)
        .expect("claim command");
    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].attempts, 1);
    assert!(
        !store
            .complete_pending_network_command(scope_id, "command-1", "wrong-token")
            .expect("reject stale completion")
    );
    assert!(
        store
            .claim_due_pending_network_commands(scope_id, "worker-b", now + 50, 100, 10)
            .expect("skip active lease")
            .is_empty()
    );
    assert_eq!(
        store
            .recover_expired_pending_network_command_leases(scope_id, now + 100)
            .expect("recover command lease"),
        1
    );

    let second_claim = store
        .claim_due_pending_network_commands(scope_id, "worker-b", now + 100, 100, 10)
        .expect("reclaim command");
    assert_eq!(second_claim[0].attempts, 2);
    assert!(
        store
            .retry_pending_network_command(
                scope_id,
                "command-1",
                &second_claim[0].lease_token,
                now + 300,
                "temporary failure",
            )
            .expect("schedule retry")
    );
    assert!(
        store
            .claim_due_pending_network_commands(scope_id, "worker-a", now + 299, 100, 10)
            .expect("skip future retry")
            .is_empty()
    );
    let final_claim = store
        .claim_due_pending_network_commands(scope_id, "worker-a", now + 300, 100, 10)
        .expect("claim retry");
    assert_eq!(final_claim[0].attempts, 3);
    assert!(
        store
            .complete_pending_network_command(scope_id, "command-1", &final_claim[0].lease_token)
            .expect("complete command")
    );
    assert_eq!(
        store
            .pending_network_command_count(scope_id)
            .expect("count completed commands"),
        0
    );

    let mut lifecycle_command = command.clone();
    lifecycle_command.command_id = "command-lifecycle".to_owned();
    lifecycle_command.dedup_key = None;
    assert!(
        store
            .enqueue_pending_network_command(scope_id, &lifecycle_command)
            .expect("enqueue lifecycle command")
    );
    let lifecycle_claim = store
        .claim_due_pending_network_commands(scope_id, "worker-a", now + 400, 100, 1)
        .expect("claim lifecycle command")
        .remove(0);
    assert!(
        store
            .fail_pending_network_command(
                scope_id,
                &lifecycle_claim.command_id,
                &lifecycle_claim.lease_token,
                "permanent failure",
                now + 401,
            )
            .expect("retain failed command")
    );
    let failed = store
        .list_pending_network_commands(scope_id)
        .expect("list failed command");
    assert_eq!(failed[0].status, "failed");
    assert_eq!(failed[0].last_error.as_deref(), Some("permanent failure"));
    assert!(
        store
            .requeue_pending_network_command(scope_id, "command-lifecycle", now + 500)
            .expect("requeue failed command")
    );
    let requeued = store
        .claim_due_pending_network_commands(scope_id, "worker-b", now + 500, 100, 1)
        .expect("claim requeued command")
        .remove(0);
    assert_eq!(requeued.attempts, 1);
    assert!(
        store
            .await_pending_network_command_remote_ack(
                scope_id,
                &requeued.command_id,
                &requeued.lease_token,
                now + 600,
            )
            .expect("await remote ack")
    );
    assert!(
        store
            .acknowledge_pending_network_command(scope_id, "command-lifecycle")
            .expect("acknowledge command")
    );

    let event = LocalAgentEventRow {
        event_id: "event-1".to_owned(),
        event_type: "task_update".to_owned(),
        source_kind: "network".to_owned(),
        source_node_id: Some("node-a".to_owned()),
        target_agent_id: Some("agent-a".to_owned()),
        target_executor: Some("executor-a".to_owned()),
        agent_envelope_json: None,
        payload_json: r#"{"task_id":"task-1"}"#.to_owned(),
        allowed_actions_json: "[]".to_owned(),
        requires_commit: false,
        status: "pending".to_owned(),
        dedupe_key: Some("task-update:task-1".to_owned()),
        correlation_id: Some("correlation-1".to_owned()),
        created_at: now,
        updated_at: now,
    };
    assert!(
        store
            .enqueue_local_agent_event(scope_id, &event)
            .expect("enqueue event")
    );
    assert!(
        !store
            .enqueue_local_agent_event(scope_id, &event)
            .expect("deduplicate event")
    );

    let event_claim = store
        .claim_due_local_agent_events(scope_id, "inbox-a", now, 100, 10)
        .expect("claim event");
    assert_eq!(event_claim.len(), 1);
    assert_eq!(event_claim[0].attempts, 1);
    assert!(
        !store
            .finish_local_agent_event_claim(
                scope_id,
                "event-1",
                "wrong-token",
                "delivered",
                None,
                now + 1,
            )
            .expect("reject stale event completion")
    );
    assert!(
        store
            .retry_local_agent_event_claim(
                scope_id,
                "event-1",
                &event_claim[0].lease_token,
                now + 200,
                "executor unavailable",
                now + 1,
            )
            .expect("schedule event retry")
    );
    assert!(
        store
            .claim_due_local_agent_events(scope_id, "inbox-b", now + 199, 100, 10)
            .expect("skip future event retry")
            .is_empty()
    );
    let retried_event = store
        .claim_due_local_agent_events(scope_id, "inbox-b", now + 200, 100, 10)
        .expect("claim event retry");
    assert_eq!(retried_event[0].attempts, 2);
    assert!(
        store
            .finish_local_agent_event_claim(
                scope_id,
                "event-1",
                &retried_event[0].lease_token,
                "delivered",
                None,
                now + 201,
            )
            .expect("finish event")
    );
}

#[test]
fn sqlite_migrates_legacy_pending_command_status_constraint() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("legacy-command-status.sqlite3");
    let setup = Connection::open_sqlite(&path).expect("open legacy sqlite");
    setup
        .execute_batch(
            "CREATE TABLE pending_network_commands_local (
                 scope_id TEXT NOT NULL DEFAULT '', command_id TEXT NOT NULL,
                 dedup_key TEXT, command_kind TEXT NOT NULL, payload_json TEXT NOT NULL,
                 status TEXT NOT NULL DEFAULT 'queued', attempts BIGINT NOT NULL DEFAULT 0,
                 next_retry_at TIMESTAMPTZ, last_error TEXT, lease_owner TEXT,
                 lease_token TEXT, lease_expires_at TIMESTAMPTZ,
                 created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY(scope_id, command_id),
                 CHECK(status IN ('queued', 'in_flight', 'awaiting_ack'))
             );
             INSERT INTO pending_network_commands_local(
                 scope_id, command_id, command_kind, payload_json, status,
                 created_at, updated_at
             ) VALUES ('scope', 'legacy-command', 'test', '{}', 'queued', 1, 1);",
        )
        .expect("create legacy command table");
    drop(setup);

    let store = PgStore::open_sqlite(&path).expect("migrate legacy sqlite");
    let claim = store
        .claim_due_pending_network_commands("scope", "worker", 2, 100, 1)
        .expect("claim migrated command")
        .remove(0);
    assert!(
        store
            .fail_pending_network_command(
                "scope",
                &claim.command_id,
                &claim.lease_token,
                "terminal",
                3,
            )
            .expect("write failed status after migration")
    );
}

#[test]
fn sqlite_migrates_legacy_client_server_transport_columns() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("legacy-client-server.sqlite3");
    let setup = Connection::open_sqlite(&path).expect("open legacy sqlite");
    setup
        .execute_batch(
            "CREATE TABLE cs_outbound_progress_local (
                 scope_id TEXT NOT NULL DEFAULT '', source_id TEXT NOT NULL,
                 outbound_partition TEXT NOT NULL, scanned_sequence BIGINT NOT NULL,
                 cutover_sequence BIGINT NOT NULL, retry_attempts BIGINT NOT NULL DEFAULT 0,
                 next_retry_at TIMESTAMPTZ, last_error TEXT, updated_at TIMESTAMPTZ NOT NULL,
                 PRIMARY KEY(scope_id, source_id, outbound_partition));
             INSERT INTO cs_outbound_progress_local(
                 scope_id, source_id, outbound_partition, scanned_sequence,
                 cutover_sequence, updated_at
             ) VALUES ('scope','events','global_bulk',7,5,10);
             CREATE TABLE cs_mailbox_gap_local (
                 scope_id TEXT NOT NULL DEFAULT '', gap_id TEXT NOT NULL,
                 delivery_class TEXT NOT NULL, delivery_policy_version BIGINT NOT NULL,
                 route_json TEXT NOT NULL, reason TEXT NOT NULL,
                 first_affected_at TIMESTAMPTZ NOT NULL, last_affected_at TIMESTAMPTZ NOT NULL,
                 approximate_count BIGINT NOT NULL, acknowledged_at TIMESTAMPTZ,
                 updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY(scope_id, gap_id));
             INSERT INTO cs_mailbox_gap_local(
                 scope_id, gap_id, delivery_class, delivery_policy_version, route_json,
                 reason, first_affected_at, last_affected_at, approximate_count, updated_at
             ) VALUES ('scope','gap-legacy','interactive',1,'{}','expired',1,1,1,1);",
        )
        .expect("create legacy ClientServer tables");
    drop(setup);

    let store = PgStore::open_sqlite(&path).expect("migrate legacy ClientServer tables");
    let progress = store
        .load_cs_outbound_progress("scope", "events", "global_bulk")
        .expect("load migrated progress")
        .expect("legacy progress retained");
    assert_eq!(progress.delivery_policy_version, 1);
    store
        .save_cs_mailbox_gap(
            "scope",
            "gap-legacy",
            "interactive",
            1,
            "{}",
            "expired",
            1,
            2,
            2,
            "page-after-migration",
            20,
        )
        .expect("write migrated gap page binding");
}

#[test]
fn sqlite_client_server_progress_and_delivery_state_are_durable() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("client-server-state.sqlite3");
    let store = PgStore::open_sqlite(&path).expect("open sqlite store");
    let tenant_instance = store
        .load_or_create_cs_tenant_instance_id("scope", 90)
        .expect("create tenant instance");
    assert_eq!(
        store
            .load_or_create_cs_tenant_instance_id("scope", 91)
            .expect("reload tenant instance"),
        tenant_instance
    );
    let partitions = [
        "global_interactive",
        "global_bulk",
        "non_global_interactive",
        "non_global_bulk",
    ];
    store
        .initialize_cs_outbound_progress("scope", "events", &partitions, 40, 1, 40, 100)
        .expect("initialize progress");
    store
        .initialize_cs_outbound_progress("scope", "events", &partitions, 99, 1, 44, 101)
        .expect("idempotent progress initialization");
    let initial = store
        .load_cs_outbound_progress("scope", "events", "global_bulk")
        .expect("load progress")
        .expect("progress exists");
    assert_eq!(initial.scanned_sequence, 40);
    assert_eq!(initial.cutover_sequence, 40);
    assert_eq!(initial.delivery_policy_version, 1);
    assert!(
        store
            .retry_cs_outbound_progress(
                "scope",
                "events",
                "global_bulk",
                40,
                200,
                "backpressure",
                110,
            )
            .expect("persist retry")
    );
    assert!(
        store
            .advance_cs_outbound_progress("scope", "events", "global_bulk", 40, 44, 120)
            .expect("advance accepted partition")
    );
    assert!(
        !store
            .advance_cs_outbound_progress("scope", "events", "global_bulk", 40, 45, 121)
            .expect("reject stale cursor advance")
    );
    assert_eq!(
        store
            .load_cs_outbound_progress("scope", "events", "non_global_interactive")
            .expect("load independent partition")
            .expect("independent partition exists")
            .scanned_sequence,
        40
    );
    assert!(
        store
            .initialize_cs_outbound_progress("scope", "events", &partitions, 40, 2, 44, 121)
            .is_err(),
        "policy upgrade must wait until every partition reaches the source head"
    );
    for partition in [
        "global_interactive",
        "non_global_interactive",
        "non_global_bulk",
    ] {
        assert!(
            store
                .advance_cs_outbound_progress("scope", "events", partition, 40, 44, 122)
                .expect("drain partition")
        );
    }
    store
        .initialize_cs_outbound_progress("scope", "events", &partitions, 40, 2, 44, 123)
        .expect("upgrade drained policy");
    assert_eq!(
        store
            .load_cs_outbound_progress("scope", "events", "global_bulk")
            .expect("load upgraded progress")
            .expect("upgraded progress exists")
            .delivery_policy_version,
        2
    );

    let delivery = CsMailboxDeliveryState {
        delivery_id: "delivery-1".to_owned(),
        record_id: "event-1".to_owned(),
        delivery_class: "interactive".to_owned(),
        delivery_policy_version: 1,
        result_status: "commit_pending".to_owned(),
        page_id: Some("page-1".to_owned()),
        pending_commit_token: Some("secret-token".to_owned()),
        last_error: None,
    };
    store
        .save_cs_mailbox_delivery_state("scope", &delivery, 130)
        .expect("save delivery state");
    store
        .save_cs_mailbox_pending_commit("scope", "page-1", "interactive", "page-secret", 131)
        .expect("save pending page commit");
    store
        .save_cs_mailbox_gap(
            "scope",
            "gap-1",
            "interactive",
            1,
            "{}",
            "expired",
            100,
            110,
            2,
            "page-1",
            131,
        )
        .expect("save local mailbox gap");
    assert_eq!(
        store
            .unacknowledged_cs_mailbox_gap_count("scope", "interactive")
            .expect("count gaps"),
        1
    );
    assert!(
        !format!(
            "{:?}",
            store
                .list_cs_mailbox_pending_commits("scope")
                .expect("list pending commits")
        )
        .contains("page-secret")
    );
    assert!(
        store
            .record_cs_mailbox_pending_commit_error(
                "scope",
                "page-1",
                "interactive",
                "gateway unavailable",
                132,
            )
            .expect("record pending commit error")
    );
    drop(store);
    let store = PgStore::open_sqlite(&path).expect("reopen sqlite store");
    let pending = store
        .list_cs_mailbox_pending_commits("scope")
        .expect("load pending commit")
        .remove(0);
    assert_eq!(pending.commit_token, "page-secret");
    assert_eq!(pending.attempts, 1);
    assert_eq!(pending.last_error.as_deref(), Some("gateway unavailable"));
    assert_eq!(
        store
            .load_cs_mailbox_delivery_state("scope", "delivery-1")
            .expect("load delivery")
            .expect("delivery exists"),
        delivery
    );
    assert_eq!(
        store
            .mark_cs_mailbox_page_committed("scope", "page-1", "bulk", 140)
            .expect("prevent cross-class commit"),
        0
    );
    assert_eq!(
        store
            .mark_cs_mailbox_page_committed("scope", "page-1", "interactive", 140)
            .expect("commit page"),
        1
    );
    let committed = store
        .load_cs_mailbox_delivery_state("scope", "delivery-1")
        .expect("load committed delivery")
        .expect("committed delivery exists");
    assert_eq!(committed.result_status, "committed");
    assert_eq!(committed.pending_commit_token, None);
    assert_eq!(
        store
            .mark_cs_mailbox_gaps_acknowledged("scope", "page-1", "interactive", 140)
            .expect("acknowledge gap"),
        1
    );
    assert_eq!(
        store
            .unacknowledged_cs_mailbox_gap_count("scope", "interactive")
            .expect("count acknowledged gaps"),
        0
    );
    assert_eq!(
        store
            .cs_mailbox_gap_count("scope", "interactive")
            .expect("retain history-integrity gap"),
        1
    );
    let backend_status = NetworkBackendStatusRow {
        backend: Some("client_server".to_owned()),
        status: "ready".to_owned(),
        reason: None,
        published: 3,
        received: 4,
        retries: 1,
        backend_details_json: r#"{"history_unavailable":true}"#.to_owned(),
        updated_at: 150,
    };
    store
        .store_network_backend_status("scope", &backend_status)
        .expect("store backend status");
    assert_eq!(
        store
            .load_network_backend_status("scope")
            .expect("load backend status"),
        Some(backend_status)
    );
    assert!(
        store
            .clear_cs_mailbox_pending_commit("scope", "page-1", "interactive")
            .expect("clear pending page commit")
    );
    assert!(
        store
            .list_cs_mailbox_pending_commits("scope")
            .expect("list cleared pending commits")
            .is_empty()
    );
}
