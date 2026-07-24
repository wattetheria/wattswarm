use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;
use tempfile::tempdir;
use wattswarm_storage_core::storage::pg::{
    BackendKind, Connection, DatabaseClient, Error, ErrorCode, OptionalExtension, types::ValueRef,
};
use wattswarm_storage_core::storage::sqlite_layout::{
    LEGACY_LOCAL_CONTROL_SQLITE_FILE, LEGACY_MAIN_SQLITE_FILE, WATTSWARM_SQLITE_FILE,
};
use wattswarm_storage_core::storage::{LocalExecutorEntryRow, PgStore};

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
