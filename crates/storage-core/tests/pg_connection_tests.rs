use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use wattswarm_crypto::NodeIdentity;
use wattswarm_storage_core::storage::pg::ErrorCode;
use wattswarm_storage_core::storage::pg::{
    Connection, Error, OptionalExtension, ParamValue, types::ValueRef,
};
use wattswarm_storage_core::storage::{PgStore, local_control_scope_id};

const TEST_SCHEMA: &str = "test";
const TEST_DB_LOCK_KEY: i64 = 1_987_654_321;
static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn test_iroh_endpoint_id(seed: u8) -> String {
    NodeIdentity::from_seed([seed; 32]).node_id()
}

fn test_iroh_contact_material(endpoint_id: &str, generated_at: u64) -> serde_json::Value {
    serde_json::json!({
        "node_id": "node-a",
        "peer_id": endpoint_id,
        "listen_addrs": ["127.0.0.1:4001"],
        "transports": [{
            "transport": "iroh_direct",
            "peer_id": endpoint_id,
            "metadata": {
                "route": "iroh_direct",
                "generated_at": generated_at,
                "endpoint_id": endpoint_id,
                "alpn": "/wattswarm/iroh/1",
                "listen_addrs": ["127.0.0.1:4001"],
                "capabilities": {
                    "supports_iroh_direct": true,
                    "supports_http_gateway": false,
                    "max_chunk_bytes": 16777216_u64
                }
            },
            "extra": {
                "endpoint_id": endpoint_id,
                "alpn": "/wattswarm/iroh/1",
                "direct_addrs": ["127.0.0.1:4001"],
                "relay_urls": []
            }
        }]
    })
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
        let conn = Connection::open("pg-connection-db-lock").expect("open db lock connection");
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
    .expect("reset test schema");
    // SAFETY: tests serialize env mutations via ENV_LOCK.
    unsafe {
        if let Some(value) = prev_schema {
            std::env::set_var("WATTSWARM_PG_SCHEMA", value);
        } else {
            std::env::remove_var("WATTSWARM_PG_SCHEMA");
        }
    }
}

fn with_test_schema<T>(f: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    let _backend_guard = EnvVarGuard::set("WATTSWARM_STORAGE_BACKEND", "postgres");
    let _db_lock = DbTestLock::acquire();
    reset_test_schema(TEST_SCHEMA);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", TEST_SCHEMA);
    f()
}

fn open_test_connection() -> Connection {
    Connection::open("pg-connection-tests.state").expect("open pg-backed test schema")
}

#[test]
fn connection_round_trip_covers_value_and_index_conversions() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.query_row(
            "SELECT
            12::SMALLINT AS i2,
            34::INTEGER AS i4,
            56::BIGINT AS i8,
            1.25::REAL AS f4,
            2.5::DOUBLE PRECISION AS f8,
            TRUE AS b,
            'hello'::TEXT AS t,
            E'\\\\x000102'::BYTEA AS by,
            NULL::BIGINT AS n",
            wattswarm_storage_core::params![],
            |row| {
                assert_eq!(row.get::<i32, i32>(1)?, 34);
                assert_eq!(row.get::<usize, i64>(2)?, 56);
                assert_eq!(row.get::<i64, i64>(2)?, 56);
                assert!((row.get::<usize, f64>(4)? - 2.5).abs() < 1e-9);
                assert!(row.get::<usize, bool>(5)?);
                assert_eq!(row.get::<usize, String>(6)?, "hello");
                assert_eq!(row.get::<usize, Option<i64>>(8)?, None);

                assert!(matches!(row.get_ref(0)?, ValueRef::Integer(12)));
                assert!(matches!(row.get_ref(3)?, ValueRef::Real(v) if (v - 1.25).abs() < 1e-6));
                assert!(matches!(row.get_ref(5)?, ValueRef::Integer(1)));
                assert!(matches!(row.get_ref(6)?, ValueRef::Text(ref t) if t == "hello"));
                assert!(matches!(row.get_ref(7)?, ValueRef::Blob(ref by) if by == &vec![0, 1, 2]));
                assert!(matches!(row.get_ref(8)?, ValueRef::Null));

                assert_eq!(row.column_count(), 9);
                assert_eq!(row.column_name(0), "i2");
                Ok(())
            },
        )
        .expect("query and decode row");
    });
}

#[test]
fn pg_store_can_open_same_schema_multiple_times_in_one_process() {
    with_test_schema(|| {
        let state_path = "pg-store-reopen.state";
        let _first = PgStore::open(state_path).expect("open first pg store");
        let conn = open_test_connection();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pg_store_reopen_probe (
                id BIGINT PRIMARY KEY
            )",
        )
        .expect("create probe table");

        let _second = PgStore::open(state_path).expect("open second pg store");
        let count = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = current_schema()
                   AND table_name = 'pg_store_reopen_probe'",
                wattswarm_storage_core::params![],
                |row| row.get::<_, i64>(0),
            )
            .expect("probe table should remain visible");
        assert_eq!(count, 1);
    });
}

#[test]
fn pg_store_recreates_required_tables_removed_after_schema_cache_hit() {
    with_test_schema(|| {
        let state_path = "pg-store-schema-repair.state";
        let _first = PgStore::open(state_path).expect("initialize pg store");
        let conn = open_test_connection();
        conn.execute_batch(
            "DROP TABLE discovered_peers_local;
             DROP TABLE network_ban_windows;",
        )
        .expect("drop required tables");

        let _second = PgStore::open(state_path).expect("repair missing required tables");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM information_schema.tables
                 WHERE table_schema = current_schema()
                   AND table_name IN ('discovered_peers_local', 'network_ban_windows')",
                wattswarm_storage_core::params![],
                |row| row.get(0),
            )
            .expect("query repaired required tables");
        assert_eq!(count, 2);
    });
}

fn postgres_primary_keys(conn: &Connection) -> BTreeMap<String, Vec<String>> {
    let mut statement = conn
        .prepare(
            "SELECT tc.table_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON kcu.constraint_schema = tc.constraint_schema
              AND kcu.constraint_name = tc.constraint_name
              AND kcu.table_name = tc.table_name
             WHERE tc.table_schema = current_schema()
               AND tc.constraint_type = 'PRIMARY KEY'
             ORDER BY tc.table_name, kcu.ordinal_position",
        )
        .expect("prepare PostgreSQL primary key query");
    let rows = statement
        .query_map(wattswarm_storage_core::params![], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query PostgreSQL primary keys");
    let mut keys = BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        let (table, column) = row.expect("decode PostgreSQL primary key");
        keys.entry(table).or_default().push(column);
    }
    keys
}

fn sqlite_primary_keys(
    conn: &Connection,
    tables: &BTreeSet<String>,
) -> BTreeMap<String, Vec<String>> {
    let mut keys = BTreeMap::new();
    for table in tables {
        let mut statement = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .expect("prepare SQLite primary key query");
        let rows = statement
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((row.get::<_, i64>(5)?, row.get::<_, String>(1)?))
            })
            .expect("query SQLite primary key");
        let mut columns = rows
            .collect::<Result<Vec<_>, _>>()
            .expect("collect SQLite primary key")
            .into_iter()
            .filter(|(position, _)| *position > 0)
            .collect::<Vec<_>>();
        columns.sort_by_key(|(position, _)| *position);
        if !columns.is_empty() {
            keys.insert(
                table.clone(),
                columns.into_iter().map(|(_, column)| column).collect(),
            );
        }
    }
    keys
}

fn postgres_unique_keys(conn: &Connection) -> BTreeSet<(String, Vec<String>)> {
    let mut statement = conn
        .prepare(
            "SELECT tc.table_name, tc.constraint_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON kcu.constraint_schema = tc.constraint_schema
              AND kcu.constraint_name = tc.constraint_name
              AND kcu.table_name = tc.table_name
             WHERE tc.table_schema = current_schema()
               AND tc.constraint_type = 'UNIQUE'
             ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position",
        )
        .expect("prepare PostgreSQL unique key query");
    let rows = statement
        .query_map(wattswarm_storage_core::params![], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .expect("query PostgreSQL unique keys");
    let mut grouped = BTreeMap::<(String, String), Vec<String>>::new();
    for row in rows {
        let (table, constraint, column) = row.expect("decode PostgreSQL unique key");
        grouped.entry((table, constraint)).or_default().push(column);
    }
    grouped
        .into_iter()
        .map(|((table, _), columns)| (table, columns))
        .collect()
}

fn sqlite_unique_keys(
    conn: &Connection,
    tables: &BTreeSet<String>,
) -> BTreeSet<(String, Vec<String>)> {
    let mut keys = BTreeSet::new();
    for table in tables {
        let mut statement = conn
            .prepare(&format!("PRAGMA index_list({table})"))
            .expect("prepare SQLite index list");
        let indexes = statement
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .expect("query SQLite index list")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect SQLite index list");
        for (index_name, unique, origin) in indexes {
            if unique == 0 || origin != "u" {
                continue;
            }
            let escaped_index = index_name.replace('"', "\"\"");
            let mut index_statement = conn
                .prepare(&format!("PRAGMA index_info(\"{escaped_index}\")"))
                .expect("prepare SQLite unique key query");
            let mut columns = index_statement
                .query_map(wattswarm_storage_core::params![], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(2)?))
                })
                .expect("query SQLite unique key")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect SQLite unique key");
            columns.sort_by_key(|(position, _)| *position);
            keys.insert((
                table.clone(),
                columns.into_iter().map(|(_, column)| column).collect(),
            ));
        }
    }
    keys
}

#[test]
fn sqlite_and_postgres_initialize_the_same_storage_tables() {
    with_test_schema(|| {
        let _pg_store = PgStore::open("schema-parity.state").expect("initialize postgres schema");
        let pg_conn = open_test_connection();
        let mut pg_stmt = pg_conn
            .prepare(
                "SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = current_schema()
                   AND table_type = 'BASE TABLE'
                 ORDER BY table_name",
            )
            .expect("prepare postgres table query");
        let pg_tables = pg_stmt
            .query_map(wattswarm_storage_core::params![], |row| {
                row.get::<_, String>(0)
            })
            .expect("query postgres tables")
            .collect::<Result<BTreeSet<_>, _>>()
            .expect("collect postgres tables");

        let dir = tempfile::tempdir().expect("temp dir");
        let sqlite_path = dir.path().join("schema-parity.sqlite3");
        let _sqlite_store = PgStore::open_sqlite(&sqlite_path).expect("initialize sqlite schema");
        let sqlite_conn = Connection::open_sqlite(&sqlite_path).expect("open sqlite schema probe");
        let mut sqlite_stmt = sqlite_conn
            .prepare(
                "SELECT name
                 FROM sqlite_master
                 WHERE type = 'table'
                   AND name NOT LIKE 'sqlite_%'
                 ORDER BY name",
            )
            .expect("prepare sqlite table query");
        let sqlite_tables = sqlite_stmt
            .query_map(wattswarm_storage_core::params![], |row| {
                row.get::<_, String>(0)
            })
            .expect("query sqlite tables")
            .collect::<Result<BTreeSet<_>, _>>()
            .expect("collect sqlite tables");

        assert_eq!(sqlite_tables, pg_tables);
        assert_eq!(
            sqlite_primary_keys(&sqlite_conn, &sqlite_tables),
            postgres_primary_keys(&pg_conn)
        );
        assert_eq!(
            sqlite_unique_keys(&sqlite_conn, &sqlite_tables),
            postgres_unique_keys(&pg_conn)
        );
    });
}

#[test]
fn statement_query_map_populates_column_names() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch("CREATE TABLE pg_map_rows (x BIGINT, y TEXT)")
            .expect("create table");
        conn.execute(
            "INSERT INTO pg_map_rows(x, y) VALUES ($1, $2)",
            wattswarm_storage_core::params![1_i64, "a"],
        )
        .expect("insert row 1");
        conn.execute(
            "INSERT INTO pg_map_rows(x, y) VALUES ($1, $2)",
            wattswarm_storage_core::params![2_i64, "b"],
        )
        .expect("insert row 2");

        let mut stmt = conn
            .prepare("SELECT x, y FROM pg_map_rows ORDER BY x ASC")
            .expect("prepare query");
        assert!(stmt.column_names().is_empty());

        let mapped = stmt
            .query_map(wattswarm_storage_core::params![], |r| {
                Ok((r.get::<usize, i64>(0)?, r.get::<usize, String>(1)?))
            })
            .expect("query map");
        let rows: Vec<(i64, String)> = mapped.collect::<Result<_, _>>().expect("collect mapped");

        assert_eq!(stmt.column_names(), vec!["x", "y"]);
        assert_eq!(rows, vec![(1, "a".to_owned()), (2, "b".to_owned())]);
    });
}

#[test]
fn optional_extension_maps_no_rows_to_none() {
    with_test_schema(|| {
        let conn = open_test_connection();

        let no_rows = conn
            .query_row(
                "SELECT 1 WHERE FALSE",
                wattswarm_storage_core::params![],
                |r| r.get::<usize, i64>(0),
            )
            .optional()
            .expect("optional result");
        assert_eq!(no_rows, None);

        let err = conn
            .query_row(
                "SELECT 1 WHERE FALSE",
                wattswarm_storage_core::params![],
                |r| r.get::<usize, i64>(0),
            )
            .expect_err("no rows should be an error without optional");
        assert!(matches!(err, Error::QueryReturnedNoRows));
    });
}

#[test]
fn postgres_json_path_query_works() {
    with_test_schema(|| {
        let conn = open_test_connection();

        let extracted = conn
            .query_row(
                "SELECT ('{\"meta\":{\"score\":\"9\"}}'::jsonb #>> '{meta,score}')",
                wattswarm_storage_core::params![],
                |r| r.get::<usize, String>(0),
            )
            .expect("json path should work");
        assert_eq!(extracted, "9");
    });
}

#[test]
fn legacy_bigint_bool_columns_are_migrated_to_boolean() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch(
            "
            DROP TABLE IF EXISTS verifier_results;
            DROP TABLE IF EXISTS knowledge_lookups;
            CREATE TABLE verifier_results (
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                verifier_node_id TEXT NOT NULL,
                result_json TEXT NOT NULL,
                passed BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(task_id, candidate_id, verifier_node_id)
            );
            CREATE TABLE knowledge_lookups (
                task_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                lookup_time TIMESTAMPTZ NOT NULL,
                hit_count BIGINT NOT NULL,
                hits_digest TEXT NOT NULL,
                reuse_applied BIGINT NOT NULL DEFAULT 0
            );
            ",
        )
        .expect("create legacy tables");

        let store = PgStore::open("legacy-bool-migration.state").expect("open store");
        drop(store);

        let passed_type = conn
            .query_row(
                "SELECT data_type
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'verifier_results'
                   AND column_name = 'passed'",
                wattswarm_storage_core::params![],
                |r| r.get::<usize, String>(0),
            )
            .expect("passed type");
        assert_eq!(passed_type, "boolean");

        let reuse_type = conn
            .query_row(
                "SELECT data_type
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'knowledge_lookups'
                   AND column_name = 'reuse_applied'",
                wattswarm_storage_core::params![],
                |r| r.get::<usize, String>(0),
            )
            .expect("reuse_applied type");
        assert_eq!(reuse_type, "boolean");
    });
}

#[test]
fn legacy_network_ban_windows_are_migrated_to_org_scoped_primary_key() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch(
            "
            DROP TABLE IF EXISTS network_ban_windows;
            CREATE TABLE network_ban_windows (
                node_id TEXT NOT NULL,
                starts_at BIGINT NOT NULL,
                until_ms BIGINT,
                reason TEXT NOT NULL,
                penalized_by_node_id TEXT NOT NULL,
                PRIMARY KEY(node_id, starts_at)
            );
            INSERT INTO network_ban_windows(
                node_id,
                starts_at,
                until_ms,
                reason,
                penalized_by_node_id
            )
            VALUES ('node-a', 100, 200, 'legacy', 'genesis');
            ",
        )
        .expect("create legacy network ban windows table");

        let store = PgStore::open("legacy-network-ban-windows-migration.state")
            .expect("open store")
            .for_org("local:test-storage:bootstrap");

        assert!(
            !store
                .is_node_network_banned_at("node-a", 150)
                .expect("query migrated network ban windows"),
            "legacy unset-org rows should not leak into the configured org"
        );
        store
            .put_node_penalty_with_network_ban(
                "node-a",
                "org-scoped",
                true,
                true,
                Some(300),
                "genesis",
                100,
            )
            .expect("insert org-scoped network ban window");
        assert!(
            store
                .is_node_network_banned_at("node-a", 150)
                .expect("query org-scoped network ban window"),
            "org-scoped network ban should be readable after legacy migration"
        );

        let org_id_count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'network_ban_windows'
                   AND column_name = 'org_id'",
                wattswarm_storage_core::params![],
                |row| row.get(0),
            )
            .expect("query org_id column count");
        assert_eq!(org_id_count, 1);

        let rows_for_node: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM network_ban_windows WHERE node_id = $1 AND starts_at = $2",
                wattswarm_storage_core::params!["node-a", 100_i64],
                |row| row.get(0),
            )
            .expect("query migrated and org-scoped rows");
        assert_eq!(rows_for_node, 2);
    });
}

#[test]
fn legacy_discovered_peers_rows_are_migrated_to_scope_and_real_node_id() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch(
            "
            DROP TABLE IF EXISTS discovered_peers_local;
            CREATE TABLE discovered_peers_local (
                node_id TEXT PRIMARY KEY,
                listen_addr TEXT,
                discovered_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            ",
        )
        .expect("create legacy discovered peers table");

        let scope_id = local_control_scope_id(std::path::Path::new("/tmp/wattswarm-state"));
        let legacy_node_id = format!("{scope_id}\u{1f}peer-a");
        conn.execute(
            "INSERT INTO discovered_peers_local(node_id, listen_addr, discovered_at, updated_at)
             VALUES (
                $1,
                $2,
                TIMESTAMPTZ 'epoch' + (1700000000000::bigint * INTERVAL '1 millisecond'),
                TIMESTAMPTZ 'epoch' + (1700000000000::bigint * INTERVAL '1 millisecond')
             )",
            wattswarm_storage_core::params![legacy_node_id, "127.0.0.1:4001"],
        )
        .expect("insert legacy discovered peer");

        let store = PgStore::open("legacy-discovered-peers-migration.state").expect("open store");
        drop(store);

        let rows = conn
            .prepare(
                "SELECT scope_id, node_id, source_kind
                 FROM discovered_peers_local",
            )
            .expect("prepare migrated discovered peers query")
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, String>(1)?,
                    row.get::<usize, String>(2)?,
                ))
            })
            .expect("query migrated discovered peers")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect migrated discovered peers");

        assert_eq!(
            rows,
            vec![(scope_id, "peer-a".to_owned(), "unknown".to_owned(),)]
        );
        let remaining_listen_addr_columns: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'discovered_peers_local'
                   AND column_name = 'listen_addr'",
                wattswarm_storage_core::params![],
                |row| row.get(0),
            )
            .expect("query discovered peers column check");
        assert_eq!(remaining_listen_addr_columns, 0);
    });
}

#[test]
fn legacy_executor_registry_rows_are_migrated_to_scope_and_real_executor_name() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch(
            "
            DROP TABLE IF EXISTS executor_registry_local;
            CREATE TABLE executor_registry_local (
                executor_name TEXT PRIMARY KEY,
                base_url TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            ",
        )
        .expect("create legacy executor registry table");

        let scope_id = local_control_scope_id(std::path::Path::new("/tmp/wattswarm-state"));
        let legacy_executor_name = format!("{scope_id}\u{1f}Boss");
        conn.execute(
            "INSERT INTO executor_registry_local(executor_name, base_url, updated_at)
             VALUES (
                $1,
                $2,
                TIMESTAMPTZ 'epoch' + (1700000000000::bigint * INTERVAL '1 millisecond')
             )",
            wattswarm_storage_core::params![
                legacy_executor_name,
                "http://host.docker.internal:9077/runtime/Boss"
            ],
        )
        .expect("insert legacy executor row");

        let store = PgStore::open("legacy-executor-migration.state").expect("open store");
        drop(store);

        let rows = conn
            .prepare(
                "SELECT scope_id, executor_name, base_url, kind, target_node_id, scope_hint
                 FROM executor_registry_local",
            )
            .expect("prepare migrated executor query")
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, String>(1)?,
                    row.get::<usize, String>(2)?,
                    row.get::<usize, String>(3)?,
                    row.get::<usize, Option<String>>(4)?,
                    row.get::<usize, Option<String>>(5)?,
                ))
            })
            .expect("query migrated executors")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect migrated executors");

        assert_eq!(
            rows,
            vec![(
                scope_id,
                "Boss".to_owned(),
                "http://host.docker.internal:9077/runtime/Boss".to_owned(),
                "local".to_owned(),
                None,
                None,
            )]
        );
    });
}

#[test]
fn local_control_peer_metadata_and_relationship_roundtrip() {
    with_test_schema(|| {
        let store =
            PgStore::open("peer-metadata-relationship-roundtrip.state").expect("open store");
        let scope_id = local_control_scope_id(std::path::Path::new("/tmp/wattswarm-state"));
        let iroh_endpoint_id = test_iroh_endpoint_id(41);

        store
            .upsert_local_peer_metadata(
                &scope_id,
                &wattswarm_storage_core::storage::LocalPeerMetadataRow {
                    node_id: "peer-a".to_owned(),
                    network_id: Some("mainnet:watt-galaxy".to_owned()),
                    params_version: Some(7),
                    params_hash: Some("params-abc".to_owned()),
                    agent_version_raw: Some(
                        "wattswarm-network-p2p|mainnet:watt-galaxy|7|params-abc".to_owned(),
                    ),
                    agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
                    protocol_version: Some("wattswarm/1.0.0".to_owned()),
                    observed_addr: Some("198.51.100.2:4001".to_owned()),
                    listen_addrs_json: serde_json::to_string(&vec!["203.0.113.10:4001".to_owned()])
                        .expect("listen_addrs_json"),
                    protocols_json: serde_json::to_string(&vec!["/meshsub/1.1.0".to_owned()])
                        .expect("protocols_json"),
                    handshake_status: "identified".to_owned(),
                    last_error: None,
                    contact_material_json: Some(
                        serde_json::to_string(&test_iroh_contact_material(
                            &iroh_endpoint_id,
                            1_700_000_000_400,
                        ))
                        .expect("contact material"),
                    ),
                    contact_material_signature: Some("sig-peer-a".to_owned()),
                    contact_material_updated_at: Some(1_700_000_000_400),
                    first_identified_at: 1_700_000_000_000,
                    last_identified_at: 1_700_000_000_500,
                },
            )
            .expect("save peer metadata");

        store
            .upsert_local_peer_relationship(
                &scope_id,
                &wattswarm_storage_core::storage::LocalPeerRelationshipRow {
                    remote_node_id: "peer-a".to_owned(),
                    relationship_state: "accepted".to_owned(),
                    last_action: "accept".to_owned(),
                    initiated_by: "local".to_owned(),
                    agent_envelope_json: None,
                    requested_at: Some(1_700_000_000_100),
                    responded_at: Some(1_700_000_000_200),
                    blocked_at: None,
                    cleared_at: None,
                    updated_at: 1_700_000_000_200,
                },
            )
            .expect("save peer relationship");

        let metadata = store
            .list_local_peer_metadata(&scope_id)
            .expect("list peer metadata");
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].node_id, "peer-a");
        assert_eq!(
            metadata[0].network_id.as_deref(),
            Some("mainnet:watt-galaxy")
        );
        assert_eq!(metadata[0].params_version, Some(7));
        assert_eq!(metadata[0].handshake_status, "identified");
        assert_eq!(
            metadata[0].contact_material_signature.as_deref(),
            Some("sig-peer-a")
        );
        assert_eq!(
            metadata[0].contact_material_updated_at,
            Some(1_700_000_000_400)
        );
        let contact_material = metadata[0]
            .contact_material_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .expect("stored iroh contact material json");
        assert_eq!(contact_material["peer_id"], iroh_endpoint_id);
        assert_eq!(
            contact_material["transports"][0]["transport"],
            "iroh_direct"
        );
        assert_eq!(
            contact_material["transports"][0]["metadata"]["endpoint_id"],
            iroh_endpoint_id
        );

        let relationships = store
            .list_local_peer_relationships(&scope_id)
            .expect("list peer relationships");
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].remote_node_id, "peer-a");
        assert_eq!(relationships[0].relationship_state, "accepted");
        assert_eq!(relationships[0].last_action, "accept");

        store
            .upsert_local_data_source_binding(
                &scope_id,
                &wattswarm_storage_core::storage::LocalDataSourceBindingRow {
                    binding_kind: "checkpoint".to_owned(),
                    binding_scope: "global".to_owned(),
                    binding_key: "cp-1".to_owned(),
                    source_node_id: "peer-a".to_owned(),
                    source_uri: Some("ipfs://checkpoint-1".to_owned()),
                    updated_at: 1_700_000_000_500,
                },
            )
            .expect("save data source binding");

        let binding = store
            .get_local_data_source_binding(&scope_id, "checkpoint", "global", "cp-1")
            .expect("get data source binding")
            .expect("binding should exist");
        assert_eq!(binding.source_node_id, "peer-a");
        assert_eq!(binding.source_uri.as_deref(), Some("ipfs://checkpoint-1"));

        store
            .upsert_local_peer_dm_thread(
                &scope_id,
                &wattswarm_storage_core::storage::LocalPeerDmThreadRow {
                    remote_node_id: "peer-a".to_owned(),
                    thread_id: "dm:peer-a:peer-b".to_owned(),
                    thread_kind: "direct".to_owned(),
                    session_state: "ready".to_owned(),
                    relationship_established_at: Some(1_700_000_000_600),
                    created_at: 1_700_000_000_600,
                    updated_at: 1_700_000_000_700,
                    last_message_at: Some(1_700_000_000_700),
                },
            )
            .expect("save peer dm thread");
        store
            .upsert_local_peer_dm_message(
                &scope_id,
                &wattswarm_storage_core::storage::LocalPeerDmMessageRow {
                    thread_id: "dm:peer-a:peer-b".to_owned(),
                    message_id: "msg-1".to_owned(),
                    remote_node_id: "peer-a".to_owned(),
                    message_kind: "message".to_owned(),
                    direction: "outbound".to_owned(),
                    delivery_state: "delivered".to_owned(),
                    a2a_protocol: "google_a2a".to_owned(),
                    content_json: Some(
                        serde_json::json!({
                            "text": "hello canonical"
                        })
                        .to_string(),
                    ),
                    agent_envelope_json: Some(
                        serde_json::json!({
                            "protocol": "google_a2a",
                            "message": {"content": {"text":"hello envelope"}}
                        })
                        .to_string(),
                    ),
                    created_at: 1_700_000_000_700,
                    acknowledged_at: Some(1_700_000_000_800),
                },
            )
            .expect("save peer dm message");

        let threads = store
            .list_local_peer_dm_threads(&scope_id)
            .expect("list peer dm threads");
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].thread_id, "dm:peer-a:peer-b");
        assert_eq!(threads[0].session_state, "ready");

        let messages = store
            .list_local_peer_dm_messages(&scope_id, "dm:peer-a:peer-b")
            .expect("list peer dm messages");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_id, "msg-1");
        assert_eq!(messages[0].delivery_state, "delivered");
        assert_eq!(
            messages[0].content_json.as_deref(),
            Some("{\"text\":\"hello canonical\"}")
        );

        store
            .upsert_local_data_plane_status(
                &scope_id,
                &wattswarm_storage_core::storage::LocalDataPlaneStatusRow {
                    object_kind: "dm_message".to_owned(),
                    object_id: "msg-1".to_owned(),
                    remote_node_id: Some("peer-a".to_owned()),
                    route: "iroh_direct".to_owned(),
                    status: "content_hydrated".to_owned(),
                    detail: None,
                    updated_at: 1_700_000_000_900,
                },
            )
            .expect("save data plane status");

        let statuses = store
            .list_local_data_plane_statuses(&scope_id)
            .expect("list data plane statuses");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].object_kind, "dm_message");
        assert_eq!(statuses[0].object_id, "msg-1");
        assert_eq!(statuses[0].route, "iroh_direct");
        assert_eq!(statuses[0].status, "content_hydrated");
    });
}

#[test]
fn local_control_network_peer_sync_state_roundtrip() {
    with_test_schema(|| {
        let store = PgStore::open("network-peer-sync-state-roundtrip.state").expect("open store");
        let scope_id = local_control_scope_id(std::path::Path::new("/tmp/wattswarm-state"));
        store
            .upsert_local_network_peer_sync_state(
                &scope_id,
                &wattswarm_storage_core::storage::LocalNetworkPeerSyncStateRow {
                    network_peer_id: test_iroh_endpoint_id(42),
                    known_scopes_json: serde_json::json!([
                        {"kind": "global"},
                        {"kind": "group", "value": "crew-7"}
                    ])
                    .to_string(),
                    backfill_cursors_json: serde_json::json!([
                        {
                            "lane": {
                                "scope": {"kind": "group", "value": "crew-7"},
                                "feed_key": "crew.chat"
                            },
                            "cursor": 42
                        }
                    ])
                    .to_string(),
                    remote_heads_json: serde_json::json!([
                        {
                            "lane": {
                                "scope": {"kind": "group", "value": "crew-7"},
                                "feed_key": "crew.chat"
                            },
                            "head_event_ids": ["evt-head"]
                        }
                    ])
                    .to_string(),
                    backfill_successes: 3,
                    backfill_failures: 1,
                    last_observed_at: Some(1_699_999_999_000),
                    updated_at: 1_700_000_000_000,
                },
            )
            .expect("upsert peer sync state");

        let rows = store
            .list_local_network_peer_sync_states(&scope_id)
            .expect("list peer sync state");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].network_peer_id, test_iroh_endpoint_id(42));
        assert_eq!(rows[0].backfill_successes, 3);
        assert_eq!(rows[0].backfill_failures, 1);
        assert_eq!(rows[0].last_observed_at, Some(1_699_999_999_000));
        assert_eq!(rows[0].updated_at, 1_700_000_000_000);
        assert!(rows[0].known_scopes_json.contains("crew-7"));
        assert!(rows[0].backfill_cursors_json.contains("\"cursor\":42"));
        assert!(rows[0].remote_heads_json.contains("evt-head"));
    });
}

#[test]
fn local_control_identity_columns_use_network_peer_or_node_naming() {
    with_test_schema(|| {
        let _store = PgStore::open("identity-column-audit.state").expect("open store");
        let conn = open_test_connection();
        let rows = conn
            .prepare(
                "SELECT table_name, column_name
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name IN (
                     'peer_metadata_local',
                     'discovered_peers_local',
                     'peer_relationships_local',
                     'network_peer_sync_state_local',
                     'peer_dm_threads_local',
                     'peer_dm_messages_local'
                   )
                   AND column_name LIKE '%peer_id%'
                 ORDER BY table_name, column_name",
            )
            .expect("prepare identity column audit")
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query identity columns")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect identity columns");

        assert_eq!(
            rows,
            vec![(
                "network_peer_sync_state_local".to_owned(),
                "network_peer_id".to_owned()
            )]
        );
    });
}

#[test]
fn local_control_agent_event_and_delivery_roundtrip() {
    with_test_schema(|| {
        let store = PgStore::open("agent-event-roundtrip.state").expect("open store");
        let scope_id = local_control_scope_id(std::path::Path::new("/tmp/wattswarm-state"));

        store
            .upsert_local_agent_event(
                &scope_id,
                &wattswarm_storage_core::storage::LocalAgentEventRow {
                    event_id: "evt-1".to_owned(),
                    event_type: "friend_request".to_owned(),
                    source_kind: "peer_relationship".to_owned(),
                    source_node_id: Some("peer-a".to_owned()),
                    target_agent_id: Some("agent-b".to_owned()),
                    target_executor: Some("core-agent".to_owned()),
                    agent_envelope_json: Some(
                        serde_json::json!({
                            "protocol": "google_a2a",
                            "source_agent_id": "agent-a",
                            "target_agent_id": "agent-b",
                            "capability": "peer.relationship.request",
                            "message_json": "{\"kind\":\"friend_request\"}",
                        })
                        .to_string(),
                    ),
                    payload_json: serde_json::to_string(&serde_json::json!({
                        "remote_node_id": "peer-a"
                    }))
                    .expect("payload_json"),
                    allowed_actions_json: serde_json::to_string(&vec![
                        "accept".to_owned(),
                        "reject".to_owned(),
                    ])
                    .expect("allowed_actions_json"),
                    requires_commit: true,
                    status: "pending".to_owned(),
                    dedupe_key: Some("friend_request:peer-a".to_owned()),
                    correlation_id: Some("corr-1".to_owned()),
                    created_at: 1_700_000_000_000,
                    updated_at: 1_700_000_000_000,
                },
            )
            .expect("upsert local agent event");

        store
            .append_local_agent_event_delivery(
                &scope_id,
                &wattswarm_storage_core::storage::LocalAgentEventDeliveryRow {
                    delivery_id: "delivery-1".to_owned(),
                    event_id: "evt-1".to_owned(),
                    attempt_no: 1,
                    endpoint_url: "http://127.0.0.1:8787/agent-events".to_owned(),
                    delivery_status: "acked".to_owned(),
                    response_code: Some(200),
                    response_body: Some("{\"ok\":true}".to_owned()),
                    error_text: None,
                    next_retry_at: None,
                    created_at: 1_700_000_000_010,
                },
            )
            .expect("append local agent event delivery");

        let event = store
            .find_local_agent_event_by_dedupe_key(&scope_id, "friend_request:peer-a")
            .expect("lookup local agent event")
            .expect("local agent event exists");
        assert_eq!(event.event_id, "evt-1");
        assert_eq!(event.status, "pending");

        let conn = open_test_connection();
        let deliveries = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_event_delivery_local WHERE scope_id = $1 AND event_id = $2",
                wattswarm_storage_core::params![scope_id, "evt-1"],
                |row| row.get::<usize, i64>(0),
            )
            .expect("count deliveries");
        assert_eq!(deliveries, 1);
    });
}

#[test]
fn to_param_value_handles_option_and_unit_variants() {
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(Option::<i64>::None),
        ParamValue::Null
    ));
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(Option::<bool>::Some(true)),
        ParamValue::Bool(true)
    ));
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(()),
        ParamValue::Null
    ));
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(Option::<&str>::Some("a")),
        ParamValue::Text(v) if v == "a"
    ));
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(Option::<u64>::Some(7)),
        ParamValue::I64(7)
    ));
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(Option::<i32>::Some(9)),
        ParamValue::I64(9)
    ));
    assert!(matches!(
        wattswarm_storage_core::storage::pg::to_param_value(Option::<f64>::Some(1.5)),
        ParamValue::F64(v) if (v - 1.5).abs() < 1e-9
    ));
}

#[test]
fn query_map_empty_result_keeps_column_names_empty() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch("CREATE TABLE pg_empty_rows (x BIGINT)")
            .expect("create table");

        let mut stmt = conn
            .prepare("SELECT x FROM pg_empty_rows WHERE x > 0")
            .expect("prepare query");
        let mapped = stmt
            .query_map(wattswarm_storage_core::params![], |r| {
                r.get::<usize, i64>(0)
            })
            .expect("query map");
        let rows: Vec<i64> = mapped.collect::<Result<_, _>>().expect("collect mapped");

        assert!(rows.is_empty());
        assert!(stmt.column_names().is_empty());
    });
}

#[test]
fn db_constraint_violation_maps_to_constraint_error_code() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.execute_batch("CREATE TABLE pg_uniq_case (v TEXT UNIQUE)")
            .expect("create table");
        conn.execute(
            "INSERT INTO pg_uniq_case(v) VALUES ($1)",
            wattswarm_storage_core::params!["dup"],
        )
        .expect("insert first row");

        let err = conn
            .execute(
                "INSERT INTO pg_uniq_case(v) VALUES ($1)",
                wattswarm_storage_core::params!["dup"],
            )
            .expect_err("duplicate insert should fail");
        assert!(matches!(
            err,
            Error::DbFailure(ref failure, _) if failure.code == ErrorCode::ConstraintViolation
        ));
    });
}

#[test]
fn option_cell_decoding_covers_multiple_pg_types() {
    with_test_schema(|| {
        let conn = open_test_connection();
        conn.query_row(
            "SELECT
            NULL::TEXT AS t,
            NULL::BOOL AS b,
            NULL::DOUBLE PRECISION AS f,
            NULL::BIGINT AS i",
            wattswarm_storage_core::params![],
            |row| {
                assert_eq!(row.get::<usize, Option<String>>(0)?, None);
                assert_eq!(row.get::<usize, Option<bool>>(1)?, None);
                assert_eq!(row.get::<usize, Option<f64>>(2)?, None);
                assert_eq!(row.get::<usize, Option<u64>>(3)?, None);
                Ok(())
            },
        )
        .expect("decode option cells");
    });
}

#[test]
fn open_ignores_path_and_uses_configured_schema() {
    with_test_schema(|| {
        let dir = std::env::temp_dir();
        let path_a = dir.join(format!("ws-pg-a-{}.db", uuid::Uuid::new_v4().simple()));
        let path_b = dir.join(format!("ws-pg-b-{}.db", uuid::Uuid::new_v4().simple()));

        let conn_a = Connection::open(&path_a).expect("open connection A");
        let conn_b = Connection::open(&path_b).expect("open connection B");

        let table = format!("pg_shared_path_{}", uuid::Uuid::new_v4().simple());
        conn_a
            .execute_batch(&format!(
                "CREATE TABLE {table} (id BIGINT); INSERT INTO {table}(id) VALUES (1)"
            ))
            .expect("setup shared table in A");

        let count_b = conn_b
            .query_row(
                &format!(
                    "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name = '{table}'"
                ),
                wattswarm_storage_core::params![],
                |r| r.get::<usize, i64>(0),
            )
            .expect("count table in B");
        assert_eq!(count_b, 1);

        conn_a
            .execute_batch(&format!("DROP TABLE IF EXISTS {table}"))
            .expect("drop shared table");
    });
}
