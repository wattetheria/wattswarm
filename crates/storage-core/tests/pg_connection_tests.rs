use std::sync::{Mutex, OnceLock};

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
            wattswarm_storage_core::params![legacy_node_id, "/ip4/127.0.0.1/tcp/4001"],
        )
        .expect("insert legacy discovered peer");

        let store = PgStore::open("legacy-discovered-peers-migration.state").expect("open store");
        drop(store);

        let rows = conn
            .prepare(
                "SELECT scope_id, node_id, listen_addr, source_kind
                 FROM discovered_peers_local",
            )
            .expect("prepare migrated discovered peers query")
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, String>(1)?,
                    row.get::<usize, Option<String>>(2)?,
                    row.get::<usize, String>(3)?,
                ))
            })
            .expect("query migrated discovered peers")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect migrated discovered peers");

        assert_eq!(
            rows,
            vec![(
                scope_id,
                "peer-a".to_owned(),
                Some("/ip4/127.0.0.1/tcp/4001".to_owned()),
                "unknown".to_owned(),
            )]
        );
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
                "SELECT scope_id, executor_name, base_url
                 FROM executor_registry_local",
            )
            .expect("prepare migrated executor query")
            .query_map(wattswarm_storage_core::params![], |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, String>(1)?,
                    row.get::<usize, String>(2)?,
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
                    observed_addr: Some("/ip4/198.51.100.2/tcp/4001".to_owned()),
                    listen_addrs_json: serde_json::to_string(&vec![
                        "/ip4/203.0.113.10/tcp/4001".to_owned(),
                    ])
                    .expect("listen_addrs_json"),
                    protocols_json: serde_json::to_string(&vec!["/meshsub/1.1.0".to_owned()])
                        .expect("protocols_json"),
                    handshake_status: "identified".to_owned(),
                    last_error: None,
                    contact_material_json: Some(
                        serde_json::to_string(&serde_json::json!({
                            "peer_id": "peer-a",
                            "listen_addrs": ["/ip4/203.0.113.10/tcp/4001"]
                        }))
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
                    agent_signature: None,
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
                    content_json: serde_json::to_string(&serde_json::json!({"text":"hello"}))
                        .expect("content json"),
                    agent_envelope_json: None,
                    agent_signature: None,
                    encrypted_body: None,
                    content_encoding: None,
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
