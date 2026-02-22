use std::sync::{Mutex, OnceLock};

use wattswarm_storage_core::storage::pg::ErrorCode;
use wattswarm_storage_core::storage::pg::{
    Connection, Error, OptionalExtension, ParamValue, types::ValueRef,
};

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
