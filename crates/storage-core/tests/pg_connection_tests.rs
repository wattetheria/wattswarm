use wattswarm_storage_core::storage::pg::ErrorCode;
use wattswarm_storage_core::storage::pg::{
    Connection, Error, OptionalExtension, ParamValue, types::ValueRef,
};

use std::sync::{Mutex, OnceLock};

fn open_test_connection() -> Connection {
    Connection::open_in_memory().expect("open in-memory pg-backed schema")
}

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

#[test]
fn connection_round_trip_covers_value_and_index_conversions() {
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
}

#[test]
fn statement_query_map_populates_column_names() {
    let conn = open_test_connection();
    conn.execute_batch("CREATE TABLE map_rows (x BIGINT, y TEXT)")
        .expect("create table");
    conn.execute(
        "INSERT INTO map_rows(x, y) VALUES (?1, ?2)",
        wattswarm_storage_core::params![1_i64, "a"],
    )
    .expect("insert row 1");
    conn.execute(
        "INSERT INTO map_rows(x, y) VALUES (?1, ?2)",
        wattswarm_storage_core::params![2_i64, "b"],
    )
    .expect("insert row 2");

    let mut stmt = conn
        .prepare("SELECT x, y FROM map_rows ORDER BY x ASC")
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
}

#[test]
fn optional_extension_maps_no_rows_to_none() {
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
}

#[test]
fn sql_translation_handles_json_extract_and_placeholders() {
    let conn = open_test_connection();

    let extracted = conn
        .query_row(
            "SELECT json_extract('{\"meta\":{\"score\":\"9\"}}', '$.meta.score')",
            wattswarm_storage_core::params![],
            |r| r.get::<usize, String>(0),
        )
        .expect("json_extract should be translated");
    assert_eq!(extracted, "9");
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
    let conn = open_test_connection();
    conn.execute_batch("CREATE TABLE empty_rows (x BIGINT)")
        .expect("create table");

    let mut stmt = conn
        .prepare("SELECT x FROM empty_rows WHERE x > 0")
        .expect("prepare query");
    let mapped = stmt
        .query_map(wattswarm_storage_core::params![], |r| {
            r.get::<usize, i64>(0)
        })
        .expect("query map");
    let rows: Vec<i64> = mapped.collect::<Result<_, _>>().expect("collect mapped");

    assert!(rows.is_empty());
    assert!(stmt.column_names().is_empty());
}

#[test]
fn db_constraint_violation_maps_to_constraint_error_code() {
    let conn = open_test_connection();
    conn.execute_batch("CREATE TABLE uniq_case (v TEXT UNIQUE)")
        .expect("create table");
    conn.execute(
        "INSERT INTO uniq_case(v) VALUES (?1)",
        wattswarm_storage_core::params!["dup"],
    )
    .expect("insert first row");

    let err = conn
        .execute(
            "INSERT INTO uniq_case(v) VALUES (?1)",
            wattswarm_storage_core::params!["dup"],
        )
        .expect_err("duplicate insert should fail");
    assert!(matches!(
        err,
        Error::DbFailure(ref failure, _) if failure.code == ErrorCode::ConstraintViolation
    ));
}

#[test]
fn option_cell_decoding_covers_multiple_pg_types() {
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
}

#[test]
fn open_with_path_isolation_uses_distinct_schemas() {
    let _env_guard = env_lock();
    let _isolate = EnvVarGuard::set("WATTSWARM_PG_ISOLATE_BY_PATH", "1");

    let dir = std::env::temp_dir();
    let path_a = dir.join(format!("ws-pg-a-{}.db", uuid::Uuid::new_v4().simple()));
    let path_b = dir.join(format!("ws-pg-b-{}.db", uuid::Uuid::new_v4().simple()));

    let conn_a = Connection::open(&path_a).expect("open connection A");
    let conn_b = Connection::open(&path_b).expect("open connection B");

    conn_a
        .execute_batch("CREATE TABLE iso_only (id BIGINT); INSERT INTO iso_only(id) VALUES (1)")
        .expect("setup isolated table in A");

    let count_b = conn_b
        .query_row(
            "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name = 'iso_only'",
            wattswarm_storage_core::params![],
            |r| r.get::<usize, i64>(0),
        )
        .expect("count table in B");
    assert_eq!(count_b, 0);
}
