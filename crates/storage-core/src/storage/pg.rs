use super::sqlite_layout::sqlite_database_path;
use postgres::types::{ToSql, Type as PgType};
use postgres::{Client, NoTls, Row as PgRow};
use rusqlite::types::ValueRef as SqliteValueRef;
use rusqlite::{Connection as SqliteConnection, OpenFlags};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub mod types {
    #[derive(Debug, Clone, Copy)]
    pub enum Type {
        Text,
    }

    #[derive(Debug, Clone)]
    pub enum ValueRef {
        Null,
        Integer(i64),
        Real(f64),
        Text(String),
        Blob(Vec<u8>),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    ConstraintViolation,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct DbFailureError {
    pub code: ErrorCode,
}

#[derive(Debug)]
pub enum Error {
    QueryReturnedNoRows,
    DbFailure(DbFailureError, Option<String>),
    FromSqlConversionFailure(usize, types::Type, Box<dyn StdError + Send + Sync>),
    Db(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::QueryReturnedNoRows => write!(f, "query returned no rows"),
            Error::DbFailure(_, msg) => {
                write!(f, "sql failure: {}", msg.as_deref().unwrap_or("unknown"))
            }
            Error::FromSqlConversionFailure(idx, _, err) => {
                write!(f, "from-sql conversion failed at column {}: {}", idx, err)
            }
            Error::Db(msg) => write!(f, "db error: {msg}"),
        }
    }
}

impl StdError for Error {}

pub type Result<T> = std::result::Result<T, Error>;

fn map_db_err(err: postgres::Error) -> Error {
    if let Some(db_err) = err.as_db_error() {
        let code = if db_err.code().code().starts_with("23") {
            ErrorCode::ConstraintViolation
        } else {
            ErrorCode::Unknown
        };
        return Error::DbFailure(DbFailureError { code }, Some(db_err.message().to_owned()));
    }
    Error::Db(err.to_string())
}

fn map_sqlite_err(err: rusqlite::Error) -> Error {
    match err {
        rusqlite::Error::QueryReturnedNoRows => Error::QueryReturnedNoRows,
        rusqlite::Error::SqliteFailure(inner, message) => {
            let code = if inner.code == rusqlite::ErrorCode::ConstraintViolation {
                ErrorCode::ConstraintViolation
            } else {
                ErrorCode::Unknown
            };
            Error::DbFailure(DbFailureError { code }, message)
        }
        rusqlite::Error::FromSqlConversionFailure(index, kind, source) => {
            let kind = match kind {
                rusqlite::types::Type::Text => types::Type::Text,
                _ => types::Type::Text,
            };
            Error::FromSqlConversionFailure(index, kind, source)
        }
        other => Error::Db(other.to_string()),
    }
}

fn default_pg_url() -> String {
    std::env::var("WATTSWARM_CORE_PG_URL")
        .ok()
        .or_else(|| std::env::var("WATTSWARM_PG_URL").ok())
        .unwrap_or_else(|| "postgres://postgres:postgres@127.0.0.1:55432/wattswarm".to_owned())
}

const PRIMARY_SCHEMA: &str = "public";
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_CONFIG_RETRY_DELAY: Duration = Duration::from_millis(50);
const SQLITE_CONFIG_RETRY_ATTEMPTS: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Postgres,
    Sqlite,
}

fn sanitize_ident(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().max(8));
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("ws");
    }
    if !out
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        out.insert(0, '_');
    }
    out
}

fn configured_schema_name() -> String {
    std::env::var("WATTSWARM_PG_SCHEMA")
        .ok()
        .map(|v| sanitize_ident(v.trim()))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| PRIMARY_SCHEMA.to_owned())
}

pub fn configured_backend_kind() -> Result<BackendKind> {
    match std::env::var("WATTSWARM_STORAGE_BACKEND")
        .unwrap_or_else(|_| "postgres".to_owned())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "postgres" | "postgresql" => Ok(BackendKind::Postgres),
        "sqlite" => Ok(BackendKind::Sqlite),
        other => Err(Error::Db(format!(
            "unsupported WATTSWARM_STORAGE_BACKEND '{other}'; expected postgres or sqlite"
        ))),
    }
}

#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
}

struct Inner {
    backend: Backend,
}

enum Backend {
    Postgres(Box<PostgresBackend>),
    Sqlite(SqliteBackend),
}

struct PostgresBackend {
    client: Mutex<Client>,
    schema: String,
    database_url: String,
    cleanup_on_drop: bool,
}

struct SqliteBackend {
    client: Mutex<SqliteConnection>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let Backend::Postgres(backend) = &self.backend else {
            return;
        };
        if !backend.cleanup_on_drop || backend.schema == PRIMARY_SCHEMA {
            return;
        }
        let mut client = match Client::connect(&backend.database_url, NoTls) {
            Ok(c) => c,
            Err(_) => return,
        };
        let ddl = format!("DROP SCHEMA IF EXISTS {} CASCADE;", backend.schema);
        let _ = client.batch_execute(&ddl);
    }
}

impl Connection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        match configured_backend_kind()? {
            BackendKind::Postgres => Self::open_postgres(),
            BackendKind::Sqlite => {
                let requested_path = path.as_ref();
                let state_dir = requested_path.parent().unwrap_or_else(|| Path::new("."));
                Self::open_sqlite(sqlite_database_path(state_dir))
            }
        }
    }

    pub fn open_postgres() -> Result<Self> {
        let schema = configured_schema_name();
        let create_schema = schema != PRIMARY_SCHEMA;
        Self::connect_for_schema(schema, create_schema, false)
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self> {
        let client = SqliteConnection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(map_sqlite_err)?;
        Self::from_sqlite_client(client)
    }

    pub fn open_in_memory() -> Result<Self> {
        match configured_backend_kind()? {
            BackendKind::Postgres => {
                let schema = sanitize_ident(&format!("ws_test_{}", uuid::Uuid::new_v4().simple()));
                // Test-only "in-memory" mode is schema-backed in PostgreSQL.
                // Auto-clean the schema on drop to avoid long-term schema clutter.
                Self::connect_for_schema(schema, true, true)
            }
            BackendKind::Sqlite => Self::open_in_memory_sqlite(),
        }
    }

    pub fn open_in_memory_sqlite() -> Result<Self> {
        let client = SqliteConnection::open_in_memory().map_err(map_sqlite_err)?;
        Self::from_sqlite_client(client)
    }

    fn from_sqlite_client(client: SqliteConnection) -> Result<Self> {
        configure_sqlite_client(&client)?;
        Ok(Self {
            inner: Arc::new(Inner {
                backend: Backend::Sqlite(SqliteBackend {
                    client: Mutex::new(client),
                }),
            }),
        })
    }

    fn connect_for_schema(
        schema: String,
        create_schema: bool,
        cleanup_on_drop: bool,
    ) -> Result<Self> {
        let url = default_pg_url();
        let mut client = Client::connect(&url, NoTls).map_err(map_db_err)?;
        if create_schema {
            let ddl = format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema}, {PRIMARY_SCHEMA};"
            );
            client.batch_execute(&ddl).map_err(map_db_err)?;
        } else {
            let ddl = format!("SET search_path TO {PRIMARY_SCHEMA};");
            client.batch_execute(&ddl).map_err(map_db_err)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                backend: Backend::Postgres(Box::new(PostgresBackend {
                    client: Mutex::new(client),
                    schema,
                    database_url: url,
                    cleanup_on_drop,
                })),
            }),
        })
    }

    pub fn backend_kind(&self) -> BackendKind {
        match &self.inner.backend {
            Backend::Postgres(_) => BackendKind::Postgres,
            Backend::Sqlite(_) => BackendKind::Sqlite,
        }
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        match &self.inner.backend {
            Backend::Postgres(backend) => {
                let mut client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                client.batch_execute(sql).map_err(map_db_err)?;
                Ok(())
            }
            Backend::Sqlite(backend) => {
                let client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                execute_sqlite_batch(&client, sql)
            }
        }
    }

    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        let values = params.to_values();
        match &self.inner.backend {
            Backend::Postgres(backend) => {
                let mut client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                let refs = build_param_refs(&values);
                Ok(client.execute(sql, &refs).map_err(map_db_err)? as usize)
            }
            Backend::Sqlite(backend) => {
                let client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                let translated = translate_sqlite_sql(sql);
                match sqlite_statement_action(&translated)? {
                    SqliteStatementAction::Execute => {}
                    SqliteStatementAction::AdvisoryLockNoOp => return Ok(0),
                }
                client
                    .execute(&translated, rusqlite::params_from_iter(values.iter()))
                    .map_err(map_sqlite_err)
            }
        }
    }

    pub fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row) -> Result<T>,
    {
        let values = params.to_values();
        match &self.inner.backend {
            Backend::Postgres(backend) => {
                let mut client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                let refs = build_param_refs(&values);
                let maybe = client.query_opt(sql, &refs).map_err(map_db_err)?;
                let row = maybe.ok_or(Error::QueryReturnedNoRows)?;
                f(&Row::new_postgres(row))
            }
            Backend::Sqlite(backend) => {
                let client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                let translated = translate_sqlite_sql(sql);
                let mut statement = client.prepare(&translated).map_err(map_sqlite_err)?;
                let mut rows = statement
                    .query(rusqlite::params_from_iter(values.iter()))
                    .map_err(map_sqlite_err)?;
                let row = rows
                    .next()
                    .map_err(map_sqlite_err)?
                    .ok_or(Error::QueryReturnedNoRows)?;
                let owned = Row::new_sqlite(row)?;
                f(&owned)
            }
        }
    }

    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        Ok(Statement {
            conn: self.clone(),
            sql: sql.to_owned(),
            column_names: Vec::new(),
        })
    }
}

pub struct Statement {
    conn: Connection,
    sql: String,
    column_names: Vec<String>,
}

impl Statement {
    pub fn column_names(&self) -> Vec<&str> {
        self.column_names.iter().map(String::as_str).collect()
    }

    pub fn query_map<P, F, T>(&mut self, params: P, mut f: F) -> Result<MappedRows<T>>
    where
        P: Params,
        F: FnMut(&Row) -> Result<T>,
    {
        let values = params.to_values();
        let mut mapped = Vec::new();
        match &self.conn.inner.backend {
            Backend::Postgres(backend) => {
                let mut client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                let refs = build_param_refs(&values);
                let rows = client.query(&self.sql, &refs).map_err(map_db_err)?;
                if let Some(row) = rows.first() {
                    self.column_names = row.columns().iter().map(|c| c.name().to_owned()).collect();
                }
                mapped.reserve(rows.len());
                for row in rows {
                    mapped.push(f(&Row::new_postgres(row)));
                }
            }
            Backend::Sqlite(backend) => {
                let client = backend
                    .client
                    .lock()
                    .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
                let translated = translate_sqlite_sql(&self.sql);
                let mut statement = client.prepare(&translated).map_err(map_sqlite_err)?;
                let column_names: Vec<String> = statement
                    .column_names()
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect();
                let mut rows = statement
                    .query(rusqlite::params_from_iter(values.iter()))
                    .map_err(map_sqlite_err)?;
                while let Some(row) = rows.next().map_err(map_sqlite_err)? {
                    if mapped.is_empty() {
                        self.column_names.clone_from(&column_names);
                    }
                    mapped.push(f(&Row::new_sqlite(row)?));
                }
            }
        }
        Ok(MappedRows {
            inner: mapped.into_iter(),
        })
    }
}

pub struct MappedRows<T> {
    inner: std::vec::IntoIter<Result<T>>,
}

impl<T> Iterator for MappedRows<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct Row {
    inner: RowInner,
}

enum RowInner {
    Postgres(PgRow),
    Sqlite {
        column_names: Vec<String>,
        values: Vec<types::ValueRef>,
    },
}

impl Row {
    fn new_postgres(row: PgRow) -> Self {
        Self {
            inner: RowInner::Postgres(row),
        }
    }

    fn new_sqlite(row: &rusqlite::Row<'_>) -> Result<Self> {
        let column_count = row.as_ref().column_count();
        let column_names = (0..column_count)
            .map(|index| {
                row.as_ref()
                    .column_name(index)
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect();
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            let value = match row.get_ref(index).map_err(map_sqlite_err)? {
                SqliteValueRef::Null => types::ValueRef::Null,
                SqliteValueRef::Integer(value) => types::ValueRef::Integer(value),
                SqliteValueRef::Real(value) => types::ValueRef::Real(value),
                SqliteValueRef::Text(value) => {
                    types::ValueRef::Text(String::from_utf8_lossy(value).into_owned())
                }
                SqliteValueRef::Blob(value) => types::ValueRef::Blob(value.to_vec()),
            };
            values.push(value);
        }
        Ok(Self {
            inner: RowInner::Sqlite {
                column_names,
                values,
            },
        })
    }

    pub fn get<I, T>(&self, idx: I) -> Result<T>
    where
        I: IntoRowIndex,
        T: FromCell,
    {
        T::from_cell(self, idx.to_index())
    }

    pub fn get_ref(&self, idx: usize) -> Result<types::ValueRef> {
        let RowInner::Postgres(row) = &self.inner else {
            let RowInner::Sqlite { values, .. } = &self.inner else {
                unreachable!()
            };
            return values
                .get(idx)
                .cloned()
                .ok_or_else(|| Error::Db(format!("column index out of range: {idx}")));
        };
        let t = row.columns()[idx].type_();
        if *t == PgType::INT2 {
            let v = row
                .try_get::<usize, Option<i16>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Integer(inner as i64))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::INT4 {
            let v = row
                .try_get::<usize, Option<i32>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Integer(inner as i64))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::INT8 {
            let v = row
                .try_get::<usize, Option<i64>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Integer)
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::FLOAT4 {
            let v = row
                .try_get::<usize, Option<f32>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Real(inner as f64))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::FLOAT8 || *t == PgType::NUMERIC {
            let v = row
                .try_get::<usize, Option<f64>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Real)
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::BOOL {
            let v = row
                .try_get::<usize, Option<bool>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Integer(i64::from(inner)))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::BYTEA {
            let v = row
                .try_get::<usize, Option<Vec<u8>>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Blob)
                .unwrap_or(types::ValueRef::Null));
        }
        if is_textual_type(t) {
            let v = row
                .try_get::<usize, Option<String>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Text)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = row.try_get::<usize, Option<String>>(idx) {
            return Ok(v
                .map(types::ValueRef::Text)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = row.try_get::<usize, Option<i64>>(idx) {
            return Ok(v
                .map(types::ValueRef::Integer)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = row.try_get::<usize, Option<f64>>(idx) {
            return Ok(v
                .map(types::ValueRef::Real)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = row.try_get::<usize, Option<bool>>(idx) {
            return Ok(v
                .map(|inner| types::ValueRef::Integer(i64::from(inner)))
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = row.try_get::<usize, Option<Vec<u8>>>(idx) {
            return Ok(v
                .map(types::ValueRef::Blob)
                .unwrap_or(types::ValueRef::Null));
        }
        Err(Error::Db(format!(
            "unsupported column type for get_ref: {}",
            t.name()
        )))
    }

    pub fn column_count(&self) -> usize {
        match &self.inner {
            RowInner::Postgres(row) => row.columns().len(),
            RowInner::Sqlite { values, .. } => values.len(),
        }
    }

    pub fn column_name(&self, idx: usize) -> &str {
        match &self.inner {
            RowInner::Postgres(row) => row.columns()[idx].name(),
            RowInner::Sqlite { column_names, .. } => &column_names[idx],
        }
    }
}

pub trait IntoRowIndex {
    fn to_index(self) -> usize;
}

impl IntoRowIndex for usize {
    fn to_index(self) -> usize {
        self
    }
}

impl IntoRowIndex for i32 {
    fn to_index(self) -> usize {
        self as usize
    }
}

impl IntoRowIndex for i64 {
    fn to_index(self) -> usize {
        self as usize
    }
}

fn is_textual_type(pg_type: &PgType) -> bool {
    *pg_type == PgType::TEXT
        || *pg_type == PgType::VARCHAR
        || *pg_type == PgType::BPCHAR
        || *pg_type == PgType::NAME
        || *pg_type == PgType::JSON
        || *pg_type == PgType::JSONB
}

fn replace_ascii_case_insensitive(input: &str, needle: &str, replacement: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let needle = needle.to_ascii_lowercase();
    let mut output = String::with_capacity(input.len());
    let mut offset = 0;
    while let Some(relative) = lower[offset..].find(&needle) {
        let start = offset + relative;
        output.push_str(&input[offset..start]);
        output.push_str(replacement);
        offset = start + needle.len();
    }
    output.push_str(&input[offset..]);
    output
}

fn translate_extract_epoch(input: &str) -> String {
    const MARKER: &str = "extract(epoch from ";
    let mut output = input.to_owned();
    loop {
        let lower = output.to_ascii_lowercase();
        let Some(start) = lower.find(MARKER) else {
            break;
        };
        let content_start = start + MARKER.len();
        let mut depth = 1_i32;
        let mut closing = None;
        for (relative, byte) in output.as_bytes()[content_start..].iter().enumerate() {
            match byte {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        closing = Some(content_start + relative);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(closing) = closing else {
            break;
        };
        let inner = output[content_start..closing].trim();
        output.replace_range(start..=closing, &format!("(({inner}) / 1000.0)"));
    }
    output
}

fn translate_postgres_params(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut in_string = false;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            output.push('\'');
            if in_string && bytes.get(index + 1) == Some(&b'\'') {
                output.push('\'');
                index += 2;
                continue;
            }
            in_string = !in_string;
            index += 1;
            continue;
        }
        if !in_string
            && bytes[index] == b'$'
            && bytes.get(index + 1).is_some_and(u8::is_ascii_digit)
        {
            output.push('?');
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                output.push(bytes[index] as char);
                index += 1;
            }
            continue;
        }
        output.push(bytes[index] as char);
        index += 1;
    }
    output
}

fn translate_sqlite_sql(sql: &str) -> String {
    if sql.trim().eq_ignore_ascii_case("BEGIN") {
        return "BEGIN IMMEDIATE".to_owned();
    }
    let mut translated = translate_extract_epoch(sql);
    for (postgres, sqlite) in [
        ("TIMESTAMPTZ 'epoch'", "0"),
        ("INTERVAL '1 millisecond'", "1"),
        (
            "NOW()",
            "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
        ),
        ("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("BIGSERIAL", "INTEGER"),
        ("TIMESTAMPTZ", "INTEGER"),
        ("DOUBLE PRECISION", "REAL"),
        ("GREATEST(", "MAX("),
        ("FOR UPDATE SKIP LOCKED", ""),
        ("FOR UPDATE", ""),
        ("DROP COLUMN IF EXISTS", "DROP COLUMN"),
        ("::double precision", ""),
        ("::bigint", ""),
        ("::integer", ""),
        ("::text", ""),
        ("::jsonb", ""),
        ("::boolean", ""),
    ] {
        translated = replace_ascii_case_insensitive(&translated, postgres, sqlite);
    }
    translate_postgres_params(&translated)
}

fn execute_sqlite_batch(client: &SqliteConnection, sql: &str) -> Result<()> {
    let translated = translate_sqlite_sql(sql);
    for statement in translated.split(';').map(str::trim) {
        if statement.is_empty() {
            continue;
        }
        match sqlite_statement_action(statement)? {
            SqliteStatementAction::Execute => {}
            SqliteStatementAction::AdvisoryLockNoOp => continue,
        }
        client
            .execute_batch(&format!("{statement};"))
            .map_err(map_sqlite_err)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteStatementAction {
    Execute,
    AdvisoryLockNoOp,
}

fn sqlite_statement_action(statement: &str) -> Result<SqliteStatementAction> {
    let normalized = statement
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if normalized.starts_with("select pg_advisory_") {
        return Ok(SqliteStatementAction::AdvisoryLockNoOp);
    }
    if normalized.starts_with("alter table ")
        && (normalized.contains(" drop constraint ")
            || normalized.contains(" add constraint ")
            || normalized.contains(" alter column "))
    {
        return Err(Error::Db(format!(
            "unsupported SQLite schema migration statement; add an explicit SQLite migration: \
             {statement}"
        )));
    }
    Ok(SqliteStatementAction::Execute)
}

pub trait FromCell: Sized {
    fn from_cell(row: &Row, idx: usize) -> Result<Self>;
}

impl FromCell for String {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        match &row.inner {
            RowInner::Postgres(row) => row
                .try_get::<usize, String>(idx)
                .map_err(|e| Error::Db(e.to_string())),
            RowInner::Sqlite { values, .. } => match values.get(idx) {
                Some(types::ValueRef::Text(value)) => Ok(value.clone()),
                Some(other) => Err(Error::Db(format!(
                    "error deserializing SQLite column as text: {other:?}"
                ))),
                None => Err(Error::Db(format!("column index out of range: {idx}"))),
            },
        }
    }
}

impl FromCell for i64 {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        match &row.inner {
            RowInner::Postgres(row) => {
                if let Ok(v) = row.try_get::<usize, i64>(idx) {
                    return Ok(v);
                }
                if let Ok(v) = row.try_get::<usize, i32>(idx) {
                    return Ok(v as i64);
                }
                if let Ok(v) = row.try_get::<usize, bool>(idx) {
                    return Ok(i64::from(v));
                }
                Err(Error::Db("error deserializing column as i64".to_owned()))
            }
            RowInner::Sqlite { values, .. } => match values.get(idx) {
                Some(types::ValueRef::Integer(value)) => Ok(*value),
                Some(types::ValueRef::Real(value)) => Ok(*value as i64),
                Some(other) => Err(Error::Db(format!(
                    "error deserializing SQLite column as i64: {other:?}"
                ))),
                None => Err(Error::Db(format!("column index out of range: {idx}"))),
            },
        }
    }
}

impl FromCell for i32 {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        match &row.inner {
            RowInner::Postgres(row) => row
                .try_get::<usize, i32>(idx)
                .map_err(|e| Error::Db(e.to_string())),
            RowInner::Sqlite { .. } => i64::from_cell(row, idx).and_then(|value| {
                i32::try_from(value)
                    .map_err(|_| Error::Db(format!("SQLite integer out of i32 range: {value}")))
            }),
        }
    }
}

impl FromCell for u64 {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        let v = i64::from_cell(row, idx)?;
        Ok(v.max(0) as u64)
    }
}

impl FromCell for u32 {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        let v = i64::from_cell(row, idx)?;
        Ok(v.max(0) as u32)
    }
}

impl FromCell for f64 {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        match &row.inner {
            RowInner::Postgres(row) => row
                .try_get::<usize, f64>(idx)
                .map_err(|e| Error::Db(e.to_string())),
            RowInner::Sqlite { values, .. } => match values.get(idx) {
                Some(types::ValueRef::Real(value)) => Ok(*value),
                Some(types::ValueRef::Integer(value)) => Ok(*value as f64),
                Some(other) => Err(Error::Db(format!(
                    "error deserializing SQLite column as f64: {other:?}"
                ))),
                None => Err(Error::Db(format!("column index out of range: {idx}"))),
            },
        }
    }
}

impl FromCell for bool {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        match &row.inner {
            RowInner::Postgres(row) => {
                if let Ok(v) = row.try_get::<usize, bool>(idx) {
                    return Ok(v);
                }
                let v = row
                    .try_get::<usize, i64>(idx)
                    .map_err(|e| Error::Db(e.to_string()))?;
                Ok(v != 0)
            }
            RowInner::Sqlite { values, .. } => match values.get(idx) {
                Some(types::ValueRef::Integer(value)) => Ok(*value != 0),
                Some(types::ValueRef::Real(value)) => Ok(*value != 0.0),
                Some(other) => Err(Error::Db(format!(
                    "error deserializing SQLite column as bool: {other:?}"
                ))),
                None => Err(Error::Db(format!("column index out of range: {idx}"))),
            },
        }
    }
}

fn postgres_cell_is_null(row: &PgRow, idx: usize) -> Result<bool> {
    let pg_type = row.columns()[idx].type_();
    if *pg_type == PgType::INT2 {
        return row
            .try_get::<usize, Option<i16>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if *pg_type == PgType::INT4 {
        return row
            .try_get::<usize, Option<i32>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if *pg_type == PgType::INT8 {
        return row
            .try_get::<usize, Option<i64>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if *pg_type == PgType::FLOAT4 {
        return row
            .try_get::<usize, Option<f32>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if *pg_type == PgType::FLOAT8 || *pg_type == PgType::NUMERIC {
        return row
            .try_get::<usize, Option<f64>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if *pg_type == PgType::BOOL {
        return row
            .try_get::<usize, Option<bool>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if *pg_type == PgType::BYTEA {
        return row
            .try_get::<usize, Option<Vec<u8>>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }
    if is_textual_type(pg_type) {
        return row
            .try_get::<usize, Option<String>>(idx)
            .map(|v| v.is_none())
            .map_err(|e| Error::Db(e.to_string()));
    }

    if let Ok(v) = row.try_get::<usize, Option<String>>(idx) {
        return Ok(v.is_none());
    }
    if let Ok(v) = row.try_get::<usize, Option<i64>>(idx) {
        return Ok(v.is_none());
    }
    if let Ok(v) = row.try_get::<usize, Option<f64>>(idx) {
        return Ok(v.is_none());
    }
    if let Ok(v) = row.try_get::<usize, Option<bool>>(idx) {
        return Ok(v.is_none());
    }
    if let Ok(v) = row.try_get::<usize, Option<Vec<u8>>>(idx) {
        return Ok(v.is_none());
    }

    Err(Error::Db(format!(
        "unsupported column type for Option<T>: {}",
        pg_type.name()
    )))
}

impl<T: FromCell> FromCell for Option<T> {
    fn from_cell(row: &Row, idx: usize) -> Result<Self> {
        let is_null = match &row.inner {
            RowInner::Postgres(row) => postgres_cell_is_null(row, idx)?,
            RowInner::Sqlite { values, .. } => {
                matches!(values.get(idx), Some(types::ValueRef::Null))
            }
        };
        if is_null {
            return Ok(None);
        }
        Ok(Some(T::from_cell(row, idx)?))
    }
}

#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    I64(i64),
    F64(f64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

impl rusqlite::ToSql for ParamValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let value = match self {
            ParamValue::Null => rusqlite::types::Value::Null,
            ParamValue::I64(value) => rusqlite::types::Value::Integer(*value),
            ParamValue::F64(value) => rusqlite::types::Value::Real(*value),
            ParamValue::Bool(value) => rusqlite::types::Value::Integer(i64::from(*value)),
            ParamValue::Text(value) => rusqlite::types::Value::Text(value.clone()),
            ParamValue::Bytes(value) => rusqlite::types::Value::Blob(value.clone()),
        };
        Ok(rusqlite::types::ToSqlOutput::Owned(value))
    }
}

pub trait IntoParamValue {
    fn into_param_value(self) -> ParamValue;
}

pub fn to_param_value<T: IntoParamValue>(v: T) -> ParamValue {
    v.into_param_value()
}

impl IntoParamValue for ParamValue {
    fn into_param_value(self) -> ParamValue {
        self
    }
}

impl IntoParamValue for i64 {
    fn into_param_value(self) -> ParamValue {
        ParamValue::I64(self)
    }
}

impl IntoParamValue for i32 {
    fn into_param_value(self) -> ParamValue {
        ParamValue::I64(self as i64)
    }
}

impl IntoParamValue for u64 {
    fn into_param_value(self) -> ParamValue {
        ParamValue::I64(self as i64)
    }
}

impl IntoParamValue for u32 {
    fn into_param_value(self) -> ParamValue {
        ParamValue::I64(self as i64)
    }
}

impl IntoParamValue for usize {
    fn into_param_value(self) -> ParamValue {
        ParamValue::I64(self as i64)
    }
}

impl IntoParamValue for f64 {
    fn into_param_value(self) -> ParamValue {
        ParamValue::F64(self)
    }
}

impl IntoParamValue for bool {
    fn into_param_value(self) -> ParamValue {
        ParamValue::Bool(self)
    }
}

impl IntoParamValue for String {
    fn into_param_value(self) -> ParamValue {
        ParamValue::Text(self)
    }
}

impl IntoParamValue for &str {
    fn into_param_value(self) -> ParamValue {
        ParamValue::Text(self.to_owned())
    }
}

impl IntoParamValue for Vec<u8> {
    fn into_param_value(self) -> ParamValue {
        ParamValue::Bytes(self)
    }
}

impl<T> IntoParamValue for &T
where
    T: IntoParamValue + Clone,
{
    fn into_param_value(self) -> ParamValue {
        self.clone().into_param_value()
    }
}

impl IntoParamValue for () {
    fn into_param_value(self) -> ParamValue {
        ParamValue::Null
    }
}

impl IntoParamValue for Option<String> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::Text(v),
            None => ParamValue::Null,
        }
    }
}

impl IntoParamValue for Option<&str> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::Text(v.to_owned()),
            None => ParamValue::Null,
        }
    }
}

impl IntoParamValue for Option<i64> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::I64(v),
            None => ParamValue::Null,
        }
    }
}

impl IntoParamValue for Option<u64> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::I64(v as i64),
            None => ParamValue::Null,
        }
    }
}

impl IntoParamValue for Option<i32> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::I64(v as i64),
            None => ParamValue::Null,
        }
    }
}

impl IntoParamValue for Option<f64> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::F64(v),
            None => ParamValue::Null,
        }
    }
}

impl IntoParamValue for Option<bool> {
    fn into_param_value(self) -> ParamValue {
        match self {
            Some(v) => ParamValue::Bool(v),
            None => ParamValue::Null,
        }
    }
}

pub trait Params {
    fn to_values(self) -> Vec<ParamValue>;
}

impl Params for Vec<ParamValue> {
    fn to_values(self) -> Vec<ParamValue> {
        self
    }
}

impl Params for [(); 0] {
    fn to_values(self) -> Vec<ParamValue> {
        Vec::new()
    }
}

fn build_param_refs(values: &[ParamValue]) -> Vec<&(dyn ToSql + Sync)> {
    values
        .iter()
        .map(|v| match v {
            ParamValue::Null => &None::<String> as &(dyn ToSql + Sync),
            ParamValue::I64(v) => v as &(dyn ToSql + Sync),
            ParamValue::F64(v) => v as &(dyn ToSql + Sync),
            ParamValue::Bool(v) => v as &(dyn ToSql + Sync),
            ParamValue::Text(v) => v as &(dyn ToSql + Sync),
            ParamValue::Bytes(v) => v as &(dyn ToSql + Sync),
        })
        .collect()
}

pub trait DbParam: Sync {
    fn as_postgres(&self) -> &(dyn ToSql + Sync);
    fn sqlite_value(&self) -> rusqlite::types::Value;
}

macro_rules! impl_db_param {
    ($type:ty, $sqlite:expr) => {
        impl DbParam for $type {
            fn as_postgres(&self) -> &(dyn ToSql + Sync) {
                self
            }

            fn sqlite_value(&self) -> rusqlite::types::Value {
                $sqlite(self)
            }
        }
    };
}

impl_db_param!(i64, |value: &i64| rusqlite::types::Value::Integer(*value));
impl_db_param!(i32, |value: &i32| rusqlite::types::Value::Integer(
    i64::from(*value)
));
impl_db_param!(f64, |value: &f64| rusqlite::types::Value::Real(*value));
impl_db_param!(bool, |value: &bool| rusqlite::types::Value::Integer(
    i64::from(*value)
));
impl_db_param!(String, |value: &String| rusqlite::types::Value::Text(
    value.clone()
));
impl_db_param!(&str, |value: &&str| rusqlite::types::Value::Text(
    (*value).to_owned()
));
impl_db_param!(Vec<u8>, |value: &Vec<u8>| rusqlite::types::Value::Blob(
    value.clone()
));

impl<T> DbParam for Option<T>
where
    T: DbParam + ToSql + Sync,
    Option<T>: ToSql + Sync,
{
    fn as_postgres(&self) -> &(dyn ToSql + Sync) {
        self
    }

    fn sqlite_value(&self) -> rusqlite::types::Value {
        self.as_ref()
            .map(DbParam::sqlite_value)
            .unwrap_or(rusqlite::types::Value::Null)
    }
}

pub enum DatabaseClient {
    Postgres(Box<Client>),
    Sqlite(SqliteConnection),
}

impl DatabaseClient {
    pub fn connect_postgres(database_url: &str) -> Result<Self> {
        Client::connect(database_url, NoTls)
            .map(Box::new)
            .map(Self::Postgres)
            .map_err(map_db_err)
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self> {
        let client = SqliteConnection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(map_sqlite_err)?;
        configure_sqlite_client(&client)?;
        Ok(Self::Sqlite(client))
    }

    pub fn backend_kind(&self) -> BackendKind {
        match self {
            Self::Postgres(_) => BackendKind::Postgres,
            Self::Sqlite(_) => BackendKind::Sqlite,
        }
    }

    pub fn batch_execute(&mut self, sql: &str) -> Result<()> {
        match self {
            Self::Postgres(client) => client.batch_execute(sql).map_err(map_db_err),
            Self::Sqlite(client) => execute_sqlite_batch(client, sql),
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[&(dyn DbParam + Sync)]) -> Result<u64> {
        match self {
            Self::Postgres(client) => {
                let refs = postgres_db_param_refs(params);
                client.execute(sql, &refs).map_err(map_db_err)
            }
            Self::Sqlite(client) => {
                let translated = translate_sqlite_sql(sql);
                match sqlite_statement_action(&translated)? {
                    SqliteStatementAction::Execute => {}
                    SqliteStatementAction::AdvisoryLockNoOp => return Ok(0),
                }
                let values = sqlite_db_param_values(params);
                client
                    .execute(&translated, rusqlite::params_from_iter(values.iter()))
                    .map(|count| count as u64)
                    .map_err(map_sqlite_err)
            }
        }
    }

    pub fn query(
        &mut self,
        sql: &str,
        params: &[&(dyn DbParam + Sync)],
    ) -> Result<Vec<DatabaseRow>> {
        match self {
            Self::Postgres(client) => {
                let refs = postgres_db_param_refs(params);
                client
                    .query(sql, &refs)
                    .map(|rows| {
                        rows.into_iter()
                            .map(Row::new_postgres)
                            .map(DatabaseRow::new)
                            .collect()
                    })
                    .map_err(map_db_err)
            }
            Self::Sqlite(client) => query_sqlite_rows(client, sql, params),
        }
    }

    pub fn query_opt(
        &mut self,
        sql: &str,
        params: &[&(dyn DbParam + Sync)],
    ) -> Result<Option<DatabaseRow>> {
        let mut rows = self.query(sql, params)?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            count => Err(Error::Db(format!(
                "query returned {count} rows where at most one was expected"
            ))),
        }
    }

    pub fn query_one(
        &mut self,
        sql: &str,
        params: &[&(dyn DbParam + Sync)],
    ) -> Result<DatabaseRow> {
        self.query_opt(sql, params)?
            .ok_or(Error::QueryReturnedNoRows)
    }

    pub fn transaction(&mut self) -> Result<DatabaseTransaction<'_>> {
        match self {
            Self::Postgres(client) => client
                .transaction()
                .map(DatabaseTransaction::Postgres)
                .map_err(map_db_err),
            Self::Sqlite(client) => client
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map(DatabaseTransaction::Sqlite)
                .map_err(map_sqlite_err),
        }
    }
}

pub enum DatabaseTransaction<'a> {
    Postgres(postgres::Transaction<'a>),
    Sqlite(rusqlite::Transaction<'a>),
}

impl DatabaseTransaction<'_> {
    pub fn batch_execute(&mut self, sql: &str) -> Result<()> {
        match self {
            Self::Postgres(transaction) => transaction.batch_execute(sql).map_err(map_db_err),
            Self::Sqlite(transaction) => execute_sqlite_batch(transaction, sql),
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[&(dyn DbParam + Sync)]) -> Result<u64> {
        match self {
            Self::Postgres(transaction) => {
                let refs = postgres_db_param_refs(params);
                transaction.execute(sql, &refs).map_err(map_db_err)
            }
            Self::Sqlite(transaction) => {
                let translated = translate_sqlite_sql(sql);
                match sqlite_statement_action(&translated)? {
                    SqliteStatementAction::Execute => {}
                    SqliteStatementAction::AdvisoryLockNoOp => return Ok(0),
                }
                let values = sqlite_db_param_values(params);
                transaction
                    .execute(&translated, rusqlite::params_from_iter(values.iter()))
                    .map(|count| count as u64)
                    .map_err(map_sqlite_err)
            }
        }
    }

    pub fn query(
        &mut self,
        sql: &str,
        params: &[&(dyn DbParam + Sync)],
    ) -> Result<Vec<DatabaseRow>> {
        match self {
            Self::Postgres(transaction) => {
                let refs = postgres_db_param_refs(params);
                transaction
                    .query(sql, &refs)
                    .map(|rows| {
                        rows.into_iter()
                            .map(Row::new_postgres)
                            .map(DatabaseRow::new)
                            .collect()
                    })
                    .map_err(map_db_err)
            }
            Self::Sqlite(transaction) => query_sqlite_rows(transaction, sql, params),
        }
    }

    pub fn query_opt(
        &mut self,
        sql: &str,
        params: &[&(dyn DbParam + Sync)],
    ) -> Result<Option<DatabaseRow>> {
        let mut rows = self.query(sql, params)?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            count => Err(Error::Db(format!(
                "query returned {count} rows where at most one was expected"
            ))),
        }
    }

    pub fn query_one(
        &mut self,
        sql: &str,
        params: &[&(dyn DbParam + Sync)],
    ) -> Result<DatabaseRow> {
        self.query_opt(sql, params)?
            .ok_or(Error::QueryReturnedNoRows)
    }

    pub fn commit(self) -> Result<()> {
        match self {
            Self::Postgres(transaction) => transaction.commit().map_err(map_db_err),
            Self::Sqlite(transaction) => transaction.commit().map_err(map_sqlite_err),
        }
    }

    pub fn rollback(self) -> Result<()> {
        match self {
            Self::Postgres(transaction) => transaction.rollback().map_err(map_db_err),
            Self::Sqlite(transaction) => transaction.rollback().map_err(map_sqlite_err),
        }
    }
}

pub struct DatabaseRow {
    inner: Row,
}

impl DatabaseRow {
    fn new(inner: Row) -> Self {
        Self { inner }
    }

    pub fn get<I, T>(&self, index: I) -> T
    where
        I: IntoRowIndex,
        T: FromCell,
    {
        self.inner
            .get(index)
            .unwrap_or_else(|error| panic!("database row conversion failed: {error}"))
    }
}

fn configure_sqlite_client(client: &SqliteConnection) -> Result<()> {
    client
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .map_err(map_sqlite_err)?;
    for attempt in 0..=SQLITE_CONFIG_RETRY_ATTEMPTS {
        match client.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        ) {
            Ok(()) => return Ok(()),
            Err(error)
                if attempt < SQLITE_CONFIG_RETRY_ATTEMPTS
                    && sqlite_error_is_busy_or_locked(&error) =>
            {
                std::thread::sleep(SQLITE_CONFIG_RETRY_DELAY);
            }
            Err(error) => return Err(map_sqlite_err(error)),
        }
    }
    unreachable!("bounded SQLite configuration retry loop must return")
}

fn sqlite_error_is_busy_or_locked(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(inner, _)
            if matches!(
                inner.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

fn postgres_db_param_refs<'a>(
    params: &'a [&'a (dyn DbParam + Sync)],
) -> Vec<&'a (dyn ToSql + Sync)> {
    params.iter().map(|value| value.as_postgres()).collect()
}

fn sqlite_db_param_values(params: &[&(dyn DbParam + Sync)]) -> Vec<rusqlite::types::Value> {
    params.iter().map(|value| value.sqlite_value()).collect()
}

fn query_sqlite_rows(
    client: &SqliteConnection,
    sql: &str,
    params: &[&(dyn DbParam + Sync)],
) -> Result<Vec<DatabaseRow>> {
    let translated = translate_sqlite_sql(sql);
    let values = sqlite_db_param_values(params);
    let mut statement = client.prepare(&translated).map_err(map_sqlite_err)?;
    let mut query = statement
        .query(rusqlite::params_from_iter(values.iter()))
        .map_err(map_sqlite_err)?;
    let mut rows = Vec::new();
    while let Some(row) = query.next().map_err(map_sqlite_err)? {
        rows.push(DatabaseRow::new(Row::new_sqlite(row)?));
    }
    Ok(rows)
}

pub trait OptionalExtension<T> {
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExtension<T> for Result<T> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[macro_export]
macro_rules! params {
    () => {
        Vec::<$crate::storage::pg::ParamValue>::new()
    };
    ($($value:expr),+ $(,)?) => {{
        vec![$($crate::storage::pg::to_param_value(&$value)),+]
    }};
}
