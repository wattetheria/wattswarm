use postgres::types::{ToSql, Type as PgType};
use postgres::{Client, NoTls, Row as PgRow};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub mod types {
    #[derive(Debug, Clone, Copy)]
    pub enum Type {
        Text,
    }

    #[derive(Debug)]
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

fn default_pg_url() -> String {
    std::env::var("WATTSWARM_CORE_PG_URL")
        .ok()
        .or_else(|| std::env::var("WATTSWARM_PG_URL").ok())
        .unwrap_or_else(|| "postgres://postgres:postgres@127.0.0.1:55432/wattswarm".to_owned())
}

const PRIMARY_SCHEMA: &str = "public";

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

#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
}

struct Inner {
    client: Mutex<Client>,
    schema: String,
    database_url: String,
    cleanup_on_drop: bool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if !self.cleanup_on_drop || self.schema == PRIMARY_SCHEMA {
            return;
        }
        let mut client = match Client::connect(&self.database_url, NoTls) {
            Ok(c) => c,
            Err(_) => return,
        };
        let ddl = format!("DROP SCHEMA IF EXISTS {} CASCADE;", self.schema);
        let _ = client.batch_execute(&ddl);
    }
}

impl Connection {
    pub fn open(_path: impl AsRef<Path>) -> Result<Self> {
        // Keep the `path` parameter for CLI/API backward compatibility.
        // PostgreSQL-only mode defaults to `public`.
        // Optional explicit schema selection is controlled by WATTSWARM_PG_SCHEMA.
        let schema = configured_schema_name();
        let create_schema = schema != PRIMARY_SCHEMA;
        Self::connect_for_schema(schema, create_schema, false)
    }

    pub fn open_in_memory() -> Result<Self> {
        let schema = sanitize_ident(&format!("ws_test_{}", uuid::Uuid::new_v4().simple()));
        // Test-only "in-memory" mode is schema-backed in PostgreSQL.
        // Auto-clean the schema on drop to avoid long-term schema clutter.
        Self::connect_for_schema(schema, true, true)
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
                client: Mutex::new(client),
                schema,
                database_url: url,
                cleanup_on_drop,
            }),
        })
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let mut client = self
            .inner
            .client
            .lock()
            .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
        client.batch_execute(sql).map_err(map_db_err)?;
        Ok(())
    }

    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        let mut client = self
            .inner
            .client
            .lock()
            .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
        let values = params.to_values();
        let refs = build_param_refs(&values);
        Ok(client.execute(sql, &refs).map_err(map_db_err)? as usize)
    }

    pub fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row) -> Result<T>,
    {
        let mut client = self
            .inner
            .client
            .lock()
            .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
        let values = params.to_values();
        let refs = build_param_refs(&values);
        let maybe = client.query_opt(sql, &refs).map_err(map_db_err)?;
        let row = maybe.ok_or(Error::QueryReturnedNoRows)?;
        f(&Row::new(row))
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
        let mut client = self
            .conn
            .inner
            .client
            .lock()
            .map_err(|_| Error::Db("mutex poisoned".to_owned()))?;
        let values = params.to_values();
        let refs = build_param_refs(&values);
        let rows = client.query(&self.sql, &refs).map_err(map_db_err)?;
        if let Some(row) = rows.first() {
            self.column_names = row.columns().iter().map(|c| c.name().to_owned()).collect();
        }
        let mut mapped = Vec::with_capacity(rows.len());
        for row in rows {
            mapped.push(f(&Row::new(row)));
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
    row: PgRow,
}

impl Row {
    fn new(row: PgRow) -> Self {
        Self { row }
    }

    pub fn get<I, T>(&self, idx: I) -> Result<T>
    where
        I: IntoRowIndex,
        T: FromCell,
    {
        T::from_cell(&self.row, idx.to_index())
    }

    pub fn get_ref(&self, idx: usize) -> Result<types::ValueRef> {
        let t = self.row.columns()[idx].type_();
        if *t == PgType::INT2 {
            let v = self
                .row
                .try_get::<usize, Option<i16>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Integer(inner as i64))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::INT4 {
            let v = self
                .row
                .try_get::<usize, Option<i32>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Integer(inner as i64))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::INT8 {
            let v = self
                .row
                .try_get::<usize, Option<i64>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Integer)
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::FLOAT4 {
            let v = self
                .row
                .try_get::<usize, Option<f32>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Real(inner as f64))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::FLOAT8 || *t == PgType::NUMERIC {
            let v = self
                .row
                .try_get::<usize, Option<f64>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Real)
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::BOOL {
            let v = self
                .row
                .try_get::<usize, Option<bool>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(|inner| types::ValueRef::Integer(i64::from(inner)))
                .unwrap_or(types::ValueRef::Null));
        }
        if *t == PgType::BYTEA {
            let v = self
                .row
                .try_get::<usize, Option<Vec<u8>>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Blob)
                .unwrap_or(types::ValueRef::Null));
        }
        if is_textual_type(t) {
            let v = self
                .row
                .try_get::<usize, Option<String>>(idx)
                .map_err(|e| Error::Db(e.to_string()))?;
            return Ok(v
                .map(types::ValueRef::Text)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = self.row.try_get::<usize, Option<String>>(idx) {
            return Ok(v
                .map(types::ValueRef::Text)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = self.row.try_get::<usize, Option<i64>>(idx) {
            return Ok(v
                .map(types::ValueRef::Integer)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = self.row.try_get::<usize, Option<f64>>(idx) {
            return Ok(v
                .map(types::ValueRef::Real)
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = self.row.try_get::<usize, Option<bool>>(idx) {
            return Ok(v
                .map(|inner| types::ValueRef::Integer(i64::from(inner)))
                .unwrap_or(types::ValueRef::Null));
        }
        if let Ok(v) = self.row.try_get::<usize, Option<Vec<u8>>>(idx) {
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
        self.row.columns().len()
    }

    pub fn column_name(&self, idx: usize) -> &str {
        self.row.columns()[idx].name()
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

pub trait FromCell: Sized {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self>;
}

impl FromCell for String {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        row.try_get::<usize, String>(idx)
            .map_err(|e| Error::Db(e.to_string()))
    }
}

impl FromCell for i64 {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
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
}

impl FromCell for i32 {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        row.try_get::<usize, i32>(idx)
            .map_err(|e| Error::Db(e.to_string()))
    }
}

impl FromCell for u64 {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        let v = row
            .try_get::<usize, i64>(idx)
            .map_err(|e| Error::Db(e.to_string()))?;
        Ok(v.max(0) as u64)
    }
}

impl FromCell for u32 {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        let v = row
            .try_get::<usize, i64>(idx)
            .map_err(|e| Error::Db(e.to_string()))?;
        Ok(v.max(0) as u32)
    }
}

impl FromCell for f64 {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        row.try_get::<usize, f64>(idx)
            .map_err(|e| Error::Db(e.to_string()))
    }
}

impl FromCell for bool {
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        if let Ok(v) = row.try_get::<usize, bool>(idx) {
            return Ok(v);
        }
        let v = row
            .try_get::<usize, i64>(idx)
            .map_err(|e| Error::Db(e.to_string()))?;
        Ok(v != 0)
    }
}

fn cell_is_null(row: &PgRow, idx: usize) -> Result<bool> {
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
    fn from_cell(row: &PgRow, idx: usize) -> Result<Self> {
        if cell_is_null(row, idx)? {
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
