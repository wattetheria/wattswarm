use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const WATTSWARM_SQLITE_FILE: &str = "wattswarm.db";
pub const LEGACY_MAIN_SQLITE_FILE: &str = "wattswarm.state";
pub const LEGACY_LOCAL_CONTROL_SQLITE_FILE: &str = "local-control.state";
pub const LEGACY_RUN_QUEUE_SQLITE_FILE: &str = "run-queue.sqlite3";

const MIGRATION_TABLE: &str = "wattswarm_sqlite_migrations";
const LEGACY_MAIN_MIGRATION: &str = "legacy-main-v1";
const LEGACY_LOCAL_CONTROL_MIGRATION: &str = "legacy-local-control-v1";
const LEGACY_RUN_QUEUE_MIGRATION: &str = "legacy-run-queue-v1";

const LOCAL_CONTROL_TABLES: &[&str] = &[
    "local_config_json",
    "executor_registry_local",
    "discovered_peers_local",
    "peer_metadata_local",
    "network_peer_sync_state_local",
    "peer_relationships_local",
    "peer_dm_threads_local",
    "peer_dm_messages_local",
    "data_plane_status_local",
    "agent_payments_local",
    "agent_event_bus_local",
    "agent_event_delivery_local",
    "remote_task_bridge_registry_local",
    "data_source_bindings_local",
];

const RUN_QUEUE_TABLES: &[&str] = &["runs", "run_steps", "run_events"];

#[derive(Clone, Copy)]
enum ConflictMode {
    Ignore,
    Replace,
}

impl ConflictMode {
    fn sql(self) -> &'static str {
        match self {
            Self::Ignore => "IGNORE",
            Self::Replace => "REPLACE",
        }
    }
}

pub fn sqlite_database_path(state_dir: &Path) -> PathBuf {
    state_dir.join(WATTSWARM_SQLITE_FILE)
}

pub fn is_unified_sqlite_database(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name == WATTSWARM_SQLITE_FILE)
}

/// Imports the two storage-core databases used by the first SQLite release.
///
/// The destination schema must already be initialized. Each source is imported
/// in one `BEGIN IMMEDIATE` transaction and recorded in the destination, so
/// concurrent kernel/worker startup remains idempotent.
pub fn migrate_legacy_storage_databases(target: &Path) -> Result<bool> {
    if !is_unified_sqlite_database(target) {
        return Ok(false);
    }
    let state_dir = target
        .parent()
        .ok_or_else(|| anyhow!("SQLite database path has no parent: {}", target.display()))?;

    let main_tables = application_tables(target)?;
    let migrated_main = migrate_tables(
        target,
        &state_dir.join(LEGACY_MAIN_SQLITE_FILE),
        LEGACY_MAIN_MIGRATION,
        &main_tables,
        ConflictMode::Ignore,
    )?;
    let migrated_local = migrate_tables(
        target,
        &state_dir.join(LEGACY_LOCAL_CONTROL_SQLITE_FILE),
        LEGACY_LOCAL_CONTROL_MIGRATION,
        LOCAL_CONTROL_TABLES,
        ConflictMode::Replace,
    )?;
    Ok(migrated_main || migrated_local)
}

/// Imports the run queue database used by the first SQLite release.
///
/// Run-queue owns the destination schema; callers invoke this only after its
/// three tables have been created in `wattswarm.db`.
pub fn migrate_legacy_run_queue_database(target: &Path) -> Result<bool> {
    if !is_unified_sqlite_database(target) {
        return Ok(false);
    }
    let state_dir = target
        .parent()
        .ok_or_else(|| anyhow!("SQLite database path has no parent: {}", target.display()))?;
    migrate_tables(
        target,
        &state_dir.join(LEGACY_RUN_QUEUE_SQLITE_FILE),
        LEGACY_RUN_QUEUE_MIGRATION,
        RUN_QUEUE_TABLES,
        ConflictMode::Ignore,
    )
}

fn application_tables(target: &Path) -> Result<Vec<String>> {
    let conn = open_migration_connection(target)?;
    let mut stmt = conn.prepare(
        "SELECT name
         FROM sqlite_schema
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite_%'
           AND name != ?1
           AND name NOT IN ('runs', 'run_steps', 'run_events')
         ORDER BY name",
    )?;
    let rows = stmt.query_map(params![MIGRATION_TABLE], |row| row.get::<_, String>(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("list initialized Wattswarm SQLite tables")
}

fn migrate_tables(
    target: &Path,
    source: &Path,
    migration_key: &str,
    tables: &[impl AsRef<str>],
    conflict_mode: ConflictMode,
) -> Result<bool> {
    if !source.is_file() || same_path(target, source) {
        return Ok(false);
    }

    let conn = open_migration_connection(target)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS wattswarm_sqlite_migrations (
             migration_key TEXT PRIMARY KEY,
             source_path TEXT NOT NULL,
             completed_at_ms INTEGER NOT NULL
         );
         PRAGMA foreign_keys = OFF;",
    )
    .context("initialize Wattswarm SQLite migration metadata")?;
    conn.execute(
        "ATTACH DATABASE ?1 AS legacy_source",
        params![source.to_string_lossy().as_ref()],
    )
    .with_context(|| format!("attach legacy SQLite database {}", source.display()))?;

    let migration_result = (|| -> Result<bool> {
        conn.execute_batch("BEGIN IMMEDIATE")
            .context("lock unified Wattswarm SQLite database for migration")?;
        let already_completed = conn
            .query_row(
                "SELECT 1
                 FROM wattswarm_sqlite_migrations
                 WHERE migration_key = ?1",
                params![migration_key],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if already_completed {
            conn.execute_batch("COMMIT")?;
            return Ok(false);
        }

        for table in tables {
            copy_common_columns(&conn, table.as_ref(), conflict_mode)?;
        }
        ensure_foreign_keys_valid(&conn)?;
        conn.execute(
            "INSERT INTO wattswarm_sqlite_migrations(
                 migration_key,
                 source_path,
                 completed_at_ms
             )
             VALUES (?1, ?2, ?3)",
            params![migration_key, source.to_string_lossy().as_ref(), now_ms()],
        )?;
        conn.execute_batch("COMMIT")?;
        Ok(true)
    })();

    if migration_result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    let detach_result = conn.execute_batch("DETACH DATABASE legacy_source");
    conn.execute_batch("PRAGMA foreign_keys = ON")?;

    let migrated = migration_result?;
    detach_result.context("detach legacy Wattswarm SQLite database")?;
    Ok(migrated)
}

fn copy_common_columns(conn: &Connection, table: &str, conflict_mode: ConflictMode) -> Result<()> {
    if !table_exists(conn, "main", table)? || !table_exists(conn, "legacy_source", table)? {
        return Ok(());
    }

    let target_columns = table_columns(conn, "main", table)?;
    let source_columns = table_columns(conn, "legacy_source", table)?
        .into_iter()
        .collect::<HashSet<_>>();
    let common_columns = target_columns
        .into_iter()
        .filter(|column| source_columns.contains(column))
        .collect::<Vec<_>>();
    if common_columns.is_empty() {
        return Ok(());
    }

    let table = quote_identifier(table);
    let columns = common_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "INSERT OR {} INTO main.{table} ({columns})
         SELECT {columns} FROM legacy_source.{table};",
        conflict_mode.sql()
    ))
    .with_context(|| format!("migrate legacy SQLite table {table}"))?;
    Ok(())
}

fn open_migration_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("open unified SQLite database {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;",
    )?;
    Ok(conn)
}

fn table_exists(conn: &Connection, schema: &str, table: &str) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS(
             SELECT 1
             FROM {schema}.sqlite_schema
             WHERE type = 'table' AND name = ?1
         )"
    );
    conn.query_row(&sql, params![table], |row| row.get(0))
        .with_context(|| format!("check SQLite table {schema}.{table}"))
}

fn table_columns(conn: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
    let sql = format!("PRAGMA {schema}.table_info({})", quote_identifier(table));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("list SQLite columns for {schema}.{table}"))
}

fn ensure_foreign_keys_valid(conn: &Connection) -> Result<()> {
    let violation = conn
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .optional()?;
    if let Some((table, row_id, parent)) = violation {
        return Err(anyhow!(
            "legacy SQLite migration created an invalid foreign key: \
             table={table}, row_id={row_id}, parent={parent}"
        ));
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn same_path(left: &Path, right: &Path) -> bool {
    left.canonicalize().ok() == right.canonicalize().ok()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
