use anyhow::{Result, anyhow};
use std::collections::HashSet;
use wattswarm_storage_core::storage::pg::{BackendKind, DatabaseClient};
use wattswarm_storage_core::storage::sqlite_layout::migrate_legacy_run_queue_database;

use super::queue::PgRunQueue;
use super::utils::ensure_run_queue_timestamp_column;

const RUN_QUEUE_SCHEMA_INIT_LOCK_KEY: i64 = 4_281_337_019;
const POSTGRES_QUEUE_MIGRATION_TABLE: &str = "wattswarm_run_queue_migrations";
const POSTGRES_QUEUE_TABLES: &[&str] = &["runs", "run_steps", "run_events"];
type SqliteForeignKeyRow = (i64, i64, String, String, String, String);
const RUN_QUEUE_BASE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
  org_id TEXT NOT NULL DEFAULT '__unset_org__',
  run_id TEXT NOT NULL,
  status TEXT NOT NULL,
  task_type TEXT NOT NULL,
  shared_inputs_json TEXT NOT NULL,
  retry_policy_json TEXT NOT NULL,
  aggregation_policy_json TEXT NOT NULL,
  market_task_id TEXT,
  feed_key TEXT,
  scope_hint TEXT,
  round_due_at_ms BIGINT,
  round_policy_json TEXT,
  round_state_json TEXT,
  result_json TEXT,
  error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  PRIMARY KEY (org_id, run_id)
);

CREATE TABLE IF NOT EXISTS run_steps (
  org_id TEXT NOT NULL DEFAULT '__unset_org__',
  step_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  executor TEXT NOT NULL,
  profile TEXT NOT NULL,
  prompt TEXT NOT NULL,
  weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
  priority INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  next_run_at TIMESTAMPTZ NOT NULL,
  lease_id TEXT,
  lease_owner TEXT,
  lease_until TIMESTAMPTZ,
  task_id TEXT,
  result_json TEXT,
  error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  PRIMARY KEY (org_id, step_id),
  UNIQUE(org_id, run_id, agent_id),
  FOREIGN KEY (org_id, run_id) REFERENCES runs(org_id, run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_events (
  id BIGSERIAL PRIMARY KEY,
  org_id TEXT NOT NULL DEFAULT '__unset_org__',
  run_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (org_id, run_id) REFERENCES runs(org_id, run_id) ON DELETE CASCADE
);
"#;

const RUN_QUEUE_SQLITE_INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_run_steps_sched
  ON run_steps(org_id, status, next_run_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_run_steps_run_status
  ON run_steps(org_id, run_id, status);
CREATE INDEX IF NOT EXISTS idx_run_steps_lease_until
  ON run_steps(org_id, lease_until);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id
  ON run_events(org_id, run_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_runs_stigmergy_market_task
  ON runs(org_id, market_task_id)
  WHERE round_policy_json IS NOT NULL AND status NOT IN ('FINALIZED', 'FAILED', 'CANCELLED');
CREATE INDEX IF NOT EXISTS idx_runs_stigmergy_round_due
  ON runs(org_id, round_due_at_ms)
  WHERE round_policy_json IS NOT NULL AND status NOT IN ('FINALIZED', 'FAILED', 'CANCELLED');
"#;

fn quote_postgres_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn postgres_table_exists(client: &mut DatabaseClient, schema: &str, table: &str) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS(
                 SELECT 1
                 FROM information_schema.tables
                 WHERE table_schema = $1 AND table_name = $2
             )",
            &[&schema, &table],
        )?
        .get(0))
}

fn postgres_table_columns(
    client: &mut DatabaseClient,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    Ok(client
        .query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &table],
        )?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

fn copy_postgres_queue_table(
    client: &mut DatabaseClient,
    source_schema: &str,
    target_schema: &str,
    table: &str,
) -> Result<()> {
    let target_columns = postgres_table_columns(client, target_schema, table)?;
    let source_columns = postgres_table_columns(client, source_schema, table)?
        .into_iter()
        .collect::<HashSet<_>>();
    let common_columns = target_columns
        .into_iter()
        .filter(|column| source_columns.contains(column))
        .filter(|column| table != "run_events" || column != "id")
        .collect::<Vec<_>>();
    if common_columns.is_empty() {
        return Err(anyhow!(
            "legacy PostgreSQL run queue table {source_schema}.{table} has no compatible columns"
        ));
    }

    let columns = common_columns
        .iter()
        .map(|column| quote_postgres_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let source_schema = quote_postgres_identifier(source_schema);
    let target_schema = quote_postgres_identifier(target_schema);
    let table = quote_postgres_identifier(table);
    client.batch_execute(&format!(
        "INSERT INTO {target_schema}.{table} ({columns})
         SELECT {columns}
         FROM {source_schema}.{table}
         ON CONFLICT DO NOTHING;"
    ))?;
    Ok(())
}

fn migrate_legacy_postgres_run_queue(
    client: &mut DatabaseClient,
    source_schema: &str,
    target_schema: &str,
) -> Result<bool> {
    if source_schema == target_schema {
        return Ok(false);
    }

    let table_presence = POSTGRES_QUEUE_TABLES
        .iter()
        .map(|table| postgres_table_exists(client, source_schema, table))
        .collect::<Result<Vec<_>>>()?;
    if table_presence.iter().all(|present| !present) {
        return Ok(false);
    }
    if !table_presence.iter().all(|present| *present) {
        return Err(anyhow!(
            "legacy PostgreSQL run queue schema {source_schema} is incomplete; expected runs, \
             run_steps, and run_events"
        ));
    }

    let target_schema_sql = quote_postgres_identifier(target_schema);
    let migration_table_sql = quote_postgres_identifier(POSTGRES_QUEUE_MIGRATION_TABLE);
    client.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {target_schema_sql}.{migration_table_sql} (
             migration_key TEXT PRIMARY KEY,
             source_schema TEXT NOT NULL,
             completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
         );"
    ))?;
    let migration_key = format!("legacy-{source_schema}-run-queue-v1");
    let completed: bool = client
        .query_one(
            &format!(
                "SELECT EXISTS(
                     SELECT 1
                     FROM {target_schema_sql}.{migration_table_sql}
                     WHERE migration_key = $1
                 )"
            ),
            &[&migration_key],
        )?
        .get(0);
    if completed {
        return Ok(false);
    }

    client.batch_execute("BEGIN")?;
    let migration_result = (|| -> Result<()> {
        for table in POSTGRES_QUEUE_TABLES {
            copy_postgres_queue_table(client, source_schema, target_schema, table)?;
        }
        client.execute(
            &format!(
                "INSERT INTO {target_schema_sql}.{migration_table_sql}(
                     migration_key,
                     source_schema
                 )
                 VALUES ($1, $2)"
            ),
            &[&migration_key, &source_schema],
        )?;
        client.batch_execute("COMMIT")?;
        Ok(())
    })();
    if migration_result.is_err() {
        let _ = client.batch_execute("ROLLBACK");
    }
    migration_result?;
    Ok(true)
}

fn sqlite_primary_key_columns(client: &mut DatabaseClient, table: &str) -> Result<Vec<String>> {
    let mut columns = client
        .query(&format!("PRAGMA table_info({table})"), &[])?
        .into_iter()
        .map(|row| (row.get::<usize, i64>(5), row.get::<usize, String>(1)))
        .filter(|(position, _)| *position > 0)
        .collect::<Vec<_>>();
    columns.sort_by_key(|(position, _)| *position);
    Ok(columns.into_iter().map(|(_, name)| name).collect())
}

fn sqlite_unique_keys(client: &mut DatabaseClient, table: &str) -> Result<HashSet<Vec<String>>> {
    let indexes = client
        .query(&format!("PRAGMA index_list({table})"), &[])?
        .into_iter()
        .map(|row| (row.get::<usize, String>(1), row.get::<usize, i64>(2)))
        .collect::<Vec<_>>();
    let mut keys = HashSet::new();
    for (index_name, unique) in indexes {
        if unique == 0 {
            continue;
        }
        let escaped_index = index_name.replace('"', "\"\"");
        let mut columns = client
            .query(&format!("PRAGMA index_info(\"{escaped_index}\")"), &[])?
            .into_iter()
            .map(|row| (row.get::<usize, i64>(0), row.get::<usize, String>(2)))
            .collect::<Vec<_>>();
        columns.sort_by_key(|(position, _)| *position);
        keys.insert(columns.into_iter().map(|(_, name)| name).collect());
    }
    Ok(keys)
}

fn sqlite_foreign_keys(
    client: &mut DatabaseClient,
    table: &str,
) -> Result<Vec<SqliteForeignKeyRow>> {
    let mut keys = client
        .query(&format!("PRAGMA foreign_key_list({table})"), &[])?
        .into_iter()
        .map(|row| {
            (
                row.get::<usize, i64>(0),
                row.get::<usize, i64>(1),
                row.get::<usize, String>(2),
                row.get::<usize, String>(3),
                row.get::<usize, String>(4),
                row.get::<usize, String>(6),
            )
        })
        .collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

fn validate_sqlite_run_queue_schema(client: &mut DatabaseClient) -> Result<()> {
    for (table, expected) in [
        ("runs", vec!["org_id", "run_id"]),
        ("run_steps", vec!["org_id", "step_id"]),
        ("run_events", vec!["id"]),
    ] {
        let actual = sqlite_primary_key_columns(client, table)?;
        if actual != expected {
            return Err(anyhow!(
                "SQLite run queue constraint mismatch for {table}: expected primary key ({}) but \
                 found ({})",
                expected.join(", "),
                actual.join(", ")
            ));
        }
    }

    let run_step_unique = vec![
        "org_id".to_owned(),
        "run_id".to_owned(),
        "agent_id".to_owned(),
    ];
    if !sqlite_unique_keys(client, "run_steps")?.contains(&run_step_unique) {
        return Err(anyhow!(
            "SQLite run queue constraint mismatch for run_steps: missing unique key \
             (org_id, run_id, agent_id)"
        ));
    }

    let expected_foreign_keys = vec![
        (
            0,
            0,
            "runs".to_owned(),
            "org_id".to_owned(),
            "org_id".to_owned(),
            "CASCADE".to_owned(),
        ),
        (
            0,
            1,
            "runs".to_owned(),
            "run_id".to_owned(),
            "run_id".to_owned(),
            "CASCADE".to_owned(),
        ),
    ];
    for table in ["run_steps", "run_events"] {
        let actual = sqlite_foreign_keys(client, table)?;
        if actual != expected_foreign_keys {
            return Err(anyhow!(
                "SQLite run queue constraint mismatch for {table}: expected composite foreign key \
                 (org_id, run_id) -> runs(org_id, run_id) ON DELETE CASCADE"
            ));
        }
    }
    Ok(())
}

impl PgRunQueue {
    pub fn init_schema(&self) -> Result<()> {
        let mut client = self.connect()?;
        if self.backend_kind() == BackendKind::Sqlite {
            client.batch_execute(RUN_QUEUE_BASE_SCHEMA)?;
            client.batch_execute(RUN_QUEUE_SQLITE_INDEXES)?;
            validate_sqlite_run_queue_schema(&mut client)?;
            drop(client);
            migrate_legacy_run_queue_database(std::path::Path::new(&self.database_url))?;
            return Ok(());
        }
        client.query_one(
            "SELECT pg_advisory_lock($1)",
            &[&RUN_QUEUE_SCHEMA_INIT_LOCK_KEY],
        )?;
        client.batch_execute(RUN_QUEUE_BASE_SCHEMA)?;
        client.batch_execute(
            r#"
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE runs ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'run_steps' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE run_steps ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'run_events' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE run_events ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'market_task_id'
  ) THEN
    ALTER TABLE runs ADD COLUMN market_task_id TEXT;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'feed_key'
  ) THEN
    ALTER TABLE runs ADD COLUMN feed_key TEXT;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'scope_hint'
  ) THEN
    ALTER TABLE runs ADD COLUMN scope_hint TEXT;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'round_due_at_ms'
  ) THEN
    ALTER TABLE runs ADD COLUMN round_due_at_ms BIGINT;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'round_policy_json'
  ) THEN
    ALTER TABLE runs ADD COLUMN round_policy_json TEXT;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'round_state_json'
  ) THEN
    ALTER TABLE runs ADD COLUMN round_state_json TEXT;
  END IF;
END $$;
"#,
        )?;
        client.batch_execute(
            r#"
ALTER TABLE run_events DROP CONSTRAINT IF EXISTS run_events_run_id_fkey;
ALTER TABLE run_events DROP CONSTRAINT IF EXISTS run_events_org_id_run_id_fkey;
ALTER TABLE run_steps DROP CONSTRAINT IF EXISTS run_steps_run_id_fkey;
ALTER TABLE run_steps DROP CONSTRAINT IF EXISTS run_steps_org_id_run_id_fkey;
ALTER TABLE run_steps DROP CONSTRAINT IF EXISTS run_steps_pkey;
ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_pkey;
ALTER TABLE run_steps DROP CONSTRAINT IF EXISTS run_steps_run_id_agent_id_key;

ALTER TABLE runs ADD CONSTRAINT runs_pkey PRIMARY KEY (org_id, run_id);
ALTER TABLE run_steps ADD CONSTRAINT run_steps_pkey PRIMARY KEY (org_id, step_id);
ALTER TABLE run_steps ADD CONSTRAINT run_steps_run_id_agent_id_key UNIQUE (org_id, run_id, agent_id);
ALTER TABLE run_steps ADD CONSTRAINT run_steps_run_id_fkey
  FOREIGN KEY (org_id, run_id) REFERENCES runs(org_id, run_id) ON DELETE CASCADE;
ALTER TABLE run_events ADD CONSTRAINT run_events_run_id_fkey
  FOREIGN KEY (org_id, run_id) REFERENCES runs(org_id, run_id) ON DELETE CASCADE;

DROP INDEX IF EXISTS idx_run_steps_sched;
DROP INDEX IF EXISTS idx_run_steps_run_status;
DROP INDEX IF EXISTS idx_run_steps_lease_until;
DROP INDEX IF EXISTS idx_run_events_run_id;

CREATE INDEX IF NOT EXISTS idx_run_steps_sched ON run_steps(org_id, status, next_run_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_run_steps_run_status ON run_steps(org_id, run_id, status);
CREATE INDEX IF NOT EXISTS idx_run_steps_lease_until ON run_steps(org_id, lease_until);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(org_id, run_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_runs_stigmergy_market_task
  ON runs(org_id, market_task_id)
  WHERE round_policy_json IS NOT NULL AND status NOT IN ('FINALIZED', 'FAILED', 'CANCELLED');
CREATE INDEX IF NOT EXISTS idx_runs_stigmergy_round_due
  ON runs(org_id, round_due_at_ms)
  WHERE round_policy_json IS NOT NULL AND status NOT IN ('FINALIZED', 'FAILED', 'CANCELLED');
"#,
        )?;
        client.batch_execute(
            r#"
ALTER TABLE runs ALTER COLUMN org_id SET DEFAULT '__unset_org__';
ALTER TABLE run_steps ALTER COLUMN org_id SET DEFAULT '__unset_org__';
ALTER TABLE run_events ALTER COLUMN org_id SET DEFAULT '__unset_org__';
"#,
        )?;
        for column in ["created_at", "updated_at", "started_at", "finished_at"] {
            ensure_run_queue_timestamp_column(&mut client, "runs", column)?;
            ensure_run_queue_timestamp_column(&mut client, "run_steps", column)?;
        }
        ensure_run_queue_timestamp_column(&mut client, "run_events", "created_at")?;
        ensure_run_queue_timestamp_column(&mut client, "run_steps", "next_run_at")?;
        ensure_run_queue_timestamp_column(&mut client, "run_steps", "lease_until")?;
        if let (Some(source_schema), Some(target_schema)) = (
            self.legacy_postgres_schema.as_deref(),
            self.schema.as_deref(),
        ) {
            migrate_legacy_postgres_run_queue(&mut client, source_schema, target_schema)?;
        }
        client.query(
            "SELECT pg_advisory_unlock($1)",
            &[&RUN_QUEUE_SCHEMA_INIT_LOCK_KEY],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_queue::{AggregationPolicy, RetryPolicy, RunAgentSpec, RunSubmitSpec};
    use serde_json::json;

    fn test_pg_url() -> String {
        std::env::var("WATTSWARM_CORE_PG_URL")
            .or_else(|_| std::env::var("WATTSWARM_PG_URL"))
            .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:55432/wattswarm".to_owned())
    }

    struct SchemaCleanup {
        client: postgres::Client,
        schemas: Vec<String>,
    }

    impl Drop for SchemaCleanup {
        fn drop(&mut self) {
            for schema in &self.schemas {
                let _ = self
                    .client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
            }
        }
    }

    fn sample_spec() -> RunSubmitSpec {
        RunSubmitSpec {
            run_id: "legacy-run".to_owned(),
            task_type: "swarm".to_owned(),
            shared_inputs: json!({"prompt":"preserve me"}),
            agents: vec![RunAgentSpec {
                agent_id: "agent-a".to_owned(),
                executor: "rt".to_owned(),
                profile: "default".to_owned(),
                prompt: "review".to_owned(),
                weight: 1.0,
                priority: 0,
            }],
            market_task_id: None,
            feed_key: None,
            scope_hint: None,
            round_policy: None,
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
    }

    #[test]
    fn sqlite_run_queue_rejects_missing_primary_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("invalid-run-queue.sqlite3");
        let mut client = DatabaseClient::open_sqlite(&path).expect("open invalid queue setup");
        client
            .batch_execute(
                "CREATE TABLE runs (
                     org_id TEXT NOT NULL DEFAULT '__unset_org__',
                     run_id TEXT NOT NULL,
                     status TEXT NOT NULL,
                     task_type TEXT NOT NULL,
                     shared_inputs_json TEXT NOT NULL,
                     retry_policy_json TEXT NOT NULL,
                     aggregation_policy_json TEXT NOT NULL,
                     market_task_id TEXT,
                     feed_key TEXT,
                     scope_hint TEXT,
                     round_due_at_ms INTEGER,
                     round_policy_json TEXT,
                     round_state_json TEXT,
                     result_json TEXT,
                     error_text TEXT,
                     created_at INTEGER NOT NULL,
                     updated_at INTEGER NOT NULL,
                     started_at INTEGER,
                     finished_at INTEGER
                 );",
            )
            .expect("create invalid runs table");
        drop(client);

        let error = PgRunQueue::open_sqlite(&path)
            .init_schema()
            .expect_err("missing queue primary key must fail initialization");
        assert!(
            error
                .to_string()
                .contains("SQLite run queue constraint mismatch for runs")
        );
    }

    #[test]
    fn custom_postgres_schema_migrates_legacy_run_queue_once() {
        let pg_url = test_pg_url();
        let Ok(mut cleanup_client) = postgres::Client::connect(&pg_url, postgres::NoTls) else {
            eprintln!("skip PostgreSQL run queue migration test: database not reachable");
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let source_schema = format!("ws_queue_legacy_{suffix}");
        let target_schema = format!("ws_queue_target_{suffix}");
        cleanup_client
            .batch_execute(&format!(
                "CREATE SCHEMA {source_schema};
                 CREATE SCHEMA {target_schema};"
            ))
            .expect("create queue migration schemas");
        let _cleanup = SchemaCleanup {
            client: cleanup_client,
            schemas: vec![source_schema.clone(), target_schema.clone()],
        };
        let org_id = "mainnet:test:bootstrap";

        let source = PgRunQueue::with_schema(&pg_url, &source_schema).for_org(org_id);
        source.init_schema().expect("initialize legacy queue");
        source
            .submit_run(sample_spec())
            .expect("write legacy queue data");

        let mut target = PgRunQueue::with_schema(&pg_url, &target_schema);
        target.legacy_postgres_schema = Some(source_schema.clone());
        let target = target.for_org(org_id);
        target
            .init_schema()
            .expect("initialize target and migrate legacy queue");
        assert_eq!(
            target
                .run_view("legacy-run")
                .expect("read migrated run")
                .run_id,
            "legacy-run"
        );

        let mut probe = target.connect().expect("open target queue probe");
        let first_counts = (
            probe
                .query_one("SELECT COUNT(*) FROM runs", &[])
                .expect("count migrated runs")
                .get::<usize, i64>(0),
            probe
                .query_one("SELECT COUNT(*) FROM run_steps", &[])
                .expect("count migrated steps")
                .get::<usize, i64>(0),
            probe
                .query_one("SELECT COUNT(*) FROM run_events", &[])
                .expect("count migrated events")
                .get::<usize, i64>(0),
        );
        drop(probe);

        target.init_schema().expect("repeat idempotent migration");
        let mut probe = target.connect().expect("reopen target queue probe");
        let second_counts = (
            probe
                .query_one("SELECT COUNT(*) FROM runs", &[])
                .expect("recount migrated runs")
                .get::<usize, i64>(0),
            probe
                .query_one("SELECT COUNT(*) FROM run_steps", &[])
                .expect("recount migrated steps")
                .get::<usize, i64>(0),
            probe
                .query_one("SELECT COUNT(*) FROM run_events", &[])
                .expect("recount migrated events")
                .get::<usize, i64>(0),
        );
        let marker_count = probe
            .query_one("SELECT COUNT(*) FROM wattswarm_run_queue_migrations", &[])
            .expect("count migration markers")
            .get::<usize, i64>(0);

        assert_eq!(first_counts, second_counts);
        assert_eq!(first_counts.0, 1);
        assert_eq!(first_counts.1, 1);
        assert!(first_counts.2 >= 1);
        assert_eq!(marker_count, 1);
    }
}
