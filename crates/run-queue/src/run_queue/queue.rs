use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wattswarm_storage_core::storage::pg::{
    BackendKind, DatabaseClient as Client, DatabaseTransaction as Transaction,
    configured_backend_kind,
};
use wattswarm_storage_core::storage::sqlite_layout::{WATTSWARM_SQLITE_FILE, sqlite_database_path};

use super::aggregation::{AggregationNextAction, build_run_summary_tx};
use super::status::{
    RUN_STATUS_CANCELLED, RUN_STATUS_CANCELLING, RUN_STATUS_FAILED, RUN_STATUS_FINALIZED,
    RUN_STATUS_QUEUED, STEP_STATUS_CANCELLED, STEP_STATUS_FAILED, STEP_STATUS_SUCCEEDED,
};
use super::types::{RunStepCounts, RunSubmitSpec};
use super::utils::{accumulate_counts, step_counts_tx};

#[derive(Debug, Clone)]
pub struct PgRunQueue {
    pub(crate) database_url: String,
    pub(crate) schema: Option<String>,
    pub(crate) org_id: Arc<String>,
    pub(crate) backend: BackendKind,
    pub(crate) legacy_postgres_schema: Option<String>,
    sqlite_cleanup: Option<Arc<SqliteCleanup>>,
}

const UNSET_RUN_QUEUE_ORG_ID: &str = "__unset_org__";
pub const DEFAULT_RUN_QUEUE_PG_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/wattswarm";
pub const RUN_QUEUE_SQLITE_FILE: &str = WATTSWARM_SQLITE_FILE;

#[derive(Debug)]
struct SqliteCleanup {
    path: PathBuf,
}

impl Drop for SqliteCleanup {
    fn drop(&mut self) {
        for path in [
            self.path.clone(),
            PathBuf::from(format!("{}-wal", self.path.display())),
            PathBuf::from(format!("{}-shm", self.path.display())),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl PgRunQueue {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            schema: None,
            org_id: Arc::new(UNSET_RUN_QUEUE_ORG_ID.to_owned()),
            backend: BackendKind::Postgres,
            legacy_postgres_schema: None,
            sqlite_cleanup: None,
        }
    }

    pub fn with_schema(database_url: impl Into<String>, schema: impl AsRef<str>) -> Self {
        Self {
            database_url: database_url.into(),
            schema: Some(sanitize_ident(schema.as_ref())),
            org_id: Arc::new(UNSET_RUN_QUEUE_ORG_ID.to_owned()),
            backend: BackendKind::Postgres,
            legacy_postgres_schema: None,
            sqlite_cleanup: None,
        }
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Self {
        Self {
            database_url: path.as_ref().to_string_lossy().into_owned(),
            schema: None,
            org_id: Arc::new(UNSET_RUN_QUEUE_ORG_ID.to_owned()),
            backend: BackendKind::Sqlite,
            legacy_postgres_schema: None,
            sqlite_cleanup: None,
        }
    }

    pub fn open_in_memory_sqlite() -> Self {
        let path = std::env::temp_dir().join(format!(
            "wattswarm-run-queue-{}.sqlite",
            uuid::Uuid::new_v4().simple()
        ));
        Self {
            database_url: path.to_string_lossy().into_owned(),
            schema: None,
            org_id: Arc::new(UNSET_RUN_QUEUE_ORG_ID.to_owned()),
            backend: BackendKind::Sqlite,
            legacy_postgres_schema: None,
            sqlite_cleanup: Some(Arc::new(SqliteCleanup { path })),
        }
    }

    pub fn from_runtime_config(
        database_url: impl Into<String>,
        state_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let database_url = database_url.into();
        match configured_backend_kind().map_err(|err| anyhow!(err.to_string()))? {
            BackendKind::Postgres => Ok(Self::from_postgres_runtime_config(
                database_url,
                std::env::var("WATTSWARM_PG_SCHEMA").ok(),
            )),
            BackendKind::Sqlite => Ok(Self::open_sqlite(sqlite_database_path(state_dir.as_ref()))),
        }
    }

    fn from_postgres_runtime_config(database_url: String, schema: Option<String>) -> Self {
        match schema {
            Some(schema) if !schema.trim().is_empty() => {
                let mut queue = Self::with_schema(database_url, schema);
                if queue.schema.as_deref() != Some("public") {
                    queue.legacy_postgres_schema = Some("public".to_owned());
                }
                queue
            }
            _ => Self::new(database_url),
        }
    }

    pub fn for_org(&self, org_id: impl Into<String>) -> Self {
        Self {
            database_url: self.database_url.clone(),
            schema: self.schema.clone(),
            org_id: Arc::new(org_id.into()),
            backend: self.backend,
            legacy_postgres_schema: self.legacy_postgres_schema.clone(),
            sqlite_cleanup: self.sqlite_cleanup.clone(),
        }
    }

    pub fn backend_kind(&self) -> BackendKind {
        self.backend
    }

    pub fn org_id(&self) -> &str {
        assert!(
            self.org_id.as_str() != UNSET_RUN_QUEUE_ORG_ID,
            "run queue org is not configured; call for_org() with the current node org"
        );
        self.org_id.as_str()
    }

    pub fn connect(&self) -> Result<Client> {
        let mut client = match self.backend {
            BackendKind::Postgres => Client::connect_postgres(&self.database_url)
                .map_err(|err| anyhow!("connect postgres {}: {err}", self.database_url))?,
            BackendKind::Sqlite => Client::open_sqlite(&self.database_url)
                .map_err(|err| anyhow!("open sqlite {}: {err}", self.database_url))?,
        };
        if self.backend == BackendKind::Postgres
            && let Some(schema) = &self.schema
        {
            client.batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema}, public;"
            ))?;
        }
        Ok(client)
    }

    pub fn finalize_run_if_terminal_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        now: i64,
    ) -> Result<()> {
        let status = self
            .run_status_tx(tx, run_id)?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        if status != RUN_STATUS_QUEUED
            && status != super::status::RUN_STATUS_RUNNING
            && status != RUN_STATUS_CANCELLING
        {
            return Ok(());
        }
        let counts = step_counts_tx(tx, self.org_id(), run_id)?;
        let active = counts.created
            + counts.queued
            + counts.leased
            + counts.retry_wait
            + counts.remote_dispatched;
        if active > 0 {
            return Ok(());
        }

        let final_status = if status == RUN_STATUS_CANCELLING {
            RUN_STATUS_CANCELLED
        } else if counts.succeeded > 0 {
            RUN_STATUS_FINALIZED
        } else {
            RUN_STATUS_FAILED
        };
        let summary = build_run_summary_tx(tx, self.org_id(), run_id, final_status, &counts)?;
        if status != RUN_STATUS_CANCELLING
            && let AggregationNextAction::ReExplore(directive) = &summary.action
        {
            let shared_inputs_raw: String = tx
                .query_one(
                    "SELECT shared_inputs_json FROM runs WHERE org_id = $1 AND run_id = $2",
                    &[&self.org_id(), &run_id],
                )?
                .get(0);
            let merged_shared_inputs = merge_reexplore_signal(
                &shared_inputs_raw,
                &directive.signal_type,
                &directive.resolution_path,
                directive.next_iteration,
                &directive.unresolved_buckets,
                directive.require_new_evidence,
                directive.null_primary_kind.as_deref(),
                directive.null_reason.as_deref(),
            )?;
            tx.execute(
                "UPDATE runs
                 SET status = $3,
                     shared_inputs_json = $4,
                     result_json = NULL,
                     error_text = NULL,
                     finished_at = NULL,
                     updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[
                    &self.org_id(),
                    &run_id,
                    &RUN_STATUS_QUEUED,
                    &merged_shared_inputs,
                    &now,
                ],
            )?;
            if counts.succeeded > 0 {
                tx.execute(
                    "UPDATE run_steps
                     SET status = $3,
                         next_run_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                         lease_id = NULL,
                         lease_owner = NULL,
                         lease_until = NULL,
                         task_id = NULL,
                         result_json = NULL,
                         error_text = NULL,
                         finished_at = NULL,
                         updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                     WHERE org_id = $1
                       AND run_id = $2
                       AND status = $5",
                    &[
                        &self.org_id(),
                        &run_id,
                        &super::status::STEP_STATUS_QUEUED,
                        &now,
                        &STEP_STATUS_SUCCEEDED,
                    ],
                )?;
            } else {
                tx.execute(
                    "UPDATE run_steps
                     SET status = $3,
                         next_run_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                         lease_id = NULL,
                         lease_owner = NULL,
                         lease_until = NULL,
                         task_id = NULL,
                         result_json = NULL,
                         error_text = NULL,
                         finished_at = NULL,
                         updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                     WHERE org_id = $1
                       AND run_id = $2
                       AND status IN ($5, $6, $7)",
                    &[
                        &self.org_id(),
                        &run_id,
                        &super::status::STEP_STATUS_QUEUED,
                        &now,
                        &STEP_STATUS_SUCCEEDED,
                        &STEP_STATUS_FAILED,
                        &STEP_STATUS_CANCELLED,
                    ],
                )?;
            }
            self.insert_event_tx(
                tx,
                run_id,
                &directive.event_type,
                &summary.event_payload,
                now,
            )?;
            return Ok(());
        }
        tx.execute(
            "UPDATE runs
             SET status = $3, result_json = $4, finished_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'), updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND run_id = $2",
            &[
                &self.org_id(),
                &run_id,
                &final_status,
                &summary.result_payload.to_string(),
                &now,
            ],
        )?;
        self.insert_event_tx(tx, run_id, "RUN_FINALIZED", &summary.event_payload, now)?;
        Ok(())
    }

    pub fn finalize_run_if_terminal(&self, run_id: &str, now: i64) -> Result<()> {
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        self.finalize_run_if_terminal_tx(&mut tx, run_id, now)?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn run_status_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
    ) -> Result<Option<String>> {
        Ok(tx
            .query_opt(
                "SELECT status FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&self.org_id(), &run_id],
            )?
            .map(|row| row.get::<_, String>(0)))
    }

    pub(crate) fn insert_event_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        event_type: &str,
        payload: &serde_json::Value,
        now: i64,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO run_events(org_id, run_id, event_type, payload_json, created_at)
             VALUES ($1,$2,$3,$4,TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'))",
            &[
                &self.org_id(),
                &run_id,
                &event_type,
                &payload.to_string(),
                &now,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn step_counts(&self, client: &mut Client, run_id: &str) -> Result<RunStepCounts> {
        let rows = client.query(
            "SELECT status, COUNT(1)
             FROM run_steps
             WHERE org_id = $1 AND run_id = $2
             GROUP BY status",
            &[&self.org_id(), &run_id],
        )?;
        Ok(accumulate_counts(rows))
    }

    pub(crate) fn validate_submit_spec(&self, spec: &RunSubmitSpec) -> Result<()> {
        if spec.run_id.trim().is_empty() {
            return Err(anyhow!("run_id is required"));
        }
        if spec.is_stigmergy() {
            if spec
                .market_task_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(anyhow!("market_task_id is required for stigmergy runs"));
            }
            if spec
                .feed_key
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(anyhow!("feed_key is required for stigmergy runs"));
            }
            if spec
                .scope_hint
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(anyhow!("scope_hint is required for stigmergy runs"));
            }
            let policy = spec
                .round_policy
                .as_ref()
                .expect("stigmergy checked by round_policy");
            if policy.min_participants == 0 {
                return Err(anyhow!(
                    "round_policy.min_participants must be > 0 for stigmergy runs"
                ));
            }
            if !(1..=100).contains(&policy.threshold_percent) {
                return Err(anyhow!(
                    "round_policy.threshold_percent must be between 1 and 100 for stigmergy runs"
                ));
            }
            if policy.round_timeout_ms == 0 {
                return Err(anyhow!(
                    "round_policy.round_timeout_ms must be > 0 for stigmergy runs"
                ));
            }
            if policy.max_rounds == 0 {
                return Err(anyhow!(
                    "round_policy.max_rounds must be > 0 for stigmergy runs"
                ));
            }
        } else if spec.agents.is_empty() {
            return Err(anyhow!("agents cannot be empty"));
        }
        if spec.retry.max_attempts == 0 {
            return Err(anyhow!("retry.max_attempts must be > 0"));
        }
        let mut ids = HashSet::new();
        for agent in &spec.agents {
            if agent.agent_id.trim().is_empty() {
                return Err(anyhow!("agent_id is required"));
            }
            if agent.executor.trim().is_empty() {
                return Err(anyhow!("executor is required for agent {}", agent.agent_id));
            }
            if agent.prompt.trim().is_empty() {
                return Err(anyhow!("prompt is required for agent {}", agent.agent_id));
            }
            if !ids.insert(agent.agent_id.clone()) {
                return Err(anyhow!("duplicate agent_id: {}", agent.agent_id));
            }
        }
        Ok(())
    }
}

fn merge_reexplore_signal(
    shared_inputs_raw: &str,
    signal_type: &str,
    resolution_path: &str,
    iteration: u32,
    unresolved_buckets: &[String],
    require_new_evidence: bool,
    null_primary_kind: Option<&str>,
    null_reason: Option<&str>,
) -> Result<String> {
    let original = serde_json::from_str::<Value>(shared_inputs_raw).unwrap_or_else(|_| json!({}));
    let signal = json!({
        "type": signal_type,
        "path": resolution_path,
        "iteration": iteration,
        "unresolved_buckets": unresolved_buckets,
        "require_new_evidence": require_new_evidence,
        "null_primary_kind": null_primary_kind,
        "null_reason": null_reason
    });
    let merged = match original {
        Value::Object(mut obj) => {
            obj.insert("_aggregation_signal".to_owned(), signal);
            Value::Object(obj)
        }
        other => json!({
            "shared_inputs": other,
            "_aggregation_signal": signal
        }),
    };
    Ok(serde_json::to_string(&merged)?)
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::super::types::{
        AggregationPolicy, NullResolverMode, NullTrigger, RetryPolicy, RunAgentSpec, RunSubmitSpec,
        TieResolverMode, TieTrigger,
    };
    use super::super::utils::{build_step_inputs, retry_delay_ms};
    use super::PgRunQueue;
    use wattswarm_control_plane::round_policy::RoundPolicy;
    use wattswarm_storage_core::storage::sqlite_layout::{
        LEGACY_RUN_QUEUE_SQLITE_FILE, WATTSWARM_SQLITE_FILE,
    };

    fn sample_agent(agent_id: &str) -> RunAgentSpec {
        RunAgentSpec {
            agent_id: agent_id.to_owned(),
            executor: "rt-local".to_owned(),
            profile: "default".to_owned(),
            prompt: "review".to_owned(),
            weight: 1.0,
            priority: 0,
        }
    }

    fn sample_spec() -> RunSubmitSpec {
        RunSubmitSpec {
            run_id: "run-1".to_owned(),
            task_type: "resume_review".to_owned(),
            shared_inputs: json!({"resume":"text"}),
            agents: vec![sample_agent("a1")],
            market_task_id: None,
            feed_key: None,
            scope_hint: None,
            round_policy: None,
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
    }

    fn sample_stigmergy_spec() -> RunSubmitSpec {
        RunSubmitSpec {
            run_id: "run-open-1".to_owned(),
            task_type: "open_task".to_owned(),
            shared_inputs: json!({}),
            agents: vec![],
            market_task_id: Some("task-open-1".to_owned()),
            feed_key: Some("tasks.open".to_owned()),
            scope_hint: Some("group:task-open-1".to_owned()),
            round_policy: Some(RoundPolicy {
                min_participants: 2,
                threshold_percent: 60,
                round_timeout_ms: 1_000,
                max_rounds: 2,
                fallback_decision: Some("abstain".to_owned()),
            }),
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
    }

    #[test]
    fn custom_runtime_postgres_schema_enables_legacy_public_queue_migration() {
        let queue = PgRunQueue::from_postgres_runtime_config(
            "postgres://example/wattswarm".to_owned(),
            Some("Tenant-A".to_owned()),
        );
        assert_eq!(queue.schema.as_deref(), Some("tenant_a"));
        assert_eq!(queue.legacy_postgres_schema.as_deref(), Some("public"));

        let public_queue = PgRunQueue::from_postgres_runtime_config(
            "postgres://example/wattswarm".to_owned(),
            Some("public".to_owned()),
        );
        assert_eq!(public_queue.schema.as_deref(), Some("public"));
        assert_eq!(public_queue.legacy_postgres_schema, None);
    }

    #[test]
    fn unified_sqlite_database_migrates_legacy_run_queue_once() {
        let dir = tempdir().expect("temp dir");
        let org_id = "local:run-queue-migration:bootstrap";
        let legacy_path = dir.path().join(LEGACY_RUN_QUEUE_SQLITE_FILE);
        let legacy = PgRunQueue::open_sqlite(&legacy_path).for_org(org_id);
        legacy.init_schema().expect("initialize legacy run queue");
        legacy
            .submit_run(sample_spec())
            .expect("write legacy run queue");
        drop(legacy);

        let unified_path = dir.path().join(WATTSWARM_SQLITE_FILE);
        let unified = PgRunQueue::open_sqlite(&unified_path).for_org(org_id);
        unified
            .init_schema()
            .expect("initialize and migrate unified run queue");
        assert_eq!(
            unified.run_view("run-1").expect("migrated run").run_id,
            "run-1"
        );

        unified.init_schema().expect("repeat idempotent migration");
        let mut client = unified.connect().expect("open migration probe");
        let run_count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&org_id, &"run-1"],
            )
            .expect("count migrated runs")
            .get(0);
        assert_eq!(run_count, 1);
        assert!(legacy_path.is_file());
    }

    #[test]
    fn unified_sqlite_rejects_invalid_legacy_queue_atomically() {
        let dir = tempdir().expect("temp dir");
        let org_id = "local:run-queue-invalid-migration:bootstrap";
        let legacy_path = dir.path().join(LEGACY_RUN_QUEUE_SQLITE_FILE);
        let legacy = PgRunQueue::open_sqlite(&legacy_path).for_org(org_id);
        legacy.init_schema().expect("initialize legacy run queue");
        legacy
            .submit_run(sample_spec())
            .expect("write legacy run queue");
        let mut legacy_client = legacy.connect().expect("open legacy queue");
        legacy_client
            .batch_execute("PRAGMA foreign_keys = OFF")
            .expect("disable legacy foreign keys");
        legacy_client
            .execute(
                "DELETE FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&org_id, &"run-1"],
            )
            .expect("leave orphaned legacy step");
        drop(legacy_client);
        drop(legacy);

        let unified_path = dir.path().join(WATTSWARM_SQLITE_FILE);
        let unified = PgRunQueue::open_sqlite(&unified_path).for_org(org_id);
        let error = unified
            .init_schema()
            .expect_err("invalid legacy queue must not migrate");
        assert!(error.to_string().contains("invalid foreign key"));

        let mut client = unified.connect().expect("open unified migration probe");
        let step_count: i64 = client
            .query_one("SELECT COUNT(*) FROM run_steps", &[])
            .expect("count rolled-back steps")
            .get(0);
        let marker_count: i64 = client
            .query_one(
                "SELECT COUNT(*)
                 FROM wattswarm_sqlite_migrations
                 WHERE migration_key = 'legacy-run-queue-v1'",
                &[],
            )
            .expect("count rolled-back migration markers")
            .get(0);
        assert_eq!(step_count, 0);
        assert_eq!(marker_count, 0);
    }

    #[test]
    fn retry_delay_grows_exponentially_with_floor() {
        assert_eq!(retry_delay_ms(50, 1), 200);
        assert_eq!(retry_delay_ms(500, 1), 500);
        assert_eq!(retry_delay_ms(500, 2), 1_000);
        assert_eq!(retry_delay_ms(500, 3), 2_000);
    }

    #[test]
    fn build_step_inputs_merges_object_and_injects_agent_fields() {
        let merged = build_step_inputs(&json!({"x": 1}), "prompt-a", "agent-a");
        assert_eq!(merged["x"], 1);
        assert_eq!(merged["prompt"], "prompt-a");
        assert_eq!(merged["agent_id"], "agent-a");
    }

    #[test]
    fn build_step_inputs_wraps_non_object_shared_inputs() {
        let merged = build_step_inputs(&json!(["a", "b"]), "prompt-a", "agent-a");
        assert_eq!(merged["shared_inputs"], json!(["a", "b"]));
        assert_eq!(merged["prompt"], "prompt-a");
        assert_eq!(merged["agent_id"], "agent-a");
    }

    #[test]
    fn validate_submit_spec_rejects_duplicate_agent_ids() {
        let queue = PgRunQueue::new("postgres://unused");
        let mut spec = sample_spec();
        spec.agents.push(sample_agent("a1"));
        let err = queue
            .validate_submit_spec(&spec)
            .expect_err("duplicate agent ids should fail");
        assert!(err.to_string().contains("duplicate agent_id"));
    }

    #[test]
    fn validate_submit_spec_rejects_empty_prompt() {
        let queue = PgRunQueue::new("postgres://unused");
        let mut spec = sample_spec();
        spec.agents[0].prompt = "   ".to_owned();
        let err = queue
            .validate_submit_spec(&spec)
            .expect_err("empty prompt should fail");
        assert!(err.to_string().contains("prompt is required"));
    }

    #[test]
    fn validate_submit_spec_allows_stigmergy_without_agents() {
        let queue = PgRunQueue::new("postgres://unused");
        queue
            .validate_submit_spec(&sample_stigmergy_spec())
            .expect("stigmergy without agents is valid");
    }

    #[test]
    fn validate_submit_spec_rejects_stigmergy_without_market_task() {
        let queue = PgRunQueue::new("postgres://unused");
        let mut spec = sample_stigmergy_spec();
        spec.market_task_id = None;
        let err = queue
            .validate_submit_spec(&spec)
            .expect_err("missing market task should fail");
        assert!(err.to_string().contains("market_task_id is required"));
    }

    #[test]
    fn submit_spec_deserialization_applies_defaults() {
        let spec: RunSubmitSpec = serde_json::from_value(json!({
            "run_id": "run-a",
            "agents": [{"agent_id": "a1", "executor": "rt", "prompt": "do it"}]
        }))
        .expect("spec json");
        assert_eq!(spec.task_type, "swarm");
        assert_eq!(spec.retry.max_attempts, 2);
        assert_eq!(spec.retry.backoff_ms, 1_500);
        assert_eq!(spec.aggregation.mode, "all_done");
        assert!(
            spec.aggregation
                .tie_policy
                .enabled_on
                .contains(&TieTrigger::Tie)
        );
        assert_eq!(
            spec.aggregation.tie_policy.chain,
            vec![TieResolverMode::Stochastic]
        );
        assert_eq!(
            spec.aggregation.null_policy.enabled_on,
            vec![NullTrigger::Empty, NullTrigger::QuorumNull]
        );
        assert_eq!(
            spec.aggregation.null_policy.chain,
            vec![NullResolverMode::ReExplore, NullResolverMode::FinalizeNull]
        );
        assert_eq!(spec.agents[0].profile, "default");
        assert_eq!(spec.agents[0].weight, 1.0);
    }

    #[test]
    fn submit_spec_deserializes_stigmergy_without_agents() {
        let spec: RunSubmitSpec = serde_json::from_value(json!({
            "run_id": "run-open",
            "market_task_id": "task-open",
            "feed_key": "tasks.open",
            "scope_hint": "group:task-open",
            "round_policy": {
                "min_participants": 2,
                "threshold_percent": 60,
                "round_timeout_ms": 1000,
                "max_rounds": 2,
                "fallback_decision": "abstain"
            }
        }))
        .expect("stigmergy spec json");
        assert!(spec.agents.is_empty());
        assert!(spec.is_stigmergy());
        assert_eq!(spec.market_task_id.as_deref(), Some("task-open"));
    }
}
