use crate::control::{RealTaskRunRequest, open_node, run_real_task_flow};
use crate::task_template::sample_contract;
use crate::types::TaskContract;
use anyhow::{Context, Result, anyhow};
use postgres::{Client, NoTls, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

const RUN_STATUS_CREATED: &str = "CREATED";
const RUN_STATUS_QUEUED: &str = "QUEUED";
const RUN_STATUS_RUNNING: &str = "RUNNING";
const RUN_STATUS_CANCELLING: &str = "CANCELLING";
const RUN_STATUS_CANCELLED: &str = "CANCELLED";
const RUN_STATUS_FINALIZED: &str = "FINALIZED";
const RUN_STATUS_FAILED: &str = "FAILED";

const STEP_STATUS_CREATED: &str = "CREATED";
const STEP_STATUS_QUEUED: &str = "QUEUED";
const STEP_STATUS_LEASED: &str = "LEASED";
const STEP_STATUS_SUCCEEDED: &str = "SUCCEEDED";
const STEP_STATUS_FAILED: &str = "FAILED";
const STEP_STATUS_RETRY_WAIT: &str = "RETRY_WAIT";
const STEP_STATUS_CANCELLED: &str = "CANCELLED";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: default_backoff_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationPolicy {
    #[serde(default = "default_aggregation_mode")]
    pub mode: String,
    pub quorum: Option<u32>,
}

impl Default for AggregationPolicy {
    fn default() -> Self {
        Self {
            mode: default_aggregation_mode(),
            quorum: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAgentSpec {
    pub agent_id: String,
    pub executor: String,
    #[serde(default = "default_profile")]
    pub profile: String,
    pub prompt: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSubmitSpec {
    pub run_id: String,
    #[serde(default = "default_task_type")]
    pub task_type: String,
    #[serde(default = "default_shared_inputs")]
    pub shared_inputs: Value,
    pub agents: Vec<RunAgentSpec>,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub aggregation: AggregationPolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunView {
    pub run_id: String,
    pub status: String,
    pub task_type: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub counts: RunStepCounts,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RunStepCounts {
    pub created: i64,
    pub queued: i64,
    pub leased: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub retry_wait: i64,
    pub cancelled: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunEvent {
    pub id: i64,
    pub run_id: String,
    pub event_type: String,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
struct ClaimedStep {
    step_id: String,
    run_id: String,
    agent_id: String,
    executor: String,
    profile: String,
    prompt: String,
    attempt: u32,
    max_attempts: u32,
    task_type: String,
    shared_inputs: Value,
    retry_policy: RetryPolicy,
    run_status: String,
    lease_id: String,
}

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    pub worker_id: String,
    pub concurrency: usize,
    pub poll_ms: u64,
    pub lease_ms: u64,
    pub once: bool,
}

#[derive(Debug, Clone)]
pub struct PgRunQueue {
    database_url: String,
}

impl PgRunQueue {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
        }
    }

    fn connect(&self) -> Result<Client> {
        Client::connect(&self.database_url, NoTls)
            .with_context(|| format!("connect postgres {}", self.database_url))
    }

    pub fn init_schema(&self) -> Result<()> {
        let mut client = self.connect()?;
        client.batch_execute(
            r#"
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  task_type TEXT NOT NULL,
  shared_inputs_json TEXT NOT NULL,
  retry_policy_json TEXT NOT NULL,
  aggregation_policy_json TEXT NOT NULL,
  result_json TEXT,
  error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS run_steps (
  step_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL,
  executor TEXT NOT NULL,
  profile TEXT NOT NULL,
  prompt TEXT NOT NULL,
  weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
  priority INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  next_run_at BIGINT NOT NULL,
  lease_id TEXT,
  lease_owner TEXT,
  lease_until BIGINT,
  task_id TEXT,
  result_json TEXT,
  error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  UNIQUE(run_id, agent_id)
);

CREATE TABLE IF NOT EXISTS run_events (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_steps_sched ON run_steps(status, next_run_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_run_steps_run_status ON run_steps(run_id, status);
CREATE INDEX IF NOT EXISTS idx_run_steps_lease_until ON run_steps(lease_until);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(run_id, id DESC);
"#,
        )?;
        for column in ["created_at", "updated_at", "started_at", "finished_at"] {
            ensure_run_queue_timestamp_column(&mut client, "runs", column)?;
            ensure_run_queue_timestamp_column(&mut client, "run_steps", column)?;
        }
        ensure_run_queue_timestamp_column(&mut client, "run_events", "created_at")?;
        Ok(())
    }

    pub fn submit_run(&self, spec: RunSubmitSpec) -> Result<()> {
        self.validate_submit_spec(&spec)?;
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let retry_json = serde_json::to_string(&spec.retry)?;
        let agg_json = serde_json::to_string(&spec.aggregation)?;
        let shared_inputs_json = serde_json::to_string(&spec.shared_inputs)?;
        tx.execute(
            "INSERT INTO runs(run_id, status, task_type, shared_inputs_json, retry_policy_json, aggregation_policy_json, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'))",
            &[
                &spec.run_id,
                &RUN_STATUS_CREATED,
                &spec.task_type,
                &shared_inputs_json,
                &retry_json,
                &agg_json,
                &now,
            ],
        )
        .with_context(|| format!("insert run {}", spec.run_id))?;

        for agent in spec.agents {
            let step_id = format!("{}:{}", spec.run_id, agent.agent_id);
            tx.execute(
                "INSERT INTO run_steps(
                    step_id, run_id, agent_id, executor, profile, prompt, weight, priority,
                    status, attempt, max_attempts, next_run_at, created_at, updated_at
                 )
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10,$11,TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond'))",
                &[
                    &step_id,
                    &spec.run_id,
                    &agent.agent_id,
                    &agent.executor,
                    &agent.profile,
                    &agent.prompt,
                    &agent.weight,
                    &agent.priority,
                    &STEP_STATUS_CREATED,
                    &(spec.retry.max_attempts as i32),
                    &now,
                ],
            )
            .with_context(|| format!("insert step {}", step_id))?;
        }
        self.insert_event_tx(
            &mut tx,
            &spec.run_id,
            "RUN_CREATED",
            &json!({"run_id": spec.run_id}),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn kickoff_run(&self, run_id: &str) -> Result<()> {
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let status = self
            .run_status_tx(&mut tx, run_id)?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        if status == RUN_STATUS_FINALIZED
            || status == RUN_STATUS_FAILED
            || status == RUN_STATUS_CANCELLED
            || status == RUN_STATUS_CANCELLING
        {
            return Ok(());
        }
        tx.execute(
            "UPDATE runs
             SET status = CASE
                WHEN status = $2 THEN $3
                ELSE status
             END,
             started_at = COALESCE(started_at, TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')),
             updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE run_id = $1",
            &[&run_id, &RUN_STATUS_CREATED, &RUN_STATUS_QUEUED, &now],
        )?;
        tx.execute(
            "UPDATE run_steps
             SET status = $2, next_run_at = $3, updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             WHERE run_id = $1 AND status IN ($4, $5)",
            &[
                &run_id,
                &STEP_STATUS_QUEUED,
                &now,
                &STEP_STATUS_CREATED,
                &STEP_STATUS_RETRY_WAIT,
            ],
        )?;
        self.insert_event_tx(
            &mut tx,
            run_id,
            "RUN_KICKOFF",
            &json!({"run_id": run_id}),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn cancel_run(&self, run_id: &str) -> Result<()> {
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let updated = tx.execute(
            "UPDATE runs
             SET status = CASE
                WHEN status IN ($2,$3,$4) THEN $5
                ELSE status
             END,
             updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
             WHERE run_id = $1",
            &[
                &run_id,
                &RUN_STATUS_CREATED,
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
                &RUN_STATUS_CANCELLING,
                &now,
            ],
        )?;
        if updated == 0 {
            return Err(anyhow!("run not found: {run_id}"));
        }
        tx.execute(
            "UPDATE run_steps
             SET status = $2, lease_id = NULL, lease_owner = NULL, lease_until = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'), finished_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             WHERE run_id = $1 AND status IN ($4, $5, $6)",
            &[
                &run_id,
                &STEP_STATUS_CANCELLED,
                &now,
                &STEP_STATUS_CREATED,
                &STEP_STATUS_QUEUED,
                &STEP_STATUS_RETRY_WAIT,
            ],
        )?;
        self.insert_event_tx(
            &mut tx,
            run_id,
            "RUN_CANCEL_REQUESTED",
            &json!({"run_id": run_id}),
            now,
        )?;
        self.finalize_run_if_terminal_tx(&mut tx, run_id, now)?;
        tx.commit()?;
        Ok(())
    }

    pub fn retry_run(&self, run_id: &str) -> Result<()> {
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let status = self
            .run_status_tx(&mut tx, run_id)?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        if status == RUN_STATUS_CANCELLED {
            return Err(anyhow!("run is cancelled, cannot retry: {run_id}"));
        }

        let moved = tx.execute(
            "UPDATE run_steps
             SET status = $2, next_run_at = $3, lease_id = NULL, lease_owner = NULL, lease_until = NULL, error_text = NULL, updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'), finished_at = NULL
             WHERE run_id = $1 AND status IN ($4, $5)",
            &[
                &run_id,
                &STEP_STATUS_QUEUED,
                &now,
                &STEP_STATUS_FAILED,
                &STEP_STATUS_RETRY_WAIT,
            ],
        )?;
        if moved > 0 {
            tx.execute(
                "UPDATE runs
                 SET status = $2, error_text = NULL, result_json = NULL, finished_at = NULL, updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                 WHERE run_id = $1",
                &[&run_id, &RUN_STATUS_QUEUED, &now],
            )?;
        }
        self.insert_event_tx(
            &mut tx,
            run_id,
            "RUN_RETRY_REQUESTED",
            &json!({"run_id": run_id, "steps_requeued": moved}),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn run_view(&self, run_id: &str) -> Result<RunView> {
        let mut client = self.connect()?;
        let row = client
            .query_opt(
                "SELECT run_id, status, task_type,
                        (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT,
                        (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT,
                        CASE WHEN started_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM started_at) * 1000)::BIGINT END,
                        CASE WHEN finished_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM finished_at) * 1000)::BIGINT END
                 FROM runs WHERE run_id = $1",
                &[&run_id],
            )?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        let counts = self.step_counts(&mut client, run_id)?;
        Ok(RunView {
            run_id: row.get::<_, String>(0),
            status: row.get::<_, String>(1),
            task_type: row.get::<_, String>(2),
            created_at: row.get::<_, i64>(3),
            updated_at: row.get::<_, i64>(4),
            started_at: row.get::<_, Option<i64>>(5),
            finished_at: row.get::<_, Option<i64>>(6),
            counts,
        })
    }

    pub fn run_result(&self, run_id: &str) -> Result<Value> {
        let mut client = self.connect()?;
        let row = client
            .query_opt(
                "SELECT status, result_json, error_text,
                        CASE WHEN finished_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM finished_at) * 1000)::BIGINT END
                 FROM runs WHERE run_id = $1",
                &[&run_id],
            )?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        let status: String = row.get(0);
        let result_raw: Option<String> = row.get(1);
        let error_text: Option<String> = row.get(2);
        let finished_at: Option<i64> = row.get(3);
        let result = result_raw
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or_else(|| json!({}));
        Ok(json!({
            "run_id": run_id,
            "status": status,
            "finished_at": finished_at,
            "error": error_text,
            "result": result
        }))
    }

    pub fn run_events(&self, run_id: &str, limit: i64) -> Result<Vec<RunEvent>> {
        let mut client = self.connect()?;
        let rows = client.query(
            "SELECT id, run_id, event_type, payload_json,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT
             FROM run_events
             WHERE run_id = $1
             ORDER BY id DESC
             LIMIT $2",
            &[&run_id, &limit],
        )?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let payload_raw: String = row.get(3);
            let payload = serde_json::from_str(&payload_raw).unwrap_or_else(|_| json!({}));
            events.push(RunEvent {
                id: row.get(0),
                run_id: row.get(1),
                event_type: row.get(2),
                payload,
                created_at: row.get(4),
            });
        }
        Ok(events)
    }

    pub fn run_worker(&self, opts: WorkerOptions, state_dir: &Path, db_path: &Path) -> Result<()> {
        loop {
            let claimed =
                self.claim_steps(&opts.worker_id, opts.concurrency.max(1), opts.lease_ms)?;
            if claimed.is_empty() {
                if opts.once {
                    break;
                }
                thread::sleep(Duration::from_millis(opts.poll_ms.max(25)));
                continue;
            }

            let mut handles = Vec::with_capacity(claimed.len());
            for step in claimed {
                let queue = self.clone();
                let state_dir = state_dir.to_path_buf();
                let db_path = db_path.to_path_buf();
                handles.push(thread::spawn(move || {
                    queue.process_step(step, &state_dir, &db_path)
                }));
            }
            for handle in handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => eprintln!("worker step error: {err:#}"),
                    Err(_) => eprintln!("worker thread panicked"),
                }
            }

            if opts.once {
                break;
            }
        }
        Ok(())
    }

    fn process_step(&self, step: ClaimedStep, state_dir: &Path, db_path: &Path) -> Result<()> {
        if step.run_status == RUN_STATUS_CANCELLING {
            return self.finish_step_terminal(
                &step,
                STEP_STATUS_CANCELLED,
                None,
                None,
                Some("run cancelling"),
            );
        }

        let mut node = open_node(state_dir, db_path)?;
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))?
            .policy_hash;
        let task_id = format!("run-{}-{}-{}", step.run_id, step.agent_id, step.attempt);
        let mut contract: TaskContract = sample_contract(&task_id, policy_hash);
        contract.task_type = step.task_type.clone();
        contract.inputs = build_step_inputs(&step.shared_inputs, &step.prompt, &step.agent_id);

        let run_result = run_real_task_flow(
            &mut node,
            state_dir,
            RealTaskRunRequest {
                executor: step.executor.clone(),
                profile: step.profile.clone(),
                task_id: Some(task_id.clone()),
                task_file: None,
                task_contract: Some(contract),
            },
        );

        match run_result {
            Ok(result) => self.finish_step_terminal(
                &step,
                STEP_STATUS_SUCCEEDED,
                Some(&task_id),
                Some(&result),
                None,
            ),
            Err(err) => {
                let err_text = format!("{err:#}");
                if step.attempt < step.max_attempts {
                    let delay_ms = retry_delay_ms(step.retry_policy.backoff_ms, step.attempt);
                    self.finish_step_retry_wait(
                        &step,
                        &err_text,
                        now_ms().saturating_add(delay_ms),
                    )
                } else {
                    self.finish_step_terminal(
                        &step,
                        STEP_STATUS_FAILED,
                        Some(&task_id),
                        None,
                        Some(&err_text),
                    )
                }
            }
        }
    }

    fn claim_steps(
        &self,
        worker_id: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<ClaimedStep>> {
        let now = now_ms();
        let lease_until = now.saturating_add(lease_ms as i64);
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let rows = tx.query(
            "SELECT s.step_id, s.run_id, s.agent_id, s.executor, s.profile, s.prompt, s.attempt, s.max_attempts,
                    r.task_type, r.shared_inputs_json, r.retry_policy_json, r.status
             FROM run_steps s
             JOIN runs r ON r.run_id = s.run_id
             WHERE r.status IN ($1, $2, $3)
               AND s.status IN ($4, $5)
               AND s.next_run_at <= $6
             ORDER BY s.priority DESC, s.next_run_at ASC, s.created_at ASC
             LIMIT $7
             FOR UPDATE SKIP LOCKED",
            &[
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
                &RUN_STATUS_CANCELLING,
                &STEP_STATUS_QUEUED,
                &STEP_STATUS_RETRY_WAIT,
                &now,
                &(limit as i64),
            ],
        )?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let step_id: String = row.get(0);
            let run_id: String = row.get(1);
            let attempt_prev: i32 = row.get(6);
            let attempt_now = attempt_prev.saturating_add(1) as u32;
            let lease_id = Uuid::new_v4().to_string();
            let updated = tx.execute(
                "UPDATE run_steps
                 SET status = $2, lease_id = $3, lease_owner = $4, lease_until = $5,
                     attempt = attempt + 1, updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'), started_at = COALESCE(started_at, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))
                 WHERE step_id = $1",
                &[
                    &step_id,
                    &STEP_STATUS_LEASED,
                    &lease_id,
                    &worker_id,
                    &lease_until,
                    &now,
                ],
            )?;
            if updated == 0 {
                continue;
            }
            tx.execute(
                "UPDATE runs
                 SET status = $2, started_at = COALESCE(started_at, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')), updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                 WHERE run_id = $1 AND status = $4",
                &[&run_id, &RUN_STATUS_RUNNING, &now, &RUN_STATUS_QUEUED],
            )?;

            let shared_inputs_raw: String = row.get(9);
            let retry_raw: String = row.get(10);
            let shared_inputs =
                serde_json::from_str(&shared_inputs_raw).unwrap_or_else(|_| json!({}));
            let retry_policy = serde_json::from_str(&retry_raw).unwrap_or_default();

            claimed.push(ClaimedStep {
                step_id,
                run_id,
                agent_id: row.get(2),
                executor: row.get(3),
                profile: row.get(4),
                prompt: row.get(5),
                attempt: attempt_now,
                max_attempts: (row.get::<_, i32>(7).max(1)) as u32,
                task_type: row.get(8),
                shared_inputs,
                retry_policy,
                run_status: row.get(11),
                lease_id,
            });
        }
        tx.commit()?;
        Ok(claimed)
    }

    fn finish_step_retry_wait(
        &self,
        step: &ClaimedStep,
        err: &str,
        next_run_at: i64,
    ) -> Result<()> {
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let updated = tx.execute(
            "UPDATE run_steps
             SET status = $3, lease_id = NULL, lease_owner = NULL, lease_until = NULL,
                 next_run_at = $4, error_text = $5, updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
             WHERE step_id = $1 AND lease_id = $2 AND status = $7",
            &[
                &step.step_id,
                &step.lease_id,
                &STEP_STATUS_RETRY_WAIT,
                &next_run_at,
                &err,
                &now,
                &STEP_STATUS_LEASED,
            ],
        )?;
        if updated == 0 {
            tx.rollback()?;
            return Ok(());
        }
        self.insert_event_tx(
            &mut tx,
            &step.run_id,
            "STEP_RETRY_WAIT",
            &json!({"step_id": step.step_id, "attempt": step.attempt, "next_run_at": next_run_at, "error": err}),
            now,
        )?;
        self.finalize_run_if_terminal_tx(&mut tx, &step.run_id, now)?;
        tx.commit()?;
        Ok(())
    }

    fn finish_step_terminal(
        &self,
        step: &ClaimedStep,
        status: &str,
        task_id: Option<&str>,
        result: Option<&Value>,
        error_text: Option<&str>,
    ) -> Result<()> {
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let result_raw = result.map(serde_json::to_string).transpose()?;
        let updated = tx.execute(
            "UPDATE run_steps
             SET status = $3, lease_id = NULL, lease_owner = NULL, lease_until = NULL,
                 task_id = COALESCE($4, task_id), result_json = COALESCE($5, result_json),
                 error_text = $6, updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'), finished_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
             WHERE step_id = $1 AND lease_id = $2 AND status = $8",
            &[
                &step.step_id,
                &step.lease_id,
                &status,
                &task_id,
                &result_raw,
                &error_text,
                &now,
                &STEP_STATUS_LEASED,
            ],
        )?;
        if updated == 0 {
            tx.rollback()?;
            return Ok(());
        }
        self.insert_event_tx(
            &mut tx,
            &step.run_id,
            "STEP_COMPLETED",
            &json!({
                "step_id": step.step_id,
                "status": status,
                "attempt": step.attempt,
                "task_id": task_id,
                "error": error_text
            }),
            now,
        )?;
        self.finalize_run_if_terminal_tx(&mut tx, &step.run_id, now)?;
        tx.commit()?;
        Ok(())
    }

    fn finalize_run_if_terminal_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        now: i64,
    ) -> Result<()> {
        let status = self
            .run_status_tx(tx, run_id)?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        let counts = step_counts_tx(tx, run_id)?;
        let active = counts.created + counts.queued + counts.leased + counts.retry_wait;
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
        let (result_payload, final_event_payload) =
            build_run_summary_tx(tx, run_id, final_status, &counts)?;
        tx.execute(
            "UPDATE runs
             SET status = $2, result_json = $3, finished_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'), updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE run_id = $1",
            &[&run_id, &final_status, &result_payload.to_string(), &now],
        )?;
        self.insert_event_tx(tx, run_id, "RUN_FINALIZED", &final_event_payload, now)?;
        Ok(())
    }

    fn run_status_tx(&self, tx: &mut Transaction<'_>, run_id: &str) -> Result<Option<String>> {
        Ok(tx
            .query_opt("SELECT status FROM runs WHERE run_id = $1", &[&run_id])?
            .map(|row| row.get::<_, String>(0)))
    }

    fn insert_event_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        event_type: &str,
        payload: &Value,
        now: i64,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO run_events(run_id, event_type, payload_json, created_at)
             VALUES ($1,$2,$3,TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))",
            &[&run_id, &event_type, &payload.to_string(), &now],
        )?;
        Ok(())
    }

    fn step_counts(&self, client: &mut Client, run_id: &str) -> Result<RunStepCounts> {
        let rows = client.query(
            "SELECT status, COUNT(1)
             FROM run_steps
             WHERE run_id = $1
             GROUP BY status",
            &[&run_id],
        )?;
        Ok(accumulate_counts(rows))
    }

    fn validate_submit_spec(&self, spec: &RunSubmitSpec) -> Result<()> {
        if spec.run_id.trim().is_empty() {
            return Err(anyhow!("run_id is required"));
        }
        if spec.agents.is_empty() {
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

fn build_run_summary_tx(
    tx: &mut Transaction<'_>,
    run_id: &str,
    final_status: &str,
    counts: &RunStepCounts,
) -> Result<(Value, Value)> {
    let aggregation_mode = tx
        .query_opt(
            "SELECT aggregation_policy_json FROM runs WHERE run_id = $1",
            &[&run_id],
        )?
        .and_then(|row| {
            let raw: String = row.get(0);
            serde_json::from_str::<Value>(&raw).ok()
        })
        .and_then(|policy| {
            policy
                .get("mode")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(default_aggregation_mode);

    let step_rows = tx.query(
        "SELECT step_id, agent_id, executor, profile, status, attempt, task_id, result_json, error_text
         FROM run_steps
         WHERE run_id = $1
         ORDER BY priority DESC, step_id ASC",
        &[&run_id],
    )?;

    let mut decision_votes: HashMap<String, u32> = HashMap::new();
    let mut answer_votes: HashMap<String, u32> = HashMap::new();
    let mut steps = Vec::with_capacity(step_rows.len());

    for row in step_rows {
        let result_raw: Option<String> = row.get(7);
        let result = parse_json_value(result_raw.as_deref());
        let conclusion = extract_step_conclusion(&result);

        if let Some(decision) = conclusion
            .get("decision")
            .and_then(Value::as_str)
            .and_then(normalize_text)
        {
            bump_vote(&mut decision_votes, &decision);
        }
        if let Some(answer) = conclusion
            .get("answer")
            .and_then(Value::as_str)
            .and_then(normalize_text)
        {
            bump_vote(&mut answer_votes, &answer);
        }

        steps.push(json!({
            "step_id": row.get::<_, String>(0),
            "agent_id": row.get::<_, String>(1),
            "executor": row.get::<_, String>(2),
            "profile": row.get::<_, String>(3),
            "status": row.get::<_, String>(4),
            "attempt": row.get::<_, i32>(5),
            "task_id": row.get::<_, Option<String>>(6),
            "error": row.get::<_, Option<String>>(8),
            "result": result,
            "conclusion": conclusion
        }));
    }

    let final_decision = pick_majority(&decision_votes);
    let final_answer = pick_majority(&answer_votes);
    let aggregation_overview = json!({
        "mode": aggregation_mode,
        "final_decision": final_decision.clone(),
        "final_answer": final_answer.clone(),
        "decision_votes": decision_votes,
        "answer_votes": answer_votes
    });
    let result_payload = json!({
        "run_id": run_id,
        "status": final_status,
        "counts": counts,
        "aggregation": {
            "mode": aggregation_mode,
            "final_decision": final_decision,
            "final_answer": final_answer,
            "decision_votes": aggregation_overview["decision_votes"].clone(),
            "answer_votes": aggregation_overview["answer_votes"].clone(),
            "steps": steps
        }
    });
    let final_event_payload = json!({
        "run_id": run_id,
        "status": final_status,
        "counts": counts,
        "aggregation": aggregation_overview
    });
    Ok((result_payload, final_event_payload))
}

fn parse_json_value(raw: Option<&str>) -> Value {
    raw.and_then(|v| serde_json::from_str::<Value>(v).ok())
        .unwrap_or_else(|| json!({}))
}

fn extract_step_conclusion(step_result: &Value) -> Value {
    if let Some(candidate_output) = step_result.get("candidate_output") {
        return candidate_output.clone();
    }
    if let Some(summary) = step_result.get("result_summary") {
        if let Some(raw) = summary.as_str() {
            if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
                return parsed;
            }
        }
        return summary.clone();
    }
    if step_result.get("decision").is_some() || step_result.get("answer").is_some() {
        return step_result.clone();
    }
    Value::Null
}

fn normalize_text(raw: &str) -> Option<String> {
    let text = raw.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_uppercase())
    }
}

fn bump_vote(counter: &mut HashMap<String, u32>, key: &str) {
    *counter.entry(key.to_owned()).or_insert(0) += 1;
}

fn pick_majority(counter: &HashMap<String, u32>) -> Option<String> {
    let mut pairs: Vec<(&String, &u32)> = counter.iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    pairs.first().map(|(key, _)| (*key).clone())
}

fn ensure_run_queue_timestamp_column(client: &mut Client, table: &str, column: &str) -> Result<()> {
    let data_type: String = client
        .query_opt(
            "SELECT data_type
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
            &[&table, &column],
        )?
        .ok_or_else(|| anyhow!("missing column {}.{}", table, column))?
        .get(0);
    if data_type == "timestamp with time zone" {
        return Ok(());
    }
    if data_type != "bigint" {
        return Err(anyhow!(
            "unsupported time column type {}.{}: {}",
            table,
            column,
            data_type
        ));
    }
    let sql = format!(
        "ALTER TABLE {table}
         ALTER COLUMN {column}
         TYPE TIMESTAMPTZ
         USING to_timestamp({column} / 1000.0)"
    );
    client.batch_execute(&sql)?;
    Ok(())
}

fn step_counts_tx(tx: &mut Transaction<'_>, run_id: &str) -> Result<RunStepCounts> {
    let rows = tx.query(
        "SELECT status, COUNT(1)
         FROM run_steps
         WHERE run_id = $1
         GROUP BY status",
        &[&run_id],
    )?;
    Ok(accumulate_counts(rows))
}

fn accumulate_counts(rows: Vec<postgres::Row>) -> RunStepCounts {
    let mut counts = RunStepCounts::default();
    for row in rows {
        let status: String = row.get(0);
        let count: i64 = row.get(1);
        match status.as_str() {
            STEP_STATUS_CREATED => counts.created += count,
            STEP_STATUS_QUEUED => counts.queued += count,
            STEP_STATUS_LEASED => counts.leased += count,
            STEP_STATUS_SUCCEEDED => counts.succeeded += count,
            STEP_STATUS_FAILED => counts.failed += count,
            STEP_STATUS_RETRY_WAIT => counts.retry_wait += count,
            STEP_STATUS_CANCELLED => counts.cancelled += count,
            _ => {}
        }
    }
    counts
}

fn build_step_inputs(shared_inputs: &Value, prompt: &str, agent_id: &str) -> Value {
    let mut merged = Map::new();
    if let Value::Object(obj) = shared_inputs {
        for (k, v) in obj {
            merged.insert(k.clone(), v.clone());
        }
    } else {
        merged.insert("shared_inputs".to_owned(), shared_inputs.clone());
    }
    merged.insert("prompt".to_owned(), Value::String(prompt.to_owned()));
    merged.insert("agent_id".to_owned(), Value::String(agent_id.to_owned()));
    Value::Object(merged)
}

fn retry_delay_ms(base: u64, attempt: u32) -> i64 {
    let exp = attempt.saturating_sub(1).min(16);
    let factor = 1_u64 << exp;
    base.saturating_mul(factor).max(200) as i64
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn default_max_attempts() -> u32 {
    2
}

fn default_backoff_ms() -> u64 {
    1_500
}

fn default_aggregation_mode() -> String {
    "all_done".to_owned()
}

fn default_profile() -> String {
    "default".to_owned()
}

fn default_weight() -> f64 {
    1.0
}

fn default_task_type() -> String {
    "swarm".to_owned()
}

fn default_shared_inputs() -> Value {
    Value::Object(Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;

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
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
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
        let merged = build_step_inputs(&json!({"x":1}), "prompt-a", "agent-a");
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
    fn submit_spec_deserialization_applies_defaults() {
        let spec: RunSubmitSpec = serde_json::from_value(json!({
            "run_id":"run-a",
            "agents":[{"agent_id":"a1","executor":"rt","prompt":"do it"}]
        }))
        .expect("spec json");
        assert_eq!(spec.task_type, "swarm");
        assert_eq!(spec.retry.max_attempts, 2);
        assert_eq!(spec.retry.backoff_ms, 1_500);
        assert_eq!(spec.aggregation.mode, "all_done");
        assert_eq!(spec.agents[0].profile, "default");
        assert_eq!(spec.agents[0].weight, 1.0);
    }
}
