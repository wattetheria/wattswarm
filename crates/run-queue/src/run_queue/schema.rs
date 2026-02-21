use anyhow::Result;

use super::queue::PgRunQueue;
use super::utils::ensure_run_queue_timestamp_column;

impl PgRunQueue {
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
}
