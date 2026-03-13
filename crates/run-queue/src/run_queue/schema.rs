use anyhow::Result;

use super::queue::PgRunQueue;
use super::utils::ensure_run_queue_timestamp_column;

impl PgRunQueue {
    pub fn init_schema(&self) -> Result<()> {
        let mut client = self.connect()?;
        client.batch_execute(
            r#"
CREATE TABLE IF NOT EXISTS runs (
  org_id TEXT NOT NULL DEFAULT 'bootstrap',
  run_id TEXT NOT NULL,
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
  finished_at TIMESTAMPTZ,
  PRIMARY KEY (org_id, run_id)
);

CREATE TABLE IF NOT EXISTS run_steps (
  org_id TEXT NOT NULL DEFAULT 'bootstrap',
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
  org_id TEXT NOT NULL DEFAULT 'bootstrap',
  run_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (org_id, run_id) REFERENCES runs(org_id, run_id) ON DELETE CASCADE
);
"#,
        )?;
        client.batch_execute(
            r#"
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'runs' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE runs ADD COLUMN org_id TEXT NOT NULL DEFAULT 'bootstrap';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'run_steps' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE run_steps ADD COLUMN org_id TEXT NOT NULL DEFAULT 'bootstrap';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'run_events' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE run_events ADD COLUMN org_id TEXT NOT NULL DEFAULT 'bootstrap';
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
"#,
        )?;
        for column in ["created_at", "updated_at", "started_at", "finished_at"] {
            ensure_run_queue_timestamp_column(&mut client, "runs", column)?;
            ensure_run_queue_timestamp_column(&mut client, "run_steps", column)?;
        }
        ensure_run_queue_timestamp_column(&mut client, "run_events", "created_at")?;
        ensure_run_queue_timestamp_column(&mut client, "run_steps", "next_run_at")?;
        ensure_run_queue_timestamp_column(&mut client, "run_steps", "lease_until")?;
        Ok(())
    }
}
