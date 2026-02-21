use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};

use super::queue::PgRunQueue;
use super::status::{
    RUN_STATUS_CANCELLED, RUN_STATUS_CANCELLING, RUN_STATUS_CREATED, RUN_STATUS_FAILED,
    RUN_STATUS_FINALIZED, RUN_STATUS_QUEUED, RUN_STATUS_RUNNING, STEP_STATUS_CANCELLED,
    STEP_STATUS_CREATED, STEP_STATUS_FAILED, STEP_STATUS_QUEUED, STEP_STATUS_RETRY_WAIT,
};
use super::types::{RunEvent, RunSubmitSpec, RunView};
use super::utils::now_ms;

impl PgRunQueue {
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
}
