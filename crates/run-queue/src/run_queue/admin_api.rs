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
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10,TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond'))",
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
             SET status = $2,
                 next_run_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
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
             SET status = $2,
                 next_run_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                 lease_id = NULL,
                 lease_owner = NULL,
                 lease_until = NULL,
                 error_text = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                 finished_at = NULL
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

#[cfg(test)]
mod tests {
    use super::super::status::{
        RUN_STATUS_FAILED, RUN_STATUS_QUEUED, STEP_STATUS_FAILED, STEP_STATUS_QUEUED,
    };
    use super::super::types::{AggregationPolicy, RetryPolicy, RunAgentSpec, RunSubmitSpec};
    use super::*;
    use serde_json::json;
    use uuid::Uuid;
    const TEST_DB_LOCK_KEY: i64 = 1_987_654_321;

    struct DbTestLock {
        client: postgres::Client,
    }

    impl DbTestLock {
        fn acquire(queue: &PgRunQueue) -> Option<Self> {
            let mut client = queue.connect().ok()?;
            client
                .query_one("SELECT pg_advisory_lock($1)", &[&TEST_DB_LOCK_KEY])
                .ok()?;
            Some(Self { client })
        }
    }

    impl Drop for DbTestLock {
        fn drop(&mut self) {
            let _ = self
                .client
                .query("SELECT pg_advisory_unlock($1)", &[&TEST_DB_LOCK_KEY]);
        }
    }

    fn test_pg_url() -> String {
        std::env::var("WATTSWARM_CORE_PG_URL")
            .or_else(|_| std::env::var("WATTSWARM_PG_URL"))
            .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:55432/wattswarm".to_owned())
    }

    fn queue_or_skip() -> Option<PgRunQueue> {
        let queue = PgRunQueue::new(test_pg_url());
        if queue.connect().is_err() {
            eprintln!("skip run-queue admin_api tests: postgres not reachable");
            return None;
        }
        queue.init_schema().expect("init schema");
        Some(queue)
    }

    fn shared_db_is_busy(queue: &PgRunQueue) -> bool {
        let Ok(mut client) = queue.connect() else {
            return false;
        };
        let row = client
            .query_one(
                "SELECT EXISTS(
                    SELECT 1
                    FROM runs
                    WHERE status IN ('QUEUED','RUNNING','CANCELLING')
                      AND run_id NOT LIKE 'worker-%'
                      AND run_id NOT LIKE 'admin-%'
                )",
                &[],
            )
            .ok();
        row.map(|r| r.get::<_, bool>(0)).unwrap_or(false)
    }

    fn sample_spec(run_id: &str) -> RunSubmitSpec {
        RunSubmitSpec {
            run_id: run_id.to_owned(),
            task_type: "swarm".to_owned(),
            shared_inputs: json!({"resume":"text"}),
            agents: vec![RunAgentSpec {
                agent_id: "agent-a".to_owned(),
                executor: "rt".to_owned(),
                profile: "default".to_owned(),
                prompt: "review".to_owned(),
                weight: 1.0,
                priority: 0,
            }],
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
    }

    fn cleanup_run(queue: &PgRunQueue, run_id: &str) {
        if let Ok(mut client) = queue.connect() {
            let _ = client.execute("DELETE FROM runs WHERE run_id = $1", &[&run_id]);
        }
    }

    #[test]
    fn submit_kickoff_cancel_and_query_views() {
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip admin_api db test: shared database has active external runs");
            return;
        }
        let run_id = format!("admin-flow-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);

        queue.submit_run(sample_spec(&run_id)).expect("submit run");

        let created_view = queue.run_view(&run_id).expect("view after submit");
        assert_eq!(created_view.status, RUN_STATUS_CREATED);
        assert_eq!(created_view.counts.created, 1);

        let created_result = queue.run_result(&run_id).expect("result after submit");
        assert_eq!(created_result["status"], RUN_STATUS_CREATED);
        assert_eq!(created_result["result"], json!({}));

        let events = queue.run_events(&run_id, 20).expect("events");
        assert!(events.iter().any(|e| e.event_type == "RUN_CREATED"));

        queue.cancel_run(&run_id).expect("cancel");
        let cancelled_result = queue.run_result(&run_id).expect("result after cancel");
        assert_eq!(cancelled_result["status"], RUN_STATUS_CANCELLED);

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn retry_requeues_failed_steps_and_resets_run_status() {
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip admin_api db test: shared database has active external runs");
            return;
        }
        let run_id = format!("admin-retry-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue.submit_run(sample_spec(&run_id)).expect("submit run");

        let mut client = queue.connect().expect("connect");
        let now = now_ms();
        client
            .execute(
                "UPDATE run_steps
                 SET status = $2,
                     error_text = 'failed',
                     finished_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                     updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                 WHERE run_id = $1",
                &[&run_id, &STEP_STATUS_FAILED, &now],
            )
            .expect("mark step failed");
        client
            .execute(
                "UPDATE runs
                 SET status = $2,
                     error_text = 'failed',
                     finished_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                     updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                 WHERE run_id = $1",
                &[&run_id, &RUN_STATUS_FAILED, &now],
            )
            .expect("mark run failed");

        queue.retry_run(&run_id).expect("retry run");
        let view = queue.run_view(&run_id).expect("view after retry");
        assert!(view.status == RUN_STATUS_QUEUED || view.status == RUN_STATUS_RUNNING);
        assert!(view.counts.queued >= 0);
        assert_eq!(view.counts.failed, 0);

        let step_row = client
            .query_one(
                "SELECT status, error_text FROM run_steps WHERE run_id = $1",
                &[&run_id],
            )
            .expect("step row");
        let step_status: String = step_row.get(0);
        let step_error: Option<String> = step_row.get(1);
        assert!(step_status == STEP_STATUS_QUEUED || step_status == "LEASED");
        if step_status == STEP_STATUS_QUEUED {
            assert_eq!(step_error, None);
        }

        let events = queue.run_events(&run_id, 10).expect("events");
        assert!(events.iter().any(|e| e.event_type == "RUN_RETRY_REQUESTED"));

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn run_events_falls_back_to_empty_payload_for_invalid_json() {
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip admin_api db test: shared database has active external runs");
            return;
        }
        let run_id = format!("admin-events-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue.submit_run(sample_spec(&run_id)).expect("submit run");

        let mut client = queue.connect().expect("connect");
        let now = now_ms();
        client
            .execute(
                "INSERT INTO run_events(run_id, event_type, payload_json, created_at)
                 VALUES ($1, $2, 'not-json', TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
                &[&run_id, &"BROKEN_EVENT", &now],
            )
            .expect("insert broken event");

        let events = queue.run_events(&run_id, 20).expect("events");
        let broken = events
            .iter()
            .find(|e| e.event_type == "BROKEN_EVENT")
            .expect("broken event present");
        assert_eq!(broken.payload, json!({}));

        cleanup_run(&queue, &run_id);
    }
}
