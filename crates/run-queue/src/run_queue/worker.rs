use anyhow::Result;
use postgres::Transaction;
use serde_json::json;
use std::path::Path;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

use crate::control::{RealTaskRunRequest, open_node, run_real_task_flow};
use crate::task_template::sample_contract;
use crate::types::TaskContract;

use super::queue::PgRunQueue;
use super::status::{
    RUN_STATUS_CANCELLING, RUN_STATUS_QUEUED, RUN_STATUS_RUNNING, STEP_STATUS_CANCELLED,
    STEP_STATUS_FAILED, STEP_STATUS_LEASED, STEP_STATUS_QUEUED, STEP_STATUS_RETRY_WAIT,
    STEP_STATUS_SUCCEEDED,
};
use super::types::{ClaimedStep, WorkerOptions};
use super::utils::{build_step_inputs, now_ms, retry_delay_ms};

impl PgRunQueue {
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

        let task_id = format!("run-{}-{}-{}", step.run_id, step.agent_id, step.attempt);
        let run_result = (|| -> Result<serde_json::Value> {
            let mut node = open_node(state_dir, db_path)?;
            let policy_hash = node
                .policy_registry()
                .binding_for("vp.schema_only.v1", json!({}))?
                .policy_hash;
            let mut contract: TaskContract = sample_contract(&task_id, policy_hash);
            contract.task_type = step.task_type.clone();
            contract.inputs = build_step_inputs(&step.shared_inputs, &step.prompt, &step.agent_id);

            run_real_task_flow(
                &mut node,
                state_dir,
                RealTaskRunRequest {
                    executor: step.executor.clone(),
                    profile: step.profile.clone(),
                    task_id: Some(task_id.clone()),
                    task_file: None,
                    task_contract: Some(contract),
                },
            )
        })();

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
                    self.finish_step_retry_wait(&step, &err_text, now_ms().saturating_add(delay_ms))
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

    fn recover_expired_leases_tx(&self, tx: &mut Transaction<'_>, now: i64) -> Result<u64> {
        let recovered = tx.execute(
            "UPDATE run_steps s
             SET status = $1,
                 lease_id = NULL,
                 lease_owner = NULL,
                 lease_until = NULL,
                 next_run_at = TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'),
                 updated_at = TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'),
                 error_text = CASE
                     WHEN s.error_text IS NULL OR s.error_text = '' THEN 'lease expired; recovered'
                     ELSE s.error_text
                 END
             FROM runs r
             WHERE s.run_id = r.run_id
               AND r.status IN ($3, $4, $5)
               AND s.status = $6
               AND s.lease_until IS NOT NULL
               AND s.lease_until < TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')",
            &[
                &STEP_STATUS_RETRY_WAIT,
                &now,
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
                &RUN_STATUS_CANCELLING,
                &STEP_STATUS_LEASED,
            ],
        )?;
        Ok(recovered as u64)
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
        let _recovered = self.recover_expired_leases_tx(&mut tx, now)?;
        let rows = tx.query(
            "SELECT s.step_id, s.run_id, s.agent_id, s.executor, s.profile, s.prompt, s.attempt, s.max_attempts,
                    r.task_type, r.shared_inputs_json, r.retry_policy_json, r.status
             FROM run_steps s
             JOIN runs r ON r.run_id = s.run_id
             WHERE r.status IN ($1, $2, $3)
               AND s.status IN ($4, $5)
               AND s.next_run_at <= TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
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
                 SET status = $2,
                     lease_id = $3,
                     lease_owner = $4,
                     lease_until = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
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
                 next_run_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                 error_text = $5,
                 updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
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
        result: Option<&serde_json::Value>,
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
}

#[cfg(test)]
mod tests {
    use super::super::status::{
        RUN_STATUS_CANCELLED, RUN_STATUS_CANCELLING, RUN_STATUS_RUNNING, STEP_STATUS_CANCELLED,
        STEP_STATUS_LEASED, STEP_STATUS_RETRY_WAIT, STEP_STATUS_SUCCEEDED,
    };
    use super::super::types::{AggregationPolicy, RetryPolicy, RunAgentSpec, RunSubmitSpec};
    use super::*;
    use serde_json::json;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::Duration;
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
            eprintln!("skip run-queue worker tests: postgres not reachable");
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
                priority: 1_000_000,
            }],
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
    }

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn purge_worker_runs(queue: &PgRunQueue) {
        if let Ok(mut client) = queue.connect() {
            let _ = client.execute("DELETE FROM runs WHERE run_id LIKE 'worker-%'", &[]);
        }
    }

    fn cleanup_run(queue: &PgRunQueue, run_id: &str) {
        if let Ok(mut client) = queue.connect() {
            let _ = client.execute("DELETE FROM runs WHERE run_id = $1", &[&run_id]);
        }
    }

    fn prepare_claimed_step(queue: &PgRunQueue, run_id: &str) -> Option<ClaimedStep> {
        queue.submit_run(sample_spec(run_id)).expect("submit");
        queue.kickoff_run(run_id).expect("kickoff");
        for _ in 0..20 {
            let claimed = queue
                .claim_steps("worker-test", 1, 30_000)
                .expect("claim step");
            if let Some(step) = claimed.into_iter().find(|s| s.run_id == run_id) {
                return Some(step);
            }
            thread::sleep(Duration::from_millis(20));
        }
        None
    }

    #[test]
    fn run_worker_once_returns_when_no_work() {
        let _guard = test_guard();
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip worker db test: shared database has active external runs");
            return;
        }
        purge_worker_runs(&queue);
        queue
            .run_worker(
                WorkerOptions {
                    worker_id: "worker-once".to_owned(),
                    concurrency: 1,
                    poll_ms: 5,
                    lease_ms: 1000,
                    once: true,
                },
                Path::new("."),
                Path::new("wattswarm.db"),
            )
            .expect("run worker once");
    }

    #[test]
    fn claim_steps_leases_step_and_promotes_run_to_running() {
        let _guard = test_guard();
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip worker db test: shared database has active external runs");
            return;
        }
        purge_worker_runs(&queue);
        let run_id = format!("worker-claim-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue.submit_run(sample_spec(&run_id)).expect("submit");
        queue.kickoff_run(&run_id).expect("kickoff");

        let mut claimed_step: Option<ClaimedStep> = None;
        for _ in 0..20 {
            let claimed = queue.claim_steps("worker-a", 1, 30_000).expect("claim");
            if let Some(step) = claimed.into_iter().find(|s| s.run_id == run_id) {
                claimed_step = Some(step);
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let Some(first) = claimed_step else {
            eprintln!("skip worker claim test: target run was not claimable in shared env");
            cleanup_run(&queue, &run_id);
            return;
        };
        assert_eq!(first.attempt, 1);

        let mut client = queue.connect().expect("connect");
        let step_row = client
            .query_one(
                "SELECT status, lease_owner, attempt FROM run_steps WHERE step_id = $1",
                &[&first.step_id],
            )
            .expect("step row");
        let step_status: String = step_row.get(0);
        let lease_owner: Option<String> = step_row.get(1);
        let attempt: i32 = step_row.get(2);
        assert_eq!(step_status, STEP_STATUS_LEASED);
        assert_eq!(lease_owner.as_deref(), Some("worker-a"));
        assert_eq!(attempt, 1);

        let run_row = client
            .query_one("SELECT status FROM runs WHERE run_id = $1", &[&run_id])
            .expect("run row");
        let run_status: String = run_row.get(0);
        assert_eq!(run_status, RUN_STATUS_RUNNING);

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn recover_expired_leases_moves_steps_to_retry_wait() {
        let _guard = test_guard();
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip worker db test: shared database has active external runs");
            return;
        }
        purge_worker_runs(&queue);
        let run_id = format!("worker-recover-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        let Some(claimed) = prepare_claimed_step(&queue, &run_id) else {
            eprintln!("skip recover lease test: target run was not claimable in shared env");
            cleanup_run(&queue, &run_id);
            return;
        };

        let mut client = queue.connect().expect("connect");
        client
            .execute(
                "UPDATE run_steps
                 SET lease_until = TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')
                 WHERE step_id = $1",
                &[&claimed.step_id, &0_i64],
            )
            .expect("expire lease");

        let now = now_ms();
        let mut tx = client.transaction().expect("tx");
        let recovered = queue
            .recover_expired_leases_tx(&mut tx, now)
            .expect("recover lease");
        tx.commit().expect("commit");
        assert_eq!(recovered, 1);

        let row = client
            .query_one(
                "SELECT status FROM run_steps WHERE step_id = $1",
                &[&claimed.step_id],
            )
            .expect("step row");
        let status: String = row.get(0);
        assert_eq!(status, STEP_STATUS_RETRY_WAIT);

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn process_step_short_circuits_when_run_is_cancelling() {
        let _guard = test_guard();
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip worker db test: shared database has active external runs");
            return;
        }
        purge_worker_runs(&queue);
        let run_id = format!("worker-cancel-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        let Some(mut claimed) = prepare_claimed_step(&queue, &run_id) else {
            eprintln!("skip cancelling test: target run was not claimable in shared env");
            cleanup_run(&queue, &run_id);
            return;
        };
        let mut client = queue.connect().expect("connect");
        client
            .execute(
                "UPDATE runs SET status = $2 WHERE run_id = $1",
                &[&run_id, &RUN_STATUS_CANCELLING],
            )
            .expect("set run cancelling");
        claimed.run_status = RUN_STATUS_CANCELLING.to_owned();

        queue
            .process_step(claimed.clone(), Path::new("."), Path::new("wattswarm.db"))
            .expect("process cancelling");

        let row = client
            .query_one(
                "SELECT status FROM run_steps WHERE step_id = $1",
                &[&claimed.step_id],
            )
            .expect("step row");
        let status: String = row.get(0);
        assert_eq!(status, STEP_STATUS_CANCELLED);

        let run_result = queue.run_result(&run_id).expect("run result");
        assert_eq!(run_result["status"], RUN_STATUS_CANCELLED);

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn finish_retry_wait_then_success_transitions_and_finalizes_run() {
        let _guard = test_guard();
        let Some(queue) = queue_or_skip() else {
            return;
        };
        let Some(_db_lock) = DbTestLock::acquire(&queue) else {
            return;
        };
        if shared_db_is_busy(&queue) {
            eprintln!("skip worker db test: shared database has active external runs");
            return;
        }
        purge_worker_runs(&queue);
        let run_id = format!("worker-finish-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        let Some(claimed) = prepare_claimed_step(&queue, &run_id) else {
            eprintln!("skip finish test: target run was not claimable in shared env");
            cleanup_run(&queue, &run_id);
            return;
        };

        queue
            .finish_step_retry_wait(&claimed, "temporary error", now_ms().saturating_sub(1))
            .expect("move to retry wait");
        let lease_id = Uuid::new_v4().to_string();
        let mut client = queue.connect().expect("connect");
        client
            .execute(
                "UPDATE run_steps
                 SET status = $2,
                     lease_id = $3,
                     lease_owner = 'worker-b',
                     lease_until = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                     attempt = 2
                 WHERE step_id = $1",
                &[
                    &claimed.step_id,
                    &STEP_STATUS_LEASED,
                    &lease_id,
                    &now_ms().saturating_add(30_000),
                ],
            )
            .expect("set leased again");
        let claimed_again = ClaimedStep {
            attempt: 2,
            lease_id,
            ..claimed
        };

        queue
            .finish_step_terminal(
                &claimed_again,
                STEP_STATUS_SUCCEEDED,
                Some("task-1"),
                Some(&json!({"candidate_output":{"decision":"approve","answer":"ok"}})),
                None,
            )
            .expect("finish terminal");

        let step_row = client
            .query_one(
                "SELECT status FROM run_steps WHERE step_id = $1",
                &[&claimed_again.step_id],
            )
            .expect("step row");
        let step_status: String = step_row.get(0);
        assert_eq!(step_status, STEP_STATUS_SUCCEEDED);

        let run_result = queue.run_result(&run_id).expect("run result");
        assert_eq!(run_result["status"], "FINALIZED");
        assert_eq!(
            run_result["result"]["aggregation"]["final_decision"],
            json!("APPROVE")
        );

        let events = queue.run_events(&run_id, 20).expect("events");
        assert!(events.iter().any(|e| e.event_type == "STEP_RETRY_WAIT"));
        assert!(events.iter().any(|e| e.event_type == "STEP_COMPLETED"));
        assert!(events.iter().any(|e| e.event_type == "RUN_FINALIZED"));

        cleanup_run(&queue, &run_id);
    }
}
