use anyhow::Result;
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
