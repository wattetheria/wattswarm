use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use wattswarm_control_plane::round_policy::RoundState;

use super::queue::PgRunQueue;
use super::status::{
    RUN_STATUS_CANCELLED, RUN_STATUS_CANCELLING, RUN_STATUS_CREATED, RUN_STATUS_FAILED,
    RUN_STATUS_FINALIZED, RUN_STATUS_QUEUED, RUN_STATUS_RUNNING, STEP_STATUS_CANCELLED,
    STEP_STATUS_CREATED, STEP_STATUS_FAILED, STEP_STATUS_QUEUED, STEP_STATUS_RETRY_WAIT,
};
use super::types::{RunEvent, RunSubmitSpec, RunView};
use super::utils::now_ms;

impl PgRunQueue {
    pub fn run_id_for_task_id(&self, task_id: &str) -> Result<Option<String>> {
        let mut client = self.connect()?;
        Ok(client
            .query_opt(
                "SELECT run_id
                 FROM run_steps
                 WHERE org_id = $1 AND task_id = $2
                 ORDER BY updated_at DESC, step_id ASC
                 LIMIT 1",
                &[&self.org_id(), &task_id],
            )?
            .map(|row| row.get::<_, String>(0)))
    }

    pub fn submit_run(&self, spec: RunSubmitSpec) -> Result<()> {
        self.validate_submit_spec(&spec)?;
        let now = now_ms();
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let retry_json = serde_json::to_string(&spec.retry)?;
        let agg_json = serde_json::to_string(&spec.aggregation)?;
        let shared_inputs_json = serde_json::to_string(&spec.shared_inputs)?;
        if let Some(round_policy) = spec.round_policy.clone() {
            let round_policy_json = serde_json::to_string(&round_policy.normalized())?;
            let round_state_json = serde_json::to_string(&RoundState::initial())?;
            let market_task_id = spec
                .market_task_id
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_owned();
            let feed_key = spec
                .feed_key
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_owned();
            let scope_hint = spec
                .scope_hint
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_owned();
            tx.execute(
                "INSERT INTO runs(
                    org_id, run_id, status, task_type, shared_inputs_json, retry_policy_json,
                    aggregation_policy_json, market_task_id, feed_key, scope_hint,
                    round_policy_json, round_state_json, created_at, updated_at
                 )
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                         TIMESTAMPTZ 'epoch' + ($13::bigint * INTERVAL '1 millisecond'),
                         TIMESTAMPTZ 'epoch' + ($13::bigint * INTERVAL '1 millisecond'))",
                &[
                    &self.org_id(),
                    &spec.run_id,
                    &RUN_STATUS_CREATED,
                    &spec.task_type,
                    &shared_inputs_json,
                    &retry_json,
                    &agg_json,
                    &market_task_id,
                    &feed_key,
                    &scope_hint,
                    &round_policy_json,
                    &round_state_json,
                    &now,
                ],
            )
            .with_context(|| format!("insert stigmergy run {}", spec.run_id))?;
            self.insert_event_tx(
                &mut tx,
                &spec.run_id,
                "RUN_CREATED",
                &json!({
                    "run_id": spec.run_id,
                    "execution": "stigmergy",
                    "market_task_id": market_task_id,
                    "feed_key": feed_key,
                    "scope_hint": scope_hint
                }),
                now,
            )?;
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "INSERT INTO runs(org_id, run_id, status, task_type, shared_inputs_json, retry_policy_json, aggregation_policy_json, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'))",
            &[
                &self.org_id(),
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
                    org_id, step_id, run_id, agent_id, executor, profile, prompt, weight, priority,
                    status, attempt, max_attempts, next_run_at, created_at, updated_at
                 )
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,$11,TIMESTAMPTZ 'epoch' + ($12::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($12::bigint * INTERVAL '1 millisecond'),TIMESTAMPTZ 'epoch' + ($12::bigint * INTERVAL '1 millisecond'))",
                &[
                    &self.org_id(),
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
        let round_row = tx.query_one(
            "SELECT round_policy_json, round_state_json
             FROM runs
             WHERE org_id = $1 AND run_id = $2
             FOR UPDATE",
            &[&self.org_id(), &run_id],
        )?;
        let round_policy_raw: Option<String> = round_row.get(0);
        if let Some(round_policy_raw) = round_policy_raw {
            let round_policy = serde_json::from_str::<
                wattswarm_control_plane::round_policy::RoundPolicy,
            >(&round_policy_raw)?
            .normalized();
            let round_state_raw: Option<String> = round_row.get(1);
            let mut round_state = round_state_raw
                .as_deref()
                .and_then(|raw| serde_json::from_str::<RoundState>(raw).ok())
                .unwrap_or_else(RoundState::initial);
            if round_state.round_opened_at_ms.is_none() {
                round_state.round_opened_at_ms = Some(now as u64);
            }
            let round_state_json = serde_json::to_string(&round_state)?;
            let round_due_at_ms = round_state
                .round_opened_at_ms
                .map(|opened_at| opened_at.saturating_add(round_policy.round_timeout_ms))
                .map(|value| value as i64);
            tx.execute(
                "UPDATE runs
                 SET status = CASE
                    WHEN status = $3 THEN $4
                    ELSE status
                 END,
                 round_state_json = $5,
                 round_due_at_ms = $6,
                 started_at = COALESCE(started_at, TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')),
                 updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[
                    &self.org_id(),
                    &run_id,
                    &RUN_STATUS_CREATED,
                    &RUN_STATUS_QUEUED,
                    &round_state_json,
                    &round_due_at_ms,
                    &now,
                ],
            )?;
            self.insert_event_tx(
                &mut tx,
                run_id,
                "RUN_KICKOFF",
                &json!({
                    "run_id": run_id,
                    "execution": "stigmergy",
                    "round_index": round_state.round_index,
                    "round_opened_at_ms": round_state.round_opened_at_ms,
                    "round_due_at_ms": round_due_at_ms
                }),
                now,
            )?;
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "UPDATE runs
             SET status = CASE
                WHEN status = $3 THEN $4
                ELSE status
             END,
             started_at = COALESCE(started_at, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')),
             updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND run_id = $2",
            &[
                &self.org_id(),
                &run_id,
                &RUN_STATUS_CREATED,
                &RUN_STATUS_QUEUED,
                &now,
            ],
        )?;
        tx.execute(
            "UPDATE run_steps
             SET status = $3,
                 next_run_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND run_id = $2 AND status IN ($5, $6)",
            &[
                &self.org_id(),
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
                WHEN status IN ($3,$4,$5) THEN $6
                ELSE status
             END,
             updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND run_id = $2",
            &[
                &self.org_id(),
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
             SET status = $3, lease_id = NULL, lease_owner = NULL, lease_until = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'), finished_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND run_id = $2 AND status IN ($5, $6, $7)",
            &[
                &self.org_id(),
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
        let round_row = tx.query_one(
            "SELECT round_policy_json, round_state_json
             FROM runs
             WHERE org_id = $1 AND run_id = $2
             FOR UPDATE",
            &[&self.org_id(), &run_id],
        )?;
        let round_policy_raw: Option<String> = round_row.get(0);
        if let Some(round_policy_raw) = round_policy_raw {
            let round_policy = serde_json::from_str::<
                wattswarm_control_plane::round_policy::RoundPolicy,
            >(&round_policy_raw)?
            .normalized();
            let round_state_raw: Option<String> = round_row.get(1);
            let mut round_state = round_state_raw
                .as_deref()
                .and_then(|raw| serde_json::from_str::<RoundState>(raw).ok())
                .unwrap_or_else(RoundState::initial);
            if round_state.round_opened_at_ms.is_some()
                && round_state.round_index < round_policy.max_rounds
            {
                round_state.round_index = round_state.round_index.saturating_add(1);
            }
            round_state.round_opened_at_ms = Some(now as u64);
            let round_due_at_ms = (now as u64).saturating_add(round_policy.round_timeout_ms) as i64;
            let round_state_json = serde_json::to_string(&round_state)?;
            tx.execute(
                "UPDATE runs
                 SET status = $3,
                     result_json = NULL,
                     error_text = NULL,
                     finished_at = NULL,
                     round_state_json = $4,
                     round_due_at_ms = $5,
                     updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[
                    &self.org_id(),
                    &run_id,
                    &RUN_STATUS_QUEUED,
                    &round_state_json,
                    &round_due_at_ms,
                    &now,
                ],
            )?;
            self.insert_event_tx(
                &mut tx,
                run_id,
                "RUN_RETRY_REQUESTED",
                &json!({
                    "run_id": run_id,
                    "execution": "stigmergy",
                    "round_index": round_state.round_index,
                    "round_opened_at_ms": round_state.round_opened_at_ms,
                    "round_due_at_ms": round_due_at_ms,
                    "steps_requeued": 0
                }),
                now,
            )?;
            tx.commit()?;
            return Ok(());
        }

        let moved = tx.execute(
            "UPDATE run_steps
             SET status = $3,
                 next_run_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                 lease_id = NULL,
                 lease_owner = NULL,
                 lease_until = NULL,
                 error_text = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                 finished_at = NULL
             WHERE org_id = $1 AND run_id = $2 AND status IN ($5, $6)",
            &[
                &self.org_id(),
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
                 SET status = $3, error_text = NULL, result_json = NULL, finished_at = NULL, updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[&self.org_id(), &run_id, &RUN_STATUS_QUEUED, &now],
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
                 FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&self.org_id(), &run_id],
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

    pub fn list_runs(&self, limit: i64) -> Result<Vec<RunView>> {
        let mut client = self.connect()?;
        let rows = client.query(
            "SELECT run_id, status, task_type,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT,
                    CASE WHEN started_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM started_at) * 1000)::BIGINT END,
                    CASE WHEN finished_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM finished_at) * 1000)::BIGINT END
             FROM runs
             WHERE org_id = $1
             ORDER BY updated_at DESC, run_id DESC
             LIMIT $2",
            &[&self.org_id(), &limit.max(1)],
        )?;
        let mut views = Vec::with_capacity(rows.len());
        for row in rows {
            let run_id = row.get::<_, String>(0);
            let counts = self.step_counts(&mut client, &run_id)?;
            views.push(RunView {
                run_id,
                status: row.get::<_, String>(1),
                task_type: row.get::<_, String>(2),
                created_at: row.get::<_, i64>(3),
                updated_at: row.get::<_, i64>(4),
                started_at: row.get::<_, Option<i64>>(5),
                finished_at: row.get::<_, Option<i64>>(6),
                counts,
            });
        }
        Ok(views)
    }

    pub fn run_result(&self, run_id: &str) -> Result<Value> {
        let mut client = self.connect()?;
        let row = client
            .query_opt(
                "SELECT status, result_json, error_text,
                        CASE WHEN finished_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM finished_at) * 1000)::BIGINT END
                 FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&self.org_id(), &run_id],
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
             WHERE org_id = $1 AND run_id = $2
             ORDER BY id DESC
             LIMIT $3",
            &[&self.org_id(), &run_id, &limit],
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
    use super::super::StigmergyCompletionSource;
    use super::super::status::{
        RUN_STATUS_FAILED, RUN_STATUS_FINALIZED, RUN_STATUS_QUEUED, STEP_STATUS_CANCELLED,
        STEP_STATUS_CREATED, STEP_STATUS_FAILED, STEP_STATUS_QUEUED,
    };
    use super::super::types::{AggregationPolicy, RetryPolicy, RunAgentSpec, RunSubmitSpec};
    use super::*;
    use serde_json::json;
    use uuid::Uuid;
    use wattswarm_control_plane::round_policy::RoundPolicy;
    const TEST_SCHEMA: &str = "test";
    const TEST_ORG_ID: &str = "local:test-admin:bootstrap";
    const TEST_DB_LOCK_KEY: i64 = 1_987_654_321;

    struct DbTestLock {
        client: postgres::Client,
    }

    impl DbTestLock {
        fn acquire() -> Option<Self> {
            let mut client = postgres::Client::connect(&test_pg_url(), postgres::NoTls).ok()?;
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

    fn reset_test_schema_or_skip() -> bool {
        let Ok(mut client) = postgres::Client::connect(&test_pg_url(), postgres::NoTls) else {
            eprintln!("skip run-queue admin_api tests: postgres not reachable");
            return false;
        };
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE;
                 CREATE SCHEMA {TEST_SCHEMA};"
            ))
            .is_ok()
    }

    fn queue_or_skip() -> Option<PgRunQueue> {
        let queue = PgRunQueue::with_schema(test_pg_url(), TEST_SCHEMA).for_org(TEST_ORG_ID);
        if queue.connect().is_err() {
            eprintln!("skip run-queue admin_api tests: postgres not reachable");
            return None;
        }
        Some(queue)
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
            market_task_id: None,
            feed_key: None,
            scope_hint: None,
            round_policy: None,
            retry: RetryPolicy::default(),
            aggregation: AggregationPolicy::default(),
        }
    }

    fn sample_stigmergy_spec(run_id: &str, task_id: &str) -> RunSubmitSpec {
        RunSubmitSpec {
            run_id: run_id.to_owned(),
            task_type: "open_task".to_owned(),
            shared_inputs: json!({"prompt":"decide"}),
            agents: vec![],
            market_task_id: Some(task_id.to_owned()),
            feed_key: Some("tasks.open".to_owned()),
            scope_hint: Some(format!("group:{task_id}")),
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

    fn cleanup_run(queue: &PgRunQueue, run_id: &str) {
        if let Ok(mut client) = queue.connect() {
            let _ = client.execute(
                "DELETE FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id],
            );
        }
    }

    #[test]
    fn submit_kickoff_cancel_and_query_views() {
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
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
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
        let run_id = format!("admin-retry-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue.submit_run(sample_spec(&run_id)).expect("submit run");

        let mut client = queue.connect().expect("connect");
        let now = now_ms();
        client
            .execute(
                "UPDATE run_steps
                 SET status = $3,
                     error_text = 'failed',
                     finished_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                     updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id, &STEP_STATUS_FAILED, &now],
            )
            .expect("mark step failed");
        client
            .execute(
                "UPDATE runs
                 SET status = $3,
                     error_text = 'failed',
                     finished_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                     updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id, &RUN_STATUS_FAILED, &now],
            )
            .expect("mark run failed");

        queue.retry_run(&run_id).expect("retry run");
        let view = queue.run_view(&run_id).expect("view after retry");
        assert!(view.status == RUN_STATUS_QUEUED || view.status == RUN_STATUS_RUNNING);
        assert!(view.counts.queued >= 0);
        assert_eq!(view.counts.failed, 0);

        let step_row = client
            .query_one(
                "SELECT status, error_text FROM run_steps WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id],
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
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
        let run_id = format!("admin-events-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue.submit_run(sample_spec(&run_id)).expect("submit run");

        let mut client = queue.connect().expect("connect");
        let now = now_ms();
        client
            .execute(
                "INSERT INTO run_events(org_id, run_id, event_type, payload_json, created_at)
                 VALUES ($1, $2, $3, 'not-json', TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))",
                &[&queue.org_id(), &run_id, &"BROKEN_EVENT", &now],
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

    #[test]
    fn stigmergy_submit_and_kickoff_start_collection_without_queueing_steps() {
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
        let run_id = format!("admin-stigmergy-kickoff-{}", Uuid::new_v4().simple());
        let task_id = format!("task-open-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue
            .submit_run(sample_stigmergy_spec(&run_id, &task_id))
            .expect("submit stigmergy run");

        let mut client = queue.connect().expect("connect");
        let row = client
            .query_one(
                "SELECT market_task_id, round_state_json, round_due_at_ms
                 FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id],
            )
            .expect("run row");
        assert_eq!(
            row.get::<_, Option<String>>(0).as_deref(),
            Some(task_id.as_str())
        );
        let round_state: serde_json::Value =
            serde_json::from_str(&row.get::<_, String>(1)).expect("round state json");
        assert_eq!(round_state["round_index"], 1);
        assert!(round_state["round_opened_at_ms"].is_null());
        assert_eq!(row.get::<_, Option<i64>>(2), None);

        assert_eq!(
            queue
                .record_stigmergy_claim(&task_id, "agent-a", "node-a", "exec-a")
                .expect("record claim"),
            1
        );
        let (observed, winning, required) = queue
            .debug_stigmergy_round_counts(&run_id, &task_id)
            .expect("round counts");
        assert_eq!((observed, winning, required), (1, 0, 2));
        queue.kickoff_run(&run_id).expect("kickoff stigmergy");
        let step_status: String = client
            .query_one(
                "SELECT status FROM run_steps WHERE org_id = $1 AND run_id = $2 AND agent_id = 'agent-a'",
                &[&queue.org_id(), &run_id],
            )
            .expect("step row")
            .get(0);
        assert_eq!(step_status, STEP_STATUS_CREATED);
        let due_after: Option<i64> = client
            .query_one(
                "SELECT round_due_at_ms FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id],
            )
            .expect("run due")
            .get(0);
        assert!(due_after.is_some());

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn stigmergy_completion_requires_matching_claim() {
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
        let run_id = format!("admin-stigmergy-no-claim-{}", Uuid::new_v4().simple());
        let task_id = format!("task-open-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue
            .submit_run(sample_stigmergy_spec(&run_id, &task_id))
            .expect("submit stigmergy run");
        queue.kickoff_run(&run_id).expect("kickoff stigmergy");
        let recorded = queue
            .record_stigmergy_completion(
                &task_id,
                "agent-a",
                "node-a",
                "exec-a",
                &json!({"decision":"support"}),
                StigmergyCompletionSource::TaskCompleted,
            )
            .expect("completion without claim is ignored");
        assert_eq!(recorded, 0);
        let view = queue.run_view(&run_id).expect("view");
        assert_eq!(view.counts.succeeded, 0);
        assert_eq!(
            queue
                .record_stigmergy_claim(&task_id, "agent-a", "node-a", "exec-a")
                .expect("claim"),
            1
        );
        let mismatched = queue
            .record_stigmergy_completion(
                &task_id,
                "agent-a",
                "node-a",
                "exec-b",
                &json!({"decision":"support"}),
                StigmergyCompletionSource::TaskCompleted,
            )
            .expect("mismatched completion is ignored");
        assert_eq!(mismatched, 0);
        let mut client = queue.connect().expect("connect");
        let step_status: String = client
            .query_one(
                "SELECT status FROM run_steps WHERE org_id = $1 AND run_id = $2 AND agent_id = 'agent-a'",
                &[&queue.org_id(), &run_id],
            )
            .expect("claimed step")
            .get(0);
        assert_eq!(step_status, STEP_STATUS_CREATED);
        let events = queue.run_events(&run_id, 20).expect("events");
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "STIGMERGY_CONTRIBUTION_IGNORED")
        );

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn stigmergy_threshold_completion_finalizes_once() {
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
        let run_id = format!("admin-stigmergy-finalize-{}", Uuid::new_v4().simple());
        let task_id = format!("task-open-{}", Uuid::new_v4().simple());
        cleanup_run(&queue, &run_id);
        queue
            .submit_run(sample_stigmergy_spec(&run_id, &task_id))
            .expect("submit stigmergy run");
        queue.kickoff_run(&run_id).expect("kickoff stigmergy");
        assert_eq!(
            queue
                .record_stigmergy_claim(&task_id, "agent-c", "node-agent-c", "exec-agent-c")
                .expect("claim incomplete participant"),
            1
        );

        for agent in ["agent-a", "agent-b"] {
            let node_id = format!("node-{agent}");
            let exec_id = format!("exec-{agent}");
            assert_eq!(
                queue
                    .record_stigmergy_claim(&task_id, agent, &node_id, &exec_id)
                    .expect("claim"),
                1
            );
            queue
                .record_stigmergy_completion(
                    &task_id,
                    agent,
                    &node_id,
                    &exec_id,
                    &json!({"decision":"support","answer":"yes"}),
                    StigmergyCompletionSource::TaskCompleted,
                )
                .expect("completion");
        }

        let result = queue.run_result(&run_id).expect("result");
        assert_eq!(result["status"], RUN_STATUS_FINALIZED);
        assert_eq!(result["result"]["aggregation"]["final_decision"], "SUPPORT");
        let mut client = queue.connect().expect("connect");
        let incomplete_status: String = client
            .query_one(
                "SELECT status FROM run_steps WHERE org_id = $1 AND run_id = $2 AND agent_id = 'agent-c'",
                &[&queue.org_id(), &run_id],
            )
            .expect("incomplete participant step")
            .get(0);
        assert_eq!(incomplete_status, STEP_STATUS_CANCELLED);
        let duplicate = queue
            .record_stigmergy_completion(
                &task_id,
                "agent-a",
                "node-agent-a",
                "exec-agent-a",
                &json!({"decision":"support","answer":"yes"}),
                StigmergyCompletionSource::TaskCompleted,
            )
            .expect("duplicate completion ignored");
        assert_eq!(duplicate, 0);

        cleanup_run(&queue, &run_id);
    }

    #[test]
    fn stigmergy_timeout_opens_next_round_then_fallback_finalizes() {
        let Some(_db_lock) = DbTestLock::acquire() else {
            return;
        };
        if !reset_test_schema_or_skip() {
            return;
        }
        let Some(queue) = queue_or_skip() else {
            return;
        };
        queue.init_schema().expect("init schema");
        let run_id = format!("admin-stigmergy-timeout-{}", Uuid::new_v4().simple());
        let task_id = format!("task-open-{}", Uuid::new_v4().simple());
        let mut spec = sample_stigmergy_spec(&run_id, &task_id);
        spec.round_policy = Some(RoundPolicy {
            min_participants: 2,
            threshold_percent: 100,
            round_timeout_ms: 1,
            max_rounds: 2,
            fallback_decision: Some("abstain".to_owned()),
        });
        cleanup_run(&queue, &run_id);
        queue.submit_run(spec).expect("submit stigmergy run");
        queue.kickoff_run(&run_id).expect("kickoff stigmergy");

        let first_due = now_ms().saturating_add(10_000) as u64;
        assert_eq!(
            queue
                .evaluate_open_stigmergy_rounds(first_due)
                .expect("first timeout"),
            1
        );
        let mut client = queue.connect().expect("connect");
        let state_raw: String = client
            .query_one(
                "SELECT round_state_json FROM runs WHERE org_id = $1 AND run_id = $2",
                &[&queue.org_id(), &run_id],
            )
            .expect("round state")
            .get(0);
        let state: serde_json::Value = serde_json::from_str(&state_raw).expect("state json");
        assert_eq!(state["round_index"], 2);

        assert_eq!(
            queue
                .evaluate_open_stigmergy_rounds(first_due.saturating_add(10))
                .expect("second timeout"),
            1
        );
        let result = queue.run_result(&run_id).expect("result");
        assert_eq!(result["status"], RUN_STATUS_FINALIZED);
        assert_eq!(result["result"]["aggregation"]["final_decision"], "abstain");

        cleanup_run(&queue, &run_id);
    }
}
