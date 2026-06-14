use anyhow::Result;
use postgres::Transaction;
use serde_json::{Value, json};
use std::collections::HashMap;
#[cfg(test)]
use wattswarm_control_plane::round_policy::required_count_for_observed;
use wattswarm_control_plane::round_policy::{
    RoundDecision, RoundPolicy, RoundState, evaluate_round_policy,
};

use super::aggregation::{AggregationNextAction, build_run_summary_tx};
use super::queue::PgRunQueue;
use super::status::{
    RUN_STATUS_CANCELLED, RUN_STATUS_CANCELLING, RUN_STATUS_CREATED, RUN_STATUS_FAILED,
    RUN_STATUS_FINALIZED, RUN_STATUS_QUEUED, RUN_STATUS_RUNNING, STEP_STATUS_CANCELLED,
    STEP_STATUS_CREATED, STEP_STATUS_FAILED, STEP_STATUS_SUCCEEDED,
};
use super::utils::step_counts_tx;

pub const STIGMERGY_EXECUTOR: &str = "stigmergy";
pub const STIGMERGY_PROFILE: &str = "market";

#[derive(Debug, Clone, Copy)]
pub enum StigmergyCompletionSource {
    TaskCompleted,
    CandidateProposed,
}

impl StigmergyCompletionSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::TaskCompleted => "stigmergy_task_completed",
            Self::CandidateProposed => "stigmergy_candidate_proposed",
        }
    }
}

impl PgRunQueue {
    pub fn record_stigmergy_claim(
        &self,
        market_task_id: &str,
        agent_id: &str,
        author_node_id: &str,
        execution_id: &str,
    ) -> Result<u64> {
        let now = chrono::Utc::now().timestamp_millis().max(0);
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let runs = self.lock_open_stigmergy_runs_for_task_tx(&mut tx, market_task_id)?;
        let mut recorded = 0_u64;
        for run_id in runs {
            let step_id = format!("{run_id}:{agent_id}");
            let result_json = json!({
                "task_id": market_task_id,
                "execution_id": execution_id,
                "author_node_id": author_node_id,
                "source": "stigmergy_task_market_claim"
            })
            .to_string();
            let updated = tx.execute(
                "INSERT INTO run_steps(
                    org_id, step_id, run_id, agent_id, executor, profile, prompt, weight, priority,
                    status, attempt, max_attempts, next_run_at, task_id, result_json, created_at, updated_at
                 )
                 VALUES ($1,$2,$3,$4,$5,$6,'',1.0,0,$7,0,1,
                         TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'),
                         $9,$10,
                         TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'),
                         TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'))
                 ON CONFLICT (org_id, run_id, agent_id) DO UPDATE
                 SET result_json = EXCLUDED.result_json,
                     updated_at = EXCLUDED.updated_at
                 WHERE run_steps.status NOT IN ($11,$12,$13)",
                &[
                    &self.org_id(),
                    &step_id,
                    &run_id,
                    &agent_id,
                    &STIGMERGY_EXECUTOR,
                    &STIGMERGY_PROFILE,
                    &STEP_STATUS_CREATED,
                    &now,
                    &market_task_id,
                    &result_json,
                    &STEP_STATUS_SUCCEEDED,
                    &STEP_STATUS_FAILED,
                    &STEP_STATUS_CANCELLED,
                ],
            )?;
            if updated == 0 {
                continue;
            }
            recorded = recorded.saturating_add(1);
            self.insert_event_tx(
                &mut tx,
                &run_id,
                "STIGMERGY_PARTICIPANT_REGISTERED",
                &json!({
                    "run_id": run_id,
                    "task_id": market_task_id,
                    "agent_id": agent_id,
                    "author_node_id": author_node_id,
                    "execution_id": execution_id
                }),
                now,
            )?;
        }
        tx.commit()?;
        Ok(recorded)
    }

    pub fn record_stigmergy_completion(
        &self,
        market_task_id: &str,
        agent_id: &str,
        author_node_id: &str,
        execution_id: &str,
        output: &Value,
        source: StigmergyCompletionSource,
    ) -> Result<u64> {
        let now = chrono::Utc::now().timestamp_millis().max(0);
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let runs = self.lock_open_stigmergy_runs_for_task_tx(&mut tx, market_task_id)?;
        let mut recorded = 0_u64;
        for run_id in runs {
            let step_id = format!("{run_id}:{agent_id}");
            let result_json = json!({
                "task_id": market_task_id,
                "execution_id": execution_id,
                "author_node_id": author_node_id,
                "source": source.as_str(),
                "candidate_output": output,
                "result": output
            })
            .to_string();
            let updated = tx.execute(
                "UPDATE run_steps
                 SET status = $4,
                     task_id = $5,
                     result_json = $6,
                     lease_id = NULL,
                     lease_owner = NULL,
                     lease_until = NULL,
                     updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'),
                     finished_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1
                   AND step_id = $2
                   AND run_id = $3
                   AND status NOT IN ($8,$9,$10)
                   AND result_json::jsonb ->> 'execution_id' = $11
                   AND result_json::jsonb ->> 'author_node_id' = $12",
                &[
                    &self.org_id(),
                    &step_id,
                    &run_id,
                    &STEP_STATUS_SUCCEEDED,
                    &market_task_id,
                    &result_json,
                    &now,
                    &STEP_STATUS_SUCCEEDED,
                    &STEP_STATUS_FAILED,
                    &STEP_STATUS_CANCELLED,
                    &execution_id,
                    &author_node_id,
                ],
            )?;
            if updated == 0 {
                self.insert_event_tx(
                    &mut tx,
                    &run_id,
                    "STIGMERGY_CONTRIBUTION_IGNORED",
                    &json!({
                        "run_id": run_id,
                        "task_id": market_task_id,
                        "agent_id": agent_id,
                        "author_node_id": author_node_id,
                        "execution_id": execution_id,
                        "source": source.as_str(),
                        "reason": "missing_or_mismatched_claim"
                    }),
                    now,
                )?;
                continue;
            }
            tx.execute(
                "UPDATE runs
                 SET status = CASE WHEN status IN ($3,$4) THEN $5 ELSE status END,
                     updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
                 WHERE org_id = $1 AND run_id = $2",
                &[
                    &self.org_id(),
                    &run_id,
                    &RUN_STATUS_CREATED,
                    &RUN_STATUS_QUEUED,
                    &RUN_STATUS_RUNNING,
                    &now,
                ],
            )?;
            recorded = recorded.saturating_add(1);
            self.insert_event_tx(
                &mut tx,
                &run_id,
                "STIGMERGY_CONTRIBUTION_COMPLETED",
                &json!({
                    "run_id": run_id,
                    "task_id": market_task_id,
                    "agent_id": agent_id,
                    "author_node_id": author_node_id,
                    "execution_id": execution_id,
                    "source": source.as_str()
                }),
                now,
            )?;
            self.evaluate_stigmergy_round_tx(&mut tx, &run_id, now as u64)?;
        }
        tx.commit()?;
        Ok(recorded)
    }

    pub fn evaluate_open_stigmergy_rounds(&self, now_ms: u64) -> Result<u64> {
        let mut client = self.connect()?;
        let rows = client.query(
            "SELECT run_id
             FROM runs
             WHERE org_id = $1
               AND round_policy_json IS NOT NULL
               AND round_due_at_ms IS NOT NULL
               AND round_due_at_ms <= $2
               AND status IN ($3,$4,$5)
             ORDER BY round_due_at_ms ASC, run_id ASC",
            &[
                &self.org_id(),
                &(now_ms as i64),
                &RUN_STATUS_CREATED,
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
            ],
        )?;
        let run_ids = rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>();
        let mut transitioned = 0_u64;
        for run_id in run_ids {
            let mut tx = client.transaction()?;
            if self.evaluate_stigmergy_round_tx(&mut tx, &run_id, now_ms)? {
                transitioned = transitioned.saturating_add(1);
            }
            tx.commit()?;
        }
        Ok(transitioned)
    }

    fn lock_open_stigmergy_runs_for_task_tx(
        &self,
        tx: &mut Transaction<'_>,
        market_task_id: &str,
    ) -> Result<Vec<String>> {
        let rows = tx.query(
            "SELECT run_id
             FROM runs
             WHERE org_id = $1
               AND market_task_id = $2
               AND round_policy_json IS NOT NULL
               AND status NOT IN ($3,$4,$5,$6)
             ORDER BY created_at ASC, run_id ASC
             FOR UPDATE",
            &[
                &self.org_id(),
                &market_task_id,
                &RUN_STATUS_FINALIZED,
                &RUN_STATUS_FAILED,
                &RUN_STATUS_CANCELLED,
                &RUN_STATUS_CANCELLING,
            ],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    }

    pub(crate) fn evaluate_stigmergy_round_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let Some(run) = self.load_stigmergy_run_for_update_tx(tx, run_id)? else {
            return Ok(false);
        };
        if run.round_state.round_opened_at_ms.is_none() {
            return Ok(false);
        }
        let observed_count = self.observed_participant_count_tx(tx, run_id, &run.market_task_id)?;
        let winning_count = self.winning_contribution_count_tx(tx, run_id, &run.market_task_id)?;
        let decision = evaluate_round_policy(
            &run.round_policy,
            &run.round_state,
            observed_count,
            winning_count,
            now_ms,
        );
        match decision {
            RoundDecision::ThresholdMet => {
                self.threshold_finalize_or_reexplore_tx(tx, run_id, &run, now_ms)
            }
            RoundDecision::OpenNextRound => {
                self.open_next_stigmergy_round_tx(tx, run_id, &run, now_ms, None)
            }
            RoundDecision::FallbackFinalize => {
                self.fallback_finalize_stigmergy_tx(tx, run_id, &run, now_ms)
            }
            RoundDecision::KeepWaiting => Ok(false),
        }
    }

    fn threshold_finalize_or_reexplore_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        run: &LoadedStigmergyRun,
        now_ms: u64,
    ) -> Result<bool> {
        let counts = step_counts_tx(tx, self.org_id(), run_id)?;
        let summary =
            build_run_summary_tx(tx, self.org_id(), run_id, RUN_STATUS_FINALIZED, &counts)?;
        if let AggregationNextAction::ReExplore(directive) = &summary.action
            && run.round_state.round_index < run.round_policy.max_rounds
        {
            self.insert_event_tx(
                tx,
                run_id,
                &directive.event_type,
                &summary.event_payload,
                now_ms as i64,
            )?;
            return self.open_next_stigmergy_round_tx(
                tx,
                run_id,
                run,
                now_ms,
                Some("aggregation_reexplore"),
            );
        }
        self.cancel_incomplete_stigmergy_steps_tx(tx, run_id, &run.market_task_id, now_ms)?;
        let final_counts = step_counts_tx(tx, self.org_id(), run_id)?;
        let final_summary = build_run_summary_tx(
            tx,
            self.org_id(),
            run_id,
            RUN_STATUS_FINALIZED,
            &final_counts,
        )?;
        self.write_stigmergy_final_result_tx(
            tx,
            run_id,
            &final_summary.result_payload,
            &final_summary.event_payload,
            now_ms,
        )
    }

    fn fallback_finalize_stigmergy_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        run: &LoadedStigmergyRun,
        now_ms: u64,
    ) -> Result<bool> {
        self.cancel_incomplete_stigmergy_steps_tx(tx, run_id, &run.market_task_id, now_ms)?;
        let counts = step_counts_tx(tx, self.org_id(), run_id)?;
        let result_payload = if let Some(decision) = run.round_policy.fallback_decision.clone() {
            json!({
                "run_id": run_id,
                "status": RUN_STATUS_FINALIZED,
                "counts": counts,
                "aggregation": {
                    "mode": "fallback",
                    "final_decision": decision,
                    "final_answer": null,
                    "resolution_path": "fallback",
                    "fallback_applied": true
                }
            })
        } else {
            build_run_summary_tx(tx, self.org_id(), run_id, RUN_STATUS_FINALIZED, &counts)?
                .result_payload
        };
        let event_payload = json!({
            "run_id": run_id,
            "status": RUN_STATUS_FINALIZED,
            "counts": counts,
            "round_index": run.round_state.round_index,
            "fallback_applied": true,
            "fallback_decision": run.round_policy.fallback_decision
        });
        self.write_stigmergy_final_result_tx(tx, run_id, &result_payload, &event_payload, now_ms)
    }

    fn open_next_stigmergy_round_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        run: &LoadedStigmergyRun,
        now_ms: u64,
        reason: Option<&str>,
    ) -> Result<bool> {
        let next_state = RoundState {
            round_index: run.round_state.round_index.saturating_add(1),
            round_opened_at_ms: Some(now_ms),
        };
        let round_due_at_ms = now_ms.saturating_add(run.round_policy.round_timeout_ms);
        let state_json = serde_json::to_string(&next_state)?;
        let updated = tx.execute(
            "UPDATE runs
             SET status = $3,
                 result_json = NULL,
                 error_text = NULL,
                 finished_at = NULL,
                 round_state_json = $4,
                 round_due_at_ms = $5,
                 updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1
               AND run_id = $2
               AND round_policy_json IS NOT NULL
               AND status IN ($7,$8,$9)",
            &[
                &self.org_id(),
                &run_id,
                &RUN_STATUS_RUNNING,
                &state_json,
                &(round_due_at_ms as i64),
                &(now_ms as i64),
                &RUN_STATUS_CREATED,
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
            ],
        )?;
        if updated == 0 {
            return Ok(false);
        }
        self.insert_event_tx(
            tx,
            run_id,
            "STIGMERGY_ROUND_OPENED",
            &json!({
                "run_id": run_id,
                "previous_round_index": run.round_state.round_index,
                "round_index": next_state.round_index,
                "round_opened_at_ms": next_state.round_opened_at_ms,
                "round_due_at_ms": round_due_at_ms,
                "reason": reason.unwrap_or("round_timeout")
            }),
            now_ms as i64,
        )?;
        Ok(true)
    }

    fn write_stigmergy_final_result_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        result_payload: &Value,
        event_payload: &Value,
        now_ms: u64,
    ) -> Result<bool> {
        let updated = tx.execute(
            "UPDATE runs
             SET status = $3,
                 result_json = $4,
                 round_due_at_ms = NULL,
                 finished_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                 updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1
               AND run_id = $2
               AND round_policy_json IS NOT NULL
               AND status IN ($6,$7,$8)",
            &[
                &self.org_id(),
                &run_id,
                &RUN_STATUS_FINALIZED,
                &result_payload.to_string(),
                &(now_ms as i64),
                &RUN_STATUS_CREATED,
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
            ],
        )?;
        if updated == 0 {
            return Ok(false);
        }
        self.insert_event_tx(tx, run_id, "RUN_FINALIZED", event_payload, now_ms as i64)?;
        Ok(true)
    }

    fn cancel_incomplete_stigmergy_steps_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        market_task_id: &str,
        now_ms: u64,
    ) -> Result<u64> {
        Ok(tx.execute(
            "UPDATE run_steps
             SET status = $5,
                 updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'),
                 finished_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1
               AND run_id = $2
               AND task_id = $3
               AND executor = $4
               AND status = $7",
            &[
                &self.org_id(),
                &run_id,
                &market_task_id,
                &STIGMERGY_EXECUTOR,
                &STEP_STATUS_CANCELLED,
                &(now_ms as i64),
                &STEP_STATUS_CREATED,
            ],
        )?)
    }

    fn load_stigmergy_run_for_update_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
    ) -> Result<Option<LoadedStigmergyRun>> {
        let Some(row) = tx.query_opt(
            "SELECT market_task_id, round_policy_json, round_state_json
             FROM runs
             WHERE org_id = $1
               AND run_id = $2
               AND round_policy_json IS NOT NULL
               AND status IN ($3,$4,$5)
             FOR UPDATE",
            &[
                &self.org_id(),
                &run_id,
                &RUN_STATUS_CREATED,
                &RUN_STATUS_QUEUED,
                &RUN_STATUS_RUNNING,
            ],
        )?
        else {
            return Ok(None);
        };
        let market_task_id: Option<String> = row.get(0);
        let round_policy_raw: String = row.get(1);
        let round_state_raw: Option<String> = row.get(2);
        let round_policy = serde_json::from_str::<RoundPolicy>(&round_policy_raw)?.normalized();
        let round_state = round_state_raw
            .as_deref()
            .and_then(|raw| serde_json::from_str::<RoundState>(raw).ok())
            .unwrap_or_else(RoundState::initial);
        Ok(Some(LoadedStigmergyRun {
            market_task_id: market_task_id.unwrap_or_default(),
            round_policy,
            round_state,
        }))
    }

    fn observed_participant_count_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        market_task_id: &str,
    ) -> Result<usize> {
        let row = tx.query_one(
            "SELECT COUNT(DISTINCT agent_id)
             FROM run_steps
             WHERE org_id = $1
               AND run_id = $2
               AND task_id = $3
               AND executor = $4
               AND profile = $5
               AND status IN ($6,$7)",
            &[
                &self.org_id(),
                &run_id,
                &market_task_id,
                &STIGMERGY_EXECUTOR,
                &STIGMERGY_PROFILE,
                &STEP_STATUS_CREATED,
                &STEP_STATUS_SUCCEEDED,
            ],
        )?;
        let count = row.get::<_, i64>(0).max(0) as usize;
        Ok(count)
    }

    fn winning_contribution_count_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        market_task_id: &str,
    ) -> Result<usize> {
        let rows = tx.query(
            "SELECT result_json
             FROM run_steps
             WHERE org_id = $1
               AND run_id = $2
               AND task_id = $3
               AND executor = $4
               AND profile = $5
               AND status = $6",
            &[
                &self.org_id(),
                &run_id,
                &market_task_id,
                &STIGMERGY_EXECUTOR,
                &STIGMERGY_PROFILE,
                &STEP_STATUS_SUCCEEDED,
            ],
        )?;
        let mut counts: HashMap<String, usize> = HashMap::new();
        for row in rows {
            let raw: Option<String> = row.get(0);
            let value = raw
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .unwrap_or(Value::Null);
            if let Some(bucket) = contribution_bucket(&value) {
                *counts.entry(bucket).or_insert(0) += 1;
            }
        }
        Ok(counts.values().copied().max().unwrap_or(0))
    }

    #[cfg(test)]
    pub(crate) fn debug_stigmergy_round_counts(
        &self,
        run_id: &str,
        market_task_id: &str,
    ) -> Result<(usize, usize, usize)> {
        let mut client = self.connect()?;
        let mut tx = client.transaction()?;
        let run = self
            .load_stigmergy_run_for_update_tx(&mut tx, run_id)?
            .expect("stigmergy run");
        let observed = self.observed_participant_count_tx(&mut tx, run_id, market_task_id)?;
        let winning = self.winning_contribution_count_tx(&mut tx, run_id, market_task_id)?;
        let required = required_count_for_observed(&run.round_policy, observed);
        tx.commit()?;
        Ok((observed, winning, required))
    }
}

#[derive(Debug, Clone)]
struct LoadedStigmergyRun {
    market_task_id: String,
    round_policy: RoundPolicy,
    round_state: RoundState,
}

fn contribution_bucket(result: &Value) -> Option<String> {
    let conclusion = if let Some(candidate_output) = result.get("candidate_output") {
        candidate_output
    } else if let Some(inner) = result.get("result") {
        inner
    } else {
        result
    };
    text_bucket(conclusion, "decision")
        .map(|value| format!("decision:{value}"))
        .or_else(|| text_bucket(conclusion, "answer").map(|value| format!("answer:{value}")))
}

fn text_bucket(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contribution_bucket_prefers_decision_then_answer() {
        assert_eq!(
            contribution_bucket(&json!({"candidate_output":{"decision":"support","answer":"A"}})),
            Some("decision:SUPPORT".to_owned())
        );
        assert_eq!(
            contribution_bucket(&json!({"candidate_output":{"answer":"A"}})),
            Some("answer:A".to_owned())
        );
        assert_eq!(contribution_bucket(&json!({})), None);
    }
}
