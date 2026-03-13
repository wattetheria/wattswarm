use anyhow::{Result, anyhow};
use postgres::{Client, NoTls, Transaction};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

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
}

const DEFAULT_RUN_QUEUE_ORG_ID: &str = "bootstrap";

impl PgRunQueue {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            schema: None,
            org_id: Arc::new(DEFAULT_RUN_QUEUE_ORG_ID.to_owned()),
        }
    }

    pub fn with_schema(database_url: impl Into<String>, schema: impl AsRef<str>) -> Self {
        Self {
            database_url: database_url.into(),
            schema: Some(sanitize_ident(schema.as_ref())),
            org_id: Arc::new(DEFAULT_RUN_QUEUE_ORG_ID.to_owned()),
        }
    }

    pub fn for_org(&self, org_id: impl Into<String>) -> Self {
        Self {
            database_url: self.database_url.clone(),
            schema: self.schema.clone(),
            org_id: Arc::new(org_id.into()),
        }
    }

    pub fn org_id(&self) -> &str {
        self.org_id.as_str()
    }

    pub(crate) fn connect(&self) -> Result<Client> {
        let mut client = Client::connect(&self.database_url, NoTls)
            .map_err(|err| anyhow!("connect postgres {}: {err}", self.database_url))?;
        if let Some(schema) = &self.schema {
            client.batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema}, public;"
            ))?;
        }
        Ok(client)
    }

    pub(crate) fn finalize_run_if_terminal_tx(
        &self,
        tx: &mut Transaction<'_>,
        run_id: &str,
        now: i64,
    ) -> Result<()> {
        let status = self
            .run_status_tx(tx, run_id)?
            .ok_or_else(|| anyhow!("run not found: {run_id}"))?;
        let counts = step_counts_tx(tx, self.org_id(), run_id)?;
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
                 SET status = $2,
                     shared_inputs_json = $3,
                     result_json = NULL,
                     error_text = NULL,
                     finished_at = NULL,
                     updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
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
                     SET status = $2,
                         next_run_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                         lease_id = NULL,
                         lease_owner = NULL,
                         lease_until = NULL,
                         task_id = NULL,
                         result_json = NULL,
                         error_text = NULL,
                         finished_at = NULL,
                         updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                     WHERE org_id = $1
                       AND run_id = $2
                       AND status = $4",
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
                     SET status = $2,
                         next_run_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                         lease_id = NULL,
                         lease_owner = NULL,
                         lease_until = NULL,
                         task_id = NULL,
                         result_json = NULL,
                         error_text = NULL,
                         finished_at = NULL,
                         updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                     WHERE org_id = $1
                       AND run_id = $2
                       AND status IN ($4, $5, $6)",
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

    use super::super::types::{
        AggregationPolicy, NullResolverMode, NullTrigger, RetryPolicy, RunAgentSpec, RunSubmitSpec,
        TieResolverMode, TieTrigger,
    };
    use super::super::utils::{build_step_inputs, retry_delay_ms};
    use super::PgRunQueue;

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
}
