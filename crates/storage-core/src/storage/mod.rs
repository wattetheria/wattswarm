use crate::crypto::candidate_hash;
use crate::error::SwarmError;
use crate::params;
use crate::storage::pg::{Connection, OptionalExtension, types::ValueRef};
use crate::types::{Candidate, Event, TaskContract, TaskTerminalState, VerifierResult, VoteChoice};
use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub mod pg;

#[derive(Clone)]
pub struct PgStore {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct LeaseRow {
    pub task_id: String,
    pub role: String,
    pub claimer_node_id: String,
    pub execution_id: String,
    pub lease_until: u64,
}

#[derive(Debug, Clone)]
pub struct VoteCommitRow {
    pub commit_hash: String,
    pub candidate_hash: String,
    pub verifier_result_hash: String,
    pub execution_id: String,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct VoteRevealRow {
    pub task_id: String,
    pub voter_node_id: String,
    pub candidate_id: String,
    pub candidate_hash: String,
    pub vote: VoteChoice,
    pub salt: String,
    pub verifier_result_hash: String,
    pub valid: bool,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct TaskProjectionRow {
    pub epoch: u64,
    pub contract: TaskContract,
    pub terminal_state: TaskTerminalState,
    pub committed_candidate_id: Option<String>,
    pub finalized_candidate_id: Option<String>,
    pub retry_attempt: u32,
}

#[derive(Debug, Clone)]
pub struct VoteCommitMetaRow {
    pub voter_node_id: String,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct TaskSettlementRow {
    pub task_id: String,
    pub epoch: u64,
    pub finalized_at: u64,
    pub window_end_at: u64,
    pub bad_feedback_exists: bool,
    pub bad_feedback_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DecisionMemoryHitRow {
    pub task_id: String,
    pub epoch: u64,
    pub final_commit_hash: String,
    pub winning_candidate_hash: String,
    pub result_summary: serde_json::Value,
    pub reason_codes: Vec<u16>,
    pub input_digest: String,
    pub output_schema_digest: String,
    pub policy_id: String,
    pub policy_params_digest: String,
    pub deprecated_as_exact: bool,
    pub finalized_at: u64,
    pub confidence_hint: f64,
}

#[derive(Debug, Clone)]
pub struct AdvisoryStateRow {
    pub advisory_id: String,
    pub policy_id: String,
    pub suggested_policy_hash: String,
    pub status: String,
    pub created_at: u64,
    pub approved_by: Option<String>,
    pub approved_at: Option<u64>,
    pub applied_policy_hash: Option<String>,
    pub applied_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RuntimeMetricObservation<'a> {
    pub runtime_id: &'a str,
    pub profile_id: &'a str,
    pub task_type: &'a str,
    pub window_start: u64,
    pub window_end: u64,
    pub finalized: bool,
    pub timeout: bool,
    pub crash: bool,
    pub invalid_output: bool,
    pub latency_ms: u64,
    pub cost_units: u64,
    pub reject_reason_codes: &'a [u16],
}

#[derive(Debug, Clone)]
pub struct TaskStageUsageRow {
    pub task_id: String,
    pub epoch: u64,
    pub explore_used: u64,
    pub verify_used: u64,
    pub finalize_used: u64,
}

impl PgStore {
    /// Begin an IMMEDIATE transaction on the underlying connection.
    ///
    /// # Safety contract
    /// The `Node` that owns this store is single-threaded, so no other thread
    /// will interleave operations between `begin_tx` and `commit_tx`/`rollback_tx`.
    pub fn begin_tx(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute_batch("BEGIN")?;
        Ok(())
    }

    pub fn commit_tx(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute_batch("COMMIT")?;
        Ok(())
    }

    pub fn rollback_tx(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let _ = conn.execute_batch("ROLLBACK");
        Ok(())
    }
}

mod dashboard;
mod event_log;
mod export;
mod metrics;
mod projection;
mod schema;

fn query_table_json(
    conn: &Connection,
    sql: &str,
    params: impl pg::Params,
) -> Result<Vec<serde_json::Value>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params, |row| {
        let mut obj = serde_json::Map::new();
        for idx in 0..row.column_count() {
            let name = row.column_name(idx);
            let value = match row.get_ref(idx)? {
                ValueRef::Null => serde_json::Value::Null,
                ValueRef::Integer(v) => serde_json::Value::from(v),
                ValueRef::Real(v) => serde_json::Value::from(v),
                ValueRef::Text(v) => serde_json::Value::from(v),
                ValueRef::Blob(v) => serde_json::Value::from(hex::encode(v)),
            };
            obj.insert(name.to_owned(), value);
        }
        Ok(serde_json::Value::Object(obj))
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn median_u64(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2)
    }
}

fn with_reputation_decimal(rows: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    rows.into_iter()
        .map(|row| match row {
            Value::Object(mut obj) => {
                let stability_units = obj
                    .get("stability_reputation")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let quality_units = obj
                    .get("quality_reputation")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                obj.insert(
                    "stability_reputation_units".to_owned(),
                    Value::from(stability_units),
                );
                obj.insert(
                    "quality_reputation_units".to_owned(),
                    Value::from(quality_units),
                );
                obj.insert(
                    "stability_reputation_decimal".to_owned(),
                    Value::from(format_units_4dp(stability_units)),
                );
                obj.insert(
                    "quality_reputation_decimal".to_owned(),
                    Value::from(format_units_4dp(quality_units)),
                );
                Value::Object(obj)
            }
            other => other,
        })
        .collect()
}

fn format_units_4dp(units: i64) -> String {
    let sign = if units < 0 { "-" } else { "" };
    let abs_units = units.saturating_abs();
    let integer = abs_units / 10_000;
    let fractional = abs_units % 10_000;
    format!("{sign}{integer}.{fractional:04}")
}
