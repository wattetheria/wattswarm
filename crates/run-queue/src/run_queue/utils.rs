use anyhow::{Result, anyhow};
use postgres::{Client, Transaction};
use serde_json::{Map, Value};

use super::status::{
    STEP_STATUS_CANCELLED, STEP_STATUS_CREATED, STEP_STATUS_FAILED, STEP_STATUS_LEASED,
    STEP_STATUS_QUEUED, STEP_STATUS_RETRY_WAIT, STEP_STATUS_SUCCEEDED,
};
use super::types::RunStepCounts;

pub(crate) fn ensure_run_queue_timestamp_column(
    client: &mut Client,
    table: &str,
    column: &str,
) -> Result<()> {
    let data_type: String = client
        .query_opt(
            "SELECT data_type
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
            &[&table, &column],
        )?
        .ok_or_else(|| anyhow!("missing column {}.{}", table, column))?
        .get(0);
    if data_type == "timestamp with time zone" {
        return Ok(());
    }
    if data_type != "bigint" {
        return Err(anyhow!(
            "unsupported time column type {}.{}: {}",
            table,
            column,
            data_type
        ));
    }
    let sql = format!(
        "ALTER TABLE {table}
         ALTER COLUMN {column}
         TYPE TIMESTAMPTZ
         USING to_timestamp({column} / 1000.0)"
    );
    client.batch_execute(&sql)?;
    Ok(())
}

pub(crate) fn step_counts_tx(tx: &mut Transaction<'_>, run_id: &str) -> Result<RunStepCounts> {
    let rows = tx.query(
        "SELECT status, COUNT(1)
         FROM run_steps
         WHERE run_id = $1
         GROUP BY status",
        &[&run_id],
    )?;
    Ok(accumulate_counts(rows))
}

pub(crate) fn accumulate_counts(rows: Vec<postgres::Row>) -> RunStepCounts {
    let mut counts = RunStepCounts::default();
    for row in rows {
        let status: String = row.get(0);
        let count: i64 = row.get(1);
        match status.as_str() {
            STEP_STATUS_CREATED => counts.created += count,
            STEP_STATUS_QUEUED => counts.queued += count,
            STEP_STATUS_LEASED => counts.leased += count,
            STEP_STATUS_SUCCEEDED => counts.succeeded += count,
            STEP_STATUS_FAILED => counts.failed += count,
            STEP_STATUS_RETRY_WAIT => counts.retry_wait += count,
            STEP_STATUS_CANCELLED => counts.cancelled += count,
            _ => {}
        }
    }
    counts
}

pub(crate) fn build_step_inputs(shared_inputs: &Value, prompt: &str, agent_id: &str) -> Value {
    let mut merged = Map::new();
    if let Value::Object(obj) = shared_inputs {
        for (k, v) in obj {
            merged.insert(k.clone(), v.clone());
        }
    } else {
        merged.insert("shared_inputs".to_owned(), shared_inputs.clone());
    }
    merged.insert("prompt".to_owned(), Value::String(prompt.to_owned()));
    merged.insert("agent_id".to_owned(), Value::String(agent_id.to_owned()));
    Value::Object(merged)
}

pub(crate) fn retry_delay_ms(base: u64, attempt: u32) -> i64 {
    let exp = attempt.saturating_sub(1).min(16);
    let factor = 1_u64 << exp;
    base.saturating_mul(factor).max(200) as i64
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
