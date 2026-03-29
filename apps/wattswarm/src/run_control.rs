use crate::control::{configured_node_mode, open_configured_node};
use crate::run_queue::{PgRunQueue, RunEvent, RunSubmitSpec, RunView, WorkerOptions};
use anyhow::Result;
use serde_json::{Value, json};
use std::path::Path;
use std::thread;
use std::time::Duration;

const DEFAULT_RUN_QUEUE_PG_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/wattswarm";
const WORKER_MODE_WAIT_POLL_MS: u64 = 250;
const WORKER_MODE_WAIT_LOG_EVERY_POLLS: u64 = 120;

pub fn resolve_run_queue_pg_url(flag_value: Option<String>) -> String {
    flag_value
        .or_else(|| std::env::var("WATTSWARM_PG_URL").ok())
        .unwrap_or_else(|| DEFAULT_RUN_QUEUE_PG_URL.to_owned())
}

pub fn init_run_queue(pg_url: &str) -> Result<()> {
    PgRunQueue::new(pg_url.to_owned()).init_schema()
}

fn current_org_queue(state_dir: &Path, db_path: &Path, pg_url: &str) -> Result<PgRunQueue> {
    let node = open_configured_node(state_dir, db_path)?;
    Ok(PgRunQueue::new(pg_url.to_owned()).for_org(node.store.org_id().to_owned()))
}

pub fn submit_run(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    spec: RunSubmitSpec,
    kickoff: bool,
) -> Result<Value> {
    let queue = current_org_queue(state_dir, db_path, pg_url)?;
    let run_id = spec.run_id.clone();
    queue.submit_run(spec)?;
    if kickoff {
        queue.kickoff_run(&run_id)?;
    }
    Ok(json!({
        "ok": true,
        "run_id": run_id,
        "kicked_off": kickoff
    }))
}

pub fn kickoff_run(state_dir: &Path, db_path: &Path, pg_url: &str, run_id: &str) -> Result<Value> {
    let queue = current_org_queue(state_dir, db_path, pg_url)?;
    queue.kickoff_run(run_id)?;
    Ok(json!({
        "ok": true,
        "run_id": run_id,
        "kicked_off": true
    }))
}

pub fn watch_run(state_dir: &Path, db_path: &Path, pg_url: &str, run_id: &str) -> Result<RunView> {
    current_org_queue(state_dir, db_path, pg_url)?.run_view(run_id)
}

pub fn run_result(state_dir: &Path, db_path: &Path, pg_url: &str, run_id: &str) -> Result<Value> {
    current_org_queue(state_dir, db_path, pg_url)?.run_result(run_id)
}

pub fn list_runs(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    limit: i64,
) -> Result<Vec<RunView>> {
    current_org_queue(state_dir, db_path, pg_url)?.list_runs(limit.max(1))
}

pub fn run_events(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    run_id: &str,
    limit: i64,
) -> Result<Vec<RunEvent>> {
    current_org_queue(state_dir, db_path, pg_url)?.run_events(run_id, limit.max(1))
}

pub fn cancel_run(state_dir: &Path, db_path: &Path, pg_url: &str, run_id: &str) -> Result<Value> {
    let queue = current_org_queue(state_dir, db_path, pg_url)?;
    queue.cancel_run(run_id)?;
    Ok(json!({
        "ok": true,
        "run_id": run_id,
        "cancelled": true
    }))
}

pub fn retry_run(state_dir: &Path, db_path: &Path, pg_url: &str, run_id: &str) -> Result<Value> {
    let queue = current_org_queue(state_dir, db_path, pg_url)?;
    queue.retry_run(run_id)?;
    Ok(json!({
        "ok": true,
        "run_id": run_id,
        "retried": true
    }))
}

pub fn run_worker(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    opts: WorkerOptions,
) -> Result<()> {
    let mut wait_polls = 0_u64;
    while configured_node_mode(state_dir)?.is_none() {
        if should_log_waiting_for_mode(wait_polls) {
            eprintln!("wattswarm worker waiting for node mode configuration");
        }
        wait_polls = wait_polls.saturating_add(1);
        thread::sleep(Duration::from_millis(WORKER_MODE_WAIT_POLL_MS));
    }
    current_org_queue(state_dir, db_path, pg_url)?.run_worker(opts, state_dir, db_path)
}

fn should_log_waiting_for_mode(wait_polls: u64) -> bool {
    wait_polls == 0 || wait_polls.is_multiple_of(WORKER_MODE_WAIT_LOG_EVERY_POLLS)
}

#[cfg(test)]
mod tests {
    use super::should_log_waiting_for_mode;

    #[test]
    fn worker_wait_logging_repeats_every_interval() {
        assert!(should_log_waiting_for_mode(0));
        assert!(!should_log_waiting_for_mode(1));
        assert!(!should_log_waiting_for_mode(119));
        assert!(should_log_waiting_for_mode(120));
        assert!(should_log_waiting_for_mode(240));
    }
}
