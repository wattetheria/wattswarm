use crate::control::open_node;
use crate::run_queue::{PgRunQueue, RunEvent, RunSubmitSpec, RunView, WorkerOptions};
use anyhow::Result;
use serde_json::{Value, json};
use std::path::Path;

const DEFAULT_RUN_QUEUE_PG_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/wattswarm";

pub fn resolve_run_queue_pg_url(flag_value: Option<String>) -> String {
    flag_value
        .or_else(|| std::env::var("WATTSWARM_PG_URL").ok())
        .unwrap_or_else(|| DEFAULT_RUN_QUEUE_PG_URL.to_owned())
}

pub fn init_run_queue(pg_url: &str) -> Result<()> {
    PgRunQueue::new(pg_url.to_owned()).init_schema()
}

fn current_org_queue(state_dir: &Path, db_path: &Path, pg_url: &str) -> Result<PgRunQueue> {
    let node = open_node(state_dir, db_path)?;
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
    current_org_queue(state_dir, db_path, pg_url)?.run_worker(opts, state_dir, db_path)
}
