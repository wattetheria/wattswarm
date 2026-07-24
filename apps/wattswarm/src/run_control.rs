use crate::control::{
    ExecutorKind, configured_node_mode, load_executor_registry_state, normalize_executor_name,
    open_configured_node,
};
use crate::run_queue::{PgRunQueue, RunEvent, RunSubmitSpec, RunView, WorkerOptions};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::path::Path;
use std::thread;
use std::time::Duration;

const DEFAULT_RUN_QUEUE_PG_URL: &str = "postgres://postgres:postgres@127.0.0.1:55432/wattswarm";
const WORKER_MODE_WAIT_POLL_MS: u64 = 250;
const WORKER_MODE_WAIT_LOG_EVERY_POLLS: u64 = 1_200;

pub fn resolve_run_queue_pg_url(flag_value: Option<String>) -> String {
    flag_value
        .or_else(|| std::env::var("WATTSWARM_PG_URL").ok())
        .unwrap_or_else(|| DEFAULT_RUN_QUEUE_PG_URL.to_owned())
}

pub fn init_run_queue(state_dir: &Path, pg_url: &str) -> Result<()> {
    PgRunQueue::from_runtime_config(pg_url.to_owned(), state_dir)?.init_schema()
}

fn current_org_queue(state_dir: &Path, db_path: &Path, pg_url: &str) -> Result<PgRunQueue> {
    let node = open_configured_node(state_dir, db_path)?;
    Ok(
        PgRunQueue::from_runtime_config(pg_url.to_owned(), state_dir)?
            .for_org(node.store.org_id().to_owned()),
    )
}

fn annotate_and_filter_distributed_run_spec(
    state_dir: &Path,
    db_path: &Path,
    mut spec: RunSubmitSpec,
) -> Result<RunSubmitSpec> {
    let mode = configured_node_mode(state_dir)?.unwrap_or(crate::control::NodeMode::Local);
    if matches!(mode, crate::control::NodeMode::Local) {
        return Ok(spec);
    }

    let node = open_configured_node(state_dir, db_path)?;
    let coordinator_node_id = node.node_id();
    let registry = load_executor_registry_state(state_dir).unwrap_or_default();
    let original_agent_count = spec.agents.len();
    spec.agents.retain(|agent| {
        let executor_name = normalize_executor_name(&agent.executor);
        !registry
            .entries
            .iter()
            .find(|entry| entry.name == executor_name)
            .is_some_and(|entry| entry.kind == ExecutorKind::Local)
    });
    if original_agent_count > 0 && spec.agents.is_empty() {
        return Err(anyhow!(
            "distributed run {} has no non-coordinator executors after filtering local executors",
            spec.run_id
        ));
    }

    let mut shared_inputs = match spec.shared_inputs {
        Value::Object(obj) => obj,
        other => {
            let mut obj = serde_json::Map::new();
            obj.insert("shared_inputs".to_owned(), other);
            obj
        }
    };
    shared_inputs.insert(
        "_run_queue_coordination".to_owned(),
        json!({
            "mode": mode.as_str(),
            "coordinator_node_id": coordinator_node_id,
            "coordinator_executes": false,
        }),
    );
    spec.shared_inputs = Value::Object(shared_inputs);
    Ok(spec)
}

pub fn submit_run(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    spec: RunSubmitSpec,
    kickoff: bool,
) -> Result<Value> {
    let queue = current_org_queue(state_dir, db_path, pg_url)?;
    let spec = annotate_and_filter_distributed_run_spec(state_dir, db_path, spec)?;
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
    crate::startup_config::ensure_default_wan_startup_config(state_dir)?;
    let mut wait_polls = 0_u64;
    let mut logged_waiting = false;
    while configured_node_mode(state_dir)?.is_none() {
        if should_log_waiting_for_mode(wait_polls) {
            eprintln!("wattswarm worker waiting for node mode configuration");
            logged_waiting = true;
        }
        wait_polls = wait_polls.saturating_add(1);
        thread::sleep(Duration::from_millis(WORKER_MODE_WAIT_POLL_MS));
    }
    if logged_waiting {
        eprintln!("wattswarm worker detected node mode configuration; resuming queue loop");
    }
    // Start network bridge with run-queue hook so remote dispatch and
    // result collection work in CLI-only mode (not just UI).
    let _ = crate::network_bridge::maybe_start_background_network_service_with_hook(
        state_dir.to_path_buf(),
        db_path.to_path_buf(),
        Some(Box::new(|node, sd| {
            crate::network_hooks::run_background_post_tick(node, sd);
        })),
    );
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
        assert!(!should_log_waiting_for_mode(1_199));
        assert!(should_log_waiting_for_mode(1_200));
        assert!(should_log_waiting_for_mode(2_400));
    }
}
