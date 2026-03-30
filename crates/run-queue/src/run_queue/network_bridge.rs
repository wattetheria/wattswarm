//! Bridge between the network gossip layer and the local run-queue.
//!
//! Two responsibilities:
//!
//! 1. **Executor side**: When a `TaskAnnounced` event arrives via gossip with
//!    feed_key `venue.run_queue`, record a pending bridge task for the background
//!    service to pick up and execute via `bridge_remote_task_into_local_execution`.
//!
//! 2. **Coordinator side**: When a `CandidateProposed` event arrives for a task
//!    that originated from a local run-queue step (tracked by the
//!    `REMOTE_DISPATCHED` status), write the result back into the run_steps
//!    table so the run-queue aggregation can finalize.

use anyhow::Result;
use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;

use super::queue::PgRunQueue;
use super::utils::STEP_STATUS_REMOTE_DISPATCHED;

/// Feed key used by the run-queue coordinator when announcing tasks.
pub const RUN_QUEUE_FEED_KEY: &str = "venue.run_queue";

// Event-level dispatch (on_event_ingested) has been removed.
// Gossip events are now routed via JSONL files:
//   - control-plane `log_run_queue_events_if_applicable()` writes to
//     `pending_bridge_tasks.jsonl` (executor side) and
//     `pending_run_queue_results.jsonl` (coordinator side).
//   - `process_pending_bridge_tasks()` and `process_pending_run_queue_results()`
//     consume them from the background service post-tick hook.

/// Resolve PgRunQueue from environment variables, using the node's real org_id.
fn resolve_run_queue(state_dir: &Path) -> Option<PgRunQueue> {
    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("WATTSWARM_PG_URL"))
        .ok()?;
    let db_path = std::path::Path::new(&db_url);
    let schema = std::env::var("WATTSWARM_PG_SCHEMA").ok();
    let queue = match schema {
        Some(s) => PgRunQueue::with_schema(&db_url, s),
        None => PgRunQueue::new(&db_url),
    };

    // Open a node to resolve the actual org_id for any mode (local/lan/network).
    let node = crate::control::open_node(state_dir, db_path).ok()?;
    let org_id = node.store.org_id().to_owned();
    Some(queue.for_org(org_id))
}

/// Complete a REMOTE_DISPATCHED step with the remote candidate result.
/// Verifies that the result comes from a confirmed execution set participant
/// by querying the node-core PgStore (which owns the execution_set_projection table).
fn complete_remote_step(
    queue: &PgRunQueue,
    task_id: &str,
    candidate_id: &str,
    output: &Value,
    author_node_id: &str,
    state_dir: &Path,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp_millis();
    let result_json = json!({
        "task_id": task_id,
        "candidate_id": candidate_id,
        "candidate_output": output,
        "author_node_id": author_node_id,
        "source": "remote_gossip",
    });
    let result_raw = serde_json::to_string(&result_json)?;

    let mut client = queue.connect()?;
    let mut tx = client.transaction()?;

    // Find the REMOTE_DISPATCHED step matching this task_id.
    let rows = tx.query(
        "SELECT step_id, run_id, executor, agent_id
         FROM run_steps
         WHERE org_id = $1
           AND task_id = $2
           AND status = $3
         LIMIT 1",
        &[&queue.org_id(), &task_id, &STEP_STATUS_REMOTE_DISPATCHED],
    )?;

    let Some(row) = rows.into_iter().next() else {
        return Ok(());
    };

    let step_id: String = row.get(0);
    let run_id: String = row.get(1);
    let _executor: String = row.get(2);
    let _agent_id: String = row.get(3);

    // Verify the author is a confirmed execution set member using the
    // node-core PgStore which owns the execution_set_projection table.
    let execution_set_id = format!("remote-bridge:{task_id}");
    if let Ok(node) =
        crate::control::open_node(state_dir, std::path::Path::new(&queue.database_url))
    {
        let members = node
            .store
            .list_execution_set_members(task_id, &execution_set_id)?;
        let is_confirmed_member = members
            .iter()
            .any(|m| m.participant_node_id == author_node_id);
        if !is_confirmed_member && !members.is_empty() {
            anyhow::bail!(
                "run_queue_bridge: author {author_node_id} is not a confirmed \
                 execution set member for {task_id}; rejecting result"
            );
        }
        // If members is empty, execution set wasn't yet confirmed — accept
        // the result (early arrival before confirmation gossip).
    }

    tx.execute(
        "UPDATE run_steps
         SET status = $3,
             result_json = $4,
             lease_id = NULL, lease_owner = NULL, lease_until = NULL,
             updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
             finished_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
         WHERE org_id = $1 AND step_id = $2 AND status = $6",
        &[
            &queue.org_id(),
            &step_id,
            &"SUCCEEDED",
            &result_raw,
            &now,
            &STEP_STATUS_REMOTE_DISPATCHED,
        ],
    )?;

    tx.execute(
        "INSERT INTO run_events (org_id, run_id, event_type, payload_json, created_at)
         VALUES ($1, $2, $3, $4, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'))",
        &[
            &queue.org_id(),
            &run_id,
            &"STEP_REMOTE_COMPLETED",
            &serde_json::to_string(&json!({
                "step_id": step_id,
                "task_id": task_id,
                "candidate_id": candidate_id,
                "source": "remote_gossip",
            }))?,
            &now,
        ],
    )?;

    queue.finalize_run_if_terminal_tx(&mut tx, &run_id, now)?;
    tx.commit()?;
    Ok(())
}

/// Process pending bridge tasks queued by `on_remote_task_announced`.
/// Call this from the background service tick loop where mutable Node access
/// is available.
pub fn process_pending_bridge_tasks(
    node: &mut crate::control::node::Node,
    state_dir: &Path,
) -> Result<u64> {
    let pending_path = state_dir.join("pending_bridge_tasks.jsonl");
    if !pending_path.exists() {
        return Ok(0);
    }

    let content = std::fs::read_to_string(&pending_path)?;
    if content.trim().is_empty() {
        return Ok(0);
    }

    // Clear before processing, but collect failures to write back.
    std::fs::write(&pending_path, "")?;

    let mut processed = 0u64;
    let mut failed_lines = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let task_id = entry.get("task_id").and_then(Value::as_str).unwrap_or("");
        let executor = entry.get("executor").and_then(Value::as_str).unwrap_or("");
        let profile = entry
            .get("profile")
            .and_then(Value::as_str)
            .unwrap_or("default");

        if task_id.is_empty() || executor.is_empty() {
            continue;
        }

        let req = crate::control::RemoteTaskBridgeRequest {
            executor: executor.to_owned(),
            profile: profile.to_owned(),
            task_id: task_id.to_owned(),
        };
        match crate::control::bridge_remote_task_into_local_execution(node, state_dir, req) {
            Ok(_) => processed += 1,
            Err(err) => {
                eprintln!("run_queue_bridge: failed to bridge task {task_id}: {err:#}");
                failed_lines.push(line.to_owned());
            }
        }
    }
    // Write back failed lines so they can be retried next tick.
    if !failed_lines.is_empty() {
        let mut retry_content = failed_lines.join("\n");
        retry_content.push('\n');
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&pending_path)
            .and_then(|mut f| f.write_all(retry_content.as_bytes()));
    }
    Ok(processed)
}

/// Process pending run-queue results written by the coordinator-side gossip hook.
/// Each line in `pending_run_queue_results.jsonl` contains a CandidateProposed
/// result that should be written back to a REMOTE_DISPATCHED run_step.
pub fn process_pending_run_queue_results(state_dir: &Path) -> Result<u64> {
    let results_path = state_dir.join("pending_run_queue_results.jsonl");
    if !results_path.exists() {
        return Ok(0);
    }

    let content = std::fs::read_to_string(&results_path)?;
    if content.trim().is_empty() {
        return Ok(0);
    }

    let queue = match resolve_run_queue(state_dir) {
        Some(q) => q,
        None => return Ok(0), // File not cleared — retry next tick.
    };

    // Clear only after queue is resolved, so failures don't lose data.
    std::fs::write(&results_path, "")?;

    let mut processed = 0u64;
    let mut failed_lines = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let task_id = entry.get("task_id").and_then(Value::as_str).unwrap_or("");
        let candidate_id = entry
            .get("candidate_id")
            .and_then(Value::as_str)
            .unwrap_or("");
        let output = entry
            .get("candidate_output")
            .cloned()
            .unwrap_or(Value::Null);
        let author_node_id = entry
            .get("author_node_id")
            .and_then(Value::as_str)
            .unwrap_or("");

        if task_id.is_empty() || candidate_id.is_empty() || author_node_id.is_empty() {
            continue;
        }

        match complete_remote_step(
            &queue,
            task_id,
            candidate_id,
            &output,
            author_node_id,
            state_dir,
        ) {
            Ok(()) => processed += 1,
            Err(err) => {
                eprintln!(
                    "run_queue_bridge: failed to complete remote step for {task_id}: {err:#}"
                );
                failed_lines.push(line.to_owned());
            }
        }
    }
    // Write back failed lines so they can be retried next tick.
    if !failed_lines.is_empty() {
        let mut retry_content = failed_lines.join("\n");
        retry_content.push('\n');
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&results_path)
            .and_then(|mut f| f.write_all(retry_content.as_bytes()));
    }
    Ok(processed)
}
