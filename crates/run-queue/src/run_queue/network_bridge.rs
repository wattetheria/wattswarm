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

use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;

use super::queue::PgRunQueue;
use super::stigmergy::StigmergyCompletionSource;
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

fn ensure_author_confirmed_execution_set_member(
    members: &[crate::control::storage::ExecutionSetMemberRow],
    task_id: &str,
    author_node_id: &str,
) -> Result<()> {
    if members.is_empty() {
        anyhow::bail!(
            "run_queue_bridge: execution set membership for {task_id} is not confirmed; \
             deferring remote result from {author_node_id}"
        );
    }

    if members
        .iter()
        .any(|m| m.participant_node_id == author_node_id && m.status == "confirmed")
    {
        return Ok(());
    }

    anyhow::bail!(
        "run_queue_bridge: author {author_node_id} is not a confirmed \
         execution set member for {task_id}; rejecting result"
    );
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
    let node = crate::control::open_node(state_dir, std::path::Path::new(&queue.database_url))
        .context("run_queue_bridge: open node to verify execution set membership")?;
    let members = node
        .store
        .list_execution_set_members(task_id, &execution_set_id)?;
    ensure_author_confirmed_execution_set_member(&members, task_id, author_node_id)?;

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
    let processing_path = state_dir.join("pending_bridge_tasks.processing.jsonl");

    if !processing_path.exists() {
        if !pending_path.exists() {
            return Ok(0);
        }
        std::fs::rename(&pending_path, &processing_path)?;
    }

    let content = std::fs::read_to_string(&processing_path)?;
    if content.trim().is_empty() {
        let _ = std::fs::remove_file(&processing_path);
        return Ok(0);
    }

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

    let _ = std::fs::remove_file(&processing_path);

    // Write back failed lines so they can be retried next tick. If the process
    // crashes mid-loop, the processing file remains on disk and the batch is
    // replayed on restart rather than being lost after truncation.
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
    let processing_path = state_dir.join("pending_run_queue_results.processing.jsonl");

    if !processing_path.exists() {
        if !results_path.exists() {
            return Ok(0);
        }
        std::fs::rename(&results_path, &processing_path)?;
    }

    let content = std::fs::read_to_string(&processing_path)?;
    if content.trim().is_empty() {
        let _ = std::fs::remove_file(&processing_path);
        return Ok(0);
    }

    let queue = match resolve_run_queue(state_dir) {
        Some(q) => q,
        None => return Ok(0), // Processing file remains — retry next tick.
    };

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
        let author_node_id = entry
            .get("author_node_id")
            .and_then(Value::as_str)
            .unwrap_or("");

        if task_id.is_empty() || candidate_id.is_empty() || author_node_id.is_empty() {
            continue;
        }

        let output =
            match crate::control::open_node(state_dir, std::path::Path::new(&queue.database_url))
                .ok()
                .and_then(|node| {
                    node.store
                        .get_candidate_by_id(task_id, candidate_id)
                        .ok()
                        .flatten()
                })
                .map(|candidate| candidate.output)
            {
                Some(output) => output,
                None => {
                    failed_lines.push(line.to_owned());
                    continue;
                }
            };

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

    let _ = std::fs::remove_file(&processing_path);

    // Write back failed lines so they can be retried next tick. Crashes keep
    // the processing file on disk, so unacked work is replayed instead of lost.
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

pub fn process_pending_stigmergy_contributions(state_dir: &Path) -> Result<u64> {
    let pending_path = state_dir.join("pending_stigmergy_contributions.jsonl");
    let processing_path = state_dir.join("pending_stigmergy_contributions.processing.jsonl");

    if !processing_path.exists() {
        if !pending_path.exists() {
            return Ok(0);
        }
        std::fs::rename(&pending_path, &processing_path)?;
    }

    let content = std::fs::read_to_string(&processing_path)?;
    if content.trim().is_empty() {
        let _ = std::fs::remove_file(&processing_path);
        return Ok(0);
    }

    let queue = match resolve_run_queue(state_dir) {
        Some(q) => q,
        None => return Ok(0),
    };

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
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("");
        let task_id = entry.get("task_id").and_then(Value::as_str).unwrap_or("");
        let agent_id = entry.get("agent_id").and_then(Value::as_str).unwrap_or("");
        let author_node_id = entry
            .get("author_node_id")
            .and_then(Value::as_str)
            .or_else(|| entry.get("claimer_node_id").and_then(Value::as_str))
            .or_else(|| entry.get("completed_by_node_id").and_then(Value::as_str))
            .unwrap_or("");
        let execution_id = entry
            .get("execution_id")
            .and_then(Value::as_str)
            .unwrap_or("");

        if kind.is_empty()
            || task_id.is_empty()
            || agent_id.is_empty()
            || author_node_id.is_empty()
            || execution_id.is_empty()
        {
            continue;
        }

        let result = match kind {
            "claim" => {
                queue.record_stigmergy_claim(task_id, agent_id, author_node_id, execution_id)
            }
            "completion" => {
                let output = entry.get("output").cloned().unwrap_or(Value::Null);
                queue.record_stigmergy_completion(
                    task_id,
                    agent_id,
                    author_node_id,
                    execution_id,
                    &output,
                    StigmergyCompletionSource::TaskCompleted,
                )
            }
            "candidate" => {
                let output = entry
                    .get("candidate_output")
                    .cloned()
                    .filter(|value| !value.is_null())
                    .or_else(|| {
                        let candidate_id = entry.get("candidate_id").and_then(Value::as_str)?;
                        crate::control::open_node(
                            state_dir,
                            std::path::Path::new(&queue.database_url),
                        )
                        .ok()
                        .and_then(|node| {
                            node.store
                                .get_candidate_by_id(task_id, candidate_id)
                                .ok()
                                .flatten()
                        })
                        .map(|candidate| candidate.output)
                    })
                    .unwrap_or(Value::Null);
                queue
                    .record_stigmergy_claim(task_id, agent_id, author_node_id, execution_id)
                    .and_then(|_| {
                        queue.record_stigmergy_completion(
                            task_id,
                            agent_id,
                            author_node_id,
                            execution_id,
                            &output,
                            StigmergyCompletionSource::CandidateProposed,
                        )
                    })
            }
            _ => Ok(0),
        };

        match result {
            Ok(count) => processed = processed.saturating_add(count),
            Err(err) => {
                eprintln!(
                    "run_queue_bridge: failed to process stigmergy contribution for {task_id}: {err:#}"
                );
                failed_lines.push(line.to_owned());
            }
        }
    }

    let _ = std::fs::remove_file(&processing_path);
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

pub fn evaluate_open_stigmergy_rounds_for_state(state_dir: &Path) -> Result<u64> {
    let Some(queue) = resolve_run_queue(state_dir) else {
        return Ok(0);
    };
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    queue.evaluate_open_stigmergy_rounds(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execution_set_member(
        participant_node_id: &str,
        status: &str,
    ) -> crate::control::storage::ExecutionSetMemberRow {
        crate::control::storage::ExecutionSetMemberRow {
            task_id: "task-1".to_owned(),
            execution_set_id: "remote-bridge:task-1".to_owned(),
            participant_node_id: participant_node_id.to_owned(),
            role_hint: "executor".to_owned(),
            scope_hint: "global".to_owned(),
            status: status.to_owned(),
            confirmed_by_node_id: Some("coordinator".to_owned()),
            updated_at: 1,
        }
    }

    #[test]
    fn remote_result_author_membership_requires_confirmed_member() {
        assert!(
            ensure_author_confirmed_execution_set_member(
                &[execution_set_member("node-a", "confirmed")],
                "task-1",
                "node-a",
            )
            .is_ok()
        );

        let empty_err = ensure_author_confirmed_execution_set_member(&[], "task-1", "node-a")
            .expect_err("empty membership must defer remote result");
        assert!(empty_err.to_string().contains("is not confirmed"));

        let pending_err = ensure_author_confirmed_execution_set_member(
            &[execution_set_member("node-a", "accepted")],
            "task-1",
            "node-a",
        )
        .expect_err("unconfirmed member must not complete remote result");
        assert!(pending_err.to_string().contains("not a confirmed"));

        let outsider_err = ensure_author_confirmed_execution_set_member(
            &[execution_set_member("node-b", "confirmed")],
            "task-1",
            "node-a",
        )
        .expect_err("non-member must not complete remote result");
        assert!(outsider_err.to_string().contains("not a confirmed"));
    }
}
