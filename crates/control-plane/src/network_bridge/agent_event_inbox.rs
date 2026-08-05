use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use wattswarm_storage_core::storage::{
    ClaimedLocalAgentEventRow, local_control_scope_id, local_control_store,
};

const AGENT_INBOX_LEASE_MS: u64 = 90_000;
const AGENT_INBOX_BATCH_SIZE: usize = 8;
const AGENT_INBOX_RETRY_MS: u64 = 1_000;
const AGENT_INBOX_WORKERS: usize = 2;
const AGENT_INBOX_QUEUE_CAPACITY: usize = 32;
const AGENT_INBOX_MAX_ATTEMPTS: u32 = 8;

struct ClaimedWork {
    state_dir: PathBuf,
    db_path: PathBuf,
    scope_id: String,
    claim: ClaimedLocalAgentEventRow,
}

pub struct AgentInboxWorker {
    sender: mpsc::SyncSender<ClaimedWork>,
}

impl AgentInboxWorker {
    pub fn start() -> Self {
        let (sender, receiver) = mpsc::sync_channel(AGENT_INBOX_QUEUE_CAPACITY);
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..AGENT_INBOX_WORKERS {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("wattswarm-agent-inbox-{index}"))
                .spawn(move || worker_loop(receiver))
                .expect("spawn bounded Agent Inbox worker");
        }
        Self { sender }
    }

    pub fn scan_once(&self, state_dir: &Path, db_path: &Path) -> Result<u64> {
        let scope_id = local_control_scope_id(state_dir);
        let claims = claim_due(state_dir, &scope_id)?;
        let mut queued = 0_u64;
        for claim in claims {
            let work = ClaimedWork {
                state_dir: state_dir.to_path_buf(),
                db_path: db_path.to_path_buf(),
                scope_id: scope_id.clone(),
                claim,
            };
            match self.sender.try_send(work) {
                Ok(()) => queued = queued.saturating_add(1),
                Err(mpsc::TrySendError::Full(work)) => {
                    release_claim(&work, "Agent Inbox worker queue is full")?;
                }
                Err(mpsc::TrySendError::Disconnected(work)) => {
                    release_claim(&work, "Agent Inbox worker queue is disconnected")?;
                    anyhow::bail!("Agent Inbox worker queue is disconnected");
                }
            }
        }
        Ok(queued)
    }
}

pub fn process_agent_event_inbox_once(state_dir: &Path, db_path: &Path) -> Result<u64> {
    let scope_id = local_control_scope_id(state_dir);
    let claims = claim_due(state_dir, &scope_id)?;
    let mut processed = 0_u64;
    for claim in claims {
        process_work(ClaimedWork {
            state_dir: state_dir.to_path_buf(),
            db_path: db_path.to_path_buf(),
            scope_id: scope_id.clone(),
            claim,
        })?;
        processed = processed.saturating_add(1);
    }
    Ok(processed)
}

fn claim_due(state_dir: &Path, scope_id: &str) -> Result<Vec<ClaimedLocalAgentEventRow>> {
    local_control_store(state_dir)?.claim_due_local_agent_events(
        scope_id,
        &format!("agent-inbox:{}", std::process::id()),
        now_ms(),
        AGENT_INBOX_LEASE_MS,
        AGENT_INBOX_BATCH_SIZE,
    )
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<ClaimedWork>>>) {
    loop {
        let work = {
            let receiver = receiver
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            receiver.recv()
        };
        let Ok(work) = work else {
            return;
        };
        if let Err(error) = process_work(work) {
            eprintln!("wattswarm Agent Inbox worker failed: {error:#}");
        }
    }
}

fn process_work(work: ClaimedWork) -> Result<()> {
    let store = local_control_store(&work.state_dir)?;
    let event = row_to_agent_event(&work.claim.event)?;
    let outcome = super::agent_delivery::attempt_persisted_agent_event_to_local_executor(
        &work.state_dir,
        Some(&work.db_path),
        &event,
        work.claim.attempts,
    );
    match outcome {
        Ok(super::agent_delivery::AgentEventAttemptOutcome::Finished { status, error }) => {
            let status = serde_json::to_value(status)?
                .as_str()
                .unwrap_or("failed")
                .to_owned();
            store.finish_local_agent_event_claim(
                &work.scope_id,
                &event.event_id,
                &work.claim.lease_token,
                &status,
                error.as_deref(),
                now_ms(),
            )?;
        }
        Ok(super::agent_delivery::AgentEventAttemptOutcome::Retry { retry_at, error }) => {
            store.retry_local_agent_event_claim(
                &work.scope_id,
                &event.event_id,
                &work.claim.lease_token,
                retry_at,
                &error,
                now_ms(),
            )?;
        }
        Err(error) if work.claim.attempts < AGENT_INBOX_MAX_ATTEMPTS => {
            store.retry_local_agent_event_claim(
                &work.scope_id,
                &event.event_id,
                &work.claim.lease_token,
                now_ms().saturating_add(AGENT_INBOX_RETRY_MS),
                &format!("{error:#}"),
                now_ms(),
            )?;
        }
        Err(error) => {
            store.finish_local_agent_event_claim(
                &work.scope_id,
                &event.event_id,
                &work.claim.lease_token,
                "failed",
                Some(&format!("{error:#}")),
                now_ms(),
            )?;
        }
    }
    Ok(())
}

fn release_claim(work: &ClaimedWork, reason: &str) -> Result<()> {
    local_control_store(&work.state_dir)?.retry_local_agent_event_claim(
        &work.scope_id,
        &work.claim.event.event_id,
        &work.claim.lease_token,
        now_ms().saturating_add(AGENT_INBOX_RETRY_MS),
        reason,
        now_ms(),
    )?;
    Ok(())
}

fn row_to_agent_event(
    row: &wattswarm_storage_core::storage::LocalAgentEventRow,
) -> Result<wattswarm_protocol::types::AgentEvent> {
    Ok(wattswarm_protocol::types::AgentEvent {
        event_id: row.event_id.clone(),
        event_type: serde_json::from_value(serde_json::Value::String(row.event_type.clone()))
            .context("decode Agent Inbox event_type")?,
        source_kind: serde_json::from_value(serde_json::Value::String(row.source_kind.clone()))
            .context("decode Agent Inbox source_kind")?,
        source_node_id: row.source_node_id.clone(),
        target_agent_id: row.target_agent_id.clone(),
        target_executor: row.target_executor.clone(),
        agent_envelope: row
            .agent_envelope_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        payload: serde_json::from_str(&row.payload_json)?,
        requires_commit: row.requires_commit,
        allowed_actions: serde_json::from_str(&row.allowed_actions_json)?,
        correlation_id: row.correlation_id.clone(),
        dedupe_key: row.dedupe_key.clone(),
        created_at: row.created_at,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_event_round_trips_to_protocol_event() {
        let row = wattswarm_storage_core::storage::LocalAgentEventRow {
            event_id: "event-1".to_owned(),
            event_type: "dm_received".to_owned(),
            source_kind: "peer_direct_message".to_owned(),
            source_node_id: Some("node-a".to_owned()),
            target_agent_id: None,
            target_executor: Some("core-agent".to_owned()),
            agent_envelope_json: None,
            payload_json: "{}".to_owned(),
            allowed_actions_json: "[]".to_owned(),
            requires_commit: false,
            status: "pending".to_owned(),
            dedupe_key: None,
            correlation_id: None,
            created_at: 1,
            updated_at: 1,
        };
        let event = row_to_agent_event(&row).unwrap();
        assert_eq!(event.event_id, "event-1");
    }
}
