use crate::crypto::sha256_hex;
use crate::node::Node;
use crate::types::TaskTerminalState;
use anyhow::Result;
use serde::Serialize;
use std::path::Path;

const SWARM_WORKER_COUNT: usize = 5;
const SWARM_LOG_LIMIT: usize = 12;
const SWARM_SCAN_LIMIT: usize = 128;

#[derive(Debug, Clone, Serialize)]
pub struct SwarmSignal {
    pub id: String,
    pub task_id: String,
    pub task_type: String,
    pub status: String,
    pub score: u32,
    pub evidence_count: u32,
    pub verified_count: u32,
    pub decision: Option<String>,
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SwarmWorker {
    pub id: String,
    pub status: String,
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SwarmLog {
    pub time: String,
    pub source: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SwarmSummary {
    pub total_signals: u32,
    pub open_signals: u32,
    pub finalized_signals: u32,
    pub terminal_signals: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SwarmDashboardState {
    pub signals: Vec<SwarmSignal>,
    pub workers: Vec<SwarmWorker>,
    pub logs: Vec<SwarmLog>,
    pub summary: SwarmSummary,
}

pub fn build_dashboard_state(node: &Node) -> Result<SwarmDashboardState> {
    let mut signals = Vec::new();
    for task_id in node.store.list_task_ids_recent(SWARM_SCAN_LIMIT)? {
        let Some(task) = node.task_view(&task_id)? else {
            continue;
        };

        let latest_candidate = node.store.latest_candidate_for_task(&task_id)?;
        let (evidence_count, verified_count) = if let Some(candidate) = &latest_candidate {
            let evidence = candidate
                .evidence_refs
                .len()
                .saturating_add(candidate.evidence_inline.len()) as u32;
            let results = node
                .store
                .list_verifier_results_for_candidate(&task_id, &candidate.candidate_id)?;
            (evidence.max(1), results.len() as u32)
        } else {
            (1, 0)
        };

        let status = match task.terminal_state {
            TaskTerminalState::Open => "OPEN",
            TaskTerminalState::Finalized => "FINALIZED",
            TaskTerminalState::Expired => "EXPIRED",
            TaskTerminalState::Stopped => "STOPPED",
            TaskTerminalState::Suspended => "SUSPENDED",
            TaskTerminalState::Killed => "KILLED",
        }
        .to_owned();
        let decision = if task.finalized_candidate_id.is_some() {
            Some("COMMIT".to_owned())
        } else {
            None
        };

        let mut score = 15_u32;
        score = score.saturating_add(evidence_count.saturating_mul(8));
        score = score.saturating_add(verified_count.saturating_mul(10));
        if task.committed_candidate_id.is_some() {
            score = score.saturating_add(12);
        }
        if task.finalized_candidate_id.is_some() {
            score = score.saturating_add(20);
        }
        score = score.min(100);

        let (x, y) = fallback_xy(&task_id);
        signals.push(SwarmSignal {
            id: short_id(&task_id),
            task_id: task_id.clone(),
            task_type: task.contract.task_type,
            status,
            score,
            evidence_count,
            verified_count,
            decision,
            x,
            y,
        });
    }
    signals.sort_by(|a, b| b.score.cmp(&a.score));

    let workers = build_workers(&signals);
    let logs = build_logs(node)?;
    let summary = SwarmSummary {
        total_signals: signals.len() as u32,
        open_signals: signals.iter().filter(|s| s.status == "OPEN").count() as u32,
        finalized_signals: signals.iter().filter(|s| s.status == "FINALIZED").count() as u32,
        terminal_signals: signals
            .iter()
            .filter(|s| s.status != "OPEN" && s.status != "FINALIZED")
            .count() as u32,
    };

    Ok(SwarmDashboardState {
        signals,
        workers,
        logs,
        summary,
    })
}

pub fn tick_real_swarm(
    node: &mut Node,
    _state_dir: &Path,
    _executor: &str,
    _profile: &str,
) -> Result<SwarmDashboardState> {
    build_dashboard_state(node)
}

fn build_workers(signals: &[SwarmSignal]) -> Vec<SwarmWorker> {
    let mut workers = (1..=SWARM_WORKER_COUNT)
        .map(|idx| SwarmWorker {
            id: format!("W-{idx:02}"),
            status: "IDLE".to_owned(),
            target_id: None,
        })
        .collect::<Vec<_>>();

    for (idx, signal) in signals
        .iter()
        .filter(|s| s.status == "OPEN")
        .take(SWARM_WORKER_COUNT)
        .enumerate()
    {
        workers[idx].status = "ASSIGNED".to_owned();
        workers[idx].target_id = Some(signal.id.clone());
    }
    workers
}

fn build_logs(node: &Node) -> Result<Vec<SwarmLog>> {
    let head = node.head_seq()?;
    let from = head.saturating_sub((SWARM_LOG_LIMIT as u64) + 16);
    let mut events = node.store.load_events_page(from, SWARM_LOG_LIMIT)?;
    events.reverse();
    let logs = events
        .into_iter()
        .map(|(_, event)| SwarmLog {
            time: fmt_time(event.created_at),
            source: event.author_node_id,
            message: format!("{:?}", event.event_kind),
        })
        .collect::<Vec<_>>();
    Ok(logs)
}

fn short_id(task_id: &str) -> String {
    let compact = task_id.replace('-', "");
    let len = compact.len();
    if len <= 6 {
        compact
    } else {
        compact[len - 6..].to_ascii_uppercase()
    }
}

fn fallback_xy(task_id: &str) -> (f64, f64) {
    let digest = sha256_hex(task_id.as_bytes());
    let hx = digest.get(0..2).unwrap_or("7f");
    let hy = digest.get(2..4).unwrap_or("a0");
    let x = u8::from_str_radix(hx, 16).unwrap_or(127) as f64;
    let y = u8::from_str_radix(hy, 16).unwrap_or(160) as f64;
    (10.0 + (x / 255.0) * 80.0, 10.0 + (y / 255.0) * 80.0)
}

fn fmt_time(ts_ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms as i64)
        .map(|dt| dt.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "00:00:00".to_owned())
}
