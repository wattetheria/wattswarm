use crate::control::{
    NodeState, RealTaskRunRequest, fetch_checkpoint_artifact_json,
    load_discovered_peer_records_state, load_peer_dm_message_records_state,
    load_peer_dm_thread_records_state, load_peer_relationship_records_state, local_node_id,
    node_state_path, open_configured_node, open_node, require_configured_node_mode,
    resolve_node_mode, run_real_task_flow,
};
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::run_control;
use crate::run_queue::{RunSubmitSpec, RunView};
use crate::startup_config::{load_startup_config, startup_config_path, update_startup_config_geo};
use crate::storage::storage::{
    LocalRemoteTaskBridgeRow, TaskCandidateRow, TaskProjectionRow, TopicCursorRow, TopicMessageRow,
    VerifierResultRow, VoteRevealRow,
};
use anyhow::{Context, Result, anyhow};
use async_stream::try_stream;
use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::time::{Duration, sleep};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("wattswarm.wattetheria.sync");
}

use proto::wattetheria_sync_service_server::{
    WattetheriaSyncService, WattetheriaSyncServiceServer,
};
use proto::{
    ProjectionFrame, ProjectionStreamRequest, UpdateStartupGeoRequest, UpdateStartupGeoResponse,
};

const DEFAULT_GRPC_LISTEN_ADDR: &str = "127.0.0.1:7791";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkProjectionSnapshot {
    pub generated_at: u64,
    pub node_id: String,
    pub org_id: String,
    pub network_id: String,
    pub running: bool,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    pub peer_protocol_distribution: BTreeMap<String, u64>,
    pub peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskProjectionSummary {
    pub task_id: String,
    pub task_type: String,
    pub epoch: u64,
    pub terminal_state: String,
    pub committed_candidate_id: Option<String>,
    pub finalized_candidate_id: Option<String>,
    pub retry_attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRunProjectionSnapshot {
    pub generated_at: u64,
    pub recent_tasks: Vec<TaskProjectionSummary>,
    pub recent_runs: Vec<RunView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFactsSnapshot {
    pub generated_at: u64,
    pub task_id: String,
    pub task: Option<TaskProjectionSummary>,
    pub coordinator_run: Option<TaskCoordinatorRunSnapshot>,
    pub execution_set_members: Vec<TaskExecutionSetMemberSnapshot>,
    pub remote_bridges: Vec<TaskRemoteBridgeSnapshot>,
    pub candidates: Vec<TaskCandidateSnapshot>,
    pub verifier_results: Vec<TaskVerifierResultSnapshot>,
    pub vote_reveals: Vec<TaskVoteRevealSnapshot>,
    pub checkpoints: Vec<TaskCheckpointSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionSetMemberSnapshot {
    pub execution_set_id: String,
    pub participant_node_id: String,
    pub role_hint: String,
    pub scope_hint: String,
    pub status: String,
    pub confirmed_by_node_id: Option<String>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRemoteBridgeSnapshot {
    pub announcement_id: String,
    pub network_id: String,
    pub source_node_id: String,
    pub source_scope_hint: String,
    pub detail_ref_digest: Option<String>,
    pub executor: String,
    pub profile: String,
    pub candidate_id: String,
    pub terminal_state: String,
    pub bridged_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCandidateSnapshot {
    pub proposer_node_id: String,
    pub candidate_hash: String,
    pub candidate_id: String,
    pub execution_id: String,
    pub output_resolved_at: Option<u64>,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskVerifierResultSnapshot {
    pub candidate_id: String,
    pub verifier_node_id: String,
    pub passed: bool,
    pub score: f64,
    pub verification_status: String,
    pub verifier_result_hash: String,
    pub reason_codes: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskVoteRevealSnapshot {
    pub voter_node_id: String,
    pub candidate_id: String,
    pub candidate_hash: String,
    pub vote: String,
    pub valid: bool,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCheckpointSnapshot {
    pub scope_key: String,
    pub checkpoint_id: String,
    pub artifact_path: String,
    pub observed_at: u64,
    pub checkpoint: Option<crate::types::RoundCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCoordinatorRunSnapshot {
    pub run_id: String,
    pub round_status: String,
    pub expected_executor_count: i64,
    pub completed_executors: i64,
    pub active_executors: i64,
    pub run: RunView,
    pub next_round_trigger: Option<TaskNextRoundTriggerSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNextRoundTriggerSnapshot {
    pub event_type: String,
    pub created_at: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicActivitySnapshot {
    pub generated_at: u64,
    pub subscriber_node_id: String,
    pub network_id: String,
    pub feed_key: String,
    pub scope_hint: String,
    pub messages: Vec<TopicMessageRow>,
    pub cursor: Option<TopicCursorRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicSubscriptionSnapshot {
    pub generated_at: u64,
    pub network_id: String,
    pub subscriptions: Vec<wattswarm_storage_core::storage::FeedSubscriptionRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocialProjectionSnapshot {
    pub generated_at: u64,
    pub relationships: Vec<Value>,
    pub threads: Vec<Value>,
    pub messages: Vec<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskRunSnapshotQuery {
    pub task_limit: Option<usize>,
    pub run_limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicActivityQuery {
    pub network_id: Option<String>,
    pub feed_key: String,
    pub scope_hint: String,
    pub limit: Option<usize>,
    pub subscriber_node_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicSubscriptionQuery {
    pub network_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrainTopicPublishRequest {
    pub network_id: Option<String>,
    pub feed_key: String,
    pub scope_hint: String,
    pub content: Value,
    pub reply_to_message_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrainRunSubmitRequest {
    #[serde(default)]
    pub kickoff: bool,
    pub spec: RunSubmitSpec,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrainTaskRealRequest {
    pub executor: String,
    pub profile: Option<String>,
    pub task_id: Option<String>,
    pub file_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunEventsSnapshotQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KnowledgeExportBody {
    pub task_type: Option<String>,
    pub task_id: Option<String>,
}

#[derive(Clone)]
struct WattetheriaSyncRuntime {
    state_dir: PathBuf,
    db_path: PathBuf,
    pg_url: String,
}

#[derive(Clone)]
struct WattetheriaSyncGrpcService {
    runtime: WattetheriaSyncRuntime,
}

type ProjectionStream =
    Pin<Box<dyn tokio_stream::Stream<Item = std::result::Result<ProjectionFrame, Status>> + Send>>;

impl NetworkProjectionSnapshot {
    fn frame(&self) -> Result<ProjectionFrame> {
        projection_frame("network_projection", self.generated_at, self)
    }
}

impl TaskRunProjectionSnapshot {
    fn frame(&self) -> Result<ProjectionFrame> {
        projection_frame("task_run_projection", self.generated_at, self)
    }
}

impl TopicActivitySnapshot {
    fn frame(&self) -> Result<ProjectionFrame> {
        projection_frame("topic_activity", self.generated_at, self)
    }
}

impl SocialProjectionSnapshot {
    fn frame(&self) -> Result<ProjectionFrame> {
        projection_frame("social_projection", self.generated_at, self)
    }
}

fn projection_frame<T: Serialize>(
    kind: &str,
    generated_at: u64,
    payload: &T,
) -> Result<ProjectionFrame> {
    let json_payload = serde_json::to_string(payload)?;
    Ok(ProjectionFrame {
        kind: kind.to_owned(),
        cursor: payload_cursor(&json_payload),
        generated_at,
        json_payload,
    })
}

fn payload_cursor(payload: &str) -> String {
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

fn read_node_running(state_dir: &Path) -> Result<(bool, String)> {
    let mode = resolve_node_mode(state_dir)?.as_str().to_owned();
    let state_path = node_state_path(state_dir);
    if !state_path.exists() {
        return Ok((false, mode));
    }
    let runtime_state: NodeState = serde_json::from_slice(&fs::read(state_path)?)?;
    Ok((
        runtime_state.running,
        runtime_state.mode.as_str().to_owned(),
    ))
}

fn ensure_sync_node_mode_configured(state_dir: &Path) -> Result<()> {
    let _ = require_configured_node_mode(state_dir)?;
    Ok(())
}

fn resolve_network_id(node: &crate::node::Node) -> String {
    if node.store.is_org_configured()
        && let Ok(topology) = node
            .store
            .load_network_topology_for_org(node.store.org_id())
    {
        return topology.network.network_id;
    }
    node.store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| format!("local:{}", node.node_id()))
}

fn task_projection_summary(task_id: &str, row: TaskProjectionRow) -> TaskProjectionSummary {
    TaskProjectionSummary {
        task_id: task_id.to_owned(),
        task_type: row.contract.task_type,
        epoch: row.epoch,
        terminal_state: match row.terminal_state {
            crate::types::TaskTerminalState::Open => "open".to_owned(),
            crate::types::TaskTerminalState::Expired => "expired".to_owned(),
            crate::types::TaskTerminalState::Finalized => "finalized".to_owned(),
            crate::types::TaskTerminalState::Stopped => "stopped".to_owned(),
            crate::types::TaskTerminalState::Suspended => "suspended".to_owned(),
            crate::types::TaskTerminalState::Killed => "killed".to_owned(),
        },
        committed_candidate_id: row.committed_candidate_id,
        finalized_candidate_id: row.finalized_candidate_id,
        retry_attempt: row.retry_attempt,
    }
}

fn candidate_snapshot(row: TaskCandidateRow) -> TaskCandidateSnapshot {
    TaskCandidateSnapshot {
        proposer_node_id: row.proposer_node_id,
        candidate_hash: row.candidate_hash,
        candidate_id: row.candidate.candidate_id,
        execution_id: row.candidate.execution_id,
        output_resolved_at: row.output_resolved_at,
        output: row.candidate.output,
    }
}

fn verifier_result_snapshot(row: VerifierResultRow) -> TaskVerifierResultSnapshot {
    TaskVerifierResultSnapshot {
        candidate_id: row.candidate_id,
        verifier_node_id: row.verifier_node_id,
        passed: row.result.passed,
        score: row.result.score,
        verification_status: match row.result.verification_status {
            crate::types::VerificationStatus::Passed => "passed".to_owned(),
            crate::types::VerificationStatus::Failed => "failed".to_owned(),
            crate::types::VerificationStatus::Inconclusive => "inconclusive".to_owned(),
        },
        verifier_result_hash: row.result.verifier_result_hash,
        reason_codes: row.result.reason_codes,
    }
}

fn remote_bridge_snapshot(row: LocalRemoteTaskBridgeRow) -> TaskRemoteBridgeSnapshot {
    TaskRemoteBridgeSnapshot {
        announcement_id: row.announcement_id,
        network_id: row.network_id,
        source_node_id: row.source_node_id,
        source_scope_hint: row.source_scope_hint,
        detail_ref_digest: row.detail_ref_digest,
        executor: row.executor,
        profile: row.profile,
        candidate_id: row.candidate_id,
        terminal_state: row.terminal_state,
        bridged_at: row.bridged_at,
    }
}

fn vote_reveal_snapshot(row: VoteRevealRow) -> TaskVoteRevealSnapshot {
    TaskVoteRevealSnapshot {
        voter_node_id: row.voter_node_id,
        candidate_id: row.candidate_id,
        candidate_hash: row.candidate_hash,
        vote: match row.vote {
            crate::types::VoteChoice::Approve => "approve".to_owned(),
            crate::types::VoteChoice::Reject => "reject".to_owned(),
        },
        valid: row.valid,
        created_at: row.created_at,
    }
}

fn task_coordinator_run_snapshot(
    _state_dir: &Path,
    org_id: &str,
    task_id: &str,
) -> Result<Option<TaskCoordinatorRunSnapshot>> {
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let queue = crate::run_queue::PgRunQueue::new(pg_url).for_org(org_id.to_owned());
    let Some(run_id) = queue.run_id_for_task_id(task_id)? else {
        return Ok(None);
    };
    let now = now_ms() as i64;
    queue.finalize_run_if_terminal(&run_id, now)?;
    let run = queue.run_view(&run_id)?;
    let events = queue.run_events(&run_id, 20)?;
    let next_round_trigger = events.into_iter().find_map(|event| {
        if event.event_type == "RUN_TIE_REEXPLORE_TRIGGERED"
            || event.event_type == "RUN_NULL_REEXPLORE_TRIGGERED"
        {
            Some(TaskNextRoundTriggerSnapshot {
                event_type: event.event_type,
                created_at: event.created_at,
                payload: event.payload,
            })
        } else {
            None
        }
    });
    let expected_executor_count = run.counts.created
        + run.counts.queued
        + run.counts.leased
        + run.counts.succeeded
        + run.counts.failed
        + run.counts.retry_wait
        + run.counts.cancelled
        + run.counts.remote_dispatched;
    let completed_executors = run.counts.succeeded + run.counts.failed + run.counts.cancelled;
    let active_executors = expected_executor_count - completed_executors;
    Ok(Some(TaskCoordinatorRunSnapshot {
        run_id,
        round_status: run.status.clone(),
        expected_executor_count,
        completed_executors,
        active_executors,
        run,
        next_round_trigger,
    }))
}

pub fn grpc_listen_addr_from_env() -> Option<String> {
    std::env::var("WATTSWARM_WATTETHERIA_SYNC_GRPC_LISTEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub fn default_grpc_listen_addr() -> &'static str {
    DEFAULT_GRPC_LISTEN_ADDR
}

pub fn build_network_projection_snapshot(
    state_dir: &Path,
    db_path: &Path,
) -> Result<NetworkProjectionSnapshot> {
    ensure_sync_node_mode_configured(state_dir)?;
    let startup = load_startup_config(&startup_config_path(state_dir))?;
    let node = open_node(state_dir, db_path)?;
    let (running, mode) = read_node_running(state_dir)?;
    let distribution = node
        .store
        .peer_protocol_version_distribution(&node.identity.node_id())?;
    let mut peers = node.peers().into_iter().collect::<BTreeSet<_>>();
    for peer in load_discovered_peer_records_state(state_dir)? {
        peers.insert(peer.node_id);
    }
    Ok(NetworkProjectionSnapshot {
        generated_at: now_ms(),
        node_id: node.node_id(),
        org_id: node.store.org_id().to_owned(),
        network_id: resolve_network_id(&node),
        running,
        mode,
        latitude: startup.latitude,
        longitude: startup.longitude,
        peer_protocol_distribution: distribution
            .into_iter()
            .map(|(version, count)| (version, u64::from(count)))
            .collect(),
        peers: peers.into_iter().collect(),
    })
}

pub fn build_task_run_projection_snapshot(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    task_limit: usize,
    run_limit: i64,
) -> Result<TaskRunProjectionSnapshot> {
    ensure_sync_node_mode_configured(state_dir)?;
    let node = open_node(state_dir, db_path)?;
    let recent_tasks = node
        .store
        .list_task_ids_recent(task_limit.clamp(1, 200))?
        .into_iter()
        .filter_map(|task_id| match node.store.task_projection(&task_id) {
            Ok(Some(row)) => Some(Ok(task_projection_summary(&task_id, row))),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>>>()?;
    let runs = crate::run_queue::PgRunQueue::new(pg_url.to_owned())
        .for_org(node.store.org_id().to_owned())
        .list_runs(run_limit.clamp(1, 200))?;
    Ok(TaskRunProjectionSnapshot {
        generated_at: now_ms(),
        recent_tasks,
        recent_runs: runs,
    })
}

pub fn build_task_facts_snapshot(
    state_dir: &Path,
    db_path: &Path,
    task_id: &str,
) -> Result<TaskFactsSnapshot> {
    let task_id = task_id.trim();
    if task_id.is_empty() {
        return Err(anyhow!("task_id is required"));
    }
    let node = open_node(state_dir, db_path)?;
    let observed_at = now_ms();
    let scope_id = crate::storage::local_control_scope_id(state_dir);
    let task = node
        .store
        .task_projection(task_id)?
        .map(|row| task_projection_summary(task_id, row));
    let coordinator_run = task_coordinator_run_snapshot(state_dir, node.store.org_id(), task_id)?;
    let execution_set_members = node
        .store
        .list_execution_set_members_for_task(task_id)?
        .into_iter()
        .map(|row| TaskExecutionSetMemberSnapshot {
            execution_set_id: row.execution_set_id,
            participant_node_id: row.participant_node_id,
            role_hint: row.role_hint,
            scope_hint: row.scope_hint,
            status: row.status,
            confirmed_by_node_id: row.confirmed_by_node_id,
            updated_at: row.updated_at,
        })
        .collect();
    let remote_bridges = crate::storage::local_control_store(state_dir)?
        .list_local_remote_task_bridges(&scope_id)?
        .into_iter()
        .filter(|row| row.task_id == task_id)
        .map(remote_bridge_snapshot)
        .collect();
    let candidates = node
        .store
        .list_candidates_for_task(task_id)?
        .into_iter()
        .map(candidate_snapshot)
        .collect();
    let verifier_results = node
        .store
        .list_verifier_results_for_task(task_id)?
        .into_iter()
        .map(verifier_result_snapshot)
        .collect();
    let vote_reveals = node
        .store
        .list_vote_reveals(task_id)?
        .into_iter()
        .map(vote_reveal_snapshot)
        .collect();
    let checkpoints = node
        .store
        .list_checkpoint_announcements_for_task(task_id, 64)?
        .into_iter()
        .map(|row| TaskCheckpointSnapshot {
            checkpoint: fetch_checkpoint_artifact_json::<crate::types::RoundCheckpoint>(
                state_dir,
                &node,
                &row.scope_key,
                &row.checkpoint_id,
                observed_at,
            )
            .ok(),
            scope_key: row.scope_key,
            checkpoint_id: row.checkpoint_id,
            artifact_path: row.artifact_path,
            observed_at: row.observed_at,
        })
        .collect();
    Ok(TaskFactsSnapshot {
        generated_at: observed_at,
        task_id: task_id.to_owned(),
        task,
        coordinator_run,
        execution_set_members,
        remote_bridges,
        candidates,
        verifier_results,
        vote_reveals,
        checkpoints,
    })
}

pub fn build_topic_activity_snapshot(
    state_dir: &Path,
    db_path: &Path,
    network_id: Option<&str>,
    feed_key: &str,
    scope_hint: &str,
    limit: usize,
    subscriber_node_id: Option<&str>,
) -> Result<TopicActivitySnapshot> {
    let feed_key = feed_key.trim();
    let scope_hint = scope_hint.trim();
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required"));
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required"));
    }
    ensure_sync_node_mode_configured(state_dir)?;
    let node = open_configured_node(state_dir, db_path)?;
    let network_id = network_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| resolve_network_id(&node));
    let subscriber_node_id = subscriber_node_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| local_node_id(state_dir).ok())
        .unwrap_or_else(|| node.node_id());
    let messages = node.store.list_topic_messages_page(
        &network_id,
        feed_key,
        scope_hint,
        None,
        None,
        limit.clamp(1, 200),
    )?;
    let cursor = node
        .store
        .get_topic_cursor(&network_id, &subscriber_node_id, feed_key)?;
    Ok(TopicActivitySnapshot {
        generated_at: now_ms(),
        subscriber_node_id,
        network_id,
        feed_key: feed_key.to_owned(),
        scope_hint: scope_hint.to_owned(),
        messages,
        cursor,
    })
}

pub fn build_topic_subscription_snapshot(
    state_dir: &Path,
    db_path: &Path,
    network_id: Option<&str>,
) -> Result<TopicSubscriptionSnapshot> {
    ensure_sync_node_mode_configured(state_dir)?;
    let node = open_configured_node(state_dir, db_path)?;
    let network_id = network_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| resolve_network_id(&node));
    let subscriptions = node
        .store
        .list_feed_subscriptions_for_network(&network_id)?;
    Ok(TopicSubscriptionSnapshot {
        generated_at: now_ms(),
        network_id,
        subscriptions,
    })
}

pub fn build_social_projection_snapshot(
    state_dir: &Path,
    limit: usize,
) -> Result<SocialProjectionSnapshot> {
    let relationships = load_peer_relationship_records_state(state_dir)?
        .into_iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let threads = load_peer_dm_thread_records_state(state_dir)?
        .into_iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut messages = Vec::new();
    for thread in load_peer_dm_thread_records_state(state_dir)? {
        let thread_messages = load_peer_dm_message_records_state(state_dir, &thread.thread_id)?
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        messages.extend(thread_messages);
    }
    messages.sort_by_key(|value| value.get("created_at").and_then(Value::as_u64).unwrap_or(0));
    if messages.len() > limit {
        let split_at = messages.len().saturating_sub(limit);
        messages.drain(0..split_at);
    }
    Ok(SocialProjectionSnapshot {
        generated_at: now_ms(),
        relationships,
        threads,
        messages,
    })
}

pub fn submit_brain_topic_publish(
    state_dir: &Path,
    db_path: &Path,
    req: BrainTopicPublishRequest,
) -> Result<Value> {
    let feed_key = req.feed_key.trim().to_owned();
    let scope_hint = req.scope_hint.trim().to_owned();
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required"));
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required"));
    }
    ensure_sync_node_mode_configured(state_dir)?;
    let mut node = open_configured_node(state_dir, db_path)?;
    let created_at = now_ms();
    let network_id = req
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| resolve_network_id(&node));
    let event = crate::control::emit_topic_message_with_content(
        &mut node,
        state_dir,
        &network_id,
        &feed_key,
        &scope_hint,
        req.content,
        req.reply_to_message_id,
        created_at,
    )?;
    let _ = crate::control::topic_interpretation::process_topic_interpretation_for_topic(
        &mut node,
        state_dir,
        &feed_key,
        &scope_hint,
    );
    let _ = crate::control::topic_consensus::process_structured_topic_consensus_for_topic(
        &mut node,
        state_dir,
        &feed_key,
        &scope_hint,
    );
    Ok(json!({
        "ok": true,
        "event_id": event.event_id,
        "message_id": event.event_id,
        "network_id": network_id,
        "feed_key": feed_key,
        "scope_hint": scope_hint,
    }))
}

pub fn submit_brain_run(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    req: BrainRunSubmitRequest,
) -> Result<Value> {
    run_control::submit_run(state_dir, db_path, pg_url, req.spec, req.kickoff)
}

pub fn submit_brain_task_real(
    state_dir: &Path,
    db_path: &Path,
    req: BrainTaskRealRequest,
) -> Result<Value> {
    ensure_sync_node_mode_configured(state_dir)?;
    let mut node = open_configured_node(state_dir, db_path)?;
    let profile = req.profile.unwrap_or_else(|| "default".to_owned());
    run_real_task_flow(
        &mut node,
        state_dir,
        RealTaskRunRequest {
            executor: req.executor,
            profile,
            task_id: req.task_id,
            task_file: req.file_path.map(PathBuf::from),
            task_contract: None,
        },
    )
}

pub fn build_task_decision_snapshot(
    state_dir: &Path,
    db_path: &Path,
    task_id: &str,
) -> Result<Value> {
    let task_id = task_id.trim();
    if task_id.is_empty() {
        return Err(anyhow!("task_id is required"));
    }
    ensure_sync_node_mode_configured(state_dir)?;
    let node = open_configured_node(state_dir, db_path)?;
    let task = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task not found: {task_id}"))?;
    Ok(json!({
        "ok": true,
        "task_id": task.contract.task_id,
        "committed_candidate_id": task.committed_candidate_id,
        "finalized_candidate_id": task.finalized_candidate_id,
    }))
}

pub fn build_run_result_snapshot(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    run_id: &str,
) -> Result<Value> {
    let run_id = run_id.trim();
    if run_id.is_empty() {
        return Err(anyhow!("run_id is required"));
    }
    let result = run_control::run_result(state_dir, db_path, pg_url, run_id)?;
    Ok(json!({
        "ok": true,
        "result": result,
    }))
}

pub fn build_run_events_snapshot(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    run_id: &str,
    limit: i64,
) -> Result<Value> {
    let run_id = run_id.trim();
    if run_id.is_empty() {
        return Err(anyhow!("run_id is required"));
    }
    let events = run_control::run_events(state_dir, db_path, pg_url, run_id, limit.max(1))?;
    Ok(json!({
        "ok": true,
        "events": events,
    }))
}

pub fn build_knowledge_export_snapshot(
    state_dir: &Path,
    db_path: &Path,
    req: KnowledgeExportBody,
) -> Result<Value> {
    ensure_sync_node_mode_configured(state_dir)?;
    let node = open_configured_node(state_dir, db_path)?;
    let knowledge = match (req.task_type.as_deref(), req.task_id.as_deref()) {
        (Some(task_type), None) => node.store.export_knowledge_by_task_type(task_type),
        (None, Some(task_id)) => node.store.export_knowledge_by_task(task_id),
        _ => Err(anyhow!("provide exactly one of task_type or task_id")),
    }?;
    Ok(json!({
        "ok": true,
        "knowledge": knowledge,
    }))
}

pub(crate) async fn network_snapshot_http(
    State(state): State<UiServerState>,
) -> Result<Json<NetworkProjectionSnapshot>, ApiError> {
    let state_clone = state.clone();
    let snapshot = run_blocking(move || {
        build_network_projection_snapshot(&state_clone.state_dir, &state_clone.db_path)
    })
    .await?;
    Ok(Json(snapshot))
}

pub(crate) async fn task_run_snapshot_http(
    State(state): State<UiServerState>,
    Query(query): Query<TaskRunSnapshotQuery>,
) -> Result<Json<TaskRunProjectionSnapshot>, ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let snapshot = run_blocking(move || {
        build_task_run_projection_snapshot(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            query.task_limit.unwrap_or(20),
            query.run_limit.unwrap_or(20),
        )
    })
    .await?;
    Ok(Json(snapshot))
}

pub(crate) async fn topic_activity_http(
    State(state): State<UiServerState>,
    Query(query): Query<TopicActivityQuery>,
) -> Result<Json<TopicActivitySnapshot>, ApiError> {
    let state_clone = state.clone();
    let snapshot = run_blocking(move || {
        build_topic_activity_snapshot(
            &state_clone.state_dir,
            &state_clone.db_path,
            query.network_id.as_deref(),
            &query.feed_key,
            &query.scope_hint,
            query.limit.unwrap_or(50),
            query.subscriber_node_id.as_deref(),
        )
    })
    .await?;
    Ok(Json(snapshot))
}

pub(crate) async fn topic_subscriptions_http(
    State(state): State<UiServerState>,
    Query(query): Query<TopicSubscriptionQuery>,
) -> Result<Json<TopicSubscriptionSnapshot>, ApiError> {
    let state_clone = state.clone();
    let snapshot = run_blocking(move || {
        build_topic_subscription_snapshot(
            &state_clone.state_dir,
            &state_clone.db_path,
            query.network_id.as_deref(),
        )
    })
    .await?;
    Ok(Json(snapshot))
}

pub(crate) async fn task_decision_snapshot_http(
    State(state): State<UiServerState>,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        build_task_decision_snapshot(&state_clone.state_dir, &state_clone.db_path, &task_id)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn task_facts_snapshot_http(
    State(state): State<UiServerState>,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<TaskFactsSnapshot>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        build_task_facts_snapshot(&state_clone.state_dir, &state_clone.db_path, &task_id)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn run_result_snapshot_http(
    State(state): State<UiServerState>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = run_blocking(move || {
        build_run_result_snapshot(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id,
        )
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn run_events_snapshot_http(
    State(state): State<UiServerState>,
    AxumPath(run_id): AxumPath<String>,
    Query(query): Query<RunEventsSnapshotQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let limit = query.limit.unwrap_or(50);
    let payload = run_blocking(move || {
        build_run_events_snapshot(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id,
            limit,
        )
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn knowledge_export_snapshot_http(
    State(state): State<UiServerState>,
    Json(req): Json<KnowledgeExportBody>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        build_knowledge_export_snapshot(&state_clone.state_dir, &state_clone.db_path, req)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn brain_publish_topic_http(
    State(state): State<UiServerState>,
    Json(req): Json<BrainTopicPublishRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        submit_brain_topic_publish(&state_clone.state_dir, &state_clone.db_path, req)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn brain_submit_run_http(
    State(state): State<UiServerState>,
    Json(req): Json<BrainRunSubmitRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = run_blocking(move || {
        submit_brain_run(&state_clone.state_dir, &state_clone.db_path, &pg_url, req)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn brain_run_task_real_http(
    State(state): State<UiServerState>,
    Json(req): Json<BrainTaskRealRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let result = run_blocking(move || {
        submit_brain_task_real(&state_clone.state_dir, &state_clone.db_path, req)
    })
    .await?;
    Ok(Json(json!({"ok": true, "result": result})))
}

impl WattetheriaSyncGrpcService {
    fn new(runtime: WattetheriaSyncRuntime) -> Self {
        Self { runtime }
    }

    fn poll_interval(request: &ProjectionStreamRequest) -> Duration {
        Duration::from_millis(request.poll_interval_ms.max(250))
    }
}

#[tonic::async_trait]
impl WattetheriaSyncService for WattetheriaSyncGrpcService {
    type StreamNetworkProjectionStream = ProjectionStream;
    type StreamTaskRunProjectionStream = ProjectionStream;
    type StreamTopicActivityStream = ProjectionStream;
    type StreamSocialProjectionStream = ProjectionStream;

    async fn update_startup_geo(
        &self,
        request: Request<UpdateStartupGeoRequest>,
    ) -> std::result::Result<Response<UpdateStartupGeoResponse>, Status> {
        let runtime = self.runtime.clone();
        let request = request.into_inner();
        if !request.latitude.is_finite() || !(-90.0..=90.0).contains(&request.latitude) {
            return Err(Status::invalid_argument(
                "latitude must be between -90 and 90",
            ));
        }
        if !request.longitude.is_finite() || !(-180.0..=180.0).contains(&request.longitude) {
            return Err(Status::invalid_argument(
                "longitude must be between -180 and 180",
            ));
        }
        let updated = tokio::task::spawn_blocking(move || {
            update_startup_config_geo(
                &startup_config_path(&runtime.state_dir),
                request.latitude,
                request.longitude,
            )
        })
        .await
        .map_err(|err| Status::internal(format!("join startup geo update task: {err}")))?
        .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(UpdateStartupGeoResponse { updated }))
    }

    async fn stream_network_projection(
        &self,
        request: Request<ProjectionStreamRequest>,
    ) -> std::result::Result<Response<Self::StreamNetworkProjectionStream>, Status> {
        let runtime = self.runtime.clone();
        let request = request.into_inner();
        let poll_interval = Self::poll_interval(&request);
        let stream = try_stream! {
            let mut last_cursor = String::new();
            loop {
                let runtime_clone = runtime.clone();
                let frame = tokio::task::spawn_blocking(move || -> Result<ProjectionFrame> {
                    build_network_projection_snapshot(&runtime_clone.state_dir, &runtime_clone.db_path)?.frame()
                })
                .await
                .map_err(|err| Status::internal(format!("join network projection task: {err}")))?
                .map_err(|err| Status::internal(err.to_string()))?;
                if frame.cursor != last_cursor {
                    last_cursor = frame.cursor.clone();
                    yield frame;
                }
                sleep(poll_interval).await;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn stream_task_run_projection(
        &self,
        request: Request<ProjectionStreamRequest>,
    ) -> std::result::Result<Response<Self::StreamTaskRunProjectionStream>, Status> {
        let runtime = self.runtime.clone();
        let request = request.into_inner();
        let poll_interval = Self::poll_interval(&request);
        let task_limit = request.limit.max(1) as usize;
        let run_limit = i64::from(request.limit.max(1));
        let stream = try_stream! {
            let mut last_cursor = String::new();
            loop {
                let runtime_clone = runtime.clone();
                let frame = tokio::task::spawn_blocking(move || -> Result<ProjectionFrame> {
                    build_task_run_projection_snapshot(
                        &runtime_clone.state_dir,
                        &runtime_clone.db_path,
                        &runtime_clone.pg_url,
                        task_limit,
                        run_limit,
                    )?.frame()
                })
                .await
                .map_err(|err| Status::internal(format!("join task/run projection task: {err}")))?
                .map_err(|err| Status::internal(err.to_string()))?;
                if frame.cursor != last_cursor {
                    last_cursor = frame.cursor.clone();
                    yield frame;
                }
                sleep(poll_interval).await;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn stream_topic_activity(
        &self,
        request: Request<ProjectionStreamRequest>,
    ) -> std::result::Result<Response<Self::StreamTopicActivityStream>, Status> {
        let runtime = self.runtime.clone();
        let request = request.into_inner();
        let feed_key = request.feed_key.trim().to_owned();
        let scope_hint = request.scope_hint.trim().to_owned();
        let network_id = request.network_id.trim().to_owned();
        let network_id = (!network_id.is_empty()).then_some(network_id);
        if feed_key.is_empty() {
            return Err(Status::invalid_argument("feed_key is required"));
        }
        if scope_hint.is_empty() {
            return Err(Status::invalid_argument("scope_hint is required"));
        }
        let subscriber_node_id = request.subscriber_node_id.trim().to_owned();
        let subscriber_node_id = (!subscriber_node_id.is_empty()).then_some(subscriber_node_id);
        let poll_interval = Self::poll_interval(&request);
        let limit = request.limit.max(1) as usize;
        let stream = try_stream! {
            let mut last_cursor = String::new();
            loop {
                let runtime_clone = runtime.clone();
                let feed_key_clone = feed_key.clone();
                let scope_hint_clone = scope_hint.clone();
                let network_id_clone = network_id.clone();
                let subscriber_clone = subscriber_node_id.clone();
                let frame = tokio::task::spawn_blocking(move || -> Result<ProjectionFrame> {
                    build_topic_activity_snapshot(
                        &runtime_clone.state_dir,
                        &runtime_clone.db_path,
                        network_id_clone.as_deref(),
                        &feed_key_clone,
                        &scope_hint_clone,
                        limit,
                        subscriber_clone.as_deref(),
                    )?.frame()
                })
                .await
                .map_err(|err| Status::internal(format!("join topic activity task: {err}")))?
                .map_err(|err| Status::internal(err.to_string()))?;
                if frame.cursor != last_cursor {
                    last_cursor = frame.cursor.clone();
                    yield frame;
                }
                sleep(poll_interval).await;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn stream_social_projection(
        &self,
        request: Request<ProjectionStreamRequest>,
    ) -> std::result::Result<Response<Self::StreamSocialProjectionStream>, Status> {
        let runtime = self.runtime.clone();
        let request = request.into_inner();
        let poll_interval = Self::poll_interval(&request);
        let limit = request.limit.max(1) as usize;
        let stream = try_stream! {
            let mut last_cursor = String::new();
            loop {
                let runtime_clone = runtime.clone();
                let frame = tokio::task::spawn_blocking(move || -> Result<ProjectionFrame> {
                    build_social_projection_snapshot(&runtime_clone.state_dir, limit)?.frame()
                })
                .await
                .map_err(|err| Status::internal(format!("join social projection task: {err}")))?
                .map_err(|err| Status::internal(err.to_string()))?;
                if frame.cursor != last_cursor {
                    last_cursor = frame.cursor.clone();
                    yield frame;
                }
                sleep(poll_interval).await;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

pub async fn serve_grpc(state: UiServerState, listen: String) -> Result<()> {
    let addr = listen
        .parse()
        .with_context(|| format!("parse Wattetheria sync gRPC listen address {listen}"))?;
    let runtime = WattetheriaSyncRuntime {
        state_dir: state.state_dir.clone(),
        db_path: state.db_path.clone(),
        pg_url: run_control::resolve_run_queue_pg_url(None),
    };
    let service = WattetheriaSyncGrpcService::new(runtime);
    Server::builder()
        .add_service(WattetheriaSyncServiceServer::new(service))
        .serve(addr)
        .await
        .context("serve Wattetheria sync gRPC")
}
