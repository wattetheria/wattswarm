use crate::control::{
    NodeState, RealTaskRunRequest, local_node_id, node_state_path, open_configured_node,
    require_configured_node_mode, resolve_node_mode, run_real_task_flow,
};
use crate::run_control;
use crate::run_queue::{RunSubmitSpec, RunView};
use crate::storage::storage::{TaskProjectionRow, TopicCursorRow, TopicMessageRow};
use crate::ui::UiServerState;
use anyhow::{Context, Result, anyhow};
use async_stream::try_stream;
use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
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
use proto::{ProjectionFrame, ProjectionStreamRequest};

const DEFAULT_GRPC_LISTEN_ADDR: &str = "127.0.0.1:7791";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkProjectionSnapshot {
    pub generated_at: u64,
    pub node_id: String,
    pub org_id: String,
    pub network_id: String,
    pub running: bool,
    pub mode: String,
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
pub struct TopicActivitySnapshot {
    pub generated_at: u64,
    pub subscriber_node_id: String,
    pub feed_key: String,
    pub scope_hint: String,
    pub messages: Vec<TopicMessageRow>,
    pub cursor: Option<TopicCursorRow>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskRunSnapshotQuery {
    pub task_limit: Option<usize>,
    pub run_limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicActivityQuery {
    pub feed_key: String,
    pub scope_hint: String,
    pub limit: Option<usize>,
    pub subscriber_node_id: Option<String>,
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
    let node = open_configured_node(state_dir, db_path)?;
    let (running, mode) = read_node_running(state_dir)?;
    let distribution = node
        .store
        .peer_protocol_version_distribution(&node.identity.node_id())?;
    let peers = node.peers();
    Ok(NetworkProjectionSnapshot {
        generated_at: now_ms(),
        node_id: node.node_id(),
        org_id: node.store.org_id().to_owned(),
        network_id: resolve_network_id(&node),
        running,
        mode,
        peer_protocol_distribution: distribution
            .into_iter()
            .map(|(version, count)| (version, u64::from(count)))
            .collect(),
        peers,
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
    let node = open_configured_node(state_dir, db_path)?;
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

pub fn build_topic_activity_snapshot(
    state_dir: &Path,
    db_path: &Path,
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
    let subscriber_node_id = subscriber_node_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| local_node_id(state_dir).ok())
        .unwrap_or_else(|| node.node_id());
    let messages = node.store.list_topic_messages_page(
        feed_key,
        scope_hint,
        None,
        None,
        limit.clamp(1, 200),
    )?;
    let cursor = node.store.get_topic_cursor(&subscriber_node_id, feed_key)?;
    Ok(TopicActivitySnapshot {
        generated_at: now_ms(),
        subscriber_node_id,
        feed_key: feed_key.to_owned(),
        scope_hint: scope_hint.to_owned(),
        messages,
        cursor,
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
    let network_id = req.network_id.unwrap_or_else(|| resolve_network_id(&node));
    let event = node.emit_at(
        1,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: network_id.clone(),
            feed_key: feed_key.clone(),
            scope_hint: scope_hint.clone(),
            content: req.content,
            reply_to_message_id: req.reply_to_message_id,
        }),
        created_at,
    )?;
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
) -> Result<Json<NetworkProjectionSnapshot>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let snapshot = crate::ui::run_blocking(move || {
        build_network_projection_snapshot(&state_clone.state_dir, &state_clone.db_path)
    })
    .await?;
    Ok(Json(snapshot))
}

pub(crate) async fn task_run_snapshot_http(
    State(state): State<UiServerState>,
    Query(query): Query<TaskRunSnapshotQuery>,
) -> Result<Json<TaskRunProjectionSnapshot>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let snapshot = crate::ui::run_blocking(move || {
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
) -> Result<Json<TopicActivitySnapshot>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let snapshot = crate::ui::run_blocking(move || {
        build_topic_activity_snapshot(
            &state_clone.state_dir,
            &state_clone.db_path,
            &query.feed_key,
            &query.scope_hint,
            query.limit.unwrap_or(50),
            query.subscriber_node_id.as_deref(),
        )
    })
    .await?;
    Ok(Json(snapshot))
}

pub(crate) async fn task_decision_snapshot_http(
    State(state): State<UiServerState>,
    AxumPath(task_id): AxumPath<String>,
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let payload = crate::ui::run_blocking(move || {
        build_task_decision_snapshot(&state_clone.state_dir, &state_clone.db_path, &task_id)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn run_result_snapshot_http(
    State(state): State<UiServerState>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = crate::ui::run_blocking(move || {
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
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let limit = query.limit.unwrap_or(50);
    let payload = crate::ui::run_blocking(move || {
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
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let payload = crate::ui::run_blocking(move || {
        build_knowledge_export_snapshot(&state_clone.state_dir, &state_clone.db_path, req)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn brain_publish_topic_http(
    State(state): State<UiServerState>,
    Json(req): Json<BrainTopicPublishRequest>,
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let payload = crate::ui::run_blocking(move || {
        submit_brain_topic_publish(&state_clone.state_dir, &state_clone.db_path, req)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn brain_submit_run_http(
    State(state): State<UiServerState>,
    Json(req): Json<BrainRunSubmitRequest>,
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = crate::ui::run_blocking(move || {
        submit_brain_run(&state_clone.state_dir, &state_clone.db_path, &pg_url, req)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn brain_run_task_real_http(
    State(state): State<UiServerState>,
    Json(req): Json<BrainTaskRealRequest>,
) -> Result<Json<Value>, crate::ui::ApiError> {
    let state_clone = state.clone();
    let result = crate::ui::run_blocking(move || {
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
                let subscriber_clone = subscriber_node_id.clone();
                let frame = tokio::task::spawn_blocking(move || -> Result<ProjectionFrame> {
                    build_topic_activity_snapshot(
                        &runtime_clone.state_dir,
                        &runtime_clone.db_path,
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
