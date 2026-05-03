use crate::control::{
    ExecutorRegistryEntry, NodeState, PeerRelationshipAction, PeerRelationshipInitiator,
    RealTaskRunRequest, apply_peer_relationship_action_state, load_executor_registry_state,
    load_peer_dm_message_records_state, load_peer_dm_thread_records_state,
    load_peer_metadata_records_state, load_peer_relationship_records_state, local_node_id,
    local_peer_id, materialize_candidate_output_artifact, node_state_path, open_configured_node,
    open_node, require_configured_node_mode, resolve_node_mode, run_real_task_flow,
    save_executor_registry_state, write_node_state,
};
use crate::egress_agent::{
    EgressAgentConfig, load_egress_agent_config_state, save_egress_agent_config_state,
};
use crate::network_bridge::{
    DiagnosticFilter, enqueue_agent_payment_command, enqueue_peer_relationship_action_command,
    latest_network_observability_snapshot, list_network_diagnostics, network_service_started,
};
use crate::network_p2p::RawAgentEnvelope;
use crate::run_control;
use crate::run_queue::RunSubmitSpec;
use crate::startup_config::{
    StartupConfig, core_agent_executor_name, load_startup_config, save_startup_config,
    startup_config_path, sync_core_agent_executor,
};
use crate::startup_template::STARTUP_HTML;
use crate::swarm_dashboard_engine::{SwarmDashboardState, build_dashboard_state, tick_real_swarm};
use crate::swarm_dashboard_template::SWARM_DASHBOARD_HTML;
use crate::task_template::sample_contract;
use crate::types::{Candidate, ClaimRole, TaskContract};
use crate::ui_template::INDEX_HTML;
use crate::wattetheria_sync;
use anyhow::{Context, Result, anyhow, bail};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone)]
pub struct UiServerState {
    pub(crate) state_dir: PathBuf,
    pub(crate) db_path: PathBuf,
}

impl UiServerState {
    pub fn new(state_dir: PathBuf, db_path: PathBuf) -> Self {
        Self { state_dir, db_path }
    }
}

#[derive(Debug)]
pub(crate) struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": self.0.to_string()
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Deserialize)]
struct SampleQuery {
    task_id: String,
}

#[derive(Debug, Deserialize)]
struct SubmitTaskRequest {
    contract: Option<TaskContract>,
    file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskAnnounceRequest {
    task_id: String,
    #[serde(default)]
    announcement_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    summary: Value,
    #[serde(default)]
    detail_ref: Option<crate::types::ArtifactRef>,
}

#[derive(Debug, Deserialize)]
struct TaskClaimRequest {
    task_id: String,
    #[serde(default)]
    role: Option<ClaimRole>,
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    lease_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TaskProposeCandidateRequest {
    task_id: String,
    output: Value,
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    candidate_id: Option<String>,
    #[serde(default)]
    evidence_inline: Vec<crate::types::InlineEvidence>,
    #[serde(default)]
    evidence_refs: Vec<crate::types::ArtifactRef>,
}

#[derive(Debug, Deserialize)]
struct TaskAcceptResultRequest {
    task_id: String,
    candidate_id: String,
}

#[derive(Debug, Deserialize)]
struct ExecutorAddRequest {
    name: String,
    base_url: String,
    #[serde(default)]
    agent_event_callback_base_url: Option<String>,
    #[serde(default)]
    remote: bool,
    #[serde(default)]
    target_node_id: Option<String>,
    #[serde(default)]
    scope_hint: Option<String>,
    #[serde(default)]
    commit_plane_endpoint: Option<String>,
    #[serde(default)]
    commit_plane_token_file: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExecutorCheckRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct PeerRelationshipActionRequest {
    remote_node_id: String,
    action: PeerRelationshipAction,
    #[serde(default)]
    initiated_by: Option<PeerRelationshipInitiator>,
    #[serde(default)]
    agent_envelope: Option<RawAgentEnvelope>,
}

#[derive(Debug, Deserialize)]
struct PeerDirectMessageSendRequest {
    remote_node_id: String,
    content: Value,
    #[serde(default)]
    agent_envelope: Option<RawAgentEnvelope>,
}

#[derive(Debug, Deserialize)]
struct AgentPaymentSendRequest {
    remote_node_id: String,
    message_kind: String,
    payment: Value,
}

#[derive(Debug, Deserialize)]
struct PeerDirectMessageQuery {
    thread_id: String,
}

const PRIVATE_DM_FEED_KEY: &str = "wattswarm.dm";

fn private_dm_pair_digest(local_node_id: &str, remote_node_id: &str) -> String {
    let mut members = [
        local_node_id.trim().to_owned(),
        remote_node_id.trim().to_owned(),
    ];
    members.sort();
    crate::crypto::sha256_hex(format!("dm-v1\0{}\0{}", members[0], members[1]).as_bytes())
}

fn private_dm_thread_id(local_node_id: &str, remote_node_id: &str) -> String {
    let digest = private_dm_pair_digest(local_node_id, remote_node_id);
    format!("dm:{}", &digest[..24])
}

fn private_dm_scope_hint(local_node_id: &str, remote_node_id: &str) -> String {
    let digest = private_dm_pair_digest(local_node_id, remote_node_id);
    format!("group:dm-{}", &digest[..24])
}

fn raw_agent_envelope_to_interaction(
    envelope: &RawAgentEnvelope,
) -> crate::control::AgentInteractionEnvelope {
    crate::control::AgentInteractionEnvelope {
        protocol: envelope.protocol.clone(),
        source_agent_id: envelope.source_agent_id.clone(),
        target_agent_id: envelope.target_agent_id.clone(),
        capability: envelope.capability.clone(),
        message: serde_json::from_str(&envelope.message_json).unwrap_or_else(|_| json!({})),
        extensions: envelope
            .extensions_json
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok()),
        signature: envelope.signature.clone(),
    }
}

#[derive(Debug, Deserialize)]
struct RunRealRequest {
    executor: String,
    profile: Option<String>,
    task_id: Option<String>,
    file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunSubmitEnvelope {
    kickoff: Option<bool>,
    spec: Option<RunSubmitSpec>,
    run_spec: Option<RunSubmitSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RunSubmitPayload {
    Spec(RunSubmitSpec),
    Envelope(RunSubmitEnvelope),
}

#[derive(Debug, Deserialize)]
struct RunEventsQuery {
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DiagnosticsQuery {
    limit: Option<usize>,
    level: Option<String>,
    component: Option<String>,
    category: Option<String>,
    phase: Option<String>,
    event_id: Option<String>,
    object_id: Option<String>,
    source_node_id: Option<String>,
    search: Option<String>,
}

impl From<DiagnosticsQuery> for DiagnosticFilter {
    fn from(query: DiagnosticsQuery) -> Self {
        Self {
            limit: query.limit,
            level: query.level,
            component: query.component,
            category: query.category,
            phase: query.phase,
            event_id: query.event_id,
            object_id: query.object_id,
            source_node_id: query.source_node_id,
            search: query.search,
        }
    }
}

#[derive(Debug, Deserialize)]
struct KnowledgeExportRequest {
    task_type: Option<String>,
    task_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SwarmTickRequest {
    executor: Option<String>,
    profile: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopicMessagesQuery {
    network_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    limit: Option<usize>,
    before_created_at: Option<u64>,
    before_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopicCursorQuery {
    network_id: Option<String>,
    feed_key: String,
    subscriber_node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopicSubscriptionWriteRequest {
    network_id: Option<String>,
    subscriber_node_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    active: bool,
}

#[derive(Debug, Deserialize)]
struct TopicMessageWriteRequest {
    network_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    content: Value,
    reply_to_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StartupConfigSaveRequest {
    display_name: String,
    #[serde(default)]
    latitude: Option<f64>,
    #[serde(default)]
    longitude: Option<f64>,
    network_mode: crate::startup_config::NetworkMode,
    #[serde(default)]
    bootstrap_peers: Vec<String>,
    #[serde(default)]
    gateway_urls: Vec<String>,
    #[serde(default)]
    core_agent: Option<crate::startup_config::CoreAgentConfig>,
}

pub fn run(state_dir: PathBuf, db_path: PathBuf, listen: String) -> Result<()> {
    fs::create_dir_all(&state_dir)?;
    let node_id = local_node_id(&state_dir).ok();
    let network_enabled = crate::network_bridge::network_enabled_from_env();
    if network_enabled {
        if let Some(id) = &node_id {
            crate::udp_announce::maybe_start_listener(state_dir.clone(), id.clone());
        }
    }
    let network_started = crate::network_bridge::maybe_start_background_network_service_with_hook(
        state_dir.clone(),
        db_path.clone(),
        Some(Box::new(|node, sd| {
            let _ = crate::run_queue::network_bridge::process_pending_bridge_tasks(node, sd);
            let _ = crate::run_queue::network_bridge::process_pending_run_queue_results(sd);
            let _ = crate::control::topic_interpretation::process_topic_interpretation(node, sd);
            let _ = crate::control::topic_consensus::process_structured_topic_consensus(node, sd);
        })),
    )?;
    if network_started {
        mark_node_running_if_service_started(&state_dir, true)?;
        eprintln!("wattswarm p2p network enabled");
    } else {
        eprintln!("wattswarm p2p network disabled");
    }
    if network_enabled {
        crate::udp_announce::announce_startup("ui-startup", Some(&listen), node_id.as_deref());
    }
    let state = UiServerState { state_dir, db_path };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime for UI")?;

    runtime.block_on(async move {
        if let Some(grpc_listen) = wattetheria_sync::grpc_listen_addr_from_env() {
            let grpc_state = state.clone();
            let grpc_listen_task = grpc_listen.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    wattetheria_sync::serve_grpc(grpc_state, grpc_listen_task.clone()).await
                {
                    eprintln!(
                        "wattswarm Wattetheria sync gRPC failed on {grpc_listen_task}: {error}"
                    );
                }
            });
            eprintln!("wattswarm Wattetheria sync gRPC listening on {grpc_listen}");
        }
        let app = build_app(state);
        let listener = tokio::net::TcpListener::bind(&listen)
            .await
            .with_context(|| format!("bind UI on {}", listen))?;
        println!("wattswarm-ui listening on http://{}", listen);
        axum::serve(listener, app).await.context("serve UI")
    })
}

pub fn build_app(state: UiServerState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/diagnostics", get(diagnostics_page))
        .route("/console", get(legacy_console_redirect))
        .route("/swarm", get(swarm_page))
        .route("/api/node/up", post(node_up))
        .route("/api/node/down", post(node_down))
        .route("/api/node/status", get(node_status))
        .route("/api/network/local", get(network_local))
        .route("/api/network/bootstrap", get(network_bootstrap))
        .route("/api/startup-config", get(startup_config_get))
        .route("/api/startup-config", post(startup_config_save))
        .route("/api/peers/list", get(peers_list))
        .route("/api/peers/relationships", get(peer_relationships_list))
        .route("/api/peers/relationships", post(peer_relationships_update))
        .route("/api/peers/dm/threads", get(peer_dm_threads_list))
        .route("/api/peers/dm/messages", get(peer_dm_messages_list))
        .route("/api/peers/dm/messages", post(peer_dm_messages_send))
        .route("/api/payments/messages", post(agent_payment_send))
        .route("/api/log/head", get(log_head))
        .route("/api/log/replay", post(log_replay))
        .route("/api/log/verify", post(log_verify))
        .route("/api/diagnostics", get(diagnostics))
        .route("/api/executors/add", post(executors_add))
        .route("/api/executors/list", get(executors_list))
        .route("/api/executors/check", post(executors_check))
        .route("/api/egress-agent", get(egress_agent_get))
        .route("/api/egress-agent", post(egress_agent_save))
        .route("/api/a2a/google/agent-card", get(google_a2a_agent_card))
        .route(
            "/api/a2a/google/message/send",
            post(google_a2a_message_send),
        )
        .route("/api/task/sample", get(task_sample))
        .route("/api/task/submit", post(task_submit))
        .route("/api/task/announce", post(task_announce))
        .route("/api/task/claim", post(task_claim))
        .route("/api/task/propose-candidate", post(task_propose_candidate))
        .route("/api/task/accept-result", post(task_accept_result))
        .route("/api/task/watch/:task_id", get(task_watch))
        .route("/api/task/decision/:task_id", get(task_decision))
        .route(
            "/api/task/facts/:task_id",
            get(wattetheria_sync::task_facts_snapshot_http),
        )
        .route("/api/task/run-real", post(task_run_real))
        .route("/api/run/submit", post(run_submit))
        .route("/api/run/kickoff/:run_id", post(run_kickoff))
        .route("/api/run/watch/:run_id", get(run_watch))
        .route("/api/run/result/:run_id", get(run_result))
        .route("/api/run/events/:run_id", get(run_events))
        .route("/api/run/cancel/:run_id", post(run_cancel))
        .route("/api/run/retry/:run_id", post(run_retry))
        .route("/api/knowledge/export", post(knowledge_export))
        .route("/api/topic/messages", get(topic_messages))
        .route("/api/topic/messages", post(topic_message_post))
        .route("/api/topic/cursor", get(topic_cursor))
        .route("/api/topic/subscriptions", post(topic_subscription_post))
        .route(
            "/api/wattetheria/network/snapshot",
            get(wattetheria_sync::network_snapshot_http),
        )
        .route(
            "/api/wattetheria/task-run/snapshot",
            get(wattetheria_sync::task_run_snapshot_http),
        )
        .route(
            "/api/wattetheria/task/decision/:task_id",
            get(wattetheria_sync::task_decision_snapshot_http),
        )
        .route(
            "/api/wattetheria/task/facts/:task_id",
            get(wattetheria_sync::task_facts_snapshot_http),
        )
        .route(
            "/api/wattetheria/run/result/:run_id",
            get(wattetheria_sync::run_result_snapshot_http),
        )
        .route(
            "/api/wattetheria/run/events/:run_id",
            get(wattetheria_sync::run_events_snapshot_http),
        )
        .route(
            "/api/wattetheria/topic/activity",
            get(wattetheria_sync::topic_activity_http),
        )
        .route(
            "/api/wattetheria/knowledge/export",
            post(wattetheria_sync::knowledge_export_snapshot_http),
        )
        .route(
            "/api/wattetheria/brain/publish-topic",
            post(wattetheria_sync::brain_publish_topic_http),
        )
        .route(
            "/api/wattetheria/brain/submit-run",
            post(wattetheria_sync::brain_submit_run_http),
        )
        .route(
            "/api/wattetheria/brain/run-task-real",
            post(wattetheria_sync::brain_run_task_real_http),
        )
        .route("/api/swarm/state", get(swarm_state))
        .route("/api/swarm/tick", post(swarm_tick))
        .route("/.well-known/agent.json", get(google_a2a_well_known))
        .with_state(state)
}

pub(crate) async fn run_blocking<T, F>(f: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| anyhow!("task join error: {err}"))?
        .map_err(Into::into)
}

async fn index() -> Html<&'static str> {
    Html(STARTUP_HTML)
}

async fn diagnostics_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn legacy_console_redirect() -> Redirect {
    Redirect::permanent("/diagnostics")
}

async fn swarm_page() -> Html<&'static str> {
    Html(SWARM_DASHBOARD_HTML)
}

async fn node_up(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
        let mode = require_configured_node_mode(&state_clone.state_dir)?;
        write_node_state(&state_clone.state_dir, true, mode)?;
        let _ = crate::network_bridge::maybe_start_background_network_service_with_hook(
            state_clone.state_dir.clone(),
            state_clone.db_path.clone(),
            Some(Box::new(|node, sd| {
                let _ = crate::run_queue::network_bridge::process_pending_bridge_tasks(node, sd);
                let _ = crate::run_queue::network_bridge::process_pending_run_queue_results(sd);
                let _ =
                    crate::control::topic_interpretation::process_topic_interpretation(node, sd);
                let _ =
                    crate::control::topic_consensus::process_structured_topic_consensus(node, sd);
            })),
        )?;
        if crate::network_bridge::network_enabled_from_env() {
            crate::udp_announce::announce_startup("node-up-api", None, Some(&node.node_id()));
        }
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true})))
}

async fn node_down(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let mode = resolve_node_mode(&state.state_dir)?;
    write_node_state(&state.state_dir, false, mode)?;
    Ok(Json(json!({"ok": true})))
}

async fn node_status(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let result = run_blocking(move || -> Result<Value> {
        let state_path = node_state_path(&state_clone.state_dir);
        let runtime_state: NodeState = if state_path.exists() {
            serde_json::from_slice(&fs::read(state_path)?)?
        } else {
            NodeState {
                running: false,
                mode: resolve_node_mode(&state_clone.state_dir)?,
            }
        };
        let node_id = local_node_id(&state_clone.state_dir).unwrap_or_default();

        // Only read peer info if node has been explicitly started (has topology in DB).
        let dist = if runtime_state.running {
            match open_configured_node(&state_clone.state_dir, &state_clone.db_path) {
                Ok(node) => {
                    let peers = node
                        .store
                        .peer_protocol_version_distribution(&node.identity.node_id())?;
                    let mut dist = serde_json::Map::new();
                    for (version, count) in peers {
                        dist.insert(version, Value::from(count));
                    }
                    dist
                }
                Err(_) => serde_json::Map::new(),
            }
        } else {
            serde_json::Map::new()
        };
        Ok(json!({
            "ok": true,
            "running": runtime_state.running,
            "node_id": node_id,
            "mode": runtime_state.mode.as_str(),
            "local_protocol_version": crate::constants::LOCAL_PROTOCOL_VERSION,
            "peer_protocol_distribution": dist
        }))
    })
    .await?;
    Ok(Json(result))
}

async fn network_local(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let (peer_id, listen_addrs) = run_blocking(move || -> Result<(String, Vec<String>)> {
        let peer_id = local_peer_id(&state_clone.state_dir)?;
        let listen_addrs =
            crate::network_bridge::network_config_from_state_dir(&state_clone.state_dir)
                .listen_addrs;
        Ok((peer_id, listen_addrs))
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "network_enabled": crate::network_bridge::network_enabled_from_env(),
        "local_peer_id": peer_id,
        "listen_addrs": listen_addrs
    })))
}

async fn network_bootstrap(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let bundle = run_blocking(move || -> Result<crate::types::NetworkBootstrapBundle> {
        let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.store.load_network_bootstrap_bundle()
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "bundle": bundle,
    })))
}

async fn startup_config_get(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let config =
        run_blocking(move || load_startup_config(&startup_config_path(&state_clone.state_dir)))
            .await?;
    Ok(Json(json!({
        "ok": true,
        "config": config,
        "core_agent_executor": core_agent_executor_name()
    })))
}

async fn startup_config_save(
    State(state): State<UiServerState>,
    Json(req): Json<StartupConfigSaveRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let existing_state_dir = state_clone.state_dir.clone();
    let existing =
        run_blocking(move || load_startup_config(&startup_config_path(&existing_state_dir)))
            .await?;
    let payload = StartupConfig {
        display_name: req.display_name,
        latitude: req.latitude.or(existing.latitude),
        longitude: req.longitude.or(existing.longitude),
        network_mode: req.network_mode,
        bootstrap_peers: req.bootstrap_peers,
        gateway_urls: req.gateway_urls,
        core_agent: req.core_agent.clone().unwrap_or(existing.core_agent),
    }
    .normalized();
    payload.validate()?;
    let saved = payload.clone();
    let should_sync_core_agent = req.core_agent.is_some();
    let executor_registered = run_blocking(move || -> Result<bool> {
        let path = startup_config_path(&state_clone.state_dir);
        save_startup_config(&path, &saved)?;
        sync_runtime_node_mode_with_startup_config(&state_clone.state_dir, saved.network_mode)?;
        if should_sync_core_agent {
            sync_core_agent_executor(&state_clone.state_dir, &saved)
        } else {
            Ok(false)
        }
    })
    .await?;
    let network_started = crate::network_bridge::maybe_start_background_network_service_with_hook(
        state.state_dir.clone(),
        state.db_path.clone(),
        Some(Box::new(|node, sd| {
            let _ = crate::run_queue::network_bridge::process_pending_bridge_tasks(node, sd);
            let _ = crate::run_queue::network_bridge::process_pending_run_queue_results(sd);
            let _ = crate::control::topic_interpretation::process_topic_interpretation(node, sd);
            let _ = crate::control::topic_consensus::process_structured_topic_consensus(node, sd);
        })),
    )?;
    mark_node_running_if_service_started(&state.state_dir, network_started)?;
    Ok(Json(json!({
        "ok": true,
        "config": payload,
        "core_agent_executor": core_agent_executor_name(),
        "executor_registered": executor_registered
    })))
}

async fn peers_list(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        build_peers_list_payload(&state_clone.state_dir, &state_clone.db_path)
    })
    .await?;
    Ok(Json(payload))
}

async fn peer_relationships_list(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload =
        run_blocking(move || build_peer_relationships_payload(&state_clone.state_dir)).await?;
    Ok(Json(payload))
}

async fn peer_relationships_update(
    State(state): State<UiServerState>,
    Json(req): Json<PeerRelationshipActionRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        if network_service_started(&state_clone.state_dir) {
            let agent_envelope = req.agent_envelope.ok_or_else(|| {
                anyhow!("agent_envelope is required for network peer relationship actions")
            })?;
            enqueue_peer_relationship_action_command(
                &state_clone.state_dir,
                &req.remote_node_id,
                req.action,
                agent_envelope,
            )?;
            Ok(json!({
                "ok": true,
                "queued": true,
                "remote_node_id": req.remote_node_id,
                "action": req.action,
            }))
        } else {
            update_peer_relationship_payload(
                &state_clone.state_dir,
                &req.remote_node_id,
                req.action,
                req.initiated_by.unwrap_or(PeerRelationshipInitiator::Local),
            )
        }
    })
    .await?;
    Ok(Json(payload))
}

async fn peer_dm_threads_list(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let threads = load_peer_dm_thread_records_state(&state_clone.state_dir)?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "threads": threads,
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn peer_dm_messages_list(
    State(state): State<UiServerState>,
    Query(query): Query<PeerDirectMessageQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let messages =
            load_peer_dm_message_records_state(&state_clone.state_dir, &query.thread_id)?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "thread_id": query.thread_id,
            "messages": messages,
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn peer_dm_messages_send(
    State(state): State<UiServerState>,
    Json(req): Json<PeerDirectMessageSendRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let remote_node_id = req.remote_node_id.trim().to_owned();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let agent_envelope = req.agent_envelope.ok_or_else(|| {
            anyhow!("agent_envelope is required for private group direct messages")
        })?;
        agent_envelope.validate()?;
        let content = req.content;
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let local_node_id = node.node_id();
        let network_id = resolve_network_id(&node);
        let scope_hint = private_dm_scope_hint(&local_node_id, &remote_node_id);
        let thread_id = private_dm_thread_id(&local_node_id, &remote_node_id);
        let message_id = format!("dm-msg-{}", Uuid::new_v4());
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let control_envelope = raw_agent_envelope_to_interaction(&agent_envelope);
        let participants = {
            let mut members = vec![local_node_id.clone(), remote_node_id.clone()];
            members.sort();
            members
        };
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: network_id.clone(),
                    subscriber_node_id: local_node_id.clone(),
                    feed_key: PRIVATE_DM_FEED_KEY.to_owned(),
                    scope_hint: scope_hint.clone(),
                    gossip_kinds: vec!["messages".to_owned()],
                    active: true,
                },
            ),
            now,
        )?;
        let event = crate::control::emit_topic_message_with_content(
            &mut node,
            &state_clone.state_dir,
            &network_id,
            PRIVATE_DM_FEED_KEY,
            &scope_hint,
            json!({
                "kind": "direct_message",
                "thread_id": thread_id,
                "message_id": message_id,
                "participants": participants,
                "remote_node_id": remote_node_id,
                "content": content.clone(),
                "agent_envelope": control_envelope.clone(),
            }),
            None,
            now.saturating_add(1),
        )?;
        crate::control::save_peer_dm_thread_record_state(
            &state_clone.state_dir,
            &crate::control::PeerDmThreadRecord {
                remote_node_id: remote_node_id.clone(),
                thread_id: thread_id.clone(),
                thread_kind: crate::control::PeerDmThreadKind::Direct,
                session_state: crate::control::PeerDmSessionState::Ready,
                relationship_established_at: None,
                created_at: now,
                updated_at: now.saturating_add(1),
                last_message_at: Some(now.saturating_add(1)),
            },
        )?;
        crate::control::save_peer_dm_message_record_state(
            &state_clone.state_dir,
            &crate::control::PeerDmMessageRecord {
                thread_id: thread_id.clone(),
                message_id: message_id.clone(),
                remote_node_id: remote_node_id.clone(),
                message_kind: crate::control::PeerDmMessageKind::Message,
                direction: crate::control::PeerDmDirection::Outbound,
                delivery_state: crate::control::PeerDmDeliveryState::Delivered,
                a2a_protocol: agent_envelope.protocol.clone(),
                agent_envelope: Some(control_envelope),
                content,
                created_at: now.saturating_add(1),
                acknowledged_at: None,
            },
        )?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "queued": false,
            "remote_node_id": remote_node_id,
            "thread_id": thread_id,
            "message_id": message_id,
            "event_id": event.event_id,
            "feed_key": PRIVATE_DM_FEED_KEY,
            "scope_hint": scope_hint,
            "gossip_kinds": ["messages"],
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn agent_payment_send(
    State(state): State<UiServerState>,
    Json(req): Json<AgentPaymentSendRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        if !network_service_started(&state_clone.state_dir) {
            bail!("agent payments require the background network service to be running");
        }
        let remote_node_id = req.remote_node_id.trim().to_owned();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let message_kind = req.message_kind.trim().to_owned();
        if message_kind.is_empty() {
            bail!("message_kind is required");
        }
        enqueue_agent_payment_command(
            &state_clone.state_dir,
            &remote_node_id,
            &message_kind,
            req.payment,
        )?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "queued": true,
            "remote_node_id": remote_node_id,
            "message_kind": message_kind,
        }))
    })
    .await?;
    Ok(Json(payload))
}

fn build_peers_list_payload(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
) -> Result<Value> {
    let connected_peers =
        if let Some(peers) = crate::network_bridge::latest_connected_peer_ids(state_dir) {
            peers
        } else {
            match open_configured_node(state_dir, db_path) {
                Ok(node) => node.peers(),
                Err(_) => Vec::new(),
            }
        };
    let discovered =
        crate::control::load_discovered_peer_records_state(state_dir).unwrap_or_default();
    let metadata = load_peer_metadata_records_state(state_dir).unwrap_or_default();
    let relationships = load_peer_relationship_records_state(state_dir).unwrap_or_default();
    let mut records = BTreeMap::<String, Value>::new();
    for peer in &connected_peers {
        records.insert(
            peer.clone(),
            json!({
                "node_id": peer,
                "connected": true,
                "discovery": Value::Null,
                "metadata": Value::Null,
                "relationship": Value::Null,
            }),
        );
    }
    for record in discovered {
        records.entry(record.node_id.clone()).or_insert_with(|| {
            json!({
                "node_id": record.node_id,
                "connected": false,
                "discovery": Value::Null,
                "metadata": Value::Null,
                "relationship": Value::Null,
            })
        })["discovery"] = serde_json::to_value(&record)?;
    }
    for record in metadata {
        records.entry(record.node_id.clone()).or_insert_with(|| {
            json!({
                "node_id": record.node_id,
                "connected": false,
                "discovery": Value::Null,
                "metadata": Value::Null,
                "relationship": Value::Null,
            })
        })["metadata"] = serde_json::to_value(&record)?;
    }
    for record in relationships {
        records
            .entry(record.remote_node_id.clone())
            .or_insert_with(|| {
                json!({
                    "node_id": record.remote_node_id,
                    "connected": false,
                    "discovery": Value::Null,
                    "metadata": Value::Null,
                    "relationship": Value::Null,
                })
            })["relationship"] = serde_json::to_value(&record)?;
    }
    for peer in &connected_peers {
        if let Some(entry) = records.get_mut(peer) {
            entry["connected"] = Value::Bool(true);
        }
    }
    Ok(json!({
        "ok": true,
        "peers": connected_peers,
        "records": records.into_values().collect::<Vec<_>>(),
    }))
}

fn build_peer_relationships_payload(state_dir: &std::path::Path) -> Result<Value> {
    let relationships = load_peer_relationship_records_state(state_dir)?;
    Ok(json!({"ok": true, "relationships": relationships}))
}

fn update_peer_relationship_payload(
    state_dir: &std::path::Path,
    remote_node_id: &str,
    action: PeerRelationshipAction,
    initiated_by: PeerRelationshipInitiator,
) -> Result<Value> {
    let record =
        apply_peer_relationship_action_state(state_dir, remote_node_id, action, initiated_by)?;
    Ok(json!({"ok": true, "relationship": record}))
}

fn mark_node_running_if_service_started(state_dir: &std::path::Path, started: bool) -> Result<()> {
    if !started {
        return Ok(());
    }
    let mode = require_configured_node_mode(state_dir)?;
    write_node_state(state_dir, true, mode)
}

fn sync_runtime_node_mode_with_startup_config(
    state_dir: &std::path::Path,
    network_mode: crate::startup_config::NetworkMode,
) -> Result<()> {
    let state_path = node_state_path(state_dir);
    if !state_path.exists() {
        return Ok(());
    }
    let runtime_state: NodeState = serde_json::from_slice(&fs::read(&state_path)?)?;
    write_node_state(
        state_dir,
        runtime_state.running,
        node_mode_from_startup_mode(network_mode),
    )
}

fn node_mode_from_startup_mode(
    network_mode: crate::startup_config::NetworkMode,
) -> crate::control::NodeMode {
    match network_mode {
        crate::startup_config::NetworkMode::Local => crate::control::NodeMode::Local,
        crate::startup_config::NetworkMode::Lan => crate::control::NodeMode::Lan,
        crate::startup_config::NetworkMode::Wan => crate::control::NodeMode::Network,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_peer_relationships_payload, build_peers_list_payload,
        mark_node_running_if_service_started, update_peer_relationship_payload,
    };
    use crate::control::{NodeState, node_state_path};
    use crate::startup_config::{
        CoreAgentConfig, NetworkMode, StartupConfig, save_startup_config, startup_config_path,
    };

    #[test]
    fn marks_running_true_when_background_service_starts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        save_startup_config(
            &startup_config_path(&state_dir),
            &StartupConfig {
                display_name: "Node Agent".to_owned(),
                latitude: None,
                longitude: None,
                network_mode: NetworkMode::Wan,
                bootstrap_peers: vec!["/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWBootstrap".to_owned()],
                gateway_urls: Vec::new(),
                core_agent: CoreAgentConfig::default(),
            },
        )
        .expect("save startup config");

        mark_node_running_if_service_started(&state_dir, true).expect("mark node running");

        let state: NodeState = serde_json::from_slice(
            &std::fs::read(node_state_path(&state_dir)).expect("read node state"),
        )
        .expect("parse node state");
        assert!(state.running);
        assert_eq!(state.mode.as_str(), "network");
    }

    #[test]
    fn does_not_write_running_state_when_service_not_started() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");

        mark_node_running_if_service_started(&state_dir, false)
            .expect("skip writing node running state");

        assert!(!node_state_path(&state_dir).exists());
    }

    #[test]
    fn peers_list_payload_includes_discovery_metadata_and_relationship_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        let db_path = state_dir.join("ui.state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        crate::control::save_discovered_peer_records_state(
            &state_dir,
            &[crate::control::DiscoveredPeerRecord {
                node_id: "peer-alpha".to_owned(),
                listen_addr: Some("/ip4/203.0.113.10/tcp/4001".to_owned()),
                source_kind: "bootstrap".to_owned(),
            }],
        )
        .expect("save discovered peers");
        crate::control::save_peer_metadata_record_state(
            &state_dir,
            &crate::control::PeerMetadataRecord {
                node_id: "peer-alpha".to_owned(),
                network_id: Some("mainnet:watt-galaxy".to_owned()),
                params_version: Some(7),
                params_hash: Some("params-abc".to_owned()),
                agent_version_raw: Some(
                    "wattswarm-network-p2p|mainnet:watt-galaxy|7|params-abc".to_owned(),
                ),
                agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
                protocol_version: Some("wattswarm/1.0.0".to_owned()),
                observed_addr: Some("/ip4/198.51.100.2/tcp/4001".to_owned()),
                listen_addrs: vec!["/ip4/203.0.113.10/tcp/4001".to_owned()],
                protocols: vec!["/meshsub/1.1.0".to_owned()],
                handshake_status: "identified".to_owned(),
                last_error: None,
                contact_material: None,
                contact_material_signature: None,
                contact_material_updated_at: None,
                first_identified_at: 1_700_000_000_000,
                last_identified_at: 1_700_000_000_500,
            },
        )
        .expect("save peer metadata");
        crate::control::apply_peer_relationship_action_state(
            &state_dir,
            "peer-alpha",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("save relationship");
        let payload = build_peers_list_payload(&state_dir, &db_path).expect("build peers payload");
        let records = payload["records"].as_array().expect("records array");
        let peer = records
            .iter()
            .find(|entry| entry["node_id"].as_str() == Some("peer-alpha"))
            .expect("peer-alpha record");
        assert_eq!(peer["discovery"]["source_kind"].as_str(), Some("bootstrap"));
        assert_eq!(
            peer["metadata"]["network_id"].as_str(),
            Some("mainnet:watt-galaxy")
        );
        assert_eq!(
            peer["relationship"]["relationship_state"].as_str(),
            Some("requested")
        );
    }

    #[test]
    fn peer_relationship_payloads_cover_full_lifecycle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        let requested = update_peer_relationship_payload(
            &state_dir,
            "peer-a",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("request");
        assert_eq!(
            requested["relationship"]["relationship_state"].as_str(),
            Some("requested")
        );

        let cancelled = update_peer_relationship_payload(
            &state_dir,
            "peer-a",
            crate::control::PeerRelationshipAction::Cancel,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("cancel");
        assert_eq!(
            cancelled["relationship"]["relationship_state"].as_str(),
            Some("none")
        );
        assert_eq!(
            cancelled["relationship"]["last_action"].as_str(),
            Some("cancel")
        );

        update_peer_relationship_payload(
            &state_dir,
            "peer-b",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("request peer-b");
        let accepted = update_peer_relationship_payload(
            &state_dir,
            "peer-b",
            crate::control::PeerRelationshipAction::Accept,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("accept peer-b");
        assert_eq!(
            accepted["relationship"]["relationship_state"].as_str(),
            Some("accepted")
        );

        let removed = update_peer_relationship_payload(
            &state_dir,
            "peer-b",
            crate::control::PeerRelationshipAction::Remove,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("remove peer-b");
        assert_eq!(
            removed["relationship"]["relationship_state"].as_str(),
            Some("none")
        );
        assert_eq!(
            removed["relationship"]["last_action"].as_str(),
            Some("remove")
        );

        update_peer_relationship_payload(
            &state_dir,
            "peer-c",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("request peer-c");
        let rejected = update_peer_relationship_payload(
            &state_dir,
            "peer-c",
            crate::control::PeerRelationshipAction::Reject,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("reject peer-c");
        assert_eq!(
            rejected["relationship"]["relationship_state"].as_str(),
            Some("rejected")
        );

        let blocked = update_peer_relationship_payload(
            &state_dir,
            "peer-d",
            crate::control::PeerRelationshipAction::Block,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("block peer-d");
        assert_eq!(
            blocked["relationship"]["relationship_state"].as_str(),
            Some("blocked")
        );
        let unblocked = update_peer_relationship_payload(
            &state_dir,
            "peer-d",
            crate::control::PeerRelationshipAction::Unblock,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("unblock peer-d");
        assert_eq!(
            unblocked["relationship"]["relationship_state"].as_str(),
            Some("none")
        );
        assert_eq!(
            unblocked["relationship"]["last_action"].as_str(),
            Some("unblock")
        );

        let payload = build_peer_relationships_payload(&state_dir).expect("list relationships");
        let relationships = payload["relationships"].as_array().expect("relationships");
        assert!(relationships.iter().any(|entry| {
            entry["remote_node_id"].as_str() == Some("peer-b")
                && entry["last_action"].as_str() == Some("remove")
        }));
        assert!(relationships.iter().any(|entry| {
            entry["remote_node_id"].as_str() == Some("peer-d")
                && entry["last_action"].as_str() == Some("unblock")
        }));
    }

    #[test]
    fn peer_relationship_payloads_reject_invalid_transitions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");

        let accept_err = update_peer_relationship_payload(
            &state_dir,
            "peer-z",
            crate::control::PeerRelationshipAction::Accept,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect_err("accept without request should fail");
        assert!(
            accept_err.to_string().contains("without a pending request"),
            "unexpected error: {accept_err}"
        );

        let unblock_err = update_peer_relationship_payload(
            &state_dir,
            "peer-z",
            crate::control::PeerRelationshipAction::Unblock,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect_err("unblock without block should fail");
        assert!(
            unblock_err.to_string().contains("is not blocked"),
            "unexpected error: {unblock_err}"
        );
    }
}

async fn log_head(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let head = run_blocking(move || -> Result<u64> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.head_seq()
    })
    .await?;
    Ok(Json(json!({"ok": true, "head": head})))
}

async fn log_replay(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.replay_rebuild_projection()?;
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true, "message": "replayed"})))
}

async fn log_verify(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.verify_log()?;
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true, "message": "verified"})))
}

async fn diagnostics(
    State(state): State<UiServerState>,
    Query(query): Query<DiagnosticsQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let filter = DiagnosticFilter::from(query);
    let payload = run_blocking(move || -> Result<Value> {
        let diagnostics = list_network_diagnostics(&state_clone.state_dir, &filter)?;
        let snapshot = latest_network_observability_snapshot(&state_clone.state_dir);
        Ok(json!({
            "ok": true,
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "network_service_started": network_service_started(&state_clone.state_dir),
            "snapshot": snapshot,
            "diagnostics": diagnostics,
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn executors_add(
    State(state): State<UiServerState>,
    Json(req): Json<ExecutorAddRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let mut reg = load_executor_registry_state(&state_clone.state_dir)?;
        let kind = if req.remote {
            crate::control::ExecutorKind::Remote
        } else {
            crate::control::ExecutorKind::Local
        };
        reg.entries.retain(|e| e.name != req.name);
        reg.entries.push(ExecutorRegistryEntry {
            name: req.name,
            base_url: req.base_url,
            agent_event_callback_base_url: req.agent_event_callback_base_url,
            kind,
            target_node_id: req.target_node_id,
            scope_hint: req.scope_hint,
            commit_plane_endpoint: req.commit_plane_endpoint,
            commit_plane_token_file: req.commit_plane_token_file,
        });
        save_executor_registry_state(&state_clone.state_dir, &reg)
    })
    .await?;
    Ok(Json(json!({"ok": true})))
}

async fn executors_list(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let reg = run_blocking(move || load_executor_registry_state(&state_clone.state_dir)).await?;
    Ok(Json(json!({"ok": true, "executors": reg.entries})))
}

async fn executors_check(
    State(state): State<UiServerState>,
    Json(req): Json<ExecutorCheckRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let reg = run_blocking(move || load_executor_registry_state(&state_clone.state_dir)).await?;
    let entry = reg
        .entries
        .iter()
        .find(|e| e.name == req.name)
        .ok_or_else(|| anyhow!("executor not found: {}", req.name))?;
    let client = reqwest::Client::new();
    client
        .get(format!("{}/health", entry.base_url.trim_end_matches('/')))
        .send()
        .await?
        .error_for_status()?;
    Ok(Json(json!({"ok": true})))
}

async fn egress_agent_get(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let config =
        run_blocking(move || load_egress_agent_config_state(&state_clone.state_dir)).await?;
    Ok(Json(json!({"ok": true, "config": config})))
}

async fn egress_agent_save(
    State(state): State<UiServerState>,
    Json(config): Json<EgressAgentConfig>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let normalized = config.normalized();
    let saved = normalized.clone();
    run_blocking(move || save_egress_agent_config_state(&state_clone.state_dir, &normalized))
        .await?;
    Ok(Json(json!({"ok": true, "config": saved})))
}

async fn google_a2a_agent_card(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let card = load_google_a2a_agent_card(&state, false).await?;
    Ok(Json(card))
}

async fn google_a2a_well_known(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let card = load_google_a2a_agent_card(&state, true).await?;
    Ok(Json(card))
}

async fn google_a2a_message_send(
    State(state): State<UiServerState>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let request = serde_json::from_value::<crate::a2a::GoogleA2aSendRequest>(payload)
        .map_err(|err| anyhow!("invalid google a2a message/send request: {err}"))?;
    let response = run_blocking(move || {
        let config = load_egress_agent_config_state(&state_clone.state_dir)?;
        if !config.enabled {
            return Err(anyhow!("egress agent is disabled"));
        }
        if !matches!(config.protocol, crate::a2a::InvocationProtocol::GoogleA2a) {
            return Err(anyhow!("egress agent protocol is not google_a2a"));
        }
        let pg_url = run_control::resolve_run_queue_pg_url(None);
        crate::a2a::handle_google_message_send(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &config,
            request,
        )
    })
    .await?;
    Ok(Json(response))
}

async fn load_google_a2a_agent_card(
    state: &UiServerState,
    require_enabled: bool,
) -> Result<Value, ApiError> {
    let state_clone = state.clone();
    let card = run_blocking(move || {
        let config = load_egress_agent_config_state(&state_clone.state_dir)?;
        if require_enabled && !config.enabled {
            return Err(anyhow!("egress agent is disabled"));
        }
        let node_id = local_node_id(&state_clone.state_dir)?;
        crate::a2a::build_agent_card(&config, &node_id)
    })
    .await?;
    Ok(card)
}

async fn task_sample(
    State(state): State<UiServerState>,
    Query(q): Query<SampleQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let task_id = q.task_id.clone();
    let policy_hash = run_blocking(move || -> Result<String> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        Ok(node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))?
            .policy_hash)
    })
    .await?;
    let contract = sample_contract(&task_id, policy_hash);
    Ok(Json(json!({"ok": true, "contract": contract})))
}

async fn task_submit(
    State(state): State<UiServerState>,
    Json(req): Json<SubmitTaskRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let contract = if let Some(contract) = req.contract {
        contract
    } else if let Some(file_path) = req.file_path {
        let bytes = fs::read(file_path)?;
        serde_json::from_slice::<TaskContract>(&bytes)?
    } else {
        return Err(anyhow!("contract or file_path is required").into());
    };
    let task_id = contract.task_id.clone();
    run_blocking(move || -> Result<()> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        node.submit_task(contract, 1, now)?;
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true, "task_id": task_id})))
}

async fn task_announce(
    State(state): State<UiServerState>,
    Json(req): Json<TaskAnnounceRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let TaskAnnounceRequest {
            task_id,
            announcement_id,
            feed_key,
            scope_hint,
            summary,
            detail_ref,
        } = req;
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let announcement_id = announcement_id.unwrap_or_else(|| format!("ann-{}", Uuid::new_v4()));
        node.announce_task(
            &task_id,
            &announcement_id,
            &feed_key,
            &scope_hint,
            summary,
            detail_ref,
            task.epoch,
            now.saturating_add(1),
        )?;
        let network_id = resolve_network_id(&node);
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: network_id.clone(),
                    subscriber_node_id: node.node_id(),
                    feed_key: feed_key.clone(),
                    scope_hint: scope_hint.clone(),
                    gossip_kinds: vec!["events".to_owned()],
                    active: true,
                },
            ),
            now.saturating_add(2),
        )?;
        Ok(json!({
            "ok": true,
            "task_id": task_id,
            "announcement_id": announcement_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
            "gossip_kinds": ["events"],
            "subscribed": true
        }))
    })
    .await?;
    Ok(Json(response))
}

async fn task_claim(
    State(state): State<UiServerState>,
    Json(req): Json<TaskClaimRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&req.task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", req.task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let execution_id = req
            .execution_id
            .unwrap_or_else(|| format!("exec-p-{}", Uuid::new_v4()));
        let role = req.role.unwrap_or(ClaimRole::Propose);
        let lease_ms = req
            .lease_ms
            .unwrap_or(task.contract.assignment.claim.lease_ms);
        let lease_until = now.saturating_add(lease_ms);
        node.claim_task(
            &req.task_id,
            role,
            &execution_id,
            lease_until,
            task.epoch,
            now,
        )?;
        let subscribed = if let Some(announcement) = node
            .store
            .get_task_announcement_detail_for_task(&req.task_id)?
        {
            let network_id = resolve_network_id(&node);
            node.emit_at(
                1,
                crate::types::EventPayload::FeedSubscriptionUpdated(
                    crate::types::FeedSubscriptionUpdatedPayload {
                        network_id: network_id.clone(),
                        subscriber_node_id: node.node_id(),
                        feed_key: announcement.announcement.feed_key.clone(),
                        scope_hint: announcement.announcement.scope_hint.clone(),
                        gossip_kinds: vec!["events".to_owned()],
                        active: true,
                    },
                ),
                now.saturating_add(2),
            )?;
            true
        } else {
            false
        };
        Ok(json!({
            "ok": true,
            "task_id": req.task_id,
            "execution_id": execution_id,
            "lease_until": lease_until,
            "subscribed": subscribed
        }))
    })
    .await?;
    Ok(Json(response))
}

async fn task_propose_candidate(
    State(state): State<UiServerState>,
    Json(req): Json<TaskProposeCandidateRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let TaskProposeCandidateRequest {
            task_id,
            output,
            execution_id,
            candidate_id,
            evidence_inline,
            evidence_refs,
        } = req;
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let execution_id = execution_id.unwrap_or_else(|| format!("exec-p-{}", Uuid::new_v4()));
        let candidate_id = candidate_id.unwrap_or_else(|| format!("cand-{execution_id}"));
        let output_ref = materialize_candidate_output_artifact(
            &state_clone.state_dir,
            &node.node_id(),
            &output,
            now,
        )?;
        let candidate = Candidate {
            candidate_id: candidate_id.clone(),
            execution_id: execution_id.clone(),
            output_ref,
            output,
            evidence_inline,
            evidence_refs,
        };
        node.propose_candidate(&task_id, candidate, task.epoch, now)?;
        Ok(json!({
            "ok": true,
            "task_id": task_id,
            "execution_id": execution_id,
            "candidate_id": candidate_id
        }))
    })
    .await?;
    Ok(Json(response))
}

async fn task_accept_result(
    State(state): State<UiServerState>,
    Json(req): Json<TaskAcceptResultRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        crate::control::accept_task_result_locally(&mut node, &req.task_id, &req.candidate_id)
    })
    .await?;
    Ok(Json(response))
}

async fn task_watch(
    State(state): State<UiServerState>,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let task = run_blocking(move || -> Result<_> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.task_view(&task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", task_id))
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "task_id": task.contract.task_id,
        "state": format!("{:?}", task.terminal_state),
        "committed_candidate_id": task.committed_candidate_id,
        "finalized_candidate_id": task.finalized_candidate_id,
        "epoch": task.epoch
    })))
}

async fn task_decision(
    State(state): State<UiServerState>,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let task = run_blocking(move || -> Result<_> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.task_view(&task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", task_id))
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "task_id": task.contract.task_id,
        "committed_candidate_id": task.committed_candidate_id,
        "finalized_candidate_id": task.finalized_candidate_id
    })))
}

async fn task_run_real(
    State(state): State<UiServerState>,
    Json(req): Json<RunRealRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let output = tokio::task::spawn_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let profile = req.profile.unwrap_or_else(|| "default".to_owned());
        run_real_task_flow(
            &mut node,
            &state_clone.state_dir,
            RealTaskRunRequest {
                executor: req.executor,
                profile,
                task_id: req.task_id,
                task_file: req.file_path.map(PathBuf::from),
                task_contract: None,
            },
        )
    })
    .await
    .map_err(|err| anyhow!("task join error: {err}"))??;
    Ok(Json(json!({"ok": true, "result": output})))
}

fn unpack_run_submit_payload(payload: RunSubmitPayload) -> Result<(RunSubmitSpec, bool)> {
    match payload {
        RunSubmitPayload::Spec(spec) => Ok((spec, false)),
        RunSubmitPayload::Envelope(envelope) => {
            let spec = envelope
                .spec
                .or(envelope.run_spec)
                .ok_or_else(|| anyhow!("spec or run_spec is required"))?;
            Ok((spec, envelope.kickoff.unwrap_or(false)))
        }
    }
}

async fn run_submit(
    State(state): State<UiServerState>,
    Json(payload): Json<RunSubmitPayload>,
) -> Result<Json<Value>, ApiError> {
    let (spec, kickoff) = unpack_run_submit_payload(payload)?;
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = run_blocking(move || {
        run_control::submit_run(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            spec,
            kickoff,
        )
    })
    .await?;
    Ok(Json(payload))
}

async fn run_kickoff(
    State(state): State<UiServerState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run_id_clone = run_id.clone();
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = run_blocking(move || {
        run_control::kickoff_run(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id_clone,
        )
    })
    .await?;
    Ok(Json(payload))
}

async fn run_watch(
    State(state): State<UiServerState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run_id_clone = run_id.clone();
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let watch = run_blocking(move || {
        run_control::watch_run(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id_clone,
        )
    })
    .await?;
    Ok(Json(json!({"ok": true, "watch": watch})))
}

async fn run_result(
    State(state): State<UiServerState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run_id_clone = run_id.clone();
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let result = run_blocking(move || {
        run_control::run_result(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id_clone,
        )
    })
    .await?;
    Ok(Json(json!({"ok": true, "result": result})))
}

async fn run_events(
    State(state): State<UiServerState>,
    Path(run_id): Path<String>,
    Query(query): Query<RunEventsQuery>,
) -> Result<Json<Value>, ApiError> {
    let run_id_clone = run_id.clone();
    let limit = query.limit.unwrap_or(50).max(1);
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let events = run_blocking(move || {
        run_control::run_events(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id_clone,
            limit,
        )
    })
    .await?;
    Ok(Json(json!({"ok": true, "events": events})))
}

async fn run_cancel(
    State(state): State<UiServerState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run_id_clone = run_id.clone();
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = run_blocking(move || {
        run_control::cancel_run(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id_clone,
        )
    })
    .await?;
    Ok(Json(payload))
}

async fn run_retry(
    State(state): State<UiServerState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run_id_clone = run_id.clone();
    let state_clone = state.clone();
    let pg_url = run_control::resolve_run_queue_pg_url(None);
    let payload = run_blocking(move || {
        run_control::retry_run(
            &state_clone.state_dir,
            &state_clone.db_path,
            &pg_url,
            &run_id_clone,
        )
    })
    .await?;
    Ok(Json(payload))
}

async fn knowledge_export(
    State(state): State<UiServerState>,
    Json(req): Json<KnowledgeExportRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || -> Result<Value> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        match (req.task_type.as_deref(), req.task_id.as_deref()) {
            (Some(task_type), None) => node.store.export_knowledge_by_task_type(task_type),
            (None, Some(task_id)) => node.store.export_knowledge_by_task(task_id),
            _ => Err(anyhow!("provide exactly one of task_type or task_id")),
        }
    })
    .await?;
    Ok(Json(json!({"ok": true, "knowledge": payload})))
}

fn clamp_topic_page_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).clamp(1, 200)
}

fn resolve_subscriber_node_id(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
    explicit: Option<String>,
) -> Result<String> {
    if let Some(subscriber_node_id) = explicit {
        let trimmed = subscriber_node_id.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("subscriber_node_id cannot be empty"));
        }
        return Ok(trimmed.to_owned());
    }
    match local_node_id(state_dir) {
        Ok(node_id) => Ok(node_id),
        Err(_) => {
            let node = open_node(state_dir, db_path)?;
            Ok(node.node_id())
        }
    }
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

async fn topic_messages(
    State(state): State<UiServerState>,
    Query(query): Query<TopicMessagesQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let limit = clamp_topic_page_limit(query.limit);
    let network_id = query
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let feed_key = query.feed_key.trim().to_owned();
    let scope_hint = query.scope_hint.trim().to_owned();
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required").into());
    }
    let before_created_at = query.before_created_at;
    let before_message_id = query
        .before_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let payload = run_blocking(move || -> Result<Value> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let messages = node.store.list_topic_messages_page(
            &network_id,
            &feed_key,
            &scope_hint,
            before_created_at,
            before_message_id.as_deref(),
            limit,
        )?;
        let next_anchor = messages.last().map(|message| {
            json!({
                "before_created_at": message.created_at,
                "before_message_id": message.message_id,
            })
        });
        Ok(json!({
            "ok": true,
            "network_id": network_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
            "messages": messages,
            "next_anchor": next_anchor,
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn topic_subscription_post(
    State(state): State<UiServerState>,
    Json(req): Json<TopicSubscriptionWriteRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let feed_key = req.feed_key.trim().to_owned();
    let scope_hint = req.scope_hint.trim().to_owned();
    let network_id = req
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let subscriber_node_id = req
        .subscriber_node_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let active = req.active;
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required").into());
    }
    let payload = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let subscriber_node_id = subscriber_node_id.unwrap_or_else(|| node.node_id());
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let created_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let event = node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: network_id.clone(),
                    subscriber_node_id: subscriber_node_id.clone(),
                    feed_key: feed_key.clone(),
                    scope_hint: scope_hint.clone(),
                    gossip_kinds: vec!["messages".to_owned()],
                    active,
                },
            ),
            created_at,
        )?;
        Ok(json!({
            "ok": true,
            "event_id": event.event_id,
            "network_id": network_id,
            "subscriber_node_id": subscriber_node_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
            "gossip_kinds": ["messages"],
            "active": active,
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn topic_message_post(
    State(state): State<UiServerState>,
    Json(req): Json<TopicMessageWriteRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let feed_key = req.feed_key.trim().to_owned();
    let scope_hint = req.scope_hint.trim().to_owned();
    let network_id = req
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let reply_to_message_id = req
        .reply_to_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let content = req.content;
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required").into());
    }
    let payload = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let created_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let event = crate::control::emit_topic_message_with_content(
            &mut node,
            &state_clone.state_dir,
            &network_id,
            &feed_key,
            &scope_hint,
            content,
            reply_to_message_id,
            created_at,
        )?;
        let _ = crate::control::topic_interpretation::process_topic_interpretation_for_topic(
            &mut node,
            &state_clone.state_dir,
            &feed_key,
            &scope_hint,
        );
        let _ = crate::control::topic_consensus::process_structured_topic_consensus_for_topic(
            &mut node,
            &state_clone.state_dir,
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
    })
    .await?;
    Ok(Json(payload))
}

async fn topic_cursor(
    State(state): State<UiServerState>,
    Query(query): Query<TopicCursorQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let feed_key = query.feed_key.trim().to_owned();
    let network_id = query
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    let subscriber_node_id = query.subscriber_node_id;
    let payload = run_blocking(move || -> Result<Value> {
        let subscriber_node_id = resolve_subscriber_node_id(
            &state_clone.state_dir,
            &state_clone.db_path,
            subscriber_node_id,
        )?;
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let cursor = node
            .store
            .get_topic_cursor(&network_id, &subscriber_node_id, &feed_key)?;
        Ok(json!({
            "ok": true,
            "network_id": network_id,
            "subscriber_node_id": subscriber_node_id,
            "feed_key": feed_key,
            "cursor": cursor,
        }))
    })
    .await?;
    Ok(Json(payload))
}

async fn swarm_state(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let dashboard = run_blocking(move || -> Result<SwarmDashboardState> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        build_dashboard_state(&node)
    })
    .await?;
    Ok(Json(json!({"ok": true, "state": dashboard})))
}

async fn swarm_tick(
    State(state): State<UiServerState>,
    Json(req): Json<SwarmTickRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let executor = req
        .executor
        .unwrap_or_else(|| core_agent_executor_name().to_owned());
    let profile = req.profile.unwrap_or_else(|| "default".to_owned());
    let dashboard = tokio::task::spawn_blocking(move || -> Result<SwarmDashboardState> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        tick_real_swarm(&mut node, &state_clone.state_dir, &executor, &profile)
    })
    .await
    .map_err(|err| anyhow!("task join error: {err}"))??;
    Ok(Json(json!({"ok": true, "state": dashboard})))
}
