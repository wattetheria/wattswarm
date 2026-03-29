use crate::control::{
    ExecutorRegistryEntry, NodeMode, NodeState, RealTaskRunRequest, executor_registry_path,
    load_executor_registry, local_node_id, local_peer_id, node_state_path, open_node,
    resolve_node_mode, run_real_task_flow, save_executor_registry, write_node_state,
};
use crate::egress_agent::{
    EgressAgentConfig, egress_agent_config_path, load_egress_agent_config, save_egress_agent_config,
};
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
use crate::types::TaskContract;
use crate::ui_template::INDEX_HTML;
use crate::wattetheria_sync;
use anyhow::{Context, Result, anyhow};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;

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
struct ExecutorAddRequest {
    name: String,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct ExecutorCheckRequest {
    name: String,
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
    feed_key: String,
    scope_hint: String,
    limit: Option<usize>,
    before_created_at: Option<u64>,
    before_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopicCursorQuery {
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
    network_mode: crate::startup_config::NetworkMode,
    core_agent: crate::startup_config::CoreAgentConfig,
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
    if crate::network_bridge::maybe_start_background_network_service(
        state_dir.clone(),
        db_path.clone(),
    )? {
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
        .route("/console", get(console_page))
        .route("/swarm", get(swarm_page))
        .route("/api/node/up", post(node_up))
        .route("/api/node/down", post(node_down))
        .route("/api/node/status", get(node_status))
        .route("/api/network/local", get(network_local))
        .route("/api/startup-config", get(startup_config_get))
        .route("/api/startup-config", post(startup_config_save))
        .route("/api/peers/list", get(peers_list))
        .route("/api/log/head", get(log_head))
        .route("/api/log/replay", post(log_replay))
        .route("/api/log/verify", post(log_verify))
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
        .route("/api/task/watch/:task_id", get(task_watch))
        .route("/api/task/decision/:task_id", get(task_decision))
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

async fn console_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn swarm_page() -> Html<&'static str> {
    Html(SWARM_DASHBOARD_HTML)
}

async fn node_up(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let mode = resolve_node_mode(&state_clone.state_dir)?;
        write_node_state(&state_clone.state_dir, true, mode)?;
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
    let (running, node_id, dist) = run_blocking(
        move || -> Result<(bool, String, serde_json::Map<String, Value>)> {
            let state_path = node_state_path(&state_clone.state_dir);
            let runtime_state: NodeState = if state_path.exists() {
                serde_json::from_slice(&fs::read(state_path)?)?
            } else {
                NodeState {
                    running: false,
                    mode: NodeMode::Local,
                }
            };
            let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
            let peers = node
                .store
                .peer_protocol_version_distribution(&node.identity.node_id())?;
            let mut dist = serde_json::Map::new();
            for (version, count) in peers {
                dist.insert(version, Value::from(count));
            }
            Ok((runtime_state.running, node.node_id(), dist))
        },
    )
    .await?;
    Ok(Json(json!({
        "ok": true,
        "running": running,
        "node_id": node_id,
        "mode": resolve_node_mode(&state.state_dir)?.as_str(),
        "local_protocol_version": crate::constants::LOCAL_PROTOCOL_VERSION,
        "peer_protocol_distribution": dist
    })))
}

async fn network_local(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let (peer_id, listen_addrs) = run_blocking(move || -> Result<(String, Vec<String>)> {
        let peer_id = local_peer_id(&state_clone.state_dir)?;
        let listen_addrs = crate::network_bridge::network_config_from_env().listen_addrs;
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
    let payload = StartupConfig {
        display_name: req.display_name,
        network_mode: req.network_mode,
        core_agent: req.core_agent,
    }
    .normalized();
    payload.validate()?;
    let saved = payload.clone();
    let executor_registered = run_blocking(move || -> Result<bool> {
        let path = startup_config_path(&state_clone.state_dir);
        save_startup_config(&path, &saved)?;
        sync_core_agent_executor(&state_clone.state_dir, &saved)
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "config": payload,
        "core_agent_executor": core_agent_executor_name(),
        "executor_registered": executor_registered
    })))
}

async fn peers_list(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let peers = run_blocking(move || -> Result<Vec<String>> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        Ok(node.peers())
    })
    .await?;
    Ok(Json(json!({"ok": true, "peers": peers})))
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

async fn executors_add(
    State(state): State<UiServerState>,
    Json(req): Json<ExecutorAddRequest>,
) -> Result<Json<Value>, ApiError> {
    let reg_path = executor_registry_path(&state.state_dir);
    let mut reg = load_executor_registry(&reg_path)?;
    reg.entries.retain(|e| e.name != req.name);
    reg.entries.push(ExecutorRegistryEntry {
        name: req.name.clone(),
        base_url: req.base_url.clone(),
    });
    save_executor_registry(&reg_path, &reg)?;
    Ok(Json(json!({"ok": true})))
}

async fn executors_list(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let reg = load_executor_registry(&executor_registry_path(&state.state_dir))?;
    Ok(Json(json!({"ok": true, "executors": reg.entries})))
}

async fn executors_check(
    State(state): State<UiServerState>,
    Json(req): Json<ExecutorCheckRequest>,
) -> Result<Json<Value>, ApiError> {
    let reg = load_executor_registry(&executor_registry_path(&state.state_dir))?;
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
    let config = run_blocking(move || {
        load_egress_agent_config(&egress_agent_config_path(&state_clone.state_dir))
    })
    .await?;
    Ok(Json(json!({"ok": true, "config": config})))
}

async fn egress_agent_save(
    State(state): State<UiServerState>,
    Json(config): Json<EgressAgentConfig>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let normalized = config.normalized();
    let saved = normalized.clone();
    run_blocking(move || {
        save_egress_agent_config(
            &egress_agent_config_path(&state_clone.state_dir),
            &normalized,
        )
    })
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
        let config = load_egress_agent_config(&egress_agent_config_path(&state_clone.state_dir))?;
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
        let config = load_egress_agent_config(&egress_agent_config_path(&state_clone.state_dir))?;
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
        let messages = node.store.list_topic_messages_page(
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
    let network_id = req.network_id.as_deref().map(str::trim).map(str::to_owned);
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
    let network_id = req.network_id.as_deref().map(str::trim).map(str::to_owned);
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
        let event = node.emit_at(
            1,
            crate::types::EventPayload::TopicMessagePosted(
                crate::types::TopicMessagePostedPayload {
                    network_id: network_id.clone(),
                    feed_key: feed_key.clone(),
                    scope_hint: scope_hint.clone(),
                    content,
                    reply_to_message_id,
                },
            ),
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
        let cursor = node
            .store
            .get_topic_cursor(&subscriber_node_id, &feed_key)?;
        Ok(json!({
            "ok": true,
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
    let executor = req.executor.unwrap_or_else(|| "rt".to_owned());
    let profile = req.profile.unwrap_or_else(|| "default".to_owned());
    let dashboard = tokio::task::spawn_blocking(move || -> Result<SwarmDashboardState> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        tick_real_swarm(&mut node, &state_clone.state_dir, &executor, &profile)
    })
    .await
    .map_err(|err| anyhow!("task join error: {err}"))??;
    Ok(Json(json!({"ok": true, "state": dashboard})))
}
