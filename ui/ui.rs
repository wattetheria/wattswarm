use crate::control::{
    ExecutorRegistryEntry, NodeState, RealTaskRunRequest, executor_registry_path,
    load_executor_registry, local_node_id, open_node, run_real_task_flow, save_executor_registry,
};
use crate::swarm_dashboard_engine::{SwarmDashboardState, build_dashboard_state, tick_real_swarm};
use crate::swarm_dashboard_template::SWARM_DASHBOARD_HTML;
use crate::task_template::sample_contract;
use crate::types::TaskContract;
use crate::ui_template::INDEX_HTML;
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
    state_dir: PathBuf,
    db_path: PathBuf,
}

impl UiServerState {
    pub fn new(state_dir: PathBuf, db_path: PathBuf) -> Self {
        Self { state_dir, db_path }
    }
}

#[derive(Debug)]
struct ApiError(anyhow::Error);

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
struct KnowledgeExportRequest {
    task_type: Option<String>,
    task_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SwarmTickRequest {
    executor: Option<String>,
    profile: Option<String>,
}

pub fn run(state_dir: PathBuf, db_path: PathBuf, listen: String) -> Result<()> {
    fs::create_dir_all(&state_dir)?;
    let node_id = local_node_id(&state_dir).ok();
    if let Some(id) = &node_id {
        crate::udp_announce::maybe_start_listener(state_dir.clone(), id.clone());
    }
    crate::udp_announce::announce_startup("ui-startup", Some(&listen), node_id.as_deref());
    let state = UiServerState { state_dir, db_path };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime for UI")?;

    runtime.block_on(async move {
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
        .route("/swarm", get(swarm_page))
        .route("/api/node/up", post(node_up))
        .route("/api/node/down", post(node_down))
        .route("/api/node/status", get(node_status))
        .route("/api/peers/list", get(peers_list))
        .route("/api/log/head", get(log_head))
        .route("/api/log/replay", post(log_replay))
        .route("/api/log/verify", post(log_verify))
        .route("/api/executors/add", post(executors_add))
        .route("/api/executors/list", get(executors_list))
        .route("/api/executors/check", post(executors_check))
        .route("/api/task/sample", get(task_sample))
        .route("/api/task/submit", post(task_submit))
        .route("/api/task/watch/:task_id", get(task_watch))
        .route("/api/task/decision/:task_id", get(task_decision))
        .route("/api/task/run-real", post(task_run_real))
        .route("/api/knowledge/export", post(knowledge_export))
        .route("/api/swarm/state", get(swarm_state))
        .route("/api/swarm/tick", post(swarm_tick))
        .with_state(state)
}

async fn run_blocking<T, F>(f: F) -> Result<T, ApiError>
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
    Html(INDEX_HTML)
}

async fn swarm_page() -> Html<&'static str> {
    Html(SWARM_DASHBOARD_HTML)
}

async fn node_up(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    run_blocking(move || -> Result<()> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let state_path = state_clone.state_dir.join("node_state.json");
        fs::write(
            state_path,
            serde_json::to_vec_pretty(&NodeState { running: true })?,
        )?;
        crate::udp_announce::announce_startup("node-up-api", None, Some(&node.node_id()));
        Ok(())
    })
    .await?;
    Ok(Json(json!({"ok": true})))
}

async fn node_down(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_path = state.state_dir.join("node_state.json");
    fs::write(
        state_path,
        serde_json::to_vec_pretty(&NodeState { running: false })?,
    )?;
    Ok(Json(json!({"ok": true})))
}

async fn node_status(State(state): State<UiServerState>) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let (running, dist) =
        run_blocking(move || -> Result<(bool, serde_json::Map<String, Value>)> {
            let state_path = state_clone.state_dir.join("node_state.json");
            let runtime_state: NodeState = if state_path.exists() {
                serde_json::from_slice(&fs::read(state_path)?)?
            } else {
                NodeState { running: false }
            };
            let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
            let peers = node
                .store
                .peer_protocol_version_distribution(&node.identity.node_id())?;
            let mut dist = serde_json::Map::new();
            for (version, count) in peers {
                dist.insert(version, Value::from(count));
            }
            Ok((runtime_state.running, dist))
        })
        .await?;
    Ok(Json(json!({
        "ok": true,
        "running": running,
        "local_protocol_version": crate::constants::LOCAL_PROTOCOL_VERSION,
        "peer_protocol_distribution": dist
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
