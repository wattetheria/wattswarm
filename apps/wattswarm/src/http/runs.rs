use crate::control::open_node;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::run_control;
use crate::run_queue::RunSubmitSpec;
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub(crate) struct RunSubmitEnvelope {
    kickoff: Option<bool>,
    spec: Option<RunSubmitSpec>,
    run_spec: Option<RunSubmitSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RunSubmitPayload {
    Spec(RunSubmitSpec),
    Envelope(RunSubmitEnvelope),
}

#[derive(Debug, Deserialize)]
pub(crate) struct RunEventsQuery {
    limit: Option<i64>,
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

pub(crate) async fn run_submit(
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

pub(crate) async fn run_kickoff(
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

pub(crate) async fn run_watch(
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

pub(crate) async fn run_result(
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

pub(crate) async fn run_events(
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

pub(crate) async fn run_cancel(
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

pub(crate) async fn run_retry(
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

#[derive(Debug, Deserialize)]
pub(crate) struct KnowledgeExportRequest {
    task_type: Option<String>,
    task_id: Option<String>,
}

pub(crate) async fn knowledge_export(
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
