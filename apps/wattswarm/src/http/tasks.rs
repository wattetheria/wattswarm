use crate::control::{
    RealTaskRunRequest, materialize_candidate_output_artifact, open_node, run_real_task_flow,
};
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::task_template::sample_contract;
use crate::types::{Candidate, ClaimRole, TaskContract};
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub(crate) struct SampleQuery {
    task_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubmitTaskRequest {
    contract: Option<TaskContract>,
    file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskAnnounceRequest {
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
pub(crate) struct TaskClaimRequest {
    task_id: String,
    #[serde(default)]
    role: Option<ClaimRole>,
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    lease_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskProposeCandidateRequest {
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
pub(crate) struct TaskAcceptResultRequest {
    task_id: String,
    candidate_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RunRealRequest {
    executor: String,
    profile: Option<String>,
    task_id: Option<String>,
    file_path: Option<String>,
}

pub(crate) async fn task_sample(
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

pub(crate) async fn task_submit(
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

pub(crate) async fn task_announce(
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

pub(crate) async fn task_claim(
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

pub(crate) async fn task_propose_candidate(
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

pub(crate) async fn task_accept_result(
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

pub(crate) async fn task_watch(
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

pub(crate) async fn task_decision(
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

pub(crate) async fn task_run_real(
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
