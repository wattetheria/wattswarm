use crate::control::{
    RealTaskRunRequest, materialize_candidate_output_artifact, open_node, run_real_task_flow,
};
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::run_queue::{PgRunQueue, StigmergyCompletionSource};
use crate::task_template::sample_contract;
use crate::types::{AgentEnvelope, Candidate, ClaimRole, TaskContract};
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
pub(crate) struct ImportTaskContractRequest {
    contract: TaskContract,
    #[serde(default)]
    epoch: Option<u64>,
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
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
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
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskClaimDecisionRequest {
    task_id: String,
    execution_id: String,
    claimer_node_id: String,
    approved: bool,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskCompleteRequest {
    task_id: String,
    execution_id: String,
    output: Value,
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskCompletionDecisionRequest {
    task_id: String,
    execution_id: String,
    approved: bool,
    #[serde(default)]
    retry_requested: bool,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskSettleRequest {
    task_id: String,
    execution_id: String,
    #[serde(default)]
    receipt: Option<Value>,
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
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
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
}

fn task_participant_agent_id(
    agent_envelope: Option<&AgentEnvelope>,
    fallback_node_id: &str,
) -> String {
    agent_envelope
        .and_then(|envelope| envelope.source_agent_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback_node_id)
        .to_owned()
}

fn current_org_run_queue_or_log(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
) -> Option<PgRunQueue> {
    let pg_url = crate::run_control::resolve_run_queue_pg_url(None);
    let node = match crate::control::open_configured_node(state_dir, db_path) {
        Ok(node) => node,
        Err(error) => {
            eprintln!("stigmergy local sink skipped: open node failed: {error:#}");
            return None;
        }
    };
    Some(PgRunQueue::new(pg_url).for_org(node.store.org_id().to_owned()))
}

fn record_local_stigmergy_claim_if_applicable(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
    task_id: &str,
    agent_id: &str,
    author_node_id: &str,
    execution_id: &str,
) {
    let Some(queue) = current_org_run_queue_or_log(state_dir, db_path) else {
        return;
    };
    if let Err(error) =
        queue.record_stigmergy_claim(task_id, agent_id, author_node_id, execution_id)
    {
        eprintln!("stigmergy local claim sink skipped for task {task_id}: {error:#}");
    }
}

fn record_local_stigmergy_completion_if_applicable(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
    task_id: &str,
    agent_id: &str,
    author_node_id: &str,
    execution_id: &str,
    output: &Value,
    source: StigmergyCompletionSource,
) {
    let Some(queue) = current_org_run_queue_or_log(state_dir, db_path) else {
        return;
    };
    if let Err(error) = queue.record_stigmergy_completion(
        task_id,
        agent_id,
        author_node_id,
        execution_id,
        output,
        source,
    ) {
        eprintln!("stigmergy local completion sink skipped for task {task_id}: {error:#}");
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskAcceptResultRequest {
    task_id: String,
    candidate_id: String,
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RunRealRequest {
    executor: String,
    profile: Option<String>,
    task_id: Option<String>,
    file_path: Option<String>,
}

fn task_contract_scope_hint(contract: &TaskContract) -> Option<String> {
    if let Some(scope_hint) = contract
        .inputs
        .get("swarm_scope")
        .and_then(Value::as_str)
        .and_then(crate::types::canonical_scope_hint)
    {
        return Some(scope_hint);
    }
    let scope = contract.inputs.get("swarm_scope")?.as_object()?;
    let kind = scope.get("kind").and_then(Value::as_str)?;
    let id = scope.get("id").and_then(Value::as_str).unwrap_or_default();
    crate::types::ScopeHint::from_kind_id(kind, id).map(|scope| scope.canonical())
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

pub(crate) async fn task_import_contract(
    State(state): State<UiServerState>,
    Json(req): Json<ImportTaskContractRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let task_id = req.contract.task_id.clone();
    let scope_hint = task_contract_scope_hint(&req.contract);
    let epoch = req.epoch.unwrap_or(1);
    run_blocking(move || -> Result<()> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.store.upsert_task_contract(&req.contract, epoch)?;
        Ok(())
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "task_id": task_id,
        "epoch": epoch,
        "scope_hint": scope_hint
    })))
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
            agent_envelope,
        } = req;
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let announcement_id = announcement_id.unwrap_or_else(|| format!("ann-{}", Uuid::new_v4()));
        node.announce_task_with_agent_envelope(
            &task_id,
            &announcement_id,
            &feed_key,
            &scope_hint,
            summary,
            detail_ref,
            agent_envelope,
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
                    provider_capabilities: Some(
                        crate::types::TopicProviderCapabilities::local_history_provider(),
                    ),
                    agent_envelope: None,
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
        let agent_envelope = req.agent_envelope;
        let author_node_id = node.node_id();
        let agent_id = task_participant_agent_id(agent_envelope.as_ref(), &author_node_id);
        let lease_ms = req
            .lease_ms
            .unwrap_or(task.contract.assignment.claim.lease_ms);
        let lease_until = now.saturating_add(lease_ms);
        let mut subscription_scope_hint = None;
        let subscription = if let Some(announcement) = node
            .store
            .get_task_announcement_detail_for_task(&req.task_id)?
        {
            let scope_hint = task_contract_scope_hint(&task.contract)
                .unwrap_or_else(|| announcement.announcement.scope_hint.clone());
            Some((announcement.announcement.feed_key, scope_hint))
        } else {
            task_contract_scope_hint(&task.contract)
                .map(|scope_hint| (format!("task.lifecycle.{}", req.task_id), scope_hint))
        };
        let subscription_feed_key = if let Some((feed_key, scope_hint)) = subscription {
            let network_id = resolve_network_id(&node);
            let subscriber_node_id = node.node_id();
            let gossip_kinds = vec!["events".to_owned()];
            let already_subscribed = node
                .store
                .get_feed_subscription(&network_id, &subscriber_node_id, &feed_key)?
                .is_some_and(|subscription| {
                    subscription.active
                        && subscription.scope_hint == scope_hint
                        && subscription.gossip_kinds == gossip_kinds
                });
            if !already_subscribed {
                node.emit_at(
                    1,
                    crate::types::EventPayload::FeedSubscriptionUpdated(
                        crate::types::FeedSubscriptionUpdatedPayload {
                            network_id: network_id.clone(),
                            subscriber_node_id,
                            feed_key: feed_key.clone(),
                            scope_hint: scope_hint.clone(),
                            gossip_kinds,
                            provider_capabilities: Some(
                                crate::types::TopicProviderCapabilities::local_history_provider(),
                            ),
                            agent_envelope: None,
                            active: true,
                        },
                    ),
                    now,
                )?;
            }
            subscription_scope_hint = Some(scope_hint);
            Some(feed_key)
        } else {
            None
        };
        node.claim_task_with_agent_envelope(
            &req.task_id,
            role,
            &execution_id,
            lease_until,
            agent_envelope,
            task.epoch,
            now.saturating_add(1),
        )?;
        record_local_stigmergy_claim_if_applicable(
            &state_clone.state_dir,
            &state_clone.db_path,
            &req.task_id,
            &agent_id,
            &author_node_id,
            &execution_id,
        );
        Ok(json!({
            "ok": true,
            "task_id": req.task_id,
            "execution_id": execution_id,
            "lease_until": lease_until,
            "subscribed": subscription_feed_key.is_some(),
            "subscription_feed_key": subscription_feed_key,
            "subscription_scope_hint": subscription_scope_hint
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn task_claim_decision(
    State(state): State<UiServerState>,
    Json(req): Json<TaskClaimDecisionRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&req.task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", req.task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        node.decide_task_claim_with_agent_envelope(
            &req.task_id,
            &req.execution_id,
            &req.claimer_node_id,
            req.approved,
            req.reason.clone(),
            req.agent_envelope,
            task.epoch,
            now,
        )?;
        Ok(json!({
            "ok": true,
            "task_id": req.task_id,
            "execution_id": req.execution_id,
            "claimer_node_id": req.claimer_node_id,
            "approved": req.approved,
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn task_complete(
    State(state): State<UiServerState>,
    Json(req): Json<TaskCompleteRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&req.task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", req.task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let agent_envelope = req.agent_envelope;
        let author_node_id = node.node_id();
        let agent_id = task_participant_agent_id(agent_envelope.as_ref(), &author_node_id);
        node.complete_task_with_agent_envelope(
            &req.task_id,
            &req.execution_id,
            req.output.clone(),
            agent_envelope,
            task.epoch,
            now,
        )?;
        record_local_stigmergy_completion_if_applicable(
            &state_clone.state_dir,
            &state_clone.db_path,
            &req.task_id,
            &agent_id,
            &author_node_id,
            &req.execution_id,
            &req.output,
            StigmergyCompletionSource::TaskCompleted,
        );
        Ok(json!({
            "ok": true,
            "task_id": req.task_id,
            "execution_id": req.execution_id,
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn task_completion_decision(
    State(state): State<UiServerState>,
    Json(req): Json<TaskCompletionDecisionRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&req.task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", req.task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        node.decide_task_completion_with_agent_envelope(
            &req.task_id,
            &req.execution_id,
            req.approved,
            req.retry_requested,
            req.reason.clone(),
            req.agent_envelope,
            task.epoch,
            now,
        )?;
        Ok(json!({
            "ok": true,
            "task_id": req.task_id,
            "execution_id": req.execution_id,
            "approved": req.approved,
            "retry_requested": req.retry_requested,
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn task_settle(
    State(state): State<UiServerState>,
    Json(req): Json<TaskSettleRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&req.task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", req.task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        node.settle_task_with_agent_envelope(
            &req.task_id,
            &req.execution_id,
            req.receipt,
            req.agent_envelope,
            task.epoch,
            now,
        )?;
        Ok(json!({
            "ok": true,
            "task_id": req.task_id,
            "execution_id": req.execution_id,
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
            agent_envelope,
        } = req;
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let task = node
            .task_view(&task_id)?
            .ok_or_else(|| anyhow!("task not found: {}", task_id))?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let execution_id = execution_id.unwrap_or_else(|| format!("exec-p-{}", Uuid::new_v4()));
        let candidate_id = candidate_id.unwrap_or_else(|| format!("cand-{execution_id}"));
        let author_node_id = node.node_id();
        let agent_id = task_participant_agent_id(agent_envelope.as_ref(), &author_node_id);
        let output_for_stigmergy = output.clone();
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
        node.propose_candidate_with_agent_envelope(
            &task_id,
            candidate,
            agent_envelope,
            task.epoch,
            now,
        )?;
        record_local_stigmergy_claim_if_applicable(
            &state_clone.state_dir,
            &state_clone.db_path,
            &task_id,
            &agent_id,
            &author_node_id,
            &execution_id,
        );
        record_local_stigmergy_completion_if_applicable(
            &state_clone.state_dir,
            &state_clone.db_path,
            &task_id,
            &agent_id,
            &author_node_id,
            &execution_id,
            &output_for_stigmergy,
            StigmergyCompletionSource::CandidateProposed,
        );
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
        crate::control::accept_task_result_locally_with_agent_envelope(
            &mut node,
            &req.task_id,
            &req.candidate_id,
            req.agent_envelope,
        )
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
