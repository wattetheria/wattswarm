use super::*;

fn agent_event_status_label(status: wattswarm_protocol::types::AgentEventStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "pending".to_owned())
}

fn load_commit_plane_token(token_file: &str) -> Result<String> {
    let token = std::fs::read_to_string(token_file)
        .with_context(|| format!("read Wattetheria control token file: {token_file}"))?;
    let token = token.trim().to_owned();
    if token.is_empty() {
        bail!("Wattetheria control token file is empty: {token_file}");
    }
    Ok(token)
}

fn route_agent_decision_to_wattetheria(
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
    entry: &crate::control::ExecutorRegistryEntry,
) -> Result<(i64, String)> {
    let commit_plane_endpoint = entry
        .commit_plane_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("core-agent executor missing commit_plane_endpoint"))?;
    let commit_plane_token_file = entry
        .commit_plane_token_file
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("core-agent executor missing commit_plane_token_file"))?;
    let token = load_commit_plane_token(commit_plane_token_file)?;
    let client = reqwest::blocking::Client::new();
    let response = client
        .post(format!(
            "{}/v1/agent-actions/commit",
            commit_plane_endpoint.trim_end_matches('/')
        ))
        .header("authorization", format!("Bearer {token}"))
        .header("x-agent-event-id", &event.event_id)
        .header("x-agent-decision-id", &decision.decision_id)
        .json(&json!({
            "event": event,
            "decision": decision,
        }))
        .send()
        .context("POST Wattetheria agent action commit")?;
    let status_code = response.status().as_u16() as i64;
    let body = response.text().unwrap_or_default();
    if status_code < 200 || status_code >= 300 {
        bail!("Wattetheria agent action commit returned {status_code}: {body}");
    }
    Ok((status_code, body))
}

fn required_event_payload_string(
    payload: &Value,
    key: &str,
    event_type: &wattswarm_protocol::types::AgentEventType,
) -> Result<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("agent event {:?} missing {}", event_type, key))
}

fn route_topic_message_reply_to_wattswarm(
    state_dir: &Path,
    db_path: &Path,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    match decision.action.as_str() {
        "ignore" => {
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "ignored",
                    "store": "wattswarm",
                    "event_id": event.event_id,
                })
                .to_string(),
            ));
        }
        "reply" => {}
        action => {
            bail!(
                "unsupported wattswarm_direct topic action {action} for event_type={:?}",
                event.event_type
            );
        }
    }

    let feed_key = required_event_payload_string(&event.payload, "feed_key", &event.event_type)?;
    let scope_hint =
        required_event_payload_string(&event.payload, "scope_hint", &event.event_type)?;
    let content = decision
        .payload
        .get("content")
        .cloned()
        .ok_or_else(|| anyhow!("topic reply decision requires payload.content"))?;
    let reply_to_message_id = decision
        .payload
        .get("reply_to_message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            event
                .payload
                .get("message_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });

    let mut node = crate::control::open_node(state_dir, db_path)?;
    let current_network_id = current_network_context_id(&node);
    let network_id = event
        .payload
        .get("network_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| *value == current_network_id)
        .map(ToOwned::to_owned)
        .unwrap_or(current_network_id);
    let created_at = observed_at_ms();
    let emitted = crate::control::emit_topic_message_with_content(
        &mut node,
        state_dir,
        &network_id,
        &feed_key,
        &scope_hint,
        content,
        reply_to_message_id,
        created_at,
    )?;
    let _ = crate::topic_interpretation::process_topic_interpretation_for_topic(
        &mut node,
        state_dir,
        &feed_key,
        &scope_hint,
    );
    let _ = crate::topic_consensus::process_structured_topic_consensus_for_topic(
        &mut node,
        state_dir,
        &feed_key,
        &scope_hint,
    );

    Ok((
        201,
        json!({
            "ok": true,
            "status": "posted",
            "store": "wattswarm",
            "message_id": emitted.event_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
        })
        .to_string(),
    ))
}

fn route_task_result_to_wattswarm(
    state_dir: &Path,
    db_path: &Path,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    let task_id = required_event_payload_string(&event.payload, "task_id", &event.event_type)?;
    let event_kind =
        required_event_payload_string(&event.payload, "event_kind", &event.event_type)?;
    let mut node = crate::control::open_node(state_dir, db_path)?;
    let task = node
        .task_view(&task_id)?
        .ok_or_else(|| anyhow!("task not found for task_result_received: {task_id}"))?;
    let now = observed_at_ms();

    match decision.action.as_str() {
        "accept_result" => {
            if event_kind == "candidate_proposed" {
                let candidate_id = required_event_payload_string(
                    &event.payload,
                    "candidate_id",
                    &event.event_type,
                )?;
                let result =
                    crate::control::accept_task_result_locally(&mut node, &task_id, &candidate_id)?;
                return Ok((200, result.to_string()));
            }
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "observed",
                    "store": "wattswarm",
                    "task_id": task_id,
                    "event_kind": event_kind,
                })
                .to_string(),
            ));
        }
        "reject_result" | "request_retry" => {
            let next_attempt = task.retry_attempt.saturating_add(1);
            let run_at = now.saturating_add(task.contract.acceptance.vote.reveal_deadline_ms);
            node.schedule_retry(&task_id, next_attempt, run_at, task.epoch, now)?;
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "retry_scheduled",
                    "store": "wattswarm",
                    "task_id": task_id,
                    "attempt": next_attempt,
                    "run_at": run_at,
                })
                .to_string(),
            ));
        }
        "inspect_task" => {
            return Ok((
                200,
                json!({
                    "ok": true,
                    "status": "observed",
                    "store": "wattswarm",
                    "task_id": task_id,
                })
                .to_string(),
            ));
        }
        action => {
            bail!(
                "unsupported wattswarm_direct task result action {action} for event_type={:?}",
                event.event_type
            );
        }
    }
}

fn route_task_claim_to_wattswarm(
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    let task_id = required_event_payload_string(&event.payload, "task_id", &event.event_type)?;
    match decision.action.as_str() {
        "decide_claim" | "inspect_task" => Ok((
            200,
            json!({
                "ok": true,
                "status": "recorded",
                "store": "wattswarm",
                "task_id": task_id,
                "detail": "task claim lifecycle is already authoritative in wattswarm; no secondary semantic commit is applied here"
            })
            .to_string(),
        )),
        action => bail!(
            "unsupported wattswarm_direct task claim action {action} for event_type={:?}",
            event.event_type
        ),
    }
}

fn route_agent_decision_to_wattswarm(
    state_dir: &Path,
    db_path: Option<&Path>,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
) -> Result<(i64, String)> {
    match event.event_type {
        wattswarm_protocol::types::AgentEventType::TopicMessageRequiresReply => {
            let db_path = db_path.ok_or_else(|| {
                anyhow!("wattswarm_direct topic routing requires a configured db_path")
            })?;
            route_topic_message_reply_to_wattswarm(state_dir, db_path, event, decision)
        }
        wattswarm_protocol::types::AgentEventType::TaskResultReceived => {
            let db_path = db_path.ok_or_else(|| {
                anyhow!("wattswarm_direct task result routing requires a configured db_path")
            })?;
            route_task_result_to_wattswarm(state_dir, db_path, event, decision)
        }
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived => {
            route_task_claim_to_wattswarm(event, decision)
        }
        _ => bail!(
            "wattswarm_direct decision routing is not supported yet for event_type={:?}",
            event.event_type
        ),
    }
}

fn route_agent_decision(
    state_dir: &Path,
    db_path: Option<&Path>,
    event: &wattswarm_protocol::types::AgentEvent,
    decision: &wattswarm_protocol::types::AgentDecision,
    entry: &crate::control::ExecutorRegistryEntry,
) -> Result<Option<(String, i64, String)>> {
    match decision.route {
        wattswarm_protocol::types::AgentDecisionRoute::Noop => Ok(None),
        wattswarm_protocol::types::AgentDecisionRoute::WattetheriaCommit => {
            let (status_code, body) = route_agent_decision_to_wattetheria(event, decision, entry)?;
            Ok(Some((
                entry
                    .commit_plane_endpoint
                    .clone()
                    .unwrap_or_else(|| String::from("wattetheria")),
                status_code,
                body,
            )))
        }
        wattswarm_protocol::types::AgentDecisionRoute::WattswarmDirect => {
            let (status_code, body) =
                route_agent_decision_to_wattswarm(state_dir, db_path, event, decision)?;
            Ok(Some(("wattswarm:direct".to_owned(), status_code, body)))
        }
    }
}

fn topic_message_kind(content: &Value) -> Option<&str> {
    content
        .as_object()
        .and_then(|object| object.get("kind"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(super) fn topic_message_requires_reply(content: &Value) -> bool {
    !matches!(
        topic_message_kind(content),
        Some("proposal" | "stance" | "interpreted_stance" | "consensus_result")
    )
}

pub(super) fn task_claim_agent_event(
    node: &Node,
    event: &crate::types::Event,
    payload: &crate::types::ClaimPayload,
) -> Result<wattswarm_protocol::types::AgentEvent> {
    let task_inputs = node
        .task_view(&payload.task_id)?
        .map(|task| task.contract.inputs)
        .unwrap_or(Value::Null);
    Ok(build_agent_event(
        wattswarm_protocol::types::AgentEventType::TaskClaimReceived,
        wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
        Some(event.author_node_id.clone()),
        None,
        json!({
            "event_id": &event.event_id,
            "event_kind": "task_claimed",
            "task_id": &payload.task_id,
            "claimer_node_id": &payload.claimer_node_id,
            "execution_id": &payload.execution_id,
            "role": payload.role,
            "lease_until": payload.lease_until,
            "task_inputs": task_inputs,
            "created_at": event.created_at,
        }),
        false,
        vec!["inspect_task".to_owned(), "decide_claim".to_owned()],
        Some(payload.task_id.clone()),
        Some(format!(
            "task_claim:{}:{}",
            payload.task_id, payload.execution_id
        )),
    ))
}

pub(super) fn task_result_agent_event(
    node: &Node,
    event: &crate::types::Event,
) -> Result<Option<wattswarm_protocol::types::AgentEvent>> {
    match &event.payload {
        crate::types::EventPayload::CandidateProposed(payload) => {
            let candidate_output = node
                .store
                .get_candidate_by_id(&payload.task_id, &payload.candidate.candidate_id)?
                .map(|candidate| candidate.output)
                .unwrap_or(Value::Null);
            Ok(Some(build_agent_event(
                wattswarm_protocol::types::AgentEventType::TaskResultReceived,
                wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
                Some(event.author_node_id.clone()),
                None,
                json!({
                    "event_id": &event.event_id,
                    "event_kind": "candidate_proposed",
                    "task_id": &payload.task_id,
                    "candidate_id": &payload.candidate.candidate_id,
                    "execution_id": &payload.candidate.execution_id,
                    "candidate_hash": &payload.candidate.output_ref.digest,
                    "candidate_output": candidate_output,
                    "created_at": event.created_at,
                }),
                false,
                vec![
                    "inspect_task".to_owned(),
                    "accept_result".to_owned(),
                    "reject_result".to_owned(),
                    "request_retry".to_owned(),
                ],
                Some(payload.task_id.clone()),
                Some(format!(
                    "task_result:{}:{}",
                    payload.task_id, payload.candidate.candidate_id
                )),
            )))
        }
        crate::types::EventPayload::DecisionFinalized(payload) => Ok(Some(build_agent_event(
            wattswarm_protocol::types::AgentEventType::TaskResultReceived,
            wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
            Some(event.author_node_id.clone()),
            None,
            json!({
                "event_id": &event.event_id,
                "event_kind": "decision_finalized",
                "task_id": &payload.task_id,
                "candidate_id": &payload.candidate_id,
                "winning_candidate_hash": &payload.winning_candidate_hash,
                "finality_proof": &payload.finality_proof,
                "created_at": event.created_at,
            }),
            false,
            vec!["inspect_task".to_owned(), "accept_result".to_owned()],
            Some(payload.task_id.clone()),
            Some(format!(
                "task_result:{}:finalized:{}",
                payload.task_id, payload.candidate_id
            )),
        ))),
        crate::types::EventPayload::TaskError(payload) => Ok(Some(build_agent_event(
            wattswarm_protocol::types::AgentEventType::TaskResultReceived,
            wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
            Some(event.author_node_id.clone()),
            None,
            json!({
                "event_id": &event.event_id,
                "event_kind": "task_error",
                "task_id": &payload.task_id,
                "reason": payload.reason,
                "reason_codes": &payload.reason_codes,
                "message": &payload.message,
                "custom_reason_namespace": &payload.custom_reason_namespace,
                "custom_reason_code": &payload.custom_reason_code,
                "custom_reason_message": &payload.custom_reason_message,
                "created_at": event.created_at,
            }),
            false,
            vec!["inspect_task".to_owned(), "request_retry".to_owned()],
            Some(payload.task_id.clone()),
            Some(format!(
                "task_result:{}:error:{}",
                payload.task_id, event.event_id
            )),
        ))),
        crate::types::EventPayload::TaskRetryScheduled(payload) => Ok(Some(build_agent_event(
            wattswarm_protocol::types::AgentEventType::TaskResultReceived,
            wattswarm_protocol::types::AgentEventSourceKind::TaskLifecycle,
            Some(event.author_node_id.clone()),
            None,
            json!({
                "event_id": &event.event_id,
                "event_kind": "task_retry_scheduled",
                "task_id": &payload.task_id,
                "attempt": payload.attempt,
                "run_at": payload.run_at,
                "created_at": event.created_at,
            }),
            false,
            vec!["inspect_task".to_owned()],
            Some(payload.task_id.clone()),
            Some(format!(
                "task_result:{}:retry:{}",
                payload.task_id, payload.attempt
            )),
        ))),
        _ => Ok(None),
    }
}

pub(super) fn topic_message_agent_event(
    node: &Node,
    event: &crate::types::Event,
    payload: &crate::types::TopicMessagePostedPayload,
) -> Result<Option<wattswarm_protocol::types::AgentEvent>> {
    let Some(topic_message) = node.store.get_topic_message(&event.event_id)? else {
        return Ok(None);
    };
    if !topic_message_requires_reply(&topic_message.content) {
        return Ok(None);
    }
    Ok(Some(build_agent_event(
        wattswarm_protocol::types::AgentEventType::TopicMessageRequiresReply,
        wattswarm_protocol::types::AgentEventSourceKind::TopicMessage,
        Some(event.author_node_id.clone()),
        None,
        json!({
            "message_id": &event.event_id,
            "network_id": &payload.network_id,
            "feed_key": &payload.feed_key,
            "scope_hint": &payload.scope_hint,
            "author_node_id": &event.author_node_id,
            "content": topic_message.content,
            "reply_to_message_id": &payload.reply_to_message_id,
            "created_at": event.created_at,
        }),
        false,
        vec!["reply".to_owned(), "ignore".to_owned()],
        Some(payload.feed_key.clone()),
        Some(format!("topic_message:{}", event.event_id)),
    )))
}

pub(super) fn build_agent_event(
    event_type: wattswarm_protocol::types::AgentEventType,
    source_kind: wattswarm_protocol::types::AgentEventSourceKind,
    source_node_id: Option<String>,
    target_agent_id: Option<String>,
    payload: Value,
    requires_commit: bool,
    allowed_actions: Vec<String>,
    correlation_id: Option<String>,
    dedupe_key: Option<String>,
) -> wattswarm_protocol::types::AgentEvent {
    wattswarm_protocol::types::AgentEvent {
        event_id: Uuid::new_v4().to_string(),
        event_type,
        source_kind,
        source_node_id,
        target_agent_id,
        target_executor: Some(CORE_AGENT_EXECUTOR_NAME.to_owned()),
        payload,
        requires_commit,
        allowed_actions,
        correlation_id,
        dedupe_key,
        created_at: observed_at_ms(),
    }
}

pub(super) fn deliver_agent_event_to_local_executor(
    state_dir: &Path,
    db_path: Option<&Path>,
    event: &wattswarm_protocol::types::AgentEvent,
) -> Result<()> {
    if let Some(dedupe_key) = event.dedupe_key.as_deref()
        && crate::control::find_agent_event_record_by_dedupe_key(state_dir, dedupe_key)?.is_some()
    {
        diagnostics::record_diagnostic(
            Some(state_dir),
            diagnostics::DiagnosticEvent::new(
                "info",
                "agent_event",
                "delivery.dedupe",
                "skipped",
                format!("agent event dedupe skipped: {:?}", event.event_type),
            )
            .event_id(event.event_id.clone())
            .object("agent_event", event.correlation_id.clone())
            .source_node_id(event.source_node_id.clone())
            .details(json!({
                "event_type": format!("{:?}", event.event_type),
                "dedupe_key": dedupe_key,
            })),
        );
        return Ok(());
    }

    let now = observed_at_ms();
    crate::control::save_agent_event_record_state(
        state_dir,
        event,
        wattswarm_protocol::types::AgentEventStatus::Pending,
        now,
    )?;
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new(
            "info",
            "agent_event",
            "delivery.queued",
            "pending",
            format!(
                "agent event queued for local executor: {:?}",
                event.event_type
            ),
        )
        .event_id(event.event_id.clone())
        .object("agent_event", event.correlation_id.clone())
        .source_node_id(event.source_node_id.clone())
        .details(json!({
            "event_type": format!("{:?}", event.event_type),
            "target_agent_id": event.target_agent_id,
            "target_executor": event.target_executor,
            "allowed_actions": event.allowed_actions,
            "requires_commit": event.requires_commit,
        })),
    );

    let registry = crate::control::load_executor_registry_state(state_dir)?;
    let Some(entry) = registry
        .entries
        .into_iter()
        .find(|candidate| candidate.name == CORE_AGENT_EXECUTOR_NAME)
    else {
        crate::control::save_agent_event_record_state(
            state_dir,
            event,
            wattswarm_protocol::types::AgentEventStatus::Failed,
            observed_at_ms(),
        )?;
        crate::control::append_agent_event_delivery_record_state(
            state_dir,
            &crate::storage::LocalAgentEventDeliveryRow {
                delivery_id: Uuid::new_v4().to_string(),
                event_id: event.event_id.clone(),
                attempt_no: 1,
                endpoint_url: String::new(),
                delivery_status: "failed".to_owned(),
                response_code: None,
                response_body: None,
                error_text: Some("core-agent executor is not registered".to_owned()),
                next_retry_at: None,
                created_at: observed_at_ms(),
            },
        )?;
        diagnostics::record_diagnostic(
            Some(state_dir),
            diagnostics::DiagnosticEvent::new(
                "error",
                "agent_event",
                "delivery.executor",
                "failed",
                "core-agent executor is not registered",
            )
            .event_id(event.event_id.clone())
            .object("agent_event", event.correlation_id.clone())
            .source_node_id(event.source_node_id.clone())
            .details(json!({
                "event_type": format!("{:?}", event.event_type),
            })),
        );
        return Ok(());
    };

    let callback_base_url = entry
        .agent_event_callback_base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| entry.base_url.trim_end_matches('/'));
    let endpoint_url = format!("{}/agent-events", callback_base_url.trim_end_matches('/'));
    let client = reqwest::blocking::Client::new();
    let delivery_started_at = observed_at_ms();
    let callback_request = wattswarm_protocol::types::AgentEventCallbackRequest {
        event: event.clone(),
    };
    let result = client.post(&endpoint_url).json(&callback_request).send();
    match result {
        Ok(response) => {
            let response_code = response.status().as_u16() as i64;
            let body = response.text().unwrap_or_default();
            let parsed = serde_json::from_str::<
                wattswarm_protocol::types::AgentEventCallbackResponse,
            >(&body)
            .ok();
            let status = if response_code >= 200 && response_code < 300 {
                if parsed.as_ref().is_some_and(|ack| ack.ok) {
                    wattswarm_protocol::types::AgentEventStatus::Acked
                } else {
                    wattswarm_protocol::types::AgentEventStatus::Delivered
                }
            } else {
                wattswarm_protocol::types::AgentEventStatus::Failed
            };
            crate::control::save_agent_event_record_state(
                state_dir,
                event,
                status.clone(),
                observed_at_ms(),
            )?;
            crate::control::append_agent_event_delivery_record_state(
                state_dir,
                &crate::storage::LocalAgentEventDeliveryRow {
                    delivery_id: Uuid::new_v4().to_string(),
                    event_id: event.event_id.clone(),
                    attempt_no: 1,
                    endpoint_url: endpoint_url.clone(),
                    delivery_status: agent_event_status_label(status),
                    response_code: Some(response_code),
                    response_body: Some(body),
                    error_text: None,
                    next_retry_at: None,
                    created_at: delivery_started_at,
                },
            )?;
            diagnostics::record_diagnostic(
                Some(state_dir),
                diagnostics::DiagnosticEvent::new(
                    if response_code >= 200 && response_code < 300 {
                        "info"
                    } else {
                        "warn"
                    },
                    "agent_event",
                    "delivery.callback",
                    if response_code >= 200 && response_code < 300 {
                        "delivered"
                    } else {
                        "failed"
                    },
                    format!("agent event callback returned {response_code}"),
                )
                .event_id(event.event_id.clone())
                .object("agent_event", event.correlation_id.clone())
                .source_node_id(event.source_node_id.clone())
                .details(json!({
                    "event_type": format!("{:?}", event.event_type),
                    "endpoint_url": endpoint_url,
                    "response_code": response_code,
                })),
            );
            if let Some(decision) = parsed.and_then(|ack| ack.decision) {
                match route_agent_decision(state_dir, db_path, event, &decision, &entry) {
                    Ok(commit_result) => {
                        crate::control::save_agent_event_record_state(
                            state_dir,
                            event,
                            wattswarm_protocol::types::AgentEventStatus::Completed,
                            observed_at_ms(),
                        )?;
                        if let Some((endpoint_url, commit_code, commit_body)) = commit_result {
                            crate::control::append_agent_event_delivery_record_state(
                                state_dir,
                                &crate::storage::LocalAgentEventDeliveryRow {
                                    delivery_id: Uuid::new_v4().to_string(),
                                    event_id: event.event_id.clone(),
                                    attempt_no: 2,
                                    endpoint_url,
                                    delivery_status: "completed".to_owned(),
                                    response_code: Some(commit_code),
                                    response_body: Some(commit_body),
                                    error_text: None,
                                    next_retry_at: None,
                                    created_at: observed_at_ms(),
                                },
                            )?;
                        }
                        diagnostics::record_diagnostic(
                            Some(state_dir),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "agent_event",
                                "decision.route",
                                "completed",
                                format!("agent decision routed: {:?}", decision.route),
                            )
                            .event_id(event.event_id.clone())
                            .object("agent_event", event.correlation_id.clone())
                            .source_node_id(event.source_node_id.clone())
                            .details(json!({
                                "event_type": format!("{:?}", event.event_type),
                                "decision_id": decision.decision_id,
                                "action": decision.action,
                                "route": format!("{:?}", decision.route),
                            })),
                        );
                    }
                    Err(error) => {
                        crate::control::save_agent_event_record_state(
                            state_dir,
                            event,
                            wattswarm_protocol::types::AgentEventStatus::Failed,
                            observed_at_ms(),
                        )?;
                        crate::control::append_agent_event_delivery_record_state(
                            state_dir,
                            &crate::storage::LocalAgentEventDeliveryRow {
                                delivery_id: Uuid::new_v4().to_string(),
                                event_id: event.event_id.clone(),
                                attempt_no: 2,
                                endpoint_url: entry
                                    .commit_plane_endpoint
                                    .clone()
                                    .unwrap_or_else(String::new),
                                delivery_status: "failed".to_owned(),
                                response_code: None,
                                response_body: None,
                                error_text: Some(error.to_string()),
                                next_retry_at: None,
                                created_at: observed_at_ms(),
                            },
                        )?;
                        diagnostics::record_diagnostic(
                            Some(state_dir),
                            diagnostics::DiagnosticEvent::new(
                                "error",
                                "agent_event",
                                "decision.route",
                                "failed",
                                format!("agent decision routing failed: {error}"),
                            )
                            .event_id(event.event_id.clone())
                            .object("agent_event", event.correlation_id.clone())
                            .source_node_id(event.source_node_id.clone())
                            .details(json!({
                                "event_type": format!("{:?}", event.event_type),
                                "decision_id": decision.decision_id,
                                "action": decision.action,
                                "route": format!("{:?}", decision.route),
                            })),
                        );
                    }
                }
            }
        }
        Err(error) => {
            crate::control::save_agent_event_record_state(
                state_dir,
                event,
                wattswarm_protocol::types::AgentEventStatus::Failed,
                observed_at_ms(),
            )?;
            crate::control::append_agent_event_delivery_record_state(
                state_dir,
                &crate::storage::LocalAgentEventDeliveryRow {
                    delivery_id: Uuid::new_v4().to_string(),
                    event_id: event.event_id.clone(),
                    attempt_no: 1,
                    endpoint_url: endpoint_url.clone(),
                    delivery_status: "failed".to_owned(),
                    response_code: None,
                    response_body: None,
                    error_text: Some(error.to_string()),
                    next_retry_at: None,
                    created_at: delivery_started_at,
                },
            )?;
            diagnostics::record_diagnostic(
                Some(state_dir),
                diagnostics::DiagnosticEvent::new(
                    "error",
                    "agent_event",
                    "delivery.callback",
                    "failed",
                    format!("agent event callback failed: {error}"),
                )
                .event_id(event.event_id.clone())
                .object("agent_event", event.correlation_id.clone())
                .source_node_id(event.source_node_id.clone())
                .details(json!({
                    "event_type": format!("{:?}", event.event_type),
                    "endpoint_url": endpoint_url,
                })),
            );
        }
    }

    Ok(())
}
