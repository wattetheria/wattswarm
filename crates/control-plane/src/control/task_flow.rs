use super::*;

const RUN_QUEUE_ANNOUNCEMENT_FEED_KEY: &str = "venue.run_queue";
const RUN_QUEUE_TASK_FEED_KEY: &str = "venue.run_queue.task";

fn load_remote_task_bridge_registry_file(path: &Path) -> Result<RemoteTaskBridgeRegistry> {
    if !path.exists() {
        return Ok(RemoteTaskBridgeRegistry::default());
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn load_remote_task_bridge_registry(state_dir: &Path) -> Result<RemoteTaskBridgeRegistry> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    let entries = store.list_local_remote_task_bridges(&scope_id)?;
    if !entries.is_empty() {
        return Ok(RemoteTaskBridgeRegistry {
            entries: entries
                .into_iter()
                .map(|entry| RemoteTaskBridgeRecord {
                    task_id: entry.task_id,
                    announcement_id: entry.announcement_id,
                    network_id: entry.network_id,
                    source_node_id: entry.source_node_id,
                    source_scope_hint: entry.source_scope_hint,
                    detail_ref_digest: entry.detail_ref_digest,
                    executor: entry.executor,
                    profile: entry.profile,
                    candidate_id: entry.candidate_id,
                    terminal_state: entry.terminal_state,
                    bridged_at: entry.bridged_at,
                })
                .collect(),
        });
    }
    let path = remote_task_bridge_registry_path(state_dir);
    let legacy = load_remote_task_bridge_registry_file(&path)?;
    if !legacy.entries.is_empty() {
        for entry in &legacy.entries {
            store.upsert_local_remote_task_bridge(
                &scope_id,
                &crate::storage::LocalRemoteTaskBridgeRow {
                    task_id: entry.task_id.clone(),
                    announcement_id: entry.announcement_id.clone(),
                    network_id: entry.network_id.clone(),
                    source_node_id: entry.source_node_id.clone(),
                    source_scope_hint: entry.source_scope_hint.clone(),
                    detail_ref_digest: entry.detail_ref_digest.clone(),
                    executor: entry.executor.clone(),
                    profile: entry.profile.clone(),
                    candidate_id: entry.candidate_id.clone(),
                    terminal_state: entry.terminal_state.clone(),
                    bridged_at: entry.bridged_at,
                },
            )?;
        }
    }
    Ok(legacy)
}

fn save_remote_task_bridge_registry(
    state_dir: &Path,
    reg: &RemoteTaskBridgeRegistry,
) -> Result<()> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    store.replace_local_remote_task_bridges(
        &scope_id,
        &reg.entries
            .iter()
            .map(|entry| crate::storage::LocalRemoteTaskBridgeRow {
                task_id: entry.task_id.clone(),
                announcement_id: entry.announcement_id.clone(),
                network_id: entry.network_id.clone(),
                source_node_id: entry.source_node_id.clone(),
                source_scope_hint: entry.source_scope_hint.clone(),
                detail_ref_digest: entry.detail_ref_digest.clone(),
                executor: entry.executor.clone(),
                profile: entry.profile.clone(),
                candidate_id: entry.candidate_id.clone(),
                terminal_state: entry.terminal_state.clone(),
                bridged_at: entry.bridged_at,
            })
            .collect::<Vec<_>>(),
    )
}

fn runtime_client_for_executor(entry: &ExecutorRegistryEntry) -> std::sync::Arc<dyn RuntimeClient> {
    std::sync::Arc::new(HttpRuntimeClient::new(entry.base_url.clone()))
}

pub(crate) fn prepare_runtime_for_executor(
    state_dir: &Path,
    executor: &str,
    profile: &str,
) -> Result<PreparedRuntime> {
    let reg = load_executor_registry_state(state_dir)?;
    let executor = normalize_executor_name(executor);
    let entry = reg
        .entries
        .iter()
        .find(|e| e.name == executor)
        .ok_or_else(|| anyhow!("executor not found: {executor}"))?;

    let runtime = runtime_client_for_executor(entry);
    retry_runtime_probe(|| runtime.health()).with_context(|| {
        format!(
            "runtime /health failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    let capabilities = retry_runtime_probe(|| runtime.capabilities()).with_context(|| {
        format!(
            "runtime /capabilities failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    if !capabilities
        .profiles
        .iter()
        .any(|candidate| candidate == profile)
    {
        return Err(anyhow!(
            "profile '{}' not supported by executor '{}'",
            profile,
            executor
        ));
    }
    Ok(PreparedRuntime {
        runtime,
        capabilities,
    })
}

pub(super) fn retry_runtime_probe<T, F>(mut op: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    const MAX_ATTEMPTS: usize = 5;
    const BASE_BACKOFF_MS: u64 = 50;

    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_err = Some(err);
                if attempt + 1 < MAX_ATTEMPTS {
                    thread::sleep(Duration::from_millis(
                        BASE_BACKOFF_MS * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("runtime probe failed without an error")))
}

pub(crate) fn run_existing_task_with_runtime(
    node: &mut Node,
    state_dir: &Path,
    runtime: &dyn RuntimeClient,
    capabilities: &RuntimeCapabilities,
    executor: &str,
    profile: &str,
    task_id: &str,
    now: u64,
) -> Result<Value> {
    let task = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task view missing for {task_id}"))?;
    let propose_execution_id = format!("exec-p-{}", Uuid::new_v4());
    let verify_execution_id = format!("exec-v-{}", Uuid::new_v4());
    let lease_until = now.saturating_add(task.contract.assignment.claim.lease_ms);

    node.claim_task(
        task_id,
        ClaimRole::Propose,
        &propose_execution_id,
        lease_until,
        1,
        now.saturating_add(1),
    )?;
    let execute_req = node.build_execute_request_for_runtime(
        task_id,
        profile,
        &propose_execution_id,
        1,
        now.saturating_add(2),
    )?;
    let execute_res = runtime.execute(&execute_req)?;
    let candidate_output_ref = materialize_candidate_output_artifact(
        state_dir,
        &node.node_id(),
        &execute_res.candidate_output,
        now.saturating_add(2),
    )?;
    let candidate = Candidate {
        candidate_id: format!("cand-{}", propose_execution_id),
        execution_id: propose_execution_id.clone(),
        output_ref: candidate_output_ref,
        output: execute_res.candidate_output,
        evidence_inline: execute_res.evidence_inline,
        evidence_refs: execute_res.evidence_refs,
    };
    node.propose_candidate(task_id, candidate, 1, now.saturating_add(2))?;

    let candidate_id = format!("cand-{propose_execution_id}");
    node.claim_task(
        task_id,
        ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
        1,
        now.saturating_add(3),
    )?;
    node.auto_verify_candidate_with_runtime(
        runtime,
        task_id,
        &candidate_id,
        &verify_execution_id,
        1,
        now.saturating_add(4),
    )?;

    let verifier_result = node
        .store
        .list_verifier_results_for_candidate(task_id, &candidate_id)?
        .into_iter()
        .find(|row| row.execution_id == verify_execution_id)
        .ok_or_else(|| {
            anyhow!(
                "missing verifier result for execution {}",
                verify_execution_id
            )
        })?;
    if !verifier_result.passed {
        return Err(anyhow!(
            "verifier did not pass candidate; reason_codes={:?}",
            verifier_result.reason_codes
        ));
    }
    let candidate = node
        .store
        .get_candidate_by_id(task_id, &candidate_id)?
        .ok_or_else(|| anyhow!("candidate not found after execute: {candidate_id}"))?;
    let candidate_hash = candidate_hash(&candidate)?;

    let salt = Uuid::new_v4().to_string();
    let commit_hash = vote_commit_hash(
        VoteChoice::Approve,
        &salt,
        &verifier_result.verifier_result_hash,
    );
    node.submit_vote_commit(
        VoteCommitPayload {
            task_id: task_id.to_owned(),
            candidate_id: candidate_id.clone(),
            candidate_hash: candidate_hash.clone(),
            execution_id: verify_execution_id.clone(),
            verifier_result_hash: verifier_result.verifier_result_hash.clone(),
            commit_hash,
        },
        1,
        now.saturating_add(5),
    )?;
    node.submit_vote_reveal(
        VoteRevealPayload {
            task_id: task_id.to_owned(),
            candidate_id: candidate_id.clone(),
            candidate_hash,
            execution_id: verify_execution_id,
            verifier_result_hash: verifier_result.verifier_result_hash.clone(),
            vote: VoteChoice::Approve,
            salt,
        },
        1,
        now.saturating_add(6),
    )?;
    node.commit_decision(task_id, 1, &candidate_id, now.saturating_add(7))?;
    node.finalize_decision(
        task_id,
        1,
        &candidate_id,
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(&node.identity, task_id, 1, &candidate_id)],
        },
        now.saturating_add(8),
    )?;

    let view = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task view not found after run"))?;
    let final_decision = candidate
        .output
        .get("decision")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let final_answer = candidate
        .output
        .get("answer")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let _ = materialize_round_checkpoint_artifact(
        state_dir,
        node,
        &task.contract,
        1,
        &node.node_id(),
        crate::types::RoundCheckpointPhase::Finalized,
        Vec::new(),
        final_decision.clone(),
        None,
        now.saturating_add(9),
    )?;
    let evidence_digests = candidate
        .evidence_refs
        .iter()
        .map(|r| r.digest.clone())
        .collect::<Vec<_>>();
    Ok(json!({
        "task_id": task_id,
        "candidate_id": candidate_id,
        "executor": executor,
        "profile": profile,
        "provider_family": capabilities.provider_family,
        "model_id": capabilities.model_id,
        "candidate_output": candidate.output,
        "evidence_digests": evidence_digests,
        "final_decision": final_decision,
        "final_answer": final_answer,
        "terminal_state": format!("{:?}", view.terminal_state),
        "committed_candidate_id": view.committed_candidate_id,
        "finalized_candidate_id": view.finalized_candidate_id
    }))
}

fn remote_bridge_scope_hint(raw: &str) -> String {
    crate::types::normalized_scope_hint(raw)
}

fn remote_task_bridge_allowed_for_scope(
    node_id: &str,
    scope_hint: &str,
    route: Option<&crate::types::TransportRoute>,
) -> bool {
    match crate::types::ScopeHint::parse_with_prefix_fallback(scope_hint) {
        Some(crate::types::ScopeHint::Node(target)) => target == node_id,
        Some(crate::types::ScopeHint::Group(group_id)) => {
            route.is_some_and(|route| route.matches_group(&group_id) && route.allows_node(node_id))
        }
        Some(crate::types::ScopeHint::Global | crate::types::ScopeHint::Region(_)) => {
            route.is_none_or(|route| route.allows_node(node_id))
        }
        None => scope_hint.trim().is_empty(),
    }
}

fn remote_bridge_execution_set_id(task_id: &str) -> String {
    format!("remote-bridge:{task_id}")
}

fn ensure_remote_execution_participation(
    node: &mut Node,
    task_id: &str,
    network_id: &str,
    scope_hint: &str,
    created_at: u64,
) -> Result<()> {
    let execution_set_id = remote_bridge_execution_set_id(task_id);
    let local_node_id = node.node_id();
    let existing = node
        .store
        .list_execution_set_members(task_id, &execution_set_id)?
        .into_iter()
        .any(|member| member.participant_node_id == local_node_id);
    if existing {
        return Ok(());
    }

    node.emit_at(
        1,
        EventPayload::ExecutionIntentDeclared(ExecutionIntentDeclaredPayload {
            network_id: network_id.to_owned(),
            task_id: task_id.to_owned(),
            execution_set_id: execution_set_id.clone(),
            participant_node_id: local_node_id.clone(),
            role_hint: "executor".to_owned(),
            scope_hint: scope_hint.to_owned(),
            intent: "accepted".to_owned(),
        }),
        created_at,
    )?;
    node.emit_at(
        1,
        EventPayload::ExecutionSetConfirmed(ExecutionSetConfirmedPayload {
            network_id: network_id.to_owned(),
            task_id: task_id.to_owned(),
            execution_set_id,
            confirmed_by_node_id: local_node_id.clone(),
            scope_hint: scope_hint.to_owned(),
            members: vec![ExecutionSetMember {
                participant_node_id: local_node_id,
                role_hint: "executor".to_owned(),
            }],
        }),
        created_at.saturating_add(1),
    )?;
    Ok(())
}

fn bridged_task_contract(node: &Node, state_dir: &Path, task_id: &str) -> Result<TaskContract> {
    if let Some(task) = node.task_view(task_id)? {
        return Ok(task.contract);
    }
    let detail = node
        .store
        .get_task_announcement_detail_for_task(task_id)?
        .ok_or_else(|| anyhow!("task announcement missing for task {task_id}"))?;
    if let Some(contract) = detail.contract {
        return Ok(contract);
    }
    if let Some(contract) =
        task_contract_from_announcement_summary(task_id, &detail.announcement.summary)?
    {
        return Ok(contract);
    }
    let bytes = fetch_task_detail_artifact(state_dir, node, task_id, observed_at_ms())?;
    let contract = serde_json::from_slice::<TaskContract>(&bytes)
        .with_context(|| format!("parse bridged task detail for {task_id}"))?;
    if contract.task_id != task_id {
        return Err(anyhow!(
            "bridged task detail task_id mismatch: expected {}, got {}",
            task_id,
            contract.task_id
        ));
    }
    Ok(contract)
}

fn task_contract_from_announcement_summary(
    task_id: &str,
    summary: &Value,
) -> Result<Option<TaskContract>> {
    let Some(raw_contract) = summary.get("task_contract") else {
        return Ok(None);
    };
    let contract: TaskContract = serde_json::from_value(raw_contract.clone())
        .with_context(|| format!("parse task_contract from announcement summary for {task_id}"))?;
    if contract.task_id != task_id {
        return Err(anyhow!(
            "announcement summary task_contract task_id mismatch: expected {}, got {}",
            task_id,
            contract.task_id
        ));
    }
    Ok(Some(contract))
}

fn run_queue_task_scope_hint(task_id: &str) -> String {
    format!("group:{task_id}")
}

fn ensure_run_queue_task_scope(
    node: &Node,
    task_id: &str,
    contract: &TaskContract,
    updated_at: u64,
) -> Result<()> {
    let expected_scope = run_queue_task_scope_hint(task_id);
    let actual_scope = contract
        .inputs
        .get("swarm_scope")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if actual_scope != expected_scope {
        return Err(anyhow!(
            "run-queue task {} must use task-id scope {}; got {}",
            task_id,
            expected_scope,
            actual_scope
        ));
    }
    let network_id = current_network_context_id(node);
    let node_id = node.node_id();
    node.store.upsert_feed_subscription(
        &network_id,
        &node_id,
        RUN_QUEUE_TASK_FEED_KEY,
        &expected_scope,
        &["events".to_owned()],
        true,
        updated_at,
    )?;
    Ok(())
}

fn bridge_origin_payload(
    task_id: &str,
    deduped: bool,
    record: &RemoteTaskBridgeRecord,
    run: Value,
) -> Value {
    let mut value = run;
    let bridge = json!({
        "deduped": deduped,
        "announcement_id": record.announcement_id,
        "network_id": record.network_id,
        "source_node_id": record.source_node_id,
        "source_scope_hint": record.source_scope_hint,
        "detail_ref_digest": record.detail_ref_digest,
        "bridged_at": record.bridged_at
    });
    if let Some(obj) = value.as_object_mut() {
        obj.insert("task_id".to_owned(), json!(task_id));
        obj.insert("bridge".to_owned(), bridge);
    }
    value
}

pub fn bridge_remote_task_into_local_execution(
    node: &mut Node,
    state_dir: &Path,
    req: RemoteTaskBridgeRequest,
) -> Result<Value> {
    let RemoteTaskBridgeRequest {
        executor,
        profile,
        task_id,
    } = req;
    let executor = normalize_executor_name(&executor).to_owned();
    let detail = node
        .store
        .get_task_announcement_detail_for_task(&task_id)?
        .ok_or_else(|| anyhow!("remote task announcement missing for task {}", task_id))?;
    let announcement = detail.announcement.clone();

    // Coordinator-only constraint: the node that announced the task must not
    // execute it locally. The initiator coordinates; remote nodes execute.
    if announcement.announced_by_node_id == node.node_id() {
        return Err(anyhow!(
            "coordinator-only constraint: node {} announced task {} and cannot also execute it",
            node.node_id(),
            task_id
        ));
    }

    let network_id = current_network_context_id(node);
    let contract = bridged_task_contract(node, state_dir, &task_id)?;
    let route = contract.transport_route();
    let scope_hint = remote_bridge_scope_hint(&announcement.scope_hint);
    if !remote_task_bridge_allowed_for_scope(&node.node_id(), &scope_hint, route.as_ref()) {
        return Err(anyhow!(
            "remote task {} is not eligible for local node {} under scope {}",
            task_id,
            node.node_id(),
            scope_hint
        ));
    }
    if announcement.feed_key == RUN_QUEUE_ANNOUNCEMENT_FEED_KEY {
        ensure_run_queue_task_scope(node, &task_id, &contract, observed_at_ms())?;
    }
    let mut registry = load_remote_task_bridge_registry(state_dir)?;

    if let Some(existing) = registry.entries.iter().find(|entry| {
        entry.task_id == task_id
            && entry.announcement_id == announcement.announcement_id
            && entry.executor == executor
            && entry.profile == profile
    }) {
        return Ok(bridge_origin_payload(
            &task_id,
            true,
            existing,
            json!({
                "task_id": task_id,
                "candidate_id": existing.candidate_id,
                "executor": existing.executor,
                "profile": existing.profile,
                "terminal_state": existing.terminal_state
            }),
        ));
    }

    let prepared = prepare_runtime_for_executor(state_dir, &executor, &profile)?;
    if node.task_view(&task_id)?.is_none() {
        node.submit_task(contract, 1, observed_at_ms())?;
    }
    ensure_remote_execution_participation(
        node,
        &task_id,
        &network_id,
        &scope_hint,
        observed_at_ms().saturating_add(1),
    )?;
    let run = run_existing_task_with_runtime(
        node,
        state_dir,
        prepared.runtime.as_ref(),
        &prepared.capabilities,
        &executor,
        &profile,
        &task_id,
        observed_at_ms().saturating_add(2),
    )?;
    let candidate_id = run
        .get("candidate_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing candidate_id after bridged run"))?
        .to_owned();
    let terminal_state = run
        .get("terminal_state")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing terminal_state after bridged run"))?
        .to_owned();

    let record = RemoteTaskBridgeRecord {
        task_id: task_id.clone(),
        announcement_id: announcement.announcement_id,
        network_id,
        source_node_id: announcement.announced_by_node_id,
        source_scope_hint: scope_hint,
        detail_ref_digest: announcement.detail_ref.map(|reference| reference.digest),
        executor: executor.clone(),
        profile: profile.clone(),
        candidate_id,
        terminal_state,
        bridged_at: observed_at_ms(),
    };
    registry.entries.retain(|entry| {
        !(entry.task_id == record.task_id
            && entry.executor == record.executor
            && entry.profile == record.profile)
    });
    registry.entries.push(record.clone());
    registry.entries.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.executor.cmp(&right.executor))
            .then_with(|| left.profile.cmp(&right.profile))
    });
    save_remote_task_bridge_registry(state_dir, &registry)?;

    Ok(bridge_origin_payload(&task_id, false, &record, run))
}

pub fn run_real_task_flow(
    node: &mut Node,
    state_dir: &Path,
    req: RealTaskRunRequest,
) -> Result<Value> {
    let RealTaskRunRequest {
        executor,
        profile,
        task_id,
        task_file,
        task_contract,
    } = req;
    let prepared = prepare_runtime_for_executor(state_dir, &executor, &profile)?;

    let mut contract = if let Some(contract) = task_contract {
        contract
    } else if let Some(file) = task_file {
        let raw = fs::read(&file)?;
        serde_json::from_slice::<TaskContract>(&raw)
            .with_context(|| format!("parse task contract from {}", file.display()))?
    } else {
        let real_task_id = task_id
            .clone()
            .unwrap_or_else(|| format!("task-{}", Uuid::new_v4()));
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))?
            .policy_hash;
        sample_contract(&real_task_id, policy_hash)
    };

    if let Some(task_id) = task_id {
        contract.task_id = task_id;
    }

    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    node.submit_task(contract.clone(), 1, now)?;
    let _ = materialize_round_checkpoint_artifact(
        state_dir,
        node,
        &contract,
        1,
        &node.node_id(),
        crate::types::RoundCheckpointPhase::Opening,
        Vec::new(),
        None,
        None,
        now,
    )?;
    run_existing_task_with_runtime(
        node,
        state_dir,
        prepared.runtime.as_ref(),
        &prepared.capabilities,
        &executor,
        &profile,
        &contract.task_id,
        now,
    )
}
