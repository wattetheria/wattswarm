use super::*;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct KnowledgeSummaryBundle {
    pub(super) source_node_id: String,
    pub(super) task_type: String,
    pub(super) decisions: Vec<crate::storage::DecisionMemoryHitRow>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct ReputationSummaryBundle {
    pub(super) source_node_id: String,
    pub(super) entries: Vec<crate::storage::ReputationSnapshotRow>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct TaskOutcomeSummaryBundle {
    pub(super) source_node_id: String,
    pub(super) scope_hint: String,
    pub(super) task_id: String,
    pub(super) task_type: String,
    pub(super) candidate_id: String,
    pub(super) output_digest: String,
    pub(super) result_summary: serde_json::Value,
    pub(super) evidence_digest_count: u32,
    pub(super) checkpoint_id: String,
    pub(super) proof_artifact_path: String,
    pub(super) finalized_at: u64,
}

fn build_summary_id(
    source_node_id: &str,
    scope: &SwarmScope,
    summary_kind: &str,
    payload: &serde_json::Value,
) -> Result<String> {
    Ok(crate::crypto::sha256_hex(&serde_json::to_vec(
        &serde_json::json!({
            "source_node_id": source_node_id,
            "scope": scope,
            "summary_kind": summary_kind,
            "payload": payload,
        }),
    )?))
}

pub fn build_knowledge_summary_for_task_type(
    node: &Node,
    scope: &SwarmScope,
    task_type: &str,
) -> Result<Option<SummaryAnnouncement>> {
    build_knowledge_summary_for_task_type_with_limit(
        node,
        scope,
        task_type,
        wattswarm_protocol::types::NetworkProtocolParams::default().summary_decision_memory_limit,
    )
}

pub(super) fn build_knowledge_summary_for_task_type_with_limit(
    node: &Node,
    scope: &SwarmScope,
    task_type: &str,
    decision_memory_limit: u32,
) -> Result<Option<SummaryAnnouncement>> {
    let decisions = node
        .store
        .list_local_decision_memory_hits_by_task_type(task_type, decision_memory_limit)?;
    if decisions.is_empty() {
        return Ok(None);
    }
    let source_node_id = node.node_id();
    let payload = serde_json::to_value(KnowledgeSummaryBundle {
        source_node_id: source_node_id.clone(),
        task_type: task_type.to_owned(),
        decisions,
    })?;
    Ok(Some(SummaryAnnouncement {
        summary_id: build_summary_id(&source_node_id, scope, KNOWLEDGE_SUMMARY_KIND, &payload)?,
        source_node_id,
        scope: scope.clone(),
        summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload,
    }))
}

pub(super) fn knowledge_summary_for_event(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
    decision_memory_limit: u32,
) -> Result<Option<SummaryAnnouncement>> {
    let Some(task_id) = event.task_id.as_deref() else {
        return Ok(None);
    };
    if !matches!(
        event.payload,
        crate::types::EventPayload::DecisionFinalized(_)
    ) {
        return Ok(None);
    }
    let Some(task) = node.task_view(task_id)? else {
        return Ok(None);
    };
    build_knowledge_summary_for_task_type_with_limit(
        node,
        scope,
        &task.contract.task_type,
        decision_memory_limit,
    )
}

pub fn build_reputation_summary_for_runtime(
    node: &Node,
    runtime_id: &str,
    profile_id: &str,
) -> Result<Option<SummaryAnnouncement>> {
    let Some(entry) = node
        .store
        .get_local_reputation_snapshot(runtime_id, profile_id)?
    else {
        return Ok(None);
    };
    let source_node_id = node.node_id();
    let payload = serde_json::to_value(ReputationSummaryBundle {
        source_node_id: source_node_id.clone(),
        entries: vec![entry],
    })?;
    Ok(Some(SummaryAnnouncement {
        summary_id: build_summary_id(
            &source_node_id,
            &SwarmScope::Global,
            REPUTATION_SUMMARY_KIND,
            &payload,
        )?,
        source_node_id,
        scope: SwarmScope::Global,
        summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload,
    }))
}

pub(super) fn reputation_summary_for_event(
    node: &Node,
    event: &crate::types::Event,
) -> Result<Option<SummaryAnnouncement>> {
    let crate::types::EventPayload::VerifierResultSubmitted(payload) = &event.payload else {
        return Ok(None);
    };
    build_reputation_summary_for_runtime(
        node,
        &payload.result.provider_family,
        &payload.result.model_id,
    )
}

fn outcome_summary_excerpt(output: &serde_json::Value) -> serde_json::Value {
    let Some(obj) = output.as_object() else {
        return output.clone();
    };
    let mut excerpt = serde_json::Map::new();
    for key in ["decision", "answer", "confidence", "check_summary"] {
        if let Some(value) = obj.get(key) {
            excerpt.insert(key.to_owned(), value.clone());
        }
    }
    if excerpt.is_empty() {
        output.clone()
    } else {
        serde_json::Value::Object(excerpt)
    }
}

fn canonical_scope_hint(scope: &SwarmScope) -> String {
    match scope {
        SwarmScope::Global => "global".to_owned(),
        SwarmScope::Region(id) => format!("region:{id}"),
        SwarmScope::Node(id) => format!("node:{id}"),
    }
}

pub(super) fn task_outcome_summary_for_event(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
) -> Result<Option<SummaryAnnouncement>> {
    let crate::types::EventPayload::DecisionFinalized(payload) = &event.payload else {
        return Ok(None);
    };
    let Some(task) = node.task_view(&payload.task_id)? else {
        return Ok(None);
    };
    let Some(candidate) = node
        .store
        .get_candidate_by_id(&payload.task_id, &payload.candidate_id)?
    else {
        return Ok(None);
    };
    let checkpoint =
        super::announcements::checkpoint_announcement_for_event(node, event, scope)?
            .ok_or_else(|| anyhow::anyhow!("checkpoint announcement missing for finalized task"))?;
    let output_digest = crate::crypto::sha256_hex(&serde_json::to_vec(&candidate.output)?);
    let evidence_digest_count = candidate
        .evidence_refs
        .iter()
        .filter(|reference| !reference.digest.trim().is_empty())
        .count() as u32;
    let bundle = TaskOutcomeSummaryBundle {
        source_node_id: node.node_id(),
        scope_hint: canonical_scope_hint(scope),
        task_id: payload.task_id.clone(),
        task_type: task.contract.task_type,
        candidate_id: payload.candidate_id.clone(),
        output_digest,
        result_summary: outcome_summary_excerpt(&candidate.output),
        evidence_digest_count,
        checkpoint_id: checkpoint.checkpoint_id,
        proof_artifact_path: checkpoint.artifact_path,
        finalized_at: event.created_at,
    };
    let payload = serde_json::to_value(&bundle)?;
    Ok(Some(SummaryAnnouncement {
        summary_id: build_summary_id(&node.node_id(), scope, TASK_OUTCOME_SUMMARY_KIND, &payload)?,
        source_node_id: node.node_id(),
        scope: scope.clone(),
        summary_kind: TASK_OUTCOME_SUMMARY_KIND.to_owned(),
        artifact_path: Some(bundle.proof_artifact_path.clone()),
        payload,
    }))
}

fn apply_summary_announcement_to_store(
    store: &crate::storage::PgStore,
    summary: &SummaryAnnouncement,
) -> Result<()> {
    if store.is_summary_revoked(&summary.summary_id)?
        || store.is_node_penalized(&summary.source_node_id)?
    {
        return Ok(());
    }
    match summary.summary_kind.as_str() {
        KNOWLEDGE_SUMMARY_KIND => {
            let payload: KnowledgeSummaryBundle = serde_json::from_value(summary.payload.clone())?;
            for decision in payload.decisions {
                store.put_imported_decision_memory(
                    &summary.summary_id,
                    &summary.source_node_id,
                    &decision,
                )?;
            }
        }
        REPUTATION_SUMMARY_KIND => {
            let payload: ReputationSummaryBundle = serde_json::from_value(summary.payload.clone())?;
            for entry in payload.entries {
                store.put_imported_reputation_snapshot(
                    &summary.summary_id,
                    &summary.source_node_id,
                    &entry,
                )?;
            }
        }
        TASK_OUTCOME_SUMMARY_KIND => {
            let payload: TaskOutcomeSummaryBundle =
                serde_json::from_value(summary.payload.clone())?;
            store.put_imported_task_outcome(&crate::storage::ImportedTaskOutcomeRow {
                summary_id: summary.summary_id.clone(),
                source_node_id: summary.source_node_id.clone(),
                scope_hint: payload.scope_hint,
                task_id: payload.task_id,
                task_type: payload.task_type,
                candidate_id: payload.candidate_id,
                output_digest: payload.output_digest,
                result_summary: payload.result_summary,
                evidence_digest_count: payload.evidence_digest_count,
                checkpoint_id: payload.checkpoint_id,
                proof_artifact_path: payload.proof_artifact_path,
                finalized_at: payload.finalized_at,
                revoked: false,
            })?;
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn mirror_summary_to_parent_network(
    node: &Node,
    summary: &SummaryAnnouncement,
) -> Result<bool> {
    let Some(parent_store) = super::parent_uplink_store(node)? else {
        return Ok(false);
    };
    apply_summary_announcement_to_store(&parent_store, summary)?;
    Ok(true)
}

pub(super) fn mirror_summary_controls_to_parent_network(
    node: &Node,
    event: &crate::types::Event,
) -> Result<bool> {
    let Some(parent_store) = super::parent_uplink_store(node)? else {
        return Ok(false);
    };
    match &event.payload {
        crate::types::EventPayload::SummaryRevoked(payload) => {
            parent_store.put_summary_revocation(
                &payload.target_summary_id,
                &payload.summary_kind,
                &payload.reason,
                &event.author_node_id,
                event.created_at,
            )?;
            parent_store.revoke_imported_decision_memory_by_summary(&payload.target_summary_id)?;
            parent_store.revoke_imported_reputation_by_summary(&payload.target_summary_id)?;
            parent_store.revoke_imported_task_outcomes_by_summary(&payload.target_summary_id)?;
            Ok(true)
        }
        crate::types::EventPayload::NodePenalized(payload) => {
            parent_store.put_node_penalty(
                &payload.penalized_node_id,
                &payload.reason,
                payload.block_summaries,
                &event.author_node_id,
                event.created_at,
            )?;
            for summary_id in &payload.revoked_summary_ids {
                parent_store.put_summary_revocation(
                    summary_id,
                    "penalty_cascade_v1",
                    &payload.reason,
                    &event.author_node_id,
                    event.created_at,
                )?;
                parent_store.revoke_imported_decision_memory_by_summary(summary_id)?;
                parent_store.revoke_imported_reputation_by_summary(summary_id)?;
                parent_store.revoke_imported_task_outcomes_by_summary(summary_id)?;
            }
            if payload.block_summaries {
                parent_store
                    .revoke_imported_decision_memory_by_source(&payload.penalized_node_id)?;
                parent_store.revoke_imported_reputation_by_source(&payload.penalized_node_id)?;
                parent_store.revoke_imported_task_outcomes_by_source(&payload.penalized_node_id)?;
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub fn apply_summary_announcement(node: &mut Node, summary: &SummaryAnnouncement) -> Result<()> {
    apply_summary_announcement_to_store(&node.store, summary)
}
