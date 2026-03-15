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
        _ => {}
    }
    Ok(())
}

fn parent_uplink_store(node: &Node) -> Result<Option<crate::storage::PgStore>> {
    if !node.store.is_org_configured() {
        return Ok(None);
    }
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())?;
    if !topology.network.is_subnet() {
        return Ok(None);
    }
    let Some(parent_topology) = node
        .store
        .load_parent_network_topology_for_org(node.store.org_id())?
    else {
        return Ok(None);
    };
    if !node
        .store
        .node_has_network_membership(&node.node_id(), &parent_topology.network.network_id)?
    {
        return Ok(None);
    }
    Ok(Some(node.store.for_org(parent_topology.org.org_id)))
}

pub(super) fn mirror_summary_to_parent_network(
    node: &Node,
    summary: &SummaryAnnouncement,
) -> Result<bool> {
    let Some(parent_store) = parent_uplink_store(node)? else {
        return Ok(false);
    };
    apply_summary_announcement_to_store(&parent_store, summary)?;
    Ok(true)
}

pub(super) fn mirror_summary_controls_to_parent_network(
    node: &Node,
    event: &crate::types::Event,
) -> Result<bool> {
    let Some(parent_store) = parent_uplink_store(node)? else {
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
            }
            if payload.block_summaries {
                parent_store.revoke_imported_decision_memory_by_source(&payload.penalized_node_id)?;
                parent_store.revoke_imported_reputation_by_source(&payload.penalized_node_id)?;
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub fn apply_summary_announcement(node: &mut Node, summary: &SummaryAnnouncement) -> Result<()> {
    apply_summary_announcement_to_store(&node.store, summary)
}
