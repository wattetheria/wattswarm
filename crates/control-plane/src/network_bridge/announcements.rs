use super::*;

pub(super) fn apply_checkpoint_announcement_to_store(
    store: &crate::storage::PgStore,
    checkpoint: &crate::network_p2p::CheckpointAnnouncement,
) -> Result<()> {
    store.put_checkpoint_announcement(
        &checkpoint.scope.label()?,
        &checkpoint.checkpoint_id,
        &checkpoint.artifact_path,
        observed_at_ms(),
    )
}

pub fn apply_rule_announcement(
    node: &mut Node,
    rule: &crate::network_p2p::RuleAnnouncement,
) -> Result<()> {
    node.store.put_rule_announcement(
        &rule.scope.label()?,
        &rule.rule_set,
        rule.rule_version,
        rule.activation_epoch,
        observed_at_ms(),
    )
}

pub fn apply_checkpoint_announcement(
    node: &mut Node,
    checkpoint: &crate::network_p2p::CheckpointAnnouncement,
) -> Result<()> {
    apply_checkpoint_announcement_to_store(&node.store, checkpoint)
}

pub(super) fn checkpoint_announcement_for_event(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
) -> Result<Option<crate::network_p2p::CheckpointAnnouncement>> {
    let crate::types::EventPayload::DecisionFinalized(payload) = &event.payload else {
        return Ok(None);
    };
    let Some(task) = node.task_view(&payload.task_id)? else {
        return Ok(None);
    };
    let checkpoint_id = crate::crypto::sha256_hex(&serde_json::to_vec(&serde_json::json!({
        "task_id": payload.task_id,
        "epoch": payload.epoch,
        "candidate_id": payload.candidate_id,
        "winning_candidate_hash": payload.winning_candidate_hash,
        "scope": scope,
    }))?);
    let artifact_path = format!(
        "finality://{}/{}/{}/{}",
        task.contract.task_type, payload.task_id, payload.epoch, payload.candidate_id
    );
    Ok(Some(crate::network_p2p::CheckpointAnnouncement {
        scope: scope.clone(),
        checkpoint_id,
        artifact_path,
    }))
}

pub(super) fn mirror_checkpoint_to_parent_network(
    node: &Node,
    checkpoint: &crate::network_p2p::CheckpointAnnouncement,
) -> Result<bool> {
    let Some(parent_store) = super::parent_uplink_store(node)? else {
        return Ok(false);
    };
    apply_checkpoint_announcement_to_store(&parent_store, checkpoint)?;
    Ok(true)
}
