use super::*;

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
    node.store.put_checkpoint_announcement(
        &checkpoint.scope.label()?,
        &checkpoint.checkpoint_id,
        &checkpoint.artifact_path,
        observed_at_ms(),
    )
}
