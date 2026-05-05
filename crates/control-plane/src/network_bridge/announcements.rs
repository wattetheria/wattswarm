use super::*;

fn mainnet_global_genesis_node_id(node: &Node, scope: &SwarmScope) -> Result<Option<String>> {
    if *scope != SwarmScope::Global {
        return Ok(None);
    }
    let Ok(topology) = node
        .store
        .load_network_topology_for_org(node.store.org_id())
    else {
        return Ok(None);
    };
    if topology.network.network_kind == crate::types::NetworkKind::Mainnet {
        Ok(Some(topology.network.genesis_node_id))
    } else {
        Ok(None)
    }
}

fn rule_authority_message(rule: &crate::network_p2p::RuleAnnouncement) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&serde_json::json!({
        "scope": rule.scope,
        "rule_set": rule.rule_set,
        "rule_version": rule.rule_version,
        "activation_epoch": rule.activation_epoch,
    }))?)
}

fn checkpoint_authority_message(
    checkpoint: &crate::network_p2p::CheckpointAnnouncement,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&serde_json::json!({
        "scope": checkpoint.scope,
        "checkpoint_id": checkpoint.checkpoint_id,
        "artifact_path": checkpoint.artifact_path,
    }))?)
}

fn verify_mainnet_authority(
    authority_signer_node_id: Option<&String>,
    authority_signature_hex: Option<&String>,
    genesis_node_id: &str,
    message: &[u8],
    label: &str,
) -> Result<()> {
    let (Some(authority_signer_node_id), Some(authority_signature_hex)) =
        (authority_signer_node_id, authority_signature_hex)
    else {
        bail!("{label} requires mainnet genesis authority signature");
    };
    if authority_signer_node_id != genesis_node_id {
        bail!("{label} authority signer must be mainnet genesis node");
    }
    crate::crypto::verify_signature(genesis_node_id, message, authority_signature_hex)
        .with_context(|| format!("{label} mainnet genesis authority signature verify failed"))?;
    Ok(())
}

pub fn sign_rule_announcement(
    node: &Node,
    mut rule: crate::network_p2p::RuleAnnouncement,
) -> Result<Option<crate::network_p2p::RuleAnnouncement>> {
    let Some(genesis_node_id) = mainnet_global_genesis_node_id(node, &rule.scope)? else {
        return Ok(Some(rule));
    };
    if node.node_id() != genesis_node_id {
        return Ok(None);
    }
    let message = rule_authority_message(&rule)?;
    rule.authority_signer_node_id = Some(node.node_id());
    rule.authority_signature_hex = Some(node.identity.sign_bytes(&message));
    Ok(Some(rule))
}

pub(super) fn sign_checkpoint_announcement(
    node: &Node,
    mut checkpoint: crate::network_p2p::CheckpointAnnouncement,
) -> Result<Option<crate::network_p2p::CheckpointAnnouncement>> {
    let Some(genesis_node_id) = mainnet_global_genesis_node_id(node, &checkpoint.scope)? else {
        return Ok(Some(checkpoint));
    };
    if node.node_id() != genesis_node_id {
        return Ok(None);
    }
    let message = checkpoint_authority_message(&checkpoint)?;
    checkpoint.authority_signer_node_id = Some(node.node_id());
    checkpoint.authority_signature_hex = Some(node.identity.sign_bytes(&message));
    Ok(Some(checkpoint))
}

fn validate_rule_announcement_authority(
    node: &Node,
    rule: &crate::network_p2p::RuleAnnouncement,
) -> Result<()> {
    if let Some(genesis_node_id) = mainnet_global_genesis_node_id(node, &rule.scope)? {
        let message = rule_authority_message(rule)?;
        verify_mainnet_authority(
            rule.authority_signer_node_id.as_ref(),
            rule.authority_signature_hex.as_ref(),
            &genesis_node_id,
            &message,
            "rule announcement",
        )?;
    }
    Ok(())
}

fn validate_checkpoint_announcement_authority(
    node: &Node,
    checkpoint: &crate::network_p2p::CheckpointAnnouncement,
) -> Result<()> {
    if let Some(genesis_node_id) = mainnet_global_genesis_node_id(node, &checkpoint.scope)? {
        let message = checkpoint_authority_message(checkpoint)?;
        verify_mainnet_authority(
            checkpoint.authority_signer_node_id.as_ref(),
            checkpoint.authority_signature_hex.as_ref(),
            &genesis_node_id,
            &message,
            "checkpoint announcement",
        )?;
    }
    Ok(())
}

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
    validate_rule_announcement_authority(node, rule)?;
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
    validate_checkpoint_announcement_authority(node, checkpoint)?;
    apply_checkpoint_announcement_to_store(&node.store, checkpoint)
}

pub(super) fn checkpoint_announcement_for_event(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
) -> Result<Option<crate::network_p2p::CheckpointAnnouncement>> {
    match &event.payload {
        crate::types::EventPayload::DecisionFinalized(payload) => {
            let Some(task) = node.task_view(&payload.task_id)? else {
                return Ok(None);
            };
            let checkpoint_id =
                crate::crypto::sha256_hex(&serde_json::to_vec(&serde_json::json!({
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
            let checkpoint = crate::network_p2p::CheckpointAnnouncement {
                scope: scope.clone(),
                checkpoint_id,
                artifact_path,
                authority_signer_node_id: None,
                authority_signature_hex: None,
            };
            sign_checkpoint_announcement(node, checkpoint)
        }
        crate::types::EventPayload::CheckpointCreated(payload) => {
            let Some(checkpoint) = node
                .store
                .find_checkpoint_announcement(&payload.checkpoint_id)?
            else {
                return Ok(None);
            };
            let scope = match crate::storage::ProjectionScope::parse(&checkpoint.scope_key) {
                Some(crate::storage::ProjectionScope::Global) => SwarmScope::Global,
                Some(crate::storage::ProjectionScope::Region(id)) => SwarmScope::Region(id),
                Some(crate::storage::ProjectionScope::Node(id)) => SwarmScope::Node(id),
                Some(crate::storage::ProjectionScope::Group(id)) => SwarmScope::Group(id),
                None => return Ok(None),
            };
            let checkpoint = crate::network_p2p::CheckpointAnnouncement {
                scope,
                checkpoint_id: checkpoint.checkpoint_id,
                artifact_path: checkpoint.artifact_path,
                authority_signer_node_id: None,
                authority_signature_hex: None,
            };
            sign_checkpoint_announcement(node, checkpoint)
        }
        _ => Ok(None),
    }
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
