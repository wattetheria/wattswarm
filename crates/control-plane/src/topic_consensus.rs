use crate::node::Node;
use crate::storage::storage::TopicMessageRow;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};

const TOPIC_CONSENSUS_MESSAGE_KIND_PROPOSAL: &str = "proposal";
const TOPIC_CONSENSUS_MESSAGE_KIND_STANCE: &str = "stance";
const TOPIC_CONSENSUS_MESSAGE_KIND_INTERPRETED_STANCE: &str = "interpreted_stance";
const TOPIC_CONSENSUS_MESSAGE_KIND_RESULT: &str = "consensus_result";

#[derive(Debug, Clone)]
struct StructuredProposal {
    proposal_id: String,
    source_message_id: String,
    coordinator_node_id: String,
    participants: Vec<String>,
    min_participants: usize,
    threshold_percent: u32,
    result_feed_key: String,
    goal: Option<String>,
    round_index: u32,
    max_rounds: u32,
    round_timeout_ms: u64,
    fallback_decision: Option<String>,
    created_at: u64,
}

#[derive(Debug, Clone)]
struct StructuredStance {
    proposal_message_id: String,
    author_node_id: String,
    stance: String,
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

fn resolve_network_id(node: &Node) -> String {
    if node.store.is_org_configured()
        && let Ok(topology) = node
            .store
            .load_network_topology_for_org(node.store.org_id())
    {
        return topology.network.network_id;
    }
    node.store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| format!("local:{}", node.node_id()))
}

fn distributed_topic_mode(node: &Node) -> bool {
    !matches!(
        node.store
            .load_network_topology_for_org(node.store.org_id())
            .map(|topology| topology.network.network_kind)
            .unwrap_or(crate::types::NetworkKind::Local),
        crate::types::NetworkKind::Local
    )
}

fn effective_participant_list(
    proposal: &StructuredProposal,
    coordinator_node_id: &str,
    distributed_mode: bool,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    proposal
        .participants
        .iter()
        .map(|participant| participant.trim().to_owned())
        .filter(|participant| !participant.is_empty())
        .filter(|participant| !(distributed_mode && participant == coordinator_node_id))
        .filter(|participant| seen.insert(participant.clone()))
        .collect()
}

fn parse_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_structured_proposal(message: &TopicMessageRow) -> Option<StructuredProposal> {
    let obj = message.content.as_object()?;
    if obj.get("kind")?.as_str()? != TOPIC_CONSENSUS_MESSAGE_KIND_PROPOSAL {
        return None;
    }
    let proposal_id = obj.get("proposal_id")?.as_str()?.trim().to_owned();
    if proposal_id.is_empty() {
        return None;
    }
    let participants = parse_string_array(obj.get("participants"));
    let threshold_percent = obj
        .get("threshold_percent")
        .and_then(Value::as_u64)
        .unwrap_or(60)
        .clamp(1, 100) as u32;
    let round_index = obj
        .get("round_index")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1) as u32;
    let max_rounds = obj
        .get("max_rounds")
        .and_then(Value::as_u64)
        .unwrap_or(u64::from(round_index))
        .max(u64::from(round_index)) as u32;
    let round_timeout_ms = obj
        .get("round_timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let result_feed_key = obj
        .get("result_feed_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{}.result", message.feed_key));
    let coordinator_node_id = obj
        .get("coordinator_node_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| message.author_node_id.clone());
    let fallback_decision = obj
        .get("fallback_decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| matches!(*value, "support" | "reject" | "abstain"))
        .map(ToOwned::to_owned);
    let min_participants = obj
        .get("min_participants")
        .and_then(Value::as_u64)
        .map(|value| value.max(1) as usize)
        .unwrap_or_else(|| participants.len().max(1));
    Some(StructuredProposal {
        proposal_id,
        source_message_id: message.message_id.clone(),
        coordinator_node_id,
        participants,
        min_participants,
        threshold_percent,
        result_feed_key,
        goal: obj
            .get("goal")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        round_index,
        max_rounds,
        round_timeout_ms,
        fallback_decision,
        created_at: message.created_at,
    })
}

fn parse_artifact_ref(value: &Value) -> Option<crate::types::ArtifactRef> {
    let obj = value.as_object()?;
    let uri = obj.get("uri")?.as_str()?.trim().to_owned();
    let digest = obj.get("digest")?.as_str()?.trim().to_owned();
    if uri.is_empty() || digest.is_empty() {
        return None;
    }
    Some(crate::types::ArtifactRef {
        uri,
        digest,
        size_bytes: obj.get("size_bytes").and_then(Value::as_u64).unwrap_or(0),
        mime: obj
            .get("mime")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "application/json".to_owned()),
        created_at: obj
            .get("created_at")
            .and_then(Value::as_u64)
            .unwrap_or_else(now_ms),
        producer: obj
            .get("producer")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "topic-consensus".to_owned()),
    })
}

fn resolve_proposal_message_id(
    message: &TopicMessageRow,
    proposal_id: &str,
    proposals: &[StructuredProposal],
) -> Option<String> {
    if let Some(proposal_message_id) = message
        .content
        .get("proposal_message_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return proposals
            .iter()
            .find(|proposal| {
                proposal.proposal_id == proposal_id
                    && proposal.source_message_id == proposal_message_id
            })
            .map(|proposal| proposal.source_message_id.clone());
    }
    if let Some(reply_to_message_id) = message.reply_to_message_id.as_deref()
        && let Some(proposal) = proposals.iter().find(|proposal| {
            proposal.proposal_id == proposal_id && proposal.source_message_id == reply_to_message_id
        })
    {
        return Some(proposal.source_message_id.clone());
    }
    let matches = proposals
        .iter()
        .filter(|proposal| proposal.proposal_id == proposal_id)
        .map(|proposal| proposal.source_message_id.clone())
        .collect::<Vec<_>>();
    (matches.len() == 1).then(|| matches[0].clone())
}

fn parse_structured_stance(
    message: &TopicMessageRow,
    proposals: &[StructuredProposal],
) -> Option<StructuredStance> {
    let obj = message.content.as_object()?;
    let kind = obj.get("kind")?.as_str()?;
    if !matches!(
        kind,
        TOPIC_CONSENSUS_MESSAGE_KIND_STANCE | TOPIC_CONSENSUS_MESSAGE_KIND_INTERPRETED_STANCE
    ) {
        return None;
    }
    let proposal_id = obj.get("proposal_id")?.as_str()?.trim().to_owned();
    let stance = obj.get("stance")?.as_str()?.trim().to_ascii_lowercase();
    if proposal_id.is_empty() || !matches!(stance.as_str(), "support" | "reject" | "abstain") {
        return None;
    }
    let _evidence_refs = obj
        .get("evidence_refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_artifact_ref)
        .collect::<Vec<_>>();
    let proposal_message_id = resolve_proposal_message_id(message, &proposal_id, proposals)?;
    Some(StructuredStance {
        proposal_message_id,
        author_node_id: obj
            .get("source_author_node_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| message.author_node_id.clone()),
        stance,
    })
}

fn list_all_topic_messages(
    node: &Node,
    feed_key: &str,
    scope_hint: &str,
    max_messages: usize,
) -> Result<Vec<TopicMessageRow>> {
    let mut out = Vec::new();
    let mut before_created_at = None;
    let mut before_message_id = None;
    while out.len() < max_messages {
        let page = node.store.list_topic_messages_page(
            feed_key,
            scope_hint,
            before_created_at,
            before_message_id.as_deref(),
            100.min(max_messages.saturating_sub(out.len())),
        )?;
        if page.is_empty() {
            break;
        }
        before_created_at = page.last().map(|message| message.created_at);
        before_message_id = page.last().map(|message| message.message_id.clone());
        out.extend(page);
        if out.len() >= max_messages {
            break;
        }
    }
    out.reverse();
    Ok(out)
}

fn threshold_count(total: usize, threshold_percent: u32) -> usize {
    if total == 0 {
        return 0;
    }
    let total = total as u64;
    let threshold = threshold_percent.clamp(1, 100) as u64;
    total.saturating_mul(threshold).div_ceil(100) as usize
}

fn proposal_round_exists(
    proposals: &[StructuredProposal],
    proposal_id: &str,
    round_index: u32,
) -> bool {
    proposals
        .iter()
        .any(|proposal| proposal.proposal_id == proposal_id && proposal.round_index == round_index)
}

fn resolve_required_participant_count(
    proposal: &StructuredProposal,
    effective_participants: &[String],
    observed_stances: usize,
) -> usize {
    if proposal.participants.is_empty() {
        proposal.min_participants.max(observed_stances)
    } else {
        proposal.min_participants.max(effective_participants.len())
    }
}

fn fallback_decision(
    proposal: &StructuredProposal,
    support_count: usize,
    reject_count: usize,
) -> String {
    if let Some(value) = &proposal.fallback_decision {
        return value.clone();
    }
    match support_count.cmp(&reject_count) {
        std::cmp::Ordering::Greater if support_count > 0 => "support".to_owned(),
        std::cmp::Ordering::Less if reject_count > 0 => "reject".to_owned(),
        _ => "abstain".to_owned(),
    }
}

fn result_message_exists(
    node: &Node,
    feed_key: &str,
    scope_hint: &str,
    proposal_message_id: &str,
) -> Result<bool> {
    Ok(list_all_topic_messages(node, feed_key, scope_hint, 200)?
        .into_iter()
        .any(|message| {
            message
                .content
                .get("kind")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == TOPIC_CONSENSUS_MESSAGE_KIND_RESULT)
                && message
                    .content
                    .get("proposal_message_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == proposal_message_id)
        }))
}

#[allow(clippy::too_many_arguments)]
fn publish_consensus_result_message(
    node: &mut Node,
    state_dir: &std::path::Path,
    source_feed_key: &str,
    scope_hint: &str,
    result_feed_key: &str,
    proposal: &StructuredProposal,
    effective_participants: &[String],
    decision: &str,
    support_count: usize,
    reject_count: usize,
    abstain_count: usize,
    required_count: usize,
    fallback_applied: bool,
    close_reason: Option<&str>,
    observed_participant_count: usize,
    created_at: u64,
) -> Result<()> {
    let network_id = resolve_network_id(node);
    crate::control::emit_topic_message_with_content(
        node,
        state_dir,
        &network_id,
        result_feed_key,
        scope_hint,
        json!({
            "kind": TOPIC_CONSENSUS_MESSAGE_KIND_RESULT,
            "proposal_id": proposal.proposal_id.clone(),
            "proposal_message_id": proposal.source_message_id.clone(),
            "decision": decision,
            "support_count": support_count,
            "reject_count": reject_count,
            "abstain_count": abstain_count,
            "required_count": required_count,
            "observed_participant_count": observed_participant_count,
            "min_participants": proposal.min_participants,
            "threshold_percent": proposal.threshold_percent,
            "round_index": proposal.round_index,
            "max_rounds": proposal.max_rounds,
            "fallback_applied": fallback_applied,
            "close_reason": close_reason,
            "participants": effective_participants,
            "coordinator_node_id": proposal.coordinator_node_id.clone(),
            "source_feed_key": source_feed_key,
            "source_scope_hint": scope_hint,
            "goal": proposal.goal.clone(),
        }),
        None,
        created_at,
    )?;
    Ok(())
}

fn maybe_publish_next_round_proposal(
    node: &mut Node,
    state_dir: &std::path::Path,
    feed_key: &str,
    scope_hint: &str,
    proposal: &StructuredProposal,
    effective_participants: &[String],
    distributed_mode: bool,
    proposals: &[StructuredProposal],
    created_at: u64,
) -> Result<bool> {
    let next_round_index = proposal.round_index.saturating_add(1);
    if proposal_round_exists(proposals, &proposal.proposal_id, next_round_index) {
        return Ok(false);
    }
    let network_id = resolve_network_id(node);
    crate::control::emit_topic_message_with_content(
        node,
        state_dir,
        &network_id,
        feed_key,
        scope_hint,
        json!({
            "kind": TOPIC_CONSENSUS_MESSAGE_KIND_PROPOSAL,
            "proposal_id": proposal.proposal_id.clone(),
            "goal": proposal.goal.clone(),
            "participants": if distributed_mode {
                json!(effective_participants)
            } else {
                json!(proposal.participants)
            },
            "min_participants": proposal.min_participants,
            "threshold_percent": proposal.threshold_percent,
            "result_feed_key": proposal.result_feed_key.clone(),
            "coordinator_node_id": node.node_id(),
            "round_index": next_round_index,
            "max_rounds": proposal.max_rounds,
            "round_timeout_ms": proposal.round_timeout_ms,
            "fallback_decision": proposal.fallback_decision.clone(),
            "previous_round_proposal_message_id": proposal.source_message_id.clone(),
        }),
        Some(proposal.source_message_id.clone()),
        created_at,
    )?;
    Ok(true)
}

pub fn process_structured_topic_consensus_for_topic(
    node: &mut Node,
    state_dir: &std::path::Path,
    feed_key: &str,
    scope_hint: &str,
) -> Result<usize> {
    let local_node_id = node.node_id();
    let messages = list_all_topic_messages(node, feed_key, scope_hint, 500)?;
    if messages.is_empty() {
        return Ok(0);
    }

    let mut proposals = Vec::new();
    for message in &messages {
        if let Some(proposal) = parse_structured_proposal(message) {
            proposals.push(proposal);
        }
    }
    let mut latest_stances: HashMap<(String, String), StructuredStance> = HashMap::new();
    for message in &messages {
        if let Some(stance) = parse_structured_stance(message, &proposals) {
            latest_stances.insert(
                (
                    stance.proposal_message_id.clone(),
                    stance.author_node_id.clone(),
                ),
                stance,
            );
        }
    }

    let distributed_mode = distributed_topic_mode(node);
    let observed_at = now_ms();
    let mut processed = 0_usize;
    for proposal in proposals.clone() {
        if proposal.coordinator_node_id != local_node_id {
            continue;
        }
        let effective_participants =
            effective_participant_list(&proposal, &proposal.coordinator_node_id, distributed_mode);
        let participant_filter = if proposal.participants.is_empty() {
            None
        } else {
            Some(
                effective_participants
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
            )
        };
        let mut stances = latest_stances
            .iter()
            .filter(|((proposal_message_id, _), _)| {
                proposal_message_id == &proposal.source_message_id
            })
            .map(|(_, stance)| stance.clone())
            .collect::<Vec<_>>();
        if distributed_mode {
            stances.retain(|stance| stance.author_node_id != proposal.coordinator_node_id);
        }
        if let Some(participants) = participant_filter.as_ref() {
            stances.retain(|stance| participants.contains(&stance.author_node_id));
        }
        stances.sort_by(|left, right| left.author_node_id.cmp(&right.author_node_id));

        let total_participants =
            resolve_required_participant_count(&proposal, &effective_participants, stances.len());
        let required_count = threshold_count(total_participants, proposal.threshold_percent);
        if required_count == 0 {
            continue;
        }
        let support_count = stances
            .iter()
            .filter(|stance| stance.stance == "support")
            .count();
        let reject_count = stances
            .iter()
            .filter(|stance| stance.stance == "reject")
            .count();
        let abstain_count = stances
            .iter()
            .filter(|stance| stance.stance == "abstain")
            .count();
        let decision = if support_count >= required_count {
            Some("support")
        } else if reject_count >= required_count {
            Some("reject")
        } else {
            None
        };
        if let Some(decision) = decision {
            if !result_message_exists(
                node,
                &proposal.result_feed_key,
                scope_hint,
                &proposal.source_message_id,
            )? {
                publish_consensus_result_message(
                    node,
                    state_dir,
                    feed_key,
                    scope_hint,
                    &proposal.result_feed_key,
                    &proposal,
                    &effective_participants,
                    decision,
                    support_count,
                    reject_count,
                    abstain_count,
                    required_count,
                    false,
                    Some("threshold_met"),
                    stances.len(),
                    now_ms(),
                )?;
            }
            processed = processed.saturating_add(1);
            continue;
        }

        if proposal.round_timeout_ms == 0
            || observed_at
                < proposal
                    .created_at
                    .saturating_add(proposal.round_timeout_ms)
        {
            continue;
        }

        if proposal.round_index < proposal.max_rounds {
            if maybe_publish_next_round_proposal(
                node,
                state_dir,
                feed_key,
                scope_hint,
                &proposal,
                &effective_participants,
                distributed_mode,
                &proposals,
                observed_at.saturating_add(1),
            )? {
                processed = processed.saturating_add(1);
            }
            continue;
        }

        let fallback = fallback_decision(&proposal, support_count, reject_count);
        if !result_message_exists(
            node,
            &proposal.result_feed_key,
            scope_hint,
            &proposal.source_message_id,
        )? {
            publish_consensus_result_message(
                node,
                state_dir,
                feed_key,
                scope_hint,
                &proposal.result_feed_key,
                &proposal,
                &effective_participants,
                &fallback,
                support_count,
                reject_count,
                abstain_count,
                required_count,
                true,
                Some("max_rounds_fallback"),
                stances.len(),
                now_ms(),
            )?;
        }
        processed = processed.saturating_add(1);
    }

    Ok(processed)
}

pub fn process_structured_topic_consensus(
    node: &mut Node,
    state_dir: &std::path::Path,
) -> Result<usize> {
    let subscriptions = node.store.list_active_feed_subscriptions(&node.node_id())?;
    let mut processed = 0_usize;
    for subscription in subscriptions {
        processed = processed.saturating_add(process_structured_topic_consensus_for_topic(
            node,
            state_dir,
            &subscription.feed_key,
            &subscription.scope_hint,
        )?);
    }
    Ok(processed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal_with_participants(participants: &[&str]) -> StructuredProposal {
        StructuredProposal {
            proposal_id: "proposal-1".to_owned(),
            source_message_id: "message-1".to_owned(),
            coordinator_node_id: "node-a".to_owned(),
            participants: participants
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            min_participants: participants.len().max(1),
            threshold_percent: 60,
            result_feed_key: "feed.result".to_owned(),
            goal: Some("decide".to_owned()),
            round_index: 1,
            max_rounds: 1,
            round_timeout_ms: 0,
            fallback_decision: None,
            created_at: 10,
        }
    }

    #[test]
    fn effective_participant_list_excludes_steward_in_distributed_mode() {
        let proposal = proposal_with_participants(&["node-a", "node-b", "node-c", "node-b"]);
        let participants = effective_participant_list(&proposal, "node-a", true);
        assert_eq!(participants, vec!["node-b".to_owned(), "node-c".to_owned()]);
    }

    #[test]
    fn effective_participant_list_keeps_steward_in_local_mode() {
        let proposal = proposal_with_participants(&["node-a", "node-b"]);
        let participants = effective_participant_list(&proposal, "node-a", false);
        assert_eq!(participants, vec!["node-a".to_owned(), "node-b".to_owned()]);
    }

    #[test]
    fn effective_participant_list_dedups_participants_when_only_steward_exists() {
        let proposal = proposal_with_participants(&["node-a"]);
        let participants = effective_participant_list(&proposal, "node-a", true);
        assert!(participants.is_empty());
    }

    #[test]
    fn resolve_required_participant_count_prefers_min_participants() {
        let mut proposal = proposal_with_participants(&["node-a", "node-b"]);
        proposal.min_participants = 3;
        let count = resolve_required_participant_count(&proposal, &["node-b".to_owned()], 1);
        assert_eq!(count, 3);
    }
}
