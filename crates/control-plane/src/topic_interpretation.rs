use crate::control::{prepare_runtime_for_executor, run_existing_task_with_runtime};
use crate::crypto::sha256_hex;
use crate::node::Node;
use crate::storage::storage::TopicMessageRow;
use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const TOPIC_INTERPRETATION_TASK_TYPE: &str = "topic_interpretation";
const TOPIC_INTERPRETATION_MESSAGE_KIND: &str = "interpreted_stance";
const CORE_AGENT_EXECUTOR_NAME: &str = "core-agent";
const DEFAULT_PROFILE: &str = "default";
const MIN_INTERPRETATION_CONFIDENCE: f64 = 0.65;

#[derive(Debug, Clone)]
struct StructuredProposal {
    proposal_id: String,
    source_message_id: String,
    coordinator_node_id: String,
    participants: Vec<String>,
    threshold_percent: u32,
    result_feed_key: String,
    goal: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct InterpretationOutput {
    source_message_id: String,
    proposal_id: String,
    #[serde(default)]
    proposal_message_id: String,
    stance: String,
    answer: String,
    summary: String,
    confidence: f64,
    #[serde(default)]
    evidence: Vec<Value>,
    #[serde(default)]
    needs_review: bool,
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
    if obj.get("kind")?.as_str()? != "proposal" {
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
    Some(StructuredProposal {
        proposal_id,
        source_message_id: message.message_id.clone(),
        coordinator_node_id,
        participants,
        threshold_percent,
        result_feed_key,
        goal: obj
            .get("goal")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
    })
}

fn list_all_topic_messages(
    node: &Node,
    feed_key: &str,
    scope_hint: &str,
    max_messages: usize,
) -> Result<Vec<TopicMessageRow>> {
    let network_id = resolve_network_id(node);
    let mut out = Vec::new();
    let mut before_created_at = None;
    let mut before_message_id = None;
    while out.len() < max_messages {
        let page = node.store.list_topic_messages_page(
            &network_id,
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
                .is_some_and(|kind| kind == "consensus_result")
                && message
                    .content
                    .get("proposal_message_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == proposal_message_id)
        }))
}

fn is_structured_topic_message(message: &TopicMessageRow) -> bool {
    matches!(
        message.content.get("kind").and_then(Value::as_str),
        Some("proposal" | "stance" | "consensus_result" | "interpreted_stance")
    )
}

fn extract_message_text(content: &Value) -> Option<String> {
    if let Some(text) = content.as_str() {
        let trimmed = text.trim();
        return (!trimmed.is_empty()).then_some(trimmed.to_owned());
    }
    let obj = content.as_object()?;
    for key in ["text", "body", "message", "summary"] {
        if let Some(text) = obj.get(key).and_then(Value::as_str) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    None
}

fn interpreted_message_exists(messages: &[TopicMessageRow], source_message_id: &str) -> bool {
    messages.iter().any(|message| {
        message
            .content
            .get("kind")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == TOPIC_INTERPRETATION_MESSAGE_KIND)
            && message
                .content
                .get("source_message_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value == source_message_id)
    })
}

fn deterministic_topic_interpretation_task_id(
    network_id: &str,
    feed_key: &str,
    scope_hint: &str,
    source_message_id: &str,
) -> String {
    let digest = sha256_hex(
        format!("topic-interpretation-v1|{network_id}|{feed_key}|{scope_hint}|{source_message_id}")
            .as_bytes(),
    );
    format!("topic-interpretation-{}", &digest[..24])
}

fn build_thread_context(messages: &[TopicMessageRow], source_message_id: &str) -> Vec<Value> {
    let source_index = messages
        .iter()
        .position(|message| message.message_id == source_message_id)
        .unwrap_or(messages.len());
    let start = source_index.saturating_sub(5);
    messages[start..source_index]
        .iter()
        .filter_map(|message| {
            extract_message_text(&message.content).map(|text| {
                json!({
                    "message_id": message.message_id,
                    "author_node_id": message.author_node_id,
                    "reply_to_message_id": message.reply_to_message_id,
                    "text": text
                })
            })
        })
        .collect()
}

fn select_candidate_proposals(
    message: &TopicMessageRow,
    proposals: &[StructuredProposal],
) -> Vec<StructuredProposal> {
    if let Some(reply_to_message_id) = message.reply_to_message_id.as_deref()
        && let Some(proposal) = proposals
            .iter()
            .find(|proposal| proposal.source_message_id == reply_to_message_id)
    {
        return vec![proposal.clone()];
    }
    proposals.to_vec()
}

fn local_executor_proposals(
    proposals: &[StructuredProposal],
    local_node_id: &str,
    distributed_mode: bool,
) -> Vec<StructuredProposal> {
    proposals
        .iter()
        .filter(|proposal| {
            if !distributed_mode {
                return true;
            }
            if proposal.coordinator_node_id == local_node_id {
                return false;
            }
            if proposal.participants.is_empty() {
                return true;
            }
            proposal
                .participants
                .iter()
                .any(|participant| participant == local_node_id)
        })
        .cloned()
        .collect()
}

fn build_interpretation_task_contract(
    node: &Node,
    feed_key: &str,
    scope_hint: &str,
    message: &TopicMessageRow,
    proposals: &[StructuredProposal],
    all_messages: &[TopicMessageRow],
) -> Result<crate::types::TaskContract> {
    let task_id = deterministic_topic_interpretation_task_id(
        &resolve_network_id(node),
        feed_key,
        scope_hint,
        &message.message_id,
    );
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))?
        .policy_hash;
    let mut contract = crate::task_template::sample_contract(&task_id, policy_hash);
    contract.task_type = TOPIC_INTERPRETATION_TASK_TYPE.to_owned();
    contract.output_schema = json!({
        "type": "object",
        "required": [
            "source_message_id",
            "proposal_id",
            "proposal_message_id",
            "stance",
            "answer",
            "summary",
            "confidence",
            "needs_review"
        ],
        "properties": {
            "source_message_id": {"type": "string"},
            "proposal_id": {"type": "string"},
            "proposal_message_id": {"type": "string"},
            "stance": {"type": "string"},
            "answer": {"type": "string"},
            "summary": {"type": "string"},
            "confidence": {"type": "number"},
            "needs_review": {"type": "boolean"},
            "evidence": {"type": "array"}
        }
    });
    contract.inputs = json!({
        "feed_key": feed_key,
        "scope_hint": scope_hint,
        "source_message": {
            "message_id": message.message_id,
            "author_node_id": message.author_node_id,
            "reply_to_message_id": message.reply_to_message_id,
            "content": message.content,
            "text": extract_message_text(&message.content).unwrap_or_default()
        },
        "candidate_proposals": proposals.iter().map(|proposal| {
            json!({
                "proposal_id": proposal.proposal_id,
                "proposal_message_id": proposal.source_message_id,
                "coordinator_node_id": proposal.coordinator_node_id,
                "participants": proposal.participants,
                "threshold_percent": proposal.threshold_percent,
                "result_feed_key": proposal.result_feed_key,
                "goal": proposal.goal
            })
        }).collect::<Vec<_>>(),
        "thread_context": build_thread_context(all_messages, &message.message_id),
        "prior_deliberations": load_prior_deliberations(node, feed_key, scope_hint, proposals)?
    });
    Ok(contract)
}

fn parse_prior_deliberation_message(
    message: &TopicMessageRow,
    excluded_proposal_message_ids: &[String],
) -> Option<Value> {
    let content = message.content.as_object()?;
    if content.get("kind")?.as_str()? != "consensus_result" {
        return None;
    }
    let proposal_message_id = content.get("proposal_message_id")?.as_str()?;
    if excluded_proposal_message_ids
        .iter()
        .any(|current| current == proposal_message_id)
    {
        return None;
    }
    Some(json!({
        "message_id": message.message_id,
        "finalized_at": message.created_at,
        "proposal_id": content.get("proposal_id").and_then(Value::as_str).unwrap_or_default(),
        "proposal_message_id": proposal_message_id,
        "decision": content
            .get("decision")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase(),
        "goal": content.get("goal").and_then(Value::as_str),
        "participants": content.get("participants").cloned().unwrap_or_else(|| json!([])),
        "source_feed_key": content.get("source_feed_key").cloned().unwrap_or_else(|| json!(null)),
        "source_scope_hint": content
            .get("source_scope_hint")
            .cloned()
            .unwrap_or_else(|| json!(null))
    }))
}

fn load_prior_deliberations(
    node: &Node,
    _feed_key: &str,
    scope_hint: &str,
    proposals: &[StructuredProposal],
) -> Result<Vec<Value>> {
    let network_id = resolve_network_id(node);
    let excluded_proposal_message_ids = proposals
        .iter()
        .map(|proposal| proposal.source_message_id.clone())
        .collect::<Vec<_>>();
    let result_feeds = proposals
        .iter()
        .map(|proposal| proposal.result_feed_key.clone())
        .collect::<BTreeSet<_>>();
    let mut prior = Vec::new();
    for result_feed_key in result_feeds {
        prior.extend(
            node.store
                .list_topic_messages(&network_id, &result_feed_key, scope_hint, 64)?
                .into_iter()
                .filter_map(|message| {
                    parse_prior_deliberation_message(&message, &excluded_proposal_message_ids)
                }),
        );
    }
    prior.sort_by(|left, right| {
        left["finalized_at"]
            .as_u64()
            .cmp(&right["finalized_at"].as_u64())
    });
    Ok(prior)
}

fn finalized_interpretation_output(
    node: &Node,
    task_id: &str,
) -> Result<Option<InterpretationOutput>> {
    let Some(task) = node.task_view(task_id)? else {
        return Ok(None);
    };
    let Some(candidate_id) = task.finalized_candidate_id.as_deref() else {
        return Ok(None);
    };
    let Some(candidate) = node.store.get_candidate_by_id(task_id, candidate_id)? else {
        return Ok(None);
    };
    let parsed = serde_json::from_value::<InterpretationOutput>(candidate.output)?;
    Ok(Some(parsed))
}

fn should_publish_interpreted_stance(
    message: &TopicMessageRow,
    proposals: &[StructuredProposal],
    output: &InterpretationOutput,
) -> bool {
    if output.source_message_id != message.message_id {
        return false;
    }
    if output.proposal_message_id.is_empty() {
        return false;
    }
    if output.needs_review || output.confidence < MIN_INTERPRETATION_CONFIDENCE {
        return false;
    }
    if !matches!(output.stance.as_str(), "support" | "reject" | "abstain") {
        return false;
    }
    proposals.iter().any(|proposal| {
        proposal.proposal_id == output.proposal_id
            && proposal.source_message_id == output.proposal_message_id
    })
}

fn publish_interpreted_stance(
    node: &mut Node,
    state_dir: &std::path::Path,
    message: &TopicMessageRow,
    output: &InterpretationOutput,
    created_at: u64,
) -> Result<()> {
    let network_id = resolve_network_id(node);
    crate::control::emit_topic_message_with_content(
        node,
        state_dir,
        &network_id,
        &message.feed_key,
        &message.scope_hint,
        json!({
            "kind": TOPIC_INTERPRETATION_MESSAGE_KIND,
            "proposal_id": output.proposal_id,
            "proposal_message_id": output.proposal_message_id,
            "stance": output.stance,
            "summary": output.summary,
            "confidence": output.confidence,
            "source_message_id": message.message_id,
            "source_author_node_id": message.author_node_id,
            "interpretation_answer": output.answer,
            "evidence": output.evidence
        }),
        Some(message.message_id.clone()),
        created_at,
    )?;
    Ok(())
}

fn maybe_interpret_message(
    node: &mut Node,
    state_dir: &std::path::Path,
    message: &TopicMessageRow,
    proposals: &[StructuredProposal],
    all_messages: &[TopicMessageRow],
) -> Result<bool> {
    let message_text = extract_message_text(&message.content);
    if message_text.is_none() || is_structured_topic_message(message) {
        return Ok(false);
    }
    if interpreted_message_exists(all_messages, &message.message_id) {
        return Ok(false);
    }
    let candidate_proposals = select_candidate_proposals(message, proposals);
    if candidate_proposals.is_empty() {
        return Ok(false);
    }

    let task_id = deterministic_topic_interpretation_task_id(
        &resolve_network_id(node),
        &message.feed_key,
        &message.scope_hint,
        &message.message_id,
    );
    let mut output = finalized_interpretation_output(node, &task_id)?;
    if output.is_none() {
        let prepared = match prepare_runtime_for_executor(
            state_dir,
            CORE_AGENT_EXECUTOR_NAME,
            DEFAULT_PROFILE,
        ) {
            Ok(prepared) => prepared,
            Err(err) if err.to_string().contains("executor not found") => return Ok(false),
            Err(err) => return Err(err),
        };
        if node.task_view(&task_id)?.is_none() {
            let contract = build_interpretation_task_contract(
                node,
                &message.feed_key,
                &message.scope_hint,
                message,
                &candidate_proposals,
                all_messages,
            )?;
            let checkpoint_contract = contract.clone();
            let observed_at = now_ms();
            node.submit_task(contract, 1, observed_at)?;
            let _ = crate::control::materialize_round_checkpoint_artifact(
                state_dir,
                node,
                &checkpoint_contract,
                1,
                &node.node_id(),
                crate::types::RoundCheckpointPhase::Opening,
                Vec::new(),
                None,
                None,
                observed_at,
            )?;
        }
        let _ = run_existing_task_with_runtime(
            node,
            state_dir,
            prepared.runtime.as_ref(),
            &prepared.capabilities,
            CORE_AGENT_EXECUTOR_NAME,
            DEFAULT_PROFILE,
            &task_id,
            now_ms(),
        )?;
        output = finalized_interpretation_output(node, &task_id)?;
    }

    let Some(output) = output else {
        return Ok(false);
    };
    if !should_publish_interpreted_stance(message, &candidate_proposals, &output) {
        return Ok(false);
    }
    publish_interpreted_stance(node, state_dir, message, &output, now_ms())?;
    Ok(true)
}

pub fn process_topic_interpretation_for_topic(
    node: &mut Node,
    state_dir: &std::path::Path,
    feed_key: &str,
    scope_hint: &str,
) -> Result<usize> {
    let messages = list_all_topic_messages(node, feed_key, scope_hint, 500)?;
    if messages.is_empty() {
        return Ok(0);
    }

    let mut proposals = Vec::new();
    for message in &messages {
        if let Some(proposal) = parse_structured_proposal(message)
            && !result_message_exists(
                node,
                &proposal.result_feed_key,
                scope_hint,
                &proposal.source_message_id,
            )?
        {
            proposals.push(proposal);
        }
    }
    if proposals.is_empty() {
        return Ok(0);
    }
    let proposals =
        local_executor_proposals(&proposals, &node.node_id(), distributed_topic_mode(node));
    if proposals.is_empty() {
        return Ok(0);
    }

    let mut published = 0_usize;
    for message in &messages {
        if maybe_interpret_message(node, state_dir, message, &proposals, &messages)? {
            published = published.saturating_add(1);
        }
    }
    Ok(published)
}

pub fn process_topic_interpretation(node: &mut Node, state_dir: &std::path::Path) -> Result<usize> {
    let network_id = resolve_network_id(node);
    let subscriptions = node
        .store
        .list_active_feed_subscriptions(&network_id, &node.node_id())?;
    let mut processed = 0_usize;
    for subscription in subscriptions {
        processed = processed.saturating_add(process_topic_interpretation_for_topic(
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

    fn proposal(participants: &[&str]) -> StructuredProposal {
        StructuredProposal {
            proposal_id: "proposal-1".to_owned(),
            source_message_id: "message-1".to_owned(),
            coordinator_node_id: "node-a".to_owned(),
            participants: participants
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            threshold_percent: 60,
            result_feed_key: "feed.result".to_owned(),
            goal: Some("decide".to_owned()),
        }
    }

    #[test]
    fn local_executor_proposals_excludes_coordinator_in_distributed_mode() {
        let proposals = vec![proposal(&["node-a", "node-b"])];
        assert!(local_executor_proposals(&proposals, "node-a", true).is_empty());
        assert_eq!(
            local_executor_proposals(&proposals, "node-b", true).len(),
            1
        );
    }

    #[test]
    fn local_executor_proposals_keeps_local_behavior() {
        let proposals = vec![proposal(&["node-a", "node-b"])];
        assert_eq!(
            local_executor_proposals(&proposals, "node-a", false).len(),
            1
        );
    }
}
