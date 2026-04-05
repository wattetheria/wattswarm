use crate::crypto::{candidate_hash, sha256_hex, vote_commit_hash};
use crate::node::{Node, finality_sign};
use crate::storage::storage::TopicMessageRow;
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};

pub const TOPIC_CONSENSUS_TASK_TYPE: &str = "topic_consensus";
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
    threshold_percent: u32,
    result_feed_key: String,
    goal: Option<String>,
}

#[derive(Debug, Clone)]
struct StructuredStance {
    proposal_message_id: String,
    author_node_id: String,
    stance: String,
    summary: Option<String>,
    evidence_refs: Vec<crate::types::ArtifactRef>,
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
            .unwrap_or_else(|| "topic-consensus-bridge".to_owned()),
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
    let evidence_refs = obj
        .get("evidence_refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_artifact_ref)
        .collect();
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
        summary: obj
            .get("summary")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        evidence_refs,
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

fn deterministic_topic_consensus_task_id(
    network_id: &str,
    feed_key: &str,
    scope_hint: &str,
    proposal_message_id: &str,
) -> String {
    let digest = sha256_hex(
        format!("topic-consensus-v2|{network_id}|{feed_key}|{scope_hint}|{proposal_message_id}")
            .as_bytes(),
    );
    format!("topic-consensus-{}", &digest[..24])
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

fn publish_consensus_result_message(
    node: &mut Node,
    state_dir: &std::path::Path,
    scope_hint: &str,
    result_feed_key: &str,
    proposal: &StructuredProposal,
    task_id: &str,
    decision: &str,
    support_count: usize,
    reject_count: usize,
    abstain_count: usize,
    required_count: usize,
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
            "threshold_percent": proposal.threshold_percent,
            "participants": proposal.participants.clone(),
            "coordinator_node_id": proposal.coordinator_node_id.clone(),
            "consensus_task_id": task_id,
            "goal": proposal.goal.clone(),
        }),
        None,
        created_at,
    )?;
    Ok(())
}

fn finalize_topic_consensus_task(
    node: &mut Node,
    state_dir: &std::path::Path,
    feed_key: &str,
    scope_hint: &str,
    proposal: &StructuredProposal,
    stances: &[StructuredStance],
    decision: &str,
    required_count: usize,
) -> Result<String> {
    let now = now_ms();
    let task_id = deterministic_topic_consensus_task_id(
        &resolve_network_id(node),
        feed_key,
        scope_hint,
        &proposal.source_message_id,
    );
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))?
        .policy_hash;
    let mut contract = crate::task_template::sample_contract(&task_id, policy_hash);
    contract.task_type = TOPIC_CONSENSUS_TASK_TYPE.to_owned();
    contract.inputs = json!({
        "proposal_id": proposal.proposal_id.clone(),
        "proposal_message_id": proposal.source_message_id.clone(),
        "goal": proposal.goal.clone(),
        "feed_key": feed_key,
        "scope_hint": scope_hint,
        "threshold_percent": proposal.threshold_percent,
        "required_count": required_count,
        "participants": proposal.participants.clone(),
        "stances": stances.iter().map(|stance| {
            json!({
                "author_node_id": stance.author_node_id,
                "proposal_message_id": stance.proposal_message_id,
                "stance": stance.stance,
                "summary": stance.summary,
                "evidence_refs": stance.evidence_refs,
            })
        }).collect::<Vec<_>>(),
    });
    contract.acceptance.quorum_threshold = 1;
    contract.acceptance.da_quorum_threshold = 1;

    if node.task_view(&task_id)?.is_none() {
        node.submit_task(contract, 1, now)?;
    }

    let candidate_id = format!("consensus-{}", proposal.proposal_id);
    let execution_id = format!(
        "topic-consensus-{}",
        &task_id[task_id.len().saturating_sub(8)..]
    );
    let verify_execution_id = format!("verify-{execution_id}");
    let lease_until = now.saturating_add(4_000);
    let candidate_output = json!({
        "decision": decision.to_ascii_uppercase(),
        "answer": decision,
        "confidence": 1.0,
        "check_summary": format!("structured topic consensus reached for proposal {}", proposal.proposal_id),
        "proposal_id": proposal.proposal_id.clone(),
        "proposal_message_id": proposal.source_message_id.clone(),
        "feed_key": feed_key,
        "scope_hint": scope_hint,
        "goal": proposal.goal.clone(),
        "participants": proposal.participants.clone(),
    });
    let output_ref = crate::control::materialize_candidate_output_artifact(
        state_dir,
        &node.node_id(),
        &candidate_output,
        now,
    )?;
    let candidate = crate::types::Candidate {
        candidate_id: candidate_id.clone(),
        execution_id: execution_id.clone(),
        output_ref,
        output: candidate_output,
        evidence_inline: vec![crate::types::InlineEvidence {
            mime: "application/json".to_owned(),
            content: serde_json::to_string(&json!({
                "proposal_id": proposal.proposal_id.clone(),
                "proposal_message_id": proposal.source_message_id.clone(),
                "feed_key": feed_key,
                "scope_hint": scope_hint,
                "goal": proposal.goal.clone(),
                "stances": stances.iter().map(|stance| {
                    json!({
                        "author_node_id": stance.author_node_id,
                        "proposal_message_id": stance.proposal_message_id,
                        "stance": stance.stance,
                        "summary": stance.summary
                    })
                }).collect::<Vec<_>>(),
            }))?,
        }],
        evidence_refs: stances
            .iter()
            .flat_map(|stance| stance.evidence_refs.clone())
            .collect(),
    };
    let candidate_hash_value = candidate_hash(&candidate)?;
    let verifier_result_hash = sha256_hex(
        serde_json::to_string(&json!({
            "task_id": task_id.clone(),
            "candidate_id": candidate_id.clone(),
            "decision": decision,
            "proposal_id": proposal.proposal_id.clone()
            ,
            "proposal_message_id": proposal.source_message_id.clone()
        }))?
        .as_bytes(),
    );

    node.claim_task(
        &task_id,
        crate::types::ClaimRole::Propose,
        &execution_id,
        lease_until,
        1,
        now.saturating_add(1),
    )
    .context("claim topic consensus propose lease")?;
    node.propose_candidate(&task_id, candidate.clone(), 1, now.saturating_add(2))
        .context("propose topic consensus candidate")?;
    if !candidate.evidence_refs.is_empty() {
        node.add_evidence(
            &task_id,
            &candidate_id,
            &execution_id,
            candidate.evidence_refs.clone(),
            1,
            now.saturating_add(3),
        )
        .context("attach topic consensus evidence refs")?;
    }
    node.claim_task(
        &task_id,
        crate::types::ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
        1,
        now.saturating_add(5),
    )
    .context("claim topic consensus verify lease")?;
    for evidence in &candidate.evidence_refs {
        node.evidence_available(
            &task_id,
            &candidate_id,
            &verify_execution_id,
            &evidence.digest,
            1,
            now.saturating_add(6),
        )
        .with_context(|| {
            format!(
                "mark topic consensus evidence available {}",
                evidence.digest
            )
        })?;
    }
    node.submit_verifier_result(
        &task_id,
        crate::types::VerifierResult {
            candidate_id: candidate_id.clone(),
            execution_id: verify_execution_id.clone(),
            verification_status: crate::types::VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![wattswarm_protocol::reason_codes::REASON_SCHEMA_OK],
            verifier_result_hash: verifier_result_hash.clone(),
            provider_family: "topic-consensus-bridge".to_owned(),
            model_id: "structured-v1".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: node
                .task_view(&task_id)?
                .ok_or_else(|| anyhow!("task missing after submit: {task_id}"))?
                .contract
                .acceptance
                .verifier_policy
                .policy_hash,
        },
        1,
        now.saturating_add(7),
    )
    .context("submit topic consensus verifier result")?;
    let salt = format!("topic-consensus-salt-{}", proposal.proposal_id);
    let commit_hash = vote_commit_hash(
        crate::types::VoteChoice::Approve,
        &salt,
        &verifier_result_hash,
    );
    node.submit_vote_commit(
        crate::types::VoteCommitPayload {
            task_id: task_id.clone(),
            candidate_id: candidate_id.clone(),
            candidate_hash: candidate_hash_value.clone(),
            execution_id: verify_execution_id.clone(),
            verifier_result_hash: verifier_result_hash.clone(),
            commit_hash,
        },
        1,
        now.saturating_add(8),
    )
    .context("submit topic consensus vote commit")?;
    node.submit_vote_reveal(
        crate::types::VoteRevealPayload {
            task_id: task_id.clone(),
            candidate_id: candidate_id.clone(),
            candidate_hash: candidate_hash_value,
            execution_id: verify_execution_id,
            verifier_result_hash,
            vote: crate::types::VoteChoice::Approve,
            salt,
        },
        1,
        now.saturating_add(9),
    )
    .context("submit topic consensus vote reveal")?;
    node.commit_decision(&task_id, 1, &candidate_id, now.saturating_add(10))
        .context("commit topic consensus decision")?;
    node.finalize_decision(
        &task_id,
        1,
        &candidate_id,
        crate::types::FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(&node.identity, &task_id, 1, &candidate_id)],
        },
        now.saturating_add(11),
    )
    .context("finalize topic consensus decision")?;
    Ok(task_id)
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

    let mut processed = 0_usize;
    for proposal in proposals {
        if proposal.coordinator_node_id != local_node_id {
            continue;
        }
        let participant_filter = if proposal.participants.is_empty() {
            None
        } else {
            Some(
                proposal
                    .participants
                    .iter()
                    .map(|participant| participant.trim().to_owned())
                    .filter(|participant| !participant.is_empty())
                    .collect::<BTreeSet<_>>(),
            )
        };
        let mut stances = latest_stances
            .iter()
            .filter(|((proposal_message_id, _), _)| {
                proposal_message_id == &proposal.source_message_id
            })
            .filter(|((_, author_node_id), _)| {
                participant_filter
                    .as_ref()
                    .is_none_or(|participants| participants.contains(author_node_id))
            })
            .map(|(_, stance)| stance.clone())
            .collect::<Vec<_>>();
        if stances.is_empty() {
            continue;
        }
        stances.sort_by(|left, right| left.author_node_id.cmp(&right.author_node_id));

        let total_participants = if proposal.participants.is_empty() {
            stances.len()
        } else {
            proposal.participants.len()
        };
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
        let Some(decision) = decision else {
            continue;
        };

        let deterministic_task_id = deterministic_topic_consensus_task_id(
            &resolve_network_id(node),
            feed_key,
            scope_hint,
            &proposal.source_message_id,
        );
        let already_finalized = node
            .task_view(&deterministic_task_id)?
            .is_some_and(|task| task.terminal_state == crate::types::TaskTerminalState::Finalized);
        let task_id = if already_finalized {
            deterministic_task_id
        } else {
            finalize_topic_consensus_task(
                node,
                state_dir,
                feed_key,
                scope_hint,
                &proposal,
                &stances,
                decision,
                required_count,
            )?
        };
        if !result_message_exists(
            node,
            &proposal.result_feed_key,
            scope_hint,
            &proposal.source_message_id,
        )? {
            publish_consensus_result_message(
                node,
                state_dir,
                scope_hint,
                &proposal.result_feed_key,
                &proposal,
                &task_id,
                decision,
                support_count,
                reject_count,
                abstain_count,
                required_count,
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
