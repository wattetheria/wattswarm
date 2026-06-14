use super::scope::parse_scope_hint_string;
use super::*;

pub(super) struct EventRelevanceFilter;

impl EventRelevanceFilter {
    pub(super) fn should_deliver(
        node: &Node,
        state_dir: &Path,
        local_node_id: &str,
        event: &crate::types::Event,
    ) -> bool {
        if event.author_node_id == local_node_id {
            return false;
        }
        if let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload {
            return topic_message_is_relevant(node, state_dir, local_node_id, event, payload);
        }
        if agent_envelope_targets_local(event, local_node_id) {
            return true;
        }
        match &event.payload {
            crate::types::EventPayload::TaskClaimed(payload) => {
                task_originated_by_local(node, &payload.task_id, local_node_id)
            }
            crate::types::EventPayload::TaskClaimDecided(payload) => {
                payload.claimer_node_id == local_node_id
                    || task_execution_owned_by_local(
                        node,
                        &payload.task_id,
                        &payload.execution_id,
                        local_node_id,
                    )
            }
            crate::types::EventPayload::TaskCompleted(payload) => {
                task_originated_by_local(node, &payload.task_id, local_node_id)
            }
            crate::types::EventPayload::TaskCompletionDecided(payload) => {
                task_execution_owned_by_local(
                    node,
                    &payload.task_id,
                    &payload.execution_id,
                    local_node_id,
                )
            }
            crate::types::EventPayload::TaskSettled(payload) => task_execution_owned_by_local(
                node,
                &payload.task_id,
                &payload.execution_id,
                local_node_id,
            ),
            crate::types::EventPayload::CandidateProposed(payload) => {
                task_originated_by_local(node, &payload.task_id, local_node_id)
            }
            crate::types::EventPayload::DecisionFinalized(payload) => {
                task_originated_by_local(node, &payload.task_id, local_node_id)
            }
            crate::types::EventPayload::TaskError(payload) => {
                task_originated_by_local(node, &payload.task_id, local_node_id)
            }
            crate::types::EventPayload::TaskRetryScheduled(payload) => {
                task_originated_by_local(node, &payload.task_id, local_node_id)
            }
            _ => false,
        }
    }
}

fn agent_envelope_targets_local(event: &crate::types::Event, local_node_id: &str) -> bool {
    event_agent_envelope(event).and_then(|envelope| envelope.target_node_id.as_deref())
        == Some(local_node_id)
}

fn event_agent_envelope(event: &crate::types::Event) -> Option<&crate::types::AgentEnvelope> {
    match &event.payload {
        crate::types::EventPayload::TaskClaimed(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TaskClaimDecided(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TaskCompleted(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TaskCompletionDecided(payload) => {
            payload.agent_envelope.as_ref()
        }
        crate::types::EventPayload::TaskSettled(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::CandidateProposed(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::DecisionFinalized(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TaskError(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TaskRetryScheduled(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TaskAnnounced(payload) => payload.agent_envelope.as_ref(),
        crate::types::EventPayload::TopicMessagePosted(payload) => payload.agent_envelope.as_ref(),
        _ => None,
    }
}

fn task_originated_by_local(node: &Node, task_id: &str, local_node_id: &str) -> bool {
    node.store
        .get_task_announcement_for_task(task_id)
        .ok()
        .flatten()
        .is_some_and(|announcement| announcement.announced_by_node_id == local_node_id)
        || node
            .store
            .task_created_by_node_id(task_id)
            .ok()
            .flatten()
            .is_some_and(|creator_node_id| creator_node_id == local_node_id)
}

fn task_execution_owned_by_local(
    node: &Node,
    task_id: &str,
    execution_id: &str,
    local_node_id: &str,
) -> bool {
    ["propose", "verify"]
        .iter()
        .any(|role| local_lease_matches(node, task_id, role, execution_id, local_node_id))
        || node
            .store
            .get_candidate_proposer_by_execution(task_id, execution_id)
            .ok()
            .flatten()
            .is_some_and(|proposer_node_id| proposer_node_id == local_node_id)
}

fn local_lease_matches(
    node: &Node,
    task_id: &str,
    role: &str,
    execution_id: &str,
    local_node_id: &str,
) -> bool {
    node.store
        .get_lease(task_id, role)
        .ok()
        .flatten()
        .is_some_and(|lease| {
            lease.execution_id == execution_id && lease.claimer_node_id == local_node_id
        })
}

fn topic_message_is_relevant(
    node: &Node,
    state_dir: &Path,
    local_node_id: &str,
    event: &crate::types::Event,
    payload: &crate::types::TopicMessagePostedPayload,
) -> bool {
    if !topic_message_content_is_agent_deliverable(node, event, payload) {
        return false;
    }
    local_has_active_topic_subscription(node, local_node_id, payload)
        || local_has_ready_dm_thread(state_dir, &event.author_node_id, payload)
}

fn topic_message_content_is_agent_deliverable(
    node: &Node,
    event: &crate::types::Event,
    payload: &crate::types::TopicMessagePostedPayload,
) -> bool {
    if !crate::control::is_private_hive_route(&payload.feed_key, &payload.scope_hint) {
        return true;
    }
    node.store
        .get_topic_message(&event.event_id)
        .ok()
        .flatten()
        .is_some_and(|topic_message| {
            topic_message.content.get("kind").and_then(Value::as_str) != Some("private_encrypted")
        })
}

fn local_has_active_topic_subscription(
    node: &Node,
    local_node_id: &str,
    payload: &crate::types::TopicMessagePostedPayload,
) -> bool {
    let Some(subscription) = node
        .store
        .get_feed_subscription(&payload.network_id, local_node_id, &payload.feed_key)
        .ok()
        .flatten()
    else {
        return false;
    };
    if !subscription.active || subscription.scope_hint != canonical_scope_hint(&payload.scope_hint)
    {
        return false;
    }
    true
}

fn canonical_scope_hint(raw: &str) -> String {
    parse_scope_hint_string(raw).map_or_else(|| raw.to_owned(), |scope| scope_hint_label(&scope))
}

fn local_has_ready_dm_thread(
    state_dir: &Path,
    remote_node_id: &str,
    payload: &crate::types::TopicMessagePostedPayload,
) -> bool {
    if payload.feed_key != crate::control::PRIVATE_DM_FEED_KEY {
        return false;
    }
    crate::control::load_peer_dm_thread_record_for_remote_state(state_dir, remote_node_id)
        .ok()
        .flatten()
        .is_some_and(|record| {
            matches!(
                record.session_state,
                crate::control::PeerDmSessionState::Established
                    | crate::control::PeerDmSessionState::Ready
            )
        })
}
