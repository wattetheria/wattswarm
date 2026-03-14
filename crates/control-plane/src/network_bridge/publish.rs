use super::*;

const AUTO_PUBLISH_BATCH_LIMIT: usize = 64;

fn is_high_frequency_global_event(payload: &crate::types::EventPayload) -> bool {
    matches!(
        payload,
        crate::types::EventPayload::TaskClaimed(_)
            | crate::types::EventPayload::TaskClaimRenewed(_)
            | crate::types::EventPayload::TaskClaimReleased(_)
            | crate::types::EventPayload::CandidateProposed(_)
            | crate::types::EventPayload::EvidenceAdded(_)
            | crate::types::EventPayload::EvidenceAvailable(_)
            | crate::types::EventPayload::VerifierResultSubmitted(_)
            | crate::types::EventPayload::VoteCommit(_)
            | crate::types::EventPayload::VoteReveal(_)
            | crate::types::EventPayload::DecisionCommitted(_)
    )
}

#[derive(Debug, Clone)]
pub(super) struct GlobalPublishRateGuard {
    window_started_at: Instant,
    high_frequency_events_in_window: usize,
}

impl GlobalPublishRateGuard {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            window_started_at: now,
            high_frequency_events_in_window: 0,
        }
    }

    pub(super) fn allow(&mut self, scope: &SwarmScope, event: &crate::types::Event) -> bool {
        if *scope != SwarmScope::Global || !is_high_frequency_global_event(&event.payload) {
            return true;
        }
        let now = Instant::now();
        if now.duration_since(self.window_started_at) >= GLOBAL_HIGH_FREQUENCY_WINDOW {
            self.window_started_at = now;
            self.high_frequency_events_in_window = 0;
        }
        if self.high_frequency_events_in_window >= GLOBAL_HIGH_FREQUENCY_LIMIT {
            return false;
        }
        self.high_frequency_events_in_window += 1;
        true
    }
}

pub fn publish_pending_scoped_updates(
    service: &mut NetworkBridgeService,
    node: &Node,
    local_node_id: &str,
    from_event_seq: u64,
) -> Result<u64> {
    let rows = node
        .store
        .load_events_page(from_event_seq, AUTO_PUBLISH_BATCH_LIMIT)?;
    let mut last_published_seq = from_event_seq;
    let local_node_penalized = node.store.is_node_penalized(local_node_id)?;
    let publish_summaries = super::should_publish_summaries(node.head_seq()?, from_event_seq);
    for (seq, event) in rows {
        if event.author_node_id != local_node_id {
            last_published_seq = seq;
            continue;
        }
        if !super::should_sync_event(node, &event)? {
            last_published_seq = seq;
            continue;
        }
        let scope = super::event_scope(node, &event)?;
        if let crate::types::EventPayload::FeedSubscriptionUpdated(payload) = &event.payload
            && payload.subscriber_node_id == local_node_id
        {
            if payload.active {
                service.subscribe_scope(&scope)?;
            } else if !super::node_has_active_subscription_scope(node, local_node_id, &scope)? {
                service.unsubscribe_scope(&scope)?;
            }
        }
        let is_local_subscription_control_event = matches!(
            &event.payload,
            crate::types::EventPayload::FeedSubscriptionUpdated(_)
        );
        match service.publish_event_for_scope(&scope, event.clone()) {
            Ok(()) => last_published_seq = seq,
            Err(err)
                if err.to_string().contains("NoPeersSubscribedToTopic")
                    && is_local_subscription_control_event =>
            {
                last_published_seq = seq;
                continue;
            }
            Err(err) if err.to_string().contains("NoPeersSubscribedToTopic") => break,
            Err(err) if err.to_string().contains("GlobalTopicRateLimited") => break,
            Err(err) if err.to_string().contains("LocalTopicRateLimited") => break,
            Err(err) => return Err(err),
        }
        if publish_summaries
            && !local_node_penalized
            && let Some(summary) = super::knowledge_summary_for_event(
                node,
                &event,
                &scope,
                service.summary_decision_memory_limit,
            )?
        {
            let _ = service.publish_summary(summary);
        }
        if publish_summaries
            && !local_node_penalized
            && let Some(summary) = super::reputation_summary_for_event(node, &event)?
        {
            let _ = service.publish_summary(summary);
        }
    }
    Ok(last_published_seq)
}

pub fn publish_pending_global_events(
    service: &mut NetworkBridgeService,
    node: &Node,
    local_node_id: &str,
    from_event_seq: u64,
) -> Result<u64> {
    publish_pending_scoped_updates(service, node, local_node_id, from_event_seq)
}
