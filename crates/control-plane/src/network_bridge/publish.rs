use super::*;

const AUTO_PUBLISH_BATCH_LIMIT: usize = 64;

fn is_high_frequency_global_event(payload: &crate::types::EventPayload) -> bool {
    payload.dissemination_layer() == crate::types::TaskDisseminationLayer::Process
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
        if scope == SwarmScope::Global && !event.payload.allows_global_dissemination() {
            last_published_seq = seq;
            continue;
        }
        if let crate::types::EventPayload::FeedSubscriptionUpdated(payload) = &event.payload
            && payload.subscriber_node_id == local_node_id
        {
            let subscription_scope = super::feed_subscription_target_scope(payload);
            let gossip_kinds = super::feed_subscription_gossip_kinds(&payload.gossip_kinds);
            if payload.active {
                service.subscribe_scope_kinds(&subscription_scope, &gossip_kinds)?;
            } else if !super::node_has_active_subscription_scope_kinds(
                node,
                local_node_id,
                &subscription_scope,
                &gossip_kinds,
            )? {
                service.unsubscribe_scope_kinds(&subscription_scope, &gossip_kinds)?;
            }
        }
        if let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload {
            super::maybe_record_topic_cursor(
                node,
                local_node_id,
                &payload.feed_key,
                &scope,
                seq,
                event.created_at,
            )?;
        }
        let is_local_subscription_control_event = matches!(
            &event.payload,
            crate::types::EventPayload::FeedSubscriptionUpdated(_)
        );
        let publish_result = if matches!(
            &event.payload,
            crate::types::EventPayload::TopicMessagePosted(_)
        ) {
            service.publish_chat_for_scope(&scope, event.clone())
        } else {
            service.publish_event_for_scope(&scope, event.clone())
        };
        match publish_result {
            Ok(()) => {
                diagnostics::record_diagnostic(
                    service.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "info",
                        "gossip",
                        "publish.event",
                        "ok",
                        format!("published local event: {:?}", event.event_kind),
                    )
                    .event_id(event.event_id.clone())
                    .object("task", event.task_id.clone())
                    .source_node_id(Some(event.author_node_id.clone()))
                    .scope(&scope)
                    .details(json!({
                        "seq": seq,
                        "event_kind": format!("{:?}", event.event_kind),
                        "author_node_id": event.author_node_id.clone(),
                        "payload_kind": format!("{:?}", event.payload.kind()),
                    })),
                );
                service.record_scope_event_published(&scope);
                last_published_seq = seq;
            }
            Err(err)
                if err.to_string().contains("NoPeersSubscribedToTopic")
                    && is_local_subscription_control_event =>
            {
                diagnostics::record_diagnostic(
                    service.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "warn",
                        "gossip",
                        "publish.event",
                        "skipped",
                        "local subscription control event had no subscribed peers",
                    )
                    .event_id(event.event_id.clone())
                    .object("task", event.task_id.clone())
                    .source_node_id(Some(event.author_node_id.clone()))
                    .scope(&scope)
                    .details(json!({
                        "seq": seq,
                        "event_kind": format!("{:?}", event.event_kind),
                        "error": err.to_string(),
                    })),
                );
                last_published_seq = seq;
                continue;
            }
            Err(err) if err.to_string().contains("NoPeersSubscribedToTopic") => {
                diagnostics::record_diagnostic(
                    service.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "warn",
                        "gossip",
                        "publish.event",
                        "blocked",
                        "publish stopped because no peers are subscribed to topic",
                    )
                    .event_id(event.event_id.clone())
                    .object("task", event.task_id.clone())
                    .source_node_id(Some(event.author_node_id.clone()))
                    .scope(&scope)
                    .details(json!({
                        "seq": seq,
                        "event_kind": format!("{:?}", event.event_kind),
                        "error": err.to_string(),
                    })),
                );
                break;
            }
            Err(err) if err.to_string().contains("GlobalTopicRateLimited") => {
                diagnostics::record_diagnostic(
                    service.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "warn",
                        "gossip",
                        "publish.event",
                        "rate_limited",
                        "publish stopped by global topic rate limit",
                    )
                    .event_id(event.event_id.clone())
                    .object("task", event.task_id.clone())
                    .source_node_id(Some(event.author_node_id.clone()))
                    .scope(&scope)
                    .details(json!({
                        "seq": seq,
                        "event_kind": format!("{:?}", event.event_kind),
                        "error": err.to_string(),
                    })),
                );
                break;
            }
            Err(err) if err.to_string().contains("LocalTopicRateLimited") => {
                diagnostics::record_diagnostic(
                    service.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "warn",
                        "gossip",
                        "publish.event",
                        "rate_limited",
                        "publish stopped by local topic rate limit",
                    )
                    .event_id(event.event_id.clone())
                    .object("task", event.task_id.clone())
                    .source_node_id(Some(event.author_node_id.clone()))
                    .scope(&scope)
                    .details(json!({
                        "seq": seq,
                        "event_kind": format!("{:?}", event.event_kind),
                        "error": err.to_string(),
                    })),
                );
                break;
            }
            Err(err) => return Err(err),
        }
        let _ = super::mirror_summary_controls_to_parent_network(node, &event);
        if let Some(checkpoint) =
            super::announcements::checkpoint_announcement_for_event(node, &event, &scope)?
        {
            let _ = super::announcements::apply_checkpoint_announcement_to_store(
                &node.store,
                &checkpoint,
            );
            let _ = service.publish_checkpoint(checkpoint.clone());
            let _ = super::announcements::mirror_checkpoint_to_parent_network(node, &checkpoint);
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
            let _ = service.publish_summary(summary.clone());
            let _ = super::mirror_summary_to_parent_network(node, &summary);
        }
        if publish_summaries
            && !local_node_penalized
            && let Some(summary) = super::task_outcome_summary_for_event(node, &event, &scope)?
        {
            let _ = service.publish_summary(summary.clone());
            let _ = super::mirror_summary_to_parent_network(node, &summary);
        }
        if publish_summaries
            && !local_node_penalized
            && let Some(summary) = super::reputation_summary_for_event(node, &event)?
        {
            let _ = service.publish_summary(summary.clone());
            let _ = super::mirror_summary_to_parent_network(node, &summary);
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
