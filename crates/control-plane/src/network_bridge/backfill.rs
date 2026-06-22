use super::*;
use crate::network_p2p::BackfillResponse;

const BACKFILL_HEAD_EVENT_IDS_LIMIT: usize = 8;
pub(super) const BACKFILL_KNOWN_EVENT_IDS_LIMIT: usize = 64;

/// Backward-scan page size for `recent_backfill_lane_event_ids`. Bounds memory
/// while paginating a scope's events newest-first to absorb events filtered out
/// by `should_sync_event` (revoked/banned) before reaching the requested limit.
const SCOPE_LANE_SCAN_BATCH: usize = 256;

fn event_matches_backfill_lane(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
    feed_key: Option<&str>,
) -> Result<bool> {
    if let Some(feed_key) = feed_key {
        let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload else {
            return Ok(false);
        };
        if payload.feed_key != feed_key {
            return Ok(false);
        }
    }
    let Some(route) = event_transport_route(node, event)? else {
        return Ok(false);
    };
    if route.scope != *scope {
        return Ok(false);
    }
    if route.scope == SwarmScope::Global && !route.public_global_control {
        return Ok(false);
    }
    Ok(true)
}

pub(super) fn recent_backfill_lane_event_ids(
    node: &Node,
    scope: &SwarmScope,
    feed_key: Option<&str>,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let scope_label = scope_hint_label(scope);
    let mut event_ids = Vec::new();
    let mut before: Option<u64> = None;
    loop {
        let batch =
            node.store
                .load_scope_events_before(&scope_label, before, SCOPE_LANE_SCAN_BATCH)?;
        let batch_len = batch.len();
        for (seq, event) in batch {
            before = Some(seq);
            if !should_sync_event(node, &event)? {
                continue;
            }
            if !event_matches_backfill_lane(node, &event, scope, feed_key)? {
                continue;
            }
            event_ids.push(event.event_id);
            if event_ids.len() >= limit {
                return Ok(event_ids);
            }
        }
        if batch_len < SCOPE_LANE_SCAN_BATCH {
            break;
        }
    }
    Ok(event_ids)
}

pub fn backfill_response_for_request(
    node: &Node,
    local_transport_node_id: &str,
    request: &BackfillRequest,
    max_limit: usize,
    hard_limit: usize,
) -> Result<BackfillResponse> {
    request.validate(max_limit, hard_limit)?;
    let scope_label = scope_hint_label(&request.scope);
    let mut next_from_event_seq = request.from_event_seq;
    let mut from_event_seq = request.from_event_seq;
    let mut envelopes = Vec::new();
    let known_event_ids = request
        .known_event_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let head_event_ids = recent_backfill_lane_event_ids(
        node,
        &request.scope,
        request.feed_key.as_deref(),
        BACKFILL_HEAD_EVENT_IDS_LIMIT,
    )?;

    while envelopes.len() < request.limit {
        let rows = node.store.load_scope_events_page(
            &scope_label,
            from_event_seq,
            request.limit.saturating_sub(envelopes.len()).max(32),
        )?;
        if rows.is_empty() {
            break;
        }
        for (seq, event) in rows {
            from_event_seq = seq;
            next_from_event_seq = seq;
            if !should_sync_event(node, &event)? {
                continue;
            }
            if known_event_ids.contains(event.event_id.as_str()) {
                continue;
            }
            if !event_matches_backfill_lane(
                node,
                &event,
                &request.scope,
                request.feed_key.as_deref(),
            )? {
                continue;
            }
            envelopes.push(EventEnvelope {
                scope: request.scope.clone(),
                event,
                content_source_node_id: Some(local_transport_node_id.to_owned()),
            });
            if envelopes.len() >= request.limit {
                break;
            }
        }
    }

    Ok(BackfillResponse {
        scope: request.scope.clone(),
        next_from_event_seq,
        feed_key: request.feed_key.clone(),
        head_event_ids,
        events: envelopes,
    })
}

pub(super) fn ingest_backfill_response_events(
    node: &mut Node,
    response: &BackfillResponse,
) -> Result<Vec<EventEnvelope>> {
    let mut applied = Vec::new();
    for envelope in &response.events {
        if envelope.scope != response.scope {
            return Err(anyhow!("backfill response scope mismatch"));
        }
        if !event_matches_signed_scope(&envelope.event, &response.scope) {
            continue;
        }
        if let Some(feed_key) = &response.feed_key {
            let crate::types::EventPayload::TopicMessagePosted(payload) = &envelope.event.payload
            else {
                return Err(anyhow!("topic backfill response contains non-topic event"));
            };
            if payload.feed_key != *feed_key {
                return Err(anyhow!("topic backfill response feed_key mismatch"));
            }
        }
        if ingest_event_envelope(node, envelope).is_ok() {
            applied.push(envelope.clone());
        }
    }
    Ok(applied)
}

pub fn ingest_backfill_response(node: &mut Node, response: &BackfillResponse) -> Result<usize> {
    Ok(ingest_backfill_response_events(node, response)?.len())
}

pub(super) fn should_publish_summaries(head_seq: u64, from_event_seq: u64) -> bool {
    head_seq.saturating_sub(from_event_seq) <= SUMMARY_BACKPRESSURE_HIGH_WATERMARK
}

pub(super) fn should_sync_event(node: &Node, event: &crate::types::Event) -> Result<bool> {
    if let Some(network_id) = super::network_id_for_network_substrate_event(event)
        && network_id != super::current_network_context_id(node)
    {
        return Ok(false);
    }
    if node
        .store
        .is_node_network_banned_at(&event.author_node_id, event.created_at)?
    {
        return Ok(false);
    }
    if matches!(
        event.payload,
        crate::types::EventPayload::EventRevoked(_)
            | crate::types::EventPayload::SummaryRevoked(_)
            | crate::types::EventPayload::NodePenalized(_)
    ) {
        return Ok(true);
    }
    Ok(!node.store.is_event_revoked(&event.event_id)?)
}
