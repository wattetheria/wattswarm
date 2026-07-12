use super::*;
use crate::network_p2p::BackfillResponse;
use std::time::{Duration, Instant};

const BACKFILL_HEAD_EVENT_IDS_LIMIT: usize = 8;
pub(super) const BACKFILL_KNOWN_EVENT_IDS_LIMIT: usize = 64;

/// Backward-scan page size for `recent_backfill_lane_event_ids`. Bounds memory
/// while paginating a scope's events newest-first to absorb events filtered out
/// by `should_sync_event` (revoked/banned) before reaching the requested limit.
const SCOPE_LANE_SCAN_BATCH: usize = 256;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct BackfillServeMetrics {
    pub head_scan_ms: u64,
    pub page_read_ms: u64,
    pub sync_filter_ms: u64,
    pub lane_filter_ms: u64,
    pub pages_read: u64,
    pub rows_read: u64,
    pub events_returned: u64,
    pub known_event_skips: u64,
    pub sync_filter_skips: u64,
    pub lane_filter_skips: u64,
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn event_matches_backfill_lane(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
    feed_key: Option<&str>,
    exclude_topic_events: bool,
) -> Result<bool> {
    match &event.payload {
        crate::types::EventPayload::TopicMessagePosted(_) if exclude_topic_events => {
            return Ok(false);
        }
        crate::types::EventPayload::TopicMessagePosted(payload) => {
            if let Some(feed_key) = feed_key
                && payload.feed_key != feed_key
            {
                return Ok(false);
            }
        }
        _ if feed_key.is_some() => return Ok(false),
        _ => {}
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
    exclude_topic_events: bool,
    limit: usize,
) -> Result<Vec<String>> {
    Ok(recent_backfill_lane_digest(node, scope, feed_key, exclude_topic_events, limit)?.0)
}

fn recent_backfill_lane_digest(
    node: &Node,
    scope: &SwarmScope,
    feed_key: Option<&str>,
    exclude_topic_events: bool,
    limit: usize,
) -> Result<(Vec<String>, u64)> {
    if limit == 0 {
        return Ok((Vec::new(), 0));
    }
    let scope_label = scope_hint_label(scope);
    let mut event_ids = Vec::new();
    let mut head_event_seq = 0;
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
            if !event_matches_backfill_lane(node, &event, scope, feed_key, exclude_topic_events)? {
                continue;
            }
            head_event_seq = head_event_seq.max(seq);
            event_ids.push(event.event_id);
            if event_ids.len() >= limit {
                return Ok((event_ids, head_event_seq));
            }
        }
        if batch_len < SCOPE_LANE_SCAN_BATCH {
            break;
        }
    }
    Ok((event_ids, head_event_seq))
}

pub fn backfill_response_for_request(
    node: &Node,
    local_transport_node_id: &str,
    request: &BackfillRequest,
    max_limit: usize,
    hard_limit: usize,
) -> Result<BackfillResponse> {
    backfill_response_for_request_inner(
        node,
        local_transport_node_id,
        request,
        max_limit,
        hard_limit,
        None,
    )
}

pub(super) fn backfill_response_for_request_with_metrics(
    node: &Node,
    local_transport_node_id: &str,
    request: &BackfillRequest,
    max_limit: usize,
    hard_limit: usize,
) -> Result<(BackfillResponse, BackfillServeMetrics)> {
    let mut metrics = BackfillServeMetrics::default();
    let response = backfill_response_for_request_inner(
        node,
        local_transport_node_id,
        request,
        max_limit,
        hard_limit,
        Some(&mut metrics),
    )?;
    Ok((response, metrics))
}

fn backfill_response_for_request_inner(
    node: &Node,
    local_transport_node_id: &str,
    request: &BackfillRequest,
    max_limit: usize,
    hard_limit: usize,
    mut metrics: Option<&mut BackfillServeMetrics>,
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
    let head_started_at = metrics.is_some().then(Instant::now);
    let (head_event_ids, head_event_seq) = recent_backfill_lane_digest(
        node,
        &request.scope,
        request.feed_key.as_deref(),
        request.exclude_topic_events,
        BACKFILL_HEAD_EVENT_IDS_LIMIT,
    )?;
    if let (Some(metrics), Some(started_at)) = (metrics.as_deref_mut(), head_started_at) {
        metrics.head_scan_ms = duration_millis(started_at.elapsed());
    }

    if request.head_only {
        return Ok(BackfillResponse {
            scope: request.scope.clone(),
            next_from_event_seq: head_event_seq,
            head_only: true,
            feed_key: request.feed_key.clone(),
            head_event_ids,
            events: Vec::new(),
        });
    }

    while envelopes.len() < request.limit {
        let page_started_at = metrics.is_some().then(Instant::now);
        let rows = node.store.load_scope_events_page(
            &scope_label,
            from_event_seq,
            request.limit.saturating_sub(envelopes.len()).max(32),
        )?;
        if let (Some(metrics), Some(started_at)) = (metrics.as_deref_mut(), page_started_at) {
            metrics.page_read_ms = metrics
                .page_read_ms
                .saturating_add(duration_millis(started_at.elapsed()));
        }
        if rows.is_empty() {
            break;
        }
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.pages_read = metrics.pages_read.saturating_add(1);
            metrics.rows_read = metrics.rows_read.saturating_add(rows.len() as u64);
        }
        for (seq, event) in rows {
            from_event_seq = seq;
            next_from_event_seq = seq;
            let sync_started_at = metrics.is_some().then(Instant::now);
            let should_sync = should_sync_event(node, &event)?;
            if let (Some(metrics), Some(started_at)) = (metrics.as_deref_mut(), sync_started_at) {
                metrics.sync_filter_ms = metrics
                    .sync_filter_ms
                    .saturating_add(duration_millis(started_at.elapsed()));
            }
            if !should_sync {
                if let Some(metrics) = metrics.as_deref_mut() {
                    metrics.sync_filter_skips = metrics.sync_filter_skips.saturating_add(1);
                }
                continue;
            }
            if known_event_ids.contains(event.event_id.as_str()) {
                if let Some(metrics) = metrics.as_deref_mut() {
                    metrics.known_event_skips = metrics.known_event_skips.saturating_add(1);
                }
                continue;
            }
            let lane_started_at = metrics.is_some().then(Instant::now);
            let matches_lane = event_matches_backfill_lane(
                node,
                &event,
                &request.scope,
                request.feed_key.as_deref(),
                request.exclude_topic_events,
            )?;
            if let (Some(metrics), Some(started_at)) = (metrics.as_deref_mut(), lane_started_at) {
                metrics.lane_filter_ms = metrics
                    .lane_filter_ms
                    .saturating_add(duration_millis(started_at.elapsed()));
            }
            if !matches_lane {
                if let Some(metrics) = metrics.as_deref_mut() {
                    metrics.lane_filter_skips = metrics.lane_filter_skips.saturating_add(1);
                }
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

    if let Some(metrics) = metrics.as_deref_mut() {
        metrics.events_returned = envelopes.len() as u64;
    }
    Ok(BackfillResponse {
        scope: request.scope.clone(),
        next_from_event_seq,
        head_only: false,
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
        if node
            .ingest_remote_if_new(envelope.event.clone())
            .unwrap_or(false)
        {
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
