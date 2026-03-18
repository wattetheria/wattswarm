use super::*;
use crate::control::load_discovered_peer_records;
use crate::network_p2p::BackfillResponse;

const DISCOVERY_DIAL_RETRY_AFTER: Duration = Duration::from_secs(3);

pub fn backfill_response_for_request(
    node: &Node,
    request: &BackfillRequest,
    max_limit: usize,
    hard_limit: usize,
) -> Result<BackfillResponse> {
    request.validate(max_limit, hard_limit)?;
    let mut next_from_event_seq = request.from_event_seq;
    let mut from_event_seq = request.from_event_seq;
    let mut envelopes = Vec::new();

    while envelopes.len() < request.limit {
        let rows = node.store.load_events_page(
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
            if let Some(feed_key) = &request.feed_key {
                let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload else {
                    continue;
                };
                if payload.feed_key != *feed_key {
                    continue;
                }
            }
            if event_scope(node, &event)? != request.scope {
                continue;
            }
            if request.scope == SwarmScope::Global && !event.payload.allows_global_dissemination() {
                continue;
            }
            envelopes.push(EventEnvelope {
                scope: request.scope.clone(),
                event,
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
        events: envelopes,
    })
}

pub fn ingest_backfill_response(node: &mut Node, response: &BackfillResponse) -> Result<usize> {
    let mut applied = 0usize;
    for envelope in &response.events {
        if envelope.scope != response.scope {
            return Err(anyhow!("backfill response scope mismatch"));
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
            applied += 1;
        }
    }
    Ok(applied)
}

pub(super) fn should_publish_summaries(head_seq: u64, from_event_seq: u64) -> bool {
    head_seq.saturating_sub(from_event_seq) <= SUMMARY_BACKPRESSURE_HIGH_WATERMARK
}

pub(super) fn latest_scoped_event_seq(node: &Node, scope: &SwarmScope) -> Result<u64> {
    let mut latest = 0u64;
    for (seq, event) in node.store.load_all_events()? {
        if !should_sync_event(node, &event)? {
            continue;
        }
        if event_scope(node, &event)? == *scope {
            latest = seq;
        }
    }
    Ok(latest)
}

pub(super) fn should_sync_event(node: &Node, event: &crate::types::Event) -> Result<bool> {
    if let Some(network_id) = super::network_id_for_network_substrate_event(event)
        && network_id != super::current_network_context_id(node)
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

pub fn dial_discovered_peer_endpoints(
    service: &mut NetworkBridgeService,
    state_dir: &Path,
    local_node_id: &str,
    next_attempt_at: &mut HashMap<String, std::time::Instant>,
) -> Result<usize> {
    let records = load_discovered_peer_records(&crate::control::discovered_peers_path(state_dir))?;
    let now = std::time::Instant::now();
    let mut dialed = 0usize;
    for record in records {
        if record.node_id == local_node_id {
            continue;
        }
        let Some(raw_addr) = record.listen_addr.as_deref() else {
            continue;
        };
        let addr = match raw_addr.parse::<Multiaddr>() {
            Ok(addr) => addr,
            Err(_) => continue,
        };
        if next_attempt_at
            .get(raw_addr)
            .is_some_and(|deadline| *deadline > now)
        {
            continue;
        }
        next_attempt_at.insert(raw_addr.to_owned(), now + DISCOVERY_DIAL_RETRY_AFTER);
        match service.dial(addr) {
            Ok(()) => dialed += 1,
            Err(err)
                if err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("duplicate connection") => {}
            Err(err) => eprintln!("p2p dial failed for {raw_addr}: {err}"),
        }
    }
    Ok(dialed)
}

pub fn dial_bootstrap_peer_endpoints(
    service: &mut NetworkBridgeService,
    bootstrap_peers: &[String],
    next_attempt_at: &mut HashMap<String, std::time::Instant>,
) -> Result<usize> {
    let now = std::time::Instant::now();
    let mut dialed = 0usize;
    for raw_addr in bootstrap_peers {
        if next_attempt_at
            .get(raw_addr)
            .is_some_and(|deadline| *deadline > now)
        {
            continue;
        }
        let addr = match raw_addr.parse::<Multiaddr>() {
            Ok(addr) => addr,
            Err(_) => continue,
        };
        next_attempt_at.insert(raw_addr.clone(), now + DISCOVERY_DIAL_RETRY_AFTER);
        match service.dial(addr) {
            Ok(()) => dialed += 1,
            Err(err)
                if err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("duplicate connection") => {}
            Err(err) => eprintln!("bootstrap dial failed for {raw_addr}: {err}"),
        }
    }
    Ok(dialed)
}
