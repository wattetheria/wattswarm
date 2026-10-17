use crate::control::open_node;
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use wattswarm_protocol::types::{EventKind, EventPayload};

const GLOBAL_EVENT_KINDS: &[EventKind] = &[
    EventKind::MembershipUpdated,
    EventKind::PolicyTuned,
    EventKind::NetworkParamsUpdated,
    EventKind::CheckpointCreated,
    EventKind::AdvisoryCreated,
    EventKind::AdvisoryApproved,
    EventKind::AdvisoryApplied,
    EventKind::EventRevoked,
    EventKind::SummaryRevoked,
    EventKind::NodePenalized,
    EventKind::EventAnnouncement,
];

#[derive(Debug, Deserialize)]
pub(crate) struct GlobalMessagesQuery {
    limit: Option<usize>,
    before_sequence: Option<u64>,
    after_sequence: Option<u64>,
    network_id: Option<String>,
    announcement_id: Option<String>,
}

fn global_event_kind_is_board_visible(kind: &EventKind) -> bool {
    GLOBAL_EVENT_KINDS.iter().any(|candidate| candidate == kind)
}

pub(crate) async fn global_messages(
    State(state): State<UiServerState>,
    Query(query): Query<GlobalMessagesQuery>,
) -> Result<Json<Value>, ApiError> {
    if query.before_sequence.is_some() && query.after_sequence.is_some() {
        return Err(
            anyhow::anyhow!("before_sequence and after_sequence cannot be combined").into(),
        );
    }
    let announcement_id = query.announcement_id.map(|value| value.trim().to_owned());
    let lookup_network_id = query.network_id.map(|value| value.trim().to_owned());
    if let Some(announcement_id) = &announcement_id {
        if announcement_id.is_empty() || lookup_network_id.as_deref().is_none_or(str::is_empty) {
            return Err(
                anyhow::anyhow!("announcement_id and network_id are required for lookup").into(),
            );
        }
        if query.limit.is_some()
            || query.before_sequence.is_some()
            || query.after_sequence.is_some()
        {
            return Err(
                anyhow::anyhow!("announcement_id cannot be combined with pagination").into(),
            );
        }
    } else if lookup_network_id.is_some() {
        return Err(anyhow::anyhow!("network_id requires announcement_id").into());
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let before_sequence = query.before_sequence;
    let after_sequence = query.after_sequence;
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        if let Some(announcement_id) = announcement_id {
            let network_id = lookup_network_id.expect("validated lookup network_id");
            let announcement = node.store.get_global_announcement(&network_id, &announcement_id)?
                .filter(|event| event.swarm_scope == "global"
                    && event.event_kind == EventKind::EventAnnouncement)
                .and_then(|event| match event.payload {
                    EventPayload::EventAnnouncement(payload)
                        if payload.network_id == network_id && payload.announcement_id == announcement_id => Some(payload),
                    _ => None,
                });
            return Ok(json!({ "ok": true, "network_id": network_id, "announcement": announcement }));
        }
        let network_id = resolve_network_id(&node);
        let scan_limit = limit.saturating_mul(4).clamp(limit, 1000);
        let rows = if let Some(after_sequence) = after_sequence {
            node.store.load_scope_events_page("global", after_sequence, limit)?
        } else {
            node.store.load_scope_events_before("global", before_sequence, scan_limit)?
        };
        let messages = rows
            .iter()
            .filter(|(_, event)| global_event_kind_is_board_visible(&event.event_kind))
            .take(limit)
            .map(|(sequence, event)| {
                Ok(json!({
                    "sequence": sequence,
                    "message_id": event.event_id,
                    "kind": format!("{:?}", event.event_kind),
                    "lane": "events",
                    "scope_hint": event.swarm_scope,
                    "author_node_id": event.author_node_id,
                    "content": serde_json::to_value(&event.payload)?,
                    "created_at": event.created_at,
                }))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(json!({
            "ok": true,
            "network_id": network_id,
            "scope_hint": "global",
            "lane": "events",
            "messages": messages,
            "next_before_sequence": messages.last().and_then(|message| message.get("sequence")),
            "next_after_sequence": after_sequence.and_then(|_| rows.last().map(|(sequence, _)| sequence)),
        }))
    })
    .await?;
    Ok(Json(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn board_global_filter_contains_only_the_confirmed_event_kinds() {
        assert!(global_event_kind_is_board_visible(
            &EventKind::MembershipUpdated
        ));
        assert!(global_event_kind_is_board_visible(
            &EventKind::NodePenalized
        ));
        assert!(!global_event_kind_is_board_visible(&EventKind::TaskCreated));
    }
}
