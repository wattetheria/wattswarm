use crate::control::open_node;
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use wattswarm_protocol::types::EventKind;

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
];

#[derive(Debug, Deserialize)]
pub(crate) struct GlobalMessagesQuery {
    limit: Option<usize>,
    before_sequence: Option<u64>,
}

fn global_event_kind_is_board_visible(kind: &EventKind) -> bool {
    GLOBAL_EVENT_KINDS.iter().any(|candidate| candidate == kind)
}

pub(crate) async fn global_messages(
    State(state): State<UiServerState>,
    Query(query): Query<GlobalMessagesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let before_sequence = query.before_sequence;
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = resolve_network_id(&node);
        let scan_limit = limit.saturating_mul(4).clamp(limit, 1000);
        let rows = node
            .store
            .load_scope_events_before("global", before_sequence, scan_limit)?;
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
