use crate::control::{local_node_id, open_node};
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub(crate) struct TopicMessagesQuery {
    network_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    limit: Option<usize>,
    before_created_at: Option<u64>,
    before_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopicCursorQuery {
    network_id: Option<String>,
    feed_key: String,
    subscriber_node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopicSubscriptionWriteRequest {
    network_id: Option<String>,
    subscriber_node_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    active: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopicMessageWriteRequest {
    network_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    content: Value,
    reply_to_message_id: Option<String>,
}

fn clamp_topic_page_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).clamp(1, 200)
}

fn resolve_subscriber_node_id(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
    explicit: Option<String>,
) -> Result<String> {
    if let Some(subscriber_node_id) = explicit {
        let trimmed = subscriber_node_id.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("subscriber_node_id cannot be empty"));
        }
        return Ok(trimmed.to_owned());
    }
    match local_node_id(state_dir) {
        Ok(node_id) => Ok(node_id),
        Err(_) => {
            let node = open_node(state_dir, db_path)?;
            Ok(node.node_id())
        }
    }
}

pub(crate) async fn topic_messages(
    State(state): State<UiServerState>,
    Query(query): Query<TopicMessagesQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let limit = clamp_topic_page_limit(query.limit);
    let network_id = query
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let feed_key = query.feed_key.trim().to_owned();
    let scope_hint = query.scope_hint.trim().to_owned();
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required").into());
    }
    let before_created_at = query.before_created_at;
    let before_message_id = query
        .before_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let payload = run_blocking(move || -> Result<Value> {
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let messages = node.store.list_topic_messages_page(
            &network_id,
            &feed_key,
            &scope_hint,
            before_created_at,
            before_message_id.as_deref(),
            limit,
        )?;
        let next_anchor = messages.last().map(|message| {
            json!({
                "before_created_at": message.created_at,
                "before_message_id": message.message_id,
            })
        });
        Ok(json!({
            "ok": true,
            "network_id": network_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
            "messages": messages,
            "next_anchor": next_anchor,
        }))
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn topic_subscription_post(
    State(state): State<UiServerState>,
    Json(req): Json<TopicSubscriptionWriteRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let feed_key = req.feed_key.trim().to_owned();
    let scope_hint = req.scope_hint.trim().to_owned();
    let network_id = req
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let subscriber_node_id = req
        .subscriber_node_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let active = req.active;
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required").into());
    }
    let payload = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let subscriber_node_id = subscriber_node_id.unwrap_or_else(|| node.node_id());
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let created_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let event = node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: network_id.clone(),
                    subscriber_node_id: subscriber_node_id.clone(),
                    feed_key: feed_key.clone(),
                    scope_hint: scope_hint.clone(),
                    gossip_kinds: vec!["messages".to_owned()],
                    provider_capabilities: active
                        .then(crate::types::TopicProviderCapabilities::local_history_provider),
                    active,
                },
            ),
            created_at,
        )?;
        Ok(json!({
            "ok": true,
            "event_id": event.event_id,
            "network_id": network_id,
            "subscriber_node_id": subscriber_node_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
            "gossip_kinds": ["messages"],
            "active": active,
        }))
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn topic_message_post(
    State(state): State<UiServerState>,
    Json(req): Json<TopicMessageWriteRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let feed_key = req.feed_key.trim().to_owned();
    let scope_hint = req.scope_hint.trim().to_owned();
    let network_id = req
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let reply_to_message_id = req
        .reply_to_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let content = req.content;
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    if scope_hint.is_empty() {
        return Err(anyhow!("scope_hint is required").into());
    }
    let payload = run_blocking(move || -> Result<Value> {
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let created_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let event = crate::control::emit_topic_message_with_content(
            &mut node,
            &state_clone.state_dir,
            &network_id,
            &feed_key,
            &scope_hint,
            content,
            reply_to_message_id,
            created_at,
        )?;
        let _ = crate::control::topic_interpretation::process_topic_interpretation_for_topic(
            &mut node,
            &state_clone.state_dir,
            &feed_key,
            &scope_hint,
        );
        let _ = crate::control::topic_consensus::process_structured_topic_consensus_for_topic(
            &mut node,
            &state_clone.state_dir,
            &feed_key,
            &scope_hint,
        );
        Ok(json!({
            "ok": true,
            "event_id": event.event_id,
            "message_id": event.event_id,
            "network_id": network_id,
            "feed_key": feed_key,
            "scope_hint": scope_hint,
        }))
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn topic_cursor(
    State(state): State<UiServerState>,
    Query(query): Query<TopicCursorQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let feed_key = query.feed_key.trim().to_owned();
    let network_id = query
        .network_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if feed_key.is_empty() {
        return Err(anyhow!("feed_key is required").into());
    }
    let subscriber_node_id = query.subscriber_node_id;
    let payload = run_blocking(move || -> Result<Value> {
        let subscriber_node_id = resolve_subscriber_node_id(
            &state_clone.state_dir,
            &state_clone.db_path,
            subscriber_node_id,
        )?;
        let node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let network_id = network_id.unwrap_or_else(|| resolve_network_id(&node));
        let cursor = node
            .store
            .get_topic_cursor(&network_id, &subscriber_node_id, &feed_key)?;
        Ok(json!({
            "ok": true,
            "network_id": network_id,
            "subscriber_node_id": subscriber_node_id,
            "feed_key": feed_key,
            "cursor": cursor,
        }))
    })
    .await?;
    Ok(Json(payload))
}
