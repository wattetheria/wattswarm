use crate::control::{local_node_id, open_node};
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::network_bridge::{PrivateHiveCryptoDiagnostic, record_private_hive_crypto_diagnostic};
use crate::types::AgentEnvelope;
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;
use wattswarm_storage_core::storage::TopicMessageRow;

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
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
    active: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopicMessageWriteRequest {
    network_id: Option<String>,
    feed_key: String,
    scope_hint: String,
    content: Value,
    reply_to_message_id: Option<String>,
    #[serde(default)]
    agent_envelope: Option<AgentEnvelope>,
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

fn maybe_decrypt_private_hive_topic_messages_for_response(
    state_dir: &std::path::Path,
    node: &crate::node::Node,
    messages: &mut [TopicMessageRow],
) {
    for message in messages {
        if let Err(error) =
            maybe_decrypt_private_hive_topic_message_for_response(state_dir, node, message)
        {
            eprintln!(
                "private hive topic message decrypt for response failed for {}: {error:#}",
                message.message_id
            );
        }
    }
}

fn maybe_decrypt_private_hive_topic_message_for_response(
    state_dir: &std::path::Path,
    node: &crate::node::Node,
    message: &mut TopicMessageRow,
) -> Result<()> {
    if !crate::control::is_private_hive_route(&message.feed_key, &message.scope_hint) {
        return Ok(());
    }
    if message.content.get("kind").and_then(Value::as_str) != Some("private_encrypted")
        || message.content.get("private_kind").and_then(Value::as_str) != Some("hive_message")
    {
        return Ok(());
    }
    let Some(private_message_id) = message
        .content
        .get("message_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(());
    };
    let Some(key) = crate::control::find_private_hive_key_record_state(
        state_dir,
        &message.feed_key,
        &message.scope_hint,
    )?
    else {
        return Ok(());
    };
    let Some(encrypted) = message.content.get("encrypted") else {
        return Ok(());
    };
    let encrypted =
        serde_json::from_value::<crate::crypto::PrivateGroupEncryptedPayload>(encrypted.clone())?;
    let plaintext = crate::crypto::decrypt_private_group_content(
        &key.shared_secret_b64,
        &encrypted,
        &crate::control::private_hive_encryption_aad(
            &message.feed_key,
            &message.scope_hint,
            &private_message_id,
        ),
    )?;
    let private_plaintext = serde_json::from_slice::<Value>(&plaintext)?;
    let (content, agent_envelope) =
        crate::control::decode_private_hive_plaintext_payload(private_plaintext)?;
    node.store.update_topic_message_content_and_agent_envelope(
        &message.message_id,
        &content,
        agent_envelope.as_ref(),
        message.created_at,
    )?;
    message.content = content;
    message.agent_envelope = agent_envelope;
    message.content_resolved_at = Some(message.created_at);
    record_private_hive_crypto_diagnostic(
        state_dir,
        PrivateHiveCryptoDiagnostic {
            phase: "private_hive.decrypt.read",
            message: "private hive message decrypted for topic API response",
            event_id: Some(&message.message_id),
            source_node_id: &message.author_node_id,
            local_node_id: &node.node_id(),
            feed_key: &message.feed_key,
            scope_hint: &message.scope_hint,
            message_id: &private_message_id,
            group_id: &encrypted.group_id,
            epoch: encrypted.epoch,
            scheme: &encrypted.scheme,
            cipher: &encrypted.cipher,
        },
    );
    Ok(())
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
        let mut messages = node.store.list_topic_messages_page(
            &network_id,
            &feed_key,
            &scope_hint,
            before_created_at,
            before_message_id.as_deref(),
            limit,
        )?;
        maybe_decrypt_private_hive_topic_messages_for_response(
            &state_clone.state_dir,
            &node,
            &mut messages,
        );
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
    let agent_envelope = req.agent_envelope;
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
        let private_hive_key_share = if active
            && crate::control::is_private_hive_route(&feed_key, &scope_hint)
            && agent_envelope
                .as_ref()
                .and_then(|envelope| envelope.capability.as_deref())
                == Some("hive.create")
        {
            let existing = crate::control::find_private_hive_key_record_state(
                &state_clone.state_dir,
                &feed_key,
                &scope_hint,
            )?;
            let record = existing.unwrap_or_else(|| crate::control::PrivateHiveKeyRecord {
                feed_key: feed_key.clone(),
                scope_hint: scope_hint.clone(),
                group_id: crate::control::private_hive_group_id(&feed_key, &scope_hint),
                epoch: 1,
                shared_secret_b64: crate::crypto::generate_private_group_secret_b64(),
                updated_at: created_at,
            });
            crate::control::upsert_private_hive_key_record_state(
                &state_clone.state_dir,
                record.clone(),
            )?;
            Some(json!({
                "kind": "private_hive_key_share",
                "feed_key": record.feed_key,
                "scope_hint": record.scope_hint,
                "group_id": record.group_id,
                "epoch": record.epoch,
                "shared_secret_b64": record.shared_secret_b64,
            }))
        } else {
            None
        };
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
                    agent_envelope,
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
            "private_hive_key_share": private_hive_key_share,
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
    let mut agent_envelope = req.agent_envelope;
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
        let local_node_id = node.node_id();
        let created_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let original_content = content;
        let mut local_content_override = None;
        let mut local_agent_envelope_override = None;
        let mut private_hive_crypto = None;
        let content = if crate::control::is_private_hive_route(&feed_key, &scope_hint) {
            let key = crate::control::find_private_hive_key_record_state(
                &state_clone.state_dir,
                &feed_key,
                &scope_hint,
            )?
            .ok_or_else(|| anyhow!("missing private hive key for encrypted topic message"))?;
            let private_message_id = format!("private-hive-msg-{}", Uuid::new_v4());
            let private_agent_envelope = agent_envelope.take();
            let private_plaintext = crate::control::private_hive_plaintext_payload(
                original_content.clone(),
                private_agent_envelope.clone(),
            );
            let encrypted = crate::crypto::encrypt_private_group_content(
                &key.shared_secret_b64,
                &key.group_id,
                key.epoch,
                &serde_json::to_vec(&private_plaintext)?,
                &crate::control::private_hive_encryption_aad(
                    &feed_key,
                    &scope_hint,
                    &private_message_id,
                ),
            )?;
            private_hive_crypto = Some((
                private_message_id.clone(),
                encrypted.scheme.clone(),
                encrypted.cipher.clone(),
                encrypted.group_id.clone(),
                encrypted.epoch,
            ));
            local_content_override = Some(original_content.clone());
            local_agent_envelope_override = private_agent_envelope;
            json!({
                "kind": "private_encrypted",
                "private_kind": "hive_message",
                "message_id": private_message_id,
                "encrypted": encrypted,
            })
        } else {
            original_content
        };
        let event = crate::control::emit_topic_message_with_content_and_agent_envelope(
            &mut node,
            &state_clone.state_dir,
            &network_id,
            &feed_key,
            &scope_hint,
            content,
            reply_to_message_id,
            agent_envelope,
            created_at,
        )?;
        if let Some((message_id, scheme, cipher, group_id, epoch)) = private_hive_crypto.as_ref() {
            record_private_hive_crypto_diagnostic(
                &state_clone.state_dir,
                PrivateHiveCryptoDiagnostic {
                    phase: "private_hive.encrypt",
                    message: "private hive message encrypted for network transport",
                    event_id: Some(&event.event_id),
                    source_node_id: &local_node_id,
                    local_node_id: &local_node_id,
                    feed_key: &feed_key,
                    scope_hint: &scope_hint,
                    message_id,
                    group_id,
                    epoch: *epoch,
                    scheme,
                    cipher,
                },
            );
        }
        if let Some(local_content) = local_content_override {
            node.store.update_topic_message_content_and_agent_envelope(
                &event.event_id,
                &local_content,
                local_agent_envelope_override.as_ref(),
                created_at,
            )?;
        }
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
