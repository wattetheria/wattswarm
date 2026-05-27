use crate::control::{
    PRIVATE_DM_FEED_KEY, PeerRelationshipAction, PeerRelationshipInitiator,
    apply_peer_relationship_action_state, load_peer_dm_message_records_state,
    load_peer_dm_thread_records_state, load_peer_metadata_records_state,
    load_peer_relationship_records_state, open_configured_node, open_node, private_dm_scope_hint,
    private_dm_thread_id,
};
use crate::http::helpers::resolve_network_id;
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::network_bridge::{
    default_agent_envelope, enqueue_agent_payment_command,
    enqueue_peer_relationship_action_command, network_service_started,
};
use crate::network_p2p::RawAgentEnvelope;
use anyhow::{Result, anyhow, bail};
use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub(crate) struct PeerRelationshipActionRequest {
    remote_node_id: String,
    action: PeerRelationshipAction,
    #[serde(default)]
    initiated_by: Option<PeerRelationshipInitiator>,
    #[serde(default)]
    agent_envelope: Option<RawAgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerDirectMessageSendRequest {
    remote_node_id: String,
    content: Value,
    #[serde(default)]
    agent_envelope: Option<RawAgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AgentPaymentSendRequest {
    remote_node_id: String,
    message_kind: String,
    payment: Value,
    #[serde(default)]
    agent_envelope: Option<RawAgentEnvelope>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerDirectMessageQuery {
    thread_id: String,
}

fn raw_agent_envelope_to_interaction(
    envelope: &RawAgentEnvelope,
) -> crate::control::AgentInteractionEnvelope {
    crate::control::AgentInteractionEnvelope {
        protocol: envelope.protocol.clone(),
        transport_profile: envelope.transport_profile.clone(),
        source_agent_id: envelope.source_agent_id.clone(),
        target_agent_id: envelope.target_agent_id.clone(),
        source_node_id: envelope.source_node_id.clone(),
        target_node_id: envelope.target_node_id.clone(),
        capability: envelope.capability.clone(),
        source_agent_card: envelope
            .source_agent_card
            .as_ref()
            .and_then(|card| serde_json::to_value(card).ok()),
        message: serde_json::from_str(&envelope.message_json).unwrap_or_else(|_| json!({})),
        extensions: envelope
            .extensions_json
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok()),
        signature: envelope.signature.clone(),
    }
}

pub(crate) async fn peers_list(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        build_peers_list_payload(&state_clone.state_dir, &state_clone.db_path)
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn peer_relationships_list(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload =
        run_blocking(move || build_peer_relationships_payload(&state_clone.state_dir)).await?;
    Ok(Json(payload))
}

pub(crate) async fn peer_relationships_update(
    State(state): State<UiServerState>,
    Json(req): Json<PeerRelationshipActionRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        if network_service_started(&state_clone.state_dir) {
            let agent_envelope = req.agent_envelope.ok_or_else(|| {
                anyhow!("agent_envelope is required for network peer relationship actions")
            })?;
            enqueue_peer_relationship_action_command(
                &state_clone.state_dir,
                &req.remote_node_id,
                req.action,
                agent_envelope,
            )?;
            Ok(json!({
                "ok": true,
                "queued": true,
                "remote_node_id": req.remote_node_id,
                "action": req.action,
            }))
        } else {
            update_peer_relationship_payload(
                &state_clone.state_dir,
                &req.remote_node_id,
                req.action,
                req.initiated_by.unwrap_or(PeerRelationshipInitiator::Local),
            )
        }
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn peer_dm_threads_list(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let threads = load_peer_dm_thread_records_state(&state_clone.state_dir)?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "threads": threads,
        }))
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn peer_dm_messages_list(
    State(state): State<UiServerState>,
    Query(query): Query<PeerDirectMessageQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let messages =
            load_peer_dm_message_records_state(&state_clone.state_dir, &query.thread_id)?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "thread_id": query.thread_id,
            "messages": messages,
        }))
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn peer_dm_messages_send(
    State(state): State<UiServerState>,
    Json(req): Json<PeerDirectMessageSendRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        let remote_node_id = req.remote_node_id.trim().to_owned();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let agent_envelope = req.agent_envelope.ok_or_else(|| {
            anyhow!("agent_envelope is required for private group direct messages")
        })?;
        agent_envelope.validate()?;
        let content = req.content;
        let mut node = open_node(&state_clone.state_dir, &state_clone.db_path)?;
        let local_node_id = node.node_id();
        let network_id = resolve_network_id(&node);
        let scope_hint = private_dm_scope_hint(&local_node_id, &remote_node_id);
        let thread_id = private_dm_thread_id(&local_node_id, &remote_node_id);
        let message_id = format!("dm-msg-{}", Uuid::new_v4());
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let control_envelope = raw_agent_envelope_to_interaction(&agent_envelope);
        let participants = {
            let mut members = vec![local_node_id.clone(), remote_node_id.clone()];
            members.sort();
            members
        };
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: network_id.clone(),
                    subscriber_node_id: local_node_id.clone(),
                    feed_key: PRIVATE_DM_FEED_KEY.to_owned(),
                    scope_hint: scope_hint.clone(),
                    gossip_kinds: vec!["messages".to_owned()],
                    active: true,
                },
            ),
            now,
        )?;
        let event = crate::control::emit_topic_message_with_content(
            &mut node,
            &state_clone.state_dir,
            &network_id,
            PRIVATE_DM_FEED_KEY,
            &scope_hint,
            json!({
                "kind": "direct_message",
                "thread_id": thread_id,
                "message_id": message_id,
                "participants": participants,
                "remote_node_id": remote_node_id,
                "content": content.clone(),
                "agent_envelope": control_envelope.clone(),
            }),
            None,
            now.saturating_add(1),
        )?;
        crate::control::save_peer_dm_thread_record_state(
            &state_clone.state_dir,
            &crate::control::PeerDmThreadRecord {
                remote_node_id: remote_node_id.clone(),
                thread_id: thread_id.clone(),
                thread_kind: crate::control::PeerDmThreadKind::Direct,
                session_state: crate::control::PeerDmSessionState::Ready,
                relationship_established_at: None,
                created_at: now,
                updated_at: now.saturating_add(1),
                last_message_at: Some(now.saturating_add(1)),
            },
        )?;
        crate::control::save_peer_dm_message_record_state(
            &state_clone.state_dir,
            &crate::control::PeerDmMessageRecord {
                thread_id: thread_id.clone(),
                message_id: message_id.clone(),
                remote_node_id: remote_node_id.clone(),
                message_kind: crate::control::PeerDmMessageKind::Message,
                direction: crate::control::PeerDmDirection::Outbound,
                delivery_state: crate::control::PeerDmDeliveryState::Delivered,
                a2a_protocol: agent_envelope.protocol.clone(),
                agent_envelope: Some(control_envelope),
                content,
                created_at: now.saturating_add(1),
                acknowledged_at: None,
            },
        )?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "queued": false,
            "remote_node_id": remote_node_id,
            "thread_id": thread_id,
            "message_id": message_id,
            "event_id": event.event_id,
            "feed_key": PRIVATE_DM_FEED_KEY,
            "scope_hint": scope_hint,
            "gossip_kinds": ["messages"],
        }))
    })
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn agent_payment_send(
    State(state): State<UiServerState>,
    Json(req): Json<AgentPaymentSendRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let payload = run_blocking(move || {
        if !network_service_started(&state_clone.state_dir) {
            bail!("agent payments require the background network service to be running");
        }
        let remote_node_id = req.remote_node_id.trim().to_owned();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let message_kind = req.message_kind.trim().to_owned();
        if message_kind.is_empty() {
            bail!("message_kind is required");
        }
        let agent_envelope = if let Some(agent_envelope) = req.agent_envelope {
            agent_envelope
        } else {
            let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
            default_agent_envelope(
                &node.node_id(),
                &remote_node_id,
                "payment.agent_message",
                json!({
                    "message_kind": message_kind,
                    "payment": req.payment.clone(),
                }),
            )
        };
        enqueue_agent_payment_command(
            &state_clone.state_dir,
            &remote_node_id,
            &message_kind,
            req.payment,
            agent_envelope,
        )?;
        Ok::<Value, anyhow::Error>(json!({
            "ok": true,
            "queued": true,
            "remote_node_id": remote_node_id,
            "message_kind": message_kind,
        }))
    })
    .await?;
    Ok(Json(payload))
}

fn build_peers_list_payload(
    state_dir: &std::path::Path,
    db_path: &std::path::Path,
) -> Result<Value> {
    let connected_peers =
        if let Some(peers) = crate::network_bridge::latest_connected_peer_ids(state_dir) {
            peers
        } else {
            match open_configured_node(state_dir, db_path) {
                Ok(node) => node.peers(),
                Err(_) => Vec::new(),
            }
        };
    let discovered =
        crate::control::load_discovered_peer_records_state(state_dir).unwrap_or_default();
    let metadata = load_peer_metadata_records_state(state_dir).unwrap_or_default();
    let relationships = load_peer_relationship_records_state(state_dir).unwrap_or_default();
    let mut records = BTreeMap::<String, Value>::new();
    for peer in &connected_peers {
        records.insert(
            peer.clone(),
            json!({
                "node_id": peer,
                "connected": true,
                "discovery": Value::Null,
                "metadata": Value::Null,
                "relationship": Value::Null,
            }),
        );
    }
    for record in discovered {
        records.entry(record.node_id.clone()).or_insert_with(|| {
            json!({
                "node_id": record.node_id,
                "connected": false,
                "discovery": Value::Null,
                "metadata": Value::Null,
                "relationship": Value::Null,
            })
        })["discovery"] = serde_json::to_value(&record)?;
    }
    for record in metadata {
        records.entry(record.node_id.clone()).or_insert_with(|| {
            json!({
                "node_id": record.node_id,
                "connected": false,
                "discovery": Value::Null,
                "metadata": Value::Null,
                "relationship": Value::Null,
            })
        })["metadata"] = serde_json::to_value(&record)?;
    }
    for record in relationships {
        records
            .entry(record.remote_node_id.clone())
            .or_insert_with(|| {
                json!({
                    "node_id": record.remote_node_id,
                    "connected": false,
                    "discovery": Value::Null,
                    "metadata": Value::Null,
                    "relationship": Value::Null,
                })
            })["relationship"] = serde_json::to_value(&record)?;
    }
    for peer in &connected_peers {
        if let Some(entry) = records.get_mut(peer) {
            entry["connected"] = Value::Bool(true);
        }
    }
    Ok(json!({
        "ok": true,
        "peers": connected_peers,
        "records": records.into_values().collect::<Vec<_>>(),
    }))
}

fn build_peer_relationships_payload(state_dir: &std::path::Path) -> Result<Value> {
    let relationships = load_peer_relationship_records_state(state_dir)?;
    Ok(json!({"ok": true, "relationships": relationships}))
}

fn update_peer_relationship_payload(
    state_dir: &std::path::Path,
    remote_node_id: &str,
    action: PeerRelationshipAction,
    initiated_by: PeerRelationshipInitiator,
) -> Result<Value> {
    let record =
        apply_peer_relationship_action_state(state_dir, remote_node_id, action, initiated_by)?;
    Ok(json!({"ok": true, "relationship": record}))
}

#[cfg(test)]
mod tests {
    use super::{
        build_peer_relationships_payload, build_peers_list_payload,
        update_peer_relationship_payload,
    };
    use crate::control::{NodeState, node_state_path};
    use crate::http::background::mark_node_running_if_service_started;
    use crate::startup_config::{
        CoreAgentConfig, NetworkMode, StartupConfig, save_startup_config, startup_config_path,
    };

    #[test]
    fn marks_running_true_when_background_service_starts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        save_startup_config(
            &startup_config_path(&state_dir),
            &StartupConfig {
                display_name: "Node Agent".to_owned(),
                latitude: None,
                longitude: None,
                network_mode: NetworkMode::Wan,
                bootstrap_contacts: vec!["iroh-bootstrap-contact".to_owned()],
                gateway_urls: Vec::new(),
                core_agent: CoreAgentConfig::default(),
            },
        )
        .expect("save startup config");

        mark_node_running_if_service_started(&state_dir, true).expect("mark node running");

        let state: NodeState = serde_json::from_slice(
            &std::fs::read(node_state_path(&state_dir)).expect("read node state"),
        )
        .expect("parse node state");
        assert!(state.running);
        assert_eq!(state.mode.as_str(), "network");
    }

    #[test]
    fn does_not_write_running_state_when_service_not_started() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");

        mark_node_running_if_service_started(&state_dir, false)
            .expect("skip writing node running state");

        assert!(!node_state_path(&state_dir).exists());
    }

    #[test]
    fn peers_list_payload_includes_discovery_metadata_and_relationship_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        let db_path = state_dir.join("ui.state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        crate::control::save_discovered_peer_records_state(
            &state_dir,
            &[crate::control::DiscoveredPeerRecord {
                node_id: "peer-alpha".to_owned(),
                source_kind: "bootstrap".to_owned(),
            }],
        )
        .expect("save discovered peers");
        crate::control::save_peer_metadata_record_state(
            &state_dir,
            &crate::control::PeerMetadataRecord {
                node_id: "peer-alpha".to_owned(),
                network_id: Some("mainnet:watt-galaxy".to_owned()),
                params_version: Some(7),
                params_hash: Some("params-abc".to_owned()),
                agent_version_raw: Some(
                    "wattswarm-network-p2p|mainnet:watt-galaxy|7|params-abc".to_owned(),
                ),
                agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
                protocol_version: Some("wattswarm/1.0.0".to_owned()),
                observed_addr: Some("198.51.100.2:4001".to_owned()),
                listen_addrs: vec!["203.0.113.10:4001".to_owned()],
                protocols: vec!["/meshsub/1.1.0".to_owned()],
                handshake_status: "identified".to_owned(),
                last_error: None,
                contact_material: None,
                contact_material_signature: None,
                contact_material_updated_at: None,
                first_identified_at: 1_700_000_000_000,
                last_identified_at: 1_700_000_000_500,
            },
        )
        .expect("save peer metadata");
        crate::control::apply_peer_relationship_action_state(
            &state_dir,
            "peer-alpha",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("save relationship");
        let payload = build_peers_list_payload(&state_dir, &db_path).expect("build peers payload");
        let records = payload["records"].as_array().expect("records array");
        let peer = records
            .iter()
            .find(|entry| entry["node_id"].as_str() == Some("peer-alpha"))
            .expect("peer-alpha record");
        assert_eq!(peer["discovery"]["source_kind"].as_str(), Some("bootstrap"));
        assert_eq!(
            peer["metadata"]["network_id"].as_str(),
            Some("mainnet:watt-galaxy")
        );
        assert_eq!(
            peer["relationship"]["relationship_state"].as_str(),
            Some("requested")
        );
    }

    #[test]
    fn peer_relationship_payloads_cover_full_lifecycle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        let requested = update_peer_relationship_payload(
            &state_dir,
            "peer-a",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("request");
        assert_eq!(
            requested["relationship"]["relationship_state"].as_str(),
            Some("requested")
        );

        let cancelled = update_peer_relationship_payload(
            &state_dir,
            "peer-a",
            crate::control::PeerRelationshipAction::Cancel,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("cancel");
        assert_eq!(
            cancelled["relationship"]["relationship_state"].as_str(),
            Some("none")
        );
        assert_eq!(
            cancelled["relationship"]["last_action"].as_str(),
            Some("cancel")
        );

        update_peer_relationship_payload(
            &state_dir,
            "peer-b",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("request peer-b");
        let accepted = update_peer_relationship_payload(
            &state_dir,
            "peer-b",
            crate::control::PeerRelationshipAction::Accept,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("accept peer-b");
        assert_eq!(
            accepted["relationship"]["relationship_state"].as_str(),
            Some("accepted")
        );

        let removed = update_peer_relationship_payload(
            &state_dir,
            "peer-b",
            crate::control::PeerRelationshipAction::Remove,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("remove peer-b");
        assert_eq!(
            removed["relationship"]["relationship_state"].as_str(),
            Some("none")
        );
        assert_eq!(
            removed["relationship"]["last_action"].as_str(),
            Some("remove")
        );

        update_peer_relationship_payload(
            &state_dir,
            "peer-c",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("request peer-c");
        let rejected = update_peer_relationship_payload(
            &state_dir,
            "peer-c",
            crate::control::PeerRelationshipAction::Reject,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("reject peer-c");
        assert_eq!(
            rejected["relationship"]["relationship_state"].as_str(),
            Some("rejected")
        );

        let blocked = update_peer_relationship_payload(
            &state_dir,
            "peer-d",
            crate::control::PeerRelationshipAction::Block,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("block peer-d");
        assert_eq!(
            blocked["relationship"]["relationship_state"].as_str(),
            Some("blocked")
        );
        let unblocked = update_peer_relationship_payload(
            &state_dir,
            "peer-d",
            crate::control::PeerRelationshipAction::Unblock,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("unblock peer-d");
        assert_eq!(
            unblocked["relationship"]["relationship_state"].as_str(),
            Some("none")
        );
        assert_eq!(
            unblocked["relationship"]["last_action"].as_str(),
            Some("unblock")
        );

        let payload = build_peer_relationships_payload(&state_dir).expect("list relationships");
        let relationships = payload["relationships"].as_array().expect("relationships");
        assert!(relationships.iter().any(|entry| {
            entry["remote_node_id"].as_str() == Some("peer-b")
                && entry["last_action"].as_str() == Some("remove")
        }));
        assert!(relationships.iter().any(|entry| {
            entry["remote_node_id"].as_str() == Some("peer-d")
                && entry["last_action"].as_str() == Some("unblock")
        }));
    }

    #[test]
    fn peer_relationship_payloads_reject_invalid_transitions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");

        let accept_err = update_peer_relationship_payload(
            &state_dir,
            "peer-z",
            crate::control::PeerRelationshipAction::Accept,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect_err("accept without request should fail");
        assert!(
            accept_err.to_string().contains("without a pending request"),
            "unexpected error: {accept_err}"
        );

        let unblock_err = update_peer_relationship_payload(
            &state_dir,
            "peer-z",
            crate::control::PeerRelationshipAction::Unblock,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect_err("unblock without block should fail");
        assert!(
            unblock_err.to_string().contains("is not blocked"),
            "unexpected error: {unblock_err}"
        );
    }
}
