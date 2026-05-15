use super::*;

pub(super) fn wire_peer_relationship_action(
    action: crate::control::PeerRelationshipAction,
) -> RawPeerRelationshipAction {
    match action {
        crate::control::PeerRelationshipAction::Request => RawPeerRelationshipAction::Request,
        crate::control::PeerRelationshipAction::Accept => RawPeerRelationshipAction::Accept,
        crate::control::PeerRelationshipAction::Reject => RawPeerRelationshipAction::Reject,
        crate::control::PeerRelationshipAction::Cancel => RawPeerRelationshipAction::Cancel,
        crate::control::PeerRelationshipAction::Remove => RawPeerRelationshipAction::Remove,
        crate::control::PeerRelationshipAction::Block => RawPeerRelationshipAction::Block,
        crate::control::PeerRelationshipAction::Unblock => RawPeerRelationshipAction::Unblock,
    }
}

pub(super) fn control_peer_relationship_action(
    action: RawPeerRelationshipAction,
) -> crate::control::PeerRelationshipAction {
    match action {
        RawPeerRelationshipAction::Request => crate::control::PeerRelationshipAction::Request,
        RawPeerRelationshipAction::Accept => crate::control::PeerRelationshipAction::Accept,
        RawPeerRelationshipAction::Reject => crate::control::PeerRelationshipAction::Reject,
        RawPeerRelationshipAction::Cancel => crate::control::PeerRelationshipAction::Cancel,
        RawPeerRelationshipAction::Remove => crate::control::PeerRelationshipAction::Remove,
        RawPeerRelationshipAction::Block => crate::control::PeerRelationshipAction::Block,
        RawPeerRelationshipAction::Unblock => crate::control::PeerRelationshipAction::Unblock,
    }
}

#[derive(Debug, Clone)]
pub(super) struct PendingPeerRelationshipRequest {
    pub(super) peer: NetworkNodeId,
    pub(super) remote_node_id: String,
    pub(super) action: crate::control::PeerRelationshipAction,
}

#[derive(Debug, Clone)]
pub(super) struct PendingPeerDirectMessageRequest {
    pub(super) peer: NetworkNodeId,
    pub(super) remote_node_id: String,
    pub(super) thread_id: String,
    pub(super) message_id: String,
    pub(super) kind: crate::control::PeerDmMessageKind,
    pub(super) a2a_protocol: String,
}

#[derive(Debug, Clone)]
pub(super) struct PendingContactMaterialRequest {
    pub(super) peer: NetworkNodeId,
    pub(super) remote_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PendingNetworkCommand {
    PeerRelationship {
        remote_node_id: String,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: RawAgentEnvelope,
    },
    PeerDirectMessage {
        remote_node_id: String,
        agent_envelope: RawAgentEnvelope,
        content: Value,
    },
    AgentPayment {
        remote_node_id: String,
        message_kind: String,
        payment: Value,
    },
}

fn pending_network_commands_path(state_dir: &Path) -> PathBuf {
    state_dir.join("pending_network_commands.jsonl")
}

fn enqueue_pending_network_command(
    state_dir: &Path,
    command: &PendingNetworkCommand,
) -> Result<()> {
    let path = pending_network_commands_path(state_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{}", serde_json::to_string(command)?)?;
    Ok(())
}

pub fn enqueue_peer_relationship_action_command(
    state_dir: &Path,
    remote_node_id: &str,
    action: crate::control::PeerRelationshipAction,
    agent_envelope: RawAgentEnvelope,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::PeerRelationship {
            remote_node_id: remote_node_id.trim().to_owned(),
            action,
            agent_envelope,
        },
    )
}

pub fn enqueue_peer_direct_message_command(
    state_dir: &Path,
    remote_node_id: &str,
    agent_envelope: RawAgentEnvelope,
    content: Value,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::PeerDirectMessage {
            remote_node_id: remote_node_id.trim().to_owned(),
            agent_envelope,
            content,
        },
    )
}

pub fn enqueue_agent_payment_command(
    state_dir: &Path,
    remote_node_id: &str,
    message_kind: &str,
    payment: Value,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::AgentPayment {
            remote_node_id: remote_node_id.trim().to_owned(),
            message_kind: message_kind.trim().to_owned(),
            payment,
        },
    )
}

fn raw_agent_envelope_to_control_record(
    envelope: &RawAgentEnvelope,
) -> crate::control::AgentInteractionEnvelope {
    crate::control::AgentInteractionEnvelope {
        protocol: envelope.protocol.clone(),
        source_agent_id: envelope.source_agent_id.clone(),
        target_agent_id: envelope.target_agent_id.clone(),
        capability: envelope.capability.clone(),
        message: serde_json::from_str(&envelope.message_json).unwrap_or_else(|_| json!({})),
        extensions: envelope
            .extensions_json
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok()),
        signature: envelope.signature.clone(),
    }
}

fn build_agent_payment_summary(
    remote_node_id: &str,
    message_kind: &str,
    payment: Value,
) -> SummaryAnnouncement {
    let remote_node_id = remote_node_id.trim();
    let payment_id = payment
        .get("payment_id")
        .and_then(Value::as_str)
        .unwrap_or("payment");
    SummaryAnnouncement {
        summary_id: format!("payment:{payment_id}:{}", Uuid::new_v4()),
        source_node_id: String::new(),
        scope: SwarmScope::Node(remote_node_id.to_owned()),
        summary_kind: AGENT_PAYMENT_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: json!({
            "message_kind": message_kind,
            "payment": payment,
        }),
    }
}

#[derive(Debug, Serialize)]
struct UnsignedAgentEnvelope<'a> {
    protocol: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_agent_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_agent_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability: Option<&'a String>,
    message_json: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    extensions_json: Option<&'a String>,
}

pub(super) fn verify_agent_envelope_signature(envelope: &RawAgentEnvelope) -> Result<()> {
    let Some(signature) = envelope.signature.as_deref() else {
        return Ok(());
    };
    let signer_ref = envelope.source_agent_id.as_deref().ok_or_else(|| {
        anyhow!("agent envelope source_agent_id is required when signature is set")
    })?;
    let unsigned = UnsignedAgentEnvelope {
        protocol: &envelope.protocol,
        source_agent_id: envelope.source_agent_id.as_ref(),
        target_agent_id: envelope.target_agent_id.as_ref(),
        capability: envelope.capability.as_ref(),
        message_json: &envelope.message_json,
        extensions_json: envelope.extensions_json.as_ref(),
    };
    crate::crypto::verify_signature_ref(
        signer_ref,
        serde_jcs::to_string(&unsigned)?.as_bytes(),
        signature,
    )
    .context("verify agent envelope signature")
}

pub(super) fn attach_agent_envelope_to_relationship(
    state_dir: &Path,
    remote_node_id: &str,
    envelope: &RawAgentEnvelope,
) -> Result<()> {
    let Some(mut record) = crate::control::load_peer_relationship_records_state(state_dir)?
        .into_iter()
        .find(|entry| entry.remote_node_id == remote_node_id)
    else {
        return Ok(());
    };
    record.agent_envelope = Some(raw_agent_envelope_to_control_record(envelope));
    crate::control::save_peer_relationship_record_state(state_dir, &record)
}

pub(super) fn default_agent_envelope(
    local_node_id: &str,
    remote_node_id: &str,
    capability: &str,
    payload: Value,
) -> RawAgentEnvelope {
    RawAgentEnvelope {
        protocol: "google_a2a".to_owned(),
        source_agent_id: Some(local_node_id.to_owned()),
        target_agent_id: Some(remote_node_id.to_owned()),
        capability: Some(capability.to_owned()),
        message_json: serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned()),
        extensions_json: None,
        signature: None,
    }
}

fn peer_dm_content_from_control_envelope(
    envelope: &crate::control::AgentInteractionEnvelope,
) -> Value {
    crate::control::peer_dm_content_from_envelope(envelope)
}

pub(super) fn peer_dm_thread_id(local_node_id: &str, remote_node_id: &str) -> String {
    let mut members = [local_node_id.to_owned(), remote_node_id.to_owned()];
    members.sort();
    format!("dm:{}:{}", members[0], members[1])
}

pub(super) fn relationship_state_for(
    state_dir: &Path,
    remote_node_id: &str,
) -> Result<Option<crate::control::PeerRelationshipState>> {
    Ok(
        crate::control::load_peer_relationship_records_state(state_dir)?
            .into_iter()
            .find(|record| record.remote_node_id == remote_node_id)
            .map(|record| record.relationship_state),
    )
}

pub(super) fn upsert_dm_thread(
    state_dir: &Path,
    remote_node_id: &str,
    thread_id: &str,
    session_state: crate::control::PeerDmSessionState,
    relationship_established_at: Option<u64>,
    last_message_at: Option<u64>,
) -> Result<crate::control::PeerDmThreadRecord> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_dm_thread_records_state(state_dir)?
        .into_iter()
        .find(|record| record.thread_id == thread_id);
    let session_rank = |state: crate::control::PeerDmSessionState| match state {
        crate::control::PeerDmSessionState::Established => 0_u8,
        crate::control::PeerDmSessionState::SessionPending => 1_u8,
        crate::control::PeerDmSessionState::Ready => 2_u8,
        crate::control::PeerDmSessionState::Blocked => 3_u8,
    };
    let merged_session_state = existing
        .as_ref()
        .map(|record| record.session_state)
        .map(|current| {
            if session_rank(session_state) >= session_rank(current) {
                session_state
            } else {
                current
            }
        })
        .unwrap_or(session_state);
    let record = crate::control::PeerDmThreadRecord {
        remote_node_id: remote_node_id.to_owned(),
        thread_id: thread_id.to_owned(),
        thread_kind: crate::control::PeerDmThreadKind::Direct,
        session_state: merged_session_state,
        relationship_established_at: relationship_established_at.or_else(|| {
            existing
                .as_ref()
                .and_then(|record| record.relationship_established_at)
        }),
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        updated_at: now,
        last_message_at: last_message_at
            .or_else(|| existing.as_ref().and_then(|record| record.last_message_at)),
    };
    crate::control::save_peer_dm_thread_record_state(state_dir, &record)?;
    Ok(record)
}

pub(super) fn save_dm_message(
    state_dir: &Path,
    remote_node_id: &str,
    thread_id: &str,
    message_id: &str,
    message_kind: crate::control::PeerDmMessageKind,
    direction: crate::control::PeerDmDirection,
    delivery_state: crate::control::PeerDmDeliveryState,
    a2a_protocol: &str,
    agent_envelope: Option<&RawAgentEnvelope>,
    content: Value,
    acknowledged_at: Option<u64>,
) -> Result<crate::control::PeerDmMessageRecord> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_dm_message_records_state(state_dir, thread_id)?
        .into_iter()
        .find(|record| record.message_id == message_id);
    let agent_envelope = agent_envelope
        .map(raw_agent_envelope_to_control_record)
        .or_else(|| {
            existing
                .as_ref()
                .and_then(|record| record.agent_envelope.clone())
        })
        .or_else(|| {
            Some(crate::control::synthesize_peer_dm_envelope(
                a2a_protocol,
                &content,
            ))
        });
    let record = crate::control::PeerDmMessageRecord {
        thread_id: thread_id.to_owned(),
        message_id: message_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        message_kind,
        direction,
        delivery_state,
        a2a_protocol: a2a_protocol.to_owned(),
        content: agent_envelope
            .as_ref()
            .map(peer_dm_content_from_control_envelope)
            .or_else(|| existing.as_ref().map(|record| record.content.clone()))
            .unwrap_or(content),
        agent_envelope,
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        acknowledged_at: acknowledged_at
            .or_else(|| existing.as_ref().and_then(|record| record.acknowledged_at)),
    };
    crate::control::save_peer_dm_message_record_state(state_dir, &record)?;
    Ok(record)
}

pub(super) fn save_agent_payment_summary(
    state_dir: &Path,
    remote_node_id: &str,
    summary: &SummaryAnnouncement,
) -> Result<crate::control::AgentPaymentRecord> {
    let message_kind = summary
        .payload
        .get("message_kind")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("agent payment summary missing message_kind"))?;
    let payment = summary
        .payload
        .get("payment")
        .cloned()
        .ok_or_else(|| anyhow!("agent payment summary missing payment payload"))?;
    let payment_id = payment
        .get("payment_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("agent payment summary missing payment_id"))?;
    let record = crate::control::AgentPaymentRecord {
        payment_id: payment_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        summary_id: summary.summary_id.clone(),
        message_kind: message_kind.to_owned(),
        payment,
        updated_at: observed_at_ms(),
    };
    crate::control::save_agent_payment_record_state(state_dir, &record)?;
    let event_type = if message_kind == "payment_request" {
        wattswarm_protocol::types::AgentEventType::PaymentRequest
    } else {
        wattswarm_protocol::types::AgentEventType::PaymentUpdate
    };
    let allowed_actions = payment_allowed_actions(message_kind);
    let event = build_agent_event(
        event_type,
        wattswarm_protocol::types::AgentEventSourceKind::PaymentSummary,
        Some(remote_node_id.to_owned()),
        None,
        json!({
            "summary_id": summary.summary_id,
            "message_kind": message_kind,
            "payment": record.payment,
        }),
        true,
        allowed_actions,
        Some(record.payment_id.clone()),
        Some(format!("payment:{}:{}", record.payment_id, message_kind)),
    );
    deliver_agent_event_to_local_executor(state_dir, None, &event)?;
    Ok(record)
}

pub(super) fn payment_allowed_actions(message_kind: &str) -> Vec<String> {
    match message_kind {
        "payment_request" => vec![
            "authorize".to_owned(),
            "reject".to_owned(),
            "cancel".to_owned(),
        ],
        "payment_authorized" => vec!["submit".to_owned(), "cancel".to_owned()],
        "payment_submitted" => vec!["settle".to_owned()],
        "payment_settled" | "payment_rejected" | "payment_cancelled" => Vec::new(),
        _ => Vec::new(),
    }
}

pub(super) fn process_pending_network_commands(
    service: &mut NetworkBridgeService,
    state_dir: &Path,
) -> Result<u64> {
    let pending_path = pending_network_commands_path(state_dir);
    if !pending_path.exists() {
        return Ok(0);
    }
    let content = fs::read_to_string(&pending_path)?;
    if content.trim().is_empty() {
        return Ok(0);
    }
    fs::write(&pending_path, "")?;

    let mut processed = 0_u64;
    let mut failed = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let command: PendingNetworkCommand = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let result = match command {
            PendingNetworkCommand::PeerRelationship {
                remote_node_id,
                action,
                agent_envelope,
            } => service
                .send_peer_relationship_action(&remote_node_id, action, Some(agent_envelope))
                .map(|_| ()),
            PendingNetworkCommand::PeerDirectMessage {
                remote_node_id,
                agent_envelope,
                content,
            } => service
                .send_peer_direct_message(&remote_node_id, Some(agent_envelope), content)
                .map(|_| ()),
            PendingNetworkCommand::AgentPayment {
                remote_node_id,
                message_kind,
                payment,
            } => {
                let mut summary =
                    build_agent_payment_summary(&remote_node_id, &message_kind, payment);
                summary.source_node_id = service.local_peer_id().to_string();
                service.publish_summary(summary).map(|_| ())
            }
        };
        match result {
            Ok(()) => processed += 1,
            Err(err) => {
                eprintln!("network_bridge: failed to process queued network command: {err:#}");
                failed.push(line.to_owned());
            }
        }
    }
    if !failed.is_empty() {
        let mut retry = failed.join("\n");
        retry.push('\n');
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&pending_path)
            .and_then(|mut file| file.write_all(retry.as_bytes()));
    }
    Ok(processed)
}
