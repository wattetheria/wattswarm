use super::*;
use watt_did::{Did, VerifiedAgentContext};

const PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS: i64 = 5_000;
const PENDING_NETWORK_COMMAND_MAX_RETRY_MS: i64 = 60_000;
const PENDING_NETWORK_COMMAND_MAX_ATTEMPTS: u32 = 10;

fn is_zero(value: &u32) -> bool {
    *value == 0
}

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
        #[serde(default, skip_serializing_if = "is_zero")]
        attempts: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_retry_at: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
    },
    AgentPayment {
        remote_node_id: String,
        message_kind: String,
        payment: Value,
        agent_envelope: RawAgentEnvelope,
        #[serde(default, skip_serializing_if = "is_zero")]
        attempts: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_retry_at: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
    },
}

impl PendingNetworkCommand {
    fn is_due(&self, now_ms: i64) -> bool {
        self.next_retry_at().is_none_or(|next| next <= now_ms)
    }

    fn next_retry_at(&self) -> Option<i64> {
        match self {
            Self::PeerRelationship { next_retry_at, .. }
            | Self::AgentPayment { next_retry_at, .. } => *next_retry_at,
        }
    }

    fn remote_node_id(&self) -> &str {
        match self {
            Self::PeerRelationship { remote_node_id, .. }
            | Self::AgentPayment { remote_node_id, .. } => remote_node_id,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::PeerRelationship { attempts, .. } | Self::AgentPayment { attempts, .. } => {
                *attempts
            }
        }
    }

    fn record_failure(&mut self, error: &str, now_ms: i64) {
        let next_attempts = self.attempts().saturating_add(1);
        let delay = PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS
            .saturating_mul(2_i64.saturating_pow(next_attempts.saturating_sub(1)))
            .min(PENDING_NETWORK_COMMAND_MAX_RETRY_MS);
        let next_retry_at = Some(now_ms.saturating_add(delay));
        match self {
            Self::PeerRelationship {
                attempts,
                next_retry_at: retry_at,
                last_error,
                ..
            }
            | Self::AgentPayment {
                attempts,
                next_retry_at: retry_at,
                last_error,
                ..
            } => {
                *attempts = next_attempts;
                *retry_at = next_retry_at;
                *last_error = Some(error.to_owned());
            }
        }
    }

    fn should_abandon(&self) -> bool {
        self.attempts() >= PENDING_NETWORK_COMMAND_MAX_ATTEMPTS
    }
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
            attempts: 0,
            next_retry_at: None,
            last_error: None,
        },
    )
}

pub fn enqueue_agent_payment_command(
    state_dir: &Path,
    remote_node_id: &str,
    message_kind: &str,
    payment: Value,
    agent_envelope: RawAgentEnvelope,
) -> Result<()> {
    enqueue_pending_network_command(
        state_dir,
        &PendingNetworkCommand::AgentPayment {
            remote_node_id: remote_node_id.trim().to_owned(),
            message_kind: message_kind.trim().to_owned(),
            payment,
            agent_envelope,
            attempts: 0,
            next_retry_at: None,
            last_error: None,
        },
    )
}

fn raw_agent_envelope_to_control_record(
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

pub(super) fn raw_agent_envelope_to_protocol(
    envelope: &RawAgentEnvelope,
) -> wattswarm_protocol::types::AgentEnvelope {
    wattswarm_protocol::types::AgentEnvelope {
        protocol: envelope.protocol.clone(),
        transport_profile: envelope.transport_profile.clone(),
        source_agent_id: envelope.source_agent_id.clone(),
        target_agent_id: envelope.target_agent_id.clone(),
        source_node_id: envelope.source_node_id.clone(),
        target_node_id: envelope.target_node_id.clone(),
        capability: envelope.capability.clone(),
        source_agent_card: envelope.source_agent_card.as_ref().map(|card| {
            wattswarm_protocol::types::SourceAgentCard {
                agent_id: card.agent_id.clone(),
                node_id: card.node_id.clone(),
                card_hash: card.card_hash.clone(),
                issued_at: card.issued_at,
                card: card.card.clone(),
                signature: card.signature.clone(),
            }
        }),
        message_json: envelope.message_json.clone(),
        extensions_json: envelope.extensions_json.clone(),
        signature: envelope.signature.clone(),
    }
}

fn build_agent_payment_summary(
    remote_node_id: &str,
    message_kind: &str,
    payment: Value,
    agent_envelope: RawAgentEnvelope,
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
            "agent_envelope": agent_envelope,
        }),
    }
}

#[derive(Debug, Serialize)]
struct UnsignedAgentEnvelope<'a> {
    protocol: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    transport_profile: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_agent_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_agent_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_node_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_node_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_agent_card_hash: Option<&'a String>,
    message_json: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    extensions_json: Option<&'a String>,
}

#[derive(Debug, Serialize)]
struct UnsignedSourceAgentCard<'a> {
    agent_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a String>,
    card_hash: &'a str,
    issued_at: u64,
}

pub(super) fn verify_agent_envelope_signature_for_source(
    envelope: &RawAgentEnvelope,
    expected_source_node_id: Option<&str>,
) -> Result<()> {
    if let Some(card) = &envelope.source_agent_card {
        verify_source_agent_card(envelope, card, expected_source_node_id)?;
    }
    let Some(signature) = envelope.signature.as_deref() else {
        return Ok(());
    };
    let signer_ref = envelope.source_agent_id.as_deref().ok_or_else(|| {
        anyhow!("agent envelope source_agent_id is required when signature is set")
    })?;
    let unsigned = UnsignedAgentEnvelope {
        protocol: &envelope.protocol,
        transport_profile: envelope.transport_profile.as_ref(),
        source_agent_id: envelope.source_agent_id.as_ref(),
        target_agent_id: envelope.target_agent_id.as_ref(),
        source_node_id: envelope.source_node_id.as_ref(),
        target_node_id: envelope.target_node_id.as_ref(),
        capability: envelope.capability.as_ref(),
        source_agent_card_hash: envelope
            .source_agent_card
            .as_ref()
            .map(|card| &card.card_hash),
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

pub fn verified_agent_context_for_source(
    envelope: &RawAgentEnvelope,
    expected_source_node_id: &str,
) -> Result<VerifiedAgentContext> {
    verify_agent_envelope_signature_for_source(envelope, Some(expected_source_node_id))?;
    if envelope.signature.as_deref().unwrap_or_default().is_empty() {
        bail!("verified agent context requires signed agent envelope");
    }
    let source_agent_id = envelope
        .source_agent_id
        .as_deref()
        .ok_or_else(|| anyhow!("verified agent context requires source_agent_id"))?;
    let agent_did = Did::parse(source_agent_id)
        .map_err(|error| anyhow!("verified agent context requires DID source_agent_id: {error}"))?;
    let context = VerifiedAgentContext {
        agent_did,
        controller_node_id: expected_source_node_id.to_owned(),
        source_node_id: envelope.source_node_id.clone(),
        envelope_verified: true,
        source_node_verified: true,
        controller_binding_verified: false,
        controller_binding_proof: None,
        payment_account_binding: None,
        verified_at_ms: observed_at_ms(),
        expires_at_ms: None,
    };
    context
        .validate_basic()
        .map_err(|error| anyhow!("invalid verified agent context: {error}"))?;
    Ok(context)
}

fn verify_source_agent_card(
    envelope: &RawAgentEnvelope,
    card: &RawSourceAgentCard,
    expected_source_node_id: Option<&str>,
) -> Result<()> {
    if let Some(source_agent_id) = envelope.source_agent_id.as_deref()
        && source_agent_id != card.agent_id
    {
        bail!("agent envelope source_agent_id does not match source_agent_card agent_id");
    }
    if let Some(envelope_source_node_id) = envelope.source_node_id.as_deref()
        && card.node_id.as_deref() != Some(envelope_source_node_id)
    {
        bail!("agent envelope source_node_id does not match source_agent_card node_id");
    }
    if let Some(expected_source_node_id) = expected_source_node_id {
        if envelope.source_node_id.as_deref() != Some(expected_source_node_id) {
            bail!("agent envelope source_node_id does not match network source node");
        }
        if let Some(card_node_id) = card.node_id.as_deref()
            && card_node_id != expected_source_node_id
        {
            bail!("source_agent_card node_id does not match network source node");
        }
    }

    let card_hash = format!(
        "sha256:{}",
        crate::crypto::sha256_hex(serde_jcs::to_string(&card.card)?.as_bytes())
    );
    if card.card_hash != card_hash {
        bail!("source_agent_card hash mismatch");
    }

    let Some(signature) = card.signature.as_deref() else {
        return Ok(());
    };
    let unsigned = UnsignedSourceAgentCard {
        agent_id: &card.agent_id,
        node_id: card.node_id.as_ref(),
        card_hash: &card.card_hash,
        issued_at: card.issued_at,
    };
    crate::crypto::verify_signature_ref(
        &card.agent_id,
        serde_jcs::to_string(&unsigned)?.as_bytes(),
        signature,
    )
    .context("verify source_agent_card signature")
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

pub fn default_agent_envelope(
    local_node_id: &str,
    remote_node_id: &str,
    capability: &str,
    payload: Value,
) -> RawAgentEnvelope {
    RawAgentEnvelope {
        protocol: "google_a2a".to_owned(),
        transport_profile: Some("wattswarm_mesh".to_owned()),
        source_agent_id: Some(local_node_id.to_owned()),
        target_agent_id: Some(remote_node_id.to_owned()),
        source_node_id: Some(local_node_id.to_owned()),
        target_node_id: Some(remote_node_id.to_owned()),
        capability: Some(capability.to_owned()),
        source_agent_card: None,
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
    crate::control::private_dm_thread_id(local_node_id, remote_node_id)
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
    let canonical_content = if content.is_null() {
        existing
            .as_ref()
            .map(|record| record.content.clone())
            .or_else(|| {
                agent_envelope
                    .as_ref()
                    .map(peer_dm_content_from_control_envelope)
            })
            .unwrap_or(content)
    } else {
        content
    };
    let record = crate::control::PeerDmMessageRecord {
        thread_id: thread_id.to_owned(),
        message_id: message_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        message_kind,
        direction,
        delivery_state,
        a2a_protocol: a2a_protocol.to_owned(),
        content: canonical_content,
        agent_envelope,
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        acknowledged_at: acknowledged_at
            .or_else(|| existing.as_ref().and_then(|record| record.acknowledged_at)),
    };
    crate::control::save_peer_dm_message_record_state(state_dir, &record)?;
    Ok(record)
}

pub(super) fn save_inbound_private_dm_topic_message(
    state_dir: &Path,
    local_node_id: &str,
    author_node_id: &str,
    event_id: &str,
    content: &Value,
    created_at: u64,
) -> Result<Option<crate::control::PeerDmMessageRecord>> {
    let kind = content.get("kind").and_then(Value::as_str).map(str::trim);
    if kind != Some("direct_message") {
        return Ok(None);
    }
    let remote_node_id = author_node_id.trim();
    if remote_node_id.is_empty() {
        return Ok(None);
    }
    if remote_node_id == local_node_id.trim() {
        return Ok(None);
    }
    let thread_id = content
        .get("thread_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| peer_dm_thread_id(local_node_id, remote_node_id));
    let message_id = content
        .get("message_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(event_id);
    let message_content = content.get("content").cloned().unwrap_or(Value::Null);
    let parsed_envelope = content.get("agent_envelope").cloned().and_then(|value| {
        serde_json::from_value::<crate::control::AgentInteractionEnvelope>(value).ok()
    });
    let a2a_protocol = parsed_envelope
        .as_ref()
        .map(|envelope| envelope.protocol.trim())
        .filter(|protocol| !protocol.is_empty())
        .unwrap_or("google_a2a")
        .to_owned();
    let agent_envelope = parsed_envelope.or_else(|| {
        Some(crate::control::synthesize_peer_dm_envelope(
            &a2a_protocol,
            &message_content,
        ))
    });

    upsert_dm_thread(
        state_dir,
        remote_node_id,
        &thread_id,
        crate::control::PeerDmSessionState::Ready,
        None,
        Some(created_at),
    )?;

    let record = crate::control::PeerDmMessageRecord {
        thread_id,
        message_id: message_id.to_owned(),
        remote_node_id: remote_node_id.to_owned(),
        message_kind: crate::control::PeerDmMessageKind::Message,
        direction: crate::control::PeerDmDirection::Inbound,
        delivery_state: crate::control::PeerDmDeliveryState::Delivered,
        a2a_protocol,
        content: message_content,
        agent_envelope,
        created_at,
        acknowledged_at: Some(created_at),
    };
    crate::control::save_peer_dm_message_record_state(state_dir, &record)?;
    Ok(Some(record))
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
    let agent_envelope = summary
        .payload
        .get("agent_envelope")
        .cloned()
        .map(serde_json::from_value::<wattswarm_protocol::types::AgentEnvelope>)
        .transpose()?;
    let verified_context = match agent_envelope.as_ref() {
        Some(envelope) => {
            verify_protocol_agent_envelope_for_source(envelope, Some(remote_node_id))?;
            optional_verified_agent_context_for_protocol_source(envelope, remote_node_id)?
        }
        None => None,
    };
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
    let payload = payload_with_verified_agent_context(
        json!({
            "summary_id": summary.summary_id,
            "message_kind": message_kind,
            "payment": record.payment,
        }),
        verified_context.as_ref(),
    )?;
    let event = build_agent_event_with_agent_envelope(
        event_type,
        wattswarm_protocol::types::AgentEventSourceKind::PaymentSummary,
        Some(remote_node_id.to_owned()),
        None,
        agent_envelope.clone(),
        payload,
        true,
        allowed_actions,
        Some(record.payment_id.clone()),
        Some(format!("payment:{}:{}", record.payment_id, message_kind)),
    );
    deliver_agent_event_to_local_executor(state_dir, None, &event)?;
    Ok(record)
}

pub(super) fn verify_protocol_agent_envelope_for_source(
    envelope: &wattswarm_protocol::types::AgentEnvelope,
    expected_source_node_id: Option<&str>,
) -> Result<()> {
    let raw = protocol_agent_envelope_to_raw(envelope);
    verify_agent_envelope_signature_for_source(&raw, expected_source_node_id)
}

pub(super) fn verified_agent_context_for_protocol_source(
    envelope: &wattswarm_protocol::types::AgentEnvelope,
    expected_source_node_id: &str,
) -> Result<VerifiedAgentContext> {
    let raw = protocol_agent_envelope_to_raw(envelope);
    verified_agent_context_for_source(&raw, expected_source_node_id)
}

/// Derive a verified context only when the envelope can support one.
///
/// Returns `Ok(None)` for unsigned envelopes or envelopes whose
/// `source_agent_id` is not a DID, preserving the lenient acceptance done by
/// [`verify_protocol_agent_envelope_for_source`]. Returns `Err` only when an
/// envelope that should produce a context fails verification.
pub(super) fn optional_verified_agent_context_for_protocol_source(
    envelope: &wattswarm_protocol::types::AgentEnvelope,
    expected_source_node_id: &str,
) -> Result<Option<VerifiedAgentContext>> {
    if envelope.signature.as_deref().unwrap_or_default().is_empty() {
        return Ok(None);
    }
    let Some(source_agent_id) = envelope.source_agent_id.as_deref() else {
        return Ok(None);
    };
    if Did::parse(source_agent_id).is_err() {
        return Ok(None);
    }
    Ok(Some(verified_agent_context_for_protocol_source(
        envelope,
        expected_source_node_id,
    )?))
}

pub(super) const VERIFIED_AGENT_CONTEXT_PAYLOAD_KEY: &str = "__verified_agent_context";

pub(super) fn payload_with_verified_agent_context(
    mut payload: Value,
    context: Option<&VerifiedAgentContext>,
) -> Result<Value> {
    let Some(context) = context else {
        return Ok(payload);
    };
    let serialized =
        serde_json::to_value(context).context("serialize verified agent context for payload")?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(VERIFIED_AGENT_CONTEXT_PAYLOAD_KEY.to_owned(), serialized);
    }
    Ok(payload)
}

fn protocol_agent_envelope_to_raw(
    envelope: &wattswarm_protocol::types::AgentEnvelope,
) -> RawAgentEnvelope {
    RawAgentEnvelope {
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
            .map(|card| RawSourceAgentCard {
                agent_id: card.agent_id.clone(),
                node_id: card.node_id.clone(),
                card_hash: card.card_hash.clone(),
                issued_at: card.issued_at,
                card: card.card.clone(),
                signature: card.signature.clone(),
            }),
        message_json: envelope.message_json.clone(),
        extensions_json: envelope.extensions_json.clone(),
        signature: envelope.signature.clone(),
    }
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
    let now_ms = observed_at_ms() as i64;
    let has_due_command = content.lines().any(|line| {
        serde_json::from_str::<PendingNetworkCommand>(line.trim())
            .is_ok_and(|command| command.is_due(now_ms))
    });
    if !has_due_command {
        return Ok(0);
    }
    fs::write(&pending_path, "")?;

    let mut processed = 0_u64;
    let mut retry = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut command: PendingNetworkCommand = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !command.is_due(now_ms) {
            retry.push(line.to_owned());
            continue;
        }
        let result = match command.clone() {
            PendingNetworkCommand::PeerRelationship {
                remote_node_id,
                action,
                agent_envelope,
                ..
            } => service
                .send_peer_relationship_action(&remote_node_id, action, Some(agent_envelope))
                .map(|_| ()),
            PendingNetworkCommand::AgentPayment {
                remote_node_id,
                message_kind,
                payment,
                agent_envelope,
                ..
            } => {
                let mut summary = build_agent_payment_summary(
                    &remote_node_id,
                    &message_kind,
                    payment,
                    agent_envelope,
                );
                summary.source_node_id = service.local_peer_id().to_string();
                service.publish_summary(summary).map(|_| ())
            }
        };
        match result {
            Ok(()) => processed += 1,
            Err(err) => {
                let error = format!("{err:#}");
                command.record_failure(&error, now_ms);
                if command.should_abandon() {
                    eprintln!(
                        "network_bridge: abandoning queued network command after {} attempts for {}: {error}",
                        command.attempts(),
                        command.remote_node_id()
                    );
                } else {
                    if command.attempts() == 1 {
                        eprintln!(
                            "network_bridge: failed to process queued network command: {error}"
                        );
                    }
                    retry.push(serde_json::to_string(&command)?);
                }
            }
        }
    }
    if !retry.is_empty() {
        let mut retry = retry.join("\n");
        retry.push('\n');
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&pending_path)
            .and_then(|mut file| file.write_all(retry.as_bytes()));
    }
    Ok(processed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use serde::Serialize;
    use serde_json::json;

    fn did_key_for_identity(identity: &crate::crypto::NodeIdentity) -> String {
        let mut encoded = vec![0xed, 0x01];
        encoded.extend_from_slice(identity.verifying_key().as_bytes());
        format!("did:key:z{}", bs58::encode(encoded).into_string())
    }

    fn sign_json<T: Serialize>(identity: &crate::crypto::NodeIdentity, payload: &T) -> String {
        let bytes = serde_jcs::to_string(payload)
            .expect("canonical payload")
            .into_bytes();
        let signing_key = SigningKey::from_bytes(&identity.secret_bytes());
        base64::engine::general_purpose::STANDARD.encode(signing_key.sign(&bytes).to_bytes())
    }

    fn signed_envelope_with_card(source_node: &str, target_node: &str) -> RawAgentEnvelope {
        let identity = crate::crypto::NodeIdentity::random();
        let agent_id = did_key_for_identity(&identity);
        let card = json!({
            "protocolVersion": "0.3.0",
            "name": "Test Agent",
            "skills": [{"id": "task", "name": "Task", "tags": ["task"]}],
            "metadata": {
                "agent_id": agent_id,
                "node_id": source_node,
                "transport_profile": "wattswarm_mesh"
            }
        });
        let card_hash = format!(
            "sha256:{}",
            crate::crypto::sha256_hex(
                serde_jcs::to_string(&card)
                    .expect("canonical card")
                    .as_bytes()
            )
        );
        let source_node_id = source_node.to_owned();
        let source_agent_card = RawSourceAgentCard {
            agent_id: agent_id.clone(),
            node_id: Some(source_node_id.clone()),
            card_hash: card_hash.clone(),
            issued_at: 42,
            card,
            signature: Some(sign_json(
                &identity,
                &UnsignedSourceAgentCard {
                    agent_id: &agent_id,
                    node_id: Some(&source_node_id),
                    card_hash: &card_hash,
                    issued_at: 42,
                },
            )),
        };
        let message_json = json!({
            "task_id": "task-1",
            "action": "claim"
        })
        .to_string();
        let protocol = "google_a2a".to_owned();
        let transport_profile = Some("wattswarm_mesh".to_owned());
        let capability = Some("task.claim".to_owned());
        let source_agent_id = Some(agent_id.clone());
        let target_agent_id = Some("did:key:ztarget".to_owned());
        let source_node_id = Some(source_node_id);
        let target_node_id = Some(target_node.to_owned());
        let unsigned = UnsignedAgentEnvelope {
            protocol: &protocol,
            transport_profile: transport_profile.as_ref(),
            source_agent_id: source_agent_id.as_ref(),
            target_agent_id: target_agent_id.as_ref(),
            source_node_id: source_node_id.as_ref(),
            target_node_id: target_node_id.as_ref(),
            capability: capability.as_ref(),
            source_agent_card_hash: Some(&card_hash),
            message_json: &message_json,
            extensions_json: None,
        };
        let signature = sign_json(&identity, &unsigned);
        RawAgentEnvelope {
            protocol,
            transport_profile,
            source_agent_id,
            target_agent_id,
            source_node_id,
            target_node_id,
            capability,
            source_agent_card: Some(source_agent_card),
            message_json,
            extensions_json: None,
            signature: Some(signature),
        }
    }

    #[test]
    fn verify_agent_envelope_accepts_signed_source_agent_card() {
        let envelope = signed_envelope_with_card("node-a", "node-b");
        verify_agent_envelope_signature_for_source(&envelope, Some("node-a"))
            .expect("valid source agent card and envelope signature");
    }

    #[test]
    fn verified_agent_context_contains_did_and_verified_source_node() {
        let envelope = signed_envelope_with_card("node-a", "node-b");
        let context = verified_agent_context_for_source(&envelope, "node-a")
            .expect("verified context from signed envelope");

        assert_eq!(
            context.agent_did.to_string(),
            envelope.source_agent_id.as_deref().unwrap()
        );
        assert_eq!(context.controller_node_id, "node-a");
        assert_eq!(context.source_node_id.as_deref(), Some("node-a"));
        assert!(context.envelope_verified);
        assert!(context.source_node_verified);
        assert!(!context.controller_binding_verified);
    }

    #[test]
    fn verify_agent_envelope_rejects_wrong_network_source_node() {
        let envelope = signed_envelope_with_card("node-a", "node-b");
        let err = verify_agent_envelope_signature_for_source(&envelope, Some("node-c"))
            .expect_err("network source node must match source card node");
        assert!(
            err.to_string()
                .contains("agent envelope source_node_id does not match network source node"),
            "{err:#}"
        );
    }

    #[test]
    fn verified_agent_context_for_protocol_source_matches_raw_envelope() {
        let raw = signed_envelope_with_card("node-a", "node-b");
        let protocol_envelope = raw_agent_envelope_to_protocol(&raw);
        let context = verified_agent_context_for_protocol_source(&protocol_envelope, "node-a")
            .expect("verified context from signed protocol envelope");
        assert_eq!(
            context.agent_did.to_string(),
            raw.source_agent_id.as_deref().unwrap()
        );
        assert_eq!(context.controller_node_id, "node-a");
        assert_eq!(context.source_node_id.as_deref(), Some("node-a"));
    }

    #[test]
    fn optional_verified_agent_context_skips_unsigned_envelope() {
        let raw = signed_envelope_with_card("node-a", "node-b");
        let mut protocol_envelope = raw_agent_envelope_to_protocol(&raw);
        protocol_envelope.signature = None;
        let context =
            optional_verified_agent_context_for_protocol_source(&protocol_envelope, "node-a")
                .expect("unsigned envelope skipped without error");
        assert!(context.is_none());
    }

    #[test]
    fn optional_verified_agent_context_skips_non_did_source_agent_id() {
        let raw = signed_envelope_with_card("node-a", "node-b");
        let mut protocol_envelope = raw_agent_envelope_to_protocol(&raw);
        protocol_envelope.source_agent_id = Some("not-a-did".to_owned());
        let context =
            optional_verified_agent_context_for_protocol_source(&protocol_envelope, "node-a")
                .expect("non-DID source agent id skipped without error");
        assert!(context.is_none());
    }

    #[test]
    fn payload_with_verified_agent_context_inserts_key_when_present() {
        let raw = signed_envelope_with_card("node-a", "node-b");
        let protocol_envelope = raw_agent_envelope_to_protocol(&raw);
        let context = verified_agent_context_for_protocol_source(&protocol_envelope, "node-a")
            .expect("verified context");
        let payload =
            payload_with_verified_agent_context(json!({"summary_id": "s"}), Some(&context))
                .expect("payload with context");
        let context_value = payload
            .get(VERIFIED_AGENT_CONTEXT_PAYLOAD_KEY)
            .expect("context key present");
        let round_trip: VerifiedAgentContext = serde_json::from_value(context_value.clone())
            .expect("payload context deserializes back to VerifiedAgentContext");
        assert_eq!(round_trip, context);
        assert_eq!(round_trip.controller_node_id, "node-a");
        assert_eq!(round_trip.source_node_id.as_deref(), Some("node-a"));
        assert!(round_trip.envelope_verified);
        assert!(round_trip.source_node_verified);
        assert_eq!(payload.get("summary_id").and_then(Value::as_str), Some("s"));
    }

    #[test]
    fn payload_with_verified_agent_context_passes_through_when_none() {
        let payload =
            payload_with_verified_agent_context(json!({"summary_id": "s"}), None).expect("payload");
        assert!(payload.get(VERIFIED_AGENT_CONTEXT_PAYLOAD_KEY).is_none());
        assert_eq!(payload.get("summary_id").and_then(Value::as_str), Some("s"));
    }

    #[test]
    fn pending_network_command_defaults_retry_metadata_for_legacy_lines() {
        let command: PendingNetworkCommand = serde_json::from_value(json!({
            "kind": "peer_relationship",
            "remote_node_id": "node-a",
            "action": "request",
            "agent_envelope": {
                "protocol": "google_a2a",
                "message": {}
            }
        }))
        .expect("legacy command parses");

        assert_eq!(command.attempts(), 0);
        assert!(command.is_due(100));
        assert_eq!(command.next_retry_at(), None);
    }

    #[test]
    fn pending_network_command_failure_sets_retry_backoff() {
        let mut command: PendingNetworkCommand = serde_json::from_value(json!({
            "kind": "peer_relationship",
            "remote_node_id": "node-a",
            "action": "request",
            "agent_envelope": {
                "protocol": "google_a2a",
                "message": {}
            }
        }))
        .expect("command parses");

        command.record_failure("not connected", 1_000);

        assert_eq!(command.attempts(), 1);
        assert_eq!(
            command.next_retry_at(),
            Some(1_000 + PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS)
        );
        assert!(!command.is_due(1_001));
        assert!(command.is_due(1_000 + PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS));
        assert!(!command.should_abandon());
    }
}
