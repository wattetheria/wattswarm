use super::*;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use watt_did::{Did, DidKey, DidKeyPublicKey, VerifiedAgentContext};
use wattswarm_network_client_server::ClientServerTransport as _;

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
    pub(super) agent_envelope: RawAgentEnvelope,
    pub(super) started_at_ms: i64,
}

#[derive(Debug, Clone)]
pub(super) struct PendingContactMaterialRequest {
    pub(super) peer: NetworkNodeId,
    pub(super) remote_node_id: String,
    pub(super) started_at: Instant,
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
    fn command_kind(&self) -> &'static str {
        match self {
            Self::PeerRelationship { .. } => "peer_relationship",
            Self::AgentPayment { .. } => "agent_payment",
        }
    }

    fn dedup_key(&self) -> Result<String> {
        let stable = match self {
            Self::PeerRelationship {
                remote_node_id,
                action,
                agent_envelope,
                ..
            } => serde_json::to_vec(&json!({
                "kind": "peer_relationship",
                "remote_node_id": remote_node_id,
                "action": action,
                "message_json": agent_envelope.message_json,
            }))?,
            Self::AgentPayment {
                remote_node_id,
                message_kind,
                payment,
                agent_envelope,
                ..
            } => serde_json::to_vec(&json!({
                "kind": "agent_payment",
                "remote_node_id": remote_node_id,
                "message_kind": message_kind,
                "payment": payment,
                "message_json": agent_envelope.message_json,
            }))?,
        };
        Ok(format!(
            "{}:{}",
            self.command_kind(),
            hex::encode(Sha256::digest(stable))
        ))
    }

    #[cfg(test)]
    fn set_attempts(&mut self, value: u32) {
        match self {
            Self::PeerRelationship { attempts, .. } | Self::AgentPayment { attempts, .. } => {
                *attempts = value;
            }
        }
    }

    #[cfg(test)]
    fn set_retry_state(&mut self, retry_at: Option<i64>, error: Option<String>) {
        match self {
            Self::PeerRelationship {
                next_retry_at,
                last_error,
                ..
            }
            | Self::AgentPayment {
                next_retry_at,
                last_error,
                ..
            } => {
                *next_retry_at = retry_at;
                *last_error = error;
            }
        }
    }

    #[cfg(test)]
    fn is_due(&self, now_ms: i64) -> bool {
        self.next_retry_at().is_none_or(|next| next <= now_ms)
    }

    fn next_retry_at(&self) -> Option<i64> {
        match self {
            Self::PeerRelationship { next_retry_at, .. }
            | Self::AgentPayment { next_retry_at, .. } => *next_retry_at,
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

    #[cfg(test)]
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
    let dedup_key = command.dedup_key()?;
    let now = observed_at_ms();
    crate::storage::local_control_store(state_dir)?.enqueue_pending_network_command(
        &crate::storage::local_control_scope_id(state_dir),
        &crate::storage::PendingNetworkCommandInsert {
            command_id: hex::encode(Sha256::digest(dedup_key.as_bytes())),
            dedup_key: Some(dedup_key),
            command_kind: command.command_kind().to_owned(),
            payload_json: serde_json::to_string(command)?,
            attempts: command.attempts(),
            next_retry_at: command.next_retry_at().map(|value| value as u64),
            last_error: None,
            created_at: now,
        },
    )?;
    Ok(())
}

#[cfg(test)]
fn load_pending_network_commands(state_dir: &Path) -> Result<Vec<PendingNetworkCommand>> {
    let store = crate::storage::local_control_store(state_dir)?;
    let scope_id = crate::storage::local_control_scope_id(state_dir);
    store
        .list_pending_network_commands(&scope_id)?
        .into_iter()
        .map(|row| {
            let mut command: PendingNetworkCommand = serde_json::from_str(&row.payload_json)?;
            command.set_attempts(row.attempts);
            command.set_retry_state(row.next_retry_at.map(|value| value as i64), row.last_error);
            Ok(command)
        })
        .collect()
}

fn upsert_peer_relationship_action_command(
    state_dir: &Path,
    command: PendingNetworkCommand,
) -> Result<()> {
    if !matches!(command, PendingNetworkCommand::PeerRelationship { .. }) {
        bail!("peer relationship command upsert received non-relationship command");
    }
    enqueue_pending_network_command(state_dir, &command)
}

pub(super) fn remove_peer_relationship_action_command(
    state_dir: &Path,
    remote_node_id: &str,
    action: crate::control::PeerRelationshipAction,
    agent_envelope: &RawAgentEnvelope,
) -> Result<()> {
    let command = PendingNetworkCommand::PeerRelationship {
        remote_node_id: remote_node_id.trim().to_owned(),
        action,
        agent_envelope: agent_envelope.clone(),
        attempts: 0,
        next_retry_at: None,
        last_error: None,
    };
    crate::storage::local_control_store(state_dir)?.remove_pending_network_command_by_dedup_key(
        &crate::storage::local_control_scope_id(state_dir),
        &command.dedup_key()?,
    )?;
    Ok(())
}

pub(super) fn record_peer_relationship_action_command_failure(
    state_dir: &Path,
    remote_node_id: &str,
    action: crate::control::PeerRelationshipAction,
    agent_envelope: RawAgentEnvelope,
    error: &str,
) -> Result<()> {
    let now_ms = observed_at_ms() as i64;
    let command = PendingNetworkCommand::PeerRelationship {
        remote_node_id: remote_node_id.trim().to_owned(),
        action,
        agent_envelope,
        attempts: 0,
        next_retry_at: None,
        last_error: None,
    };
    let store = crate::storage::local_control_store(state_dir)?;
    let scope_id = crate::storage::local_control_scope_id(state_dir);
    let dedup_key = command.dedup_key()?;
    if !store.retry_pending_network_command_by_dedup_key(
        &scope_id,
        &dedup_key,
        now_ms.saturating_add(PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS) as u64,
        error,
    )? {
        let mut command = command;
        command.record_failure(error, now_ms);
        enqueue_pending_network_command(state_dir, &command)?;
    }
    Ok(())
}

pub fn enqueue_peer_relationship_action_command(
    state_dir: &Path,
    remote_node_id: &str,
    action: crate::control::PeerRelationshipAction,
    agent_envelope: RawAgentEnvelope,
) -> Result<()> {
    upsert_peer_relationship_action_command(
        state_dir,
        PendingNetworkCommand::PeerRelationship {
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

pub(super) fn raw_relationship_request_id(envelope: &RawAgentEnvelope) -> Option<String> {
    serde_json::from_str::<Value>(&envelope.message_json)
        .ok()?
        .get("request_id")?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn stored_relationship_request_id(record: &crate::control::PeerRelationshipRecord) -> Option<&str> {
    record
        .agent_envelope
        .as_ref()?
        .message
        .get("request_id")?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn seed_legacy_relationship_request(
    state_dir: &Path,
    request_id: &str,
    node_record: Option<&crate::control::PeerRelationshipRecord>,
) -> Result<()> {
    let Some(node_record) =
        node_record.filter(|record| stored_relationship_request_id(record) == Some(request_id))
    else {
        return Ok(());
    };
    let already_seeded = crate::control::load_peer_relationship_request_records_state(state_dir)?
        .iter()
        .any(|record| {
            record.remote_node_id == node_record.remote_node_id && record.request_id == request_id
        });
    if already_seeded {
        return Ok(());
    }
    let Some(envelope) = node_record.agent_envelope.clone() else {
        return Ok(());
    };
    let request_initiator =
        if envelope.source_node_id.as_deref() == Some(node_record.remote_node_id.as_str()) {
            crate::control::PeerRelationshipInitiator::Remote
        } else {
            crate::control::PeerRelationshipInitiator::Local
        };
    crate::control::apply_peer_relationship_request_action_state(
        state_dir,
        &node_record.remote_node_id,
        request_id,
        crate::control::PeerRelationshipAction::Request,
        request_initiator,
        envelope.clone(),
    )?;
    let final_action = match node_record.relationship_state {
        crate::control::PeerRelationshipState::Accepted => {
            Some(crate::control::PeerRelationshipAction::Accept)
        }
        crate::control::PeerRelationshipState::Rejected => {
            Some(crate::control::PeerRelationshipAction::Reject)
        }
        _ => None,
    };
    if let Some(final_action) = final_action {
        crate::control::apply_peer_relationship_request_action_state(
            state_dir,
            &node_record.remote_node_id,
            request_id,
            final_action,
            node_record.initiated_by,
            envelope,
        )?;
    }
    Ok(())
}

pub(super) fn apply_peer_relationship_action_projection(
    state_dir: &Path,
    remote_node_id: &str,
    action: crate::control::PeerRelationshipAction,
    initiated_by: crate::control::PeerRelationshipInitiator,
    envelope: &RawAgentEnvelope,
) -> Result<(crate::control::PeerRelationshipRecord, bool)> {
    let request_id = raw_relationship_request_id(envelope);
    let request_scoped = matches!(
        action,
        crate::control::PeerRelationshipAction::Request
            | crate::control::PeerRelationshipAction::Accept
            | crate::control::PeerRelationshipAction::Reject
            | crate::control::PeerRelationshipAction::Cancel
            | crate::control::PeerRelationshipAction::Remove
    );
    let Some(request_id) = request_id.filter(|_| request_scoped) else {
        let mut record = crate::control::apply_peer_relationship_action_state(
            state_dir,
            remote_node_id,
            action,
            initiated_by,
        )?;
        if initiated_by == crate::control::PeerRelationshipInitiator::Remote {
            record.agent_envelope = Some(raw_agent_envelope_to_control_record(envelope));
            crate::control::save_peer_relationship_record_state(state_dir, &record)?;
        }
        return Ok((record, false));
    };
    let existing_node = crate::control::load_peer_relationship_records_state(state_dir)?
        .into_iter()
        .find(|record| record.remote_node_id == remote_node_id);
    if action == crate::control::PeerRelationshipAction::Request
        && existing_node.as_ref().is_some_and(|record| {
            record.relationship_state == crate::control::PeerRelationshipState::Blocked
        })
    {
        bail!("cannot request relationship while remote_node_id={remote_node_id} is blocked");
    };
    seed_legacy_relationship_request(state_dir, &request_id, existing_node.as_ref())?;
    let (request_record, replayed) = crate::control::apply_peer_relationship_request_action_state(
        state_dir,
        remote_node_id,
        &request_id,
        action,
        initiated_by,
        raw_agent_envelope_to_control_record(envelope),
    )?;
    if replayed {
        return Ok((request_record.into(), true));
    }

    let related_requests = crate::control::load_peer_relationship_request_records_state(state_dir)?;
    let has_other_pending = related_requests.iter().any(|record| {
        record.remote_node_id == remote_node_id
            && record.request_id != request_id
            && record.relationship_state == crate::control::PeerRelationshipState::Requested
    });
    let has_other_accepted = related_requests.iter().any(|record| {
        record.remote_node_id == remote_node_id
            && record.request_id != request_id
            && record.relationship_state == crate::control::PeerRelationshipState::Accepted
    });
    let node_state = existing_node
        .as_ref()
        .map(|record| record.relationship_state)
        .unwrap_or(crate::control::PeerRelationshipState::None);
    let should_apply_node_action = match action {
        crate::control::PeerRelationshipAction::Request => !matches!(
            node_state,
            crate::control::PeerRelationshipState::Requested
                | crate::control::PeerRelationshipState::Accepted
        ),
        crate::control::PeerRelationshipAction::Accept => {
            node_state != crate::control::PeerRelationshipState::Accepted
        }
        crate::control::PeerRelationshipAction::Reject
        | crate::control::PeerRelationshipAction::Cancel => {
            node_state != crate::control::PeerRelationshipState::Accepted && !has_other_pending
        }
        crate::control::PeerRelationshipAction::Remove => !has_other_accepted,
        crate::control::PeerRelationshipAction::Block
        | crate::control::PeerRelationshipAction::Unblock => true,
    };
    if should_apply_node_action {
        crate::control::apply_peer_relationship_action_state(
            state_dir,
            remote_node_id,
            action,
            initiated_by,
        )?;
        if initiated_by == crate::control::PeerRelationshipInitiator::Remote {
            attach_agent_envelope_to_relationship(state_dir, remote_node_id, envelope)?;
        }
    }
    Ok((request_record.into(), false))
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

fn verify_agent_signature_ref(
    public_key_ref: &str,
    message: &[u8],
    signature_b64: &str,
) -> Result<()> {
    let public_key = if public_key_ref.starts_with("did:key:") {
        let did = Did::parse(public_key_ref).context("parse did:key")?;
        let did_key = DidKey::from_did(did).context("validate did:key")?;
        match did_key.decode_public_key().context("decode did:key")? {
            DidKeyPublicKey::Ed25519(bytes) => bytes.to_vec(),
            DidKeyPublicKey::X25519(_) | DidKeyPublicKey::Secp256k1Compressed(_) => {
                bail!("did:key is not an Ed25519 verification key")
            }
        }
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(public_key_ref)
            .context("decode base64 public key")?
    };
    let signature = base64::engine::general_purpose::STANDARD
        .decode(signature_b64)
        .context("decode signature")?;
    crate::crypto::verify_signature_bytes(&public_key, message, &signature)
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
    verify_agent_signature_ref(
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
    verify_agent_signature_ref(
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
    let incoming_envelope = raw_agent_envelope_to_control_record(envelope);
    record.agent_envelope = Some(relationship_agent_envelope_for_update(
        record.agent_envelope.as_ref(),
        incoming_envelope,
    ));
    crate::control::save_peer_relationship_record_state(state_dir, &record)
}

fn relationship_agent_envelope_for_update(
    existing: Option<&crate::control::AgentInteractionEnvelope>,
    incoming: crate::control::AgentInteractionEnvelope,
) -> crate::control::AgentInteractionEnvelope {
    if let Some(existing) = existing
        && relationship_envelope_has_visible_message(existing)
        && !relationship_envelope_has_visible_message(&incoming)
    {
        return existing.clone();
    }
    incoming
}

fn relationship_envelope_has_visible_message(
    envelope: &crate::control::AgentInteractionEnvelope,
) -> bool {
    ["text", "payload", "message"].iter().any(|key| {
        envelope
            .message
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
    })
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
    let merged_relationship_established_at = relationship_established_at.or_else(|| {
        existing
            .as_ref()
            .and_then(|record| record.relationship_established_at)
    });
    let merged_last_message_at =
        last_message_at.or_else(|| existing.as_ref().and_then(|record| record.last_message_at));
    let record = crate::control::PeerDmThreadRecord {
        remote_node_id: remote_node_id.to_owned(),
        thread_id: thread_id.to_owned(),
        thread_kind: crate::control::PeerDmThreadKind::Direct,
        session_state: merged_session_state,
        relationship_established_at: merged_relationship_established_at,
        created_at: existing.as_ref().map_or(now, |record| record.created_at),
        updated_at: existing
            .as_ref()
            .map_or(now, |record| record.updated_at.max(now)),
        last_message_at: merged_last_message_at,
    }
    .normalized_lifetime();
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

pub(super) struct InboundPrivateDmProjection {
    pub topic_content: Value,
}

pub(super) fn save_inbound_private_dm_topic_message(
    state_dir: &Path,
    local_node_id: &str,
    author_node_id: &str,
    event_id: &str,
    content: &Value,
    created_at: u64,
) -> Result<Option<InboundPrivateDmProjection>> {
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
    let (message_content, parsed_envelope, topic_content) = if let Some(encrypted) =
        content.get("encrypted")
    {
        let encrypted =
            serde_json::from_value::<crate::crypto::PrivateEncryptedPayload>(encrypted.clone())
                .context("decode private dm encrypted payload")?;
        let keypair = crate::control::load_or_create_private_message_keypair_state(state_dir)?;
        let plaintext = crate::crypto::decrypt_private_message(
            &keypair.secret_key_b64,
            &encrypted,
            &crate::control::private_dm_encryption_aad(
                remote_node_id,
                local_node_id,
                &thread_id,
                message_id,
            ),
        )?;
        let private_payload: Value =
            serde_json::from_slice(&plaintext).context("decode private dm plaintext")?;
        record_private_dm_crypto_diagnostic(
            state_dir,
            PrivateDmCryptoDiagnostic {
                phase: "private_dm.decrypt",
                message: "private DM message decrypted from network transport",
                event_id: Some(event_id),
                local_node_id,
                remote_node_id,
                thread_id: &thread_id,
                message_id,
                scope_hint: None,
                scheme: &encrypted.scheme,
                key_agreement: &encrypted.key_agreement,
                cipher: &encrypted.cipher,
                sender_public_key_len: encrypted.sender_public_key_b64.len(),
                recipient_public_key_len: encrypted.recipient_public_key_b64.len(),
            },
        );
        let message_content = private_payload
            .get("content")
            .cloned()
            .unwrap_or(Value::Null);
        let parsed_envelope = private_payload
            .get("agent_envelope")
            .cloned()
            .and_then(|value| {
                serde_json::from_value::<crate::control::AgentInteractionEnvelope>(value).ok()
            });
        let mut topic_content = content.clone();
        if let Some(object) = topic_content.as_object_mut() {
            object.remove("encrypted");
            object.insert("content".to_owned(), message_content.clone());
            if let Some(envelope) = parsed_envelope.as_ref() {
                object.insert("agent_envelope".to_owned(), serde_json::to_value(envelope)?);
            }
        }
        (message_content, parsed_envelope, topic_content)
    } else {
        let message_content = content.get("content").cloned().unwrap_or(Value::Null);
        let parsed_envelope = content.get("agent_envelope").cloned().and_then(|value| {
            serde_json::from_value::<crate::control::AgentInteractionEnvelope>(value).ok()
        });
        (message_content, parsed_envelope, content.clone())
    };
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

    maybe_store_private_hive_key_share(state_dir, &message_content, created_at)?;

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
    Ok(Some(InboundPrivateDmProjection { topic_content }))
}

fn maybe_store_private_hive_key_share(
    state_dir: &Path,
    content: &Value,
    created_at: u64,
) -> Result<()> {
    let key_payload =
        if content.get("kind").and_then(Value::as_str) == Some("private_hive_key_share") {
            content
        } else if let Some(payload) = content.get("private_hive_key") {
            payload
        } else {
            return Ok(());
        };
    let Some(feed_key) = key_payload
        .get("feed_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(scope_hint) = key_payload
        .get("scope_hint")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(shared_secret_b64) = key_payload
        .get("shared_secret_b64")
        .or_else(|| key_payload.get("group_key_b64"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let epoch = key_payload
        .get("epoch")
        .or_else(|| key_payload.get("secret_epoch"))
        .and_then(Value::as_u64)
        .unwrap_or(1);
    let group_id = key_payload
        .get("group_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| crate::control::private_hive_group_id(feed_key, scope_hint));
    crate::control::upsert_private_hive_key_record_state(
        state_dir,
        crate::control::PrivateHiveKeyRecord {
            feed_key: feed_key.to_owned(),
            scope_hint: scope_hint.to_owned(),
            group_id,
            epoch,
            shared_secret_b64: shared_secret_b64.to_owned(),
            updated_at: created_at,
        },
    )
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
    enqueue_agent_event_for_local_executor(state_dir, &event)?;
    Ok(record)
}

pub(super) fn save_agent_payment_event(
    state_dir: &Path,
    remote_node_id: &str,
    event_id: &str,
    payload: &crate::types::AgentPaymentPostedPayload,
) -> Result<crate::control::AgentPaymentRecord> {
    let payment_id = payload
        .payment
        .get("payment_id")
        .and_then(Value::as_str)
        .unwrap_or("payment");
    let summary = SummaryAnnouncement {
        summary_id: format!("payment:{payment_id}:{event_id}"),
        source_node_id: remote_node_id.to_owned(),
        scope: SwarmScope::Node(payload.remote_node_id.clone()),
        summary_kind: AGENT_PAYMENT_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: json!({
            "message_kind": payload.message_kind,
            "payment": payload.payment,
            "agent_envelope": payload.agent_envelope,
        }),
    };
    save_agent_payment_summary(state_dir, remote_node_id, &summary)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CsPeerRelationshipControlPayload {
    pub(super) action: crate::control::PeerRelationshipAction,
    pub(super) agent_envelope: RawAgentEnvelope,
    pub(super) contact_material: RawContactMaterial,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CsPeerRelationshipAckPayload {
    pub(super) acknowledged_correlation_id: String,
}

pub(super) fn finalize_client_server_dm_session(
    node: &mut Node,
    state_dir: &Path,
    remote_node_id: &str,
    direction: crate::control::PeerDmDirection,
    a2a_protocol: &str,
    established_at: u64,
) -> Result<()> {
    let local_node_id = node.node_id();
    let scope = SwarmScope::Group(crate::control::private_dm_group_id(
        &local_node_id,
        remote_node_id,
    ));
    if !node_has_active_subscription_scope_kinds(
        node,
        &local_node_id,
        &scope,
        &[GossipKind::Messages],
    )? {
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: current_network_context_id(node),
                    subscriber_node_id: local_node_id.clone(),
                    feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
                    scope_hint: crate::control::private_dm_scope_hint(
                        &local_node_id,
                        remote_node_id,
                    ),
                    gossip_kinds: vec!["messages".to_owned()],
                    provider_capabilities: Some(
                        crate::types::TopicProviderCapabilities::local_history_provider(),
                    ),
                    agent_envelope: None,
                    active: true,
                },
            ),
            established_at,
        )?;
    }
    let thread_id = peer_dm_thread_id(&local_node_id, remote_node_id);
    upsert_dm_thread(
        state_dir,
        remote_node_id,
        &thread_id,
        crate::control::PeerDmSessionState::Ready,
        Some(established_at),
        Some(established_at),
    )?;
    for (message_id, kind, payload) in [
        (
            format!("relationship-established:{thread_id}"),
            crate::control::PeerDmMessageKind::RelationshipEstablished,
            json!({
                "relationship_state": "accepted",
                "thread_id": thread_id,
                "established_at": established_at,
                "synthetic": true,
            }),
        ),
        (
            format!("session-init:{thread_id}"),
            crate::control::PeerDmMessageKind::SessionInit,
            json!({
                "thread_id": thread_id,
                "session_state": "ready",
                "synthetic": true,
            }),
        ),
    ] {
        save_dm_message(
            state_dir,
            remote_node_id,
            &thread_id,
            &message_id,
            kind,
            direction,
            crate::control::PeerDmDeliveryState::Delivered,
            a2a_protocol,
            None,
            payload,
            Some(established_at),
        )?;
    }
    Ok(())
}

fn consume_pending_network_commands(
    state_dir: &Path,
    owner: &str,
    dispatcher: &mut dyn crate::network_service::NetworkCommandDispatcher,
) -> Result<u64> {
    migrate_pending_network_commands_jsonl(state_dir)?;
    let store = crate::storage::local_control_store(state_dir)?;
    let scope_id = crate::storage::local_control_scope_id(state_dir);
    let now_ms = observed_at_ms();
    let claimed = store.claim_due_pending_network_commands(&scope_id, owner, now_ms, 30_000, 64)?;
    let mut processed = 0_u64;
    for claimed_row in claimed {
        if pending_command_attempts_exhausted(claimed_row.attempts) {
            store.fail_pending_network_command(
                &scope_id,
                &claimed_row.command_id,
                &claimed_row.lease_token,
                "maximum network command attempts reached",
                now_ms,
            )?;
            continue;
        }
        let disposition = dispatcher.dispatch(&claimed_row).unwrap_or_else(|error| {
            let exponent = claimed_row.attempts.saturating_sub(1);
            let delay = PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS
                .saturating_mul(2_i64.saturating_pow(exponent))
                .min(PENDING_NETWORK_COMMAND_MAX_RETRY_MS) as u64;
            crate::network_service::CommandDisposition::Retry {
                retry_at: now_ms.saturating_add(delay),
                error: format!("{error:#}"),
            }
        });
        match disposition {
            crate::network_service::CommandDisposition::Complete => {
                store.complete_pending_network_command(
                    &scope_id,
                    &claimed_row.command_id,
                    &claimed_row.lease_token,
                )?;
                processed = processed.saturating_add(1);
            }
            crate::network_service::CommandDisposition::AwaitRemoteAck { retry_at } => {
                store.await_pending_network_command_remote_ack(
                    &scope_id,
                    &claimed_row.command_id,
                    &claimed_row.lease_token,
                    retry_at,
                )?;
                processed = processed.saturating_add(1);
            }
            crate::network_service::CommandDisposition::Retry { retry_at, error } => {
                store.retry_pending_network_command(
                    &scope_id,
                    &claimed_row.command_id,
                    &claimed_row.lease_token,
                    retry_at,
                    &error,
                )?;
            }
        }
    }
    Ok(processed)
}

fn pending_command_attempts_exhausted(claimed_attempts: u32) -> bool {
    claimed_attempts > PENDING_NETWORK_COMMAND_MAX_ATTEMPTS
}

struct ClientServerCommandDispatcher<'a> {
    node: &'a mut Node,
    client: &'a wattswarm_network_client_server::ClientServerClient,
    session_token: &'a str,
    identity: &'a crate::crypto::NodeIdentity,
    state_dir: &'a Path,
    gateway_url: &'a str,
}

impl crate::network_service::NetworkCommandDispatcher for ClientServerCommandDispatcher<'_> {
    fn dispatch(
        &mut self,
        claimed_row: &crate::storage::ClaimedPendingNetworkCommandRow,
    ) -> Result<crate::network_service::CommandDisposition> {
        let command: PendingNetworkCommand = serde_json::from_str(&claimed_row.payload_json)
            .context("decode pending ClientServer command payload")?;
        let now_ms = observed_at_ms();
        match command {
            PendingNetworkCommand::PeerRelationship {
                remote_node_id,
                action,
                agent_envelope,
                ..
            } => {
                let contact_material =
                    super::client_server_network::build_client_server_contact_material(
                        self.state_dir,
                        self.identity,
                        self.gateway_url,
                    )?;
                let contact_request = RawContactMaterialRequest {
                    source_node_id: self.node.node_id(),
                    target_node_id: remote_node_id.clone(),
                };
                let mut contact_frame = wattswarm_network_client_server::ControlFrame {
                    framing_version: "1".to_owned(),
                    network_id: current_network_context_id(self.node),
                    correlation_id: format!("contact:{}", claimed_row.command_id),
                    source_principal_id: self.node.node_id(),
                    target_principal_id: remote_node_id.clone(),
                    control_kind:
                        wattswarm_network_client_server::ControlFrameKind::ContactMaterialRequest,
                    payload: wattswarm_network_transport_core::OpaqueSignedRecord::new(
                        serde_json::to_vec(&contact_request)?,
                    )?,
                    signature_hex: String::new(),
                };
                contact_frame.signature_hex = self.identity.sign_bytes(
                    &wattswarm_network_client_server::control_frame_signing_message(
                        &contact_frame,
                    )?,
                );
                self.client
                    .send_control(self.session_token, &contact_frame)?;
                let (relationship, _) = apply_peer_relationship_action_projection(
                    self.state_dir,
                    &remote_node_id,
                    action,
                    crate::control::PeerRelationshipInitiator::Local,
                    &agent_envelope,
                )?;
                if action == crate::control::PeerRelationshipAction::Accept
                    && relationship.relationship_state
                        == crate::control::PeerRelationshipState::Accepted
                {
                    finalize_client_server_dm_session(
                        self.node,
                        self.state_dir,
                        &remote_node_id,
                        crate::control::PeerDmDirection::Outbound,
                        &agent_envelope.protocol,
                        relationship.updated_at,
                    )?;
                }
                let payload = CsPeerRelationshipControlPayload {
                    action,
                    agent_envelope,
                    contact_material,
                };
                let mut frame = wattswarm_network_client_server::ControlFrame {
                    framing_version: "1".to_owned(),
                    network_id: current_network_context_id(self.node),
                    correlation_id: claimed_row.command_id.clone(),
                    source_principal_id: self.node.node_id(),
                    target_principal_id: remote_node_id,
                    control_kind:
                        wattswarm_network_client_server::ControlFrameKind::PeerRelationship,
                    payload: wattswarm_network_transport_core::OpaqueSignedRecord::new(
                        serde_json::to_vec(&payload)?,
                    )?,
                    signature_hex: String::new(),
                };
                frame.signature_hex = self.identity.sign_bytes(
                    &wattswarm_network_client_server::control_frame_signing_message(&frame)?,
                );
                self.client.send_control(self.session_token, &frame)?;
                Ok(crate::network_service::CommandDisposition::AwaitRemoteAck {
                    retry_at: now_ms
                        .saturating_add(PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS as u64),
                })
            }
            PendingNetworkCommand::AgentPayment {
                remote_node_id,
                message_kind,
                payment,
                agent_envelope,
                ..
            } => {
                self.node.emit_at(
                    0,
                    crate::types::EventPayload::AgentPaymentPosted(
                        crate::types::AgentPaymentPostedPayload {
                            network_id: current_network_context_id(self.node),
                            remote_node_id,
                            message_kind,
                            payment,
                            agent_envelope: raw_agent_envelope_to_protocol(&agent_envelope),
                        },
                    ),
                    now_ms,
                )?;
                Ok(crate::network_service::CommandDisposition::Complete)
            }
        }
    }
}

pub(super) fn process_pending_client_server_network_commands(
    node: &mut Node,
    client: &wattswarm_network_client_server::ClientServerClient,
    session_token: &str,
    identity: &crate::crypto::NodeIdentity,
    state_dir: &Path,
    gateway_url: &str,
) -> Result<u64> {
    let mut dispatcher = ClientServerCommandDispatcher {
        node,
        client,
        session_token,
        identity,
        state_dir,
        gateway_url,
    };
    consume_pending_network_commands(
        state_dir,
        &format!("client-server-command-dispatcher:{}", std::process::id()),
        &mut dispatcher,
    )
}

struct P2pNetworkCommandDispatcher<'a> {
    node: &'a mut Node,
    service: &'a mut NetworkBridgeService,
}

impl crate::network_service::NetworkCommandDispatcher for P2pNetworkCommandDispatcher<'_> {
    fn dispatch(
        &mut self,
        claimed_row: &crate::storage::ClaimedPendingNetworkCommandRow,
    ) -> Result<crate::network_service::CommandDisposition> {
        let command: PendingNetworkCommand = serde_json::from_str(&claimed_row.payload_json)
            .context("decode pending P2P command payload")?;
        let now_ms = observed_at_ms();
        match command {
            PendingNetworkCommand::PeerRelationship {
                remote_node_id,
                action,
                agent_envelope,
                ..
            } => {
                if self.service.release_stale_peer_relationship_action(
                    &remote_node_id,
                    action,
                    &agent_envelope,
                    now_ms as i64,
                ) {
                    bail!("peer relationship request timed out without runtime result")
                } else if self.service.has_pending_peer_relationship_action(
                    &remote_node_id,
                    action,
                    &agent_envelope,
                ) {
                    Ok(crate::network_service::CommandDisposition::AwaitRemoteAck {
                        retry_at: now_ms
                            .saturating_add(PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS as u64),
                    })
                } else {
                    self.service.send_peer_relationship_action(
                        &remote_node_id,
                        action,
                        Some(agent_envelope),
                    )?;
                    Ok(crate::network_service::CommandDisposition::AwaitRemoteAck {
                        retry_at: now_ms
                            .saturating_add(PENDING_NETWORK_COMMAND_INITIAL_RETRY_MS as u64),
                    })
                }
            }
            PendingNetworkCommand::AgentPayment {
                remote_node_id,
                message_kind,
                payment,
                agent_envelope,
                ..
            } => {
                let protocol_envelope = raw_agent_envelope_to_protocol(&agent_envelope);
                self.node.emit_at(
                    0,
                    crate::types::EventPayload::AgentPaymentPosted(
                        crate::types::AgentPaymentPostedPayload {
                            network_id: current_network_context_id(self.node),
                            remote_node_id: remote_node_id.clone(),
                            message_kind: message_kind.clone(),
                            payment: payment.clone(),
                            agent_envelope: protocol_envelope,
                        },
                    ),
                    now_ms,
                )?;
                let mut summary = build_agent_payment_summary(
                    &remote_node_id,
                    &message_kind,
                    payment,
                    agent_envelope,
                );
                summary.source_node_id = self.service.local_peer_id().to_string();
                let _ = self.service.publish_summary(summary);
                Ok(crate::network_service::CommandDisposition::Complete)
            }
        }
    }
}

pub(super) fn process_pending_network_commands(
    node: &mut Node,
    service: &mut NetworkBridgeService,
    state_dir: &Path,
) -> Result<u64> {
    let mut dispatcher = P2pNetworkCommandDispatcher { node, service };
    consume_pending_network_commands(
        state_dir,
        &format!("p2p-command-dispatcher:{}", std::process::id()),
        &mut dispatcher,
    )
}

fn migrate_pending_network_commands_jsonl(state_dir: &Path) -> Result<()> {
    let path = pending_network_commands_path(state_dir);
    if !path.exists() {
        return Ok(());
    }
    let content = fs::read_to_string(&path)?;
    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let command: PendingNetworkCommand =
            serde_json::from_str(line).context("decode legacy pending network command")?;
        enqueue_pending_network_command(state_dir, &command)?;
    }
    let backup = state_dir.join("pending_network_commands.jsonl.migrated-v1.bak");
    if backup.exists() {
        fs::remove_file(&path)?;
    } else {
        fs::rename(&path, &backup)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use serde::Serialize;
    use serde_json::json;

    #[test]
    fn pending_command_runs_the_configured_final_attempt() {
        assert!(!pending_command_attempts_exhausted(
            PENDING_NETWORK_COMMAND_MAX_ATTEMPTS
        ));
        assert!(pending_command_attempts_exhausted(
            PENDING_NETWORK_COMMAND_MAX_ATTEMPTS + 1
        ));
    }

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
            "protocolVersion": "1.0",
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
    fn agent_signature_ref_accepts_legacy_base64_public_key() {
        let identity = crate::crypto::NodeIdentity::random();
        let message = b"agent-envelope";
        let public_key =
            base64::engine::general_purpose::STANDARD.encode(identity.verifying_key().as_bytes());
        let signature = base64::engine::general_purpose::STANDARD.encode(
            SigningKey::from_bytes(&identity.secret_bytes())
                .sign(message)
                .to_bytes(),
        );

        verify_agent_signature_ref(&public_key, message, &signature)
            .expect("verify legacy Base64 public key");
    }

    #[test]
    fn agent_signature_ref_rejects_non_ed25519_did_key() {
        let identity = crate::crypto::NodeIdentity::random();
        let message = b"agent-envelope";
        let signature = base64::engine::general_purpose::STANDARD.encode(
            SigningKey::from_bytes(&identity.secret_bytes())
                .sign(message)
                .to_bytes(),
        );
        let mut multicodec = vec![0xec, 0x01];
        multicodec.extend_from_slice(&[7u8; 32]);
        let did_key = format!("did:key:z{}", bs58::encode(multicodec).into_string());

        let error = verify_agent_signature_ref(&did_key, message, &signature)
            .expect_err("X25519 did:key must not be accepted for signature verification");

        assert!(
            error
                .to_string()
                .contains("did:key is not an Ed25519 verification key")
        );
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
    fn relationship_agent_envelope_preserves_existing_visible_message_over_retry_metadata() {
        let existing = crate::control::AgentInteractionEnvelope {
            protocol: "google_a2a".to_owned(),
            capability: Some("social.friend.request".to_owned()),
            message: json!({
                "action": "request",
                "payload": "Hello from the original friend request",
                "request_id": "request-1"
            }),
            signature: Some("original-signature".to_owned()),
            ..Default::default()
        };
        let incoming = crate::control::AgentInteractionEnvelope {
            protocol: "google_a2a".to_owned(),
            capability: Some("social.friend.request".to_owned()),
            message: json!({
                "action": "request",
                "retry": true,
                "request_id": "request-1"
            }),
            signature: Some("retry-signature".to_owned()),
            ..Default::default()
        };

        let merged = relationship_agent_envelope_for_update(Some(&existing), incoming);

        assert_eq!(
            merged.message.get("payload").and_then(Value::as_str),
            Some("Hello from the original friend request")
        );
        assert_eq!(merged.signature.as_deref(), Some("original-signature"));
    }

    #[test]
    fn local_relationship_decision_preserves_remote_agent_card_on_node_record() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-remote-card-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&state_dir).expect("create state dir");
        let mut remote_request = signed_envelope_with_card("remote-node", "local-node");
        remote_request.capability = Some("social.friend.request".to_owned());
        remote_request.message_json = json!({
            "action": "request",
            "request_id": "request-remote-card"
        })
        .to_string();
        apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Remote,
            &remote_request,
        )
        .expect("apply remote request");
        let local_reject = default_agent_envelope(
            "local-node",
            "remote-node",
            "social.friend.reject",
            json!({
                "action": "reject",
                "request_id": "request-remote-card"
            }),
        );

        apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Reject,
            crate::control::PeerRelationshipInitiator::Local,
            &local_reject,
        )
        .expect("reject remote request");

        let relationship = crate::control::load_peer_relationship_records_state(&state_dir)
            .expect("load node relationships")
            .into_iter()
            .next()
            .expect("node relationship");
        let stored_envelope = relationship.agent_envelope.expect("remote agent envelope");
        assert_eq!(
            stored_envelope.source_node_id.as_deref(),
            Some("remote-node")
        );
        assert!(stored_envelope.source_agent_card.is_some());
        fs::remove_dir_all(state_dir).expect("remove state dir");
    }

    #[test]
    fn local_relationship_request_does_not_bind_local_card_to_remote_node() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-local-card-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&state_dir).expect("create state dir");
        let mut local_request = signed_envelope_with_card("local-node", "remote-node");
        local_request.capability = Some("social.friend.request".to_owned());
        local_request.message_json = json!({
            "action": "request",
            "request_id": "request-local-card"
        })
        .to_string();

        apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
            &local_request,
        )
        .expect("apply local request");

        let relationship = crate::control::load_peer_relationship_records_state(&state_dir)
            .expect("load node relationships")
            .into_iter()
            .next()
            .expect("node relationship");
        assert!(relationship.agent_envelope.is_none());
        let requests = crate::control::load_peer_relationship_request_records_state(&state_dir)
            .expect("load request-scoped relationships");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].agent_envelope.source_agent_card.is_some());
        fs::remove_dir_all(state_dir).expect("remove state dir");
    }

    #[test]
    fn accepted_node_allows_second_identity_request_and_rejects_stale_response() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-identity-requests-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&state_dir).expect("create state dir");
        let first_request = default_agent_envelope(
            "remote-node",
            "local-node",
            "social.friend.request",
            json!({"request_id": "request-first"}),
        );
        apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Remote,
            &first_request,
        )
        .expect("apply first identity request");
        let requested_node = crate::control::load_peer_relationship_records_state(&state_dir)
            .expect("load requested node relationship")
            .into_iter()
            .next()
            .expect("requested node relationship");
        assert_eq!(
            stored_relationship_request_id(&requested_node),
            Some("request-first")
        );
        let first_accept = default_agent_envelope(
            "local-node",
            "remote-node",
            "social.friend.accept",
            json!({"request_id": "request-first"}),
        );
        apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Accept,
            crate::control::PeerRelationshipInitiator::Local,
            &first_accept,
        )
        .expect("accept first identity request");
        let second_request = default_agent_envelope(
            "remote-node",
            "local-node",
            "social.friend.request",
            json!({"request_id": "request-second"}),
        );

        let (second_projection, replayed) = apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Remote,
            &second_request,
        )
        .expect("accepted node must allow a second identity request");

        assert!(!replayed);
        assert_eq!(
            second_projection.relationship_state,
            crate::control::PeerRelationshipState::Requested
        );
        let node_relationship = crate::control::load_peer_relationship_records_state(&state_dir)
            .expect("load node relationship")
            .into_iter()
            .next()
            .expect("node relationship");
        assert_eq!(
            node_relationship.relationship_state,
            crate::control::PeerRelationshipState::Accepted
        );
        assert_eq!(
            stored_relationship_request_id(&node_relationship),
            Some("request-first")
        );

        let stale_reject = default_agent_envelope(
            "local-node",
            "remote-node",
            "social.friend.reject",
            json!({"request_id": "request-first"}),
        );
        let error = apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Reject,
            crate::control::PeerRelationshipInitiator::Local,
            &stale_reject,
        )
        .expect_err("accepted request must reject a stale reject action");
        assert!(error.to_string().contains("from state=accepted"));
        let requests = crate::control::load_peer_relationship_request_records_state(&state_dir)
            .expect("load identity requests");
        assert!(requests.iter().any(|record| {
            record.request_id == "request-second"
                && record.relationship_state == crate::control::PeerRelationshipState::Requested
        }));
        fs::remove_dir_all(state_dir).expect("remove state dir");
    }

    #[test]
    fn legacy_accepted_relationship_replay_seeds_request_ledger_without_new_event() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-legacy-request-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&state_dir).expect("create state dir");
        let request = default_agent_envelope(
            "remote-node",
            "local-node",
            "social.friend.request",
            json!({"request_id": "legacy-request"}),
        );
        crate::control::apply_peer_relationship_action_state(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Remote,
        )
        .expect("seed legacy node request");
        attach_agent_envelope_to_relationship(&state_dir, "remote-node", &request)
            .expect("attach legacy request envelope");
        crate::control::apply_peer_relationship_action_state(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Accept,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("accept legacy node relationship");

        let (projection, replayed) = apply_peer_relationship_action_projection(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Remote,
            &request,
        )
        .expect("replay legacy request");

        assert!(replayed);
        assert_eq!(
            projection.relationship_state,
            crate::control::PeerRelationshipState::Accepted
        );
        let requests = crate::control::load_peer_relationship_request_records_state(&state_dir)
            .expect("load seeded request ledger");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].relationship_state,
            crate::control::PeerRelationshipState::Accepted
        );
        fs::remove_dir_all(state_dir).expect("remove state dir");
    }

    #[test]
    fn relationship_agent_envelope_uses_incoming_visible_message() {
        let existing = crate::control::AgentInteractionEnvelope {
            protocol: "google_a2a".to_owned(),
            capability: Some("social.friend.request".to_owned()),
            message: json!({
                "action": "request",
                "retry": true,
                "request_id": "request-1"
            }),
            signature: Some("retry-signature".to_owned()),
            ..Default::default()
        };
        let incoming = crate::control::AgentInteractionEnvelope {
            protocol: "google_a2a".to_owned(),
            capability: Some("social.friend.request".to_owned()),
            message: json!({
                "action": "request",
                "text": "Fresh request message",
                "request_id": "request-1"
            }),
            signature: Some("fresh-signature".to_owned()),
            ..Default::default()
        };

        let merged = relationship_agent_envelope_for_update(Some(&existing), incoming);

        assert_eq!(
            merged.message.get("text").and_then(Value::as_str),
            Some("Fresh request message")
        );
        assert_eq!(merged.signature.as_deref(), Some("fresh-signature"));
    }

    #[test]
    fn attach_agent_envelope_persists_relationship_message() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-envelope-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&state_dir).expect("create temp state dir");
        let remote_node_id = "remote-node";
        crate::control::apply_peer_relationship_action_state(
            &state_dir,
            remote_node_id,
            crate::control::PeerRelationshipAction::Request,
            crate::control::PeerRelationshipInitiator::Local,
        )
        .expect("create local relationship record");
        let envelope = default_agent_envelope(
            "local-node",
            remote_node_id,
            "social.friend.request",
            json!({
                "action": "request",
                "payload": "hello from original request",
                "request_id": "request-1"
            }),
        );

        attach_agent_envelope_to_relationship(&state_dir, remote_node_id, &envelope)
            .expect("attach agent envelope");

        let record = crate::control::load_peer_relationship_records_state(&state_dir)
            .expect("load relationship records")
            .into_iter()
            .find(|record| record.remote_node_id == remote_node_id)
            .expect("relationship record exists");
        assert_eq!(
            record.agent_envelope.and_then(|envelope| envelope
                .message
                .get("payload")
                .and_then(Value::as_str)
                .map(str::to_owned)),
            Some("hello from original request".to_owned())
        );
        let _ = std::fs::remove_dir_all(state_dir);
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

    #[test]
    fn peer_relationship_command_upserts_duplicate_request() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-command-upsert-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&state_dir).expect("create temp state dir");
        let envelope = default_agent_envelope(
            "local-node",
            "remote-node",
            "social.friend.request",
            json!({
                "action": "request",
                "correlation_id": "correlation-1",
                "request_id": "request-1"
            }),
        );

        enqueue_peer_relationship_action_command(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            envelope.clone(),
        )
        .expect("enqueue first command");
        enqueue_peer_relationship_action_command(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            envelope,
        )
        .expect("upsert duplicate command");

        let commands = load_pending_network_commands(&state_dir).expect("load commands");
        assert_eq!(commands.len(), 1);
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[test]
    fn peer_relationship_command_failure_updates_existing_retry() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-command-failure-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&state_dir).expect("create temp state dir");
        let envelope = default_agent_envelope(
            "local-node",
            "remote-node",
            "social.friend.request",
            json!({
                "action": "request",
                "correlation_id": "correlation-1",
                "request_id": "request-1"
            }),
        );
        enqueue_peer_relationship_action_command(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            envelope.clone(),
        )
        .expect("enqueue command");

        record_peer_relationship_action_command_failure(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            envelope,
            "control stream timed out",
        )
        .expect("record failure");

        let commands = load_pending_network_commands(&state_dir).expect("load commands");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].attempts(), 1);
        assert!(commands[0].next_retry_at().is_some());
        match &commands[0] {
            PendingNetworkCommand::PeerRelationship { last_error, .. } => {
                assert_eq!(last_error.as_deref(), Some("control stream timed out"));
            }
            PendingNetworkCommand::AgentPayment { .. } => panic!("expected peer relationship"),
        }
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[test]
    fn peer_relationship_command_removed_after_matching_ack() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-peer-command-remove-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&state_dir).expect("create temp state dir");
        let envelope = default_agent_envelope(
            "local-node",
            "remote-node",
            "social.friend.request",
            json!({
                "action": "request",
                "correlation_id": "correlation-1",
                "request_id": "request-1"
            }),
        );
        enqueue_peer_relationship_action_command(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            envelope.clone(),
        )
        .expect("enqueue command");

        remove_peer_relationship_action_command(
            &state_dir,
            "remote-node",
            crate::control::PeerRelationshipAction::Request,
            &envelope,
        )
        .expect("remove command");

        let commands = load_pending_network_commands(&state_dir).expect("load commands");
        assert!(commands.is_empty());
        let _ = std::fs::remove_dir_all(state_dir);
    }
}
