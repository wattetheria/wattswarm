use crate::constants::{
    BACKFILL_BATCH_EVENTS, CLOCK_SKEW_TOLERANCE_MS, INLINE_MIME_ALLOWLIST, LOCAL_PROTOCOL_VERSION,
    MAX_EVENT_BYTES, MAX_EVENT_PAYLOAD_BYTES, MAX_INLINE_EVIDENCE_BYTES, MAX_INLINE_MEDIA_BYTES,
    MAX_STRUCTURED_SUMMARY_BYTES, REPUTATION_SCALE,
};
use crate::crypto::{
    NodeIdentity, candidate_hash, event_digest, sha256_hex, verify_event_signature,
    verify_signature, vote_commit_hash,
};
use crate::error::SwarmError;
use crate::policy::{PolicyRegistry, validate_schema_minimal};
use crate::reason_codes::{
    REASON_CUSTOM_ERROR, REASON_EVIDENCE_AUTH_DENIED, REASON_EVIDENCE_TIMEOUT,
    REASON_EVIDENCE_UNREACHABLE, REASON_INVALID_OUTPUT, REASON_RUNTIME_CRASH, REASON_SCHEMA_OK,
    REASON_TASK_EXPIRED, REASON_TASK_TIMEOUT, REASON_UNKNOWN, is_protocol_reason_code,
};
use crate::runtime::{ExecuteRequest, RuntimeClient, VerifyRequest, verifier_result_from_response};
use crate::storage::pg::ErrorCode;
use crate::storage::{PgStore, RuntimeMetricObservation, TaskProjectionRow, VoteRevealRow};
use crate::types::{
    Candidate, CandidateProposedPayload, ClaimPayload, ClaimReleasePayload, ClaimRenewPayload,
    ClaimRole, DecisionCommittedPayload, DecisionFinalizedPayload, EpochEndReason, Event,
    EventPayload, EvidenceAddedPayload, EvidenceAvailablePayload, KnowledgeHit, KnowledgeHitType,
    Membership, MembershipUpdatedPayload, Role, SeedBundle, SeedConstraints, SignatureEnvelope,
    TaskContract, TaskErrorPayload, TaskRetryScheduledPayload, TaskTerminalState, UnsignedEvent,
    VerificationStatus, VerifierResult, VerifierResultSubmittedPayload, VoteChoice,
    VoteCommitPayload, VoteRevealPayload,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use chrono::Utc;
use serde_json::Value;
use std::collections::HashSet;

pub struct Node {
    pub identity: NodeIdentity,
    pub store: PgStore,
    policy_registry: PolicyRegistry,
    peers: HashSet<String>,
    genesis_membership: Membership,
}

mod core;
mod knowledge;
mod projection;
mod validation_core;
mod validation_decide;

fn claim_role_str(role: ClaimRole) -> &'static str {
    match role {
        ClaimRole::Propose => "propose",
        ClaimRole::Verify => "verify",
    }
}

fn claim_role_to_permission(role: ClaimRole) -> Role {
    match role {
        ClaimRole::Propose => Role::Proposer,
        ClaimRole::Verify => Role::Verifier,
    }
}

pub fn finality_message(task_id: &str, epoch: u64, candidate_id: &str) -> String {
    format!("{task_id}:{epoch}:{candidate_id}")
}

pub fn finality_sign(
    identity: &NodeIdentity,
    task_id: &str,
    epoch: u64,
    candidate_id: &str,
) -> SignatureEnvelope {
    let msg = finality_message(task_id, epoch, candidate_id);
    SignatureEnvelope {
        signer_node_id: identity.node_id(),
        signature_hex: identity.sign_bytes(msg.as_bytes()),
    }
}

pub fn membership_update_message(payload: &Membership) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(payload)?)
}

pub fn feedback_message(
    task_id: &str,
    epoch: u64,
    outcome: &str,
    reason: &str,
    timestamp: u64,
) -> String {
    format!("{task_id}|{epoch}|{outcome}|{reason}|{timestamp}")
}

pub fn sign_membership_quorum(
    identity: &NodeIdentity,
    new_membership: &Membership,
) -> Result<SignatureEnvelope> {
    let msg = membership_update_message(new_membership)?;
    Ok(SignatureEnvelope {
        signer_node_id: identity.node_id(),
        signature_hex: identity.sign_bytes(&msg),
    })
}

pub fn build_event_for_external(
    identity: &NodeIdentity,
    epoch: u64,
    created_at: u64,
    payload: EventPayload,
) -> Result<Event> {
    let unsigned = UnsignedEvent::from_payload(
        LOCAL_PROTOCOL_VERSION.to_owned(),
        identity.node_id(),
        epoch,
        created_at,
        payload,
    );
    identity.sign_unsigned_event(&unsigned).and_then(|event| {
        let digest = event_digest(&unsigned)?;
        if digest != event.event_id {
            return Err(anyhow!("event digest mismatch"));
        }
        Ok(event)
    })
}

fn is_duplicate_event_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<crate::storage::pg::Error>()
            .map(|e| match e {
                crate::storage::pg::Error::DbFailure(inner, message) => {
                    inner.code == ErrorCode::ConstraintViolation
                        && message
                            .as_deref()
                            .map(|m| m.contains("events.event_id"))
                            .unwrap_or(false)
                }
                _ => false,
            })
            .unwrap_or(false)
    })
}

fn is_timeout_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .map(|e| e.is_timeout())
            .unwrap_or(false)
            || cause.to_string().to_lowercase().contains("timed out")
    })
}

fn inline_contains_forbidden_media(content: &str) -> bool {
    if contains_media_data_uri(content) {
        return true;
    }

    extract_base64_tokens(content)
        .into_iter()
        .any(|token| base64_token_decodes_to_media(&token))
}

fn contains_media_data_uri(content: &str) -> bool {
    let lower = content.to_ascii_lowercase();
    lower.contains("data:image/") || lower.contains("data:video/") || lower.contains("data:audio/")
}

fn extract_base64_tokens(content: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in content.chars() {
        if matches!(ch, 'A'..='Z' | 'a'..='z' | '0'..='9' | '+' | '/' | '-' | '_' | '=') {
            current.push(ch);
            continue;
        }
        if current.len() >= 64 {
            tokens.push(current.clone());
        }
        current.clear();
    }
    if current.len() >= 64 {
        tokens.push(current);
    }
    tokens
}

fn base64_token_decodes_to_media(token: &str) -> bool {
    let engines = [
        base64::engine::general_purpose::STANDARD,
        base64::engine::general_purpose::STANDARD_NO_PAD,
        base64::engine::general_purpose::URL_SAFE,
        base64::engine::general_purpose::URL_SAFE_NO_PAD,
    ];
    engines.iter().any(|engine| {
        engine
            .decode(token)
            .map(|decoded| decoded_looks_like_media(&decoded))
            .unwrap_or(false)
    })
}

fn decoded_looks_like_media(bytes: &[u8]) -> bool {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return true;
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return true;
    }
    if bytes.starts_with(b"GIF87a")
        || bytes.starts_with(b"GIF89a")
        || bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3])
        || bytes.starts_with(b"OggS")
        || bytes.starts_with(b"fLaC")
        || bytes.starts_with(b"ID3")
    {
        return true;
    }
    if bytes.len() > 12 && bytes.starts_with(b"RIFF") {
        return &bytes[8..12] == b"WAVE" || &bytes[8..12] == b"WEBP" || &bytes[8..12] == b"AVI ";
    }
    if bytes.len() > 8 && &bytes[4..8] == b"ftyp" {
        return true;
    }
    false
}

fn is_deadline_expired(deadline: u64, now: u64) -> bool {
    deadline.saturating_add(CLOCK_SKEW_TOLERANCE_MS) < now
}

fn stage_cost_for_payload(payload: &EventPayload) -> Option<(&'static str, u64)> {
    match payload {
        EventPayload::CandidateProposed(_) | EventPayload::EvidenceAdded(_) => Some(("explore", 1)),
        EventPayload::EvidenceAvailable(_)
        | EventPayload::VerifierResultSubmitted(_)
        | EventPayload::VoteCommit(_)
        | EventPayload::VoteReveal(_) => Some(("verify", 1)),
        EventPayload::DecisionCommitted(_)
        | EventPayload::DecisionFinalized(_)
        | EventPayload::TaskExpired(_) => Some(("finalize", 1)),
        _ => None,
    }
}

fn daily_window(ts_ms: u64) -> (u64, u64) {
    let day_ms = 86_400_000_u64;
    let start = (ts_ms / day_ms) * day_ms;
    (start, start.saturating_add(day_ms))
}

fn task_error_reason_codes(reason: crate::types::TaskErrorReason) -> Vec<u16> {
    match reason {
        crate::types::TaskErrorReason::Timeout => vec![REASON_TASK_TIMEOUT],
        crate::types::TaskErrorReason::InvalidOutput => vec![REASON_INVALID_OUTPUT],
        crate::types::TaskErrorReason::Other => vec![REASON_UNKNOWN],
    }
}

pub fn implicit_stability_delta_units(
    n_no_bad: u32,
    implicit_weight: f64,
    w: u32,
    k: u32,
) -> Result<i64> {
    if k == 0 {
        return Err(anyhow!("K must be >= 1"));
    }
    if !(0.0..=1.0).contains(&implicit_weight) {
        return Err(anyhow!("implicit_weight must be within [0,1]"));
    }
    let weight_units = ((implicit_weight * REPUTATION_SCALE as f64) + 0.5).floor() as i64;
    let n = i64::from(n_no_bad);
    let k_i64 = i64::from(k);
    let f_units = if n == 0 {
        0
    } else {
        (n * REPUTATION_SCALE) / (n + k_i64)
    };
    let w_units = i64::from(w) * REPUTATION_SCALE;
    let delta_units = ((weight_units * w_units) / REPUTATION_SCALE * f_units) / REPUTATION_SCALE;
    Ok(delta_units)
}
