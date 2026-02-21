pub mod types {
    pub use wattswarm_protocol::types::*;
}

use crate::types::{Candidate, Event, UnsignedEvent, VoteChoice};
use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};

pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub fn event_digest(unsigned: &UnsignedEvent) -> Result<String> {
    let bytes = serde_json::to_vec(unsigned)?;
    Ok(sha256_hex(&bytes))
}

pub fn vote_commit_hash(vote: VoteChoice, salt: &str, verifier_result_hash: &str) -> String {
    let vote_str = match vote {
        VoteChoice::Approve => "approve",
        VoteChoice::Reject => "reject",
    };
    let payload = format!("{vote_str}|{salt}|{verifier_result_hash}");
    sha256_hex(payload.as_bytes())
}

pub fn candidate_hash(candidate: &Candidate) -> Result<String> {
    let payload = serde_json::to_vec(candidate)?;
    Ok(sha256_hex(&payload))
}

#[derive(Clone)]
pub struct NodeIdentity {
    signing_key: SigningKey,
}

impl NodeIdentity {
    pub fn random() -> Self {
        let mut rng = OsRng;
        Self {
            signing_key: SigningKey::generate(&mut rng),
        }
    }

    pub fn from_seed(seed_32: [u8; 32]) -> Self {
        Self {
            signing_key: SigningKey::from_bytes(&seed_32),
        }
    }

    pub fn node_id(&self) -> String {
        hex::encode(self.signing_key.verifying_key().as_bytes())
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    pub fn sign_bytes(&self, bytes: &[u8]) -> String {
        let sig = self.signing_key.sign(bytes);
        hex::encode(sig.to_bytes())
    }

    pub fn sign_unsigned_event(&self, unsigned: &UnsignedEvent) -> Result<Event> {
        let digest = event_digest(unsigned)?;
        let signature_hex = self.sign_bytes(digest.as_bytes());
        Ok(Event {
            event_id: digest,
            protocol_version: unsigned.protocol_version.clone(),
            event_kind: unsigned.event_kind.clone(),
            task_id: unsigned.task_id.clone(),
            epoch: unsigned.epoch,
            author_node_id: unsigned.author_node_id.clone(),
            created_at: unsigned.created_at,
            payload: unsigned.payload.clone(),
            signature_hex,
        })
    }
}

pub fn verify_signature(public_key_hex: &str, message: &[u8], signature_hex: &str) -> Result<()> {
    let key_bytes = hex::decode(public_key_hex).context("decode pubkey")?;
    let sig_bytes = hex::decode(signature_hex).context("decode signature")?;
    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid pubkey length"))?;
    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid signature length"))?;

    let key = VerifyingKey::from_bytes(&key_array)?;
    let signature = Signature::from_bytes(&sig_array);
    key.verify(message, &signature)?;
    Ok(())
}

pub fn verify_event_signature(event: &Event) -> Result<()> {
    let unsigned = UnsignedEvent {
        protocol_version: event.protocol_version.clone(),
        event_kind: event.event_kind.clone(),
        task_id: event.task_id.clone(),
        epoch: event.epoch,
        author_node_id: event.author_node_id.clone(),
        created_at: event.created_at,
        payload: event.payload.clone(),
    };
    let digest = event_digest(&unsigned)?;
    if digest != event.event_id {
        anyhow::bail!("event digest mismatch");
    }
    verify_signature(
        &event.author_node_id,
        digest.as_bytes(),
        &event.signature_hex,
    )
}
