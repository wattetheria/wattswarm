pub mod types {
    pub use wattswarm_protocol::types::*;
}

use crate::types::{Candidate, Event, UnsignedEvent, VoteChoice};
use anyhow::{Context, Result};
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};

const DID_KEY_PREFIX: &str = "did:key:";
const ED25519_MULTICODEC_PREFIX: [u8; 2] = [0xed, 0x01];

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
    let payload = candidate.control_bytes()?;
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

    pub fn secret_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
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

pub fn verify_signature_ref(
    public_key_ref: &str,
    message: &[u8],
    signature_b64: &str,
) -> Result<()> {
    let key_bytes = decode_public_key_from_ref(public_key_ref)?;
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(signature_b64)
        .context("decode signature")?;
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

fn decode_public_key_from_ref(public_key_ref: &str) -> Result<Vec<u8>> {
    if let Some(encoded) = public_key_ref.strip_prefix(DID_KEY_PREFIX) {
        let encoded = encoded
            .strip_prefix('z')
            .ok_or_else(|| anyhow::anyhow!("unsupported did:key multibase"))?;
        let decoded = bs58::decode(encoded).into_vec().context("decode did:key")?;
        if decoded.len() < ED25519_MULTICODEC_PREFIX.len() + 32 {
            anyhow::bail!("did:key is too short");
        }
        if decoded[..2] != ED25519_MULTICODEC_PREFIX {
            anyhow::bail!("did:key is not ed25519");
        }
        return Ok(decoded[2..].to_vec());
    }
    base64::engine::general_purpose::STANDARD
        .decode(public_key_ref)
        .context("decode base64 public key")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_signature_ref_accepts_base64_and_did_key() {
        let identity = NodeIdentity::random();
        let message = b"agent-envelope";
        let signature = base64::engine::general_purpose::STANDARD
            .encode(identity.signing_key.sign(message).to_bytes());
        let public_key_b64 =
            base64::engine::general_purpose::STANDARD.encode(identity.verifying_key().as_bytes());
        verify_signature_ref(&public_key_b64, message, &signature).expect("verify b64");

        let mut multicodec = Vec::with_capacity(2 + 32);
        multicodec.extend_from_slice(&ED25519_MULTICODEC_PREFIX);
        multicodec.extend_from_slice(identity.verifying_key().as_bytes());
        let did_key = format!("did:key:z{}", bs58::encode(multicodec).into_string());
        verify_signature_ref(&did_key, message, &signature).expect("verify did:key");
    }
}
