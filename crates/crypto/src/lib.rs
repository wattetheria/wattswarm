pub mod types {
    pub use wattswarm_protocol::types::*;
}

use crate::types::{Candidate, Event, UnsignedEvent, VoteChoice};
use anyhow::{Context, Result};
use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

const DID_KEY_PREFIX: &str = "did:key:";
const ED25519_MULTICODEC_PREFIX: [u8; 2] = [0xed, 0x01];
const PRIVATE_DM_SCHEME: &str = "wattswarm.private.dm.v1";
const PRIVATE_GROUP_SCHEME: &str = "wattswarm.private.group.gss.v1";
const PRIVATE_DM_HKDF_SALT: &[u8] = b"wattswarm-private-dm-v1";
const PRIVATE_GROUP_HKDF_SALT: &[u8] = b"wattswarm-private-group-gss-v1";

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
            swarm_scope: unsigned.swarm_scope.clone(),
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
        swarm_scope: event.swarm_scope.clone(),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateMessageKeypair {
    pub secret_key_b64: String,
    pub public_key_b64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateEncryptedPayload {
    pub scheme: String,
    pub key_agreement: String,
    pub cipher: String,
    pub sender_public_key_b64: String,
    pub recipient_public_key_b64: String,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateGroupEncryptedPayload {
    pub scheme: String,
    pub cipher: String,
    pub group_id: String,
    pub epoch: u64,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

pub fn generate_private_message_keypair() -> PrivateMessageKeypair {
    let secret = StaticSecret::random_from_rng(OsRng);
    let public = X25519PublicKey::from(&secret);
    PrivateMessageKeypair {
        secret_key_b64: base64::engine::general_purpose::STANDARD.encode(secret.to_bytes()),
        public_key_b64: base64::engine::general_purpose::STANDARD.encode(public.as_bytes()),
    }
}

pub fn private_message_public_key_from_secret(secret_key_b64: &str) -> Result<String> {
    let secret = decode_x25519_secret(secret_key_b64)?;
    let public = X25519PublicKey::from(&secret);
    Ok(base64::engine::general_purpose::STANDARD.encode(public.as_bytes()))
}

pub fn encrypt_private_message(
    sender_secret_key_b64: &str,
    recipient_public_key_b64: &str,
    plaintext: &[u8],
    aad: &[u8],
) -> Result<PrivateEncryptedPayload> {
    let sender_secret = decode_x25519_secret(sender_secret_key_b64)?;
    let sender_public = X25519PublicKey::from(&sender_secret);
    let recipient_public = decode_x25519_public(recipient_public_key_b64)?;
    let key = derive_private_dm_key(&sender_secret, &recipient_public)?;
    let ciphertext = encrypt_aead(&key, plaintext, aad)?;
    Ok(PrivateEncryptedPayload {
        scheme: PRIVATE_DM_SCHEME.to_owned(),
        key_agreement: "x25519".to_owned(),
        cipher: "chacha20poly1305".to_owned(),
        sender_public_key_b64: base64::engine::general_purpose::STANDARD
            .encode(sender_public.as_bytes()),
        recipient_public_key_b64: recipient_public_key_b64.to_owned(),
        nonce_b64: ciphertext.0,
        ciphertext_b64: ciphertext.1,
    })
}

pub fn decrypt_private_message(
    recipient_secret_key_b64: &str,
    payload: &PrivateEncryptedPayload,
    aad: &[u8],
) -> Result<Vec<u8>> {
    if payload.scheme != PRIVATE_DM_SCHEME {
        anyhow::bail!("unsupported private message scheme");
    }
    let recipient_secret = decode_x25519_secret(recipient_secret_key_b64)?;
    let sender_public = decode_x25519_public(&payload.sender_public_key_b64)?;
    let key = derive_private_dm_key(&recipient_secret, &sender_public)?;
    decrypt_aead(&key, &payload.nonce_b64, &payload.ciphertext_b64, aad)
}

pub fn generate_private_group_secret_b64() -> String {
    let mut secret = [0_u8; 32];
    OsRng.fill_bytes(&mut secret);
    base64::engine::general_purpose::STANDARD.encode(secret)
}

pub fn encrypt_private_group_content(
    shared_secret_b64: &str,
    group_id: &str,
    epoch: u64,
    plaintext: &[u8],
    aad: &[u8],
) -> Result<PrivateGroupEncryptedPayload> {
    let key = derive_private_group_key(shared_secret_b64, group_id, epoch)?;
    let ciphertext = encrypt_aead(&key, plaintext, aad)?;
    Ok(PrivateGroupEncryptedPayload {
        scheme: PRIVATE_GROUP_SCHEME.to_owned(),
        cipher: "chacha20poly1305".to_owned(),
        group_id: group_id.to_owned(),
        epoch,
        nonce_b64: ciphertext.0,
        ciphertext_b64: ciphertext.1,
    })
}

pub fn decrypt_private_group_content(
    shared_secret_b64: &str,
    payload: &PrivateGroupEncryptedPayload,
    aad: &[u8],
) -> Result<Vec<u8>> {
    if payload.scheme != PRIVATE_GROUP_SCHEME {
        anyhow::bail!("unsupported private group scheme");
    }
    let key = derive_private_group_key(shared_secret_b64, &payload.group_id, payload.epoch)?;
    decrypt_aead(&key, &payload.nonce_b64, &payload.ciphertext_b64, aad)
}

fn decode_x25519_secret(secret_key_b64: &str) -> Result<StaticSecret> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(secret_key_b64)
        .context("decode x25519 secret key")?;
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid x25519 secret key length"))?;
    Ok(StaticSecret::from(key))
}

fn decode_x25519_public(public_key_b64: &str) -> Result<X25519PublicKey> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(public_key_b64)
        .context("decode x25519 public key")?;
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid x25519 public key length"))?;
    Ok(X25519PublicKey::from(key))
}

fn derive_private_dm_key(secret: &StaticSecret, public: &X25519PublicKey) -> Result<[u8; 32]> {
    let shared = secret.diffie_hellman(public);
    let hk = Hkdf::<Sha256>::new(Some(PRIVATE_DM_HKDF_SALT), shared.as_bytes());
    let mut key = [0_u8; 32];
    hk.expand(b"dm-content-key", &mut key)
        .map_err(|_| anyhow::anyhow!("derive private dm key"))?;
    Ok(key)
}

fn derive_private_group_key(
    shared_secret_b64: &str,
    group_id: &str,
    epoch: u64,
) -> Result<[u8; 32]> {
    let secret = base64::engine::general_purpose::STANDARD
        .decode(shared_secret_b64)
        .context("decode private group secret")?;
    if secret.len() != 32 {
        anyhow::bail!("private group secret must be 32 bytes");
    }
    let mut salt = Vec::with_capacity(PRIVATE_GROUP_HKDF_SALT.len() + group_id.len() + 8);
    salt.extend_from_slice(PRIVATE_GROUP_HKDF_SALT);
    salt.extend_from_slice(group_id.as_bytes());
    salt.extend_from_slice(&epoch.to_be_bytes());
    let hk = Hkdf::<Sha256>::new(Some(&salt), &secret);
    let mut key = [0_u8; 32];
    hk.expand(b"group-content-key", &mut key)
        .map_err(|_| anyhow::anyhow!("derive private group key"))?;
    Ok(key)
}

fn encrypt_aead(key: &[u8; 32], plaintext: &[u8], aad: &[u8]) -> Result<(String, String)> {
    let cipher = ChaCha20Poly1305::new_from_slice(key)
        .map_err(|err| anyhow::anyhow!("private cipher init: {err}"))?;
    let mut nonce = [0_u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|err| anyhow::anyhow!("private encrypt: {err}"))?;
    Ok((
        base64::engine::general_purpose::STANDARD.encode(nonce),
        base64::engine::general_purpose::STANDARD.encode(ciphertext),
    ))
}

fn decrypt_aead(
    key: &[u8; 32],
    nonce_b64: &str,
    ciphertext_b64: &str,
    aad: &[u8],
) -> Result<Vec<u8>> {
    let nonce = base64::engine::general_purpose::STANDARD
        .decode(nonce_b64)
        .context("decode private nonce")?;
    if nonce.len() != 12 {
        anyhow::bail!("private nonce must be 12 bytes");
    }
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(ciphertext_b64)
        .context("decode private ciphertext")?;
    let cipher = ChaCha20Poly1305::new_from_slice(key)
        .map_err(|err| anyhow::anyhow!("private cipher init: {err}"))?;
    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad,
            },
        )
        .map_err(|err| anyhow::anyhow!("private decrypt: {err}"))
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

    #[test]
    fn private_message_encryption_round_trips_with_aad() {
        let sender = generate_private_message_keypair();
        let recipient = generate_private_message_keypair();
        let aad = b"dm:sender:recipient:thread:message";
        let payload = encrypt_private_message(
            &sender.secret_key_b64,
            &recipient.public_key_b64,
            br#"{"content":{"text":"hello"}}"#,
            aad,
        )
        .expect("encrypt private dm");

        assert_eq!(payload.scheme, PRIVATE_DM_SCHEME);
        assert_eq!(payload.recipient_public_key_b64, recipient.public_key_b64);
        assert_ne!(
            payload.ciphertext_b64,
            base64::engine::general_purpose::STANDARD.encode("hello")
        );

        let plaintext =
            decrypt_private_message(&recipient.secret_key_b64, &payload, aad).expect("decrypt dm");
        assert_eq!(plaintext, br#"{"content":{"text":"hello"}}"#);
        assert!(
            decrypt_private_message(&recipient.secret_key_b64, &payload, b"wrong-aad").is_err()
        );
    }

    #[test]
    fn private_message_decryption_rejects_wrong_recipient_key() {
        let sender = generate_private_message_keypair();
        let recipient = generate_private_message_keypair();
        let other = generate_private_message_keypair();
        let payload = encrypt_private_message(
            &sender.secret_key_b64,
            &recipient.public_key_b64,
            b"private",
            b"aad",
        )
        .expect("encrypt private dm");

        assert!(decrypt_private_message(&other.secret_key_b64, &payload, b"aad").is_err());
    }

    #[test]
    fn private_group_encryption_round_trips_by_epoch() {
        let secret = generate_private_group_secret_b64();
        let payload = encrypt_private_group_content(
            &secret,
            "group:dm-private",
            3,
            br#"{"body":"private hive"}"#,
            b"feed:scope:message",
        )
        .expect("encrypt private group");

        assert_eq!(payload.scheme, PRIVATE_GROUP_SCHEME);
        assert_eq!(payload.group_id, "group:dm-private");
        assert_eq!(payload.epoch, 3);
        let plaintext = decrypt_private_group_content(&secret, &payload, b"feed:scope:message")
            .expect("decrypt private group");
        assert_eq!(plaintext, br#"{"body":"private hive"}"#);
        assert!(decrypt_private_group_content(&secret, &payload, b"wrong").is_err());

        let wrong_secret = generate_private_group_secret_b64();
        assert!(
            decrypt_private_group_content(&wrong_secret, &payload, b"feed:scope:message").is_err()
        );
    }
}
