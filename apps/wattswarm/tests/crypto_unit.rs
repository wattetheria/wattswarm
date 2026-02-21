use serde_json::json;
use wattswarm::crypto::{
    NodeIdentity, candidate_hash, event_digest, sha256_hex, verify_event_signature,
    verify_signature, vote_commit_hash,
};
use wattswarm::types::{Candidate, EventPayload, TaskExpiredPayload, UnsignedEvent, VoteChoice};

fn sample_candidate() -> Candidate {
    Candidate {
        candidate_id: "cand-1".to_owned(),
        execution_id: "exec-1".to_owned(),
        output: json!({"answer":"ok","confidence":0.8}),
        evidence_inline: vec![],
        evidence_refs: vec![],
    }
}

#[test]
fn sha256_hex_is_deterministic() {
    let left = sha256_hex(b"wattswarm");
    let right = sha256_hex(b"wattswarm");
    assert_eq!(left, right);
    assert_eq!(left.len(), 64);
}

#[test]
fn vote_commit_hash_changes_with_vote_and_salt() {
    let approve = vote_commit_hash(VoteChoice::Approve, "salt-a", "vrh");
    let reject = vote_commit_hash(VoteChoice::Reject, "salt-a", "vrh");
    let approve_other_salt = vote_commit_hash(VoteChoice::Approve, "salt-b", "vrh");
    assert_ne!(approve, reject);
    assert_ne!(approve, approve_other_salt);
}

#[test]
fn candidate_hash_is_stable_for_same_candidate() {
    let c = sample_candidate();
    let h1 = candidate_hash(&c).expect("hash");
    let h2 = candidate_hash(&c).expect("hash");
    assert_eq!(h1, h2);
}

#[test]
fn signature_roundtrip_for_message() {
    let id = NodeIdentity::from_seed([7_u8; 32]);
    let msg = b"hello-sign";
    let sig = id.sign_bytes(msg);
    assert!(verify_signature(&id.node_id(), msg, &sig).is_ok());
    assert!(verify_signature(&id.node_id(), b"tampered", &sig).is_err());
}

#[test]
fn sign_and_verify_unsigned_event() {
    let id = NodeIdentity::from_seed([9_u8; 32]);
    let payload = EventPayload::TaskExpired(TaskExpiredPayload {
        task_id: "task-1".to_owned(),
    });
    let unsigned = UnsignedEvent::from_payload("0.1.0".to_owned(), id.node_id(), 1, 12345, payload);
    let event = id.sign_unsigned_event(&unsigned).expect("signed event");
    assert!(verify_event_signature(&event).is_ok());
}

#[test]
fn verify_event_signature_rejects_tampered_event_id() {
    let id = NodeIdentity::from_seed([11_u8; 32]);
    let payload = EventPayload::TaskExpired(TaskExpiredPayload {
        task_id: "task-2".to_owned(),
    });
    let unsigned = UnsignedEvent::from_payload("0.1.0".to_owned(), id.node_id(), 1, 12345, payload);
    let mut event = id.sign_unsigned_event(&unsigned).expect("signed event");
    let original = event.event_id.clone();
    event.event_id = format!("{}00", original);
    assert!(verify_event_signature(&event).is_err());

    let digest = event_digest(&unsigned).expect("digest");
    assert_eq!(digest, original);
}
