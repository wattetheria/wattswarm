use wattswarm::constants::INLINE_MIME_ALLOWLIST;
use wattswarm::task_template::{sample_artifact_ref, sample_contract};
use wattswarm::types::TaskMode;

#[test]
fn sample_contract_sets_expected_core_fields() {
    let contract = sample_contract("task-xyz", "policy-hash-1".to_owned());
    assert_eq!(contract.task_id, "task-xyz");
    assert_eq!(contract.protocol_version, "v0.1");
    assert_eq!(
        contract.acceptance.verifier_policy.policy_hash,
        "policy-hash-1"
    );
    assert!(matches!(contract.task_mode, TaskMode::OneShot));
}

#[test]
fn sample_contract_uses_expected_inline_mime_allowlist() {
    let contract = sample_contract("task-xyz", "policy-hash-1".to_owned());
    for mime in INLINE_MIME_ALLOWLIST {
        assert!(
            contract
                .evidence_policy
                .inline_mime_allowlist
                .iter()
                .any(|v| v == mime),
            "missing mime allowlist value: {mime}"
        );
    }
}

#[test]
fn sample_artifact_ref_has_required_fields() {
    let artifact = sample_artifact_ref();
    assert!(artifact.uri.starts_with("https://"));
    assert!(artifact.digest.starts_with("sha256:"));
    assert!(artifact.size_bytes > 0);
    assert!(!artifact.mime.is_empty());
    assert!(!artifact.producer.is_empty());
}
