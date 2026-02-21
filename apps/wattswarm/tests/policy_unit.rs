use serde_json::{Value, json};
use wattswarm::policy::{
    CrossCheckV1, Policy, PolicyRegistry, SchemaOnlyV1, SchemaThresholdsV1, validate_schema_minimal,
};
use wattswarm::reason_codes::{
    REASON_CHECK_SUMMARY_MISSING, REASON_CONFIDENCE_TOO_LOW, REASON_EVIDENCE_AUTH_DENIED,
    REASON_EVIDENCE_DIGEST_MISMATCH, REASON_EVIDENCE_TIMEOUT, REASON_EVIDENCE_UNREACHABLE,
    REASON_SCHEMA_INVALID, REASON_SCORE_TOO_LOW,
};
use wattswarm::types::Candidate;

fn candidate(output: Value) -> Candidate {
    Candidate {
        candidate_id: "c1".to_owned(),
        execution_id: "e1".to_owned(),
        output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    }
}

#[test]
fn schema_validator_checks_required_and_types() {
    let schema = json!({
        "type":"object",
        "required":["answer"],
        "properties":{"answer":{"type":"string"}}
    });
    assert!(validate_schema_minimal(&schema, &json!({"answer":"ok"})).is_ok());
    assert!(validate_schema_minimal(&schema, &json!({})).is_err());
    assert!(validate_schema_minimal(&schema, &json!({"answer": 1})).is_err());
}

#[test]
fn schema_only_fails_on_schema_mismatch() {
    let policy = SchemaOnlyV1;
    let schema =
        json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}});
    let result = policy.evaluate(&candidate(json!({"answer": 1})), &schema, &json!({}));
    assert!(!result.passed);
    assert!(result.reason_codes.contains(&REASON_SCHEMA_INVALID));
}

#[test]
fn schema_thresholds_respects_confidence() {
    let policy = SchemaThresholdsV1;
    let schema = json!({"type":"object","properties":{"confidence":{"type":"number"}}});
    let result = policy.evaluate(
        &candidate(json!({"confidence":0.3})),
        &schema,
        &json!({"min_confidence":0.8}),
    );
    assert!(!result.passed);
    assert!(result.reason_codes.contains(&REASON_CONFIDENCE_TOO_LOW));
}

#[test]
fn crosscheck_requires_check_summary() {
    let policy = CrossCheckV1;
    let schema = json!({"type":"object"});
    let result = policy.evaluate(&candidate(json!({"answer":"x"})), &schema, &json!({}));
    assert!(!result.passed);
    assert!(result.reason_codes.contains(&REASON_CHECK_SUMMARY_MISSING));
}

#[test]
fn crosscheck_min_score_gate_emits_reason() {
    let policy = CrossCheckV1;
    let schema = json!({"type":"object"});
    let result = policy.evaluate(
        &candidate(json!({"answer":"x","check_summary":"ok"})),
        &schema,
        &json!({"min_score":0.95}),
    );
    assert!(!result.passed);
    assert!(result.reason_codes.contains(&REASON_SCORE_TOO_LOW));
}

#[test]
fn registry_rejects_wrong_hash() {
    let reg = PolicyRegistry::with_builtin();
    let mut binding = reg
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("binding");
    binding.policy_hash = "bad-hash".to_owned();
    assert!(reg.require_binding(&binding).is_err());
}

#[test]
fn registry_accepts_compatibility_hash_window() {
    let mut reg = PolicyRegistry::with_builtin();
    let mut binding = reg
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("binding");
    binding.policy_hash = "legacy-hash-v1".to_owned();
    assert!(reg.require_binding(&binding).is_err());

    reg.allow_compatible_hash("vp.schema_only.v1", "1", "legacy-hash-v1");
    assert!(reg.require_binding(&binding).is_ok());
}

#[test]
fn registry_rejects_wrong_version() {
    let reg = PolicyRegistry::with_builtin();
    let mut binding = reg
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("binding");
    binding.policy_version = "2".to_owned();
    assert!(reg.require_binding(&binding).is_err());
}

#[test]
fn policy_specs_include_evidence_reason_codes() {
    let required = [
        REASON_EVIDENCE_UNREACHABLE,
        REASON_EVIDENCE_TIMEOUT,
        REASON_EVIDENCE_AUTH_DENIED,
        REASON_EVIDENCE_DIGEST_MISMATCH,
    ];
    for spec in [
        SchemaOnlyV1.spec(),
        SchemaThresholdsV1.spec(),
        CrossCheckV1.spec(),
    ] {
        for code in required {
            assert!(
                spec.reason_codes.contains(&code),
                "policy {} missing evidence code {}",
                spec.policy_id,
                code
            );
        }
    }
}
