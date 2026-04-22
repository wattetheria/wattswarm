use super::*;

#[test]
fn task_expired_is_terminal_state() {
    let id = identity(13);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let p_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(mk_contract("task-dead", p_hash, 50, 1), 1, 10)
        .unwrap();
    node.expire_task("task-dead", 1, 60).unwrap();

    let retry = node.schedule_retry("task-dead", 1, 70, 1, 61);
    assert!(retry.is_err());
}

#[test]
fn task_feedback_reported_bad_marks_settlement_window() {
    let node_identity = identity(35);
    let feedback_identity = identity(36);
    let membership = membership_all(&[node_identity.node_id()]);
    let mut node = mk_node(node_identity, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let mut contract = mk_contract("task-feedback", policy_hash, 1_000_000, 1);
    contract.acceptance.settlement.feedback.authority_pubkey =
        format!("ed25519:{}", feedback_identity.node_id());
    contract.acceptance.settlement.window_ms = 1_000;
    node.submit_task(contract, 1, 10).unwrap();

    node.claim_task("task-feedback", ClaimRole::Propose, "p", 7_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-feedback",
        make_candidate(
            "cf",
            "p",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        ),
        1,
        30,
    )
    .unwrap();
    node.claim_task("task-feedback", ClaimRole::Verify, "v", 7_000, 1, 40)
        .unwrap();
    node.evidence_available("task-feedback", "cf", "v", "sha256:1", 1, 45)
        .unwrap();
    let p_hash = node
        .task_view("task-feedback")
        .unwrap()
        .unwrap()
        .contract
        .acceptance
        .verifier_policy
        .policy_hash;
    node.submit_verifier_result(
        "task-feedback",
        wattswarm::types::VerifierResult {
            candidate_id: "cf".to_owned(),
            execution_id: "v".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-f".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: p_hash,
        },
        1,
        50,
    )
    .unwrap();
    let c_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-feedback", "cf")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, "sf", "vrh-f");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-feedback".to_owned(),
            candidate_id: "cf".to_owned(),
            candidate_hash: c_hash.clone(),
            execution_id: "v".to_owned(),
            verifier_result_hash: "vrh-f".to_owned(),
            commit_hash,
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-feedback".to_owned(),
            candidate_id: "cf".to_owned(),
            candidate_hash: c_hash,
            execution_id: "v".to_owned(),
            verifier_result_hash: "vrh-f".to_owned(),
            vote: VoteChoice::Approve,
            salt: "sf".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-feedback", 1, "cf", 80).unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(&node.identity, "task-feedback", 1, "cf")],
    };
    node.finalize_decision("task-feedback", 1, "cf", proof, 90)
        .unwrap();

    let signed_by = format!("ed25519:{}", feedback_identity.node_id());
    let reason = "wrong answer";
    let timestamp = 120;
    let signature = feedback_identity
        .sign_bytes(feedback_message("task-feedback", 1, "BAD", reason, timestamp).as_bytes());
    node.emit_at(
        1,
        wattswarm::types::EventPayload::TaskFeedbackReported(
            wattswarm::types::TaskFeedbackReportedPayload {
                task_id: "task-feedback".to_owned(),
                epoch: 1,
                outcome: "BAD".to_owned(),
                reason: reason.to_owned(),
                signed_by,
                signature,
                timestamp,
            },
        ),
        121,
    )
    .unwrap();

    let settlement = node
        .store
        .get_task_settlement("task-feedback")
        .unwrap()
        .unwrap();
    assert!(settlement.bad_feedback_exists);
    assert_eq!(settlement.bad_feedback_at, Some(timestamp));
}

#[test]
fn task_creation_accepts_compatible_policy_hash_window() {
    let id = identity(34);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let legacy_hash = "legacy-hash-v1";

    let invalid_before_window = node.submit_task(
        mk_contract("task-policy-old-1", legacy_hash.to_owned(), 1_000_000, 1),
        1,
        10,
    );
    assert!(invalid_before_window.is_err());

    node.policy_registry_mut()
        .allow_compatible_hash("vp.schema_only.v1", "1", legacy_hash);

    node.submit_task(
        mk_contract("task-policy-old-2", legacy_hash.to_owned(), 1_000_000, 1),
        1,
        20,
    )
    .unwrap();
}

#[test]
fn implicit_stability_delta_uses_deterministic_fixed_point() {
    let zero = wattswarm::node::implicit_stability_delta_units(0, 0.1, 10, 50).unwrap();
    assert_eq!(zero, 0);
    let non_zero = wattswarm::node::implicit_stability_delta_units(100, 0.1, 10, 50).unwrap();
    assert!(non_zero > 0);
}

#[test]
fn finalize_applies_due_implicit_settlement_bonus() {
    let id = identity(40);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    let mut contract = mk_contract("task-implicit-bonus", policy_hash.clone(), 1_000_000, 1);
    contract.acceptance.settlement.window_ms = 0;
    node.submit_task(contract, 1, 10).unwrap();
    node.claim_task(
        "task-implicit-bonus",
        ClaimRole::Propose,
        "exec-implicit-p",
        5_000,
        1,
        20,
    )
    .unwrap();
    node.propose_candidate(
        "task-implicit-bonus",
        make_candidate(
            "cand-implicit",
            "exec-implicit-p",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/implicit".to_owned(),
                digest: "sha256:implicit".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        ),
        1,
        30,
    )
    .unwrap();
    node.claim_task(
        "task-implicit-bonus",
        ClaimRole::Verify,
        "exec-implicit-v",
        5_000,
        1,
        40,
    )
    .unwrap();
    node.evidence_available(
        "task-implicit-bonus",
        "cand-implicit",
        "exec-implicit-v",
        "sha256:implicit",
        1,
        45,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-implicit-bonus",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-implicit".to_owned(),
            execution_id: "exec-implicit-v".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-implicit".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: policy_hash.clone(),
        },
        1,
        50,
    )
    .unwrap();

    let candidate_hash_v = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-implicit-bonus", "cand-implicit")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, "s-implicit", "vrh-implicit");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-implicit-bonus".to_owned(),
            candidate_id: "cand-implicit".to_owned(),
            candidate_hash: candidate_hash_v.clone(),
            execution_id: "exec-implicit-v".to_owned(),
            verifier_result_hash: "vrh-implicit".to_owned(),
            commit_hash,
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-implicit-bonus".to_owned(),
            candidate_id: "cand-implicit".to_owned(),
            candidate_hash: candidate_hash_v,
            execution_id: "exec-implicit-v".to_owned(),
            verifier_result_hash: "vrh-implicit".to_owned(),
            vote: VoteChoice::Approve,
            salt: "s-implicit".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-implicit-bonus", 1, "cand-implicit", 80)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(
            &node.identity,
            "task-implicit-bonus",
            1,
            "cand-implicit",
        )],
    };
    node.finalize_decision("task-implicit-bonus", 1, "cand-implicit", proof, 90)
        .unwrap();

    let settlement = node
        .store
        .get_task_settlement("task-implicit-bonus")
        .unwrap()
        .unwrap();
    assert!(settlement.implicit_settled);
    assert_eq!(settlement.implicit_settled_at, Some(90));

    let exported = node
        .store
        .export_knowledge_by_task("task-implicit-bonus")
        .unwrap();
    let stability = exported["reputation_state"][0]["stability_reputation"]
        .as_i64()
        .unwrap_or(0);
    assert!(stability > 0);
}
