use super::*;

#[test]
fn continuous_task_requires_epoch_renew_budget_mode() {
    let id = identity(47);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let mut contract = mk_contract("task-continuous-mode", policy_hash, 1_000_000, 1);
    contract.task_mode = TaskMode::Continuous;
    contract.budget.mode = BudgetMode::Lifetime;
    assert!(node.submit_task(contract.clone(), 1, 10).is_err());

    contract.budget.mode = BudgetMode::EpochRenew;
    node.submit_task(contract, 1, 20).unwrap();
}

#[test]
fn epoch_ended_advances_epoch_for_continuous_task() {
    let id = identity(48);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let mut contract = mk_contract("task-continuous-epoch", policy_hash, 1_000_000, 1);
    contract.task_mode = TaskMode::Continuous;
    contract.budget.mode = BudgetMode::EpochRenew;
    node.submit_task(contract, 1, 10).unwrap();
    node.end_epoch("task-continuous-epoch", 1, EpochEndReason::Finalized, 20)
        .unwrap();
    let view = node.task_view("task-continuous-epoch").unwrap().unwrap();
    assert_eq!(view.epoch, 2);
    assert_eq!(
        view.terminal_state,
        wattswarm::types::TaskTerminalState::Open
    );
}

#[test]
fn explore_stage_budget_exhaustion_blocks_more_candidates() {
    let id = identity(49);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let mut contract = mk_contract("task-budget-explore", policy_hash, 1_000_000, 1);
    contract.budget.explore_cost_units = 1;
    node.submit_task(contract, 1, 10).unwrap();
    node.claim_task(
        "task-budget-explore",
        ClaimRole::Propose,
        "ep",
        5_000,
        1,
        20,
    )
    .unwrap();
    node.propose_candidate(
        "task-budget-explore",
        make_candidate(
            "c1",
            "ep",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/e1".to_owned(),
                digest: "sha256:e1".to_owned(),
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
    let second = node.propose_candidate(
        "task-budget-explore",
        make_candidate(
            "c2",
            "ep",
            serde_json::json!({"answer":"ok-2"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/e2".to_owned(),
                digest: "sha256:e2".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        ),
        1,
        31,
    );
    assert!(second.is_err());
}

#[test]
fn finalize_rejects_when_da_quorum_missing() {
    let id = identity(50);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let mut contract = mk_contract("task-da-missing", policy_hash, 1_000_000, 1);
    contract.acceptance.da_quorum_threshold = 1;
    node.submit_task(contract, 1, 10).unwrap();
    node.claim_task("task-da-missing", ClaimRole::Propose, "ep", 5_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-da-missing",
        make_candidate(
            "c-da",
            "ep",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/da".to_owned(),
                digest: "sha256:da".to_owned(),
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
    node.claim_task("task-da-missing", ClaimRole::Verify, "ev", 5_000, 1, 40)
        .unwrap();
    let c_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-da-missing", "c-da")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-da-missing".to_owned(),
            candidate_id: "c-da".to_owned(),
            candidate_hash: c_hash.clone(),
            execution_id: "ev".to_owned(),
            verifier_result_hash: "vrh-da".to_owned(),
            commit_hash: vote_commit_hash(VoteChoice::Approve, "s", "vrh-da"),
        },
        1,
        50,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-da-missing".to_owned(),
            candidate_id: "c-da".to_owned(),
            candidate_hash: c_hash,
            execution_id: "ev".to_owned(),
            verifier_result_hash: "vrh-da".to_owned(),
            vote: VoteChoice::Approve,
            salt: "s".to_owned(),
        },
        1,
        60,
    )
    .unwrap();
    node.commit_decision("task-da-missing", 1, "c-da", 70)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(&node.identity, "task-da-missing", 1, "c-da")],
    };
    assert!(
        node.finalize_decision("task-da-missing", 1, "c-da", proof, 80)
            .is_err()
    );
}

#[test]
fn task_cost_report_export_has_required_fields() {
    let id = identity(51);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-cost-export", policy_hash, 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.claim_task("task-cost-export", ClaimRole::Propose, "ep", 5_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-cost-export",
        make_candidate(
            "cc",
            "ep",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/c".to_owned(),
                digest: "sha256:c".to_owned(),
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
    let exported = node
        .store
        .export_knowledge_by_task("task-cost-export")
        .unwrap();
    let reports = exported["task_cost_reports"].as_array().unwrap();
    assert!(!reports.is_empty());
    let first = &reports[0];
    assert!(first.get("cost_units_by_stage_json").is_some());
    assert!(first.get("latency_by_stage_json").is_some());
    assert!(first.get("events_emitted_count").is_some());
    assert!(first.get("cache_hit_rate").is_some());
}

#[test]
fn epoch_end_allowed_after_finalize_budget_is_spent() {
    let id = identity(52);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    let mut contract = mk_contract("task-epoch-roll", policy_hash, 1_000_000, 1);
    contract.task_mode = TaskMode::Continuous;
    contract.budget.mode = BudgetMode::EpochRenew;
    contract.budget.finalize_cost_units = 2;
    node.submit_task(contract, 1, 10).unwrap();
    node.claim_task("task-epoch-roll", ClaimRole::Propose, "ep", 5_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-epoch-roll",
        make_candidate(
            "c-roll",
            "ep",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/roll".to_owned(),
                digest: "sha256:roll".to_owned(),
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
    node.claim_task("task-epoch-roll", ClaimRole::Verify, "ev", 5_000, 1, 40)
        .unwrap();
    node.evidence_available("task-epoch-roll", "c-roll", "ev", "sha256:roll", 1, 45)
        .unwrap();
    node.submit_verifier_result(
        "task-epoch-roll",
        wattswarm::types::VerifierResult {
            candidate_id: "c-roll".to_owned(),
            execution_id: "ev".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-roll".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: node
                .task_view("task-epoch-roll")
                .unwrap()
                .unwrap()
                .contract
                .acceptance
                .verifier_policy
                .policy_hash,
        },
        1,
        50,
    )
    .unwrap();
    let c_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-epoch-roll", "c-roll")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-epoch-roll".to_owned(),
            candidate_id: "c-roll".to_owned(),
            candidate_hash: c_hash.clone(),
            execution_id: "ev".to_owned(),
            verifier_result_hash: "vrh-roll".to_owned(),
            commit_hash: vote_commit_hash(VoteChoice::Approve, "s-roll", "vrh-roll"),
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-epoch-roll".to_owned(),
            candidate_id: "c-roll".to_owned(),
            candidate_hash: c_hash,
            execution_id: "ev".to_owned(),
            verifier_result_hash: "vrh-roll".to_owned(),
            vote: VoteChoice::Approve,
            salt: "s-roll".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-epoch-roll", 1, "c-roll", 80)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(
            &node.identity,
            "task-epoch-roll",
            1,
            "c-roll",
        )],
    };
    node.finalize_decision("task-epoch-roll", 1, "c-roll", proof, 90)
        .unwrap();
    node.end_epoch("task-epoch-roll", 1, EpochEndReason::Finalized, 100)
        .unwrap();
    let view = node.task_view("task-epoch-roll").unwrap().unwrap();
    assert_eq!(view.epoch, 2);
}

#[test]
fn evidence_added_accepts_large_external_artifact_metadata() {
    let id = identity(53);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    let mut contract = mk_contract("task-evidence-size", policy_hash, 1_000_000, 1);
    contract.evidence_policy.max_snippet_bytes = 32;
    node.submit_task(contract, 1, 10).unwrap();
    node.claim_task("task-evidence-size", ClaimRole::Propose, "ep", 5_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-evidence-size",
        make_candidate(
            "c-es",
            "ep",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![],
        ),
        1,
        30,
    )
    .unwrap();

    node.add_evidence(
        "task-evidence-size",
        "c-es",
        "ep",
        vec![wattswarm::types::ArtifactRef {
            uri: "https://example.com/huge".to_owned(),
            digest: "sha256:huge".to_owned(),
            size_bytes: 5_000_000,
            mime: "application/octet-stream".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
        1,
        40,
    )
    .unwrap();
}
