use super::*;

#[test]
fn knowledge_lookup_exact_switches_execute_stage_to_decide_with_seed() {
    let id = identity(41);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(
        mk_contract("task-seed-base", policy_hash.clone(), 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.claim_task(
        "task-seed-base",
        ClaimRole::Propose,
        "exec-p0",
        5_000,
        1,
        20,
    )
    .unwrap();
    node.propose_candidate(
        "task-seed-base",
        make_candidate(
            "cand-seed-base",
            "exec-p0",
            serde_json::json!({"answer":"seeded"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/seed".to_owned(),
                digest: "sha256:seed".to_owned(),
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
    node.claim_task("task-seed-base", ClaimRole::Verify, "exec-v0", 5_000, 1, 40)
        .unwrap();
    node.evidence_available(
        "task-seed-base",
        "cand-seed-base",
        "exec-v0",
        "sha256:seed",
        1,
        45,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-seed-base",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-seed-base".to_owned(),
            execution_id: "exec-v0".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-seed-base".to_owned(),
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
    let c_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-seed-base", "cand-seed-base")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, "seed-salt", "vrh-seed-base");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-seed-base".to_owned(),
            candidate_id: "cand-seed-base".to_owned(),
            candidate_hash: c_hash.clone(),
            execution_id: "exec-v0".to_owned(),
            verifier_result_hash: "vrh-seed-base".to_owned(),
            commit_hash,
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-seed-base".to_owned(),
            candidate_id: "cand-seed-base".to_owned(),
            candidate_hash: c_hash,
            execution_id: "exec-v0".to_owned(),
            verifier_result_hash: "vrh-seed-base".to_owned(),
            vote: VoteChoice::Approve,
            salt: "seed-salt".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-seed-base", 1, "cand-seed-base", 80)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(
            &node.identity,
            "task-seed-base",
            1,
            "cand-seed-base",
        )],
    };
    node.finalize_decision("task-seed-base", 1, "cand-seed-base", proof, 90)
        .unwrap();

    node.submit_task(
        mk_contract("task-seed-next", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-seed-next",
        ClaimRole::Propose,
        "exec-p1",
        7_000,
        1,
        110,
    )
    .unwrap();

    let observed = Arc::new(Mutex::new((String::new(), false)));
    let observed_clone = observed.clone();
    let runtime = MockRuntime {
        execute: Box::new(move |req| {
            let has_exact = req.seed_bundle.as_ref().is_some_and(|bundle| {
                bundle
                    .hits
                    .iter()
                    .any(|hit| hit.hit_type == wattswarm::types::KnowledgeHitType::Exact)
            });
            *observed_clone.lock().unwrap() = (req.stage.clone(), has_exact);
            Ok(ExecuteResponse {
                candidate_output: serde_json::json!({
                    "answer": "from-runtime",
                    "confidence": 0.9,
                    "check_summary": "ok"
                }),
                evidence_inline: vec![],
                evidence_refs: vec![wattswarm::types::ArtifactRef {
                    uri: "https://example.com/r".to_owned(),
                    digest: "sha256:r".to_owned(),
                    size_bytes: 1,
                    mime: "text/plain".to_owned(),
                    created_at: 1,
                    producer: "runtime/default".to_owned(),
                }],
            })
        }),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };
    node.auto_execute_with_runtime(&runtime, "task-seed-next", "default", "exec-p1", 1, 120)
        .unwrap();

    let (stage, has_exact) = observed.lock().unwrap().clone();
    assert_eq!(stage, "DECIDE");
    assert!(has_exact);
    assert_eq!(
        node.store
            .count_reuse_applied_lookups("task-seed-next")
            .unwrap(),
        1
    );
}

#[test]
fn knowledge_lookup_reuse_time_budget_forces_explore_stage() {
    let id = identity(141);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    let mut contract = mk_contract("task-reuse-time-budget", policy_hash, 1_000_000, 1);
    contract.budget.reuse_verify_time_ms = 5;
    node.submit_task(contract.clone(), 1, 100).unwrap();
    seed_exact_decision_memory(
        &node,
        "task-reuse-time-budget-history",
        &contract,
        serde_json::json!({"answer":"seeded"}),
    );
    node.claim_task(
        "task-reuse-time-budget",
        ClaimRole::Propose,
        "exec-reuse-time",
        7_000,
        1,
        110,
    )
    .unwrap();

    let observed_stage = Arc::new(Mutex::new(String::new()));
    let observed_stage_clone = observed_stage.clone();
    let runtime = MockRuntime {
        execute: Box::new(move |req| {
            *observed_stage_clone.lock().unwrap() = req.stage.clone();
            Ok(ExecuteResponse {
                candidate_output: serde_json::json!({
                    "answer": "from-runtime",
                    "confidence": 0.9,
                    "check_summary": "ok"
                }),
                evidence_inline: vec![],
                evidence_refs: vec![wattswarm::types::ArtifactRef {
                    uri: "https://example.com/r".to_owned(),
                    digest: "sha256:r".to_owned(),
                    size_bytes: 1,
                    mime: "text/plain".to_owned(),
                    created_at: 1,
                    producer: "runtime/default".to_owned(),
                }],
            })
        }),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };

    node.auto_execute_with_runtime(
        &runtime,
        "task-reuse-time-budget",
        "default",
        "exec-reuse-time",
        1,
        120,
    )
    .unwrap();

    assert_eq!(*observed_stage.lock().unwrap(), "EXPLORE");
    assert_eq!(
        node.store
            .count_reuse_applied_lookups("task-reuse-time-budget")
            .unwrap(),
        0
    );
}

#[test]
fn seed_bundle_drops_oversized_hits_and_falls_back_to_explore() {
    let id = identity(142);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    let contract = mk_contract("task-seed-size-guard", policy_hash, 1_000_000, 1);
    node.submit_task(contract.clone(), 1, 100).unwrap();
    let oversized = "x".repeat(40_000);
    seed_exact_decision_memory(
        &node,
        "task-seed-size-guard-history",
        &contract,
        serde_json::json!({"answer": oversized}),
    );
    node.claim_task(
        "task-seed-size-guard",
        ClaimRole::Propose,
        "exec-seed-size",
        7_000,
        1,
        110,
    )
    .unwrap();

    let observed = Arc::new(Mutex::new((String::new(), false)));
    let observed_clone = observed.clone();
    let runtime = MockRuntime {
        execute: Box::new(move |req| {
            let has_seed = req
                .seed_bundle
                .as_ref()
                .is_some_and(|bundle| !bundle.hits.is_empty());
            *observed_clone.lock().unwrap() = (req.stage.clone(), has_seed);
            Ok(ExecuteResponse {
                candidate_output: serde_json::json!({
                    "answer": "from-runtime",
                    "confidence": 0.9,
                    "check_summary": "ok"
                }),
                evidence_inline: vec![],
                evidence_refs: vec![wattswarm::types::ArtifactRef {
                    uri: "https://example.com/r".to_owned(),
                    digest: "sha256:r".to_owned(),
                    size_bytes: 1,
                    mime: "text/plain".to_owned(),
                    created_at: 1,
                    producer: "runtime/default".to_owned(),
                }],
            })
        }),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };

    node.auto_execute_with_runtime(
        &runtime,
        "task-seed-size-guard",
        "default",
        "exec-seed-size",
        1,
        120,
    )
    .unwrap();

    let (stage, has_seed) = observed.lock().unwrap().clone();
    assert_eq!(stage, "EXPLORE");
    assert!(!has_seed);
    assert_eq!(
        node.store
            .count_reuse_applied_lookups("task-seed-size-guard")
            .unwrap(),
        0
    );
}

#[test]
fn advisory_requires_created_approved_applied_before_policy_tuned() {
    let id = identity(42);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    node.emit_at(
        1,
        wattswarm::types::EventPayload::AdvisoryCreated(wattswarm::types::AdvisoryCreatedPayload {
            advisory_id: "adv-1".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            suggested_policy_hash: "compat-hash-v2".to_owned(),
            reason: "lower threshold under admin approval".to_owned(),
        }),
        10,
    )
    .unwrap();

    assert!(
        node.emit_at(
            1,
            wattswarm::types::EventPayload::PolicyTuned(wattswarm::types::PolicyTunedPayload {
                policy_id: "vp.schema_only.v1".to_owned(),
                from_policy_hash: "old".to_owned(),
                to_policy_hash: "compat-hash-v2".to_owned(),
                advisory_id: "adv-1".to_owned(),
            }),
            11,
        )
        .is_err()
    );

    node.emit_at(
        1,
        wattswarm::types::EventPayload::AdvisoryApproved(
            wattswarm::types::AdvisoryApprovedPayload {
                advisory_id: "adv-1".to_owned(),
                admin_node_id: node.identity.node_id(),
            },
        ),
        12,
    )
    .unwrap();
    node.emit_at(
        1,
        wattswarm::types::EventPayload::AdvisoryApplied(wattswarm::types::AdvisoryAppliedPayload {
            advisory_id: "adv-1".to_owned(),
            applied_policy_hash: "compat-hash-v2".to_owned(),
        }),
        13,
    )
    .unwrap();
    node.emit_at(
        1,
        wattswarm::types::EventPayload::PolicyTuned(wattswarm::types::PolicyTunedPayload {
            policy_id: "vp.schema_only.v1".to_owned(),
            from_policy_hash: "old".to_owned(),
            to_policy_hash: "compat-hash-v2".to_owned(),
            advisory_id: "adv-1".to_owned(),
        }),
        14,
    )
    .unwrap();

    let state = node.store.get_advisory_state("adv-1").unwrap().unwrap();
    assert_eq!(state.status, "applied");
}

#[test]
fn unknown_reason_codes_are_recorded_with_protocol_versions() {
    let id = identity(43);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-unknown-reason", policy_hash.clone(), 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.claim_task(
        "task-unknown-reason",
        ClaimRole::Propose,
        "exec-p",
        5_000,
        1,
        20,
    )
    .unwrap();
    node.propose_candidate(
        "task-unknown-reason",
        make_candidate(
            "cand-unknown",
            "exec-p",
            serde_json::json!({"answer":"unknown-reason"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/u".to_owned(),
                digest: "sha256:u".to_owned(),
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
        "task-unknown-reason",
        ClaimRole::Verify,
        "exec-v",
        5_000,
        1,
        40,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-unknown-reason",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-unknown".to_owned(),
            execution_id: "exec-v".to_owned(),
            verification_status: VerificationStatus::Failed,
            passed: false,
            score: 0.1,
            reason_codes: vec![7777],
            verifier_result_hash: "vrh-unknown".to_owned(),
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

    let exported = node
        .store
        .export_knowledge_by_task("task-unknown-reason")
        .unwrap();
    let observations = exported["unknown_reason_observations"].as_array().unwrap();
    assert!(!observations.is_empty());
    assert!(
        observations
            .iter()
            .any(|o| o["unknown_reason_code"].as_i64() == Some(7777))
    );
    assert!(
        observations
            .iter()
            .all(|o| o["peer_protocol_version"].as_str().is_some())
    );
    assert!(
        observations
            .iter()
            .all(|o| o["local_protocol_version"].as_str().is_some())
    );
}

#[test]
fn reuse_blacklist_prevents_same_candidate_hash_reproposal() {
    let id = identity(44);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(
        mk_contract("task-reuse-ref", policy_hash.clone(), 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.claim_task(
        "task-reuse-ref",
        ClaimRole::Propose,
        "exec-ref-p",
        5_000,
        1,
        20,
    )
    .unwrap();
    node.propose_candidate(
        "task-reuse-ref",
        make_candidate(
            "cand-ref",
            "exec-ref-p",
            serde_json::json!({"answer":"ref"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/ref".to_owned(),
                digest: "sha256:ref".to_owned(),
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
        "task-reuse-ref",
        ClaimRole::Verify,
        "exec-ref-v",
        5_000,
        1,
        40,
    )
    .unwrap();
    node.evidence_available(
        "task-reuse-ref",
        "cand-ref",
        "exec-ref-v",
        "sha256:ref",
        1,
        45,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-reuse-ref",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-ref".to_owned(),
            execution_id: "exec-ref-v".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-ref".to_owned(),
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
    let ref_candidate_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-reuse-ref", "cand-ref")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ref_commit_hash = vote_commit_hash(VoteChoice::Approve, "salt-ref", "vrh-ref");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-reuse-ref".to_owned(),
            candidate_id: "cand-ref".to_owned(),
            candidate_hash: ref_candidate_hash.clone(),
            execution_id: "exec-ref-v".to_owned(),
            verifier_result_hash: "vrh-ref".to_owned(),
            commit_hash: ref_commit_hash,
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-reuse-ref".to_owned(),
            candidate_id: "cand-ref".to_owned(),
            candidate_hash: ref_candidate_hash,
            execution_id: "exec-ref-v".to_owned(),
            verifier_result_hash: "vrh-ref".to_owned(),
            vote: VoteChoice::Approve,
            salt: "salt-ref".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-reuse-ref", 1, "cand-ref", 80)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(
            &node.identity,
            "task-reuse-ref",
            1,
            "cand-ref",
        )],
    };
    let ref_finalize = node
        .finalize_decision("task-reuse-ref", 1, "cand-ref", proof, 90)
        .unwrap();

    node.submit_task(
        mk_contract("task-reuse-live", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-reuse-live",
        ClaimRole::Propose,
        "exec-live-p",
        7_000,
        1,
        110,
    )
    .unwrap();
    node.propose_candidate(
        "task-reuse-live",
        make_candidate(
            "cand-live",
            "exec-live-p",
            serde_json::json!({"answer":"x"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/live".to_owned(),
                digest: "sha256:live".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        ),
        1,
        120,
    )
    .unwrap();
    node.claim_task(
        "task-reuse-live",
        ClaimRole::Verify,
        "exec-live-v",
        7_000,
        1,
        130,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-reuse-live",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-live".to_owned(),
            execution_id: "exec-live-v".to_owned(),
            verification_status: VerificationStatus::Failed,
            passed: false,
            score: 0.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-live".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: node
                .task_view("task-reuse-live")
                .unwrap()
                .unwrap()
                .contract
                .acceptance
                .verifier_policy
                .policy_hash,
        },
        1,
        140,
    )
    .unwrap();
    let live_candidate = make_candidate(
        "cand-live",
        "exec-live-p",
        serde_json::json!({"answer":"x"}),
        vec![],
        vec![wattswarm::types::ArtifactRef {
            uri: "https://example.com/live".to_owned(),
            digest: "sha256:live".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    );
    let live_hash = candidate_hash(&live_candidate).unwrap();
    let reject_commit_hash = vote_commit_hash(VoteChoice::Reject, "salt-live", "vrh-live");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-reuse-live".to_owned(),
            candidate_id: "cand-live".to_owned(),
            candidate_hash: live_hash.clone(),
            execution_id: "exec-live-v".to_owned(),
            verifier_result_hash: "vrh-live".to_owned(),
            commit_hash: reject_commit_hash,
        },
        1,
        150,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-reuse-live".to_owned(),
            candidate_id: "cand-live".to_owned(),
            candidate_hash: live_hash.clone(),
            execution_id: "exec-live-v".to_owned(),
            verifier_result_hash: "vrh-live".to_owned(),
            vote: VoteChoice::Reject,
            salt: "salt-live".to_owned(),
        },
        1,
        160,
    )
    .unwrap();

    node.emit_at(
        1,
        wattswarm::types::EventPayload::ReuseRejectRecorded(
            wattswarm::types::ReuseRejectRecordedPayload {
                task_id: "task-reuse-live".to_owned(),
                decision_ref: wattswarm::types::DecisionReference {
                    task_id: "task-reuse-ref".to_owned(),
                    epoch: 1,
                    final_commit_hash: ref_finalize.event_id,
                },
                candidate_hash: live_hash.clone(),
                reject_quorum_proof: vec!["reject-proof-1".to_owned()],
                reason_codes: vec![REASON_SCHEMA_OK],
            },
        ),
        170,
    )
    .unwrap();

    assert!(
        node.propose_candidate("task-reuse-live", live_candidate, 1, 180)
            .is_err()
    );
}

#[test]
fn reconcile_timeouts_auto_records_reuse_reject_quorum() {
    let id = identity(143);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(
        mk_contract("task-reuse-auto-ref", policy_hash.clone(), 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.claim_task(
        "task-reuse-auto-ref",
        ClaimRole::Propose,
        "exec-auto-ref-p",
        5_000,
        1,
        20,
    )
    .unwrap();
    node.propose_candidate(
        "task-reuse-auto-ref",
        make_candidate(
            "cand-auto-ref",
            "exec-auto-ref-p",
            serde_json::json!({"answer":"ref"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/auto-ref".to_owned(),
                digest: "sha256:auto-ref".to_owned(),
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
        "task-reuse-auto-ref",
        ClaimRole::Verify,
        "exec-auto-ref-v",
        5_000,
        1,
        40,
    )
    .unwrap();
    node.evidence_available(
        "task-reuse-auto-ref",
        "cand-auto-ref",
        "exec-auto-ref-v",
        "sha256:auto-ref",
        1,
        45,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-reuse-auto-ref",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-auto-ref".to_owned(),
            execution_id: "exec-auto-ref-v".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-auto-ref".to_owned(),
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
    let ref_candidate_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-reuse-auto-ref", "cand-auto-ref")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ref_commit_hash = vote_commit_hash(VoteChoice::Approve, "salt-auto-ref", "vrh-auto-ref");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-reuse-auto-ref".to_owned(),
            candidate_id: "cand-auto-ref".to_owned(),
            candidate_hash: ref_candidate_hash.clone(),
            execution_id: "exec-auto-ref-v".to_owned(),
            verifier_result_hash: "vrh-auto-ref".to_owned(),
            commit_hash: ref_commit_hash,
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-reuse-auto-ref".to_owned(),
            candidate_id: "cand-auto-ref".to_owned(),
            candidate_hash: ref_candidate_hash,
            execution_id: "exec-auto-ref-v".to_owned(),
            verifier_result_hash: "vrh-auto-ref".to_owned(),
            vote: VoteChoice::Approve,
            salt: "salt-auto-ref".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-reuse-auto-ref", 1, "cand-auto-ref", 80)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(
            &node.identity,
            "task-reuse-auto-ref",
            1,
            "cand-auto-ref",
        )],
    };
    let ref_finalize = node
        .finalize_decision("task-reuse-auto-ref", 1, "cand-auto-ref", proof, 90)
        .unwrap();

    node.submit_task(
        mk_contract("task-reuse-auto-live", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-reuse-auto-live",
        ClaimRole::Propose,
        "exec-auto-live-p",
        7_000,
        1,
        110,
    )
    .unwrap();
    let runtime_exec = MockRuntime {
        execute: Box::new(|_| {
            Ok(ExecuteResponse {
                candidate_output: serde_json::json!({
                    "answer": "reuse-live",
                    "confidence": 0.8,
                    "check_summary": "reuse-attempt"
                }),
                evidence_inline: vec![],
                evidence_refs: vec![wattswarm::types::ArtifactRef {
                    uri: "https://example.com/auto-live".to_owned(),
                    digest: "sha256:auto-live".to_owned(),
                    size_bytes: 1,
                    mime: "text/plain".to_owned(),
                    created_at: 1,
                    producer: "r/p".to_owned(),
                }],
            })
        }),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };
    node.auto_execute_with_runtime(
        &runtime_exec,
        "task-reuse-auto-live",
        "default",
        "exec-auto-live-p",
        1,
        120,
    )
    .unwrap();
    let live_candidate = node
        .store
        .get_candidate_by_id("task-reuse-auto-live", "cand-exec-auto-live-p")
        .unwrap()
        .unwrap();
    let live_hash = candidate_hash(&live_candidate).unwrap();
    node.claim_task(
        "task-reuse-auto-live",
        ClaimRole::Verify,
        "exec-auto-live-v",
        7_000,
        1,
        130,
    )
    .unwrap();
    node.evidence_available(
        "task-reuse-auto-live",
        "cand-exec-auto-live-p",
        "exec-auto-live-v",
        "sha256:auto-live",
        1,
        135,
    )
    .unwrap();
    node.submit_verifier_result(
        "task-reuse-auto-live",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-exec-auto-live-p".to_owned(),
            execution_id: "exec-auto-live-v".to_owned(),
            verification_status: VerificationStatus::Failed,
            passed: false,
            score: 0.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-auto-live".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: node
                .task_view("task-reuse-auto-live")
                .unwrap()
                .unwrap()
                .contract
                .acceptance
                .verifier_policy
                .policy_hash,
        },
        1,
        140,
    )
    .unwrap();
    let reject_commit_hash =
        vote_commit_hash(VoteChoice::Reject, "salt-auto-live", "vrh-auto-live");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-reuse-auto-live".to_owned(),
            candidate_id: "cand-exec-auto-live-p".to_owned(),
            candidate_hash: live_hash.clone(),
            execution_id: "exec-auto-live-v".to_owned(),
            verifier_result_hash: "vrh-auto-live".to_owned(),
            commit_hash: reject_commit_hash,
        },
        1,
        150,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-reuse-auto-live".to_owned(),
            candidate_id: "cand-exec-auto-live-p".to_owned(),
            candidate_hash: live_hash.clone(),
            execution_id: "exec-auto-live-v".to_owned(),
            verifier_result_hash: "vrh-auto-live".to_owned(),
            vote: VoteChoice::Reject,
            salt: "salt-auto-live".to_owned(),
        },
        1,
        160,
    )
    .unwrap();

    let emitted = node.reconcile_timeouts(1, 10_000).unwrap();
    let reject_payload = emitted.iter().find_map(|event| match &event.payload {
        wattswarm::types::EventPayload::ReuseRejectRecorded(payload) => Some(payload.clone()),
        _ => None,
    });
    assert!(emitted.iter().any(|event| {
        matches!(
            event.payload,
            wattswarm::types::EventPayload::TaskRetryScheduled(_)
        )
    }));
    let reject_payload = reject_payload.expect("reuse reject record should be emitted");
    assert_eq!(reject_payload.task_id, "task-reuse-auto-live");
    assert_eq!(reject_payload.candidate_hash, live_hash);
    assert_eq!(reject_payload.decision_ref.task_id, "task-reuse-auto-ref");
    assert_eq!(
        reject_payload.decision_ref.final_commit_hash,
        ref_finalize.event_id
    );
    assert!(
        node.store
            .is_reuse_blacklisted("task-reuse-auto-live", 1, &reject_payload.candidate_hash)
            .unwrap()
    );
}

#[test]
fn knowledge_lookup_ignores_error_only_decision_memory_entries() {
    let id = identity(45);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(
        mk_contract("task-error-mem", policy_hash.clone(), 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.task_error(
        "task-error-mem",
        TaskErrorReason::InvalidOutput,
        "schema mismatch",
        1,
        20,
    )
    .unwrap();

    node.submit_task(
        mk_contract("task-after-error", policy_hash, 1_000_000, 1),
        1,
        30,
    )
    .unwrap();
    node.claim_task(
        "task-after-error",
        ClaimRole::Propose,
        "exec-after",
        7_000,
        1,
        40,
    )
    .unwrap();

    let observed_stage = Arc::new(Mutex::new(String::new()));
    let observed_stage_clone = observed_stage.clone();
    let runtime = MockRuntime {
        execute: Box::new(move |req| {
            *observed_stage_clone.lock().unwrap() = req.stage.clone();
            Ok(ExecuteResponse {
                candidate_output: serde_json::json!({
                    "answer": "ok",
                    "confidence": 0.9,
                    "check_summary": "ok"
                }),
                evidence_inline: vec![],
                evidence_refs: vec![wattswarm::types::ArtifactRef {
                    uri: "https://example.com/error-only".to_owned(),
                    digest: "sha256:e".to_owned(),
                    size_bytes: 1,
                    mime: "text/plain".to_owned(),
                    created_at: 1,
                    producer: "runtime/default".to_owned(),
                }],
            })
        }),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };
    node.auto_execute_with_runtime(&runtime, "task-after-error", "default", "exec-after", 1, 50)
        .unwrap();

    assert_eq!(*observed_stage.lock().unwrap(), "EXPLORE");
}

#[test]
fn runtime_metrics_cost_units_accumulates_with_observations() {
    let store = PgStore::open_in_memory()
        .unwrap()
        .for_org("local:test-app:bootstrap");
    let obs_1 = RuntimeMetricObservation {
        runtime_id: "rt",
        profile_id: "p",
        task_type: "test",
        window_start: 0,
        window_end: 86_400_000,
        finalized: true,
        timeout: false,
        crash: false,
        invalid_output: false,
        latency_ms: 100,
        cost_units: 100,
        reject_reason_codes: &[],
        reuse_hit_exact: false,
        reuse_hit_similar: true,
        reuse_applied: false,
    };
    let obs_2 = RuntimeMetricObservation {
        cost_units: 250,
        latency_ms: 220,
        reuse_hit_exact: true,
        reuse_hit_similar: false,
        reuse_applied: true,
        ..obs_1
    };
    store.upsert_runtime_metric_observation(&obs_1).unwrap();
    store.upsert_runtime_metric_observation(&obs_2).unwrap();

    let exported = store.export_knowledge_by_task_type("test").unwrap();
    let metrics = exported["runtime_metrics"].as_array().unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0]["cost_units"].as_i64(), Some(350));
    assert_eq!(metrics[0]["time_to_finality_p95"].as_i64(), Some(220));
    let exact_rate = metrics[0]["reuse_hit_rate_exact"]
        .as_f64()
        .unwrap_or_default();
    let similar_rate = metrics[0]["reuse_hit_rate_similar"]
        .as_f64()
        .unwrap_or_default();
    let accept_rate = metrics[0]["reuse_candidate_accept_rate"]
        .as_f64()
        .unwrap_or_default();
    assert!((exact_rate - 0.5).abs() < 0.0001);
    assert!((similar_rate - 0.5).abs() < 0.0001);
    assert!((accept_rate - 0.5).abs() < 0.0001);
}

#[test]
fn reuse_reject_requires_matching_decision_reference_tuple() {
    let id = identity(46);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(
        mk_contract("task-ref-src", policy_hash.clone(), 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    node.claim_task("task-ref-src", ClaimRole::Propose, "ep", 5_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-ref-src",
        make_candidate(
            "c-ref-src",
            "ep",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/r1".to_owned(),
                digest: "sha256:r1".to_owned(),
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
    node.claim_task("task-ref-src", ClaimRole::Verify, "ev", 5_000, 1, 40)
        .unwrap();
    node.evidence_available("task-ref-src", "c-ref-src", "ev", "sha256:r1", 1, 45)
        .unwrap();
    node.submit_verifier_result(
        "task-ref-src",
        wattswarm::types::VerifierResult {
            candidate_id: "c-ref-src".to_owned(),
            execution_id: "ev".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-ref-src".to_owned(),
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
    let c_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-ref-src", "c-ref-src")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, "s", "vrh-ref-src");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-ref-src".to_owned(),
            candidate_id: "c-ref-src".to_owned(),
            candidate_hash: c_hash.clone(),
            execution_id: "ev".to_owned(),
            verifier_result_hash: "vrh-ref-src".to_owned(),
            commit_hash,
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-ref-src".to_owned(),
            candidate_id: "c-ref-src".to_owned(),
            candidate_hash: c_hash,
            execution_id: "ev".to_owned(),
            verifier_result_hash: "vrh-ref-src".to_owned(),
            vote: VoteChoice::Approve,
            salt: "s".to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-ref-src", 1, "c-ref-src", 80)
        .unwrap();
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(
            &node.identity,
            "task-ref-src",
            1,
            "c-ref-src",
        )],
    };
    let finalized = node
        .finalize_decision("task-ref-src", 1, "c-ref-src", proof, 90)
        .unwrap();

    node.submit_task(
        mk_contract("task-ref-live", policy_hash.clone(), 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task("task-ref-live", ClaimRole::Propose, "ep2", 5_000, 1, 110)
        .unwrap();
    let live_candidate = make_candidate(
        "c-ref-live",
        "ep2",
        serde_json::json!({"answer":"live"}),
        vec![],
        vec![wattswarm::types::ArtifactRef {
            uri: "https://example.com/r2".to_owned(),
            digest: "sha256:r2".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    );
    let live_hash = candidate_hash(&live_candidate).unwrap();
    node.propose_candidate("task-ref-live", live_candidate, 1, 120)
        .unwrap();
    node.claim_task("task-ref-live", ClaimRole::Verify, "ev2", 5_000, 1, 130)
        .unwrap();
    let reject_commit_hash = vote_commit_hash(VoteChoice::Reject, "x", "vrh-live");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-ref-live".to_owned(),
            candidate_id: "c-ref-live".to_owned(),
            candidate_hash: live_hash.clone(),
            execution_id: "ev2".to_owned(),
            verifier_result_hash: "vrh-live".to_owned(),
            commit_hash: reject_commit_hash,
        },
        1,
        140,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-ref-live".to_owned(),
            candidate_id: "c-ref-live".to_owned(),
            candidate_hash: live_hash,
            execution_id: "ev2".to_owned(),
            verifier_result_hash: "vrh-live".to_owned(),
            vote: VoteChoice::Reject,
            salt: "x".to_owned(),
        },
        1,
        150,
    )
    .unwrap();

    let result = node.emit_at(
        1,
        wattswarm::types::EventPayload::ReuseRejectRecorded(
            wattswarm::types::ReuseRejectRecordedPayload {
                task_id: "task-ref-live".to_owned(),
                decision_ref: wattswarm::types::DecisionReference {
                    task_id: "task-ref-live".to_owned(),
                    epoch: 1,
                    final_commit_hash: finalized.event_id,
                },
                candidate_hash: "sha256:live-missing".to_owned(),
                reject_quorum_proof: vec!["proof".to_owned()],
                reason_codes: vec![REASON_SCHEMA_OK],
            },
        ),
        160,
    );
    assert!(result.is_err());
}
