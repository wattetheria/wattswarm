use super::*;

#[test]
fn single_machine_loop_submit_to_finalize() {
    let id = identity(1);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    let contract = mk_contract("task-1", policy_hash.clone(), 1_000_000, 1);
    node.submit_task(contract, 1, 1_000).unwrap();

    node.claim_task("task-1", ClaimRole::Propose, "exec-p", 8_000, 1, 1_010)
        .unwrap();

    let runtime_exec = MockRuntime {
        execute: Box::new(|req| {
            Ok(ExecuteResponse {
                candidate_output: serde_json::json!({
                    "answer": format!("ok:{}", req.execution_id),
                    "confidence": 0.95,
                    "check_summary": "cross-check complete"
                }),
                evidence_inline: vec![wattswarm::types::InlineEvidence {
                    mime: "text/plain".to_owned(),
                    content: "small-evidence".to_owned(),
                }],
                evidence_refs: vec![wattswarm::types::ArtifactRef {
                    uri: "https://example.com/a1".to_owned(),
                    digest: "sha256:abcd".to_owned(),
                    size_bytes: 10,
                    mime: "application/json".to_owned(),
                    created_at: 1,
                    producer: "runtime/default".to_owned(),
                }],
            })
        }),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };
    let runtime_verify = MockRuntime {
        execute: Box::new(|_| Err(anyhow!("unused"))),
        verify: Box::new(|req| {
            let payload = serde_json::json!({
                "candidate_id": req.candidate.candidate_id,
                "passed": true,
                "policy_hash": req.policy.policy_hash,
            });
            Ok(VerifyResponse {
                verification_status: Some(VerificationStatus::Passed),
                passed: true,
                score: 0.9,
                reason_codes: vec![REASON_SCHEMA_OK],
                verifier_result_hash: sha256_hex(serde_json::to_vec(&payload).unwrap().as_slice()),
                provider_family: "api-agent".to_owned(),
                model_id: "aa-2".to_owned(),
            })
        }),
    };

    node.auto_execute_with_runtime(&runtime_exec, "task-1", "default", "exec-p", 1, 1_020)
        .unwrap();

    node.claim_task("task-1", ClaimRole::Verify, "exec-v", 9_000, 1, 1_030)
        .unwrap();

    node.auto_verify_candidate_with_runtime(
        &runtime_verify,
        "task-1",
        "cand-exec-p",
        "exec-v",
        1,
        1_040,
    )
    .unwrap();

    let verify_hash = node
        .store
        .list_verifier_results_for_candidate("task-1", "cand-exec-p")
        .unwrap()[0]
        .verifier_result_hash
        .clone();
    let candidate_hash_v = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-1", "cand-exec-p")
            .unwrap()
            .unwrap(),
    )
    .unwrap();

    let salt = "salt-1";
    let commit_hash = vote_commit_hash(VoteChoice::Approve, salt, &verify_hash);
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-1".to_owned(),
            candidate_id: "cand-exec-p".to_owned(),
            candidate_hash: candidate_hash_v.clone(),
            execution_id: "exec-v".to_owned(),
            verifier_result_hash: verify_hash.clone(),
            commit_hash: commit_hash.clone(),
        },
        1,
        1_050,
    )
    .unwrap();

    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-1".to_owned(),
            candidate_id: "cand-exec-p".to_owned(),
            candidate_hash: candidate_hash_v,
            execution_id: "exec-v".to_owned(),
            verifier_result_hash: verify_hash.clone(),
            vote: VoteChoice::Approve,
            salt: salt.to_owned(),
        },
        1,
        1_060,
    )
    .unwrap();

    node.commit_decision("task-1", 1, "cand-exec-p", 1_070)
        .unwrap();

    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(&node.identity, "task-1", 1, "cand-exec-p")],
    };
    node.finalize_decision("task-1", 1, "cand-exec-p", proof, 1_080)
        .unwrap();

    let view = node.task_view("task-1").unwrap().unwrap();
    assert_eq!(
        view.terminal_state,
        wattswarm::types::TaskTerminalState::Finalized
    );
    assert_eq!(view.finalized_candidate_id.as_deref(), Some("cand-exec-p"));
}

#[test]
fn auto_execute_failure_emits_task_error() {
    let id = identity(21);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-auto-exec", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-auto-exec",
        ClaimRole::Propose,
        "exec-auto-prop",
        5_000,
        1,
        110,
    )
    .unwrap();

    let timeout_runtime = MockRuntime {
        execute: Box::new(|_| Err(anyhow!("timed out while calling /execute"))),
        verify: Box::new(|_| Err(anyhow!("unused"))),
    };
    assert!(
        node.auto_execute_with_runtime(
            &timeout_runtime,
            "task-auto-exec",
            "default",
            "exec-auto-prop",
            1,
            120,
        )
        .is_err()
    );

    let events = node.store.load_all_events().unwrap();
    let last = events.last().expect("events exist").1.clone();
    match last.payload {
        wattswarm::types::EventPayload::TaskError(err) => {
            assert_eq!(err.reason, TaskErrorReason::Timeout);
            assert_eq!(err.task_id, "task-auto-exec");
        }
        other => panic!("expected TaskError event, got {other:?}"),
    }
}

#[test]
fn auto_verify_invalid_output_emits_task_error() {
    let id = identity(22);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-auto-verify", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-auto-verify",
        ClaimRole::Propose,
        "exec-auto-prop",
        5_000,
        1,
        110,
    )
    .unwrap();
    node.propose_candidate(
        "task-auto-verify",
        make_candidate(
            "cand-av",
            "exec-auto-prop",
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
        120,
    )
    .unwrap();
    node.claim_task(
        "task-auto-verify",
        ClaimRole::Verify,
        "exec-auto-ver",
        6_000,
        1,
        130,
    )
    .unwrap();

    let invalid_runtime = MockRuntime {
        execute: Box::new(|_| Err(anyhow!("unused"))),
        verify: Box::new(|_| {
            Ok(VerifyResponse {
                verification_status: Some(VerificationStatus::Passed),
                passed: true,
                score: 1.2,
                reason_codes: vec![REASON_SCHEMA_OK],
                verifier_result_hash: "hash".to_owned(),
                provider_family: "mock".to_owned(),
                model_id: "m".to_owned(),
            })
        }),
    };

    assert!(
        node.auto_verify_candidate_with_runtime(
            &invalid_runtime,
            "task-auto-verify",
            "cand-av",
            "exec-auto-ver",
            1,
            140,
        )
        .is_err()
    );

    let events = node.store.load_all_events().unwrap();
    let last = events.last().expect("events exist").1.clone();
    match last.payload {
        wattswarm::types::EventPayload::TaskError(err) => {
            assert_eq!(err.reason, TaskErrorReason::InvalidOutput);
            assert_eq!(err.task_id, "task-auto-verify");
        }
        other => panic!("expected TaskError event, got {other:?}"),
    }
}

#[test]
fn auto_verify_network_unreachable_marks_inconclusive() {
    let id = identity(25);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-net-split", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-net-split",
        ClaimRole::Propose,
        "exec-p",
        7_000,
        1,
        110,
    )
    .unwrap();
    node.propose_candidate(
        "task-net-split",
        make_candidate(
            "cand-net",
            "exec-p",
            serde_json::json!({"answer":"ok"}),
            vec![],
            vec![wattswarm::types::ArtifactRef {
                uri: "https://blocked.example.com/artifact".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "application/json".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        ),
        1,
        120,
    )
    .unwrap();
    node.claim_task("task-net-split", ClaimRole::Verify, "exec-v", 7_000, 1, 130)
        .unwrap();

    let runtime = MockRuntime {
        execute: Box::new(|_| Err(anyhow!("unused"))),
        verify: Box::new(|_| Err(anyhow!("network unreachable"))),
    };
    node.auto_verify_candidate_with_runtime(
        &runtime,
        "task-net-split",
        "cand-net",
        "exec-v",
        1,
        140,
    )
    .unwrap();

    let results = node
        .store
        .list_verifier_results_for_candidate("task-net-split", "cand-net")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].verification_status,
        VerificationStatus::Inconclusive
    );
    assert!(!results[0].passed);
    assert!(
        results[0]
            .reason_codes
            .contains(&REASON_EVIDENCE_UNREACHABLE)
    );
}

#[test]
fn multi_node_sync_backfill_replay_checkpoint() {
    let id_a = identity(2);
    let id_b = identity(3);
    let membership = membership_all(&[id_a.node_id(), id_b.node_id()]);
    let mut a = mk_node(id_a, membership.clone());
    let mut b = mk_node(id_b, membership);

    let policy_hash = a
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    a.submit_task(mk_contract("task-sync", policy_hash, 1_000_000, 1), 1, 100)
        .unwrap();
    a.create_checkpoint("cp-1".to_owned(), 1, 110).unwrap();

    let applied = b.sync_from_peer(&a).unwrap();
    assert!(applied >= 2);
    assert_eq!(a.head_seq().unwrap(), b.head_seq().unwrap());

    b.replay_rebuild_projection().unwrap();
    assert!(b.task_view("task-sync").unwrap().is_some());

    a.claim_task("task-sync", ClaimRole::Propose, "exec-a", 1000, 1, 120)
        .unwrap();
    let (to_a, to_b) = a.anti_entropy_with(&mut b).unwrap();
    assert!(to_a + to_b >= 1);
}
