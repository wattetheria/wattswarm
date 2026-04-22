use super::*;

#[test]
fn batched_backfill_allows_interleaving_local_lease_renew() {
    let id_a = identity(31);
    let id_b = identity(32);
    let membership = membership_all(&[id_a.node_id(), id_b.node_id()]);
    let mut a = mk_node(id_a, membership.clone());
    let mut b = mk_node(id_b, membership);

    let policy_hash = a
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    for i in 0..1_000 {
        a.submit_task(
            mk_contract(
                &format!("task-backfill-{i}"),
                policy_hash.clone(),
                1_000_000,
                1,
            ),
            1,
            100 + i,
        )
        .unwrap();
    }

    b.submit_task(
        mk_contract("task-local-renew", policy_hash, 1_000_000, 1),
        1,
        10,
    )
    .unwrap();
    b.claim_task(
        "task-local-renew",
        ClaimRole::Propose,
        "exec-local",
        5_000,
        1,
        20,
    )
    .unwrap();

    let mut rounds = 0u32;
    while b.task_view("task-backfill-999").unwrap().is_none() {
        let applied = b.sync_from_peer(&a).unwrap();
        assert!(applied <= wattswarm::constants::BACKFILL_BATCH_EVENTS);
        assert!(applied > 0);

        rounds += 1;
        let now = 200 + rounds as u64 * 10;
        b.renew_claim(
            "task-local-renew",
            ClaimRole::Propose,
            "exec-local",
            now + 7_900,
            1,
            now,
        )
        .unwrap();
    }

    assert!(rounds > 1);
    let lease = b
        .store
        .get_lease("task-local-renew", "propose")
        .unwrap()
        .unwrap();
    assert!(lease.lease_until >= 4_200);
}

#[test]
fn lease_conflict_tiebreak_and_renew_behavior() {
    let id = identity(4);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(mk_contract("task-lease", policy_hash, 1_000_000, 1), 1, 100)
        .unwrap();

    node.claim_task("task-lease", ClaimRole::Propose, "z-exec", 1_000, 1, 110)
        .unwrap();

    node.claim_task("task-lease", ClaimRole::Propose, "a-exec", 1_100, 1, 120)
        .unwrap();

    let lease = node
        .store
        .get_lease("task-lease", "propose")
        .unwrap()
        .unwrap();
    assert_eq!(lease.execution_id, "a-exec");

    let loser = node.claim_task("task-lease", ClaimRole::Propose, "m-exec", 1_200, 1, 130);
    assert!(loser.is_err());

    node.renew_claim("task-lease", ClaimRole::Propose, "a-exec", 2_000, 1, 200)
        .unwrap();

    let candidate = make_candidate(
        "c-lease",
        "a-exec",
        serde_json::json!({"answer":"x"}),
        vec![],
        vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "sha256:1".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    );
    node.propose_candidate("task-lease", candidate, 1, 1500)
        .unwrap();
}

#[test]
fn lease_tolerance_accepts_small_clock_skew() {
    let id = identity(23);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-skew-ok", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();

    node.claim_task(
        "task-skew-ok",
        ClaimRole::Propose,
        "exec-skew",
        1_000,
        1,
        200,
    )
    .unwrap();

    let candidate = make_candidate(
        "c-skew-ok",
        "exec-skew",
        serde_json::json!({"answer":"x"}),
        vec![],
        vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "sha256:1".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    );

    let slightly_after_expiry = 1_000 + CLOCK_SKEW_TOLERANCE_MS - 1;
    node.propose_candidate("task-skew-ok", candidate, 1, slightly_after_expiry)
        .unwrap();
}

#[test]
fn lease_tolerance_rejects_beyond_clock_window() {
    let id = identity(24);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-skew-bad", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();

    node.claim_task(
        "task-skew-bad",
        ClaimRole::Propose,
        "exec-skew",
        1_000,
        1,
        200,
    )
    .unwrap();

    let candidate = make_candidate(
        "c-skew-bad",
        "exec-skew",
        serde_json::json!({"answer":"x"}),
        vec![],
        vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "sha256:1".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    );

    let far_after_expiry = 1_000 + CLOCK_SKEW_TOLERANCE_MS + 1;
    assert!(
        node.propose_candidate("task-skew-bad", candidate, 1, far_after_expiry)
            .is_err()
    );
}

#[test]
fn evidence_policy_and_event_size_rules() {
    let id = identity(5);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(mk_contract("task-evi", policy_hash, 1_000_000, 1), 1, 100)
        .unwrap();
    node.claim_task("task-evi", ClaimRole::Propose, "exec-evi", 8_000, 1, 110)
        .unwrap();

    let too_big_inline = make_candidate(
        "c-big",
        "exec-evi",
        serde_json::json!({"answer":"x"}),
        vec![wattswarm::types::InlineEvidence {
            mime: "text/plain".to_owned(),
            content: "a".repeat(70_000),
        }],
        vec![],
    );
    assert!(
        node.propose_candidate("task-evi", too_big_inline, 1, 120)
            .is_err()
    );

    let media_inline = make_candidate(
        "c-media",
        "exec-evi",
        serde_json::json!({"answer":"x"}),
        vec![wattswarm::types::InlineEvidence {
            mime: "image/png".to_owned(),
            content: "tiny".to_owned(),
        }],
        vec![],
    );
    assert!(
        node.propose_candidate("task-evi", media_inline, 1, 121)
            .is_err()
    );

    let missing_digest = make_candidate(
        "c-ref",
        "exec-evi",
        serde_json::json!({"answer":"x"}),
        vec![],
        vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    );
    assert!(
        node.propose_candidate("task-evi", missing_digest, 1, 122)
            .is_err()
    );

    let huge_msg = "z".repeat(140_000);
    assert!(
        node.task_error("task-evi", TaskErrorReason::Other, &huge_msg, 1, 130)
            .is_err()
    );
}

#[test]
fn evidence_policy_rejects_hidden_media_base64_inside_json() {
    let id = identity(33);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(
        mk_contract("task-evi-b64", policy_hash, 1_000_000, 1),
        1,
        100,
    )
    .unwrap();
    node.claim_task(
        "task-evi-b64",
        ClaimRole::Propose,
        "exec-evi",
        8_000,
        1,
        110,
    )
    .unwrap();

    let mut media_bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    media_bytes.extend(std::iter::repeat_n(0_u8, 256));
    let encoded_media = base64::engine::general_purpose::STANDARD.encode(media_bytes);
    let json_payload = serde_json::json!({
        "kind": "analysis",
        "artifact": encoded_media
    })
    .to_string();
    let hidden_media = make_candidate(
        "c-hidden",
        "exec-evi",
        serde_json::json!({"answer":"x"}),
        vec![wattswarm::types::InlineEvidence {
            mime: "application/json".to_owned(),
            content: json_payload,
        }],
        vec![],
    );
    assert!(
        node.propose_candidate("task-evi-b64", hidden_media, 1, 120)
            .is_err()
    );
}

#[test]
fn commit_reveal_enforced_and_deadline() {
    let id = identity(6);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(mk_contract("task-vote", policy_hash, 1_000_000, 1), 1, 100)
        .unwrap();

    node.claim_task("task-vote", ClaimRole::Propose, "exec-p", 8_000, 1, 110)
        .unwrap();
    node.propose_candidate(
        "task-vote",
        make_candidate(
            "c-vote",
            "exec-p",
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

    node.claim_task("task-vote", ClaimRole::Verify, "exec-v", 8_000, 1, 130)
        .unwrap();

    node.submit_verifier_result(
        "task-vote",
        wattswarm::types::VerifierResult {
            candidate_id: "c-vote".to_owned(),
            execution_id: "exec-v".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-1".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m1".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: node
                .task_view("task-vote")
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

    let good_salt = "good";
    let c_vote_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-vote", "c-vote")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, good_salt, "vrh-1");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-vote".to_owned(),
            candidate_id: "c-vote".to_owned(),
            candidate_hash: c_vote_hash.clone(),
            execution_id: "exec-v".to_owned(),
            verifier_result_hash: "vrh-1".to_owned(),
            commit_hash: commit_hash.clone(),
        },
        1,
        150,
    )
    .unwrap();

    assert!(
        node.submit_vote_reveal(
            wattswarm::types::VoteRevealPayload {
                task_id: "task-vote".to_owned(),
                candidate_id: "c-vote".to_owned(),
                candidate_hash: c_vote_hash.clone(),
                execution_id: "exec-v".to_owned(),
                verifier_result_hash: "vrh-1".to_owned(),
                vote: VoteChoice::Approve,
                salt: "bad".to_owned(),
            },
            1,
            160,
        )
        .is_err()
    );

    assert!(
        node.submit_vote_reveal(
            wattswarm::types::VoteRevealPayload {
                task_id: "task-vote".to_owned(),
                candidate_id: "c-vote".to_owned(),
                candidate_hash: c_vote_hash.clone(),
                execution_id: "exec-v".to_owned(),
                verifier_result_hash: "vrh-1".to_owned(),
                vote: VoteChoice::Approve,
                salt: good_salt.to_owned(),
            },
            1,
            3_500,
        )
        .is_err()
    );

    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-vote".to_owned(),
            candidate_id: "c-vote".to_owned(),
            candidate_hash: c_vote_hash,
            execution_id: "exec-v".to_owned(),
            verifier_result_hash: "vrh-1".to_owned(),
            vote: VoteChoice::Approve,
            salt: good_salt.to_owned(),
        },
        1,
        200,
    )
    .unwrap();

    node.commit_decision("task-vote", 1, "c-vote", 250).unwrap();
}

#[test]
fn reveal_deadlock_schedules_retry_before_task_expiry() {
    let id = identity(26);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    let mut contract = mk_contract("task-retry", policy_hash, 10_000, 1);
    contract.acceptance.vote.reveal_deadline_ms = 100;
    node.submit_task(contract, 1, 100).unwrap();

    node.claim_task("task-retry", ClaimRole::Propose, "exec-p", 8_000, 1, 110)
        .unwrap();
    node.propose_candidate(
        "task-retry",
        make_candidate(
            "c-retry",
            "exec-p",
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

    node.claim_task("task-retry", ClaimRole::Verify, "exec-v", 8_000, 1, 130)
        .unwrap();
    node.submit_verifier_result(
        "task-retry",
        wattswarm::types::VerifierResult {
            candidate_id: "c-retry".to_owned(),
            execution_id: "exec-v".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh-retry".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m1".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: node
                .task_view("task-retry")
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

    let c_retry_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-retry", "c-retry")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, "salt-retry", "vrh-retry");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-retry".to_owned(),
            candidate_id: "c-retry".to_owned(),
            candidate_hash: c_retry_hash,
            execution_id: "exec-v".to_owned(),
            verifier_result_hash: "vrh-retry".to_owned(),
            commit_hash,
        },
        1,
        150,
    )
    .unwrap();

    let no_retry_yet = node.reconcile_timeouts(1, 220).unwrap();
    assert!(no_retry_yet.is_empty());

    let retry_events = node.reconcile_timeouts(1, 251).unwrap();
    assert_eq!(retry_events.len(), 1);
    match &retry_events[0].payload {
        wattswarm::types::EventPayload::TaskRetryScheduled(payload) => {
            assert_eq!(payload.task_id, "task-retry");
            assert_eq!(payload.attempt, 1);
        }
        other => panic!("expected TaskRetryScheduled, got {other:?}"),
    }

    let view = node.task_view("task-retry").unwrap().unwrap();
    assert_eq!(view.retry_attempt, 1);
    assert_eq!(
        node.store
            .list_vote_commits_meta("task-retry")
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn expiry_terminal_blocks_commit_and_finalize() {
    let id = identity(7);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;

    node.submit_task(mk_contract("task-exp", policy_hash, 50, 1), 1, 10)
        .unwrap();
    node.expire_task("task-exp", 1, 60).unwrap();

    assert!(node.commit_decision("task-exp", 1, "any", 70).is_err());
    let proof = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(&node.identity, "task-exp", 1, "any")],
    };
    assert!(
        node.finalize_decision("task-exp", 1, "any", proof, 80)
            .is_err()
    );
}

#[test]
fn finality_disallows_fork_finalize() {
    let id = identity(8);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(mk_contract("task-final", policy_hash, 1_000_000, 1), 1, 10)
        .unwrap();

    node.claim_task("task-final", ClaimRole::Propose, "e1", 7_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-final",
        make_candidate(
            "cand-1",
            "e1",
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

    node.claim_task("task-final", ClaimRole::Verify, "e2", 7_000, 1, 40)
        .unwrap();
    node.evidence_available("task-final", "cand-1", "e2", "sha256:1", 1, 45)
        .unwrap();
    let p_hash = node
        .task_view("task-final")
        .unwrap()
        .unwrap()
        .contract
        .acceptance
        .verifier_policy
        .policy_hash;
    node.submit_verifier_result(
        "task-final",
        wattswarm::types::VerifierResult {
            candidate_id: "cand-1".to_owned(),
            execution_id: "e2".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "vrh".to_owned(),
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

    let salt = "s";
    let c_final_hash = candidate_hash(
        &node
            .store
            .get_candidate_by_id("task-final", "cand-1")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, salt, "vrh");
    node.submit_vote_commit(
        wattswarm::types::VoteCommitPayload {
            task_id: "task-final".to_owned(),
            candidate_id: "cand-1".to_owned(),
            candidate_hash: c_final_hash.clone(),
            execution_id: "e2".to_owned(),
            verifier_result_hash: "vrh".to_owned(),
            commit_hash: commit_hash.clone(),
        },
        1,
        60,
    )
    .unwrap();
    node.submit_vote_reveal(
        wattswarm::types::VoteRevealPayload {
            task_id: "task-final".to_owned(),
            candidate_id: "cand-1".to_owned(),
            candidate_hash: c_final_hash,
            execution_id: "e2".to_owned(),
            verifier_result_hash: "vrh".to_owned(),
            vote: VoteChoice::Approve,
            salt: salt.to_owned(),
        },
        1,
        70,
    )
    .unwrap();
    node.commit_decision("task-final", 1, "cand-1", 80).unwrap();

    let proof_1 = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(&node.identity, "task-final", 1, "cand-1")],
    };
    node.finalize_decision("task-final", 1, "cand-1", proof_1, 90)
        .unwrap();

    let proof_2 = wattswarm::types::FinalityProof {
        threshold: 1,
        signatures: vec![finality_sign(&node.identity, "task-final", 1, "cand-2")],
    };
    assert!(
        node.finalize_decision("task-final", 1, "cand-2", proof_2, 95)
            .is_err()
    );
}

#[test]
fn verifier_result_requires_provider_source_fields() {
    let id = identity(9);
    let membership = membership_all(&[id.node_id()]);
    let mut node = mk_node(id, membership);

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .unwrap()
        .policy_hash;
    node.submit_task(mk_contract("task-src", policy_hash, 1_000_000, 1), 1, 10)
        .unwrap();
    node.claim_task("task-src", ClaimRole::Propose, "ep", 7_000, 1, 20)
        .unwrap();
    node.propose_candidate(
        "task-src",
        make_candidate(
            "c1",
            "ep",
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
    node.claim_task("task-src", ClaimRole::Verify, "ev", 7_000, 1, 40)
        .unwrap();

    let p_hash = node
        .task_view("task-src")
        .unwrap()
        .unwrap()
        .contract
        .acceptance
        .verifier_policy
        .policy_hash;
    let bad = node.submit_verifier_result(
        "task-src",
        wattswarm::types::VerifierResult {
            candidate_id: "c1".to_owned(),
            execution_id: "ev".to_owned(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![REASON_SCHEMA_OK],
            verifier_result_hash: "v".to_owned(),
            provider_family: "".to_owned(),
            model_id: "".to_owned(),
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: p_hash,
        },
        1,
        50,
    );
    assert!(bad.is_err());
}

#[test]
fn membership_update_requires_quorum_signatures() {
    let finalizer_1 = identity(10);
    let finalizer_2 = identity(11);
    let author = identity(12);

    let mut membership = Membership::new();
    for id in [
        finalizer_1.node_id(),
        finalizer_2.node_id(),
        author.node_id(),
    ] {
        membership.grant(&id, Role::Finalizer);
        membership.grant(&id, Role::Committer);
    }

    let mut node = mk_node(author, membership.clone());
    let mut next_membership = membership.clone();
    next_membership.grant("new-node", Role::Verifier);

    let sig1 = sign_membership_quorum(&finalizer_1, &next_membership).unwrap();
    let sig2 = sign_membership_quorum(&finalizer_2, &next_membership).unwrap();

    node.emit_at(
        1,
        wattswarm::types::EventPayload::MembershipUpdated(MembershipUpdatedPayload {
            new_membership: next_membership,
            quorum_threshold: 2,
            quorum_signatures: vec![sig1, sig2],
        }),
        100,
    )
    .unwrap();
}
