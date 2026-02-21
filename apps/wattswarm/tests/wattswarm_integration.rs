use anyhow::anyhow;
use base64::Engine;
use std::sync::{Arc, Mutex};
use wattswarm::constants::CLOCK_SKEW_TOLERANCE_MS;
use wattswarm::crypto::{NodeIdentity, candidate_hash, sha256_hex, vote_commit_hash};
use wattswarm::node::{Node, feedback_message, finality_sign, sign_membership_quorum};
use wattswarm::reason_codes::{REASON_EVIDENCE_UNREACHABLE, REASON_SCHEMA_OK};
use wattswarm::runtime::{
    ExecuteRequest, ExecuteResponse, RuntimeCapabilities, RuntimeClient, VerifyRequest,
    VerifyResponse,
};
use wattswarm::storage::{PgStore, RuntimeMetricObservation};
use wattswarm::types::{
    Acceptance, Assignment, Budget, BudgetMode, Candidate, ClaimPolicy, ClaimRole, EpochEndReason,
    EvidencePolicy, ExploreAssignment, ExploreStopPolicy, FeedbackCapabilityPolicy,
    FinalizeAssignment, MaxConcurrency, Membership, MembershipUpdatedPayload, Role,
    SettlementBadPenalty, SettlementDiminishingReturns, SettlementPolicy, TaskContract,
    TaskErrorReason, TaskMode, VerificationStatus, VerifyAssignment, VoteChoice, VotePolicy,
};

struct MockRuntime {
    execute: Box<ExecuteFn>,
    verify: Box<VerifyFn>,
}

type ExecuteFn = dyn Fn(&ExecuteRequest) -> anyhow::Result<ExecuteResponse> + Send + Sync;
type VerifyFn = dyn Fn(&VerifyRequest) -> anyhow::Result<VerifyResponse> + Send + Sync;

impl RuntimeClient for MockRuntime {
    fn health(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn capabilities(&self) -> anyhow::Result<RuntimeCapabilities> {
        Ok(RuntimeCapabilities {
            task_types: vec!["test".to_owned()],
            profiles: vec!["default".to_owned()],
            provider_family: "mock".to_owned(),
            model_id: "mock-1".to_owned(),
        })
    }

    fn execute(&self, req: &ExecuteRequest) -> anyhow::Result<ExecuteResponse> {
        (self.execute)(req)
    }

    fn verify(&self, req: &VerifyRequest) -> anyhow::Result<VerifyResponse> {
        (self.verify)(req)
    }
}

fn identity(seed: u8) -> NodeIdentity {
    NodeIdentity::from_seed([seed; 32])
}

fn membership_all(node_ids: &[String]) -> Membership {
    let mut m = Membership::new();
    for node_id in node_ids {
        for role in [
            Role::Proposer,
            Role::Verifier,
            Role::Committer,
            Role::Finalizer,
        ] {
            m.grant(node_id, role);
        }
    }
    m
}

fn mk_contract(
    task_id: &str,
    policy_hash: String,
    expiry_ms: u64,
    quorum_threshold: u32,
) -> TaskContract {
    let feedback_authority = format!("ed25519:{}", identity(201).node_id());
    TaskContract {
        protocol_version: "v0.1".to_owned(),
        task_id: task_id.to_owned(),
        task_type: "test".to_owned(),
        inputs: serde_json::json!({"prompt":"hello"}),
        output_schema: serde_json::json!({
          "type":"object",
          "required":["answer"],
          "properties":{
            "answer":{"type":"string"},
            "confidence":{"type":"number"},
            "check_summary":{"type":"string"}
          }
        }),
        budget: Budget {
            time_ms: 20_000,
            max_steps: 10,
            cost_units: 1_000,
            mode: BudgetMode::Lifetime,
            explore_cost_units: 350,
            verify_cost_units: 450,
            finalize_cost_units: 200,
            reuse_verify_time_ms: 20_000,
            reuse_verify_cost_units: 200,
            reuse_max_attempts: 1,
        },
        assignment: Assignment {
            mode: "CLAIM".to_owned(),
            claim: ClaimPolicy {
                lease_ms: 5_000,
                max_concurrency: MaxConcurrency {
                    propose: 1,
                    verify: 1,
                },
            },
            explore: ExploreAssignment {
                max_proposers: 1,
                topk: 3,
                stop: ExploreStopPolicy {
                    no_new_evidence_rounds: 3,
                },
            },
            verify: VerifyAssignment { max_verifiers: 1 },
            finalize: FinalizeAssignment { max_finalizers: 1 },
        },
        acceptance: Acceptance {
            quorum_threshold,
            verifier_policy: wattswarm::types::PolicyBinding {
                policy_id: "vp.schema_only.v1".to_owned(),
                policy_version: "1".to_owned(),
                policy_hash,
                policy_params: serde_json::json!({}),
            },
            vote: VotePolicy {
                commit_reveal: true,
                reveal_deadline_ms: 3_000,
            },
            settlement: SettlementPolicy {
                window_ms: 86_400_000,
                implicit_weight: 0.1,
                implicit_diminishing_returns: SettlementDiminishingReturns { w: 10, k: 50 },
                bad_penalty: SettlementBadPenalty { p: 3 },
                feedback: FeedbackCapabilityPolicy {
                    mode: "CAPABILITY".to_owned(),
                    authority_pubkey: feedback_authority,
                },
            },
            da_quorum_threshold: 1,
        },
        task_mode: TaskMode::OneShot,
        expiry_ms,
        evidence_policy: EvidencePolicy {
            max_inline_evidence_bytes: 65_536,
            max_inline_media_bytes: 0,
            inline_mime_allowlist: vec![
                "application/json".to_owned(),
                "text/plain".to_owned(),
                "text/markdown".to_owned(),
                "text/csv".to_owned(),
            ],
            max_snippet_bytes: 8_192,
            max_snippet_tokens: 2_048,
        },
    }
}

fn mk_node(identity: NodeIdentity, membership: Membership) -> Node {
    Node::new(identity, PgStore::open_in_memory().unwrap(), membership).unwrap()
}

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
        Candidate {
            candidate_id: "cand-av".to_owned(),
            execution_id: "exec-auto-prop".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cand-net".to_owned(),
            execution_id: "exec-p".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://blocked.example.com/artifact".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "application/json".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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

    let candidate = Candidate {
        candidate_id: "c-lease".to_owned(),
        execution_id: "a-exec".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![],
        evidence_refs: vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "sha256:1".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    };
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

    let candidate = Candidate {
        candidate_id: "c-skew-ok".to_owned(),
        execution_id: "exec-skew".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![],
        evidence_refs: vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "sha256:1".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    };

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

    let candidate = Candidate {
        candidate_id: "c-skew-bad".to_owned(),
        execution_id: "exec-skew".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![],
        evidence_refs: vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "sha256:1".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    };

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

    let too_big_inline = Candidate {
        candidate_id: "c-big".to_owned(),
        execution_id: "exec-evi".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![wattswarm::types::InlineEvidence {
            mime: "text/plain".to_owned(),
            content: "a".repeat(70_000),
        }],
        evidence_refs: vec![],
    };
    assert!(
        node.propose_candidate("task-evi", too_big_inline, 1, 120)
            .is_err()
    );

    let media_inline = Candidate {
        candidate_id: "c-media".to_owned(),
        execution_id: "exec-evi".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![wattswarm::types::InlineEvidence {
            mime: "image/png".to_owned(),
            content: "tiny".to_owned(),
        }],
        evidence_refs: vec![],
    };
    assert!(
        node.propose_candidate("task-evi", media_inline, 1, 121)
            .is_err()
    );

    let missing_digest = Candidate {
        candidate_id: "c-ref".to_owned(),
        execution_id: "exec-evi".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![],
        evidence_refs: vec![wattswarm::types::ArtifactRef {
            uri: "https://e".to_owned(),
            digest: "".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    };
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
    let hidden_media = Candidate {
        candidate_id: "c-hidden".to_owned(),
        execution_id: "exec-evi".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![wattswarm::types::InlineEvidence {
            mime: "application/json".to_owned(),
            content: json_payload,
        }],
        evidence_refs: vec![],
    };
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
        Candidate {
            candidate_id: "c-vote".to_owned(),
            execution_id: "exec-p".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "c-retry".to_owned(),
            execution_id: "exec-p".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cand-1".to_owned(),
            execution_id: "e1".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "c1".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cf".to_owned(),
            execution_id: "p".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://e".to_owned(),
                digest: "sha256:1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cand-seed-base".to_owned(),
            execution_id: "exec-p0".to_owned(),
            output: serde_json::json!({"answer":"seeded"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/seed".to_owned(),
                digest: "sha256:seed".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cand-unknown".to_owned(),
            execution_id: "exec-p".to_owned(),
            output: serde_json::json!({"answer":"unknown-reason"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/u".to_owned(),
                digest: "sha256:u".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cand-ref".to_owned(),
            execution_id: "exec-ref-p".to_owned(),
            output: serde_json::json!({"answer":"ref"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/ref".to_owned(),
                digest: "sha256:ref".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cand-live".to_owned(),
            execution_id: "exec-live-p".to_owned(),
            output: serde_json::json!({"answer":"x"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/live".to_owned(),
                digest: "sha256:live".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
    let live_candidate = Candidate {
        candidate_id: "cand-live".to_owned(),
        execution_id: "exec-live-p".to_owned(),
        output: serde_json::json!({"answer":"x"}),
        evidence_inline: vec![],
        evidence_refs: vec![wattswarm::types::ArtifactRef {
            uri: "https://example.com/live".to_owned(),
            digest: "sha256:live".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    };
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
    let store = PgStore::open_in_memory().unwrap();
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
    };
    let obs_2 = RuntimeMetricObservation {
        cost_units: 250,
        ..obs_1
    };
    store.upsert_runtime_metric_observation(&obs_1).unwrap();
    store.upsert_runtime_metric_observation(&obs_2).unwrap();

    let exported = store.export_knowledge_by_task_type("test").unwrap();
    let metrics = exported["runtime_metrics"].as_array().unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0]["cost_units"].as_i64(), Some(350));
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
        Candidate {
            candidate_id: "c-ref-src".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/r1".to_owned(),
                digest: "sha256:r1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
    let live_candidate = Candidate {
        candidate_id: "c-ref-live".to_owned(),
        execution_id: "ep2".to_owned(),
        output: serde_json::json!({"answer":"live"}),
        evidence_inline: vec![],
        evidence_refs: vec![wattswarm::types::ArtifactRef {
            uri: "https://example.com/r2".to_owned(),
            digest: "sha256:r2".to_owned(),
            size_bytes: 1,
            mime: "text/plain".to_owned(),
            created_at: 1,
            producer: "r/p".to_owned(),
        }],
    };
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
        Candidate {
            candidate_id: "c1".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/e1".to_owned(),
                digest: "sha256:e1".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
        1,
        30,
    )
    .unwrap();
    let second = node.propose_candidate(
        "task-budget-explore",
        Candidate {
            candidate_id: "c2".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok-2"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/e2".to_owned(),
                digest: "sha256:e2".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "c-da".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/da".to_owned(),
                digest: "sha256:da".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "cc".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/c".to_owned(),
                digest: "sha256:c".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "c-roll".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![wattswarm::types::ArtifactRef {
                uri: "https://example.com/roll".to_owned(),
                digest: "sha256:roll".to_owned(),
                size_bytes: 1,
                mime: "text/plain".to_owned(),
                created_at: 1,
                producer: "r/p".to_owned(),
            }],
        },
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
        Candidate {
            candidate_id: "c-es".to_owned(),
            execution_id: "ep".to_owned(),
            output: serde_json::json!({"answer":"ok"}),
            evidence_inline: vec![],
            evidence_refs: vec![],
        },
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
