use wattswarm_storage_core::PgStore;
use wattswarm_storage_core::types::{
    Acceptance, ArtifactRef, Assignment, Budget, BudgetMode, Candidate, ClaimPolicy,
    EvidencePolicy, ExploreAssignment, ExploreStopPolicy, FeedbackCapabilityPolicy,
    FinalizeAssignment, MaxConcurrency, PolicyBinding, SettlementBadPenalty,
    SettlementDiminishingReturns, SettlementPolicy, TaskContract, TaskMode, VerificationStatus,
    VerifierResult, VerifyAssignment, VotePolicy,
};

fn sample_contract(task_id: &str) -> TaskContract {
    TaskContract {
        protocol_version: "v0.1".to_owned(),
        task_id: task_id.to_owned(),
        task_type: "resume_review".to_owned(),
        inputs: serde_json::json!({"city":"San Francisco"}),
        output_schema: serde_json::json!({"type":"object"}),
        budget: Budget {
            time_ms: 30_000,
            max_steps: 10,
            cost_units: 1_000,
            mode: BudgetMode::Lifetime,
            explore_cost_units: 350,
            verify_cost_units: 450,
            finalize_cost_units: 200,
            reuse_verify_time_ms: 20_000,
            reuse_verify_cost_units: 100,
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
                    no_new_evidence_rounds: 2,
                },
            },
            verify: VerifyAssignment { max_verifiers: 2 },
            finalize: FinalizeAssignment { max_finalizers: 1 },
        },
        acceptance: Acceptance {
            quorum_threshold: 1,
            verifier_policy: PolicyBinding {
                policy_id: "vp.schema_only.v1".to_owned(),
                policy_version: "1".to_owned(),
                policy_hash: "policy-hash".to_owned(),
                policy_params: serde_json::json!({}),
            },
            vote: VotePolicy {
                commit_reveal: true,
                reveal_deadline_ms: 10_000,
            },
            settlement: SettlementPolicy {
                window_ms: 86_400_000,
                implicit_weight: 0.1,
                implicit_diminishing_returns: SettlementDiminishingReturns { w: 10, k: 50 },
                bad_penalty: SettlementBadPenalty { p: 3 },
                feedback: FeedbackCapabilityPolicy {
                    mode: "CAPABILITY".to_owned(),
                    authority_pubkey: "ed25519:placeholder".to_owned(),
                },
            },
            da_quorum_threshold: 1,
        },
        task_mode: TaskMode::OneShot,
        expiry_ms: 2_000_000_000_000,
        evidence_policy: EvidencePolicy {
            max_inline_evidence_bytes: 65_536,
            max_inline_media_bytes: 0,
            inline_mime_allowlist: vec!["application/json".to_owned(), "text/plain".to_owned()],
            max_snippet_bytes: 8_192,
            max_snippet_tokens: 2_048,
        },
    }
}

fn sample_candidate(candidate_id: &str, execution_id: &str) -> Candidate {
    Candidate {
        candidate_id: candidate_id.to_owned(),
        execution_id: execution_id.to_owned(),
        output: serde_json::json!({"result":"ok"}),
        evidence_inline: vec![],
        evidence_refs: vec![ArtifactRef {
            uri: "https://example.com/artifacts/1".to_owned(),
            digest: "sha256:abc123".to_owned(),
            size_bytes: 123,
            mime: "application/json".to_owned(),
            created_at: 1_700_000_000_000,
            producer: "runtime/profile".to_owned(),
        }],
    }
}

fn sample_verifier_result(candidate_id: &str, execution_id: &str, passed: bool) -> VerifierResult {
    VerifierResult {
        candidate_id: candidate_id.to_owned(),
        execution_id: execution_id.to_owned(),
        verification_status: if passed {
            VerificationStatus::Passed
        } else {
            VerificationStatus::Failed
        },
        passed,
        score: if passed { 0.99 } else { 0.2 },
        reason_codes: vec![1001],
        verifier_result_hash: format!("vrh-{candidate_id}-{execution_id}"),
        provider_family: "openai".to_owned(),
        model_id: "gpt-5".to_owned(),
        policy_id: "vp.schema_only.v1".to_owned(),
        policy_version: "1".to_owned(),
        policy_hash: "policy-hash".to_owned(),
    }
}

#[test]
fn list_task_ids_recent_orders_by_epoch_then_task_id_desc() {
    let store = PgStore::open_in_memory().expect("open store");
    store
        .upsert_task_contract(&sample_contract("task-a"), 1)
        .expect("upsert task-a");
    store
        .upsert_task_contract(&sample_contract("task-b"), 5)
        .expect("upsert task-b");
    store
        .upsert_task_contract(&sample_contract("task-c"), 5)
        .expect("upsert task-c");

    let ids = store.list_task_ids_recent(2).expect("list recent ids");
    assert_eq!(ids, vec!["task-c".to_owned(), "task-b".to_owned()]);
}

#[test]
fn latest_candidate_for_task_returns_latest_candidate_id() {
    let store = PgStore::open_in_memory().expect("open store");
    let task_id = "task-latest-candidate";
    store
        .upsert_task_contract(&sample_contract(task_id), 1)
        .expect("upsert task");

    let none = store
        .latest_candidate_for_task(task_id)
        .expect("query empty candidate");
    assert!(none.is_none());

    store
        .put_candidate(task_id, "node-a", &sample_candidate("cand-1", "exec-1"))
        .expect("insert cand-1");
    store
        .put_candidate(task_id, "node-a", &sample_candidate("cand-2", "exec-2"))
        .expect("insert cand-2");

    let latest = store
        .latest_candidate_for_task(task_id)
        .expect("query latest candidate")
        .expect("candidate exists");
    assert_eq!(latest.candidate_id, "cand-2");
    assert_eq!(latest.execution_id, "exec-2");
}

#[test]
fn count_verifier_results_for_candidate_only_counts_target_candidate() {
    let store = PgStore::open_in_memory().expect("open store");
    let task_id = "task-verifier-count";
    store
        .upsert_task_contract(&sample_contract(task_id), 1)
        .expect("upsert task");

    store
        .put_candidate(task_id, "node-a", &sample_candidate("cand-1", "exec-1"))
        .expect("insert cand-1");
    store
        .put_candidate(task_id, "node-b", &sample_candidate("cand-2", "exec-2"))
        .expect("insert cand-2");

    store
        .put_verifier_result(
            task_id,
            "verifier-a",
            &sample_verifier_result("cand-1", "exec-1", true),
        )
        .expect("insert result 1");
    store
        .put_verifier_result(
            task_id,
            "verifier-b",
            &sample_verifier_result("cand-1", "exec-1", false),
        )
        .expect("insert result 2");
    store
        .put_verifier_result(
            task_id,
            "verifier-c",
            &sample_verifier_result("cand-2", "exec-2", true),
        )
        .expect("insert result 3");

    let cand1_count = store
        .count_verifier_results_for_candidate(task_id, "cand-1")
        .expect("count cand-1");
    let cand2_count = store
        .count_verifier_results_for_candidate(task_id, "cand-2")
        .expect("count cand-2");

    assert_eq!(cand1_count, 2);
    assert_eq!(cand2_count, 1);
}

#[test]
fn has_vote_commit_for_execution_matches_task_and_execution() {
    let store = PgStore::open_in_memory().expect("open store");
    let task_id = "task-vote-commit";
    store
        .upsert_task_contract(&sample_contract(task_id), 1)
        .expect("upsert task");

    store
        .put_vote_commit(
            task_id,
            "voter-a",
            "candidate-hash",
            "commit-hash",
            "vrh",
            "exec-123",
            1_700_000_000_123,
        )
        .expect("insert vote commit");

    let matched = store
        .has_vote_commit_for_execution(task_id, "exec-123")
        .expect("query matched vote commit");
    let wrong_execution = store
        .has_vote_commit_for_execution(task_id, "exec-999")
        .expect("query wrong execution");
    let wrong_task = store
        .has_vote_commit_for_execution("other-task", "exec-123")
        .expect("query wrong task");

    assert!(matched);
    assert!(!wrong_execution);
    assert!(!wrong_task);
}
