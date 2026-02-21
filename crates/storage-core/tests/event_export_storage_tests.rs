use wattswarm_storage_core::types::{
    Acceptance, ArtifactRef, Assignment, Budget, BudgetMode, Candidate, CheckpointCreatedPayload,
    ClaimPolicy, DecisionCommittedPayload, Event, EventPayload, EvidencePolicy, ExploreAssignment,
    ExploreStopPolicy, FeedbackCapabilityPolicy, FinalizeAssignment, MaxConcurrency, PolicyBinding,
    SettlementBadPenalty, SettlementDiminishingReturns, SettlementPolicy, TaskContract,
    TaskExpiredPayload, TaskMode, VerificationStatus, VerifierResult, VerifyAssignment, VoteChoice,
    VotePolicy,
};
use wattswarm_storage_core::{AdvisoryStateRow, PgStore};

fn sample_contract(task_id: &str, task_type: &str) -> TaskContract {
    TaskContract {
        protocol_version: "v0.1".to_owned(),
        task_id: task_id.to_owned(),
        task_type: task_type.to_owned(),
        inputs: serde_json::json!({"prompt":"hello"}),
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

fn sample_event(
    event_id: &str,
    protocol_version: &str,
    author_node_id: &str,
    epoch: u64,
    created_at: u64,
    payload: EventPayload,
) -> Event {
    Event {
        event_id: event_id.to_owned(),
        protocol_version: protocol_version.to_owned(),
        event_kind: payload.kind(),
        task_id: payload.task_id().map(ToOwned::to_owned),
        epoch,
        author_node_id: author_node_id.to_owned(),
        created_at,
        payload,
        signature_hex: "deadbeef".to_owned(),
    }
}

fn sample_candidate(candidate_id: &str, execution_id: &str) -> Candidate {
    Candidate {
        candidate_id: candidate_id.to_owned(),
        execution_id: execution_id.to_owned(),
        output: serde_json::json!({"answer":"ok"}),
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
fn event_log_round_trip_and_clear_projection_keeps_events() {
    let store = PgStore::open_in_memory().expect("open store");
    let task_id = "task-event-log";
    let contract = sample_contract(task_id, "resume_review");
    store
        .upsert_task_contract(&contract, 1)
        .expect("upsert contract");

    let created = sample_event(
        "evt-1",
        "0.1.0",
        "node-local",
        1,
        1_700_000_000_100,
        EventPayload::TaskCreated(contract.clone()),
    );
    let expired = sample_event(
        "evt-2",
        "0.2.0",
        "node-peer",
        1,
        1_700_000_000_200,
        EventPayload::TaskExpired(TaskExpiredPayload {
            task_id: task_id.to_owned(),
        }),
    );

    let seq1 = store.append_event(&created).expect("append evt-1");
    let seq2 = store.append_event(&expired).expect("append evt-2");
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);
    assert_eq!(store.head_seq().expect("head seq"), 2);

    let from_1 = store.load_events_from(1).expect("events after 1");
    assert_eq!(from_1.len(), 1);
    assert_eq!(from_1[0].0, 2);
    assert_eq!(from_1[0].1.event_id, "evt-2");

    let first_page = store.load_events_page(0, 1).expect("page");
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].1.event_id, "evt-1");

    let all = store.load_all_events().expect("all events");
    assert_eq!(all.len(), 2);

    let created_at = store.task_created_at(task_id).expect("task created at");
    assert_eq!(created_at, Some(1_700_000_000_100));

    let peer_dist = store
        .peer_protocol_version_distribution("node-local")
        .expect("peer version dist");
    assert_eq!(peer_dist, vec![("0.2.0".to_owned(), 1)]);

    assert_eq!(
        store
            .task_projection(task_id)
            .expect("projection")
            .expect("projection exists")
            .contract
            .task_id,
        task_id
    );

    store.clear_projection().expect("clear projection");
    assert!(
        store
            .task_projection(task_id)
            .expect("projection after clear")
            .is_none()
    );
    assert_eq!(store.head_seq().expect("head seq after clear"), 2);
}

#[test]
fn export_and_vote_checkpoint_membership_paths_work() {
    let store = PgStore::open_in_memory().expect("open store");
    let task_id = "task-export";
    let task_type = "resume_review";
    store
        .upsert_task_contract(&sample_contract(task_id, task_type), 1)
        .expect("upsert task");

    store
        .put_decision_memory(
            task_id,
            1,
            "final-commit-1",
            1_700_000_000_300,
            "winner-hash",
            "output-digest",
            &serde_json::json!({"winner":"cand-1"}),
            &[101, 202],
            "policy-snapshot",
            task_type,
            "input-digest",
            "output-schema-digest",
            "policy-id",
            "policy-params-digest",
        )
        .expect("put decision memory");
    store
        .upsert_evidence_summary(
            "cid-1",
            "application/json",
            111,
            "source-hint",
            1_700_000_000_250,
        )
        .expect("upsert evidence summary");

    let reject_codes = vec![7_u16, 8_u16];
    store
        .upsert_runtime_metric_observation(&wattswarm_storage_core::RuntimeMetricObservation {
            runtime_id: "rt",
            profile_id: "default",
            task_type,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
            finalized: true,
            timeout: false,
            crash: false,
            invalid_output: false,
            latency_ms: 120,
            cost_units: 9,
            reject_reason_codes: &reject_codes,
        })
        .expect("upsert runtime metric");

    store
        .put_task_settlement(task_id, 1, 1_700_000_001_200, 1_700_000_001_900)
        .expect("put settlement");
    store
        .mark_task_bad_feedback(task_id, 1, 1_700_000_001_500)
        .expect("mark bad feedback");
    store
        .upsert_task_cost_report(
            task_id,
            1,
            &serde_json::json!({"explore": 1, "verify": 2}),
            &serde_json::json!({"explore_ms": 30, "verify_ms": 50}),
            10,
            5,
            0.25,
        )
        .expect("upsert task cost");
    store
        .adjust_reputation("rt", "default", 12_345, -600, 1_700_000_002_000)
        .expect("adjust reputation");
    store
        .put_knowledge_lookup(
            task_id,
            task_type,
            "input-digest",
            1_700_000_002_500,
            1,
            "hits-digest",
            true,
        )
        .expect("put knowledge lookup");

    store
        .put_advisory_created("adv-1", "policy-id", "hash-a", 1_700_000_003_000)
        .expect("advisory created");
    store
        .mark_advisory_approved("adv-1", "admin-1", 1_700_000_003_500)
        .expect("advisory approved");
    store
        .mark_advisory_applied("adv-1", "hash-b", 1_700_000_004_000)
        .expect("advisory applied");

    let advisory: AdvisoryStateRow = store
        .get_advisory_state("adv-1")
        .expect("get advisory state")
        .expect("advisory exists");
    assert_eq!(advisory.status, "applied");
    assert_eq!(advisory.approved_by.as_deref(), Some("admin-1"));

    store
        .record_unknown_reason_observation(
            Some(task_id),
            901,
            "0.0.9",
            "0.1.0",
            "peer-2",
            1_700_000_004_500,
        )
        .expect("record unknown reason");

    assert!(
        store
            .get_vote_commit(task_id, "voter-a")
            .expect("empty vote commit")
            .is_none()
    );
    store
        .put_vote_commit(
            task_id,
            "voter-a",
            "cand-hash",
            "commit-hash",
            "vrh",
            "exec-1",
            1_700_000_005_000,
        )
        .expect("put vote commit");
    let commit = store
        .get_vote_commit(task_id, "voter-a")
        .expect("get vote commit")
        .expect("vote commit exists");
    assert_eq!(commit.execution_id, "exec-1");

    let reveal = wattswarm_storage_core::VoteRevealRow {
        task_id: task_id.to_owned(),
        voter_node_id: "voter-a".to_owned(),
        candidate_id: "cand-1".to_owned(),
        candidate_hash: "cand-hash".to_owned(),
        vote: VoteChoice::Approve,
        salt: "salt-1".to_owned(),
        verifier_result_hash: "vrh".to_owned(),
        valid: true,
        created_at: 1_700_000_005_100,
    };
    store.put_vote_reveal(&reveal).expect("put vote reveal");

    assert_eq!(
        store
            .count_valid_votes_for_candidate(task_id, "cand-1", VoteChoice::Approve)
            .expect("count valid approve"),
        1
    );
    assert_eq!(
        store
            .count_valid_votes_for_candidate(task_id, "cand-1", VoteChoice::Reject)
            .expect("count valid reject"),
        0
    );
    assert_eq!(
        store
            .count_valid_votes_for_candidate_hash(task_id, "cand-hash", VoteChoice::Approve)
            .expect("count by hash"),
        1
    );

    store
        .put_finalization(task_id, 1, "cand-1", "{\"sig\":\"ok\"}", "evt-final-1")
        .expect("put finalization");
    assert_eq!(
        store
            .get_finalization_candidate(task_id, 1)
            .expect("get finalization candidate")
            .as_deref(),
        Some("cand-1")
    );

    assert_eq!(
        store.latest_checkpoint_seq().expect("empty checkpoint"),
        None
    );
    store
        .put_checkpoint("cp-1", 10, "evt-10")
        .expect("put checkpoint 1");
    store
        .put_checkpoint("cp-2", 20, "evt-20")
        .expect("put checkpoint 2");
    assert_eq!(
        store.latest_checkpoint_seq().expect("latest checkpoint"),
        Some(20)
    );

    assert!(
        store
            .load_membership()
            .expect("load membership empty")
            .is_none()
    );
    store
        .put_membership("{\"members\":{}}")
        .expect("put membership");
    assert_eq!(
        store.load_membership().expect("load membership").as_deref(),
        Some("{\"members\":{}}")
    );

    let export_task = store
        .export_knowledge_by_task(task_id)
        .expect("export by task");
    assert_eq!(
        export_task["decision_memory"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["runtime_metrics"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["task_settlement"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["task_cost_reports"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["knowledge_lookups"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["unknown_reason_observations"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["advisory_state"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["reputation_state"][0]["stability_reputation_decimal"],
        "1.2345"
    );

    let export_task_type = store
        .export_knowledge_by_task_type(task_type)
        .expect("export by task type");
    assert_eq!(export_task_type["task_type"], task_type);
    assert_eq!(
        export_task_type["knowledge_lookups"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task_type["runtime_metrics"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task_type["unknown_reason_observations"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );

    let decision_event = sample_event(
        "evt-decision",
        "0.1.0",
        "node-a",
        1,
        1_700_000_000_400,
        EventPayload::DecisionCommitted(DecisionCommittedPayload {
            task_id: task_id.to_owned(),
            epoch: 1,
            candidate_id: "cand-1".to_owned(),
            candidate_hash: "cand-hash".to_owned(),
        }),
    );
    let checkpoint_event = sample_event(
        "evt-checkpoint",
        "0.1.0",
        "node-a",
        1,
        1_700_000_000_500,
        EventPayload::CheckpointCreated(CheckpointCreatedPayload {
            checkpoint_id: "cp-2".to_owned(),
            up_to_seq: 20,
        }),
    );
    store
        .append_event(&decision_event)
        .expect("append decision event");
    store
        .append_event(&checkpoint_event)
        .expect("append checkpoint event");

    let candidate = sample_candidate("cand-1", "exec-1");
    store
        .put_candidate(task_id, "node-a", &candidate)
        .expect("put candidate");
    store
        .put_verifier_result(
            task_id,
            "verifier-a",
            &sample_verifier_result("cand-1", "exec-1", true),
        )
        .expect("put verifier result");
}
