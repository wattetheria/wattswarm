use crate::types::{
    Acceptance, ArtifactRef, Assignment, Budget, BudgetMode, ClaimPolicy, EvidencePolicy,
    ExploreAssignment, ExploreStopPolicy, FinalizeAssignment, MaxConcurrency, SettlementBadPenalty,
    SettlementDiminishingReturns, SettlementPolicy, TaskContract, TaskMode, VerifyAssignment,
    VotePolicy,
};

pub fn sample_contract(task_id: &str, policy_hash: String) -> TaskContract {
    TaskContract {
        protocol_version: "v0.1".to_owned(),
        task_id: task_id.to_owned(),
        task_type: "swarm".to_owned(),
        inputs: serde_json::json!({"prompt":"hello"}),
        output_schema: serde_json::json!({
            "type":"object",
            "required":["answer"],
            "properties":{"answer":{"type":"string"},"confidence":{"type":"number"},"check_summary":{"type":"string"}}
        }),
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
            verifier_policy: crate::types::PolicyBinding {
                policy_id: "vp.schema_only.v1".to_owned(),
                policy_version: "1".to_owned(),
                policy_hash,
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
                feedback: crate::types::FeedbackCapabilityPolicy {
                    mode: "CAPABILITY".to_owned(),
                    authority_pubkey: "ed25519:placeholder".to_owned(),
                },
            },
            da_quorum_threshold: 1,
        },
        task_mode: TaskMode::OneShot,
        expiry_ms: (chrono::Utc::now().timestamp_millis().max(0) as u64) + 120_000,
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

pub fn sample_artifact_ref() -> ArtifactRef {
    ArtifactRef {
        uri: "https://example.com/artifacts/1".to_owned(),
        digest: "sha256:1234".to_owned(),
        size_bytes: 123,
        mime: "application/json".to_owned(),
        created_at: chrono::Utc::now().timestamp_millis().max(0) as u64,
        producer: "runtime/profile".to_owned(),
    }
}
