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

fn candidate_output_ref(
    candidate_id: &str,
    output: &serde_json::Value,
) -> wattswarm::types::ArtifactRef {
    let bytes = serde_json::to_vec(output).expect("serialize output");
    let digest = sha256_hex(&bytes);
    wattswarm::types::ArtifactRef {
        uri: format!("artifact://reference/{candidate_id}"),
        digest: format!("sha256:{digest}"),
        size_bytes: bytes.len() as u64,
        mime: "application/json".to_owned(),
        created_at: 1,
        producer: "test-producer".to_owned(),
    }
}

fn make_candidate(
    candidate_id: &str,
    execution_id: &str,
    output: serde_json::Value,
    evidence_inline: Vec<wattswarm::types::InlineEvidence>,
    evidence_refs: Vec<wattswarm::types::ArtifactRef>,
) -> Candidate {
    Candidate {
        candidate_id: candidate_id.to_owned(),
        execution_id: execution_id.to_owned(),
        output_ref: candidate_output_ref(candidate_id, &output),
        output,
        evidence_inline,
        evidence_refs,
    }
}

fn seed_exact_decision_memory(
    node: &Node,
    history_task_id: &str,
    contract: &TaskContract,
    summary: serde_json::Value,
) {
    let policy_snapshot_digest =
        sha256_hex(&serde_json::to_vec(&contract.acceptance.verifier_policy).unwrap());
    let input_digest = sha256_hex(&serde_json::to_vec(&contract.inputs).unwrap());
    let output_schema_digest = sha256_hex(&serde_json::to_vec(&contract.output_schema).unwrap());
    let policy_params_digest = sha256_hex(
        &serde_json::to_vec(&contract.acceptance.verifier_policy.policy_params).unwrap(),
    );
    node.store
        .put_decision_memory(
            history_task_id,
            1,
            &format!("final-{history_task_id}"),
            10,
            "winner-hash",
            "output-digest",
            &summary,
            &serde_json::json!({"approvals": 1, "rejects": 0, "quorum_threshold": 1}),
            &[REASON_SCHEMA_OK],
            &serde_json::json!({"status":"FINALIZED"}),
            &policy_snapshot_digest,
            &contract.task_type,
            &input_digest,
            &output_schema_digest,
            &contract.acceptance.verifier_policy.policy_id,
            &policy_params_digest,
        )
        .unwrap();
}

#[path = "wattswarm_integration/ws_integration_epoch_tests.rs"]
mod ws_integration_epoch_tests;
#[path = "wattswarm_integration/ws_integration_execution_tests.rs"]
mod ws_integration_execution_tests;
#[path = "wattswarm_integration/ws_integration_governance_tests.rs"]
mod ws_integration_governance_tests;
#[path = "wattswarm_integration/ws_integration_knowledge_tests.rs"]
mod ws_integration_knowledge_tests;
#[path = "wattswarm_integration/ws_integration_validation_tests.rs"]
mod ws_integration_validation_tests;
