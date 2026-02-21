use wattswarm_protocol::types::{
    BudgetMode, CheckpointCreatedPayload, EventKind, EventPayload, Membership, PolicyBinding, Role,
    TaskContract, TaskExpiredPayload, TaskMode, UnsignedEvent,
};

#[test]
fn membership_grant_has_role_and_holders_work() {
    let mut membership = Membership::new();
    membership.grant("node-a", Role::Proposer);
    membership.grant("node-a", Role::Finalizer);
    membership.grant("node-b", Role::Proposer);

    assert!(membership.has_role("node-a", Role::Proposer));
    assert!(membership.has_role("node-a", Role::Finalizer));
    assert!(!membership.has_role("node-b", Role::Finalizer));

    let mut proposers = membership.holders(Role::Proposer);
    proposers.sort();
    assert_eq!(proposers, vec!["node-a".to_owned(), "node-b".to_owned()]);
}

#[test]
fn event_payload_kind_and_task_id_cover_task_and_non_task_variants() {
    let task_payload = EventPayload::TaskExpired(TaskExpiredPayload {
        task_id: "task-1".to_owned(),
    });
    assert_eq!(task_payload.kind(), EventKind::TaskExpired);
    assert_eq!(task_payload.task_id(), Some("task-1"));

    let non_task_payload = EventPayload::CheckpointCreated(CheckpointCreatedPayload {
        checkpoint_id: "cp-1".to_owned(),
        up_to_seq: 10,
    });
    assert_eq!(non_task_payload.kind(), EventKind::CheckpointCreated);
    assert_eq!(non_task_payload.task_id(), None);
}

#[test]
fn unsigned_event_from_payload_derives_kind_and_task_id() {
    let payload = EventPayload::TaskExpired(TaskExpiredPayload {
        task_id: "task-from-payload".to_owned(),
    });

    let event =
        UnsignedEvent::from_payload("0.1.0".to_owned(), "node-a".to_owned(), 3, 123_456, payload);

    assert_eq!(event.event_kind, EventKind::TaskExpired);
    assert_eq!(event.task_id.as_deref(), Some("task-from-payload"));
    assert_eq!(event.author_node_id, "node-a");
    assert_eq!(event.epoch, 3);
    assert_eq!(event.created_at, 123_456);
}

#[test]
fn task_contract_deserialization_applies_expected_defaults() {
    let contract: TaskContract = serde_json::from_value(serde_json::json!({
        "protocol_version": "v0.1",
        "task_id": "task-1",
        "task_type": "resume_review",
        "inputs": {"prompt": "hello"},
        "output_schema": {"type": "object"},
        "budget": {
            "time_ms": 30_000,
            "max_steps": 10,
            "cost_units": 100,
            "reuse_verify_time_ms": 20_000,
            "reuse_verify_cost_units": 50,
            "reuse_max_attempts": 2
        },
        "assignment": {
            "mode": "CLAIM",
            "claim": {
                "lease_ms": 5_000,
                "max_concurrency": {"propose": 1, "verify": 2}
            }
        },
        "acceptance": {
            "quorum_threshold": 1,
            "verifier_policy": {
                "policy_id": "vp.schema_only.v1",
                "policy_version": "1",
                "policy_hash": "hash-1",
                "policy_params": {}
            },
            "vote": {
                "commit_reveal": true,
                "reveal_deadline_ms": 10_000
            },
            "settlement": {
                "window_ms": 86_400_000,
                "implicit_weight": 0.1,
                "implicit_diminishing_returns": {"W": 10, "K": 50},
                "bad_penalty": {"P": 3},
                "feedback": {
                    "mode": "CAPABILITY",
                    "authority_pubkey": "ed25519:placeholder"
                }
            }
        },
        "expiry_ms": 9_999_999,
        "evidence_policy": {
            "max_inline_evidence_bytes": 1024,
            "max_inline_media_bytes": 0,
            "inline_mime_allowlist": ["application/json"]
        }
    }))
    .expect("deserialize task contract with defaults");

    assert_eq!(contract.task_mode, TaskMode::OneShot);
    assert!(matches!(contract.budget.mode, BudgetMode::Lifetime));
    assert_eq!(contract.budget.explore_cost_units, 0);
    assert_eq!(contract.budget.verify_cost_units, 0);
    assert_eq!(contract.budget.finalize_cost_units, 0);
    assert_eq!(contract.assignment.explore.max_proposers, 1);
    assert_eq!(contract.assignment.explore.topk, 3);
    assert_eq!(contract.assignment.explore.stop.no_new_evidence_rounds, 3);
    assert_eq!(contract.assignment.verify.max_verifiers, 1);
    assert_eq!(contract.assignment.finalize.max_finalizers, 1);
    assert_eq!(contract.acceptance.da_quorum_threshold, 1);
    assert_eq!(contract.evidence_policy.max_snippet_bytes, 8_192);
    assert_eq!(contract.evidence_policy.max_snippet_tokens, 2_048);
}

#[test]
fn policy_binding_roundtrip_is_stable() {
    let binding = PolicyBinding {
        policy_id: "vp.schema_only.v1".to_owned(),
        policy_version: "1".to_owned(),
        policy_hash: "hash-1".to_owned(),
        policy_params: serde_json::json!({"strict": true}),
    };
    let encoded = serde_json::to_string(&binding).expect("encode policy binding");
    let decoded: PolicyBinding = serde_json::from_str(&encoded).expect("decode policy binding");
    assert_eq!(decoded, binding);
}
