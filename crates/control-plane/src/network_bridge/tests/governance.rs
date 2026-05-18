use super::*;

#[test]
fn task_announcement_event_persists_summary_and_detail_reference() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-announced-1", policy_hash);
    contract.inputs = json!({"prompt":"lightweight discovery"});
    node.submit_task(contract.clone(), 1, 90)
        .expect("submit task detail");
    node.emit_at(
        1,
        crate::types::EventPayload::TaskAnnounced(crate::types::TaskAnnouncedPayload {
            network_id: "default".to_owned(),
            task_id: "task-announced-1".to_owned(),
            announcement_id: "announce-1".to_owned(),
            feed_key: "venue.market".to_owned(),
            scope_hint: "region:sol-2".to_owned(),
            summary: json!({"reward": 42, "headline": "Explore relay beacon"}),
            detail_ref: Some(crate::types::ArtifactRef {
                uri: "ipfs://task-detail-1".to_owned(),
                digest: "digest-task-detail-1".to_owned(),
                size_bytes: 128,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: node.node_id(),
            }),
            agent_envelope: None,
        }),
        100,
    )
    .expect("announce task");

    let announcement = node
        .store
        .get_task_announcement("announce-1")
        .expect("load announcement")
        .expect("announcement exists");
    assert_eq!(announcement.task_id, "task-announced-1");
    assert_eq!(announcement.feed_key, "venue.market");
    assert_eq!(announcement.scope_hint, "region:sol-2");
    assert_eq!(
        announcement.scope(),
        Some(ProjectionScope::Region("sol-2".to_owned()))
    );
    assert_eq!(announcement.summary["reward"], json!(42));
    assert_eq!(
        announcement.detail_ref.expect("detail ref").uri,
        "ipfs://task-detail-1"
    );

    let detail = node
        .store
        .get_task_announcement_detail_for_task("task-announced-1")
        .expect("load announcement detail")
        .expect("announcement detail exists");
    assert_eq!(detail.task_id(), "task-announced-1");
    assert_eq!(
        detail.contract.as_ref().expect("task contract").task_id,
        contract.task_id
    );
    assert_eq!(
        detail.detail_ref().expect("detail ref").digest,
        "digest-task-detail-1"
    );
}

#[test]
fn execution_set_events_persist_intent_and_confirmation() {
    let mut intent_node = Node::open_in_memory_with_roles(&[]).expect("intent node");
    let participant_node_id = intent_node.node_id();
    intent_node
        .emit_at(
            1,
            crate::types::EventPayload::ExecutionIntentDeclared(
                crate::types::ExecutionIntentDeclaredPayload {
                    network_id: "default".to_owned(),
                    task_id: "task-execution-1".to_owned(),
                    execution_set_id: "exec-set-1".to_owned(),
                    participant_node_id: participant_node_id.clone(),
                    role_hint: "writer".to_owned(),
                    scope_hint: "region:sol-3".to_owned(),
                    intent: "interested".to_owned(),
                },
            ),
            100,
        )
        .expect("intent event");

    let members = intent_node
        .store
        .list_execution_set_members("task-execution-1", "exec-set-1")
        .expect("members after intent");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].status, "interested");
    assert_eq!(
        members[0].scope(),
        Some(ProjectionScope::Region("sol-3".to_owned()))
    );

    let mut confirmer = Node::open_in_memory_with_roles(&[Role::Committer]).expect("confirmer");
    let confirmed_by_node_id = confirmer.node_id();
    confirmer
        .emit_at(
            1,
            crate::types::EventPayload::ExecutionSetConfirmed(
                crate::types::ExecutionSetConfirmedPayload {
                    network_id: "default".to_owned(),
                    task_id: "task-execution-1".to_owned(),
                    execution_set_id: "exec-set-1".to_owned(),
                    confirmed_by_node_id: confirmed_by_node_id.clone(),
                    scope_hint: "region:sol-3".to_owned(),
                    members: vec![crate::types::ExecutionSetMember {
                        participant_node_id: "peer-a".to_owned(),
                        role_hint: "writer".to_owned(),
                    }],
                },
            ),
            101,
        )
        .expect("confirmation event");

    let members = confirmer
        .store
        .list_execution_set_members("task-execution-1", "exec-set-1")
        .expect("members after confirmation");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].status, "confirmed");
    assert_eq!(
        members[0].scope(),
        Some(ProjectionScope::Region("sol-3".to_owned()))
    );
    assert_eq!(
        members[0].confirmed_by_node_id.as_deref(),
        Some(confirmed_by_node_id.as_str())
    );
}

#[test]
fn rule_and_checkpoint_gossip_are_applied_to_projection_store() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");

    apply_rule_announcement(
        &mut node,
        &crate::network_p2p::RuleAnnouncement {
            scope: SwarmScope::Region("sol-4".to_owned()),
            rule_set: "market-routing".to_owned(),
            rule_version: 3,
            activation_epoch: Some(9),
            authority_signer_node_id: None,
            authority_signature_hex: None,
        },
    )
    .expect("apply rule");
    apply_checkpoint_announcement(
        &mut node,
        &crate::network_p2p::CheckpointAnnouncement {
            scope: SwarmScope::Region("sol-4".to_owned()),
            checkpoint_id: "cp-sol-4".to_owned(),
            artifact_path: "ipfs://checkpoint-sol-4".to_owned(),
            authority_signer_node_id: None,
            authority_signature_hex: None,
        },
    )
    .expect("apply checkpoint");

    let rule = node
        .store
        .latest_rule_announcement("region.sol-4", "market-routing")
        .expect("load rule")
        .expect("rule exists");
    assert_eq!(rule.rule_version, 3);
    assert_eq!(rule.activation_epoch, Some(9));

    let checkpoint = node
        .store
        .get_checkpoint_announcement("region.sol-4", "cp-sol-4")
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.artifact_path, "ipfs://checkpoint-sol-4");
}

#[test]
fn mainnet_global_rule_and_checkpoint_require_genesis_authority() {
    let genesis = NodeIdentity::random();
    let genesis_node_id = genesis.node_id();
    let non_genesis = NodeIdentity::random();
    let mut node = mainnet_node(genesis);

    let unsigned_rule = crate::network_p2p::RuleAnnouncement {
        scope: SwarmScope::Global,
        rule_set: "mainnet-routing".to_owned(),
        rule_version: 1,
        activation_epoch: Some(1),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    };
    assert!(apply_rule_announcement(&mut node, &unsigned_rule).is_err());

    let signed_rule = announcements::sign_rule_announcement(&node, unsigned_rule)
        .expect("sign rule")
        .expect("genesis signs rule");
    apply_rule_announcement(&mut node, &signed_rule).expect("signed rule applies");

    let unsigned_checkpoint = crate::network_p2p::CheckpointAnnouncement {
        scope: SwarmScope::Global,
        checkpoint_id: "mainnet-cp-1".to_owned(),
        artifact_path: "finality://mainnet/task/1/candidate".to_owned(),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    };
    assert!(apply_checkpoint_announcement(&mut node, &unsigned_checkpoint).is_err());

    let signed_checkpoint = announcements::sign_checkpoint_announcement(&node, unsigned_checkpoint)
        .expect("sign checkpoint")
        .expect("genesis signs checkpoint");
    apply_checkpoint_announcement(&mut node, &signed_checkpoint)
        .expect("signed checkpoint applies");

    let non_genesis_node = mainnet_node_with_genesis(non_genesis, &genesis_node_id);
    let non_genesis_rule = crate::network_p2p::RuleAnnouncement {
        scope: SwarmScope::Global,
        rule_set: "mainnet-routing".to_owned(),
        rule_version: 2,
        activation_epoch: Some(2),
        authority_signer_node_id: None,
        authority_signature_hex: None,
    };
    assert!(
        announcements::sign_rule_announcement(&non_genesis_node, non_genesis_rule)
            .expect("non-genesis auth check")
            .is_none()
    );
}

#[test]
fn mainnet_governance_events_require_genesis_author() {
    let genesis = NodeIdentity::random();
    let non_genesis = NodeIdentity::random();
    let mut node = mainnet_node(genesis.clone());

    let mut new_membership = Membership::new();
    new_membership.grant(&non_genesis.node_id(), Role::Verifier);
    let membership_event = build_event_for_external(
        &genesis,
        1,
        9,
        crate::types::EventPayload::MembershipUpdated(crate::types::MembershipUpdatedPayload {
            new_membership: new_membership.clone(),
            quorum_threshold: 1,
            quorum_signatures: vec![
                sign_membership_quorum(&genesis, &new_membership).expect("genesis quorum sig"),
            ],
        }),
    )
    .expect("genesis membership update");
    node.ingest_remote(membership_event)
        .expect("genesis membership update applies on empty mainnet membership");

    let non_genesis_event = build_event_for_external(
        &non_genesis,
        1,
        10,
        crate::types::EventPayload::NodePenalized(crate::types::NodePenalizedPayload {
            penalized_node_id: "node-bad".to_owned(),
            reason: "bad summary".to_owned(),
            revoked_event_ids: Vec::new(),
            revoked_summary_ids: vec!["summary-bad".to_owned()],
            block_summaries: true,
        }),
    )
    .expect("non-genesis event");
    assert!(node.ingest_remote(non_genesis_event).is_err());

    let genesis_event = build_event_for_external(
        &genesis,
        1,
        11,
        crate::types::EventPayload::NodePenalized(crate::types::NodePenalizedPayload {
            penalized_node_id: "node-bad".to_owned(),
            reason: "bad summary".to_owned(),
            revoked_event_ids: Vec::new(),
            revoked_summary_ids: vec!["summary-bad".to_owned()],
            block_summaries: true,
        }),
    )
    .expect("genesis event");
    node.ingest_remote(genesis_event)
        .expect("genesis governance event applies without membership role");

    let policy_tuned_event = build_event_for_external(
        &genesis,
        1,
        12,
        crate::types::EventPayload::PolicyTuned(crate::types::PolicyTunedPayload {
            policy_id: "vp.schema_only.v1".to_owned(),
            from_policy_hash: "policy:old".to_owned(),
            to_policy_hash: "policy:new".to_owned(),
            advisory_id: "advisory-mainnet-policy".to_owned(),
        }),
    )
    .expect("policy tuned event");
    let err = node
        .ingest_remote(policy_tuned_event)
        .expect_err("policy tuning still uses membership governance path");
    assert!(err.to_string().contains("author lacks role Finalizer"));
}

#[test]
fn ordinary_task_and_topic_events_do_not_require_membership_roles() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let mut node = Node::new(
        local,
        PgStore::open_in_memory().expect("store"),
        Membership::new(),
    )
    .expect("node");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TaskCreated(sample_contract("task-no-role", policy_hash)),
    )
    .expect("task event");
    node.ingest_remote(task_event)
        .expect("task event applies without membership role");

    let topic_event = build_event_for_external(
        &remote,
        1,
        11,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "agent.chat".to_owned(),
            scope_hint: "global".to_owned(),
            content_ref: sample_topic_content_ref("sha256:no-role-topic", &remote.node_id()),
            local_content_cache: None,
            reply_to_message_id: None,
        }),
    )
    .expect("topic event");
    node.ingest_remote(topic_event)
        .expect("topic event applies without membership role");
}

#[test]
fn ordinary_task_finalization_does_not_require_membership_finalizer_role() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let mut node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        Membership::new(),
    )
    .expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let contract = sample_contract("task-finality-no-role", policy_hash);
    node.submit_task(contract, 1, 10).expect("submit task");
    node.claim_task(
        "task-finality-no-role",
        ClaimRole::Propose,
        "exec-propose-no-role",
        100,
        1,
        11,
    )
    .expect("claim propose");
    let output = json!({"answer": "ok"});
    let output_bytes = serde_json::to_vec(&output).expect("candidate output bytes");
    let output_digest = format!("sha256:{}", sha256_hex(&output_bytes));
    let candidate = crate::types::Candidate {
        candidate_id: "cand-finality-no-role".to_owned(),
        execution_id: "exec-propose-no-role".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: format!("inline://candidate-output/{output_digest}"),
            digest: output_digest,
            size_bytes: output_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 12,
            producer: node_id,
        },
        output,
        evidence_inline: Vec::new(),
        evidence_refs: Vec::new(),
    };
    let candidate_hash = candidate_hash(&candidate).expect("candidate hash");
    node.propose_candidate("task-finality-no-role", candidate, 1, 12)
        .expect("propose candidate");
    node.claim_task(
        "task-finality-no-role",
        ClaimRole::Verify,
        "exec-verify-no-role",
        100,
        1,
        13,
    )
    .expect("claim verify");
    let verifier_result_hash = "sha256:verify-finality-no-role".to_owned();
    let salt = "salt-finality-no-role".to_owned();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, &salt, &verifier_result_hash);
    node.submit_vote_commit(
        VoteCommitPayload {
            task_id: "task-finality-no-role".to_owned(),
            candidate_id: "cand-finality-no-role".to_owned(),
            candidate_hash: candidate_hash.clone(),
            execution_id: "exec-verify-no-role".to_owned(),
            verifier_result_hash: verifier_result_hash.clone(),
            commit_hash,
        },
        1,
        14,
    )
    .expect("vote commit");
    node.submit_vote_reveal(
        VoteRevealPayload {
            task_id: "task-finality-no-role".to_owned(),
            candidate_id: "cand-finality-no-role".to_owned(),
            candidate_hash,
            execution_id: "exec-verify-no-role".to_owned(),
            verifier_result_hash,
            vote: VoteChoice::Approve,
            salt,
        },
        1,
        15,
    )
    .expect("vote reveal");
    node.commit_decision("task-finality-no-role", 1, "cand-finality-no-role", 16)
        .expect("commit decision");
    node.finalize_decision(
        "task-finality-no-role",
        1,
        "cand-finality-no-role",
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(
                &identity,
                "task-finality-no-role",
                1,
                "cand-finality-no-role",
            )],
        },
        17,
    )
    .expect("finalize without membership finalizer role");
}

#[test]
fn mainnet_non_genesis_finalization_does_not_break_outcome_summary_generation() {
    let genesis = NodeIdentity::random();
    let non_genesis = NodeIdentity::random();
    let mut node = mainnet_node_with_genesis(non_genesis.clone(), &genesis.node_id());
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let contract = sample_contract("task-mainnet-non-genesis-summary", policy_hash);
    node.submit_task(contract, 1, 10).expect("submit task");
    node.claim_task(
        "task-mainnet-non-genesis-summary",
        ClaimRole::Propose,
        "exec-mainnet-non-genesis-summary",
        100,
        1,
        11,
    )
    .expect("claim propose");
    let output = json!({"answer": "ok"});
    let output_bytes = serde_json::to_vec(&output).expect("candidate output bytes");
    let output_digest = format!("sha256:{}", sha256_hex(&output_bytes));
    let candidate = crate::types::Candidate {
        candidate_id: "cand-mainnet-non-genesis-summary".to_owned(),
        execution_id: "exec-mainnet-non-genesis-summary".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: format!("inline://candidate-output/{output_digest}"),
            digest: output_digest,
            size_bytes: output_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 12,
            producer: non_genesis.node_id(),
        },
        output,
        evidence_inline: Vec::new(),
        evidence_refs: Vec::new(),
    };
    let winning_candidate_hash = candidate_hash(&candidate).expect("candidate hash");
    node.propose_candidate("task-mainnet-non-genesis-summary", candidate, 1, 12)
        .expect("propose candidate");

    let finalized = build_event_for_external(
        &non_genesis,
        1,
        13,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: "task-mainnet-non-genesis-summary".to_owned(),
            epoch: 1,
            candidate_id: "cand-mainnet-non-genesis-summary".to_owned(),
            winning_candidate_hash,
            finality_proof: FinalityProof {
                threshold: 1,
                signatures: vec![finality_sign(
                    &non_genesis,
                    "task-mainnet-non-genesis-summary",
                    1,
                    "cand-mainnet-non-genesis-summary",
                )],
            },
            agent_envelope: None,
        }),
    )
    .expect("finalized event");
    let summary = task_outcome_summary_for_event(&node, &finalized, &SwarmScope::Global)
        .expect("non-genesis mainnet finalization does not error");
    assert!(summary.is_none());
}

#[test]
fn administrative_task_lifecycle_events_require_roles() {
    let identity = NodeIdentity::random();
    let mut node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        Membership::new(),
    )
    .expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    node.submit_task(sample_contract("task-admin-no-role", policy_hash), 1, 10)
        .expect("submit task");

    let retry_event = build_event_for_external(
        &identity,
        1,
        11,
        crate::types::EventPayload::TaskRetryScheduled(crate::types::TaskRetryScheduledPayload {
            task_id: "task-admin-no-role".to_owned(),
            attempt: 1,
            run_at: 12,
            agent_envelope: None,
        }),
    )
    .expect("retry event");
    let retry_err = node
        .ingest_remote(retry_event)
        .expect_err("retry requires committer");
    assert!(
        retry_err
            .to_string()
            .contains("author lacks role Committer")
    );

    for payload in [
        crate::types::EventPayload::EpochEnded(crate::types::EpochEndedPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: crate::types::EpochEndReason::TimeboxReached,
        }),
        crate::types::EventPayload::TaskStopped(crate::types::TaskStoppedPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: "admin stop".to_owned(),
        }),
        crate::types::EventPayload::TaskSuspended(crate::types::TaskSuspendedPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: "admin suspend".to_owned(),
        }),
        crate::types::EventPayload::TaskKilled(crate::types::TaskKilledPayload {
            task_id: "task-admin-no-role".to_owned(),
            epoch: 1,
            reason: "admin kill".to_owned(),
        }),
    ] {
        let event = build_event_for_external(&identity, 1, 12, payload).expect("admin event");
        let err = node
            .ingest_remote(event)
            .expect_err("admin lifecycle event requires finalizer");
        assert!(err.to_string().contains("author lacks role Finalizer"));
    }
}

#[test]
fn lan_open_does_not_seed_default_protocol_roles() {
    let local_dir = temp_startup_dir("local-default-roles");
    let local_node = crate::control::open_node_in_mode(
        &local_dir,
        &local_dir.join("node.state"),
        crate::control::NodeMode::Local,
    )
    .expect("open local node");
    let local_raw = local_node
        .store
        .load_membership()
        .expect("load local membership")
        .expect("local membership");
    let local_membership: Membership =
        serde_json::from_str(&local_raw).expect("local membership json");
    assert!(local_membership.has_role(&local_node.node_id(), Role::Proposer));
    assert!(local_membership.has_role(&local_node.node_id(), Role::Verifier));
    assert!(local_membership.has_role(&local_node.node_id(), Role::Committer));
    assert!(local_membership.has_role(&local_node.node_id(), Role::Finalizer));

    let lan_dir = temp_startup_dir("lan-no-default-roles");
    let lan_node = crate::control::open_node_in_mode(
        &lan_dir,
        &lan_dir.join("node.state"),
        crate::control::NodeMode::Lan,
    )
    .expect("open lan node");
    let lan_raw = lan_node
        .store
        .load_membership()
        .expect("load lan membership")
        .expect("lan membership");
    let lan_membership: Membership = serde_json::from_str(&lan_raw).expect("lan membership json");
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Proposer));
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Verifier));
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Committer));
    assert!(!lan_membership.has_role(&lan_node.node_id(), Role::Finalizer));
}

#[test]
fn network_config_ignores_legacy_bootstrap_peers_env() {
    let _lock = lock_env_test_mutex();
    let _bootstrap = EnvVarGuard::set(
        "WATTSWARM_P2P_BOOTSTRAP_PEERS",
        Some("node-a@127.0.0.1:4001"),
    );
    let config = network_config_from_env();
    assert!(config.bootstrap_peers.is_empty());
}

#[test]
fn network_config_does_not_read_bootstrap_contacts_as_legacy_peers() {
    let _lock = lock_env_test_mutex();
    let _bootstrap = EnvVarGuard::set("WATTSWARM_P2P_BOOTSTRAP_PEERS", None);
    let dir = temp_startup_dir("startup-bootstrap");
    fs::write(
        dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "bootstrap_contacts": [
                "{\"transport\":\"iroh_direct\",\"peer_id\":\"node-a\",\"metadata\":{\"route\":\"iroh_direct\",\"generated_at\":1,\"endpoint_id\":\"node-a\",\"alpn\":\"/wattswarm/iroh/1\",\"listen_addrs\":[\"127.0.0.1:4002\"],\"capabilities\":{\"supports_iroh_direct\":true,\"supports_streaming\":true,\"max_recommended_inline_bytes\":16384,\"preferred_data_route\":\"iroh_direct\"}},\"extra\":{\"endpoint_id\":\"node-a\",\"alpn\":\"/wattswarm/iroh/1\",\"direct_addrs\":[\"127.0.0.1:4002\"],\"relay_urls\":[]}}"
            ]
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");

    let config = network_config_from_state_dir(&dir);
    assert!(config.bootstrap_peers.is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn global_publish_rate_guard_limits_only_high_frequency_global_events() {
    let mut guard = GlobalPublishRateGuard::new(Instant::now());
    let event = build_event_for_external(
        &NodeIdentity::random(),
        1,
        10,
        crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
            task_id: "task-rate-limit".to_owned(),
            role: crate::types::ClaimRole::Propose,
            claimer_node_id: "node-a".to_owned(),
            execution_id: "exec-1".to_owned(),
            lease_until: 20,
            agent_envelope: None,
        }),
    )
    .expect("event");

    for _ in 0..GLOBAL_HIGH_FREQUENCY_LIMIT {
        assert!(guard.allow(&SwarmScope::Global, &event));
    }
    assert!(!guard.allow(&SwarmScope::Global, &event));
    assert!(guard.allow(&SwarmScope::Node("lab-1".to_owned()), &event));
}
