use super::*;

#[test]
fn summary_publish_is_suppressed_when_backlog_is_high() {
    assert!(should_publish_summaries(
        SUMMARY_BACKPRESSURE_HIGH_WATERMARK,
        0
    ));
    assert!(!should_publish_summaries(
        SUMMARY_BACKPRESSURE_HIGH_WATERMARK + 1,
        0
    ));
}

#[test]
fn apply_summary_announcement_imports_knowledge_and_reputation() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Region("sol-1".to_owned()),
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-a".to_owned(),
                task_type: "summary-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-knowledge-summary".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-1".to_owned(),
                    winning_candidate_hash: "candidate-hash-1".to_owned(),
                    output_digest: "output-digest-1".to_owned(),
                    result_summary: json!({"answer":"summary-import"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"ok"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "summary-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 100,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply knowledge summary");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-reputation-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::ReputationSummaryBundle {
                source_node_id: "node-a".to_owned(),
                entries: vec![crate::storage::ReputationSnapshotRow {
                    runtime_id: "runtime-a".to_owned(),
                    profile_id: "model-a".to_owned(),
                    stability_reputation: 10,
                    quality_reputation: 20,
                    last_updated_at: 101,
                }],
            })
            .expect("reputation payload"),
        },
    )
    .expect("apply reputation summary");

    let hits = node
        .store
        .list_decision_memory_hits_by_task_type("summary-type", 8)
        .expect("list hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].result_summary["answer"], json!("summary-import"));

    let reputation = node
        .store
        .get_reputation_snapshot("runtime-a", "model-a")
        .expect("get reputation")
        .expect("reputation row");
    assert_eq!(reputation.stability_reputation, 10);
    assert_eq!(reputation.quality_reputation, 20);
}

#[test]
fn subnet_summary_is_mirrored_into_parent_network_store() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let base_store = PgStore::open_in_memory().expect("store");
    let mainnet = base_store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = base_store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let subnet_store = base_store.for_org(&subnet.org.org_id);
    let mut node = Node::new(identity, subnet_store, membership).expect("subnet node");
    node.store
        .put_decision_memory(
            "task-parent-uplink",
            1,
            "commit-parent-uplink",
            100,
            "candidate-parent-uplink",
            "digest-parent-uplink",
            &json!({"answer":"compress-me"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"ok"}),
            "policy-snap",
            "uplink-type",
            "input-digest",
            "schema-digest",
            "vp.schema_only.v1",
            "params-digest",
        )
        .expect("put decision memory");

    let summary = build_knowledge_summary_for_task_type(&node, &SwarmScope::Global, "uplink-type")
        .expect("build summary")
        .expect("summary exists");
    assert!(
        mirror_summary_to_parent_network(&node, &summary).expect("mirror summary"),
        "subnet summary should mirror to parent network"
    );

    let parent_store = base_store.for_org(&mainnet.org.org_id);
    let hits = parent_store
        .list_decision_memory_hits_by_task_type("uplink-type", 8)
        .expect("list mirrored hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].result_summary["answer"], json!("compress-me"));

    let revoke_event = node
        .revoke_summary(
            &summary.summary_id,
            KNOWLEDGE_SUMMARY_KIND,
            "bad subnet summary",
            1,
            200,
        )
        .expect("revoke summary");
    assert!(
        mirror_summary_controls_to_parent_network(&node, &revoke_event)
            .expect("mirror revoke to parent"),
        "summary revoke should mirror to parent network"
    );
    assert!(
        parent_store
            .list_decision_memory_hits_by_task_type("uplink-type", 8)
            .expect("list mirrored hits after revoke")
            .is_empty()
    );
}

#[test]
fn task_outcome_summary_imports_compressed_result_facts() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let store = PgStore::open_in_memory().expect("store");
    let mut node = Node::new(identity.clone(), store, membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-outcome-summary", policy_hash);
    contract.task_type = "cross-layer-type".to_owned();
    node.store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-outcome".to_owned(),
        execution_id: "exec-outcome".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: "artifact://reference/cand-outcome".to_owned(),
            digest: "sha256:cand-outcome".to_owned(),
            size_bytes: 90,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        },
        output: json!({
            "answer": "compressed answer",
            "decision": "approve",
            "confidence": 0.98,
            "check_summary": "ok",
            "raw": {"large":"payload"}
        }),
        evidence_inline: vec![],
        evidence_refs: vec![crate::types::ArtifactRef {
            uri: "ipfs://evidence-1".to_owned(),
            digest: "sha256:evidence-1".to_owned(),
            size_bytes: 32,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        }],
    };
    node.store
        .put_candidate(&contract.task_id, &node_id, &candidate)
        .expect("put candidate");
    let winning_candidate_hash = crate::crypto::candidate_hash(&candidate).expect("candidate hash");
    let finalized = build_event_for_external(
        &identity,
        1,
        100,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: contract.task_id.clone(),
            epoch: 1,
            candidate_id: candidate.candidate_id.clone(),
            winning_candidate_hash,
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: vec![crate::types::SignatureEnvelope {
                    signer_node_id: node_id.clone(),
                    signature_hex: "sig-1".to_owned(),
                }],
            },
            agent_envelope: None,
        }),
    )
    .expect("finalized event");

    let summary = summary::task_outcome_summary_for_event(
        &node,
        &finalized,
        &SwarmScope::Region("sol-1".to_owned()),
    )
    .expect("build outcome summary")
    .expect("outcome summary exists");
    apply_summary_announcement(&mut node, &summary).expect("apply outcome summary");

    let outcomes = node
        .store
        .list_imported_task_outcomes_by_task_type("cross-layer-type", 8)
        .expect("list imported outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].task_id, "task-outcome-summary");
    assert_eq!(outcomes[0].candidate_id, "cand-outcome");
    assert_eq!(
        outcomes[0].result_summary["answer"],
        json!("compressed answer")
    );
    assert_eq!(outcomes[0].result_summary["check_summary"], json!("ok"));
    assert_eq!(outcomes[0].evidence_digest_count, 1);
    assert_eq!(outcomes[0].scope_hint, "region:sol-1");
}

#[test]
fn subnet_outcome_summary_and_checkpoint_anchor_mirror_to_parent_network() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let base_store = PgStore::open_in_memory().expect("store");
    let mainnet = base_store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = base_store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let subnet_store = base_store.for_org(&subnet.org.org_id);
    let mut node = Node::new(identity.clone(), subnet_store, membership).expect("subnet node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-parent-outcome", policy_hash);
    contract.task_type = "parent-outcome-type".to_owned();
    node.store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-parent-outcome".to_owned(),
        execution_id: "exec-parent-outcome".to_owned(),
        output_ref: crate::types::ArtifactRef {
            uri: "artifact://reference/cand-parent-outcome".to_owned(),
            digest: "sha256:cand-parent-outcome".to_owned(),
            size_bytes: 64,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        },
        output: json!({"answer":"uplinked","decision":"approve","check_summary":"compressed"}),
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.store
        .put_candidate(&contract.task_id, &node_id, &candidate)
        .expect("put candidate");
    let winning_candidate_hash = crate::crypto::candidate_hash(&candidate).expect("candidate hash");
    let finalized = build_event_for_external(
        &identity,
        1,
        120,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: contract.task_id.clone(),
            epoch: 1,
            candidate_id: candidate.candidate_id.clone(),
            winning_candidate_hash,
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: vec![crate::types::SignatureEnvelope {
                    signer_node_id: node_id.clone(),
                    signature_hex: "sig-parent".to_owned(),
                }],
            },
            agent_envelope: None,
        }),
    )
    .expect("finalized event");
    let scope = SwarmScope::Global;
    let summary = summary::task_outcome_summary_for_event(&node, &finalized, &scope)
        .expect("build outcome summary")
        .expect("outcome summary exists");
    let checkpoint = announcements::checkpoint_announcement_for_event(&node, &finalized, &scope)
        .expect("build checkpoint announcement")
        .expect("checkpoint announcement exists");

    assert!(
        mirror_summary_to_parent_network(&node, &summary).expect("mirror outcome summary"),
        "subnet outcome summary should mirror to parent network"
    );
    assert!(
        announcements::mirror_checkpoint_to_parent_network(&node, &checkpoint)
            .expect("mirror checkpoint"),
        "subnet checkpoint anchor should mirror to parent network"
    );

    let parent_store = base_store.for_org(&mainnet.org.org_id);
    let outcomes = parent_store
        .list_imported_task_outcomes_by_task_type("parent-outcome-type", 8)
        .expect("list mirrored outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].result_summary["answer"], json!("uplinked"));
    assert_eq!(outcomes[0].checkpoint_id, checkpoint.checkpoint_id);
    let mirrored_checkpoint = parent_store
        .get_checkpoint_announcement("global", &checkpoint.checkpoint_id)
        .expect("load mirrored checkpoint")
        .expect("mirrored checkpoint exists");
    assert_eq!(mirrored_checkpoint.artifact_path, checkpoint.artifact_path);

    let revoke_event = node
        .revoke_summary(
            &summary.summary_id,
            TASK_OUTCOME_SUMMARY_KIND,
            "bad outcome summary",
            1,
            200,
        )
        .expect("revoke outcome summary");
    assert!(
        mirror_summary_controls_to_parent_network(&node, &revoke_event)
            .expect("mirror outcome revoke to parent"),
        "outcome summary revoke should mirror to parent network"
    );
    assert!(
        parent_store
            .list_imported_task_outcomes_by_task_type("parent-outcome-type", 8)
            .expect("list mirrored outcomes after revoke")
            .is_empty()
    );
}

#[test]
fn knowledge_summary_builder_respects_protocol_limit() {
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    for task_id in ["task-dm-1", "task-dm-2"] {
        node.store
            .put_decision_memory(
                task_id,
                1,
                &format!("commit-{task_id}"),
                100,
                &format!("candidate-{task_id}"),
                &format!("digest-{task_id}"),
                &json!({"quorum":"ok"}),
                &json!({"answer": task_id}),
                &[100],
                &json!({"detail":"ok"}),
                "policy-snap",
                "limit-type",
                "input-digest",
                "schema-digest",
                "vp.schema_only.v1",
                "params-digest",
            )
            .expect("put decision memory");
    }

    let summary = summary::build_knowledge_summary_for_task_type_with_limit(
        &node,
        &SwarmScope::Global,
        "limit-type",
        1,
    )
    .expect("build summary")
    .expect("summary exists");
    let payload: summary::KnowledgeSummaryBundle =
        serde_json::from_value(summary.payload).expect("decode payload");

    assert_eq!(payload.decisions.len(), 1);
    assert!(matches!(
        payload.decisions[0].task_id.as_str(),
        "task-dm-1" | "task-dm-2"
    ));
}

#[test]
fn service_initializes_summary_limits_from_protocol_params() {
    let params = NetworkProtocolParams {
        summary_reputation_limit: 1,
        summary_decision_memory_limit: 2,
        ..NetworkProtocolParams::default()
    };
    let service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &params,
    )
    .expect("service");
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    node.store
        .put_reputation_snapshot("runtime-a", "model-a", 10, 20, 100)
        .expect("put reputation a");
    node.store
        .put_reputation_snapshot("runtime-b", "model-b", 30, 40, 200)
        .expect("put reputation b");

    assert_eq!(service.summary_reputation_limit, 1);
    assert_eq!(service.summary_decision_memory_limit, 2);
    assert_eq!(
        node.store
            .list_local_reputation_snapshots(service.summary_reputation_limit)
            .expect("list local reputation")
            .len(),
        1
    );
}

#[test]
fn observability_snapshot_reports_network_and_sync_health() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let store = PgStore::open_in_memory().expect("store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let node = Node::new(
        identity,
        store.for_org(subnet.org.org_id.clone()),
        membership,
    )
    .expect("subnet node");

    node.store
        .put_imported_decision_memory(
            "summary-knowledge",
            "peer-a",
            &DecisionMemoryHitRow {
                task_id: "task-observe".to_owned(),
                epoch: 1,
                final_commit_hash: "commit-hash".to_owned(),
                winning_candidate_hash: "candidate-hash".to_owned(),
                output_digest: "output-digest".to_owned(),
                result_summary: json!({"answer":"ok"}),
                quorum_result: json!({"quorum":"pass"}),
                reason_codes: vec![100],
                reason_details: json!({"detail":"good"}),
                policy_snapshot_digest: "policy-snapshot".to_owned(),
                input_digest: "input-digest".to_owned(),
                output_schema_digest: "schema-digest".to_owned(),
                policy_id: "vp.schema_only.v1".to_owned(),
                task_type: "resume_review".to_owned(),
                policy_params_digest: "params-digest".to_owned(),
                deprecated_as_exact: false,
                finalized_at: 10,
                confidence_hint: 0.9,
            },
        )
        .expect("import decision memory");
    node.store
        .put_imported_reputation_snapshot(
            "summary-reputation",
            "peer-a",
            &ReputationSnapshotRow {
                runtime_id: "stub".to_owned(),
                profile_id: "p1".to_owned(),
                stability_reputation: 5,
                quality_reputation: 7,
                last_updated_at: 11,
            },
        )
        .expect("import reputation");
    node.store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-outcome".to_owned(),
            source_node_id: "peer-a".to_owned(),
            scope_hint: "global".to_owned(),
            task_id: "task-observe".to_owned(),
            task_type: "resume_review".to_owned(),
            candidate_id: "cand-1".to_owned(),
            output_digest: "digest-1".to_owned(),
            result_summary: json!({"answer":"ok"}),
            evidence_digest_count: 1,
            checkpoint_id: "cp-1".to_owned(),
            proof_artifact_path: "ipfs://cp-1".to_owned(),
            finalized_at: 12,
            revoked: false,
        })
        .expect("import task outcome");
    node.store
        .put_checkpoint_announcement("global", "cp-local", "ipfs://cp-local", 13)
        .expect("checkpoint");
    node.store
        .upsert_execution_set_member(
            "task-observe",
            "exec-1",
            "peer-a",
            "worker",
            "region:sol-1",
            "confirmed",
            Some("peer-a"),
            14,
        )
        .expect("execution set");

    let parent_store = store.for_org(mainnet.org.org_id.clone());
    parent_store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-parent".to_owned(),
            source_node_id: node.node_id(),
            scope_hint: "global".to_owned(),
            task_id: "task-parent".to_owned(),
            task_type: "resume_review".to_owned(),
            candidate_id: "cand-parent".to_owned(),
            output_digest: "digest-parent".to_owned(),
            result_summary: json!({"answer":"uplink"}),
            evidence_digest_count: 0,
            checkpoint_id: "cp-parent".to_owned(),
            proof_artifact_path: "ipfs://cp-parent".to_owned(),
            finalized_at: 15,
            revoked: false,
        })
        .expect("parent outcome");
    parent_store
        .put_checkpoint_announcement("global", "cp-parent", "ipfs://cp-parent", 16)
        .expect("parent checkpoint");

    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    let peer = random_network_node_id();
    service.connected_peers.insert(peer.clone());
    let mut peer_state = PeerSyncState::new(Instant::now());
    peer_state.known_scopes.insert(SwarmScope::Global);
    peer_state
        .known_scopes
        .insert(SwarmScope::Region("sol-1".to_owned()));
    peer_state.record_pending_backfill(
        BackfillRequestId::new(42),
        SwarmScope::Global,
        None,
        Instant::now(),
    );
    peer_state.next_retry_at = Instant::now() + Duration::from_secs(5);
    service.peer_sync_state.insert(peer, peer_state);
    service.scope_traffic.insert(
        SwarmScope::Global,
        ScopeTrafficStats {
            published_events: 2,
            ingested_events: 3,
            summaries_applied: 1,
            rules_applied: 0,
            checkpoints_applied: 1,
            backfills_applied: 1,
            backfill_events_applied: 4,
        },
    );

    let snapshot = service
        .observability_snapshot(&node)
        .expect("observability snapshot");

    assert_eq!(snapshot.connected_peer_count, 1);
    assert_eq!(snapshot.p2p_foundation, "iroh");
    assert!(snapshot.local_iroh_endpoint_id.is_some());
    assert!(!snapshot.subscribed_iroh_gossip_topics.is_empty());
    assert_eq!(snapshot.known_iroh_contacts, 0);
    assert!(!snapshot.legacy_transport_active);
    assert_eq!(snapshot.nat_status, "iroh-direct");
    assert_eq!(snapshot.nat_public_address, None);
    assert_eq!(snapshot.nat_confidence, 0);
    assert_eq!(snapshot.peer_health.len(), 1);
    assert!(snapshot.peer_health[0].connected);
    assert!(!snapshot.peer_health[0].blacklisted);
    assert_eq!(snapshot.peer_health[0].score, 0);
    assert_eq!(snapshot.peer_health[0].reputation_tier, "healthy");
    assert!(!snapshot.peer_health[0].quarantined);
    assert_eq!(snapshot.peer_health[0].quarantine_remaining_ms, 0);
    assert_eq!(snapshot.peer_health[0].ban_remaining_ms, 0);
    assert_eq!(snapshot.peer_health[0].throttle_factor_percent, 100);
    assert_eq!(
        snapshot.peer_health[0].known_scopes,
        vec!["global".to_owned(), "region:sol-1".to_owned()]
    );
    assert_eq!(snapshot.scope_traffic.len(), 1);
    assert_eq!(snapshot.scope_traffic[0].scope, "global");
    assert_eq!(snapshot.scope_traffic[0].stats.published_events, 2);
    assert_eq!(snapshot.summary_health.imported_decision_memory_rows, 1);
    assert_eq!(snapshot.summary_health.imported_reputation_rows, 1);
    assert_eq!(snapshot.summary_health.imported_task_outcome_rows, 1);
    assert_eq!(snapshot.summary_health.checkpoint_rows, 1);
    assert_eq!(
        snapshot.subnet_sync_health.network_kind,
        NetworkKind::Subnet.as_str()
    );
    assert_eq!(
        snapshot.subnet_sync_health.parent_network_id.as_deref(),
        Some(mainnet.network.network_id.as_str())
    );
    assert!(snapshot.subnet_sync_health.parent_uplink_available);
    assert_eq!(
        snapshot
            .subnet_sync_health
            .parent_imported_task_outcome_rows,
        Some(1)
    );
    assert_eq!(snapshot.subnet_sync_health.parent_checkpoint_rows, Some(1));
    assert_eq!(snapshot.execution_set_health.execution_set_count, 1);
    assert_eq!(snapshot.execution_set_health.execution_set_member_count, 1);
}

#[test]
fn summary_revocation_and_penalty_remove_imported_state() {
    let mut node =
        Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-2".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                task_type: "revoked-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-revoked-summary".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-revoked".to_owned(),
                    winning_candidate_hash: "candidate-revoked".to_owned(),
                    output_digest: "output-revoked".to_owned(),
                    result_summary: json!({"answer":"bad summary"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"bad"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 100,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply knowledge summary");
    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-reputation-2".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::ReputationSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                entries: vec![crate::storage::ReputationSnapshotRow {
                    runtime_id: "runtime-bad".to_owned(),
                    profile_id: "model-bad".to_owned(),
                    stability_reputation: 999,
                    quality_reputation: 888,
                    last_updated_at: 101,
                }],
            })
            .expect("reputation payload"),
        },
    )
    .expect("apply reputation summary");

    assert_eq!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions")
            .len(),
        1
    );
    assert!(
        node.store
            .get_reputation_snapshot("runtime-bad", "model-bad")
            .expect("get reputation")
            .is_some()
    );

    node.revoke_summary(
        "summary-knowledge-2",
        KNOWLEDGE_SUMMARY_KIND,
        "bad knowledge",
        1,
        200,
    )
    .expect("revoke summary");
    assert!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions after revoke")
            .is_empty()
    );

    node.penalize_node(
        "node-bad",
        "malicious source",
        Vec::new(),
        Vec::new(),
        1,
        201,
    )
    .expect("penalize node");
    assert!(
        node.store
            .get_reputation_snapshot("runtime-bad", "model-bad")
            .expect("get reputation after penalty")
            .is_none()
    );

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-3".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                task_type: "revoked-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-revoked-summary-2".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-revoked-2".to_owned(),
                    winning_candidate_hash: "candidate-revoked-2".to_owned(),
                    output_digest: "output-revoked-2".to_owned(),
                    result_summary: json!({"answer":"should be ignored"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"ignored"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 102,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply blocked summary");
    assert!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions after penalty")
            .is_empty()
    );
}

#[test]
fn revoked_event_rebuild_removes_previous_projection_state() {
    let mut node =
        Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-revoked-event", policy_hash);
    contract.inputs = json!({"prompt":"revoke me"});
    let created = node.submit_task(contract, 1, 100).expect("submit task");
    assert!(
        node.task_view("task-revoked-event")
            .expect("task view")
            .is_some()
    );

    node.revoke_event(&created.event_id, "malicious task", 1, 101)
        .expect("revoke event");

    assert!(
        node.task_view("task-revoked-event")
            .expect("task view after revoke")
            .is_none()
    );
}

#[test]
fn publish_pending_global_events_publishes_local_rows_and_skips_remote_rows() {
    let local = NodeIdentity::random();
    let local_node_id = local.node_id();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local_node_id.clone(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: DEFAULT_NETWORK_CONTEXT_ID.to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.publish".to_owned(),
                scope_hint: "global".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    )
    .expect("local subscription event");

    let remote_event = build_event_for_external(
        &remote,
        1,
        101,
        crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
            checkpoint_id: "cp-remote".to_owned(),
            up_to_seq: 0,
        }),
    )
    .expect("remote event");
    node.ingest_remote(remote_event).expect("ingest remote");

    let service_dir = temp_startup_dir("summary-publish-service");
    let service_seed = [131u8; 32];
    fs::write(service_dir.join("node_seed.hex"), hex::encode(service_seed))
        .expect("write node seed");
    fs::write(
        service_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write startup config");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            service_dir.clone(),
            service_seed,
        )
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("network service");

    let mut last = 0;
    let publish_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < publish_deadline {
        last = publish_pending_global_events(&mut service, &node, &local_node_id, 0)
            .expect("publish pending");
        if last == 2 {
            break;
        }
        let _ = service.try_tick(&mut node).expect("service tick");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(last, 2);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&service_dir);
    fs::remove_dir_all(service_dir).expect("cleanup service dir");
}

#[test]
fn latest_connected_peer_ids_uses_runtime_observability_snapshot() {
    let dir = std::env::temp_dir().join(format!(
        "wattswarm-network-bridge-observability-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");

    store_latest_network_observability_snapshot(
        &dir,
        NetworkBridgeObservabilitySnapshot {
            local_network_peer_id: "local-peer".to_owned(),
            local_endpoint_addrs: vec!["127.0.0.1:4001".to_owned()],
            p2p_foundation: "iroh".to_owned(),
            local_iroh_endpoint_id: Some("iroh-endpoint".to_owned()),
            subscribed_iroh_gossip_topics: vec!["global:task:abc123".to_owned()],
            known_iroh_contacts: 1,
            legacy_transport_active: false,
            subscribed_scopes: vec!["global".to_owned()],
            connected_peer_count: 1,
            nat_status: "unknown".to_owned(),
            nat_public_address: None,
            nat_confidence: 0,
            relay_reservations: vec!["https://relay2.wattetheria.com".to_owned()],
            peer_health: vec![
                NetworkBridgePeerHealth {
                    network_peer_id: "peer-connected".to_owned(),
                    connected: true,
                    score: 0,
                    blacklisted: false,
                    reputation_tier: "healthy".to_owned(),
                    quarantined: false,
                    quarantine_remaining_ms: 0,
                    ban_remaining_ms: 0,
                    throttle_factor_percent: 100,
                    known_scopes: vec!["global".to_owned()],
                    inflight_backfills: 0,
                    next_retry_in_ms: 0,
                },
                NetworkBridgePeerHealth {
                    network_peer_id: "peer-disconnected".to_owned(),
                    connected: false,
                    score: 0,
                    blacklisted: false,
                    reputation_tier: "healthy".to_owned(),
                    quarantined: false,
                    quarantine_remaining_ms: 0,
                    ban_remaining_ms: 0,
                    throttle_factor_percent: 100,
                    known_scopes: vec!["global".to_owned()],
                    inflight_backfills: 0,
                    next_retry_in_ms: 0,
                },
            ],
            scope_traffic: Vec::new(),
            dropped_malformed_gossip: 0,
            invalid_control_payloads: 0,
            dial_failures: 0,
            response_validation_failures: 0,
            retry_suppressed_dials: 0,
            summary_health: NetworkBridgeSummaryHealth {
                imported_decision_memory_rows: 0,
                imported_reputation_rows: 0,
                imported_task_outcome_rows: 0,
                checkpoint_rows: 0,
            },
            subnet_sync_health: NetworkBridgeSubnetSyncHealth {
                network_id: "mainnet:watt-etheria".to_owned(),
                network_kind: "mainnet".to_owned(),
                parent_network_id: None,
                parent_uplink_available: false,
                parent_imported_task_outcome_rows: None,
                parent_checkpoint_rows: None,
            },
            execution_set_health: NetworkBridgeExecutionSetHealth {
                execution_set_count: 0,
                execution_set_member_count: 0,
            },
        },
    );

    assert_eq!(
        latest_connected_peer_ids(&dir),
        Some(vec!["peer-connected".to_owned()])
    );
    assert_eq!(
        latest_network_observability_snapshot(&dir)
            .expect("stored observability snapshot")
            .relay_reservations,
        vec!["https://relay2.wattetheria.com".to_owned()]
    );

    clear_latest_network_observability_snapshot(&dir);
    assert_eq!(latest_connected_peer_ids(&dir), None);
    let _ = std::fs::remove_dir_all(dir);
}
