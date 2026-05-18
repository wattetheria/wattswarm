use super::*;

#[test]
fn task_detail_and_evidence_artifacts_roundtrip_and_missing_refs_schedule_repair() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("artifact-detail-evidence");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let network_id = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology")
        .network
        .network_id;

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-artifact-detail", policy_hash);
    contract.inputs = json!({"prompt":"artifact detail"});
    node.submit_task(contract, 1, 10).expect("submit task");

    let detail_bytes = br#"{"task":"detail","v":1}"#;
    let detail_digest = format!("sha256:{}", sha256_hex(detail_bytes));
    node.emit_at(
        1,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(
            wattswarm_control_plane::types::TaskAnnouncedPayload {
                network_id,
                task_id: "task-artifact-detail".to_owned(),
                announcement_id: "announce-artifact-detail".to_owned(),
                feed_key: "feed.detail".to_owned(),
                scope_hint: "global".to_owned(),
                summary: json!({"headline":"detail"}),
                detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                    uri: "ipfs://task-artifact-detail".to_owned(),
                    digest: detail_digest.clone(),
                    size_bytes: detail_bytes.len() as u64,
                    mime: "application/json".to_owned(),
                    created_at: 11,
                    producer: node.node_id(),
                }),
                agent_envelope: None,
            },
        ),
        11,
    )
    .expect("announce detail");

    let missing_detail = fetch_task_detail_artifact(&state_dir, &node, "task-artifact-detail", 20);
    assert!(missing_detail.is_err());

    let pending = list_artifacts_needing_repair(&state_dir, 20 + 30_000).expect("list repairs");
    assert!(pending.iter().any(|manifest| {
        manifest.artifact_kind == ArtifactKind::Reference
            && manifest.artifact_id == detail_digest
            && manifest.status == ArtifactAvailabilityStatus::Missing
    }));

    let detail_manifest = materialize_task_detail_artifact(
        &state_dir,
        &node,
        "task-artifact-detail",
        detail_bytes,
        21,
    )
    .expect("materialize task detail");
    assert_eq!(
        detail_manifest.status,
        ArtifactAvailabilityStatus::Available
    );

    let loaded_detail = fetch_task_detail_artifact(&state_dir, &node, "task-artifact-detail", 22)
        .expect("fetch task detail");
    assert_eq!(loaded_detail, detail_bytes);

    node.claim_task(
        "task-artifact-detail",
        wattswarm_control_plane::types::ClaimRole::Propose,
        "exec-artifact-detail",
        500,
        1,
        23,
    )
    .expect("claim task");
    let evidence_bytes = br#"{"evidence":"blob"}"#;
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_bytes));
    let candidate_output = json!({"answer":"ok"});
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-artifact-1".to_owned(),
        execution_id: "exec-artifact-detail".to_owned(),
        output_ref: candidate_output_ref("cand-artifact-1", &candidate_output, 24, &node.node_id()),
        output: candidate_output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.propose_candidate("task-artifact-detail", candidate, 1, 24)
        .expect("propose candidate");
    node.add_evidence(
        "task-artifact-detail",
        "cand-artifact-1",
        "exec-artifact-detail",
        vec![wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://evidence-artifact".to_owned(),
            digest: evidence_digest.clone(),
            size_bytes: evidence_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 24,
            producer: node.node_id(),
        }],
        1,
        25,
    )
    .expect("add evidence");

    materialize_evidence_artifact(
        &state_dir,
        &node,
        "task-artifact-detail",
        "cand-artifact-1",
        &evidence_digest,
        evidence_bytes,
        26,
    )
    .expect("materialize evidence");
    let loaded_evidence = fetch_evidence_artifact(
        &state_dir,
        &node,
        "task-artifact-detail",
        "cand-artifact-1",
        &evidence_digest,
        27,
    )
    .expect("fetch evidence");
    assert_eq!(loaded_evidence, evidence_bytes);

    cleanup_dir(&dir);
}

#[test]
fn checkpoint_and_snapshot_artifacts_roundtrip_and_record_missing_retries() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("artifact-checkpoint-snapshot");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node = open_node(&state_dir, &db_path).expect("open node");

    node.store
        .put_checkpoint_announcement("global", "cp-artifact-1", "ipfs://checkpoint-1", 30)
        .expect("put checkpoint announcement");

    let missing_checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-artifact-1",
        31,
    );
    assert!(missing_checkpoint.is_err());

    let checkpoint_manifest = materialize_checkpoint_artifact_json(
        &state_dir,
        &node,
        "global",
        "cp-artifact-1",
        &json!({"checkpoint":"cp-artifact-1","up_to_seq":42}),
        32,
    )
    .expect("materialize checkpoint");
    assert_eq!(checkpoint_manifest.artifact_kind, ArtifactKind::Checkpoint);

    let checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-artifact-1",
        33,
    )
    .expect("fetch checkpoint");
    assert_eq!(checkpoint["up_to_seq"], json!(42));

    let missing_snapshot =
        fetch_snapshot_artifact_json::<serde_json::Value>(&state_dir, "region:sol-1", "snap-1", 34);
    assert!(missing_snapshot.is_err());

    let pending = list_artifacts_needing_repair(&state_dir, 34 + 30_000).expect("list repairs");
    assert!(pending.iter().any(|manifest| {
        manifest.artifact_kind == ArtifactKind::Snapshot
            && manifest.artifact_id == "snap-1"
            && manifest.status == ArtifactAvailabilityStatus::Missing
    }));

    materialize_snapshot_artifact_json(
        &state_dir,
        "region:sol-1",
        "snap-1",
        Some("ipfs://snapshot-1"),
        &json!({"snapshot":"snap-1","epoch":7}),
        35,
    )
    .expect("materialize snapshot");
    let snapshot =
        fetch_snapshot_artifact_json::<serde_json::Value>(&state_dir, "region:sol-1", "snap-1", 36)
            .expect("fetch snapshot");
    assert_eq!(snapshot["epoch"], json!(7));

    cleanup_dir(&dir);
}

#[test]
fn missing_task_and_evidence_artifacts_record_remote_source_route() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("artifact-source-route");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-remote-artifact", policy_hash);
    contract.inputs = json!({"prompt":"remote artifact"});
    node.submit_task(contract, 1, 39).expect("submit task");

    save_iroh_peer_metadata(&state_dir, "node-remote", 40);

    let detail_bytes = br#"{"detail":"remote"}"#;
    let detail_digest = format!("sha256:{}", sha256_hex(detail_bytes));
    node.announce_task(
        "task-remote-artifact",
        "announce-remote-artifact",
        "feed-artifacts",
        "global",
        json!({"headline":"remote detail"}),
        Some(wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://task-remote-artifact".to_owned(),
            digest: detail_digest.clone(),
            size_bytes: detail_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 40,
            producer: "node-remote".to_owned(),
        }),
        1,
        40,
    )
    .expect("announce remote detail");

    let missing_detail = fetch_task_detail_artifact(&state_dir, &node, "task-remote-artifact", 41);
    assert!(missing_detail.is_err());

    node.claim_task(
        "task-remote-artifact",
        wattswarm_control_plane::types::ClaimRole::Propose,
        "exec-remote-evidence",
        500,
        1,
        42,
    )
    .expect("claim task");
    let evidence_bytes = br#"{"evidence":"remote"}"#;
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_bytes));
    let candidate_output = json!({"answer":"ok"});
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-remote-evidence".to_owned(),
        execution_id: "exec-remote-evidence".to_owned(),
        output_ref: candidate_output_ref(
            "cand-remote-evidence",
            &candidate_output,
            43,
            &node.node_id(),
        ),
        output: candidate_output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.propose_candidate("task-remote-artifact", candidate, 1, 43)
        .expect("propose candidate");
    node.add_evidence(
        "task-remote-artifact",
        "cand-remote-evidence",
        "exec-remote-evidence",
        vec![wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://evidence-remote-artifact".to_owned(),
            digest: evidence_digest.clone(),
            size_bytes: evidence_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 43,
            producer: "node-remote".to_owned(),
        }],
        1,
        44,
    )
    .expect("add remote evidence");

    let missing_evidence = fetch_evidence_artifact(
        &state_dir,
        &node,
        "task-remote-artifact",
        "cand-remote-evidence",
        &evidence_digest,
        45,
    );
    assert!(missing_evidence.is_err());

    let decisions = load_transport_decisions(&state_dir);
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("artifact_blob")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-remote")
            && entry["subject"]["artifact_kind"] == json!("task_detail")
    }));
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("evidence_blob")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-remote")
            && entry["subject"]["artifact_kind"] == json!("evidence")
    }));

    cleanup_dir(&dir);
}

#[test]
fn fetch_task_and_evidence_artifacts_over_iroh_when_remote_contact_material_is_available() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = temp_test_dir("artifact-source-iroh");
    let state_dir = dir.join("state");
    let remote_state_dir = dir.join("remote-state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
    let db_path = state_dir.join("test.state");
    let mut node = open_node(&state_dir, &db_path).expect("open node");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-remote-artifact-iroh", policy_hash);
    contract.inputs = json!({"prompt":"remote artifact over iroh"});
    node.submit_task(contract, 1, 60).expect("submit task");

    let _ = local_node_id(&remote_state_dir).expect("create remote node seed");
    save_real_iroh_peer_metadata(&state_dir, "node-remote", &remote_state_dir, 61);

    let remote_store = ArtifactStore::new(artifact_store_path(&remote_state_dir));
    remote_store.ensure_layout().expect("remote store layout");

    let detail_bytes = br#"{"detail":"remote-via-iroh"}"#;
    let detail_digest = format!("sha256:{}", sha256_hex(detail_bytes));
    remote_store
        .write_validated_bytes(
            ArtifactKind::Reference,
            &detail_digest,
            None,
            detail_bytes,
            Some(&detail_digest),
            Some(detail_bytes.len() as u64),
        )
        .expect("write remote detail");
    node.announce_task(
        "task-remote-artifact-iroh",
        "announce-remote-artifact-iroh",
        "feed-artifacts",
        "global",
        json!({"headline":"remote detail"}),
        Some(wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://task-remote-artifact-iroh".to_owned(),
            digest: detail_digest.clone(),
            size_bytes: detail_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 61,
            producer: "node-remote".to_owned(),
        }),
        1,
        61,
    )
    .expect("announce remote detail");

    let loaded_detail =
        fetch_task_detail_artifact(&state_dir, &node, "task-remote-artifact-iroh", 62)
            .expect("fetch detail over iroh");
    assert_eq!(loaded_detail, detail_bytes);

    node.claim_task(
        "task-remote-artifact-iroh",
        wattswarm_control_plane::types::ClaimRole::Propose,
        "exec-remote-evidence-iroh",
        500,
        1,
        63,
    )
    .expect("claim task");
    let candidate_output = json!({"answer":"ok"});
    let candidate = wattswarm_control_plane::types::Candidate {
        candidate_id: "cand-remote-evidence-iroh".to_owned(),
        execution_id: "exec-remote-evidence-iroh".to_owned(),
        output_ref: candidate_output_ref(
            "cand-remote-evidence-iroh",
            &candidate_output,
            64,
            &node.node_id(),
        ),
        output: candidate_output,
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.propose_candidate("task-remote-artifact-iroh", candidate, 1, 64)
        .expect("propose candidate");

    let evidence_bytes = br#"{"evidence":"remote-via-iroh"}"#;
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_bytes));
    remote_store
        .write_validated_bytes(
            ArtifactKind::Evidence,
            &evidence_digest,
            None,
            evidence_bytes,
            Some(&evidence_digest),
            Some(evidence_bytes.len() as u64),
        )
        .expect("write remote evidence");
    node.add_evidence(
        "task-remote-artifact-iroh",
        "cand-remote-evidence-iroh",
        "exec-remote-evidence-iroh",
        vec![wattswarm_control_plane::types::ArtifactRef {
            uri: "ipfs://evidence-remote-artifact-iroh".to_owned(),
            digest: evidence_digest.clone(),
            size_bytes: evidence_bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 65,
            producer: "node-remote".to_owned(),
        }],
        1,
        65,
    )
    .expect("add evidence");

    let loaded_evidence = fetch_evidence_artifact(
        &state_dir,
        &node,
        "task-remote-artifact-iroh",
        "cand-remote-evidence-iroh",
        &evidence_digest,
        66,
    )
    .expect("fetch evidence over iroh");
    assert_eq!(loaded_evidence, evidence_bytes);

    cleanup_dir(&dir);
}

#[test]
fn missing_checkpoint_and_snapshot_artifacts_use_saved_source_binding_for_route() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("checkpoint-snapshot-source-route");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node = open_node(&state_dir, &db_path).expect("open node");

    save_iroh_peer_metadata(&state_dir, "node-sync", 50);
    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Checkpoint,
            binding_scope: Some("global".to_owned()),
            binding_key: "cp-source-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://checkpoint-source-1".to_owned()),
            updated_at: 50,
        },
    )
    .expect("save checkpoint binding");
    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Snapshot,
            binding_scope: Some("region:sol-1".to_owned()),
            binding_key: "snap-source-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://snapshot-source-1".to_owned()),
            updated_at: 50,
        },
    )
    .expect("save snapshot binding");

    node.store
        .put_checkpoint_announcement("global", "cp-source-1", "ipfs://checkpoint-source-1", 51)
        .expect("put checkpoint announcement");

    let missing_checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-source-1",
        52,
    );
    assert!(missing_checkpoint.is_err());

    let missing_snapshot = fetch_snapshot_artifact_json::<serde_json::Value>(
        &state_dir,
        "region:sol-1",
        "snap-source-1",
        53,
    );
    assert!(missing_snapshot.is_err());

    let decisions = load_transport_decisions(&state_dir);
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("checkpoint_snapshot")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-sync")
            && entry["subject"]["artifact_kind"] == json!("checkpoint")
    }));
    assert!(decisions.iter().any(|entry| {
        entry["kind"] == json!("checkpoint_snapshot")
            && entry["route"] == json!("iroh_direct")
            && entry["source_node_id"] == json!("node-sync")
            && entry["subject"]["artifact_kind"] == json!("snapshot")
    }));

    cleanup_dir(&dir);
}

#[test]
fn fetch_checkpoint_and_snapshot_artifacts_over_iroh_when_remote_contact_material_is_available() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);

    let dir = temp_test_dir("checkpoint-snapshot-iroh");
    let state_dir = dir.join("state");
    let remote_state_dir = dir.join("remote-state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
    let db_path = state_dir.join("test.state");
    let node = open_node(&state_dir, &db_path).expect("open node");

    let _ = local_node_id(&remote_state_dir).expect("create remote node seed");
    save_real_iroh_peer_metadata(&state_dir, "node-sync", &remote_state_dir, 80);

    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Checkpoint,
            binding_scope: Some("global".to_owned()),
            binding_key: "cp-iroh-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://checkpoint-iroh-1".to_owned()),
            updated_at: 80,
        },
    )
    .expect("save checkpoint binding");
    save_data_source_binding_record_state(
        &state_dir,
        &DataSourceBindingRecord {
            binding_kind: DataSourceBindingKind::Snapshot,
            binding_scope: Some("region:sol-1".to_owned()),
            binding_key: "snap-iroh-1".to_owned(),
            source_node_id: "node-sync".to_owned(),
            source_uri: Some("ipfs://snapshot-iroh-1".to_owned()),
            updated_at: 80,
        },
    )
    .expect("save snapshot binding");

    node.store
        .put_checkpoint_announcement("global", "cp-iroh-1", "ipfs://checkpoint-iroh-1", 81)
        .expect("checkpoint announcement");

    let remote_store = ArtifactStore::new(artifact_store_path(&remote_state_dir));
    remote_store.ensure_layout().expect("remote store layout");
    remote_store
        .write_json(
            &remote_store
                .checkpoint_path("cp-iroh-1")
                .expect("checkpoint path"),
            &json!({"checkpoint":"cp-iroh-1","up_to_seq":77}),
        )
        .expect("write remote checkpoint");
    remote_store
        .write_json(
            &remote_store
                .snapshot_path("region:sol-1", "snap-iroh-1")
                .expect("snapshot path"),
            &json!({"snapshot":"snap-iroh-1","epoch":9}),
        )
        .expect("write remote snapshot");

    let checkpoint = fetch_checkpoint_artifact_json::<serde_json::Value>(
        &state_dir,
        &node,
        "global",
        "cp-iroh-1",
        82,
    )
    .expect("fetch checkpoint over iroh");
    assert_eq!(checkpoint["up_to_seq"], json!(77));

    let snapshot = fetch_snapshot_artifact_json::<serde_json::Value>(
        &state_dir,
        "region:sol-1",
        "snap-iroh-1",
        83,
    )
    .expect("fetch snapshot over iroh");
    assert_eq!(snapshot["epoch"], json!(9));

    cleanup_dir(&dir);
}
