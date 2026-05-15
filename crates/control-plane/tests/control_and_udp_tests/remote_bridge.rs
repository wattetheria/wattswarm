use super::*;

#[test]
fn remote_task_bridge_materializes_executes_and_dedupes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("remote-task-bridge");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let remote = NodeIdentity::random();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&node.node_id(), role);
    }
    membership.grant(&remote.node_id(), Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("remote-bridge-task-{}", Uuid::new_v4().simple());
    let contract = sample_contract(&task_id, policy_hash);
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement_unsigned = UnsignedEvent::from_payload(
        "0.1.0".to_owned(),
        remote.node_id(),
        1,
        100,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(TaskAnnouncedPayload {
            network_id: topology.network.network_id.clone(),
            task_id: task_id.clone(),
            announcement_id: format!("ann-{}", Uuid::new_v4().simple()),
            feed_key: "remote-feed".to_owned(),
            scope_hint: "global".to_owned(),
            summary: json!({"title":"remote bridge task"}),
            detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                uri: "ipfs://remote-task-detail".to_owned(),
                digest: digest.clone(),
                size_bytes: contract_bytes.len() as u64,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: remote.node_id(),
            }),
        }),
    );
    let announcement = remote
        .sign_unsigned_event(&announcement_unsigned)
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");

    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");

    let out = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("bridge remote task");
    assert_eq!(out["task_id"], json!(task_id.clone()));
    assert_eq!(out["profile"], json!("default"));
    assert_eq!(out["terminal_state"], json!("Finalized"));
    assert_eq!(out["bridge"]["deduped"], json!(false));
    assert_eq!(out["bridge"]["source_node_id"], json!(remote.node_id()));

    let members = node
        .store
        .list_execution_set_members(&task_id, &format!("remote-bridge:{task_id}"))
        .expect("list execution set members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].participant_node_id, node.node_id());
    assert_eq!(members[0].status, "confirmed");

    let bridge_rows =
        wattswarm_control_plane::storage::PgStore::open(state_dir.join("local-control.state"))
            .expect("open local control store")
            .list_local_remote_task_bridges(&local_control_scope_id(&state_dir))
            .expect("list local remote task bridges");
    let registry = RemoteTaskBridgeRegistry {
        entries: bridge_rows
            .into_iter()
            .map(
                |entry| wattswarm_control_plane::control::RemoteTaskBridgeRecord {
                    task_id: entry.task_id,
                    announcement_id: entry.announcement_id,
                    network_id: entry.network_id,
                    source_node_id: entry.source_node_id,
                    source_scope_hint: entry.source_scope_hint,
                    detail_ref_digest: entry.detail_ref_digest,
                    executor: entry.executor,
                    profile: entry.profile,
                    candidate_id: entry.candidate_id,
                    terminal_state: entry.terminal_state,
                    bridged_at: entry.bridged_at,
                },
            )
            .collect(),
    };
    assert_eq!(registry.entries.len(), 1);
    assert_eq!(
        registry.entries[0].detail_ref_digest.as_deref(),
        Some(digest.as_str())
    );

    let deduped = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("dedupe bridged task");
    assert_eq!(deduped["bridge"]["deduped"], json!(true));
    assert_eq!(deduped["candidate_id"], out["candidate_id"]);

    let registry_after =
        wattswarm_control_plane::storage::PgStore::open(state_dir.join("local-control.state"))
            .expect("open local control store after")
            .list_local_remote_task_bridges(&local_control_scope_id(&state_dir))
            .expect("list local remote task bridges after");
    assert_eq!(registry_after.len(), 1);

    cleanup_dir(&dir);
}

#[test]
fn remote_task_bridge_rejects_node_scoped_tasks_for_other_nodes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("remote-task-bridge-scope-reject");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let remote = NodeIdentity::random();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&node.node_id(), role);
    }
    membership.grant(&remote.node_id(), Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("remote-bridge-node-scope-{}", Uuid::new_v4().simple());
    let contract = sample_contract(&task_id, policy_hash);
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement_unsigned = UnsignedEvent::from_payload(
        "0.1.0".to_owned(),
        remote.node_id(),
        1,
        100,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(TaskAnnouncedPayload {
            network_id: topology.network.network_id.clone(),
            task_id: task_id.clone(),
            announcement_id: format!("ann-{}", Uuid::new_v4().simple()),
            feed_key: "remote-feed".to_owned(),
            scope_hint: "node:not-this-node".to_owned(),
            summary: json!({"title":"node scoped bridge task"}),
            detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                uri: "ipfs://remote-task-detail".to_owned(),
                digest,
                size_bytes: contract_bytes.len() as u64,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: remote.node_id(),
            }),
        }),
    );
    let announcement = remote
        .sign_unsigned_event(&announcement_unsigned)
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");
    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");

    let err = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect_err("bridge should reject mismatched node scope");
    assert!(err.to_string().contains("not eligible for local node"));
    assert!(
        wattswarm_control_plane::storage::PgStore::open(state_dir.join("local-control.state"))
            .expect("open local control store")
            .list_local_remote_task_bridges(&local_control_scope_id(&state_dir))
            .expect("list local remote task bridges")
            .is_empty(),
        "rejected bridge should not persist dedupe registry rows"
    );

    cleanup_dir(&dir);
}

#[test]
fn remote_task_bridge_allows_group_scoped_tasks_for_target_nodes() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("remote-task-bridge-group-scope-allow");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let stub = RuntimeStub::start(&["default"]);
    wait_for_stub_listener(&stub);

    save_executor_registry_state(
        &state_dir,
        &ExecutorRegistry {
            entries: vec![ExecutorRegistryEntry {
                name: "core-agent".to_owned(),
                base_url: stub.base_url(),
                agent_event_callback_base_url: None,
                kind: Default::default(),
                target_node_id: None,
                scope_hint: None,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            }],
        },
    )
    .expect("save executor registry");

    let mut node = open_node(&state_dir, &db_path).expect("open node");
    let remote = NodeIdentity::random();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&node.node_id(), role);
    }
    membership.grant(&remote.node_id(), Role::Proposer);
    node.store
        .put_membership(&serde_json::to_string(&membership).expect("membership json"))
        .expect("put membership");

    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", serde_json::json!({}))
        .expect("policy binding")
        .policy_hash;
    let task_id = format!("remote-bridge-group-scope-{}", Uuid::new_v4().simple());
    let mut contract = sample_contract(&task_id, policy_hash);
    contract.task_type = "group:crew-7:swarm".to_owned();
    contract.inputs = json!({
        "prompt":"group scoped bridge task",
        "swarm_scope":"group:crew-7",
        "swarm_route":{
            "group_id":"crew-7",
            "target_node_ids":[node.node_id()],
            "relation_tags":["crew"],
            "forward_budget":1
        }
    });
    let contract_bytes = serde_json::to_vec(&contract).expect("contract bytes");
    let digest = format!("sha256:{}", sha256_hex(&contract_bytes));
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load topology");
    let announcement_unsigned = UnsignedEvent::from_payload(
        "0.1.0".to_owned(),
        remote.node_id(),
        1,
        100,
        wattswarm_control_plane::types::EventPayload::TaskAnnounced(TaskAnnouncedPayload {
            network_id: topology.network.network_id.clone(),
            task_id: task_id.clone(),
            announcement_id: format!("ann-{}", Uuid::new_v4().simple()),
            feed_key: "remote-feed".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            summary: json!({"title":"group scoped bridge task"}),
            detail_ref: Some(wattswarm_control_plane::types::ArtifactRef {
                uri: "ipfs://remote-task-detail".to_owned(),
                digest,
                size_bytes: contract_bytes.len() as u64,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: remote.node_id(),
            }),
        }),
    );
    let announcement = remote
        .sign_unsigned_event(&announcement_unsigned)
        .expect("sign remote announcement");
    node.ingest_remote(announcement)
        .expect("ingest remote announcement");
    materialize_task_detail_artifact(&state_dir, &node, &task_id, &contract_bytes, 101)
        .expect("materialize remote task detail");

    let out = bridge_remote_task_into_local_execution(
        &mut node,
        &state_dir,
        RemoteTaskBridgeRequest {
            executor: "core-agent".to_owned(),
            profile: "default".to_owned(),
            task_id: task_id.clone(),
        },
    )
    .expect("bridge should allow targeted group scope");

    assert_eq!(out["task_id"], json!(task_id));
    assert_eq!(out["bridge"]["source_scope_hint"], json!("group:crew-7"));

    cleanup_dir(&dir);
}
