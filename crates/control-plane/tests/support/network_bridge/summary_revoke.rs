use super::*;

pub fn summary_gossip_imports_knowledge_and_reputation_into_remote_store() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    node_a
        .store
        .put_decision_memory(
            "task-summary",
            1,
            "commit-1",
            100,
            "candidate-hash-1",
            "output-digest-1",
            &json!({"answer":"from summary"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"summary"}),
            "policy-snap-1",
            "summary-type",
            "input-digest-1",
            "schema-digest-1",
            "vp.schema_only.v1",
            "params-digest-1",
        )
        .expect("put decision memory");
    node_a
        .store
        .put_reputation_snapshot("runtime-a", "model-a", 1200, 3400, 111)
        .expect("put reputation snapshot");

    let knowledge =
        build_knowledge_summary_for_task_type(&node_a, &SwarmScope::Global, "summary-type")
            .expect("build knowledge summary")
            .expect("knowledge summary");
    let reputation = build_reputation_summary_for_runtime(&node_a, "runtime-a", "model-a")
        .expect("build reputation summary")
        .expect("reputation summary");

    service_a
        .publish_summary(knowledge)
        .expect("publish knowledge");
    service_a
        .publish_summary(reputation)
        .expect("publish reputation");

    let mut imported = false;
    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .store
            .list_decision_memory_hits_by_task_type("summary-type", 8)
            .expect("list decisions")
            .len()
            == 1
            && node_b
                .store
                .get_reputation_snapshot("runtime-a", "model-a")
                .expect("get reputation")
                .is_some()
        {
            imported = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(imported);

    let imported_reputation = node_b
        .store
        .get_reputation_snapshot("runtime-a", "model-a")
        .expect("get imported reputation")
        .expect("reputation exists");
    assert_eq!(imported_reputation.stability_reputation, 1200);
    assert_eq!(imported_reputation.quality_reputation, 3400);

    let imported_decisions = node_b
        .store
        .list_decision_memory_hits_by_task_type("summary-type", 8)
        .expect("list imported decisions");
    assert_eq!(imported_decisions.len(), 1);
    assert_eq!(
        imported_decisions[0].result_summary["answer"],
        json!("from summary")
    );
}

pub fn revoked_event_propagates_and_removes_remote_projection_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-revocation-network", policy_hash);
    contract.inputs = json!({"prompt":"revoke over network"});
    let created = node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish task event");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-revocation-network")
            .expect("task view")
            .is_some()
        {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        node_b
            .task_view("task-revocation-network")
            .expect("task view after sync")
            .is_some()
    );

    node_a
        .revoke_event(&created.event_id, "malicious task", 1, 101)
        .expect("revoke event");

    let mut revoked = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish revoke event");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-revocation-network")
            .expect("task view after revoke")
            .is_none()
        {
            revoked = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(revoked);
}

pub fn revoked_summary_event_removes_remote_imported_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    node_a
        .store
        .put_decision_memory(
            "task-summary-revoke",
            1,
            "commit-revoke",
            100,
            "candidate-revoke",
            "output-revoke",
            &json!({"answer":"revoke this summary"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"summary"}),
            "policy-snap-1",
            "summary-revoke-type",
            "input-digest-1",
            "schema-digest-1",
            "vp.schema_only.v1",
            "params-digest-1",
        )
        .expect("put decision memory");
    let knowledge =
        build_knowledge_summary_for_task_type(&node_a, &SwarmScope::Global, "summary-revoke-type")
            .expect("build knowledge summary")
            .expect("knowledge summary");

    service_a
        .publish_summary(knowledge.clone())
        .expect("publish knowledge");

    let mut imported = false;
    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .store
            .list_decision_memory_hits_by_task_type("summary-revoke-type", 8)
            .expect("list imported decisions")
            .len()
            == 1
        {
            imported = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(imported);

    node_a
        .revoke_summary(
            &knowledge.summary_id,
            &knowledge.summary_kind,
            "summary was malicious",
            1,
            110,
        )
        .expect("revoke summary");

    let mut last_published_seq = 0;
    let mut removed = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish summary revoke event");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .store
            .list_decision_memory_hits_by_task_type("summary-revoke-type", 8)
            .expect("list imported decisions after revoke")
            .is_empty()
        {
            removed = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(removed);
}
