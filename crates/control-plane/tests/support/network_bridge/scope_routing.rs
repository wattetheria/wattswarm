use super::*;

pub fn region_scoped_backfill_only_reaches_region_subscribers() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut service_a =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())]);
    let mut service_b =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())]);
    let mut service_c = make_service_with_scopes(&[SwarmScope::Global]);

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-region-backfill", policy_hash);
    contract.task_type = "region:sol-1:swarm".to_owned();
    contract.inputs = json!({"prompt":"region sync", "swarm_scope":"region:sol-1"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut region_synced = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending scoped");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_b
            .task_view("task-region-backfill")
            .expect("task view")
            .is_some()
        {
            region_synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(region_synced);
    assert!(
        node_c
            .task_view("task-region-backfill")
            .expect("task view")
            .is_none()
    );
}

pub fn node_scoped_live_sync_only_reaches_matching_node_scope() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut service_a =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Node("lab-1".to_owned())]);
    let mut service_b =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Node("lab-1".to_owned())]);
    let mut service_c =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Node("lab-2".to_owned())]);

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-local-live", policy_hash);
    contract.task_type = "node:lab-1:swarm".to_owned();
    contract.inputs = json!({"prompt":"node-scoped sync", "swarm_scope":"node:lab-1"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut local_synced = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending scoped");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_b
            .task_view("task-local-live")
            .expect("task view")
            .is_some()
        {
            local_synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(local_synced);
    assert!(
        node_c
            .task_view("task-local-live")
            .expect("task view")
            .is_none()
    );
}

pub fn group_scoped_live_sync_only_reaches_matching_group_scope() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut service_a =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())]);
    let mut service_b =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())]);
    let mut service_c =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Group("crew-8".to_owned())]);

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-group-live", policy_hash);
    contract.task_type = "group:crew-7:swarm".to_owned();
    contract.inputs = json!({
        "prompt":"group-scoped sync",
        "swarm_scope":"group:crew-7",
        "swarm_route":{
            "group_id":"crew-7",
            "target_node_ids":[node_a.node_id(), node_b.node_id()],
            "relation_tags":["crew"],
            "forward_budget":1
        }
    });
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut group_synced = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending scoped");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_b
            .task_view("task-group-live")
            .expect("task view")
            .is_some()
        {
            group_synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(group_synced);
    assert!(
        node_c
            .task_view("task-group-live")
            .expect("task view")
            .is_none()
    );
}

pub fn group_subscribed_middle_peer_relays_group_events() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut service_a = make_service_with_scopes(&[SwarmScope::Global]);
    let mut service_b = make_service_with_scopes(&[SwarmScope::Global]);
    let mut service_c = make_service_with_scopes(&[SwarmScope::Global]);
    let group_scope = SwarmScope::Group("crew-7".to_owned());

    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);
    connect_services(&mut service_b, &mut node_b, &mut service_c, &mut node_c);

    service_a
        .subscribe_scope_kinds(&group_scope, &[GossipKind::Events])
        .expect("node a joins group events");
    service_c
        .subscribe_scope_kinds(&group_scope, &[GossipKind::Events])
        .expect("node c relays group events");

    service_b
        .subscribe_scope_kinds(&group_scope, &[GossipKind::Events])
        .expect("node b joins group events");
    let policy_hash = node_b
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-group-relayed", policy_hash);
    contract.task_type = "group:crew-7:swarm".to_owned();
    contract.inputs = json!({"prompt":"relayed group sync", "swarm_scope":"group:crew-7"});
    node_b.submit_task(contract, 1, 110).expect("submit task");

    let mut last_b = 0;
    let mut relayed_to_a = false;
    for _ in 0..4_096 {
        last_b = publish_pending_scoped_updates(&mut service_b, &node_b, &node_b.node_id(), last_b)
            .expect("publish node b task");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_a
            .task_view("task-group-relayed")
            .expect("task view")
            .is_some()
        {
            relayed_to_a = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(
        relayed_to_a,
        "A and B are not directly connected, so the group event must relay through group-subscribed C"
    );
}

pub fn subnet_nodes_sync_and_mainnet_overlay_stays_isolated() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut subnet_a = make_service_for_network("subnet:alpha");
    let mut subnet_b = make_service_for_network("subnet:alpha");
    let mut mainnet_c = make_service_for_network("mainnet:watt-galaxy");

    connect_services(&mut subnet_a, &mut node_a, &mut subnet_b, &mut node_b);
    wait_for_listen_addrs(&mut mainnet_c, &mut node_c);
    subnet_a
        .dial(iroh_peer_addr(&mainnet_c))
        .expect("dial mainnet overlay");

    let event = node_a
        .emit_at(
            1,
            EventPayload::CheckpointCreated(CheckpointCreatedPayload {
                checkpoint_id: "cp-subnet-overlay".to_owned(),
                up_to_seq: 0,
            }),
            100,
        )
        .expect("emit checkpoint");

    let mut last_published_seq = 0;
    let subnet_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        last_published_seq = publish_pending_global_events(
            &mut subnet_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending subnet events");
        let _ = pump_once(&mut subnet_a, &mut node_a);
        let _ = pump_once(&mut subnet_b, &mut node_b);
        let _ = pump_once(&mut mainnet_c, &mut node_c);
        node_b
            .store
            .load_all_events()
            .expect("load subnet events")
            .into_iter()
            .any(|(_, candidate)| candidate.event_id == event.event_id)
    });
    assert!(
        subnet_synced,
        "subnet peer should receive subnet control event"
    );
    assert!(
        node_c
            .store
            .load_all_events()
            .expect("load mainnet events")
            .into_iter()
            .all(|(_, candidate)| candidate.event_id != event.event_id),
        "mainnet overlay must not receive subnet control event"
    );
}
