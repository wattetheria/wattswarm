use super::*;

pub fn anti_entropy_syncs_missed_event_without_live_publish() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let scope = SwarmScope::Group("anti-entropy".to_owned());
    let scopes = [SwarmScope::Global, scope];
    let mut service_a = make_fast_service_with_scopes(&scopes);
    let bootstrap_peer = bootstrap_peer_addr(&mut service_a, &mut node_a);
    let mut service_b =
        make_bootstrap_service_with_params(&scopes, &test_protocol_params(), &[bootstrap_peer]);

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-anti-entropy", policy_hash);
    contract.inputs = json!({
        "prompt":"catch me via anti entropy",
        "swarm_scope":"group:anti-entropy"
    });
    node_a.submit_task(contract, 1, 100).expect("submit task");

    // Drain the initial empty backfill round-trip from connect before the
    // recovery loop begins.
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(1)),
    );

    let synced = drive_anti_entropy_until(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(30)),
        |node_b| {
            node_b
                .task_view("task-anti-entropy")
                .expect("task view")
                .is_some()
        },
    );

    assert!(synced);
}

pub fn anti_entropy_uses_scope_specific_cursor_for_recovery() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let scopes = [SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())];
    let mut service_a = make_fast_service_with_scopes(&scopes);
    let bootstrap_peer = bootstrap_peer_addr(&mut service_a, &mut node_a);
    let mut service_b =
        make_bootstrap_service_with_params(&scopes, &test_protocol_params(), &[bootstrap_peer]);

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut region_contract = sample_contract("task-region-cursor", policy_hash.clone());
    region_contract.task_type = "region:sol-1:cursor".to_owned();
    region_contract.inputs = json!({"prompt":"recover region gap", "swarm_scope":"region:sol-1"});
    node_a
        .submit_task(region_contract, 1, 100)
        .expect("submit region task");

    let mut global_contract = sample_contract("task-global-local-head", policy_hash);
    global_contract.inputs = json!({"prompt":"advance local head"});
    node_b
        .submit_task(global_contract, 1, 101)
        .expect("submit local global task");

    // Drain the initial empty backfill round-trip from connect before the
    // recovery loop begins.
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(1)),
    );

    let recovered = drive_anti_entropy_until(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(30)),
        |node_b| {
            node_b
                .task_view("task-region-cursor")
                .expect("task view")
                .is_some()
        },
    );

    assert!(recovered);
}

pub fn reconnect_recovers_missing_events_after_partition_like_disconnect() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let scope = SwarmScope::Group("partition-recovery".to_owned());
    let scopes = [SwarmScope::Global, scope.clone()];
    let mut service_a = make_service_with_scopes(&scopes);
    let bootstrap_peer = bootstrap_peer_addr(&mut service_a, &mut node_a);
    let mut service_b = make_bootstrap_service_with_params(
        &scopes,
        &NetworkProtocolParams::default(),
        &[bootstrap_peer.clone()],
    );

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut first_contract = sample_contract("task-partition-first", policy_hash.clone());
    first_contract.inputs = json!({
        "prompt":"before disconnect",
        "swarm_scope":"group:partition-recovery"
    });
    node_a
        .submit_task(first_contract, 1, 100)
        .expect("submit first task");

    let mut last_published_seq = 0;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish first task");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-partition-first")
            .expect("task view")
            .is_some()
        {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        node_b
            .task_view("task-partition-first")
            .expect("task view first")
            .is_some()
    );

    // Drain inflight messages before disconnect to avoid iroh
    // request-response debug assertion on stale connection state.
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );
    let peer_b_old = service_b.local_peer_id();
    drop(service_b);
    // Pump service_a until it observes the disconnect event.
    let _ = wait_until(scaled_timeout(Duration::from_secs(5)), || {
        matches!(
            pump_once(&mut service_a, &mut node_a),
            Some(NetworkBridgeTick::Disconnected { peer }) if peer == peer_b_old
        )
    });

    let mut second_contract = sample_contract("task-partition-second", policy_hash);
    second_contract.inputs = json!({
        "prompt":"after reconnect",
        "swarm_scope":"group:partition-recovery"
    });
    node_a
        .submit_task(second_contract, 1, 101)
        .expect("submit second task");

    let service_b_state_dir = temp_test_dir("partition-service-b-reconnect");
    wattswarm_control_plane::control::save_network_peer_sync_state_record_state(
        &service_b_state_dir,
        &wattswarm_control_plane::control::NetworkPeerSyncStateRecord {
            network_peer_id: service_a.local_peer_id().to_string(),
            known_scopes_json: serde_json::to_string(&vec![scope]).expect("known scopes json"),
            backfill_cursors_json: "[]".to_owned(),
            remote_heads_json: "[]".to_owned(),
            backfill_successes: 0,
            backfill_failures: 0,
            updated_at: test_contact_generated_at_ms(),
        },
    )
    .expect("seed peer sync scope");
    let service_b_config = NetworkP2pConfig {
        listen_addrs: vec!["127.0.0.1:0".to_owned()],
        enable_local_discovery: false,
        bootstrap_peers: vec![bootstrap_peer],
        ..NetworkP2pConfig::default()
    };
    let mut service_b = make_service_with_config_and_state_dir(
        &scopes,
        &NetworkProtocolParams::default(),
        service_b_config,
        &service_b_state_dir,
    );
    wait_for_connected_pair(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    // Recovery relies on the initial backfill triggered by ConnectionEstablished,
    // not on anti-entropy.  Just pump until the backfill round-trip completes.
    let recovered = wait_until(scaled_timeout(Duration::from_secs(30)), || {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        node_b
            .task_view("task-partition-second")
            .expect("task view second")
            .is_some()
    });

    assert!(recovered);
    cleanup_dir(&service_b_state_dir);
}
