use super::*;

pub fn two_nodes_execute_peer_relationship_request_and_accept_over_network() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let dir_a = temp_test_dir("peer-relationship-a");
    let dir_b = temp_test_dir("peer-relationship-b");
    let mut service_a = make_service_with_config_and_state_dir(
        &[SwarmScope::Global],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        &[SwarmScope::Global],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );

    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();

    service_a
        .send_peer_relationship_action(&remote_b, PeerRelationshipAction::Request, None)
        .expect("send relationship request");

    let mut last_tick_a = None;
    let mut last_tick_b = None;
    let request_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            last_tick_a = pump_once(&mut service_a, &mut node_a);
            last_tick_b = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Requested)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Requested)
    });
    assert!(
        request_synced,
        "relationship request should sync to both peers; a={:?} b={:?} last_tick_a={last_tick_a:?} last_tick_b={last_tick_b:?}",
        relationship_state_for(&dir_a, &remote_b),
        relationship_state_for(&dir_b, &remote_a)
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Accept, None)
        .expect("send relationship accept");

    let accepted = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Accepted)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Accepted)
    });
    assert!(accepted, "relationship accept should sync to both peers");
    let expected_thread_id = private_dm_thread_id(&remote_a, &remote_b);
    assert_eq!(
        dm_thread_state_for(&dir_a, &remote_b),
        Some(PeerDmSessionState::Ready)
    );
    assert_eq!(
        dm_thread_state_for(&dir_b, &remote_a),
        Some(PeerDmSessionState::Ready)
    );
    assert!(
        load_peer_dm_thread_records_state(&dir_a)
            .expect("load a dm threads")
            .iter()
            .any(|thread| thread.thread_id == expected_thread_id
                && thread.remote_node_id == remote_b),
        "accepted relationship should create the private group dm thread on a"
    );
    assert!(
        load_peer_dm_thread_records_state(&dir_b)
            .expect("load b dm threads")
            .iter()
            .any(|thread| thread.thread_id == expected_thread_id
                && thread.remote_node_id == remote_a),
        "accepted relationship should create the private group dm thread on b"
    );
    assert!(
        has_private_dm_subscription(&dir_a, &remote_a, &remote_b),
        "requesting peer should subscribe to the private dm group after accept"
    );
    assert!(
        has_private_dm_subscription(&dir_b, &remote_b, &remote_a),
        "accepting peer should subscribe to the private dm group after accept"
    );

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}

pub fn two_nodes_execute_peer_relationship_request_and_block_over_network() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let dir_a = temp_test_dir("peer-relationship-block-a");
    let dir_b = temp_test_dir("peer-relationship-block-b");
    let mut service_a = make_service_with_config_and_state_dir(
        &[],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        &[],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );

    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();

    service_a
        .send_peer_relationship_action(&remote_b, PeerRelationshipAction::Request, None)
        .expect("send relationship request");

    let request_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Requested)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Requested)
    });
    assert!(
        request_synced,
        "relationship request should sync to both peers"
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Block, None)
        .expect("send relationship block");

    let blocked = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Blocked)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Blocked)
    });
    assert!(blocked, "relationship block should sync to both peers");

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}
