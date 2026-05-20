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

pub fn two_nodes_establish_dm_session_and_exchange_messages_over_network() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let dir_a = temp_test_dir("peer-dm-a");
    let dir_b = temp_test_dir("peer-dm-b");
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
    let thread_id = dm_thread_id(&remote_a, &remote_b);

    let send_without_relationship =
        service_a.send_peer_direct_message(&remote_b, None, json!({"text":"pre-relationship"}));
    assert!(
        send_without_relationship.is_err(),
        "dm should be rejected before an accepted relationship exists"
    );

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
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Accept, None)
        .expect("send relationship accept");

    let mut last_direct_tick_a = String::new();
    let mut last_direct_tick_b = String::new();
    let session_ready = wait_until(scaled_timeout(Duration::from_secs(30)), || {
        for _ in 0..64 {
            let tick_a = pump_once(&mut service_a, &mut node_a);
            let tick_b = pump_once(&mut service_b, &mut node_b);
            if let Some(
                NetworkBridgeTick::PeerDirectMessageUpdated { .. }
                | NetworkBridgeTick::PeerDirectMessageFailed { .. },
            ) = tick_a.as_ref()
            {
                last_direct_tick_a = format!("{tick_a:?}");
            }
            if let Some(
                NetworkBridgeTick::PeerDirectMessageUpdated { .. }
                | NetworkBridgeTick::PeerDirectMessageFailed { .. },
            ) = tick_b.as_ref()
            {
                last_direct_tick_b = format!("{tick_b:?}");
            }
        }
        let rel_a = relationship_state_for(&dir_a, &remote_b);
        let rel_b = relationship_state_for(&dir_b, &remote_a);
        let dm_a = dm_thread_state_for(&dir_a, &remote_b);
        let dm_b = dm_thread_state_for(&dir_b, &remote_a);
        let contact_a = contact_material_for(&dir_a, &remote_b).is_some();
        let contact_b = contact_material_for(&dir_b, &remote_a).is_some();
        rel_a == Some(PeerRelationshipState::Accepted)
            && rel_b == Some(PeerRelationshipState::Accepted)
            && dm_a == Some(PeerDmSessionState::Ready)
            && dm_b == Some(PeerDmSessionState::Ready)
            && contact_a
            && contact_b
    });
    assert!(
        session_ready,
        "accepted relationship should establish a ready dm session and exchange contact material; states: a={:?} b={:?} contact_a={} contact_b={} connected_a={:?} connected_b={:?} last_tick_a={} last_tick_b={}",
        dm_thread_state_for(&dir_a, &remote_b),
        dm_thread_state_for(&dir_b, &remote_a),
        contact_material_for(&dir_a, &remote_b).is_some(),
        contact_material_for(&dir_b, &remote_a).is_some(),
        latest_connected_peer_ids(&dir_a),
        latest_connected_peer_ids(&dir_b),
        last_direct_tick_a,
        last_direct_tick_b,
    );

    let contact_a = contact_material_for(&dir_a, &remote_b).expect("contact material for b on a");
    let contact_b = contact_material_for(&dir_b, &remote_a).expect("contact material for a on b");
    for contact in [&contact_a, &contact_b] {
        let transports = contact
            .get("transports")
            .and_then(|value| value.as_array())
            .expect("transports array");
        assert!(
            transports.iter().any(|entry| {
                entry.get("transport").and_then(|value| value.as_str()) == Some("iroh_direct")
            }),
            "contact material should advertise iroh_direct transport"
        );
        let routes = contact
            .get("recommended_routes")
            .expect("recommended_routes object");
        assert_eq!(
            routes
                .get("direct_message")
                .and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes
                .get("backfill_chunk")
                .and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("topic_sync").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("task_sync").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("artifact_blob").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("evidence_blob").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes
                .get("checkpoint_snapshot")
                .and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
    }

    service_a
        .send_peer_direct_message(&remote_b, None, json!({"text":"hello from a"}))
        .expect("send direct message");

    let delivered = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..64 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        let messages_a = dm_messages_for(&dir_a, &thread_id);
        let messages_b = dm_messages_for(&dir_b, &thread_id);
        let outbound_delivered = messages_a.iter().any(|message| {
            message.message_kind == PeerDmMessageKind::Message
                && message.direction == wattswarm_control_plane::control::PeerDmDirection::Outbound
                && message.delivery_state == PeerDmDeliveryState::Delivered
                && message.content == json!({"text":"hello from a"})
        });
        let inbound_delivered = messages_b.iter().any(|message| {
            message.message_kind == PeerDmMessageKind::Message
                && message.direction == wattswarm_control_plane::control::PeerDmDirection::Inbound
                && message.delivery_state == PeerDmDeliveryState::Delivered
                && message.content == json!({"text":"hello from a"})
        });
        outbound_delivered && inbound_delivered
    });
    assert!(delivered, "accepted peers should exchange direct messages");

    let messages_a = dm_messages_for(&dir_a, &thread_id);
    let messages_b = dm_messages_for(&dir_b, &thread_id);
    let outbound_message = messages_a
        .iter()
        .find(|message| {
            message.message_kind == PeerDmMessageKind::Message
                && message.direction == wattswarm_control_plane::control::PeerDmDirection::Outbound
                && message.content == json!({"text":"hello from a"})
        })
        .expect("outbound dm message");
    let sender_statuses =
        data_plane_statuses_for(&dir_a, "dm_message", &outbound_message.message_id);
    let receiver_statuses =
        data_plane_statuses_for(&dir_b, "dm_message", &outbound_message.message_id);
    let expected_dm_digest = format!(
        "sha256:{}",
        sha256_hex(&serde_json::to_vec(&json!({"text":"hello from a"})).expect("dm bytes"))
    );
    assert!(
        content_artifact_exists(&dir_a, ArtifactKind::DirectMessage, &expected_dm_digest),
        "sender should keep local direct-message artifact"
    );
    assert!(
        content_artifact_exists(&dir_b, ArtifactKind::DirectMessage, &expected_dm_digest),
        "receiver should fetch direct-message artifact over iroh"
    );
    assert!(
        messages_a
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::RelationshipEstablished),
        "local side should persist relationship established confirmation"
    );
    assert!(
        messages_a
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::SessionInit),
        "local side should persist session init"
    );
    assert!(
        messages_b
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::RelationshipEstablished),
        "remote side should persist relationship established confirmation"
    );
    assert!(
        messages_b
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::SessionInit),
        "remote side should persist session init"
    );
    assert_eq!(
        sender_statuses.len(),
        1,
        "sender should persist one latest dm data-plane status row"
    );
    assert_eq!(
        receiver_statuses.len(),
        1,
        "receiver should persist one latest dm data-plane status row"
    );
    assert!(
        sender_statuses[0].route == "iroh_control"
            && sender_statuses[0].status == "control_acknowledged",
        "sender should persist the final dm control-plane acknowledgement status"
    );
    assert!(
        receiver_statuses[0].route == "iroh_direct"
            && receiver_statuses[0].status == "content_hydrated",
        "receiver should persist the final dm content hydration status"
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Block, None)
        .expect("send relationship block");
    let blocked = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..64 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Blocked)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Blocked)
    });
    assert!(blocked, "block should sync to both peers");

    let blocked_send =
        service_a.send_peer_direct_message(&remote_b, None, json!({"text":"post-block"}));
    assert!(
        blocked_send.is_err(),
        "blocked peers should no longer be able to send direct messages"
    );

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}
