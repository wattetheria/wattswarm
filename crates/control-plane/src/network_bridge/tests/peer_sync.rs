use super::*;

fn register_contacted_peer(
    service: &mut NetworkBridgeService,
    label: &str,
    seed: [u8; 32],
) -> (PathBuf, NetworkNodeId) {
    let dir = temp_startup_dir(label);
    std::fs::write(dir.join("node_seed.hex"), hex::encode(seed)).expect("write seed");
    ensure_test_relay_urls(&dir);
    let peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
            .expect("peer endpoint")
            .to_string(),
    )
    .expect("peer id");
    let contact =
        export_local_contact_material_for_network_peer_id(&dir, peer.as_str(), observed_at_ms())
            .expect("contact");
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("upsert contact");
    service.mark_peer_connected(peer.clone());
    (dir, peer)
}

#[test]
fn probe_peer_contact_material_sends_for_disconnected_peer_with_contact_material() {
    let dir = temp_startup_dir("contact-material-disconnected-peer");
    std::fs::write(dir.join("node_seed.hex"), hex::encode([121_u8; 32])).expect("write seed");
    ensure_test_relay_urls(&dir);
    let peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
            .expect("peer endpoint")
            .to_string(),
    )
    .expect("peer id");
    let contact =
        export_local_contact_material_for_network_peer_id(&dir, peer.as_str(), observed_at_ms())
            .expect("contact");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("upsert contact");

    let request_id = service
        .probe_peer_contact_material(&peer)
        .expect("probe disconnected peer");

    assert!(request_id.is_some());
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&peer),
        1
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn successful_contact_probe_emits_connected_only_on_first_liveness_transition() {
    let local_dir = temp_startup_dir("contact-material-connected-tick-local");
    let remote_dir = temp_startup_dir("contact-material-connected-tick-remote");
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode([122_u8; 32]))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode([123_u8; 32]))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string(),
    )
    .expect("remote peer id");
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        remote_peer.as_str(),
        observed_at_ms(),
    )
    .expect("remote contact");
    let raw_remote_contact =
        build_contact_material(&remote_dir, remote_peer.as_str()).expect("raw remote contact");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            [122_u8; 32],
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    service
        .upsert_remote_contact_material(remote_peer.to_string(), remote_contact.clone())
        .expect("upsert remote contact");

    let response_for = |target_node_id: String| RawContactMaterialResponse {
        source_node_id: remote_peer.to_string(),
        target_node_id,
        applied: true,
        contact_material: Some(raw_remote_contact.clone()),
        detail: None,
        updated_at: observed_at_ms(),
    };
    let first_request_id = service
        .probe_peer_contact_material(&remote_peer)
        .expect("first contact probe")
        .expect("first request id");
    let first = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ContactMaterialResponse {
                peer: remote_peer.clone(),
                request_id: first_request_id,
                response: response_for(service.local_peer_id().to_string()),
            },
        )
        .expect("first contact response");
    assert!(matches!(
        first,
        NetworkBridgeTick::Connected { peer } if peer == remote_peer
    ));

    let repeated_request_id = service
        .probe_peer_contact_material(&remote_peer)
        .expect("repeated contact probe")
        .expect("repeated request id");
    let repeated = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ContactMaterialResponse {
                peer: remote_peer.clone(),
                request_id: repeated_request_id,
                response: response_for(service.local_peer_id().to_string()),
            },
        )
        .expect("repeated contact response");
    assert!(matches!(
        repeated,
        NetworkBridgeTick::TransportNotice { detail }
            if detail == format!("contact_material_updated peer={remote_peer}")
    ));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn smarter_backfill_prefers_peer_with_known_scope_activity() {
    let dir_a = temp_startup_dir("backfill-peer-a");
    let dir_b = temp_startup_dir("backfill-peer-b");
    std::fs::write(dir_a.join("node_seed.hex"), hex::encode([41_u8; 32])).expect("write seed a");
    std::fs::write(dir_b.join("node_seed.hex"), hex::encode([42_u8; 32])).expect("write seed b");
    ensure_test_relay_urls(&dir_a);
    ensure_test_relay_urls(&dir_b);
    let node_a = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_a)
            .expect("node a endpoint")
            .to_string(),
    )
    .expect("node a id");
    let node_b = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_b)
            .expect("node b endpoint")
            .to_string(),
    )
    .expect("node b id");
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let other_scope = SwarmScope::Node("lab-2".to_owned());
    let now = Instant::now();

    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let contact_a = export_local_contact_material_for_network_peer_id(
        &dir_a,
        node_a.as_str(),
        observed_at_ms(),
    )
    .expect("contact a");
    let contact_b = export_local_contact_material_for_network_peer_id(
        &dir_b,
        node_b.as_str(),
        observed_at_ms(),
    )
    .expect("contact b");
    service
        .runtime
        .upsert_remote_contact_material(node_a.to_string(), contact_a)
        .expect("upsert contact a");
    service
        .runtime
        .upsert_remote_contact_material(node_b.to_string(), contact_b)
        .expect("upsert contact b");
    service.connected_peers.insert(node_a.clone());
    service.connected_peers.insert(node_b.clone());

    let mut state_a = PeerSyncState::new(now - Duration::from_secs(30));
    state_a.known_scopes.insert(other_scope);
    let mut state_b = PeerSyncState::new(now - Duration::from_secs(30));
    state_b.known_scopes.insert(target_scope.clone());
    service.peer_sync_state.insert(node_a, state_a);
    service.peer_sync_state.insert(node_b.clone(), state_b);

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&target_scope, now),
        Some(node_b)
    );
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_a);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_b);
    let _ = std::fs::remove_dir_all(dir_a);
    let _ = std::fs::remove_dir_all(dir_b);
}

#[test]
fn global_backfill_requires_provider_and_scoped_backfill_requires_known_scope() {
    let dir = temp_startup_dir("backfill-peer-unknown-scope");
    std::fs::write(dir.join("node_seed.hex"), hex::encode([44_u8; 32])).expect("write seed");
    ensure_test_relay_urls(&dir);
    let peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
            .expect("peer endpoint")
            .to_string(),
    )
    .expect("peer id");
    let contact =
        export_local_contact_material_for_network_peer_id(&dir, peer.as_str(), observed_at_ms())
            .expect("contact");
    let target_scope = SwarmScope::Group("crew-7".to_owned());
    let now = Instant::now();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("upsert contact");
    service.connected_peers.insert(peer.clone());
    service.peer_sync_state.insert(
        peer.clone(),
        PeerSyncState::new(now - Duration::from_secs(30)),
    );

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, now),
        None
    );
    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        Vec::<SwarmScope>::new()
    );
    assert!(
        !service
            .request_backfill_scopes_for_peer_now(&peer, &node, &[SwarmScope::Global])
            .expect("skip non-provider global backfill")
    );

    service.remember_global_backfill_provider(peer.clone());
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, now),
        Some(peer.clone())
    );
    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![SwarmScope::Global]
    );
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&target_scope, now),
        None
    );

    service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::GossipNeighborUp {
                peer: peer.clone(),
                scope: target_scope.clone(),
                kind: GossipKind::Events,
            }),
        )
        .expect("gossip neighbor up");
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&target_scope, now),
        Some(peer.clone())
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn outbound_backfill_request_does_not_record_peer_liveness() {
    let dir = temp_startup_dir("outbound-backfill-no-liveness");
    std::fs::write(dir.join("node_seed.hex"), hex::encode([45_u8; 32])).expect("write seed");
    ensure_test_relay_urls(&dir);
    let peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
            .expect("peer endpoint")
            .to_string(),
    )
    .expect("peer id");
    let contact =
        export_local_contact_material_for_network_peer_id(&dir, peer.as_str(), observed_at_ms())
            .expect("contact");
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("service node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("upsert contact");
    service.remember_global_backfill_provider(peer.clone());

    assert!(
        service
            .request_backfill_scopes_for_peer_now(&peer, &node, &[SwarmScope::Global])
            .expect("send outbound backfill request")
    );
    let state = service
        .peer_sync_state
        .get(&peer)
        .expect("outbound peer sync state");
    assert!(!state.recently_seen_at(Instant::now()));
    assert_eq!(state.last_seen_age_ms_at(observed_at_ms()), None);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stale_connected_peer_is_not_backfill_eligible_until_liveness_refresh() {
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let (dir, peer) =
        register_contacted_peer(&mut service, "stale-connected-peer-backfill", [47_u8; 32]);
    let now = Instant::now();
    let mut state = PeerSyncState::new(now - PEER_LAST_SEEN_TTL - Duration::from_secs(1));
    state.known_scopes.insert(SwarmScope::Global);
    service.peer_sync_state.insert(peer.clone(), state);
    service.remember_global_backfill_provider(peer.clone());

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, now),
        None
    );

    service.record_peer_liveness(peer.clone());

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, Instant::now()),
        Some(peer.clone())
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn peer_liveness_refresh_does_not_persist_peer_sync_state() {
    let dir = temp_startup_dir("peer-liveness-memory-only");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    let peer = random_network_node_id();

    service.record_peer_liveness(peer.clone());

    assert!(service.peer_recently_seen_at(&peer, Instant::now()));
    let persisted = crate::control::load_network_peer_sync_state_records_state(&dir)
        .expect("load peer sync rows");
    assert!(persisted.is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn persisted_last_seen_survives_reload_without_restoring_liveness() {
    let dir = temp_startup_dir("peer-last-seen-reload");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    let peer = random_network_node_id();
    let observed_after = observed_at_ms();

    assert!(service.mark_peer_connected(peer.clone()));
    service.record_peer_liveness(peer.clone());
    assert!(service.mark_peer_disconnected(peer.clone()));

    let persisted = crate::control::load_network_peer_sync_state_records_state(&dir)
        .expect("load peer sync rows");
    let last_observed_at = persisted[0]
        .last_observed_at
        .expect("persisted last observed timestamp");
    assert!(last_observed_at >= observed_after);

    let mut reloaded = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("reloaded node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("reloaded service");
    reloaded.set_state_dir(dir.clone(), dir.join("ui.state"));
    let state = reloaded
        .peer_sync_state
        .get(&peer)
        .expect("reloaded peer sync state");
    assert!(!state.recently_seen_at(Instant::now()));
    assert!(state.last_seen_age_ms_at(observed_at_ms()).is_some());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stale_connected_peer_expires_and_becomes_reconnect_candidate() {
    let service_dir = temp_startup_dir("stale-connected-peer-diagnostic");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(service_dir.clone(), service_dir.join("ui.state"));
    let (dir, peer) =
        register_contacted_peer(&mut service, "stale-connected-peer-expire", [48_u8; 32]);
    let now = Instant::now();
    service
        .peer_sync_state
        .get_mut(&peer)
        .expect("peer state")
        .last_seen_at = now - PEER_LAST_SEEN_TTL - Duration::from_secs(1);

    assert_eq!(service.expire_stale_connected_peers(now), 1);
    assert!(!service.connected_peers.contains(&peer));
    assert_eq!(service.reconnect_attempts_for_peer(&peer), Some(0));
    let raw = fs::read_to_string(service_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostic log");
    let diagnostic = raw
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("diagnostic json"))
        .find(|entry| entry["phase"] == "connection.expired_stale")
        .expect("stale connection diagnostic");
    assert_eq!(diagnostic["status"], "disconnected");
    assert_eq!(diagnostic["object_kind"], "peer");
    assert_eq!(diagnostic["object_id"], peer.to_string());
    assert_eq!(diagnostic["source_node_id"], peer.to_string());
    assert_eq!(
        diagnostic["details"]["reason"],
        "peer_last_seen_ttl_expired"
    );
    assert_eq!(diagnostic["details"]["peer_id"], peer.to_string());
    assert_eq!(diagnostic["details"]["inflight_backfills"], 0);
    assert_eq!(diagnostic["details"]["known_scope_count"], 0);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(service_dir);
}

#[test]
fn peer_relationship_action_allows_recently_seen_peer_without_live_connection() {
    let local_dir = temp_startup_dir("relationship-action-contact-local");
    let remote_dir = temp_startup_dir("relationship-action-contact-remote");
    let local_seed = [113u8; 32];
    let remote_seed = [114u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);

    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("local node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), remote_contact)
        .expect("upsert contact");
    assert!(!service.connected_peers.contains(&peer));
    service.record_peer_liveness(peer.clone());

    let request_id = service
        .send_peer_relationship_action(
            &remote_endpoint,
            crate::control::PeerRelationshipAction::Request,
            None,
        )
        .expect("relationship request should be queued through runtime contact material");

    assert!(
        service
            .pending_relationship_requests
            .contains_key(&request_id)
    );
    assert!(service.reconnect_attempts_for_peer(&peer).is_some());

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    let _ = std::fs::remove_dir_all(local_dir);
    let _ = std::fs::remove_dir_all(remote_dir);
}

#[test]
fn peer_relationship_action_records_outbound_contact_material_diagnostic() {
    let local_dir = temp_startup_dir("relationship-action-diagnostic-local");
    let remote_dir = temp_startup_dir("relationship-action-diagnostic-remote");
    let local_seed = [117u8; 32];
    let remote_seed = [118u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);

    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("local node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), remote_contact)
        .expect("upsert contact");
    service.record_peer_liveness(peer);

    service
        .send_peer_relationship_action(
            &remote_endpoint,
            crate::control::PeerRelationshipAction::Request,
            None,
        )
        .expect("send relationship request");

    let raw = std::fs::read_to_string(local_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostics");
    assert!(raw.contains("\"phase\":\"peer_relationship.request\""));
    assert!(raw.contains("\"status\":\"sent\""));
    assert!(raw.contains("\"has_contact_material\":true"));
    assert!(raw.contains("\"contact_material_has_private_message_key\":true"));
    assert!(raw.contains("\"contact_material_private_message_key_len\":44"));
    assert!(!raw.contains("\"contact_material\":"));
    assert!(!raw.contains("\"public_key_b64\""));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    let _ = std::fs::remove_dir_all(local_dir);
    let _ = std::fs::remove_dir_all(remote_dir);
}

#[test]
fn contact_material_failures_record_peer_diagnostics() {
    let dir = temp_startup_dir("contact-material-failure-diagnostic");
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));

    service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::ContactMaterialOutboundFailure {
                peer: peer.clone(),
                request_id: crate::network_p2p::ContactMaterialRequestId::new(42),
                error: "relay stream closed".to_owned(),
            }),
        )
        .expect("outbound failure");
    service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::ContactMaterialInboundFailure {
                peer: peer.clone(),
                error: "malformed contact material".to_owned(),
            }),
        )
        .expect("inbound failure");

    let raw =
        fs::read_to_string(dir.join("diagnostics/wattswarm_node.jsonl")).expect("diagnostic log");
    assert!(raw.contains("\"phase\":\"contact_material.outbound\""));
    assert!(raw.contains("\"phase\":\"contact_material.inbound\""));
    assert!(raw.contains("\"status\":\"failed\""));
    assert!(raw.contains(&format!("\"object_id\":\"{}\"", peer)));
    assert!(raw.contains(&format!("\"source_node_id\":\"{}\"", peer)));
    assert!(raw.contains("\"request_id\":\"ContactMaterialRequestId(42)\""));
    assert!(raw.contains("\"error\":\"relay stream closed\""));
    assert!(raw.contains("\"error\":\"malformed contact material\""));

    let rows = diagnostics::list_diagnostics(
        &dir,
        &diagnostics::DiagnosticFilter {
            source_node_id: Some(peer.to_string()),
            limit: Some(10),
            ..diagnostics::DiagnosticFilter::default()
        },
    )
    .expect("list diagnostics");
    assert_eq!(rows.len(), 2);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn contact_material_inbound_request_records_handler_diagnostics() {
    let dir = temp_startup_dir("contact-material-inbound-diagnostic");
    let local_seed = [119u8; 32];
    std::fs::write(dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    ensure_test_relay_urls(&dir);
    let peer = random_network_node_id();
    let request_id: crate::network_p2p::InboundRequestId =
        serde_json::from_value(json!(7)).expect("request id");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(NetworkP2pConfig::default(), dir.clone(), local_seed)
            .expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));

    let err = service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::ContactMaterialRequest {
                peer: peer.clone(),
                request: crate::network_p2p::ContactMaterialRequest {
                    source_node_id: peer.to_string(),
                    target_node_id: service.local_peer_id().to_string(),
                },
                request_id,
            }),
        )
        .expect_err("missing response channel");
    assert!(
        err.to_string()
            .contains("unknown contact material response request id 7")
    );

    let raw =
        fs::read_to_string(dir.join("diagnostics/wattswarm_node.jsonl")).expect("diagnostic log");
    assert!(raw.contains("\"phase\":\"contact_material.inbound.request\""));
    assert!(raw.contains("\"status\":\"received\""));
    assert!(raw.contains("\"phase\":\"contact_material.inbound.response\""));
    assert!(raw.contains("\"status\":\"failed\""));
    assert!(raw.contains(&format!("\"object_id\":\"{}\"", peer)));
    assert!(raw.contains(&format!("\"source_node_id\":\"{}\"", peer)));
    assert!(raw.contains("\"request_id\":\"7\""));
    assert!(raw.contains("\"error\":\"unknown contact material response request id 7\""));
    assert!(raw.contains("\"duration_ms\""));
    assert!(!raw.contains("\"contact_material\":"));
    assert!(!raw.contains("\"public_key_b64\""));

    let rows = diagnostics::list_diagnostics(
        &dir,
        &diagnostics::DiagnosticFilter {
            source_node_id: Some(peer.to_string()),
            limit: Some(10),
            ..diagnostics::DiagnosticFilter::default()
        },
    )
    .expect("list diagnostics");
    assert_eq!(rows.len(), 2);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn queued_peer_relationship_command_remains_pending_until_runtime_ack() {
    let local_dir = temp_startup_dir("relationship-command-pending-local");
    let remote_dir = temp_startup_dir("relationship-command-pending-remote");
    let local_seed = [115u8; 32];
    let remote_seed = [116u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);

    let local_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&local_dir)
            .expect("local endpoint")
            .to_string();
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    let local = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("local node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), remote_contact)
        .expect("upsert contact");
    service.record_peer_liveness(peer);
    let envelope = default_agent_envelope(
        &local_endpoint,
        &remote_endpoint,
        "social.friend.request",
        json!({
            "action": "request",
            "correlation_id": "correlation-1",
            "request_id": "request-1"
        }),
    );
    enqueue_peer_relationship_action_command(
        &local_dir,
        &remote_endpoint,
        crate::control::PeerRelationshipAction::Request,
        envelope,
    )
    .expect("enqueue relationship command");

    let processed = process_pending_network_commands(&mut node, &mut service, &local_dir)
        .expect("process pending commands");

    assert_eq!(processed, 1);
    assert_eq!(service.pending_relationship_requests.len(), 1);
    let pending = std::fs::read_to_string(local_dir.join("pending_network_commands.jsonl"))
        .expect("read pending commands");
    assert_eq!(
        pending
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
        1
    );
    for pending in service.pending_relationship_requests.values_mut() {
        pending.started_at_ms = (observed_at_ms() as i64).saturating_sub(40_000);
    }
    let pending_path = local_dir.join("pending_network_commands.jsonl");
    let mut pending_command: serde_json::Value =
        serde_json::from_str(pending.lines().next().expect("pending command line"))
            .expect("pending command json");
    pending_command["next_retry_at"] = json!(0);
    std::fs::write(&pending_path, format!("{pending_command}\n"))
        .expect("force pending command due");

    let processed = process_pending_network_commands(&mut node, &mut service, &local_dir)
        .expect("process stale in-flight pending command");

    assert_eq!(processed, 0);
    assert!(service.pending_relationship_requests.is_empty());
    let pending = std::fs::read_to_string(&pending_path).expect("read pending commands");
    let pending_command: serde_json::Value =
        serde_json::from_str(pending.lines().next().expect("pending command line"))
            .expect("pending command json");
    assert_eq!(pending_command["attempts"], json!(1));
    assert_eq!(
        pending_command["last_error"],
        json!("peer relationship request timed out without runtime result")
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    let _ = std::fs::remove_dir_all(local_dir);
    let _ = std::fs::remove_dir_all(remote_dir);
}

#[test]
fn gossip_source_does_not_mark_peer_connected_for_backfill() {
    let dir = temp_startup_dir("gossip-source-not-connected");
    std::fs::write(dir.join("node_seed.hex"), hex::encode([43_u8; 32])).expect("write seed");
    ensure_test_relay_urls(&dir);
    let peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
            .expect("peer endpoint")
            .to_string(),
    )
    .expect("peer id");
    let contact =
        export_local_contact_material_for_network_peer_id(&dir, peer.as_str(), observed_at_ms())
            .expect("contact");
    let target_scope = SwarmScope::Group("crew-7".to_owned());
    let remote = NodeIdentity::random();
    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: remote.node_id(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
    )
    .expect("signed event");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service
        .runtime
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("upsert contact");

    service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::Gossip {
                propagation_source: peer.clone(),
                message: GossipMessage::Event(EventEnvelope {
                    scope: target_scope.clone(),
                    event: remote_event,
                    content_source_node_id: None,
                }),
            }),
        )
        .expect("gossip event");

    assert!(service.peer_has_scope_activity(&peer, &target_scope));
    assert!(!service.connected_peers.contains(&peer));
    assert_eq!(service.run_anti_entropy(&node).expect("anti entropy"), 0);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn peer_backfill_cursor_is_tracked_per_scope_and_feed() {
    let scope = SwarmScope::Group("crew-7".to_owned());
    let mut state = PeerSyncState::new(Instant::now());

    assert_eq!(state.backfill_cursor(&scope, None), 0);
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 0);

    state.record_backfill_cursor(&scope, None, 5);
    state.record_backfill_cursor(&scope, Some("crew.chat"), 9);
    state.record_backfill_cursor(&scope, None, 3);
    state.reset_backfill_cursor(&scope, Some("market.chat"));

    assert_eq!(state.backfill_cursor(&scope, None), 5);
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 9);
    assert_eq!(state.backfill_cursor(&scope, Some("market.chat")), 0);
}

#[test]
fn inbound_backfill_authorization_keeps_global_open_and_requires_scoped_activity() {
    let peer = random_network_node_id();
    let served_scope = SwarmScope::Group("crew-7".to_owned());
    let unserved_scope = SwarmScope::Group("private-7".to_owned());
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, served_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let global_request = BackfillRequest {
        scope: SwarmScope::Global,
        from_event_seq: 0,
        limit: 8,
        head_only: false,
        feed_key: None,
        known_event_ids: Vec::new(),
    };
    let served_request = BackfillRequest {
        scope: served_scope.clone(),
        from_event_seq: 0,
        limit: 8,
        head_only: false,
        feed_key: None,
        known_event_ids: Vec::new(),
    };
    let unserved_request = BackfillRequest {
        scope: unserved_scope.clone(),
        from_event_seq: 0,
        limit: 8,
        head_only: false,
        feed_key: None,
        known_event_ids: Vec::new(),
    };

    assert!(service.inbound_backfill_authorized(&peer, &global_request));
    assert!(service.inbound_backfill_authorized(&peer, &served_request));
    assert!(!service.inbound_backfill_authorized(&peer, &unserved_request));

    service.record_peer_scope_activity(peer.clone(), &unserved_scope);
    assert!(service.inbound_backfill_authorized(&peer, &unserved_request));
}

#[test]
fn peer_sync_state_persists_scope_cursor_and_remote_heads() {
    let dir = temp_startup_dir("peer-sync-state-persist");
    let peer = random_network_node_id();
    let scope = SwarmScope::Group("crew-7".to_owned());
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("store.state"));

    service.record_peer_scope_activity(peer.clone(), &scope);
    service.record_peer_backfill_cursor(peer.clone(), &scope, Some("crew.chat"), 42);
    service.record_peer_remote_head_event_ids(
        peer.clone(),
        &scope,
        Some("crew.chat"),
        &["evt-head".to_owned()],
    );

    let mut reloaded = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    reloaded.set_state_dir(dir.clone(), dir.join("store.state"));
    let state = reloaded
        .peer_sync_state
        .get(&peer)
        .expect("reloaded peer sync state");

    assert!(state.known_scopes.contains(&scope));
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 42);
    assert_eq!(
        state
            .remote_head_event_ids
            .get(&BackfillLaneKey::new(&scope, Some("crew.chat"))),
        Some(&vec!["evt-head".to_owned()])
    );
    assert!(!state.recently_seen_at(Instant::now()));
    assert!(state.last_seen_age_ms_at(observed_at_ms()).is_some());
    assert!(!dir.join("network_peer_sync_state.json").exists());
    let persisted = crate::control::load_network_peer_sync_state_records_state(&dir)
        .expect("load peer sync DB rows");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].network_peer_id, peer.to_string());
    assert_eq!(
        serde_json::from_str::<Vec<SwarmScope>>(&persisted[0].known_scopes_json)
            .expect("known scopes JSON"),
        vec![scope]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn peer_scope_activity_persists_only_changed_peer() {
    let dir = temp_startup_dir("peer-sync-state-single-peer-persist");
    let changed_peer = random_network_node_id();
    let unchanged_peer = random_network_node_id();
    let scope = SwarmScope::Group("crew-7".to_owned());
    for (peer, updated_at) in [(&changed_peer, 10), (&unchanged_peer, 20)] {
        crate::control::save_network_peer_sync_state_record_state(
            &dir,
            &crate::control::NetworkPeerSyncStateRecord {
                network_peer_id: peer.to_string(),
                known_scopes_json: "[]".to_owned(),
                backfill_cursors_json: "[]".to_owned(),
                remote_heads_json: "[]".to_owned(),
                backfill_successes: 0,
                backfill_failures: 0,
                last_observed_at: None,
                updated_at,
            },
        )
        .expect("save peer sync row");
    }
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("store.state"));

    service.record_peer_scope_activity(changed_peer.clone(), &scope);

    let records =
        crate::control::load_network_peer_sync_state_records_state(&dir).expect("load rows");
    let changed = records
        .iter()
        .find(|record| record.network_peer_id == changed_peer.to_string())
        .expect("changed peer row");
    let unchanged = records
        .iter()
        .find(|record| record.network_peer_id == unchanged_peer.to_string())
        .expect("unchanged peer row");
    assert!(changed.updated_at > 10);
    assert_eq!(unchanged.updated_at, 20);
    assert_eq!(
        serde_json::from_str::<Vec<SwarmScope>>(&changed.known_scopes_json)
            .expect("known scopes JSON"),
        vec![scope]
    );
    assert_eq!(unchanged.known_scopes_json, "[]");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn backfill_response_progress_persists_peer_sync_state_once() {
    let dir = temp_startup_dir("peer-sync-backfill-response-progress");
    let peer = random_network_node_id();
    let scope = SwarmScope::Group("crew-7".to_owned());
    let feed_key = "crew.chat";
    let request_id = BackfillRequestId::new(77);
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("store.state"));
    let mut state = PeerSyncState::new(Instant::now());
    state.record_pending_backfill(
        request_id,
        scope.clone(),
        Some(feed_key.to_owned()),
        Instant::now(),
    );
    service.peer_sync_state.insert(peer.clone(), state);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillResponse {
                peer: peer.clone(),
                request_id,
                response: BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 42,
                    head_only: false,
                    feed_key: Some(feed_key.to_owned()),
                    head_event_ids: Vec::new(),
                    events: Vec::new(),
                },
            },
        )
        .expect("backfill response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::BackfillApplied {
            peer: seen,
            request_id: seen_request_id,
            events: 0,
        } if seen == peer && seen_request_id == request_id
    ));
    let state = service
        .peer_sync_state
        .get(&peer)
        .expect("updated peer state");
    assert!(state.known_scopes.contains(&scope));
    assert_eq!(state.inflight_backfills(), 0);
    assert_eq!(state.backfill_successes, 1);
    assert_eq!(state.backfill_failures, 0);
    assert_eq!(state.backfill_cursor(&scope, Some(feed_key)), 42);

    let mut reloaded = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    reloaded.set_state_dir(dir.clone(), dir.join("store.state"));
    let reloaded_state = reloaded
        .peer_sync_state
        .get(&peer)
        .expect("reloaded peer sync state");
    assert!(reloaded_state.known_scopes.contains(&scope));
    assert_eq!(reloaded_state.inflight_backfills(), 0);
    assert_eq!(reloaded_state.backfill_successes, 1);
    assert_eq!(reloaded_state.backfill_failures, 0);
    assert_eq!(reloaded_state.backfill_cursor(&scope, Some(feed_key)), 42);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn head_only_response_without_gap_does_not_schedule_event_page() {
    let peer = random_network_node_id();
    let scope = SwarmScope::Global;
    let request_id = BackfillRequestId::new(78);
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let local_event = node
        .emit_at(
            1,
            crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
                checkpoint_id: "cp-local-head".to_owned(),
                up_to_seq: 0,
            }),
            100,
        )
        .expect("local event");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("network node"),
        std::slice::from_ref(&scope),
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let mut state = PeerSyncState::new(Instant::now());
    state.record_backfill_cursor(&scope, None, 1);
    state.record_pending_backfill(request_id, scope.clone(), None, Instant::now());
    service.peer_sync_state.insert(peer.clone(), state);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillResponse {
                peer: peer.clone(),
                request_id,
                response: BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 1,
                    head_only: true,
                    feed_key: None,
                    head_event_ids: vec![local_event.event_id],
                    events: Vec::new(),
                },
            },
        )
        .expect("head response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::TransportNotice { detail }
            if detail.contains("gap=false") && detail.contains("full_request_sent=false")
    ));
    assert_eq!(service.peer_sync_state[&peer].inflight_backfills(), 0);
}

#[test]
fn head_only_response_with_unknown_head_schedules_one_event_page() {
    let peer = random_network_node_id();
    let scope = SwarmScope::Global;
    let request_id = BackfillRequestId::new(79);
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("network node"),
        std::slice::from_ref(&scope),
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let mut state = PeerSyncState::new(Instant::now());
    state.record_backfill_cursor(&scope, None, 42);
    state.record_pending_backfill(request_id, scope.clone(), None, Instant::now());
    service.peer_sync_state.insert(peer.clone(), state);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillResponse {
                peer: peer.clone(),
                request_id,
                response: BackfillResponse {
                    scope: scope.clone(),
                    next_from_event_seq: 42,
                    head_only: true,
                    feed_key: None,
                    head_event_ids: vec!["missing-event".to_owned()],
                    events: Vec::new(),
                },
            },
        )
        .expect("head response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::TransportNotice { detail }
            if detail.contains("gap=true") && detail.contains("full_request_sent=true")
    ));
    let state = &service.peer_sync_state[&peer];
    assert_eq!(state.inflight_backfills(), 1);
    assert_eq!(state.backfill_cursor(&scope, None), 0);
}

#[test]
fn peer_sync_state_migrates_legacy_json_to_db() {
    let dir = temp_startup_dir("peer-sync-state-legacy-migrate");
    let peer = random_network_node_id();
    let scope = SwarmScope::Group("crew-7".to_owned());
    std::fs::write(
        legacy_peer_sync_state_path(&dir),
        serde_json::to_vec_pretty(&vec![LegacyPeerSyncStateRecord {
            peer_id: peer.to_string(),
            known_scopes: vec![scope.clone()],
            backfill_cursors: vec![PersistedBackfillCursorRecord {
                lane: BackfillLaneKey::new(&scope, Some("crew.chat")),
                cursor: 51,
            }],
            remote_heads: vec![PersistedBackfillRemoteHeadRecord {
                lane: BackfillLaneKey::new(&scope, Some("crew.chat")),
                head_event_ids: vec!["evt-head".to_owned()],
            }],
            backfill_successes: 2,
            backfill_failures: 1,
        }])
        .expect("legacy peer sync JSON"),
    )
    .expect("write legacy peer sync JSON");

    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("store.state"));

    assert!(!legacy_peer_sync_state_path(&dir).exists());
    let state = service
        .peer_sync_state
        .get(&peer)
        .expect("migrated peer sync state");
    assert!(state.known_scopes.contains(&scope));
    assert_eq!(state.backfill_cursor(&scope, Some("crew.chat")), 51);
    assert_eq!(state.backfill_successes, 2);
    assert_eq!(state.backfill_failures, 1);
    let persisted = crate::control::load_network_peer_sync_state_records_state(&dir)
        .expect("load migrated peer sync DB rows");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].network_peer_id, peer.to_string());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn connection_closed_with_remaining_established_keeps_peer_state() {
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(peer.clone());
    service
        .peer_sync_state
        .insert(peer.clone(), PeerSyncState::new(Instant::now()));

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer: peer.clone(),
                remaining_established: 1,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    assert!(service.connected_peers.contains(&peer));
    assert!(service.peer_sync_state.contains_key(&peer));
}

#[test]
fn connection_closed_with_zero_remaining_preserves_peer_sync_state() {
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(peer.clone());
    service
        .peer_sync_state
        .insert(peer.clone(), PeerSyncState::new(Instant::now()));

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer: peer.clone(),
                remaining_established: 0,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::Disconnected { peer: seen } if seen == peer));
    assert!(!service.connected_peers.contains(&peer));
    assert!(service.peer_sync_state.contains_key(&peer));
    assert_eq!(service.peer_sync_state[&peer].inflight_backfills(), 0);
}

#[test]
fn stale_backfill_requests_expire_and_release_peer_slot() {
    let state_dir = temp_startup_dir("backfill-timeout-release");
    let peer = random_network_node_id();
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(state_dir.clone(), state_dir.join("control.sqlite"));
    let mut state = PeerSyncState::new(Instant::now());
    state.record_pending_backfill(
        BackfillRequestId::new(99),
        SwarmScope::Global,
        None,
        Instant::now() - Duration::from_secs(60),
    );
    service.peer_sync_state.insert(peer.clone(), state);

    let expired = service.expire_stale_backfill_requests(Instant::now());

    assert_eq!(expired, 1);
    let state = service.peer_sync_state.get(&peer).expect("peer state");
    assert_eq!(state.inflight_backfills(), 0);
    assert_eq!(state.backfill_failures, 1);
    let lane = BackfillLaneKey::new(&SwarmScope::Global, None);
    let health = state
        .backfill_lane_health
        .get(&lane)
        .expect("global lane health");
    assert_eq!(health.cooldown, BACKFILL_LANE_COOLDOWN_INITIAL);
    assert!(health.cooldown_until.is_some());
    let raw = fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostics");
    assert!(raw.contains("\"phase\":\"request.timeout\""));
    assert!(raw.contains("\"cooldown_ms\":30000"));
}

#[test]
fn backfill_timeout_cools_lane_and_removes_peer_from_selection_until_probe_window() {
    let now = Instant::now();
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let (dir, peer) =
        register_contacted_peer(&mut service, "backfill-timeout-cools-peer", [45_u8; 32]);
    service.remember_global_backfill_provider(peer.clone());
    let mut state = PeerSyncState::new(now - Duration::from_secs(60));
    state.record_pending_backfill(
        BackfillRequestId::new(100),
        SwarmScope::Global,
        None,
        now - BACKFILL_TIMEOUT_FALLBACK,
    );
    service.peer_sync_state.insert(peer.clone(), state);

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, now),
        Some(peer.clone())
    );
    assert_eq!(service.expire_stale_backfill_requests(now), 1);
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, now),
        None
    );

    let probe_at = now + BACKFILL_LANE_COOLDOWN_INITIAL + Duration::from_millis(1);
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, probe_at),
        Some(peer.clone())
    );
    service
        .peer_sync_state
        .get_mut(&peer)
        .expect("peer state")
        .record_pending_backfill(
            BackfillRequestId::new(101),
            SwarmScope::Global,
            None,
            probe_at,
        );
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&SwarmScope::Global, probe_at),
        None
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn multi_lane_backfill_timeouts_cool_peer_once_and_allow_single_probe() {
    let now = Instant::now() - BACKFILL_PEER_COOLDOWN_INITIAL - Duration::from_secs(1);
    let global = SwarmScope::Global;
    let crew_a = SwarmScope::Group("crew-a".to_owned());
    let crew_b = SwarmScope::Group("crew-b".to_owned());
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[global.clone(), crew_a.clone(), crew_b.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let (dir, peer) = register_contacted_peer(
        &mut service,
        "backfill-timeout-cools-peer-once",
        [46_u8; 32],
    );
    service.remember_global_backfill_provider(peer.clone());
    let mut state = PeerSyncState::new(now - Duration::from_secs(60));
    for (offset, scope) in [global.clone(), crew_a.clone(), crew_b.clone()]
        .into_iter()
        .enumerate()
    {
        state.record_pending_backfill(
            BackfillRequestId::new(400 + offset as u64),
            scope,
            None,
            now - BACKFILL_TIMEOUT_FALLBACK,
        );
    }
    service.peer_sync_state.insert(peer.clone(), state);

    assert_eq!(service.expire_stale_backfill_requests(now), 3);
    let state = service.peer_sync_state.get(&peer).expect("peer state");
    assert_eq!(
        state.backfill_peer_health.cooldown,
        BACKFILL_PEER_COOLDOWN_INITIAL
    );
    assert!(state.backfill_peer_health.cooldown_until.is_some());
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&global, now),
        None
    );

    let probe_at = now + BACKFILL_PEER_COOLDOWN_INITIAL + Duration::from_millis(1);
    assert_eq!(
        service.preferred_backfill_peer_for_scope(&global, probe_at),
        Some(peer.clone())
    );
    assert!(
        service
            .request_backfill_scopes_for_peer_now(
                &peer,
                &node,
                &[global.clone(), crew_a.clone(), crew_b.clone()],
            )
            .expect("request probe")
    );
    let state = service.peer_sync_state.get(&peer).expect("peer state");
    assert_eq!(state.inflight_backfills(), 1);
    assert!(!state.backfill_peer_available_at(probe_at));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn failed_recovery_probe_extends_backfill_lane_cooldown() {
    let now = Instant::now();
    let mut state = PeerSyncState::new(now);
    state.record_pending_backfill(
        BackfillRequestId::new(200),
        SwarmScope::Global,
        None,
        now - BACKFILL_TIMEOUT_FALLBACK,
    );
    let first = state
        .record_backfill_request_failed_at(BackfillRequestId::new(200), now)
        .expect("first timeout");
    assert_eq!(first.cooldown, BACKFILL_LANE_COOLDOWN_INITIAL);
    assert!(!first.was_recovery_probe);

    let probe_at = now + BACKFILL_LANE_COOLDOWN_INITIAL + Duration::from_millis(1);
    state.record_pending_backfill(
        BackfillRequestId::new(201),
        SwarmScope::Global,
        None,
        probe_at,
    );
    let second = state
        .record_backfill_request_failed_at(BackfillRequestId::new(201), probe_at)
        .expect("probe timeout");
    assert_eq!(second.cooldown, BACKFILL_LANE_COOLDOWN_INITIAL * 2);
    assert!(second.was_recovery_probe);
}

#[test]
fn successful_recovery_probe_clears_backfill_lane_cooldown() {
    let now = Instant::now();
    let mut state = PeerSyncState::new(now);
    state.record_pending_backfill(
        BackfillRequestId::new(300),
        SwarmScope::Global,
        None,
        now - BACKFILL_TIMEOUT_FALLBACK,
    );
    state
        .record_backfill_request_failed_at(BackfillRequestId::new(300), now)
        .expect("first timeout");
    state.record_peer_backfill_timeout_at(now);

    let probe_at = now + BACKFILL_LANE_COOLDOWN_INITIAL + Duration::from_millis(1);
    state.record_pending_backfill(
        BackfillRequestId::new(301),
        SwarmScope::Global,
        None,
        probe_at,
    );
    assert!(
        state
            .backfill_lane_health
            .contains_key(&BackfillLaneKey::new(&SwarmScope::Global, None))
    );
    assert!(state.record_backfill_request_completed(BackfillRequestId::new(301), probe_at));
    assert!(state.backfill_peer_available_at(probe_at));
    assert_eq!(state.backfill_peer_health.cooldown, Duration::ZERO);
    assert!(
        !state
            .backfill_lane_health
            .contains_key(&BackfillLaneKey::new(&SwarmScope::Global, None))
    );
}

#[test]
fn backfill_attempt_timeout_scales_and_clamps_for_batch_requests() {
    // No sample yet → conservative fallback (matches the prior fixed window).
    assert_eq!(backfill_attempt_timeout(None), BACKFILL_TIMEOUT_FALLBACK);
    assert_eq!(backfill_attempt_timeout(Some(0)), BACKFILL_TIMEOUT_FALLBACK);
    // Fast peer: 500ms * 4 = 2s, floored to the minimum.
    assert_eq!(backfill_attempt_timeout(Some(500)), BACKFILL_TIMEOUT_MIN);
    // Mid-range scales linearly: 3s * 4 = 12s.
    assert_eq!(
        backfill_attempt_timeout(Some(3_000)),
        Duration::from_secs(12)
    );
    // Slow relay peer clamped to the ceiling: 100s * 4 → 90s.
    assert_eq!(
        backfill_attempt_timeout(Some(100_000)),
        BACKFILL_TIMEOUT_MAX
    );
}

#[test]
fn backfill_retry_jitter_is_bounded() {
    let base = Duration::from_secs(20);
    for _ in 0..32 {
        let delay = backfill_retry_delay_with_jitter(base);
        assert!(delay >= base);
        assert!(delay <= base + BACKFILL_RETRY_JITTER_MAX);
    }
}

#[test]
fn peer_sync_allows_only_one_inflight_request_per_lane() {
    let now = Instant::now();
    let scope = SwarmScope::Group("crew-lane".to_owned());
    let mut state = PeerSyncState::new(now);
    state.record_pending_backfill(
        BackfillRequestId::new(650),
        scope.clone(),
        Some("crew.chat".to_owned()),
        now,
    );

    assert!(state.has_inflight_backfill_lane(&scope, Some("crew.chat")));
    assert!(!state.has_inflight_backfill_lane(&scope, Some("crew.tasks")));
}

#[test]
fn remote_busy_defers_without_marking_peer_failed() {
    let peer = random_network_node_id();
    let request_id = BackfillRequestId::new(651);
    let now = Instant::now();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let mut state = PeerSyncState::new(now);
    state.record_pending_backfill(request_id, SwarmScope::Global, None, now);
    service.peer_sync_state.insert(peer.clone(), state);

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::BackfillOutboundFailure {
                peer: peer.clone(),
                request_id,
                error: "control_busy retry_after_ms=1000".to_owned(),
            },
        )
        .expect("busy response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::TransportNotice { detail }
            if detail.contains("backfill_deferred_remote_busy")
    ));
    let state = &service.peer_sync_state[&peer];
    assert_eq!(state.inflight_backfills(), 0);
    assert_eq!(state.backfill_failures, 0);
    assert_eq!(state.backfill_peer_health.cooldown, Duration::ZERO);
    assert!(state.next_retry_at >= now + service.backfill_retry_after);
}

#[test]
fn backfill_latency_ewma_smooths_round_trip_samples() {
    let mut state = PeerSyncState::new(Instant::now());
    assert_eq!(state.smoothed_backfill_latency_ms, None);
    assert_eq!(state.backfill_request_timeout(), BACKFILL_TIMEOUT_FALLBACK);

    // First sample seeds the EWMA directly.
    state.record_backfill_latency(Duration::from_millis(4_000));
    assert_eq!(state.smoothed_backfill_latency_ms, Some(4_000));

    // Subsequent samples blend: (4000*3 + 8000)/4 = 5000.
    state.record_backfill_latency(Duration::from_millis(8_000));
    assert_eq!(state.smoothed_backfill_latency_ms, Some(5_000));

    // Adaptive timeout follows the smoothed latency: 5s * 4 = 20s.
    assert_eq!(state.backfill_request_timeout(), Duration::from_secs(20));

    // A pathologically large sample saturates without overflow and clamps to the ceiling.
    state.record_backfill_latency(Duration::from_secs(u64::MAX / 1000));
    assert_eq!(state.backfill_request_timeout(), BACKFILL_TIMEOUT_MAX);
}

#[test]
fn expire_stale_backfill_requests_uses_per_peer_adaptive_timeout() {
    let now = Instant::now();
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    // Fast peer: 1s smoothed latency → adaptive timeout floored to MIN (5s).
    let fast_peer = random_network_node_id();
    let mut fast_state = PeerSyncState::new(now);
    fast_state.record_backfill_latency(Duration::from_secs(1));
    fast_state.record_pending_backfill(
        BackfillRequestId::new(700),
        SwarmScope::Global,
        None,
        now - Duration::from_secs(6),
    );
    service
        .peer_sync_state
        .insert(fast_peer.clone(), fast_state);

    // Cold peer: no sample → 30s fallback; the same 6s-old request must survive.
    let cold_peer = random_network_node_id();
    let mut cold_state = PeerSyncState::new(now);
    cold_state.record_pending_backfill(
        BackfillRequestId::new(701),
        SwarmScope::Global,
        None,
        now - Duration::from_secs(6),
    );
    service
        .peer_sync_state
        .insert(cold_peer.clone(), cold_state);

    // Only the fast peer's request exceeds its (5s) adaptive timeout.
    assert_eq!(service.expire_stale_backfill_requests(now), 1);
    assert_eq!(service.peer_sync_state[&fast_peer].inflight_backfills(), 0);
    assert_eq!(service.peer_sync_state[&cold_peer].inflight_backfills(), 1);
}

#[test]
fn pending_backfill_request_keeps_timeout_chosen_at_send_time() {
    let now = Instant::now();
    let peer = random_network_node_id();
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let mut state = PeerSyncState::new(now);

    state.record_backfill_latency(Duration::from_secs(1));
    assert_eq!(state.backfill_request_timeout(), BACKFILL_TIMEOUT_MIN);
    state.record_pending_backfill(
        BackfillRequestId::new(702),
        SwarmScope::Global,
        None,
        now - BACKFILL_TIMEOUT_MIN - Duration::from_millis(1),
    );

    state.record_backfill_latency(Duration::from_secs(100));
    assert!(state.backfill_request_timeout() > BACKFILL_TIMEOUT_MIN);
    service.peer_sync_state.insert(peer.clone(), state);

    assert_eq!(service.expire_stale_backfill_requests(now), 1);
    assert_eq!(service.peer_sync_state[&peer].inflight_backfills(), 0);
}

#[test]
fn backfill_requests_respect_remaining_peer_slots() {
    let peer = random_network_node_id();
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, SwarmScope::Group("crew-a".to_owned())],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let mut state = PeerSyncState::new(Instant::now());
    for index in 0..(MAX_INFLIGHT_BACKFILLS_PER_PEER - 1) {
        state.record_pending_backfill(
            BackfillRequestId::new(1_000 + index as u64),
            SwarmScope::Global,
            None,
            Instant::now(),
        );
    }
    service.peer_sync_state.insert(peer.clone(), state);

    assert!(
        service
            .request_backfill_scopes_for_peer_now(
                &peer,
                &node,
                &[SwarmScope::Global, SwarmScope::Group("crew-a".to_owned())],
            )
            .expect("request backfill")
    );

    assert_eq!(
        service
            .peer_sync_state
            .get(&peer)
            .expect("peer state")
            .inflight_backfills(),
        MAX_INFLIGHT_BACKFILLS_PER_PEER
    );
}

#[test]
fn bounded_backfill_scheduler_rotates_across_lanes() {
    let peer = random_network_node_id();
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let scopes = vec![
        SwarmScope::Group("crew-a".to_owned()),
        SwarmScope::Group("crew-b".to_owned()),
        SwarmScope::Group("crew-c".to_owned()),
    ];
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("network node"),
        &scopes,
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert!(
        service
            .request_backfill_scopes_for_peer_now(&peer, &node, &scopes)
            .expect("first scheduling pass")
    );
    let first_lanes = service.peer_sync_state[&peer]
        .inflight_backfill_requests
        .values()
        .map(PendingBackfillRequest::lane_key)
        .collect::<HashSet<_>>();
    assert_eq!(first_lanes.len(), MAX_INFLIGHT_BACKFILLS_PER_PEER);
    let completed = service.peer_sync_state[&peer]
        .inflight_backfill_requests
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for request_id in completed {
        assert!(
            service
                .peer_sync_state
                .get_mut(&peer)
                .expect("peer state")
                .record_backfill_request_completed(request_id, Instant::now())
        );
    }
    service
        .peer_sync_state
        .get_mut(&peer)
        .expect("peer state")
        .next_retry_at = Instant::now();

    assert!(
        service
            .request_backfill_scopes_for_peer_now(&peer, &node, &scopes)
            .expect("second scheduling pass")
    );
    let all_lanes = service.peer_sync_state[&peer]
        .inflight_backfill_requests
        .values()
        .map(PendingBackfillRequest::lane_key)
        .chain(first_lanes)
        .collect::<HashSet<_>>();
    assert_eq!(all_lanes.len(), scopes.len());
}

#[test]
fn connection_established_diagnostics_skip_duplicate_peer_events() {
    let dir = temp_startup_dir("connection-established-diagnostics-dedupe");
    let peer = random_network_node_id();
    let address = "203.0.113.10:4001".parse::<NetworkAddress>().expect("addr");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));

    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionEstablished {
                peer: peer.clone(),
                remote_addr: address.clone(),
            },
        )
        .expect("first connection event");
    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionEstablished {
                peer: peer.clone(),
                remote_addr: address,
            },
        )
        .expect("duplicate connection event");

    let raw =
        fs::read_to_string(dir.join("diagnostics/wattswarm_node.jsonl")).expect("diagnostic log");
    assert_eq!(
        raw.matches("\"phase\":\"connection.established\"").count(),
        1
    );
    let rows = crate::control::load_discovered_peer_records_state(&dir).expect("load rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, peer.to_string());
    assert_eq!(rows[0].source_kind, "connected");

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn connection_established_persists_relay_contact_material_from_configured_relay() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com/"),
    );
    let dir = temp_startup_dir("connection-established-relay-contact");
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));

    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionEstablished {
                peer: peer.clone(),
                remote_addr: NetworkAddress::new(peer.to_string()).expect("peer address"),
            },
        )
        .expect("connection event");

    assert_eq!(service.known_remote_contact_count(), 1);
    let rows = crate::control::load_peer_metadata_records_state(&dir).expect("peer metadata rows");
    let row = rows
        .iter()
        .find(|row| row.node_id == peer.to_string())
        .expect("peer metadata row");
    let material = row
        .contact_material
        .as_ref()
        .expect("persisted contact material");
    assert_eq!(
        material["transports"][0]["metadata"]["endpoint_id"],
        peer.to_string()
    );
    assert_eq!(
        material["transports"][0]["extra"]["relay_urls"],
        json!(["https://relay.wattetheria.com"])
    );

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn gossip_from_peer_persists_relay_contact_material_from_configured_relay() {
    let _env_lock = lock_env_test_mutex();
    let _relay_urls = EnvVarGuard::set(
        wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS,
        Some("https://relay.wattetheria.com"),
    );
    let dir = temp_startup_dir("gossip-relay-contact");
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let peer = random_network_node_id();
    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: remote.node_id(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
    )
    .expect("signed event");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));

    service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::Gossip {
                propagation_source: peer.clone(),
                message: GossipMessage::Event(EventEnvelope {
                    scope: SwarmScope::Global,
                    event: remote_event,
                    content_source_node_id: None,
                }),
            }),
        )
        .expect("gossip event");

    assert_eq!(service.known_remote_contact_count(), 1);
    let rows = crate::control::load_peer_metadata_records_state(&dir).expect("peer metadata rows");
    assert!(rows.iter().any(|row| row.node_id == peer.to_string()
        && row.contact_material.as_ref().is_some_and(|material| {
            material["transports"][0]["extra"]["relay_urls"]
                == json!(["https://relay.wattetheria.com"])
        })));

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn connection_closed_diagnostics_skip_duplicate_peer_events() {
    let dir = temp_startup_dir("connection-closed-diagnostics-dedupe");
    let peer = random_network_node_id();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    service.connected_peers.insert(peer.clone());

    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer: peer.clone(),
                remaining_established: 0,
            },
        )
        .expect("first close event");
    service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer: peer.clone(),
                remaining_established: 0,
            },
        )
        .expect("duplicate close event");

    let raw =
        fs::read_to_string(dir.join("diagnostics/wattswarm_node.jsonl")).expect("diagnostic log");
    assert_eq!(raw.matches("\"phase\":\"connection.closed\"").count(), 1);
    assert!(!service.connected_peers.contains(&peer));

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn diagnostics_append_skips_stable_duplicate_discovery_records() {
    let dir = temp_startup_dir("diagnostics-discovery-stable-dedupe");
    let peer = random_network_node_id();

    for distance_km in [1.0, 2.0] {
        diagnostics::record_diagnostic(
            Some(&dir),
            diagnostics::DiagnosticEvent::new(
                "info",
                "discovery",
                "discovery_v1.peer.accepted",
                "ok",
                format!("discovery v1 peer accepted: {peer}"),
            )
            .object("peer", Some(peer.to_string()))
            .source_node_id(Some(peer.to_string()))
            .details(json!({
                "distance_km": distance_km,
                "registered_contact": true,
                "radius_km": 20.0,
            })),
        );
    }

    let raw =
        fs::read_to_string(dir.join("diagnostics/wattswarm_node.jsonl")).expect("diagnostic log");
    assert_eq!(
        raw.matches("\"phase\":\"discovery_v1.peer.accepted\"")
            .count(),
        1
    );

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn diagnostics_append_keeps_repeated_startup_records() {
    let dir = temp_startup_dir("diagnostics-startup-no-dedupe");

    for _ in 0..2 {
        diagnostics::record_diagnostic(
            Some(&dir),
            diagnostics::DiagnosticEvent::new(
                "info",
                "transport",
                "startup.ready",
                "ok",
                "network bridge startup completed",
            ),
        );
    }

    let raw =
        fs::read_to_string(dir.join("diagnostics/wattswarm_node.jsonl")).expect("diagnostic log");
    assert_eq!(raw.matches("\"phase\":\"startup.ready\"").count(), 2);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn diagnostics_list_collapses_legacy_duplicates_without_time_window() {
    let dir = temp_startup_dir("diagnostics-list-stable-dedupe");
    let path = dir.join("diagnostics/wattswarm_node.jsonl");
    fs::create_dir_all(path.parent().expect("diagnostics parent")).expect("create diagnostics dir");
    let peer = random_network_node_id();
    let older = json!({
        "id": "older",
        "timestamp_ms": 1_u64,
        "level": "info",
        "component": "wattswarm.network_bridge",
        "category": "discovery",
        "phase": "discovery_v1.peer.accepted",
        "status": "ok",
        "message": format!("discovery v1 peer accepted: {peer}"),
        "object_kind": "peer",
        "object_id": peer.to_string(),
        "source_node_id": peer.to_string(),
        "details": {
            "distance_km": 1.0,
            "registered_contact": true,
            "radius_km": 20.0
        }
    });
    let newer = json!({
        "id": "newer",
        "timestamp_ms": 3_600_000_u64,
        "level": "info",
        "component": "wattswarm.network_bridge",
        "category": "discovery",
        "phase": "discovery_v1.peer.accepted",
        "status": "ok",
        "message": format!("discovery v1 peer accepted: {peer}"),
        "object_kind": "peer",
        "object_id": peer.to_string(),
        "source_node_id": peer.to_string(),
        "details": {
            "distance_km": 2.0,
            "registered_contact": true,
            "radius_km": 20.0
        }
    });
    fs::write(&path, format!("{older}\n{newer}\n")).expect("write legacy diagnostics");

    let rows = diagnostics::list_diagnostics(
        &dir,
        &diagnostics::DiagnosticFilter {
            limit: Some(10),
            ..diagnostics::DiagnosticFilter::default()
        },
    )
    .expect("list diagnostics");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "newer");
    assert_eq!(rows[0].timestamp_ms, 3_600_000);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn peer_discovered_event_persists_wan_source_into_local_registry() {
    let dir = temp_startup_dir("peer-discovered-persist");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    let peer = random_network_node_id();
    let address = "203.0.113.10:4001".parse::<NetworkAddress>().expect("addr");

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::PeerDiscovered {
                peer: peer.clone(),
                address: address.clone(),
                source: PeerDiscoverySourceKind::Bootstrap,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    let rows = crate::control::load_discovered_peer_records_state(&dir).expect("load rows");
    assert_eq!(
        rows,
        vec![crate::control::DiscoveredPeerRecord {
            node_id: peer.to_string(),
            source_kind: "bootstrap".to_owned(),
        }]
    );

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn peer_identified_event_persists_peer_metadata_locally() {
    let dir = temp_startup_dir("peer-identified-metadata");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(dir.clone(), dir.join("ui.state"));
    let peer = random_network_node_id();

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::PeerMetadataObserved {
                peer: peer.clone(),
                metadata: crate::network_p2p::PeerMetadata {
                    network_id: "mainnet:watt-galaxy".to_owned(),
                    params_version: 7,
                    params_hash: "params-abc".to_owned(),
                    agent_version_raw: "wattswarm-network-p2p|mainnet:watt-galaxy|7|params-abc"
                        .to_owned(),
                    agent_version_prefix: "wattswarm-network-p2p".to_owned(),
                    protocol_version: "wattswarm/1.0.0".to_owned(),
                    observed_addr: "198.51.100.2:4001".to_owned(),
                    listen_addrs: vec!["203.0.113.10:4001".to_owned()],
                    protocols: vec!["/meshsub/1.1.0".to_owned()],
                },
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    let rows = crate::control::load_peer_metadata_records_state(&dir).expect("load rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, peer.to_string());
    assert_eq!(rows[0].network_id.as_deref(), Some("mainnet:watt-galaxy"));
    assert_eq!(rows[0].params_version, Some(7));
    assert_eq!(rows[0].params_hash.as_deref(), Some("params-abc"));
    assert_eq!(rows[0].handshake_status, "identified");
    assert_eq!(rows[0].listen_addrs, vec!["203.0.113.10:4001"]);
    assert_eq!(rows[0].protocols, vec!["/meshsub/1.1.0"]);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn build_contact_material_accepts_iroh_endpoint_network_peer_id() {
    let dir = temp_startup_dir("iroh-contact-material");
    std::fs::write(
        dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write startup config");
    crate::control::local_node_id(&dir).expect("create local identity");
    let endpoint_id = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir)
        .expect("endpoint id")
        .to_string();

    let contact = build_contact_material(&dir, &endpoint_id).expect("contact material");
    let material: serde_json::Value =
        serde_json::from_str(&contact.material_json).expect("contact material json");

    assert_eq!(material["peer_id"], endpoint_id);
    assert_eq!(material["transports"][0]["transport"], "iroh_direct");
    assert_eq!(
        material["transports"][0]["metadata"]["endpoint_id"],
        endpoint_id
    );
    assert_eq!(
        material["recommended_routes"]["topic_sync"],
        serde_json::json!("iroh_direct")
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir);
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn peer_relationship_response_persists_private_message_contact_material() {
    let local_dir = temp_startup_dir("relationship-contact-local");
    let remote_dir = temp_startup_dir("relationship-contact-remote");
    let local_seed = [111u8; 32];
    let remote_seed = [112u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    for dir in [&local_dir, &remote_dir] {
        std::fs::write(
            dir.join("startup_config.json"),
            serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
                .expect("startup config json"),
        )
        .expect("write startup config");
    }

    let local_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&local_dir)
            .expect("local endpoint")
            .to_string();
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer");
    let remote_contact =
        build_contact_material(&remote_dir, &remote_endpoint).expect("remote contact material");

    crate::control::apply_peer_relationship_action_state(
        &local_dir,
        &remote_endpoint,
        crate::control::PeerRelationshipAction::Request,
        crate::control::PeerRelationshipInitiator::Local,
    )
    .expect("local relationship request");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    let request_id = PeerRelationshipRequestId::new(7);
    let agent_envelope = default_agent_envelope(
        &local_endpoint,
        &remote_endpoint,
        "peer.relationship.request",
        json!({
            "action": "request",
            "remote_node_id": remote_endpoint.clone(),
        }),
    );
    service.pending_relationship_requests.insert(
        request_id,
        PendingPeerRelationshipRequest {
            peer: remote_peer.clone(),
            remote_node_id: remote_endpoint.clone(),
            action: crate::control::PeerRelationshipAction::Request,
            agent_envelope,
            started_at_ms: observed_at_ms() as i64,
        },
    );

    let tick = service
        .process_runtime_event(
            &mut crate::control::open_node(&local_dir, &local_dir.join("ui.state"))
                .expect("open node"),
            NetworkRuntimeEvent::PeerRelationshipResponse {
                peer: remote_peer,
                request_id,
                response: PeerRelationshipResponse {
                    source_node_id: remote_endpoint.clone(),
                    target_node_id: local_endpoint,
                    action: RawPeerRelationshipAction::Request,
                    applied: true,
                    agent_envelope: None,
                    contact_material: Some(remote_contact),
                    relationship_state: Some("requested".to_owned()),
                    detail: None,
                    updated_at: observed_at_ms(),
                },
            },
        )
        .expect("process response");

    assert!(matches!(
        tick,
        NetworkBridgeTick::PeerRelationshipUpdated { .. }
    ));
    let peer_metadata = crate::control::load_peer_metadata_records_state(&local_dir)
        .expect("peer metadata")
        .into_iter()
        .find(|record| record.node_id == remote_endpoint)
        .expect("remote metadata");
    assert!(peer_metadata.private_message_public_key_b64().is_some());

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_contact_material_preserves_private_message_key() {
    let local_dir = temp_startup_dir("relationship-contact-preserve-local");
    let remote_dir = temp_startup_dir("relationship-contact-preserve-remote");
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode([113u8; 32]))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode([114u8; 32]))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let relationship_contact =
        build_contact_material(&remote_dir, &remote_endpoint).expect("relationship contact");

    upsert_contact_material_for_peer(&local_dir, &remote_endpoint, &relationship_contact)
        .expect("save relationship contact");
    let original_key = crate::control::load_peer_metadata_records_state(&local_dir)
        .expect("load peer metadata")
        .into_iter()
        .find(|record| record.node_id == remote_endpoint)
        .and_then(|record| record.private_message_public_key_b64())
        .expect("relationship contact key");

    let discovery_contact = RawContactMaterial {
        material_json: serde_json::to_string(&json!({
            "node_id": remote_endpoint,
            "peer_id": remote_endpoint,
            "listen_addrs": [],
            "generated_at": observed_at_ms(),
            "transports": [],
            "recommended_routes": {
                "direct_message": "iroh_direct"
            },
            "discovery_protocol": "wattswarm-discovery/1"
        }))
        .expect("discovery contact json"),
        signature: Some("discovery-signature".to_owned()),
        generated_at: observed_at_ms(),
    };
    upsert_contact_material_for_peer(&local_dir, &remote_endpoint, &discovery_contact)
        .expect("save discovery contact");

    let peer_metadata = crate::control::load_peer_metadata_records_state(&local_dir)
        .expect("reload peer metadata")
        .into_iter()
        .find(|record| record.node_id == remote_endpoint)
        .expect("remote metadata");
    assert_eq!(
        peer_metadata.private_message_public_key_b64().as_deref(),
        Some(original_key.as_str())
    );
    assert_eq!(
        peer_metadata
            .contact_material
            .as_ref()
            .and_then(|material| material.get("discovery_protocol"))
            .and_then(Value::as_str),
        Some("wattswarm-discovery/1")
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn set_state_dir_loads_persisted_iroh_contact_material_into_runtime() {
    let local_dir = temp_startup_dir("iroh-contact-load-local");
    let remote_dir = temp_startup_dir("iroh-contact-load-remote");
    let local_seed = [81u8; 32];
    let remote_seed = [82u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    std::fs::write(
        remote_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write remote startup config");
    for dir in [&local_dir, &remote_dir] {
        std::fs::write(
            dir.join("startup_config.json"),
            serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
                .expect("startup config json"),
        )
        .expect("write startup config");
    }
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    crate::control::save_peer_metadata_record_state(
        &local_dir,
        &crate::control::PeerMetadataRecord {
            node_id: remote_endpoint.clone(),
            network_id: Some("mainnet".to_owned()),
            params_version: Some(1),
            params_hash: Some("params".to_owned()),
            agent_version_raw: None,
            agent_version_prefix: None,
            protocol_version: None,
            observed_addr: None,
            listen_addrs: Vec::new(),
            protocols: Vec::new(),
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(json!({
                "node_id": remote_endpoint,
                "peer_id": remote_contact.peer_id,
                "transports": [remote_contact],
            })),
            contact_material_signature: None,
            contact_material_updated_at: Some(observed_at_ms()),
            first_identified_at: observed_at_ms(),
            last_identified_at: observed_at_ms(),
        },
    )
    .expect("save peer metadata");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    assert_eq!(service.known_remote_contact_count(), 1);
    let remote_peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), None);
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("persisted contact remains an address-book entry"),
        0
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn persisted_dead_contacts_do_not_become_startup_reconnect_jobs() {
    let local_dir = temp_startup_dir("persisted-dead-contacts-local");
    let remote_dir = temp_startup_dir("persisted-dead-contacts-remote");
    let local_seed = [201u8; 32];
    let remote_seed = [202u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let base_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");

    for _ in 0..20 {
        let peer = random_network_node_id();
        let mut contact = base_contact.clone();
        contact.peer_id = peer.to_string();
        contact.metadata.endpoint_id = Some(peer.to_string());
        contact.extra["endpoint_id"] = json!(peer.to_string());
        crate::control::save_peer_metadata_record_state(
            &local_dir,
            &crate::control::PeerMetadataRecord {
                node_id: peer.to_string(),
                network_id: Some("mainnet".to_owned()),
                params_version: None,
                params_hash: None,
                agent_version_raw: None,
                agent_version_prefix: None,
                protocol_version: None,
                observed_addr: None,
                listen_addrs: Vec::new(),
                protocols: Vec::new(),
                handshake_status: "contact_material".to_owned(),
                last_error: None,
                contact_material: Some(json!({
                    "node_id": peer,
                    "peer_id": contact.peer_id,
                    "transports": [contact],
                })),
                contact_material_signature: None,
                contact_material_updated_at: Some(observed_at_ms()),
                first_identified_at: observed_at_ms(),
                last_identified_at: observed_at_ms(),
            },
        )
        .expect("save dead peer contact");
    }

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    assert_eq!(service.known_remote_contact_count(), 20);
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("historical contacts are address book entries only"),
        0
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn set_state_dir_loads_startup_iroh_bootstrap_contacts_into_runtime() {
    let local_dir = temp_startup_dir("iroh-startup-contact-local");
    let remote_dir = temp_startup_dir("iroh-startup-contact-remote");
    let local_seed = [91u8; 32];
    let remote_seed = [92u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    std::fs::write(
        remote_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write remote startup config");
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    std::fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "relay_urls":["https://relay.example.invalid/"],
            "bootstrap_contacts": [serde_json::to_string(&remote_contact).expect("contact json")]
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    assert_eq!(service.known_remote_contact_count(), 1);
    let remote_peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), Some(0));
    assert!(service.is_global_backfill_provider(&remote_peer));
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("startup contact reconnect supervision"),
        1
    );
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), Some(1));
    let rows =
        crate::control::load_peer_metadata_records_state(&local_dir).expect("peer metadata rows");
    assert!(rows.iter().any(|row| {
        row.node_id == remote_endpoint && row.handshake_status == "startup_contact"
    }));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn network_config_bootstrap_peers_are_global_backfill_providers() {
    let local_dir = temp_startup_dir("iroh-config-bootstrap-local");
    let remote_dir = temp_startup_dir("iroh-config-bootstrap-remote");
    let local_seed = [191u8; 32];
    let remote_seed = [192u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let remote_peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig {
                listen_addrs: vec!["127.0.0.1:0".to_owned()],
                enable_local_discovery: false,
                bootstrap_peers: vec![format!("{remote_endpoint}@127.0.0.1:4002")],
                ..NetworkP2pConfig::default()
            },
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert!(service.is_global_backfill_provider(&remote_peer));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn reconnect_supervision_requires_registered_iroh_contact() {
    let local_dir = temp_startup_dir("reconnect-local");
    let remote_dir = temp_startup_dir("reconnect-remote");
    let local_seed = [93u8; 32];
    let remote_seed = [94u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    std::fs::write(
        remote_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write remote startup config");
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint).expect("remote peer id");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    service.remember_peer_address(
        peer.clone(),
        NetworkAddress::new("127.0.0.1:4002".to_owned()).expect("address"),
    );
    let attempts = service
        .run_reconnect_supervision()
        .expect("run reconnect supervision");

    assert_eq!(attempts, 0);
    assert_eq!(service.known_remote_contact_count(), 0);
    assert_eq!(service.reconnect_attempts_for_peer(&peer), Some(0));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn reconnect_supervision_uses_iroh_contact_with_direct_and_relay_candidates() {
    let local_dir = temp_startup_dir("reconnect-full-contact-local");
    let remote_dir = temp_startup_dir("reconnect-full-contact-remote");
    let local_seed = [103u8; 32];
    let remote_seed = [104u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let mut remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    remote_contact.metadata.listen_addrs = vec!["127.0.0.1:4002".to_owned()];
    remote_contact.extra["direct_addrs"] = json!(["127.0.0.1:4002"]);
    remote_contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let peer = NetworkNodeId::new(remote_endpoint).expect("remote peer id");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    service
        .upsert_remote_contact_material(peer.to_string(), remote_contact)
        .expect("upsert full remote contact");

    let attempts = service
        .run_reconnect_supervision()
        .expect("run reconnect supervision");

    assert_eq!(attempts, 1);
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&peer),
        1,
        "one Iroh control probe must receive all direct and relay candidates"
    );
    let diagnostics = std::fs::read_to_string(local_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("reconnect diagnostics");
    assert!(diagnostics.contains("\"phase\":\"reconnect.bootstrap\""));
    assert!(diagnostics.contains("\"route\":\"iroh_contact\""));
    assert!(!diagnostics.contains("\"route\":\"direct_addr\""));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn remote_contact_uses_extra_direct_address_when_metadata_listen_addrs_are_empty() {
    let local_dir = temp_startup_dir("reconnect-extra-direct-local");
    let remote_dir = temp_startup_dir("reconnect-extra-direct-remote");
    let local_seed = [124u8; 32];
    let remote_seed = [125u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let mut contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    contact.metadata.listen_addrs.clear();
    contact.extra["direct_addrs"] = json!(["18.206.214.6:4002"]);
    contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    service
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("upsert contact");

    assert_eq!(
        service
            .known_peer_addrs
            .get(&peer)
            .map(NetworkAddress::as_str),
        Some("18.206.214.6:4002")
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn reconnect_supervision_fairly_rotates_serialized_contact_probes() {
    let local_dir = temp_startup_dir("reconnect-fair-local");
    let remote_dir = temp_startup_dir("reconnect-fair-remote");
    let local_seed = [105u8; 32];
    let remote_seed = [106u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let mut base_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    base_contact.metadata.listen_addrs.clear();
    base_contact.extra["direct_addrs"] = json!([]);
    base_contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    let mut peers = (0..3).map(|_| random_network_node_id()).collect::<Vec<_>>();
    peers.sort_by_key(ToString::to_string);
    for peer in &peers {
        let mut contact = base_contact.clone();
        contact.peer_id = peer.to_string();
        contact.metadata.endpoint_id = Some(peer.to_string());
        contact.extra["endpoint_id"] = json!(peer.to_string());
        assert!(
            service
                .upsert_remote_contact_material(peer.to_string(), contact)
                .expect("upsert relay-only contact")
        );
    }

    let mut selected = Vec::new();
    for _ in 0..peers.len() {
        if let Some(previous) = selected.last() {
            service.force_pending_contact_material_request_stale_for_peer(previous);
        }
        for peer in &peers {
            service.force_reconnect_due_for_peer(peer);
        }
        assert_eq!(
            service
                .run_reconnect_supervision()
                .expect("run fair reconnect supervision"),
            1
        );
        let current = peers
            .iter()
            .find(|peer| service.pending_contact_material_request_count_for_peer(peer) == 1)
            .expect("one pending contact probe")
            .clone();
        assert!(
            !selected.contains(&current),
            "a peer must not receive a second probe before every due peer receives one"
        );
        selected.push(current);
    }
    assert_eq!(selected.len(), peers.len());

    let previous = selected.last().expect("last selected peer").clone();
    service.force_pending_contact_material_request_stale_for_peer(&previous);
    for peer in &peers {
        service.force_reconnect_due_for_peer(peer);
    }
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("same activity does not restart fair rotation"),
        0
    );
    service.record_reconnect_candidate_activity(selected[0].clone(), Instant::now());
    service.force_reconnect_due_for_peer(&selected[0]);
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("new activity starts one new probe"),
        1
    );
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&selected[0]),
        1,
        "only a peer with new activity may be probed again"
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn reconnect_supervision_skips_expired_contact_candidates() {
    let local_dir = temp_startup_dir("reconnect-active-retention-local");
    let remote_dir = temp_startup_dir("reconnect-active-retention-remote");
    let local_seed = [126u8; 32];
    let remote_seed = [127u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let mut base_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    base_contact.metadata.listen_addrs.clear();
    base_contact.extra["direct_addrs"] = json!([]);
    base_contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));

    let expired_activity = Instant::now() - RECONNECT_CANDIDATE_RETENTION - Duration::from_secs(1);
    let mut expired_peers = Vec::new();
    for _ in 0..20 {
        let peer = random_network_node_id();
        let mut contact = base_contact.clone();
        contact.peer_id = peer.to_string();
        contact.metadata.endpoint_id = Some(peer.to_string());
        contact.extra["endpoint_id"] = json!(peer.to_string());
        service
            .upsert_remote_contact_material(peer.to_string(), contact)
            .expect("upsert expired contact");
        service
            .reconnect_candidate_activity
            .insert(peer.clone(), expired_activity);
        expired_peers.push(peer);
    }
    let fresh_peer = random_network_node_id();
    let mut fresh_contact = base_contact;
    fresh_contact.peer_id = fresh_peer.to_string();
    fresh_contact.metadata.endpoint_id = Some(fresh_peer.to_string());
    fresh_contact.extra["endpoint_id"] = json!(fresh_peer.to_string());
    service
        .upsert_remote_contact_material(fresh_peer.to_string(), fresh_contact)
        .expect("upsert fresh contact");

    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("run active reconnect supervision"),
        1
    );
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&fresh_peer),
        1
    );
    assert!(expired_peers.iter().all(|peer| {
        service.pending_contact_material_request_count_for_peer(peer) == 0
            && service.reconnect_attempts_for_peer(peer) == Some(0)
    }));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn reconnect_supervision_abandons_stale_peer_until_rediscovered() {
    let local_dir = temp_startup_dir("reconnect-abandon-local");
    let remote_dir = temp_startup_dir("reconnect-abandon-remote");
    let local_seed = [107u8; 32];
    let remote_seed = [108u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    let address = NetworkAddress::new("127.0.0.1:4002".to_owned()).expect("address");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    service
        .upsert_remote_contact_material(peer.to_string(), remote_contact)
        .expect("upsert remote contact");
    service.remember_peer_address(peer.clone(), address.clone());
    for _ in 0..PEER_RECONNECT_MAX_ATTEMPTS {
        service.record_reconnect_candidate_activity(peer.clone(), Instant::now());
        service.force_pending_contact_material_request_stale_for_peer(&peer);
        service.force_reconnect_due_for_peer(&peer);
        let attempts = service
            .run_reconnect_supervision()
            .expect("run reconnect supervision");
        assert_eq!(attempts, 1);
    }

    assert!(service.reconnect_abandoned_for_peer(&peer));
    assert_eq!(service.reconnect_attempts_for_peer(&peer), None);
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("abandoned peer is not retried"),
        0
    );

    service.remember_peer_address(peer.clone(), address);
    service.record_peer_liveness(peer.clone());
    assert!(!service.reconnect_abandoned_for_peer(&peer));
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("rediscovered peer is retried"),
        1
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn repeated_contact_generation_does_not_reactivate_abandoned_peer() {
    let local_dir = temp_startup_dir("reconnect-generation-local");
    let remote_dir = temp_startup_dir("reconnect-generation-remote");
    let local_seed = [111u8; 32];
    let remote_seed = [112u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let peer = NetworkNodeId::new(
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string(),
    )
    .expect("remote peer id");
    let mut contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        peer.as_str(),
        observed_at_ms(),
    )
    .expect("remote contact");
    contact.metadata.listen_addrs.clear();
    contact.extra["direct_addrs"] = json!([]);
    contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert!(
        service
            .upsert_remote_contact_material(peer.to_string(), contact.clone())
            .expect("insert contact")
    );
    service.abandon_peer_reconnect(
        peer.clone(),
        PEER_RECONNECT_MAX_ATTEMPTS,
        "iroh_contact",
        None,
    );
    assert!(service.reconnect_abandoned_for_peer(&peer));

    let repeated = service
        .upsert_remote_contact_material(peer.to_string(), contact.clone())
        .expect("repeat contact generation");
    assert!(!repeated);
    service.reactivate_discovered_peer_reconnect(peer.clone(), repeated);
    assert!(service.reconnect_abandoned_for_peer(&peer));

    contact.metadata.generated_at = contact.metadata.generated_at.saturating_add(1);
    let updated = service
        .upsert_remote_contact_material(peer.to_string(), contact)
        .expect("update contact generation");
    assert!(updated);
    service.reactivate_discovered_peer_reconnect(peer.clone(), updated);
    assert!(!service.reconnect_abandoned_for_peer(&peer));
    assert_eq!(service.reconnect_attempts_for_peer(&peer), Some(0));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn fresh_discovery_allows_one_abandoned_recovery_probe_until_success() {
    let local_dir = temp_startup_dir("reconnect-abandoned-recovery-local");
    let remote_dir = temp_startup_dir("reconnect-abandoned-recovery-remote");
    let local_seed = [128u8; 32];
    let remote_seed = [129u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    ensure_test_relay_urls(&remote_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint.clone()).expect("remote peer id");
    let mut contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    contact.metadata.listen_addrs.clear();
    contact.extra["direct_addrs"] = json!([]);
    contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let raw_contact =
        build_contact_material(&remote_dir, &remote_endpoint).expect("raw remote contact");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    assert!(
        service
            .upsert_remote_contact_material(peer.to_string(), contact.clone())
            .expect("insert contact")
    );
    service.abandon_peer_reconnect(
        peer.clone(),
        PEER_RECONNECT_MAX_ATTEMPTS,
        "iroh_contact",
        None,
    );

    assert!(service.reconnect_abandoned_for_peer(&peer));
    assert!(
        !service
            .upsert_remote_contact_material(peer.to_string(), contact.clone())
            .expect("repeat contact generation")
    );
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("equal contact alone does not recover"),
        0
    );

    contact.metadata.generated_at = contact.metadata.generated_at.saturating_add(1);
    assert!(
        service
            .upsert_remote_contact_material(peer.to_string(), contact)
            .expect("updated contact generation")
    );
    service.reactivate_discovered_peer_reconnect(peer.clone(), true);
    assert_eq!(
        service
            .run_reconnect_supervision()
            .expect("fresh discovery recovery probe"),
        1
    );
    assert!(!service.reconnect_abandoned_for_peer(&peer));
    let request_id = *service
        .pending_contact_material_requests
        .keys()
        .next()
        .expect("pending recovery probe");
    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ContactMaterialResponse {
                peer: peer.clone(),
                request_id,
                response: RawContactMaterialResponse {
                    source_node_id: peer.to_string(),
                    target_node_id: service.local_peer_id().to_string(),
                    applied: true,
                    contact_material: Some(raw_contact),
                    detail: None,
                    updated_at: observed_at_ms(),
                },
            },
        )
        .expect("successful recovery response");
    assert!(matches!(
        tick,
        NetworkBridgeTick::Connected { peer: connected } if connected == peer
    ));
    assert!(!service.reconnect_abandoned_for_peer(&peer));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn reconnect_supervision_probes_contact_material_for_disconnected_peer() {
    let local_dir = temp_startup_dir("reconnect-relay-only-local");
    let remote_dir = temp_startup_dir("reconnect-relay-only-remote");
    let local_seed = [109u8; 32];
    let remote_seed = [110u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    std::fs::write(
        remote_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write remote startup config");
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let mut remote_contact = export_local_contact_material_for_network_peer_id(
        &remote_dir,
        &remote_endpoint,
        observed_at_ms(),
    )
    .expect("remote contact");
    remote_contact.metadata.listen_addrs.clear();
    remote_contact.extra["direct_addrs"] = json!([]);
    remote_contact.extra["relay_urls"] = json!(["https://relay.wattetheria.com"]);
    let peer = NetworkNodeId::new(remote_endpoint).expect("remote peer id");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.set_state_dir(local_dir.clone(), local_dir.join("ui.state"));
    service
        .upsert_remote_contact_material(peer.to_string(), remote_contact.clone())
        .expect("upsert relay-only remote contact");

    assert_eq!(service.known_remote_contact_count(), 1);
    assert_eq!(service.reconnect_attempts_for_peer(&peer), Some(0));
    let attempts = service
        .run_reconnect_supervision()
        .expect("run reconnect supervision");

    assert_eq!(attempts, 1);
    assert_eq!(service.reconnect_attempts_for_peer(&peer), Some(1));
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&peer),
        1
    );
    service.force_reconnect_due_for_peer(&peer);
    let attempts = service
        .run_reconnect_supervision()
        .expect("pending contact material request blocks duplicate reconnect probe");
    assert_eq!(attempts, 0);
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&peer),
        1
    );
    service.force_pending_contact_material_request_stale_for_peer(&peer);
    service.force_reconnect_due_for_peer(&peer);
    let attempts = service
        .run_reconnect_supervision()
        .expect("stale pending contact material request does not loop");
    assert_eq!(attempts, 0);
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&peer),
        0
    );
    remote_contact.metadata.generated_at = remote_contact.metadata.generated_at.saturating_add(1);
    assert!(
        service
            .upsert_remote_contact_material(peer.to_string(), remote_contact)
            .expect("updated contact generation")
    );
    service.reactivate_discovered_peer_reconnect(peer.clone(), true);
    service.force_reconnect_due_for_peer(&peer);
    let attempts = service
        .run_reconnect_supervision()
        .expect("fresh discovery triggers one new probe");
    assert_eq!(attempts, 1);
    assert_eq!(
        service.pending_contact_material_request_count_for_peer(&peer),
        1
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn urgent_backfill_request_bypasses_retry_delay_for_local_scope() {
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let local_dir = temp_startup_dir("urgent-backfill-local");
    let remote_dir = temp_startup_dir("urgent-backfill-remote");
    let local_seed = [95u8; 32];
    let remote_seed = [96u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    ensure_test_relay_urls(&local_dir);
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint).expect("remote peer id");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service
        .runtime
        .dial(NetworkAddress::new(format!("{peer}@127.0.0.1:4003")).expect("dial target"))
        .expect("dial peer");
    service.connected_peers.insert(peer.clone());
    let scope = SwarmScope::Group("crew-urgent".to_owned());
    service.subscribe_scope(&scope).expect("subscribe scope");
    let mut state = PeerSyncState::new(Instant::now());
    state.next_retry_at = Instant::now() + Duration::from_secs(60);
    state.known_scopes.insert(scope.clone());
    service.peer_sync_state.insert(peer.clone(), state);

    assert!(
        !service
            .request_backfill_scopes_for_peer(&peer, &node, std::slice::from_ref(&scope))
            .expect("retry-gated backfill")
    );
    assert!(
        service
            .request_backfill_scopes_for_peer_now(&peer, &node, &[scope])
            .expect("urgent backfill")
    );
    assert_eq!(
        service
            .peer_sync_state
            .get(&peer)
            .expect("peer sync state")
            .inflight_backfills(),
        1
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}
