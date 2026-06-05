use super::*;

#[test]
fn smarter_backfill_prefers_peer_with_known_scope_activity() {
    let dir_a = temp_startup_dir("backfill-peer-a");
    let dir_b = temp_startup_dir("backfill-peer-b");
    std::fs::write(dir_a.join("node_seed.hex"), hex::encode([41_u8; 32])).expect("write seed a");
    std::fs::write(dir_b.join("node_seed.hex"), hex::encode([42_u8; 32])).expect("write seed b");
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, served_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let global_request = BackfillRequest {
        scope: SwarmScope::Global,
        from_event_seq: 0,
        limit: 8,
        feed_key: None,
        known_event_ids: Vec::new(),
    };
    let served_request = BackfillRequest {
        scope: served_scope.clone(),
        from_event_seq: 0,
        limit: 8,
        feed_key: None,
        known_event_ids: Vec::new(),
    };
    let unserved_request = BackfillRequest {
        scope: unserved_scope.clone(),
        from_event_seq: 0,
        limit: 8,
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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

    let expired = service.expire_stale_backfill_requests(Instant::now(), Duration::from_millis(1));

    assert_eq!(expired, 1);
    let state = service.peer_sync_state.get(&peer).expect("peer state");
    assert_eq!(state.inflight_backfills(), 0);
    assert_eq!(state.backfill_failures, 1);
    let raw = fs::read_to_string(state_dir.join("diagnostics/wattswarm_node.jsonl"))
        .expect("diagnostics");
    assert!(raw.contains("\"phase\":\"request.timeout\""));
}

#[test]
fn backfill_requests_respect_remaining_peer_slots() {
    let peer = random_network_node_id();
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
fn connection_established_diagnostics_skip_duplicate_peer_events() {
    let dir = temp_startup_dir("connection-established-diagnostics-dedupe");
    let peer = random_network_node_id();
    let address = "203.0.113.10:4001".parse::<NetworkAddress>().expect("addr");
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
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
fn set_state_dir_loads_persisted_iroh_contact_material_into_runtime() {
    let local_dir = temp_startup_dir("iroh-contact-load-local");
    let remote_dir = temp_startup_dir("iroh-contact-load-remote");
    let local_seed = [81u8; 32];
    let remote_seed = [82u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
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
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), Some(0));

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
fn reconnect_supervision_redials_remembered_peer_address() {
    let local_dir = temp_startup_dir("reconnect-local");
    let remote_dir = temp_startup_dir("reconnect-remote");
    let local_seed = [93u8; 32];
    let remote_seed = [94u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
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

    assert_eq!(attempts, 1);
    assert_eq!(service.known_remote_contact_count(), 1);
    assert_eq!(service.reconnect_attempts_for_peer(&peer), Some(1));

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
    let remote_endpoint =
        wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&remote_dir)
            .expect("remote endpoint")
            .to_string();
    let peer = NetworkNodeId::new(remote_endpoint).expect("remote peer id");
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

    service.remember_peer_address(peer.clone(), address.clone());
    for _ in 0..PEER_RECONNECT_MAX_ATTEMPTS {
        let attempts = service
            .run_reconnect_supervision()
            .expect("run reconnect supervision");
        assert_eq!(attempts, 1);
        service.force_reconnect_due_for_peer(&peer);
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
fn reconnect_supervision_probes_relay_only_bootstrap_contact() {
    let local_dir = temp_startup_dir("reconnect-relay-only-local");
    let remote_dir = temp_startup_dir("reconnect-relay-only-remote");
    let local_seed = [109u8; 32];
    let remote_seed = [110u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
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
    std::fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "bootstrap_contacts": [json!({
                "node_id": remote_endpoint,
                "peer_id": remote_contact.peer_id,
                "transports": [remote_contact]
            }).to_string()]
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
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

    assert_eq!(service.known_remote_contact_count(), 1);
    assert_eq!(service.reconnect_attempts_for_peer(&peer), None);
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
        .expect("pending probe suppresses duplicate reconnect");
    assert_eq!(attempts, 0);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn urgent_backfill_request_bypasses_retry_delay_for_relay_scope() {
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let local_dir = temp_startup_dir("urgent-backfill-local");
    let remote_dir = temp_startup_dir("urgent-backfill-remote");
    let local_seed = [95u8; 32];
    let remote_seed = [96u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
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

#[test]
fn anti_entropy_requests_relay_scope_backfill() {
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let local_dir = temp_startup_dir("relay-anti-entropy-local");
    let remote_dir = temp_startup_dir("relay-anti-entropy-remote");
    let local_seed = [97u8; 32];
    let remote_seed = [98u8; 32];
    std::fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed))
        .expect("write local seed");
    std::fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
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
        .dial(NetworkAddress::new(format!("{peer}@127.0.0.1:4004")).expect("dial target"))
        .expect("dial peer");
    service.connected_peers.insert(peer.clone());
    let scope = SwarmScope::Group("crew-relay".to_owned());
    service
        .relay_scope_kinds
        .insert(scope.clone(), HashSet::from([GossipKind::Events]));
    let mut state = PeerSyncState::new(Instant::now());
    state.known_scopes.insert(scope);
    service.peer_sync_state.insert(peer.clone(), state);

    assert_eq!(
        service
            .run_anti_entropy(&node)
            .expect("run anti-entropy for relay scope"),
        1
    );
    assert!(
        service
            .peer_sync_state
            .get(&peer)
            .expect("peer sync state")
            .inflight_backfills()
            >= 1
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    std::fs::remove_dir_all(local_dir).expect("cleanup local");
    std::fs::remove_dir_all(remote_dir).expect("cleanup remote");
}
