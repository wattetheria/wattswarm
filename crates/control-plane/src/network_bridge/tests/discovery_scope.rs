use super::*;

#[test]
fn discovery_bootnode_record_registers_iroh_contact_inside_radius_without_marking_connected() {
    let _lock = lock_env_test_mutex();
    let local_dir = temp_startup_dir("discovery-v1-local");
    let remote_dir = temp_startup_dir("discovery-v1-remote");
    let local_seed = [103u8; 32];
    let remote_seed = [104u8; 32];
    fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "latitude": 0.0,
            "longitude": 0.0,
            "nearby_radius_km": 20.0
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");
    let record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        0.1,
        0.1,
        20.0,
    );
    let remote_node_id = record.body.node_id.clone();
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

    let local_peer_id = service.local_peer_id().to_string();
    let registered = apply_discovery_bootnode_record(
        &mut service,
        &mut node,
        &local_dir,
        &local_peer_id,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        record,
        observed_at_ms(),
    )
    .expect("apply discovery record");

    assert!(registered);
    assert!(node.peers().contains(&remote_node_id));
    assert_eq!(service.known_remote_contact_count(), 1);
    let discovered =
        crate::control::load_discovered_peer_records_state(&local_dir).expect("discovered peers");
    assert!(discovered.is_empty());
    let metadata =
        crate::control::load_peer_metadata_records_state(&local_dir).expect("peer metadata");
    assert_eq!(metadata[0].node_id, remote_node_id);
    assert_eq!(metadata[0].handshake_status, "discovery_v1");

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_bootnode_record_accepts_candidate_without_local_geo() {
    let local_dir = temp_startup_dir("discovery-v1-no-local-geo");
    let remote_dir = temp_startup_dir("discovery-v1-no-local-geo-remote");
    let local_seed = [113u8; 32];
    let remote_seed = [114u8; 32];
    fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan"
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");
    assert!(settings.enabled);
    assert!(!settings.has_local_geo());
    let record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        70.0,
        70.0,
        1.0,
    );
    let remote_node_id = record.body.node_id.clone();
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

    let local_peer_id = service.local_peer_id().to_string();
    let registered = apply_discovery_bootnode_record(
        &mut service,
        &mut node,
        &local_dir,
        &local_peer_id,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        record,
        observed_at_ms(),
    )
    .expect("apply discovery record");

    assert!(registered);
    assert!(node.peers().contains(&remote_node_id));
    let metadata =
        crate::control::load_peer_metadata_records_state(&local_dir).expect("peer metadata");
    assert_eq!(metadata[0].node_id, remote_node_id);
    assert_eq!(metadata[0].handshake_status, "discovery_v1");

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_bootnode_record_rejects_self_stale_and_out_of_radius_records() {
    let local_dir = temp_startup_dir("discovery-v1-reject-local");
    let remote_dir = temp_startup_dir("discovery-v1-reject-remote");
    let local_seed = [105u8; 32];
    let remote_seed = [106u8; 32];
    fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "latitude": 0.0,
            "longitude": 0.0,
            "nearby_radius_km": 5.0
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");
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
    let local_peer_id = service.local_peer_id().to_string();

    let out_of_radius = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        1.0,
        1.0,
        5.0,
    );
    assert!(
        !apply_discovery_bootnode_record(
            &mut service,
            &mut node,
            &local_dir,
            &local_peer_id,
            DEFAULT_NETWORK_CONTEXT_ID,
            &settings,
            out_of_radius,
            observed_at_ms(),
        )
        .expect("reject out of radius")
    );

    let stale = signed_discovery_record_for_test_at(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        0.1,
        0.1,
        5.0,
        1,
        1,
    );
    assert!(
        apply_discovery_bootnode_record(
            &mut service,
            &mut node,
            &local_dir,
            &local_peer_id,
            DEFAULT_NETWORK_CONTEXT_ID,
            &settings,
            stale,
            observed_at_ms(),
        )
        .is_err()
    );

    let self_record = signed_discovery_record_for_test(
        &local_dir,
        local_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        0.1,
        0.1,
        5.0,
    );
    assert!(
        !apply_discovery_bootnode_record(
            &mut service,
            &mut node,
            &local_dir,
            &local_peer_id,
            DEFAULT_NETWORK_CONTEXT_ID,
            &settings,
            self_record,
            observed_at_ms(),
        )
        .expect("reject self")
    );
    assert_eq!(service.known_remote_contact_count(), 0);

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_bootnode_query_fetches_and_verifies_nearby_records() {
    let local_dir = temp_startup_dir("discovery-v1-query-local");
    let remote_dir = temp_startup_dir("discovery-v1-query-remote");
    let remote_seed = [107u8; 32];
    fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "latitude": 0.0,
            "longitude": 0.0,
            "nearby_radius_km": 20.0
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        0.1,
        0.1,
        20.0,
    );
    let response_body = json!({
        "ok": true,
        "records": [{
            "distance_km": 0.0,
            "record": record,
        }],
    })
    .to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind discovery listener");
    let addr = listener.local_addr().expect("discovery listener addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept discovery query");
        let request = read_http_request(&mut stream);
        assert!(request.starts_with("GET /api/network/discovery/nearby?"));
        assert!(request.contains("network_id=default"));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write discovery response");
    });
    crate::control::save_discovery_bootnode_urls_state(
        &local_dir,
        &[format!("http://{addr}/api/network/discovery")],
    )
    .expect("save discovery urls");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        observed_at_ms(),
    )
    .expect("query discovery bootnode");

    assert_eq!(records.len(), 1);
    records[0].verify().expect("record verifies");
    handle.join().expect("join discovery listener");
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_bootnode_query_without_local_geo_uses_capability_records() {
    let local_dir = temp_startup_dir("discovery-v1-query-no-geo-local");
    let remote_dir = temp_startup_dir("discovery-v1-query-no-geo-remote");
    let remote_seed = [115u8; 32];
    fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan"
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        70.0,
        70.0,
        1.0,
    );
    let response_body = json!({
        "ok": true,
        "records": [record],
    })
    .to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind discovery listener");
    let addr = listener.local_addr().expect("discovery listener addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept discovery query");
        let request = read_http_request(&mut stream);
        assert!(request.starts_with("GET /api/network/discovery/capability?"));
        assert!(request.contains("network_id=default"));
        assert!(request.contains("capability=wattswarm.node"));
        assert!(!request.contains("latitude="));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write discovery response");
    });
    crate::control::save_discovery_bootnode_urls_state(
        &local_dir,
        &[format!("http://{addr}/api/network/discovery")],
    )
    .expect("save discovery urls");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        observed_at_ms(),
    )
    .expect("query discovery bootnode");

    assert_eq!(records.len(), 1);
    records[0].verify().expect("record verifies");
    handle.join().expect("join discovery listener");
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_bootnode_query_timeout_allows_proxy_jitter() {
    assert_eq!(DISCOVERY_BOOTNODE_QUERY_TIMEOUT, Duration::from_secs(10));
}

#[test]
fn discovery_bootnode_query_failure_logging_suppresses_repeated_endpoint_failures() {
    let endpoint = format!(
        "https://example.invalid/api/network/discovery/nearby/{}",
        Uuid::new_v4().simple()
    );
    reset_discovery_bootnode_failure_log_state(&endpoint);

    assert_eq!(discovery_bootnode_failure_log_decision(&endpoint), Some(0));
    for _ in 2..DISCOVERY_BOOTNODE_FAILURE_LOG_EVERY {
        assert_eq!(discovery_bootnode_failure_log_decision(&endpoint), None);
    }
    assert_eq!(
        discovery_bootnode_failure_log_decision(&endpoint),
        Some(DISCOVERY_BOOTNODE_FAILURE_LOG_EVERY - 2)
    );

    reset_discovery_bootnode_failure_log_state(&endpoint);
    assert_eq!(discovery_bootnode_failure_log_decision(&endpoint), Some(0));
}

#[test]
fn scopes_to_request_for_peer_falls_back_to_all_scopes_until_peer_is_profiled() {
    let peer = random_network_node_id();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![SwarmScope::Global, target_scope]
    );
}

#[test]
fn scopes_to_request_for_peer_prioritizes_known_scopes_without_dropping_subscriptions() {
    let peer = random_network_node_id();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let other_scope = SwarmScope::Node("lab-1".to_owned());
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[
            SwarmScope::Global,
            target_scope.clone(),
            other_scope.clone(),
        ],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let mut state = PeerSyncState::new(Instant::now());
    state.known_scopes.insert(target_scope.clone());
    service.peer_sync_state.insert(peer.clone(), state);

    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![target_scope, SwarmScope::Global, other_scope]
    );
}

#[test]
fn local_scope_aliases_map_to_node_scope() {
    let policy_hash = "policy-hash-alias".to_owned();
    let mut contract = sample_contract("task-local-alias", policy_hash);
    contract.inputs = json!({"swarm_scope":" local:lab-1 "});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-1".to_owned())
    );

    contract.inputs = json!({"swarm_scope":{"kind":"local","id":"lab-2"}});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-2".to_owned())
    );

    contract.inputs = json!({});
    contract.task_type = "local:lab-3:swarm".to_owned();
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-3".to_owned())
    );
}

#[test]
fn parse_scope_hint_string_preserves_task_type_style_prefix_fallback() {
    assert_eq!(
        scope::parse_scope_hint_string("region:sol-1:swarm"),
        Some(SwarmScope::Region("sol-1".to_owned()))
    );
    assert_eq!(
        scope::parse_scope_hint_string(" local:lab-7:worker "),
        Some(SwarmScope::Node("lab-7".to_owned()))
    );
    assert_eq!(
        scope::parse_scope_hint_string(" group:crew-7:worker "),
        Some(SwarmScope::Group("crew-7".to_owned()))
    );
}

#[test]
fn contract_scope_object_uses_shared_canonical_scope_rules() {
    let mut contract = sample_contract("task-object-scope", "policy-hash-object".to_owned());
    contract.inputs = json!({"swarm_scope":{"kind":" local ","id":" lab-8 "}});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-8".to_owned())
    );
}

#[test]
fn backfill_response_filters_by_scope() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut global_contract = sample_contract("task-global-backfill", policy_hash.clone());
    global_contract.inputs = json!({"prompt":"global"});
    node.submit_task(global_contract, 1, 10)
        .expect("submit global");

    let mut region_contract = sample_contract("task-region-backfill", policy_hash);
    region_contract.task_type = "region:sol-1:swarm".to_owned();
    region_contract.inputs = json!({"prompt":"region", "swarm_scope":"region:sol-1"});
    node.submit_task(region_contract, 1, 11)
        .expect("submit region");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Region("sol-1".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert_eq!(response.events.len(), 1);
    assert_eq!(
        response.events[0].scope,
        SwarmScope::Region("sol-1".to_owned())
    );
    assert_eq!(
        response.events[0].event.task_id.as_deref(),
        Some("task-region-backfill")
    );
}

#[test]
fn backfill_response_skips_network_substrate_events_for_other_networks() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("node");

    let mismatched_event = build_event_for_external(
        &identity,
        1,
        100,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "mainnet:other".to_owned(),
                subscriber_node_id: node_id,
                feed_key: "market.alpha".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            },
        ),
    )
    .expect("event");
    node.store
        .append_event(&mismatched_event)
        .expect("append event");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Region("sol-1".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(response.events.is_empty());
}

#[test]
fn global_backfill_skips_task_process_layer_events() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-global-process-backfill", policy_hash);
    contract.inputs = json!({"prompt":"backfill without process"});
    node.submit_task(contract, 1, 100).expect("submit task");
    node.claim_task(
        "task-global-process-backfill",
        crate::types::ClaimRole::Propose,
        "exec-backfill-1",
        500,
        1,
        101,
    )
    .expect("claim task");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 16,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(
        response.events.iter().any(|envelope| matches!(
            envelope.event.payload,
            crate::types::EventPayload::TaskCreated(_)
        )),
        "global backfill should still include task detail events"
    );
    assert!(
        response.events.iter().all(|envelope| {
            !matches!(
                envelope.event.payload,
                crate::types::EventPayload::TaskClaimed(_)
                    | crate::types::EventPayload::TaskClaimRenewed(_)
                    | crate::types::EventPayload::TaskClaimReleased(_)
                    | crate::types::EventPayload::CandidateProposed(_)
                    | crate::types::EventPayload::EvidenceAdded(_)
                    | crate::types::EventPayload::EvidenceAvailable(_)
                    | crate::types::EventPayload::VerifierResultSubmitted(_)
                    | crate::types::EventPayload::VoteCommit(_)
                    | crate::types::EventPayload::VoteReveal(_)
                    | crate::types::EventPayload::DecisionCommitted(_)
                    | crate::types::EventPayload::TaskError(_)
                    | crate::types::EventPayload::TaskRetryScheduled(_)
            )
        }),
        "global backfill should suppress task process traffic"
    );
}
