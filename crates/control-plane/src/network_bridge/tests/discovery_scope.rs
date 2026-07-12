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
            "relay_urls": ["https://relay.example.invalid/"],
            "latitude": 0.0,
            "longitude": 0.0,
            "nearby_radius_km": 20.0
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");
    let mut record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        0.1,
        0.1,
        20.0,
    );
    let remote_node_id = record.body.node_id.clone();
    let agent_card = json!({
        "name": "Remote Discovery Agent",
        "description": "Public discovery agent card",
        "metadata": {
            "agent_id": "did:key:zRemoteDiscoveryAgent",
            "node_id": remote_node_id,
        },
    });
    let card_hash = format!(
        "sha256:{}",
        sha256_hex(serde_jcs::to_string(&agent_card).unwrap().as_bytes())
    );
    record.body.source_agent_card = Some(SourceAgentCard {
        agent_id: "did:key:zRemoteDiscoveryAgent".to_owned(),
        node_id: Some(remote_node_id.clone()),
        card_hash,
        issued_at: observed_at_ms(),
        card: agent_card,
        signature: Some("agent-card-signature".to_owned()),
    });
    record = SignedDiscoveryNodeRecord::sign(record.body, &NodeIdentity::from_seed(remote_seed))
        .expect("resign discovery record with source agent card");
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
        record.clone(),
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
    assert_eq!(
        metadata[0]
            .contact_material
            .as_ref()
            .and_then(|material| material.pointer("/source_agent_card/card/name"))
            .and_then(Value::as_str),
        Some("Remote Discovery Agent")
    );
    assert_eq!(
        metadata[0]
            .contact_material
            .as_ref()
            .and_then(|material| material.pointer("/source_agent_card/agent_id"))
            .and_then(Value::as_str),
        Some("did:key:zRemoteDiscoveryAgent")
    );
    let remote_peer = NetworkNodeId::new(remote_node_id.clone()).expect("remote peer id");
    assert!(!service.is_global_backfill_provider(&remote_peer));
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), Some(0));

    service.abandon_peer_reconnect(
        remote_peer.clone(),
        PEER_RECONNECT_MAX_ATTEMPTS,
        "iroh_contact",
        None,
    );
    assert!(service.reconnect_abandoned_for_peer(&remote_peer));
    let repeated = apply_discovery_bootnode_record(
        &mut service,
        &mut node,
        &local_dir,
        &local_peer_id,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        record.clone(),
        observed_at_ms(),
    )
    .expect("reapply same discovery record");
    assert!(!repeated);
    assert!(service.reconnect_abandoned_for_peer(&remote_peer));

    let mut updated_body = record.body;
    updated_body.seq = updated_body.seq.saturating_add(1);
    updated_body.updated_at_ms = updated_body.updated_at_ms.saturating_add(1);
    updated_body
        .transport_contact
        .as_mut()
        .expect("transport contact")
        .metadata
        .generated_at = updated_body.updated_at_ms;
    let updated_record =
        SignedDiscoveryNodeRecord::sign(updated_body, &NodeIdentity::from_seed(remote_seed))
            .expect("sign updated discovery record");
    let updated = apply_discovery_bootnode_record(
        &mut service,
        &mut node,
        &local_dir,
        &local_peer_id,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        updated_record,
        observed_at_ms(),
    )
    .expect("apply updated discovery record");
    assert!(updated);
    assert!(!service.reconnect_abandoned_for_peer(&remote_peer));
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), Some(0));

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_topic_provider_record_does_not_mark_global_backfill_provider() {
    let local_dir = temp_startup_dir("discovery-v1-topic-provider-local");
    let remote_dir = temp_startup_dir("discovery-v1-topic-provider-remote");
    let local_seed = [123u8; 32];
    let remote_seed = [124u8; 32];
    fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    fs::write(remote_dir.join("node_seed.hex"), hex::encode(remote_seed))
        .expect("write remote seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan",
            "relay_urls": ["https://relay.example.invalid/"]
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");
    let mut record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        70.0,
        70.0,
        1.0,
    );
    record.body.topic_providers.push(DiscoveryTopicProvider {
        feed_key: "sydney-weather".to_owned(),
        scope_hint: "group:sydney-weather".to_owned(),
        capabilities: DiscoveryTopicProviderCapabilities::local_history_provider(),
        updated_at_ms: observed_at_ms(),
    });
    record = SignedDiscoveryNodeRecord::sign(record.body, &NodeIdentity::from_seed(remote_seed))
        .expect("resign topic provider record");
    let remote_node_id = record.body.node_id.clone();
    let remote_peer = NetworkNodeId::new(remote_node_id.clone()).expect("remote peer id");
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
    .expect("apply discovery provider record");

    assert!(registered);
    assert!(node.peers().contains(&remote_node_id));
    assert!(!service.is_global_backfill_provider(&remote_peer));

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
            "network_mode": "wan",
            "relay_urls": ["https://relay.example.invalid/"]
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
    let remote_peer = NetworkNodeId::new(remote_node_id.clone()).expect("remote peer id");
    assert_eq!(service.reconnect_attempts_for_peer(&remote_peer), Some(0));

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
            "relay_urls": ["https://relay.example.invalid/"],
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
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        &node,
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
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        &node,
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
fn discovery_bootnode_query_dedups_topic_provider_records_for_known_nodes() {
    let local_dir = temp_startup_dir("discovery-v1-query-topic-provider-local");
    let remote_dir = temp_startup_dir("discovery-v1-query-topic-provider-remote");
    let local_identity = NodeIdentity::from_seed([116u8; 32]);
    let remote_seed = [117u8; 32];
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
    let node = Node::new(
        local_identity.clone(),
        PgStore::open_in_memory().expect("store"),
        membership_with_roles(&[local_identity.node_id()]),
    )
    .expect("node");
    node.store
        .upsert_feed_subscription_with_provider_capabilities(
            crate::storage::FeedSubscriptionUpsert {
                network_id: DEFAULT_NETWORK_CONTEXT_ID,
                subscriber_node_id: &node.node_id(),
                feed_key: "sydney-weather",
                scope_hint: "group:sydney-weather",
                gossip_kinds: &["events".to_owned()],
                provider_capabilities: Some(
                    &crate::types::TopicProviderCapabilities::local_history_provider(),
                ),
                active: true,
                updated_at: observed_at_ms(),
            },
        )
        .expect("upsert subscription");
    let plain_record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        70.0,
        70.0,
        1.0,
    );
    let mut record = plain_record.clone();
    record.body.topic_providers.push(DiscoveryTopicProvider {
        feed_key: "sydney-weather".to_owned(),
        scope_hint: "group:sydney-weather".to_owned(),
        capabilities: DiscoveryTopicProviderCapabilities::local_history_provider(),
        updated_at_ms: observed_at_ms(),
    });
    record = SignedDiscoveryNodeRecord::sign(record.body, &NodeIdentity::from_seed(remote_seed))
        .expect("resign topic provider record");
    let capability_response_body = json!({
        "ok": true,
        "records": [plain_record],
    })
    .to_string();
    let topic_response_body = json!({
        "ok": true,
        "records": [record],
    })
    .to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind discovery listener");
    let addr = listener.local_addr().expect("discovery listener addr");
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept discovery query");
            let request = read_http_request(&mut stream);
            let body = if request.starts_with("GET /api/network/discovery/capability?") {
                assert!(request.contains("network_id=default"));
                assert!(request.contains("capability=wattswarm.node"));
                &capability_response_body
            } else {
                assert!(request.starts_with("POST /api/network/discovery/topic-providers/batch?"));
                assert!(request.contains("network_id=default"));
                assert!(request.contains("feed_key\":\"sydney-weather"));
                assert!(request.contains("scope_hint\":\"group:sydney-weather"));
                &topic_response_body
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write discovery response");
        }
    });
    crate::control::save_discovery_bootnode_urls_state(
        &local_dir,
        &[format!("http://{addr}/api/network/discovery")],
    )
    .expect("save discovery urls");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        &node,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        observed_at_ms(),
    )
    .expect("query discovery bootnode");

    assert_eq!(records.len(), 1);
    assert!(records[0].body.topic_providers.is_empty());
    records[0].verify().expect("plain record verifies");
    handle.join().expect("join discovery listener");
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&remote_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
    fs::remove_dir_all(remote_dir).expect("cleanup remote");
}

#[test]
fn discovery_bootnode_query_falls_back_when_topic_provider_batch_is_unsupported() {
    let local_dir = temp_startup_dir("discovery-v1-query-topic-provider-fallback-local");
    let remote_dir = temp_startup_dir("discovery-v1-query-topic-provider-fallback-remote");
    let local_identity = NodeIdentity::from_seed([118u8; 32]);
    let remote_seed = [119u8; 32];
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
    let node = Node::new(
        local_identity.clone(),
        PgStore::open_in_memory().expect("store"),
        membership_with_roles(&[local_identity.node_id()]),
    )
    .expect("node");
    node.store
        .upsert_feed_subscription_with_provider_capabilities(
            crate::storage::FeedSubscriptionUpsert {
                network_id: DEFAULT_NETWORK_CONTEXT_ID,
                subscriber_node_id: &node.node_id(),
                feed_key: "sydney-weather",
                scope_hint: "group:sydney-weather",
                gossip_kinds: &["events".to_owned()],
                provider_capabilities: Some(
                    &crate::types::TopicProviderCapabilities::local_history_provider(),
                ),
                active: true,
                updated_at: observed_at_ms(),
            },
        )
        .expect("upsert subscription");
    let mut record = signed_discovery_record_for_test(
        &remote_dir,
        remote_seed,
        DEFAULT_NETWORK_CONTEXT_ID,
        70.0,
        70.0,
        1.0,
    );
    record.body.topic_providers.push(DiscoveryTopicProvider {
        feed_key: "sydney-weather".to_owned(),
        scope_hint: "group:sydney-weather".to_owned(),
        capabilities: DiscoveryTopicProviderCapabilities::local_history_provider(),
        updated_at_ms: observed_at_ms(),
    });
    record = SignedDiscoveryNodeRecord::sign(record.body, &NodeIdentity::from_seed(remote_seed))
        .expect("resign topic provider record");
    let capability_response_body = json!({
        "ok": true,
        "records": [],
    })
    .to_string();
    let topic_response_body = json!({
        "ok": true,
        "records": [record],
    })
    .to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind discovery listener");
    let addr = listener.local_addr().expect("discovery listener addr");
    let handle = thread::spawn(move || {
        for request_index in 0..3 {
            let (mut stream, _) = listener.accept().expect("accept discovery query");
            let request = read_http_request(&mut stream);
            let response = if request_index == 0 {
                assert!(request.starts_with("GET /api/network/discovery/capability?"));
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    capability_response_body.len(),
                    capability_response_body
                )
            } else if request_index == 1 {
                assert!(request.starts_with("POST /api/network/discovery/topic-providers/batch?"));
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned()
            } else {
                assert!(request.starts_with("GET /api/network/discovery/topic-providers?"));
                assert!(request.contains("feed_key=sydney-weather"));
                assert!(request.contains("scope_hint=group%3Asydney-weather"));
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    topic_response_body.len(),
                    topic_response_body
                )
            };
            stream
                .write_all(response.as_bytes())
                .expect("write discovery response");
        }
    });
    crate::control::save_discovery_bootnode_urls_state(
        &local_dir,
        &[format!("http://{addr}/api/network/discovery")],
    )
    .expect("save discovery urls");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        &node,
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
fn discovery_bootnode_query_skips_private_dm_topic_provider_requests() {
    let local_dir = temp_startup_dir("discovery-v1-query-private-dm-local");
    let local_identity = NodeIdentity::from_seed([120u8; 32]);
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({
            "network_mode": "wan"
        }))
        .expect("startup config json"),
    )
    .expect("write startup config");
    let node = Node::new(
        local_identity.clone(),
        PgStore::open_in_memory().expect("store"),
        membership_with_roles(&[local_identity.node_id()]),
    )
    .expect("node");
    node.store
        .upsert_feed_subscription_with_provider_capabilities(
            crate::storage::FeedSubscriptionUpsert {
                network_id: DEFAULT_NETWORK_CONTEXT_ID,
                subscriber_node_id: &node.node_id(),
                feed_key: crate::control::PRIVATE_DM_FEED_KEY,
                scope_hint: "group:dm-private",
                gossip_kinds: &["events".to_owned()],
                provider_capabilities: Some(
                    &crate::types::TopicProviderCapabilities::local_history_provider(),
                ),
                active: true,
                updated_at: observed_at_ms(),
            },
        )
        .expect("upsert private dm subscription");
    let capability_response_body = json!({
        "ok": true,
        "records": [],
    })
    .to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind discovery listener");
    let addr = listener.local_addr().expect("discovery listener addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept discovery query");
        let request = read_http_request(&mut stream);
        assert!(request.starts_with("GET /api/network/discovery/capability?"));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            capability_response_body.len(),
            capability_response_body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write discovery response");
        listener
            .set_nonblocking(true)
            .expect("set discovery listener nonblocking");
        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut extra_stream, _)) => {
                    let extra_request = read_http_request(&mut extra_stream);
                    panic!("unexpected private DM topic-provider request: {extra_request}");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept discovery query: {error}"),
            }
        }
    });
    crate::control::save_discovery_bootnode_urls_state(
        &local_dir,
        &[format!("http://{addr}/api/network/discovery")],
    )
    .expect("save discovery urls");
    let settings = discovery_bootnode_settings_from_state_dir(&local_dir).expect("settings");

    let records = query_discovery_bootnodes_for_candidate_records(
        &local_dir,
        &node,
        DEFAULT_NETWORK_CONTEXT_ID,
        &settings,
        observed_at_ms(),
    )
    .expect("query discovery bootnode");

    assert!(records.is_empty());
    handle.join().expect("join discovery listener");
    fs::remove_dir_all(local_dir).expect("cleanup local");
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
fn scopes_to_request_for_peer_skips_global_for_unknown_non_provider_peer() {
    let local_dir = temp_startup_dir("scope-request-unknown");
    let local_seed = [121u8; 32];
    fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write startup config");
    let peer = random_network_node_id();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        Vec::<SwarmScope>::new()
    );

    service.remember_global_backfill_provider(peer.clone());
    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![SwarmScope::Global]
    );
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
}

#[test]
fn scopes_to_request_for_peer_requests_known_scopes_and_provider_global() {
    let local_dir = temp_startup_dir("scope-request-known");
    let local_seed = [122u8; 32];
    fs::write(local_dir.join("node_seed.hex"), hex::encode(local_seed)).expect("write local seed");
    fs::write(
        local_dir.join("startup_config.json"),
        serde_json::to_vec(&json!({"relay_urls":["https://relay.example.invalid/"]}))
            .expect("startup config json"),
    )
    .expect("write startup config");
    let peer = random_network_node_id();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let other_scope = SwarmScope::Node("lab-1".to_owned());
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig::default(),
            local_dir.clone(),
            local_seed,
        )
        .expect("iroh node"),
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
        vec![target_scope.clone()]
    );

    service.remember_global_backfill_provider(peer.clone());
    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![target_scope, SwarmScope::Global]
    );
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&local_dir);
    fs::remove_dir_all(local_dir).expect("cleanup local");
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
            head_only: false,
            feed_key: None,
            exclude_topic_events: false,
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
                provider_capabilities: None,
                agent_envelope: None,
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
            head_only: false,
            feed_key: None,
            exclude_topic_events: false,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(response.events.is_empty());
}

#[test]
fn global_backfill_skips_task_and_business_events() {
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
            head_only: false,
            feed_key: None,
            exclude_topic_events: false,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(
        response.events.is_empty(),
        "global backfill should not return task events for unscoped catch-up"
    );
    assert!(
        response.events.iter().all(|envelope| {
            !matches!(
                envelope.event.payload,
                crate::types::EventPayload::TaskCreated(_)
                    | crate::types::EventPayload::TaskAnnounced(_)
                    | crate::types::EventPayload::ExecutionIntentDeclared(_)
                    | crate::types::EventPayload::ExecutionSetConfirmed(_)
                    | crate::types::EventPayload::TopicMessagePosted(_)
                    | crate::types::EventPayload::AgentPaymentPosted(_)
                    | crate::types::EventPayload::TaskCompleted(_)
                    | crate::types::EventPayload::TaskClaimed(_)
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
        "global backfill should suppress task and business traffic"
    );
}
