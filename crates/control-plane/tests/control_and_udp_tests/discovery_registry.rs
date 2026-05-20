use super::*;

#[test]
fn discovered_peers_registry_roundtrip_and_legacy_parse() {
    let dir = temp_test_dir("peers-registry");
    let path = discovered_peers_path(&dir);

    let peers = vec!["node-b".to_owned(), "node-a".to_owned()];
    save_discovered_peers(&path, &peers).expect("save discovered peers");
    let loaded = load_discovered_peers(&path).expect("load discovered peers");
    assert_eq!(loaded, peers);
    let loaded_records = load_discovered_peer_records(&path).expect("load discovered peer records");
    assert_eq!(
        loaded_records,
        vec![
            DiscoveredPeerRecord {
                node_id: "node-b".to_owned(),
                source_kind: "unknown".to_owned(),
            },
            DiscoveredPeerRecord {
                node_id: "node-a".to_owned(),
                source_kind: "unknown".to_owned(),
            }
        ]
    );

    let legacy = vec!["legacy-a", "legacy-b"];
    fs::write(&path, serde_json::to_vec(&legacy).expect("legacy vec json")).expect("write legacy");
    let loaded_legacy = load_discovered_peers(&path).expect("load legacy vec");
    assert_eq!(
        loaded_legacy,
        vec!["legacy-a".to_owned(), "legacy-b".to_owned()]
    );

    fs::write(&path, b"{not-json").expect("write invalid json");
    let err = load_discovered_peers(&path).expect_err("invalid json should fail");
    assert!(err.to_string().contains("parse discovered peers registry"));

    cleanup_dir(&dir);
}

#[test]
fn add_discovered_peer_dedups_and_sorts() {
    let _env_guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("peers-add");

    assert!(add_discovered_peer(&dir, "node-c").expect("add node-c"));
    assert!(add_discovered_peer(&dir, "node-a").expect("add node-a"));
    assert!(add_discovered_peer(&dir, "node-b").expect("add node-b"));
    assert!(!add_discovered_peer(&dir, "node-a").expect("add duplicate"));
    assert!(
        add_discovered_peer_endpoint(&dir, "node-a", Some("127.0.0.1:4001"))
            .expect("add node-a endpoint")
    );
    assert!(
        !add_discovered_peer_endpoint(&dir, "node-a", Some("127.0.0.1:4001"))
            .expect("dedup node-a endpoint")
    );
    assert!(!add_discovered_peer(&dir, "   ").expect("add empty"));

    let peers = load_discovered_peers_state(&dir).expect("load peers");
    assert_eq!(peers, vec!["node-a", "node-b", "node-c"]);
    let records = load_discovered_peer_records_state(&dir).expect("load peer records");
    assert_eq!(
        records,
        vec![
            DiscoveredPeerRecord {
                node_id: "node-a".to_owned(),
                source_kind: "udp".to_owned(),
            },
            DiscoveredPeerRecord {
                node_id: "node-b".to_owned(),
                source_kind: "unknown".to_owned(),
            },
            DiscoveredPeerRecord {
                node_id: "node-c".to_owned(),
                source_kind: "unknown".to_owned(),
            }
        ]
    );

    let conn = Connection::open(dir.join("local-control.state")).expect("open local control db");
    let mut stmt = conn
        .prepare(
            "SELECT scope_id, node_id, source_kind
             FROM discovered_peers_local
             ORDER BY node_id ASC",
        )
        .expect("prepare discovered peers raw query");
    let raw_rows = stmt
        .query_map(wattswarm_storage_core::params![], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .expect("query discovered peers raw rows")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect discovered peers raw rows");
    assert_eq!(
        raw_rows,
        vec![
            (
                local_control_scope_id(&dir),
                "node-a".to_owned(),
                "udp".to_owned(),
            ),
            (
                local_control_scope_id(&dir),
                "node-b".to_owned(),
                "unknown".to_owned(),
            ),
            (
                local_control_scope_id(&dir),
                "node-c".to_owned(),
                "unknown".to_owned(),
            ),
        ]
    );

    cleanup_dir(&dir);
}

#[test]
fn network_directory_snapshot_lists_networks_feeds_domains_and_sync_endpoints() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("network-directory-snapshot");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let node = open_node(&state_dir, &db_path).expect("open node");
    let udp_endpoint = test_iroh_endpoint_id(102);
    add_discovered_peer_endpoint(&state_dir, "peer-udp", Some("127.0.0.1:4999"))
        .expect("save discovered peer");
    save_peer_metadata_record_state(
        &state_dir,
        &PeerMetadataRecord {
            node_id: "peer-udp".to_owned(),
            network_id: Some("local".to_owned()),
            params_version: Some(1),
            params_hash: Some("params-local".to_owned()),
            agent_version_raw: Some("wattswarm-network-p2p|local|1|params-local".to_owned()),
            agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
            protocol_version: Some("wattswarm/1.0.0".to_owned()),
            observed_addr: Some("127.0.0.1:4999".to_owned()),
            listen_addrs: vec!["127.0.0.1:4999".to_owned()],
            protocols: vec![],
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(test_iroh_contact_material(
                &udp_endpoint,
                42,
                &["127.0.0.1:4999"],
            )),
            contact_material_signature: Some("sig-peer-udp".to_owned()),
            contact_material_updated_at: Some(42),
            first_identified_at: 42,
            last_identified_at: 42,
        },
    )
    .expect("save peer metadata");
    node.store
        .upsert_feed_subscription(
            "default",
            "node-a",
            "feed-market",
            "region:sol-1",
            &[],
            true,
            10,
        )
        .expect("feed subscription");
    node.store
        .put_task_announcement(
            "task-dir-1",
            "announce-dir-1",
            "feed-market",
            "region:sol-1",
            &json!({"headline":"dir"}),
            None,
            "node-source",
            11,
        )
        .expect("task announcement");
    node.store
        .upsert_execution_set_member(
            "task-dir-1",
            "exec-dir-1",
            "node-worker",
            "worker",
            "node:lab-7",
            "active",
            Some("node-source"),
            12,
        )
        .expect("execution set");

    let snapshot =
        load_network_directory_snapshot(&node, &state_dir, 16).expect("directory snapshot");

    assert_eq!(
        snapshot.current_topology.network.network_kind,
        NetworkKind::Local
    );
    assert_eq!(snapshot.networks.len(), 1);
    assert!(snapshot.networks[0].is_current);
    assert_eq!(snapshot.feeds.len(), 1);
    assert_eq!(snapshot.feeds[0].feed_key, "feed-market");
    assert_eq!(snapshot.feeds[0].scope_hint, "region:sol-1");
    assert_eq!(snapshot.feeds[0].subscriber_count, 1);
    assert_eq!(
        snapshot.active_dissemination_domains,
        vec!["node:lab-7".to_owned(), "region:sol-1".to_owned()]
    );
    assert_eq!(snapshot.sync_endpoints.len(), 1);
    assert!(
        snapshot
            .sync_endpoints
            .iter()
            .any(|entry| entry.source_kind == "contact_material" && entry.node_id == "peer-udp")
    );
    let udp_entry = snapshot
        .sync_endpoints
        .iter()
        .find(|entry| entry.node_id == "peer-udp")
        .expect("udp sync endpoint");
    assert_eq!(udp_entry.transports, vec!["iroh_direct".to_owned()]);
    assert_eq!(
        udp_entry
            .recommended_routes
            .get("backfill_chunk")
            .map(String::as_str),
        Some("iroh_direct")
    );
    assert_eq!(
        udp_entry
            .recommended_routes
            .get("artifact_blob")
            .map(String::as_str),
        Some("iroh_direct")
    );
    assert_eq!(
        udp_entry
            .recommended_routes
            .get("checkpoint_snapshot")
            .map(String::as_str),
        Some("iroh_direct")
    );

    cleanup_dir(&dir);
}

#[test]
fn recommended_transfer_route_uses_peer_contact_material_capabilities() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("recommended-transfer-route");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let iroh_endpoint = test_iroh_endpoint_id(103);
    save_peer_metadata_record_state(
        &state_dir,
        &PeerMetadataRecord {
            node_id: "peer-iroh".to_owned(),
            network_id: Some("wan".to_owned()),
            params_version: Some(7),
            params_hash: Some("params-iroh".to_owned()),
            agent_version_raw: Some("wattswarm-network-p2p|wan|7|params-iroh".to_owned()),
            agent_version_prefix: Some("wattswarm-network-p2p".to_owned()),
            protocol_version: Some("wattswarm/1.0.0".to_owned()),
            observed_addr: Some("198.51.100.7:4001".to_owned()),
            listen_addrs: vec![],
            protocols: vec![],
            handshake_status: "contact_material".to_owned(),
            last_error: None,
            contact_material: Some(test_iroh_contact_material(
                &iroh_endpoint,
                7,
                &["127.0.0.1:4997"],
            )),
            contact_material_signature: None,
            contact_material_updated_at: Some(7),
            first_identified_at: 7,
            last_identified_at: 7,
        },
    )
    .expect("save peer metadata");

    let route = recommended_transfer_route_for_remote_node(
        &state_dir,
        "peer-iroh",
        &TransferIntent {
            kind: TransferKind::BackfillChunk,
            payload_bytes: 64 * 1024,
            requires_streaming: true,
        },
    )
    .expect("recommended route");
    assert_eq!(route, TransportRoute::IrohDirect);

    cleanup_dir(&dir);
}
