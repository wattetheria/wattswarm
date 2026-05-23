use super::*;

#[test]
fn open_node_network_mode_uses_existing_network_topology_only() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = temp_test_dir("open-node-network-mode");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = "mainnet:watt-galaxy";
    let org_id = "mainnet:watt-galaxy:bootstrap";

    let _bootstrap_store = wattswarm_control_plane::storage::PgStore::open(&db_path)
        .expect("open store for bootstrap network setup");
    let conn = Connection::open("network-bootstrap-setup").expect("open setup connection");
    conn.execute(
        "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
         VALUES ($1, 'mainnet', NULL, 'Watt Galaxy', 'active', $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, &node_id, 1_700_000_000_000_i64],
    )
    .expect("insert network registry");
    conn.execute(
        "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
         VALUES ($1, 'manual_owner', 1, 1, '{}', TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, 1_700_000_000_000_i64],
    )
    .expect("insert network params");
    conn.execute(
        "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
         VALUES ($1, $2, 'bootstrap', 'Watt Galaxy Bootstrap', 'active', TRUE, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![org_id, network_id, 1_700_000_000_000_i64],
    )
    .expect("insert org registry");

    let node = open_node(&state_dir, &db_path).expect("open node in network mode");
    assert_eq!(node.store.org_id(), org_id);

    let conn = Connection::open("network-bootstrap-verify").expect("open verification connection");
    let local_network_count = conn
        .query_row(
            "SELECT COUNT(*) FROM network_registry WHERE network_id = $1",
            wattswarm_storage_core::params![format!("local:{node_id}")],
            |r| r.get::<_, i64>(0),
        )
        .expect("count local network row");
    assert_eq!(local_network_count, 0);

    let home_network = conn
        .query_row(
            "SELECT home_network_id FROM node_registry WHERE node_id = $1",
            wattswarm_storage_core::params![&node_id],
            |r| r.get::<_, String>(0),
        )
        .expect("node registry row");
    assert_eq!(home_network, network_id);

    let signed_params: SignedNetworkProtocolParamsEnvelope = conn
        .query_row(
            "SELECT params_json FROM network_params WHERE network_id = $1",
            wattswarm_storage_core::params![network_id],
            |r| r.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).expect("signed network params"))
        .expect("network params row");
    assert_eq!(signed_params.network_id, network_id);
    assert_eq!(signed_params.version, 1);
    assert_eq!(signed_params.signed_by, node_id);

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_rejects_unsigned_network_params() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = temp_test_dir("open-node-network-mode-unsigned-params");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let _node_id = local_node_id(&state_dir).expect("local node id");
    let genesis_identity = NodeIdentity::random();
    let network_id = "mainnet:watt-galaxy-unsigned";
    let org_id = "mainnet:watt-galaxy-unsigned:bootstrap";

    let _bootstrap_store = wattswarm_control_plane::storage::PgStore::open(&db_path)
        .expect("open store for unsigned bootstrap setup");
    let conn = Connection::open("network-bootstrap-unsigned-setup").expect("open setup connection");
    conn.execute(
        "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
         VALUES ($1, 'mainnet', NULL, 'Watt Galaxy', 'active', $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, &genesis_identity.node_id(), 1_700_000_000_000_i64],
    )
    .expect("insert network registry");
    conn.execute(
        "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
         VALUES ($1, 'manual_owner', 1, 1, '{}', TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, 1_700_000_000_000_i64],
    )
    .expect("insert unsigned network params");
    conn.execute(
        "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
         VALUES ($1, $2, 'bootstrap', 'Watt Galaxy Bootstrap', 'active', TRUE, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![org_id, network_id, 1_700_000_000_000_i64],
    )
    .expect("insert org registry");

    let err = match open_node(&state_dir, &db_path) {
        Ok(_) => panic!("unsigned network params must fail"),
        Err(err) => err,
    };
    assert!(!err.to_string().trim().is_empty());

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_auto_syncs_signed_bootstrap_bundle_from_remote() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let local_schema = format!("test_local_{}", Uuid::new_v4().simple());
    let remote_schema = format!("test_remote_{}", Uuid::new_v4().simple());
    reset_test_schema(&local_schema);
    reset_test_schema(&remote_schema);

    let remote_identity = NodeIdentity::random();
    let remote_node_id = remote_identity.node_id();
    let network_id = "mainnet:watt-etheria";
    let org_id = "mainnet:watt-etheria:bootstrap";
    let bundle = {
        let _remote_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &remote_schema);
        let remote_dir = temp_test_dir("remote-bootstrap-bundle");
        let remote_state_dir = remote_dir.join("state");
        fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
        let remote_db_path = remote_state_dir.join("remote.state");
        let remote_store = wattswarm_control_plane::storage::PgStore::open(&remote_db_path)
            .expect("open remote bootstrap store");
        remote_store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "Watt Etheria",
                &remote_node_id,
                &remote_node_id,
                1_700_000_000_000,
            )
            .expect("create remote mainnet bootstrap topology");
        let conn = Connection::open("remote-bootstrap-bundle-setup")
            .expect("open remote bootstrap bundle setup connection");
        conn.execute(
            "UPDATE org_registry SET name = 'Aether Genesis' WHERE org_id = $1",
            wattswarm_storage_core::params![org_id],
        )
        .expect("rename remote bootstrap org");
        remote_store
            .put_network_protocol_params(
                network_id,
                &remote_identity,
                &NetworkProtocolParams::default(),
            )
            .expect("sign remote network params");
        let bundle = remote_store
            .for_org(org_id)
            .load_network_bootstrap_bundle()
            .expect("load remote bootstrap bundle");
        cleanup_dir(&remote_dir);
        bundle
    };

    let bootstrap_stub = BootstrapBundleStub::start(bundle.clone());
    let _local_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &local_schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _bootstrap_url_guard = EnvVarGuard::set(
        "WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS",
        &format!("http://127.0.0.1:{}", bootstrap_stub.addr.port()),
    );
    let dir = temp_test_dir("open-node-network-mode-auto-sync");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create local state dir");
    let db_path = state_dir.join("local.state");
    let local_node_id = local_node_id(&state_dir).expect("local node id");

    let node = open_node(&state_dir, &db_path).expect("open node after bootstrap sync");
    assert_eq!(node.store.org_id(), org_id);
    assert_eq!(node.node_id(), local_node_id);

    let verified = node
        .store
        .load_verified_network_protocol_params()
        .expect("load synced network params");
    assert_eq!(verified.network_id, network_id);
    assert_eq!(verified.genesis_node_id, remote_node_id);
    assert_eq!(verified.signed.signed_by, remote_node_id);
    assert_eq!(verified.signed, bundle.signed_params);

    let conn = Connection::open("network-bootstrap-auto-sync-verify")
        .expect("open auto-sync verification connection");
    let imported_org: (String, bool) = conn
        .query_row(
            "SELECT name, is_default FROM org_registry WHERE org_id = $1",
            wattswarm_storage_core::params![org_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("load imported org");
    assert_eq!(imported_org.0, "Aether Genesis");
    assert!(imported_org.1);

    let home_network = conn
        .query_row(
            "SELECT home_network_id FROM node_registry WHERE node_id = $1",
            wattswarm_storage_core::params![&local_node_id],
            |r| r.get::<_, String>(0),
        )
        .expect("load local node registry row");
    assert_eq!(home_network, network_id);

    cleanup_dir(&dir);
}

#[test]
fn network_mode_loads_subnet_topology_as_network_subtype() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let dir = temp_test_dir("open-node-subnet-network-mode");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = "subnet:alpha";
    let parent_network_id = "mainnet:watt-galaxy";
    let org_id = "subnet:alpha:bootstrap";

    let store = wattswarm_control_plane::storage::PgStore::open(&db_path).expect("open store");
    let conn = Connection::open("subnet-bootstrap-setup").expect("open setup connection");
    conn.execute(
        "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
         VALUES ($1, 'subnet', $2, 'Subnet Alpha', 'active', $3, TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, parent_network_id, &node_id, 1_700_000_000_000_i64],
    )
    .expect("insert subnet network registry");
    conn.execute(
        "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
         VALUES ($1, 'manual_owner', 1, 1, '{}', TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![network_id, 1_700_000_000_000_i64],
    )
    .expect("insert subnet network params");
    conn.execute(
        "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
         VALUES ($1, $2, 'bootstrap', 'Subnet Alpha Bootstrap', 'active', TRUE, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))",
        wattswarm_storage_core::params![org_id, network_id, 1_700_000_000_000_i64],
    )
    .expect("insert subnet org registry");

    let node = open_node(&state_dir, &db_path).expect("open node in subnet mode");
    assert_eq!(node.store.org_id(), org_id);

    let topology = store
        .load_network_topology_for_org(org_id)
        .expect("load subnet topology");
    assert_eq!(topology.network.network_kind, NetworkKind::Subnet);
    assert_eq!(
        topology.network.parent_network_id.as_deref(),
        Some(parent_network_id)
    );
    assert_eq!(topology.org.network_id, topology.network.network_id);

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_auto_syncs_signed_bundle_from_join_manifest() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let local_schema = format!("test_local_{}", Uuid::new_v4().simple());
    let remote_schema = format!("test_remote_{}", Uuid::new_v4().simple());
    reset_test_schema(&local_schema);
    reset_test_schema(&remote_schema);

    let remote_identity = NodeIdentity::random();
    let remote_node_id = remote_identity.node_id();
    let network_id = "mainnet:wattetheria";
    let org_id = "mainnet:wattetheria:bootstrap";
    let bundle = {
        let _remote_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &remote_schema);
        let remote_dir = temp_test_dir("remote-join-manifest-bundle");
        let remote_state_dir = remote_dir.join("state");
        fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
        let remote_db_path = remote_state_dir.join("remote.state");
        let remote_store = wattswarm_control_plane::storage::PgStore::open(&remote_db_path)
            .expect("open remote bootstrap store");
        remote_store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "Wattetheria",
                &remote_node_id,
                &remote_node_id,
                1_700_000_000_000,
            )
            .expect("create remote mainnet bootstrap topology");
        remote_store
            .put_network_protocol_params(
                network_id,
                &remote_identity,
                &NetworkProtocolParams::default(),
            )
            .expect("sign remote network params");
        let bundle = remote_store
            .for_org(org_id)
            .load_network_bootstrap_bundle()
            .expect("load remote bootstrap bundle");
        cleanup_dir(&remote_dir);
        bundle
    };
    let manifest = NetworkJoinManifest {
        network_id: network_id.to_owned(),
        genesis_node_id: remote_node_id.clone(),
        params_hash: bundle.signed_params.params_hash.clone(),
        bootstrap_urls: Vec::new(),
        bootstrap_contacts: vec!["iroh-bootstrap-contact-json".to_owned()],
        gateway_urls: vec!["https://gateway.wattetheria.com/".to_owned()],
        discovery_urls: vec![
            "https://bootstrap.wattetheria.com/api/network/discovery/".to_owned(),
            "https://bootstrap.wattetheria.com/api/network/discovery".to_owned(),
        ],
    };
    let bootstrap_stub = BootstrapBundleStub::start_with_manifest(bundle.clone(), Some(manifest));
    let _local_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &local_schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _bootstrap_url_guard = EnvVarGuard::remove("WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS");
    let _bootstrap_peers_guard = EnvVarGuard::remove("WATTSWARM_P2P_BOOTSTRAP_PEERS");
    let _manifest_guard = EnvVarGuard::set(
        "WATTSWARM_NETWORK_JOIN_MANIFEST_URLS",
        &format!(
            "http://127.0.0.1:{}/.well-known/wattswarm/join.json",
            bootstrap_stub.addr.port()
        ),
    );

    let dir = temp_test_dir("open-node-network-mode-join-manifest");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create local state dir");
    fs::write(
        state_dir.join("startup_config.json"),
        json!({
            "display_name": "Existing Node",
            "latitude": 37.0,
            "longitude": -122.0,
            "network_mode": "wan",
            "bootstrap_contacts": ["stale-bootstrap-contact"],
            "gateway_urls": ["https://old-gateway.wattetheria.com"]
        })
        .to_string(),
    )
    .expect("write existing startup config");
    let db_path = state_dir.join("local.state");
    let node = open_node(&state_dir, &db_path).expect("open node after join manifest sync");
    let verified = node
        .store
        .load_verified_network_protocol_params()
        .expect("load synced network params");
    assert_eq!(verified.network_id, network_id);
    assert_eq!(verified.genesis_node_id, remote_node_id);
    assert_eq!(verified.signed, bundle.signed_params);
    let startup_config: serde_json::Value =
        serde_json::from_slice(&fs::read(state_dir.join("startup_config.json")).unwrap()).unwrap();
    assert_eq!(startup_config["network_mode"].as_str(), Some("wan"));
    assert_eq!(
        startup_config["display_name"].as_str(),
        Some("Existing Node")
    );
    assert_eq!(startup_config["latitude"].as_f64(), Some(37.0));
    assert_eq!(startup_config["longitude"].as_f64(), Some(-122.0));
    assert_eq!(
        startup_config["bootstrap_contacts"]
            .as_array()
            .expect("bootstrap contacts")
            .len(),
        1
    );
    assert_eq!(
        startup_config["bootstrap_contacts"][0].as_str(),
        Some("iroh-bootstrap-contact-json")
    );
    assert_eq!(
        startup_config["gateway_urls"]
            .as_array()
            .expect("gateway urls")
            .len(),
        1
    );
    assert_eq!(
        startup_config["gateway_urls"][0].as_str(),
        Some("https://gateway.wattetheria.com")
    );
    assert_eq!(
        wattswarm_control_plane::load_discovery_bootnode_urls_state(&state_dir)
            .expect("load discovery urls"),
        vec!["https://bootstrap.wattetheria.com/api/network/discovery".to_owned()]
    );
    assert!(startup_config.get("servicenet_urls").is_none());

    cleanup_dir(&dir);
}

#[test]
fn open_node_network_mode_rejects_join_manifest_params_hash_mismatch() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let local_schema = format!("test_local_{}", Uuid::new_v4().simple());
    let remote_schema = format!("test_remote_{}", Uuid::new_v4().simple());
    reset_test_schema(&local_schema);
    reset_test_schema(&remote_schema);

    let remote_identity = NodeIdentity::random();
    let remote_node_id = remote_identity.node_id();
    let network_id = "mainnet:wattetheria";
    let org_id = "mainnet:wattetheria:bootstrap";
    let bundle = {
        let _remote_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &remote_schema);
        let remote_dir = temp_test_dir("remote-join-mismatch-bundle");
        let remote_state_dir = remote_dir.join("state");
        fs::create_dir_all(&remote_state_dir).expect("create remote state dir");
        let remote_db_path = remote_state_dir.join("remote.state");
        let remote_store = wattswarm_control_plane::storage::PgStore::open(&remote_db_path)
            .expect("open remote bootstrap store");
        remote_store
            .ensure_mainnet_bootstrap_network_topology(
                network_id,
                "Wattetheria",
                &remote_node_id,
                &remote_node_id,
                1_700_000_000_000,
            )
            .expect("create remote mainnet bootstrap topology");
        remote_store
            .put_network_protocol_params(
                network_id,
                &remote_identity,
                &NetworkProtocolParams::default(),
            )
            .expect("sign remote network params");
        let bundle = remote_store
            .for_org(org_id)
            .load_network_bootstrap_bundle()
            .expect("load remote bootstrap bundle");
        cleanup_dir(&remote_dir);
        bundle
    };
    let manifest = NetworkJoinManifest {
        network_id: network_id.to_owned(),
        genesis_node_id: remote_node_id,
        params_hash: "wrong-params-hash".to_owned(),
        bootstrap_urls: Vec::new(),
        bootstrap_contacts: Vec::new(),
        gateway_urls: Vec::new(),
        discovery_urls: Vec::new(),
    };
    let bootstrap_stub = BootstrapBundleStub::start_with_manifest(bundle, Some(manifest));
    let _local_schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &local_schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "network");
    let _bootstrap_url_guard = EnvVarGuard::remove("WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS");
    let _bootstrap_peers_guard = EnvVarGuard::remove("WATTSWARM_P2P_BOOTSTRAP_PEERS");
    let _manifest_guard = EnvVarGuard::set(
        "WATTSWARM_NETWORK_JOIN_MANIFEST_URLS",
        &format!(
            "http://127.0.0.1:{}/.well-known/wattswarm/join.json",
            bootstrap_stub.addr.port()
        ),
    );

    let dir = temp_test_dir("open-node-network-mode-join-mismatch");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create local state dir");
    let db_path = state_dir.join("local.state");
    let err = match open_node(&state_dir, &db_path) {
        Ok(_) => panic!("mismatched params hash must fail"),
        Err(err) => err,
    };
    assert!(err.chain().any(|cause| {
        cause
            .to_string()
            .contains("join manifest params_hash mismatch")
    }));

    cleanup_dir(&dir);
}

#[test]
fn open_node_on_network_id_joins_and_binds_subnet_overlay() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    reset_test_schema(&schema);
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = temp_test_dir("open-node-on-subnet");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let genesis_identity = NodeIdentity::random();
    let genesis_node_id = genesis_identity.node_id();
    let now = 1_700_000_000_000_u64;

    let store = wattswarm_control_plane::storage::PgStore::open(&db_path).expect("open store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &genesis_node_id,
            &genesis_node_id,
            now,
        )
        .expect("create mainnet topology");
    store
        .ensure_bootstrap_signed_network_protocol_params(
            &mainnet.network.network_id,
            &genesis_identity,
        )
        .expect("sign mainnet params");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &genesis_node_id,
            &genesis_node_id,
            now + 1,
        )
        .expect("create subnet topology");
    store
        .ensure_bootstrap_signed_network_protocol_params(
            &subnet.network.network_id,
            &genesis_identity,
        )
        .expect("sign subnet params");

    let node = open_node_on_network_id(&state_dir, &db_path, &subnet.network.network_id)
        .expect("open node on subnet");
    assert_eq!(node.store.org_id(), subnet.org.org_id);

    let opened_topology = store
        .load_network_topology_for_org(node.store.org_id())
        .expect("load opened topology");
    assert_eq!(opened_topology.network.network_kind, NetworkKind::Subnet);
    assert_eq!(
        opened_topology.network.parent_network_id.as_deref(),
        Some(mainnet.network.network_id.as_str())
    );

    let joined_node_id = local_node_id(&state_dir).expect("joined node id");
    assert!(
        store
            .node_has_network_membership(&joined_node_id, &subnet.network.network_id)
            .expect("subnet membership")
    );
    assert!(
        store
            .node_has_network_membership(&joined_node_id, &mainnet.network.network_id)
            .expect("parent membership")
    );

    cleanup_dir(&dir);
}
