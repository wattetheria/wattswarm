use super::*;

#[test]
fn local_node_id_is_stable_for_same_state_dir() {
    let dir = temp_test_dir("local-node-id");

    let node_id_1 = local_node_id(&dir).expect("first node id");
    let node_id_2 = local_node_id(&dir).expect("second node id");
    assert_eq!(node_id_1, node_id_2);

    cleanup_dir(&dir);
}

#[test]
fn open_node_creates_local_artifact_store_layout() {
    let dir = temp_test_dir("artifact-layout");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let _node = open_node(&state_dir, &db_path).expect("open node");

    assert!(artifact_store_path(&state_dir).join("references").exists());
    assert!(artifact_store_path(&state_dir).join("evidence").exists());
    assert!(artifact_store_path(&state_dir).join("checkpoints").exists());
    assert!(artifact_store_path(&state_dir).join("snapshots").exists());
    assert!(
        artifact_store_path(&state_dir)
            .join("availability")
            .exists()
    );

    cleanup_dir(&dir);
}

#[test]
fn open_node_bootstraps_local_network_and_default_org() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let dir = temp_test_dir("open-node-bootstrap");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let _node = open_node(&state_dir, &db_path).expect("open node");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = format!("local:{node_id}");
    let org_id = format!("{network_id}:bootstrap");

    let conn = Connection::open(&db_path).expect("open verification connection");
    let network_row = conn
        .query_row(
            "SELECT network_kind, genesis_node_id FROM network_registry WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .expect("network registry row");
    assert_eq!(network_row.0, "local");
    assert_eq!(network_row.1, node_id);

    let org_row = conn
        .query_row(
            "SELECT network_id, org_kind, is_default FROM org_registry WHERE org_id = $1",
            wattswarm_storage_core::params![&org_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            },
        )
        .expect("org registry row");
    assert_eq!(org_row.0, format!("local:{node_id}"));
    assert_eq!(org_row.1, "bootstrap");
    assert!(org_row.2);

    let node_row = conn
        .query_row(
            "SELECT public_key, home_network_id FROM node_registry WHERE node_id = $1",
            wattswarm_storage_core::params![&node_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .expect("node registry row");
    assert_eq!(node_row.0, node_id);
    assert_eq!(node_row.1, format!("local:{node_id}"));

    let signed_params: SignedNetworkProtocolParamsEnvelope = conn
        .query_row(
            "SELECT params_json FROM network_params WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
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
fn open_node_bootstraps_lan_network_and_default_org() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", "test");
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "lan");
    let dir = temp_test_dir("open-node-lan-bootstrap");
    let state_dir = dir.join("state");
    fs::create_dir_all(&state_dir).expect("create state dir");
    let db_path = state_dir.join("test.state");

    let _node = open_node(&state_dir, &db_path).expect("open node");
    let node_id = local_node_id(&state_dir).expect("local node id");
    let network_id = format!("lan:{node_id}");
    let org_id = format!("{network_id}:bootstrap");

    let conn = Connection::open(&db_path).expect("open verification connection");
    let network_row = conn
        .query_row(
            "SELECT network_kind, genesis_node_id FROM network_registry WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .expect("network registry row");
    assert_eq!(network_row.0, "lan");
    assert_eq!(network_row.1, node_id);

    let org_row = conn
        .query_row(
            "SELECT network_id, org_kind, is_default FROM org_registry WHERE org_id = $1",
            wattswarm_storage_core::params![&org_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            },
        )
        .expect("org registry row");
    assert_eq!(org_row.0, network_id);
    assert_eq!(org_row.1, "bootstrap");
    assert!(org_row.2);

    let signed_params: SignedNetworkProtocolParamsEnvelope = conn
        .query_row(
            "SELECT params_json FROM network_params WHERE network_id = $1",
            wattswarm_storage_core::params![&network_id],
            |r| r.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).expect("signed network params"))
        .expect("network params row");
    assert_eq!(signed_params.network_id, network_id);
    assert_eq!(signed_params.version, 1);
    assert_eq!(signed_params.signed_by, node_id);

    cleanup_dir(&dir);
}
