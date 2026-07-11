use super::*;

#[test]
fn udp_announce_startup_and_listener_paths_execute() {
    let _guard = env_lock();
    let dir = temp_test_dir("udp-announce");
    let identity = NodeIdentity::from_seed([210u8; 32]);
    let remote_node_id = identity.node_id();
    let endpoint_id = test_iroh_endpoint_id(211);
    let contact = test_iroh_contact_material(&endpoint_id, 1, &["127.0.0.1:4001"]);
    let material = json!({
        "node_id": remote_node_id.clone(),
        "peer_id": endpoint_id,
        "generated_at": 1,
        "transports": contact["transports"].clone(),
    });
    let material_json = serde_json::to_string(&material).expect("serialize contact");

    let payload = serde_json::json!({
        "kind": "wattswarm_udp_announce_v1",
        "node_id": remote_node_id.clone(),
        "listen_addr": "127.0.0.1:4001",
        "contact_material": {
            "material_json": material_json.clone(),
            "signature": identity.sign_bytes(material_json.as_bytes()),
            "generated_at": 1
        }
    })
    .to_string();

    let mut found = false;
    for _ in 0..5 {
        let probe = UdpSocket::bind("127.0.0.1:0").expect("bind probe udp socket");
        let port = probe.local_addr().expect("probe local addr").port();
        drop(probe);

        with_udp_env("1", "broadcast", "127.0.0.1", port);
        announce_startup("kernel", Some("127.0.0.1:7788"), Some("node-self"));
        maybe_start_listener(dir.clone(), "node-self".to_owned());

        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind sender socket");
        let _ = sender.send_to(payload.as_bytes(), ("127.0.0.1", port));

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(700) {
            if let Ok(records) = load_discovered_peer_records_state(&dir)
                && records
                    .iter()
                    .any(|record| record.node_id == remote_node_id && record.source_kind == "udp")
                && wattswarm_control_plane::control::load_peer_metadata_records_state(&dir)
                    .is_ok_and(|records| {
                        records.iter().any(|record| {
                            record.node_id == remote_node_id
                                && record.transport_contact_materials().len() == 1
                        })
                    })
            {
                found = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        if found {
            break;
        }
    }
    assert!(found, "listener should persist discovered peer");

    clear_udp_env();
    cleanup_dir(&dir);
}

#[test]
fn udp_listener_noop_when_disabled() {
    let _guard = env_lock();
    let dir = temp_test_dir("udp-disabled");

    with_udp_env("0", "broadcast", "127.0.0.1", 37931);
    maybe_start_listener(dir.clone(), "node-self".to_owned());

    thread::sleep(Duration::from_millis(100));
    let path = discovered_peers_path(&dir);
    assert!(!path.exists());

    clear_udp_env();
    cleanup_dir(&dir);
}
