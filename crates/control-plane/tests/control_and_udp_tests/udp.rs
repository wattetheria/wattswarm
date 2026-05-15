use super::*;

#[test]
fn udp_announce_startup_and_listener_paths_execute() {
    let _guard = env_lock();
    let dir = temp_test_dir("udp-announce");

    let payload = serde_json::json!({
        "kind": "wattswarm_udp_announce_v1",
        "node_id": "node-peer",
        "listen_addr": "/ip4/127.0.0.1/tcp/4001"
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
                    .any(|record| record.node_id == "node-peer" && record.source_kind == "udp")
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
