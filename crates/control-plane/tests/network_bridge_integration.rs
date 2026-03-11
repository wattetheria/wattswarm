use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use wattswarm_control_plane::control::add_discovered_peer_endpoint;
use wattswarm_control_plane::crypto::NodeIdentity;
use wattswarm_control_plane::network_bridge::{
    NetworkBridgeService, NetworkBridgeTick, dial_discovered_peer_endpoints,
    publish_pending_global_events,
};
use wattswarm_control_plane::network_p2p::{NetworkP2pConfig, NetworkP2pNode};
use wattswarm_control_plane::node::Node;
use wattswarm_control_plane::storage::PgStore;
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::types::{Membership, Role};

fn membership_with_roles(node_ids: &[String]) -> Membership {
    let mut membership = Membership::new();
    for node_id in node_ids {
        for role in [
            Role::Proposer,
            Role::Verifier,
            Role::Committer,
            Role::Finalizer,
        ] {
            membership.grant(node_id, role);
        }
    }
    membership
}

fn make_node(identity: NodeIdentity, membership: Membership) -> Node {
    Node::new(
        identity,
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("node")
}

fn make_service() -> NetworkBridgeService {
    let config = NetworkP2pConfig {
        listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
        enable_mdns: false,
        ..NetworkP2pConfig::default()
    };
    NetworkBridgeService::new(NetworkP2pNode::generate(config).expect("network node"))
        .expect("network service")
}

fn pump_once(service: &mut NetworkBridgeService, node: &mut Node) -> Option<NetworkBridgeTick> {
    service.try_tick(node).expect("tick")
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("wattswarm-{prefix}-{}", Uuid::new_v4().simple()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn cleanup_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

#[test]
fn two_nodes_sync_global_event_over_libp2p() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    for _ in 0..4_096 {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if !service_a.listen_addrs().is_empty() && !service_b.listen_addrs().is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(!service_a.listen_addrs().is_empty());
    assert!(!service_b.listen_addrs().is_empty());

    service_a
        .dial(service_b.listen_addrs()[0].clone())
        .expect("dial peer");

    let peer_a = service_a.local_peer_id();
    let peer_b = service_b.local_peer_id();
    let mut a_connected = false;
    let mut b_connected = false;
    for _ in 0..4_096 {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if matches!(
            tick_a.as_ref(),
            Some(NetworkBridgeTick::Connected { peer }) if *peer == peer_b
        ) {
            a_connected = true;
        }
        if matches!(
            tick_b.as_ref(),
            Some(NetworkBridgeTick::Connected { peer }) if *peer == peer_a
        ) {
            b_connected = true;
        }
        if a_connected && b_connected {
            break;
        }
        std::thread::yield_now();
    }
    assert!(a_connected && b_connected);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-gossip", policy_hash);
    contract.inputs = json!({"prompt":"ship this over gossip"});
    node_a.submit_task(contract, 1, 100).expect("submit task");
    let last_published_seq = 0;
    let mut published = false;
    for _ in 0..4_096 {
        let new_last = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending");
        if new_last > last_published_seq {
            published = true;
            break;
        }
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        std::thread::yield_now();
    }
    assert!(
        published,
        "local event should auto-publish over bridge helper"
    );

    let mut synced = false;
    for _ in 0..4_096 {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if node_b
            .task_view("task-network-gossip")
            .expect("task view")
            .is_some()
        {
            synced = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(synced);

    let task = node_b
        .task_view("task-network-gossip")
        .expect("task view")
        .expect("task exists");
    assert_eq!(
        task.contract.inputs["prompt"],
        json!("ship this over gossip")
    );
}

#[test]
fn two_nodes_backfill_missing_events_over_request_response() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    for _ in 0..4_096 {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if !service_a.listen_addrs().is_empty() && !service_b.listen_addrs().is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(!service_a.listen_addrs().is_empty());
    assert!(!service_b.listen_addrs().is_empty());

    service_b
        .dial(service_a.listen_addrs()[0].clone())
        .expect("dial peer");

    let peer_a = service_a.local_peer_id();
    let peer_b = service_b.local_peer_id();
    let mut a_connected = false;
    let mut b_connected = false;
    for _ in 0..4_096 {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if matches!(
            tick_a.as_ref(),
            Some(NetworkBridgeTick::Connected { peer }) if *peer == peer_b
        ) {
            a_connected = true;
        }
        if matches!(
            tick_b.as_ref(),
            Some(NetworkBridgeTick::Connected { peer }) if *peer == peer_a
        ) {
            b_connected = true;
        }
        if a_connected && b_connected {
            break;
        }
        std::thread::yield_now();
    }
    assert!(a_connected && b_connected);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-backfill", policy_hash);
    contract.inputs = json!({"prompt":"recover me via backfill"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut synced = false;
    for _ in 0..4_096 {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if node_b
            .task_view("task-network-backfill")
            .expect("task view")
            .is_some()
        {
            synced = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(synced);

    let task = node_b
        .task_view("task-network-backfill")
        .expect("task view")
        .expect("task exists");
    assert_eq!(
        task.contract.inputs["prompt"],
        json!("recover me via backfill")
    );
}

#[test]
fn discovered_peer_endpoint_helper_dials_and_syncs_over_lan_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();
    let dir_a = temp_test_dir("network-dial-a");
    let dir_b = temp_test_dir("network-dial-b");

    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if !service_a.listen_addrs().is_empty() && !service_b.listen_addrs().is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(!service_a.listen_addrs().is_empty());
    assert!(!service_b.listen_addrs().is_empty());

    add_discovered_peer_endpoint(
        &dir_a,
        &node_b.node_id(),
        Some(&service_b.listen_addrs()[0].to_string()),
    )
    .expect("record node b endpoint");
    add_discovered_peer_endpoint(
        &dir_b,
        &node_a.node_id(),
        Some(&service_a.listen_addrs()[0].to_string()),
    )
    .expect("record node a endpoint");

    let mut attempts_a = HashMap::new();
    let mut attempts_b = HashMap::new();
    assert_eq!(
        dial_discovered_peer_endpoints(&mut service_a, &dir_a, &node_a.node_id(), &mut attempts_a)
            .expect("dial discovered peers a"),
        1
    );
    assert_eq!(
        dial_discovered_peer_endpoints(&mut service_b, &dir_b, &node_b.node_id(), &mut attempts_b)
            .expect("dial discovered peers b"),
        1
    );

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-lan", policy_hash);
    contract.inputs = json!({"prompt":"lan discovery sync"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut synced = false;
    for _ in 0..4_096 {
        let _ = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .map(|seq| last_published_seq = seq);
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-network-lan")
            .expect("task view")
            .is_some()
        {
            synced = true;
            break;
        }
        std::thread::yield_now();
    }

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
    assert!(
        synced,
        "discovered peer endpoint dialing should sync local event"
    );
}
