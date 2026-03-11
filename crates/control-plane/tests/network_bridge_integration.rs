use serde_json::json;
use wattswarm_control_plane::crypto::NodeIdentity;
use wattswarm_control_plane::network_bridge::{NetworkBridgeService, NetworkBridgeTick};
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
    let event = node_a.submit_task(contract, 1, 100).expect("submit task");
    let mut published = false;
    for _ in 0..4_096 {
        match service_a.publish_global_event(event.clone()) {
            Ok(()) => {
                published = true;
                break;
            }
            Err(err) if err.to_string().contains("NoPeersSubscribedToTopic") => {
                let _ = pump_once(&mut service_a, &mut node_a);
                let _ = pump_once(&mut service_b, &mut node_b);
                std::thread::yield_now();
            }
            Err(err) => panic!("publish global event: {err}"),
        }
    }
    assert!(published);

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

    service_b
        .request_global_backfill(&service_a.local_peer_id(), 0, 16)
        .expect("request backfill");

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
