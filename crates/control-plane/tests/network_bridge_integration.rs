use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use uuid::Uuid;
use wattswarm_control_plane::control::add_discovered_peer_endpoint;
use wattswarm_control_plane::crypto::NodeIdentity;
use wattswarm_control_plane::network_bridge::{
    NetworkBridgeService, NetworkBridgeTick, build_knowledge_summary_for_task_type,
    build_reputation_summary_for_runtime, dial_discovered_peer_endpoints,
    publish_pending_global_events, publish_pending_scoped_updates,
};
use wattswarm_control_plane::network_p2p::{NetworkP2pConfig, NetworkP2pNode, SwarmScope};
use wattswarm_control_plane::node::Node;
use wattswarm_control_plane::storage::PgStore;
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::types::{Membership, Role};
use wattswarm_protocol::types::NetworkProtocolParams;

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

/// Short protocol params for tests — the genesis node of this test network
/// sets a 1-second anti-entropy interval instead of the production 15 seconds.
fn test_protocol_params() -> NetworkProtocolParams {
    NetworkProtocolParams {
        anti_entropy_interval_secs: 1,
        backfill_retry_after_secs: 1,
        ..NetworkProtocolParams::default()
    }
}

fn make_service() -> NetworkBridgeService {
    make_service_with_scopes(&[SwarmScope::Global])
}

fn make_service_with_scopes(scopes: &[SwarmScope]) -> NetworkBridgeService {
    make_service_with_params(scopes, &NetworkProtocolParams::default())
}

fn make_fast_service() -> NetworkBridgeService {
    make_service_with_params(&[SwarmScope::Global], &test_protocol_params())
}

fn make_fast_service_with_scopes(scopes: &[SwarmScope]) -> NetworkBridgeService {
    make_service_with_params(scopes, &test_protocol_params())
}

fn make_service_with_params(
    scopes: &[SwarmScope],
    params: &NetworkProtocolParams,
) -> NetworkBridgeService {
    let config = NetworkP2pConfig {
        listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
        enable_mdns: false,
        ..NetworkP2pConfig::default()
    }
    .apply_protocol_params(params);
    NetworkBridgeService::new(
        NetworkP2pNode::generate(config).expect("network node"),
        scopes,
        params,
    )
    .expect("network service")
}

fn pump_once(service: &mut NetworkBridgeService, node: &mut Node) -> Option<NetworkBridgeTick> {
    service.try_tick(node).expect("tick")
}

fn wait_until(timeout: Duration, mut step: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if step() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    step()
}

fn slow_env_multiplier() -> u32 {
    if std::env::var_os("LLVM_PROFILE_FILE").is_some() || std::env::var_os("CI").is_some() {
        3
    } else {
        1
    }
}

fn scaled_timeout(base: Duration) -> Duration {
    base.saturating_mul(slow_env_multiplier())
}

fn reconnect_quiet_period() -> Duration {
    scaled_timeout(Duration::from_millis(750))
}

fn pump_services_for(
    service_a: &mut NetworkBridgeService,
    node_a: &mut Node,
    service_b: &mut NetworkBridgeService,
    node_b: &mut Node,
    duration: Duration,
) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let _ = pump_once(service_a, node_a);
        let _ = pump_once(service_b, node_b);
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("wattswarm-{prefix}-{}", Uuid::new_v4().simple()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn cleanup_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn connect_services(
    dialer: &mut NetworkBridgeService,
    dialer_node: &mut Node,
    receiver: &mut NetworkBridgeService,
    receiver_node: &mut Node,
) {
    for _ in 0..4_096 {
        let _ = pump_once(dialer, dialer_node);
        let _ = pump_once(receiver, receiver_node);
        if !dialer.listen_addrs().is_empty() && !receiver.listen_addrs().is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    dialer
        .dial(receiver.listen_addrs()[0].clone())
        .expect("dial peer");
    for _ in 0..4_096 {
        let _ = pump_once(dialer, dialer_node);
        let _ = pump_once(receiver, receiver_node);
        std::thread::yield_now();
    }
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
    assert_eq!(
        dial_discovered_peer_endpoints(&mut service_a, &dir_a, &node_a.node_id(), &mut attempts_a)
            .expect("dial discovered peers a"),
        1
    );

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

#[test]
fn region_scoped_backfill_only_reaches_region_subscribers() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut service_a =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())]);
    let mut service_b =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())]);
    let mut service_c = make_service_with_scopes(&[SwarmScope::Global]);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-region-backfill", policy_hash);
    contract.task_type = "region:sol-1:swarm".to_owned();
    contract.inputs = json!({"prompt":"region sync", "swarm_scope":"region:sol-1"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);

    let mut region_synced = false;
    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_b
            .task_view("task-region-backfill")
            .expect("task view")
            .is_some()
        {
            region_synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(region_synced);
    assert!(
        node_c
            .task_view("task-region-backfill")
            .expect("task view")
            .is_none()
    );
}

#[test]
fn node_scoped_live_sync_only_reaches_matching_node_scope() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let identity_c = NodeIdentity::random();
    let membership = membership_with_roles(&[
        identity_a.node_id(),
        identity_b.node_id(),
        identity_c.node_id(),
    ]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership.clone());
    let mut node_c = make_node(identity_c, membership);
    let mut service_a =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Node("lab-1".to_owned())]);
    let mut service_b =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Node("lab-1".to_owned())]);
    let mut service_c =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Node("lab-2".to_owned())]);

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-local-live", policy_hash);
    contract.task_type = "node:lab-1:swarm".to_owned();
    contract.inputs = json!({"prompt":"node-scoped sync", "swarm_scope":"node:lab-1"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut local_synced = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending scoped");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_b
            .task_view("task-local-live")
            .expect("task view")
            .is_some()
        {
            local_synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(local_synced);
    assert!(
        node_c
            .task_view("task-local-live")
            .expect("task view")
            .is_none()
    );
}

#[test]
fn summary_gossip_imports_knowledge_and_reputation_into_remote_store() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    node_a
        .store
        .put_decision_memory(
            "task-summary",
            1,
            "commit-1",
            100,
            "candidate-hash-1",
            "output-digest-1",
            &json!({"answer":"from summary"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"summary"}),
            "policy-snap-1",
            "summary-type",
            "input-digest-1",
            "schema-digest-1",
            "vp.schema_only.v1",
            "params-digest-1",
        )
        .expect("put decision memory");
    node_a
        .store
        .put_reputation_snapshot("runtime-a", "model-a", 1200, 3400, 111)
        .expect("put reputation snapshot");

    let knowledge =
        build_knowledge_summary_for_task_type(&node_a, &SwarmScope::Global, "summary-type")
            .expect("build knowledge summary")
            .expect("knowledge summary");
    let reputation = build_reputation_summary_for_runtime(&node_a, "runtime-a", "model-a")
        .expect("build reputation summary")
        .expect("reputation summary");

    service_a
        .publish_summary(knowledge)
        .expect("publish knowledge");
    service_a
        .publish_summary(reputation)
        .expect("publish reputation");

    let mut imported = false;
    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .store
            .list_decision_memory_hits_by_task_type("summary-type", 8)
            .expect("list decisions")
            .len()
            == 1
            && node_b
                .store
                .get_reputation_snapshot("runtime-a", "model-a")
                .expect("get reputation")
                .is_some()
        {
            imported = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(imported);

    let imported_reputation = node_b
        .store
        .get_reputation_snapshot("runtime-a", "model-a")
        .expect("get imported reputation")
        .expect("reputation exists");
    assert_eq!(imported_reputation.stability_reputation, 1200);
    assert_eq!(imported_reputation.quality_reputation, 3400);

    let imported_decisions = node_b
        .store
        .list_decision_memory_hits_by_task_type("summary-type", 8)
        .expect("list imported decisions");
    assert_eq!(imported_decisions.len(), 1);
    assert_eq!(
        imported_decisions[0].result_summary["answer"],
        json!("from summary")
    );
}

#[test]
fn revoked_event_propagates_and_removes_remote_projection_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-revocation-network", policy_hash);
    contract.inputs = json!({"prompt":"revoke over network"});
    let created = node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish task event");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-revocation-network")
            .expect("task view")
            .is_some()
        {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        node_b
            .task_view("task-revocation-network")
            .expect("task view after sync")
            .is_some()
    );

    node_a
        .revoke_event(&created.event_id, "malicious task", 1, 101)
        .expect("revoke event");

    let mut revoked = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish revoke event");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-revocation-network")
            .expect("task view after revoke")
            .is_none()
        {
            revoked = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(revoked);
}

#[test]
fn revoked_summary_event_removes_remote_imported_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    node_a
        .store
        .put_decision_memory(
            "task-summary-revoke",
            1,
            "commit-revoke",
            100,
            "candidate-revoke",
            "output-revoke",
            &json!({"answer":"revoke this summary"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"summary"}),
            "policy-snap-1",
            "summary-revoke-type",
            "input-digest-1",
            "schema-digest-1",
            "vp.schema_only.v1",
            "params-digest-1",
        )
        .expect("put decision memory");
    let knowledge =
        build_knowledge_summary_for_task_type(&node_a, &SwarmScope::Global, "summary-revoke-type")
            .expect("build knowledge summary")
            .expect("knowledge summary");

    service_a
        .publish_summary(knowledge.clone())
        .expect("publish knowledge");

    let mut imported = false;
    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .store
            .list_decision_memory_hits_by_task_type("summary-revoke-type", 8)
            .expect("list imported decisions")
            .len()
            == 1
        {
            imported = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(imported);

    node_a
        .revoke_summary(
            &knowledge.summary_id,
            &knowledge.summary_kind,
            "summary was malicious",
            1,
            110,
        )
        .expect("revoke summary");

    let mut last_published_seq = 0;
    let mut removed = false;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish summary revoke event");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .store
            .list_decision_memory_hits_by_task_type("summary-revoke-type", 8)
            .expect("list imported decisions after revoke")
            .is_empty()
        {
            removed = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(removed);
}

#[test]
fn anti_entropy_syncs_missed_event_without_live_publish() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_fast_service();
    let mut service_b = make_fast_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-anti-entropy", policy_hash);
    contract.inputs = json!({"prompt":"catch me via anti entropy"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    // Wait for the governance-configured anti-entropy interval (1s in test
    // network) to elapse after the initial empty backfill on connect.
    std::thread::sleep(Duration::from_secs(2));

    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        let _ = service_b
            .run_anti_entropy(&node_b)
            .expect("run anti entropy");
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_a, &mut node_a);
        node_b
            .task_view("task-anti-entropy")
            .expect("task view")
            .is_some()
    });

    assert!(synced);
}

#[test]
fn anti_entropy_uses_scope_specific_cursor_for_recovery() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_fast_service_with_scopes(&[
        SwarmScope::Global,
        SwarmScope::Region("sol-1".to_owned()),
    ]);
    let mut service_b = make_fast_service_with_scopes(&[
        SwarmScope::Global,
        SwarmScope::Region("sol-1".to_owned()),
    ]);

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut region_contract = sample_contract("task-region-cursor", policy_hash.clone());
    region_contract.task_type = "region:sol-1:cursor".to_owned();
    region_contract.inputs = json!({"prompt":"recover region gap", "swarm_scope":"region:sol-1"});
    node_a
        .submit_task(region_contract, 1, 100)
        .expect("submit region task");

    let mut global_contract = sample_contract("task-global-local-head", policy_hash);
    global_contract.inputs = json!({"prompt":"advance local head"});
    node_b
        .submit_task(global_contract, 1, 101)
        .expect("submit local global task");

    // Wait for the governance-configured anti-entropy interval (1s in test
    // network) to elapse after the initial empty backfill on connect.
    std::thread::sleep(Duration::from_secs(2));

    let recovered = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        let _ = service_b
            .run_anti_entropy(&node_b)
            .expect("run anti entropy");
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_a, &mut node_a);
        node_b
            .task_view("task-region-cursor")
            .expect("task view")
            .is_some()
    });

    assert!(recovered);
}

#[test]
fn reconnect_recovers_missing_events_after_partition_like_disconnect() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_fast_service();
    let mut service_b = make_fast_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut first_contract = sample_contract("task-partition-first", policy_hash.clone());
    first_contract.inputs = json!({"prompt":"before disconnect"});
    node_a
        .submit_task(first_contract, 1, 100)
        .expect("submit first task");

    let mut last_published_seq = 0;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish first task");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-partition-first")
            .expect("task view")
            .is_some()
        {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        node_b
            .task_view("task-partition-first")
            .expect("task view first")
            .is_some()
    );

    // Drain inflight messages before disconnect to avoid libp2p
    // request-response assertion on stale connection state.
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );
    let peer_b_old = service_b.local_peer_id();
    drop(service_b);
    // Pump service_a until it observes the disconnect event.
    let _ = wait_until(scaled_timeout(Duration::from_secs(5)), || {
        matches!(
            pump_once(&mut service_a, &mut node_a),
            Some(NetworkBridgeTick::Disconnected { peer }) if peer == peer_b_old
        )
    });

    let mut second_contract = sample_contract("task-partition-second", policy_hash);
    second_contract.inputs = json!({"prompt":"after reconnect"});
    node_a
        .submit_task(second_contract, 1, 101)
        .expect("submit second task");

    let mut service_b = make_fast_service();
    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let recovered = wait_until(scaled_timeout(Duration::from_secs(20)), || {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        node_b
            .task_view("task-partition-second")
            .expect("task view second")
            .is_some()
    });

    assert!(recovered);
}
