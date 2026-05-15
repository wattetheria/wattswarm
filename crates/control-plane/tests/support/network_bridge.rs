#![allow(dead_code)]

use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use uuid::Uuid;
use wattswarm_artifact_store::{ArtifactKind, ArtifactStore};
use wattswarm_control_plane::control::{
    PeerDmDeliveryState, PeerDmMessageKind, PeerDmSessionState, PeerRelationshipAction,
    PeerRelationshipState, artifact_store_path, emit_topic_message_with_content,
    load_data_plane_status_records_state, load_peer_dm_message_records_state,
    load_peer_dm_thread_records_state, load_peer_metadata_records_state,
    load_peer_relationship_records_state,
};
use wattswarm_control_plane::crypto::{NodeIdentity, sha256_hex};
use wattswarm_control_plane::network_bridge::{
    NetworkBridgeService, NetworkBridgeTick, build_knowledge_summary_for_task_type,
    build_reputation_summary_for_runtime, latest_connected_peer_ids, publish_pending_global_events,
    publish_pending_scoped_updates,
};
use wattswarm_control_plane::network_p2p::{
    GossipKind, NetworkAddress, NetworkP2pConfig, NetworkP2pNode, PeerHandshakeMetadata,
    SwarmScope, TopicNamespace,
};
use wattswarm_control_plane::node::Node;
use wattswarm_control_plane::storage::PgStore;
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::types::{
    EventPayload, FeedSubscriptionUpdatedPayload, Membership, Role,
};
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

fn make_service_for_network(network_id: &str) -> NetworkBridgeService {
    make_service_for_network_with_scopes(network_id, &[SwarmScope::Global])
}

fn make_service_for_network_with_scopes(
    network_id: &str,
    scopes: &[SwarmScope],
) -> NetworkBridgeService {
    let params = NetworkProtocolParams::default();
    let config = NetworkP2pConfig {
        namespace: TopicNamespace {
            network: params.namespace_network.clone(),
            network_id: network_id.to_owned(),
        },
        identify_agent_version:
            wattswarm_control_plane::network_p2p::encode_wattswarm_agent_version(
                &PeerHandshakeMetadata {
                    network_id: network_id.to_owned(),
                    params_version: 1,
                    params_hash: format!("params-{network_id}"),
                },
            ),
        listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
        enable_local_discovery: false,
        ..NetworkP2pConfig::default()
    };
    make_service_with_config(scopes, &params, config)
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
    make_service_with_config(
        scopes,
        params,
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
    )
}

fn make_bootstrap_service_with_params(
    scopes: &[SwarmScope],
    params: &NetworkProtocolParams,
    bootstrap_peers: &[String],
) -> NetworkBridgeService {
    make_service_with_config(
        scopes,
        params,
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            bootstrap_peers: bootstrap_peers.to_vec(),
            ..NetworkP2pConfig::default()
        },
    )
}

fn make_service_with_config(
    scopes: &[SwarmScope],
    params: &NetworkProtocolParams,
    config: NetworkP2pConfig,
) -> NetworkBridgeService {
    let config = NetworkP2pConfig { ..config }.apply_protocol_params(params);
    NetworkBridgeService::new(
        NetworkP2pNode::generate(config).expect("network node"),
        scopes,
        params,
    )
    .expect("network service")
}

fn make_service_with_config_and_state_dir(
    scopes: &[SwarmScope],
    params: &NetworkProtocolParams,
    config: NetworkP2pConfig,
    state_dir: &Path,
) -> NetworkBridgeService {
    let config = NetworkP2pConfig { ..config }.apply_protocol_params(params);
    let identity = ensure_seeded_test_dir(state_dir);
    let db_path = state_dir.join("ui.state");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_ed25519_secret_bytes(config, identity.secret_bytes())
            .expect("seeded network node"),
        scopes,
        params,
    )
    .expect("network service");
    service.set_state_dir(state_dir.to_path_buf(), db_path.to_path_buf());
    service
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
    scaled_timeout(Duration::from_millis(2_000))
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

fn drive_anti_entropy_until(
    service_a: &mut NetworkBridgeService,
    node_a: &mut Node,
    service_b: &mut NetworkBridgeService,
    node_b: &mut Node,
    timeout: Duration,
    mut condition: impl FnMut(&Node) -> bool,
) -> bool {
    wait_until(timeout, || {
        for _ in 0..32 {
            let _ = pump_once(service_b, node_b);
            let _ = pump_once(service_a, node_a);
        }
        let _ = service_b
            .run_anti_entropy(node_b)
            .expect("run anti entropy");
        for _ in 0..32 {
            let _ = pump_once(service_b, node_b);
            let _ = pump_once(service_a, node_a);
        }
        condition(node_b)
    })
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("wattswarm-{prefix}-{}", Uuid::new_v4().simple()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn ensure_seeded_test_dir(path: &Path) -> NodeIdentity {
    let seed_file = path.join("node_seed.hex");
    if let Ok(raw) = fs::read_to_string(&seed_file) {
        let bytes = hex::decode(raw.trim()).expect("decode node seed");
        let seed: [u8; 32] = bytes.try_into().expect("32-byte node seed");
        return NodeIdentity::from_seed(seed);
    }
    let seed: [u8; 32] = rand::random();
    fs::write(&seed_file, hex::encode(seed)).expect("write node seed");
    NodeIdentity::from_seed(seed)
}

fn cleanup_dir(path: &Path) {
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(path);
    let _ = fs::remove_dir_all(path);
}

fn wait_for_listen_addrs(service: &mut NetworkBridgeService, node: &mut Node) {
    let listening = wait_until(scaled_timeout(Duration::from_secs(5)), || {
        let _ = pump_once(service, node);
        !service.listen_addrs().is_empty()
    });
    assert!(listening, "service should expose a listen address");
}

fn wait_for_connected_pair(
    service_a: &mut NetworkBridgeService,
    node_a: &mut Node,
    service_b: &mut NetworkBridgeService,
    node_b: &mut Node,
) {
    let peer_a = service_a.local_peer_id();
    let peer_b = service_b.local_peer_id();
    let mut a_connected = false;
    let mut b_connected = false;
    let connected = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        let tick_a = pump_once(service_a, node_a);
        let tick_b = pump_once(service_b, node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if matches!(
            tick_a.as_ref(),
            Some(NetworkBridgeTick::Connected { peer, .. }) if *peer == peer_b
        ) {
            a_connected = true;
        }
        if matches!(
            tick_b.as_ref(),
            Some(NetworkBridgeTick::Connected { peer, .. }) if *peer == peer_a
        ) {
            b_connected = true;
        }
        a_connected && b_connected
    });
    assert!(connected, "services should connect to each other");
    pump_services_for(
        service_a,
        node_a,
        service_b,
        node_b,
        scaled_timeout(Duration::from_millis(250)),
    );
}

fn iroh_peer_addr(service: &NetworkBridgeService) -> NetworkAddress {
    format!("{}@{}", service.local_peer_id(), service.listen_addrs()[0])
        .parse()
        .expect("iroh peer bootstrap address")
}

fn bootstrap_peer_addr(service: &mut NetworkBridgeService, node: &mut Node) -> String {
    wait_for_listen_addrs(service, node);
    iroh_peer_addr(service).to_string()
}

fn connect_services(
    dialer: &mut NetworkBridgeService,
    dialer_node: &mut Node,
    receiver: &mut NetworkBridgeService,
    receiver_node: &mut Node,
) {
    wait_for_listen_addrs(dialer, dialer_node);
    wait_for_listen_addrs(receiver, receiver_node);
    let receiver_addr = iroh_peer_addr(receiver);
    let dialer_addr = iroh_peer_addr(dialer);
    dialer.dial(receiver_addr).expect("dial peer");
    receiver.dial(dialer_addr).expect("dial reciprocal peer");
    wait_for_connected_pair(dialer, dialer_node, receiver, receiver_node);
}

fn relationship_state_for(path: &Path, remote_node_id: &str) -> Option<PeerRelationshipState> {
    load_peer_relationship_records_state(path)
        .expect("load peer relationships")
        .into_iter()
        .find(|record| record.remote_node_id == remote_node_id)
        .map(|record| record.relationship_state)
}

fn dm_thread_id(local_node_id: &str, remote_node_id: &str) -> String {
    let mut members = [local_node_id.to_owned(), remote_node_id.to_owned()];
    members.sort();
    format!("dm:{}:{}", members[0], members[1])
}

fn dm_thread_state_for(path: &Path, remote_node_id: &str) -> Option<PeerDmSessionState> {
    load_peer_dm_thread_records_state(path)
        .expect("load peer dm threads")
        .into_iter()
        .find(|record| record.remote_node_id == remote_node_id)
        .map(|record| record.session_state)
}

fn dm_messages_for(
    path: &Path,
    thread_id: &str,
) -> Vec<wattswarm_control_plane::control::PeerDmMessageRecord> {
    load_peer_dm_message_records_state(path, thread_id).expect("load peer dm messages")
}

fn contact_material_for(path: &Path, remote_node_id: &str) -> Option<serde_json::Value> {
    load_peer_metadata_records_state(path)
        .expect("load peer metadata")
        .into_iter()
        .find(|record| record.node_id == remote_node_id)
        .and_then(|record| record.contact_material)
}

fn content_artifact_exists(path: &Path, kind: ArtifactKind, digest: &str) -> bool {
    let store = ArtifactStore::new(artifact_store_path(path));
    store
        .read_validated_bytes(kind, digest, None, Some(digest), None)
        .is_ok()
}

fn data_plane_statuses_for(
    path: &Path,
    object_kind: &str,
    object_id: &str,
) -> Vec<wattswarm_control_plane::storage::LocalDataPlaneStatusRow> {
    load_data_plane_status_records_state(path)
        .expect("load data plane status rows")
        .into_iter()
        .filter(|row| row.object_kind == object_kind && row.object_id == object_id)
        .collect()
}

pub fn two_nodes_sync_global_event_over_iroh() {
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

    let mut last_tick_a = None;
    let mut last_tick_b = None;
    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
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
            return true;
        }
        last_tick_a = tick_a;
        last_tick_b = tick_b;
        false
    });
    let node_b_events = node_b.store.load_all_events().expect("load node b events");
    assert!(
        synced,
        "node B should ingest the task over iroh gossip; node_b_events={} last_tick_a={last_tick_a:?} last_tick_b={last_tick_b:?}",
        node_b_events.len()
    );

    let task = node_b
        .task_view("task-network-gossip")
        .expect("task view")
        .expect("task exists");
    assert_eq!(
        task.contract.inputs["prompt"],
        json!("ship this over gossip")
    );
}

pub fn two_nodes_execute_peer_relationship_request_and_accept_over_network() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let dir_a = temp_test_dir("peer-relationship-a");
    let dir_b = temp_test_dir("peer-relationship-b");
    let mut service_a = make_service_with_config_and_state_dir(
        &[SwarmScope::Global],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        &[SwarmScope::Global],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );

    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();

    service_a
        .send_peer_relationship_action(&remote_b, PeerRelationshipAction::Request, None)
        .expect("send relationship request");

    let mut last_tick_a = None;
    let mut last_tick_b = None;
    let request_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            last_tick_a = pump_once(&mut service_a, &mut node_a);
            last_tick_b = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Requested)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Requested)
    });
    assert!(
        request_synced,
        "relationship request should sync to both peers; a={:?} b={:?} last_tick_a={last_tick_a:?} last_tick_b={last_tick_b:?}",
        relationship_state_for(&dir_a, &remote_b),
        relationship_state_for(&dir_b, &remote_a)
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Accept, None)
        .expect("send relationship accept");

    let accepted = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Accepted)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Accepted)
    });
    assert!(accepted, "relationship accept should sync to both peers");

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}

pub fn two_nodes_execute_peer_relationship_request_and_block_over_network() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let dir_a = temp_test_dir("peer-relationship-block-a");
    let dir_b = temp_test_dir("peer-relationship-block-b");
    let mut service_a = make_service_with_config_and_state_dir(
        &[],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        &[],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );

    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();

    service_a
        .send_peer_relationship_action(&remote_b, PeerRelationshipAction::Request, None)
        .expect("send relationship request");

    let request_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Requested)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Requested)
    });
    assert!(
        request_synced,
        "relationship request should sync to both peers"
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Block, None)
        .expect("send relationship block");

    let blocked = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Blocked)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Blocked)
    });
    assert!(blocked, "relationship block should sync to both peers");

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}

pub fn two_nodes_establish_dm_session_and_exchange_messages_over_network() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let dir_a = temp_test_dir("peer-dm-a");
    let dir_b = temp_test_dir("peer-dm-b");
    let mut service_a = make_service_with_config_and_state_dir(
        &[],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        &[],
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );

    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();
    let thread_id = dm_thread_id(&remote_a, &remote_b);

    let send_without_relationship =
        service_a.send_peer_direct_message(&remote_b, None, json!({"text":"pre-relationship"}));
    assert!(
        send_without_relationship.is_err(),
        "dm should be rejected before an accepted relationship exists"
    );

    service_a
        .send_peer_relationship_action(&remote_b, PeerRelationshipAction::Request, None)
        .expect("send relationship request");
    let request_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Requested)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Requested)
    });
    assert!(
        request_synced,
        "relationship request should sync to both peers"
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Accept, None)
        .expect("send relationship accept");

    let mut last_direct_tick_a = String::new();
    let mut last_direct_tick_b = String::new();
    let session_ready = wait_until(scaled_timeout(Duration::from_secs(30)), || {
        for _ in 0..64 {
            let tick_a = pump_once(&mut service_a, &mut node_a);
            let tick_b = pump_once(&mut service_b, &mut node_b);
            if let Some(
                NetworkBridgeTick::PeerDirectMessageUpdated { .. }
                | NetworkBridgeTick::PeerDirectMessageFailed { .. },
            ) = tick_a.as_ref()
            {
                last_direct_tick_a = format!("{tick_a:?}");
            }
            if let Some(
                NetworkBridgeTick::PeerDirectMessageUpdated { .. }
                | NetworkBridgeTick::PeerDirectMessageFailed { .. },
            ) = tick_b.as_ref()
            {
                last_direct_tick_b = format!("{tick_b:?}");
            }
        }
        let rel_a = relationship_state_for(&dir_a, &remote_b);
        let rel_b = relationship_state_for(&dir_b, &remote_a);
        let dm_a = dm_thread_state_for(&dir_a, &remote_b);
        let dm_b = dm_thread_state_for(&dir_b, &remote_a);
        let contact_a = contact_material_for(&dir_a, &remote_b).is_some();
        let contact_b = contact_material_for(&dir_b, &remote_a).is_some();
        rel_a == Some(PeerRelationshipState::Accepted)
            && rel_b == Some(PeerRelationshipState::Accepted)
            && dm_a == Some(PeerDmSessionState::Ready)
            && dm_b == Some(PeerDmSessionState::Ready)
            && contact_a
            && contact_b
    });
    assert!(
        session_ready,
        "accepted relationship should establish a ready dm session and exchange contact material; states: a={:?} b={:?} contact_a={} contact_b={} connected_a={:?} connected_b={:?} last_tick_a={} last_tick_b={}",
        dm_thread_state_for(&dir_a, &remote_b),
        dm_thread_state_for(&dir_b, &remote_a),
        contact_material_for(&dir_a, &remote_b).is_some(),
        contact_material_for(&dir_b, &remote_a).is_some(),
        latest_connected_peer_ids(&dir_a),
        latest_connected_peer_ids(&dir_b),
        last_direct_tick_a,
        last_direct_tick_b,
    );

    let contact_a = contact_material_for(&dir_a, &remote_b).expect("contact material for b on a");
    let contact_b = contact_material_for(&dir_b, &remote_a).expect("contact material for a on b");
    for contact in [&contact_a, &contact_b] {
        let transports = contact
            .get("transports")
            .and_then(|value| value.as_array())
            .expect("transports array");
        assert!(
            transports.iter().any(|entry| {
                entry.get("transport").and_then(|value| value.as_str()) == Some("iroh_direct")
            }),
            "contact material should advertise iroh_direct transport"
        );
        let routes = contact
            .get("recommended_routes")
            .expect("recommended_routes object");
        assert_eq!(
            routes
                .get("direct_message")
                .and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes
                .get("backfill_chunk")
                .and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("topic_sync").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("task_sync").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("artifact_blob").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes.get("evidence_blob").and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
        assert_eq!(
            routes
                .get("checkpoint_snapshot")
                .and_then(|value| value.as_str()),
            Some("iroh_direct")
        );
    }

    service_a
        .send_peer_direct_message(&remote_b, None, json!({"text":"hello from a"}))
        .expect("send direct message");

    let delivered = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..64 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        let messages_a = dm_messages_for(&dir_a, &thread_id);
        let messages_b = dm_messages_for(&dir_b, &thread_id);
        let outbound_delivered = messages_a.iter().any(|message| {
            message.message_kind == PeerDmMessageKind::Message
                && message.direction == wattswarm_control_plane::control::PeerDmDirection::Outbound
                && message.delivery_state == PeerDmDeliveryState::Delivered
                && message.content == json!({"text":"hello from a"})
        });
        let inbound_delivered = messages_b.iter().any(|message| {
            message.message_kind == PeerDmMessageKind::Message
                && message.direction == wattswarm_control_plane::control::PeerDmDirection::Inbound
                && message.delivery_state == PeerDmDeliveryState::Delivered
                && message.content == json!({"text":"hello from a"})
        });
        outbound_delivered && inbound_delivered
    });
    assert!(delivered, "accepted peers should exchange direct messages");

    let messages_a = dm_messages_for(&dir_a, &thread_id);
    let messages_b = dm_messages_for(&dir_b, &thread_id);
    let outbound_message = messages_a
        .iter()
        .find(|message| {
            message.message_kind == PeerDmMessageKind::Message
                && message.direction == wattswarm_control_plane::control::PeerDmDirection::Outbound
                && message.content == json!({"text":"hello from a"})
        })
        .expect("outbound dm message");
    let sender_statuses =
        data_plane_statuses_for(&dir_a, "dm_message", &outbound_message.message_id);
    let receiver_statuses =
        data_plane_statuses_for(&dir_b, "dm_message", &outbound_message.message_id);
    let expected_dm_digest = format!(
        "sha256:{}",
        sha256_hex(&serde_json::to_vec(&json!({"text":"hello from a"})).expect("dm bytes"))
    );
    assert!(
        content_artifact_exists(&dir_a, ArtifactKind::DirectMessage, &expected_dm_digest),
        "sender should keep local direct-message artifact"
    );
    assert!(
        content_artifact_exists(&dir_b, ArtifactKind::DirectMessage, &expected_dm_digest),
        "receiver should fetch direct-message artifact over iroh"
    );
    assert!(
        messages_a
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::RelationshipEstablished),
        "local side should persist relationship established confirmation"
    );
    assert!(
        messages_a
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::SessionInit),
        "local side should persist session init"
    );
    assert!(
        messages_b
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::RelationshipEstablished),
        "remote side should persist relationship established confirmation"
    );
    assert!(
        messages_b
            .iter()
            .any(|message| message.message_kind == PeerDmMessageKind::SessionInit),
        "remote side should persist session init"
    );
    assert_eq!(
        sender_statuses.len(),
        1,
        "sender should persist one latest dm data-plane status row"
    );
    assert_eq!(
        receiver_statuses.len(),
        1,
        "receiver should persist one latest dm data-plane status row"
    );
    assert!(
        sender_statuses[0].route == "iroh_control"
            && sender_statuses[0].status == "control_acknowledged",
        "sender should persist the final dm control-plane acknowledgement status"
    );
    assert!(
        receiver_statuses[0].route == "iroh_direct"
            && receiver_statuses[0].status == "content_hydrated",
        "receiver should persist the final dm content hydration status"
    );

    service_b
        .send_peer_relationship_action(&remote_a, PeerRelationshipAction::Block, None)
        .expect("send relationship block");
    let blocked = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..64 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        relationship_state_for(&dir_a, &remote_b) == Some(PeerRelationshipState::Blocked)
            && relationship_state_for(&dir_b, &remote_a) == Some(PeerRelationshipState::Blocked)
    });
    assert!(blocked, "block should sync to both peers");

    let blocked_send =
        service_a.send_peer_direct_message(&remote_b, None, json!({"text":"post-block"}));
    assert!(
        blocked_send.is_err(),
        "blocked peers should no longer be able to send direct messages"
    );

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}

pub fn two_nodes_sync_topic_message_content_over_iroh() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let scope = SwarmScope::Group("crew-7".to_owned());
    let dir_a = temp_test_dir("topic-content-a");
    let dir_b = temp_test_dir("topic-content-b");
    let mut service_a = make_service_with_config_and_state_dir(
        std::slice::from_ref(&scope),
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        std::slice::from_ref(&scope),
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );
    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();
    let contact_ready = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        contact_material_for(&dir_a, &remote_b).is_some()
            && contact_material_for(&dir_b, &remote_a).is_some()
    });
    assert!(
        contact_ready,
        "connected peers should exchange contact material before topic content sync"
    );

    let content = json!({
        "text": "hello crew from iroh topic sync",
        "kind": "topic.message"
    });
    let event = emit_topic_message_with_content(
        &mut node_a,
        &dir_a,
        "default",
        "crew.chat",
        "group:crew-7",
        content.clone(),
        None,
        100,
    )
    .expect("emit topic message with content");
    let expected_digest = match &event.payload {
        wattswarm_control_plane::types::EventPayload::TopicMessagePosted(payload) => {
            payload.content_ref.digest.clone()
        }
        other => panic!("expected topic message payload, got {other:?}"),
    };
    let mut last_published_seq = 0;

    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish scoped updates");
        for _ in 0..64 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        node_b
            .store
            .list_topic_messages("default", "crew.chat", "group:crew-7", 10)
            .expect("list topic messages")
            .into_iter()
            .any(|message| {
                message.message_id == event.event_id
                    && message.content_ref.digest == expected_digest
                    && message.content == content
            })
    });
    assert!(
        synced,
        "remote peer should fetch topic message content over iroh using content_ref"
    );
    assert!(
        content_artifact_exists(&dir_a, ArtifactKind::TopicMessage, &expected_digest),
        "publisher should keep local topic-message artifact"
    );
    assert!(
        content_artifact_exists(&dir_b, ArtifactKind::TopicMessage, &expected_digest),
        "subscriber should fetch topic-message artifact over iroh"
    );
    let subscriber_statuses = data_plane_statuses_for(&dir_b, "topic_message", &event.event_id);
    assert_eq!(
        subscriber_statuses.len(),
        1,
        "subscriber should persist one latest topic data-plane status row"
    );
    assert!(
        subscriber_statuses[0].route == "iroh_direct"
            && subscriber_statuses[0].status == "content_hydrated",
        "subscriber should persist the final topic content hydration status"
    );

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}

pub fn global_task_detail_sync_excludes_process_firehose() {
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
    let mut contract = sample_contract("task-global-process-filter", policy_hash);
    contract.inputs = json!({"prompt":"sync task without process firehose"});
    node_a.submit_task(contract, 1, 100).expect("submit task");
    node_a
        .claim_task(
            "task-global-process-filter",
            wattswarm_control_plane::types::ClaimRole::Propose,
            "exec-filter-1",
            500,
            1,
            101,
        )
        .expect("claim task");

    let mut last_published_seq = 0;
    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        last_published_seq = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        node_b
            .task_view("task-global-process-filter")
            .expect("task view")
            .is_some()
    });
    assert!(synced, "remote node should receive task detail layer");
    assert!(
        node_b
            .store
            .get_lease("task-global-process-filter", "propose")
            .expect("load lease")
            .is_none(),
        "global dissemination should not replay task process traffic"
    );
}

pub fn two_nodes_backfill_missing_events_over_request_response() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-backfill", policy_hash);
    contract.inputs = json!({"prompt":"recover me via backfill"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
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
            return true;
        }
        false
    });
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

pub fn connected_peer_helper_syncs_over_lan_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();
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

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

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
    let mut last_tick_a = None;
    let mut last_tick_b = None;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish discovered peer event");
        last_tick_a = pump_once(&mut service_a, &mut node_a);
        last_tick_b = pump_once(&mut service_b, &mut node_b);
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

    assert!(
        synced,
        "connected peers should sync local event; last_published_seq={last_published_seq} node_b_events={} last_tick_a={last_tick_a:?} last_tick_b={last_tick_b:?}",
        node_b
            .store
            .load_all_events()
            .expect("load node b events")
            .len()
    );
}

pub fn region_scoped_backfill_only_reaches_region_subscribers() {
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

pub fn node_scoped_live_sync_only_reaches_matching_node_scope() {
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

pub fn group_scoped_live_sync_only_reaches_matching_group_scope() {
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
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())]);
    let mut service_b =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())]);
    let mut service_c =
        make_service_with_scopes(&[SwarmScope::Global, SwarmScope::Group("crew-8".to_owned())]);

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-group-live", policy_hash);
    contract.task_type = "group:crew-7:swarm".to_owned();
    contract.inputs = json!({
        "prompt":"group-scoped sync",
        "swarm_scope":"group:crew-7",
        "swarm_route":{
            "group_id":"crew-7",
            "target_node_ids":[node_a.node_id(), node_b.node_id()],
            "relation_tags":["crew"],
            "forward_budget":1
        }
    });
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut group_synced = false;
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
            .task_view("task-group-live")
            .expect("task view")
            .is_some()
        {
            group_synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(group_synced);
    assert!(
        node_c
            .task_view("task-group-live")
            .expect("task view")
            .is_none()
    );
}

pub fn remote_subscription_turns_middle_peer_into_group_relay() {
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
    let mut service_a = make_service_with_scopes(&[SwarmScope::Global]);
    let mut service_b = make_service_with_scopes(&[SwarmScope::Global]);
    let mut service_c = make_service_with_scopes(&[SwarmScope::Global]);
    let group_scope = SwarmScope::Group("crew-7".to_owned());

    connect_services(&mut service_a, &mut node_a, &mut service_c, &mut node_c);
    connect_services(&mut service_b, &mut node_b, &mut service_c, &mut node_c);

    node_a
        .emit_at(
            1,
            EventPayload::FeedSubscriptionUpdated(FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: node_a.node_id(),
                feed_key: "market.crew-7".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                active: true,
            }),
            100,
        )
        .expect("node a subscription");

    let mut last_a = 0;
    for iteration in 0..4_096 {
        last_a = publish_pending_scoped_updates(&mut service_a, &node_a, &node_a.node_id(), last_a)
            .expect("publish node a subscription");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if last_a > 0 && iteration > 512 {
            break;
        }
        std::thread::yield_now();
    }

    service_b
        .subscribe_scope_kinds(&group_scope, &[GossipKind::Events])
        .expect("node b joins group events");
    let policy_hash = node_b
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-group-relayed", policy_hash);
    contract.task_type = "group:crew-7:swarm".to_owned();
    contract.inputs = json!({"prompt":"relayed group sync", "swarm_scope":"group:crew-7"});
    node_b.submit_task(contract, 1, 110).expect("submit task");

    let mut last_b = 0;
    let mut relayed_to_a = false;
    for _ in 0..4_096 {
        last_b = publish_pending_scoped_updates(&mut service_b, &node_b, &node_b.node_id(), last_b)
            .expect("publish node b task");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        let _ = pump_once(&mut service_c, &mut node_c);
        if node_a
            .task_view("task-group-relayed")
            .expect("task view")
            .is_some()
        {
            relayed_to_a = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(
        relayed_to_a,
        "A and B are not directly connected, so the group event must relay through C"
    );
}

pub fn subnet_nodes_sync_and_mainnet_overlay_stays_isolated() {
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
    let mut subnet_a = make_service_for_network("subnet:alpha");
    let mut subnet_b = make_service_for_network("subnet:alpha");
    let mut mainnet_c = make_service_for_network("mainnet:watt-galaxy");

    connect_services(&mut subnet_a, &mut node_a, &mut subnet_b, &mut node_b);
    wait_for_listen_addrs(&mut mainnet_c, &mut node_c);
    subnet_a
        .dial(iroh_peer_addr(&mainnet_c))
        .expect("dial mainnet overlay");

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-subnet-overlay", policy_hash);
    contract.inputs = json!({"prompt":"subnet only"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let subnet_synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        last_published_seq = publish_pending_global_events(
            &mut subnet_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending subnet events");
        let _ = pump_once(&mut subnet_a, &mut node_a);
        let _ = pump_once(&mut subnet_b, &mut node_b);
        let _ = pump_once(&mut mainnet_c, &mut node_c);
        node_b
            .task_view("task-subnet-overlay")
            .expect("subnet task view")
            .is_some()
    });
    assert!(subnet_synced, "subnet peer should receive subnet task");
    assert!(
        node_c
            .task_view("task-subnet-overlay")
            .expect("mainnet task view")
            .is_none(),
        "mainnet overlay must not receive subnet task"
    );
}

pub fn summary_gossip_imports_knowledge_and_reputation_into_remote_store() {
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

pub fn revoked_event_propagates_and_removes_remote_projection_state() {
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

pub fn revoked_summary_event_removes_remote_imported_state() {
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

pub fn anti_entropy_syncs_missed_event_without_live_publish() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_fast_service();
    let bootstrap_peer = bootstrap_peer_addr(&mut service_a, &mut node_a);
    let mut service_b = make_bootstrap_service_with_params(
        &[SwarmScope::Global],
        &test_protocol_params(),
        &[bootstrap_peer],
    );

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-anti-entropy", policy_hash);
    contract.inputs = json!({"prompt":"catch me via anti entropy"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    // Drain the initial empty backfill round-trip from connect before the
    // recovery loop begins.
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(1)),
    );

    let synced = drive_anti_entropy_until(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(30)),
        |node_b| {
            node_b
                .task_view("task-anti-entropy")
                .expect("task view")
                .is_some()
        },
    );

    assert!(synced);
}

pub fn anti_entropy_uses_scope_specific_cursor_for_recovery() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let scopes = [SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())];
    let mut service_a = make_fast_service_with_scopes(&scopes);
    let bootstrap_peer = bootstrap_peer_addr(&mut service_a, &mut node_a);
    let mut service_b =
        make_bootstrap_service_with_params(&scopes, &test_protocol_params(), &[bootstrap_peer]);

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

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

    // Drain the initial empty backfill round-trip from connect before the
    // recovery loop begins.
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(1)),
    );

    let recovered = drive_anti_entropy_until(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        scaled_timeout(Duration::from_secs(30)),
        |node_b| {
            node_b
                .task_view("task-region-cursor")
                .expect("task view")
                .is_some()
        },
    );

    assert!(recovered);
}

pub fn reconnect_recovers_missing_events_after_partition_like_disconnect() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let bootstrap_peer = bootstrap_peer_addr(&mut service_a, &mut node_a);
    let mut service_b = make_bootstrap_service_with_params(
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
        &[bootstrap_peer.clone()],
    );

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

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

    // Drain inflight messages before disconnect to avoid iroh
    // request-response debug assertion on stale connection state.
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

    let mut service_b = make_bootstrap_service_with_params(
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
        &[bootstrap_peer],
    );
    wait_for_connected_pair(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    // Recovery relies on the initial backfill triggered by ConnectionEstablished,
    // not on anti-entropy.  Just pump until the backfill round-trip completes.
    let recovered = wait_until(scaled_timeout(Duration::from_secs(30)), || {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        node_b
            .task_view("task-partition-second")
            .expect("task view second")
            .is_some()
    });

    assert!(recovered);
}
