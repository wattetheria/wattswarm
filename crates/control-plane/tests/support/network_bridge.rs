#![allow(dead_code, unused_imports)]

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

#[path = "network_bridge/anti_entropy.rs"]
mod anti_entropy;
#[path = "network_bridge/peer_interactions.rs"]
mod peer_interactions;
#[path = "network_bridge/scope_routing.rs"]
mod scope_routing;
#[path = "network_bridge/summary_revoke.rs"]
mod summary_revoke;
#[path = "network_bridge/sync.rs"]
mod sync;
pub use anti_entropy::*;
pub use peer_interactions::*;
pub use scope_routing::*;
pub use summary_revoke::*;
pub use sync::*;

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
