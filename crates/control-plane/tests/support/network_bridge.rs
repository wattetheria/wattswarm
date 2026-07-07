#![allow(dead_code, unused_imports)]

use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use wattswarm_artifact_store::{ArtifactKind, ArtifactStore};
use wattswarm_control_plane::control::{
    PRIVATE_DM_FEED_KEY, PeerDmDeliveryState, PeerDmMessageKind, PeerDmSessionState,
    PeerRelationshipAction, PeerRelationshipState, artifact_store_path,
    emit_topic_message_with_content, load_data_plane_status_records_state,
    load_peer_dm_message_records_state, load_peer_dm_thread_records_state,
    load_peer_metadata_records_state, load_peer_relationship_records_state, open_node,
    private_dm_scope_hint, private_dm_thread_id,
};
use wattswarm_control_plane::crypto::{NodeIdentity, sha256_hex};
use wattswarm_control_plane::network_bridge::{
    NetworkBridgeService, NetworkBridgeTick, build_knowledge_summary_for_task_type,
    build_reputation_summary_for_runtime, default_agent_envelope, latest_connected_peer_ids,
    publish_pending_global_events, publish_pending_scoped_updates,
};
use wattswarm_control_plane::network_p2p::{
    GossipKind, NetworkAddress, NetworkP2pConfig, NetworkP2pNode, PeerHandshakeMetadata,
    SwarmScope, TopicNamespace,
};
use wattswarm_control_plane::node::Node;
use wattswarm_control_plane::storage::PgStore;
use wattswarm_control_plane::task_template::sample_contract;
use wattswarm_control_plane::types::{
    CheckpointCreatedPayload, EventPayload, FeedSubscriptionUpdatedPayload, Membership, Role,
};
use wattswarm_network_transport_core::TransportContactMaterial;
use wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id;
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
        listen_addrs: vec!["127.0.0.1:0".to_owned()],
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
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
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
    let bootstrap_peers = bootstrap_peers
        .iter()
        .filter(|peer| peer.contains('@'))
        .cloned()
        .collect::<Vec<_>>();
    make_service_with_config(
        scopes,
        params,
        NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            bootstrap_peers,
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
    let seed: [u8; 32] = rand::random();
    let state_dir = std::env::temp_dir().join(format!("wattswarm-iroh-node-{}", hex::encode(seed)));
    ensure_test_relay_urls(&state_dir);
    unsafe {
        std::env::set_var(
            wattswarm_network_transport_iroh::ENV_IROH_BIND_ADDR,
            "127.0.0.1:0",
        );
        std::env::set_var(
            wattswarm_network_transport_iroh::ENV_IROH_PUBLISH_DIRECT_ADDRS,
            "true",
        );
    }
    let service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(config, state_dir.clone(), seed).expect("network node"),
        scopes,
        params,
    )
    .expect("network service");
    remember_service_state_dir(&service, state_dir);
    service
}

fn make_service_with_config_and_state_dir(
    scopes: &[SwarmScope],
    params: &NetworkProtocolParams,
    config: NetworkP2pConfig,
    state_dir: &Path,
) -> NetworkBridgeService {
    let config = NetworkP2pConfig { ..config }.apply_protocol_params(params);
    let identity = ensure_seeded_test_dir(state_dir);
    ensure_test_relay_urls(state_dir);
    unsafe {
        std::env::set_var(
            wattswarm_network_transport_iroh::ENV_IROH_BIND_ADDR,
            "127.0.0.1:0",
        );
        std::env::set_var(
            wattswarm_network_transport_iroh::ENV_IROH_PUBLISH_DIRECT_ADDRS,
            "true",
        );
    }
    let db_path = state_dir.join("ui.state");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            config,
            state_dir.to_path_buf(),
            identity.secret_bytes(),
        )
        .expect("seeded network node"),
        scopes,
        params,
    )
    .expect("network service");
    remember_service_state_dir(&service, state_dir.to_path_buf());
    service.set_state_dir(state_dir.to_path_buf(), db_path.to_path_buf());
    service
}

fn service_state_dirs() -> &'static Mutex<HashMap<String, PathBuf>> {
    static STATE_DIRS: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();
    STATE_DIRS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn remember_service_state_dir(service: &NetworkBridgeService, state_dir: PathBuf) {
    service_state_dirs()
        .lock()
        .expect("service state dirs")
        .insert(service.local_peer_id().to_string(), state_dir);
}

fn state_dir_for_service(service: &NetworkBridgeService) -> PathBuf {
    service_state_dirs()
        .lock()
        .expect("service state dirs")
        .get(&service.local_peer_id().to_string())
        .cloned()
        .expect("service state dir")
}

fn test_contact_generated_at_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn service_contact_material(
    service: &mut NetworkBridgeService,
    node: &mut Node,
) -> TransportContactMaterial {
    let state_dir = state_dir_for_service(service);
    let peer_id = service.local_peer_id().to_string();
    let ready = wait_until(scaled_timeout(Duration::from_secs(5)), || {
        let _ = pump_once(service, node);
        export_local_contact_material_for_network_peer_id(
            &state_dir,
            &peer_id,
            test_contact_generated_at_ms(),
        )
        .is_ok()
    });
    assert!(ready, "service should expose iroh contact material");
    export_local_contact_material_for_network_peer_id(
        &state_dir,
        &peer_id,
        test_contact_generated_at_ms(),
    )
    .expect("iroh contact material")
}

fn ensure_test_relay_urls(state_dir: &Path) {
    fs::create_dir_all(state_dir).expect("test state dir");
    let path = state_dir.join("startup_config.json");
    let mut value = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .unwrap_or_else(|| json!({}));
    if value.get("relay_urls").is_some() {
        return;
    }
    value["relay_urls"] = json!(["https://relay.example.invalid/"]);
    fs::write(
        path,
        serde_json::to_vec(&value).expect("startup config json"),
    )
    .expect("write startup config relay urls");
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

fn has_private_dm_subscription(
    state_dir: &Path,
    local_node_id: &str,
    remote_node_id: &str,
) -> bool {
    let db_path = state_dir.join("ui.state");
    let Ok(node) = open_node(state_dir, &db_path) else {
        return false;
    };
    node.store
        .load_all_events()
        .unwrap_or_default()
        .into_iter()
        .any(|(_, event)| {
            matches!(
                event.payload,
                EventPayload::FeedSubscriptionUpdated(FeedSubscriptionUpdatedPayload {
                    ref subscriber_node_id,
                    ref feed_key,
                    ref scope_hint,
                    ref gossip_kinds,
                    active: true,
                    ..
                }) if subscriber_node_id == local_node_id
                    && feed_key == PRIVATE_DM_FEED_KEY
                    && scope_hint == &private_dm_scope_hint(local_node_id, remote_node_id)
                    && gossip_kinds.iter().any(|kind| kind == "messages")
            )
        })
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
    let _ = service_contact_material(service, node);
    service
        .listen_addrs()
        .first()
        .map(|addr| format!("{}@{}", service.local_peer_id(), addr))
        .unwrap_or_default()
}

fn connect_services(
    dialer: &mut NetworkBridgeService,
    dialer_node: &mut Node,
    receiver: &mut NetworkBridgeService,
    receiver_node: &mut Node,
) {
    let receiver_contact = service_contact_material(receiver, receiver_node);
    let dialer_contact = service_contact_material(dialer, dialer_node);
    dialer
        .upsert_remote_contact_material(receiver.local_peer_id().to_string(), receiver_contact)
        .expect("dialer remote contact material");
    receiver
        .upsert_remote_contact_material(dialer.local_peer_id().to_string(), dialer_contact)
        .expect("receiver remote contact material");
    dialer
        .run_reconnect_supervision()
        .expect("dialer reconnect supervision");
    receiver
        .run_reconnect_supervision()
        .expect("receiver reconnect supervision");
    wait_for_connected_pair(dialer, dialer_node, receiver, receiver_node);
}

fn relationship_state_for(path: &Path, remote_node_id: &str) -> Option<PeerRelationshipState> {
    load_peer_relationship_records_state(path)
        .expect("load peer relationships")
        .into_iter()
        .find(|record| record.remote_node_id == remote_node_id)
        .map(|record| record.relationship_state)
}

fn relationship_message_for(path: &Path, remote_node_id: &str) -> Option<serde_json::Value> {
    load_peer_relationship_records_state(path)
        .expect("load peer relationships")
        .into_iter()
        .find(|record| record.remote_node_id == remote_node_id)
        .and_then(|record| record.agent_envelope.map(|envelope| envelope.message))
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
