use super::*;
use crate::crypto::{NodeIdentity, candidate_hash, sha256_hex, vote_commit_hash};
use crate::network_p2p::PeerDiscoverySourceKind;
use crate::node::{build_event_for_external, finality_sign, sign_membership_quorum};
use crate::storage::{
    DecisionMemoryHitRow, ImportedTaskOutcomeRow, PgStore, ProjectionScope, ReputationSnapshotRow,
    local_control_scope_id, local_control_store,
};
use crate::types::{
    ClaimRole, FinalityProof, Membership, NetworkKind, Role, VoteChoice, VoteCommitPayload,
    VoteRevealPayload,
};
use crate::{node::Node, task_template::sample_contract};
use serde_json::json;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;
use wattswarm_network_discovery::{DEFAULT_RECORD_TTL_MS, DiscoveryNodeRecordBody};

mod discovery_scope;
mod event_routing;
mod gossip_backfill;
mod governance;
mod network_substrate;
mod peer_sync;
mod subscriptions;
mod summary_observability;

fn env_test_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env_test_mutex() -> std::sync::MutexGuard<'static, ()> {
    env_test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn temp_startup_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wattswarm-network-bridge-{prefix}-{}",
        Uuid::new_v4().simple()
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn random_network_node_id() -> NetworkNodeId {
    let uuid = Uuid::new_v4();
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(uuid.as_bytes());
    bytes[16..].copy_from_slice(uuid.as_bytes());
    crate::network_p2p::network_peer_id_from_ed25519_public_key(
        NodeIdentity::from_seed(bytes).verifying_key().to_bytes(),
    )
    .expect("valid test network node id")
}

fn sample_topic_content_ref(digest: &str, producer: &str) -> crate::types::ArtifactRef {
    crate::types::ArtifactRef {
        uri: format!("artifact://topic-message/{digest}"),
        digest: digest.to_owned(),
        size_bytes: 64,
        mime: "application/json".to_owned(),
        created_at: 10,
        producer: producer.to_owned(),
    }
}

fn read_http_request(stream: &mut std::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut header_end = None;
    loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).expect("read request");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if header_end.is_none()
            && let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
        {
            header_end = Some(position + 4);
            break;
        }
    }

    let header_end = header_end.expect("header terminator");
    let header_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                Some(value.trim().parse::<usize>().expect("content length"))
            } else {
                None
            }
        })
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let mut chunk = vec![0_u8; header_end + content_length - buffer.len()];
        let read = stream.read(&mut chunk).expect("read request body");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(buffer).expect("utf8 request")
}

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

fn mainnet_node(identity: NodeIdentity) -> Node {
    let genesis_node_id = identity.node_id();
    mainnet_node_with_genesis(identity, &genesis_node_id)
}

fn mainnet_node_with_genesis(identity: NodeIdentity, genesis_node_id: &str) -> Node {
    let store = PgStore::open_in_memory().expect("store");
    let topology = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet-auth-test",
            "Mainnet Auth Test",
            genesis_node_id,
            genesis_node_id,
            100,
        )
        .expect("mainnet topology");
    Node::new(
        identity,
        store.for_org(topology.org.org_id),
        Membership::new(),
    )
    .expect("node")
}

struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let prev = env::var(key).ok();
        // SAFETY: unit tests in this module only mutate a small set of env vars locally.
        unsafe {
            if let Some(value) = value {
                env::set_var(key, value);
            } else {
                env::remove_var(key);
            }
        }
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: unit tests in this module only mutate a small set of env vars locally.
        unsafe {
            if let Some(prev) = &self.prev {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }
}

fn signed_discovery_record_for_test(
    state_dir: &Path,
    seed: [u8; 32],
    network_id: &str,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
) -> SignedDiscoveryNodeRecord {
    let now = observed_at_ms();
    signed_discovery_record_for_test_at(
        state_dir,
        seed,
        network_id,
        latitude,
        longitude,
        radius_km,
        now,
        DEFAULT_RECORD_TTL_MS,
    )
}

fn signed_discovery_record_for_test_at(
    state_dir: &Path,
    seed: [u8; 32],
    network_id: &str,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
    updated_at_ms: u64,
    ttl_ms: u64,
) -> SignedDiscoveryNodeRecord {
    let identity = NodeIdentity::from_seed(seed);
    let peer_id = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(state_dir)
        .expect("endpoint id")
        .to_string();
    let contact =
        export_local_contact_material_for_network_peer_id(state_dir, &peer_id, updated_at_ms)
            .expect("contact");
    let mut body = DiscoveryNodeRecordBody::new(
        network_id,
        identity.node_id(),
        identity.node_id(),
        updated_at_ms,
        updated_at_ms,
    );
    body.ttl_ms = ttl_ms;
    body.geo = Some(DiscoveryGeo {
        latitude,
        longitude,
        radius_km,
    });
    body.transport_contact = Some(contact);
    SignedDiscoveryNodeRecord::sign(body, &identity).expect("signed discovery record")
}
