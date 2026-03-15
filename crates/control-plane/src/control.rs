use crate::crypto::{NodeIdentity, candidate_hash, vote_commit_hash};
use crate::node::{Node, finality_sign};
use crate::runtime::{HttpRuntimeClient, RuntimeClient};
use crate::storage::PgStore;
use crate::task_template::sample_contract;
use crate::types::{
    ClaimRole, FinalityProof, Membership, Role, TaskContract, VoteChoice, VoteCommitPayload,
    VoteRevealPayload,
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use wattswarm_artifact_store::{
    ArtifactAvailabilityManifest, ArtifactAvailabilityStatus, ArtifactKind, ArtifactStore,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub running: bool,
    #[serde(default = "default_node_mode")]
    pub mode: NodeMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    Local,
    Lan,
    Network,
}

fn default_node_mode() -> NodeMode {
    NodeMode::Local
}

impl Default for NodeMode {
    fn default() -> Self {
        Self::Local
    }
}

impl NodeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Lan => "lan",
            Self::Network => "network",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "lan" => Ok(Self::Lan),
            "network" => Ok(Self::Network),
            other => Err(anyhow!(
                "unsupported node mode '{other}'; expected one of: local, lan, network"
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorRegistryEntry {
    pub name: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutorRegistry {
    pub entries: Vec<ExecutorRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DiscoveredPeersRegistry {
    pub peers: Vec<DiscoveredPeerRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DiscoveredPeerRecord {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RealTaskRunRequest {
    pub executor: String,
    pub profile: String,
    pub task_id: Option<String>,
    pub task_file: Option<PathBuf>,
    pub task_contract: Option<TaskContract>,
}

pub fn executor_registry_path(state_dir: &Path) -> PathBuf {
    state_dir.join("executors.json")
}

pub fn node_state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("node_state.json")
}

pub fn discovered_peers_path(state_dir: &Path) -> PathBuf {
    state_dir.join("discovered_peers.json")
}

pub fn artifact_store_path(state_dir: &Path) -> PathBuf {
    state_dir.join("artifacts")
}

fn open_local_artifact_store(state_dir: &Path) -> Result<ArtifactStore> {
    let store = ArtifactStore::new(artifact_store_path(state_dir));
    store.ensure_layout()?;
    Ok(store)
}

fn default_artifact_retry_after_ms() -> u64 {
    30_000
}

fn availability_manifest(
    kind: ArtifactKind,
    artifact_id: &str,
    scope: Option<&str>,
    source_uri: Option<&str>,
    expected_digest: Option<&str>,
    mime: Option<&str>,
    size_bytes: Option<u64>,
    local_path: Option<&Path>,
    status: ArtifactAvailabilityStatus,
    observed_at: u64,
    repair_attempts: u32,
    next_retry_at: Option<u64>,
    last_error: Option<String>,
) -> ArtifactAvailabilityManifest {
    ArtifactAvailabilityManifest {
        artifact_kind: kind,
        artifact_id: artifact_id.to_owned(),
        scope: scope.map(ToOwned::to_owned),
        source_uri: source_uri.map(ToOwned::to_owned),
        expected_digest: expected_digest.map(ToOwned::to_owned),
        mime: mime.map(ToOwned::to_owned),
        size_bytes,
        local_path: local_path.map(|path| path.display().to_string()),
        status,
        observed_at,
        last_checked_at: Some(observed_at),
        missing_since: (status == ArtifactAvailabilityStatus::Missing).then_some(observed_at),
        repair_attempts,
        next_retry_at,
        last_error,
    }
}

fn load_task_detail_reference(node: &Node, task_id: &str) -> Result<crate::types::ArtifactRef> {
    let detail = node
        .store
        .get_task_announcement_detail_for_task(task_id)?
        .ok_or_else(|| anyhow!("task announcement missing for task {task_id}"))?;
    detail
        .detail_ref()
        .cloned()
        .ok_or_else(|| anyhow!("task announcement detail_ref missing for task {task_id}"))
}

fn load_evidence_reference(
    node: &Node,
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
) -> Result<crate::types::ArtifactRef> {
    if let Some(reference) =
        node.store
            .get_evidence_reference(task_id, candidate_id, evidence_digest)?
    {
        return Ok(reference);
    }
    let candidate = node
        .store
        .get_candidate_by_id(task_id, candidate_id)?
        .ok_or_else(|| anyhow!("candidate missing for evidence {candidate_id}"))?;
    candidate
        .evidence_refs
        .into_iter()
        .find(|reference| reference.digest == evidence_digest)
        .ok_or_else(|| {
            anyhow!("evidence ref {evidence_digest} missing for candidate {candidate_id}")
        })
}

fn write_missing_manifest(
    artifact_store: &ArtifactStore,
    kind: ArtifactKind,
    artifact_id: &str,
    scope: Option<&str>,
    source_uri: Option<&str>,
    expected_digest: Option<&str>,
    mime: Option<&str>,
    size_bytes: Option<u64>,
    observed_at: u64,
    error: anyhow::Error,
) -> Result<()> {
    let repair_attempts = artifact_store
        .read_availability_manifest(kind, artifact_id, scope)?
        .map(|manifest| manifest.repair_attempts.saturating_add(1))
        .unwrap_or(1);
    artifact_store.write_availability_manifest(&availability_manifest(
        kind,
        artifact_id,
        scope,
        source_uri,
        expected_digest,
        mime,
        size_bytes,
        None,
        ArtifactAvailabilityStatus::Missing,
        observed_at,
        repair_attempts,
        Some(observed_at.saturating_add(default_artifact_retry_after_ms())),
        Some(error.to_string()),
    ))?;
    Ok(())
}

fn materialize_reference_artifact(
    state_dir: &Path,
    kind: ArtifactKind,
    artifact_id: &str,
    source_uri: &str,
    expected_digest: &str,
    mime: &str,
    size_bytes: u64,
    bytes: &[u8],
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.write_validated_bytes(
        kind,
        artifact_id,
        None,
        bytes,
        Some(expected_digest),
        Some(size_bytes),
    )?;
    let manifest = availability_manifest(
        kind,
        artifact_id,
        None,
        Some(source_uri),
        Some(expected_digest),
        Some(mime),
        Some(size_bytes),
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(manifest)
}

fn fetch_reference_artifact(
    state_dir: &Path,
    kind: ArtifactKind,
    artifact_id: &str,
    source_uri: &str,
    expected_digest: &str,
    mime: &str,
    size_bytes: u64,
    observed_at: u64,
) -> Result<Vec<u8>> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    match artifact_store.read_validated_bytes(
        kind,
        artifact_id,
        None,
        Some(expected_digest),
        Some(size_bytes),
    ) {
        Ok(bytes) => {
            let path = match kind {
                ArtifactKind::Reference => artifact_store.reference_path(artifact_id)?,
                ArtifactKind::Evidence => artifact_store.evidence_path(artifact_id)?,
                _ => unreachable!("reference artifact kind"),
            };
            artifact_store.write_availability_manifest(&availability_manifest(
                kind,
                artifact_id,
                None,
                Some(source_uri),
                Some(expected_digest),
                Some(mime),
                Some(size_bytes),
                Some(&path),
                ArtifactAvailabilityStatus::Available,
                observed_at,
                artifact_store
                    .read_availability_manifest(kind, artifact_id, None)?
                    .map(|manifest| manifest.repair_attempts)
                    .unwrap_or(0),
                None,
                None,
            ))?;
            Ok(bytes)
        }
        Err(err) => {
            write_missing_manifest(
                &artifact_store,
                kind,
                artifact_id,
                None,
                Some(source_uri),
                Some(expected_digest),
                Some(mime),
                Some(size_bytes),
                observed_at,
                err.context("fetch reference artifact"),
            )?;
            Err(anyhow!("artifact {} is not locally available", artifact_id))
        }
    }
}

pub fn materialize_task_detail_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    bytes: &[u8],
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let reference = load_task_detail_reference(node, task_id)?;
    materialize_reference_artifact(
        state_dir,
        ArtifactKind::Reference,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        bytes,
        observed_at,
    )
}

pub fn fetch_task_detail_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    observed_at: u64,
) -> Result<Vec<u8>> {
    let reference = load_task_detail_reference(node, task_id)?;
    fetch_reference_artifact(
        state_dir,
        ArtifactKind::Reference,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        observed_at,
    )
}

pub fn materialize_evidence_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
    bytes: &[u8],
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let reference = load_evidence_reference(node, task_id, candidate_id, evidence_digest)?;
    materialize_reference_artifact(
        state_dir,
        ArtifactKind::Evidence,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        bytes,
        observed_at,
    )
}

pub fn fetch_evidence_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
    observed_at: u64,
) -> Result<Vec<u8>> {
    let reference = load_evidence_reference(node, task_id, candidate_id, evidence_digest)?;
    fetch_reference_artifact(
        state_dir,
        ArtifactKind::Evidence,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        observed_at,
    )
}

pub fn materialize_checkpoint_artifact_json<T: Serialize>(
    state_dir: &Path,
    node: &Node,
    scope_key: &str,
    checkpoint_id: &str,
    value: &T,
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let checkpoint = node
        .store
        .get_checkpoint_announcement(scope_key, checkpoint_id)?
        .ok_or_else(|| {
            anyhow!("checkpoint announcement missing for {scope_key}/{checkpoint_id}")
        })?;
    let path = artifact_store.checkpoint_path(checkpoint_id)?;
    artifact_store.write_json(&path, value)?;
    let manifest = availability_manifest(
        ArtifactKind::Checkpoint,
        checkpoint_id,
        Some(scope_key),
        Some(&checkpoint.artifact_path),
        None,
        Some("application/json"),
        None,
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(manifest)
}

pub fn fetch_checkpoint_artifact_json<T: DeserializeOwned>(
    state_dir: &Path,
    node: &Node,
    scope_key: &str,
    checkpoint_id: &str,
    observed_at: u64,
) -> Result<T> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let checkpoint = node
        .store
        .get_checkpoint_announcement(scope_key, checkpoint_id)?
        .ok_or_else(|| {
            anyhow!("checkpoint announcement missing for {scope_key}/{checkpoint_id}")
        })?;
    let path = artifact_store.checkpoint_path(checkpoint_id)?;
    match artifact_store.read_json::<T>(&path) {
        Ok(value) => {
            artifact_store.write_availability_manifest(&availability_manifest(
                ArtifactKind::Checkpoint,
                checkpoint_id,
                Some(scope_key),
                Some(&checkpoint.artifact_path),
                None,
                Some("application/json"),
                None,
                Some(&path),
                ArtifactAvailabilityStatus::Available,
                observed_at,
                artifact_store
                    .read_availability_manifest(
                        ArtifactKind::Checkpoint,
                        checkpoint_id,
                        Some(scope_key),
                    )?
                    .map(|manifest| manifest.repair_attempts)
                    .unwrap_or(0),
                None,
                None,
            ))?;
            Ok(value)
        }
        Err(err) => {
            write_missing_manifest(
                &artifact_store,
                ArtifactKind::Checkpoint,
                checkpoint_id,
                Some(scope_key),
                Some(&checkpoint.artifact_path),
                None,
                Some("application/json"),
                None,
                observed_at,
                err.context("fetch checkpoint artifact"),
            )?;
            Err(anyhow!(
                "checkpoint artifact {checkpoint_id} is not locally available"
            ))
        }
    }
}

pub fn materialize_snapshot_artifact_json<T: Serialize>(
    state_dir: &Path,
    scope_key: &str,
    snapshot_id: &str,
    source_uri: Option<&str>,
    value: &T,
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.snapshot_path(scope_key, snapshot_id)?;
    artifact_store.write_json(&path, value)?;
    let manifest = availability_manifest(
        ArtifactKind::Snapshot,
        snapshot_id,
        Some(scope_key),
        source_uri,
        None,
        Some("application/json"),
        None,
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(manifest)
}

pub fn fetch_snapshot_artifact_json<T: DeserializeOwned>(
    state_dir: &Path,
    scope_key: &str,
    snapshot_id: &str,
    observed_at: u64,
) -> Result<T> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.snapshot_path(scope_key, snapshot_id)?;
    match artifact_store.read_json::<T>(&path) {
        Ok(value) => {
            artifact_store.write_availability_manifest(&availability_manifest(
                ArtifactKind::Snapshot,
                snapshot_id,
                Some(scope_key),
                None,
                None,
                Some("application/json"),
                None,
                Some(&path),
                ArtifactAvailabilityStatus::Available,
                observed_at,
                artifact_store
                    .read_availability_manifest(
                        ArtifactKind::Snapshot,
                        snapshot_id,
                        Some(scope_key),
                    )?
                    .map(|manifest| manifest.repair_attempts)
                    .unwrap_or(0),
                None,
                None,
            ))?;
            Ok(value)
        }
        Err(err) => {
            write_missing_manifest(
                &artifact_store,
                ArtifactKind::Snapshot,
                snapshot_id,
                Some(scope_key),
                None,
                None,
                Some("application/json"),
                None,
                observed_at,
                err.context("fetch snapshot artifact"),
            )?;
            Err(anyhow!(
                "snapshot artifact {snapshot_id} is not locally available"
            ))
        }
    }
}

pub fn list_artifacts_needing_repair(
    state_dir: &Path,
    now_ms: u64,
) -> Result<Vec<ArtifactAvailabilityManifest>> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    artifact_store.list_manifests_needing_repair(now_ms)
}

pub fn local_node_id(state_dir: &Path) -> Result<String> {
    Ok(load_or_create_identity(&state_dir.join("node_seed.hex"))?.node_id())
}

pub fn resolve_node_mode(state_dir: &Path) -> Result<NodeMode> {
    let state_path = node_state_path(state_dir);
    if state_path.exists() {
        let state: NodeState = serde_json::from_slice(&fs::read(&state_path)?)?;
        return Ok(state.mode);
    }
    match env::var("WATTSWARM_NODE_MODE") {
        Ok(value) => NodeMode::parse(&value),
        Err(_) => Ok(NodeMode::Local),
    }
}

pub fn write_node_state(state_dir: &Path, running: bool, mode: NodeMode) -> Result<()> {
    fs::create_dir_all(state_dir)?;
    fs::write(
        node_state_path(state_dir),
        serde_json::to_vec_pretty(&NodeState { running, mode })?,
    )?;
    Ok(())
}

pub fn open_node(state_dir: &Path, db_path: &Path) -> Result<Node> {
    let mode = resolve_node_mode(state_dir)?;
    open_node_in_mode(state_dir, db_path, mode)
}

fn open_node_for_topology(
    state_dir: &Path,
    topology: crate::types::NetworkTopology,
    identity: NodeIdentity,
    store: PgStore,
) -> Result<Node> {
    let _ = open_local_artifact_store(state_dir)?;
    store
        .ensure_bootstrap_signed_network_protocol_params(&topology.network.network_id, &identity)?;
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&identity.node_id(), role);
    }

    let self_node_id = identity.node_id();
    let bound_store = store.for_org(&topology.org.org_id);
    bound_store
        .load_verified_network_protocol_params()
        .with_context(|| {
            format!(
                "load verified network params for org {}",
                topology.org.org_id
            )
        })?;
    let mut node = Node::new(identity, bound_store, membership)?;
    let replay_on_open = env::var("WATTSWARM_REPLAY_ON_OPEN").ok().is_some_and(|v| {
        let t = v.trim().to_ascii_lowercase();
        t == "1" || t == "true" || t == "yes"
    });
    if replay_on_open {
        node.replay_rebuild_projection()?;
    }
    if let Ok(peers) = load_discovered_peers(&discovered_peers_path(state_dir)) {
        for peer_id in peers {
            if peer_id != self_node_id {
                node.discover_peer(peer_id);
            }
        }
    }
    Ok(node)
}

pub fn open_node_in_mode(state_dir: &Path, db_path: &Path, mode: NodeMode) -> Result<Node> {
    let identity = load_or_create_identity(&state_dir.join("node_seed.hex"))?;
    let self_node_id = identity.node_id();
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let store = PgStore::open(db_path)?;
    let topology = match mode {
        NodeMode::Local => {
            store.ensure_local_bootstrap_network_topology(&self_node_id, &self_node_id, now)?
        }
        NodeMode::Lan => {
            store.ensure_lan_bootstrap_network_topology(&self_node_id, &self_node_id, now)?
        }
        NodeMode::Network => store.resolve_network_bootstrap_topology_descriptor(
            &self_node_id,
            &self_node_id,
            now,
        )?,
    };
    open_node_for_topology(state_dir, topology, identity, store)
}

pub fn open_node_on_network_id(state_dir: &Path, db_path: &Path, network_id: &str) -> Result<Node> {
    let identity = load_or_create_identity(&state_dir.join("node_seed.hex"))?;
    let self_node_id = identity.node_id();
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let store = PgStore::open(db_path)?;
    let topology =
        store.join_node_to_network_topology(network_id, &self_node_id, &self_node_id, now)?;
    open_node_for_topology(state_dir, topology, identity, store)
}

pub fn load_discovered_peers(path: &Path) -> Result<Vec<String>> {
    Ok(load_discovered_peer_records(path)?
        .into_iter()
        .map(|record| record.node_id)
        .collect())
}

pub fn load_discovered_peer_records(path: &Path) -> Result<Vec<DiscoveredPeerRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read(path)?;
    if let Ok(reg) = serde_json::from_slice::<DiscoveredPeersRegistry>(&raw) {
        return Ok(reg.peers);
    }
    if let Ok(v) = serde_json::from_slice::<Vec<String>>(&raw) {
        return Ok(v
            .into_iter()
            .map(|node_id| DiscoveredPeerRecord {
                node_id,
                listen_addr: None,
            })
            .collect());
    }
    Err(anyhow!(
        "parse discovered peers registry from {}",
        path.display()
    ))
}

pub fn save_discovered_peers(path: &Path, peers: &[String]) -> Result<()> {
    let records = peers
        .iter()
        .cloned()
        .map(|node_id| DiscoveredPeerRecord {
            node_id,
            listen_addr: None,
        })
        .collect::<Vec<_>>();
    save_discovered_peer_records(path, &records)
}

pub fn save_discovered_peer_records(path: &Path, peers: &[DiscoveredPeerRecord]) -> Result<()> {
    let reg = DiscoveredPeersRegistry {
        peers: peers.to_vec(),
    };
    fs::write(path, serde_json::to_vec_pretty(&reg)?)?;
    Ok(())
}

pub fn add_discovered_peer(state_dir: &Path, peer_node_id: &str) -> Result<bool> {
    add_discovered_peer_endpoint(state_dir, peer_node_id, None)
}

pub fn add_discovered_peer_endpoint(
    state_dir: &Path,
    peer_node_id: &str,
    listen_addr: Option<&str>,
) -> Result<bool> {
    let peer = peer_node_id.trim();
    if peer.is_empty() {
        return Ok(false);
    }
    fs::create_dir_all(state_dir)?;
    let path = discovered_peers_path(state_dir);
    let mut peers = load_discovered_peer_records(&path)?;
    let listen_addr = listen_addr
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    if let Some(existing) = peers.iter_mut().find(|record| record.node_id == peer) {
        if listen_addr.is_some() && existing.listen_addr != listen_addr {
            existing.listen_addr = listen_addr;
            peers.sort();
            save_discovered_peer_records(&path, &peers)?;
            return Ok(true);
        }
        return Ok(false);
    }
    peers.push(DiscoveredPeerRecord {
        node_id: peer.to_owned(),
        listen_addr,
    });
    peers.sort();
    peers.dedup();
    save_discovered_peer_records(&path, &peers)?;
    Ok(true)
}

pub fn load_executor_registry(path: &Path) -> Result<ExecutorRegistry> {
    if !path.exists() {
        return Ok(ExecutorRegistry::default());
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn save_executor_registry(path: &Path, reg: &ExecutorRegistry) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(reg)?)?;
    Ok(())
}

pub fn run_real_task_flow(
    node: &mut Node,
    state_dir: &Path,
    req: RealTaskRunRequest,
) -> Result<Value> {
    let RealTaskRunRequest {
        executor,
        profile,
        task_id,
        task_file,
        task_contract,
    } = req;

    let reg = load_executor_registry(&executor_registry_path(state_dir))?;
    let entry = reg
        .entries
        .iter()
        .find(|e| e.name == executor)
        .ok_or_else(|| anyhow!("executor not found: {executor}"))?;

    let runtime = HttpRuntimeClient::new(entry.base_url.clone());
    runtime.health().with_context(|| {
        format!(
            "runtime /health failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    let capabilities = runtime.capabilities().with_context(|| {
        format!(
            "runtime /capabilities failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    if !capabilities.profiles.iter().any(|p| p == &profile) {
        return Err(anyhow!(
            "profile '{}' not supported by executor '{}'",
            profile,
            executor
        ));
    }

    let mut contract = if let Some(contract) = task_contract {
        contract
    } else if let Some(file) = task_file {
        let raw = fs::read(&file)?;
        serde_json::from_slice::<TaskContract>(&raw)
            .with_context(|| format!("parse task contract from {}", file.display()))?
    } else {
        let real_task_id = task_id
            .clone()
            .unwrap_or_else(|| format!("task-{}", Uuid::new_v4()));
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))?
            .policy_hash;
        sample_contract(&real_task_id, policy_hash)
    };

    if let Some(task_id) = task_id {
        contract.task_id = task_id;
    }

    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    node.submit_task(contract.clone(), 1, now)?;

    let propose_execution_id = format!("exec-p-{}", Uuid::new_v4());
    let verify_execution_id = format!("exec-v-{}", Uuid::new_v4());
    let lease_until = now.saturating_add(contract.assignment.claim.lease_ms);
    node.claim_task(
        &contract.task_id,
        ClaimRole::Propose,
        &propose_execution_id,
        lease_until,
        1,
        now.saturating_add(1),
    )?;
    node.auto_execute_with_runtime(
        &runtime,
        &contract.task_id,
        &profile,
        &propose_execution_id,
        1,
        now.saturating_add(2),
    )?;

    let candidate_id = format!("cand-{propose_execution_id}");
    node.claim_task(
        &contract.task_id,
        ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
        1,
        now.saturating_add(3),
    )?;
    node.auto_verify_candidate_with_runtime(
        &runtime,
        &contract.task_id,
        &candidate_id,
        &verify_execution_id,
        1,
        now.saturating_add(4),
    )?;

    let verifier_result = node
        .store
        .list_verifier_results_for_candidate(&contract.task_id, &candidate_id)?
        .into_iter()
        .find(|row| row.execution_id == verify_execution_id)
        .ok_or_else(|| {
            anyhow!(
                "missing verifier result for execution {}",
                verify_execution_id
            )
        })?;
    if !verifier_result.passed {
        return Err(anyhow!(
            "verifier did not pass candidate; reason_codes={:?}",
            verifier_result.reason_codes
        ));
    }
    let candidate = node
        .store
        .get_candidate_by_id(&contract.task_id, &candidate_id)?
        .ok_or_else(|| anyhow!("candidate not found after execute: {candidate_id}"))?;
    let candidate_hash = candidate_hash(&candidate)?;

    let salt = Uuid::new_v4().to_string();
    let commit_hash = vote_commit_hash(
        VoteChoice::Approve,
        &salt,
        &verifier_result.verifier_result_hash,
    );
    node.submit_vote_commit(
        VoteCommitPayload {
            task_id: contract.task_id.clone(),
            candidate_id: candidate_id.clone(),
            candidate_hash: candidate_hash.clone(),
            execution_id: verify_execution_id.clone(),
            verifier_result_hash: verifier_result.verifier_result_hash.clone(),
            commit_hash,
        },
        1,
        now.saturating_add(5),
    )?;
    node.submit_vote_reveal(
        VoteRevealPayload {
            task_id: contract.task_id.clone(),
            candidate_id: candidate_id.clone(),
            candidate_hash,
            execution_id: verify_execution_id,
            verifier_result_hash: verifier_result.verifier_result_hash.clone(),
            vote: VoteChoice::Approve,
            salt,
        },
        1,
        now.saturating_add(6),
    )?;
    node.commit_decision(&contract.task_id, 1, &candidate_id, now.saturating_add(7))?;
    node.finalize_decision(
        &contract.task_id,
        1,
        &candidate_id,
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(
                &node.identity,
                &contract.task_id,
                1,
                &candidate_id,
            )],
        },
        now.saturating_add(8),
    )?;

    let view = node
        .task_view(&contract.task_id)?
        .ok_or_else(|| anyhow!("task view not found after run"))?;
    let final_decision = candidate
        .output
        .get("decision")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let final_answer = candidate
        .output
        .get("answer")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let evidence_digests = candidate
        .evidence_refs
        .iter()
        .map(|r| r.digest.clone())
        .collect::<Vec<_>>();
    Ok(json!({
        "task_id": contract.task_id,
        "candidate_id": candidate_id,
        "executor": executor,
        "profile": profile,
        "provider_family": capabilities.provider_family,
        "model_id": capabilities.model_id,
        "candidate_output": candidate.output,
        "evidence_digests": evidence_digests,
        "final_decision": final_decision,
        "final_answer": final_answer,
        "terminal_state": format!("{:?}", view.terminal_state),
        "committed_candidate_id": view.committed_candidate_id,
        "finalized_candidate_id": view.finalized_candidate_id
    }))
}

fn load_or_create_identity(seed_file: &Path) -> Result<NodeIdentity> {
    if seed_file.exists() {
        let hex_seed = fs::read_to_string(seed_file)?;
        let bytes = hex::decode(hex_seed.trim())?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("seed must be 32 bytes"))?;
        return Ok(NodeIdentity::from_seed(arr));
    }

    let random_seed: [u8; 32] = rand::random();
    fs::write(seed_file, hex::encode(random_seed))?;
    Ok(NodeIdentity::from_seed(random_seed))
}
