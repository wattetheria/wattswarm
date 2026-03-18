use crate::crypto::{NodeIdentity, candidate_hash, vote_commit_hash};
use crate::node::{Node, finality_sign};
use crate::runtime::{HttpRuntimeClient, RuntimeCapabilities, RuntimeClient};
use crate::storage::PgStore;
use crate::task_template::sample_contract;
use crate::types::{
    ClaimRole, EventPayload, ExecutionIntentDeclaredPayload, ExecutionSetConfirmedPayload,
    ExecutionSetMember, FinalityProof, Membership, Role, TaskContract, VoteChoice,
    VoteCommitPayload, VoteRevealPayload,
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryNetworkInstance {
    pub topology: crate::types::NetworkTopology,
    pub is_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryFeedSource {
    pub feed_key: String,
    pub scope_hint: String,
    pub subscriber_count: u32,
    pub latest_announcement_id: Option<String>,
    pub latest_task_id: Option<String>,
    pub latest_source_node_id: Option<String>,
    pub latest_announced_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirectorySyncEndpoint {
    pub network_id: String,
    pub node_id: String,
    pub listen_addr: String,
    pub source_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkDirectorySnapshot {
    pub current_topology: crate::types::NetworkTopology,
    pub networks: Vec<DirectoryNetworkInstance>,
    pub active_dissemination_domains: Vec<String>,
    pub feeds: Vec<DirectoryFeedSource>,
    pub sync_endpoints: Vec<DirectorySyncEndpoint>,
}

#[derive(Debug, Clone)]
pub struct RealTaskRunRequest {
    pub executor: String,
    pub profile: String,
    pub task_id: Option<String>,
    pub task_file: Option<PathBuf>,
    pub task_contract: Option<TaskContract>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTaskBridgeRequest {
    pub executor: String,
    pub profile: String,
    pub task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RemoteTaskBridgeRegistry {
    pub entries: Vec<RemoteTaskBridgeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteTaskBridgeRecord {
    pub task_id: String,
    pub announcement_id: String,
    pub network_id: String,
    pub source_node_id: String,
    pub source_scope_hint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_ref_digest: Option<String>,
    pub executor: String,
    pub profile: String,
    pub candidate_id: String,
    pub terminal_state: String,
    pub bridged_at: u64,
}

#[derive(Clone)]
struct PreparedRuntime {
    runtime: HttpRuntimeClient,
    capabilities: RuntimeCapabilities,
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

pub fn remote_task_bridge_registry_path(state_dir: &Path) -> PathBuf {
    state_dir.join("remote_task_bridge_registry.json")
}

fn open_local_artifact_store(state_dir: &Path) -> Result<ArtifactStore> {
    let store = ArtifactStore::new(artifact_store_path(state_dir));
    store.ensure_layout()?;
    Ok(store)
}

fn load_remote_task_bridge_registry(path: &Path) -> Result<RemoteTaskBridgeRegistry> {
    if !path.exists() {
        return Ok(RemoteTaskBridgeRegistry::default());
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn save_remote_task_bridge_registry(path: &Path, reg: &RemoteTaskBridgeRegistry) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(reg)?)?;
    Ok(())
}

fn prepare_runtime_for_executor(
    state_dir: &Path,
    executor: &str,
    profile: &str,
) -> Result<PreparedRuntime> {
    let reg = load_executor_registry(&executor_registry_path(state_dir))?;
    let entry = reg
        .entries
        .iter()
        .find(|e| e.name == executor)
        .ok_or_else(|| anyhow!("executor not found: {executor}"))?;

    let runtime = HttpRuntimeClient::new(entry.base_url.clone());
    retry_runtime_probe(|| runtime.health()).with_context(|| {
        format!(
            "runtime /health failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    let capabilities = retry_runtime_probe(|| runtime.capabilities()).with_context(|| {
        format!(
            "runtime /capabilities failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    if !capabilities
        .profiles
        .iter()
        .any(|candidate| candidate == profile)
    {
        return Err(anyhow!(
            "profile '{}' not supported by executor '{}'",
            profile,
            executor
        ));
    }
    Ok(PreparedRuntime {
        runtime,
        capabilities,
    })
}

fn retry_runtime_probe<T, F>(mut op: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    const MAX_ATTEMPTS: usize = 5;
    const BASE_BACKOFF_MS: u64 = 50;

    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_err = Some(err);
                if attempt + 1 < MAX_ATTEMPTS {
                    thread::sleep(Duration::from_millis(
                        BASE_BACKOFF_MS * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("runtime probe failed without an error")))
}

fn default_artifact_retry_after_ms() -> u64 {
    30_000
}

fn observed_at_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

fn current_network_context_id(node: &Node) -> String {
    if node.store.is_org_configured()
        && let Ok(topology) = node
            .store
            .load_network_topology_for_org(node.store.org_id())
    {
        return topology.network.network_id;
    }
    node.store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| "default".to_owned())
}

fn current_network_topology(node: &Node) -> Result<crate::types::NetworkTopology> {
    node.store
        .load_network_topology_for_org(node.store.org_id())
}

pub fn load_network_directory_snapshot(
    node: &Node,
    state_dir: &Path,
    limit: usize,
) -> Result<NetworkDirectorySnapshot> {
    let current_topology = current_network_topology(node)?;
    let current_network_id = current_topology.network.network_id.clone();

    let mut networks = node
        .store
        .list_discoverable_network_topologies(limit)?
        .into_iter()
        .map(|topology| DirectoryNetworkInstance {
            is_current: topology.network.network_id == current_network_id,
            topology,
        })
        .collect::<Vec<_>>();
    networks.sort_by(|left, right| {
        right.is_current.cmp(&left.is_current).then_with(|| {
            left.topology
                .network
                .network_id
                .cmp(&right.topology.network.network_id)
        })
    });

    let active_dissemination_domains = node.store.list_active_dissemination_domains(limit)?;
    let feeds = node
        .store
        .list_discoverable_feed_sources(limit)?
        .into_iter()
        .map(|row| DirectoryFeedSource {
            feed_key: row.feed_key,
            scope_hint: row.scope_hint,
            subscriber_count: row.subscriber_count,
            latest_announcement_id: row.latest_announcement_id,
            latest_task_id: row.latest_task_id,
            latest_source_node_id: row.latest_source_node_id,
            latest_announced_at: row.latest_announced_at,
        })
        .collect::<Vec<_>>();

    let mut sync_endpoints = BTreeMap::<(String, String), DirectorySyncEndpoint>::new();
    for record in load_discovered_peer_records(&discovered_peers_path(state_dir))? {
        let Some(listen_addr) = record.listen_addr else {
            continue;
        };
        sync_endpoints.insert(
            (record.node_id.clone(), listen_addr.clone()),
            DirectorySyncEndpoint {
                network_id: current_network_id.clone(),
                node_id: record.node_id,
                listen_addr,
                source_kind: "udp_discovery".to_owned(),
            },
        );
    }
    let config = crate::network_bridge::network_config_from_env();
    for raw_addr in config.bootstrap_peers {
        let Some((listen_addr, node_id)) = raw_addr.rsplit_once("/p2p/") else {
            continue;
        };
        if listen_addr.trim().is_empty() || node_id.trim().is_empty() {
            continue;
        }
        let node_id = node_id.trim().to_owned();
        let key = (node_id.clone(), raw_addr.clone());
        sync_endpoints
            .entry(key)
            .and_modify(|entry| entry.source_kind = "bootstrap".to_owned())
            .or_insert_with(|| DirectorySyncEndpoint {
                network_id: current_network_id.clone(),
                node_id,
                listen_addr: raw_addr,
                source_kind: "bootstrap".to_owned(),
            });
    }

    Ok(NetworkDirectorySnapshot {
        current_topology,
        networks,
        active_dissemination_domains,
        feeds,
        sync_endpoints: sync_endpoints.into_values().take(limit).collect(),
    })
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

fn run_existing_task_with_runtime(
    node: &mut Node,
    runtime: &dyn RuntimeClient,
    capabilities: &RuntimeCapabilities,
    executor: &str,
    profile: &str,
    task_id: &str,
    now: u64,
) -> Result<Value> {
    let task = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task view missing for {task_id}"))?;
    let propose_execution_id = format!("exec-p-{}", Uuid::new_v4());
    let verify_execution_id = format!("exec-v-{}", Uuid::new_v4());
    let lease_until = now.saturating_add(task.contract.assignment.claim.lease_ms);

    node.claim_task(
        task_id,
        ClaimRole::Propose,
        &propose_execution_id,
        lease_until,
        1,
        now.saturating_add(1),
    )?;
    node.auto_execute_with_runtime(
        runtime,
        task_id,
        profile,
        &propose_execution_id,
        1,
        now.saturating_add(2),
    )?;

    let candidate_id = format!("cand-{propose_execution_id}");
    node.claim_task(
        task_id,
        ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
        1,
        now.saturating_add(3),
    )?;
    node.auto_verify_candidate_with_runtime(
        runtime,
        task_id,
        &candidate_id,
        &verify_execution_id,
        1,
        now.saturating_add(4),
    )?;

    let verifier_result = node
        .store
        .list_verifier_results_for_candidate(task_id, &candidate_id)?
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
        .get_candidate_by_id(task_id, &candidate_id)?
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
            task_id: task_id.to_owned(),
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
            task_id: task_id.to_owned(),
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
    node.commit_decision(task_id, 1, &candidate_id, now.saturating_add(7))?;
    node.finalize_decision(
        task_id,
        1,
        &candidate_id,
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(&node.identity, task_id, 1, &candidate_id)],
        },
        now.saturating_add(8),
    )?;

    let view = node
        .task_view(task_id)?
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
        "task_id": task_id,
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

fn remote_bridge_scope_hint(raw: &str) -> String {
    crate::types::normalized_scope_hint(raw)
}

fn remote_task_bridge_allowed_for_scope(
    node_id: &str,
    scope_hint: &str,
    route: Option<&crate::types::TransportRoute>,
) -> bool {
    match crate::types::ScopeHint::parse_with_prefix_fallback(scope_hint) {
        Some(crate::types::ScopeHint::Node(target)) => target == node_id,
        Some(crate::types::ScopeHint::Group(group_id)) => {
            route.is_some_and(|route| route.matches_group(&group_id) && route.allows_node(node_id))
        }
        Some(crate::types::ScopeHint::Global | crate::types::ScopeHint::Region(_)) => {
            route.is_none_or(|route| route.allows_node(node_id))
        }
        None => scope_hint.trim().is_empty(),
    }
}

fn remote_bridge_execution_set_id(task_id: &str) -> String {
    format!("remote-bridge:{task_id}")
}

fn ensure_remote_execution_participation(
    node: &mut Node,
    task_id: &str,
    network_id: &str,
    scope_hint: &str,
    created_at: u64,
) -> Result<()> {
    let execution_set_id = remote_bridge_execution_set_id(task_id);
    let local_node_id = node.node_id();
    let existing = node
        .store
        .list_execution_set_members(task_id, &execution_set_id)?
        .into_iter()
        .any(|member| member.participant_node_id == local_node_id);
    if existing {
        return Ok(());
    }

    node.emit_at(
        1,
        EventPayload::ExecutionIntentDeclared(ExecutionIntentDeclaredPayload {
            network_id: network_id.to_owned(),
            task_id: task_id.to_owned(),
            execution_set_id: execution_set_id.clone(),
            participant_node_id: local_node_id.clone(),
            role_hint: "executor".to_owned(),
            scope_hint: scope_hint.to_owned(),
            intent: "accepted".to_owned(),
        }),
        created_at,
    )?;
    node.emit_at(
        1,
        EventPayload::ExecutionSetConfirmed(ExecutionSetConfirmedPayload {
            network_id: network_id.to_owned(),
            task_id: task_id.to_owned(),
            execution_set_id,
            confirmed_by_node_id: local_node_id.clone(),
            scope_hint: scope_hint.to_owned(),
            members: vec![ExecutionSetMember {
                participant_node_id: local_node_id,
                role_hint: "executor".to_owned(),
            }],
        }),
        created_at.saturating_add(1),
    )?;
    Ok(())
}

fn bridged_task_contract(node: &Node, state_dir: &Path, task_id: &str) -> Result<TaskContract> {
    if let Some(task) = node.task_view(task_id)? {
        return Ok(task.contract);
    }
    let detail = node
        .store
        .get_task_announcement_detail_for_task(task_id)?
        .ok_or_else(|| anyhow!("task announcement missing for task {task_id}"))?;
    if let Some(contract) = detail.contract {
        return Ok(contract);
    }
    let bytes = fetch_task_detail_artifact(state_dir, node, task_id, observed_at_ms())?;
    let contract = serde_json::from_slice::<TaskContract>(&bytes)
        .with_context(|| format!("parse bridged task detail for {task_id}"))?;
    if contract.task_id != task_id {
        return Err(anyhow!(
            "bridged task detail task_id mismatch: expected {}, got {}",
            task_id,
            contract.task_id
        ));
    }
    Ok(contract)
}

fn bridge_origin_payload(
    task_id: &str,
    deduped: bool,
    record: &RemoteTaskBridgeRecord,
    run: Value,
) -> Value {
    let mut value = run;
    let bridge = json!({
        "deduped": deduped,
        "announcement_id": record.announcement_id,
        "network_id": record.network_id,
        "source_node_id": record.source_node_id,
        "source_scope_hint": record.source_scope_hint,
        "detail_ref_digest": record.detail_ref_digest,
        "bridged_at": record.bridged_at
    });
    if let Some(obj) = value.as_object_mut() {
        obj.insert("task_id".to_owned(), json!(task_id));
        obj.insert("bridge".to_owned(), bridge);
    }
    value
}

pub fn bridge_remote_task_into_local_execution(
    node: &mut Node,
    state_dir: &Path,
    req: RemoteTaskBridgeRequest,
) -> Result<Value> {
    let RemoteTaskBridgeRequest {
        executor,
        profile,
        task_id,
    } = req;
    let detail = node
        .store
        .get_task_announcement_detail_for_task(&task_id)?
        .ok_or_else(|| anyhow!("remote task announcement missing for task {}", task_id))?;
    let announcement = detail.announcement.clone();
    let network_id = current_network_context_id(node);
    let contract = bridged_task_contract(node, state_dir, &task_id)?;
    let route = contract.transport_route();
    let scope_hint = remote_bridge_scope_hint(&announcement.scope_hint);
    if !remote_task_bridge_allowed_for_scope(&node.node_id(), &scope_hint, route.as_ref()) {
        return Err(anyhow!(
            "remote task {} is not eligible for local node {} under scope {}",
            task_id,
            node.node_id(),
            scope_hint
        ));
    }
    let reg_path = remote_task_bridge_registry_path(state_dir);
    let mut registry = load_remote_task_bridge_registry(&reg_path)?;

    if let Some(existing) = registry.entries.iter().find(|entry| {
        entry.task_id == task_id
            && entry.announcement_id == announcement.announcement_id
            && entry.executor == executor
            && entry.profile == profile
    }) {
        return Ok(bridge_origin_payload(
            &task_id,
            true,
            existing,
            json!({
                "task_id": task_id,
                "candidate_id": existing.candidate_id,
                "executor": existing.executor,
                "profile": existing.profile,
                "terminal_state": existing.terminal_state
            }),
        ));
    }

    let prepared = prepare_runtime_for_executor(state_dir, &executor, &profile)?;
    if node.task_view(&task_id)?.is_none() {
        node.submit_task(contract, 1, observed_at_ms())?;
    }
    ensure_remote_execution_participation(
        node,
        &task_id,
        &network_id,
        &scope_hint,
        observed_at_ms().saturating_add(1),
    )?;
    let run = run_existing_task_with_runtime(
        node,
        &prepared.runtime,
        &prepared.capabilities,
        &executor,
        &profile,
        &task_id,
        observed_at_ms().saturating_add(2),
    )?;
    let candidate_id = run
        .get("candidate_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing candidate_id after bridged run"))?
        .to_owned();
    let terminal_state = run
        .get("terminal_state")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing terminal_state after bridged run"))?
        .to_owned();

    let record = RemoteTaskBridgeRecord {
        task_id: task_id.clone(),
        announcement_id: announcement.announcement_id,
        network_id,
        source_node_id: announcement.announced_by_node_id,
        source_scope_hint: scope_hint,
        detail_ref_digest: announcement.detail_ref.map(|reference| reference.digest),
        executor: executor.clone(),
        profile: profile.clone(),
        candidate_id,
        terminal_state,
        bridged_at: observed_at_ms(),
    };
    registry.entries.retain(|entry| {
        !(entry.task_id == record.task_id
            && entry.executor == record.executor
            && entry.profile == record.profile)
    });
    registry.entries.push(record.clone());
    registry.entries.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.executor.cmp(&right.executor))
            .then_with(|| left.profile.cmp(&right.profile))
    });
    save_remote_task_bridge_registry(&reg_path, &registry)?;

    Ok(bridge_origin_payload(&task_id, false, &record, run))
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
    let prepared = prepare_runtime_for_executor(state_dir, &executor, &profile)?;

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
    run_existing_task_with_runtime(
        node,
        &prepared.runtime,
        &prepared.capabilities,
        &executor,
        &profile,
        &contract.task_id,
        now,
    )
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
