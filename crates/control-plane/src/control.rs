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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub running: bool,
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
    pub peers: Vec<String>,
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

pub fn discovered_peers_path(state_dir: &Path) -> PathBuf {
    state_dir.join("discovered_peers.json")
}

pub fn local_node_id(state_dir: &Path) -> Result<String> {
    Ok(load_or_create_identity(&state_dir.join("node_seed.hex"))?.node_id())
}

pub fn open_node(state_dir: &Path, db_path: &Path) -> Result<Node> {
    let identity = load_or_create_identity(&state_dir.join("node_seed.hex"))?;
    let self_node_id = identity.node_id();
    let mut membership = Membership::new();
    for role in [
        Role::Proposer,
        Role::Verifier,
        Role::Committer,
        Role::Finalizer,
    ] {
        membership.grant(&identity.node_id(), role);
    }

    let mut node = Node::new(identity, PgStore::open(db_path)?, membership)?;
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

pub fn load_discovered_peers(path: &Path) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read(path)?;
    if let Ok(reg) = serde_json::from_slice::<DiscoveredPeersRegistry>(&raw) {
        return Ok(reg.peers);
    }
    if let Ok(v) = serde_json::from_slice::<Vec<String>>(&raw) {
        return Ok(v);
    }
    Err(anyhow!(
        "parse discovered peers registry from {}",
        path.display()
    ))
}

pub fn save_discovered_peers(path: &Path, peers: &[String]) -> Result<()> {
    let reg = DiscoveredPeersRegistry {
        peers: peers.to_vec(),
    };
    fs::write(path, serde_json::to_vec_pretty(&reg)?)?;
    Ok(())
}

pub fn add_discovered_peer(state_dir: &Path, peer_node_id: &str) -> Result<bool> {
    let peer = peer_node_id.trim();
    if peer.is_empty() {
        return Ok(false);
    }
    fs::create_dir_all(state_dir)?;
    let path = discovered_peers_path(state_dir);
    let mut peers = load_discovered_peers(&path)?;
    if peers.iter().any(|p| p == peer) {
        return Ok(false);
    }
    peers.push(peer.to_owned());
    peers.sort();
    peers.dedup();
    save_discovered_peers(&path, &peers)?;
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
    Ok(json!({
        "task_id": contract.task_id,
        "candidate_id": candidate_id,
        "executor": executor,
        "profile": profile,
        "provider_family": capabilities.provider_family,
        "model_id": capabilities.model_id,
        "candidate_output": candidate.output,
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
