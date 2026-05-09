use crate::control::{
    ExecutorRegistryEntry, NodeMode, NodeState, RealTaskRunRequest, load_executor_registry_state,
    node_state_path, open_node, open_node_in_mode, resolve_node_mode, run_real_task_flow,
    save_executor_registry_state, write_node_state,
};
use crate::run_control;
use crate::run_queue::{RunSubmitSpec, WorkerOptions};
use crate::startup_config::{
    NetworkMode, load_startup_config, save_startup_config, startup_config_path,
};
pub use crate::task_template::{sample_artifact_ref, sample_contract};
use crate::types::{
    AuthoritySet, Event, EventPayload, Membership, MembershipUpdatedPayload, NetworkKind,
    NetworkProtocolParams, SignedNetworkAuthoritySetEnvelope, SignedNetworkProtocolParamsEnvelope,
    TaskContract,
};
use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "wattswarm")]
#[command(about = "WattSwarm coordination kernel CLI")]
pub struct Cli {
    #[command(subcommand)]
    command: RootCommand,

    #[arg(long, default_value = ".wattswarm")]
    state_dir: PathBuf,

    #[arg(long = "store", default_value = "wattswarm.state")]
    store: PathBuf,
}

#[derive(Subcommand, Debug)]
enum RootCommand {
    Node(NodeCommand),
    Peers(PeersCommand),
    Log(LogCommand),
    Executors(ExecutorsCommand),
    Task(TaskCommand),
    Run(RunCommand),
    Knowledge(KnowledgeCommand),
    Governance(GovernanceCommand),
    Ui(UiCommand),
}

#[derive(Subcommand, Debug)]
enum NodeAction {
    Up {
        #[arg(long, default_value = "local")]
        mode: String,
    },
    Down,
    Status,
    ExportContact {
        #[arg(long)]
        json: bool,
    },
    AddBootstrapContact {
        contact: String,
        #[arg(long, default_value = "wan")]
        mode: String,
    },
    BootstrapContacts,
    SignNetworkParams {
        #[arg(long = "network-id")]
        network_id: String,
        #[arg(long = "params-file")]
        params_file: Option<PathBuf>,
        #[arg(long = "pg-url")]
        pg_url: Option<String>,
    },
    UpdateAuthoritySet {
        #[arg(long = "network-id")]
        network_id: String,
        #[arg(long = "authority-set-file")]
        authority_set_file: Option<PathBuf>,
        #[arg(long = "pg-url")]
        pg_url: Option<String>,
    },
}

#[derive(Args, Debug)]
struct NodeCommand {
    #[command(subcommand)]
    action: NodeAction,
}

#[derive(Subcommand, Debug)]
enum PeersAction {
    List,
}

#[derive(Args, Debug)]
struct PeersCommand {
    #[command(subcommand)]
    action: PeersAction,
}

#[derive(Subcommand, Debug)]
enum LogAction {
    Head,
    Replay,
    Verify,
}

#[derive(Args, Debug)]
struct LogCommand {
    #[command(subcommand)]
    action: LogAction,
}

#[derive(Subcommand, Debug)]
enum ExecutorsAction {
    Add {
        name: String,
        base_url: String,
        /// Register as a remote executor (dispatched via network gossip).
        #[arg(long)]
        remote: bool,
        /// For remote executors: target node_id for directed dispatch.
        #[arg(long)]
        target_node: Option<String>,
        /// For remote executors: network scope hint for announcement routing.
        #[arg(long)]
        scope: Option<String>,
    },
    List,
    Check {
        name: String,
    },
}

#[derive(Args, Debug)]
struct ExecutorsCommand {
    #[command(subcommand)]
    action: ExecutorsAction,
}

#[derive(Subcommand, Debug)]
enum TaskAction {
    Submit {
        file: PathBuf,
    },
    Watch {
        task_id: String,
    },
    Decision {
        task_id: String,
    },
    RunReal {
        #[arg(long)]
        executor: String,
        #[arg(long, default_value = "default")]
        profile: String,
        #[arg(long = "task_id", alias = "task-id")]
        task_id: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct TaskCommand {
    #[command(subcommand)]
    action: TaskAction,
}

#[derive(Args, Debug)]
struct RunCommand {
    #[arg(long = "pg-url")]
    pg_url: Option<String>,
    #[command(subcommand)]
    action: RunAction,
}

#[derive(Subcommand, Debug)]
enum RunAction {
    Init,
    Submit {
        file: PathBuf,
        #[arg(long)]
        kickoff: bool,
    },
    Kickoff {
        run_id: String,
    },
    Watch {
        run_id: String,
    },
    Result {
        run_id: String,
    },
    Events {
        run_id: String,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    Cancel {
        run_id: String,
    },
    Retry {
        run_id: String,
    },
    Worker {
        #[arg(long)]
        worker_id: Option<String>,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = 250)]
        poll_ms: u64,
        #[arg(long, default_value_t = 30_000)]
        lease_ms: u64,
        #[arg(long)]
        once: bool,
    },
}

#[derive(Args, Debug)]
struct KnowledgeCommand {
    #[command(subcommand)]
    action: KnowledgeAction,
}

#[derive(Args, Debug)]
struct GovernanceCommand {
    #[command(subcommand)]
    action: GovernanceAction,
}

#[derive(Subcommand, Debug)]
enum GovernanceAction {
    MembershipUpdate {
        file: PathBuf,
        #[arg(long, default_value_t = 1)]
        epoch: u64,
        #[arg(long = "quorum-threshold", default_value_t = 1)]
        quorum_threshold: u32,
    },
    RevokeEvent {
        #[arg(long = "event-id")]
        event_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value_t = 1)]
        epoch: u64,
    },
    RevokeSummary {
        #[arg(long = "summary-id")]
        summary_id: String,
        #[arg(long = "kind")]
        summary_kind: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value_t = 1)]
        epoch: u64,
    },
    PenalizeNode {
        #[arg(long = "node-id")]
        node_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long = "revoke-event")]
        revoked_event_ids: Vec<String>,
        #[arg(long = "revoke-summary")]
        revoked_summary_ids: Vec<String>,
        #[arg(long, default_value_t = 1)]
        epoch: u64,
    },
}

#[derive(Args, Debug)]
struct UiCommand {
    #[arg(long, default_value = "127.0.0.1:7788")]
    listen: String,
}

#[derive(Subcommand, Debug)]
enum KnowledgeAction {
    Export {
        #[arg(long = "task_type", alias = "task-type")]
        task_type: Option<String>,
        #[arg(long = "task_id", alias = "task-id")]
        task_id: Option<String>,
        #[arg(long)]
        out: PathBuf,
    },
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.state_dir)?;

    let store_path = if cli.store.is_absolute() {
        cli.store
    } else {
        cli.state_dir.join(cli.store)
    };

    match cli.command {
        RootCommand::Node(cmd) => handle_node(cmd, &cli.state_dir, &store_path),
        RootCommand::Peers(cmd) => handle_peers(cmd, &cli.state_dir, &store_path),
        RootCommand::Log(cmd) => handle_log(cmd, &cli.state_dir, &store_path),
        RootCommand::Executors(cmd) => handle_executors(cmd, &cli.state_dir),
        RootCommand::Task(cmd) => handle_task(cmd, &cli.state_dir, &store_path),
        RootCommand::Run(cmd) => handle_run(cmd, &cli.state_dir, &store_path),
        RootCommand::Knowledge(cmd) => handle_knowledge(cmd, &cli.state_dir, &store_path),
        RootCommand::Governance(cmd) => handle_governance(cmd, &cli.state_dir, &store_path),
        RootCommand::Ui(cmd) => crate::ui::run(cli.state_dir, store_path, cmd.listen),
    }
}

fn handle_node(cmd: NodeCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let state_path = node_state_path(state_dir);
    match cmd.action {
        NodeAction::Up { mode } => {
            let mode = NodeMode::parse(&mode)?;
            let node = open_node_in_mode(state_dir, db_path, mode)?;
            write_node_state(state_dir, true, mode)?;
            if crate::network_bridge::network_enabled_from_env() {
                crate::udp_announce::announce_startup("node-up", None, Some(&node.node_id()));
            }
            println!("node is up");
        }
        NodeAction::Down => {
            let mode = resolve_node_mode(state_dir)?;
            write_node_state(state_dir, false, mode)?;
            println!("node is down");
        }
        NodeAction::Status => {
            let state: NodeState = if state_path.exists() {
                serde_json::from_slice(&fs::read(&state_path)?)?
            } else {
                NodeState {
                    running: false,
                    mode: NodeMode::Local,
                }
            };
            let node = open_node(state_dir, db_path)?;
            let peers = node
                .store
                .peer_protocol_version_distribution(&node.identity.node_id())?;
            let mut dist = serde_json::Map::new();
            for (version, count) in peers {
                dist.insert(version, serde_json::Value::from(count));
            }
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "running": state.running,
                    "mode": state.mode.as_str(),
                    "local_protocol_version": crate::constants::LOCAL_PROTOCOL_VERSION,
                    "peer_protocol_distribution": dist
                }))?
            );
        }
        NodeAction::ExportContact { json } => {
            let contact = if json {
                crate::network_bridge::export_local_bootstrap_contact_json(state_dir)?
            } else {
                crate::network_bridge::export_local_bootstrap_contact(state_dir)?
            };
            println!("{contact}");
        }
        NodeAction::AddBootstrapContact { contact, mode } => {
            let trimmed = contact.trim();
            if trimmed.is_empty() {
                return Err(anyhow!("bootstrap contact is required"));
            }
            crate::network_bridge::validate_bootstrap_contact(trimmed)
                .context("bootstrap contact must be exported by node export-contact")?;
            let network_mode = parse_bootstrap_contact_mode(&mode)?;
            let path = startup_config_path(state_dir);
            let mut config = load_startup_config(&path)?;
            config.network_mode = network_mode;
            config.bootstrap_contacts.push(trimmed.to_owned());
            save_startup_config(&path, &config)?;
            println!("bootstrap contact added");
        }
        NodeAction::BootstrapContacts => {
            let config = load_startup_config(&startup_config_path(state_dir))?;
            for contact in config.bootstrap_contacts {
                println!("{contact}");
            }
        }
        NodeAction::SignNetworkParams {
            network_id,
            params_file,
            pg_url,
        } => {
            if let Some(pg_url) = pg_url {
                unsafe {
                    std::env::set_var("WATTSWARM_PG_URL", pg_url);
                }
            }
            let identity = load_node_identity_seed(state_dir)?;
            let params = load_network_protocol_params(params_file.as_deref())?;
            let store = crate::storage::storage::PgStore::open(db_path)?;
            let signed = store.put_network_protocol_params(&network_id, &identity, &params)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "network_id": signed.network_id,
                    "version": signed.version,
                    "prev_hash": signed.prev_hash,
                    "params_hash": signed.params_hash,
                    "signed_by": signed.signed_by,
                }))?
            );
        }
        NodeAction::UpdateAuthoritySet {
            network_id,
            authority_set_file,
            pg_url,
        } => {
            if let Some(pg_url) = pg_url {
                unsafe {
                    std::env::set_var("WATTSWARM_PG_URL", pg_url);
                }
            }
            let identity = load_node_identity_seed(state_dir)?;
            let authority_set =
                load_network_authority_set(authority_set_file.as_deref(), &identity)?;
            let store = crate::storage::storage::PgStore::open(db_path)?;
            let signed = store.put_network_authority_set(&network_id, &identity, &authority_set)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "network_id": signed.network_id,
                    "authority_set_id": signed.authority_set.authority_set_id,
                    "prev_hash": signed.prev_hash,
                    "authority_set_hash": signed.authority_set_hash,
                    "signed_by": signed.signed_by,
                }))?
            );
        }
    }
    Ok(())
}

fn load_node_identity_seed(state_dir: &Path) -> Result<crate::crypto::NodeIdentity> {
    let seed_file = state_dir.join("node_seed.hex");
    let hex_seed = fs::read_to_string(&seed_file)
        .with_context(|| format!("read node identity seed from {}", seed_file.display()))?;
    let bytes = decode_hex_seed(hex_seed.trim())?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("seed must be 32 bytes"))?;
    Ok(crate::crypto::NodeIdentity::from_seed(arr))
}

fn decode_hex_seed(raw: &str) -> Result<Vec<u8>> {
    let trimmed = raw.trim();
    if !trimmed.len().is_multiple_of(2) {
        return Err(anyhow!("seed hex must have an even number of characters"));
    }
    (0..trimmed.len())
        .step_by(2)
        .map(|idx| {
            u8::from_str_radix(&trimmed[idx..idx + 2], 16)
                .with_context(|| format!("invalid seed hex at byte {}", idx / 2))
        })
        .collect()
}

fn load_network_protocol_params(path: Option<&Path>) -> Result<NetworkProtocolParams> {
    let Some(path) = path else {
        return Ok(NetworkProtocolParams::default());
    };
    let raw = fs::read(path).with_context(|| format!("read params from {}", path.display()))?;
    serde_json::from_slice::<NetworkProtocolParams>(&raw)
        .or_else(|_| {
            serde_json::from_slice::<SignedNetworkProtocolParamsEnvelope>(&raw)
                .map(|signed| signed.params)
        })
        .with_context(|| {
            format!(
                "parse network params from {}; expected NetworkProtocolParams or signed envelope",
                path.display()
            )
        })
}

fn load_network_authority_set(
    path: Option<&Path>,
    identity: &crate::crypto::NodeIdentity,
) -> Result<AuthoritySet> {
    let Some(path) = path else {
        return Ok(AuthoritySet::genesis(&identity.node_id()));
    };
    let raw =
        fs::read(path).with_context(|| format!("read authority set from {}", path.display()))?;
    serde_json::from_slice::<AuthoritySet>(&raw)
        .or_else(|_| {
            serde_json::from_slice::<SignedNetworkAuthoritySetEnvelope>(&raw)
                .map(|signed| signed.authority_set)
        })
        .with_context(|| {
            format!(
                "parse authority set from {}; expected AuthoritySet or signed envelope",
                path.display()
            )
        })
}

fn parse_bootstrap_contact_mode(mode: &str) -> Result<NetworkMode> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "lan" => Ok(NetworkMode::Lan),
        "wan" | "network" => Ok(NetworkMode::Wan),
        "local" => Err(anyhow!("bootstrap contacts require lan or wan mode")),
        other => Err(anyhow!("unsupported bootstrap contact mode: {other}")),
    }
}

fn handle_peers(cmd: PeersCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let node = open_node(state_dir, db_path)?;
    match cmd.action {
        PeersAction::List => {
            println!("{}", serde_json::to_string(&node.peers())?);
        }
    }
    Ok(())
}

fn handle_log(cmd: LogCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let mut node = open_node(state_dir, db_path)?;
    match cmd.action {
        LogAction::Head => {
            println!("{}", node.head_seq()?);
        }
        LogAction::Replay => {
            node.replay_rebuild_projection()?;
            println!("replayed");
        }
        LogAction::Verify => {
            node.verify_log()?;
            println!("verified");
        }
    }
    Ok(())
}

fn handle_executors(cmd: ExecutorsCommand, state_dir: &Path) -> Result<()> {
    let mut reg = load_executor_registry_state(state_dir)?;
    match cmd.action {
        ExecutorsAction::Add {
            name,
            base_url,
            remote,
            target_node,
            scope,
        } => {
            let kind = if remote {
                crate::control::ExecutorKind::Remote
            } else {
                crate::control::ExecutorKind::Local
            };
            reg.entries.retain(|e| e.name != name);
            reg.entries.push(ExecutorRegistryEntry {
                name: name.clone(),
                base_url,
                agent_event_callback_base_url: None,
                kind,
                target_node_id: target_node,
                scope_hint: scope,
                commit_plane_endpoint: None,
                commit_plane_token_file: None,
            });
            save_executor_registry_state(state_dir, &reg)?;
            let kind_label = if remote { "remote" } else { "local" };
            println!("executor added: {name} ({kind_label})");
        }
        ExecutorsAction::List => {
            for entry in &reg.entries {
                let kind_label = if entry.is_remote() { "remote" } else { "local" };
                let target = entry
                    .target_node_id
                    .as_deref()
                    .map(|n| format!(" target={n}"))
                    .unwrap_or_default();
                let scope = entry
                    .scope_hint
                    .as_deref()
                    .map(|s| format!(" scope={s}"))
                    .unwrap_or_default();
                println!(
                    "{} {} [{}{}{}]",
                    entry.name, entry.base_url, kind_label, target, scope
                );
            }
        }
        ExecutorsAction::Check { name } => {
            let entry = reg
                .entries
                .iter()
                .find(|e| e.name == name)
                .ok_or_else(|| anyhow!("executor not found"))?;
            let client = reqwest::blocking::Client::new();
            client
                .get(format!("{}/health", entry.base_url.trim_end_matches('/')))
                .send()?
                .error_for_status()?;
            println!("ok");
        }
    }
    Ok(())
}

fn handle_task(cmd: TaskCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let mut node = open_node(state_dir, db_path)?;
    match cmd.action {
        TaskAction::Submit { file } => {
            let raw = fs::read(&file)?;
            let contract: TaskContract = serde_json::from_slice(&raw)
                .with_context(|| format!("parse task contract from {}", file.display()))?;
            let created_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
            node.submit_task(contract.clone(), 1, created_at)?;
            println!("submitted {}", contract.task_id);
        }
        TaskAction::Watch { task_id } => {
            let task = node
                .task_view(&task_id)?
                .ok_or_else(|| anyhow!("task not found"))?;
            println!(
                "task={} state={:?} committed={:?} finalized={:?}",
                task.contract.task_id,
                task.terminal_state,
                task.committed_candidate_id,
                task.finalized_candidate_id
            );
        }
        TaskAction::Decision { task_id } => {
            let task = node
                .task_view(&task_id)?
                .ok_or_else(|| anyhow!("task not found"))?;
            println!(
                "task={} committed={:?} finalized={:?}",
                task.contract.task_id, task.committed_candidate_id, task.finalized_candidate_id
            );
        }
        TaskAction::RunReal {
            executor,
            profile,
            task_id,
            file,
        } => {
            let result = run_real_task_flow(
                &mut node,
                state_dir,
                RealTaskRunRequest {
                    executor,
                    profile,
                    task_id,
                    task_file: file,
                    task_contract: None,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}

fn handle_run(cmd: RunCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let pg_url = run_control::resolve_run_queue_pg_url(cmd.pg_url);
    match cmd.action {
        RunAction::Init => {
            run_control::init_run_queue(&pg_url)?;
            println!("run queue schema initialized");
        }
        RunAction::Submit { file, kickoff } => {
            let raw = fs::read(&file)?;
            let spec: RunSubmitSpec = serde_json::from_slice(&raw)
                .with_context(|| format!("parse run submit spec from {}", file.display()))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&run_control::submit_run(
                    state_dir, db_path, &pg_url, spec, kickoff,
                )?)?
            );
        }
        RunAction::Kickoff { run_id } => {
            run_control::kickoff_run(state_dir, db_path, &pg_url, &run_id)?;
            println!("kicked off {}", run_id);
        }
        RunAction::Watch { run_id } => {
            let view = run_control::watch_run(state_dir, db_path, &pg_url, &run_id)?;
            println!("{}", serde_json::to_string_pretty(&view)?);
        }
        RunAction::Result { run_id } => {
            let result = run_control::run_result(state_dir, db_path, &pg_url, &run_id)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        RunAction::Events { run_id, limit } => {
            let events = run_control::run_events(state_dir, db_path, &pg_url, &run_id, limit)?;
            println!("{}", serde_json::to_string_pretty(&events)?);
        }
        RunAction::Cancel { run_id } => {
            run_control::cancel_run(state_dir, db_path, &pg_url, &run_id)?;
            println!("cancel requested {}", run_id);
        }
        RunAction::Retry { run_id } => {
            run_control::retry_run(state_dir, db_path, &pg_url, &run_id)?;
            println!("retry requested {}", run_id);
        }
        RunAction::Worker {
            worker_id,
            concurrency,
            poll_ms,
            lease_ms,
            once,
        } => {
            let worker_id = worker_id.unwrap_or_else(|| format!("worker-{}", Uuid::new_v4()));
            run_control::run_worker(
                state_dir,
                db_path,
                &pg_url,
                WorkerOptions {
                    worker_id,
                    concurrency: concurrency.max(1),
                    poll_ms,
                    lease_ms,
                    once,
                },
            )?;
        }
    }
    Ok(())
}

fn handle_knowledge(cmd: KnowledgeCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let node = open_node(state_dir, db_path)?;
    match cmd.action {
        KnowledgeAction::Export {
            task_type,
            task_id,
            out,
        } => {
            let payload = match (task_type.as_deref(), task_id.as_deref()) {
                (Some(tt), None) => node.store.export_knowledge_by_task_type(tt)?,
                (None, Some(id)) => node.store.export_knowledge_by_task(id)?,
                _ => {
                    return Err(anyhow!(
                        "knowledge export requires exactly one of --task_type or --task_id"
                    ));
                }
            };
            fs::write(&out, serde_json::to_vec_pretty(&payload)?)?;
            println!("exported {}", out.display());
        }
    }
    Ok(())
}

fn handle_governance(cmd: GovernanceCommand, state_dir: &Path, db_path: &Path) -> Result<()> {
    let mut node = open_node(state_dir, db_path)?;
    ensure_mainnet_genesis_node(&node)?;
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let (action, event) = match cmd.action {
        GovernanceAction::MembershipUpdate {
            file,
            epoch,
            quorum_threshold,
        } => {
            let raw = fs::read(&file)?;
            let new_membership: Membership = serde_json::from_slice(&raw)
                .with_context(|| format!("parse membership from {}", file.display()))?;
            let signature = crate::node::sign_membership_quorum(&node.identity, &new_membership)?;
            let event = node.emit_at(
                epoch,
                EventPayload::MembershipUpdated(MembershipUpdatedPayload {
                    new_membership,
                    quorum_threshold,
                    quorum_signatures: vec![signature],
                }),
                now,
            )?;
            ("membership_update", event)
        }
        GovernanceAction::RevokeEvent {
            event_id,
            reason,
            epoch,
        } => (
            "revoke_event",
            node.revoke_event(&event_id, &reason, epoch, now)?,
        ),
        GovernanceAction::RevokeSummary {
            summary_id,
            summary_kind,
            reason,
            epoch,
        } => (
            "revoke_summary",
            node.revoke_summary(&summary_id, &summary_kind, &reason, epoch, now)?,
        ),
        GovernanceAction::PenalizeNode {
            node_id,
            reason,
            revoked_event_ids,
            revoked_summary_ids,
            epoch,
        } => (
            "penalize_node",
            node.penalize_node(
                &node_id,
                &reason,
                revoked_event_ids,
                revoked_summary_ids,
                epoch,
                now,
            )?,
        ),
    };
    print_governance_event(action, &event)
}

fn ensure_mainnet_genesis_node(node: &crate::node::Node) -> Result<()> {
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())?;
    if topology.network.network_kind != NetworkKind::Mainnet {
        return Err(anyhow!("governance command requires a mainnet node"));
    }
    let local_node_id = node.node_id();
    if local_node_id != topology.network.genesis_node_id {
        return Err(anyhow!(
            "governance command requires mainnet genesis node; local_node_id={} genesis_node_id={}",
            local_node_id,
            topology.network.genesis_node_id
        ));
    }
    Ok(())
}

fn print_governance_event(action: &str, event: &Event) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "ok": true,
            "action": action,
            "event_id": event.event_id,
            "event_kind": format!("{:?}", event.event_kind),
            "author_node_id": event.author_node_id,
            "epoch": event.epoch,
            "created_at": event.created_at,
        }))?
    );
    Ok(())
}
