use crate::control::{
    ExecutorRegistryEntry, NodeMode, NodeState, RealTaskRunRequest, executor_registry_path,
    load_executor_registry, node_state_path, open_node, open_node_in_mode, resolve_node_mode,
    run_real_task_flow, save_executor_registry, write_node_state,
};
use crate::run_control;
use crate::run_queue::{RunSubmitSpec, WorkerOptions};
pub use crate::task_template::{sample_artifact_ref, sample_contract};
use crate::types::TaskContract;
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
    Add { name: String, base_url: String },
    List,
    Check { name: String },
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
    }
    Ok(())
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
    let reg_path = executor_registry_path(state_dir);
    let mut reg = load_executor_registry(&reg_path)?;
    match cmd.action {
        ExecutorsAction::Add { name, base_url } => {
            reg.entries.retain(|e| e.name != name);
            reg.entries.push(ExecutorRegistryEntry {
                name: name.clone(),
                base_url,
            });
            save_executor_registry(&reg_path, &reg)?;
            println!("executor added: {name}");
        }
        ExecutorsAction::List => {
            for entry in reg.entries {
                println!("{} {}", entry.name, entry.base_url);
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
