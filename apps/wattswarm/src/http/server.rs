use crate::control::local_node_id;
use crate::http::UiServerState;
use crate::http::background::mark_node_running_if_service_started;
use crate::http::{
    diagnostics, discovery, egress, executors, node, pages, peers, runs, startup, swarm, tasks,
    topics,
};
use crate::wattetheria_sync;
use anyhow::{Context, Result};
use axum::Router;
use axum::routing::{get, post};
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

const DISCOVERY_BOOTNODE_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);

pub fn run(state_dir: PathBuf, db_path: PathBuf, listen: String) -> Result<()> {
    fs::create_dir_all(&state_dir)?;
    crate::startup_config::ensure_default_wan_startup_config(&state_dir)?;
    let node_id = local_node_id(&state_dir).ok();
    let network_enabled = crate::network_bridge::network_enabled_from_env();
    if network_enabled {
        if let Some(id) = &node_id {
            crate::udp_announce::maybe_start_listener(state_dir.clone(), id.clone());
        }
    }
    if network_enabled {
        // Refresh once per process start, before the network service builds the
        // iroh endpoint, so a restart picks up the network's current relay set.
        match crate::control::refresh_startup_config_relay_urls_from_join_manifest(&state_dir) {
            Ok(true) => eprintln!("wattswarm relay urls refreshed from join manifest"),
            Ok(false) => {}
            Err(error) => eprintln!("wattswarm relay urls refresh skipped: {error:#}"),
        }
    }
    let network_started = crate::network_bridge::maybe_start_background_network_service_with_hook(
        state_dir.clone(),
        db_path.clone(),
        Some(Box::new(|node, sd| {
            crate::network_hooks::run_background_post_tick(node, sd);
        })),
    )?;
    if network_started {
        mark_node_running_if_service_started(&state_dir, true)?;
        spawn_periodic_discovery_bootnode_announce(state_dir.clone(), db_path.clone());
        eprintln!("wattswarm p2p network enabled");
    } else {
        eprintln!("wattswarm p2p network disabled");
    }
    if network_enabled {
        crate::udp_announce::announce_startup("ui-startup", Some(&listen), node_id.as_deref());
    }
    let state = UiServerState { state_dir, db_path };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime for UI")?;

    runtime.block_on(async move {
        if let Some(grpc_listen) = wattetheria_sync::grpc_listen_addr_from_env() {
            let grpc_state = state.clone();
            let grpc_listen_task = grpc_listen.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    wattetheria_sync::serve_grpc(grpc_state, grpc_listen_task.clone()).await
                {
                    eprintln!(
                        "wattswarm Wattetheria sync gRPC failed on {grpc_listen_task}: {error}"
                    );
                }
            });
            eprintln!("wattswarm Wattetheria sync gRPC listening on {grpc_listen}");
        }
        let app = build_app(state);
        let listener = tokio::net::TcpListener::bind(&listen)
            .await
            .with_context(|| format!("bind UI on {}", listen))?;
        println!("wattswarm-ui listening on http://{}", listen);
        axum::serve(listener, app).await.context("serve UI")
    })
}

pub fn spawn_discovery_bootnode_announce(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        run_discovery_bootnode_announce(&state_dir, &db_path);
    })
}

pub fn spawn_periodic_discovery_bootnode_announce(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> thread::JoinHandle<()> {
    spawn_discovery_bootnode_announce_loop(
        state_dir,
        db_path,
        DISCOVERY_BOOTNODE_ANNOUNCE_INTERVAL,
        None,
    )
}

#[doc(hidden)]
pub fn spawn_discovery_bootnode_announce_loop(
    state_dir: PathBuf,
    db_path: PathBuf,
    interval: Duration,
    max_runs: Option<usize>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut runs = 0usize;
        loop {
            run_discovery_bootnode_announce(&state_dir, &db_path);
            runs = runs.saturating_add(1);
            if max_runs.is_some_and(|max_runs| runs >= max_runs) {
                break;
            }
            thread::sleep(interval);
        }
    })
}

fn run_discovery_bootnode_announce(state_dir: &std::path::Path, db_path: &std::path::Path) {
    match discovery::maybe_announce_local_record_to_discovery_bootnodes(state_dir, db_path) {
        Ok(report) if report.attempted > 0 => {
            eprintln!(
                "wattswarm discovery announce attempted={} succeeded={} failed={}",
                report.attempted, report.succeeded, report.failed
            );
        }
        Ok(_) => {}
        Err(error) => eprintln!("wattswarm discovery announce failed: {error}"),
    }
}

pub fn build_app(state: UiServerState) -> Router {
    Router::new()
        .route("/", get(pages::index))
        .route("/favicon.png", get(pages::favicon_png))
        .route("/diagnostics", get(pages::diagnostics_page))
        .route("/console", get(pages::legacy_console_redirect))
        .route("/swarm", get(pages::swarm_page))
        .route("/api/node/up", post(node::node_up))
        .route("/api/node/down", post(node::node_down))
        .route("/api/node/status", get(node::node_status))
        .route(
            "/api/network/local",
            get(crate::http::network_bootstrap::network_local),
        )
        .route(
            "/api/network/bootstrap",
            get(crate::http::network_bootstrap::network_bootstrap),
        )
        .route(
            "/.well-known/wattswarm/join.json",
            get(crate::http::network_bootstrap::network_join_manifest),
        )
        .route(
            "/api/network/discovery/records",
            post(discovery::discovery_announce_record),
        )
        .route(
            "/api/network/discovery/nearby",
            get(discovery::discovery_find_nearby),
        )
        .route(
            "/api/network/discovery/capability",
            get(discovery::discovery_find_capability),
        )
        .route(
            "/api/network/discovery/topic-providers",
            get(discovery::discovery_find_topic_providers),
        )
        .route(
            "/api/network/discovery/agent",
            get(discovery::discovery_find_agent),
        )
        .route(
            "/api/network/discovery/node/:node_id",
            get(discovery::discovery_find_node),
        )
        .route("/api/startup-config", get(startup::startup_config_get))
        .route("/api/startup-config", post(startup::startup_config_save))
        .route("/api/peers/list", get(peers::peers_list))
        .route(
            "/api/peers/relationships",
            get(peers::peer_relationships_list),
        )
        .route(
            "/api/peers/relationships",
            post(peers::peer_relationships_update),
        )
        .route("/api/peers/dm/threads", get(peers::peer_dm_threads_list))
        .route("/api/peers/dm/messages", get(peers::peer_dm_messages_list))
        .route("/api/peers/dm/messages", post(peers::peer_dm_messages_send))
        .route(
            "/api/peers/dm/private-hive-key-shares",
            post(peers::private_hive_key_share_send),
        )
        .route("/api/payments/messages", post(peers::agent_payment_send))
        .route("/api/log/head", get(diagnostics::log_head))
        .route("/api/log/replay", post(diagnostics::log_replay))
        .route("/api/log/verify", post(diagnostics::log_verify))
        .route("/api/diagnostics", get(diagnostics::diagnostics))
        .route("/api/executors/add", post(executors::executors_add))
        .route("/api/executors/list", get(executors::executors_list))
        .route("/api/executors/check", post(executors::executors_check))
        .route("/api/egress-agent", get(egress::egress_agent_get))
        .route("/api/egress-agent", post(egress::egress_agent_save))
        .route(
            "/api/a2a/google/agent-card",
            get(egress::google_a2a_agent_card),
        )
        .route(
            "/.well-known/agent.json",
            get(egress::google_a2a_well_known),
        )
        .route(
            "/api/a2a/google/message/send",
            post(egress::google_a2a_message_send),
        )
        .route("/api/task/sample", get(tasks::task_sample))
        .route("/api/task/submit", post(tasks::task_submit))
        .route(
            "/api/task/import-contract",
            post(tasks::task_import_contract),
        )
        .route("/api/task/announce", post(tasks::task_announce))
        .route("/api/task/claim", post(tasks::task_claim))
        .route("/api/task/claim-decision", post(tasks::task_claim_decision))
        .route("/api/task/complete", post(tasks::task_complete))
        .route(
            "/api/task/completion-decision",
            post(tasks::task_completion_decision),
        )
        .route("/api/task/settle", post(tasks::task_settle))
        .route(
            "/api/task/propose-candidate",
            post(tasks::task_propose_candidate),
        )
        .route("/api/task/accept-result", post(tasks::task_accept_result))
        .route("/api/task/watch/:task_id", get(tasks::task_watch))
        .route("/api/task/decision/:task_id", get(tasks::task_decision))
        .route(
            "/api/task/facts/:task_id",
            get(wattetheria_sync::task_facts_snapshot_http),
        )
        .route("/api/task/run-real", post(tasks::task_run_real))
        .route("/api/run/submit", post(runs::run_submit))
        .route("/api/run/kickoff/:run_id", post(runs::run_kickoff))
        .route("/api/run/watch/:run_id", get(runs::run_watch))
        .route("/api/run/result/:run_id", get(runs::run_result))
        .route("/api/run/events/:run_id", get(runs::run_events))
        .route("/api/run/cancel/:run_id", post(runs::run_cancel))
        .route("/api/run/retry/:run_id", post(runs::run_retry))
        .route("/api/knowledge/export", post(runs::knowledge_export))
        .route("/api/topic/messages", get(topics::topic_messages))
        .route("/api/topic/messages", post(topics::topic_message_post))
        .route("/api/topic/cursor", get(topics::topic_cursor))
        .route(
            "/api/topic/subscriptions",
            post(topics::topic_subscription_post),
        )
        .route(
            "/api/wattetheria/network/snapshot",
            get(wattetheria_sync::network_snapshot_http),
        )
        .route(
            "/api/wattetheria/task-run/snapshot",
            get(wattetheria_sync::task_run_snapshot_http),
        )
        .route(
            "/api/wattetheria/task/decision/:task_id",
            get(wattetheria_sync::task_decision_snapshot_http),
        )
        .route(
            "/api/wattetheria/task/facts/:task_id",
            get(wattetheria_sync::task_facts_snapshot_http),
        )
        .route(
            "/api/wattetheria/run/result/:run_id",
            get(wattetheria_sync::run_result_snapshot_http),
        )
        .route(
            "/api/wattetheria/run/events/:run_id",
            get(wattetheria_sync::run_events_snapshot_http),
        )
        .route(
            "/api/wattetheria/topic/activity",
            get(wattetheria_sync::topic_activity_http),
        )
        .route(
            "/api/wattetheria/topic/subscriptions",
            get(wattetheria_sync::topic_subscriptions_http),
        )
        .route(
            "/api/wattetheria/knowledge/export",
            post(wattetheria_sync::knowledge_export_snapshot_http),
        )
        .route(
            "/api/wattetheria/brain/publish-topic",
            post(wattetheria_sync::brain_publish_topic_http),
        )
        .route(
            "/api/wattetheria/brain/submit-run",
            post(wattetheria_sync::brain_submit_run_http),
        )
        .route(
            "/api/wattetheria/brain/run-task-real",
            post(wattetheria_sync::brain_run_task_real_http),
        )
        .route("/api/swarm/state", get(swarm::swarm_state))
        .route("/api/swarm/tick", post(swarm::swarm_tick))
        .with_state(state)
}
