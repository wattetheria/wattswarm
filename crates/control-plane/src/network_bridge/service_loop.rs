use super::*;

static NETWORK_SERVICE_STATUSES: OnceLock<Mutex<HashMap<PathBuf, NetworkServiceStatus>>> =
    OnceLock::new();
static LATEST_NETWORK_OBSERVABILITY_SNAPSHOTS: OnceLock<
    Mutex<HashMap<PathBuf, NetworkBridgeObservabilitySnapshot>>,
> = OnceLock::new();

const NETWORK_SERVICE_START_RETRY_DELAY: Duration = Duration::from_secs(5);
/// Cap retry attempts so a stuck dependency (e.g. an unreachable relay) doesn't
/// keep the network bridge in `retrying` forever. After this many consecutive
/// retryable failures the bridge thread exits with status=Failed; operators
/// see a clear terminal state and the container can be restarted by docker.
const MAX_NETWORK_SERVICE_START_RETRIES: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkServiceStatus {
    Starting,
    Running,
    Retrying,
    Failed,
    Stopped,
}

impl NetworkServiceStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Retrying => "retrying",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }

    fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Retrying)
    }
}

fn network_service_statuses() -> &'static Mutex<HashMap<PathBuf, NetworkServiceStatus>> {
    NETWORK_SERVICE_STATUSES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn set_network_service_status(state_dir: &Path, status: NetworkServiceStatus) {
    let mut statuses = network_service_statuses()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    statuses.insert(state_dir.to_path_buf(), status);
}

fn record_startup_step(state_dir: &Path, step: &'static str) {
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new(
            "info",
            "transport",
            "startup.step",
            "running",
            format!("network bridge startup step: {step}"),
        )
        .details(json!({ "step": step })),
    );
}

fn record_loop_error(state_dir: &Path, phase: &'static str, message: &'static str, error: String) {
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new("error", "transport", phase, "failed", message)
            .details(json!({ "error": error })),
    );
}

fn is_retryable_network_service_startup_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    // Permanent failures: in-process retry cannot fix these and only delays
    // surfacing the real cause to the operator.
    //
    // - "iroh data plane already locked": another process holds the redb
    //   flock on this state_dir's iroh-blobs/. Retrying just rediscovers the
    //   same lock holder. Operator must reconcile the deployment topology.
    // - "initialize iroh router/endpoint timed out": ~always means a missing
    //   precondition (lock contention, unreachable relay, broken DNS). The
    //   underlying work is not making forward progress, so 120s timeouts
    //   stacked on top of each other waste CPU and obscure the real cause.
    if error.contains("iroh data plane already locked")
        || error.contains("initialize iroh router timed out")
        || error.contains("initialize iroh endpoint timed out")
    {
        return false;
    }
    error.contains("timed out") || error.contains("deadline has elapsed")
}

fn record_peer_announcement(
    announced_peers: &mut HashMap<String, Instant>,
    peer: &str,
    now: Instant,
) -> bool {
    announced_peers.retain(|_, announced_at| {
        now.saturating_duration_since(*announced_at) <= ANNOUNCED_PEER_TTL
    });
    announced_peers.insert(peer.to_owned(), now).is_none()
}

fn latest_network_observability_snapshots()
-> &'static Mutex<HashMap<PathBuf, NetworkBridgeObservabilitySnapshot>> {
    LATEST_NETWORK_OBSERVABILITY_SNAPSHOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn store_latest_network_observability_snapshot(
    state_dir: &Path,
    snapshot: NetworkBridgeObservabilitySnapshot,
) {
    let mut snapshots = latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    snapshots.insert(state_dir.to_path_buf(), snapshot);
}

pub(super) fn clear_latest_network_observability_snapshot(state_dir: &Path) {
    let mut snapshots = latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    snapshots.remove(state_dir);
}

pub fn latest_connected_peer_ids(state_dir: &Path) -> Option<Vec<String>> {
    let snapshots = latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let snapshot = snapshots.get(state_dir)?;
    let mut peers = snapshot
        .peer_health
        .iter()
        .filter(|entry| entry.connected)
        .map(|entry| entry.network_peer_id.clone())
        .collect::<Vec<_>>();
    peers.sort();
    peers.dedup();
    Some(peers)
}

pub fn latest_network_observability_snapshot(
    state_dir: &Path,
) -> Option<NetworkBridgeObservabilitySnapshot> {
    latest_network_observability_snapshots()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(state_dir)
        .cloned()
}

pub fn network_service_started(state_dir: &Path) -> bool {
    network_service_statuses()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(state_dir)
        .is_some_and(|status| *status == NetworkServiceStatus::Running)
}

pub fn network_service_status(state_dir: &Path) -> String {
    network_service_statuses()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(state_dir)
        .copied()
        .unwrap_or(NetworkServiceStatus::Stopped)
        .as_str()
        .to_owned()
}

fn parse_bool_env_with_default(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn parse_listen_addrs_env(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_scope_id_env(raw: &str, kind: fn(String) -> SwarmScope) -> Vec<SwarmScope> {
    raw.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| kind(segment.to_owned()))
        .collect()
}

pub fn configured_network_scopes_from_env() -> Vec<SwarmScope> {
    let mut scopes = vec![SwarmScope::Global];
    if let Ok(raw) = env::var(ENV_P2P_REGION_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Region) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
    }
    if let Ok(raw) = env::var(ENV_P2P_NODE_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Node) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
    }
    if let Ok(raw) = env::var(ENV_P2P_LOCAL_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Node) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
    }
    scopes
}

pub fn network_enabled_from_env() -> bool {
    parse_bool_env_with_default(ENV_P2P_ENABLED, true)
}

pub fn network_config_from_env() -> NetworkP2pConfig {
    let listen_addrs = env::var(ENV_P2P_LISTEN_ADDRS)
        .ok()
        .map(|raw| parse_listen_addrs_env(&raw))
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            let port = env::var(ENV_P2P_PORT)
                .ok()
                .and_then(|raw| raw.parse::<u16>().ok())
                .unwrap_or(DEFAULT_P2P_PORT);
            vec![format!("0.0.0.0:{port}")]
        });
    NetworkP2pConfig {
        listen_addrs,
        bootstrap_peers: Vec::new(),
        enable_local_discovery: parse_bool_env_with_default(ENV_P2P_LOCAL_DISCOVERY, true),
        ..NetworkP2pConfig::default()
    }
}

pub fn network_config_from_state_dir(_state_dir: &Path) -> NetworkP2pConfig {
    network_config_from_env()
}

fn network_node_from_state_dir(
    state_dir: &Path,
    config: NetworkP2pConfig,
) -> Result<NetworkP2pNode> {
    let identity = crate::control::load_local_identity(state_dir)?;
    NetworkP2pNode::from_iroh_state_dir(config, state_dir.to_path_buf(), identity.secret_bytes())
}

pub fn maybe_start_background_network_service(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> Result<bool> {
    maybe_start_background_network_service_with_hook(state_dir, db_path, None)
}

/// Start the background network service with an optional per-tick hook.
/// The hook is invoked once per tick with mutable Node access and state_dir,
/// enabling external modules (e.g. run-queue bridge) to process pending work.
pub fn maybe_start_background_network_service_with_hook(
    state_dir: PathBuf,
    db_path: PathBuf,
    post_tick_hook: Option<PostTickHook>,
) -> Result<bool> {
    if !network_enabled_from_env() {
        return Ok(false);
    }
    let Some(mode) = crate::control::configured_node_mode(&state_dir)? else {
        eprintln!("wattswarm p2p network deferred (node mode not configured yet)");
        return Ok(false);
    };
    if matches!(mode, crate::control::NodeMode::Local) {
        return Ok(false);
    }

    let config = network_config_from_state_dir(&state_dir);
    let scopes = configured_network_scopes_from_env();
    config.validate()?;
    {
        let mut statuses = network_service_statuses()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if statuses
            .get(&state_dir)
            .is_some_and(|status| status.is_active())
        {
            return Ok(true);
        }
        statuses.insert(state_dir.clone(), NetworkServiceStatus::Starting);
    }
    let state_dir_for_registry = state_dir.clone();
    thread::spawn(move || {
        let mut retry_attempts: u32 = 0;
        loop {
            clear_latest_network_observability_snapshot(&state_dir_for_registry);
            set_network_service_status(&state_dir_for_registry, NetworkServiceStatus::Starting);
            diagnostics::record_diagnostic(
                Some(&state_dir_for_registry),
                diagnostics::DiagnosticEvent::new(
                    "info",
                    "transport",
                    "startup.starting",
                    "starting",
                    "network bridge startup requested",
                ),
            );
            match run_background_network_service_with_hook(
                &state_dir,
                &db_path,
                config.clone(),
                scopes.clone(),
                post_tick_hook.as_ref(),
            ) {
                Ok(()) => {
                    set_network_service_status(
                        &state_dir_for_registry,
                        NetworkServiceStatus::Stopped,
                    );
                    diagnostics::record_diagnostic(
                        Some(&state_dir_for_registry),
                        diagnostics::DiagnosticEvent::new(
                            "warn",
                            "transport",
                            "startup.stopped",
                            "stopped",
                            "network bridge stopped",
                        ),
                    );
                    clear_latest_network_observability_snapshot(&state_dir_for_registry);
                    break;
                }
                Err(err) => {
                    let error = format!("{err:#}");
                    eprintln!("network bridge stopped: {error}");
                    clear_latest_network_observability_snapshot(&state_dir_for_registry);
                    let retryable = is_retryable_network_service_startup_error(&error);
                    let attempts_exhausted = retry_attempts >= MAX_NETWORK_SERVICE_START_RETRIES;
                    if !retryable || attempts_exhausted {
                        set_network_service_status(
                            &state_dir_for_registry,
                            NetworkServiceStatus::Failed,
                        );
                        let reason = if !retryable {
                            "non-retryable startup error"
                        } else {
                            "retry attempts exhausted"
                        };
                        diagnostics::record_diagnostic(
                            Some(&state_dir_for_registry),
                            diagnostics::DiagnosticEvent::new(
                                "error",
                                "transport",
                                "startup.failed",
                                "failed",
                                "network bridge startup failed",
                            )
                            .details(json!({
                                "error": error,
                                "reason": reason,
                                "attempts": retry_attempts,
                                "max_attempts": MAX_NETWORK_SERVICE_START_RETRIES,
                            })),
                        );
                        break;
                    }
                    retry_attempts = retry_attempts.saturating_add(1);
                    set_network_service_status(
                        &state_dir_for_registry,
                        NetworkServiceStatus::Retrying,
                    );
                    diagnostics::record_diagnostic(
                        Some(&state_dir_for_registry),
                        diagnostics::DiagnosticEvent::new(
                            "warn",
                            "transport",
                            "startup.retrying",
                            "retrying",
                            "network bridge startup failed; retrying",
                        )
                        .details(json!({
                            "error": error,
                            "retry_delay_ms": NETWORK_SERVICE_START_RETRY_DELAY.as_millis(),
                            "attempt": retry_attempts,
                            "max_attempts": MAX_NETWORK_SERVICE_START_RETRIES,
                        })),
                    );
                    thread::sleep(NETWORK_SERVICE_START_RETRY_DELAY);
                }
            }
        }
    });
    Ok(true)
}

/// Callback invoked once per tick of the background network service.
/// Receives mutable Node access and the state_dir path.
pub type PostTickHook = Box<dyn Fn(&mut Node, &Path) + Send + 'static>;

fn run_background_network_service_with_hook(
    state_dir: &Path,
    db_path: &Path,
    config: NetworkP2pConfig,
    configured_scopes: Vec<SwarmScope>,
    post_tick_hook: Option<&PostTickHook>,
) -> Result<()> {
    record_startup_step(state_dir, "open_configured_node.begin");
    let mut node = crate::control::open_configured_node(state_dir, db_path)
        .context("network bridge startup open configured node")?;
    record_startup_step(state_dir, "open_configured_node.end");
    let node_id = node.node_id();
    let scopes = merge_scopes(configured_scopes);
    record_startup_step(state_dir, "dynamic_subscriptions.begin");
    let dynamic_subscriptions = dynamic_subscription_scope_kinds_for_node(&node, &node_id)
        .context("network bridge startup load dynamic subscriptions")?;
    record_startup_step(state_dir, "dynamic_subscriptions.end");
    record_startup_step(state_dir, "protocol_params.begin");
    let verified_protocol_params = node
        .store
        .load_verified_network_protocol_params()
        .context("network bridge startup load verified protocol params")?;
    record_startup_step(state_dir, "protocol_params.end");
    let protocol_params = verified_protocol_params.params().clone();
    let mut config = config.apply_protocol_params(&protocol_params);
    let handshake_network_id = verified_protocol_params.network_id.clone();
    config.namespace.network_id = handshake_network_id.clone();
    let handshake_params_version = verified_protocol_params.signed.version;
    let handshake_params_hash = verified_protocol_params.params_hash().to_owned();
    config.identify_agent_version =
        crate::network_p2p::encode_wattswarm_agent_version(&PeerHandshakeMetadata {
            network_id: handshake_network_id,
            params_version: handshake_params_version,
            params_hash: handshake_params_hash,
        });
    config
        .validate()
        .context("network bridge startup validate p2p config")?;
    record_startup_step(state_dir, "create_p2p_node.begin");
    let network_node = network_node_from_state_dir(state_dir, config)
        .context("network bridge startup create network p2p node")?;
    record_startup_step(state_dir, "create_p2p_node.end");
    record_startup_step(state_dir, "create_bridge_service.begin");
    let mut service = NetworkBridgeService::new(network_node, &scopes, &protocol_params)
        .context("network bridge startup create bridge service")?;
    record_startup_step(state_dir, "create_bridge_service.end");
    record_startup_step(state_dir, "subscribe_dynamic_scopes.begin");
    for (scope, gossip_kinds) in dynamic_subscriptions {
        service
            .subscribe_scope_kinds(&scope, &gossip_kinds)
            .context("network bridge startup subscribe dynamic scope kinds")?;
    }
    record_startup_step(state_dir, "subscribe_dynamic_scopes.end");
    record_startup_step(state_dir, "restore_remote_relay.begin");
    let restored_relay_scopes = service
        .restore_remote_feed_subscriptions_for_relay(&node, &node_id)
        .context("network bridge startup restore remote relay subscriptions")?;
    record_startup_step(state_dir, "restore_remote_relay.end");
    if !restored_relay_scopes.is_empty() {
        eprintln!(
            "network bridge restored {} remote relay subscription scopes",
            restored_relay_scopes.len()
        );
    }
    record_startup_step(state_dir, "set_state_dir.begin");
    service.set_state_dir(state_dir.to_path_buf(), db_path.to_path_buf());
    record_startup_step(state_dir, "set_state_dir.end");
    record_startup_step(state_dir, "initial_snapshot.begin");
    store_latest_network_observability_snapshot(
        state_dir,
        service
            .observability_snapshot(&node)
            .context("network bridge startup build initial observability snapshot")?,
    );
    record_startup_step(state_dir, "initial_snapshot.end");
    set_network_service_status(state_dir, NetworkServiceStatus::Running);
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new(
            "info",
            "transport",
            "startup.ready",
            "ok",
            "network bridge startup completed",
        ),
    );
    let mut announced_listen = false;
    let mut announced_peers: HashMap<String, Instant> = HashMap::new();
    let mut last_published_seq = node.head_seq()?;
    let mut next_discovery_bootnode_query_at = Instant::now();

    loop {
        let mut did_work = false;
        loop {
            match service.try_tick(&mut node) {
                Ok(Some(NetworkBridgeTick::Listening { address })) => {
                    did_work = true;
                    if !announced_listen {
                        crate::udp_announce::announce_startup(
                            "p2p-startup",
                            Some(&address.to_string()),
                            Some(&node_id),
                        );
                        announced_listen = true;
                    }
                }
                Ok(Some(NetworkBridgeTick::Connected { peer })) => {
                    did_work = true;
                    let peer_str = peer.to_string();
                    if record_peer_announcement(&mut announced_peers, &peer_str, Instant::now()) {
                        eprintln!("p2p peer connected: {peer}");
                    }
                }
                Ok(Some(_)) => {
                    did_work = true;
                }
                Ok(None) => break,
                Err(err) => {
                    eprintln!("network bridge tick failed: {err}");
                    thread::sleep(Duration::from_millis(250));
                    break;
                }
            }
        }
        let processed_pending_commands =
            match process_pending_network_commands(&mut node, &mut service, state_dir) {
                Ok(count) => count,
                Err(err) => {
                    eprintln!("network bridge pending command processing failed: {err}");
                    0
                }
            };
        if processed_pending_commands > 0 {
            did_work = true;
        }
        match service
            .run_reconnect_supervision()
            .context("network bridge run reconnect supervision")
        {
            Ok(count) if count > 0 => did_work = true,
            Ok(_) => {}
            Err(err) => {
                let error = format!("{err:#}");
                eprintln!("network bridge reconnect supervision failed: {error}");
                record_loop_error(
                    state_dir,
                    "reconnect.failed",
                    "network bridge reconnect supervision failed",
                    error,
                );
            }
        }
        match publish_pending_scoped_updates(&mut service, &node, &node_id, last_published_seq)
            .context("network bridge publish pending scoped updates")
        {
            Ok(new_last_published_seq) if new_last_published_seq != last_published_seq => {
                did_work = true;
                last_published_seq = new_last_published_seq;
            }
            Ok(_) => {}
            Err(err) => {
                let error = format!("{err:#}");
                eprintln!("network bridge publish pending scoped updates failed: {error}");
                record_loop_error(
                    state_dir,
                    "publish.failed",
                    "network bridge publish pending scoped updates failed",
                    error,
                );
            }
        }
        if Instant::now() >= next_discovery_bootnode_query_at {
            match discovery_bootnode_settings_from_state_dir(state_dir) {
                Ok(settings) => {
                    next_discovery_bootnode_query_at = Instant::now() + settings.interval;
                    let network_id = current_network_context_id(&node);
                    match query_discovery_bootnodes_for_candidate_records(
                        state_dir,
                        &node,
                        &network_id,
                        &settings,
                        observed_at_ms(),
                    ) {
                        Ok(records) => {
                            for record in records {
                                let local_peer_id = service.local_peer_id().to_string();
                                match apply_discovery_bootnode_record(
                                    &mut service,
                                    &mut node,
                                    state_dir,
                                    &local_peer_id,
                                    &network_id,
                                    &settings,
                                    record,
                                    observed_at_ms(),
                                ) {
                                    Ok(true) => did_work = true,
                                    Ok(false) => {}
                                    Err(err) => {
                                        eprintln!("discovery bootnode record skipped: {err}")
                                    }
                                }
                            }
                        }
                        Err(err) => eprintln!("discovery bootnode query skipped: {err}"),
                    }
                }
                Err(err) => {
                    next_discovery_bootnode_query_at =
                        Instant::now() + DEFAULT_DISCOVERY_BOOTNODE_QUERY_INTERVAL;
                    eprintln!("discovery bootnode query skipped: {err}");
                }
            }
        }
        if service.expire_stale_backfill_requests(Instant::now(), BACKFILL_REQUEST_TIMEOUT) > 0 {
            did_work = true;
        }
        match service
            .run_anti_entropy(&node)
            .context("network bridge run anti-entropy")
        {
            Ok(count) if count > 0 => did_work = true,
            Ok(_) => {}
            Err(err) => {
                let error = format!("{err:#}");
                eprintln!("network bridge anti-entropy failed: {error}");
                record_loop_error(
                    state_dir,
                    "anti_entropy.failed",
                    "network bridge anti-entropy failed",
                    error,
                );
            }
        }
        match service.observability_snapshot(&node) {
            Ok(snapshot) => store_latest_network_observability_snapshot(state_dir, snapshot),
            Err(err) => eprintln!("network bridge observability snapshot failed: {err}"),
        }
        if let Some(hook) = post_tick_hook {
            hook(&mut node, state_dir);
        }
        if !did_work {
            thread::sleep(IDLE_NETWORK_SLEEP);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_state_dir(label: &str) -> PathBuf {
        env::temp_dir().join(format!("wattswarm-{label}-{}", Uuid::new_v4()))
    }

    #[test]
    fn network_service_started_only_reports_running_after_ready() {
        let state_dir = unique_state_dir("network-service-status");

        set_network_service_status(&state_dir, NetworkServiceStatus::Starting);
        assert_eq!(network_service_status(&state_dir), "starting");
        assert!(!network_service_started(&state_dir));

        set_network_service_status(&state_dir, NetworkServiceStatus::Running);
        assert_eq!(network_service_status(&state_dir), "running");
        assert!(network_service_started(&state_dir));

        set_network_service_status(&state_dir, NetworkServiceStatus::Retrying);
        assert_eq!(network_service_status(&state_dir), "retrying");
        assert!(!network_service_started(&state_dir));
        assert!(NetworkServiceStatus::Retrying.is_active());

        set_network_service_status(&state_dir, NetworkServiceStatus::Failed);
        assert_eq!(network_service_status(&state_dir), "failed");
        assert!(!network_service_started(&state_dir));

        set_network_service_status(&state_dir, NetworkServiceStatus::Stopped);
        assert_eq!(network_service_status(&state_dir), "stopped");
        assert!(!network_service_started(&state_dir));
    }

    #[test]
    fn retryable_network_service_startup_error_treats_iroh_init_as_permanent() {
        // Iroh init timeouts are NOT retryable: in-process retries cannot fix
        // a held redb lock or an unreachable relay, and stacking 120s timeouts
        // only obscures the real cause.
        assert!(!is_retryable_network_service_startup_error(
            "network bridge startup create bridge service: initialize iroh router timed out after 120000ms"
        ));
        assert!(!is_retryable_network_service_startup_error(
            "network bridge startup create bridge service: initialize iroh endpoint timed out after 120000ms"
        ));
        // Lock-collision errors are emitted by the new sentinel pre-check and
        // are explicitly non-retryable.
        assert!(!is_retryable_network_service_startup_error(
            "iroh data plane already locked at /var/lib/wattswarm/iroh-blobs/.wattswarm-data-plane.lock"
        ));
        // Other transient timeouts (e.g. database connection startup) remain
        // retryable — we still want a few attempts before giving up.
        assert!(is_retryable_network_service_startup_error(
            "network bridge startup load verified protocol params: postgres connect timed out"
        ));
        // Pure configuration errors are non-retryable, as before.
        assert!(!is_retryable_network_service_startup_error(
            "network bridge startup validate p2p config: invalid listen address"
        ));
    }
}
