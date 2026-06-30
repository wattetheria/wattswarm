use super::*;

static NETWORK_SERVICE_STATUSES: OnceLock<Mutex<HashMap<PathBuf, NetworkServiceStatus>>> =
    OnceLock::new();
static LATEST_NETWORK_OBSERVABILITY_SNAPSHOTS: OnceLock<
    Mutex<HashMap<PathBuf, NetworkBridgeObservabilitySnapshot>>,
> = OnceLock::new();

const NETWORK_SERVICE_START_RETRY_DELAY: Duration = Duration::from_secs(5);
const NETWORK_LOOP_DIAGNOSTIC_INTERVAL: Duration = Duration::from_secs(1);
/// Cap retry attempts so a stuck dependency (e.g. an unreachable relay) doesn't
/// keep the network bridge in `retrying` forever. After this many consecutive
/// retryable failures the bridge thread exits with status=Failed; operators
/// see a clear terminal state and the container can be restarted by docker.
const MAX_NETWORK_SERVICE_START_RETRIES: u32 = 5;

#[derive(Debug, Default)]
struct DurationStats {
    count: u64,
    total_ms: u128,
    max_ms: u128,
}

impl DurationStats {
    fn record(&mut self, elapsed: Duration) {
        let elapsed_ms = elapsed.as_millis();
        self.count = self.count.saturating_add(1);
        self.total_ms = self.total_ms.saturating_add(elapsed_ms);
        self.max_ms = self.max_ms.max(elapsed_ms);
    }
}

#[derive(Debug)]
struct NetworkLoopDiagnostics {
    window_started_at: Instant,
    iterations: u64,
    busy_iterations: u64,
    idle_sleeps: u64,
    ticks_total: u64,
    listening_ticks: u64,
    transport_notice_ticks: u64,
    connected_ticks: u64,
    disconnected_ticks: u64,
    event_ingested_ticks: u64,
    summary_applied_ticks: u64,
    rule_applied_ticks: u64,
    checkpoint_applied_ticks: u64,
    gossip_ignored_ticks: u64,
    backfill_served_ticks: u64,
    backfill_applied_ticks: u64,
    backfill_failed_ticks: u64,
    peer_relationship_updated_ticks: u64,
    peer_relationship_failed_ticks: u64,
    transport_notice_prefix_counts: HashMap<String, u64>,
    connected_peer_counts: HashMap<String, u64>,
    disconnected_peer_counts: HashMap<String, u64>,
    tick_duration_stats: HashMap<String, DurationStats>,
    phase_duration_stats: HashMap<String, DurationStats>,
    snapshot_count: u64,
    snapshot_total_ms: u128,
    snapshot_max_ms: u128,
}

impl NetworkLoopDiagnostics {
    fn new() -> Self {
        Self::new_at(Instant::now())
    }

    fn new_at(window_started_at: Instant) -> Self {
        Self {
            window_started_at,
            iterations: 0,
            busy_iterations: 0,
            idle_sleeps: 0,
            ticks_total: 0,
            listening_ticks: 0,
            transport_notice_ticks: 0,
            connected_ticks: 0,
            disconnected_ticks: 0,
            event_ingested_ticks: 0,
            summary_applied_ticks: 0,
            rule_applied_ticks: 0,
            checkpoint_applied_ticks: 0,
            gossip_ignored_ticks: 0,
            backfill_served_ticks: 0,
            backfill_applied_ticks: 0,
            backfill_failed_ticks: 0,
            peer_relationship_updated_ticks: 0,
            peer_relationship_failed_ticks: 0,
            transport_notice_prefix_counts: HashMap::new(),
            connected_peer_counts: HashMap::new(),
            disconnected_peer_counts: HashMap::new(),
            tick_duration_stats: HashMap::new(),
            phase_duration_stats: HashMap::new(),
            snapshot_count: 0,
            snapshot_total_ms: 0,
            snapshot_max_ms: 0,
        }
    }

    fn record_tick(&mut self, tick: &NetworkBridgeTick, elapsed: Duration) {
        self.ticks_total = self.ticks_total.saturating_add(1);
        self.record_duration("tick", tick_duration_label(tick), elapsed);
        match tick {
            NetworkBridgeTick::Listening { .. } => {
                self.listening_ticks = self.listening_ticks.saturating_add(1);
            }
            NetworkBridgeTick::TransportNotice { detail } => {
                self.transport_notice_ticks = self.transport_notice_ticks.saturating_add(1);
                let prefix = transport_notice_prefix(detail).to_owned();
                *self
                    .transport_notice_prefix_counts
                    .entry(prefix)
                    .or_default() += 1;
            }
            NetworkBridgeTick::Connected { peer } => {
                self.connected_ticks = self.connected_ticks.saturating_add(1);
                *self
                    .connected_peer_counts
                    .entry(peer.to_string())
                    .or_default() += 1;
            }
            NetworkBridgeTick::Disconnected { peer } => {
                self.disconnected_ticks = self.disconnected_ticks.saturating_add(1);
                *self
                    .disconnected_peer_counts
                    .entry(peer.to_string())
                    .or_default() += 1;
            }
            NetworkBridgeTick::EventIngested { .. } => {
                self.event_ingested_ticks = self.event_ingested_ticks.saturating_add(1);
            }
            NetworkBridgeTick::SummaryApplied { .. } => {
                self.summary_applied_ticks = self.summary_applied_ticks.saturating_add(1);
            }
            NetworkBridgeTick::RuleApplied { .. } => {
                self.rule_applied_ticks = self.rule_applied_ticks.saturating_add(1);
            }
            NetworkBridgeTick::CheckpointApplied { .. } => {
                self.checkpoint_applied_ticks = self.checkpoint_applied_ticks.saturating_add(1);
            }
            NetworkBridgeTick::GossipIgnored { .. } => {
                self.gossip_ignored_ticks = self.gossip_ignored_ticks.saturating_add(1);
            }
            NetworkBridgeTick::BackfillServed { .. } => {
                self.backfill_served_ticks = self.backfill_served_ticks.saturating_add(1);
            }
            NetworkBridgeTick::BackfillApplied { .. } => {
                self.backfill_applied_ticks = self.backfill_applied_ticks.saturating_add(1);
            }
            NetworkBridgeTick::BackfillFailed { .. } => {
                self.backfill_failed_ticks = self.backfill_failed_ticks.saturating_add(1);
            }
            NetworkBridgeTick::PeerRelationshipUpdated { .. } => {
                self.peer_relationship_updated_ticks =
                    self.peer_relationship_updated_ticks.saturating_add(1);
            }
            NetworkBridgeTick::PeerRelationshipFailed { .. } => {
                self.peer_relationship_failed_ticks =
                    self.peer_relationship_failed_ticks.saturating_add(1);
            }
        }
    }

    fn record_iteration(&mut self, did_work: bool) {
        self.iterations = self.iterations.saturating_add(1);
        if did_work {
            self.busy_iterations = self.busy_iterations.saturating_add(1);
        }
    }

    fn record_idle_sleep(&mut self) {
        self.idle_sleeps = self.idle_sleeps.saturating_add(1);
    }

    fn record_snapshot_duration(&mut self, elapsed: Duration) {
        let elapsed_ms = elapsed.as_millis();
        self.snapshot_count = self.snapshot_count.saturating_add(1);
        self.snapshot_total_ms = self.snapshot_total_ms.saturating_add(elapsed_ms);
        self.snapshot_max_ms = self.snapshot_max_ms.max(elapsed_ms);
        self.record_phase_duration("observability_snapshot", elapsed);
    }

    fn record_phase_duration(&mut self, phase: &'static str, elapsed: Duration) {
        self.record_duration("phase", phase.to_owned(), elapsed);
    }

    fn record_duration(&mut self, kind: &'static str, label: String, elapsed: Duration) {
        let stats = match kind {
            "tick" => &mut self.tick_duration_stats,
            "phase" => &mut self.phase_duration_stats,
            _ => return,
        };
        stats.entry(label).or_default().record(elapsed);
    }

    fn maybe_emit(&mut self, now: Instant) -> Option<String> {
        let elapsed = now.saturating_duration_since(self.window_started_at);
        if elapsed < NETWORK_LOOP_DIAGNOSTIC_INTERVAL {
            return None;
        }
        let line = self.has_activity().then(|| self.render(elapsed));
        *self = Self::new_at(now);
        line
    }

    fn has_activity(&self) -> bool {
        self.busy_iterations > 0 || self.ticks_total > 0
    }

    fn render(&self, elapsed: Duration) -> String {
        format!(
            "network bridge loop diagnostics: window_ms={} iterations={} busy_iterations={} idle_sleeps={} ticks_total={} listening={} transport_notice={} connected={} disconnected={} event_ingested={} summary_applied={} rule_applied={} checkpoint_applied={} gossip_ignored={} backfill_served={} backfill_applied={} backfill_failed={} peer_relationship_updated={} peer_relationship_failed={} transport_notice_prefixes={} connected_peers={} disconnected_peers={} tick_durations={} phase_durations={} snapshots={} snapshot_total_ms={} snapshot_max_ms={}",
            elapsed.as_millis(),
            self.iterations,
            self.busy_iterations,
            self.idle_sleeps,
            self.ticks_total,
            self.listening_ticks,
            self.transport_notice_ticks,
            self.connected_ticks,
            self.disconnected_ticks,
            self.event_ingested_ticks,
            self.summary_applied_ticks,
            self.rule_applied_ticks,
            self.checkpoint_applied_ticks,
            self.gossip_ignored_ticks,
            self.backfill_served_ticks,
            self.backfill_applied_ticks,
            self.backfill_failed_ticks,
            self.peer_relationship_updated_ticks,
            self.peer_relationship_failed_ticks,
            format_counts(&self.transport_notice_prefix_counts),
            format_counts(&self.connected_peer_counts),
            format_counts(&self.disconnected_peer_counts),
            format_duration_stats(&self.tick_duration_stats),
            format_duration_stats(&self.phase_duration_stats),
            self.snapshot_count,
            self.snapshot_total_ms,
            self.snapshot_max_ms,
        )
    }
}

fn tick_duration_label(tick: &NetworkBridgeTick) -> String {
    match tick {
        NetworkBridgeTick::Listening { .. } => "listening".to_owned(),
        NetworkBridgeTick::TransportNotice { detail } => {
            format!("transport_notice:{}", transport_notice_prefix(detail))
        }
        NetworkBridgeTick::Connected { .. } => "connected".to_owned(),
        NetworkBridgeTick::Disconnected { .. } => "disconnected".to_owned(),
        NetworkBridgeTick::EventIngested { .. } => "event_ingested".to_owned(),
        NetworkBridgeTick::SummaryApplied { .. } => "summary_applied".to_owned(),
        NetworkBridgeTick::RuleApplied { .. } => "rule_applied".to_owned(),
        NetworkBridgeTick::CheckpointApplied { .. } => "checkpoint_applied".to_owned(),
        NetworkBridgeTick::GossipIgnored { .. } => "gossip_ignored".to_owned(),
        NetworkBridgeTick::BackfillServed { .. } => "backfill_served".to_owned(),
        NetworkBridgeTick::BackfillApplied { .. } => "backfill_applied".to_owned(),
        NetworkBridgeTick::BackfillFailed { .. } => "backfill_failed".to_owned(),
        NetworkBridgeTick::PeerRelationshipUpdated { .. } => "peer_relationship_updated".to_owned(),
        NetworkBridgeTick::PeerRelationshipFailed { .. } => "peer_relationship_failed".to_owned(),
    }
}

fn transport_notice_prefix(detail: &str) -> &str {
    detail
        .split_whitespace()
        .next()
        .unwrap_or("empty_transport_notice")
}

fn format_counts(counts: &HashMap<String, u64>) -> String {
    if counts.is_empty() {
        return "[]".to_owned();
    }
    let mut entries = counts.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| *key);
    let body = entries
        .into_iter()
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

fn format_duration_stats(stats: &HashMap<String, DurationStats>) -> String {
    if stats.is_empty() {
        return "[]".to_owned();
    }
    let mut entries = stats.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| *key);
    let body = entries
        .into_iter()
        .map(|(key, stats)| {
            format!(
                "{key}:count={},total_ms={},max_ms={}",
                stats.count, stats.total_ms, stats.max_ms
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

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
    let mut loop_diagnostics = NetworkLoopDiagnostics::new();
    let debug_diagnostics = diagnostics::debug_diagnostics_enabled();

    loop {
        let mut did_work = false;
        let try_tick_drain_started_at = Instant::now();
        loop {
            let try_tick_started_at = Instant::now();
            match service.try_tick(&mut node) {
                Ok(Some(tick)) => {
                    loop_diagnostics.record_tick(&tick, try_tick_started_at.elapsed());
                    did_work = true;
                    match tick {
                        NetworkBridgeTick::Listening { address } => {
                            if !announced_listen {
                                crate::udp_announce::announce_startup(
                                    "p2p-startup",
                                    Some(&address.to_string()),
                                    Some(&node_id),
                                );
                                announced_listen = true;
                            }
                        }
                        NetworkBridgeTick::Connected { peer } => {
                            let peer_str = peer.to_string();
                            if record_peer_announcement(
                                &mut announced_peers,
                                &peer_str,
                                Instant::now(),
                            ) {
                                eprintln!("p2p peer connected: {peer}");
                            }
                        }
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    eprintln!("network bridge tick failed: {err}");
                    thread::sleep(Duration::from_millis(250));
                    break;
                }
            }
        }
        loop_diagnostics
            .record_phase_duration("try_tick_drain", try_tick_drain_started_at.elapsed());
        let pending_commands_started_at = Instant::now();
        let processed_pending_commands =
            match process_pending_network_commands(&mut node, &mut service, state_dir) {
                Ok(count) => count,
                Err(err) => {
                    eprintln!("network bridge pending command processing failed: {err}");
                    0
                }
            };
        loop_diagnostics
            .record_phase_duration("pending_commands", pending_commands_started_at.elapsed());
        if processed_pending_commands > 0 {
            did_work = true;
        }
        let expire_connected_started_at = Instant::now();
        let expired_connected = service.expire_stale_connected_peers(Instant::now());
        loop_diagnostics.record_phase_duration(
            "expire_stale_connected_peers",
            expire_connected_started_at.elapsed(),
        );
        if expired_connected > 0 {
            did_work = true;
        }
        let reconnect_started_at = Instant::now();
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
        loop_diagnostics
            .record_phase_duration("reconnect_supervision", reconnect_started_at.elapsed());
        let publish_started_at = Instant::now();
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
        loop_diagnostics.record_phase_duration(
            "publish_pending_scoped_updates",
            publish_started_at.elapsed(),
        );
        if Instant::now() >= next_discovery_bootnode_query_at {
            let discovery_started_at = Instant::now();
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
            loop_diagnostics
                .record_phase_duration("discovery_bootnode_query", discovery_started_at.elapsed());
        }
        let expire_backfill_started_at = Instant::now();
        let expired_backfills = service.expire_stale_backfill_requests(Instant::now());
        loop_diagnostics.record_phase_duration(
            "expire_stale_backfill_requests",
            expire_backfill_started_at.elapsed(),
        );
        if expired_backfills > 0 {
            did_work = true;
        }
        let anti_entropy_started_at = Instant::now();
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
        loop_diagnostics.record_phase_duration("anti_entropy", anti_entropy_started_at.elapsed());
        let snapshot_started_at = Instant::now();
        match service.observability_snapshot(&node) {
            Ok(snapshot) => store_latest_network_observability_snapshot(state_dir, snapshot),
            Err(err) => eprintln!("network bridge observability snapshot failed: {err}"),
        }
        loop_diagnostics.record_snapshot_duration(snapshot_started_at.elapsed());
        if let Some(hook) = post_tick_hook {
            let post_tick_hook_started_at = Instant::now();
            hook(&mut node, state_dir);
            loop_diagnostics
                .record_phase_duration("post_tick_hook", post_tick_hook_started_at.elapsed());
        }
        loop_diagnostics.record_iteration(did_work);
        if !did_work {
            loop_diagnostics.record_idle_sleep();
            thread::sleep(IDLE_NETWORK_SLEEP);
        }
        if let Some(line) = loop_diagnostics.maybe_emit(Instant::now())
            && debug_diagnostics
        {
            eprintln!("{line}");
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

    #[test]
    fn loop_diagnostics_aggregates_tick_and_snapshot_counts() {
        let started_at = Instant::now();
        let mut diagnostics = NetworkLoopDiagnostics::new_at(started_at);

        diagnostics.record_tick(
            &NetworkBridgeTick::TransportNotice {
                detail: "gossip_neighbor_up peer=peer-a scope=Global kind=Events".to_owned(),
            },
            Duration::from_millis(11),
        );
        diagnostics.record_tick(
            &NetworkBridgeTick::TransportNotice {
                detail: "peer_discovered peer=peer-b source=bootstrap address=peer-b".to_owned(),
            },
            Duration::from_millis(13),
        );
        diagnostics.record_iteration(true);
        diagnostics.record_iteration(false);
        diagnostics.record_idle_sleep();
        diagnostics.record_phase_duration("try_tick_drain", Duration::from_millis(30));
        diagnostics.record_phase_duration("anti_entropy", Duration::from_millis(2));
        diagnostics.record_snapshot_duration(Duration::from_millis(3));
        diagnostics.record_snapshot_duration(Duration::from_millis(7));

        let line = diagnostics
            .maybe_emit(started_at + NETWORK_LOOP_DIAGNOSTIC_INTERVAL)
            .expect("diagnostic line emitted after interval");

        assert!(line.contains("iterations=2"));
        assert!(line.contains("busy_iterations=1"));
        assert!(line.contains("idle_sleeps=1"));
        assert!(line.contains("ticks_total=2"));
        assert!(line.contains("transport_notice=2"));
        assert!(
            line.contains("transport_notice_prefixes=[gossip_neighbor_up:1,peer_discovered:1]")
        );
        assert!(line.contains("transport_notice:gossip_neighbor_up:count=1,total_ms=11,max_ms=11"));
        assert!(line.contains("transport_notice:peer_discovered:count=1,total_ms=13,max_ms=13"));
        assert!(line.contains("anti_entropy:count=1,total_ms=2,max_ms=2"));
        assert!(line.contains("observability_snapshot:count=2,total_ms=10,max_ms=7"));
        assert!(line.contains("try_tick_drain:count=1,total_ms=30,max_ms=30"));
        assert!(line.contains("snapshots=2"));
        assert!(line.contains("snapshot_total_ms=10"));
        assert!(line.contains("snapshot_max_ms=7"));
        assert_eq!(diagnostics.iterations, 0);
        assert_eq!(diagnostics.ticks_total, 0);
    }

    #[test]
    fn loop_diagnostics_waits_for_reporting_interval() {
        let started_at = Instant::now();
        let mut diagnostics = NetworkLoopDiagnostics::new_at(started_at);

        diagnostics.record_iteration(true);

        assert!(
            diagnostics
                .maybe_emit(started_at + Duration::from_millis(999))
                .is_none()
        );
        assert_eq!(diagnostics.iterations, 1);
    }

    #[test]
    fn loop_diagnostics_suppresses_idle_windows() {
        let started_at = Instant::now();
        let mut diagnostics = NetworkLoopDiagnostics::new_at(started_at);

        diagnostics.record_iteration(false);
        diagnostics.record_idle_sleep();
        diagnostics.record_snapshot_duration(Duration::from_millis(5));

        assert!(
            diagnostics
                .maybe_emit(started_at + NETWORK_LOOP_DIAGNOSTIC_INTERVAL)
                .is_none()
        );
        assert_eq!(diagnostics.iterations, 0);
        assert_eq!(diagnostics.snapshot_count, 0);
    }
}
