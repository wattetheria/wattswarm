use super::*;

static STARTED_NETWORK_SERVICES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
static LATEST_NETWORK_OBSERVABILITY_SNAPSHOTS: OnceLock<
    Mutex<HashMap<PathBuf, NetworkBridgeObservabilitySnapshot>>,
> = OnceLock::new();

fn started_network_services() -> &'static Mutex<HashSet<PathBuf>> {
    STARTED_NETWORK_SERVICES.get_or_init(|| Mutex::new(HashSet::new()))
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
    started_network_services()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(state_dir)
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
            vec![format!("/ip4/0.0.0.0/tcp/{port}")]
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
        let mut started = started_network_services()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if started.contains(&state_dir) {
            return Ok(true);
        }
        started.insert(state_dir.clone());
    }
    let state_dir_for_registry = state_dir.clone();
    thread::spawn(move || {
        if let Err(err) = run_background_network_service_with_hook(
            &state_dir,
            &db_path,
            config,
            scopes,
            post_tick_hook,
        ) {
            eprintln!("network bridge stopped: {err:#}");
        }
        clear_latest_network_observability_snapshot(&state_dir_for_registry);
        let mut started = started_network_services()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        started.remove(&state_dir_for_registry);
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
    post_tick_hook: Option<PostTickHook>,
) -> Result<()> {
    let mut node = crate::control::open_configured_node(state_dir, db_path)
        .context("network bridge startup open configured node")?;
    let node_id = node.node_id();
    let scopes = merge_scopes(configured_scopes);
    let dynamic_subscriptions = dynamic_subscription_scope_kinds_for_node(&node, &node_id)
        .context("network bridge startup load dynamic subscriptions")?;
    let verified_protocol_params = node
        .store
        .load_verified_network_protocol_params()
        .context("network bridge startup load verified protocol params")?;
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
    let mut service = NetworkBridgeService::new(
        network_node_from_state_dir(state_dir, config)
            .context("network bridge startup create network p2p node")?,
        &scopes,
        &protocol_params,
    )
    .context("network bridge startup create bridge service")?;
    for (scope, gossip_kinds) in dynamic_subscriptions {
        service
            .subscribe_scope_kinds(&scope, &gossip_kinds)
            .context("network bridge startup subscribe dynamic scope kinds")?;
    }
    let restored_relay_scopes = service
        .restore_remote_feed_subscriptions_for_relay(&node, &node_id)
        .context("network bridge startup restore remote relay subscriptions")?;
    if !restored_relay_scopes.is_empty() {
        eprintln!(
            "network bridge restored {} remote relay subscription scopes",
            restored_relay_scopes.len()
        );
    }
    service.set_state_dir(state_dir.to_path_buf(), db_path.to_path_buf());
    store_latest_network_observability_snapshot(
        state_dir,
        service
            .observability_snapshot(&node)
            .context("network bridge startup build initial observability snapshot")?,
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
            match process_pending_network_commands(&mut service, state_dir) {
                Ok(count) => count,
                Err(err) => {
                    eprintln!("network bridge pending command processing failed: {err}");
                    0
                }
            };
        if processed_pending_commands > 0 {
            did_work = true;
        }
        if service
            .run_reconnect_supervision()
            .context("network bridge run reconnect supervision")?
            > 0
        {
            did_work = true;
        }
        let new_last_published_seq =
            publish_pending_scoped_updates(&mut service, &node, &node_id, last_published_seq)
                .context("network bridge publish pending scoped updates")?;
        if new_last_published_seq != last_published_seq {
            did_work = true;
            last_published_seq = new_last_published_seq;
        }
        if Instant::now() >= next_discovery_bootnode_query_at {
            match discovery_bootnode_settings_from_state_dir(state_dir) {
                Ok(settings) => {
                    next_discovery_bootnode_query_at = Instant::now() + settings.interval;
                    let network_id = current_network_context_id(&node);
                    match query_discovery_bootnodes_for_candidate_records(
                        state_dir,
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
        if service
            .run_anti_entropy(&node)
            .context("network bridge run anti-entropy")?
            > 0
        {
            did_work = true;
        }
        match service.observability_snapshot(&node) {
            Ok(snapshot) => store_latest_network_observability_snapshot(state_dir, snapshot),
            Err(err) => eprintln!("network bridge observability snapshot failed: {err}"),
        }
        if let Some(hook) = &post_tick_hook {
            hook(&mut node, state_dir);
        }
        if !did_work {
            thread::sleep(IDLE_NETWORK_SLEEP);
        }
    }
}
