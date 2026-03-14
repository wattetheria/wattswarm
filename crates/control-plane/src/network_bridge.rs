use anyhow::{Result, anyhow, bail};

use crate::constants::BACKFILL_BATCH_EVENTS;
use crate::control::load_discovered_peer_records;
use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, BackfillResponse, EventEnvelope, GossipMessage, Multiaddr,
    NetworkP2pConfig, NetworkP2pNode, NetworkRuntime, NetworkRuntimeEvent, PeerId,
    SummaryAnnouncement, SwarmScope,
};
use crate::node::Node;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

const ENV_P2P_REGION_IDS: &str = "WATTSWARM_P2P_REGION_IDS";
const ENV_P2P_LOCAL_IDS: &str = "WATTSWARM_P2P_LOCAL_IDS";
const ENV_P2P_NODE_IDS: &str = "WATTSWARM_P2P_NODE_IDS";
const KNOWLEDGE_SUMMARY_KIND: &str = "knowledge_task_type_v1";
const REPUTATION_SUMMARY_KIND: &str = "reputation_runtime_profile_v1";
const SUMMARY_REPUTATION_LIMIT: usize = 64;
const SUMMARY_DECISION_MEMORY_LIMIT: u32 = 16;
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(15);
const BACKFILL_RETRY_AFTER: Duration = Duration::from_secs(5);
const MAX_INFLIGHT_BACKFILLS_PER_PEER: usize = 1;
const SUMMARY_BACKPRESSURE_HIGH_WATERMARK: u64 = 256;
const IDLE_NETWORK_SLEEP: Duration = Duration::from_millis(50);

fn observed_at_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone)]
struct PeerSyncState {
    inflight_backfills: usize,
    last_backfill_request_at: Option<Instant>,
    next_retry_at: Instant,
    known_scopes: HashSet<SwarmScope>,
}

impl PeerSyncState {
    fn new(now: Instant) -> Self {
        Self {
            inflight_backfills: 0,
            last_backfill_request_at: None,
            next_retry_at: now,
            known_scopes: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct KnowledgeSummaryBundle {
    source_node_id: String,
    task_type: String,
    decisions: Vec<crate::storage::DecisionMemoryHitRow>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct ReputationSummaryBundle {
    source_node_id: String,
    entries: Vec<crate::storage::ReputationSnapshotRow>,
}

pub fn ingest_event_envelope(node: &mut Node, envelope: &EventEnvelope) -> Result<()> {
    node.ingest_remote(envelope.event.clone())
}

pub fn event_envelope_from_gossip(message: &GossipMessage) -> Result<&EventEnvelope> {
    match message {
        GossipMessage::Event(envelope) => Ok(envelope),
        _ => bail!("gossip message is not an event payload"),
    }
}

pub fn summary_announcement_from_gossip(message: &GossipMessage) -> Result<&SummaryAnnouncement> {
    match message {
        GossipMessage::Summary(summary) => Ok(summary),
        _ => bail!("gossip message is not a summary payload"),
    }
}

pub fn event_gossip(envelope: EventEnvelope) -> GossipMessage {
    GossipMessage::Event(envelope)
}

pub fn global_event_gossip(envelope: EventEnvelope) -> Result<GossipMessage> {
    if envelope.scope != SwarmScope::Global {
        bail!("global event gossip requires global scope");
    }
    Ok(event_gossip(envelope))
}

const ENV_P2P_ENABLED: &str = "WATTSWARM_P2P_ENABLED";
const ENV_P2P_MDNS: &str = "WATTSWARM_P2P_MDNS";
const ENV_P2P_PORT: &str = "WATTSWARM_P2P_PORT";
const ENV_P2P_LISTEN_ADDRS: &str = "WATTSWARM_P2P_LISTEN_ADDRS";
const ENV_P2P_BOOTSTRAP_PEERS: &str = "WATTSWARM_P2P_BOOTSTRAP_PEERS";
const DEFAULT_P2P_PORT: u16 = 4001;
const GLOBAL_HIGH_FREQUENCY_WINDOW: Duration = Duration::from_secs(5);
const GLOBAL_HIGH_FREQUENCY_LIMIT: usize = 32;

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

fn merge_scopes(scopes: impl IntoIterator<Item = SwarmScope>) -> Vec<SwarmScope> {
    let mut merged = Vec::new();
    for scope in scopes {
        if !merged.contains(&scope) {
            merged.push(scope);
        }
    }
    if !merged.contains(&SwarmScope::Global) {
        merged.insert(0, SwarmScope::Global);
    }
    merged
}

fn dynamic_subscription_scopes_for_node(node: &Node, node_id: &str) -> Result<Vec<SwarmScope>> {
    let mut scopes = Vec::new();
    for hint in node.store.list_active_feed_subscription_scope_hints(node_id)? {
        if let Some(scope) = parse_scope_hint_string(&hint)
            && !scopes.contains(&scope)
        {
            scopes.push(scope);
        }
    }
    Ok(scopes)
}

fn node_has_active_subscription_scope(node: &Node, node_id: &str, scope: &SwarmScope) -> Result<bool> {
    Ok(dynamic_subscription_scopes_for_node(node, node_id)?
        .into_iter()
        .any(|candidate| candidate == *scope))
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
    let bootstrap_peers = env::var(ENV_P2P_BOOTSTRAP_PEERS)
        .ok()
        .map(|raw| parse_listen_addrs_env(&raw))
        .unwrap_or_default();

    NetworkP2pConfig {
        listen_addrs,
        bootstrap_peers,
        enable_mdns: parse_bool_env_with_default(ENV_P2P_MDNS, true),
        ..NetworkP2pConfig::default()
    }
}

pub fn maybe_start_background_network_service(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> Result<bool> {
    if !network_enabled_from_env() {
        return Ok(false);
    }

    let config = network_config_from_env();
    let scopes = configured_network_scopes_from_env();
    config.validate()?;
    thread::spawn(move || {
        if let Err(err) = run_background_network_service(&state_dir, &db_path, config, scopes) {
            eprintln!("network bridge stopped: {err}");
        }
    });
    Ok(true)
}

fn run_background_network_service(
    state_dir: &Path,
    db_path: &Path,
    config: NetworkP2pConfig,
    configured_scopes: Vec<SwarmScope>,
) -> Result<()> {
    let bootstrap_peers = config.bootstrap_peers.clone();
    let mut node = crate::control::open_node(state_dir, db_path)?;
    let node_id = node.node_id();
    let scopes = merge_scopes(
        configured_scopes
            .into_iter()
            .chain(dynamic_subscription_scopes_for_node(&node, &node_id)?),
    );
    let mut service =
        NetworkBridgeService::new_with_scopes(NetworkP2pNode::generate(config)?, &scopes)?;
    let mut announced_listen = false;
    let mut last_published_seq = node.head_seq()?;
    let mut next_dial_attempt_at = HashMap::new();

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
        if dial_discovered_peer_endpoints(
            &mut service,
            state_dir,
            &node_id,
            &mut next_dial_attempt_at,
        )? > 0
        {
            did_work = true;
        }
        if dial_bootstrap_peer_endpoints(&mut service, &bootstrap_peers, &mut next_dial_attempt_at)? > 0
        {
            did_work = true;
        }
        let new_last_published_seq =
            publish_pending_scoped_updates(&mut service, &node, &node_id, last_published_seq)?;
        if new_last_published_seq != last_published_seq {
            did_work = true;
            last_published_seq = new_last_published_seq;
        }
        if service.run_anti_entropy(&node)? > 0 {
            did_work = true;
        }
        if !did_work {
            thread::sleep(IDLE_NETWORK_SLEEP);
        }
    }
}

pub fn backfill_response_for_request(
    node: &Node,
    request: &BackfillRequest,
    max_limit: usize,
) -> Result<BackfillResponse> {
    request.validate(max_limit)?;
    let mut next_from_event_seq = request.from_event_seq;
    let mut from_event_seq = request.from_event_seq;
    let mut envelopes = Vec::new();

    while envelopes.len() < request.limit {
        let rows = node.store.load_events_page(
            from_event_seq,
            request.limit.saturating_sub(envelopes.len()).max(32),
        )?;
        if rows.is_empty() {
            break;
        }
        for (seq, event) in rows {
            from_event_seq = seq;
            next_from_event_seq = seq;
            if !should_sync_event(node, &event)? {
                continue;
            }
            if event_scope(node, &event)? != request.scope {
                continue;
            }
            envelopes.push(EventEnvelope {
                scope: request.scope.clone(),
                event,
            });
            if envelopes.len() >= request.limit {
                break;
            }
        }
    }

    Ok(BackfillResponse {
        scope: request.scope.clone(),
        next_from_event_seq,
        events: envelopes,
    })
}

pub fn ingest_backfill_response(node: &mut Node, response: &BackfillResponse) -> Result<usize> {
    let mut applied = 0usize;
    for envelope in &response.events {
        if envelope.scope != response.scope {
            return Err(anyhow!("backfill response scope mismatch"));
        }
        if ingest_event_envelope(node, envelope).is_ok() {
            applied += 1;
        }
    }
    Ok(applied)
}

const AUTO_PUBLISH_BATCH_LIMIT: usize = 64;
const DISCOVERY_DIAL_RETRY_AFTER: Duration = Duration::from_secs(3);

fn should_publish_summaries(head_seq: u64, from_event_seq: u64) -> bool {
    head_seq.saturating_sub(from_event_seq) <= SUMMARY_BACKPRESSURE_HIGH_WATERMARK
}

fn latest_scoped_event_seq(node: &Node, scope: &SwarmScope) -> Result<u64> {
    let mut latest = 0u64;
    for (seq, event) in node.store.load_all_events()? {
        if !should_sync_event(node, &event)? {
            continue;
        }
        if event_scope(node, &event)? == *scope {
            latest = seq;
        }
    }
    Ok(latest)
}

fn parse_scope_hint_string(raw: &str) -> Option<SwarmScope> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("global") {
        return Some(SwarmScope::Global);
    }
    let (kind, rest) = trimmed.split_once(':')?;
    let id = rest
        .split([':', '/'])
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if id.is_empty() {
        return None;
    }
    match kind.trim().to_ascii_lowercase().as_str() {
        "region" => Some(SwarmScope::Region(id.to_owned())),
        "local" | "node" => Some(SwarmScope::Node(id.to_owned())),
        _ => None,
    }
}

fn contract_scope(contract: &crate::types::TaskContract) -> SwarmScope {
    if let Some(raw) = contract
        .inputs
        .get("swarm_scope")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_scope_hint_string)
    {
        return raw;
    }
    if let Some(obj) = contract
        .inputs
        .get("swarm_scope")
        .and_then(serde_json::Value::as_object)
    {
        let kind = obj.get("kind").and_then(serde_json::Value::as_str);
        let id = obj.get("id").and_then(serde_json::Value::as_str);
        match (
            kind.map(|v| v.trim().to_ascii_lowercase()),
            id.map(str::trim),
        ) {
            (Some(kind), Some(id)) if !id.is_empty() && kind == "region" => {
                return SwarmScope::Region(id.to_owned());
            }
            (Some(kind), Some(id))
                if !id.is_empty() && matches!(kind.as_str(), "local" | "node") =>
            {
                return SwarmScope::Node(id.to_owned());
            }
            (Some(kind), _) if kind == "global" => return SwarmScope::Global,
            _ => {}
        }
    }
    parse_scope_hint_string(&contract.task_type).unwrap_or(SwarmScope::Global)
}

fn scope_from_hint_or_global(raw: &str) -> SwarmScope {
    parse_scope_hint_string(raw).unwrap_or(SwarmScope::Global)
}

fn event_scope(node: &Node, event: &crate::types::Event) -> Result<SwarmScope> {
    match &event.payload {
        crate::types::EventPayload::TaskCreated(contract) => Ok(contract_scope(contract)),
        crate::types::EventPayload::FeedSubscriptionUpdated(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::TaskAnnounced(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::ExecutionIntentDeclared(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::ExecutionSetConfirmed(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::MembershipUpdated(_)
        | crate::types::EventPayload::PolicyTuned(_)
        | crate::types::EventPayload::CheckpointCreated(_)
        | crate::types::EventPayload::AdvisoryCreated(_)
        | crate::types::EventPayload::AdvisoryApproved(_)
        | crate::types::EventPayload::AdvisoryApplied(_)
        | crate::types::EventPayload::EventRevoked(_)
        | crate::types::EventPayload::SummaryRevoked(_)
        | crate::types::EventPayload::NodePenalized(_) => Ok(SwarmScope::Global),
        _ => {
            if let Some(task_id) = event.task_id.as_deref()
                && let Some(task) = node.task_view(task_id)?
            {
                return Ok(contract_scope(&task.contract));
            }
            Ok(SwarmScope::Global)
        }
    }
}

fn should_sync_event(node: &Node, event: &crate::types::Event) -> Result<bool> {
    if matches!(
        event.payload,
        crate::types::EventPayload::EventRevoked(_)
            | crate::types::EventPayload::SummaryRevoked(_)
            | crate::types::EventPayload::NodePenalized(_)
    ) {
        return Ok(true);
    }
    Ok(!node.store.is_event_revoked(&event.event_id)?)
}

pub fn apply_rule_announcement(node: &mut Node, rule: &crate::network_p2p::RuleAnnouncement) -> Result<()> {
    node.store.put_rule_announcement(
        &rule.scope.label()?,
        &rule.rule_set,
        rule.rule_version,
        rule.activation_epoch,
        observed_at_ms(),
    )
}

pub fn apply_checkpoint_announcement(
    node: &mut Node,
    checkpoint: &crate::network_p2p::CheckpointAnnouncement,
) -> Result<()> {
    node.store.put_checkpoint_announcement(
        &checkpoint.scope.label()?,
        &checkpoint.checkpoint_id,
        &checkpoint.artifact_path,
        observed_at_ms(),
    )
}

fn build_summary_id(
    source_node_id: &str,
    scope: &SwarmScope,
    summary_kind: &str,
    payload: &serde_json::Value,
) -> Result<String> {
    Ok(crate::crypto::sha256_hex(&serde_json::to_vec(
        &serde_json::json!({
            "source_node_id": source_node_id,
            "scope": scope,
            "summary_kind": summary_kind,
            "payload": payload,
        }),
    )?))
}

pub fn build_knowledge_summary_for_task_type(
    node: &Node,
    scope: &SwarmScope,
    task_type: &str,
) -> Result<Option<SummaryAnnouncement>> {
    let decisions = node
        .store
        .list_local_decision_memory_hits_by_task_type(task_type, SUMMARY_DECISION_MEMORY_LIMIT)?;
    if decisions.is_empty() {
        return Ok(None);
    }
    let source_node_id = node.node_id();
    let payload = serde_json::to_value(KnowledgeSummaryBundle {
        source_node_id: source_node_id.clone(),
        task_type: task_type.to_owned(),
        decisions,
    })?;
    Ok(Some(SummaryAnnouncement {
        summary_id: build_summary_id(&source_node_id, scope, KNOWLEDGE_SUMMARY_KIND, &payload)?,
        source_node_id,
        scope: scope.clone(),
        summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload,
    }))
}

fn knowledge_summary_for_event(
    node: &Node,
    event: &crate::types::Event,
    scope: &SwarmScope,
) -> Result<Option<SummaryAnnouncement>> {
    let Some(task_id) = event.task_id.as_deref() else {
        return Ok(None);
    };
    if !matches!(
        event.payload,
        crate::types::EventPayload::DecisionFinalized(_)
    ) {
        return Ok(None);
    }
    let Some(task) = node.task_view(task_id)? else {
        return Ok(None);
    };
    build_knowledge_summary_for_task_type(node, scope, &task.contract.task_type)
}

pub fn build_reputation_summary_for_runtime(
    node: &Node,
    runtime_id: &str,
    profile_id: &str,
) -> Result<Option<SummaryAnnouncement>> {
    let Some(entry) = node
        .store
        .get_local_reputation_snapshot(runtime_id, profile_id)?
    else {
        return Ok(None);
    };
    let source_node_id = node.node_id();
    let payload = serde_json::to_value(ReputationSummaryBundle {
        source_node_id: source_node_id.clone(),
        entries: vec![entry],
    })?;
    Ok(Some(SummaryAnnouncement {
        summary_id: build_summary_id(
            &source_node_id,
            &SwarmScope::Global,
            REPUTATION_SUMMARY_KIND,
            &payload,
        )?,
        source_node_id,
        scope: SwarmScope::Global,
        summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload,
    }))
}

fn reputation_summary_for_event(
    node: &Node,
    event: &crate::types::Event,
) -> Result<Option<SummaryAnnouncement>> {
    let crate::types::EventPayload::VerifierResultSubmitted(payload) = &event.payload else {
        return Ok(None);
    };
    build_reputation_summary_for_runtime(
        node,
        &payload.result.provider_family,
        &payload.result.model_id,
    )
}

pub fn apply_summary_announcement(node: &mut Node, summary: &SummaryAnnouncement) -> Result<()> {
    if node.store.is_summary_revoked(&summary.summary_id)?
        || node.store.is_node_penalized(&summary.source_node_id)?
    {
        return Ok(());
    }
    match summary.summary_kind.as_str() {
        KNOWLEDGE_SUMMARY_KIND => {
            let payload: KnowledgeSummaryBundle = serde_json::from_value(summary.payload.clone())?;
            for decision in payload.decisions {
                node.store.put_imported_decision_memory(
                    &summary.summary_id,
                    &summary.source_node_id,
                    &decision,
                )?;
            }
        }
        REPUTATION_SUMMARY_KIND => {
            let payload: ReputationSummaryBundle = serde_json::from_value(summary.payload.clone())?;
            for entry in payload.entries {
                node.store.put_imported_reputation_snapshot(
                    &summary.summary_id,
                    &summary.source_node_id,
                    &entry,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_high_frequency_global_event(payload: &crate::types::EventPayload) -> bool {
    matches!(
        payload,
        crate::types::EventPayload::TaskClaimed(_)
            | crate::types::EventPayload::TaskClaimRenewed(_)
            | crate::types::EventPayload::TaskClaimReleased(_)
            | crate::types::EventPayload::CandidateProposed(_)
            | crate::types::EventPayload::EvidenceAdded(_)
            | crate::types::EventPayload::EvidenceAvailable(_)
            | crate::types::EventPayload::VerifierResultSubmitted(_)
            | crate::types::EventPayload::VoteCommit(_)
            | crate::types::EventPayload::VoteReveal(_)
            | crate::types::EventPayload::DecisionCommitted(_)
    )
}

pub fn publish_pending_scoped_updates(
    service: &mut NetworkBridgeService,
    node: &Node,
    local_node_id: &str,
    from_event_seq: u64,
) -> Result<u64> {
    let rows = node
        .store
        .load_events_page(from_event_seq, AUTO_PUBLISH_BATCH_LIMIT)?;
    let mut last_published_seq = from_event_seq;
    let local_node_penalized = node.store.is_node_penalized(local_node_id)?;
    let publish_summaries = should_publish_summaries(node.head_seq()?, from_event_seq);
    for (seq, event) in rows {
        if event.author_node_id != local_node_id {
            last_published_seq = seq;
            continue;
        }
        if !should_sync_event(node, &event)? {
            last_published_seq = seq;
            continue;
        }
        let scope = event_scope(node, &event)?;
        if let crate::types::EventPayload::FeedSubscriptionUpdated(payload) = &event.payload
            && payload.subscriber_node_id == local_node_id
        {
            if payload.active {
                service.subscribe_scope(&scope)?;
            } else if !node_has_active_subscription_scope(node, local_node_id, &scope)? {
                service.unsubscribe_scope(&scope)?;
            }
        }
        let is_local_subscription_control_event = matches!(
            &event.payload,
            crate::types::EventPayload::FeedSubscriptionUpdated(_)
        );
        match service.publish_event_for_scope(&scope, event.clone()) {
            Ok(()) => last_published_seq = seq,
            Err(err)
                if err.to_string().contains("NoPeersSubscribedToTopic")
                    && is_local_subscription_control_event =>
            {
                last_published_seq = seq;
                continue;
            }
            Err(err) if err.to_string().contains("NoPeersSubscribedToTopic") => break,
            Err(err) if err.to_string().contains("GlobalTopicRateLimited") => break,
            Err(err) if err.to_string().contains("LocalTopicRateLimited") => break,
            Err(err) => return Err(err),
        }
        if publish_summaries
            && !local_node_penalized
            && let Some(summary) = knowledge_summary_for_event(node, &event, &scope)?
        {
            let _ = service.publish_summary(summary);
        }
        if publish_summaries
            && !local_node_penalized
            && let Some(summary) = reputation_summary_for_event(node, &event)?
        {
            let _ = service.publish_summary(summary);
        }
    }
    Ok(last_published_seq)
}

pub fn publish_pending_global_events(
    service: &mut NetworkBridgeService,
    node: &Node,
    local_node_id: &str,
    from_event_seq: u64,
) -> Result<u64> {
    publish_pending_scoped_updates(service, node, local_node_id, from_event_seq)
}

pub fn dial_discovered_peer_endpoints(
    service: &mut NetworkBridgeService,
    state_dir: &Path,
    local_node_id: &str,
    next_attempt_at: &mut HashMap<String, std::time::Instant>,
) -> Result<usize> {
    let records = load_discovered_peer_records(&crate::control::discovered_peers_path(state_dir))?;
    let now = std::time::Instant::now();
    let mut dialed = 0usize;
    for record in records {
        if record.node_id == local_node_id {
            continue;
        }
        let Some(raw_addr) = record.listen_addr.as_deref() else {
            continue;
        };
        let addr = match raw_addr.parse::<Multiaddr>() {
            Ok(addr) => addr,
            Err(_) => continue,
        };
        if next_attempt_at
            .get(raw_addr)
            .is_some_and(|deadline| *deadline > now)
        {
            continue;
        }
        next_attempt_at.insert(raw_addr.to_owned(), now + DISCOVERY_DIAL_RETRY_AFTER);
        match service.dial(addr) {
            Ok(()) => dialed += 1,
            Err(err)
                if err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("duplicate connection") => {}
            Err(err) => eprintln!("p2p dial failed for {raw_addr}: {err}"),
        }
    }
    Ok(dialed)
}

pub fn dial_bootstrap_peer_endpoints(
    service: &mut NetworkBridgeService,
    bootstrap_peers: &[String],
    next_attempt_at: &mut HashMap<String, std::time::Instant>,
) -> Result<usize> {
    let now = std::time::Instant::now();
    let mut dialed = 0usize;
    for raw_addr in bootstrap_peers {
        if next_attempt_at
            .get(raw_addr)
            .is_some_and(|deadline| *deadline > now)
        {
            continue;
        }
        let addr = match raw_addr.parse::<Multiaddr>() {
            Ok(addr) => addr,
            Err(_) => continue,
        };
        next_attempt_at.insert(raw_addr.clone(), now + DISCOVERY_DIAL_RETRY_AFTER);
        match service.dial(addr) {
            Ok(()) => dialed += 1,
            Err(err)
                if err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("duplicate connection") => {}
            Err(err) => eprintln!("bootstrap dial failed for {raw_addr}: {err}"),
        }
    }
    Ok(dialed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBridgeTick {
    Listening {
        address: Multiaddr,
    },
    TransportNotice {
        detail: String,
    },
    Connected {
        peer: PeerId,
    },
    Disconnected {
        peer: PeerId,
    },
    EventIngested {
        peer: PeerId,
        event_id: String,
    },
    SummaryApplied {
        peer: PeerId,
        summary_kind: String,
    },
    RuleApplied {
        peer: PeerId,
        rule_set: String,
        rule_version: u64,
    },
    CheckpointApplied {
        peer: PeerId,
        checkpoint_id: String,
    },
    GossipIgnored {
        peer: PeerId,
        message_kind: String,
    },
    BackfillServed {
        peer: PeerId,
        events: usize,
    },
    BackfillApplied {
        peer: PeerId,
        request_id: BackfillRequestId,
        events: usize,
    },
    BackfillFailed {
        peer: PeerId,
        request_id: BackfillRequestId,
        error: String,
    },
}

#[derive(Debug, Clone)]
struct GlobalPublishRateGuard {
    window_started_at: Instant,
    high_frequency_events_in_window: usize,
}

impl GlobalPublishRateGuard {
    fn new(now: Instant) -> Self {
        Self {
            window_started_at: now,
            high_frequency_events_in_window: 0,
        }
    }

    fn allow(&mut self, scope: &SwarmScope, event: &crate::types::Event) -> bool {
        if *scope != SwarmScope::Global || !is_high_frequency_global_event(&event.payload) {
            return true;
        }
        let now = Instant::now();
        if now.duration_since(self.window_started_at) >= GLOBAL_HIGH_FREQUENCY_WINDOW {
            self.window_started_at = now;
            self.high_frequency_events_in_window = 0;
        }
        if self.high_frequency_events_in_window >= GLOBAL_HIGH_FREQUENCY_LIMIT {
            return false;
        }
        self.high_frequency_events_in_window += 1;
        true
    }
}

pub struct NetworkBridgeService {
    runtime: NetworkRuntime,
    tokio_runtime: Runtime,
    subscribed_scopes: Vec<SwarmScope>,
    pinned_scopes: Vec<SwarmScope>,
    peer_sync_state: HashMap<PeerId, PeerSyncState>,
    connected_peers: HashSet<PeerId>,
    global_publish_rate_guard: GlobalPublishRateGuard,
}

impl NetworkBridgeService {
    pub fn new(node: NetworkP2pNode) -> Result<Self> {
        Self::new_with_scopes(node, &[SwarmScope::Global])
    }

    pub fn new_with_scopes(node: NetworkP2pNode, scopes: &[SwarmScope]) -> Result<Self> {
        let tokio_runtime = Runtime::new()?;
        let mut runtime = tokio_runtime.block_on(async { NetworkRuntime::new(node) })?;
        let mut subscribed_scopes = Vec::new();
        for scope in scopes {
            runtime.subscribe_scope(scope)?;
            if !subscribed_scopes.contains(scope) {
                subscribed_scopes.push(scope.clone());
            }
        }
        if subscribed_scopes.is_empty() {
            runtime.subscribe_scope(&SwarmScope::Global)?;
            subscribed_scopes.push(SwarmScope::Global);
        }
        Ok(Self {
            runtime,
            tokio_runtime,
            pinned_scopes: subscribed_scopes.clone(),
            subscribed_scopes,
            peer_sync_state: HashMap::new(),
            connected_peers: HashSet::new(),
            global_publish_rate_guard: GlobalPublishRateGuard::new(Instant::now()),
        })
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.runtime.local_peer_id()
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        self.runtime.listen_addrs()
    }

    pub fn subscribed_scopes(&self) -> &[SwarmScope] {
        &self.subscribed_scopes
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.subscribe_scope(scope)?;
        if !self.subscribed_scopes.contains(scope) {
            self.subscribed_scopes.push(scope.clone());
        }
        Ok(())
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        if *scope == SwarmScope::Global
            || self.pinned_scopes.contains(scope)
            || !self.subscribed_scopes.contains(scope)
        {
            return Ok(());
        }
        let _guard = self.tokio_runtime.enter();
        self.runtime.unsubscribe_scope(scope)?;
        self.subscribed_scopes.retain(|existing| existing != scope);
        Ok(())
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.dial(addr)
    }

    pub fn publish_event_for_scope(
        &mut self,
        scope: &SwarmScope,
        event: crate::types::Event,
    ) -> Result<()> {
        if !self.global_publish_rate_guard.allow(scope, &event) {
            bail!("GlobalTopicRateLimited");
        }
        self.runtime.publish_gossip(&event_gossip(EventEnvelope {
            scope: scope.clone(),
            event,
        }))
    }

    pub fn publish_global_event(&mut self, event: crate::types::Event) -> Result<()> {
        self.publish_event_for_scope(&SwarmScope::Global, event)
    }

    pub fn publish_summary(&mut self, summary: SummaryAnnouncement) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Summary(summary))
    }

    pub fn request_global_backfill(
        &mut self,
        peer: &PeerId,
        from_event_seq: u64,
        limit: usize,
    ) -> Result<BackfillRequestId> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.send_backfill_request(
            peer,
            BackfillRequest {
                scope: SwarmScope::Global,
                from_event_seq,
                limit,
            },
        )
    }

    pub fn tick(&mut self, node: &mut Node) -> Result<NetworkBridgeTick> {
        let event = self.tokio_runtime.block_on(self.runtime.next_event());
        self.handle_runtime_event(node, event)
    }

    fn request_backfill_for_peer(&mut self, peer: &PeerId, node: &Node) -> Result<bool> {
        let scopes = self.scopes_to_request_for_peer(peer);
        self.request_backfill_scopes_for_peer(peer, node, &scopes)
    }

    fn request_backfill_scopes_for_peer(
        &mut self,
        peer: &PeerId,
        node: &Node,
        scopes: &[SwarmScope],
    ) -> Result<bool> {
        let now = Instant::now();
        let state = self
            .peer_sync_state
            .entry(*peer)
            .or_insert_with(|| PeerSyncState::new(now));
        if state.inflight_backfills >= MAX_INFLIGHT_BACKFILLS_PER_PEER || state.next_retry_at > now
        {
            return Ok(false);
        }
        if scopes.is_empty() {
            return Ok(false);
        }
        let mut requests_sent = 0usize;
        for scope in scopes.iter().cloned() {
            let from_event_seq = latest_scoped_event_seq(node, &scope)?;
            let _guard = self.tokio_runtime.enter();
            let _ = self.runtime.send_backfill_request(
                peer,
                BackfillRequest {
                    scope,
                    from_event_seq,
                    limit: BACKFILL_BATCH_EVENTS,
                },
            )?;
            requests_sent += 1;
        }
        state.inflight_backfills += requests_sent;
        state.last_backfill_request_at = Some(now);
        state.next_retry_at = now + BACKFILL_RETRY_AFTER;
        Ok(requests_sent > 0)
    }

    pub fn run_anti_entropy(&mut self, node: &Node) -> Result<usize> {
        let now = Instant::now();
        let mut requested = 0usize;
        let mut scopes = self.subscribed_scopes.clone();
        scopes.sort_by_key(|scope| matches!(scope, SwarmScope::Global));
        let mut requests_by_peer: HashMap<PeerId, Vec<SwarmScope>> = HashMap::new();
        for scope in scopes {
            let Some(peer) = self.preferred_backfill_peer_for_scope(&scope, now) else {
                continue;
            };
            requests_by_peer.entry(peer).or_default().push(scope);
        }
        for (peer, scopes) in requests_by_peer {
            if self.request_backfill_scopes_for_peer(&peer, node, &scopes)? {
                requested += 1;
            }
        }
        Ok(requested)
    }

    fn scopes_to_request_for_peer(&self, peer: &PeerId) -> Vec<SwarmScope> {
        let Some(state) = self.peer_sync_state.get(peer) else {
            return self.subscribed_scopes.clone();
        };
        if state.known_scopes.is_empty() {
            return self.subscribed_scopes.clone();
        }
        self.subscribed_scopes
            .iter()
            .filter(|scope| state.known_scopes.contains(*scope))
            .cloned()
            .collect()
    }

    fn preferred_backfill_peer_for_scope(&self, scope: &SwarmScope, now: Instant) -> Option<PeerId> {
        let peers = self.connected_peers.iter().copied().collect::<Vec<_>>();
        peers
            .iter()
            .copied()
            .find(|peer| self.peer_is_eligible_for_scope(peer, scope, now, true))
            .or_else(|| {
                peers.into_iter()
                    .find(|peer| self.peer_is_eligible_for_scope(peer, scope, now, false))
            })
    }

    fn peer_is_eligible_for_scope(
        &self,
        peer: &PeerId,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> bool {
        match self.peer_sync_state.get(peer) {
            Some(state) => {
                if require_known_scope && !state.known_scopes.contains(scope) {
                    return false;
                }
                state.inflight_backfills < MAX_INFLIGHT_BACKFILLS_PER_PEER
                    && state.next_retry_at <= now
                    && state.last_backfill_request_at.map_or(true, |last| {
                        now.duration_since(last) >= ANTI_ENTROPY_INTERVAL
                    })
            }
            None => !require_known_scope,
        }
    }

    fn record_peer_scope_activity(&mut self, peer: PeerId, scope: &SwarmScope) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .known_scopes
            .insert(scope.clone());
    }

    fn mark_peer_connected(&mut self, peer: PeerId) {
        self.connected_peers.insert(peer);
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()));
    }

    fn mark_peer_disconnected(&mut self, peer: PeerId) {
        self.connected_peers.remove(&peer);
        self.peer_sync_state.remove(&peer);
    }

    fn mark_backfill_completed(&mut self, peer: PeerId) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfills = state.inflight_backfills.saturating_sub(1);
            state.next_retry_at = Instant::now() + ANTI_ENTROPY_INTERVAL;
        }
    }

    fn mark_backfill_failed(&mut self, peer: PeerId) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfills = state.inflight_backfills.saturating_sub(1);
            state.next_retry_at = Instant::now() + BACKFILL_RETRY_AFTER;
        }
    }

    pub fn try_tick(&mut self, node: &mut Node) -> Result<Option<NetworkBridgeTick>> {
        let event = {
            let _guard = self.tokio_runtime.enter();
            self.runtime.try_next_event()?
        };
        let Some(event) = event else {
            return Ok(None);
        };
        Ok(Some(self.process_runtime_event(node, event)?))
    }

    fn handle_runtime_event(
        &mut self,
        node: &mut Node,
        event: Result<NetworkRuntimeEvent>,
    ) -> Result<NetworkBridgeTick> {
        self.process_runtime_event(node, event?)
    }

    fn process_runtime_event(
        &mut self,
        node: &mut Node,
        event: NetworkRuntimeEvent,
    ) -> Result<NetworkBridgeTick> {
        match event {
            NetworkRuntimeEvent::NewListenAddr { address } => {
                Ok(NetworkBridgeTick::Listening { address })
            }
            NetworkRuntimeEvent::NatStatusChanged {
                old,
                new,
                public_address,
                confidence,
            } => Ok(NetworkBridgeTick::TransportNotice {
                detail: format!(
                    "nat_status_changed old={old} new={new} public_address={} confidence={confidence}",
                    public_address
                        .map(|addr| addr.to_string())
                        .unwrap_or_else(|| "none".to_owned())
                ),
            }),
            NetworkRuntimeEvent::RelayReservationAccepted {
                relay_peer,
                renewal,
            } => Ok(NetworkBridgeTick::TransportNotice {
                detail: format!(
                    "relay_reservation_accepted relay_peer={relay_peer} renewal={renewal}"
                ),
            }),
            NetworkRuntimeEvent::RelayCircuitEstablished { relay_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("relay_circuit_established relay_peer={relay_peer}"),
                })
            }
            NetworkRuntimeEvent::RelayInboundCircuitEstablished { source_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("relay_inbound_circuit_established source_peer={source_peer}"),
                })
            }
            NetworkRuntimeEvent::DcutrConnectionUpgradeSucceeded { remote_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("dcutr_upgrade_succeeded remote_peer={remote_peer}"),
                })
            }
            NetworkRuntimeEvent::DcutrConnectionUpgradeFailed { remote_peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "dcutr_upgrade_failed remote_peer={remote_peer} error={error}"
                    ),
                })
            }
            NetworkRuntimeEvent::ConnectionEstablished { peer } => {
                self.mark_peer_connected(peer);
                let _ = self.request_backfill_for_peer(&peer, node)?;
                if !node.store.is_node_penalized(&node.node_id())? {
                    for entry in node
                        .store
                        .list_local_reputation_snapshots(SUMMARY_REPUTATION_LIMIT)?
                    {
                        if let Some(summary) = build_reputation_summary_for_runtime(
                            node,
                            &entry.runtime_id,
                            &entry.profile_id,
                        )? {
                            let _ = self.publish_summary(summary);
                        }
                    }
                }
                Ok(NetworkBridgeTick::Connected { peer })
            }
            NetworkRuntimeEvent::ConnectionClosed { peer } => {
                self.mark_peer_disconnected(peer);
                Ok(NetworkBridgeTick::Disconnected { peer })
            }
            NetworkRuntimeEvent::Gossip {
                propagation_source,
                message,
            } => match message {
                GossipMessage::Event(envelope) => {
                    self.record_peer_scope_activity(propagation_source, &envelope.scope);
                    ingest_event_envelope(node, &envelope)?;
                    Ok(NetworkBridgeTick::EventIngested {
                        peer: propagation_source,
                        event_id: envelope.event.event_id.clone(),
                    })
                }
                GossipMessage::Summary(summary) => {
                    self.record_peer_scope_activity(propagation_source, &summary.scope);
                    apply_summary_announcement(node, &summary)?;
                    Ok(NetworkBridgeTick::SummaryApplied {
                        peer: propagation_source,
                        summary_kind: summary.summary_kind,
                    })
                }
                GossipMessage::Rule(rule) => {
                    self.record_peer_scope_activity(propagation_source, &rule.scope);
                    apply_rule_announcement(node, &rule)?;
                    Ok(NetworkBridgeTick::RuleApplied {
                        peer: propagation_source,
                        rule_set: rule.rule_set,
                        rule_version: rule.rule_version,
                    })
                }
                GossipMessage::Checkpoint(checkpoint) => {
                    self.record_peer_scope_activity(propagation_source, &checkpoint.scope);
                    apply_checkpoint_announcement(node, &checkpoint)?;
                    Ok(NetworkBridgeTick::CheckpointApplied {
                        peer: propagation_source,
                        checkpoint_id: checkpoint.checkpoint_id,
                    })
                }
            },
            NetworkRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            } => {
                let response = backfill_response_for_request(
                    node,
                    &request,
                    self.runtime.config().max_backfill_events,
                )?;
                let events = response.events.len();
                self.runtime.send_backfill_response(channel, response)?;
                Ok(NetworkBridgeTick::BackfillServed { peer, events })
            }
            NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response,
            } => {
                self.record_peer_scope_activity(peer, &response.scope);
                self.mark_backfill_completed(peer);
                Ok(NetworkBridgeTick::BackfillApplied {
                    peer,
                    request_id,
                    events: ingest_backfill_response(node, &response)?,
                })
            }
            NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                self.mark_backfill_failed(peer);
                Ok(NetworkBridgeTick::BackfillFailed {
                    peer,
                    request_id,
                    error,
                })
            }
            NetworkRuntimeEvent::BackfillInboundFailure { peer, error } => {
                Err(anyhow!("backfill inbound failure from {peer}: {error}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::NodeIdentity;
    use crate::node::build_event_for_external;
    use crate::storage::PgStore;
    use crate::types::{Membership, Role};
    use crate::{node::Node, task_template::sample_contract};
    use serde_json::json;
    use std::collections::HashMap;
    use std::env;
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn lock_env_test_mutex() -> std::sync::MutexGuard<'static, ()> {
        env_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    #[test]
    fn ingest_event_envelope_applies_remote_event_to_local_node() {
        let local = NodeIdentity::random();
        let remote = NodeIdentity::random();
        let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
        let mut node =
            Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");

        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("policy binding")
            .policy_hash;
        let mut contract = sample_contract("task-network-1", policy_hash);
        contract.inputs = json!({"prompt":"hello from network"});
        let remote_event = build_event_for_external(
            &remote,
            1,
            10,
            crate::types::EventPayload::TaskCreated(contract.clone()),
        )
        .expect("signed event");
        let envelope = EventEnvelope {
            scope: SwarmScope::Global,
            event: remote_event,
        };

        ingest_event_envelope(&mut node, &envelope).expect("ingest envelope");

        let task = node
            .task_view("task-network-1")
            .expect("task view")
            .expect("task exists");
        assert_eq!(task.contract.task_id, "task-network-1");
        assert_eq!(task.contract.inputs["prompt"], json!("hello from network"));
    }

    #[test]
    fn event_envelope_from_gossip_rejects_non_event_messages() {
        let message = GossipMessage::Rule(crate::network_p2p::RuleAnnouncement {
            scope: SwarmScope::Global,
            rule_set: "galaxy".to_owned(),
            rule_version: 2,
            activation_epoch: Some(7),
        });
        assert!(event_envelope_from_gossip(&message).is_err());
    }

    #[test]
    fn global_event_gossip_rejects_non_global_scope() {
        let envelope = EventEnvelope {
            scope: SwarmScope::Region("sol-1".to_owned()),
            event: build_event_for_external(
                &NodeIdentity::random(),
                1,
                10,
                crate::types::EventPayload::CheckpointCreated(
                    crate::types::CheckpointCreatedPayload {
                        checkpoint_id: "cp-1".to_owned(),
                        up_to_seq: 1,
                    },
                ),
            )
            .expect("event"),
        };

        assert!(global_event_gossip(envelope).is_err());
    }

    #[test]
    fn backfill_response_for_request_wraps_global_events() {
        let local = NodeIdentity::random();
        let remote = NodeIdentity::random();
        let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
        let mut node =
            Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("policy binding")
            .policy_hash;
        let mut contract = sample_contract("task-backfill-1", policy_hash);
        contract.inputs = json!({"prompt":"backfill me"});
        node.submit_task(contract, 1, 100).expect("submit task");

        let response = backfill_response_for_request(
            &node,
            &BackfillRequest {
                scope: SwarmScope::Global,
                from_event_seq: 0,
                limit: 8,
            },
            32,
        )
        .expect("backfill response");

        assert_eq!(response.events.len(), 1);
        assert_eq!(response.scope, SwarmScope::Global);
        assert_eq!(response.next_from_event_seq, 1);
    }

    #[test]
    fn ingest_backfill_response_rejects_scope_mismatch() {
        let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
        let response = BackfillResponse {
            scope: SwarmScope::Global,
            next_from_event_seq: 1,
            events: vec![EventEnvelope {
                scope: SwarmScope::Region("sol".to_owned()),
                event: build_event_for_external(
                    &NodeIdentity::random(),
                    1,
                    10,
                    crate::types::EventPayload::CheckpointCreated(
                        crate::types::CheckpointCreatedPayload {
                            checkpoint_id: "cp-2".to_owned(),
                            up_to_seq: 0,
                        },
                    ),
                )
                .expect("event"),
            }],
        };

        assert!(ingest_backfill_response(&mut node, &response).is_err());
    }

    #[test]
    fn network_config_defaults_to_enabled_with_fixed_tcp_port() {
        let _lock = lock_env_test_mutex();
        let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, None);
        let _mdns = EnvVarGuard::set(ENV_P2P_MDNS, None);
        let _port = EnvVarGuard::set(ENV_P2P_PORT, None);
        let _listen = EnvVarGuard::set(ENV_P2P_LISTEN_ADDRS, None);
        let _bootstrap = EnvVarGuard::set(ENV_P2P_BOOTSTRAP_PEERS, None);
        assert!(network_enabled_from_env());
        let config = network_config_from_env();
        assert!(config.enable_mdns);
        assert_eq!(config.listen_addrs, vec!["/ip4/0.0.0.0/tcp/4001"]);
        assert!(config.bootstrap_peers.is_empty());
    }

    #[test]
    fn network_enabled_can_be_explicitly_disabled() {
        let _lock = lock_env_test_mutex();
        let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, Some("false"));
        assert!(!network_enabled_from_env());
    }

    #[test]
    fn configured_network_scopes_include_global_by_default() {
        let _lock = lock_env_test_mutex();
        let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, None);
        let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, None);
        let _nodes = EnvVarGuard::set(ENV_P2P_NODE_IDS, None);
        assert_eq!(configured_network_scopes_from_env(), vec![SwarmScope::Global]);
    }

    #[test]
    fn configured_network_scopes_include_region_and_node_aliases() {
        let _lock = lock_env_test_mutex();
        let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, Some("sol-1,sol-2"));
        let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, Some("lab-a"));
        let _nodes = EnvVarGuard::set(ENV_P2P_NODE_IDS, Some("lab-b"));
        let scopes = configured_network_scopes_from_env();
        assert_eq!(scopes.len(), 5);
        assert_eq!(scopes[0], SwarmScope::Global);
        assert!(scopes.contains(&SwarmScope::Region("sol-1".to_owned())));
        assert!(scopes.contains(&SwarmScope::Region("sol-2".to_owned())));
        assert!(scopes.contains(&SwarmScope::Node("lab-a".to_owned())));
        assert!(scopes.contains(&SwarmScope::Node("lab-b".to_owned())));
    }

    #[test]
    fn dynamic_subscription_scopes_merge_with_configured_scopes() {
        let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
        let node_id = node.node_id();
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    subscriber_node_id: node_id.clone(),
                    feed_key: "market.alpha".to_owned(),
                    scope_hint: "region:sol-1".to_owned(),
                    active: true,
                },
            ),
            100,
        )
        .expect("subscription event");

        let scopes = merge_scopes(
            configured_network_scopes_from_env()
                .into_iter()
                .chain(dynamic_subscription_scopes_for_node(&node, &node_id).expect("scopes")),
        );
        assert!(scopes.contains(&SwarmScope::Global));
        assert!(scopes.contains(&SwarmScope::Region("sol-1".to_owned())));
    }

    #[test]
    fn publish_pending_updates_subscribes_runtime_for_local_feed_subscription() {
        let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
        let local_node_id = node.node_id();
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    subscriber_node_id: local_node_id.clone(),
                    feed_key: "market.beta".to_owned(),
                    scope_hint: "node:lab-9".to_owned(),
                    active: true,
                },
            ),
            100,
        )
        .expect("subscription event");

        let mut service = NetworkBridgeService::new(
            NetworkP2pNode::generate(NetworkP2pConfig {
                listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
                enable_mdns: false,
                ..NetworkP2pConfig::default()
            })
            .expect("network node"),
        )
        .expect("service");

        publish_pending_scoped_updates(&mut service, &node, &local_node_id, 0)
            .expect("publish pending updates");

        assert!(
            service
                .subscribed_scopes()
                .contains(&SwarmScope::Node("lab-9".to_owned()))
        );
    }

    #[test]
    fn publish_pending_updates_unsubscribes_scope_when_local_subscription_is_disabled() {
        let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
        let local_node_id = node.node_id();
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    subscriber_node_id: local_node_id.clone(),
                    feed_key: "market.gamma".to_owned(),
                    scope_hint: "region:sol-8".to_owned(),
                    active: true,
                },
            ),
            100,
        )
        .expect("subscription on");
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    subscriber_node_id: local_node_id.clone(),
                    feed_key: "market.gamma".to_owned(),
                    scope_hint: "region:sol-8".to_owned(),
                    active: false,
                },
            ),
            101,
        )
        .expect("subscription off");

        let mut service = NetworkBridgeService::new(
            NetworkP2pNode::generate(NetworkP2pConfig {
                listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
                enable_mdns: false,
                ..NetworkP2pConfig::default()
            })
            .expect("network node"),
        )
        .expect("service");

        publish_pending_scoped_updates(&mut service, &node, &local_node_id, 0)
            .expect("publish pending updates");

        assert!(
            !service
                .subscribed_scopes()
                .contains(&SwarmScope::Region("sol-8".to_owned()))
        );
    }

    #[test]
    fn invalid_scope_hints_are_rejected_for_network_substrate_events() {
        let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
        let subscriber_node_id = node.node_id();

        let result = node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    subscriber_node_id,
                    feed_key: "market.invalid".to_owned(),
                    scope_hint: "bad-scope".to_owned(),
                    active: true,
                },
            ),
            100,
        );

        assert!(result.is_err());
    }

    #[test]
    fn task_announcement_event_persists_summary_and_detail_reference() {
        let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
        node.emit_at(
            1,
            crate::types::EventPayload::TaskAnnounced(crate::types::TaskAnnouncedPayload {
                task_id: "task-announced-1".to_owned(),
                announcement_id: "announce-1".to_owned(),
                feed_key: "venue.market".to_owned(),
                scope_hint: "region:sol-2".to_owned(),
                summary: json!({"reward": 42, "headline": "Explore relay beacon"}),
                detail_ref: Some(crate::types::ArtifactRef {
                    uri: "ipfs://task-detail-1".to_owned(),
                    digest: "digest-task-detail-1".to_owned(),
                    size_bytes: 128,
                    mime: "application/json".to_owned(),
                    created_at: 100,
                    producer: node.node_id(),
                }),
            }),
            100,
        )
        .expect("announce task");

        let announcement = node
            .store
            .get_task_announcement("announce-1")
            .expect("load announcement")
            .expect("announcement exists");
        assert_eq!(announcement.task_id, "task-announced-1");
        assert_eq!(announcement.feed_key, "venue.market");
        assert_eq!(announcement.scope_hint, "region:sol-2");
        assert_eq!(announcement.summary["reward"], json!(42));
        assert_eq!(
            announcement.detail_ref.expect("detail ref").uri,
            "ipfs://task-detail-1"
        );
    }

    #[test]
    fn execution_set_events_persist_intent_and_confirmation() {
        let mut intent_node = Node::open_in_memory_with_roles(&[]).expect("intent node");
        let participant_node_id = intent_node.node_id();
        intent_node
            .emit_at(
                1,
                crate::types::EventPayload::ExecutionIntentDeclared(
                    crate::types::ExecutionIntentDeclaredPayload {
                        task_id: "task-execution-1".to_owned(),
                        execution_set_id: "exec-set-1".to_owned(),
                        participant_node_id: participant_node_id.clone(),
                        role_hint: "writer".to_owned(),
                        scope_hint: "region:sol-3".to_owned(),
                        intent: "interested".to_owned(),
                    },
                ),
                100,
            )
            .expect("intent event");

        let members = intent_node
            .store
            .list_execution_set_members("task-execution-1", "exec-set-1")
            .expect("members after intent");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].status, "interested");

        let mut confirmer = Node::open_in_memory_with_roles(&[Role::Committer]).expect("confirmer");
        let confirmed_by_node_id = confirmer.node_id();
        confirmer
            .emit_at(
                1,
                crate::types::EventPayload::ExecutionSetConfirmed(
                    crate::types::ExecutionSetConfirmedPayload {
                        task_id: "task-execution-1".to_owned(),
                        execution_set_id: "exec-set-1".to_owned(),
                        confirmed_by_node_id: confirmed_by_node_id.clone(),
                        scope_hint: "region:sol-3".to_owned(),
                        members: vec![crate::types::ExecutionSetMember {
                            participant_node_id: "peer-a".to_owned(),
                            role_hint: "writer".to_owned(),
                        }],
                    },
                ),
                101,
            )
            .expect("confirmation event");

        let members = confirmer
            .store
            .list_execution_set_members("task-execution-1", "exec-set-1")
            .expect("members after confirmation");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].status, "confirmed");
        assert_eq!(members[0].confirmed_by_node_id.as_deref(), Some(confirmed_by_node_id.as_str()));
    }

    #[test]
    fn rule_and_checkpoint_gossip_are_applied_to_projection_store() {
        let mut node = Node::open_in_memory_with_roles(&[]).expect("node");

        apply_rule_announcement(
            &mut node,
            &crate::network_p2p::RuleAnnouncement {
                scope: SwarmScope::Region("sol-4".to_owned()),
                rule_set: "market-routing".to_owned(),
                rule_version: 3,
                activation_epoch: Some(9),
            },
        )
        .expect("apply rule");
        apply_checkpoint_announcement(
            &mut node,
            &crate::network_p2p::CheckpointAnnouncement {
                scope: SwarmScope::Region("sol-4".to_owned()),
                checkpoint_id: "cp-sol-4".to_owned(),
                artifact_path: "ipfs://checkpoint-sol-4".to_owned(),
            },
        )
        .expect("apply checkpoint");

        let rule = node
            .store
            .latest_rule_announcement("region.sol-4", "market-routing")
            .expect("load rule")
            .expect("rule exists");
        assert_eq!(rule.rule_version, 3);
        assert_eq!(rule.activation_epoch, Some(9));

        let checkpoint = node
            .store
            .get_checkpoint_announcement("region.sol-4", "cp-sol-4")
            .expect("load checkpoint")
            .expect("checkpoint exists");
        assert_eq!(checkpoint.artifact_path, "ipfs://checkpoint-sol-4");
    }

    #[test]
    fn network_config_reads_bootstrap_peers_from_env() {
        let _lock = lock_env_test_mutex();
        let _bootstrap = EnvVarGuard::set(
            ENV_P2P_BOOTSTRAP_PEERS,
            Some("/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWJ5r1D8N8QYp8JDs7u9mM4rY2kQ6xXK7Z6A6V4t7N3sQX"),
        );
        let config = network_config_from_env();
        assert_eq!(config.bootstrap_peers.len(), 1);
        assert_eq!(
            config.bootstrap_peers[0],
            "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWJ5r1D8N8QYp8JDs7u9mM4rY2kQ6xXK7Z6A6V4t7N3sQX"
        );
    }

    #[test]
    fn global_publish_rate_guard_limits_only_high_frequency_global_events() {
        let mut guard = GlobalPublishRateGuard::new(Instant::now());
        let event = build_event_for_external(
            &NodeIdentity::random(),
            1,
            10,
            crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
                task_id: "task-rate-limit".to_owned(),
                role: crate::types::ClaimRole::Propose,
                claimer_node_id: "node-a".to_owned(),
                execution_id: "exec-1".to_owned(),
                lease_until: 20,
            }),
        )
        .expect("event");

        for _ in 0..GLOBAL_HIGH_FREQUENCY_LIMIT {
            assert!(guard.allow(&SwarmScope::Global, &event));
        }
        assert!(!guard.allow(&SwarmScope::Global, &event));
        assert!(guard.allow(&SwarmScope::Node("lab-1".to_owned()), &event));
    }

    #[test]
    fn smarter_backfill_prefers_peer_with_known_scope_activity() {
        let node_a = PeerId::random();
        let node_b = PeerId::random();
        let target_scope = SwarmScope::Region("sol-1".to_owned());
        let other_scope = SwarmScope::Node("lab-2".to_owned());
        let now = Instant::now();

        let mut service = NetworkBridgeService::new_with_scopes(
            NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
            &[SwarmScope::Global, target_scope.clone()],
        )
        .expect("service");
        service.connected_peers.insert(node_a);
        service.connected_peers.insert(node_b);

        let mut state_a = PeerSyncState::new(now - Duration::from_secs(30));
        state_a.known_scopes.insert(other_scope);
        let mut state_b = PeerSyncState::new(now - Duration::from_secs(30));
        state_b.known_scopes.insert(target_scope.clone());
        service.peer_sync_state.insert(node_a, state_a);
        service.peer_sync_state.insert(node_b, state_b);

        assert_eq!(
            service.preferred_backfill_peer_for_scope(&target_scope, now),
            Some(node_b)
        );
    }

    #[test]
    fn scopes_to_request_for_peer_falls_back_to_all_scopes_until_peer_is_profiled() {
        let peer = PeerId::random();
        let target_scope = SwarmScope::Region("sol-1".to_owned());
        let service = NetworkBridgeService::new_with_scopes(
            NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
            &[SwarmScope::Global, target_scope.clone()],
        )
        .expect("service");

        assert_eq!(
            service.scopes_to_request_for_peer(&peer),
            vec![SwarmScope::Global, target_scope]
        );
    }

    #[test]
    fn local_scope_aliases_map_to_node_scope() {
        let policy_hash =
            "policy-hash-alias".to_owned();
        let mut contract = sample_contract("task-local-alias", policy_hash);
        contract.inputs = json!({"swarm_scope":"local:lab-1"});
        assert_eq!(contract_scope(&contract), SwarmScope::Node("lab-1".to_owned()));

        contract.inputs = json!({"swarm_scope":{"kind":"local","id":"lab-2"}});
        assert_eq!(contract_scope(&contract), SwarmScope::Node("lab-2".to_owned()));

        contract.inputs = json!({});
        contract.task_type = "local:lab-3:swarm".to_owned();
        assert_eq!(contract_scope(&contract), SwarmScope::Node("lab-3".to_owned()));
    }

    #[test]
    fn backfill_response_filters_by_scope() {
        let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("policy binding")
            .policy_hash;
        let mut global_contract = sample_contract("task-global-backfill", policy_hash.clone());
        global_contract.inputs = json!({"prompt":"global"});
        node.submit_task(global_contract, 1, 10)
            .expect("submit global");

        let mut region_contract = sample_contract("task-region-backfill", policy_hash);
        region_contract.task_type = "region:sol-1:swarm".to_owned();
        region_contract.inputs = json!({"prompt":"region", "swarm_scope":"region:sol-1"});
        node.submit_task(region_contract, 1, 11)
            .expect("submit region");

        let response = backfill_response_for_request(
            &node,
            &BackfillRequest {
                scope: SwarmScope::Region("sol-1".to_owned()),
                from_event_seq: 0,
                limit: 8,
            },
            32,
        )
        .expect("backfill response");

        assert_eq!(response.events.len(), 1);
        assert_eq!(
            response.events[0].scope,
            SwarmScope::Region("sol-1".to_owned())
        );
        assert_eq!(
            response.events[0].event.task_id.as_deref(),
            Some("task-region-backfill")
        );
    }

    #[test]
    fn summary_publish_is_suppressed_when_backlog_is_high() {
        assert!(should_publish_summaries(
            SUMMARY_BACKPRESSURE_HIGH_WATERMARK,
            0
        ));
        assert!(!should_publish_summaries(
            SUMMARY_BACKPRESSURE_HIGH_WATERMARK + 1,
            0
        ));
    }

    #[test]
    fn apply_summary_announcement_imports_knowledge_and_reputation() {
        let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

        apply_summary_announcement(
            &mut node,
            &SummaryAnnouncement {
                summary_id: "summary-knowledge-1".to_owned(),
                source_node_id: "node-a".to_owned(),
                scope: SwarmScope::Region("sol-1".to_owned()),
                summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
                artifact_path: None,
                payload: serde_json::to_value(KnowledgeSummaryBundle {
                    source_node_id: "node-a".to_owned(),
                    task_type: "summary-type".to_owned(),
                    decisions: vec![crate::storage::DecisionMemoryHitRow {
                        task_id: "task-knowledge-summary".to_owned(),
                        epoch: 1,
                        final_commit_hash: "commit-1".to_owned(),
                        winning_candidate_hash: "candidate-hash-1".to_owned(),
                        output_digest: "output-digest-1".to_owned(),
                        result_summary: json!({"answer":"summary-import"}),
                        quorum_result: json!({"quorum":"ok"}),
                        reason_codes: vec![100],
                        reason_details: json!({"detail":"ok"}),
                        policy_snapshot_digest: "policy-snap".to_owned(),
                        input_digest: "input-digest".to_owned(),
                        output_schema_digest: "schema-digest".to_owned(),
                        policy_id: "vp.schema_only.v1".to_owned(),
                        task_type: "summary-type".to_owned(),
                        policy_params_digest: "params-digest".to_owned(),
                        deprecated_as_exact: false,
                        finalized_at: 100,
                        confidence_hint: 0.5,
                    }],
                })
                .expect("knowledge payload"),
            },
        )
        .expect("apply knowledge summary");

        apply_summary_announcement(
            &mut node,
            &SummaryAnnouncement {
                summary_id: "summary-reputation-1".to_owned(),
                source_node_id: "node-a".to_owned(),
                scope: SwarmScope::Global,
                summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
                artifact_path: None,
                payload: serde_json::to_value(ReputationSummaryBundle {
                    source_node_id: "node-a".to_owned(),
                    entries: vec![crate::storage::ReputationSnapshotRow {
                        runtime_id: "runtime-a".to_owned(),
                        profile_id: "model-a".to_owned(),
                        stability_reputation: 10,
                        quality_reputation: 20,
                        last_updated_at: 101,
                    }],
                })
                .expect("reputation payload"),
            },
        )
        .expect("apply reputation summary");

        let hits = node
            .store
            .list_decision_memory_hits_by_task_type("summary-type", 8)
            .expect("list hits");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].result_summary["answer"], json!("summary-import"));

        let reputation = node
            .store
            .get_reputation_snapshot("runtime-a", "model-a")
            .expect("get reputation")
            .expect("reputation row");
        assert_eq!(reputation.stability_reputation, 10);
        assert_eq!(reputation.quality_reputation, 20);
    }

    #[test]
    fn summary_revocation_and_penalty_remove_imported_state() {
        let mut node =
            Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");

        apply_summary_announcement(
            &mut node,
            &SummaryAnnouncement {
                summary_id: "summary-knowledge-2".to_owned(),
                source_node_id: "node-bad".to_owned(),
                scope: SwarmScope::Global,
                summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
                artifact_path: None,
                payload: serde_json::to_value(KnowledgeSummaryBundle {
                    source_node_id: "node-bad".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    decisions: vec![crate::storage::DecisionMemoryHitRow {
                        task_id: "task-revoked-summary".to_owned(),
                        epoch: 1,
                        final_commit_hash: "commit-revoked".to_owned(),
                        winning_candidate_hash: "candidate-revoked".to_owned(),
                        output_digest: "output-revoked".to_owned(),
                        result_summary: json!({"answer":"bad summary"}),
                        quorum_result: json!({"quorum":"ok"}),
                        reason_codes: vec![100],
                        reason_details: json!({"detail":"bad"}),
                        policy_snapshot_digest: "policy-snap".to_owned(),
                        input_digest: "input-digest".to_owned(),
                        output_schema_digest: "schema-digest".to_owned(),
                        policy_id: "vp.schema_only.v1".to_owned(),
                        task_type: "revoked-type".to_owned(),
                        policy_params_digest: "params-digest".to_owned(),
                        deprecated_as_exact: false,
                        finalized_at: 100,
                        confidence_hint: 0.5,
                    }],
                })
                .expect("knowledge payload"),
            },
        )
        .expect("apply knowledge summary");
        apply_summary_announcement(
            &mut node,
            &SummaryAnnouncement {
                summary_id: "summary-reputation-2".to_owned(),
                source_node_id: "node-bad".to_owned(),
                scope: SwarmScope::Global,
                summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
                artifact_path: None,
                payload: serde_json::to_value(ReputationSummaryBundle {
                    source_node_id: "node-bad".to_owned(),
                    entries: vec![crate::storage::ReputationSnapshotRow {
                        runtime_id: "runtime-bad".to_owned(),
                        profile_id: "model-bad".to_owned(),
                        stability_reputation: 999,
                        quality_reputation: 888,
                        last_updated_at: 101,
                    }],
                })
                .expect("reputation payload"),
            },
        )
        .expect("apply reputation summary");

        assert_eq!(
            node.store
                .list_decision_memory_hits_by_task_type("revoked-type", 8)
                .expect("list decisions")
                .len(),
            1
        );
        assert!(
            node.store
                .get_reputation_snapshot("runtime-bad", "model-bad")
                .expect("get reputation")
                .is_some()
        );

        node.revoke_summary(
            "summary-knowledge-2",
            KNOWLEDGE_SUMMARY_KIND,
            "bad knowledge",
            1,
            200,
        )
        .expect("revoke summary");
        assert!(
            node.store
                .list_decision_memory_hits_by_task_type("revoked-type", 8)
                .expect("list decisions after revoke")
                .is_empty()
        );

        node.penalize_node(
            "node-bad",
            "malicious source",
            Vec::new(),
            Vec::new(),
            1,
            201,
        )
        .expect("penalize node");
        assert!(
            node.store
                .get_reputation_snapshot("runtime-bad", "model-bad")
                .expect("get reputation after penalty")
                .is_none()
        );

        apply_summary_announcement(
            &mut node,
            &SummaryAnnouncement {
                summary_id: "summary-knowledge-3".to_owned(),
                source_node_id: "node-bad".to_owned(),
                scope: SwarmScope::Global,
                summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
                artifact_path: None,
                payload: serde_json::to_value(KnowledgeSummaryBundle {
                    source_node_id: "node-bad".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    decisions: vec![crate::storage::DecisionMemoryHitRow {
                        task_id: "task-revoked-summary-2".to_owned(),
                        epoch: 1,
                        final_commit_hash: "commit-revoked-2".to_owned(),
                        winning_candidate_hash: "candidate-revoked-2".to_owned(),
                        output_digest: "output-revoked-2".to_owned(),
                        result_summary: json!({"answer":"should be ignored"}),
                        quorum_result: json!({"quorum":"ok"}),
                        reason_codes: vec![100],
                        reason_details: json!({"detail":"ignored"}),
                        policy_snapshot_digest: "policy-snap".to_owned(),
                        input_digest: "input-digest".to_owned(),
                        output_schema_digest: "schema-digest".to_owned(),
                        policy_id: "vp.schema_only.v1".to_owned(),
                        task_type: "revoked-type".to_owned(),
                        policy_params_digest: "params-digest".to_owned(),
                        deprecated_as_exact: false,
                        finalized_at: 102,
                        confidence_hint: 0.5,
                    }],
                })
                .expect("knowledge payload"),
            },
        )
        .expect("apply blocked summary");
        assert!(
            node.store
                .list_decision_memory_hits_by_task_type("revoked-type", 8)
                .expect("list decisions after penalty")
                .is_empty()
        );
    }

    #[test]
    fn revoked_event_rebuild_removes_previous_projection_state() {
        let mut node =
            Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("policy binding")
            .policy_hash;
        let mut contract = sample_contract("task-revoked-event", policy_hash);
        contract.inputs = json!({"prompt":"revoke me"});
        let created = node.submit_task(contract, 1, 100).expect("submit task");
        assert!(
            node.task_view("task-revoked-event")
                .expect("task view")
                .is_some()
        );

        node.revoke_event(&created.event_id, "malicious task", 1, 101)
            .expect("revoke event");

        assert!(
            node.task_view("task-revoked-event")
                .expect("task view after revoke")
                .is_none()
        );
    }

    #[test]
    fn publish_pending_global_events_publishes_local_rows_and_skips_remote_rows() {
        let local = NodeIdentity::random();
        let local_node_id = local.node_id();
        let remote = NodeIdentity::random();
        let membership = membership_with_roles(&[local_node_id.clone(), remote.node_id()]);
        let mut node =
            Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("policy binding")
            .policy_hash;
        let mut local_contract = sample_contract("task-publish-local", policy_hash.clone());
        local_contract.inputs = json!({"prompt":"publish me"});
        node.submit_task(local_contract, 1, 100)
            .expect("local task");

        let remote_event = build_event_for_external(
            &remote,
            1,
            101,
            crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
                checkpoint_id: "cp-remote".to_owned(),
                up_to_seq: 1,
            }),
        )
        .expect("remote event");
        node.ingest_remote(remote_event).expect("ingest remote");

        let mut service = NetworkBridgeService::new(
            NetworkP2pNode::generate(NetworkP2pConfig {
                listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
                enable_mdns: false,
                ..NetworkP2pConfig::default()
            })
            .expect("network node"),
        )
        .expect("network service");
        let mut peer_node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("peer node");
        let mut peer_service = NetworkBridgeService::new(
            NetworkP2pNode::generate(NetworkP2pConfig {
                listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
                enable_mdns: false,
                ..NetworkP2pConfig::default()
            })
            .expect("peer network node"),
        )
        .expect("peer network service");

        for _ in 0..4_096 {
            let _ = service.try_tick(&mut node).expect("service tick");
            let _ = peer_service
                .try_tick(&mut peer_node)
                .expect("peer service tick");
            if !service.listen_addrs().is_empty() && !peer_service.listen_addrs().is_empty() {
                break;
            }
            std::thread::yield_now();
        }
        service
            .dial(peer_service.listen_addrs()[0].clone())
            .expect("dial peer");
        let mut connected = false;
        for _ in 0..4_096 {
            if let Some(NetworkBridgeTick::Connected { .. }) =
                service.try_tick(&mut node).expect("service tick")
            {
                connected = true;
            }
            let _ = peer_service
                .try_tick(&mut peer_node)
                .expect("peer service tick");
            if connected {
                break;
            }
            std::thread::yield_now();
        }
        assert!(connected);

        let mut last = 0;
        for _ in 0..4_096 {
            last = publish_pending_global_events(&mut service, &node, &local_node_id, 0)
                .expect("publish pending");
            if last == 2 {
                break;
            }
            let _ = service.try_tick(&mut node).expect("service tick");
            let _ = peer_service
                .try_tick(&mut peer_node)
                .expect("peer service tick");
            std::thread::yield_now();
        }
        assert_eq!(last, 2);
    }

    #[test]
    fn dial_discovered_peer_endpoints_skips_invalid_self_and_missing_addrs() {
        let dir = std::env::temp_dir().join(format!(
            "wattswarm-network-bridge-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        crate::control::save_discovered_peer_records(
            &crate::control::discovered_peers_path(&dir),
            &[
                crate::control::DiscoveredPeerRecord {
                    node_id: "self".to_owned(),
                    listen_addr: Some("/ip4/127.0.0.1/tcp/4001".to_owned()),
                },
                crate::control::DiscoveredPeerRecord {
                    node_id: "peer-a".to_owned(),
                    listen_addr: None,
                },
                crate::control::DiscoveredPeerRecord {
                    node_id: "peer-b".to_owned(),
                    listen_addr: Some("not-a-multiaddr".to_owned()),
                },
            ],
        )
        .expect("save discovered peers");

        let mut service = NetworkBridgeService::new(
            NetworkP2pNode::generate(NetworkP2pConfig {
                listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
                enable_mdns: false,
                ..NetworkP2pConfig::default()
            })
            .expect("network node"),
        )
        .expect("network service");

        let mut attempts = HashMap::new();
        let dialed = dial_discovered_peer_endpoints(&mut service, &dir, "self", &mut attempts)
            .expect("dial discovered peers");
        assert_eq!(dialed, 0);
        assert!(attempts.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
