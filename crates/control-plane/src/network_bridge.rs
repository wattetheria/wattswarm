use anyhow::{Result, anyhow, bail};

use crate::constants::BACKFILL_BATCH_EVENTS;
use crate::control::load_discovered_peer_records;
use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, BackfillResponse, EventEnvelope, GossipMessage, Multiaddr,
    NetworkP2pConfig, NetworkP2pNode, NetworkRuntime, NetworkRuntimeEvent, PeerId,
    SummaryAnnouncement, SwarmScope,
};
use crate::node::Node;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

const ENV_P2P_REGION_IDS: &str = "WATTSWARM_P2P_REGION_IDS";
const ENV_P2P_LOCAL_IDS: &str = "WATTSWARM_P2P_LOCAL_IDS";
const KNOWLEDGE_SUMMARY_KIND: &str = "knowledge_task_type_v1";
const REPUTATION_SUMMARY_KIND: &str = "reputation_runtime_profile_v1";
const SUMMARY_REPUTATION_LIMIT: usize = 64;
const SUMMARY_DECISION_MEMORY_LIMIT: u32 = 16;

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
const DEFAULT_P2P_PORT: u16 = 4001;

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
    if let Ok(raw) = env::var(ENV_P2P_LOCAL_IDS) {
        for scope in parse_scope_id_env(&raw, SwarmScope::Local) {
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
    scopes: Vec<SwarmScope>,
) -> Result<()> {
    let mut node = crate::control::open_node(state_dir, db_path)?;
    let node_id = node.node_id();
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
        let new_last_published_seq =
            publish_pending_scoped_updates(&mut service, &node, &node_id, last_published_seq)?;
        if new_last_published_seq != last_published_seq {
            did_work = true;
            last_published_seq = new_last_published_seq;
        }
        if !did_work {
            thread::sleep(Duration::from_millis(50));
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
        "local" => Some(SwarmScope::Local(id.to_owned())),
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
            (Some(kind), Some(id)) if !id.is_empty() && kind == "local" => {
                return SwarmScope::Local(id.to_owned());
            }
            (Some(kind), _) if kind == "global" => return SwarmScope::Global,
            _ => {}
        }
    }
    parse_scope_hint_string(&contract.task_type).unwrap_or(SwarmScope::Global)
}

fn event_scope(node: &Node, event: &crate::types::Event) -> Result<SwarmScope> {
    match &event.payload {
        crate::types::EventPayload::TaskCreated(contract) => Ok(contract_scope(contract)),
        crate::types::EventPayload::MembershipUpdated(_)
        | crate::types::EventPayload::PolicyTuned(_)
        | crate::types::EventPayload::CheckpointCreated(_)
        | crate::types::EventPayload::AdvisoryCreated(_)
        | crate::types::EventPayload::AdvisoryApproved(_)
        | crate::types::EventPayload::AdvisoryApplied(_) => Ok(SwarmScope::Global),
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

pub fn build_knowledge_summary_for_task_type(
    node: &Node,
    scope: &SwarmScope,
    task_type: &str,
) -> Result<Option<SummaryAnnouncement>> {
    let decisions = node
        .store
        .list_decision_memory_hits_by_task_type(task_type, SUMMARY_DECISION_MEMORY_LIMIT)?;
    if decisions.is_empty() {
        return Ok(None);
    }
    Ok(Some(SummaryAnnouncement {
        scope: scope.clone(),
        summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: serde_json::to_value(KnowledgeSummaryBundle {
            source_node_id: node.node_id(),
            task_type: task_type.to_owned(),
            decisions,
        })?,
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
    let Some(entry) = node.store.get_reputation_snapshot(runtime_id, profile_id)? else {
        return Ok(None);
    };
    Ok(Some(SummaryAnnouncement {
        scope: SwarmScope::Global,
        summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
        artifact_path: None,
        payload: serde_json::to_value(ReputationSummaryBundle {
            source_node_id: node.node_id(),
            entries: vec![entry],
        })?,
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
    match summary.summary_kind.as_str() {
        KNOWLEDGE_SUMMARY_KIND => {
            let payload: KnowledgeSummaryBundle = serde_json::from_value(summary.payload.clone())?;
            for decision in payload.decisions {
                node.store.put_decision_memory(
                    &decision.task_id,
                    decision.epoch,
                    &decision.final_commit_hash,
                    decision.finalized_at,
                    &decision.winning_candidate_hash,
                    &decision.output_digest,
                    &decision.result_summary,
                    &decision.quorum_result,
                    &decision.reason_codes,
                    &decision.reason_details,
                    &decision.policy_snapshot_digest,
                    &decision.task_type,
                    &decision.input_digest,
                    &decision.output_schema_digest,
                    &decision.policy_id,
                    &decision.policy_params_digest,
                )?;
            }
        }
        REPUTATION_SUMMARY_KIND => {
            let payload: ReputationSummaryBundle = serde_json::from_value(summary.payload.clone())?;
            for entry in payload.entries {
                node.store.put_reputation_snapshot(
                    &entry.runtime_id,
                    &entry.profile_id,
                    entry.stability_reputation,
                    entry.quality_reputation,
                    entry.last_updated_at,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
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
    for (seq, event) in rows {
        if event.author_node_id != local_node_id {
            last_published_seq = seq;
            continue;
        }
        let scope = event_scope(node, &event)?;
        match service.publish_event_for_scope(&scope, event.clone()) {
            Ok(()) => last_published_seq = seq,
            Err(err) if err.to_string().contains("NoPeersSubscribedToTopic") => break,
            Err(err) => return Err(err),
        }
        if let Some(summary) = knowledge_summary_for_event(node, &event, &scope)? {
            let _ = service.publish_summary(summary);
        }
        if let Some(summary) = reputation_summary_for_event(node, &event)? {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBridgeTick {
    Listening {
        address: Multiaddr,
    },
    Connected {
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

pub struct NetworkBridgeService {
    runtime: NetworkRuntime,
    tokio_runtime: Runtime,
    subscribed_scopes: Vec<SwarmScope>,
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
            subscribed_scopes,
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

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.dial(addr)
    }

    pub fn publish_event_for_scope(
        &mut self,
        scope: &SwarmScope,
        event: crate::types::Event,
    ) -> Result<()> {
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

    fn request_backfill_for_peer(&mut self, peer: &PeerId, from_event_seq: u64) -> Result<()> {
        for scope in self.subscribed_scopes.clone() {
            let _guard = self.tokio_runtime.enter();
            let _ = self.runtime.send_backfill_request(
                peer,
                BackfillRequest {
                    scope,
                    from_event_seq,
                    limit: BACKFILL_BATCH_EVENTS,
                },
            )?;
        }
        Ok(())
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
            NetworkRuntimeEvent::ConnectionEstablished { peer } => {
                let from_event_seq = node.head_seq()?;
                self.request_backfill_for_peer(&peer, from_event_seq)?;
                for entry in node
                    .store
                    .list_reputation_snapshots(SUMMARY_REPUTATION_LIMIT)?
                {
                    let _ = self.publish_summary(SummaryAnnouncement {
                        scope: SwarmScope::Global,
                        summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
                        artifact_path: None,
                        payload: serde_json::to_value(ReputationSummaryBundle {
                            source_node_id: node.node_id(),
                            entries: vec![entry],
                        })?,
                    });
                }
                Ok(NetworkBridgeTick::Connected { peer })
            }
            NetworkRuntimeEvent::Gossip {
                propagation_source,
                message,
            } => match message {
                GossipMessage::Event(envelope) => {
                    ingest_event_envelope(node, &envelope)?;
                    Ok(NetworkBridgeTick::EventIngested {
                        peer: propagation_source,
                        event_id: envelope.event.event_id.clone(),
                    })
                }
                GossipMessage::Summary(summary) => {
                    apply_summary_announcement(node, &summary)?;
                    Ok(NetworkBridgeTick::SummaryApplied {
                        peer: propagation_source,
                        summary_kind: summary.summary_kind,
                    })
                }
                GossipMessage::Rule(_) => Ok(NetworkBridgeTick::GossipIgnored {
                    peer: propagation_source,
                    message_kind: "rule".to_owned(),
                }),
                GossipMessage::Checkpoint(_) => Ok(NetworkBridgeTick::GossipIgnored {
                    peer: propagation_source,
                    message_kind: "checkpoint".to_owned(),
                }),
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
            } => Ok(NetworkBridgeTick::BackfillApplied {
                peer,
                request_id,
                events: ingest_backfill_response(node, &response)?,
            }),
            NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => Ok(NetworkBridgeTick::BackfillFailed {
                peer,
                request_id,
                error,
            }),
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
        let _lock = env_test_lock().lock().expect("env test lock");
        let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, None);
        let _mdns = EnvVarGuard::set(ENV_P2P_MDNS, None);
        let _port = EnvVarGuard::set(ENV_P2P_PORT, None);
        let _listen = EnvVarGuard::set(ENV_P2P_LISTEN_ADDRS, None);
        assert!(network_enabled_from_env());
        let config = network_config_from_env();
        assert!(config.enable_mdns);
        assert_eq!(config.listen_addrs, vec!["/ip4/0.0.0.0/tcp/4001"]);
    }

    #[test]
    fn network_enabled_can_be_explicitly_disabled() {
        let _lock = env_test_lock().lock().expect("env test lock");
        let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, Some("false"));
        assert!(!network_enabled_from_env());
    }

    #[test]
    fn configured_network_scopes_include_region_and_local_from_env() {
        let _lock = env_test_lock().lock().expect("env test lock");
        let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, Some("sol-1,sol-2"));
        let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, Some("lab-a"));
        assert_eq!(
            configured_network_scopes_from_env(),
            vec![
                SwarmScope::Global,
                SwarmScope::Region("sol-1".to_owned()),
                SwarmScope::Region("sol-2".to_owned()),
                SwarmScope::Local("lab-a".to_owned()),
            ]
        );
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
    fn apply_summary_announcement_imports_knowledge_and_reputation() {
        let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

        apply_summary_announcement(
            &mut node,
            &SummaryAnnouncement {
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
