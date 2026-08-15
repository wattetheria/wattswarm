use super::service_loop::{
    NetworkServiceStatus, network_service_statuses, set_network_service_status,
};
use crate::network_service::{ContentFetcher, HttpObjectStoreContentFetcher, event_artifact_refs};
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wattswarm_network_client_server::{
    AutoRegistrationRequest, ChallengeRequest, ClientServerClient, ClientServerConfig,
    ClientServerTransport, CommitRequest, ControlFrame, ControlFrameKind, DeliveryClassInput,
    DeliveryScheduler, EventDeliveryUrgency, GrantAdmissionRequest, HistoryStatus,
    LogicalNodePrincipalClaim, LogicalNodePrincipalProof, PublishFrame, PublishPayloadType,
    PublishRoute, SessionProofRequest, SessionResponse, WeightedDeliveryScheduler,
    control_frame_signing_message, delivery_class_for_record, session_proof_message,
};
use wattswarm_network_transport_core::{
    CheckpointAnnouncement, DeliveryClass, DeliveryPage, EventTransportRoute,
    MailboxControlDelivery, OpaqueCommitToken, OpaqueSignedRecord, PropagationLane,
    SummaryAnnouncement, SwarmScope,
};
use wattswarm_protocol::types::NetworkMembershipGrant;
use wattswarm_storage_core::storage::{
    CsMailboxDeliveryState, NetworkBackendStatusRow, PgStore, local_control_scope_id,
};

const EVENT_SOURCE_ID: &str = "events";
const CS_SCAN_LIMIT: usize = 128;
const CS_IDLE_SLEEP: Duration = Duration::from_millis(100);

pub(super) fn build_client_server_contact_material(
    state_dir: &Path,
    identity: &crate::crypto::NodeIdentity,
    gateway_url: &str,
) -> Result<super::RawContactMaterial> {
    let generated_at = now_ms();
    let private_message_keypair =
        crate::control::load_or_create_private_message_keypair_state(state_dir)?;
    let material = serde_json::json!({
        "node_id": identity.node_id(),
        "peer_id": identity.node_id(),
        "generated_at": generated_at,
        "transports": [{
            "transport": "client_server_gateway",
            "gateway_url": gateway_url,
        }],
        "recommended_routes": ["client_server_gateway"],
        "encryption": {
            "private_message": {
                "scheme": "wattswarm.private.dm.v1",
                "key_agreement": "x25519",
                "cipher": "chacha20poly1305",
                "public_key_b64": private_message_keypair.public_key_b64,
            }
        },
    });
    Ok(super::RawContactMaterial {
        material_json: serde_json::to_string(&material)?,
        signature: Some(identity.sign_bytes(&serde_json::to_vec(&material)?)),
        generated_at,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CsOutboundPartition {
    GlobalInteractive,
    GlobalBulk,
    NonGlobalInteractive,
    NonGlobalBulk,
}

#[derive(Debug, Default)]
struct CsRuntimeObservability {
    session: CsLatencyObservability,
    publisher_confirm: CsLatencyObservability,
    delivery_page: CsLatencyObservability,
    commit: CsLatencyObservability,
    cumulative_commit_retries: u64,
}

#[derive(Debug, Default)]
struct CsLatencyObservability {
    count: u64,
    total_ms: u64,
    max_ms: u64,
    last_ms: u64,
}

impl CsLatencyObservability {
    fn observe(&mut self, started: Instant) {
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.count = self.count.saturating_add(1);
        self.total_ms = self.total_ms.saturating_add(elapsed_ms);
        self.max_ms = self.max_ms.max(elapsed_ms);
        self.last_ms = elapsed_ms;
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count,
            "last_ms": self.last_ms,
            "average_ms": self.total_ms.checked_div(self.count).unwrap_or_default(),
            "max_ms": self.max_ms,
        })
    }
}

impl CsOutboundPartition {
    const ALL: [Self; 4] = [
        Self::GlobalInteractive,
        Self::GlobalBulk,
        Self::NonGlobalInteractive,
        Self::NonGlobalBulk,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::GlobalInteractive => "global_interactive",
            Self::GlobalBulk => "global_bulk",
            Self::NonGlobalInteractive => "non_global_interactive",
            Self::NonGlobalBulk => "non_global_bulk",
        }
    }

    fn for_route(scope: &SwarmScope, delivery_class: DeliveryClass) -> Self {
        match (scope == &SwarmScope::Global, delivery_class) {
            (true, DeliveryClass::Interactive) => Self::GlobalInteractive,
            (true, DeliveryClass::Bulk) => Self::GlobalBulk,
            (false, DeliveryClass::Interactive) => Self::NonGlobalInteractive,
            (false, DeliveryClass::Bulk) => Self::NonGlobalBulk,
        }
    }
}

pub(super) fn maybe_start_client_server_network_service(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> Result<bool> {
    let gateway_url =
        crate::network_service::client_server_url_from_env_or_startup_config(&state_dir)?;
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
    let state_dir_for_status = state_dir.clone();
    thread::spawn(move || {
        let result = run_client_server_network_service(&state_dir, &db_path, &gateway_url);
        if let Err(error) = result {
            eprintln!("client_server network service stopped: {error:#}");
            set_network_service_status(&state_dir_for_status, NetworkServiceStatus::Failed);
        } else {
            set_network_service_status(&state_dir_for_status, NetworkServiceStatus::Stopped);
        }
    });
    Ok(true)
}

fn run_client_server_network_service(
    state_dir: &Path,
    db_path: &Path,
    gateway_url: &str,
) -> Result<()> {
    let mut node = crate::control::open_configured_node(state_dir, db_path)
        .context("ClientServer startup open configured node")?;
    let identity = crate::control::load_local_identity(state_dir)?;
    let principal_id = identity.node_id();
    let network_id = super::current_network_context_id(&node);
    let scope_id = local_control_scope_id(state_dir);
    let tenant_instance_id = node
        .store
        .load_or_create_cs_tenant_instance_id(&scope_id, now_ms())?;
    let config = ClientServerConfig::from_gateway_url(gateway_url);
    let delivery_policy_version = config.delivery_policy_version;
    let client = ClientServerClient::new(config)?;
    let content_fetcher = HttpObjectStoreContentFetcher::new(
        Some(gateway_url.to_owned()),
        Duration::from_secs(10),
        3,
    )?;
    let mut observability = CsRuntimeObservability::default();
    let auth_started = Instant::now();
    let _grant = load_or_register_grant(
        &client,
        state_dir,
        &node.store,
        &identity,
        &network_id,
        &tenant_instance_id,
    )?;
    let mut session = authenticate(
        &client,
        &identity,
        &network_id,
        &principal_id,
        &tenant_instance_id,
        delivery_policy_version,
    )?;
    observability.session.observe(auth_started);
    let mut history_unavailable = session.history_status == HistoryStatus::HistoryUnavailable;
    content_fetcher.set_bearer_token(Some(session.session_token.clone()));
    let partition_names = CsOutboundPartition::ALL.map(CsOutboundPartition::as_str);
    let source_head = node.head_seq()?;
    let existing_progress = CsOutboundPartition::ALL
        .into_iter()
        .map(|partition| {
            node.store
                .load_cs_outbound_progress(&scope_id, EVENT_SOURCE_ID, partition.as_str())
        })
        .collect::<Result<Vec<_>>>()?;
    let initialized = existing_progress.iter().flatten().count();
    validate_client_server_outbound_progress_state(
        initialized,
        CsOutboundPartition::ALL.len(),
        source_head,
    )?;
    node.store.initialize_cs_outbound_progress(
        &scope_id,
        EVENT_SOURCE_ID,
        &partition_names,
        delivery_policy_version,
        source_head,
        now_ms(),
    )?;
    set_network_service_status(state_dir, NetworkServiceStatus::Running);
    store_backend_status(
        &node.store,
        &scope_id,
        "ready",
        None,
        0,
        0,
        0,
        Some(session.expires_at),
        history_unavailable,
        &observability,
    )?;

    let mut scheduler = WeightedDeliveryScheduler::new(4);
    let discovery_settings = super::discovery_bootnode_settings_from_state_dir(state_dir)?;
    let mut next_discovery_at = Instant::now();
    let mut published = 0_u64;
    let mut received = 0_u64;
    let mut retries = 0_u64;
    loop {
        if session.expires_at <= now_ms().saturating_add(60_000) {
            let auth_started = Instant::now();
            session = authenticate(
                &client,
                &identity,
                &network_id,
                &principal_id,
                &tenant_instance_id,
                delivery_policy_version,
            )?;
            observability.session.observe(auth_started);
            history_unavailable |= session.history_status == HistoryStatus::HistoryUnavailable;
            content_fetcher.set_bearer_token(Some(session.session_token.clone()));
        }
        let mut did_work = false;
        let pending_commit_count =
            node.store.list_cs_mailbox_pending_commits(&scope_id)?.len() as u64;
        let commit_started = Instant::now();
        match retry_pending_commits(&client, &session.session_token, &node.store, &scope_id) {
            Ok(count) => did_work |= count > 0,
            Err(error) => {
                observability.cumulative_commit_retries = observability
                    .cumulative_commit_retries
                    .saturating_add(pending_commit_count);
                if is_session_rejected(&error) {
                    session.expires_at = 0;
                }
                retries = retries.saturating_add(1);
                store_backend_status(
                    &node.store,
                    &scope_id,
                    "degraded",
                    Some(&format!("pending commit: {error}")),
                    published,
                    received,
                    retries,
                    Some(session.expires_at),
                    history_unavailable,
                    &observability,
                )?;
            }
        }
        if pending_commit_count > 0 {
            observability.commit.observe(commit_started);
        }
        if discovery_settings.enabled && Instant::now() >= next_discovery_at {
            let discovery_result = (|| {
                let records = super::query_discovery_bootnodes_for_candidate_records(
                    state_dir,
                    &node,
                    &network_id,
                    &discovery_settings,
                    now_ms(),
                )?;
                let mut applied = false;
                for record in records {
                    applied |= super::apply_discovery_bootnode_record_client_server(
                        &mut node,
                        state_dir,
                        &principal_id,
                        &network_id,
                        &discovery_settings,
                        record,
                        now_ms(),
                    )?;
                }
                Ok::<_, anyhow::Error>(applied)
            })();
            match discovery_result {
                Ok(applied) => did_work |= applied,
                Err(error) => {
                    retries = retries.saturating_add(1);
                    store_backend_status(
                        &node.store,
                        &scope_id,
                        "degraded",
                        Some(&format!("discovery: {error}")),
                        published,
                        received,
                        retries,
                        Some(session.expires_at),
                        history_unavailable,
                        &observability,
                    )?;
                }
            }
            next_discovery_at = Instant::now() + discovery_settings.interval;
        }
        match super::process_pending_client_server_network_commands(
            &mut node,
            &client,
            &session.session_token,
            &identity,
            state_dir,
            gateway_url,
        ) {
            Ok(count) => did_work |= count > 0,
            Err(error) => {
                if is_session_rejected(&error) {
                    session.expires_at = 0;
                }
                retries = retries.saturating_add(1);
                store_backend_status(
                    &node.store,
                    &scope_id,
                    "degraded",
                    Some(&format!("command: {error}")),
                    published,
                    received,
                    retries,
                    Some(session.expires_at),
                    history_unavailable,
                    &observability,
                )?;
            }
        }
        for partition in CsOutboundPartition::ALL {
            let publish_started = Instant::now();
            match publish_partition(
                &client,
                &session.session_token,
                &node,
                &network_id,
                &principal_id,
                &scope_id,
                partition,
                delivery_policy_version,
            ) {
                Ok(count) => {
                    if count > 0 {
                        observability.publisher_confirm.observe(publish_started);
                    }
                    published = published.saturating_add(count);
                    did_work |= count > 0;
                }
                Err(error) => {
                    observability.publisher_confirm.observe(publish_started);
                    if is_session_rejected(&error) {
                        session.expires_at = 0;
                    }
                    retries = retries.saturating_add(1);
                    store_backend_status(
                        &node.store,
                        &scope_id,
                        "degraded",
                        Some(&format!("publish: {error}")),
                        published,
                        received,
                        retries,
                        Some(session.expires_at),
                        history_unavailable,
                        &observability,
                    )?;
                }
            }
        }
        let class = scheduler
            .next_class(true, true)
            .unwrap_or(DeliveryClass::Interactive);
        let delivery_page_started = Instant::now();
        match receive_page(
            &client,
            &session.session_token,
            &mut node,
            &scope_id,
            class,
            &content_fetcher,
            state_dir,
            &identity,
        ) {
            Ok(count) => {
                observability.delivery_page.observe(delivery_page_started);
                received = received.saturating_add(count);
                did_work |= count > 0;
            }
            Err(error) => {
                observability.delivery_page.observe(delivery_page_started);
                if is_session_rejected(&error) {
                    session.expires_at = 0;
                }
                retries = retries.saturating_add(1);
                store_backend_status(
                    &node.store,
                    &scope_id,
                    "degraded",
                    Some(&format!("delivery: {error}")),
                    published,
                    received,
                    retries,
                    Some(session.expires_at),
                    history_unavailable,
                    &observability,
                )?;
            }
        }
        if did_work {
            store_backend_status(
                &node.store,
                &scope_id,
                "ready",
                None,
                published,
                received,
                retries,
                Some(session.expires_at),
                history_unavailable,
                &observability,
            )?;
        } else {
            thread::sleep(CS_IDLE_SLEEP);
        }
    }
}

fn validate_client_server_outbound_progress_state(
    initialized: usize,
    partition_count: usize,
    source_head: u64,
) -> Result<()> {
    match initialized {
        0 if source_head == 0 => Ok(()),
        0 => bail!(
            "ClientServer requires a fresh Event Store; redeploy the node before enabling ClientServer"
        ),
        count if count == partition_count => Ok(()),
        _ => bail!("ClientServer outbound progress is only partially initialized"),
    }
}

fn load_or_register_grant(
    client: &ClientServerClient,
    state_dir: &Path,
    store: &PgStore,
    identity: &crate::crypto::NodeIdentity,
    network_id: &str,
    tenant_instance_id: &str,
) -> Result<NetworkMembershipGrant> {
    let topology = store.load_network_topology_for_org(store.org_id())?;
    let expected_genesis = topology.network.genesis_node_id;
    let principal_id = identity.node_id();
    if let Some(grant) = store.load_network_membership_grant(network_id, &principal_id)?
        && crate::crypto::verify_network_membership_grant(&grant, &expected_genesis, now_ms())
            .is_ok()
    {
        client.admit_grant(&GrantAdmissionRequest {
            grant: grant.clone(),
        })?;
        return Ok(grant);
    }
    if !crate::network_service::client_server_auto_registration_enabled(state_dir)? {
        bail!("ClientServer node membership grant is missing or invalid");
    }
    let registration_url =
        crate::network_service::network_registration_url_from_env_or_startup_config(state_dir)?;
    let mut request = AutoRegistrationRequest {
        network_id: network_id.to_owned(),
        principal_id: principal_id.clone(),
        public_key_hex: principal_id,
        tenant_instance_id: Some(tenant_instance_id.to_owned()),
        nonce: uuid::Uuid::new_v4().to_string(),
        signature_hex: String::new(),
    };
    request.signature_hex = identity.sign_bytes(&request.signing_message()?);
    let response = client
        .auto_register(&registration_url, &request)
        .context("register ClientServer node automatically")?;
    crate::crypto::verify_network_membership_grant(&response.grant, &expected_genesis, now_ms())?;
    store.put_network_membership_grant(&response.grant, now_ms())?;
    client.admit_grant(&GrantAdmissionRequest {
        grant: response.grant.clone(),
    })?;
    Ok(response.grant)
}

fn authenticate(
    client: &ClientServerClient,
    identity: &crate::crypto::NodeIdentity,
    network_id: &str,
    principal_id: &str,
    tenant_instance_id: &str,
    delivery_policy_version: u64,
) -> Result<SessionResponse> {
    let principals = vec![LogicalNodePrincipalClaim {
        principal_id: principal_id.to_owned(),
        public_key_hex: principal_id.to_owned(),
        tenant_instance_id: Some(tenant_instance_id.to_owned()),
    }];
    let challenge = client.challenge(&ChallengeRequest {
        network_id: network_id.to_owned(),
        principals: principals.clone(),
    })?;
    let proof_message = session_proof_message(network_id, &principals, &challenge)?;
    let response = client.prove_session(&SessionProofRequest {
        challenge_id: challenge.challenge_id,
        network_id: network_id.to_owned(),
        principals,
        proofs: vec![LogicalNodePrincipalProof {
            principal_id: principal_id.to_owned(),
            signature_hex: identity.sign_bytes(&proof_message),
        }],
        delivery_policy_version,
    })?;
    if response.principal_id != principal_id
        || response.network_id != network_id
        || response.delivery_policy_version != delivery_policy_version
    {
        bail!("Gateway session binding does not match local node");
    }
    Ok(response)
}

#[allow(clippy::too_many_arguments)]
fn publish_partition(
    client: &ClientServerClient,
    token: &str,
    node: &crate::node::Node,
    network_id: &str,
    local_node_id: &str,
    scope_id: &str,
    partition: CsOutboundPartition,
    delivery_policy_version: u64,
) -> Result<u64> {
    let progress = node
        .store
        .load_cs_outbound_progress(scope_id, EVENT_SOURCE_ID, partition.as_str())?
        .context("missing ClientServer outbound progress")?;
    if progress
        .next_retry_at
        .is_some_and(|retry_at| retry_at > now_ms())
    {
        return Ok(0);
    }
    let rows = node
        .store
        .load_events_page(progress.scanned_sequence, CS_SCAN_LIMIT)?;
    let publish_summaries =
        super::should_publish_summaries(node.head_seq()?, progress.scanned_sequence);
    let local_node_penalized = node.store.is_node_penalized(local_node_id)?;
    let mut cursor = progress.scanned_sequence;
    let mut published = 0_u64;
    for (sequence, event) in rows {
        let eligible =
            event.author_node_id == local_node_id && super::should_sync_event(node, &event)?;
        let route = if eligible {
            super::event_transport_route(node, &event)?
        } else {
            None
        };
        let Some(route) = route else {
            advance_partition(node, scope_id, partition, cursor, sequence)?;
            cursor = sequence;
            continue;
        };
        if route.scope == SwarmScope::Global && !route.public_global_control {
            advance_partition(node, scope_id, partition, cursor, sequence)?;
            cursor = sequence;
            continue;
        }
        let urgency = if matches!(route.scope, SwarmScope::Node(_)) {
            EventDeliveryUrgency::ExplicitRecipient
        } else if route.public_global_control {
            EventDeliveryUrgency::TimeSensitiveControl
        } else {
            EventDeliveryUrgency::Background
        };
        let delivery_class = delivery_class_for_record(DeliveryClassInput {
            lane: route.lane,
            event_urgency: urgency,
        });
        let mut frames = Vec::new();
        if CsOutboundPartition::for_route(&route.scope, delivery_class) == partition {
            frames.push((
                PublishFrame {
                    framing_version: "1".to_owned(),
                    delivery_policy_version,
                    record_id: event.event_id.clone(),
                    route: PublishRoute {
                        network_id: network_id.to_owned(),
                        transport: route.clone(),
                    },
                    payload_type: PublishPayloadType::Event,
                    payload: OpaqueSignedRecord::new(serde_json::to_vec(&event)?)?,
                },
                delivery_class,
            ));
        }

        if CsOutboundPartition::for_route(&route.scope, DeliveryClass::Bulk) == partition {
            if let Some(checkpoint) =
                super::announcements::checkpoint_announcement_for_event(node, &event, &route.scope)?
            {
                super::announcements::apply_checkpoint_announcement_to_store(
                    &node.store,
                    &checkpoint,
                )?;
                frames.push((
                    checkpoint_frame(network_id, delivery_policy_version, checkpoint)?,
                    DeliveryClass::Bulk,
                ));
            }
            if publish_summaries && !local_node_penalized {
                let mut summaries = Vec::new();
                if let Some(summary) = super::knowledge_summary_for_event(
                    node,
                    &event,
                    &route.scope,
                    node.store
                        .load_verified_network_protocol_params()
                        .map(|verified| verified.params().summary_decision_memory_limit)
                        .unwrap_or_else(|_| {
                            wattswarm_protocol::types::NetworkProtocolParams::default()
                                .summary_decision_memory_limit
                        }),
                )? {
                    summaries.push(summary);
                }
                if let Some(summary) =
                    super::task_outcome_summary_for_event(node, &event, &route.scope)?
                {
                    summaries.push(summary);
                }
                if let Some(summary) = super::reputation_summary_for_event(node, &event)? {
                    summaries.push(summary);
                }
                for summary in summaries {
                    frames.push((
                        summary_frame(network_id, delivery_policy_version, summary)?,
                        DeliveryClass::Bulk,
                    ));
                }
            }
        }

        let mut accepted = true;
        for (frame, expected_class) in &frames {
            match client.publish(token, frame) {
                Ok(acceptance) if acceptance.delivery_class == *expected_class => {
                    published = published.saturating_add(1);
                }
                Ok(_) => bail!("Gateway acceptance delivery class differs from frozen policy"),
                Err(error) => {
                    accepted = false;
                    node.store.retry_cs_outbound_progress(
                        scope_id,
                        EVENT_SOURCE_ID,
                        partition.as_str(),
                        cursor,
                        now_ms().saturating_add(1_000),
                        &error.to_string(),
                        now_ms(),
                    )?;
                    break;
                }
            }
        }
        if !accepted {
            break;
        }
        advance_partition(node, scope_id, partition, cursor, sequence)?;
        cursor = sequence;
    }
    Ok(published)
}

fn checkpoint_frame(
    network_id: &str,
    delivery_policy_version: u64,
    checkpoint: CheckpointAnnouncement,
) -> Result<PublishFrame> {
    let route = announcement_route(
        checkpoint.scope.clone(),
        PropagationLane::Checkpoints,
        checkpoint.scope == SwarmScope::Global,
    )?;
    Ok(PublishFrame {
        framing_version: "1".to_owned(),
        delivery_policy_version,
        record_id: checkpoint.checkpoint_id.clone(),
        route: PublishRoute {
            network_id: network_id.to_owned(),
            transport: route,
        },
        payload_type: PublishPayloadType::Checkpoint,
        payload: OpaqueSignedRecord::new(serde_json::to_vec(&checkpoint)?)?,
    })
}

fn summary_frame(
    network_id: &str,
    delivery_policy_version: u64,
    summary: SummaryAnnouncement,
) -> Result<PublishFrame> {
    let route = announcement_route(summary.scope.clone(), PropagationLane::Summaries, false)?;
    Ok(PublishFrame {
        framing_version: "1".to_owned(),
        delivery_policy_version,
        record_id: summary.summary_id.clone(),
        route: PublishRoute {
            network_id: network_id.to_owned(),
            transport: route,
        },
        payload_type: PublishPayloadType::Summary,
        payload: OpaqueSignedRecord::new(serde_json::to_vec(&summary)?)?,
    })
}

fn announcement_route(
    scope: SwarmScope,
    lane: PropagationLane,
    public_global_control: bool,
) -> Result<EventTransportRoute> {
    EventTransportRoute::from_kind_label(scope, lane, lane.as_str(), public_global_control)
}

fn advance_partition(
    node: &crate::node::Node,
    scope_id: &str,
    partition: CsOutboundPartition,
    expected: u64,
    sequence: u64,
) -> Result<()> {
    if !node.store.advance_cs_outbound_progress(
        scope_id,
        EVENT_SOURCE_ID,
        partition.as_str(),
        expected,
        sequence,
        now_ms(),
    )? {
        bail!("ClientServer outbound progress changed concurrently");
    }
    Ok(())
}

fn validate_delivery_page_binding(
    page: &DeliveryPage,
    expected_network_id: &str,
    expected_principal_id: &str,
    expected_delivery_class: DeliveryClass,
) -> Result<()> {
    page.validate()?;
    if page.binding.network_id != expected_network_id
        || page.binding.recipient_principal_id != expected_principal_id
        || page.binding.delivery_class != expected_delivery_class
    {
        bail!("ClientServer delivery page binding does not match the local mailbox");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn receive_page(
    client: &ClientServerClient,
    token: &str,
    node: &mut crate::node::Node,
    scope_id: &str,
    delivery_class: DeliveryClass,
    content_fetcher: &dyn ContentFetcher,
    state_dir: &Path,
    identity: &crate::crypto::NodeIdentity,
) -> Result<u64> {
    let Some(page) = client.pull_page(token, delivery_class)? else {
        return Ok(0);
    };
    validate_delivery_page_binding(
        &page,
        &super::current_network_context_id(node),
        &node.node_id(),
        delivery_class,
    )?;
    for gap in &page.gaps {
        node.store.save_cs_mailbox_gap(
            scope_id,
            &gap.gap_id,
            gap.delivery_class.as_str(),
            gap.delivery_policy_version,
            &serde_json::to_string(&gap.route)?,
            match gap.reason {
                wattswarm_network_transport_core::MailboxGapReason::Expired => "expired",
                wattswarm_network_transport_core::MailboxGapReason::DeliveryLimitExceeded => {
                    "delivery_limit_exceeded"
                }
                wattswarm_network_transport_core::MailboxGapReason::AdministrativeRemoval => {
                    "administrative_removal"
                }
            },
            gap.first_affected_at,
            gap.last_affected_at,
            gap.approximate_count,
            &page.page_id,
            now_ms(),
        )?;
    }
    for control in &page.controls {
        if node
            .store
            .load_cs_mailbox_delivery_state(scope_id, &control.delivery_id)?
            .is_none()
        {
            apply_control_delivery(client, token, node, state_dir, identity, control)?;
        }
        node.store.save_cs_mailbox_delivery_state(
            scope_id,
            &CsMailboxDeliveryState {
                delivery_id: control.delivery_id.clone(),
                record_id: control.correlation_id.clone(),
                delivery_class: delivery_class.as_str().to_owned(),
                delivery_policy_version: wattswarm_network_client_server::DELIVERY_POLICY_VERSION,
                result_status: "commit_pending".to_owned(),
                page_id: Some(page.page_id.clone()),
                pending_commit_token: Some(page.commit_token.expose().to_owned()),
                last_error: None,
            },
            now_ms(),
        )?;
    }
    for delivery in &page.deliveries {
        let record_id = match delivery.route.lane {
            PropagationLane::Events | PropagationLane::Messages => {
                let event: crate::types::Event =
                    serde_json::from_slice(delivery.record.as_bytes())?;
                let expected_route = super::event_transport_route(node, &event)?
                    .context("received Event has no transport route")?;
                if event.event_id != delivery.record_id || expected_route != delivery.route {
                    bail!("ClientServer delivery route does not match signed Event scope");
                }
                if event.author_node_id != node.node_id() {
                    let mut fetched = Vec::new();
                    for artifact in event_artifact_refs(&event.payload) {
                        let content = match content_fetcher.fetch(artifact) {
                            Ok(content) => content,
                            Err(error) => {
                                let detail = format!("content_unavailable: {error:#}");
                                node.store.save_cs_mailbox_delivery_state(
                                    scope_id,
                                    &CsMailboxDeliveryState {
                                        delivery_id: delivery.delivery_id.clone(),
                                        record_id: delivery.record_id.clone(),
                                        delivery_class: delivery_class.as_str().to_owned(),
                                        delivery_policy_version:
                                            wattswarm_network_client_server::DELIVERY_POLICY_VERSION,
                                        result_status: "failed".to_owned(),
                                        page_id: None,
                                        pending_commit_token: None,
                                        last_error: Some(detail.clone()),
                                    },
                                    now_ms(),
                                )?;
                                return Err(anyhow::Error::new(error).context(detail));
                            }
                        };
                        fetched.push((artifact.digest.clone(), content));
                    }
                    node.ingest_remote_if_new(event.clone())?;
                    // A crash can happen after Event ingest but before Inbox persistence. Replay
                    // must therefore repair all idempotent post-processing before page commit.
                    hydrate_client_server_event(node, state_dir, &event, &fetched)?;
                    post_process_client_server_event(
                        node,
                        state_dir,
                        &event,
                        &delivery.route.scope,
                    )?;
                }
                event.event_id
            }
            PropagationLane::Rules => {
                let record_id = crate::crypto::sha256_hex(delivery.record.as_bytes());
                if record_id != delivery.record_id {
                    bail!("ClientServer Rule identity mismatch");
                }
                let rule: wattswarm_network_transport_core::RuleAnnouncement =
                    serde_json::from_slice(delivery.record.as_bytes())?;
                if announcement_route(
                    rule.scope.clone(),
                    PropagationLane::Rules,
                    rule.scope == SwarmScope::Global,
                )? != delivery.route
                {
                    bail!("ClientServer Rule route mismatch");
                }
                super::apply_rule_announcement(node, &rule)?;
                record_id
            }
            PropagationLane::Checkpoints => {
                let checkpoint: CheckpointAnnouncement =
                    serde_json::from_slice(delivery.record.as_bytes())?;
                if checkpoint.checkpoint_id != delivery.record_id
                    || announcement_route(
                        checkpoint.scope.clone(),
                        PropagationLane::Checkpoints,
                        checkpoint.scope == SwarmScope::Global,
                    )? != delivery.route
                {
                    bail!("ClientServer Checkpoint identity or route mismatch");
                }
                super::apply_checkpoint_announcement(node, &checkpoint)?;
                checkpoint.checkpoint_id
            }
            PropagationLane::Summaries => {
                let summary: SummaryAnnouncement =
                    serde_json::from_slice(delivery.record.as_bytes())?;
                if summary.summary_id != delivery.record_id
                    || announcement_route(summary.scope.clone(), PropagationLane::Summaries, false)?
                        != delivery.route
                {
                    bail!("ClientServer Summary identity or route mismatch");
                }
                super::apply_summary_announcement(node, &summary)?;
                summary.summary_id
            }
        };
        node.store.save_cs_mailbox_delivery_state(
            scope_id,
            &CsMailboxDeliveryState {
                delivery_id: delivery.delivery_id.clone(),
                record_id,
                delivery_class: delivery_class.as_str().to_owned(),
                delivery_policy_version: wattswarm_network_client_server::DELIVERY_POLICY_VERSION,
                result_status: "commit_pending".to_owned(),
                page_id: Some(page.page_id.clone()),
                pending_commit_token: Some(page.commit_token.expose().to_owned()),
                last_error: None,
            },
            now_ms(),
        )?;
    }
    node.store.save_cs_mailbox_pending_commit(
        scope_id,
        &page.page_id,
        delivery_class.as_str(),
        page.commit_token.expose(),
        now_ms(),
    )?;
    client.commit(
        token,
        &CommitRequest {
            page_id: page.page_id.clone(),
            delivery_class,
            commit_token: page.commit_token,
        },
    )?;
    node.store.mark_cs_mailbox_page_committed(
        scope_id,
        &page.page_id,
        delivery_class.as_str(),
        now_ms(),
    )?;
    node.store.mark_cs_mailbox_gaps_acknowledged(
        scope_id,
        &page.page_id,
        delivery_class.as_str(),
        now_ms(),
    )?;
    node.store
        .clear_cs_mailbox_pending_commit(scope_id, &page.page_id, delivery_class.as_str())?;
    Ok((page.deliveries.len() + page.controls.len() + page.gaps.len()) as u64)
}

fn retry_pending_commits(
    client: &ClientServerClient,
    token: &str,
    store: &PgStore,
    scope_id: &str,
) -> Result<u64> {
    let mut committed = 0_u64;
    for pending in store.list_cs_mailbox_pending_commits(scope_id)? {
        let delivery_class = match pending.delivery_class.as_str() {
            "interactive" => DeliveryClass::Interactive,
            "bulk" => DeliveryClass::Bulk,
            _ => bail!("stored ClientServer pending commit has invalid delivery class"),
        };
        let request = CommitRequest {
            page_id: pending.page_id.clone(),
            delivery_class,
            commit_token: OpaqueCommitToken::new(pending.commit_token.clone())?,
        };
        match client.commit(token, &request) {
            Ok(()) => {
                store.mark_cs_mailbox_page_committed(
                    scope_id,
                    &pending.page_id,
                    &pending.delivery_class,
                    now_ms(),
                )?;
                store.mark_cs_mailbox_gaps_acknowledged(
                    scope_id,
                    &pending.page_id,
                    &pending.delivery_class,
                    now_ms(),
                )?;
                store.clear_cs_mailbox_pending_commit(
                    scope_id,
                    &pending.page_id,
                    &pending.delivery_class,
                )?;
                committed = committed.saturating_add(1);
            }
            Err(error) if http_error_status(&error) == Some(reqwest::StatusCode::BAD_REQUEST) => {
                store.clear_cs_mailbox_pending_commit(
                    scope_id,
                    &pending.page_id,
                    &pending.delivery_class,
                )?;
            }
            Err(error) => {
                store.record_cs_mailbox_pending_commit_error(
                    scope_id,
                    &pending.page_id,
                    &pending.delivery_class,
                    &error.to_string(),
                    now_ms(),
                )?;
                return Err(error);
            }
        }
    }
    Ok(committed)
}

fn apply_control_delivery(
    client: &ClientServerClient,
    token: &str,
    node: &mut crate::node::Node,
    state_dir: &Path,
    identity: &crate::crypto::NodeIdentity,
    control: &MailboxControlDelivery,
) -> Result<()> {
    if control.target_principal_id != node.node_id() {
        bail!("ClientServer control target does not match local node");
    }
    match control.control_kind.as_str() {
        "peer_relationship" => {
            let payload: super::peer_interactions::CsPeerRelationshipControlPayload =
                serde_json::from_slice(control.payload.as_bytes())?;
            if payload.agent_envelope.source_node_id.as_deref()
                != Some(control.source_principal_id.as_str())
                || payload.agent_envelope.target_node_id.as_deref()
                    != Some(control.target_principal_id.as_str())
            {
                bail!("ClientServer relationship AgentEnvelope binding mismatch");
            }
            super::verify_agent_envelope_signature_for_source(
                &payload.agent_envelope,
                Some(&control.source_principal_id),
            )?;
            super::upsert_contact_material_for_peer(
                state_dir,
                &control.source_principal_id,
                &payload.contact_material,
            )?;
            let (record, replayed) = super::apply_peer_relationship_action_projection(
                state_dir,
                &control.source_principal_id,
                payload.action,
                crate::control::PeerRelationshipInitiator::Remote,
                &payload.agent_envelope,
            )?;
            if payload.action == crate::control::PeerRelationshipAction::Accept
                && record.relationship_state == crate::control::PeerRelationshipState::Accepted
            {
                super::peer_interactions::finalize_client_server_dm_session(
                    node,
                    state_dir,
                    &control.source_principal_id,
                    crate::control::PeerDmDirection::Inbound,
                    &payload.agent_envelope.protocol,
                    record.updated_at,
                )?;
            }
            if payload.action == crate::control::PeerRelationshipAction::Request && !replayed {
                let requested_at = record.requested_at.unwrap_or(record.updated_at);
                let request_key = super::raw_relationship_request_id(&payload.agent_envelope)
                    .unwrap_or_else(|| requested_at.to_string());
                let event = super::build_agent_event_with_agent_envelope(
                    wattswarm_protocol::types::AgentEventType::FriendRequest,
                    wattswarm_protocol::types::AgentEventSourceKind::PeerRelationship,
                    Some(control.source_principal_id.clone()),
                    payload.agent_envelope.target_agent_id.clone(),
                    Some(super::raw_agent_envelope_to_protocol(
                        &payload.agent_envelope,
                    )),
                    serde_json::json!({
                        "source_node_id": control.source_principal_id,
                        "target_node_id": control.target_principal_id,
                        "action": payload.action,
                        "relationship_state": record.relationship_state,
                        "updated_at": record.updated_at,
                    }),
                    true,
                    vec!["accept".to_owned(), "reject".to_owned(), "block".to_owned()],
                    Some(control.source_principal_id.clone()),
                    Some(format!(
                        "friend_request:{}:{}",
                        control.source_principal_id, request_key
                    )),
                );
                super::enqueue_agent_event_for_local_executor(state_dir, &event)?;
            }
            let ack = super::peer_interactions::CsPeerRelationshipAckPayload {
                acknowledged_correlation_id: control.correlation_id.clone(),
            };
            send_signed_control(
                client,
                token,
                identity,
                super::current_network_context_id(node),
                format!("ack:{}", control.correlation_id),
                control.source_principal_id.clone(),
                ControlFrameKind::PeerRelationshipAck,
                serde_json::to_vec(&ack)?,
            )?;
        }
        "peer_relationship_ack" => {
            let ack: super::peer_interactions::CsPeerRelationshipAckPayload =
                serde_json::from_slice(control.payload.as_bytes())?;
            crate::storage::local_control_store(state_dir)?.acknowledge_pending_network_command(
                &crate::storage::local_control_scope_id(state_dir),
                &ack.acknowledged_correlation_id,
            )?;
        }
        "contact_material_request" => {
            let request: super::RawContactMaterialRequest =
                serde_json::from_slice(control.payload.as_bytes())?;
            request.validate()?;
            if request.source_node_id != control.source_principal_id
                || request.target_node_id != control.target_principal_id
            {
                bail!("ClientServer contact request binding mismatch");
            }
            let response = super::RawContactMaterialResponse {
                source_node_id: control.target_principal_id.clone(),
                target_node_id: control.source_principal_id.clone(),
                applied: true,
                contact_material: Some(build_client_server_contact_material(
                    state_dir,
                    identity,
                    &crate::network_service::client_server_url_from_env_or_startup_config(
                        state_dir,
                    )?,
                )?),
                detail: None,
                updated_at: now_ms(),
            };
            send_signed_control(
                client,
                token,
                identity,
                super::current_network_context_id(node),
                format!("response:{}", control.correlation_id),
                control.source_principal_id.clone(),
                ControlFrameKind::ContactMaterialResponse,
                serde_json::to_vec(&response)?,
            )?;
        }
        "contact_material_response" => {
            let response: super::RawContactMaterialResponse =
                serde_json::from_slice(control.payload.as_bytes())?;
            response.validate()?;
            if response.source_node_id != control.source_principal_id
                || response.target_node_id != control.target_principal_id
            {
                bail!("ClientServer contact response binding mismatch");
            }
            if !response.applied {
                bail!(
                    "ClientServer contact request was rejected: {}",
                    response.detail.as_deref().unwrap_or("unknown")
                );
            }
            let contact_material = response
                .contact_material
                .as_ref()
                .context("applied ClientServer contact response omitted contact material")?;
            super::upsert_contact_material_for_peer(
                state_dir,
                &control.source_principal_id,
                contact_material,
            )?;
        }
        "direct_rpc" => {
            bail!("unsupported optional ClientServer control kind: direct_rpc")
        }
        other => bail!("unknown ClientServer control kind: {other}"),
    }
    Ok(())
}

fn hydrate_client_server_event(
    node: &mut crate::node::Node,
    state_dir: &Path,
    event: &crate::types::Event,
    fetched: &[(String, crate::network_service::FetchedContent)],
) -> Result<()> {
    let content_for = |digest: &str| {
        fetched
            .iter()
            .find(|(candidate, _)| candidate == digest)
            .map(|(_, content)| content.bytes.as_slice())
    };
    let persist = |kind, reference: &crate::types::ArtifactRef| -> Result<()> {
        let bytes = content_for(&reference.digest)
            .with_context(|| format!("artifact {} was not fetched", reference.digest))?;
        crate::control::materialize_content_artifact_from_reference(
            state_dir,
            kind,
            reference,
            bytes,
            now_ms(),
        )?;
        Ok(())
    };
    match &event.payload {
        crate::types::EventPayload::CandidateProposed(payload) => {
            persist(
                wattswarm_artifact_store::ArtifactKind::Reference,
                &payload.candidate.output_ref,
            )?;
            for reference in &payload.candidate.evidence_refs {
                persist(wattswarm_artifact_store::ArtifactKind::Evidence, reference)?;
            }
            let bytes = content_for(&payload.candidate.output_ref.digest)
                .context("candidate output was not fetched")?;
            let output: serde_json::Value = serde_json::from_slice(bytes)?;
            node.store.update_candidate_output(
                &payload.task_id,
                &payload.candidate.candidate_id,
                &output,
                now_ms(),
            )?;
        }
        crate::types::EventPayload::EvidenceAdded(payload) => {
            for reference in &payload.evidence_refs {
                persist(wattswarm_artifact_store::ArtifactKind::Evidence, reference)?;
            }
        }
        crate::types::EventPayload::TaskAnnounced(payload) => {
            if let Some(reference) = &payload.detail_ref {
                persist(wattswarm_artifact_store::ArtifactKind::Reference, reference)?;
            }
        }
        crate::types::EventPayload::TopicMessagePosted(payload) => {
            persist(
                wattswarm_artifact_store::ArtifactKind::TopicMessage,
                &payload.content_ref,
            )?;
            let bytes = content_for(&payload.content_ref.digest)
                .context("topic message content was not fetched")?;
            let content: serde_json::Value = serde_json::from_slice(bytes)?;
            node.store
                .update_topic_message_content(&event.event_id, &content, now_ms())?;
        }
        _ => {}
    }
    Ok(())
}

fn post_process_client_server_event(
    node: &crate::node::Node,
    state_dir: &Path,
    event: &crate::types::Event,
    scope: &SwarmScope,
) -> Result<()> {
    super::log_run_queue_events_if_applicable(node, state_dir, event);
    let local_node_id = node.node_id();
    if super::EventRelevanceFilter::should_deliver(node, state_dir, &local_node_id, event) {
        let agent_event = match &event.payload {
            crate::types::EventPayload::TaskClaimed(payload) => {
                Some(super::task_claim_agent_event(node, event, payload)?)
            }
            crate::types::EventPayload::TaskClaimDecided(payload) => Some(
                super::task_claim_decision_agent_event(node, event, payload)?,
            ),
            crate::types::EventPayload::CandidateProposed(_)
            | crate::types::EventPayload::TaskCompleted(_)
            | crate::types::EventPayload::DecisionFinalized(_)
            | crate::types::EventPayload::TaskError(_)
            | crate::types::EventPayload::TaskRetryScheduled(_) => {
                super::task_result_agent_event(node, event)?
            }
            crate::types::EventPayload::TaskCompletionDecided(payload) => Some(
                super::task_completion_decision_agent_event(node, event, payload)?,
            ),
            crate::types::EventPayload::TaskSettled(payload) => {
                Some(super::task_settled_agent_event(node, event, payload)?)
            }
            _ => None,
        };
        if let Some(agent_event) = agent_event {
            super::enqueue_agent_event_for_local_executor(state_dir, &agent_event)?;
        }
    }
    if let crate::types::EventPayload::AgentPaymentPosted(payload) = &event.payload {
        super::save_agent_payment_event(
            state_dir,
            &event.author_node_id,
            &event.event_id,
            payload,
        )?;
    }
    if let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload {
        super::maybe_record_topic_cursor_for_event_id(
            node,
            &local_node_id,
            &payload.feed_key,
            scope,
            &event.event_id,
            event.created_at,
        )?;
        if payload.feed_key == crate::control::PRIVATE_DM_FEED_KEY
            && let Some(topic_message) = node.store.get_topic_message(&event.event_id)?
            && let Some(projection) = super::save_inbound_private_dm_topic_message(
                state_dir,
                &local_node_id,
                &event.author_node_id,
                &event.event_id,
                &topic_message.content,
                event.created_at,
            )?
        {
            node.store.update_topic_message_content(
                &event.event_id,
                &projection.topic_content,
                event.created_at,
            )?;
        }
        super::decrypt_private_hive_topic_event(node, state_dir, event, payload)?;
        if super::EventRelevanceFilter::should_deliver(node, state_dir, &local_node_id, event)
            && let Some(agent_event) = super::topic_message_agent_event(node, event, payload)?
        {
            super::enqueue_agent_event_for_local_executor(state_dir, &agent_event)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn send_signed_control(
    client: &ClientServerClient,
    token: &str,
    identity: &crate::crypto::NodeIdentity,
    network_id: String,
    correlation_id: String,
    target_principal_id: String,
    control_kind: ControlFrameKind,
    payload: Vec<u8>,
) -> Result<()> {
    let mut frame = ControlFrame {
        framing_version: "1".to_owned(),
        network_id,
        correlation_id,
        source_principal_id: identity.node_id(),
        target_principal_id,
        control_kind,
        payload: OpaqueSignedRecord::new(payload)?,
        signature_hex: String::new(),
    };
    frame.signature_hex = identity.sign_bytes(&control_frame_signing_message(&frame)?);
    client.send_control(token, &frame)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn store_backend_status(
    store: &PgStore,
    scope_id: &str,
    status: &str,
    reason: Option<&str>,
    published: u64,
    received: u64,
    retries: u64,
    session_expires_at: Option<u64>,
    gateway_history_unavailable: bool,
    observability: &CsRuntimeObservability,
) -> Result<()> {
    let source_head = store.head_seq()?;
    let partitions = CsOutboundPartition::ALL
        .into_iter()
        .filter_map(|partition| {
            store
                .load_cs_outbound_progress(scope_id, EVENT_SOURCE_ID, partition.as_str())
                .ok()
                .flatten()
                .map(|progress| {
                    let oldest_blocked_record = progress.last_error.as_ref().map(|error| {
                        serde_json::json!({
                            "source_sequence": progress.scanned_sequence.saturating_add(1),
                            "error": error,
                        })
                    });
                    serde_json::json!({
                        "source_id": progress.source_id,
                        "partition": progress.outbound_partition,
                        "scanned_sequence": progress.scanned_sequence,
                        "source_head": source_head,
                        "cursor_lag": source_head.saturating_sub(progress.scanned_sequence),
                        "delivery_policy_version": progress.delivery_policy_version,
                        "retry_attempts": progress.retry_attempts,
                        "consecutive_retries": progress.retry_attempts,
                        "next_retry_at": progress.next_retry_at,
                        "oldest_blocked_error": progress.last_error,
                        "oldest_blocked_record": oldest_blocked_record,
                        "partition_isolated": true,
                        "updated_at": progress.updated_at,
                    })
                })
        })
        .collect::<Vec<_>>();
    let pending_commits = store.list_cs_mailbox_pending_commits(scope_id)?;
    let backend_details_json = serde_json::to_string(&serde_json::json!({
        "gateway_connected": status != "failed",
        "session_expires_at": session_expires_at,
        "delivery_policy_version": wattswarm_network_client_server::DELIVERY_POLICY_VERSION,
        "gateway_session": {
            "expires_at": session_expires_at,
            "authentication_latency": observability.session.json(),
        },
        "publisher_confirm_latency": observability.publisher_confirm.json(),
        "delivery_page_latency": observability.delivery_page.json(),
        "outbound_progress": partitions,
        "mailbox_scheduler": {
            "interactive_burst_pages": 4,
            "bulk_min_pages_per_cycle": 1,
            "classes": ["interactive", "bulk"],
        },
        "history_integrity": {
            "history_unavailable": gateway_history_unavailable
                || store.cs_mailbox_gap_count(scope_id, "interactive")?
                    .saturating_add(store.cs_mailbox_gap_count(scope_id, "bulk")?) > 0,
            "interactive_gap_count": store.cs_mailbox_gap_count(scope_id, "interactive")?,
            "bulk_gap_count": store.cs_mailbox_gap_count(scope_id, "bulk")?,
            "interactive_pending_ack": store.unacknowledged_cs_mailbox_gap_count(scope_id, "interactive")?,
            "bulk_pending_ack": store.unacknowledged_cs_mailbox_gap_count(scope_id, "bulk")?,
        },
        "cumulative_commit": {
            "retry_count": observability.cumulative_commit_retries,
            "retry_latency": observability.commit.json(),
            "pending_pages": pending_commits.len(),
            "pending_attempts": pending_commits
                .iter()
                .map(|pending| u64::from(pending.attempts))
                .sum::<u64>(),
        },
        "last_error": reason,
    }))?;
    store.store_network_backend_status(
        scope_id,
        &NetworkBackendStatusRow {
            backend: Some("client_server".to_owned()),
            status: status.to_owned(),
            reason: reason.map(ToOwned::to_owned),
            published,
            received,
            retries,
            backend_details_json,
            updated_at: now_ms(),
        },
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_session_rejected(error: &anyhow::Error) -> bool {
    http_error_status(error) == Some(reqwest::StatusCode::UNAUTHORIZED)
}

fn http_error_status(error: &anyhow::Error) -> Option<reqwest::StatusCode> {
    error.chain().find_map(|source| {
        source
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_partitions_are_private_and_exhaustive() {
        assert_eq!(
            CsOutboundPartition::ALL.map(CsOutboundPartition::as_str),
            [
                "global_interactive",
                "global_bulk",
                "non_global_interactive",
                "non_global_bulk",
            ]
        );
        assert_eq!(
            CsOutboundPartition::for_route(&SwarmScope::Global, DeliveryClass::Bulk),
            CsOutboundPartition::GlobalBulk
        );
        assert_eq!(
            CsOutboundPartition::for_route(
                &SwarmScope::Node("node-a".to_owned()),
                DeliveryClass::Interactive,
            ),
            CsOutboundPartition::NonGlobalInteractive
        );
    }

    #[test]
    fn client_server_backend_status_exposes_partition_and_latency_details() {
        let store = PgStore::open_in_memory().unwrap().for_org("org-a");
        let partitions = CsOutboundPartition::ALL.map(CsOutboundPartition::as_str);
        store
            .initialize_cs_outbound_progress(
                "scope-a",
                EVENT_SOURCE_ID,
                &partitions,
                wattswarm_network_client_server::DELIVERY_POLICY_VERSION,
                0,
                1,
            )
            .unwrap();
        store_backend_status(
            &store,
            "scope-a",
            "ready",
            None,
            2,
            3,
            4,
            Some(5),
            false,
            &CsRuntimeObservability::default(),
        )
        .unwrap();

        let row = store
            .load_network_backend_status("scope-a")
            .unwrap()
            .unwrap();
        let details: serde_json::Value = serde_json::from_str(&row.backend_details_json).unwrap();
        assert_eq!(details["outbound_progress"].as_array().unwrap().len(), 4);
        assert_eq!(details["outbound_progress"][0]["cursor_lag"], 0);
        assert_eq!(details["outbound_progress"][0]["partition_isolated"], true);
        assert_eq!(details["outbound_progress"][0]["consecutive_retries"], 0);
        assert!(details["outbound_progress"][0]["oldest_blocked_record"].is_null());
        assert_eq!(details["gateway_session"]["expires_at"], 5);
        assert_eq!(details["publisher_confirm_latency"]["count"], 0);
        assert_eq!(details["delivery_page_latency"]["count"], 0);
        assert_eq!(details["cumulative_commit"]["pending_pages"], 0);
    }

    #[test]
    fn client_server_requires_a_fresh_event_store_for_first_start() {
        assert!(validate_client_server_outbound_progress_state(0, 4, 0).is_ok());
        assert!(validate_client_server_outbound_progress_state(4, 4, 12).is_ok());
        assert!(validate_client_server_outbound_progress_state(0, 4, 12).is_err());
        assert!(validate_client_server_outbound_progress_state(2, 4, 0).is_err());
    }

    #[test]
    fn client_server_contact_material_does_not_require_or_advertise_iroh() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-client-server-contact-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&state_dir).unwrap();
        let identity = crate::crypto::NodeIdentity::random();
        let contact = build_client_server_contact_material(
            &state_dir,
            &identity,
            "https://gateway.example.test",
        )
        .unwrap();
        let material: serde_json::Value = serde_json::from_str(&contact.material_json).unwrap();

        assert_eq!(material["node_id"], identity.node_id());
        assert_eq!(
            material["transports"][0]["transport"],
            "client_server_gateway"
        );
        assert_eq!(
            material["transports"][0]["gateway_url"],
            "https://gateway.example.test"
        );
        assert!(!contact.material_json.contains("iroh"));
        assert!(
            material["encryption"]["private_message"]["public_key_b64"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        crate::crypto::verify_signature(
            &identity.node_id(),
            contact.material_json.as_bytes(),
            contact.signature.as_deref().unwrap(),
        )
        .unwrap();
        std::fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn delivery_page_binding_must_match_the_local_mailbox() {
        let page = DeliveryPage {
            page_id: "page-1".to_owned(),
            binding: wattswarm_network_transport_core::MailboxBinding {
                network_id: "network-a".to_owned(),
                recipient_principal_id: "principal-a".to_owned(),
                delivery_class: DeliveryClass::Interactive,
            },
            deliveries: Vec::new(),
            controls: vec![MailboxControlDelivery {
                delivery_id: "delivery-1".to_owned(),
                correlation_id: "correlation-1".to_owned(),
                source_principal_id: "principal-b".to_owned(),
                target_principal_id: "principal-a".to_owned(),
                control_kind: "peer_relationship".to_owned(),
                payload: OpaqueSignedRecord::new(b"signed-control".to_vec()).unwrap(),
                enqueued_at: 1,
                expires_at: None,
            }],
            gaps: Vec::new(),
            commit_token: OpaqueCommitToken::new("commit-1").unwrap(),
        };
        validate_delivery_page_binding(
            &page,
            "network-a",
            "principal-a",
            DeliveryClass::Interactive,
        )
        .unwrap();
        assert!(
            validate_delivery_page_binding(
                &page,
                "network-b",
                "principal-a",
                DeliveryClass::Interactive,
            )
            .is_err()
        );
        assert!(
            validate_delivery_page_binding(&page, "network-a", "principal-a", DeliveryClass::Bulk,)
                .is_err()
        );
        assert!(
            validate_delivery_page_binding(
                &page,
                "network-a",
                "principal-b",
                DeliveryClass::Interactive,
            )
            .is_err()
        );
    }

    #[test]
    fn fetched_client_server_content_is_materialized_in_the_local_artifact_store() {
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-client-server-artifact-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let bytes = br#"{"result":"stored"}"#;
        let reference = crate::types::ArtifactRef {
            uri: "https://gateway.example.test/v1/objects/content".to_owned(),
            digest: format!("sha256:{}", crate::crypto::sha256_hex(bytes)),
            size_bytes: bytes.len() as u64,
            mime: "application/json".to_owned(),
            created_at: 1,
            producer: "principal-b".to_owned(),
        };
        crate::control::materialize_content_artifact_from_reference(
            &state_dir,
            wattswarm_artifact_store::ArtifactKind::Reference,
            &reference,
            bytes,
            2,
        )
        .unwrap();
        let store = wattswarm_artifact_store::ArtifactStore::new(
            crate::control::artifact_store_path(&state_dir),
        );
        assert_eq!(
            store
                .read_validated_bytes(
                    wattswarm_artifact_store::ArtifactKind::Reference,
                    &reference.digest,
                    None,
                    Some(&reference.digest),
                    Some(reference.size_bytes),
                )
                .unwrap(),
            bytes
        );
        std::fs::remove_dir_all(state_dir).unwrap();
    }
}
