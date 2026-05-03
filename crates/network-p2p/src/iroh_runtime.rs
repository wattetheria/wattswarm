use crate::{
    BackfillRequest, BackfillRequestId, BackfillResponse, ContactMaterialRequest,
    ContactMaterialRequestId, ContactMaterialResponse, GossipKind, GossipMessage, InboundRequestId,
    NetworkRuntimeEvent, PeerDirectMessageRequest, PeerDirectMessageRequestId,
    PeerDirectMessageResponse, PeerRelationshipRequest, PeerRelationshipRequestId,
    PeerRelationshipResponse, SwarmScope, decode_overlay_gossip, encode_overlay_payload,
};
use anyhow::{Result, anyhow, bail};
use iroh::EndpointId;
use iroh_gossip::api::{Event as IrohGossipEvent, GossipSender};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    str::FromStr,
    sync::mpsc::{self, Receiver, Sender},
    time::{SystemTime, UNIX_EPOCH},
};
use wattswarm_network_substrate::RawGossipMessage;
use wattswarm_network_transport_core::TransportContactMaterial;
use wattswarm_network_transport_iroh::{
    IrohControlStreamRequest, IrohControlStreamResponse, derive_gossip_topic_id,
    export_local_contact_material_for_network_peer_id,
    install_local_contact_material_control_handler_for_network_peer_id,
    local_endpoint_id_from_state_dir, local_gossip_for_network_peer_id,
    register_remote_contact_material_for_network_peer_id,
    request_contact_material_for_network_peer_id, send_control_stream_request_for_network_peer_id,
    set_local_control_stream_handler_for_network_peer_id,
};

pub const IROH_CONTROL_KIND_BACKFILL: &str = "backfill.v1";
const MAX_INFLIGHT_REQUESTS_PER_REMOTE: u32 = 8;
const CONNECT_BACKOFF_BASE_MS: u64 = 500;
const CONNECT_BACKOFF_MAX_MS: u64 = 30_000;
const MAX_BACKFILL_STREAM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohRuntimeConfig {
    pub network_id: String,
    pub state_dir: PathBuf,
    pub local_network_peer_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IrohGossipSubscription {
    pub scope: SwarmScope,
    pub kind: GossipKind,
    pub topic_id_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohRuntimeObservabilitySnapshot {
    pub local_network_peer_id: String,
    pub local_endpoint_id: String,
    pub subscribed_topics: Vec<IrohGossipSubscription>,
    pub known_remote_contacts: usize,
    pub peer_quality: Vec<IrohPeerQualitySnapshot>,
    pub counters: IrohRuntimeCounters,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohRuntimeCounters {
    pub malformed_gossip_notifications: u64,
    pub scope_kind_mismatch_drops: u64,
    pub stale_contact_material_drops: u64,
    pub invalid_control_payloads: u64,
    pub stream_request_attempts: u64,
    pub stream_request_successes: u64,
    pub stream_request_failures: u64,
    pub retry_suppressed_connects: u64,
    pub request_limit_drops: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohPeerQualitySnapshot {
    pub network_peer_id: String,
    pub inflight_requests: u32,
    pub consecutive_failures: u32,
    pub retry_after_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PeerQualityState {
    inflight_requests: u32,
    consecutive_failures: u32,
    retry_after_ms: Option<u64>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrohGossipNotification {
    pub scope: SwarmScope,
    pub kind: GossipKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IrohBackfillStreamRequest {
    pub request: BackfillRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IrohBackfillStreamResponse {
    pub response: BackfillResponse,
}

pub struct IrohNetworkRuntime {
    config: IrohRuntimeConfig,
    runtime: tokio::runtime::Runtime,
    endpoint_id: String,
    local_endpoint_addrs: Vec<String>,
    subscriptions: HashSet<IrohGossipSubscription>,
    gossip_topics: HashMap<IrohGossipSubscription, IrohGossipTopicHandle>,
    gossip_tx: Sender<IrohGossipInbound>,
    gossip_rx: Receiver<IrohGossipInbound>,
    pending_events: VecDeque<NetworkRuntimeEvent>,
    remote_contacts: HashMap<String, TransportContactMaterial>,
    peer_quality: HashMap<String, PeerQualityState>,
    counters: IrohRuntimeCounters,
    next_inbound_request_id: u64,
    next_outbound_request_id: u64,
}

struct IrohGossipTopicHandle {
    sender: GossipSender,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
enum IrohGossipInbound {
    Message {
        subscription: IrohGossipSubscription,
        delivered_from: [u8; 32],
        bytes: Vec<u8>,
    },
    Malformed {
        remote_network_peer_id: String,
        error: String,
    },
    Lagged,
}

impl IrohNetworkRuntime {
    pub fn new(config: IrohRuntimeConfig) -> Result<Self> {
        let endpoint_id = local_endpoint_id_from_state_dir(Path::new(&config.state_dir))?;
        let endpoint_id = endpoint_id.to_string();
        if config.local_network_peer_id != endpoint_id {
            bail!(
                "iroh local network peer id {} does not match endpoint id {}",
                config.local_network_peer_id,
                endpoint_id
            );
        }
        let local_contact = export_local_contact_material_for_network_peer_id(
            &config.state_dir,
            &config.local_network_peer_id,
            unix_timestamp_millis() / 1000,
        )?;
        let runtime = tokio::runtime::Runtime::new()?;
        let (gossip_tx, gossip_rx) = mpsc::channel();
        Ok(Self {
            config,
            runtime,
            endpoint_id,
            local_endpoint_addrs: local_contact.metadata.listen_addrs,
            subscriptions: HashSet::new(),
            gossip_topics: HashMap::new(),
            gossip_tx,
            gossip_rx,
            pending_events: VecDeque::new(),
            remote_contacts: HashMap::new(),
            peer_quality: HashMap::new(),
            counters: IrohRuntimeCounters::default(),
            next_inbound_request_id: 1,
            next_outbound_request_id: 1,
        })
    }

    pub fn local_network_peer_id(&self) -> &str {
        &self.config.local_network_peer_id
    }

    pub fn local_endpoint_id(&self) -> &str {
        &self.endpoint_id
    }

    pub fn local_endpoint_addrs(&self) -> &[String] {
        &self.local_endpoint_addrs
    }

    pub fn subscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.subscribe_scope_kinds(scope, &GossipKind::ALL)
    }

    pub fn subscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        for kind in kinds {
            let subscription = self.subscription_for(scope, *kind)?;
            self.ensure_gossip_topic(subscription)?;
        }
        Ok(())
    }

    pub fn unsubscribe_scope(&mut self, scope: &SwarmScope) -> Result<()> {
        self.unsubscribe_scope_kinds(scope, &GossipKind::ALL)
    }

    pub fn unsubscribe_scope_kinds(
        &mut self,
        scope: &SwarmScope,
        kinds: &[GossipKind],
    ) -> Result<()> {
        for kind in kinds {
            let subscription = self.subscription_for(scope, *kind)?;
            self.subscriptions.remove(&subscription);
            if let Some(handle) = self.gossip_topics.remove(&subscription) {
                handle.task.abort();
            }
        }
        Ok(())
    }

    pub fn publish_gossip(&mut self, message: &GossipMessage) -> Result<()> {
        let bytes = self.encode_notification_bytes(message)?;
        let subscription = self.subscription_for(message.scope(), message.kind())?;
        self.ensure_gossip_topic(subscription.clone())?;
        let topic = self
            .gossip_topics
            .get(&subscription)
            .ok_or_else(|| anyhow!("missing iroh gossip topic {}", subscription.topic_id_hex))?;
        self.runtime
            .block_on(topic.sender.broadcast(bytes.into()))
            .map_err(|err| anyhow!("publish iroh gossip notification: {err}"))?;
        Ok(())
    }

    pub fn upsert_remote_contact_material(
        &mut self,
        remote_network_peer_id: impl Into<String>,
        contact: TransportContactMaterial,
    ) -> Result<bool> {
        let remote_network_peer_id = remote_network_peer_id.into();
        let contact_peer_alias = compat_peer_id_for_contact(&contact).map(|peer| peer.to_string());
        if contact.peer_id != remote_network_peer_id
            && contact_peer_alias.as_deref() != Some(remote_network_peer_id.as_str())
        {
            bail!(
                "remote contact material peer id {} does not match {}",
                contact.peer_id,
                remote_network_peer_id
            );
        }
        if self
            .remote_contacts
            .get(&remote_network_peer_id)
            .is_some_and(|existing| existing.metadata.generated_at > contact.metadata.generated_at)
        {
            self.counters.stale_contact_material_drops =
                self.counters.stale_contact_material_drops.saturating_add(1);
            return Ok(false);
        }
        register_remote_contact_material_for_network_peer_id(
            &self.config.state_dir,
            &self.config.local_network_peer_id,
            &contact,
        )?;
        self.remote_contacts
            .insert(remote_network_peer_id, contact.clone());
        if let Some(alias) = contact_peer_alias {
            self.remote_contacts.insert(alias, contact);
        }
        Ok(true)
    }

    pub fn request_backfill_page(
        &mut self,
        remote_network_peer_id: &str,
        request: BackfillRequest,
    ) -> Result<BackfillResponse> {
        self.begin_stream_request(remote_network_peer_id)?;
        let response = (|| {
            let expected_request = request.clone();
            let remote_contact = self
                .remote_contacts
                .get(remote_network_peer_id)
                .ok_or_else(|| {
                    anyhow!("missing iroh contact material for {remote_network_peer_id}")
                })?;
            let request = encode_backfill_control_request(request)?;
            send_control_stream_request_for_network_peer_id(
                &self.config.state_dir,
                &self.config.local_network_peer_id,
                remote_contact,
                &request,
            )
            .and_then(decode_backfill_control_response)
            .and_then(|response| validate_backfill_stream_response(&expected_request, response))
        })();
        self.finish_stream_request(remote_network_peer_id, response.as_ref().map(|_| ()));
        response
    }

    pub fn record_invalid_control_payload(&mut self, remote_network_peer_id: &str, error: &str) {
        self.counters.invalid_control_payloads =
            self.counters.invalid_control_payloads.saturating_add(1);
        self.record_peer_failure(remote_network_peer_id, error);
    }

    pub fn install_contact_material_stream_handler(&mut self) -> Result<()> {
        install_local_contact_material_control_handler_for_network_peer_id(
            &self.config.state_dir,
            &self.config.local_network_peer_id,
        )
    }

    pub fn request_remote_contact_material(
        &mut self,
        remote_network_peer_id: &str,
    ) -> Result<TransportContactMaterial> {
        self.begin_stream_request(remote_network_peer_id)?;
        let response = (|| {
            let bootstrap_contact = self
                .remote_contacts
                .get(remote_network_peer_id)
                .ok_or_else(|| {
                    anyhow!("missing iroh bootstrap contact material for {remote_network_peer_id}")
                })?;
            request_contact_material_for_network_peer_id(
                &self.config.state_dir,
                &self.config.local_network_peer_id,
                bootstrap_contact,
            )
        })();
        let stream_result = response.and_then(|contact| {
            self.upsert_remote_contact_material(
                remote_network_peer_id.to_owned(),
                contact.clone(),
            )?;
            Ok(contact)
        });
        self.finish_stream_request(remote_network_peer_id, stream_result.as_ref().map(|_| ()));
        stream_result
    }

    pub fn install_backfill_stream_handler(
        &mut self,
        handler: impl Fn(BackfillRequest) -> Result<BackfillResponse> + Send + Sync + 'static,
    ) -> Result<()> {
        set_local_control_stream_handler_for_network_peer_id(
            &self.config.state_dir,
            &self.config.local_network_peer_id,
            IROH_CONTROL_KIND_BACKFILL,
            Some(move |request: IrohControlStreamRequest| {
                match decode_backfill_control_request(&request)
                    .and_then(&handler)
                    .and_then(encode_backfill_control_response)
                {
                    Ok(response) => response,
                    Err(err) => IrohControlStreamResponse {
                        ok: false,
                        error: Some(err.to_string()),
                        payload: Vec::new(),
                    },
                }
            }),
        )
    }

    pub fn send_backfill_request(
        &mut self,
        remote_network_peer_id: &str,
        request: BackfillRequest,
    ) -> Result<BackfillRequestId> {
        let request_id = self.reserve_backfill_request_id();
        let peer = compat_peer_id_for_network_peer_id(remote_network_peer_id)?;
        match self.request_backfill_page(remote_network_peer_id, request) {
            Ok(response) => {
                self.pending_events
                    .push_back(NetworkRuntimeEvent::BackfillResponse {
                        peer,
                        request_id,
                        response,
                    });
            }
            Err(err) => {
                self.pending_events
                    .push_back(NetworkRuntimeEvent::BackfillOutboundFailure {
                        peer,
                        request_id,
                        error: err.to_string(),
                    });
            }
        }
        Ok(request_id)
    }

    pub fn send_backfill_response(
        &mut self,
        _request_id: InboundRequestId,
        _response: BackfillResponse,
    ) -> Result<()> {
        bail!("iroh backfill response streams are not wired yet")
    }

    pub fn send_contact_material_request(
        &mut self,
        _remote_network_peer_id: &str,
        _request: ContactMaterialRequest,
    ) -> Result<ContactMaterialRequestId> {
        bail!("iroh contact material request streams are not wired yet")
    }

    pub fn send_contact_material_response(
        &mut self,
        _request_id: InboundRequestId,
        _response: ContactMaterialResponse,
    ) -> Result<()> {
        bail!("iroh contact material response streams are not wired yet")
    }

    pub fn send_peer_relationship_request(
        &mut self,
        _remote_network_peer_id: &str,
        _request: PeerRelationshipRequest,
    ) -> Result<PeerRelationshipRequestId> {
        bail!("iroh peer relationship request streams are not wired yet")
    }

    pub fn send_peer_relationship_response(
        &mut self,
        _request_id: InboundRequestId,
        _response: PeerRelationshipResponse,
    ) -> Result<()> {
        bail!("iroh peer relationship response streams are not wired yet")
    }

    pub fn send_peer_direct_message_request(
        &mut self,
        _remote_network_peer_id: &str,
        _request: PeerDirectMessageRequest,
    ) -> Result<PeerDirectMessageRequestId> {
        bail!("iroh private group message request streams are not wired yet")
    }

    pub fn send_peer_direct_message_response(
        &mut self,
        _request_id: InboundRequestId,
        _response: PeerDirectMessageResponse,
    ) -> Result<()> {
        bail!("iroh private group message response streams are not wired yet")
    }

    pub async fn next_event(&mut self) -> Result<NetworkRuntimeEvent> {
        loop {
            if let Some(event) = self.try_next_event()? {
                return Ok(event);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    pub fn try_next_event(&mut self) -> Result<Option<NetworkRuntimeEvent>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }
        while let Ok(inbound) = self.gossip_rx.try_recv() {
            match inbound {
                IrohGossipInbound::Message {
                    subscription,
                    delivered_from,
                    bytes,
                } => {
                    let Some(message) = self.accept_notification_bytes(
                        &subscription.scope,
                        subscription.kind,
                        &bytes,
                    ) else {
                        continue;
                    };
                    let propagation_source =
                        crate::peer_id_from_ed25519_public_key(delivered_from)?;
                    self.pending_events.push_back(NetworkRuntimeEvent::Gossip {
                        propagation_source,
                        message,
                    });
                }
                IrohGossipInbound::Malformed {
                    remote_network_peer_id,
                    error,
                } => self.record_invalid_control_payload(&remote_network_peer_id, &error),
                IrohGossipInbound::Lagged => {
                    self.counters.malformed_gossip_notifications = self
                        .counters
                        .malformed_gossip_notifications
                        .saturating_add(1);
                }
            }
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    pub fn observability_snapshot(&self) -> IrohRuntimeObservabilitySnapshot {
        let mut subscribed_topics = self
            .subscriptions
            .iter()
            .cloned()
            .collect::<Vec<IrohGossipSubscription>>();
        subscribed_topics.sort_by(|left, right| {
            left.topic_id_hex
                .cmp(&right.topic_id_hex)
                .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
        });
        let mut peer_quality = self
            .peer_quality
            .iter()
            .map(|(network_peer_id, state)| IrohPeerQualitySnapshot {
                network_peer_id: network_peer_id.clone(),
                inflight_requests: state.inflight_requests,
                consecutive_failures: state.consecutive_failures,
                retry_after_ms: state.retry_after_ms,
                last_error: state.last_error.clone(),
            })
            .collect::<Vec<_>>();
        peer_quality.sort_by(|left, right| left.network_peer_id.cmp(&right.network_peer_id));
        let known_remote_contacts = self
            .remote_contacts
            .values()
            .map(|contact| contact.peer_id.clone())
            .collect::<HashSet<_>>()
            .len();
        IrohRuntimeObservabilitySnapshot {
            local_network_peer_id: self.config.local_network_peer_id.clone(),
            local_endpoint_id: self.endpoint_id.clone(),
            subscribed_topics,
            known_remote_contacts,
            peer_quality,
            counters: self.counters.clone(),
        }
    }

    pub fn encode_notification(&self, message: &GossipMessage) -> Result<IrohGossipNotification> {
        Ok(IrohGossipNotification {
            scope: message.scope().clone(),
            kind: message.kind(),
            payload: encode_overlay_payload(message)?,
        })
    }

    pub fn encode_notification_bytes(&self, message: &GossipMessage) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self.encode_notification(message)?)?)
    }

    pub fn decode_notification(
        &mut self,
        notification: IrohGossipNotification,
    ) -> Result<GossipMessage> {
        decode_overlay_gossip(RawGossipMessage {
            scope: notification.scope,
            kind: notification.kind,
            payload: notification.payload,
        })
    }

    pub fn accept_notification_bytes(
        &mut self,
        expected_scope: &SwarmScope,
        expected_kind: GossipKind,
        bytes: &[u8],
    ) -> Option<GossipMessage> {
        let notification = match serde_json::from_slice::<IrohGossipNotification>(bytes) {
            Ok(notification) => notification,
            Err(_) => {
                self.counters.malformed_gossip_notifications = self
                    .counters
                    .malformed_gossip_notifications
                    .saturating_add(1);
                return None;
            }
        };
        if &notification.scope != expected_scope || notification.kind != expected_kind {
            self.counters.scope_kind_mismatch_drops =
                self.counters.scope_kind_mismatch_drops.saturating_add(1);
            return None;
        }
        match self.decode_notification(notification) {
            Ok(message) => Some(message),
            Err(_) => {
                self.counters.malformed_gossip_notifications = self
                    .counters
                    .malformed_gossip_notifications
                    .saturating_add(1);
                None
            }
        }
    }

    pub fn reserve_inbound_request_id(&mut self) -> InboundRequestId {
        let request_id = InboundRequestId(self.next_inbound_request_id);
        self.next_inbound_request_id = self.next_inbound_request_id.saturating_add(1).max(1);
        request_id
    }

    fn reserve_backfill_request_id(&mut self) -> BackfillRequestId {
        let request_id = BackfillRequestId::new(self.next_outbound_request_id);
        self.next_outbound_request_id = self.next_outbound_request_id.saturating_add(1).max(1);
        request_id
    }

    fn begin_stream_request(&mut self, remote_network_peer_id: &str) -> Result<()> {
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        let now_ms = unix_timestamp_millis();
        let state = self
            .peer_quality
            .entry(remote_network_peer_id.to_owned())
            .or_default();
        if state
            .retry_after_ms
            .is_some_and(|retry_after_ms| retry_after_ms > now_ms)
        {
            self.counters.retry_suppressed_connects =
                self.counters.retry_suppressed_connects.saturating_add(1);
            bail!("iroh retry suppressed for {remote_network_peer_id}");
        }
        if state.inflight_requests >= MAX_INFLIGHT_REQUESTS_PER_REMOTE {
            self.counters.request_limit_drops = self.counters.request_limit_drops.saturating_add(1);
            bail!("iroh per-peer request limit reached for {remote_network_peer_id}");
        }
        state.inflight_requests = state.inflight_requests.saturating_add(1);
        Ok(())
    }

    fn finish_stream_request(
        &mut self,
        remote_network_peer_id: &str,
        result: Result<(), &anyhow::Error>,
    ) {
        if let Some(state) = self.peer_quality.get_mut(remote_network_peer_id) {
            state.inflight_requests = state.inflight_requests.saturating_sub(1);
        }
        match result {
            Ok(_) => {
                self.counters.stream_request_successes =
                    self.counters.stream_request_successes.saturating_add(1);
                if let Some(state) = self.peer_quality.get_mut(remote_network_peer_id) {
                    state.consecutive_failures = 0;
                    state.retry_after_ms = None;
                    state.last_error = None;
                }
            }
            Err(err) => {
                self.counters.stream_request_failures =
                    self.counters.stream_request_failures.saturating_add(1);
                self.record_peer_failure(remote_network_peer_id, &err.to_string());
            }
        }
    }

    fn record_peer_failure(&mut self, remote_network_peer_id: &str, error: &str) {
        let state = self
            .peer_quality
            .entry(remote_network_peer_id.to_owned())
            .or_default();
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.last_error = Some(error.to_owned());
        let shift = state.consecutive_failures.saturating_sub(1).min(6);
        let backoff_ms = CONNECT_BACKOFF_BASE_MS
            .saturating_mul(1u64 << shift)
            .min(CONNECT_BACKOFF_MAX_MS);
        state.retry_after_ms = Some(unix_timestamp_millis().saturating_add(backoff_ms));
    }

    fn subscription_for(
        &self,
        scope: &SwarmScope,
        kind: GossipKind,
    ) -> Result<IrohGossipSubscription> {
        let scope_hint = scope.label()?;
        let topic_id = derive_gossip_topic_id(&self.config.network_id, &scope_hint, kind.as_str());
        Ok(IrohGossipSubscription {
            scope: scope.clone(),
            kind,
            topic_id_hex: hex::encode(topic_id.as_bytes()),
        })
    }

    fn ensure_gossip_topic(&mut self, subscription: IrohGossipSubscription) -> Result<()> {
        self.subscriptions.insert(subscription.clone());
        if self.gossip_topics.contains_key(&subscription) {
            return Ok(());
        }
        let topic_id_bytes =
            hex::decode(&subscription.topic_id_hex).map_err(|err| anyhow!("{err}"))?;
        let topic_id_bytes: [u8; 32] = topic_id_bytes
            .try_into()
            .map_err(|_| anyhow!("iroh gossip topic id must be 32 bytes"))?;
        let topic_id = iroh_gossip::TopicId::from_bytes(topic_id_bytes);
        let bootstrap = self
            .remote_contacts
            .values()
            .filter_map(contact_endpoint_id)
            .collect::<Vec<_>>();
        let gossip = local_gossip_for_network_peer_id(
            &self.config.state_dir,
            &self.config.local_network_peer_id,
        )?;
        let topic = self
            .runtime
            .block_on(gossip.subscribe(topic_id, bootstrap))
            .map_err(|err| anyhow!("subscribe iroh gossip topic: {err}"))?;
        let (sender, mut receiver) = topic.split();
        let inbound_tx = self.gossip_tx.clone();
        let task_subscription = subscription.clone();
        let task = self.runtime.spawn(async move {
            while let Some(event) = receiver.next().await {
                match event {
                    Ok(IrohGossipEvent::Received(message)) => {
                        let delivered_from = *message.delivered_from.as_bytes();
                        if inbound_tx
                            .send(IrohGossipInbound::Message {
                                subscription: task_subscription.clone(),
                                delivered_from,
                                bytes: message.content.to_vec(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(IrohGossipEvent::Lagged) => {
                        if inbound_tx.send(IrohGossipInbound::Lagged).is_err() {
                            break;
                        }
                    }
                    Ok(IrohGossipEvent::NeighborUp(_)) | Ok(IrohGossipEvent::NeighborDown(_)) => {}
                    Err(err) => {
                        let _ = inbound_tx.send(IrohGossipInbound::Malformed {
                            remote_network_peer_id: task_subscription.topic_id_hex.clone(),
                            error: err.to_string(),
                        });
                        break;
                    }
                }
            }
        });
        self.gossip_topics
            .insert(subscription, IrohGossipTopicHandle { sender, task });
        Ok(())
    }
}

fn contact_endpoint_id(contact: &TransportContactMaterial) -> Option<EndpointId> {
    contact
        .metadata
        .endpoint_id
        .as_deref()
        .and_then(|endpoint_id| EndpointId::from_str(endpoint_id).ok())
}

fn compat_peer_id_for_contact(contact: &TransportContactMaterial) -> Option<crate::PeerId> {
    contact_endpoint_id(contact).and_then(|endpoint_id| {
        crate::peer_id_from_ed25519_public_key(*endpoint_id.as_bytes()).ok()
    })
}

fn compat_peer_id_for_network_peer_id(network_peer_id: &str) -> Result<crate::PeerId> {
    if let Ok(peer_id) = network_peer_id.parse::<crate::PeerId>() {
        return Ok(peer_id);
    }
    let endpoint_id = EndpointId::from_str(network_peer_id)
        .map_err(|err| anyhow!("parse iroh endpoint id: {err}"))?;
    crate::peer_id_from_ed25519_public_key(*endpoint_id.as_bytes())
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

pub fn encode_backfill_control_request(
    request: BackfillRequest,
) -> Result<IrohControlStreamRequest> {
    Ok(IrohControlStreamRequest {
        kind: IROH_CONTROL_KIND_BACKFILL.to_owned(),
        payload: serde_json::to_vec(&IrohBackfillStreamRequest { request })?,
    })
}

pub fn decode_backfill_control_request(
    request: &IrohControlStreamRequest,
) -> Result<BackfillRequest> {
    if request.kind != IROH_CONTROL_KIND_BACKFILL {
        bail!("unexpected iroh control request kind {}", request.kind);
    }
    Ok(serde_json::from_slice::<IrohBackfillStreamRequest>(&request.payload)?.request)
}

pub fn encode_backfill_control_response(
    response: BackfillResponse,
) -> Result<IrohControlStreamResponse> {
    Ok(IrohControlStreamResponse {
        ok: true,
        error: None,
        payload: serde_json::to_vec(&IrohBackfillStreamResponse { response })?,
    })
}

pub fn decode_backfill_control_response(
    response: IrohControlStreamResponse,
) -> Result<BackfillResponse> {
    if !response.ok {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "remote iroh backfill stream failed".to_owned())
        );
    }
    if response.payload.len() > MAX_BACKFILL_STREAM_RESPONSE_BYTES {
        bail!("iroh backfill stream response exceeds max payload bytes");
    }
    Ok(serde_json::from_slice::<IrohBackfillStreamResponse>(&response.payload)?.response)
}

pub fn validate_backfill_stream_response(
    request: &BackfillRequest,
    response: BackfillResponse,
) -> Result<BackfillResponse> {
    if response.scope != request.scope {
        bail!("iroh backfill response scope mismatch");
    }
    if response.feed_key != request.feed_key {
        bail!("iroh backfill response feed_key mismatch");
    }
    if response.events.len() > request.limit {
        bail!("iroh backfill response exceeds requested event limit");
    }
    for envelope in &response.events {
        if envelope.scope != response.scope {
            bail!("iroh backfill response event scope mismatch");
        }
        if let Some(feed_key) = &request.feed_key {
            let wattswarm_protocol::types::EventPayload::TopicMessagePosted(payload) =
                &envelope.event.payload
            else {
                bail!("iroh topic backfill response contains non-topic event");
            };
            if payload.feed_key != *feed_key {
                bail!("iroh topic backfill response feed_key mismatch");
            }
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SummaryAnnouncement;
    use tempfile::tempdir;
    use wattswarm_network_transport_iroh::{
        export_local_contact_material_for_network_peer_id, shutdown_local_iroh_data_plane,
    };

    fn seed_state_dir(dir: &Path, seed: [u8; 32]) {
        std::fs::write(dir.join("node_seed.hex"), hex::encode(seed)).expect("write node seed");
    }

    fn runtime() -> IrohNetworkRuntime {
        let dir = tempdir().expect("tempdir");
        seed_state_dir(dir.path(), [19u8; 32]);
        let endpoint_id = local_endpoint_id_from_state_dir(dir.path())
            .expect("endpoint id")
            .to_string();
        IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir.keep(),
            local_network_peer_id: endpoint_id,
        })
        .expect("iroh runtime")
    }

    #[test]
    fn subscribe_scope_kinds_tracks_deterministic_iroh_topics() {
        let mut runtime = runtime();
        runtime
            .subscribe_scope_kinds(
                &SwarmScope::Group("alpha".to_owned()),
                &[GossipKind::Messages],
            )
            .expect("subscribe");
        let snapshot = runtime.observability_snapshot();
        assert_eq!(snapshot.subscribed_topics.len(), 1);
        assert_eq!(snapshot.subscribed_topics[0].kind, GossipKind::Messages);
        assert_eq!(snapshot.subscribed_topics[0].topic_id_hex.len(), 64);

        runtime
            .unsubscribe_scope_kinds(
                &SwarmScope::Group("alpha".to_owned()),
                &[GossipKind::Messages],
            )
            .expect("unsubscribe");
        assert!(
            runtime
                .observability_snapshot()
                .subscribed_topics
                .is_empty()
        );
    }

    #[test]
    fn notification_roundtrip_reuses_overlay_payload_codec() {
        let mut runtime = runtime();
        let message = GossipMessage::Summary(SummaryAnnouncement {
            summary_id: "sum-1".to_owned(),
            source_node_id: "node-1".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: "knowledge".to_owned(),
            artifact_path: None,
            payload: serde_json::json!({"ok": true}),
        });
        let notification = runtime
            .encode_notification(&message)
            .expect("encode notification");
        let decoded = runtime
            .decode_notification(notification)
            .expect("decode notification");
        assert_eq!(decoded, message);
    }

    #[test]
    fn notification_bytes_drop_wrong_scope_or_kind_before_bridge_delivery() {
        let mut runtime = runtime();
        let message = GossipMessage::Summary(SummaryAnnouncement {
            summary_id: "sum-1".to_owned(),
            source_node_id: "node-1".to_owned(),
            scope: SwarmScope::Group("alpha".to_owned()),
            summary_kind: "knowledge".to_owned(),
            artifact_path: None,
            payload: serde_json::json!({"ok": true}),
        });
        let bytes = runtime
            .encode_notification_bytes(&message)
            .expect("encode notification bytes");

        assert!(
            runtime
                .accept_notification_bytes(
                    &SwarmScope::Group("beta".to_owned()),
                    GossipKind::Summaries,
                    &bytes,
                )
                .is_none()
        );
        assert!(
            runtime
                .accept_notification_bytes(
                    &SwarmScope::Group("alpha".to_owned()),
                    GossipKind::Messages,
                    &bytes,
                )
                .is_none()
        );
        assert_eq!(
            runtime
                .observability_snapshot()
                .counters
                .scope_kind_mismatch_drops,
            2
        );
    }

    #[test]
    fn two_iroh_runtimes_publish_and_receive_gossip_event() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [71u8; 32]);
        seed_state_dir(dir_b.path(), [72u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let contact_a = export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        let mut runtime_a = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_a.path().to_path_buf(),
            local_network_peer_id: peer_a.clone(),
        })
        .expect("runtime a");
        let mut runtime_b = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_b.path().to_path_buf(),
            local_network_peer_id: peer_b.clone(),
        })
        .expect("runtime b");
        runtime_a
            .upsert_remote_contact_material(peer_b.clone(), contact_b)
            .expect("node a learns node b");
        runtime_b
            .upsert_remote_contact_material(peer_a.clone(), contact_a)
            .expect("node b learns node a");
        runtime_a
            .subscribe_scope_kinds(&SwarmScope::Global, &[GossipKind::Summaries])
            .expect("subscribe a");

        let message = GossipMessage::Summary(SummaryAnnouncement {
            summary_id: "sum-iroh".to_owned(),
            source_node_id: peer_b.clone(),
            scope: SwarmScope::Global,
            summary_kind: "knowledge".to_owned(),
            artifact_path: None,
            payload: serde_json::json!({"ok": true}),
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut received = None;
        while std::time::Instant::now() < deadline {
            runtime_b.publish_gossip(&message).expect("publish gossip");
            if let Some(NetworkRuntimeEvent::Gossip {
                message: received_message,
                ..
            }) = runtime_a.try_next_event().expect("try next event")
            {
                received = Some(received_message);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(received, Some(message));

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn malformed_notification_bytes_are_dropped_and_counted() {
        let mut runtime = runtime();
        assert!(
            runtime
                .accept_notification_bytes(&SwarmScope::Global, GossipKind::Events, b"not-json")
                .is_none()
        );
        assert_eq!(
            runtime
                .observability_snapshot()
                .counters
                .malformed_gossip_notifications,
            1
        );
    }

    #[test]
    fn typed_backfill_request_response_roundtrips_over_iroh_control_stream() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [51u8; 32]);
        seed_state_dir(dir_b.path(), [52u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let mut runtime_a = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_a.path().to_path_buf(),
            local_network_peer_id: peer_a.clone(),
        })
        .expect("runtime a");
        let mut runtime_b = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_b.path().to_path_buf(),
            local_network_peer_id: peer_b.clone(),
        })
        .expect("runtime b");
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        runtime_b
            .install_backfill_stream_handler(|request| {
                assert_eq!(request.scope, SwarmScope::Global);
                assert_eq!(request.feed_key, Some("feed-a".to_owned()));
                Ok(BackfillResponse {
                    scope: request.scope,
                    next_from_event_seq: 8,
                    feed_key: request.feed_key,
                    head_event_ids: vec!["evt-head".to_owned()],
                    events: Vec::new(),
                })
            })
            .expect("install backfill handler");
        runtime_a
            .upsert_remote_contact_material(peer_b.clone(), contact_b)
            .expect("register remote contact material");

        let response = runtime_a
            .request_backfill_page(
                &peer_b,
                BackfillRequest {
                    scope: SwarmScope::Global,
                    from_event_seq: 3,
                    limit: 5,
                    feed_key: Some("feed-a".to_owned()),
                    known_event_ids: vec!["evt-known".to_owned()],
                },
            )
            .expect("request backfill page");
        assert_eq!(runtime_a.observability_snapshot().known_remote_contacts, 1);

        let encoded_request = encode_backfill_control_request(BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 3,
            limit: 5,
            feed_key: Some("feed-a".to_owned()),
            known_event_ids: vec!["evt-known".to_owned()],
        })
        .expect("encode backfill request");
        assert_eq!(
            decode_backfill_control_request(&encoded_request)
                .unwrap()
                .limit,
            5
        );

        assert_eq!(response.next_from_event_seq, 8);
        assert_eq!(response.feed_key, Some("feed-a".to_owned()));
        assert_eq!(response.head_event_ids, vec!["evt-head".to_owned()]);

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn iroh_send_backfill_request_queues_response_event_with_neutral_request_id() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [53u8; 32]);
        seed_state_dir(dir_b.path(), [54u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let compat_peer_b =
            crate::peer_id_from_ed25519_public_key(*endpoint_b.as_bytes()).expect("compat peer b");
        let mut runtime_a = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_a.path().to_path_buf(),
            local_network_peer_id: peer_a.clone(),
        })
        .expect("runtime a");
        let mut runtime_b = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_b.path().to_path_buf(),
            local_network_peer_id: peer_b.clone(),
        })
        .expect("runtime b");
        let contact_b = export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
            .expect("contact b");
        runtime_b
            .install_backfill_stream_handler(|request| {
                Ok(BackfillResponse {
                    scope: request.scope,
                    next_from_event_seq: 5,
                    feed_key: request.feed_key,
                    head_event_ids: vec!["evt-head".to_owned()],
                    events: Vec::new(),
                })
            })
            .expect("install backfill handler");
        runtime_a
            .upsert_remote_contact_material(peer_b.clone(), contact_b)
            .expect("register remote contact material");

        let request_id = runtime_a
            .send_backfill_request(
                &compat_peer_b.to_string(),
                BackfillRequest {
                    scope: SwarmScope::Global,
                    from_event_seq: 1,
                    limit: 5,
                    feed_key: None,
                    known_event_ids: Vec::new(),
                },
            )
            .expect("send backfill request");
        let event = runtime_a
            .try_next_event()
            .expect("try next event")
            .expect("queued backfill response");
        let NetworkRuntimeEvent::BackfillResponse {
            peer,
            request_id: event_request_id,
            response,
        } = event
        else {
            panic!("expected backfill response event");
        };
        assert_eq!(peer, compat_peer_b);
        assert_eq!(event_request_id, request_id);
        assert_eq!(response.next_from_event_seq, 5);
        assert_eq!(response.head_event_ids, vec!["evt-head".to_owned()]);

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn remote_contact_material_rejects_wrong_peer_and_drops_stale_updates() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [61u8; 32]);
        seed_state_dir(dir_b.path(), [62u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let mut runtime_a = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_a.path().to_path_buf(),
            local_network_peer_id: peer_a.clone(),
        })
        .expect("runtime a");
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let newer_contact =
            export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 100)
                .expect("newer contact");
        let stale_contact =
            export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 90)
                .expect("stale contact");

        assert!(
            runtime_a
                .upsert_remote_contact_material(peer_b.clone(), newer_contact.clone())
                .expect("upsert newer")
        );
        assert!(
            !runtime_a
                .upsert_remote_contact_material(peer_b.clone(), stale_contact)
                .expect("drop stale")
        );
        assert_eq!(
            runtime_a
                .observability_snapshot()
                .counters
                .stale_contact_material_drops,
            1
        );

        let err = runtime_a
            .upsert_remote_contact_material("wrong-peer", newer_contact)
            .expect_err("wrong peer should fail");
        assert!(err.to_string().contains("does not match"));

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn runtime_requests_contact_material_over_iroh_stream_and_tracks_success() {
        let dir_a = tempdir().expect("node a tempdir");
        let dir_b = tempdir().expect("node b tempdir");
        seed_state_dir(dir_a.path(), [63u8; 32]);
        seed_state_dir(dir_b.path(), [64u8; 32]);
        let endpoint_a = local_endpoint_id_from_state_dir(dir_a.path()).expect("endpoint a");
        let endpoint_b = local_endpoint_id_from_state_dir(dir_b.path()).expect("endpoint b");
        let peer_a = endpoint_a.to_string();
        let peer_b = endpoint_b.to_string();
        let mut runtime_a = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_a.path().to_path_buf(),
            local_network_peer_id: peer_a.clone(),
        })
        .expect("runtime a");
        let mut runtime_b = IrohNetworkRuntime::new(IrohRuntimeConfig {
            network_id: "mainnet".to_owned(),
            state_dir: dir_b.path().to_path_buf(),
            local_network_peer_id: peer_b.clone(),
        })
        .expect("runtime b");
        export_local_contact_material_for_network_peer_id(dir_a.path(), &peer_a, 1)
            .expect("contact a");
        let bootstrap_contact_b =
            export_local_contact_material_for_network_peer_id(dir_b.path(), &peer_b, 1)
                .expect("bootstrap contact b");
        runtime_b
            .install_contact_material_stream_handler()
            .expect("install contact handler");
        runtime_a
            .upsert_remote_contact_material(peer_b.clone(), bootstrap_contact_b)
            .expect("register bootstrap contact");

        let refreshed_contact = runtime_a
            .request_remote_contact_material(&peer_b)
            .expect("request remote contact material");

        assert_eq!(refreshed_contact.peer_id, peer_b);
        let snapshot = runtime_a.observability_snapshot();
        assert_eq!(snapshot.known_remote_contacts, 1);
        assert_eq!(snapshot.counters.stream_request_successes, 1);
        assert_eq!(snapshot.peer_quality[0].consecutive_failures, 0);

        shutdown_local_iroh_data_plane(dir_a.path());
        shutdown_local_iroh_data_plane(dir_b.path());
    }

    #[test]
    fn inbound_request_ids_are_runtime_local() {
        let mut runtime = runtime();
        assert_eq!(runtime.reserve_inbound_request_id(), InboundRequestId(1));
        assert_eq!(runtime.reserve_inbound_request_id(), InboundRequestId(2));
    }

    #[test]
    fn backfill_stream_response_validation_rejects_bad_pages_before_ingest() {
        use crate::EventEnvelope;
        use wattswarm_protocol::types::{
            ArtifactRef, Event, EventKind, EventPayload, TopicMessagePostedPayload,
        };

        fn topic_event(feed_key: &str) -> EventEnvelope {
            EventEnvelope {
                scope: SwarmScope::Group("crew-7".to_owned()),
                event: Event {
                    event_id: format!("evt-{feed_key}"),
                    protocol_version: "wattswarm/1.0.0".to_owned(),
                    event_kind: EventKind::TopicMessagePosted,
                    task_id: None,
                    epoch: 1,
                    author_node_id: "node-a".to_owned(),
                    created_at: 10,
                    payload: EventPayload::TopicMessagePosted(TopicMessagePostedPayload {
                        network_id: "default".to_owned(),
                        feed_key: feed_key.to_owned(),
                        scope_hint: "group:crew-7".to_owned(),
                        content_ref: ArtifactRef {
                            uri: "artifact://topic-message/evt".to_owned(),
                            digest: "sha256:evt".to_owned(),
                            size_bytes: 2,
                            mime: "application/json".to_owned(),
                            created_at: 10,
                            producer: "node-a".to_owned(),
                        },
                        local_content_cache: None,
                        reply_to_message_id: None,
                    }),
                    signature_hex: "sig".to_owned(),
                },
                content_source_node_id: Some("node-a".to_owned()),
            }
        }

        let request = BackfillRequest {
            scope: SwarmScope::Group("crew-7".to_owned()),
            from_event_seq: 0,
            limit: 1,
            feed_key: Some("crew.chat".to_owned()),
            known_event_ids: Vec::new(),
        };
        let valid = BackfillResponse {
            scope: request.scope.clone(),
            next_from_event_seq: 1,
            feed_key: request.feed_key.clone(),
            head_event_ids: vec!["evt-head".to_owned()],
            events: vec![topic_event("crew.chat")],
        };
        assert!(validate_backfill_stream_response(&request, valid).is_ok());

        let wrong_scope = BackfillResponse {
            scope: SwarmScope::Group("other".to_owned()),
            next_from_event_seq: 1,
            feed_key: request.feed_key.clone(),
            head_event_ids: Vec::new(),
            events: Vec::new(),
        };
        assert!(validate_backfill_stream_response(&request, wrong_scope).is_err());

        let wrong_feed = BackfillResponse {
            scope: request.scope.clone(),
            next_from_event_seq: 1,
            feed_key: Some("other.chat".to_owned()),
            head_event_ids: Vec::new(),
            events: Vec::new(),
        };
        assert!(validate_backfill_stream_response(&request, wrong_feed).is_err());

        let too_many_events = BackfillResponse {
            scope: request.scope.clone(),
            next_from_event_seq: 1,
            feed_key: request.feed_key.clone(),
            head_event_ids: Vec::new(),
            events: vec![topic_event("crew.chat"), topic_event("crew.chat")],
        };
        assert!(validate_backfill_stream_response(&request, too_many_events).is_err());

        let wrong_event_feed = BackfillResponse {
            scope: request.scope.clone(),
            next_from_event_seq: 1,
            feed_key: request.feed_key.clone(),
            head_event_ids: Vec::new(),
            events: vec![topic_event("other.chat")],
        };
        assert!(validate_backfill_stream_response(&request, wrong_event_feed).is_err());
    }

    #[test]
    fn oversized_backfill_stream_payload_is_rejected_before_decode() {
        let err = decode_backfill_control_response(IrohControlStreamResponse {
            ok: true,
            error: None,
            payload: vec![b'x'; MAX_BACKFILL_STREAM_RESPONSE_BYTES + 1],
        })
        .expect_err("oversized payload should fail");
        assert!(err.to_string().contains("exceeds max payload bytes"));
    }

    #[test]
    fn failed_stream_request_updates_peer_backoff_and_suppresses_retry() {
        let mut runtime = runtime();
        let err = runtime
            .request_backfill_page(
                "missing-peer",
                BackfillRequest {
                    scope: SwarmScope::Global,
                    from_event_seq: 0,
                    limit: 1,
                    feed_key: None,
                    known_event_ids: Vec::new(),
                },
            )
            .expect_err("missing contact should fail");
        assert!(err.to_string().contains("missing iroh contact material"));

        let retry_err = runtime
            .request_backfill_page(
                "missing-peer",
                BackfillRequest {
                    scope: SwarmScope::Global,
                    from_event_seq: 0,
                    limit: 1,
                    feed_key: None,
                    known_event_ids: Vec::new(),
                },
            )
            .expect_err("retry should be suppressed");
        assert!(retry_err.to_string().contains("retry suppressed"));

        let snapshot = runtime.observability_snapshot();
        assert_eq!(snapshot.counters.stream_request_attempts, 2);
        assert_eq!(snapshot.counters.stream_request_failures, 1);
        assert_eq!(snapshot.counters.retry_suppressed_connects, 1);
        assert_eq!(snapshot.peer_quality.len(), 1);
        assert_eq!(snapshot.peer_quality[0].network_peer_id, "missing-peer");
        assert_eq!(snapshot.peer_quality[0].consecutive_failures, 1);
        assert_eq!(snapshot.peer_quality[0].inflight_requests, 0);
        assert!(snapshot.peer_quality[0].retry_after_ms.is_some());
    }

    #[test]
    fn invalid_control_payload_penalizes_peer_quality() {
        let mut runtime = runtime();
        runtime.record_invalid_control_payload("peer-b", "bad-json");
        let snapshot = runtime.observability_snapshot();
        assert_eq!(snapshot.counters.invalid_control_payloads, 1);
        assert_eq!(snapshot.peer_quality[0].network_peer_id, "peer-b");
        assert_eq!(snapshot.peer_quality[0].consecutive_failures, 1);
        assert_eq!(
            snapshot.peer_quality[0].last_error,
            Some("bad-json".to_owned())
        );
    }

    #[test]
    fn per_peer_inflight_limit_rejects_before_dialing() {
        let mut runtime = runtime();
        runtime.peer_quality.insert(
            "peer-c".to_owned(),
            PeerQualityState {
                inflight_requests: MAX_INFLIGHT_REQUESTS_PER_REMOTE,
                consecutive_failures: 0,
                retry_after_ms: None,
                last_error: None,
            },
        );

        let err = runtime
            .request_backfill_page(
                "peer-c",
                BackfillRequest {
                    scope: SwarmScope::Global,
                    from_event_seq: 0,
                    limit: 1,
                    feed_key: None,
                    known_event_ids: Vec::new(),
                },
            )
            .expect_err("request limit should fail before dial");
        assert!(err.to_string().contains("request limit"));

        let snapshot = runtime.observability_snapshot();
        assert_eq!(snapshot.counters.request_limit_drops, 1);
        assert_eq!(
            snapshot.peer_quality[0].inflight_requests,
            MAX_INFLIGHT_REQUESTS_PER_REMOTE
        );
    }
}
