use super::*;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SubstrateRuntimeEvent {
    PeerDiscovered {
        peer: NetworkNodeId,
        address: NetworkAddress,
        source: PeerDiscoverySourceKind,
    },
    PeerMetadataObserved {
        peer: NetworkNodeId,
        metadata: PeerMetadata,
    },
    NewListenAddr {
        address: NetworkAddress,
    },
    ConnectionEstablished {
        peer: NetworkNodeId,
        remote_addr: NetworkAddress,
    },
    ConnectionClosed {
        peer: NetworkNodeId,
        remaining_established: u32,
    },
    PeerHandshakeRejected {
        peer: NetworkNodeId,
        detail: String,
    },
    Gossip {
        propagation_source: NetworkNodeId,
        message: RawGossipMessage,
    },
    BackfillRequest {
        peer: NetworkNodeId,
        request: RawBackfillRequest,
        channel: BackfillResponseChannel,
    },
    BackfillResponse {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        response: RawBackfillResponse,
    },
    BackfillOutboundFailure {
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
        error: String,
    },
    BackfillInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
    ContactMaterialRequest {
        peer: NetworkNodeId,
        request: RawContactMaterialRequest,
        channel: ContactMaterialResponseChannel,
    },
    ContactMaterialResponse {
        peer: NetworkNodeId,
        request_id: ContactMaterialRequestId,
        response: RawContactMaterialResponse,
    },
    ContactMaterialOutboundFailure {
        peer: NetworkNodeId,
        request_id: ContactMaterialRequestId,
        error: String,
    },
    ContactMaterialInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
    PeerRelationshipRequest {
        peer: NetworkNodeId,
        request: RawPeerRelationshipRequest,
        channel: PeerRelationshipResponseChannel,
    },
    PeerRelationshipResponse {
        peer: NetworkNodeId,
        request_id: PeerRelationshipRequestId,
        response: RawPeerRelationshipResponse,
    },
    PeerRelationshipOutboundFailure {
        peer: NetworkNodeId,
        request_id: PeerRelationshipRequestId,
        error: String,
    },
    PeerRelationshipInboundFailure {
        peer: NetworkNodeId,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IrohGossipSubscription {
    pub scope: SwarmScope,
    pub kind: GossipKind,
    pub topic_id_hex: String,
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

struct IrohGossipTopicHandle {
    sender: GossipSender,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
enum IrohGossipInbound {
    Message {
        subscription: IrohGossipSubscription,
        delivered_from: String,
        bytes: Vec<u8>,
    },
    NeighborUp {
        peer: String,
    },
    NeighborDown {
        peer: String,
    },
    Malformed {
        remote_network_peer_id: String,
        error: String,
    },
    Lagged,
}

pub struct SubstrateRuntime {
    config: SubstrateConfig,
    runtime: Option<Runtime>,
    state_dir: PathBuf,
    local_peer_id: NetworkNodeId,
    local_endpoint_id: String,
    local_listen_addrs: Vec<NetworkAddress>,
    subscriptions: HashSet<IrohGossipSubscription>,
    gossip_topics: HashMap<IrohGossipSubscription, IrohGossipTopicHandle>,
    gossip_tx: Sender<IrohGossipInbound>,
    gossip_rx: Receiver<IrohGossipInbound>,
    control_tx: Sender<SubstrateRuntimeEvent>,
    control_rx: Receiver<SubstrateRuntimeEvent>,
    pending_events: VecDeque<SubstrateRuntimeEvent>,
    remote_contacts: HashMap<String, TransportContactMaterial>,
    established_per_peer: HashMap<NetworkNodeId, u32>,
    suppressed_established_per_peer: HashMap<NetworkNodeId, u32>,
    counters: IrohRuntimeCounters,
    next_request_id: u64,
}

impl SubstrateRuntime {
    pub fn new(node: SubstrateNode) -> Result<Self> {
        let local_contact = export_local_contact_material_for_network_peer_id_with_gossip_config(
            node.state_dir(),
            node.local_peer_id.as_str(),
            unix_timestamp_millis() / 1000,
            node.config.iroh_gossip_runtime_config(),
        )?;
        install_local_contact_material_control_handler_for_network_peer_id(
            node.state_dir(),
            node.local_peer_id.as_str(),
        )?;
        let runtime = Runtime::new()?;
        let (gossip_tx, gossip_rx) = mpsc::channel();
        let (control_tx, control_rx) = mpsc::channel();
        let mut this = Self {
            local_endpoint_id: local_endpoint_id_from_state_dir(node.state_dir())?.to_string(),
            local_listen_addrs: local_contact
                .metadata
                .listen_addrs
                .iter()
                .filter_map(|addr| NetworkAddress::new(addr.clone()).ok())
                .collect(),
            config: node.config,
            runtime: Some(runtime),
            state_dir: node.state_dir,
            local_peer_id: node.local_peer_id,
            subscriptions: HashSet::new(),
            gossip_topics: HashMap::new(),
            gossip_tx,
            gossip_rx,
            control_tx,
            control_rx,
            pending_events: VecDeque::new(),
            remote_contacts: HashMap::new(),
            established_per_peer: HashMap::new(),
            suppressed_established_per_peer: HashMap::new(),
            counters: IrohRuntimeCounters::default(),
            next_request_id: 1,
        };
        this.install_control_handlers()?;
        for (peer, address) in this.config.parse_bootstrap_peers()? {
            let dial_address = NetworkAddress::new(format!("{peer}@{address}"))?;
            this.dial(dial_address)?;
        }
        Ok(this)
    }

    pub fn local_peer_id(&self) -> NetworkNodeId {
        self.local_peer_id.clone()
    }

    pub fn listen_addrs(&self) -> &[NetworkAddress] {
        &self.local_listen_addrs
    }

    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("substrate runtime must be available until drop")
    }

    fn block_on<T: Send>(&self, future: impl Future<Output = T> + Send) -> T {
        let runtime = self.runtime();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| runtime.block_on(future))
            }
            Ok(_) => std::thread::scope(|scope| {
                scope
                    .spawn(move || runtime.block_on(future))
                    .join()
                    .expect("join substrate runtime blocking operation")
            }),
            Err(_) => runtime.block_on(future),
        }
    }

    fn block_on_gossip_bootstrap<T: Send>(
        &self,
        operation: &'static str,
        future: impl Future<Output = T> + Send,
    ) -> Result<T> {
        let timeout = Duration::from_millis(DEFAULT_GOSSIP_BOOTSTRAP_TIMEOUT_MS);
        let runtime = self.runtime();
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    runtime.block_on(async move { tokio::time::timeout(timeout, future).await })
                })
                .join()
                .expect("join substrate runtime gossip bootstrap operation")
        })
        .with_context(|| {
            format!(
                "{operation} timed out after {}ms",
                DEFAULT_GOSSIP_BOOTSTRAP_TIMEOUT_MS
            )
        })
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        let mut subscribed_iroh_gossip_topics = self
            .subscriptions
            .iter()
            .map(|subscription| {
                format!(
                    "{:?}:{}:{}",
                    subscription.scope,
                    subscription.kind.as_str(),
                    subscription.topic_id_hex
                )
            })
            .collect::<Vec<_>>();
        subscribed_iroh_gossip_topics.sort();
        NetworkRuntimeObservabilitySnapshot {
            p2p_foundation: "iroh".to_owned(),
            local_iroh_endpoint_id: Some(self.local_endpoint_id.clone()),
            subscribed_iroh_gossip_topics,
            known_iroh_contacts: self.remote_contacts.len(),
            legacy_transport_active: false,
            nat_status: "iroh-direct".to_owned(),
            nat_public_address: None,
            nat_confidence: 0,
            relay_reservations: local_iroh_home_relay_urls(&self.state_dir),
            peer_health: Vec::new(),
            dropped_malformed_gossip: self.counters.malformed_gossip_notifications,
            invalid_control_payloads: self.counters.invalid_control_payloads,
            dial_failures: self.counters.stream_request_failures,
            response_validation_failures: 0,
            retry_suppressed_dials: self.counters.retry_suppressed_connects,
        }
    }

    pub fn upsert_remote_contact_material(
        &mut self,
        remote_network_peer_id: impl Into<String>,
        mut contact: TransportContactMaterial,
    ) -> Result<bool> {
        let remote_network_peer_id = NetworkNodeId::new(remote_network_peer_id.into())?.to_string();
        let contact_endpoint = contact_endpoint_id(&contact)?;
        let contact_endpoint = contact_endpoint.to_string();
        if contact.peer_id != contact_endpoint {
            bail!(
                "iroh contact material peer_id {} must match endpoint_id {}",
                contact.peer_id,
                contact_endpoint
            );
        }
        if remote_network_peer_id != contact_endpoint {
            bail!(
                "remote network node id {remote_network_peer_id} must match iroh endpoint_id {contact_endpoint}"
            );
        }
        contact.metadata.endpoint_id = Some(contact_endpoint.clone());
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
            &self.state_dir,
            self.local_peer_id.as_str(),
            &contact,
        )?;
        self.remote_contacts.insert(remote_network_peer_id, contact);
        self.schedule_gossip_bootstrap_join()?;
        Ok(true)
    }

    pub fn allows_outbound_backfill_to(&self, peer: &NetworkNodeId) -> bool {
        self.remote_contacts.contains_key(peer.as_str())
    }

    pub fn remote_contact_peer_ids(&self) -> Vec<NetworkNodeId> {
        self.remote_contacts
            .keys()
            .filter_map(|peer| NetworkNodeId::new(peer.clone()).ok())
            .collect()
    }

    pub fn rejoin_gossip_with_remote_contact(&mut self, peer: &NetworkNodeId) -> Result<bool> {
        if !self.remote_contacts.contains_key(peer.as_str()) {
            return Ok(false);
        }
        self.schedule_gossip_bootstrap_join()?;
        Ok(true)
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

    pub fn publish(&mut self, scope: &SwarmScope, kind: GossipKind, payload: &[u8]) -> Result<()> {
        let message = RawGossipMessage {
            scope: scope.clone(),
            kind,
            payload: payload.to_vec(),
        };
        let bytes = message.encode_json()?;
        if bytes.len() > self.config.gossip_mesh_max_transmit_size {
            bail!(
                "gossip message size {} exceeds configured max {}",
                bytes.len(),
                self.config.gossip_mesh_max_transmit_size
            );
        }
        let subscription = self.subscription_for(scope, kind)?;
        self.ensure_gossip_topic(subscription.clone())?;
        let topic = self
            .gossip_topics
            .get(&subscription)
            .ok_or_else(|| anyhow!("missing iroh gossip topic {}", subscription.topic_id_hex))?;
        self.block_on(topic.sender.broadcast(bytes.into()))
            .map_err(|err| anyhow!("publish iroh gossip notification: {err}"))?;
        Ok(())
    }

    pub fn dial(&mut self, addr: NetworkAddress) -> Result<()> {
        let Some((raw_peer, raw_direct_addr)) = addr.as_str().split_once('@') else {
            return Ok(());
        };
        let peer = NetworkNodeId::new(raw_peer.to_owned())?;
        EndpointId::from_str(raw_peer).context("parse iroh endpoint id from dial target")?;
        let direct_addr = NetworkAddress::new(raw_direct_addr.to_owned())?;
        let contact = TransportContactMaterial {
            transport: TransportRoute::IrohDirect.as_str().to_owned(),
            peer_id: peer.to_string(),
            metadata: TransportMetadata {
                route: TransportRoute::IrohDirect,
                generated_at: unix_timestamp_millis(),
                endpoint_id: Some(peer.to_string()),
                alpn: Some(DEFAULT_IROH_CONTROL_ALPN.to_owned()),
                listen_addrs: vec![direct_addr.to_string()],
                capabilities: PeerTransportCapabilities::iroh_direct_default(),
            },
            extra: serde_json::json!({
                "endpoint_id": peer.to_string(),
                "alpn": DEFAULT_IROH_CONTROL_ALPN,
                "direct_addrs": [direct_addr.to_string()],
                "relay_urls": [],
            }),
        };
        self.upsert_remote_contact_material(peer.to_string(), contact)?;
        self.pending_events
            .push_back(SubstrateRuntimeEvent::PeerDiscovered {
                peer: peer.clone(),
                address: direct_addr.clone(),
                source: PeerDiscoverySourceKind::Bootstrap,
            });
        Ok(())
    }

    fn schedule_gossip_bootstrap_join(&self) -> Result<()> {
        if self.gossip_topics.is_empty() {
            return Ok(());
        }
        let bootstrap = self
            .remote_contacts
            .values()
            .map(contact_endpoint_id)
            .collect::<Result<Vec<_>>>()?;
        if bootstrap.is_empty() {
            return Ok(());
        }
        let timeout = Duration::from_millis(DEFAULT_GOSSIP_BOOTSTRAP_TIMEOUT_MS);
        for topic in self.gossip_topics.values() {
            let sender = topic.sender.clone();
            let peers = bootstrap.clone();
            self.runtime().spawn(async move {
                let _ = tokio::time::timeout(timeout, sender.join_peers(peers)).await;
            });
        }
        Ok(())
    }

    pub fn send_backfill_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawBackfillRequest,
    ) -> Result<BackfillRequestId> {
        let request_id = BackfillRequestId::new(self.reserve_request_number());
        let (kind, payload) = match encode_raw_control_request(RawControlRequest::Backfill(request))
            .and_then(|(kind, payload)| {
                RawControlRequest::Backfill(serde_json::from_slice(&payload)?).validate(
                    self.config.max_backfill_events,
                    self.config.max_backfill_events_hard_limit,
                )?;
                Ok((kind, payload))
            }) {
            Ok(encoded) => encoded,
            Err(err) => {
                self.pending_events
                    .push_back(SubstrateRuntimeEvent::BackfillOutboundFailure {
                        peer: peer.clone(),
                        request_id,
                        error: err.to_string(),
                    });
                return Ok(request_id);
            }
        };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events
                .push_back(SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                });
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::Backfill(response)) => {
                    SubstrateRuntimeEvent::BackfillResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected backfill control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::BackfillOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    pub fn send_backfill_response(
        &mut self,
        channel: BackfillResponseChannel,
        response: RawBackfillResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("backfill response channel closed"))
    }

    pub fn send_contact_material_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawContactMaterialRequest,
    ) -> Result<ContactMaterialRequestId> {
        let request_id = ContactMaterialRequestId::new(self.reserve_request_number());
        let (kind, payload) =
            match encode_raw_control_request(RawControlRequest::ContactMaterial(request)) {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.pending_events.push_back(
                        SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                            peer: peer.clone(),
                            request_id,
                            error: err.to_string(),
                        },
                    );
                    return Ok(request_id);
                }
            };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events
                .push_back(SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                });
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::ContactMaterial(response)) => {
                    SubstrateRuntimeEvent::ContactMaterialResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected contact material control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::ContactMaterialOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    pub fn send_contact_material_response(
        &mut self,
        channel: ContactMaterialResponseChannel,
        response: RawContactMaterialResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("contact material response channel closed"))
    }

    pub fn send_peer_relationship_request(
        &mut self,
        peer: &NetworkNodeId,
        request: RawPeerRelationshipRequest,
    ) -> Result<PeerRelationshipRequestId> {
        let request_id = PeerRelationshipRequestId::new(self.reserve_request_number());
        let (kind, payload) =
            match encode_raw_control_request(RawControlRequest::PeerRelationship(request)) {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.pending_events.push_back(
                        SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                            peer: peer.clone(),
                            request_id,
                            error: err.to_string(),
                        },
                    );
                    return Ok(request_id);
                }
            };
        let Some(remote_contact) = self.remote_contacts.get(peer.as_str()).cloned() else {
            self.pending_events
                .push_back(SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                    peer: peer.clone(),
                    request_id,
                    error: format!("missing iroh contact material for {peer}"),
                });
            return Ok(request_id);
        };
        self.counters.stream_request_attempts =
            self.counters.stream_request_attempts.saturating_add(1);
        self.spawn_control_request(
            peer,
            kind,
            payload,
            remote_contact,
            move |peer, response| match response {
                Ok(RawControlResponse::PeerRelationship(response)) => {
                    SubstrateRuntimeEvent::PeerRelationshipResponse {
                        peer,
                        request_id,
                        response,
                    }
                }
                Ok(_) => SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                    peer,
                    request_id,
                    error: "unexpected peer relationship control response kind".to_owned(),
                },
                Err(err) => SubstrateRuntimeEvent::PeerRelationshipOutboundFailure {
                    peer,
                    request_id,
                    error: err.to_string(),
                },
            },
        );
        Ok(request_id)
    }

    pub fn send_peer_relationship_response(
        &mut self,
        channel: PeerRelationshipResponseChannel,
        response: RawPeerRelationshipResponse,
    ) -> Result<()> {
        channel
            .send(response)
            .map_err(|_| anyhow!("peer relationship response channel closed"))
    }

    fn spawn_control_request(
        &self,
        peer: &NetworkNodeId,
        kind: String,
        payload: Vec<u8>,
        remote_contact: TransportContactMaterial,
        build_event: impl FnOnce(
            NetworkNodeId,
            std::result::Result<RawControlResponse, anyhow::Error>,
        ) -> SubstrateRuntimeEvent
        + Send
        + 'static,
    ) {
        let state_dir = self.state_dir.clone();
        let local_peer_id = self.local_peer_id.clone();
        let peer = peer.clone();
        let control_tx = self.control_tx.clone();
        let timeout = Duration::from_millis(self.config.control_request_timeout_ms);
        std::thread::spawn(move || {
            let response = send_control_stream_request_for_network_peer_id_with_timeout(
                &state_dir,
                local_peer_id.as_str(),
                &remote_contact,
                &IrohControlStreamRequest {
                    kind: kind.clone(),
                    payload,
                },
                timeout,
            )
            .and_then(|response| decode_raw_control_response(&kind, response));
            let _ = control_tx.send(build_event(peer, response));
        });
    }

    pub async fn next_event(&mut self) -> Result<SubstrateRuntimeEvent> {
        loop {
            if let Some(event) = self.try_next_event()? {
                return Ok(event);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub fn try_next_event(&mut self) -> Result<Option<SubstrateRuntimeEvent>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }
        while let Ok(event) = self.control_rx.try_recv() {
            self.pending_events.push_back(event);
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
        }
        while let Ok(inbound) = self.gossip_rx.try_recv() {
            match inbound {
                IrohGossipInbound::Message {
                    subscription,
                    delivered_from,
                    bytes,
                } => {
                    if bytes.len() > self.config.gossip_mesh_max_transmit_size {
                        self.counters.malformed_gossip_notifications = self
                            .counters
                            .malformed_gossip_notifications
                            .saturating_add(1);
                        continue;
                    }
                    let message = RawGossipMessage::decode_json(&bytes)?;
                    if message.scope != subscription.scope || message.kind != subscription.kind {
                        self.counters.scope_kind_mismatch_drops =
                            self.counters.scope_kind_mismatch_drops.saturating_add(1);
                        continue;
                    }
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::Gossip {
                            propagation_source: NetworkNodeId::new(delivered_from)?,
                            message,
                        });
                }
                IrohGossipInbound::NeighborUp { peer } => {
                    let peer = NetworkNodeId::new(peer)?;
                    let established = self.established_per_peer.get(&peer).copied().unwrap_or(0);
                    if established >= self.config.max_established_per_peer {
                        self.suppressed_established_per_peer
                            .entry(peer)
                            .and_modify(|count| *count = count.saturating_add(1))
                            .or_insert(1);
                        self.counters.request_limit_drops =
                            self.counters.request_limit_drops.saturating_add(1);
                        continue;
                    }
                    let next_established = established.saturating_add(1);
                    self.established_per_peer
                        .insert(peer.clone(), next_established);
                    let remote_addr = self
                        .remote_contacts
                        .get(peer.as_str())
                        .and_then(|contact| contact.metadata.listen_addrs.first())
                        .and_then(|addr| NetworkAddress::new(addr.clone()).ok())
                        .unwrap_or_else(|| {
                            NetworkAddress::new(peer.to_string())
                                .expect("iroh node id is a network address")
                        });
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::ConnectionEstablished {
                            peer,
                            remote_addr,
                        });
                }
                IrohGossipInbound::NeighborDown { peer } => {
                    let peer = NetworkNodeId::new(peer)?;
                    if let Some(suppressed) = self.suppressed_established_per_peer.get_mut(&peer) {
                        *suppressed = suppressed.saturating_sub(1);
                        if *suppressed == 0 {
                            self.suppressed_established_per_peer.remove(&peer);
                        }
                        continue;
                    }
                    let remaining_established = match self.established_per_peer.get_mut(&peer) {
                        Some(established) => {
                            *established = established.saturating_sub(1);
                            let remaining = *established;
                            if remaining == 0 {
                                self.established_per_peer.remove(&peer);
                            }
                            remaining
                        }
                        None => 0,
                    };
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::ConnectionClosed {
                            peer,
                            remaining_established,
                        });
                }
                IrohGossipInbound::Malformed {
                    remote_network_peer_id,
                    error,
                } => {
                    self.counters.invalid_control_payloads =
                        self.counters.invalid_control_payloads.saturating_add(1);
                    self.pending_events
                        .push_back(SubstrateRuntimeEvent::PeerHandshakeRejected {
                            peer: NetworkNodeId::new(remote_network_peer_id)?,
                            detail: error,
                        });
                }
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

    fn install_control_handlers(&mut self) -> Result<()> {
        self.install_typed_control_handler::<RawBackfillRequest, RawBackfillResponse>(
            IROH_CONTROL_KIND_BACKFILL,
            |peer, request, channel| SubstrateRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            },
        )?;
        self.install_typed_control_handler::<RawContactMaterialRequest, RawContactMaterialResponse>(
            IROH_CONTROL_KIND_CONTACT_MATERIAL,
            |peer, request, channel| SubstrateRuntimeEvent::ContactMaterialRequest {
                peer,
                request,
                channel,
            },
        )?;
        self.install_typed_control_handler::<RawPeerRelationshipRequest, RawPeerRelationshipResponse>(
            IROH_CONTROL_KIND_PEER_RELATIONSHIP,
            |peer, request, channel| SubstrateRuntimeEvent::PeerRelationshipRequest {
                peer,
                request,
                channel,
            },
        )?;
        Ok(())
    }

    fn install_typed_control_handler<Req, Resp>(
        &self,
        kind: &'static str,
        build_event: impl Fn(NetworkNodeId, Req, Sender<Resp>) -> SubstrateRuntimeEvent
        + Send
        + Sync
        + 'static,
    ) -> Result<()>
    where
        Req: for<'de> Deserialize<'de> + Send + 'static,
        Resp: Serialize + Send + 'static,
        Req: InboundControlPeer,
    {
        let pending_tx = self.control_tx.clone();
        let local_peer_id = self.local_peer_id.clone();
        let timeout = Duration::from_millis(self.config.control_request_timeout_ms);
        set_local_control_stream_handler_for_network_peer_id(
            &self.state_dir,
            self.local_peer_id.as_str(),
            kind,
            Some(
                move |remote_peer_id: String, request: IrohControlStreamRequest| {
                    if request.kind != kind {
                        return IrohControlStreamResponse {
                            ok: false,
                            error: Some(format!(
                                "unexpected iroh control request kind {}",
                                request.kind
                            )),
                            payload: Vec::new(),
                        };
                    }
                    let decoded = match serde_json::from_slice::<Req>(&request.payload) {
                        Ok(decoded) => decoded,
                        Err(err) => {
                            return IrohControlStreamResponse {
                                ok: false,
                                error: Some(format!("decode iroh control request: {err}")),
                                payload: Vec::new(),
                            };
                        }
                    };
                    let remote_peer = match NetworkNodeId::new(remote_peer_id) {
                        Ok(peer) => peer,
                        Err(err) => {
                            return IrohControlStreamResponse {
                                ok: false,
                                error: Some(format!("resolve iroh remote peer: {err}")),
                                payload: Vec::new(),
                            };
                        }
                    };
                    let peer = match decoded.inbound_peer(&remote_peer, &local_peer_id) {
                        Ok(peer) => peer,
                        Err(err) => {
                            return IrohControlStreamResponse {
                                ok: false,
                                error: Some(format!("resolve iroh control peer: {err}")),
                                payload: Vec::new(),
                            };
                        }
                    };
                    let (response_tx, response_rx) = mpsc::channel::<Resp>();
                    let event = build_event(peer, decoded, response_tx);
                    if pending_tx.send(event).is_err() {
                        return IrohControlStreamResponse {
                            ok: false,
                            error: Some("control dispatch channel unavailable".to_owned()),
                            payload: Vec::new(),
                        };
                    }
                    match response_rx.recv_timeout(timeout) {
                        Ok(response) => match serde_json::to_vec(&response) {
                            Ok(payload) => IrohControlStreamResponse {
                                ok: true,
                                error: None,
                                payload,
                            },
                            Err(err) => IrohControlStreamResponse {
                                ok: false,
                                error: Some(format!("encode iroh control response: {err}")),
                                payload: Vec::new(),
                            },
                        },
                        Err(err) => IrohControlStreamResponse {
                            ok: false,
                            error: Some(format!("wait for iroh control response: {err}")),
                            payload: Vec::new(),
                        },
                    }
                },
            ),
        )
    }

    fn subscription_for(
        &self,
        scope: &SwarmScope,
        kind: GossipKind,
    ) -> Result<IrohGossipSubscription> {
        let scope_hint = scope.label()?;
        let topic_id = derive_gossip_topic_id(
            &self.config.namespace.network_id,
            &scope_hint,
            kind.as_str(),
        );
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
        let gossip =
            local_gossip_for_network_peer_id(&self.state_dir, self.local_peer_id.as_str())?;
        let topic = self
            .block_on_gossip_bootstrap(
                "subscribe iroh gossip topic",
                gossip.subscribe(topic_id, Vec::new()),
            )?
            .map_err(|err| anyhow!("subscribe iroh gossip topic: {err}"))?;
        let (sender, mut receiver) = topic.split();
        let inbound_tx = self.gossip_tx.clone();
        let task_subscription = subscription.clone();
        let task = self.runtime().spawn(async move {
            while let Some(event) = receiver.next().await {
                match event {
                    Ok(IrohGossipEvent::Received(message)) => {
                        let delivered_from = message.delivered_from.to_string();
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
                    Ok(IrohGossipEvent::NeighborUp(peer)) => {
                        if inbound_tx
                            .send(IrohGossipInbound::NeighborUp {
                                peer: peer.to_string(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(IrohGossipEvent::NeighborDown(peer)) => {
                        if inbound_tx
                            .send(IrohGossipInbound::NeighborDown {
                                peer: peer.to_string(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
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
        self.schedule_gossip_bootstrap_join()?;
        Ok(())
    }

    fn reserve_request_number(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1).max(1);
        request_id
    }
}

impl Drop for SubstrateRuntime {
    fn drop(&mut self) {
        for (_, topic) in self.gossip_topics.drain() {
            topic.task.abort();
        }
        shutdown_local_iroh_data_plane(&self.state_dir);
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| drop(runtime));
            }
            Ok(_) => {
                std::thread::scope(|scope| {
                    scope
                        .spawn(move || drop(runtime))
                        .join()
                        .expect("join substrate runtime drop thread");
                });
            }
            Err(_) => drop(runtime),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_for_test(config: SubstrateConfig) -> SubstrateRuntime {
        let (gossip_tx, gossip_rx) = mpsc::channel();
        let (control_tx, control_rx) = mpsc::channel();
        let state_dir = std::env::temp_dir().join(format!(
            "wattswarm-substrate-runtime-test-{}",
            unix_timestamp_millis()
        ));
        SubstrateRuntime {
            config,
            runtime: Some(Runtime::new().expect("tokio runtime")),
            state_dir,
            local_peer_id: NetworkNodeId::random(),
            local_endpoint_id: "local".to_owned(),
            local_listen_addrs: Vec::new(),
            subscriptions: HashSet::new(),
            gossip_topics: HashMap::new(),
            gossip_tx,
            gossip_rx,
            control_tx,
            control_rx,
            pending_events: VecDeque::new(),
            remote_contacts: HashMap::new(),
            established_per_peer: HashMap::new(),
            suppressed_established_per_peer: HashMap::new(),
            counters: IrohRuntimeCounters::default(),
            next_request_id: 1,
        }
    }

    #[test]
    fn neighbor_events_respect_max_established_per_peer() {
        let peer = NetworkNodeId::random();
        let mut runtime = runtime_for_test(SubstrateConfig {
            max_established_per_peer: 1,
            ..SubstrateConfig::default()
        });

        runtime
            .gossip_tx
            .send(IrohGossipInbound::NeighborUp {
                peer: peer.to_string(),
            })
            .unwrap();
        runtime
            .gossip_tx
            .send(IrohGossipInbound::NeighborUp {
                peer: peer.to_string(),
            })
            .unwrap();

        match runtime.try_next_event().unwrap() {
            Some(SubstrateRuntimeEvent::ConnectionEstablished { peer: got, .. }) => {
                assert_eq!(got, peer)
            }
            other => panic!("expected first connection, got {other:?}"),
        }
        assert!(runtime.try_next_event().unwrap().is_none());
        assert_eq!(runtime.counters.request_limit_drops, 1);

        runtime
            .gossip_tx
            .send(IrohGossipInbound::NeighborDown {
                peer: peer.to_string(),
            })
            .unwrap();
        assert!(runtime.try_next_event().unwrap().is_none());

        runtime
            .gossip_tx
            .send(IrohGossipInbound::NeighborDown {
                peer: peer.to_string(),
            })
            .unwrap();
        match runtime.try_next_event().unwrap() {
            Some(SubstrateRuntimeEvent::ConnectionClosed {
                peer: got,
                remaining_established,
            }) => {
                assert_eq!(got, peer);
                assert_eq!(remaining_established, 0);
            }
            other => panic!("expected final disconnection, got {other:?}"),
        }
    }

    #[test]
    fn publish_rejects_oversized_gossip_payload() {
        let mut runtime = runtime_for_test(SubstrateConfig {
            gossip_mesh_max_transmit_size: MIN_MAX_MESSAGE_SIZE,
            ..SubstrateConfig::default()
        });

        let err = runtime
            .publish(&SwarmScope::Global, GossipKind::Events, &[b'x'; 1024])
            .expect_err("oversized publish must fail");
        assert!(err.to_string().contains("exceeds configured max"));
    }
}
