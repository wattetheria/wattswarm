use super::*;

impl NetworkBridgeService {
    pub fn observability_snapshot(
        &self,
        node: &Node,
    ) -> Result<NetworkBridgeObservabilitySnapshot> {
        let runtime = self.runtime.observability_snapshot();
        let topology = node
            .store
            .load_network_topology_for_org(node.store.org_id())?;
        let parent_uplink = parent_uplink_store(node)?;
        let parent_imported_task_outcome_rows = parent_uplink
            .as_ref()
            .map(|store| store.count_imported_task_outcomes())
            .transpose()?;
        let parent_checkpoint_rows = parent_uplink
            .as_ref()
            .map(|store| store.count_checkpoint_announcements())
            .transpose()?;

        let traffic_scores = runtime
            .peer_health
            .iter()
            .map(|entry| (entry.peer.clone(), entry))
            .collect::<HashMap<_, _>>();
        let mut peer_ids = self
            .peer_sync_state
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        for entry in &runtime.peer_health {
            if !peer_ids.iter().any(|peer| peer == &entry.peer) {
                peer_ids.push(entry.peer.clone());
            }
        }
        peer_ids.sort();
        peer_ids.dedup();

        let now = Instant::now();
        let mut peer_health = Vec::with_capacity(peer_ids.len());
        for peer in peer_ids {
            let traffic = traffic_scores.get(&peer);
            let sync = self
                .peer_sync_state
                .iter()
                .find(|(peer_id, _)| peer_id.to_string() == peer)
                .map(|(_, state)| state);
            let mut known_scopes = sync
                .map(|state| {
                    let mut scopes = state
                        .known_scopes
                        .iter()
                        .map(scope_hint_label)
                        .collect::<Vec<_>>();
                    scopes.sort();
                    scopes
                })
                .unwrap_or_default();
            known_scopes.sort();
            peer_health.push(NetworkBridgePeerHealth {
                network_peer_id: peer.clone(),
                connected: self
                    .connected_peers
                    .iter()
                    .any(|peer_id| peer_id.to_string() == peer),
                score: traffic.map_or(0, |entry| entry.score),
                blacklisted: traffic.is_some_and(|entry| entry.blacklisted),
                reputation_tier: traffic
                    .map(|entry| entry.reputation_tier.clone())
                    .unwrap_or_else(|| "healthy".to_owned()),
                quarantined: traffic.is_some_and(|entry| entry.quarantined),
                quarantine_remaining_ms: traffic.map_or(0, |entry| entry.quarantine_remaining_ms),
                ban_remaining_ms: traffic.map_or(0, |entry| entry.ban_remaining_ms),
                throttle_factor_percent: traffic.map_or(100, |entry| entry.throttle_factor_percent),
                known_scopes,
                inflight_backfills: sync.map_or(0, PeerSyncState::inflight_backfills),
                next_retry_in_ms: sync.map_or(0, |state| {
                    state
                        .next_retry_at
                        .saturating_duration_since(now)
                        .as_millis() as u64
                }),
            });
        }

        let mut scope_traffic = self
            .scope_traffic
            .iter()
            .map(|(scope, stats)| NetworkBridgeScopeTraffic {
                scope: scope_hint_label(scope),
                stats: stats.clone(),
            })
            .collect::<Vec<_>>();
        scope_traffic.sort_by(|left, right| left.scope.cmp(&right.scope));

        Ok(NetworkBridgeObservabilitySnapshot {
            local_network_peer_id: self.local_peer_id().to_string(),
            local_endpoint_addrs: self
                .listen_addrs()
                .iter()
                .map(ToString::to_string)
                .collect(),
            p2p_foundation: runtime.p2p_foundation,
            local_iroh_endpoint_id: runtime.local_iroh_endpoint_id,
            subscribed_iroh_gossip_topics: runtime.subscribed_iroh_gossip_topics,
            known_iroh_contacts: runtime.known_iroh_contacts,
            legacy_transport_active: runtime.legacy_transport_active,
            subscribed_scopes: self
                .subscribed_scopes
                .iter()
                .map(scope_hint_label)
                .collect(),
            connected_peer_count: self.connected_peers.len(),
            nat_status: runtime.nat_status,
            nat_public_address: runtime.nat_public_address,
            nat_confidence: runtime.nat_confidence,
            relay_reservations: runtime.relay_reservations,
            peer_health,
            scope_traffic,
            dropped_malformed_gossip: runtime.dropped_malformed_gossip,
            invalid_control_payloads: runtime.invalid_control_payloads,
            dial_failures: runtime.dial_failures,
            response_validation_failures: runtime.response_validation_failures,
            retry_suppressed_dials: runtime.retry_suppressed_dials,
            summary_health: NetworkBridgeSummaryHealth {
                imported_decision_memory_rows: node.store.count_imported_decision_memory()?,
                imported_reputation_rows: node.store.count_imported_reputation_snapshots()?,
                imported_task_outcome_rows: node.store.count_imported_task_outcomes()?,
                checkpoint_rows: node.store.count_checkpoint_announcements()?,
            },
            subnet_sync_health: NetworkBridgeSubnetSyncHealth {
                network_id: topology.network.network_id,
                network_kind: topology.network.network_kind.as_str().to_owned(),
                parent_network_id: topology.network.parent_network_id,
                parent_uplink_available: parent_uplink.is_some(),
                parent_imported_task_outcome_rows,
                parent_checkpoint_rows,
            },
            execution_set_health: NetworkBridgeExecutionSetHealth {
                execution_set_count: node.store.count_distinct_execution_sets()?,
                execution_set_member_count: node.store.count_execution_set_members()?,
            },
        })
    }

    pub fn request_global_backfill(
        &mut self,
        peer: &NetworkNodeId,
        from_event_seq: u64,
        limit: usize,
    ) -> Result<BackfillRequestId> {
        self.runtime.send_backfill_request(
            peer,
            BackfillRequest {
                scope: SwarmScope::Global,
                from_event_seq,
                limit,
                feed_key: None,
                known_event_ids: Vec::new(),
            },
        )
    }

    pub fn tick(&mut self, node: &mut Node) -> Result<NetworkBridgeTick> {
        let event = self.tokio_runtime.block_on(self.runtime.next_event());
        self.handle_runtime_event(node, event)
    }

    pub(crate) fn request_backfill_for_peer_now(
        &mut self,
        peer: &NetworkNodeId,
        node: &Node,
    ) -> Result<bool> {
        let scopes = self.scopes_to_request_for_peer(peer);
        self.request_backfill_scopes_for_peer_now(peer, node, &scopes)
    }

    pub(crate) fn request_backfill_scopes_for_peer(
        &mut self,
        peer: &NetworkNodeId,
        node: &Node,
        scopes: &[SwarmScope],
    ) -> Result<bool> {
        self.request_backfill_scopes_for_peer_inner(peer, node, scopes, false)
    }

    pub(crate) fn request_backfill_scopes_for_peer_now(
        &mut self,
        peer: &NetworkNodeId,
        node: &Node,
        scopes: &[SwarmScope],
    ) -> Result<bool> {
        self.request_backfill_scopes_for_peer_inner(peer, node, scopes, true)
    }

    fn request_backfill_scopes_for_peer_inner(
        &mut self,
        peer: &NetworkNodeId,
        node: &Node,
        scopes: &[SwarmScope],
        ignore_retry_delay: bool,
    ) -> Result<bool> {
        let now = Instant::now();
        let inflight = {
            let state = self
                .peer_sync_state
                .entry(peer.clone())
                .or_insert_with(|| PeerSyncState::new(now));
            if state.inflight_backfills() >= MAX_INFLIGHT_BACKFILLS_PER_PEER
                || (!ignore_retry_delay && state.next_retry_at > now)
            {
                return Ok(false);
            }
            state.inflight_backfills()
        };
        if scopes.is_empty() {
            return Ok(false);
        }
        let available_slots = MAX_INFLIGHT_BACKFILLS_PER_PEER.saturating_sub(inflight);
        if available_slots == 0 {
            return Ok(false);
        }
        let local_node_id = node.node_id();
        let network_id = current_network_context_id(node);
        let active_subscriptions = node
            .store
            .list_active_feed_subscriptions(&network_id, &local_node_id)?;
        let mut requests_sent = 0usize;
        let selected_route = self
            .state_dir
            .as_ref()
            .map(|state_dir| recommended_backfill_route_for_peer(state_dir, peer))
            .transpose()?
            .unwrap_or(DataTransportRoute::IrohControl);
        if let Some(state_dir) = &self.state_dir {
            append_jsonl(
                state_dir,
                "transport_decisions.jsonl",
                &json!({
                    "kind": TransferKind::BackfillChunk.as_str(),
                    "peer_id": peer.to_string(),
                    "route": selected_route.as_str(),
                    "scopes": scopes.iter().map(scope_hint_label).collect::<Vec<_>>(),
                    "decided_at": observed_at_ms(),
                }),
            );
        }
        'scopes: for scope in scopes.iter().cloned() {
            if requests_sent >= available_slots {
                break;
            }
            let scope_hint = scope_hint_label(&scope);
            let from_event_seq = self
                .peer_sync_state
                .get(peer)
                .map_or(0, |state| state.backfill_cursor(&scope, None));
            let known_event_ids =
                recent_backfill_lane_event_ids(node, &scope, None, BACKFILL_KNOWN_EVENT_IDS_LIMIT)?;
            let request_id = self.runtime.send_backfill_request(
                peer,
                BackfillRequest {
                    scope: scope.clone(),
                    from_event_seq,
                    limit: BACKFILL_BATCH_EVENTS,
                    feed_key: None,
                    known_event_ids,
                },
            )?;
            self.peer_sync_state
                .entry(peer.clone())
                .or_insert_with(|| PeerSyncState::new(now))
                .record_pending_backfill(request_id, scope.clone(), None, now);
            requests_sent += 1;
            for subscription in active_subscriptions
                .iter()
                .filter(|subscription| subscription.scope_hint == scope_hint)
            {
                if requests_sent >= available_slots {
                    break 'scopes;
                }
                let from_event_seq = self.peer_sync_state.get(peer).map_or(0, |state| {
                    state.backfill_cursor(&scope, Some(&subscription.feed_key))
                });
                let known_event_ids = recent_backfill_lane_event_ids(
                    node,
                    &scope,
                    Some(&subscription.feed_key),
                    BACKFILL_KNOWN_EVENT_IDS_LIMIT,
                )?;
                let request_id = self.runtime.send_backfill_request(
                    peer,
                    BackfillRequest {
                        scope: scope.clone(),
                        from_event_seq,
                        limit: BACKFILL_BATCH_EVENTS,
                        feed_key: Some(subscription.feed_key.clone()),
                        known_event_ids,
                    },
                )?;
                self.peer_sync_state
                    .entry(peer.clone())
                    .or_insert_with(|| PeerSyncState::new(now))
                    .record_pending_backfill(
                        request_id,
                        scope.clone(),
                        Some(subscription.feed_key.clone()),
                        now,
                    );
                requests_sent += 1;
            }
        }
        if requests_sent > 0 {
            if let Some(state) = self.peer_sync_state.get_mut(peer) {
                state.last_backfill_request_at = Some(now);
                state.next_retry_at = now + self.backfill_retry_after;
            }
            self.persist_peer_sync_state();
        }
        Ok(requests_sent > 0)
    }

    pub fn run_anti_entropy(&mut self, node: &Node) -> Result<usize> {
        let now = Instant::now();
        let mut requested = 0usize;
        let mut scopes = self.backfill_scopes();
        scopes.sort_by_key(|scope| matches!(scope, SwarmScope::Global));
        let mut requests_by_peer: HashMap<NetworkNodeId, Vec<SwarmScope>> = HashMap::new();
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

    pub(crate) fn scopes_to_request_for_peer(&self, peer: &NetworkNodeId) -> Vec<SwarmScope> {
        let Some(state) = self.peer_sync_state.get(peer) else {
            return self.backfill_scopes();
        };
        if state.known_scopes.is_empty() {
            return self.backfill_scopes();
        }
        let mut scopes = self.backfill_scopes();
        scopes.sort_by_key(|scope| !state.known_scopes.contains(scope));
        scopes
    }

    pub(crate) fn backfill_scopes(&self) -> Vec<SwarmScope> {
        let mut scopes = self.subscribed_scopes.clone();
        for scope in self.relay_scope_kinds.keys() {
            if !scopes.contains(scope) {
                scopes.push(scope.clone());
            }
        }
        scopes
    }

    pub(crate) fn preferred_backfill_peer_for_scope(
        &self,
        scope: &SwarmScope,
        now: Instant,
    ) -> Option<NetworkNodeId> {
        let peers = self.connected_peers.iter().cloned().collect::<Vec<_>>();
        self.best_peer_for_scope(peers.iter().cloned(), scope, now, true)
            .or_else(|| self.best_peer_for_scope(peers.into_iter(), scope, now, false))
    }

    pub(crate) fn best_peer_for_scope<I>(
        &self,
        peers: I,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> Option<NetworkNodeId>
    where
        I: Iterator<Item = NetworkNodeId>,
    {
        let runtime = self.runtime.observability_snapshot();
        let peer_scores = runtime
            .peer_health
            .into_iter()
            .map(|entry| (entry.peer.clone(), entry))
            .collect::<HashMap<_, _>>();
        peers
            .filter(|peer| self.peer_is_eligible_for_scope(peer, scope, now, require_known_scope))
            .max_by(|left, right| {
                let left_sync = self.peer_sync_state.get(left);
                let right_sync = self.peer_sync_state.get(right);
                let left_key = peer_scores
                    .get(&left.to_string())
                    .map_or((0_i64, 100_u32), |entry| {
                        (entry.score, entry.throttle_factor_percent)
                    });
                let right_key = peer_scores
                    .get(&right.to_string())
                    .map_or((0_i64, 100_u32), |entry| {
                        (entry.score, entry.throttle_factor_percent)
                    });
                let left_quality =
                    left_sync.map_or((0_u64, std::cmp::Reverse(u64::MAX)), |state| {
                        (
                            state.backfill_successes,
                            std::cmp::Reverse(state.backfill_failures),
                        )
                    });
                let right_quality =
                    right_sync.map_or((0_u64, std::cmp::Reverse(u64::MAX)), |state| {
                        (
                            state.backfill_successes,
                            std::cmp::Reverse(state.backfill_failures),
                        )
                    });
                left_key
                    .cmp(&right_key)
                    .then_with(|| left_quality.cmp(&right_quality))
                    .then_with(|| left.to_string().cmp(&right.to_string()))
            })
    }

    pub(crate) fn peer_is_eligible_for_scope(
        &self,
        peer: &NetworkNodeId,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> bool {
        if !self.runtime.allows_outbound_backfill_to(peer) {
            return false;
        }
        match self.peer_sync_state.get(peer) {
            Some(state) => {
                if require_known_scope && !state.known_scopes.contains(scope) {
                    return false;
                }
                state.inflight_backfills() < MAX_INFLIGHT_BACKFILLS_PER_PEER
                    && state.next_retry_at <= now
                    && state.last_backfill_request_at.map_or(true, |last| {
                        now.duration_since(last) >= self.anti_entropy_interval
                    })
            }
            None => !require_known_scope,
        }
    }

    pub(crate) fn record_peer_scope_activity(&mut self, peer: NetworkNodeId, scope: &SwarmScope) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .known_scopes
            .insert(scope.clone());
        self.persist_peer_sync_state();
    }

    pub(crate) fn scope_traffic_mut(&mut self, scope: &SwarmScope) -> &mut ScopeTrafficStats {
        self.scope_traffic.entry(scope.clone()).or_default()
    }

    pub(crate) fn record_scope_event_published(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).published_events += 1;
    }

    pub(crate) fn record_scope_event_ingested(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).ingested_events += 1;
    }

    pub(crate) fn record_scope_summary_applied(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).summaries_applied += 1;
    }

    pub(crate) fn record_scope_rule_applied(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).rules_applied += 1;
    }

    pub(crate) fn record_scope_checkpoint_applied(&mut self, scope: &SwarmScope) {
        self.scope_traffic_mut(scope).checkpoints_applied += 1;
    }

    pub(crate) fn record_scope_backfill_applied(&mut self, scope: &SwarmScope, events: usize) {
        let stats = self.scope_traffic_mut(scope);
        stats.backfills_applied += 1;
        stats.backfill_events_applied += events as u64;
    }

    pub(crate) fn mark_peer_connected(&mut self, peer: NetworkNodeId) -> bool {
        let inserted = self.connected_peers.insert(peer.clone());
        self.peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new(Instant::now()));
        self.peer_reconnect_state.remove(&peer);
        self.persist_peer_sync_state();
        inserted
    }

    pub(crate) fn mark_peer_disconnected(&mut self, peer: NetworkNodeId) -> bool {
        let removed = self.connected_peers.remove(&peer);
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfill_requests.clear();
        }
        self.pending_relationship_requests
            .retain(|_, pending| pending.peer != peer);
        self.pending_dm_requests
            .retain(|_, pending| pending.peer != peer);
        self.persist_peer_sync_state();
        removed
    }

    pub(crate) fn remember_peer_address(&mut self, peer: NetworkNodeId, address: NetworkAddress) {
        self.known_peer_addrs.insert(peer.clone(), address);
        self.peer_reconnect_state
            .entry(peer)
            .or_insert_with(PeerReconnectState::ready_now);
    }

    pub(crate) fn remember_peer_address_from_contact(
        &mut self,
        peer: &str,
        contact: &TransportContactMaterial,
    ) {
        let Some(address) = contact.metadata.listen_addrs.first() else {
            return;
        };
        let Ok(peer) = NetworkNodeId::new(peer.to_owned()) else {
            return;
        };
        let Ok(address) = NetworkAddress::new(address.clone()) else {
            return;
        };
        self.remember_peer_address(peer, address);
    }

    pub(crate) fn schedule_peer_reconnect(&mut self, peer: NetworkNodeId) {
        if self.known_peer_addrs.contains_key(&peer)
            || self.runtime.allows_outbound_backfill_to(&peer)
        {
            self.peer_reconnect_state
                .entry(peer)
                .or_insert_with(PeerReconnectState::ready_now);
        }
    }

    pub fn run_reconnect_supervision(&mut self) -> Result<usize> {
        let now = Instant::now();
        let mut reconnect_candidates = self
            .known_peer_addrs
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        reconnect_candidates.extend(self.runtime.remote_contact_peer_ids());
        let peers = reconnect_candidates
            .into_iter()
            .filter(|peer| !self.connected_peers.contains(peer))
            .filter(|peer| {
                self.peer_reconnect_state
                    .get(peer)
                    .is_none_or(|state| state.next_attempt_at <= now)
            })
            .collect::<Vec<_>>();
        let mut attempts = 0usize;
        for peer in peers {
            let reconnect_address = self.known_peer_addrs.get(&peer).cloned();
            let reconnect_route = if reconnect_address.is_some() {
                "direct_addr"
            } else {
                "iroh_contact"
            };
            let reconnect_phase = if reconnect_address.is_some() {
                "reconnect.dial"
            } else {
                "reconnect.bootstrap"
            };
            let result = if let Some(address) = reconnect_address.clone() {
                let dial_target = NetworkAddress::new(format!("{peer}@{address}"))?;
                self.runtime.dial(dial_target)
            } else {
                self.runtime
                    .rejoin_gossip_with_remote_contact(&peer)
                    .and_then(|joined| {
                        if joined {
                            Ok(())
                        } else {
                            Err(anyhow!("missing iroh contact material for {peer}"))
                        }
                    })
            };
            let state = self
                .peer_reconnect_state
                .entry(peer.clone())
                .or_insert_with(PeerReconnectState::ready_now);
            state.schedule_next_attempt(now);
            attempts += 1;
            match result {
                Ok(()) => {
                    diagnostics::record_diagnostic(
                        self.state_dir.as_deref(),
                        diagnostics::DiagnosticEvent::new(
                            "info",
                            "transport",
                            reconnect_phase,
                            "ok",
                            format!("peer reconnect scheduled: {peer}"),
                        )
                        .object("peer", Some(peer.to_string()))
                        .source_node_id(Some(peer.to_string()))
                        .details(json!({
                            "address": reconnect_address.as_ref().map(ToString::to_string),
                            "route": reconnect_route,
                            "attempts": state.attempts,
                        })),
                    );
                }
                Err(err) => {
                    diagnostics::record_diagnostic(
                        self.state_dir.as_deref(),
                        diagnostics::DiagnosticEvent::new(
                            "warn",
                            "transport",
                            reconnect_phase,
                            "failed",
                            format!("peer reconnect failed: {peer}"),
                        )
                        .object("peer", Some(peer.to_string()))
                        .source_node_id(Some(peer.to_string()))
                        .details(json!({
                            "address": reconnect_address.as_ref().map(ToString::to_string),
                            "route": reconnect_route,
                            "attempts": state.attempts,
                            "error": err.to_string(),
                        })),
                    );
                }
            }
        }
        Ok(attempts)
    }

    pub(crate) fn mark_backfill_completed(
        &mut self,
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
    ) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            let was_pending = state
                .inflight_backfill_requests
                .remove(&request_id)
                .is_some();
            state.next_retry_at = Instant::now() + self.anti_entropy_interval;
            if was_pending {
                state.backfill_successes = state.backfill_successes.saturating_add(1);
            }
        }
        self.persist_peer_sync_state();
    }

    pub(crate) fn record_peer_remote_head_event_ids(
        &mut self,
        peer: NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        head_event_ids: &[String],
    ) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .record_remote_head_event_ids(scope, feed_key, head_event_ids);
        self.persist_peer_sync_state();
    }

    pub(crate) fn record_peer_backfill_cursor(
        &mut self,
        peer: NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        next_from_event_seq: u64,
    ) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .record_backfill_cursor(scope, feed_key, next_from_event_seq);
        self.persist_peer_sync_state();
    }

    pub(crate) fn reset_peer_backfill_cursor(
        &mut self,
        peer: NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
    ) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .reset_backfill_cursor(scope, feed_key);
        self.persist_peer_sync_state();
    }

    pub(crate) fn mark_backfill_failed(
        &mut self,
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
    ) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            let was_pending = state
                .inflight_backfill_requests
                .remove(&request_id)
                .is_some();
            if was_pending {
                state.next_retry_at = Instant::now() + self.backfill_retry_after;
                state.backfill_failures = state.backfill_failures.saturating_add(1);
            }
        }
        self.persist_peer_sync_state();
    }

    pub(crate) fn expire_stale_backfill_requests(
        &mut self,
        now: Instant,
        timeout: Duration,
    ) -> usize {
        let mut expired = Vec::new();
        for (peer, state) in &self.peer_sync_state {
            for (request_id, pending) in &state.inflight_backfill_requests {
                if now.duration_since(pending.sent_at) >= timeout {
                    expired.push((
                        peer.clone(),
                        *request_id,
                        pending.scope.clone(),
                        pending.feed_key.clone(),
                    ));
                }
            }
        }
        if expired.is_empty() {
            return 0;
        }
        for (peer, request_id, scope, feed_key) in &expired {
            if let Some(state) = self.peer_sync_state.get_mut(peer)
                && state
                    .inflight_backfill_requests
                    .remove(request_id)
                    .is_some()
            {
                state.next_retry_at = now + self.backfill_retry_after;
                state.backfill_failures = state.backfill_failures.saturating_add(1);
            }
            diagnostics::record_diagnostic(
                self.state_dir.as_deref(),
                diagnostics::DiagnosticEvent::new(
                    "warn",
                    "backfill",
                    "request.timeout",
                    "failed",
                    "backfill request timed out",
                )
                .object("backfill", Some(request_id.to_string()))
                .source_node_id(Some(peer.to_string()))
                .scope(scope)
                .details(json!({
                    "request_id": request_id.to_string(),
                    "feed_key": feed_key,
                    "timeout_ms": timeout.as_millis(),
                })),
            );
        }
        self.persist_peer_sync_state();
        expired.len()
    }

    pub fn try_tick(&mut self, node: &mut Node) -> Result<Option<NetworkBridgeTick>> {
        let event = self.runtime.try_next_event()?;
        let Some(event) = event else {
            return Ok(None);
        };
        Ok(Some(self.process_runtime_event(node, event)?))
    }

    pub(crate) fn handle_runtime_event(
        &mut self,
        node: &mut Node,
        event: Result<NetworkRuntimeEvent>,
    ) -> Result<NetworkBridgeTick> {
        self.process_runtime_event(node, event?)
    }
}
