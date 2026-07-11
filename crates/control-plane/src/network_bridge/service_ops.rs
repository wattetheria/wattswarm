use super::bootstrap_contact::transport_contact_direct_network_addrs;
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
        let now_ms = observed_at_ms();
        let connected_peer_count = self
            .connected_peers
            .iter()
            .filter(|peer| self.peer_recently_seen_at(peer, now))
            .count();
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
            let peer_id = peer.parse::<NetworkNodeId>().ok();
            let mesh_connected = peer_id
                .as_ref()
                .is_some_and(|peer_id| self.connected_peers.contains(peer_id));
            let recently_seen = peer_id
                .as_ref()
                .is_some_and(|peer_id| self.peer_recently_seen_at(peer_id, now));
            let last_seen_age_ms = sync.and_then(|state| state.last_seen_age_ms_at(now_ms));
            peer_health.push(NetworkBridgePeerHealth {
                network_peer_id: peer.clone(),
                connected: mesh_connected && recently_seen,
                recently_seen,
                stale: mesh_connected && !recently_seen,
                last_seen_age_ms,
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
            connected_peer_count,
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
            BACKFILL_TIMEOUT_FALLBACK,
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
                .or_insert_with(|| PeerSyncState::new_unobserved(now));
            if state.inflight_backfills() >= MAX_INFLIGHT_BACKFILLS_PER_PEER
                || (!ignore_retry_delay && state.next_retry_at > now)
                || !state.backfill_peer_available_at(now)
            {
                return Ok(false);
            }
            state.inflight_backfills()
        };
        let scopes = scopes
            .iter()
            .filter(|scope| **scope != SwarmScope::Global || self.is_global_backfill_provider(peer))
            .cloned()
            .collect::<Vec<_>>();
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
            let request_scope_history =
                scope == SwarmScope::Global || self.subscribed_scope_kinds.contains_key(&scope);
            if request_scope_history
                && self.peer_backfill_available(peer, now)
                && self.peer_backfill_lane_available(peer, &scope, None, now)
            {
                let from_event_seq = self
                    .peer_sync_state
                    .get(peer)
                    .map_or(0, |state| state.backfill_cursor(&scope, None));
                let known_event_ids = recent_backfill_lane_event_ids(
                    node,
                    &scope,
                    None,
                    BACKFILL_KNOWN_EVENT_IDS_LIMIT,
                )?;
                let timeout = self.peer_sync_state.get(peer).map_or(
                    BACKFILL_TIMEOUT_FALLBACK,
                    PeerSyncState::backfill_request_timeout,
                );
                let request_id = self.runtime.send_backfill_request(
                    peer,
                    BackfillRequest {
                        scope: scope.clone(),
                        from_event_seq,
                        limit: BACKFILL_BATCH_EVENTS,
                        feed_key: None,
                        known_event_ids,
                    },
                    timeout,
                )?;
                self.peer_sync_state
                    .entry(peer.clone())
                    .or_insert_with(|| PeerSyncState::new_unobserved(now))
                    .record_pending_backfill_with_timeout(
                        request_id,
                        scope.clone(),
                        None,
                        now,
                        timeout,
                    );
                requests_sent += 1;
            }
            for subscription in active_subscriptions
                .iter()
                .filter(|subscription| subscription.scope_hint == scope_hint)
            {
                if requests_sent >= available_slots {
                    break 'scopes;
                }
                if !self.peer_backfill_available(peer, now) {
                    break 'scopes;
                }
                if !self.peer_backfill_lane_available(
                    peer,
                    &scope,
                    Some(&subscription.feed_key),
                    now,
                ) {
                    continue;
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
                let timeout = self.peer_sync_state.get(peer).map_or(
                    BACKFILL_TIMEOUT_FALLBACK,
                    PeerSyncState::backfill_request_timeout,
                );
                let request_id = self.runtime.send_backfill_request(
                    peer,
                    BackfillRequest {
                        scope: scope.clone(),
                        from_event_seq,
                        limit: BACKFILL_BATCH_EVENTS,
                        feed_key: Some(subscription.feed_key.clone()),
                        known_event_ids,
                    },
                    timeout,
                )?;
                self.peer_sync_state
                    .entry(peer.clone())
                    .or_insert_with(|| PeerSyncState::new_unobserved(now))
                    .record_pending_backfill_with_timeout(
                        request_id,
                        scope.clone(),
                        Some(subscription.feed_key.clone()),
                        now,
                        timeout,
                    );
                requests_sent += 1;
            }
        }
        if requests_sent > 0 {
            if let Some(state) = self.peer_sync_state.get_mut(peer) {
                state.last_backfill_request_at = Some(now);
                state.next_retry_at = now + self.backfill_retry_after;
            }
            self.persist_peer_sync_state_for_peer(peer);
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
        let scopes = self.backfill_scopes();
        let global_scope = scopes
            .iter()
            .any(|scope| *scope == SwarmScope::Global && self.is_global_backfill_provider(peer))
            .then_some(SwarmScope::Global);
        let Some(state) = self.peer_sync_state.get(peer) else {
            return global_scope.into_iter().collect();
        };
        if state.known_scopes.is_empty() {
            return global_scope.into_iter().collect();
        }
        let mut requested = scopes
            .into_iter()
            .filter(|scope| *scope != SwarmScope::Global && state.known_scopes.contains(scope))
            .collect::<Vec<_>>();
        requested.extend(global_scope);
        requested
    }

    pub(crate) fn backfill_scopes(&self) -> Vec<SwarmScope> {
        self.subscribed_scopes.clone()
    }

    pub(crate) fn preferred_backfill_peer_for_scope(
        &self,
        scope: &SwarmScope,
        now: Instant,
    ) -> Option<NetworkNodeId> {
        let peers = self.connected_peers.iter().cloned().collect::<Vec<_>>();
        if *scope == SwarmScope::Global {
            return self.best_peer_for_scope(
                peers
                    .into_iter()
                    .filter(|peer| self.is_global_backfill_provider(peer)),
                scope,
                now,
                false,
            );
        }
        self.best_peer_for_scope(peers.into_iter(), scope, now, true)
    }

    fn peer_backfill_available(&self, peer: &NetworkNodeId, now: Instant) -> bool {
        self.peer_sync_state
            .get(peer)
            .is_none_or(|state| state.backfill_peer_available_at(now))
    }

    fn peer_backfill_lane_available(
        &self,
        peer: &NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        now: Instant,
    ) -> bool {
        self.peer_sync_state
            .get(peer)
            .is_none_or(|state| state.backfill_lane_available_at(scope, feed_key, now))
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
                if !state.recently_seen_at(now) {
                    return false;
                }
                state.inflight_backfills() < MAX_INFLIGHT_BACKFILLS_PER_PEER
                    && state.next_retry_at <= now
                    && state.backfill_peer_available_at(now)
                    && state.backfill_lane_available_at(scope, None, now)
                    && state.last_backfill_request_at.map_or(true, |last| {
                        now.duration_since(last) >= self.anti_entropy_interval
                    })
            }
            None => false,
        }
    }

    pub(crate) fn peer_recently_seen_at(&self, peer: &NetworkNodeId, now: Instant) -> bool {
        self.peer_sync_state
            .get(peer)
            .is_some_and(|state| state.recently_seen_at(now))
    }

    pub(crate) fn record_peer_liveness(&mut self, peer: NetworkNodeId) {
        let now = Instant::now();
        self.peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new(now))
            .record_seen_at(now);
        self.record_reconnect_candidate_activity(peer, now);
    }

    pub(crate) fn record_peer_scope_activity(&mut self, peer: NetworkNodeId, scope: &SwarmScope) {
        let now = Instant::now();
        let state = self
            .peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new(now));
        state.record_seen_at(now);
        state.known_scopes.insert(scope.clone());
        self.persist_peer_sync_state_for_peer(&peer);
    }

    pub(crate) fn remember_global_backfill_provider(&mut self, peer: NetworkNodeId) {
        self.global_backfill_providers.insert(peer);
    }

    pub(crate) fn is_global_backfill_provider(&self, peer: &NetworkNodeId) -> bool {
        self.global_backfill_providers.contains(peer)
    }

    pub(crate) fn peer_has_scope_activity(&self, peer: &NetworkNodeId, scope: &SwarmScope) -> bool {
        self.peer_sync_state
            .get(peer)
            .is_some_and(|state| state.known_scopes.contains(scope))
    }

    pub(crate) fn serves_backfill_scope(&self, scope: &SwarmScope) -> bool {
        *scope == SwarmScope::Global || self.subscribed_scope_kinds.contains_key(scope)
    }

    pub(crate) fn inbound_backfill_authorized(
        &self,
        peer: &NetworkNodeId,
        request: &BackfillRequest,
    ) -> bool {
        if request.scope == SwarmScope::Global {
            return true;
        }
        if self.serves_backfill_scope(&request.scope) {
            return true;
        }
        if self.peer_has_scope_activity(peer, &request.scope) {
            return true;
        }
        false
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
        let now = Instant::now();
        let inserted = self.connected_peers.insert(peer.clone());
        self.peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new(now))
            .record_seen_at(now);
        self.peer_reconnect_state.remove(&peer);
        self.abandoned_reconnect_peers.remove(&peer);
        self.abandoned_recovery_probe_activity.remove(&peer);
        self.record_reconnect_candidate_activity(peer.clone(), now);
        self.persist_peer_sync_state_for_peer(&peer);
        inserted
    }

    pub(crate) fn expire_stale_connected_peers(&mut self, now: Instant) -> usize {
        let stale_peers = self
            .connected_peers
            .iter()
            .filter(|peer| !self.peer_recently_seen_at(peer, now))
            .cloned()
            .collect::<Vec<_>>();
        let mut expired = 0usize;
        for peer in stale_peers {
            let (last_seen_age_ms, inflight_backfills, known_scope_count, next_retry_in_ms) = self
                .peer_sync_state
                .get(&peer)
                .map(|state| {
                    (
                        now.saturating_duration_since(state.last_seen_at)
                            .as_millis(),
                        state.inflight_backfills(),
                        state.known_scopes.len(),
                        state
                            .next_retry_at
                            .saturating_duration_since(now)
                            .as_millis(),
                    )
                })
                .unwrap_or((0, 0, 0, 0));
            if self.mark_peer_disconnected(peer.clone()) {
                diagnostics::record_diagnostic(
                    self.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "warn",
                        "transport",
                        "connection.expired_stale",
                        "disconnected",
                        format!("stale connected peer expired: {peer}"),
                    )
                    .event_id(format!(
                        "stale-connected-peer-expired:{peer}:{}",
                        observed_at_ms()
                    ))
                    .object("peer", Some(peer.to_string()))
                    .source_node_id(Some(peer.to_string()))
                    .details(json!({
                        "peer_id": peer.to_string(),
                        "reason": "peer_last_seen_ttl_expired",
                        "last_seen_age_ms": last_seen_age_ms,
                        "stale_after_ms": PEER_LAST_SEEN_TTL.as_millis(),
                        "inflight_backfills": inflight_backfills,
                        "known_scope_count": known_scope_count,
                        "next_retry_in_ms": next_retry_in_ms,
                    })),
                );
                self.schedule_peer_reconnect(peer);
                expired += 1;
            }
        }
        expired
    }

    pub(crate) fn mark_peer_disconnected(&mut self, peer: NetworkNodeId) -> bool {
        let removed = self.connected_peers.remove(&peer);
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfill_requests.clear();
            state.clear_backfill_recovery_probes();
        }
        self.pending_relationship_requests
            .retain(|_, pending| pending.peer != peer);
        self.persist_peer_sync_state_for_peer(&peer);
        removed
    }

    pub(crate) fn mark_peer_control_stream_failed(&mut self, peer: NetworkNodeId) {
        self.mark_peer_disconnected(peer.clone());
        self.schedule_peer_reconnect(peer);
    }

    pub(crate) fn expire_stale_contact_material_requests(&mut self, now: Instant) -> usize {
        let stale_requests = self
            .pending_contact_material_requests
            .iter()
            .filter_map(|(request_id, pending)| {
                let age = now.saturating_duration_since(pending.started_at);
                (age >= PENDING_CONTACT_MATERIAL_REQUEST_TTL).then(|| {
                    (
                        *request_id,
                        pending.peer.clone(),
                        pending.remote_node_id.clone(),
                        age,
                    )
                })
            })
            .collect::<Vec<_>>();
        for (request_id, peer, remote_node_id, age) in &stale_requests {
            self.pending_contact_material_requests.remove(request_id);
            diagnostics::record_diagnostic(
                self.state_dir.as_deref(),
                diagnostics::DiagnosticEvent::new(
                    "warn",
                    "transport",
                    "contact_material.pending.expired",
                    "expired",
                    format!("stale contact material request expired: {peer}"),
                )
                .object("peer", Some(peer.to_string()))
                .source_node_id(Some(peer.to_string()))
                .details(json!({
                    "peer_id": peer.to_string(),
                    "remote_node_id": remote_node_id,
                    "request_id": format!("{request_id:?}"),
                    "age_ms": age.as_millis(),
                    "ttl_ms": PENDING_CONTACT_MATERIAL_REQUEST_TTL.as_millis(),
                })),
            );
            self.schedule_peer_reconnect(peer.clone());
        }
        stale_requests.len()
    }

    pub(crate) fn remember_peer_address(&mut self, peer: NetworkNodeId, address: NetworkAddress) {
        self.abandoned_reconnect_peers.remove(&peer);
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
        let Some(address) = transport_contact_direct_network_addrs(contact)
            .into_iter()
            .next()
        else {
            return;
        };
        let Ok(peer) = NetworkNodeId::new(peer.to_owned()) else {
            return;
        };
        let Ok(address) = NetworkAddress::new(address) else {
            return;
        };
        self.remember_peer_address(peer, address);
    }

    pub(crate) fn record_reconnect_candidate_activity(
        &mut self,
        peer: NetworkNodeId,
        observed_at: Instant,
    ) {
        self.reconnect_candidate_activity.insert(peer, observed_at);
    }

    fn recent_reconnect_candidate_activity(
        &self,
        peer: &NetworkNodeId,
        now: Instant,
    ) -> Option<Instant> {
        self.reconnect_candidate_activity
            .get(peer)
            .copied()
            .filter(|observed_at| {
                now.saturating_duration_since(*observed_at) <= RECONNECT_CANDIDATE_RETENTION
            })
    }

    fn reconnect_candidate_is_active(&self, peer: &NetworkNodeId, now: Instant) -> bool {
        let Some(activity_at) = self.recent_reconnect_candidate_activity(peer, now) else {
            return false;
        };
        self.reconnect_probe_activity
            .get(peer)
            .is_none_or(|last_probe_activity| *last_probe_activity < activity_at)
    }

    fn abandoned_recovery_probe_is_due(&self, peer: &NetworkNodeId, now: Instant) -> bool {
        let Some(activity_at) = self.recent_reconnect_candidate_activity(peer, now) else {
            return false;
        };
        self.abandoned_recovery_probe_activity
            .get(peer)
            .is_none_or(|last_probe_activity| *last_probe_activity < activity_at)
    }

    pub(crate) fn schedule_peer_reconnect(&mut self, peer: NetworkNodeId) {
        if self.abandoned_reconnect_peers.contains(&peer) {
            return;
        }
        if self.known_peer_addrs.contains_key(&peer)
            || self.runtime.allows_outbound_backfill_to(&peer)
        {
            self.peer_reconnect_state
                .entry(peer)
                .or_insert_with(PeerReconnectState::ready_now);
        }
    }

    pub(crate) fn reactivate_discovered_peer_reconnect(
        &mut self,
        peer: NetworkNodeId,
        contact_updated: bool,
    ) {
        if !contact_updated {
            return;
        }
        if self.abandoned_reconnect_peers.remove(&peer) {
            self.abandoned_recovery_probe_activity.remove(&peer);
            self.peer_reconnect_state
                .insert(peer, PeerReconnectState::ready_now());
        } else {
            self.schedule_peer_reconnect(peer);
        }
    }

    pub fn run_reconnect_supervision(&mut self) -> Result<usize> {
        let now = Instant::now();
        self.expire_stale_contact_material_requests(now);
        let reconnect_candidates = self
            .runtime
            .remote_contact_peer_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        let mut peers = reconnect_candidates
            .into_iter()
            .filter(|peer| !self.connected_peers.contains(peer))
            .filter(|peer| {
                !self
                    .pending_contact_material_requests
                    .values()
                    .any(|pending| &pending.peer == peer)
            })
            .filter(|peer| {
                self.peer_reconnect_state
                    .get(peer)
                    .is_none_or(|state| state.next_attempt_at <= now)
            })
            .filter(|peer| {
                if self.abandoned_reconnect_peers.contains(peer) {
                    return self.abandoned_recovery_probe_is_due(peer, now);
                }
                self.reconnect_candidate_is_active(peer, now)
            })
            .collect::<Vec<_>>();
        peers.sort_by_key(|peer| {
            (
                self.peer_reconnect_state
                    .get(peer)
                    .and_then(|state| state.last_attempt_at),
                peer.to_string(),
            )
        });
        let mut attempts = 0usize;
        let mut contact_probe_pending = !self.pending_contact_material_requests.is_empty();
        for peer in peers {
            let reconnect_address = self.known_peer_addrs.get(&peer).cloned();
            if contact_probe_pending {
                continue;
            }
            let reconnect_route = "iroh_contact";
            let reconnect_phase = "reconnect.bootstrap";
            if let Some(activity_at) = self.recent_reconnect_candidate_activity(&peer, now) {
                self.reconnect_probe_activity
                    .insert(peer.clone(), activity_at);
                if self.abandoned_reconnect_peers.contains(&peer) {
                    self.abandoned_recovery_probe_activity
                        .insert(peer.clone(), activity_at);
                }
            }
            let result = match self.runtime.rejoin_gossip_with_remote_contact(&peer) {
                Ok(true) => self.probe_peer_contact_material(&peer).map(|_| ()),
                Ok(false) => Err(anyhow!("missing iroh contact material for {peer}")),
                Err(err) => Err(err),
            };
            // Iroh control requests are serialized by the data plane.
            contact_probe_pending = !self.pending_contact_material_requests.is_empty();
            let reconnect_attempts = {
                let state = self
                    .peer_reconnect_state
                    .entry(peer.clone())
                    .or_insert_with(PeerReconnectState::ready_now);
                state.schedule_next_attempt(now);
                state.attempts
            };
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
                            "attempts": reconnect_attempts,
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
                            "attempts": reconnect_attempts,
                            "error": err.to_string(),
                        })),
                    );
                }
            }
            if self
                .peer_reconnect_state
                .get(&peer)
                .is_some_and(PeerReconnectState::should_abandon)
            {
                self.abandon_peer_reconnect(
                    peer,
                    reconnect_attempts,
                    reconnect_route,
                    reconnect_address,
                );
            }
        }
        Ok(attempts)
    }

    pub(super) fn abandon_peer_reconnect(
        &mut self,
        peer: NetworkNodeId,
        attempts: u32,
        route: &str,
        address: Option<NetworkAddress>,
    ) {
        if let Some(activity_at) = self.recent_reconnect_candidate_activity(&peer, Instant::now()) {
            self.abandoned_recovery_probe_activity
                .insert(peer.clone(), activity_at);
        }
        self.pending_contact_material_requests
            .retain(|_, pending| pending.peer != peer);
        self.peer_reconnect_state.remove(&peer);
        self.known_peer_addrs.remove(&peer);
        self.abandoned_reconnect_peers.insert(peer.clone());
        diagnostics::record_diagnostic(
            self.state_dir.as_deref(),
            diagnostics::DiagnosticEvent::new(
                "warn",
                "transport",
                "reconnect.abandoned",
                "abandoned",
                format!("peer reconnect abandoned: {peer}"),
            )
            .object("peer", Some(peer.to_string()))
            .source_node_id(Some(peer.to_string()))
            .details(json!({
                "address": address.as_ref().map(ToString::to_string),
                "route": route,
                "attempts": attempts,
            })),
        );
    }

    pub(crate) fn record_peer_backfill_response_progress(
        &mut self,
        peer: NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        head_event_ids: &[String],
        request_id: BackfillRequestId,
        next_from_event_seq: u64,
        reset_cursor: bool,
    ) {
        let now = Instant::now();
        let next_retry_at = now + self.anti_entropy_interval;
        let state = self
            .peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new(now));
        state.record_seen_at(now);
        state.known_scopes.insert(scope.clone());
        state.record_remote_head_event_ids(scope, feed_key, head_event_ids);
        let was_pending = state.record_backfill_request_completed(request_id, now);
        state.next_retry_at = next_retry_at;
        if was_pending {
            state.backfill_successes = state.backfill_successes.saturating_add(1);
            state.backfill_failures = 0;
        }
        if reset_cursor {
            state.reset_backfill_cursor(scope, feed_key);
        } else {
            state.record_backfill_cursor(scope, feed_key, next_from_event_seq);
        }
        self.persist_peer_sync_state_for_peer(&peer);
    }

    #[cfg(test)]
    pub(crate) fn record_peer_remote_head_event_ids(
        &mut self,
        peer: NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        head_event_ids: &[String],
    ) {
        self.peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new_unobserved(Instant::now()))
            .record_remote_head_event_ids(scope, feed_key, head_event_ids);
        self.persist_peer_sync_state_for_peer(&peer);
    }

    #[cfg(test)]
    pub(crate) fn record_peer_backfill_cursor(
        &mut self,
        peer: NetworkNodeId,
        scope: &SwarmScope,
        feed_key: Option<&str>,
        next_from_event_seq: u64,
    ) {
        self.peer_sync_state
            .entry(peer.clone())
            .or_insert_with(|| PeerSyncState::new_unobserved(Instant::now()))
            .record_backfill_cursor(scope, feed_key, next_from_event_seq);
        self.persist_peer_sync_state_for_peer(&peer);
    }

    pub(crate) fn mark_backfill_failed(
        &mut self,
        peer: NetworkNodeId,
        request_id: BackfillRequestId,
    ) {
        let now = Instant::now();
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            let was_pending = state
                .record_backfill_request_failed_at(request_id, now)
                .is_some();
            if was_pending {
                state.record_peer_backfill_timeout_at(now);
                state.next_retry_at = now + self.backfill_retry_after;
                state.backfill_failures = state.backfill_failures.saturating_add(1);
            }
        }
        self.persist_peer_sync_state_for_peer(&peer);
    }

    pub(crate) fn expire_stale_backfill_requests(&mut self, now: Instant) -> usize {
        let mut expired = Vec::new();
        for (peer, state) in &self.peer_sync_state {
            for (request_id, pending) in &state.inflight_backfill_requests {
                if now.duration_since(pending.sent_at) >= pending.timeout {
                    expired.push((
                        peer.clone(),
                        *request_id,
                        pending.scope.clone(),
                        pending.feed_key.clone(),
                        pending.timeout,
                    ));
                }
            }
        }
        if expired.is_empty() {
            return 0;
        }
        let mut peer_cooldowns = HashMap::new();
        for (peer, request_id, scope, feed_key, timeout) in &expired {
            let mut cooldown_event = None;
            if let Some(state) = self.peer_sync_state.get_mut(peer)
                && let Some(event) = state.record_backfill_request_failed_at(*request_id, now)
            {
                peer_cooldowns
                    .entry(peer.clone())
                    .or_insert_with(|| state.record_peer_backfill_timeout_at(now));
                state.next_retry_at = now + self.backfill_retry_after;
                state.backfill_failures = state.backfill_failures.saturating_add(1);
                cooldown_event = Some(event);
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
                    "cooldown_ms": cooldown_event.map(|event| {
                        u64::try_from(event.cooldown.as_millis()).unwrap_or(u64::MAX)
                    }),
                    "recovery_probe": cooldown_event.is_some_and(|event| event.was_recovery_probe),
                    "peer_cooldown_ms": peer_cooldowns.get(peer).map(|event| {
                        u64::try_from(event.cooldown.as_millis()).unwrap_or(u64::MAX)
                    }),
                    "peer_recovery_probe": peer_cooldowns
                        .get(peer)
                        .is_some_and(|event| event.was_recovery_probe),
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
