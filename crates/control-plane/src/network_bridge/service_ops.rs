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
                peer: peer.clone(),
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
                inflight_backfills: sync.map_or(0, |state| state.inflight_backfills),
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
            local_peer_id: self.local_peer_id().to_string(),
            listen_addrs: self
                .listen_addrs()
                .iter()
                .map(ToString::to_string)
                .collect(),
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
                feed_key: None,
            },
        )
    }

    pub fn tick(&mut self, node: &mut Node) -> Result<NetworkBridgeTick> {
        let event = self.tokio_runtime.block_on(self.runtime.next_event());
        self.handle_runtime_event(node, event)
    }

    pub(crate) fn request_backfill_for_peer(&mut self, peer: &PeerId, node: &Node) -> Result<bool> {
        let scopes = self.scopes_to_request_for_peer(peer);
        self.request_backfill_scopes_for_peer(peer, node, &scopes)
    }

    pub(crate) fn request_backfill_scopes_for_peer(
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
        let local_node_id = node.node_id();
        let active_subscriptions = node.store.list_active_feed_subscriptions(&local_node_id)?;
        let mut requests_sent = 0usize;
        let selected_route = self
            .state_dir
            .as_ref()
            .map(|state_dir| recommended_backfill_route_for_peer(state_dir, peer))
            .transpose()?
            .unwrap_or(DataTransportRoute::Libp2pControl);
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
        for scope in scopes.iter().cloned() {
            let scope_hint = scope_hint_label(&scope);
            let from_event_seq = latest_scoped_event_seq(node, &scope)?;
            let _guard = self.tokio_runtime.enter();
            let _ = self.runtime.send_backfill_request(
                peer,
                BackfillRequest {
                    scope: scope.clone(),
                    from_event_seq,
                    limit: BACKFILL_BATCH_EVENTS,
                    feed_key: None,
                },
            )?;
            requests_sent += 1;
            for subscription in active_subscriptions
                .iter()
                .filter(|subscription| subscription.scope_hint == scope_hint)
            {
                let from_event_seq = node
                    .store
                    .get_topic_cursor(&local_node_id, &subscription.feed_key)?
                    .map_or(0, |cursor| cursor.last_event_seq);
                let _ = self.runtime.send_backfill_request(
                    peer,
                    BackfillRequest {
                        scope: scope.clone(),
                        from_event_seq,
                        limit: BACKFILL_BATCH_EVENTS,
                        feed_key: Some(subscription.feed_key.clone()),
                    },
                )?;
                requests_sent += 1;
            }
        }
        state.inflight_backfills += requests_sent;
        state.last_backfill_request_at = Some(now);
        state.next_retry_at = now + self.backfill_retry_after;
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

    pub(crate) fn scopes_to_request_for_peer(&self, peer: &PeerId) -> Vec<SwarmScope> {
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

    pub(crate) fn preferred_backfill_peer_for_scope(
        &self,
        scope: &SwarmScope,
        now: Instant,
    ) -> Option<PeerId> {
        let peers = self.connected_peers.iter().copied().collect::<Vec<_>>();
        self.best_peer_for_scope(peers.iter().copied(), scope, now, true)
            .or_else(|| self.best_peer_for_scope(peers.into_iter(), scope, now, false))
    }

    pub(crate) fn best_peer_for_scope<I>(
        &self,
        peers: I,
        scope: &SwarmScope,
        now: Instant,
        require_known_scope: bool,
    ) -> Option<PeerId>
    where
        I: Iterator<Item = PeerId>,
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
                left_key
                    .cmp(&right_key)
                    .then_with(|| left.to_string().cmp(&right.to_string()))
            })
    }

    pub(crate) fn peer_is_eligible_for_scope(
        &self,
        peer: &PeerId,
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
                state.inflight_backfills < MAX_INFLIGHT_BACKFILLS_PER_PEER
                    && state.next_retry_at <= now
                    && state.last_backfill_request_at.map_or(true, |last| {
                        now.duration_since(last) >= self.anti_entropy_interval
                    })
            }
            None => !require_known_scope,
        }
    }

    pub(crate) fn record_peer_scope_activity(&mut self, peer: PeerId, scope: &SwarmScope) {
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()))
            .known_scopes
            .insert(scope.clone());
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

    pub(crate) fn mark_peer_connected(&mut self, peer: PeerId) {
        self.connected_peers.insert(peer);
        self.peer_sync_state
            .entry(peer)
            .or_insert_with(|| PeerSyncState::new(Instant::now()));
    }

    pub(crate) fn mark_peer_disconnected(&mut self, peer: PeerId) {
        self.connected_peers.remove(&peer);
        self.peer_sync_state.remove(&peer);
        self.pending_relationship_requests
            .retain(|_, pending| pending.peer != peer);
        self.pending_dm_requests
            .retain(|_, pending| pending.peer != peer);
    }

    pub(crate) fn mark_backfill_completed(&mut self, peer: PeerId) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfills = state.inflight_backfills.saturating_sub(1);
            state.next_retry_at = Instant::now() + self.anti_entropy_interval;
        }
    }

    pub(crate) fn mark_backfill_failed(&mut self, peer: PeerId) {
        if let Some(state) = self.peer_sync_state.get_mut(&peer) {
            state.inflight_backfills = state.inflight_backfills.saturating_sub(1);
            state.next_retry_at = Instant::now() + self.backfill_retry_after;
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

    pub(crate) fn handle_runtime_event(
        &mut self,
        node: &mut Node,
        event: Result<NetworkRuntimeEvent>,
    ) -> Result<NetworkBridgeTick> {
        self.process_runtime_event(node, event?)
    }
}
