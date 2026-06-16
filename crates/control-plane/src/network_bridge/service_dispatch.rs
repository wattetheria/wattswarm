use super::*;

const PEER_RELATIONSHIP_CONTROL_REQUEST_TIMEOUT_MS: i64 = 30_000;
const PEER_RELATIONSHIP_IN_FLIGHT_TIMEOUT_GRACE_MS: i64 = 5_000;

impl NetworkBridgeService {
    pub fn send_peer_relationship_action(
        &mut self,
        remote_node_id: &str,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: Option<RawAgentEnvelope>,
    ) -> Result<PeerRelationshipRequestId> {
        let Some(state_dir) = self.state_dir.clone() else {
            bail!("peer relationship actions require state_dir to be configured");
        };
        let (remote_node_id, peer) =
            self.peer_for_relationship_action(&state_dir, remote_node_id)?;
        let relationship_record = crate::control::apply_peer_relationship_action_state(
            &state_dir,
            &remote_node_id,
            action,
            crate::control::PeerRelationshipInitiator::Local,
        )?;
        let local_node_id = self.local_peer_id().to_string();
        let capability = match action {
            crate::control::PeerRelationshipAction::Request => "peer.relationship.request",
            crate::control::PeerRelationshipAction::Accept => "peer.relationship.accept",
            crate::control::PeerRelationshipAction::Reject => "peer.relationship.reject",
            crate::control::PeerRelationshipAction::Cancel => "peer.relationship.cancel",
            crate::control::PeerRelationshipAction::Remove => "peer.relationship.remove",
            crate::control::PeerRelationshipAction::Block => "peer.relationship.block",
            crate::control::PeerRelationshipAction::Unblock => "peer.relationship.unblock",
        };
        let envelope = agent_envelope.unwrap_or_else(|| {
            default_agent_envelope(
                &local_node_id,
                &remote_node_id,
                capability,
                json!({
                    "action": wire_peer_relationship_action(action),
                    "remote_node_id": remote_node_id.clone(),
                }),
            )
        });
        if action == crate::control::PeerRelationshipAction::Request {
            attach_agent_envelope_to_relationship(&state_dir, &remote_node_id, &envelope)?;
        }
        if action == crate::control::PeerRelationshipAction::Accept
            && relationship_record.relationship_state
                == crate::control::PeerRelationshipState::Accepted
        {
            self.finalize_dm_session_from_relationship(
                &state_dir,
                &remote_node_id,
                crate::control::PeerDmDirection::Outbound,
                &envelope.protocol,
                relationship_record.updated_at,
            )?;
        }
        let contact_material = build_contact_material(&state_dir, &local_node_id)?;
        let pending_agent_envelope = envelope.clone();
        let request_id = self.runtime.send_peer_relationship_request(
            &peer,
            PeerRelationshipRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.clone(),
                action: wire_peer_relationship_action(action),
                agent_envelope: Some(envelope),
                contact_material: Some(contact_material),
            },
        )?;
        self.pending_relationship_requests.insert(
            request_id,
            PendingPeerRelationshipRequest {
                peer,
                remote_node_id,
                action,
                agent_envelope: pending_agent_envelope,
                started_at_ms: observed_at_ms() as i64,
            },
        );
        Ok(request_id)
    }

    pub(super) fn release_stale_peer_relationship_action(
        &mut self,
        remote_node_id: &str,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: &RawAgentEnvelope,
        now_ms: i64,
    ) -> bool {
        let timeout_ms = PEER_RELATIONSHIP_CONTROL_REQUEST_TIMEOUT_MS
            .saturating_add(PEER_RELATIONSHIP_IN_FLIGHT_TIMEOUT_GRACE_MS);
        let stale_request_id =
            self.pending_relationship_requests
                .iter()
                .find_map(|(request_id, pending)| {
                    if pending.remote_node_id == remote_node_id
                        && pending.action == action
                        && pending.agent_envelope.message_json == agent_envelope.message_json
                        && now_ms.saturating_sub(pending.started_at_ms) >= timeout_ms
                    {
                        Some(*request_id)
                    } else {
                        None
                    }
                });
        if let Some(request_id) = stale_request_id {
            if let Some(pending) = self.pending_relationship_requests.remove(&request_id) {
                self.mark_peer_control_stream_failed(pending.peer);
            }
            return true;
        }
        false
    }

    pub(super) fn has_pending_peer_relationship_action(
        &self,
        remote_node_id: &str,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: &RawAgentEnvelope,
    ) -> bool {
        self.pending_relationship_requests.values().any(|pending| {
            pending.remote_node_id == remote_node_id
                && pending.action == action
                && pending.agent_envelope.message_json == agent_envelope.message_json
        })
    }

    pub(crate) fn request_peer_contact_material(
        &mut self,
        remote_node_id: &str,
    ) -> Result<ContactMaterialRequestId> {
        let Some(state_dir) = self.state_dir.clone() else {
            bail!("contact material requests require state_dir to be configured");
        };
        let (remote_node_id, peer) = self.connected_peer_for_operation(
            &state_dir,
            remote_node_id,
            "contact material requests",
        )?;
        let request_id =
            self.send_contact_material_request_to_peer(&remote_node_id, peer.clone())?;
        Ok(request_id)
    }

    pub(crate) fn probe_peer_contact_material(
        &mut self,
        peer: &NetworkNodeId,
    ) -> Result<Option<ContactMaterialRequestId>> {
        if self
            .pending_contact_material_requests
            .values()
            .any(|pending| &pending.peer == peer)
        {
            return Ok(None);
        }
        if !self.runtime.allows_outbound_backfill_to(peer) {
            bail!("missing iroh contact material for {peer}");
        }
        let request_id = self.send_contact_material_request_to_peer(peer.as_str(), peer.clone())?;
        Ok(Some(request_id))
    }

    fn send_contact_material_request_to_peer(
        &mut self,
        remote_node_id: &str,
        peer: NetworkNodeId,
    ) -> Result<ContactMaterialRequestId> {
        let remote_node_id = remote_node_id.to_owned();
        let local_node_id = self.local_peer_id().to_string();
        let request_id = self.runtime.send_contact_material_request(
            &peer,
            RawContactMaterialRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.clone(),
            },
        )?;
        self.pending_contact_material_requests.insert(
            request_id,
            PendingContactMaterialRequest {
                peer,
                remote_node_id,
            },
        );
        Ok(request_id)
    }

    pub(crate) fn maybe_sync_topic_message_content(
        &mut self,
        node: &mut Node,
        event: &crate::types::Event,
        content_source_node_id: Option<&str>,
    ) -> Result<()> {
        let Some(state_dir) = self.state_dir.as_ref() else {
            return Ok(());
        };
        let crate::types::EventPayload::TopicMessagePosted(payload) = &event.payload else {
            return Ok(());
        };
        let remote_node_id = content_source_node_id.unwrap_or(&payload.content_ref.producer);
        record_data_plane_status(
            state_dir,
            "topic_message",
            &event.event_id,
            Some(remote_node_id),
            "iroh_control",
            "control_received",
            None,
        )?;
        let content = match crate::control::fetch_json_content_artifact_via_iroh_with_local_peer_id(
            state_dir,
            &self.local_peer_id(),
            remote_node_id,
            wattswarm_artifact_store::ArtifactKind::TopicMessage,
            &payload.content_ref,
        ) {
            Ok(content) => content,
            Err(err) => {
                let _ = record_data_plane_status(
                    state_dir,
                    "topic_message",
                    &event.event_id,
                    Some(remote_node_id),
                    "iroh_direct",
                    "hydrate_failed",
                    Some(&err.to_string()),
                );
                return Err(err);
            }
        };
        node.store
            .update_topic_message_content(&event.event_id, &content, observed_at_ms())?;
        record_data_plane_status(
            state_dir,
            "topic_message",
            &event.event_id,
            Some(remote_node_id),
            "iroh_direct",
            "content_hydrated",
            None,
        )?;
        Ok(())
    }

    pub(crate) fn maybe_sync_candidate_output(
        &mut self,
        node: &mut Node,
        event: &crate::types::Event,
    ) -> Result<()> {
        let Some(state_dir) = self.state_dir.as_ref() else {
            return Ok(());
        };
        let crate::types::EventPayload::CandidateProposed(payload) = &event.payload else {
            return Ok(());
        };
        if payload.candidate.has_resolved_output()
            || node
                .store
                .get_candidate_by_id(&payload.task_id, &payload.candidate.candidate_id)?
                .is_some_and(|candidate| candidate.has_resolved_output())
        {
            return Ok(());
        }
        record_data_plane_status(
            state_dir,
            "candidate_output",
            &payload.candidate.candidate_id,
            Some(&payload.candidate.output_ref.producer),
            "iroh_control",
            "control_received",
            None,
        )?;
        let content = match crate::control::fetch_json_content_artifact_via_iroh_with_local_peer_id(
            state_dir,
            &self.local_peer_id(),
            &payload.candidate.output_ref.producer,
            wattswarm_artifact_store::ArtifactKind::Reference,
            &payload.candidate.output_ref,
        ) {
            Ok(content) => content,
            Err(err) => {
                let _ = record_data_plane_status(
                    state_dir,
                    "candidate_output",
                    &payload.candidate.candidate_id,
                    Some(&payload.candidate.output_ref.producer),
                    "iroh_direct",
                    "hydrate_failed",
                    Some(&err.to_string()),
                );
                return Err(err);
            }
        };
        node.store.update_candidate_output(
            &payload.task_id,
            &payload.candidate.candidate_id,
            &content,
            observed_at_ms(),
        )?;
        record_data_plane_status(
            state_dir,
            "candidate_output",
            &payload.candidate.candidate_id,
            Some(&payload.candidate.output_ref.producer),
            "iroh_direct",
            "content_hydrated",
            None,
        )?;
        Ok(())
    }

    pub(crate) fn record_dm_session_ready(
        &self,
        state_dir: &Path,
        remote_node_id: &str,
        thread_id: &str,
        direction: crate::control::PeerDmDirection,
        a2a_protocol: &str,
        acknowledged_at: Option<u64>,
    ) -> Result<()> {
        save_dm_message(
            state_dir,
            remote_node_id,
            thread_id,
            &format!("session-init:{thread_id}"),
            crate::control::PeerDmMessageKind::SessionInit,
            direction,
            crate::control::PeerDmDeliveryState::Delivered,
            a2a_protocol,
            None,
            json!({
                "thread_id": thread_id,
                "session_state": "ready",
                "synthetic": true,
            }),
            acknowledged_at,
        )?;
        Ok(())
    }

    pub(crate) fn finalize_dm_session_from_relationship(
        &mut self,
        state_dir: &Path,
        remote_node_id: &str,
        direction: crate::control::PeerDmDirection,
        a2a_protocol: &str,
        established_at: u64,
    ) -> Result<()> {
        let local_node_id = self.local_peer_id().to_string();
        let thread_id = peer_dm_thread_id(&local_node_id, remote_node_id);
        self.ensure_private_dm_group_subscription(
            state_dir,
            &local_node_id,
            remote_node_id,
            established_at,
        )?;
        upsert_dm_thread(
            state_dir,
            remote_node_id,
            &thread_id,
            crate::control::PeerDmSessionState::Ready,
            Some(established_at),
            Some(established_at),
        )?;
        save_dm_message(
            state_dir,
            remote_node_id,
            &thread_id,
            &format!("relationship-established:{thread_id}"),
            crate::control::PeerDmMessageKind::RelationshipEstablished,
            direction,
            crate::control::PeerDmDeliveryState::Delivered,
            a2a_protocol,
            None,
            json!({
                "relationship_state": "accepted",
                "thread_id": thread_id,
                "established_at": established_at,
                "synthetic": true,
            }),
            Some(established_at),
        )?;
        self.record_dm_session_ready(
            state_dir,
            remote_node_id,
            &thread_id,
            direction,
            a2a_protocol,
            Some(established_at),
        )?;
        Ok(())
    }

    fn ensure_private_dm_group_subscription(
        &mut self,
        state_dir: &Path,
        local_node_id: &str,
        remote_node_id: &str,
        subscribed_at: u64,
    ) -> Result<()> {
        let Some(db_path) = self.db_path.as_ref() else {
            return Ok(());
        };
        let mut node = crate::control::open_node(state_dir, db_path)?;
        let scope = SwarmScope::Group(crate::control::private_dm_group_id(
            local_node_id,
            remote_node_id,
        ));
        if !node_has_active_subscription_scope_kinds(
            &node,
            local_node_id,
            &scope,
            &[GossipKind::Messages],
        )? {
            node.emit_at(
                1,
                crate::types::EventPayload::FeedSubscriptionUpdated(
                    crate::types::FeedSubscriptionUpdatedPayload {
                        network_id: current_network_context_id(&node),
                        subscriber_node_id: local_node_id.to_owned(),
                        feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
                        scope_hint: crate::control::private_dm_scope_hint(
                            local_node_id,
                            remote_node_id,
                        ),
                        gossip_kinds: vec!["messages".to_owned()],
                        provider_capabilities: Some(
                            crate::types::TopicProviderCapabilities::local_history_provider(),
                        ),
                        agent_envelope: None,
                        active: true,
                    },
                ),
                subscribed_at,
            )?;
        }
        self.subscribe_scope_kinds(&scope, &[GossipKind::Messages])?;
        Ok(())
    }

    pub(crate) fn ensure_peer_connected(
        &mut self,
        state_dir: &Path,
        peer: &NetworkNodeId,
        remote_node_id: &str,
    ) -> Result<()> {
        if self.connected_peers.contains(peer) && self.peer_recently_seen_at(peer, Instant::now()) {
            return Ok(());
        }
        for addr in candidate_peer_addrs(state_dir, remote_node_id)? {
            match self.dial(addr.clone()) {
                Ok(()) => {}
                Err(err)
                    if err
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate connection") => {}
                Err(err) => {
                    eprintln!("peer reconnect dial failed for {remote_node_id} via {addr}: {err}");
                }
            }
        }
        Ok(())
    }

    fn connected_peer_for_operation(
        &mut self,
        state_dir: &Path,
        remote_node_id: &str,
        operation: &str,
    ) -> Result<(String, NetworkNodeId)> {
        let remote_node_id = remote_node_id.trim();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let peer = remote_node_id
            .parse::<NetworkNodeId>()
            .map_err(|err| anyhow!("parse remote_node_id as iroh node id: {err}"))?;
        if !self.connected_peers.contains(&peer)
            || !self.peer_recently_seen_at(&peer, Instant::now())
        {
            self.ensure_peer_connected(state_dir, &peer, remote_node_id)?;
        }
        if !self.connected_peers.contains(&peer)
            || !self.peer_recently_seen_at(&peer, Instant::now())
        {
            self.schedule_peer_reconnect(peer.clone());
            bail!("{operation} require a connected peer");
        }
        Ok((remote_node_id.to_owned(), peer))
    }

    fn peer_for_relationship_action(
        &mut self,
        state_dir: &Path,
        remote_node_id: &str,
    ) -> Result<(String, NetworkNodeId)> {
        let remote_node_id = remote_node_id.trim();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let peer = remote_node_id
            .parse::<NetworkNodeId>()
            .map_err(|err| anyhow!("parse remote_node_id as iroh node id: {err}"))?;

        if !self.runtime.allows_outbound_backfill_to(&peer) {
            self.ensure_peer_connected(state_dir, &peer, remote_node_id)?;
        }
        if !self.runtime.allows_outbound_backfill_to(&peer) {
            self.schedule_peer_reconnect(peer.clone());
            bail!("peer relationship actions require peer contact material");
        }

        let now = Instant::now();
        if !self.connected_peers.contains(&peer) && !self.peer_recently_seen_at(&peer, now) {
            self.ensure_peer_connected(state_dir, &peer, remote_node_id)?;
        }
        let now = Instant::now();
        if !self.connected_peers.contains(&peer) && !self.peer_recently_seen_at(&peer, now) {
            self.schedule_peer_reconnect(peer.clone());
            bail!("peer relationship actions require a connected or recently seen peer");
        }
        if !self.connected_peers.contains(&peer) {
            self.schedule_peer_reconnect(peer.clone());
        }
        Ok((remote_node_id.to_owned(), peer))
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
            content_source_node_id: None,
        }))
    }

    pub fn publish_chat_for_scope(
        &mut self,
        scope: &SwarmScope,
        event: crate::types::Event,
    ) -> Result<()> {
        if !self.global_publish_rate_guard.allow(scope, &event) {
            bail!("GlobalTopicRateLimited");
        }
        self.runtime.publish_gossip(&chat_gossip(EventEnvelope {
            scope: scope.clone(),
            event,
            content_source_node_id: Some(self.local_peer_id().to_string()),
        }))
    }

    pub fn publish_global_event(&mut self, event: crate::types::Event) -> Result<()> {
        self.publish_event_for_scope(&SwarmScope::Global, event)
    }

    pub fn publish_summary(&mut self, summary: SummaryAnnouncement) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Summary(summary.clone()))?;
        diagnostics::record_diagnostic(
            self.state_dir.as_deref(),
            diagnostics::DiagnosticEvent::new(
                "info",
                "gossip",
                "publish.summary",
                "ok",
                format!("published local summary: {}", summary.summary_kind),
            )
            .object("summary", Some(summary.summary_id.clone()))
            .source_node_id(Some(summary.source_node_id.clone()))
            .scope(&summary.scope)
            .details(Value::Object(summary_diagnostic_details(&summary))),
        );
        Ok(())
    }

    pub fn publish_checkpoint(
        &mut self,
        checkpoint: crate::network_p2p::CheckpointAnnouncement,
    ) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Checkpoint(checkpoint))
    }
}
