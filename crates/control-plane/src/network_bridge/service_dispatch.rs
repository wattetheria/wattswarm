use super::*;

impl NetworkBridgeService {
    pub fn send_peer_relationship_action(
        &mut self,
        remote_node_id: &str,
        action: crate::control::PeerRelationshipAction,
        agent_envelope: Option<RawAgentEnvelope>,
    ) -> Result<PeerRelationshipRequestId> {
        let Some(state_dir) = &self.state_dir else {
            bail!("peer relationship actions require state_dir to be configured");
        };
        let remote_node_id = remote_node_id.trim();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let peer = remote_node_id
            .parse::<NetworkNodeId>()
            .map_err(|err| anyhow!("parse remote_node_id as iroh node id: {err}"))?;
        if !self.connected_peers.contains(&peer) {
            bail!("peer relationship actions require a connected peer");
        }
        let relationship_record = crate::control::apply_peer_relationship_action_state(
            state_dir,
            remote_node_id,
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
                remote_node_id,
                capability,
                json!({
                    "action": wire_peer_relationship_action(action),
                    "remote_node_id": remote_node_id,
                }),
            )
        });
        if action == crate::control::PeerRelationshipAction::Accept
            && relationship_record.relationship_state
                == crate::control::PeerRelationshipState::Accepted
        {
            self.finalize_dm_session_from_relationship(
                state_dir,
                remote_node_id,
                crate::control::PeerDmDirection::Outbound,
                &envelope.protocol,
                relationship_record.updated_at,
            )?;
        }
        let request_id = self.runtime.send_peer_relationship_request(
            &peer,
            PeerRelationshipRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.to_owned(),
                action: wire_peer_relationship_action(action),
                agent_envelope: Some(envelope),
            },
        )?;
        self.pending_relationship_requests.insert(
            request_id,
            PendingPeerRelationshipRequest {
                peer,
                remote_node_id: remote_node_id.to_owned(),
                action,
            },
        );
        Ok(request_id)
    }

    pub(crate) fn request_peer_contact_material(
        &mut self,
        remote_node_id: &str,
    ) -> Result<ContactMaterialRequestId> {
        let Some(state_dir) = self.state_dir.clone() else {
            bail!("contact material requests require state_dir to be configured");
        };
        let peer = remote_node_id
            .parse::<NetworkNodeId>()
            .map_err(|err| anyhow!("parse remote_node_id as iroh node id: {err}"))?;
        self.ensure_peer_connected(&state_dir, &peer, remote_node_id)?;
        if !self.connected_peers.contains(&peer) {
            bail!("contact material requests require a connected peer");
        }
        let local_node_id = self.local_peer_id().to_string();
        let request_id = self.runtime.send_contact_material_request(
            &peer,
            RawContactMaterialRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.to_owned(),
            },
        )?;
        self.pending_contact_material_requests.insert(
            request_id,
            PendingContactMaterialRequest {
                peer,
                remote_node_id: remote_node_id.to_owned(),
            },
        );
        Ok(request_id)
    }

    pub fn send_peer_direct_message(
        &mut self,
        remote_node_id: &str,
        agent_envelope: Option<RawAgentEnvelope>,
        content: Value,
    ) -> Result<PeerDirectMessageRequestId> {
        let Some(state_dir) = self.state_dir.clone() else {
            bail!("peer direct messages require state_dir to be configured");
        };
        if relationship_state_for(&state_dir, remote_node_id)?
            != Some(crate::control::PeerRelationshipState::Accepted)
        {
            bail!("peer direct messages require an accepted relationship");
        }
        let remote_node_id = remote_node_id.trim();
        if remote_node_id.is_empty() {
            bail!("remote_node_id is required");
        }
        let peer = remote_node_id
            .parse::<NetworkNodeId>()
            .map_err(|err| anyhow!("parse remote_node_id as iroh node id: {err}"))?;
        if !self.connected_peers.contains(&peer) {
            self.ensure_peer_connected(&state_dir, &peer, remote_node_id)?;
        }
        if !self.connected_peers.contains(&peer) {
            bail!("peer direct messages require a connected peer");
        }
        let local_node_id = self.local_peer_id().to_string();
        let thread_id = peer_dm_thread_id(&local_node_id, remote_node_id);
        let thread = crate::control::load_peer_dm_thread_records_state(&state_dir)?
            .into_iter()
            .find(|record| record.thread_id == thread_id)
            .ok_or_else(|| anyhow!("peer direct messages require an established session"))?;
        if thread.session_state != crate::control::PeerDmSessionState::Ready {
            bail!("peer direct messages require a ready session");
        }
        let message_id = Uuid::new_v4().to_string();
        let content_ref = crate::control::materialize_json_content_artifact(
            &state_dir,
            wattswarm_artifact_store::ArtifactKind::DirectMessage,
            &local_node_id,
            &content,
            observed_at_ms(),
        )?;
        let envelope = agent_envelope.unwrap_or_else(|| {
            default_agent_envelope(
                &local_node_id,
                remote_node_id,
                "peer.dm.message",
                content.clone(),
            )
        });
        let a2a_protocol = envelope.protocol.clone();
        save_dm_message(
            &state_dir,
            remote_node_id,
            &thread_id,
            &message_id,
            crate::control::PeerDmMessageKind::Message,
            crate::control::PeerDmDirection::Outbound,
            crate::control::PeerDmDeliveryState::Pending,
            &envelope.protocol,
            Some(&envelope),
            content.clone(),
            None,
        )?;
        record_data_plane_status(
            &state_dir,
            "dm_message",
            &message_id,
            Some(remote_node_id),
            "iroh_direct",
            "content_materialized",
            None,
        )?;
        let request_id = self.runtime.send_peer_direct_message_request(
            &peer,
            PeerDirectMessageRequest {
                source_node_id: local_node_id,
                target_node_id: remote_node_id.to_owned(),
                thread_id: thread_id.clone(),
                message_id: message_id.clone(),
                kind: RawPeerDirectMessageKind::Message,
                agent_envelope: Some(envelope),
                contact_material: None,
                content_ref: Some(content_ref),
                control_json: None,
            },
        )?;
        record_data_plane_status(
            &state_dir,
            "dm_message",
            &message_id,
            Some(remote_node_id),
            "iroh_control",
            "control_sent",
            None,
        )?;
        self.pending_dm_requests.insert(
            request_id,
            PendingPeerDirectMessageRequest {
                peer,
                remote_node_id: remote_node_id.to_owned(),
                thread_id,
                message_id,
                kind: crate::control::PeerDmMessageKind::Message,
                a2a_protocol,
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
        &self,
        state_dir: &Path,
        remote_node_id: &str,
        direction: crate::control::PeerDmDirection,
        a2a_protocol: &str,
        established_at: u64,
    ) -> Result<()> {
        let local_node_id = self.local_peer_id().to_string();
        let thread_id = peer_dm_thread_id(&local_node_id, remote_node_id);
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

    pub(crate) fn ensure_peer_connected(
        &mut self,
        state_dir: &Path,
        peer: &NetworkNodeId,
        remote_node_id: &str,
    ) -> Result<()> {
        if self.connected_peers.contains(peer) {
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
            .publish_gossip(&GossipMessage::Summary(summary))
    }

    pub fn publish_peer_discovery(&mut self, discovery: PeerDiscoveryAnnouncement) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Discovery(discovery))
    }

    pub fn publish_checkpoint(
        &mut self,
        checkpoint: crate::network_p2p::CheckpointAnnouncement,
    ) -> Result<()> {
        self.runtime
            .publish_gossip(&GossipMessage::Checkpoint(checkpoint))
    }
}
