use super::*;

fn should_record_backfill_response_diagnostic(events_applied: usize) -> bool {
    events_applied > 0
}

fn is_missing_iroh_contact_material_error(error: &str) -> bool {
    error.starts_with("missing iroh contact material for ")
}

fn peer_metadata_needs_private_message_contact_material(
    state_dir: &Path,
    peer: &NetworkNodeId,
) -> bool {
    crate::control::load_peer_metadata_records_state(state_dir)
        .ok()
        .and_then(|records| {
            records
                .into_iter()
                .find(|record| record.node_id == peer.to_string())
        })
        .is_some_and(|record| record.private_message_public_key_b64().is_none())
}

impl NetworkBridgeService {
    fn save_inbound_private_dm_topic_if_applicable(
        &self,
        node: &Node,
        event: &crate::types::Event,
        payload: &crate::types::TopicMessagePostedPayload,
    ) {
        if payload.feed_key != crate::control::PRIVATE_DM_FEED_KEY {
            return;
        }
        let Some(state_dir) = &self.state_dir else {
            return;
        };
        match node.store.get_topic_message(&event.event_id) {
            Ok(Some(topic_message)) => {
                match save_inbound_private_dm_topic_message(
                    state_dir,
                    &node.node_id(),
                    &event.author_node_id,
                    &event.event_id,
                    &topic_message.content,
                    event.created_at,
                ) {
                    Ok(Some(projection)) => {
                        let _ = node.store.update_topic_message_content(
                            &event.event_id,
                            &projection.topic_content,
                            event.created_at,
                        );
                    }
                    Ok(None) => {}
                    Err(err) => {
                        eprintln!(
                            "inbound private dm projection failed for {}: {err}",
                            event.event_id
                        );
                    }
                }
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!(
                    "inbound private dm content lookup failed for {}: {err}",
                    event.event_id
                );
            }
        }
    }

    fn decrypt_private_hive_topic_if_applicable(
        &self,
        node: &Node,
        event: &crate::types::Event,
        payload: &crate::types::TopicMessagePostedPayload,
    ) {
        if !crate::control::is_private_hive_route(&payload.feed_key, &payload.scope_hint) {
            return;
        }
        let Some(state_dir) = &self.state_dir else {
            return;
        };
        let Ok(Some(topic_message)) = node.store.get_topic_message(&event.event_id) else {
            return;
        };
        if topic_message.content.get("kind").and_then(Value::as_str) != Some("private_encrypted")
            || topic_message
                .content
                .get("private_kind")
                .and_then(Value::as_str)
                != Some("hive_message")
        {
            return;
        }
        let Some(message_id) = topic_message
            .content
            .get("message_id")
            .and_then(Value::as_str)
        else {
            return;
        };
        let Ok(Some(key)) = crate::control::find_private_hive_key_record_state(
            state_dir,
            &payload.feed_key,
            &payload.scope_hint,
        ) else {
            return;
        };
        let Some(encrypted) = topic_message.content.get("encrypted") else {
            return;
        };
        let Ok(encrypted) = serde_json::from_value::<crate::crypto::PrivateGroupEncryptedPayload>(
            encrypted.clone(),
        ) else {
            return;
        };
        let Ok(plaintext) = crate::crypto::decrypt_private_group_content(
            &key.shared_secret_b64,
            &encrypted,
            &crate::control::private_hive_encryption_aad(
                &payload.feed_key,
                &payload.scope_hint,
                message_id,
            ),
        ) else {
            return;
        };
        let Ok(content) = serde_json::from_slice::<Value>(&plaintext) else {
            return;
        };
        let _ =
            node.store
                .update_topic_message_content(&event.event_id, &content, event.created_at);
    }

    fn deliver_task_lifecycle_agent_event(&self, node: &Node, event: &crate::types::Event) {
        let Some(state_dir) = &self.state_dir else {
            return;
        };
        let local_node_id = node.node_id();
        if !EventRelevanceFilter::should_deliver(node, state_dir, &local_node_id, event) {
            return;
        }
        match &event.payload {
            crate::types::EventPayload::TaskClaimed(payload) => {
                if let Ok(agent_event) = task_claim_agent_event(node, event, payload) {
                    let _ = deliver_agent_event_to_local_executor(
                        state_dir,
                        self.db_path.as_deref(),
                        &agent_event,
                    );
                }
            }
            crate::types::EventPayload::TaskClaimDecided(payload) => {
                if let Ok(agent_event) = task_claim_decision_agent_event(node, event, payload) {
                    let _ = deliver_agent_event_to_local_executor(
                        state_dir,
                        self.db_path.as_deref(),
                        &agent_event,
                    );
                }
            }
            crate::types::EventPayload::CandidateProposed(_)
            | crate::types::EventPayload::TaskCompleted(_)
            | crate::types::EventPayload::DecisionFinalized(_)
            | crate::types::EventPayload::TaskError(_)
            | crate::types::EventPayload::TaskRetryScheduled(_) => {
                if let Ok(Some(agent_event)) = task_result_agent_event(node, event) {
                    let _ = deliver_agent_event_to_local_executor(
                        state_dir,
                        self.db_path.as_deref(),
                        &agent_event,
                    );
                }
            }
            crate::types::EventPayload::TaskCompletionDecided(payload) => {
                if let Ok(agent_event) = task_completion_decision_agent_event(node, event, payload)
                {
                    let _ = deliver_agent_event_to_local_executor(
                        state_dir,
                        self.db_path.as_deref(),
                        &agent_event,
                    );
                }
            }
            crate::types::EventPayload::TaskSettled(payload) => {
                if let Ok(agent_event) = task_settled_agent_event(node, event, payload) {
                    let _ = deliver_agent_event_to_local_executor(
                        state_dir,
                        self.db_path.as_deref(),
                        &agent_event,
                    );
                }
            }
            _ => {}
        }
    }

    fn deliver_topic_message_agent_event(
        &self,
        node: &Node,
        event: &crate::types::Event,
        payload: &crate::types::TopicMessagePostedPayload,
    ) {
        if event.author_node_id == node.node_id() {
            return;
        }
        let Some(state_dir) = &self.state_dir else {
            return;
        };
        let local_node_id = node.node_id();
        if !EventRelevanceFilter::should_deliver(node, state_dir, &local_node_id, event) {
            return;
        }
        if let Ok(Some(agent_event)) = topic_message_agent_event(node, event, payload) {
            let _ = deliver_agent_event_to_local_executor(
                state_dir,
                self.db_path.as_deref(),
                &agent_event,
            );
        }
    }

    fn save_agent_payment_event_if_applicable(&self, node: &Node, event: &crate::types::Event) {
        if event.author_node_id == node.node_id() {
            return;
        }
        let crate::types::EventPayload::AgentPaymentPosted(payload) = &event.payload else {
            return;
        };
        let Some(state_dir) = &self.state_dir else {
            return;
        };
        if let Err(err) =
            save_agent_payment_event(state_dir, &event.author_node_id, &event.event_id, payload)
        {
            eprintln!(
                "agent payment event projection failed for {}: {err}",
                event.event_id
            );
        }
    }

    pub(crate) fn process_runtime_event(
        &mut self,
        node: &mut Node,
        event: NetworkRuntimeEvent,
    ) -> Result<NetworkBridgeTick> {
        match event {
            NetworkRuntimeEvent::PeerDiscovered {
                peer,
                address,
                source,
            } => {
                if let Some(state_dir) = &self.state_dir {
                    let _ = crate::control::add_discovered_peer_endpoint_with_source(
                        state_dir,
                        &peer.to_string(),
                        Some(&address.to_string()),
                        source.as_str(),
                    );
                }
                self.remember_peer_address(peer.clone(), address.clone());
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "peer_discovered peer={peer} source={} address={address}",
                        source.as_str()
                    ),
                })
            }
            NetworkRuntimeEvent::PeerMetadataObserved { peer, metadata } => {
                let mut missing_contact_material = false;
                if let Some(state_dir) = &self.state_dir {
                    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    let existing = crate::control::load_peer_metadata_records_state(state_dir)
                        .ok()
                        .and_then(|records| {
                            records
                                .into_iter()
                                .find(|record| record.node_id == peer.to_string())
                        });
                    let record = crate::control::PeerMetadataRecord {
                        node_id: peer.to_string(),
                        network_id: Some(metadata.network_id.clone()),
                        params_version: Some(metadata.params_version),
                        params_hash: Some(metadata.params_hash.clone()),
                        agent_version_raw: Some(metadata.agent_version_raw.clone()),
                        agent_version_prefix: Some(metadata.agent_version_prefix.clone()),
                        protocol_version: Some(metadata.protocol_version.clone()),
                        observed_addr: Some(metadata.observed_addr.clone()),
                        listen_addrs: metadata.listen_addrs.clone(),
                        protocols: metadata.protocols.clone(),
                        handshake_status: "identified".to_owned(),
                        last_error: None,
                        contact_material: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material.clone()),
                        contact_material_signature: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_signature.clone()),
                        contact_material_updated_at: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_updated_at),
                        first_identified_at: existing
                            .as_ref()
                            .map_or(now, |entry| entry.first_identified_at),
                        last_identified_at: now,
                    };
                    missing_contact_material = record.private_message_public_key_b64().is_none();
                    let _ = crate::control::save_peer_metadata_record_state(state_dir, &record);
                }
                if missing_contact_material
                    && let Err(err) = self.request_peer_contact_material(&peer.to_string())
                {
                    eprintln!("contact material request failed for {peer}: {err}");
                }
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "peer_identified peer={peer} network_id={} params_version={}",
                        metadata.network_id, metadata.params_version
                    ),
                })
            }
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
            NetworkRuntimeEvent::DirectUpgradeConnectionUpgradeSucceeded { remote_peer } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("direct_upgrade_upgrade_succeeded remote_peer={remote_peer}"),
                })
            }
            NetworkRuntimeEvent::DirectUpgradeConnectionUpgradeFailed { remote_peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "direct_upgrade_upgrade_failed remote_peer={remote_peer} error={error}"
                    ),
                })
            }
            NetworkRuntimeEvent::ConnectionEstablished { peer, remote_addr } => {
                if !self.known_peer_addrs.contains_key(&peer)
                    && remote_addr.as_str() != peer.as_str()
                {
                    self.remember_peer_address(peer.clone(), remote_addr.clone());
                }
                let newly_connected = self.mark_peer_connected(peer.clone());
                if let Some(state_dir) = &self.state_dir {
                    let remote_addr_text = remote_addr.to_string();
                    let _ = crate::control::add_discovered_peer_endpoint_with_source(
                        state_dir,
                        &peer.to_string(),
                        None,
                        "connected",
                    );
                    if newly_connected {
                        diagnostics::record_diagnostic(
                            Some(state_dir),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "transport",
                                "connection.established",
                                "ok",
                                format!("iroh connection established: {peer}"),
                            )
                            .object("peer", Some(peer.to_string()))
                            .source_node_id(Some(peer.to_string()))
                            .details(json!({
                                "remote_addr": remote_addr_text,
                            })),
                        )
                    }
                }
                if let Some(state_dir) = self.state_dir.clone()
                    && let Err(err) = self.ensure_peer_relay_contact_material(&state_dir, &peer)
                {
                    eprintln!("relay contact material bootstrap failed for {peer}: {err}");
                }
                if !newly_connected {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!("iroh topic neighbor already connected: {peer}"),
                    });
                }
                if self.state_dir.is_some()
                    && let Err(err) = self.request_peer_contact_material(&peer.to_string())
                {
                    eprintln!("contact material request failed for {peer}: {err}");
                }
                let _ = self.request_backfill_for_peer_now(&peer, node)?;
                if !node.store.is_node_penalized(&node.node_id())?
                    && !node.store.is_node_network_banned(&node.node_id())?
                {
                    for entry in node
                        .store
                        .list_local_reputation_snapshots(self.summary_reputation_limit)?
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
            NetworkRuntimeEvent::PeerHandshakeRejected { peer, detail } => {
                self.mark_peer_disconnected(peer.clone());
                if let Some(state_dir) = &self.state_dir {
                    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    let existing = crate::control::load_peer_metadata_records_state(state_dir)
                        .ok()
                        .and_then(|records| {
                            records
                                .into_iter()
                                .find(|record| record.node_id == peer.to_string())
                        });
                    let record = crate::control::PeerMetadataRecord {
                        node_id: peer.to_string(),
                        network_id: existing.as_ref().and_then(|entry| entry.network_id.clone()),
                        params_version: existing.as_ref().and_then(|entry| entry.params_version),
                        params_hash: existing
                            .as_ref()
                            .and_then(|entry| entry.params_hash.clone()),
                        agent_version_raw: existing
                            .as_ref()
                            .and_then(|entry| entry.agent_version_raw.clone()),
                        agent_version_prefix: existing
                            .as_ref()
                            .and_then(|entry| entry.agent_version_prefix.clone()),
                        protocol_version: existing
                            .as_ref()
                            .and_then(|entry| entry.protocol_version.clone()),
                        observed_addr: existing
                            .as_ref()
                            .and_then(|entry| entry.observed_addr.clone()),
                        listen_addrs: existing
                            .as_ref()
                            .map_or_else(Vec::new, |entry| entry.listen_addrs.clone()),
                        protocols: existing
                            .as_ref()
                            .map_or_else(Vec::new, |entry| entry.protocols.clone()),
                        handshake_status: "rejected".to_owned(),
                        last_error: Some(detail.clone()),
                        contact_material: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material.clone()),
                        contact_material_signature: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_signature.clone()),
                        contact_material_updated_at: existing
                            .as_ref()
                            .and_then(|entry| entry.contact_material_updated_at),
                        first_identified_at: existing
                            .as_ref()
                            .map_or(now, |entry| entry.first_identified_at),
                        last_identified_at: now,
                    };
                    let _ = crate::control::save_peer_metadata_record_state(state_dir, &record);
                    diagnostics::record_diagnostic(
                        Some(state_dir),
                        diagnostics::DiagnosticEvent::new(
                            "warn",
                            "transport",
                            "handshake.rejected",
                            "rejected",
                            format!("peer handshake rejected: {peer}"),
                        )
                        .object("peer", Some(peer.to_string()))
                        .source_node_id(Some(peer.to_string()))
                        .details(json!({
                            "detail": detail,
                        })),
                    );
                }
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("peer_handshake_rejected peer={peer} {detail}"),
                })
            }
            NetworkRuntimeEvent::ConnectionClosed {
                peer,
                remaining_established,
            } => {
                let disconnected = remaining_established == 0;
                let was_connected = disconnected && self.mark_peer_disconnected(peer.clone());
                if disconnected {
                    self.schedule_peer_reconnect(peer.clone());
                }
                let should_record_diagnostic = !disconnected || was_connected;
                if should_record_diagnostic && let Some(state_dir) = &self.state_dir {
                    diagnostics::record_diagnostic(
                        Some(state_dir),
                        diagnostics::DiagnosticEvent::new(
                            "info",
                            "transport",
                            "connection.closed",
                            if disconnected { "closed" } else { "remaining" },
                            format!("iroh connection closed: {peer}"),
                        )
                        .object("peer", Some(peer.to_string()))
                        .source_node_id(Some(peer.to_string()))
                        .details(json!({
                            "remaining_established": remaining_established,
                        })),
                    );
                }
                if disconnected && was_connected {
                    Ok(NetworkBridgeTick::Disconnected { peer })
                } else if disconnected {
                    Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!("connection_closed_duplicate peer={peer}"),
                    })
                } else {
                    Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "connection_closed peer={peer} remaining_established={remaining_established}"
                        ),
                    })
                }
            }
            NetworkRuntimeEvent::Gossip {
                propagation_source,
                message,
            } => {
                self.connected_peers.insert(propagation_source.clone());
                if let Some(state_dir) = self.state_dir.clone()
                    && let Err(err) =
                        self.ensure_peer_relay_contact_material(&state_dir, &propagation_source)
                {
                    eprintln!(
                        "relay contact material bootstrap failed for {propagation_source}: {err}"
                    );
                }
                if let Some(state_dir) = self.state_dir.clone()
                    && peer_metadata_needs_private_message_contact_material(
                        &state_dir,
                        &propagation_source,
                    )
                    && let Err(err) = self.probe_peer_contact_material(&propagation_source)
                {
                    eprintln!("contact material probe failed for {propagation_source}: {err}");
                }
                match message {
                    GossipMessage::Event(envelope) => {
                        if !event_matches_signed_scope(&envelope.event, &envelope.scope) {
                            return Ok(NetworkBridgeTick::TransportNotice {
                                detail: format!(
                                    "gossip_event_dropped peer={propagation_source} reason=signed_scope_mismatch event={}",
                                    envelope.event.event_id
                                ),
                            });
                        }
                        self.record_peer_scope_activity(
                            propagation_source.clone(),
                            &envelope.scope,
                        );
                        let ingested_event = ingest_event_envelope(node, &envelope)?;
                        diagnostics::record_diagnostic(
                            self.state_dir.as_deref(),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "gossip",
                                "event.ingest.live",
                                "ok",
                                format!(
                                    "live gossip event ingested: {:?}",
                                    ingested_event.event_kind
                                ),
                            )
                            .event_id(ingested_event.event_id.clone())
                            .object("task", ingested_event.task_id.clone())
                            .source_node_id(Some(propagation_source.to_string()))
                            .scope(&envelope.scope)
                            .details(Value::Object(event_diagnostic_details(&ingested_event))),
                        );
                        if let Err(err) = self.maybe_sync_candidate_output(node, &ingested_event) {
                            eprintln!(
                                "candidate output sync failed for {}: {err}",
                                ingested_event.event_id
                            );
                        }
                        if let Some(state_dir) = &self.state_dir {
                            log_run_queue_events_if_applicable(node, state_dir, &ingested_event);
                        }
                        self.deliver_task_lifecycle_agent_event(node, &ingested_event);
                        self.save_agent_payment_event_if_applicable(node, &ingested_event);
                        self.record_scope_event_ingested(&envelope.scope);
                        Ok(NetworkBridgeTick::EventIngested {
                            peer: propagation_source,
                            event_id: ingested_event.event_id.clone(),
                        })
                    }
                    GossipMessage::Chat(envelope) => {
                        self.record_peer_scope_activity(
                            propagation_source.clone(),
                            &envelope.scope,
                        );
                        let ingested_event = ingest_event_envelope(node, &envelope)?;
                        diagnostics::record_diagnostic(
                            self.state_dir.as_deref(),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "gossip",
                                "chat.ingest.live",
                                "ok",
                                format!(
                                    "live chat event ingested: {:?}",
                                    ingested_event.event_kind
                                ),
                            )
                            .event_id(ingested_event.event_id.clone())
                            .object("event", Some(ingested_event.event_id.clone()))
                            .source_node_id(Some(propagation_source.to_string()))
                            .scope(&envelope.scope)
                            .details(Value::Object(event_diagnostic_details(&ingested_event))),
                        );
                        if let Err(err) = self.maybe_sync_candidate_output(node, &ingested_event) {
                            eprintln!(
                                "candidate output sync failed for {}: {err}",
                                ingested_event.event_id
                            );
                        }
                        if let Err(err) = self.maybe_sync_topic_message_content(
                            node,
                            &ingested_event,
                            envelope.content_source_node_id.as_deref(),
                        ) {
                            eprintln!(
                                "topic content sync failed for {}: {err}",
                                ingested_event.event_id
                            );
                        }
                        if let crate::types::EventPayload::TopicMessagePosted(payload) =
                            &ingested_event.payload
                        {
                            maybe_record_topic_cursor_for_event_id(
                                node,
                                &node.node_id(),
                                &payload.feed_key,
                                &envelope.scope,
                                &ingested_event.event_id,
                                ingested_event.created_at,
                            )?;
                            self.save_inbound_private_dm_topic_if_applicable(
                                node,
                                &ingested_event,
                                payload,
                            );
                            self.decrypt_private_hive_topic_if_applicable(
                                node,
                                &ingested_event,
                                payload,
                            );
                            self.deliver_topic_message_agent_event(node, &ingested_event, payload);
                        }
                        self.record_scope_event_ingested(&envelope.scope);
                        Ok(NetworkBridgeTick::EventIngested {
                            peer: propagation_source,
                            event_id: ingested_event.event_id.clone(),
                        })
                    }
                    GossipMessage::Summary(summary) => {
                        self.record_peer_scope_activity(propagation_source.clone(), &summary.scope);
                        apply_summary_announcement(node, &summary)?;
                        diagnostics::record_diagnostic(
                            self.state_dir.as_deref(),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "gossip",
                                "summary.ingest.live",
                                "ok",
                                format!("live summary ingested: {}", summary.summary_kind),
                            )
                            .object("summary", Some(summary.summary_id.clone()))
                            .source_node_id(Some(propagation_source.to_string()))
                            .scope(&summary.scope)
                            .details(Value::Object(summary_diagnostic_details(&summary))),
                        );
                        if summary.summary_kind == AGENT_PAYMENT_SUMMARY_KIND
                            && let Some(state_dir) = &self.state_dir
                        {
                            let _ = save_agent_payment_summary(
                                state_dir,
                                &summary.source_node_id,
                                &summary,
                            );
                        }
                        self.record_scope_summary_applied(&summary.scope);
                        Ok(NetworkBridgeTick::SummaryApplied {
                            peer: propagation_source,
                            summary_kind: summary.summary_kind,
                        })
                    }
                    GossipMessage::Rule(rule) => {
                        self.record_peer_scope_activity(propagation_source.clone(), &rule.scope);
                        apply_rule_announcement(node, &rule)?;
                        self.record_scope_rule_applied(&rule.scope);
                        Ok(NetworkBridgeTick::RuleApplied {
                            peer: propagation_source,
                            rule_set: rule.rule_set,
                            rule_version: rule.rule_version,
                        })
                    }
                    GossipMessage::Checkpoint(checkpoint) => {
                        self.record_peer_scope_activity(
                            propagation_source.clone(),
                            &checkpoint.scope,
                        );
                        apply_checkpoint_announcement(node, &checkpoint)?;
                        if let Some(state_dir) = &self.state_dir {
                            let _ = crate::control::save_data_source_binding_record_state(
                                state_dir,
                                &crate::control::DataSourceBindingRecord {
                                    binding_kind: crate::control::DataSourceBindingKind::Checkpoint,
                                    binding_scope: Some(checkpoint.scope.label()?),
                                    binding_key: checkpoint.checkpoint_id.clone(),
                                    source_node_id: propagation_source.to_string(),
                                    source_uri: Some(checkpoint.artifact_path.clone()),
                                    updated_at: observed_at_ms(),
                                },
                            );
                        }
                        self.record_scope_checkpoint_applied(&checkpoint.scope);
                        Ok(NetworkBridgeTick::CheckpointApplied {
                            peer: propagation_source,
                            checkpoint_id: checkpoint.checkpoint_id,
                        })
                    }
                }
            }
            NetworkRuntimeEvent::BackfillRequest {
                peer,
                request,
                request_id,
            } => {
                let response = if self.inbound_backfill_authorized(&peer, &request) {
                    backfill_response_for_request(
                        node,
                        &self.local_peer_id().to_string(),
                        &request,
                        self.runtime.config().max_backfill_events,
                        self.runtime.config().max_backfill_events_hard_limit,
                    )?
                } else {
                    crate::network_p2p::BackfillResponse {
                        scope: request.scope.clone(),
                        next_from_event_seq: request.from_event_seq,
                        feed_key: request.feed_key.clone(),
                        head_event_ids: Vec::new(),
                        events: Vec::new(),
                    }
                };
                let events = response.events.len();
                match self.runtime.send_backfill_response(request_id, response) {
                    Ok(()) => Ok(NetworkBridgeTick::BackfillServed { peer, events }),
                    Err(error)
                        if error
                            .to_string()
                            .contains("backfill response channel closed") =>
                    {
                        Ok(NetworkBridgeTick::TransportNotice {
                            detail: format!("backfill_response_dropped peer={peer} reason={error}"),
                        })
                    }
                    Err(error) => Err(error),
                }
            }
            NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response,
            } => {
                self.record_peer_scope_activity(peer.clone(), &response.scope);
                self.mark_backfill_completed(peer.clone(), request_id);
                self.record_peer_remote_head_event_ids(
                    peer.clone(),
                    &response.scope,
                    response.feed_key.as_deref(),
                    &response.head_event_ids,
                );
                let events = ingest_backfill_response(node, &response)?;
                let mut unknown_empty_head = false;
                if response.events.is_empty() {
                    for event_id in &response.head_event_ids {
                        if node.store.event_seq_for_event_id(event_id)?.is_none() {
                            unknown_empty_head = true;
                            break;
                        }
                    }
                }
                if unknown_empty_head {
                    self.reset_peer_backfill_cursor(
                        peer.clone(),
                        &response.scope,
                        response.feed_key.as_deref(),
                    );
                } else {
                    self.record_peer_backfill_cursor(
                        peer.clone(),
                        &response.scope,
                        response.feed_key.as_deref(),
                        response.next_from_event_seq,
                    );
                }
                if should_record_backfill_response_diagnostic(events) {
                    diagnostics::record_diagnostic(
                        self.state_dir.as_deref(),
                        diagnostics::DiagnosticEvent::new(
                            "info",
                            "backfill",
                            "response.ingest",
                            "ok",
                            format!("backfill response ingested {events} events"),
                        )
                        .source_node_id(Some(peer.to_string()))
                        .scope(&response.scope)
                        .details(json!({
                            "request_id": request_id.to_string(),
                            "events_applied": events,
                            "events_received": response.events.len(),
                            "feed_key": response.feed_key,
                            "head_event_ids": response.head_event_ids,
                            "unknown_empty_head": unknown_empty_head,
                            "next_from_event_seq": response.next_from_event_seq,
                        })),
                    );
                }
                for envelope in &response.events {
                    if !event_matches_signed_scope(&envelope.event, &response.scope) {
                        continue;
                    }
                    diagnostics::record_diagnostic(
                        self.state_dir.as_deref(),
                        diagnostics::DiagnosticEvent::new(
                            "info",
                            "backfill",
                            "event.ingest.backfill",
                            "ok",
                            format!("backfill event observed: {:?}", envelope.event.event_kind),
                        )
                        .event_id(envelope.event.event_id.clone())
                        .object("task", envelope.event.task_id.clone())
                        .source_node_id(Some(peer.to_string()))
                        .scope(&response.scope)
                        .details(Value::Object(event_diagnostic_details(&envelope.event))),
                    );
                    if let Err(err) = self.maybe_sync_candidate_output(node, &envelope.event) {
                        eprintln!(
                            "candidate output sync failed for {}: {err}",
                            envelope.event.event_id
                        );
                    }
                    self.deliver_task_lifecycle_agent_event(node, &envelope.event);
                    self.save_agent_payment_event_if_applicable(node, &envelope.event);
                    if let Err(err) = self.maybe_sync_topic_message_content(
                        node,
                        &envelope.event,
                        envelope.content_source_node_id.as_deref(),
                    ) {
                        eprintln!(
                            "topic content sync failed for {}: {err}",
                            envelope.event.event_id
                        );
                    }
                    if let crate::types::EventPayload::TopicMessagePosted(payload) =
                        &envelope.event.payload
                    {
                        self.save_inbound_private_dm_topic_if_applicable(
                            node,
                            &envelope.event,
                            payload,
                        );
                        self.decrypt_private_hive_topic_if_applicable(
                            node,
                            &envelope.event,
                            payload,
                        );
                        self.deliver_topic_message_agent_event(node, &envelope.event, payload);
                    }
                }
                maybe_record_topic_cursor_for_response(
                    node,
                    &node.node_id(),
                    &response,
                    observed_at_ms(),
                )?;
                self.record_scope_backfill_applied(&response.scope, events);
                Ok(NetworkBridgeTick::BackfillApplied {
                    peer,
                    request_id,
                    events,
                })
            }
            NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                self.mark_backfill_failed(peer.clone(), request_id);
                self.mark_peer_control_stream_failed(peer.clone());
                if is_missing_iroh_contact_material_error(&error) {
                    if self.state_dir.is_some()
                        && let Err(err) = self.request_peer_contact_material(&peer.to_string())
                    {
                        eprintln!("contact material request failed for {peer}: {err}");
                    }
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "backfill_waiting_for_contact_material peer={peer} request_id={request_id:?}"
                        ),
                    });
                }
                Ok(NetworkBridgeTick::BackfillFailed {
                    peer,
                    request_id,
                    error,
                })
            }
            NetworkRuntimeEvent::BackfillInboundFailure { peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("backfill_inbound_failure peer={peer} error={error}"),
                })
            }
            NetworkRuntimeEvent::ContactMaterialRequest {
                peer,
                request,
                request_id,
            } => {
                let local_node_id = self.local_peer_id().to_string();
                let now = observed_at_ms();
                let response = if request.source_node_id != peer.to_string() {
                    RawContactMaterialResponse {
                        source_node_id: local_node_id,
                        target_node_id: request.source_node_id,
                        applied: false,
                        contact_material: None,
                        detail: Some("contact material request source_node_id mismatch".to_owned()),
                        updated_at: now,
                    }
                } else if let Some(state_dir) = self.state_dir.as_ref() {
                    RawContactMaterialResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id,
                        applied: true,
                        contact_material: Some(build_contact_material(
                            state_dir,
                            &self.local_peer_id().to_string(),
                        )?),
                        detail: None,
                        updated_at: now,
                    }
                } else {
                    RawContactMaterialResponse {
                        source_node_id: self.local_peer_id().to_string(),
                        target_node_id: request.source_node_id,
                        applied: false,
                        contact_material: None,
                        detail: Some("state_dir is not configured".to_owned()),
                        updated_at: now,
                    }
                };
                self.runtime
                    .send_contact_material_response(request_id, response)?;
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("contact_material_served peer={peer}"),
                })
            }
            NetworkRuntimeEvent::ContactMaterialResponse {
                peer,
                request_id,
                response,
            } => {
                let Some(pending) = self.pending_contact_material_requests.remove(&request_id)
                else {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "contact_material_response_without_pending peer={peer} request_id={request_id:?}"
                        ),
                    });
                };
                if pending.peer != peer
                    || response.source_node_id != pending.remote_node_id
                    || response.target_node_id != self.local_peer_id().to_string()
                {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "contact_material_response_mismatch peer={peer} remote={} source={} target={}",
                            pending.remote_node_id,
                            response.source_node_id,
                            response.target_node_id
                        ),
                    });
                }
                if !response.applied {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "contact_material_rejected peer={peer} detail={}",
                            response.detail.unwrap_or_else(|| "unknown".to_owned())
                        ),
                    });
                }
                if let (Some(state_dir), Some(contact_material)) =
                    (self.state_dir.as_ref(), response.contact_material.as_ref())
                {
                    upsert_contact_material_for_peer(
                        state_dir,
                        &pending.remote_node_id,
                        contact_material,
                    )?;
                }
                let newly_connected = self.mark_peer_connected(peer.clone());
                if let Some(state_dir) = &self.state_dir {
                    let _ = crate::control::add_discovered_peer_endpoint_with_source(
                        state_dir,
                        &peer.to_string(),
                        None,
                        "contact_material_probe",
                    );
                    if newly_connected {
                        diagnostics::record_diagnostic(
                            Some(state_dir),
                            diagnostics::DiagnosticEvent::new(
                                "info",
                                "transport",
                                "connection.probed",
                                "ok",
                                format!("iroh contact material probe succeeded: {peer}"),
                            )
                            .object("peer", Some(peer.to_string()))
                            .source_node_id(Some(peer.to_string())),
                        );
                    }
                }
                let _ = self.request_backfill_for_peer_now(&peer, node)?;
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("contact_material_updated peer={peer}"),
                })
            }
            NetworkRuntimeEvent::ContactMaterialOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                self.pending_contact_material_requests.remove(&request_id);
                self.mark_peer_control_stream_failed(peer.clone());
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!(
                        "contact_material_outbound_failure peer={peer} request_id={request_id:?} error={error}"
                    ),
                })
            }
            NetworkRuntimeEvent::ContactMaterialInboundFailure { peer, error } => {
                Ok(NetworkBridgeTick::TransportNotice {
                    detail: format!("contact_material_inbound_failure peer={peer} error={error}"),
                })
            }
            NetworkRuntimeEvent::PeerRelationshipRequest {
                peer,
                request,
                request_id,
            } => {
                let action = control_peer_relationship_action(request.action);
                let local_node_id = self.local_peer_id().to_string();
                let now = observed_at_ms();
                diagnostics::record_diagnostic(
                    self.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "info",
                        "transport",
                        "peer_relationship.request",
                        "received",
                        "peer relationship request received",
                    )
                    .object("peer_relationship", Some(format!("{request_id:?}")))
                    .source_node_id(Some(peer.to_string()))
                    .details(json!({
                        "request_id": format!("{request_id:?}"),
                        "source_node_id": &request.source_node_id,
                        "target_node_id": &request.target_node_id,
                        "action": request.action,
                        "agent_envelope": &request.agent_envelope,
                    })),
                );
                let local_contact_material = self
                    .state_dir
                    .as_deref()
                    .and_then(|state_dir| build_contact_material(state_dir, &local_node_id).ok());
                let (response, tick) = if request.source_node_id != peer.to_string() {
                    let error = format!(
                        "peer relationship request source_node_id mismatch: payload={} transport={peer}",
                        request.source_node_id
                    );
                    (
                        PeerRelationshipResponse {
                            source_node_id: local_node_id.clone(),
                            target_node_id: request.source_node_id.clone(),
                            action: request.action,
                            applied: false,
                            agent_envelope: Some(default_agent_envelope(
                                &local_node_id,
                                &request.source_node_id,
                                "peer.relationship.response",
                                json!({
                                    "action": request.action,
                                    "applied": false,
                                    "detail": error,
                                }),
                            )),
                            contact_material: local_contact_material.clone(),
                            relationship_state: None,
                            detail: Some(error.clone()),
                            updated_at: now,
                        },
                        NetworkBridgeTick::PeerRelationshipFailed {
                            peer: peer.clone(),
                            action,
                            error,
                        },
                    )
                } else if request.target_node_id != local_node_id {
                    let error = format!(
                        "peer relationship request target_node_id mismatch: payload={} local={local_node_id}",
                        request.target_node_id
                    );
                    (
                        PeerRelationshipResponse {
                            source_node_id: local_node_id.clone(),
                            target_node_id: request.source_node_id.clone(),
                            action: request.action,
                            applied: false,
                            agent_envelope: Some(default_agent_envelope(
                                &local_node_id,
                                &request.source_node_id,
                                "peer.relationship.response",
                                json!({
                                    "action": request.action,
                                    "applied": false,
                                    "detail": error,
                                }),
                            )),
                            contact_material: local_contact_material.clone(),
                            relationship_state: None,
                            detail: Some(error.clone()),
                            updated_at: now,
                        },
                        NetworkBridgeTick::PeerRelationshipFailed {
                            peer: peer.clone(),
                            action,
                            error,
                        },
                    )
                } else if let Some(state_dir) = self.state_dir.clone() {
                    if let Some(contact_material) = request.contact_material.as_ref() {
                        upsert_contact_material_for_peer(
                            &state_dir,
                            &request.source_node_id,
                            contact_material,
                        )?;
                    }
                    if let Some(agent_envelope) = request.agent_envelope.as_ref() {
                        verify_agent_envelope_signature_for_source(
                            agent_envelope,
                            Some(&request.source_node_id),
                        )?;
                    }
                    match crate::control::apply_peer_relationship_action_state(
                        &state_dir,
                        &request.source_node_id,
                        action,
                        crate::control::PeerRelationshipInitiator::Remote,
                    ) {
                        Ok(record) => {
                            if let Some(agent_envelope) = request.agent_envelope.as_ref() {
                                attach_agent_envelope_to_relationship(
                                    &state_dir,
                                    &request.source_node_id,
                                    agent_envelope,
                                )?;
                            }
                            if action == crate::control::PeerRelationshipAction::Request {
                                let event = build_agent_event_with_agent_envelope(
                                    wattswarm_protocol::types::AgentEventType::FriendRequest,
                                    wattswarm_protocol::types::AgentEventSourceKind::PeerRelationship,
                                    Some(request.source_node_id.clone()),
                                    request
                                        .agent_envelope
                                        .as_ref()
                                        .and_then(|envelope| envelope.target_agent_id.clone()),
                                    request
                                        .agent_envelope
                                        .as_ref()
                                        .map(raw_agent_envelope_to_protocol),
                                    json!({
                                        "source_node_id": request.source_node_id,
                                        "target_node_id": request.target_node_id,
                                        "action": request.action,
                                        "relationship_state": record.relationship_state,
                                        "updated_at": record.updated_at,
                                    }),
                                    true,
                                    vec![
                                        "accept".to_owned(),
                                        "reject".to_owned(),
                                        "block".to_owned(),
                                    ],
                                    Some(request.source_node_id.clone()),
                                    Some(format!(
                                        "friend_request:{}:{}",
                                        request.source_node_id, record.updated_at
                                    )),
                                );
                                let _ = deliver_agent_event_to_local_executor(
                                    &state_dir,
                                    self.db_path.as_deref(),
                                    &event,
                                );
                            }
                            if action == crate::control::PeerRelationshipAction::Accept
                                && record.relationship_state
                                    == crate::control::PeerRelationshipState::Accepted
                            {
                                let a2a_protocol = request
                                    .agent_envelope
                                    .as_ref()
                                    .map(|envelope| envelope.protocol.clone())
                                    .unwrap_or_else(|| "google_a2a".to_owned());
                                self.finalize_dm_session_from_relationship(
                                    &state_dir,
                                    &request.source_node_id,
                                    crate::control::PeerDmDirection::Inbound,
                                    &a2a_protocol,
                                    record.updated_at,
                                )?;
                            }
                            (
                                PeerRelationshipResponse {
                                    source_node_id: local_node_id.clone(),
                                    target_node_id: request.source_node_id.clone(),
                                    action: request.action,
                                    applied: true,
                                    agent_envelope: Some(default_agent_envelope(
                                        &local_node_id,
                                        &request.source_node_id,
                                        "peer.relationship.response",
                                        json!({
                                            "action": request.action,
                                            "applied": true,
                                            "relationship_state": record.relationship_state.as_str(),
                                        }),
                                    )),
                                    contact_material: local_contact_material.clone(),
                                    relationship_state: Some(
                                        record.relationship_state.as_str().to_owned(),
                                    ),
                                    detail: None,
                                    updated_at: record.updated_at,
                                },
                                NetworkBridgeTick::PeerRelationshipUpdated {
                                    peer: peer.clone(),
                                    action,
                                    relationship_state: record.relationship_state,
                                    initiated_by: crate::control::PeerRelationshipInitiator::Remote,
                                },
                            )
                        }
                        Err(error) => {
                            let error = error.to_string();
                            (
                                PeerRelationshipResponse {
                                    source_node_id: local_node_id.clone(),
                                    target_node_id: request.source_node_id.clone(),
                                    action: request.action,
                                    applied: false,
                                    agent_envelope: Some(default_agent_envelope(
                                        &local_node_id,
                                        &request.source_node_id,
                                        "peer.relationship.response",
                                        json!({
                                            "action": request.action,
                                            "applied": false,
                                            "detail": error,
                                        }),
                                    )),
                                    contact_material: local_contact_material.clone(),
                                    relationship_state: None,
                                    detail: Some(error.clone()),
                                    updated_at: now,
                                },
                                NetworkBridgeTick::PeerRelationshipFailed {
                                    peer: peer.clone(),
                                    action,
                                    error,
                                },
                            )
                        }
                    }
                } else {
                    let error =
                        "peer relationship request received before state_dir was configured"
                            .to_owned();
                    (
                        PeerRelationshipResponse {
                            source_node_id: local_node_id.clone(),
                            target_node_id: request.source_node_id.clone(),
                            action: request.action,
                            applied: false,
                            agent_envelope: Some(default_agent_envelope(
                                &local_node_id,
                                &request.source_node_id,
                                "peer.relationship.response",
                                json!({
                                    "action": request.action,
                                    "applied": false,
                                    "detail": error,
                                }),
                            )),
                            contact_material: local_contact_material.clone(),
                            relationship_state: None,
                            detail: Some(error.clone()),
                            updated_at: now,
                        },
                        NetworkBridgeTick::PeerRelationshipFailed {
                            peer: peer.clone(),
                            action,
                            error,
                        },
                    )
                };
                match self
                    .runtime
                    .send_peer_relationship_response(request_id, response)
                {
                    Ok(()) => Ok(tick),
                    Err(error)
                        if error
                            .to_string()
                            .contains("peer relationship response channel closed") =>
                    {
                        Ok(NetworkBridgeTick::TransportNotice {
                            detail: format!(
                                "peer_relationship_response_dropped peer={peer} reason={error}"
                            ),
                        })
                    }
                    Err(error) => Err(error),
                }
            }
            NetworkRuntimeEvent::PeerRelationshipResponse {
                peer,
                request_id,
                response,
            } => {
                let Some(pending) = self.pending_relationship_requests.remove(&request_id) else {
                    return Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "peer_relationship_response_without_pending peer={peer} request_id={request_id:?}"
                        ),
                    });
                };
                let action = pending.action;
                let expected_action = wire_peer_relationship_action(action);
                let local_node_id = self.local_peer_id().to_string();
                diagnostics::record_diagnostic(
                    self.state_dir.as_deref(),
                    diagnostics::DiagnosticEvent::new(
                        "info",
                        "transport",
                        "peer_relationship.response",
                        "received",
                        "peer relationship response received",
                    )
                    .object("peer_relationship", Some(format!("{request_id:?}")))
                    .source_node_id(Some(peer.to_string()))
                    .details(json!({
                        "request_id": format!("{request_id:?}"),
                        "source_node_id": &response.source_node_id,
                        "target_node_id": &response.target_node_id,
                        "action": response.action,
                        "applied": response.applied,
                        "relationship_state": &response.relationship_state,
                        "detail": &response.detail,
                        "agent_envelope": &response.agent_envelope,
                    })),
                );
                if pending.peer != peer {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer: peer.clone(),
                        action,
                        error: format!(
                            "peer relationship response peer mismatch: expected={} actual={peer}",
                            pending.peer
                        ),
                    });
                }
                if response.source_node_id != pending.remote_node_id
                    || response.target_node_id != local_node_id
                    || response.action != expected_action
                {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: format!(
                            "peer relationship response payload mismatch source={} target={} action={:?}",
                            response.source_node_id, response.target_node_id, response.action
                        ),
                    });
                }
                let state_dir = self.state_dir.clone();
                if let (Some(state_dir), Some(contact_material)) =
                    (state_dir.as_ref(), response.contact_material.as_ref())
                {
                    upsert_contact_material_for_peer(
                        state_dir,
                        &pending.remote_node_id,
                        contact_material,
                    )?;
                }
                if !response.applied {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: response
                            .detail
                            .unwrap_or_else(|| "peer relationship action rejected".to_owned()),
                    });
                }
                let Some(state_dir) = state_dir else {
                    return Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error:
                            "peer relationship response received before state_dir was configured"
                                .to_owned(),
                    });
                };
                let local_state = crate::control::load_peer_relationship_records_state(&state_dir)?
                    .into_iter()
                    .find(|record| record.remote_node_id == pending.remote_node_id)
                    .map(|record| record.relationship_state);
                match local_state {
                    Some(relationship_state) => {
                        if action == crate::control::PeerRelationshipAction::Accept
                            && relationship_state == crate::control::PeerRelationshipState::Accepted
                        {
                            let a2a_protocol = response
                                .agent_envelope
                                .as_ref()
                                .map(|envelope| envelope.protocol.clone())
                                .unwrap_or_else(|| "google_a2a".to_owned());
                            self.finalize_dm_session_from_relationship(
                                &state_dir,
                                &pending.remote_node_id,
                                crate::control::PeerDmDirection::Outbound,
                                &a2a_protocol,
                                response.updated_at,
                            )?;
                        }
                        Ok(NetworkBridgeTick::PeerRelationshipUpdated {
                            peer,
                            action,
                            relationship_state,
                            initiated_by: crate::control::PeerRelationshipInitiator::Local,
                        })
                    }
                    None => Ok(NetworkBridgeTick::PeerRelationshipFailed {
                        peer,
                        action,
                        error: "peer relationship response applied without a local state record"
                            .to_owned(),
                    }),
                }
            }
            NetworkRuntimeEvent::PeerRelationshipOutboundFailure {
                peer,
                request_id,
                error,
            } => {
                let action = self
                    .pending_relationship_requests
                    .remove(&request_id)
                    .map(|pending| pending.action)
                    .unwrap_or(crate::control::PeerRelationshipAction::Request);
                self.mark_peer_control_stream_failed(peer.clone());
                Ok(NetworkBridgeTick::PeerRelationshipFailed {
                    peer,
                    action,
                    error,
                })
            }
            NetworkRuntimeEvent::PeerRelationshipInboundFailure { peer, error } => Err(anyhow!(
                "peer relationship inbound failure from {peer}: {error}"
            ))
            .or_else(|error| {
                let error_text = error.to_string();
                if error_text.contains("peer relationship inbound failure")
                    && error_text.contains("Timeout while receiving request or sending response")
                {
                    Ok(NetworkBridgeTick::TransportNotice {
                        detail: format!(
                            "peer_relationship_inbound_timeout peer={peer} reason={error_text}"
                        ),
                    })
                } else {
                    Err(error)
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::should_record_backfill_response_diagnostic;

    #[test]
    fn backfill_response_diagnostics_skip_empty_responses() {
        assert!(!should_record_backfill_response_diagnostic(0));
        assert!(should_record_backfill_response_diagnostic(1));
    }
}
