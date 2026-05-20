use super::*;

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
        content_source_node_id: None,
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
        authority_signer_node_id: None,
        authority_signature_hex: None,
    });
    assert!(event_envelope_from_gossip(&message).is_err());
}

#[test]
fn ingest_chat_gossip_applies_remote_topic_message_to_local_store() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");

    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:hello-crew", &remote.node_id()),
            local_content_cache: Some(json!({"text":"hello crew"})),
            reply_to_message_id: None,
        }),
    )
    .expect("signed event");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            bootstrap_peers: Vec::new(),
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global, SwarmScope::Group("crew-7".to_owned())],
        &crate::types::NetworkProtocolParams::default(),
    )
    .expect("service");

    let tick = service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::Gossip {
                propagation_source: random_network_node_id(),
                message: GossipMessage::Chat(EventEnvelope {
                    scope: SwarmScope::Group("crew-7".to_owned()),
                    event: remote_event.clone(),
                    content_source_node_id: None,
                }),
            }),
        )
        .expect("apply runtime event");

    assert!(matches!(
        tick,
        NetworkBridgeTick::EventIngested { event_id, .. } if event_id == remote_event.event_id
    ));
    let messages = node
        .store
        .list_topic_messages("default", "crew.chat", "group:crew-7", 10)
        .expect("list topic messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, remote_event.event_id);
    assert_eq!(messages[0].content["text"], json!("hello crew"));
}

#[test]
fn global_event_gossip_rejects_non_global_scope() {
    let envelope = EventEnvelope {
        scope: SwarmScope::Region("sol-1".to_owned()),
        event: build_event_for_external(
            &NodeIdentity::random(),
            1,
            10,
            crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
                checkpoint_id: "cp-1".to_owned(),
                up_to_seq: 1,
            }),
        )
        .expect("event"),
        content_source_node_id: None,
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
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("backfill response");

    assert_eq!(response.events.len(), 1);
    assert_eq!(response.scope, SwarmScope::Global);
    assert_eq!(response.next_from_event_seq, 1);
}

#[test]
fn iroh_backfill_stream_repairs_missed_global_event_and_ingest_is_idempotent() {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestIrohBackfillStreamRequest {
        request: BackfillRequest,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestIrohBackfillStreamResponse {
        response: crate::network_p2p::BackfillResponse,
    }

    const IROH_CONTROL_KIND_BACKFILL: &str = "backfill.v1";

    let dir_a = temp_startup_dir("iroh-backfill-a");
    let dir_b = temp_startup_dir("iroh-backfill-b");
    crate::control::local_node_id(&dir_a).expect("seed node a");
    crate::control::local_node_id(&dir_b).expect("seed node b");
    let peer_a = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_a)
        .expect("endpoint a")
        .to_string();
    let peer_b = wattswarm_network_transport_iroh::local_endpoint_id_from_state_dir(&dir_b)
        .expect("endpoint b")
        .to_string();

    let subscriber_identity = NodeIdentity::random();
    let publisher_identity = NodeIdentity::random();
    let membership =
        membership_with_roles(&[subscriber_identity.node_id(), publisher_identity.node_id()]);
    let mut subscriber = Node::new(
        subscriber_identity,
        PgStore::open_in_memory().expect("subscriber store"),
        membership.clone(),
    )
    .expect("subscriber node");
    let mut publisher = Node::new(
        publisher_identity,
        PgStore::open_in_memory().expect("publisher store"),
        membership,
    )
    .expect("publisher node");
    let policy_hash = publisher
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-iroh-backfill-1", policy_hash);
    contract.inputs = json!({"prompt":"repair me over iroh"});
    publisher
        .submit_task(contract, 1, 100)
        .expect("publisher submits task");

    wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id(
        &dir_a, &peer_a, 1,
    )
    .expect("contact a");
    let contact_b =
        wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id(
            &dir_b, &peer_b, 1,
        )
        .expect("contact b");

    let bridge_response = backfill_response_for_request(
        &publisher,
        &peer_b,
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("bridge backfill response");
    let bridge_response_payload = serde_json::to_vec(&TestIrohBackfillStreamResponse {
        response: bridge_response,
    })
    .expect("encode bridge response");
    wattswarm_network_transport_iroh::set_local_control_stream_handler_for_network_peer_id(
        &dir_b,
        &peer_b,
        IROH_CONTROL_KIND_BACKFILL,
        Some(
            move |request: wattswarm_network_transport_iroh::IrohControlStreamRequest| {
                if request.kind != IROH_CONTROL_KIND_BACKFILL {
                    return wattswarm_network_transport_iroh::IrohControlStreamResponse {
                        ok: false,
                        error: Some(format!("unexpected control kind {}", request.kind)),
                        payload: Vec::new(),
                    };
                }
                let decoded =
                    match serde_json::from_slice::<TestIrohBackfillStreamRequest>(&request.payload)
                    {
                        Ok(decoded) => decoded,
                        Err(err) => {
                            return wattswarm_network_transport_iroh::IrohControlStreamResponse {
                                ok: false,
                                error: Some(err.to_string()),
                                payload: Vec::new(),
                            };
                        }
                    };
                if decoded.request.scope != SwarmScope::Global {
                    return wattswarm_network_transport_iroh::IrohControlStreamResponse {
                        ok: false,
                        error: Some("unexpected scope".to_owned()),
                        payload: Vec::new(),
                    };
                }
                wattswarm_network_transport_iroh::IrohControlStreamResponse {
                    ok: true,
                    error: None,
                    payload: bridge_response_payload.clone(),
                }
            },
        ),
    )
    .expect("install backfill handler");

    wattswarm_network_transport_iroh::register_remote_contact_material_for_network_peer_id(
        &dir_a, &peer_a, &contact_b,
    )
    .expect("register remote contact material");

    let response =
        wattswarm_network_transport_iroh::send_control_stream_request_for_network_peer_id(
            &dir_a,
            &peer_a,
            &contact_b,
            &wattswarm_network_transport_iroh::IrohControlStreamRequest {
                kind: IROH_CONTROL_KIND_BACKFILL.to_owned(),
                payload: serde_json::to_vec(&TestIrohBackfillStreamRequest {
                    request: BackfillRequest {
                        scope: SwarmScope::Global,
                        from_event_seq: 0,
                        limit: 8,
                        feed_key: None,
                        known_event_ids: Vec::new(),
                    },
                })
                .expect("encode backfill request"),
            },
        )
        .expect("request iroh backfill page");
    assert!(response.ok, "{:?}", response.error);
    let response = serde_json::from_slice::<TestIrohBackfillStreamResponse>(&response.payload)
        .expect("decode backfill response")
        .response;
    assert_eq!(response.events.len(), 1);

    let applied = ingest_backfill_response(&mut subscriber, &response).expect("ingest response");
    let duplicate_applied =
        ingest_backfill_response(&mut subscriber, &response).expect("ingest duplicate response");
    assert_eq!(applied, 1);
    assert_eq!(duplicate_applied, 0);
    assert!(
        subscriber
            .store
            .task_projection("task-iroh-backfill-1")
            .expect("load task projection")
            .is_some()
    );

    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_a);
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&dir_b);
    std::fs::remove_dir_all(dir_a).expect("cleanup a");
    std::fs::remove_dir_all(dir_b).expect("cleanup b");
}

#[test]
fn ingest_backfill_response_rejects_scope_mismatch() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let response = crate::network_p2p::BackfillResponse {
        scope: SwarmScope::Global,
        next_from_event_seq: 1,
        feed_key: None,
        head_event_ids: Vec::new(),
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
            content_source_node_id: None,
        }],
    };

    assert!(ingest_backfill_response(&mut node, &response).is_err());
}

#[test]
fn topic_backfill_response_filters_by_feed_key() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    node.emit_at(
        1,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:hello-crew", "node-local"),
            local_content_cache: Some(json!({"text":"hello crew"})),
            reply_to_message_id: None,
        }),
        10,
    )
    .expect("emit topic message");
    node.emit_at(
        1,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "market.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:ignore-me", "node-local"),
            local_content_cache: Some(json!({"text":"ignore me"})),
            reply_to_message_id: None,
        }),
        11,
    )
    .expect("emit other topic message");

    let response = backfill_response_for_request(
        &node,
        "peer-local",
        &BackfillRequest {
            scope: SwarmScope::Group("crew-7".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: Some("crew.chat".to_owned()),
            known_event_ids: Vec::new(),
        },
        32,
        64,
    )
    .expect("topic backfill response");

    assert_eq!(response.feed_key.as_deref(), Some("crew.chat"));
    assert_eq!(response.events.len(), 1);
    assert!(matches!(
        &response.events[0].event.payload,
        crate::types::EventPayload::TopicMessagePosted(payload) if payload.feed_key == "crew.chat"
    ));
}

#[test]
fn topic_backfill_response_advances_local_cursor() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node = Node::new(
        local.clone(),
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("node");
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local.node_id(),
                feed_key: "crew.chat".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["messages".to_owned()],
                active: true,
            },
        ),
        5,
    )
    .expect("subscribe");

    let remote_event = build_event_for_external(
        &remote,
        1,
        10,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: "crew.chat".to_owned(),
            scope_hint: "group:crew-7".to_owned(),
            content_ref: sample_topic_content_ref("sha256:cursor-me", &remote.node_id()),
            local_content_cache: Some(json!({"text":"cursor me"})),
            reply_to_message_id: None,
        }),
    )
    .expect("remote event");
    let response = crate::network_p2p::BackfillResponse {
        scope: SwarmScope::Group("crew-7".to_owned()),
        next_from_event_seq: 7,
        feed_key: Some("crew.chat".to_owned()),
        head_event_ids: Vec::new(),
        events: vec![EventEnvelope {
            scope: SwarmScope::Group("crew-7".to_owned()),
            event: remote_event,
            content_source_node_id: None,
        }],
    };

    ingest_backfill_response(&mut node, &response).expect("apply topic backfill");
    maybe_record_topic_cursor_for_response(&node, &local.node_id(), &response, 100)
        .expect("record topic cursor");

    let cursor = node
        .store
        .get_topic_cursor("default", &local.node_id(), "crew.chat")
        .expect("get topic cursor")
        .expect("cursor exists");
    assert_eq!(cursor.last_event_seq, 2);
    assert_eq!(cursor.scope_hint, "group:crew-7");
}
