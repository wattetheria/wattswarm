use super::*;

fn verify_dm_scopes_survive_restart(store: PgStore) {
    let identity = NodeIdentity::random();
    let subscriber = identity.node_id();
    let mut node = Node::new(identity.clone(), store, Membership::new()).unwrap();
    for (scope, time) in [("group:dm-first", 100), ("group:dm-second", 110)] {
        node.emit_at(
            1,
            crate::types::EventPayload::FeedSubscriptionUpdated(
                crate::types::FeedSubscriptionUpdatedPayload {
                    network_id: "default".to_owned(),
                    subscriber_node_id: subscriber.clone(),
                    feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
                    scope_hint: scope.to_owned(),
                    gossip_kinds: vec!["messages".to_owned()],
                    provider_capabilities: None,
                    agent_envelope: None,
                    active: true,
                },
            ),
            time,
        )
        .unwrap();
    }
    let store = node.store.clone();
    drop(node);
    let node = Node::new(identity, store, Membership::new()).unwrap();
    let scopes = scope::dynamic_subscription_scopes_for_node(&node, &subscriber).unwrap();
    assert!(scopes.contains(&SwarmScope::Group("dm-first".to_owned())));
    assert!(scopes.contains(&SwarmScope::Group("dm-second".to_owned())));
    let remote = NodeIdentity::random();
    let event = build_event_for_external(
        &remote,
        1,
        120,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: "default".to_owned(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: "group:dm-first".to_owned(),
            content_ref: sample_topic_content_ref("sha256:dm-first", &remote.node_id()),
            local_content_cache: Some(json!({"text": "old thread"})),
            reply_to_message_id: None,
            agent_envelope: None,
        }),
    )
    .unwrap();
    let state_dir = temp_startup_dir("dm-scopes-restart");
    // No ready DM thread exists: relevance must come from the older subscription itself.
    assert!(event_relevance::EventRelevanceFilter::should_deliver(
        &node,
        &state_dir,
        &subscriber,
        &event
    ));
    node.store
        .upsert_feed_subscription(
            "default",
            &subscriber,
            crate::control::PRIVATE_DM_FEED_KEY,
            "group:dm-first",
            &["messages".to_owned()],
            false,
            130,
        )
        .unwrap();
    assert!(!event_relevance::EventRelevanceFilter::should_deliver(
        &node,
        &state_dir,
        &subscriber,
        &event
    ));
    assert!(
        node.store
            .get_feed_subscription(
                "default",
                &subscriber,
                crate::control::PRIVATE_DM_FEED_KEY,
                "group:dm-second"
            )
            .unwrap()
            .unwrap()
            .active
    );
}

#[test]
fn dm_scopes_survive_restart_configured_backend() {
    verify_dm_scopes_survive_restart(PgStore::open_in_memory().unwrap());
}

#[test]
fn dm_scopes_survive_restart_sqlite() {
    verify_dm_scopes_survive_restart(PgStore::open_in_memory_sqlite().unwrap());
}

#[test]
fn dm_unsubscribe_overrides_ready_thread_and_new_accept_restores_same_group() {
    let _lock = lock_env_test_mutex();
    let state_dir = temp_startup_dir("dm-unsubscribe-reaccept");
    ensure_test_relay_urls(&state_dir);
    let db_path = state_dir.join("ui.state");
    let mut node = crate::control::open_node(&state_dir, &db_path).unwrap();
    let local = node.node_id();
    let remote = NodeIdentity::random();
    let remote_id = remote.node_id();
    let network = current_network_context_id(&node);
    let scope_hint = crate::control::private_dm_scope_hint(&local, &remote_id);
    let scope = SwarmScope::Group(crate::control::private_dm_group_id(&local, &remote_id));
    let thread_id = crate::control::private_dm_thread_id(&local, &remote_id);
    let identity = crate::control::load_local_identity(&state_dir).unwrap();
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::from_iroh_state_dir(
            NetworkP2pConfig {
                listen_addrs: vec!["127.0.0.1:0".to_owned()],
                enable_local_discovery: false,
                ..NetworkP2pConfig::default()
            },
            state_dir.clone(),
            identity.secret_bytes(),
        )
        .unwrap(),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .unwrap();
    service.set_state_dir(state_dir.clone(), db_path.clone());
    let envelope = |id: &str| {
        default_agent_envelope(
            &remote_id,
            &local,
            "social.friend.request",
            json!({"request_id": id}),
        )
    };
    for action in [
        crate::control::PeerRelationshipAction::Request,
        crate::control::PeerRelationshipAction::Accept,
    ] {
        apply_peer_relationship_action_projection(
            &state_dir,
            &remote_id,
            action,
            crate::control::PeerRelationshipInitiator::Local,
            &envelope("first"),
        )
        .unwrap();
    }
    service
        .finalize_dm_session_from_relationship(
            &state_dir,
            &remote_id,
            crate::control::PeerDmDirection::Outbound,
            "google_a2a",
            10,
        )
        .unwrap();
    let event = build_event_for_external(
        &remote,
        1,
        20,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: network.clone(),
            feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
            scope_hint: scope_hint.clone(),
            content_ref: sample_topic_content_ref("sha256:dm-reaccept", &remote_id),
            local_content_cache: Some(json!({"text": "message"})),
            reply_to_message_id: None,
            agent_envelope: None,
        }),
    )
    .unwrap();
    assert!(event_relevance::EventRelevanceFilter::should_deliver(
        &node, &state_dir, &local, &event
    ));
    assert!(service.backfill_scopes().contains(&scope));
    crate::control::remove_peer_relationship_locally_state(&state_dir, &remote_id).unwrap();
    let before = node.store.head_seq().unwrap();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: network.clone(),
                subscriber_node_id: local.clone(),
                feed_key: crate::control::PRIVATE_DM_FEED_KEY.to_owned(),
                scope_hint: scope_hint.clone(),
                gossip_kinds: vec!["messages".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: false,
            },
        ),
        30,
    )
    .unwrap();
    publish_pending_scoped_updates(&mut service, &node, &local, before).unwrap();
    assert!(!service.backfill_scopes().contains(&scope));
    assert!(!event_relevance::EventRelevanceFilter::should_deliver(
        &node, &state_dir, &local, &event
    ));
    assert_eq!(
        crate::control::load_peer_dm_thread_record_for_remote_state(&state_dir, &remote_id)
            .unwrap()
            .unwrap()
            .session_state,
        crate::control::PeerDmSessionState::Ready
    );
    drop(node);
    node = crate::control::open_node(&state_dir, &db_path).unwrap();
    let (_, replayed) = apply_peer_relationship_action_projection(
        &state_dir,
        &remote_id,
        crate::control::PeerRelationshipAction::Accept,
        crate::control::PeerRelationshipInitiator::Remote,
        &envelope("first"),
    )
    .unwrap();
    assert!(replayed);
    service
        .finalize_dm_session_from_relationship(
            &state_dir,
            &remote_id,
            crate::control::PeerDmDirection::Inbound,
            "google_a2a",
            35,
        )
        .unwrap();
    assert!(!service.backfill_scopes().contains(&scope));
    assert!(
        !node
            .store
            .get_feed_subscription(
                &network,
                &local,
                crate::control::PRIVATE_DM_FEED_KEY,
                &scope_hint
            )
            .unwrap()
            .unwrap()
            .active
    );
    for action in [
        crate::control::PeerRelationshipAction::Request,
        crate::control::PeerRelationshipAction::Accept,
    ] {
        apply_peer_relationship_action_projection(
            &state_dir,
            &remote_id,
            action,
            crate::control::PeerRelationshipInitiator::Local,
            &envelope("new"),
        )
        .unwrap();
    }
    service
        .finalize_dm_session_from_relationship(
            &state_dir,
            &remote_id,
            crate::control::PeerDmDirection::Outbound,
            "google_a2a",
            40,
        )
        .unwrap();
    assert!(
        node.store
            .get_feed_subscription(
                &network,
                &local,
                crate::control::PRIVATE_DM_FEED_KEY,
                &scope_hint
            )
            .unwrap()
            .unwrap()
            .active
    );
    assert!(service.backfill_scopes().contains(&scope));
    assert!(event_relevance::EventRelevanceFilter::should_deliver(
        &node, &state_dir, &local, &event
    ));
    assert_eq!(
        crate::control::load_peer_dm_thread_record_for_remote_state(&state_dir, &remote_id)
            .unwrap()
            .unwrap()
            .thread_id,
        thread_id
    );
    wattswarm_network_transport_iroh::shutdown_local_iroh_data_plane(&state_dir);
}

#[test]
fn network_config_defaults_to_enabled_with_fixed_tcp_port() {
    let _lock = lock_env_test_mutex();
    let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, None);
    let _local_discovery = EnvVarGuard::set(ENV_P2P_LOCAL_DISCOVERY, None);
    let _port = EnvVarGuard::set(ENV_P2P_PORT, None);
    let _listen = EnvVarGuard::set(ENV_P2P_LISTEN_ADDRS, None);
    let _bootstrap = EnvVarGuard::set("WATTSWARM_P2P_BOOTSTRAP_PEERS", None);
    assert!(network_enabled_from_env());
    let config = network_config_from_env();
    assert!(config.enable_local_discovery);
    assert_eq!(config.listen_addrs, vec!["0.0.0.0:4001"]);
    assert!(config.bootstrap_peers.is_empty());
}

#[test]
fn network_enabled_can_be_explicitly_disabled() {
    let _lock = lock_env_test_mutex();
    let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, Some("false"));
    assert!(!network_enabled_from_env());
}

#[test]
fn configured_network_scopes_include_global_by_default() {
    let _lock = lock_env_test_mutex();
    let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, None);
    let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, None);
    let _nodes = EnvVarGuard::set(ENV_P2P_NODE_IDS, None);
    assert_eq!(
        configured_network_scopes_from_env(),
        vec![SwarmScope::Global]
    );
}

#[test]
fn configured_network_scopes_include_region_and_node_aliases() {
    let _lock = lock_env_test_mutex();
    let _regions = EnvVarGuard::set(ENV_P2P_REGION_IDS, Some("sol-1,sol-2"));
    let _locals = EnvVarGuard::set(ENV_P2P_LOCAL_IDS, Some("lab-a"));
    let _nodes = EnvVarGuard::set(ENV_P2P_NODE_IDS, Some("lab-b"));
    let scopes = configured_network_scopes_from_env();
    assert_eq!(scopes.len(), 5);
    assert_eq!(scopes[0], SwarmScope::Global);
    assert!(scopes.contains(&SwarmScope::Region("sol-1".to_owned())));
    assert!(scopes.contains(&SwarmScope::Region("sol-2".to_owned())));
    assert!(scopes.contains(&SwarmScope::Node("lab-a".to_owned())));
    assert!(scopes.contains(&SwarmScope::Node("lab-b".to_owned())));
}

#[test]
fn dynamic_subscription_scopes_merge_with_configured_scopes() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: node_id.clone(),
                feed_key: "market.alpha".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let scopes = scope::merge_scopes(
        configured_network_scopes_from_env()
            .into_iter()
            .chain(scope::dynamic_subscription_scopes_for_node(&node, &node_id).expect("scopes")),
    );
    assert!(scopes.contains(&SwarmScope::Global));
    assert!(scopes.contains(&SwarmScope::Region("sol-1".to_owned())));
}

#[test]
fn publish_pending_updates_subscribes_runtime_for_local_feed_subscription() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let local_node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.beta".to_owned(),
                scope_hint: "node:lab-9".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            bootstrap_peers: Vec::new(),
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    publish_pending_scoped_updates(&mut service, &node, &local_node_id, 0)
        .expect("publish pending updates");

    assert!(
        service
            .subscribed_scopes()
            .contains(&SwarmScope::Node("lab-9".to_owned()))
    );
    let subscribed_kinds = service.subscribed_gossip_kinds(&SwarmScope::Node("lab-9".to_owned()));
    assert!(subscribed_kinds.contains(&GossipKind::Events));
    assert!(!subscribed_kinds.contains(&GossipKind::Messages));
}

#[test]
fn feed_subscription_updates_route_to_target_subscription_scope() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let local_node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id,
                feed_key: "market.control".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");
    let (_, event) = node
        .store
        .load_all_events()
        .expect("load events")
        .into_iter()
        .next()
        .expect("event");

    let route = scope::event_transport_route(&node, &event)
        .expect("event route")
        .expect("subscription update route");
    assert_eq!(route.scope, SwarmScope::Group("crew-7".to_owned()));
    assert_eq!(route.address, "ws.group.crew-7.FeedSubscriptionUpdated");
    assert_eq!(
        scope::event_scope(&node, &event).expect("event scope"),
        SwarmScope::Group("crew-7".to_owned())
    );
    let crate::types::EventPayload::FeedSubscriptionUpdated(payload) = &event.payload else {
        panic!("expected subscription update");
    };
    assert_eq!(
        feed_subscription_target_scope(payload),
        Some(SwarmScope::Group("crew-7".to_owned()))
    );
}

#[test]
fn feed_subscription_updates_with_invalid_target_scope_are_unroutable() {
    let node = Node::open_in_memory_with_roles(&[]).expect("node");
    let remote = NodeIdentity::random();
    let event = build_event_for_external(
        &remote,
        1,
        100,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: remote.node_id(),
                feed_key: "market.invalid".to_owned(),
                scope_hint: "bad-scope".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
    )
    .expect("subscription event");

    assert!(
        scope::event_transport_route(&node, &event)
            .expect("route result")
            .is_none()
    );
}

#[test]
fn remote_feed_subscription_gossip_authorizes_peer_for_target_scope_backfill() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let propagation_source = random_network_node_id();
    let target_scope = SwarmScope::Group("crew-7".to_owned());
    let event = build_event_for_external(
        &remote,
        1,
        100,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: remote.node_id(),
                feed_key: "market.relay".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
    )
    .expect("subscription event");

    service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::Gossip {
                propagation_source: propagation_source.clone(),
                message: GossipMessage::Event(EventEnvelope {
                    scope: target_scope.clone(),
                    event,
                    content_source_node_id: None,
                }),
            }),
        )
        .expect("ingest subscription");

    let request = BackfillRequest {
        scope: target_scope.clone(),
        from_event_seq: 0,
        limit: 8,
        head_only: false,
        feed_key: None,
        exclude_topic_events: false,
        known_event_ids: Vec::new(),
    };
    assert!(service.peer_has_scope_activity(&propagation_source, &target_scope));
    assert!(service.inbound_backfill_authorized(&propagation_source, &request));
}

#[test]
fn remote_feed_subscription_gossip_rejects_wrong_envelope_scope() {
    let local = NodeIdentity::random();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local.node_id(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    let propagation_source = random_network_node_id();
    let target_scope = SwarmScope::Group("crew-7".to_owned());
    let event = build_event_for_external(
        &remote,
        1,
        100,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: remote.node_id(),
                feed_key: "market.relay".to_owned(),
                scope_hint: "group:crew-7".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
    )
    .expect("subscription event");

    let tick = service
        .handle_runtime_event(
            &mut node,
            Ok(NetworkRuntimeEvent::Gossip {
                propagation_source: propagation_source.clone(),
                message: GossipMessage::Event(EventEnvelope {
                    scope: SwarmScope::Global,
                    event,
                    content_source_node_id: None,
                }),
            }),
        )
        .expect("handle runtime event");

    assert!(matches!(
        tick,
        NetworkBridgeTick::TransportNotice { detail }
            if detail.contains("signed_scope_mismatch")
    ));
    assert!(!service.peer_has_scope_activity(&propagation_source, &target_scope));
    assert!(
        node.store
            .get_feed_subscription(
                "default",
                &remote.node_id(),
                "market.relay",
                &scope_hint_label(&target_scope)
            )
            .expect("load subscription")
            .is_none()
    );
}

#[test]
fn publish_pending_updates_unsubscribes_scope_when_local_subscription_is_disabled() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let local_node_id = node.node_id();
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.gamma".to_owned(),
                scope_hint: "region:sol-8".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    )
    .expect("subscription on");
    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: local_node_id.clone(),
                feed_key: "market.gamma".to_owned(),
                scope_hint: "region:sol-8".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: false,
            },
        ),
        101,
    )
    .expect("subscription off");

    let mut service = NetworkBridgeService::new(
        test_network_node(NetworkP2pConfig {
            listen_addrs: vec!["127.0.0.1:0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    publish_pending_scoped_updates(&mut service, &node, &local_node_id, 0)
        .expect("publish pending updates");

    assert!(
        !service
            .subscribed_scopes()
            .contains(&SwarmScope::Region("sol-8".to_owned()))
    );
}
