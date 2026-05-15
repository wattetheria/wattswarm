use super::*;

pub fn two_nodes_sync_global_event_over_iroh() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-gossip", policy_hash);
    contract.inputs = json!({"prompt":"ship this over gossip"});
    node_a.submit_task(contract, 1, 100).expect("submit task");
    let last_published_seq = 0;
    let mut published = false;
    for _ in 0..4_096 {
        let new_last = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending");
        if new_last > last_published_seq {
            published = true;
            break;
        }
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        std::thread::yield_now();
    }
    assert!(
        published,
        "local event should auto-publish over bridge helper"
    );

    let mut last_tick_a = None;
    let mut last_tick_b = None;
    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if node_b
            .task_view("task-network-gossip")
            .expect("task view")
            .is_some()
        {
            return true;
        }
        last_tick_a = tick_a;
        last_tick_b = tick_b;
        false
    });
    let node_b_events = node_b.store.load_all_events().expect("load node b events");
    assert!(
        synced,
        "node B should ingest the task over iroh gossip; node_b_events={} last_tick_a={last_tick_a:?} last_tick_b={last_tick_b:?}",
        node_b_events.len()
    );

    let task = node_b
        .task_view("task-network-gossip")
        .expect("task view")
        .expect("task exists");
    assert_eq!(
        task.contract.inputs["prompt"],
        json!("ship this over gossip")
    );
}

pub fn two_nodes_sync_topic_message_content_over_iroh() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let scope = SwarmScope::Group("crew-7".to_owned());
    let dir_a = temp_test_dir("topic-content-a");
    let dir_b = temp_test_dir("topic-content-b");
    let mut service_a = make_service_with_config_and_state_dir(
        std::slice::from_ref(&scope),
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_a,
    );
    let mut service_b = make_service_with_config_and_state_dir(
        std::slice::from_ref(&scope),
        &test_protocol_params(),
        NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_local_discovery: false,
            ..NetworkP2pConfig::default()
        },
        &dir_b,
    );

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);
    pump_services_for(
        &mut service_a,
        &mut node_a,
        &mut service_b,
        &mut node_b,
        reconnect_quiet_period(),
    );
    let remote_b = service_b.local_peer_id().to_string();
    let remote_a = service_a.local_peer_id().to_string();
    let contact_ready = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        for _ in 0..32 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        contact_material_for(&dir_a, &remote_b).is_some()
            && contact_material_for(&dir_b, &remote_a).is_some()
    });
    assert!(
        contact_ready,
        "connected peers should exchange contact material before topic content sync"
    );

    let content = json!({
        "text": "hello crew from iroh topic sync",
        "kind": "topic.message"
    });
    let event = emit_topic_message_with_content(
        &mut node_a,
        &dir_a,
        "default",
        "crew.chat",
        "group:crew-7",
        content.clone(),
        None,
        100,
    )
    .expect("emit topic message with content");
    let expected_digest = match &event.payload {
        wattswarm_control_plane::types::EventPayload::TopicMessagePosted(payload) => {
            payload.content_ref.digest.clone()
        }
        other => panic!("expected topic message payload, got {other:?}"),
    };
    let mut last_published_seq = 0;

    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        last_published_seq = publish_pending_scoped_updates(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish scoped updates");
        for _ in 0..64 {
            let _ = pump_once(&mut service_a, &mut node_a);
            let _ = pump_once(&mut service_b, &mut node_b);
        }
        node_b
            .store
            .list_topic_messages("default", "crew.chat", "group:crew-7", 10)
            .expect("list topic messages")
            .into_iter()
            .any(|message| {
                message.message_id == event.event_id
                    && message.content_ref.digest == expected_digest
                    && message.content == content
            })
    });
    assert!(
        synced,
        "remote peer should fetch topic message content over iroh using content_ref"
    );
    assert!(
        content_artifact_exists(&dir_a, ArtifactKind::TopicMessage, &expected_digest),
        "publisher should keep local topic-message artifact"
    );
    assert!(
        content_artifact_exists(&dir_b, ArtifactKind::TopicMessage, &expected_digest),
        "subscriber should fetch topic-message artifact over iroh"
    );
    let subscriber_statuses = data_plane_statuses_for(&dir_b, "topic_message", &event.event_id);
    assert_eq!(
        subscriber_statuses.len(),
        1,
        "subscriber should persist one latest topic data-plane status row"
    );
    assert!(
        subscriber_statuses[0].route == "iroh_direct"
            && subscriber_statuses[0].status == "content_hydrated",
        "subscriber should persist the final topic content hydration status"
    );

    cleanup_dir(&dir_a);
    cleanup_dir(&dir_b);
}

pub fn global_task_detail_sync_excludes_process_firehose() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    connect_services(&mut service_a, &mut node_a, &mut service_b, &mut node_b);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-global-process-filter", policy_hash);
    contract.inputs = json!({"prompt":"sync task without process firehose"});
    node_a.submit_task(contract, 1, 100).expect("submit task");
    node_a
        .claim_task(
            "task-global-process-filter",
            wattswarm_control_plane::types::ClaimRole::Propose,
            "exec-filter-1",
            500,
            1,
            101,
        )
        .expect("claim task");

    let mut last_published_seq = 0;
    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        last_published_seq = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish pending");
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        node_b
            .task_view("task-global-process-filter")
            .expect("task view")
            .is_some()
    });
    assert!(synced, "remote node should receive task detail layer");
    assert!(
        node_b
            .store
            .get_lease("task-global-process-filter", "propose")
            .expect("load lease")
            .is_none(),
        "global dissemination should not replay task process traffic"
    );
}

pub fn two_nodes_backfill_missing_events_over_request_response() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-backfill", policy_hash);
    contract.inputs = json!({"prompt":"recover me via backfill"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let synced = wait_until(scaled_timeout(Duration::from_secs(10)), || {
        let tick_a = pump_once(&mut service_a, &mut node_a);
        let tick_b = pump_once(&mut service_b, &mut node_b);
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_a.as_ref() {
            panic!("backfill failed on service A: {error}");
        }
        if let Some(NetworkBridgeTick::BackfillFailed { error, .. }) = tick_b.as_ref() {
            panic!("backfill failed on service B: {error}");
        }
        if node_b
            .task_view("task-network-backfill")
            .expect("task view")
            .is_some()
        {
            return true;
        }
        false
    });
    assert!(synced);

    let task = node_b
        .task_view("task-network-backfill")
        .expect("task view")
        .expect("task exists");
    assert_eq!(
        task.contract.inputs["prompt"],
        json!("recover me via backfill")
    );
}

pub fn connected_peer_helper_syncs_over_lan_state() {
    let identity_a = NodeIdentity::random();
    let identity_b = NodeIdentity::random();
    let membership = membership_with_roles(&[identity_a.node_id(), identity_b.node_id()]);
    let mut node_a = make_node(identity_a, membership.clone());
    let mut node_b = make_node(identity_b, membership);
    let mut service_a = make_service();
    let mut service_b = make_service();
    for _ in 0..4_096 {
        let _ = pump_once(&mut service_a, &mut node_a);
        let _ = pump_once(&mut service_b, &mut node_b);
        if !service_a.listen_addrs().is_empty() && !service_b.listen_addrs().is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(!service_a.listen_addrs().is_empty());
    assert!(!service_b.listen_addrs().is_empty());

    connect_services(&mut service_b, &mut node_b, &mut service_a, &mut node_a);

    let policy_hash = node_a
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-network-lan", policy_hash);
    contract.inputs = json!({"prompt":"lan discovery sync"});
    node_a.submit_task(contract, 1, 100).expect("submit task");

    let mut last_published_seq = 0;
    let mut synced = false;
    let mut last_tick_a = None;
    let mut last_tick_b = None;
    for _ in 0..4_096 {
        last_published_seq = publish_pending_global_events(
            &mut service_a,
            &node_a,
            &node_a.node_id(),
            last_published_seq,
        )
        .expect("publish discovered peer event");
        last_tick_a = pump_once(&mut service_a, &mut node_a);
        last_tick_b = pump_once(&mut service_b, &mut node_b);
        if node_b
            .task_view("task-network-lan")
            .expect("task view")
            .is_some()
        {
            synced = true;
            break;
        }
        std::thread::yield_now();
    }

    assert!(
        synced,
        "connected peers should sync local event; last_published_seq={last_published_seq} node_b_events={} last_tick_a={last_tick_a:?} last_tick_b={last_tick_b:?}",
        node_b
            .store
            .load_all_events()
            .expect("load node b events")
            .len()
    );
}
