use super::*;

#[test]
fn invalid_scope_hints_are_rejected_for_network_substrate_events() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    let result = node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id,
                feed_key: "market.invalid".to_owned(),
                scope_hint: "bad-scope".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    );

    assert!(result.is_err());
}

#[test]
fn empty_network_id_is_rejected_for_network_substrate_events() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    let result = node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: String::new(),
                subscriber_node_id,
                feed_key: "market.invalid".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    );

    assert!(result.is_err());
}

#[test]
fn mismatched_network_id_is_rejected_for_network_substrate_events() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    let result = node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "mainnet:other".to_owned(),
                subscriber_node_id,
                feed_key: "market.invalid".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    );

    assert!(result.is_err());
}

#[test]
fn network_substrate_projection_canonicalizes_scope_hints() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");
    let subscriber_node_id = node.node_id();

    node.emit_at(
        1,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "default".to_owned(),
                subscriber_node_id: subscriber_node_id.clone(),
                feed_key: "market.canonical".to_owned(),
                scope_hint: " local:lab-9 ".to_owned(),
                gossip_kinds: vec!["events".to_owned()],
                provider_capabilities: None,
                agent_envelope: None,
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let row = node
        .store
        .get_feed_subscription("default", &subscriber_node_id, "market.canonical")
        .expect("load subscription")
        .expect("subscription exists");
    assert_eq!(row.scope_hint, "node:lab-9");
    assert_eq!(row.scope(), Some(ProjectionScope::Node("lab-9".to_owned())));
}
