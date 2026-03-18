use super::*;
use crate::crypto::NodeIdentity;
use crate::node::build_event_for_external;
use crate::storage::{
    DecisionMemoryHitRow, ImportedTaskOutcomeRow, PgStore, ProjectionScope, ReputationSnapshotRow,
};
use crate::types::{Membership, NetworkKind, Role};
use crate::{node::Node, task_template::sample_contract};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::{Mutex, OnceLock};

fn env_test_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env_test_mutex() -> std::sync::MutexGuard<'static, ()> {
    env_test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn membership_with_roles(node_ids: &[String]) -> Membership {
    let mut membership = Membership::new();
    for node_id in node_ids {
        for role in [
            Role::Proposer,
            Role::Verifier,
            Role::Committer,
            Role::Finalizer,
        ] {
            membership.grant(node_id, role);
        }
    }
    membership
}

struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let prev = env::var(key).ok();
        // SAFETY: unit tests in this module only mutate a small set of env vars locally.
        unsafe {
            if let Some(value) = value {
                env::set_var(key, value);
            } else {
                env::remove_var(key);
            }
        }
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: unit tests in this module only mutate a small set of env vars locally.
        unsafe {
            if let Some(prev) = &self.prev {
                env::set_var(self.key, prev);
            } else {
                env::remove_var(self.key);
            }
        }
    }
}

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
            content: json!({"text":"hello crew"}),
            reply_to_message_id: None,
        }),
    )
    .expect("signed event");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            bootstrap_peers: vec![
                "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWJ5r1D8N8QYp8JDs7u9mM4rY2kQ6xXK7Z6A6V4t7N3sQX"
                    .to_owned(),
            ],
            enable_mdns: false,
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
                propagation_source: PeerId::random(),
                message: GossipMessage::Chat(EventEnvelope {
                    scope: SwarmScope::Group("crew-7".to_owned()),
                    event: remote_event.clone(),
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
        .list_topic_messages("crew.chat", "group:crew-7", 10)
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
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
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
fn ingest_backfill_response_rejects_scope_mismatch() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let response = crate::network_p2p::BackfillResponse {
        scope: SwarmScope::Global,
        next_from_event_seq: 1,
        feed_key: None,
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
            content: json!({"text":"hello crew"}),
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
            content: json!({"text":"ignore me"}),
            reply_to_message_id: None,
        }),
        11,
    )
    .expect("emit other topic message");

    let response = backfill_response_for_request(
        &node,
        &BackfillRequest {
            scope: SwarmScope::Group("crew-7".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: Some("crew.chat".to_owned()),
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
            content: json!({"text":"cursor me"}),
            reply_to_message_id: None,
        }),
    )
    .expect("remote event");
    let response = crate::network_p2p::BackfillResponse {
        scope: SwarmScope::Group("crew-7".to_owned()),
        next_from_event_seq: 7,
        feed_key: Some("crew.chat".to_owned()),
        events: vec![EventEnvelope {
            scope: SwarmScope::Group("crew-7".to_owned()),
            event: remote_event,
        }],
    };

    ingest_backfill_response(&mut node, &response).expect("apply topic backfill");
    maybe_record_topic_cursor_for_response(&node, &local.node_id(), &response, 100)
        .expect("record topic cursor");

    let cursor = node
        .store
        .get_topic_cursor(&local.node_id(), "crew.chat")
        .expect("get topic cursor")
        .expect("cursor exists");
    assert_eq!(cursor.last_event_seq, 7);
    assert_eq!(cursor.scope_hint, "group:crew-7");
}

#[test]
fn network_config_defaults_to_enabled_with_fixed_tcp_port() {
    let _lock = lock_env_test_mutex();
    let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, None);
    let _mdns = EnvVarGuard::set(ENV_P2P_MDNS, None);
    let _port = EnvVarGuard::set(ENV_P2P_PORT, None);
    let _listen = EnvVarGuard::set(ENV_P2P_LISTEN_ADDRS, None);
    let _bootstrap = EnvVarGuard::set(ENV_P2P_BOOTSTRAP_PEERS, None);
    assert!(network_enabled_from_env());
    let config = network_config_from_env();
    assert!(config.enable_mdns);
    assert_eq!(config.listen_addrs, vec!["/ip4/0.0.0.0/tcp/4001"]);
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
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            bootstrap_peers: vec![
                "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWJ5r1D8N8QYp8JDs7u9mM4rY2kQ6xXK7Z6A6V4t7N3sQX"
                    .to_owned(),
            ],
            enable_mdns: false,
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
                active: false,
            },
        ),
        101,
    )
    .expect("subscription off");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_mdns: false,
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
                active: true,
            },
        ),
        100,
    )
    .expect("subscription event");

    let row = node
        .store
        .get_feed_subscription(&subscriber_node_id, "market.canonical")
        .expect("load subscription")
        .expect("subscription exists");
    assert_eq!(row.scope_hint, "node:lab-9");
    assert_eq!(row.scope(), Some(ProjectionScope::Node("lab-9".to_owned())));
}

#[test]
fn task_announcement_event_persists_summary_and_detail_reference() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-announced-1", policy_hash);
    contract.inputs = json!({"prompt":"lightweight discovery"});
    node.submit_task(contract.clone(), 1, 90)
        .expect("submit task detail");
    node.emit_at(
        1,
        crate::types::EventPayload::TaskAnnounced(crate::types::TaskAnnouncedPayload {
            network_id: "default".to_owned(),
            task_id: "task-announced-1".to_owned(),
            announcement_id: "announce-1".to_owned(),
            feed_key: "venue.market".to_owned(),
            scope_hint: "region:sol-2".to_owned(),
            summary: json!({"reward": 42, "headline": "Explore relay beacon"}),
            detail_ref: Some(crate::types::ArtifactRef {
                uri: "ipfs://task-detail-1".to_owned(),
                digest: "digest-task-detail-1".to_owned(),
                size_bytes: 128,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: node.node_id(),
            }),
        }),
        100,
    )
    .expect("announce task");

    let announcement = node
        .store
        .get_task_announcement("announce-1")
        .expect("load announcement")
        .expect("announcement exists");
    assert_eq!(announcement.task_id, "task-announced-1");
    assert_eq!(announcement.feed_key, "venue.market");
    assert_eq!(announcement.scope_hint, "region:sol-2");
    assert_eq!(
        announcement.scope(),
        Some(ProjectionScope::Region("sol-2".to_owned()))
    );
    assert_eq!(announcement.summary["reward"], json!(42));
    assert_eq!(
        announcement.detail_ref.expect("detail ref").uri,
        "ipfs://task-detail-1"
    );

    let detail = node
        .store
        .get_task_announcement_detail_for_task("task-announced-1")
        .expect("load announcement detail")
        .expect("announcement detail exists");
    assert_eq!(detail.task_id(), "task-announced-1");
    assert_eq!(
        detail.contract.as_ref().expect("task contract").task_id,
        contract.task_id
    );
    assert_eq!(
        detail.detail_ref().expect("detail ref").digest,
        "digest-task-detail-1"
    );
}

#[test]
fn execution_set_events_persist_intent_and_confirmation() {
    let mut intent_node = Node::open_in_memory_with_roles(&[]).expect("intent node");
    let participant_node_id = intent_node.node_id();
    intent_node
        .emit_at(
            1,
            crate::types::EventPayload::ExecutionIntentDeclared(
                crate::types::ExecutionIntentDeclaredPayload {
                    network_id: "default".to_owned(),
                    task_id: "task-execution-1".to_owned(),
                    execution_set_id: "exec-set-1".to_owned(),
                    participant_node_id: participant_node_id.clone(),
                    role_hint: "writer".to_owned(),
                    scope_hint: "region:sol-3".to_owned(),
                    intent: "interested".to_owned(),
                },
            ),
            100,
        )
        .expect("intent event");

    let members = intent_node
        .store
        .list_execution_set_members("task-execution-1", "exec-set-1")
        .expect("members after intent");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].status, "interested");
    assert_eq!(
        members[0].scope(),
        Some(ProjectionScope::Region("sol-3".to_owned()))
    );

    let mut confirmer = Node::open_in_memory_with_roles(&[Role::Committer]).expect("confirmer");
    let confirmed_by_node_id = confirmer.node_id();
    confirmer
        .emit_at(
            1,
            crate::types::EventPayload::ExecutionSetConfirmed(
                crate::types::ExecutionSetConfirmedPayload {
                    network_id: "default".to_owned(),
                    task_id: "task-execution-1".to_owned(),
                    execution_set_id: "exec-set-1".to_owned(),
                    confirmed_by_node_id: confirmed_by_node_id.clone(),
                    scope_hint: "region:sol-3".to_owned(),
                    members: vec![crate::types::ExecutionSetMember {
                        participant_node_id: "peer-a".to_owned(),
                        role_hint: "writer".to_owned(),
                    }],
                },
            ),
            101,
        )
        .expect("confirmation event");

    let members = confirmer
        .store
        .list_execution_set_members("task-execution-1", "exec-set-1")
        .expect("members after confirmation");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].status, "confirmed");
    assert_eq!(
        members[0].scope(),
        Some(ProjectionScope::Region("sol-3".to_owned()))
    );
    assert_eq!(
        members[0].confirmed_by_node_id.as_deref(),
        Some(confirmed_by_node_id.as_str())
    );
}

#[test]
fn rule_and_checkpoint_gossip_are_applied_to_projection_store() {
    let mut node = Node::open_in_memory_with_roles(&[]).expect("node");

    apply_rule_announcement(
        &mut node,
        &crate::network_p2p::RuleAnnouncement {
            scope: SwarmScope::Region("sol-4".to_owned()),
            rule_set: "market-routing".to_owned(),
            rule_version: 3,
            activation_epoch: Some(9),
        },
    )
    .expect("apply rule");
    apply_checkpoint_announcement(
        &mut node,
        &crate::network_p2p::CheckpointAnnouncement {
            scope: SwarmScope::Region("sol-4".to_owned()),
            checkpoint_id: "cp-sol-4".to_owned(),
            artifact_path: "ipfs://checkpoint-sol-4".to_owned(),
        },
    )
    .expect("apply checkpoint");

    let rule = node
        .store
        .latest_rule_announcement("region.sol-4", "market-routing")
        .expect("load rule")
        .expect("rule exists");
    assert_eq!(rule.rule_version, 3);
    assert_eq!(rule.activation_epoch, Some(9));

    let checkpoint = node
        .store
        .get_checkpoint_announcement("region.sol-4", "cp-sol-4")
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.artifact_path, "ipfs://checkpoint-sol-4");
}

#[test]
fn network_config_reads_bootstrap_peers_from_env() {
    let _lock = lock_env_test_mutex();
    let _bootstrap = EnvVarGuard::set(
        ENV_P2P_BOOTSTRAP_PEERS,
        Some("/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWJ5r1D8N8QYp8JDs7u9mM4rY2kQ6xXK7Z6A6V4t7N3sQX"),
    );
    let config = network_config_from_env();
    assert_eq!(config.bootstrap_peers.len(), 1);
    assert_eq!(
        config.bootstrap_peers[0],
        "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWJ5r1D8N8QYp8JDs7u9mM4rY2kQ6xXK7Z6A6V4t7N3sQX"
    );
}

#[test]
fn global_publish_rate_guard_limits_only_high_frequency_global_events() {
    let mut guard = GlobalPublishRateGuard::new(Instant::now());
    let event = build_event_for_external(
        &NodeIdentity::random(),
        1,
        10,
        crate::types::EventPayload::TaskClaimed(crate::types::ClaimPayload {
            task_id: "task-rate-limit".to_owned(),
            role: crate::types::ClaimRole::Propose,
            claimer_node_id: "node-a".to_owned(),
            execution_id: "exec-1".to_owned(),
            lease_until: 20,
        }),
    )
    .expect("event");

    for _ in 0..GLOBAL_HIGH_FREQUENCY_LIMIT {
        assert!(guard.allow(&SwarmScope::Global, &event));
    }
    assert!(!guard.allow(&SwarmScope::Global, &event));
    assert!(guard.allow(&SwarmScope::Node("lab-1".to_owned()), &event));
}

#[test]
fn smarter_backfill_prefers_peer_with_known_scope_activity() {
    let node_a = PeerId::random();
    let node_b = PeerId::random();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let other_scope = SwarmScope::Node("lab-2".to_owned());
    let now = Instant::now();

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(node_a);
    service.connected_peers.insert(node_b);

    let mut state_a = PeerSyncState::new(now - Duration::from_secs(30));
    state_a.known_scopes.insert(other_scope);
    let mut state_b = PeerSyncState::new(now - Duration::from_secs(30));
    state_b.known_scopes.insert(target_scope.clone());
    service.peer_sync_state.insert(node_a, state_a);
    service.peer_sync_state.insert(node_b, state_b);

    assert_eq!(
        service.preferred_backfill_peer_for_scope(&target_scope, now),
        Some(node_b)
    );
}

#[test]
fn connection_closed_with_remaining_established_keeps_peer_state() {
    let peer = PeerId::random();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(peer);
    service
        .peer_sync_state
        .insert(peer, PeerSyncState::new(Instant::now()));

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer,
                remaining_established: 1,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::TransportNotice { .. }));
    assert!(service.connected_peers.contains(&peer));
    assert!(service.peer_sync_state.contains_key(&peer));
}

#[test]
fn connection_closed_with_zero_remaining_removes_peer_state() {
    let peer = PeerId::random();
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("service");
    service.connected_peers.insert(peer);
    service
        .peer_sync_state
        .insert(peer, PeerSyncState::new(Instant::now()));

    let tick = service
        .process_runtime_event(
            &mut node,
            NetworkRuntimeEvent::ConnectionClosed {
                peer,
                remaining_established: 0,
            },
        )
        .expect("process event");

    assert!(matches!(tick, NetworkBridgeTick::Disconnected { peer: seen } if seen == peer));
    assert!(!service.connected_peers.contains(&peer));
    assert!(!service.peer_sync_state.contains_key(&peer));
}

#[test]
fn scopes_to_request_for_peer_falls_back_to_all_scopes_until_peer_is_profiled() {
    let peer = PeerId::random();
    let target_scope = SwarmScope::Region("sol-1".to_owned());
    let service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global, target_scope.clone()],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    assert_eq!(
        service.scopes_to_request_for_peer(&peer),
        vec![SwarmScope::Global, target_scope]
    );
}

#[test]
fn local_scope_aliases_map_to_node_scope() {
    let policy_hash = "policy-hash-alias".to_owned();
    let mut contract = sample_contract("task-local-alias", policy_hash);
    contract.inputs = json!({"swarm_scope":" local:lab-1 "});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-1".to_owned())
    );

    contract.inputs = json!({"swarm_scope":{"kind":"local","id":"lab-2"}});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-2".to_owned())
    );

    contract.inputs = json!({});
    contract.task_type = "local:lab-3:swarm".to_owned();
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-3".to_owned())
    );
}

#[test]
fn parse_scope_hint_string_preserves_task_type_style_prefix_fallback() {
    assert_eq!(
        scope::parse_scope_hint_string("region:sol-1:swarm"),
        Some(SwarmScope::Region("sol-1".to_owned()))
    );
    assert_eq!(
        scope::parse_scope_hint_string(" local:lab-7:worker "),
        Some(SwarmScope::Node("lab-7".to_owned()))
    );
    assert_eq!(
        scope::parse_scope_hint_string(" group:crew-7:worker "),
        Some(SwarmScope::Group("crew-7".to_owned()))
    );
}

#[test]
fn contract_scope_object_uses_shared_canonical_scope_rules() {
    let mut contract = sample_contract("task-object-scope", "policy-hash-object".to_owned());
    contract.inputs = json!({"swarm_scope":{"kind":" local ","id":" lab-8 "}});
    assert_eq!(
        scope::contract_scope(&contract),
        SwarmScope::Node("lab-8".to_owned())
    );
}

#[test]
fn backfill_response_filters_by_scope() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut global_contract = sample_contract("task-global-backfill", policy_hash.clone());
    global_contract.inputs = json!({"prompt":"global"});
    node.submit_task(global_contract, 1, 10)
        .expect("submit global");

    let mut region_contract = sample_contract("task-region-backfill", policy_hash);
    region_contract.task_type = "region:sol-1:swarm".to_owned();
    region_contract.inputs = json!({"prompt":"region", "swarm_scope":"region:sol-1"});
    node.submit_task(region_contract, 1, 11)
        .expect("submit region");

    let response = backfill_response_for_request(
        &node,
        &BackfillRequest {
            scope: SwarmScope::Region("sol-1".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
        },
        32,
        64,
    )
    .expect("backfill response");

    assert_eq!(response.events.len(), 1);
    assert_eq!(
        response.events[0].scope,
        SwarmScope::Region("sol-1".to_owned())
    );
    assert_eq!(
        response.events[0].event.task_id.as_deref(),
        Some("task-region-backfill")
    );
}

#[test]
fn backfill_response_skips_network_substrate_events_for_other_networks() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let node = Node::new(
        identity.clone(),
        PgStore::open_in_memory().expect("store"),
        membership,
    )
    .expect("node");

    let mismatched_event = build_event_for_external(
        &identity,
        1,
        100,
        crate::types::EventPayload::FeedSubscriptionUpdated(
            crate::types::FeedSubscriptionUpdatedPayload {
                network_id: "mainnet:other".to_owned(),
                subscriber_node_id: node_id,
                feed_key: "market.alpha".to_owned(),
                scope_hint: "region:sol-1".to_owned(),
                active: true,
            },
        ),
    )
    .expect("event");
    node.store
        .append_event(&mismatched_event)
        .expect("append event");

    let response = backfill_response_for_request(
        &node,
        &BackfillRequest {
            scope: SwarmScope::Region("sol-1".to_owned()),
            from_event_seq: 0,
            limit: 8,
            feed_key: None,
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(response.events.is_empty());
}

#[test]
fn global_backfill_skips_task_process_layer_events() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-global-process-backfill", policy_hash);
    contract.inputs = json!({"prompt":"backfill without process"});
    node.submit_task(contract, 1, 100).expect("submit task");
    node.claim_task(
        "task-global-process-backfill",
        crate::types::ClaimRole::Propose,
        "exec-backfill-1",
        500,
        1,
        101,
    )
    .expect("claim task");

    let response = backfill_response_for_request(
        &node,
        &BackfillRequest {
            scope: SwarmScope::Global,
            from_event_seq: 0,
            limit: 16,
            feed_key: None,
        },
        32,
        64,
    )
    .expect("backfill response");

    assert!(
        response.events.iter().any(|envelope| matches!(
            envelope.event.payload,
            crate::types::EventPayload::TaskCreated(_)
        )),
        "global backfill should still include task detail events"
    );
    assert!(
        response.events.iter().all(|envelope| {
            !matches!(
                envelope.event.payload,
                crate::types::EventPayload::TaskClaimed(_)
                    | crate::types::EventPayload::TaskClaimRenewed(_)
                    | crate::types::EventPayload::TaskClaimReleased(_)
                    | crate::types::EventPayload::CandidateProposed(_)
                    | crate::types::EventPayload::EvidenceAdded(_)
                    | crate::types::EventPayload::EvidenceAvailable(_)
                    | crate::types::EventPayload::VerifierResultSubmitted(_)
                    | crate::types::EventPayload::VoteCommit(_)
                    | crate::types::EventPayload::VoteReveal(_)
                    | crate::types::EventPayload::DecisionCommitted(_)
                    | crate::types::EventPayload::TaskError(_)
                    | crate::types::EventPayload::TaskRetryScheduled(_)
            )
        }),
        "global backfill should suppress task process traffic"
    );
}

#[test]
fn summary_publish_is_suppressed_when_backlog_is_high() {
    assert!(should_publish_summaries(
        SUMMARY_BACKPRESSURE_HIGH_WATERMARK,
        0
    ));
    assert!(!should_publish_summaries(
        SUMMARY_BACKPRESSURE_HIGH_WATERMARK + 1,
        0
    ));
}

#[test]
fn apply_summary_announcement_imports_knowledge_and_reputation() {
    let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Region("sol-1".to_owned()),
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-a".to_owned(),
                task_type: "summary-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-knowledge-summary".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-1".to_owned(),
                    winning_candidate_hash: "candidate-hash-1".to_owned(),
                    output_digest: "output-digest-1".to_owned(),
                    result_summary: json!({"answer":"summary-import"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"ok"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "summary-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 100,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply knowledge summary");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-reputation-1".to_owned(),
            source_node_id: "node-a".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::ReputationSummaryBundle {
                source_node_id: "node-a".to_owned(),
                entries: vec![crate::storage::ReputationSnapshotRow {
                    runtime_id: "runtime-a".to_owned(),
                    profile_id: "model-a".to_owned(),
                    stability_reputation: 10,
                    quality_reputation: 20,
                    last_updated_at: 101,
                }],
            })
            .expect("reputation payload"),
        },
    )
    .expect("apply reputation summary");

    let hits = node
        .store
        .list_decision_memory_hits_by_task_type("summary-type", 8)
        .expect("list hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].result_summary["answer"], json!("summary-import"));

    let reputation = node
        .store
        .get_reputation_snapshot("runtime-a", "model-a")
        .expect("get reputation")
        .expect("reputation row");
    assert_eq!(reputation.stability_reputation, 10);
    assert_eq!(reputation.quality_reputation, 20);
}

#[test]
fn subnet_summary_is_mirrored_into_parent_network_store() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let base_store = PgStore::open_in_memory().expect("store");
    let mainnet = base_store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = base_store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let subnet_store = base_store.for_org(&subnet.org.org_id);
    let mut node = Node::new(identity, subnet_store, membership).expect("subnet node");
    node.store
        .put_decision_memory(
            "task-parent-uplink",
            1,
            "commit-parent-uplink",
            100,
            "candidate-parent-uplink",
            "digest-parent-uplink",
            &json!({"answer":"compress-me"}),
            &json!({"quorum":"ok"}),
            &[100],
            &json!({"detail":"ok"}),
            "policy-snap",
            "uplink-type",
            "input-digest",
            "schema-digest",
            "vp.schema_only.v1",
            "params-digest",
        )
        .expect("put decision memory");

    let summary = build_knowledge_summary_for_task_type(&node, &SwarmScope::Global, "uplink-type")
        .expect("build summary")
        .expect("summary exists");
    assert!(
        mirror_summary_to_parent_network(&node, &summary).expect("mirror summary"),
        "subnet summary should mirror to parent network"
    );

    let parent_store = base_store.for_org(&mainnet.org.org_id);
    let hits = parent_store
        .list_decision_memory_hits_by_task_type("uplink-type", 8)
        .expect("list mirrored hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].result_summary["answer"], json!("compress-me"));

    let revoke_event = node
        .revoke_summary(
            &summary.summary_id,
            KNOWLEDGE_SUMMARY_KIND,
            "bad subnet summary",
            1,
            200,
        )
        .expect("revoke summary");
    assert!(
        mirror_summary_controls_to_parent_network(&node, &revoke_event)
            .expect("mirror revoke to parent"),
        "summary revoke should mirror to parent network"
    );
    assert!(
        parent_store
            .list_decision_memory_hits_by_task_type("uplink-type", 8)
            .expect("list mirrored hits after revoke")
            .is_empty()
    );
}

#[test]
fn task_outcome_summary_imports_compressed_result_facts() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let store = PgStore::open_in_memory().expect("store");
    let mut node = Node::new(identity.clone(), store, membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-outcome-summary", policy_hash);
    contract.task_type = "cross-layer-type".to_owned();
    node.store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-outcome".to_owned(),
        execution_id: "exec-outcome".to_owned(),
        output: json!({
            "answer": "compressed answer",
            "decision": "approve",
            "confidence": 0.98,
            "check_summary": "ok",
            "raw": {"large":"payload"}
        }),
        evidence_inline: vec![],
        evidence_refs: vec![crate::types::ArtifactRef {
            uri: "ipfs://evidence-1".to_owned(),
            digest: "sha256:evidence-1".to_owned(),
            size_bytes: 32,
            mime: "application/json".to_owned(),
            created_at: 10,
            producer: node_id.clone(),
        }],
    };
    node.store
        .put_candidate(&contract.task_id, &node_id, &candidate)
        .expect("put candidate");
    let winning_candidate_hash = crate::crypto::candidate_hash(&candidate).expect("candidate hash");
    let finalized = build_event_for_external(
        &identity,
        1,
        100,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: contract.task_id.clone(),
            epoch: 1,
            candidate_id: candidate.candidate_id.clone(),
            winning_candidate_hash,
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: vec![crate::types::SignatureEnvelope {
                    signer_node_id: node_id.clone(),
                    signature_hex: "sig-1".to_owned(),
                }],
            },
        }),
    )
    .expect("finalized event");

    let summary = summary::task_outcome_summary_for_event(
        &node,
        &finalized,
        &SwarmScope::Region("sol-1".to_owned()),
    )
    .expect("build outcome summary")
    .expect("outcome summary exists");
    apply_summary_announcement(&mut node, &summary).expect("apply outcome summary");

    let outcomes = node
        .store
        .list_imported_task_outcomes_by_task_type("cross-layer-type", 8)
        .expect("list imported outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].task_id, "task-outcome-summary");
    assert_eq!(outcomes[0].candidate_id, "cand-outcome");
    assert_eq!(
        outcomes[0].result_summary["answer"],
        json!("compressed answer")
    );
    assert_eq!(outcomes[0].result_summary["check_summary"], json!("ok"));
    assert_eq!(outcomes[0].evidence_digest_count, 1);
    assert_eq!(outcomes[0].scope_hint, "region:sol-1");
}

#[test]
fn subnet_outcome_summary_and_checkpoint_anchor_mirror_to_parent_network() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let base_store = PgStore::open_in_memory().expect("store");
    let mainnet = base_store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = base_store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let subnet_store = base_store.for_org(&subnet.org.org_id);
    let mut node = Node::new(identity.clone(), subnet_store, membership).expect("subnet node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-parent-outcome", policy_hash);
    contract.task_type = "parent-outcome-type".to_owned();
    node.store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    let candidate = crate::types::Candidate {
        candidate_id: "cand-parent-outcome".to_owned(),
        execution_id: "exec-parent-outcome".to_owned(),
        output: json!({"answer":"uplinked","decision":"approve","check_summary":"compressed"}),
        evidence_inline: vec![],
        evidence_refs: vec![],
    };
    node.store
        .put_candidate(&contract.task_id, &node_id, &candidate)
        .expect("put candidate");
    let winning_candidate_hash = crate::crypto::candidate_hash(&candidate).expect("candidate hash");
    let finalized = build_event_for_external(
        &identity,
        1,
        120,
        crate::types::EventPayload::DecisionFinalized(crate::types::DecisionFinalizedPayload {
            task_id: contract.task_id.clone(),
            epoch: 1,
            candidate_id: candidate.candidate_id.clone(),
            winning_candidate_hash,
            finality_proof: crate::types::FinalityProof {
                threshold: 1,
                signatures: vec![crate::types::SignatureEnvelope {
                    signer_node_id: node_id.clone(),
                    signature_hex: "sig-parent".to_owned(),
                }],
            },
        }),
    )
    .expect("finalized event");
    let scope = SwarmScope::Global;
    let summary = summary::task_outcome_summary_for_event(&node, &finalized, &scope)
        .expect("build outcome summary")
        .expect("outcome summary exists");
    let checkpoint = announcements::checkpoint_announcement_for_event(&node, &finalized, &scope)
        .expect("build checkpoint announcement")
        .expect("checkpoint announcement exists");

    assert!(
        mirror_summary_to_parent_network(&node, &summary).expect("mirror outcome summary"),
        "subnet outcome summary should mirror to parent network"
    );
    assert!(
        announcements::mirror_checkpoint_to_parent_network(&node, &checkpoint)
            .expect("mirror checkpoint"),
        "subnet checkpoint anchor should mirror to parent network"
    );

    let parent_store = base_store.for_org(&mainnet.org.org_id);
    let outcomes = parent_store
        .list_imported_task_outcomes_by_task_type("parent-outcome-type", 8)
        .expect("list mirrored outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].result_summary["answer"], json!("uplinked"));
    assert_eq!(outcomes[0].checkpoint_id, checkpoint.checkpoint_id);
    let mirrored_checkpoint = parent_store
        .get_checkpoint_announcement("global", &checkpoint.checkpoint_id)
        .expect("load mirrored checkpoint")
        .expect("mirrored checkpoint exists");
    assert_eq!(mirrored_checkpoint.artifact_path, checkpoint.artifact_path);

    let revoke_event = node
        .revoke_summary(
            &summary.summary_id,
            TASK_OUTCOME_SUMMARY_KIND,
            "bad outcome summary",
            1,
            200,
        )
        .expect("revoke outcome summary");
    assert!(
        mirror_summary_controls_to_parent_network(&node, &revoke_event)
            .expect("mirror outcome revoke to parent"),
        "outcome summary revoke should mirror to parent network"
    );
    assert!(
        parent_store
            .list_imported_task_outcomes_by_task_type("parent-outcome-type", 8)
            .expect("list mirrored outcomes after revoke")
            .is_empty()
    );
}

#[test]
fn knowledge_summary_builder_respects_protocol_limit() {
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    for task_id in ["task-dm-1", "task-dm-2"] {
        node.store
            .put_decision_memory(
                task_id,
                1,
                &format!("commit-{task_id}"),
                100,
                &format!("candidate-{task_id}"),
                &format!("digest-{task_id}"),
                &json!({"quorum":"ok"}),
                &json!({"answer": task_id}),
                &[100],
                &json!({"detail":"ok"}),
                "policy-snap",
                "limit-type",
                "input-digest",
                "schema-digest",
                "vp.schema_only.v1",
                "params-digest",
            )
            .expect("put decision memory");
    }

    let summary = summary::build_knowledge_summary_for_task_type_with_limit(
        &node,
        &SwarmScope::Global,
        "limit-type",
        1,
    )
    .expect("build summary")
    .expect("summary exists");
    let payload: summary::KnowledgeSummaryBundle =
        serde_json::from_value(summary.payload).expect("decode payload");

    assert_eq!(payload.decisions.len(), 1);
    assert!(matches!(
        payload.decisions[0].task_id.as_str(),
        "task-dm-1" | "task-dm-2"
    ));
}

#[test]
fn service_initializes_summary_limits_from_protocol_params() {
    let params = NetworkProtocolParams {
        summary_reputation_limit: 1,
        summary_decision_memory_limit: 2,
        ..NetworkProtocolParams::default()
    };
    let service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig::default()).expect("node"),
        &[SwarmScope::Global],
        &params,
    )
    .expect("service");
    let node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
    node.store
        .put_reputation_snapshot("runtime-a", "model-a", 10, 20, 100)
        .expect("put reputation a");
    node.store
        .put_reputation_snapshot("runtime-b", "model-b", 30, 40, 200)
        .expect("put reputation b");

    assert_eq!(service.summary_reputation_limit, 1);
    assert_eq!(service.summary_decision_memory_limit, 2);
    assert_eq!(
        node.store
            .list_local_reputation_snapshots(service.summary_reputation_limit)
            .expect("list local reputation")
            .len(),
        1
    );
}

#[test]
fn observability_snapshot_reports_network_and_sync_health() {
    let identity = NodeIdentity::random();
    let node_id = identity.node_id();
    let store = PgStore::open_in_memory().expect("store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            &node_id,
            &node_id,
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            &node_id,
            &node_id,
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let membership = membership_with_roles(std::slice::from_ref(&node_id));
    let node = Node::new(
        identity,
        store.for_org(subnet.org.org_id.clone()),
        membership,
    )
    .expect("subnet node");

    node.store
        .put_imported_decision_memory(
            "summary-knowledge",
            "peer-a",
            &DecisionMemoryHitRow {
                task_id: "task-observe".to_owned(),
                epoch: 1,
                final_commit_hash: "commit-hash".to_owned(),
                winning_candidate_hash: "candidate-hash".to_owned(),
                output_digest: "output-digest".to_owned(),
                result_summary: json!({"answer":"ok"}),
                quorum_result: json!({"quorum":"pass"}),
                reason_codes: vec![100],
                reason_details: json!({"detail":"good"}),
                policy_snapshot_digest: "policy-snapshot".to_owned(),
                input_digest: "input-digest".to_owned(),
                output_schema_digest: "schema-digest".to_owned(),
                policy_id: "vp.schema_only.v1".to_owned(),
                task_type: "resume_review".to_owned(),
                policy_params_digest: "params-digest".to_owned(),
                deprecated_as_exact: false,
                finalized_at: 10,
                confidence_hint: 0.9,
            },
        )
        .expect("import decision memory");
    node.store
        .put_imported_reputation_snapshot(
            "summary-reputation",
            "peer-a",
            &ReputationSnapshotRow {
                runtime_id: "stub".to_owned(),
                profile_id: "p1".to_owned(),
                stability_reputation: 5,
                quality_reputation: 7,
                last_updated_at: 11,
            },
        )
        .expect("import reputation");
    node.store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-outcome".to_owned(),
            source_node_id: "peer-a".to_owned(),
            scope_hint: "global".to_owned(),
            task_id: "task-observe".to_owned(),
            task_type: "resume_review".to_owned(),
            candidate_id: "cand-1".to_owned(),
            output_digest: "digest-1".to_owned(),
            result_summary: json!({"answer":"ok"}),
            evidence_digest_count: 1,
            checkpoint_id: "cp-1".to_owned(),
            proof_artifact_path: "ipfs://cp-1".to_owned(),
            finalized_at: 12,
            revoked: false,
        })
        .expect("import task outcome");
    node.store
        .put_checkpoint_announcement("global", "cp-local", "ipfs://cp-local", 13)
        .expect("checkpoint");
    node.store
        .upsert_execution_set_member(
            "task-observe",
            "exec-1",
            "peer-a",
            "worker",
            "region:sol-1",
            "confirmed",
            Some("peer-a"),
            14,
        )
        .expect("execution set");

    let parent_store = store.for_org(mainnet.org.org_id.clone());
    parent_store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-parent".to_owned(),
            source_node_id: node.node_id(),
            scope_hint: "global".to_owned(),
            task_id: "task-parent".to_owned(),
            task_type: "resume_review".to_owned(),
            candidate_id: "cand-parent".to_owned(),
            output_digest: "digest-parent".to_owned(),
            result_summary: json!({"answer":"uplink"}),
            evidence_digest_count: 0,
            checkpoint_id: "cp-parent".to_owned(),
            proof_artifact_path: "ipfs://cp-parent".to_owned(),
            finalized_at: 15,
            revoked: false,
        })
        .expect("parent outcome");
    parent_store
        .put_checkpoint_announcement("global", "cp-parent", "ipfs://cp-parent", 16)
        .expect("parent checkpoint");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global, SwarmScope::Region("sol-1".to_owned())],
        &NetworkProtocolParams::default(),
    )
    .expect("service");

    let peer = PeerId::random();
    service.connected_peers.insert(peer);
    let mut peer_state = PeerSyncState::new(Instant::now());
    peer_state.known_scopes.insert(SwarmScope::Global);
    peer_state
        .known_scopes
        .insert(SwarmScope::Region("sol-1".to_owned()));
    peer_state.inflight_backfills = 1;
    peer_state.next_retry_at = Instant::now() + Duration::from_secs(5);
    service.peer_sync_state.insert(peer, peer_state);
    service.scope_traffic.insert(
        SwarmScope::Global,
        ScopeTrafficStats {
            published_events: 2,
            ingested_events: 3,
            summaries_applied: 1,
            rules_applied: 0,
            checkpoints_applied: 1,
            backfills_applied: 1,
            backfill_events_applied: 4,
        },
    );

    let snapshot = service
        .observability_snapshot(&node)
        .expect("observability snapshot");

    assert_eq!(snapshot.connected_peer_count, 1);
    assert_eq!(snapshot.nat_status, "unknown");
    assert_eq!(snapshot.nat_public_address, None);
    assert_eq!(snapshot.nat_confidence, 0);
    assert!(snapshot.relay_reservations.is_empty());
    assert_eq!(snapshot.peer_health.len(), 1);
    assert!(snapshot.peer_health[0].connected);
    assert!(!snapshot.peer_health[0].blacklisted);
    assert_eq!(snapshot.peer_health[0].score, 0);
    assert_eq!(snapshot.peer_health[0].reputation_tier, "healthy");
    assert!(!snapshot.peer_health[0].quarantined);
    assert_eq!(snapshot.peer_health[0].quarantine_remaining_ms, 0);
    assert_eq!(snapshot.peer_health[0].ban_remaining_ms, 0);
    assert_eq!(snapshot.peer_health[0].throttle_factor_percent, 100);
    assert_eq!(
        snapshot.peer_health[0].known_scopes,
        vec!["global".to_owned(), "region:sol-1".to_owned()]
    );
    assert_eq!(snapshot.scope_traffic.len(), 1);
    assert_eq!(snapshot.scope_traffic[0].scope, "global");
    assert_eq!(snapshot.scope_traffic[0].stats.published_events, 2);
    assert_eq!(snapshot.summary_health.imported_decision_memory_rows, 1);
    assert_eq!(snapshot.summary_health.imported_reputation_rows, 1);
    assert_eq!(snapshot.summary_health.imported_task_outcome_rows, 1);
    assert_eq!(snapshot.summary_health.checkpoint_rows, 1);
    assert_eq!(
        snapshot.subnet_sync_health.network_kind,
        NetworkKind::Subnet.as_str()
    );
    assert_eq!(
        snapshot.subnet_sync_health.parent_network_id.as_deref(),
        Some(mainnet.network.network_id.as_str())
    );
    assert!(snapshot.subnet_sync_health.parent_uplink_available);
    assert_eq!(
        snapshot
            .subnet_sync_health
            .parent_imported_task_outcome_rows,
        Some(1)
    );
    assert_eq!(snapshot.subnet_sync_health.parent_checkpoint_rows, Some(1));
    assert_eq!(snapshot.execution_set_health.execution_set_count, 1);
    assert_eq!(snapshot.execution_set_health.execution_set_member_count, 1);
}

#[test]
fn summary_revocation_and_penalty_remove_imported_state() {
    let mut node =
        Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-2".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                task_type: "revoked-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-revoked-summary".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-revoked".to_owned(),
                    winning_candidate_hash: "candidate-revoked".to_owned(),
                    output_digest: "output-revoked".to_owned(),
                    result_summary: json!({"answer":"bad summary"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"bad"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 100,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply knowledge summary");
    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-reputation-2".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: REPUTATION_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::ReputationSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                entries: vec![crate::storage::ReputationSnapshotRow {
                    runtime_id: "runtime-bad".to_owned(),
                    profile_id: "model-bad".to_owned(),
                    stability_reputation: 999,
                    quality_reputation: 888,
                    last_updated_at: 101,
                }],
            })
            .expect("reputation payload"),
        },
    )
    .expect("apply reputation summary");

    assert_eq!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions")
            .len(),
        1
    );
    assert!(
        node.store
            .get_reputation_snapshot("runtime-bad", "model-bad")
            .expect("get reputation")
            .is_some()
    );

    node.revoke_summary(
        "summary-knowledge-2",
        KNOWLEDGE_SUMMARY_KIND,
        "bad knowledge",
        1,
        200,
    )
    .expect("revoke summary");
    assert!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions after revoke")
            .is_empty()
    );

    node.penalize_node(
        "node-bad",
        "malicious source",
        Vec::new(),
        Vec::new(),
        1,
        201,
    )
    .expect("penalize node");
    assert!(
        node.store
            .get_reputation_snapshot("runtime-bad", "model-bad")
            .expect("get reputation after penalty")
            .is_none()
    );

    apply_summary_announcement(
        &mut node,
        &SummaryAnnouncement {
            summary_id: "summary-knowledge-3".to_owned(),
            source_node_id: "node-bad".to_owned(),
            scope: SwarmScope::Global,
            summary_kind: KNOWLEDGE_SUMMARY_KIND.to_owned(),
            artifact_path: None,
            payload: serde_json::to_value(summary::KnowledgeSummaryBundle {
                source_node_id: "node-bad".to_owned(),
                task_type: "revoked-type".to_owned(),
                decisions: vec![crate::storage::DecisionMemoryHitRow {
                    task_id: "task-revoked-summary-2".to_owned(),
                    epoch: 1,
                    final_commit_hash: "commit-revoked-2".to_owned(),
                    winning_candidate_hash: "candidate-revoked-2".to_owned(),
                    output_digest: "output-revoked-2".to_owned(),
                    result_summary: json!({"answer":"should be ignored"}),
                    quorum_result: json!({"quorum":"ok"}),
                    reason_codes: vec![100],
                    reason_details: json!({"detail":"ignored"}),
                    policy_snapshot_digest: "policy-snap".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    output_schema_digest: "schema-digest".to_owned(),
                    policy_id: "vp.schema_only.v1".to_owned(),
                    task_type: "revoked-type".to_owned(),
                    policy_params_digest: "params-digest".to_owned(),
                    deprecated_as_exact: false,
                    finalized_at: 102,
                    confidence_hint: 0.5,
                }],
            })
            .expect("knowledge payload"),
        },
    )
    .expect("apply blocked summary");
    assert!(
        node.store
            .list_decision_memory_hits_by_task_type("revoked-type", 8)
            .expect("list decisions after penalty")
            .is_empty()
    );
}

#[test]
fn revoked_event_rebuild_removes_previous_projection_state() {
    let mut node =
        Node::open_in_memory_with_roles(&[Role::Proposer, Role::Finalizer]).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut contract = sample_contract("task-revoked-event", policy_hash);
    contract.inputs = json!({"prompt":"revoke me"});
    let created = node.submit_task(contract, 1, 100).expect("submit task");
    assert!(
        node.task_view("task-revoked-event")
            .expect("task view")
            .is_some()
    );

    node.revoke_event(&created.event_id, "malicious task", 1, 101)
        .expect("revoke event");

    assert!(
        node.task_view("task-revoked-event")
            .expect("task view after revoke")
            .is_none()
    );
}

#[test]
fn publish_pending_global_events_publishes_local_rows_and_skips_remote_rows() {
    let local = NodeIdentity::random();
    let local_node_id = local.node_id();
    let remote = NodeIdentity::random();
    let membership = membership_with_roles(&[local_node_id.clone(), remote.node_id()]);
    let mut node =
        Node::new(local, PgStore::open_in_memory().expect("store"), membership).expect("node");
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))
        .expect("policy binding")
        .policy_hash;
    let mut local_contract = sample_contract("task-publish-local", policy_hash.clone());
    local_contract.inputs = json!({"prompt":"publish me"});
    node.submit_task(local_contract, 1, 100)
        .expect("local task");

    let remote_event = build_event_for_external(
        &remote,
        1,
        101,
        crate::types::EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
            checkpoint_id: "cp-remote".to_owned(),
            up_to_seq: 1,
        }),
    )
    .expect("remote event");
    node.ingest_remote(remote_event).expect("ingest remote");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("network service");
    let mut peer_node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("peer node");
    let mut peer_service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("peer network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("peer network service");

    for _ in 0..4_096 {
        let _ = service.try_tick(&mut node).expect("service tick");
        let _ = peer_service
            .try_tick(&mut peer_node)
            .expect("peer service tick");
        if !service.listen_addrs().is_empty() && !peer_service.listen_addrs().is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    service
        .dial(peer_service.listen_addrs()[0].clone())
        .expect("dial peer");
    let mut connected = false;
    for _ in 0..4_096 {
        if let Some(NetworkBridgeTick::Connected { .. }) =
            service.try_tick(&mut node).expect("service tick")
        {
            connected = true;
        }
        let _ = peer_service
            .try_tick(&mut peer_node)
            .expect("peer service tick");
        if connected {
            break;
        }
        std::thread::yield_now();
    }
    assert!(connected);

    let mut last = 0;
    for _ in 0..4_096 {
        last = publish_pending_global_events(&mut service, &node, &local_node_id, 0)
            .expect("publish pending");
        if last == 2 {
            break;
        }
        let _ = service.try_tick(&mut node).expect("service tick");
        let _ = peer_service
            .try_tick(&mut peer_node)
            .expect("peer service tick");
        std::thread::yield_now();
    }
    assert_eq!(last, 2);
}

#[test]
fn dial_discovered_peer_endpoints_skips_invalid_self_and_missing_addrs() {
    let dir = std::env::temp_dir().join(format!(
        "wattswarm-network-bridge-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    crate::control::save_discovered_peer_records(
        &crate::control::discovered_peers_path(&dir),
        &[
            crate::control::DiscoveredPeerRecord {
                node_id: "self".to_owned(),
                listen_addr: Some("/ip4/127.0.0.1/tcp/4001".to_owned()),
            },
            crate::control::DiscoveredPeerRecord {
                node_id: "peer-a".to_owned(),
                listen_addr: None,
            },
            crate::control::DiscoveredPeerRecord {
                node_id: "peer-b".to_owned(),
                listen_addr: Some("not-a-multiaddr".to_owned()),
            },
        ],
    )
    .expect("save discovered peers");

    let mut service = NetworkBridgeService::new(
        NetworkP2pNode::generate(NetworkP2pConfig {
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_owned()],
            enable_mdns: false,
            ..NetworkP2pConfig::default()
        })
        .expect("network node"),
        &[SwarmScope::Global],
        &NetworkProtocolParams::default(),
    )
    .expect("network service");

    let mut attempts = HashMap::new();
    let dialed = dial_discovered_peer_endpoints(&mut service, &dir, "self", &mut attempts)
        .expect("dial discovered peers");
    assert_eq!(dialed, 0);
    assert!(attempts.is_empty());
    let _ = std::fs::remove_dir_all(dir);
}
