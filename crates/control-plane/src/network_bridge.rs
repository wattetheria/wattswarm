use anyhow::{Result, anyhow, bail};

use crate::network_p2p::{
    BackfillRequest, BackfillRequestId, BackfillResponse, EventEnvelope, GossipMessage, Multiaddr,
    NetworkP2pConfig, NetworkP2pNode, NetworkRuntime, NetworkRuntimeEvent, PeerId, SwarmScope,
};
use crate::node::Node;
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

pub fn ingest_event_envelope(node: &mut Node, envelope: &EventEnvelope) -> Result<()> {
    node.ingest_remote(envelope.event.clone())
}

pub fn event_envelope_from_gossip(message: &GossipMessage) -> Result<&EventEnvelope> {
    match message {
        GossipMessage::Event(envelope) => Ok(envelope),
        _ => bail!("gossip message is not an event payload"),
    }
}

pub fn global_event_gossip(envelope: EventEnvelope) -> Result<GossipMessage> {
    if envelope.scope != SwarmScope::Global {
        bail!("global event gossip requires global scope");
    }
    Ok(GossipMessage::Event(envelope))
}

const ENV_P2P_ENABLED: &str = "WATTSWARM_P2P_ENABLED";
const ENV_P2P_MDNS: &str = "WATTSWARM_P2P_MDNS";
const ENV_P2P_PORT: &str = "WATTSWARM_P2P_PORT";
const ENV_P2P_LISTEN_ADDRS: &str = "WATTSWARM_P2P_LISTEN_ADDRS";
const DEFAULT_P2P_PORT: u16 = 4001;

fn parse_bool_env_with_default(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn parse_listen_addrs_env(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub fn network_enabled_from_env() -> bool {
    parse_bool_env_with_default(ENV_P2P_ENABLED, true)
}

pub fn network_config_from_env() -> NetworkP2pConfig {
    let listen_addrs = env::var(ENV_P2P_LISTEN_ADDRS)
        .ok()
        .map(|raw| parse_listen_addrs_env(&raw))
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            let port = env::var(ENV_P2P_PORT)
                .ok()
                .and_then(|raw| raw.parse::<u16>().ok())
                .unwrap_or(DEFAULT_P2P_PORT);
            vec![format!("/ip4/0.0.0.0/tcp/{port}")]
        });

    NetworkP2pConfig {
        listen_addrs,
        enable_mdns: parse_bool_env_with_default(ENV_P2P_MDNS, true),
        ..NetworkP2pConfig::default()
    }
}

pub fn maybe_start_background_network_service(
    state_dir: PathBuf,
    db_path: PathBuf,
) -> Result<bool> {
    if !network_enabled_from_env() {
        return Ok(false);
    }

    let config = network_config_from_env();
    config.validate()?;
    thread::spawn(move || {
        if let Err(err) = run_background_network_service(&state_dir, &db_path, config) {
            eprintln!("network bridge stopped: {err}");
        }
    });
    Ok(true)
}

fn run_background_network_service(
    state_dir: &Path,
    db_path: &Path,
    config: NetworkP2pConfig,
) -> Result<()> {
    let mut node = crate::control::open_node(state_dir, db_path)?;
    let node_id = node.node_id();
    let mut service = NetworkBridgeService::new(NetworkP2pNode::generate(config)?)?;
    let mut announced_listen = false;

    loop {
        match service.tick(&mut node) {
            Ok(NetworkBridgeTick::Listening { address }) => {
                if !announced_listen {
                    crate::udp_announce::announce_startup(
                        "p2p-startup",
                        Some(&address.to_string()),
                        Some(&node_id),
                    );
                    announced_listen = true;
                }
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("network bridge tick failed: {err}");
                thread::sleep(Duration::from_millis(250));
            }
        }
    }
}

pub fn backfill_response_for_request(
    node: &Node,
    request: &BackfillRequest,
    max_limit: usize,
) -> Result<BackfillResponse> {
    request.validate(max_limit)?;
    if request.scope != SwarmScope::Global {
        bail!("v1 backfill bridge only supports global scope");
    }

    let rows = node
        .store
        .load_events_page(request.from_event_seq, request.limit)?;
    let next_from_event_seq = rows
        .last()
        .map(|(seq, _)| *seq)
        .unwrap_or(request.from_event_seq);
    let events = rows
        .into_iter()
        .map(|(_, event)| EventEnvelope {
            scope: request.scope.clone(),
            event,
        })
        .collect();
    Ok(BackfillResponse {
        scope: request.scope.clone(),
        next_from_event_seq,
        events,
    })
}

pub fn ingest_backfill_response(node: &mut Node, response: &BackfillResponse) -> Result<usize> {
    let mut applied = 0usize;
    for envelope in &response.events {
        if envelope.scope != response.scope {
            return Err(anyhow!("backfill response scope mismatch"));
        }
        if ingest_event_envelope(node, envelope).is_ok() {
            applied += 1;
        }
    }
    Ok(applied)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBridgeTick {
    Listening {
        address: Multiaddr,
    },
    Connected {
        peer: PeerId,
    },
    EventIngested {
        peer: PeerId,
        event_id: String,
    },
    BackfillServed {
        peer: PeerId,
        events: usize,
    },
    BackfillApplied {
        peer: PeerId,
        request_id: BackfillRequestId,
        events: usize,
    },
    BackfillFailed {
        peer: PeerId,
        request_id: BackfillRequestId,
        error: String,
    },
}

pub struct NetworkBridgeService {
    runtime: NetworkRuntime,
    tokio_runtime: Runtime,
}

impl NetworkBridgeService {
    pub fn new(node: NetworkP2pNode) -> Result<Self> {
        let tokio_runtime = Runtime::new()?;
        let mut runtime = tokio_runtime.block_on(async { NetworkRuntime::new(node) })?;
        runtime.subscribe_scope(&SwarmScope::Global)?;
        Ok(Self {
            runtime,
            tokio_runtime,
        })
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.runtime.local_peer_id()
    }

    pub fn listen_addrs(&self) -> &[Multiaddr] {
        self.runtime.listen_addrs()
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        let _guard = self.tokio_runtime.enter();
        self.runtime.dial(addr)
    }

    pub fn publish_global_event(&mut self, event: crate::types::Event) -> Result<()> {
        self.runtime
            .publish_gossip(&global_event_gossip(EventEnvelope {
                scope: SwarmScope::Global,
                event,
            })?)
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
            },
        )
    }

    pub fn tick(&mut self, node: &mut Node) -> Result<NetworkBridgeTick> {
        let event = self.tokio_runtime.block_on(self.runtime.next_event());
        self.handle_runtime_event(node, event)
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

    fn handle_runtime_event(
        &mut self,
        node: &mut Node,
        event: Result<NetworkRuntimeEvent>,
    ) -> Result<NetworkBridgeTick> {
        self.process_runtime_event(node, event?)
    }

    fn process_runtime_event(
        &mut self,
        node: &mut Node,
        event: NetworkRuntimeEvent,
    ) -> Result<NetworkBridgeTick> {
        match event {
            NetworkRuntimeEvent::NewListenAddr { address } => {
                Ok(NetworkBridgeTick::Listening { address })
            }
            NetworkRuntimeEvent::ConnectionEstablished { peer } => {
                Ok(NetworkBridgeTick::Connected { peer })
            }
            NetworkRuntimeEvent::Gossip {
                propagation_source,
                message,
            } => {
                let envelope = event_envelope_from_gossip(&message)?;
                ingest_event_envelope(node, envelope)?;
                Ok(NetworkBridgeTick::EventIngested {
                    peer: propagation_source,
                    event_id: envelope.event.event_id.clone(),
                })
            }
            NetworkRuntimeEvent::BackfillRequest {
                peer,
                request,
                channel,
            } => {
                let response = backfill_response_for_request(
                    node,
                    &request,
                    self.runtime.config().max_backfill_events,
                )?;
                let events = response.events.len();
                self.runtime.send_backfill_response(channel, response)?;
                Ok(NetworkBridgeTick::BackfillServed { peer, events })
            }
            NetworkRuntimeEvent::BackfillResponse {
                peer,
                request_id,
                response,
            } => Ok(NetworkBridgeTick::BackfillApplied {
                peer,
                request_id,
                events: ingest_backfill_response(node, &response)?,
            }),
            NetworkRuntimeEvent::BackfillOutboundFailure {
                peer,
                request_id,
                error,
            } => Ok(NetworkBridgeTick::BackfillFailed {
                peer,
                request_id,
                error,
            }),
            NetworkRuntimeEvent::BackfillInboundFailure { peer, error } => {
                Err(anyhow!("backfill inbound failure from {peer}: {error}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::NodeIdentity;
    use crate::node::build_event_for_external;
    use crate::storage::PgStore;
    use crate::types::{Membership, Role};
    use crate::{node::Node, task_template::sample_contract};
    use serde_json::json;
    use std::env;

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
    fn global_event_gossip_rejects_non_global_scope() {
        let envelope = EventEnvelope {
            scope: SwarmScope::Region("sol-1".to_owned()),
            event: build_event_for_external(
                &NodeIdentity::random(),
                1,
                10,
                crate::types::EventPayload::CheckpointCreated(
                    crate::types::CheckpointCreatedPayload {
                        checkpoint_id: "cp-1".to_owned(),
                        up_to_seq: 1,
                    },
                ),
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
            },
            32,
        )
        .expect("backfill response");

        assert_eq!(response.events.len(), 1);
        assert_eq!(response.scope, SwarmScope::Global);
        assert_eq!(response.next_from_event_seq, 1);
    }

    #[test]
    fn ingest_backfill_response_rejects_scope_mismatch() {
        let mut node = Node::open_in_memory_with_roles(&[Role::Proposer]).expect("node");
        let response = BackfillResponse {
            scope: SwarmScope::Global,
            next_from_event_seq: 1,
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
    fn network_config_defaults_to_enabled_with_fixed_tcp_port() {
        let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, None);
        let _mdns = EnvVarGuard::set(ENV_P2P_MDNS, None);
        let _port = EnvVarGuard::set(ENV_P2P_PORT, None);
        let _listen = EnvVarGuard::set(ENV_P2P_LISTEN_ADDRS, None);
        assert!(network_enabled_from_env());
        let config = network_config_from_env();
        assert!(config.enable_mdns);
        assert_eq!(config.listen_addrs, vec!["/ip4/0.0.0.0/tcp/4001"]);
    }

    #[test]
    fn network_enabled_can_be_explicitly_disabled() {
        let _enabled = EnvVarGuard::set(ENV_P2P_ENABLED, Some("false"));
        assert!(!network_enabled_from_env());
    }
}
