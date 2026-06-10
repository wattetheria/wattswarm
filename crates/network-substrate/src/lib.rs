use anyhow::{Context, Result, anyhow, bail};
use iroh::EndpointId;
use iroh_gossip::api::{Event as IrohGossipEvent, GossipSender};
use iroh_gossip::proto::MIN_MAX_MESSAGE_SIZE;
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::{Runtime, RuntimeFlavor};
use wattswarm_network_transport_core::{
    PeerTransportCapabilities, TransportContactMaterial, TransportMetadata, TransportRoute,
};
use wattswarm_network_transport_iroh::{
    DEFAULT_IROH_CONTROL_ALPN, IrohControlStreamRequest, IrohControlStreamResponse,
    IrohGossipRuntimeConfig, derive_gossip_topic_id,
    export_local_contact_material_for_network_peer_id_with_gossip_config,
    install_local_contact_material_control_handler_for_network_peer_id,
    local_endpoint_id_from_state_dir, local_gossip_for_network_peer_id,
    register_remote_contact_material_for_network_peer_id,
    send_control_stream_request_for_network_peer_id_with_timeout,
    set_local_control_stream_handler_for_network_peer_id, shutdown_local_iroh_data_plane,
};

const DEFAULT_NAMESPACE: &str = "wattswarm";
const DEFAULT_IDENTIFY_AGENT_NAME: &str = "wattswarm-network-substrate";
const MAX_BACKFILL_KNOWN_EVENT_IDS: usize = 256;
const MAX_BACKFILL_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_AGENT_ENVELOPE_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTACT_MATERIAL_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTROL_DETAIL_BYTES: usize = 8 * 1024;
const DEFAULT_SUBSTRATE_CONTROL_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_GOSSIP_BOOTSTRAP_TIMEOUT_MS: u64 = 5_000;
const IROH_CONTROL_KIND_BACKFILL: &str = "backfill.v1";
const IROH_CONTROL_KIND_CONTACT_MATERIAL: &str = "contact_material.v1";
const IROH_CONTROL_KIND_PEER_RELATIONSHIP: &str = "peer_relationship.v1";

mod runtime;
mod types;
mod wire;

pub use runtime::{
    IrohGossipSubscription, IrohRuntimeCounters, SubstrateRuntime, SubstrateRuntimeEvent,
};
pub use types::{
    BackfillRequestId, BackfillResponseChannel, ContactMaterialRequestId,
    ContactMaterialResponseChannel, GossipKind, NetworkAddress, NetworkNodeId,
    PeerHandshakeMetadata, PeerMetadata, PeerRelationshipRequestId,
    PeerRelationshipResponseChannel, SubstrateConfig, SubstrateNode, SwarmScope, TopicCatalog,
    TopicNamespace,
};
use wire::InboundControlPeer;
pub use wire::{
    RawAgentEnvelope, RawBackfillRequest, RawBackfillResponse, RawContactMaterial,
    RawContactMaterialRequest, RawContactMaterialResponse, RawControlRequest, RawControlResponse,
    RawGossipMessage, RawPeerRelationshipAction, RawPeerRelationshipRequest,
    RawPeerRelationshipResponse, RawSourceAgentCard,
};

#[cfg(test)]
mod tests;

fn contact_endpoint_id(contact: &TransportContactMaterial) -> Result<EndpointId> {
    let metadata_endpoint = contact.metadata.endpoint_id.as_deref().map(str::trim);
    let extra_endpoint = contact
        .extra
        .get("endpoint_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim);
    let endpoint_id = extra_endpoint
        .or(metadata_endpoint)
        .unwrap_or(contact.peer_id.as_str())
        .trim();
    if endpoint_id.is_empty() {
        bail!("iroh contact material endpoint_id cannot be empty");
    }
    if let Some(metadata_endpoint) = metadata_endpoint
        && metadata_endpoint != endpoint_id
    {
        bail!(
            "iroh contact metadata endpoint_id {metadata_endpoint} does not match endpoint_id {endpoint_id}"
        );
    }
    EndpointId::from_str(endpoint_id).with_context(|| {
        format!("parse iroh endpoint id from contact material endpoint_id {endpoint_id}")
    })
}

fn parse_iroh_endpoint_id_string(raw: impl Into<String>, label: &str) -> Result<String> {
    let raw = raw.into();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{label} cannot be empty");
    }
    EndpointId::from_str(trimmed)
        .with_context(|| format!("{label} must be an iroh NodeId / EndpointId"))?;
    Ok(trimmed.to_owned())
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

fn encode_raw_control_request(request: RawControlRequest) -> Result<(String, Vec<u8>)> {
    match request {
        RawControlRequest::Backfill(request) => Ok((
            IROH_CONTROL_KIND_BACKFILL.to_owned(),
            serde_json::to_vec(&request)?,
        )),
        RawControlRequest::ContactMaterial(request) => Ok((
            IROH_CONTROL_KIND_CONTACT_MATERIAL.to_owned(),
            serde_json::to_vec(&request)?,
        )),
        RawControlRequest::PeerRelationship(request) => Ok((
            IROH_CONTROL_KIND_PEER_RELATIONSHIP.to_owned(),
            serde_json::to_vec(&request)?,
        )),
    }
}

fn decode_raw_control_response(
    kind: &str,
    response: IrohControlStreamResponse,
) -> Result<RawControlResponse> {
    if !response.ok {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "remote iroh control request failed".to_owned())
        );
    }
    Ok(match kind {
        IROH_CONTROL_KIND_BACKFILL => {
            RawControlResponse::Backfill(serde_json::from_slice(&response.payload)?)
        }
        IROH_CONTROL_KIND_CONTACT_MATERIAL => {
            RawControlResponse::ContactMaterial(serde_json::from_slice(&response.payload)?)
        }
        IROH_CONTROL_KIND_PEER_RELATIONSHIP => {
            RawControlResponse::PeerRelationship(serde_json::from_slice(&response.payload)?)
        }
        other => bail!("unexpected iroh control response kind {other}"),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerDiscoverySourceKind {
    Udp,
    LocalDiscovery,
    Bootstrap,
    Identify,
    BootstrapIndex,
    Unknown,
}

impl PeerDiscoverySourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::LocalDiscovery => "local_discovery",
            Self::Bootstrap => "bootstrap",
            Self::Identify => "identify",
            Self::BootstrapIndex => "bootstrap_index",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrafficGuardPeerHealth {
    pub peer: String,
    pub score: i64,
    pub blacklisted: bool,
    pub reputation_tier: String,
    pub quarantined: bool,
    pub quarantine_remaining_ms: u64,
    pub ban_remaining_ms: u64,
    pub throttle_factor_percent: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkRuntimeObservabilitySnapshot {
    pub p2p_foundation: String,
    pub local_iroh_endpoint_id: Option<String>,
    pub subscribed_iroh_gossip_topics: Vec<String>,
    pub known_iroh_contacts: usize,
    pub legacy_transport_active: bool,
    pub nat_status: String,
    pub nat_public_address: Option<String>,
    pub nat_confidence: u32,
    pub relay_reservations: Vec<String>,
    pub peer_health: Vec<TrafficGuardPeerHealth>,
    pub dropped_malformed_gossip: u64,
    pub invalid_control_payloads: u64,
    pub dial_failures: u64,
    pub response_validation_failures: u64,
    pub retry_suppressed_dials: u64,
}

fn validate_max_bytes(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.len() > max_bytes {
        bail!("{label} exceeds configured max bytes");
    }
    Ok(())
}

pub fn sanitize_segment(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("network segment cannot be empty");
    }

    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    Ok(out)
}

fn default_network_context_id() -> String {
    "default".to_owned()
}
