use super::*;
use serde::Deserializer;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackfillRequest {
    pub scope: SwarmScope,
    pub from_event_seq: u64,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub known_event_ids: Vec<String>,
}

impl RawBackfillRequest {
    pub fn validate(&self, max_limit: usize, hard_limit: usize) -> Result<()> {
        if self.limit == 0 {
            bail!("backfill limit must be > 0");
        }
        if self.limit > max_limit {
            bail!("backfill limit exceeds configured max");
        }
        if self.limit > hard_limit {
            bail!("backfill limit exceeds hard safety limit");
        }
        if let Some(feed_key) = &self.feed_key
            && feed_key.trim().is_empty()
        {
            bail!("backfill feed_key must not be empty");
        }
        if self.known_event_ids.len() > MAX_BACKFILL_KNOWN_EVENT_IDS {
            bail!("backfill known_event_ids exceeds configured max");
        }
        if self
            .known_event_ids
            .iter()
            .any(|event_id| event_id.trim().is_empty())
        {
            bail!("backfill known_event_ids must not contain empty event ids");
        }
        Ok(())
    }
}

pub(super) trait InboundControlPeer {
    fn inbound_peer(&self, fallback: &NetworkNodeId) -> Result<NetworkNodeId>;
}

impl InboundControlPeer for RawBackfillRequest {
    fn inbound_peer(&self, fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        Ok(fallback.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackfillResponse {
    pub scope: SwarmScope,
    pub next_from_event_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub head_event_ids: Vec<String>,
    pub items: Vec<Vec<u8>>,
}

impl RawBackfillResponse {
    pub fn validate(&self, max_limit: usize, hard_limit: usize) -> Result<()> {
        if self.items.len() > max_limit {
            bail!("backfill response items exceeds configured max");
        }
        if self.items.len() > hard_limit {
            bail!("backfill response items exceeds hard safety limit");
        }
        let response_bytes = self
            .items
            .iter()
            .try_fold(0usize, |total, item| total.checked_add(item.len()))
            .ok_or_else(|| anyhow!("backfill response byte count overflow"))?;
        if response_bytes > MAX_BACKFILL_RESPONSE_BYTES {
            bail!("backfill response bytes exceeds configured max");
        }
        if let Some(feed_key) = &self.feed_key
            && feed_key.trim().is_empty()
        {
            bail!("backfill response feed_key must not be empty");
        }
        if self.head_event_ids.len() > MAX_BACKFILL_KNOWN_EVENT_IDS {
            bail!("backfill response head_event_ids exceeds configured max");
        }
        if self
            .head_event_ids
            .iter()
            .any(|event_id| event_id.trim().is_empty())
        {
            bail!("backfill response head_event_ids must not contain empty event ids");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawPeerRelationshipAction {
    Request,
    Accept,
    Reject,
    Cancel,
    Remove,
    Block,
    Unblock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerRelationshipRequest {
    pub source_node_id: String,
    pub target_node_id: String,
    pub action: RawPeerRelationshipAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<RawAgentEnvelope>,
}

impl RawPeerRelationshipRequest {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer relationship request source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer relationship request target_node_id is required");
        }
        if let Some(envelope) = &self.agent_envelope {
            envelope.validate()?;
        }
        Ok(())
    }
}

impl InboundControlPeer for RawPeerRelationshipRequest {
    fn inbound_peer(&self, _fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        NetworkNodeId::new(self.source_node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerRelationshipResponse {
    pub source_node_id: String,
    pub target_node_id: String,
    pub action: RawPeerRelationshipAction,
    pub applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<RawAgentEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub updated_at: u64,
}

impl RawPeerRelationshipResponse {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer relationship response source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer relationship response target_node_id is required");
        }
        if let Some(envelope) = &self.agent_envelope {
            envelope.validate()?;
        }
        if let Some(detail) = &self.detail {
            validate_max_bytes(
                "peer relationship response detail",
                detail,
                MAX_CONTROL_DETAIL_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawSourceAgentCard {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    pub card_hash: String,
    pub issued_at: u64,
    pub card: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawAgentEnvelope {
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent_card: Option<RawSourceAgentCard>,
    #[serde(
        default,
        alias = "message",
        deserialize_with = "deserialize_json_string_field"
    )]
    pub message_json: String,
    #[serde(
        default,
        alias = "extensions",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_json_string_field"
    )]
    pub extensions_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

fn deserialize_json_string_field<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::String(value)) => value,
        Some(value) => serde_json::to_string(&value).map_err(serde::de::Error::custom)?,
        None => String::new(),
    })
}

fn deserialize_optional_json_string_field<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(serde_json::Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(value) => serde_json::to_string(&value)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

impl RawAgentEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.protocol.trim().is_empty() {
            bail!("agent envelope protocol is required");
        }
        validate_max_bytes(
            "agent envelope message_json",
            &self.message_json,
            MAX_AGENT_ENVELOPE_JSON_BYTES,
        )?;
        if let Some(extensions_json) = &self.extensions_json {
            validate_max_bytes(
                "agent envelope extensions_json",
                extensions_json,
                MAX_AGENT_ENVELOPE_JSON_BYTES,
            )?;
        }
        if let Some(source_agent_card) = &self.source_agent_card {
            if source_agent_card.agent_id.trim().is_empty() {
                bail!("source_agent_card agent_id is required");
            }
            if source_agent_card.card_hash.trim().is_empty() {
                bail!("source_agent_card card_hash is required");
            }
            validate_max_bytes(
                "source_agent_card card",
                &serde_json::to_string(&source_agent_card.card)
                    .context("serialize source_agent_card card")?,
                MAX_AGENT_ENVELOPE_JSON_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawContactMaterial {
    pub material_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub generated_at: u64,
}

impl RawContactMaterial {
    pub fn validate(&self) -> Result<()> {
        validate_max_bytes(
            "contact material",
            &self.material_json,
            MAX_CONTACT_MATERIAL_JSON_BYTES,
        )?;
        if self.generated_at == 0 {
            bail!("contact material generated_at is required");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawContactMaterialRequest {
    pub source_node_id: String,
    pub target_node_id: String,
}

impl RawContactMaterialRequest {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("contact material request source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("contact material request target_node_id is required");
        }
        Ok(())
    }
}

impl InboundControlPeer for RawContactMaterialRequest {
    fn inbound_peer(&self, _fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        NetworkNodeId::new(self.source_node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawContactMaterialResponse {
    pub source_node_id: String,
    pub target_node_id: String,
    pub applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<RawContactMaterial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub updated_at: u64,
}

impl RawContactMaterialResponse {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("contact material response source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("contact material response target_node_id is required");
        }
        if let Some(material) = &self.contact_material {
            material.validate()?;
        }
        if let Some(detail) = &self.detail {
            validate_max_bytes(
                "contact material response detail",
                detail,
                MAX_CONTROL_DETAIL_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawPeerDirectMessageKind {
    RelationshipEstablished,
    SessionInit,
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerDirectMessageRequest {
    pub source_node_id: String,
    pub target_node_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub kind: RawPeerDirectMessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_envelope: Option<RawAgentEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<RawContactMaterial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_json: Option<String>,
}

impl RawPeerDirectMessageRequest {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer direct message source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer direct message target_node_id is required");
        }
        if self.thread_id.trim().is_empty() {
            bail!("peer direct message thread_id is required");
        }
        if self.message_id.trim().is_empty() {
            bail!("peer direct message message_id is required");
        }
        if let Some(envelope) = &self.agent_envelope {
            envelope.validate()?;
        }
        if let Some(material) = &self.contact_material {
            material.validate()?;
        }
        match self.kind {
            RawPeerDirectMessageKind::Message => {
                let Some(content_ref) = &self.content_ref else {
                    bail!("peer direct message content_ref is required for message kind");
                };
                if content_ref.uri.trim().is_empty()
                    || content_ref.digest.trim().is_empty()
                    || content_ref.mime.trim().is_empty()
                    || content_ref.producer.trim().is_empty()
                    || content_ref.size_bytes == 0
                {
                    bail!("peer direct message content_ref is invalid");
                }
            }
            RawPeerDirectMessageKind::RelationshipEstablished
            | RawPeerDirectMessageKind::SessionInit => {
                let control_json = self.control_json.as_deref().unwrap_or("");
                if control_json.trim().is_empty() {
                    bail!("peer direct message control_json is required for control kinds");
                }
                validate_max_bytes(
                    "peer direct message control_json",
                    control_json,
                    MAX_CONTROL_JSON_BYTES,
                )?;
            }
        }
        Ok(())
    }
}

impl InboundControlPeer for RawPeerDirectMessageRequest {
    fn inbound_peer(&self, _fallback: &NetworkNodeId) -> Result<NetworkNodeId> {
        NetworkNodeId::new(self.source_node_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPeerDirectMessageResponse {
    pub source_node_id: String,
    pub target_node_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub kind: RawPeerDirectMessageKind,
    pub applied: bool,
    pub delivery_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_material: Option<RawContactMaterial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub updated_at: u64,
}

impl RawPeerDirectMessageResponse {
    pub fn validate(&self) -> Result<()> {
        if self.source_node_id.trim().is_empty() {
            bail!("peer direct message response source_node_id is required");
        }
        if self.target_node_id.trim().is_empty() {
            bail!("peer direct message response target_node_id is required");
        }
        if self.thread_id.trim().is_empty() {
            bail!("peer direct message response thread_id is required");
        }
        if self.message_id.trim().is_empty() {
            bail!("peer direct message response message_id is required");
        }
        if self.delivery_state.trim().is_empty() {
            bail!("peer direct message response delivery_state is required");
        }
        if let Some(material) = &self.contact_material {
            material.validate()?;
        }
        if let Some(detail) = &self.detail {
            validate_max_bytes(
                "peer direct message response detail",
                detail,
                MAX_CONTROL_DETAIL_BYTES,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawGossipMessage {
    pub scope: SwarmScope,
    pub kind: GossipKind,
    pub payload: Vec<u8>,
}

impl RawGossipMessage {
    pub fn encode_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum RawControlRequest {
    Backfill(RawBackfillRequest),
    ContactMaterial(RawContactMaterialRequest),
    PeerRelationship(RawPeerRelationshipRequest),
    PeerDirectMessage(RawPeerDirectMessageRequest),
}

impl RawControlRequest {
    pub fn validate(&self, max_backfill_events: usize, backfill_hard_limit: usize) -> Result<()> {
        match self {
            Self::Backfill(request) => request.validate(max_backfill_events, backfill_hard_limit),
            Self::ContactMaterial(request) => request.validate(),
            Self::PeerRelationship(request) => request.validate(),
            Self::PeerDirectMessage(request) => request.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RawControlResponse {
    Backfill(RawBackfillResponse),
    ContactMaterial(RawContactMaterialResponse),
    PeerRelationship(RawPeerRelationshipResponse),
    PeerDirectMessage(RawPeerDirectMessageResponse),
}

impl RawControlResponse {
    pub fn validate(&self, max_backfill_events: usize, backfill_hard_limit: usize) -> Result<()> {
        match self {
            Self::Backfill(response) => response.validate(max_backfill_events, backfill_hard_limit),
            Self::ContactMaterial(response) => response.validate(),
            Self::PeerRelationship(response) => response.validate(),
            Self::PeerDirectMessage(response) => response.validate(),
        }
    }
}
