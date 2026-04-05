use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportRoute {
    Libp2pControl,
    IrohDirect,
}

impl TransportRoute {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Libp2pControl => "libp2p_control",
            Self::IrohDirect => "iroh_direct",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferKind {
    ControlMessage,
    RelationshipControl,
    DirectMessage,
    TopicSync,
    TaskSync,
    BackfillChunk,
    ArtifactBlob,
    EvidenceBlob,
    CheckpointSnapshot,
}

impl TransferKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ControlMessage => "control_message",
            Self::RelationshipControl => "relationship_control",
            Self::DirectMessage => "direct_message",
            Self::TopicSync => "topic_sync",
            Self::TaskSync => "task_sync",
            Self::BackfillChunk => "backfill_chunk",
            Self::ArtifactBlob => "artifact_blob",
            Self::EvidenceBlob => "evidence_blob",
            Self::CheckpointSnapshot => "checkpoint_snapshot",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferIntent {
    pub kind: TransferKind,
    pub payload_bytes: usize,
    pub requires_streaming: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectDataObjectKind {
    ReferenceArtifact,
    EvidenceArtifact,
    CheckpointJson,
    SnapshotJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectDataFetchRequest {
    pub object_kind: DirectDataObjectKind,
    pub object_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectDataFetchResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerTransportCapabilities {
    pub supports_iroh_direct: bool,
    pub supports_streaming: bool,
    pub max_recommended_inline_bytes: usize,
    pub preferred_data_route: TransportRoute,
}

impl PeerTransportCapabilities {
    pub fn libp2p_control_default() -> Self {
        Self {
            supports_iroh_direct: false,
            supports_streaming: false,
            max_recommended_inline_bytes: 16 * 1024,
            preferred_data_route: TransportRoute::Libp2pControl,
        }
    }

    pub fn iroh_direct_default() -> Self {
        Self {
            supports_iroh_direct: true,
            supports_streaming: true,
            max_recommended_inline_bytes: 16 * 1024,
            preferred_data_route: TransportRoute::IrohDirect,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportMetadata {
    pub route: TransportRoute,
    pub generated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alpn: Option<String>,
    #[serde(default)]
    pub listen_addrs: Vec<String>,
    pub capabilities: PeerTransportCapabilities,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportContactMaterial {
    pub transport: String,
    pub peer_id: String,
    pub metadata: TransportMetadata,
    #[serde(default)]
    pub extra: Value,
}
