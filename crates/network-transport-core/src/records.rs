use crate::SwarmScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleAnnouncement {
    pub scope: SwarmScope,
    pub rule_set: String,
    pub rule_version: u64,
    pub activation_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signer_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signature_hex: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointAnnouncement {
    pub scope: SwarmScope,
    pub checkpoint_id: String,
    pub artifact_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signer_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_signature_hex: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryAnnouncement {
    pub summary_id: String,
    pub source_node_id: String,
    pub scope: SwarmScope,
    pub summary_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    #[serde(default)]
    pub payload: Value,
}
