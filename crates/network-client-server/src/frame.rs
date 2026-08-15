use serde::{Deserialize, Serialize};
use wattswarm_network_transport_core::{
    DeliveryClass, EventTransportRoute, OpaqueCommitToken, OpaqueSignedRecord,
};
use wattswarm_protocol::types::NetworkMembershipGrant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishPayloadType {
    Event,
    Message,
    Rule,
    Checkpoint,
    Summary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishRoute {
    pub network_id: String,
    #[serde(flatten)]
    pub transport: EventTransportRoute,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishFrame {
    pub framing_version: String,
    pub delivery_policy_version: u64,
    pub record_id: String,
    pub route: PublishRoute,
    pub payload_type: PublishPayloadType,
    pub payload: OpaqueSignedRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishAcceptance {
    pub publish_receipt: String,
    pub record_id: String,
    pub delivery_class: DeliveryClass,
    pub delivery_policy_version: u64,
    pub membership_version: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlFrameKind {
    PeerRelationship,
    PeerRelationshipAck,
    ContactMaterialRequest,
    ContactMaterialResponse,
    DirectRpc,
}

impl ControlFrameKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PeerRelationship => "peer_relationship",
            Self::PeerRelationshipAck => "peer_relationship_ack",
            Self::ContactMaterialRequest => "contact_material_request",
            Self::ContactMaterialResponse => "contact_material_response",
            Self::DirectRpc => "direct_rpc",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFrame {
    pub framing_version: String,
    pub network_id: String,
    pub correlation_id: String,
    pub source_principal_id: String,
    pub target_principal_id: String,
    pub control_kind: ControlFrameKind,
    pub payload: OpaqueSignedRecord,
    pub signature_hex: String,
}

pub fn control_frame_signing_message(frame: &ControlFrame) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&serde_json::json!({
        "domain": "wattswarm:client-server-control:v1",
        "framing_version": frame.framing_version,
        "network_id": frame.network_id,
        "correlation_id": frame.correlation_id,
        "source_principal_id": frame.source_principal_id,
        "target_principal_id": frame.target_principal_id,
        "control_kind": frame.control_kind,
        "payload": frame.payload,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlAcceptance {
    pub correlation_id: String,
    pub delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalNodePrincipalClaim {
    pub principal_id: String,
    pub public_key_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChallengeRequest {
    pub network_id: String,
    pub principals: Vec<LogicalNodePrincipalClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChallengeResponse {
    pub challenge_id: String,
    pub nonce: String,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalNodePrincipalProof {
    pub principal_id: String,
    pub signature_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionProofRequest {
    pub challenge_id: String,
    pub network_id: String,
    pub principals: Vec<LogicalNodePrincipalClaim>,
    pub proofs: Vec<LogicalNodePrincipalProof>,
    pub delivery_policy_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRegistrationRequest {
    pub network_id: String,
    pub principal_id: String,
    pub public_key_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_instance_id: Option<String>,
    pub nonce: String,
    pub signature_hex: String,
}

impl AutoRegistrationRequest {
    pub fn signing_message(&self) -> Result<Vec<u8>, serde_json::Error> {
        let payload = serde_json::json!({
            "domain": "wattswarm:network-registration-request:v1",
            "network_id": self.network_id,
            "principal_id": self.principal_id,
            "public_key_hex": self.public_key_hex,
            "tenant_instance_id": self.tenant_instance_id,
            "nonce": self.nonce,
        });
        serde_jcs::to_vec(&payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRegistrationResponse {
    pub network_id: String,
    pub principal_id: String,
    pub grant: NetworkMembershipGrant,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantAdmissionRequest {
    pub grant: NetworkMembershipGrant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantAdmissionResponse {
    pub network_id: String,
    pub principal_id: String,
    pub membership_version: String,
    pub status: String,
}

pub fn session_proof_message(
    network_id: &str,
    principals: &[LogicalNodePrincipalClaim],
    challenge: &ChallengeResponse,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&serde_json::json!({
        "domain": "wattswarm:client-server-session:v1",
        "network_id": network_id,
        "principals": principals,
        "challenge_id": challenge.challenge_id,
        "nonce": challenge.nonce,
        "expires_at": challenge.expires_at,
    }))
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session_token: String,
    pub network_id: String,
    pub principal_id: String,
    pub delivery_policy_version: u64,
    #[serde(default)]
    pub history_status: HistoryStatus,
    pub expires_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryStatus {
    #[default]
    CurrentMailboxOnly,
    HistoryUnavailable,
}

impl std::fmt::Debug for SessionResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionResponse")
            .field("session_token", &"[redacted]")
            .field("network_id", &self.network_id)
            .field("principal_id", &self.principal_id)
            .field("delivery_policy_version", &self.delivery_policy_version)
            .field("history_status", &self.history_status)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub page_id: String,
    pub delivery_class: DeliveryClass,
    pub commit_token: OpaqueCommitToken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wattswarm_network_transport_core::{PropagationLane, SwarmScope};

    #[test]
    fn event_frame_keeps_the_signed_record_opaque() {
        let frame = PublishFrame {
            framing_version: "1".to_owned(),
            delivery_policy_version: 1,
            record_id: "event-1".to_owned(),
            route: PublishRoute {
                network_id: "network-1".to_owned(),
                transport: EventTransportRoute::from_kind_label(
                    SwarmScope::Group("hive-1".to_owned()),
                    PropagationLane::Messages,
                    "TopicMessagePosted",
                    false,
                )
                .unwrap(),
            },
            payload_type: PublishPayloadType::Event,
            payload: OpaqueSignedRecord::new(br#"{"signed":true}"#.to_vec()).unwrap(),
        };
        let encoded = serde_json::to_string(&frame).unwrap();
        assert!(!encoded.contains("rabbit"));
        assert_eq!(
            serde_json::from_str::<PublishFrame>(&encoded).unwrap(),
            frame
        );
    }

    #[test]
    fn control_signature_covers_every_immutable_routing_field() {
        let frame = ControlFrame {
            framing_version: "1".to_owned(),
            network_id: "network-1".to_owned(),
            correlation_id: "correlation-1".to_owned(),
            source_principal_id: "node-a".to_owned(),
            target_principal_id: "node-b".to_owned(),
            control_kind: ControlFrameKind::PeerRelationship,
            payload: OpaqueSignedRecord::new(br#"{"opaque":true}"#.to_vec()).unwrap(),
            signature_hex: "excluded-from-signing-input".to_owned(),
        };
        let baseline = control_frame_signing_message(&frame).unwrap();

        let mut changed = frame.clone();
        changed.target_principal_id = "node-c".to_owned();
        assert_ne!(control_frame_signing_message(&changed).unwrap(), baseline);

        let mut changed = frame.clone();
        changed.control_kind = ControlFrameKind::ContactMaterialRequest;
        assert_ne!(control_frame_signing_message(&changed).unwrap(), baseline);

        let mut changed = frame.clone();
        changed.correlation_id = "correlation-2".to_owned();
        assert_ne!(control_frame_signing_message(&changed).unwrap(), baseline);

        let mut changed = frame;
        changed.payload = OpaqueSignedRecord::new(br#"{"opaque":false}"#.to_vec()).unwrap();
        assert_ne!(control_frame_signing_message(&changed).unwrap(), baseline);
    }

    #[test]
    fn session_proof_binds_tenant_instance_and_old_responses_default_history_status() {
        let challenge = ChallengeResponse {
            challenge_id: "challenge-1".to_owned(),
            nonce: "nonce-1".to_owned(),
            expires_at: 10,
        };
        let mut principal = LogicalNodePrincipalClaim {
            principal_id: "principal-a".to_owned(),
            public_key_hex: "principal-a".to_owned(),
            tenant_instance_id: Some("instance-a".to_owned()),
        };
        let baseline =
            session_proof_message("network-a", std::slice::from_ref(&principal), &challenge)
                .unwrap();
        principal.tenant_instance_id = Some("instance-b".to_owned());
        assert_ne!(
            session_proof_message("network-a", &[principal], &challenge).unwrap(),
            baseline
        );

        let response: SessionResponse = serde_json::from_value(serde_json::json!({
            "session_token": "redacted",
            "network_id": "network-a",
            "principal_id": "principal-a",
            "delivery_policy_version": 1,
            "expires_at": 10
        }))
        .unwrap();
        assert_eq!(response.history_status, HistoryStatus::CurrentMailboxOnly);
    }

    #[test]
    fn auto_registration_request_round_trips_and_signing_excludes_signature() {
        let request = AutoRegistrationRequest {
            network_id: "network-a".to_owned(),
            principal_id: "principal-a".to_owned(),
            public_key_hex: "principal-a".to_owned(),
            tenant_instance_id: Some("instance-a".to_owned()),
            nonce: "nonce-a".to_owned(),
            signature_hex: "signature-a".to_owned(),
        };
        let encoded = serde_json::to_string(&request).unwrap();
        assert!(encoded.contains("signature-a"));
        assert!(
            !String::from_utf8(request.signing_message().unwrap())
                .unwrap()
                .contains("signature-a")
        );
        assert_eq!(
            serde_json::from_str::<AutoRegistrationRequest>(&encoded).unwrap(),
            request
        );
    }
}
