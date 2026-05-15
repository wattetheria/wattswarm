use super::*;

#[test]
fn topic_catalog_uses_namespace_scope_and_kind() {
    let namespace = TopicNamespace {
        network: "swarmnet".to_owned(),
        network_id: "main/alpha".to_owned(),
    };
    let catalog =
        TopicCatalog::new(&namespace, &SwarmScope::Region("sol-1/alpha".to_owned())).unwrap();
    assert_eq!(
        catalog.events,
        "swarmnet.main-alpha.region.sol-1-alpha.events"
    );
    assert_eq!(
        catalog.checkpoints,
        "swarmnet.main-alpha.region.sol-1-alpha.checkpoints"
    );
}

#[test]
fn backfill_request_validate_enforces_bounds() {
    let req = RawBackfillRequest {
        scope: SwarmScope::Global,
        from_event_seq: 10,
        limit: 5,
        feed_key: Some("feed".to_owned()),
        known_event_ids: Vec::new(),
    };
    req.validate(10, 20).unwrap();
    assert!(
        RawBackfillRequest {
            limit: 0,
            ..req.clone()
        }
        .validate(10, 20)
        .is_err()
    );
    assert!(
        RawBackfillRequest {
            limit: 25,
            ..req.clone()
        }
        .validate(10, 20)
        .is_err()
    );
    assert!(
        RawBackfillRequest {
            limit: 5,
            known_event_ids: vec!["event".to_owned(); MAX_BACKFILL_KNOWN_EVENT_IDS + 1],
            ..req
        }
        .validate(10, 20)
        .is_err()
    );
}

#[test]
fn backfill_response_validate_enforces_bounds() {
    let response = RawBackfillResponse {
        scope: SwarmScope::Global,
        next_from_event_seq: 10,
        feed_key: Some("feed".to_owned()),
        head_event_ids: vec!["evt-1".to_owned()],
        items: vec![b"event".to_vec()],
    };
    response.validate(10, 20).unwrap();
    assert!(
        RawBackfillResponse {
            items: vec![b"event".to_vec(); 25],
            ..response.clone()
        }
        .validate(10, 20)
        .is_err()
    );
    assert!(
        RawBackfillResponse {
            items: vec![vec![b'x'; MAX_BACKFILL_RESPONSE_BYTES + 1]],
            ..response.clone()
        }
        .validate(10, 20)
        .is_err()
    );
    assert!(
        RawBackfillResponse {
            head_event_ids: vec!["event".to_owned(); MAX_BACKFILL_KNOWN_EVENT_IDS + 1],
            ..response.clone()
        }
        .validate(10, 20)
        .is_err()
    );
}

#[test]
fn raw_control_request_and_response_validate_payloads() {
    let invalid_request = RawControlRequest::PeerDirectMessage(RawPeerDirectMessageRequest {
        source_node_id: "node-a".to_owned(),
        target_node_id: "node-b".to_owned(),
        thread_id: "thread-1".to_owned(),
        message_id: "message-1".to_owned(),
        kind: RawPeerDirectMessageKind::Message,
        agent_envelope: None,
        contact_material: None,
        content_ref: None,
        control_json: None,
    });
    assert!(invalid_request.validate(10, 20).is_err());

    let invalid_response = RawControlResponse::Backfill(RawBackfillResponse {
        scope: SwarmScope::Global,
        next_from_event_seq: 1,
        feed_key: None,
        head_event_ids: Vec::new(),
        items: vec![b"event".to_vec(); 25],
    });
    assert!(invalid_response.validate(10, 20).is_err());
}

#[test]
fn control_payload_validation_rejects_oversized_json() {
    let oversized = "x".repeat(MAX_CONTROL_JSON_BYTES + 1);
    let request = RawPeerDirectMessageRequest {
        source_node_id: "node-a".to_owned(),
        target_node_id: "node-b".to_owned(),
        thread_id: "thread-1".to_owned(),
        message_id: "message-1".to_owned(),
        kind: RawPeerDirectMessageKind::SessionInit,
        agent_envelope: None,
        contact_material: None,
        content_ref: None,
        control_json: Some(oversized),
    };
    assert!(request.validate().is_err());
}

#[test]
fn peer_handshake_roundtrip() {
    let handshake = PeerHandshakeMetadata {
        network_id: "mainnet".to_owned(),
        params_version: 7,
        params_hash: "abc123".to_owned(),
    };
    let encoded = handshake.encode_agent_version();
    let decoded = PeerHandshakeMetadata::decode_agent_version(&encoded).unwrap();
    assert_eq!(decoded, handshake);
}

#[test]
fn peer_relationship_wire_supports_agent_envelope_roundtrip() {
    let envelope = RawAgentEnvelope {
        protocol: "google_a2a".to_owned(),
        source_agent_id: Some("agent-a".to_owned()),
        target_agent_id: Some("agent-b".to_owned()),
        capability: Some("peer.relationship.request".to_owned()),
        message_json: "{\"intent\":\"friend_request\"}".to_owned(),
        extensions_json: Some("{\"reason\":\"collaboration\"}".to_owned()),
        signature: Some("sig-123".to_owned()),
    };
    let request = RawPeerRelationshipRequest {
        source_node_id: "node-a".to_owned(),
        target_node_id: "node-b".to_owned(),
        action: RawPeerRelationshipAction::Request,
        agent_envelope: Some(envelope.clone()),
    };
    let response = RawPeerRelationshipResponse {
        source_node_id: "node-b".to_owned(),
        target_node_id: "node-a".to_owned(),
        action: RawPeerRelationshipAction::Accept,
        applied: true,
        agent_envelope: Some(envelope),
        relationship_state: Some("accepted".to_owned()),
        detail: None,
        updated_at: 42,
    };

    let request_roundtrip: RawPeerRelationshipRequest =
        serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
    let response_roundtrip: RawPeerRelationshipResponse =
        serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();

    assert_eq!(
        request_roundtrip
            .agent_envelope
            .as_ref()
            .and_then(|entry| entry.source_agent_id.as_deref()),
        Some("agent-a")
    );
    assert_eq!(
        response_roundtrip
            .agent_envelope
            .as_ref()
            .and_then(|entry| entry.capability.as_deref()),
        Some("peer.relationship.request")
    );
}
