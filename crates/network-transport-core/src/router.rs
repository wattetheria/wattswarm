use crate::{PeerTransportCapabilities, TransferIntent, TransferKind, TransportRoute};

pub struct TransportRouter;

impl TransportRouter {
    pub fn select(
        intent: &TransferIntent,
        remote_capabilities: Option<&PeerTransportCapabilities>,
    ) -> TransportRoute {
        let Some(capabilities) = remote_capabilities else {
            return TransportRoute::IrohControl;
        };

        if !capabilities.supports_iroh_direct {
            return TransportRoute::IrohControl;
        }

        match intent.kind {
            TransferKind::ControlMessage | TransferKind::RelationshipControl => {
                TransportRoute::IrohControl
            }
            TransferKind::DirectMessage
            | TransferKind::TopicSync
            | TransferKind::TaskSync
            | TransferKind::BackfillChunk
            | TransferKind::ArtifactBlob
            | TransferKind::EvidenceBlob
            | TransferKind::CheckpointSnapshot => {
                if intent.requires_streaming
                    || intent.payload_bytes > capabilities.max_recommended_inline_bytes
                    || capabilities.preferred_data_route == TransportRoute::IrohDirect
                {
                    TransportRoute::IrohDirect
                } else {
                    TransportRoute::IrohControl
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_messages_stay_on_iroh_control_plane() {
        let remote = PeerTransportCapabilities::iroh_direct_default();
        let route = TransportRouter::select(
            &TransferIntent {
                kind: TransferKind::ControlMessage,
                payload_bytes: 128,
                requires_streaming: false,
            },
            Some(&remote),
        );
        assert_eq!(route, TransportRoute::IrohControl);
    }

    #[test]
    fn large_data_prefers_iroh_when_supported() {
        let remote = PeerTransportCapabilities::iroh_direct_default();
        let route = TransportRouter::select(
            &TransferIntent {
                kind: TransferKind::BackfillChunk,
                payload_bytes: 128 * 1024,
                requires_streaming: false,
            },
            Some(&remote),
        );
        assert_eq!(route, TransportRoute::IrohDirect);
    }

    #[test]
    fn missing_remote_capability_falls_back_to_iroh_control() {
        let route = TransportRouter::select(
            &TransferIntent {
                kind: TransferKind::DirectMessage,
                payload_bytes: 4 * 1024,
                requires_streaming: true,
            },
            None,
        );
        assert_eq!(route, TransportRoute::IrohControl);
    }
}
