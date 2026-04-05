use crate::{PeerTransportCapabilities, TransportContactMaterial, TransportError, TransportRoute};

pub trait DirectDataTransportAdapter: Send + Sync {
    fn route(&self) -> TransportRoute;
    fn capabilities(&self) -> &PeerTransportCapabilities;
    fn export_contact_material(
        &self,
        listen_addrs: &[String],
        generated_at: u64,
    ) -> Result<TransportContactMaterial, TransportError>;
}
