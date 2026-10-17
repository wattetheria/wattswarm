use super::*;

impl Node {
    pub fn publish_global_announcement(
        &mut self,
        announcement_id: String,
        content: serde_json::Value,
        epoch: u64,
        issued_at: u64,
    ) -> Result<Event> {
        let trusted = self.store.load_verified_network_protocol_params()?;
        if let Some(event) = self
            .store
            .get_global_announcement(&trusted.network_id, &announcement_id)?
        {
            if !matches!(
                &event.payload,
                EventPayload::EventAnnouncement(payload)
                    if payload.content == content && payload.authority_node_id == self.node_id()
            ) {
                return Err(SwarmError::InvalidEvent(
                    "announcement_id already published with different content".into(),
                )
                .into());
            }
            return Ok(event);
        }
        let mut payload = crate::types::EventAnnouncementPayload {
            announcement_id,
            network_id: trusted.network_id,
            authority_node_id: self.node_id(),
            issued_at,
            content,
            authority_signature: String::new(),
        };
        payload.authority_signature = self
            .identity
            .sign_bytes(&serde_jcs::to_vec(&payload.unsigned_payload())?);
        self.emit_at(epoch, EventPayload::EventAnnouncement(payload), issued_at)
    }
}
