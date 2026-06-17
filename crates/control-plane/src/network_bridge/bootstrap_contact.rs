use super::*;

pub(super) fn build_contact_material(
    state_dir: &Path,
    local_peer_id: &str,
) -> Result<RawContactMaterial> {
    let generated_at = observed_at_ms();
    let identity = crate::control::load_local_identity(state_dir)?;
    let private_message_keypair =
        crate::control::load_or_create_private_message_keypair_state(state_dir)?;
    let iroh_contact =
        export_local_contact_material_for_network_peer_id(state_dir, local_peer_id, generated_at)?;
    let material = json!({
        "node_id": identity.node_id(),
        "peer_id": local_peer_id,
        "listen_addrs": iroh_contact.metadata.listen_addrs.clone(),
        "generated_at": generated_at,
        "transports": [iroh_contact],
        "recommended_routes": crate::control::recommended_data_routes(Some(&iroh_contact.metadata.capabilities)),
        "encryption": {
            "private_message": {
                "scheme": "wattswarm.private.dm.v1",
                "key_agreement": "x25519",
                "cipher": "chacha20poly1305",
                "public_key_b64": private_message_keypair.public_key_b64,
            }
        },
    });
    let signature = identity.sign_bytes(&serde_json::to_vec(&material)?);
    Ok(RawContactMaterial {
        material_json: serde_json::to_string(&material)?,
        signature: Some(signature),
        generated_at,
    })
}

pub fn export_local_bootstrap_contact(state_dir: &Path) -> Result<String> {
    let material = export_local_bootstrap_contact_material(state_dir)?;
    let endpoint_id = material
        .metadata
        .endpoint_id
        .as_deref()
        .unwrap_or(&material.peer_id);
    let addr = material
        .metadata
        .listen_addrs
        .first()
        .ok_or_else(|| anyhow!("local Iroh endpoint has no direct bootstrap address yet"))?;
    Ok(format!("{endpoint_id}@{addr}"))
}

pub fn export_local_bootstrap_contact_json(state_dir: &Path) -> Result<String> {
    let _ = crate::control::local_node_id(state_dir)?;
    let endpoint_id = local_endpoint_id_from_state_dir(state_dir)?.to_string();
    Ok(build_contact_material(state_dir, &endpoint_id)?.material_json)
}

fn export_local_bootstrap_contact_material(state_dir: &Path) -> Result<TransportContactMaterial> {
    let _ = crate::control::local_node_id(state_dir)?;
    let endpoint_id = local_endpoint_id_from_state_dir(state_dir)?.to_string();
    export_local_contact_material_for_network_peer_id(state_dir, &endpoint_id, observed_at_ms())
}

pub fn validate_bootstrap_contact(raw_contact: &str) -> Result<()> {
    parse_startup_bootstrap_contact(raw_contact).map(|_| ())
}

pub(super) fn upsert_contact_material_for_peer(
    state_dir: &Path,
    remote_node_id: &str,
    contact_material: &RawContactMaterial,
) -> Result<()> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.node_id == remote_node_id);
    let record = crate::control::PeerMetadataRecord {
        node_id: remote_node_id.to_owned(),
        network_id: existing.as_ref().and_then(|entry| entry.network_id.clone()),
        params_version: existing.as_ref().and_then(|entry| entry.params_version),
        params_hash: existing
            .as_ref()
            .and_then(|entry| entry.params_hash.clone()),
        agent_version_raw: existing
            .as_ref()
            .and_then(|entry| entry.agent_version_raw.clone()),
        agent_version_prefix: existing
            .as_ref()
            .and_then(|entry| entry.agent_version_prefix.clone()),
        protocol_version: existing
            .as_ref()
            .and_then(|entry| entry.protocol_version.clone()),
        observed_addr: existing
            .as_ref()
            .and_then(|entry| entry.observed_addr.clone()),
        listen_addrs: existing
            .as_ref()
            .map_or_else(Vec::new, |entry| entry.listen_addrs.clone()),
        protocols: existing
            .as_ref()
            .map_or_else(Vec::new, |entry| entry.protocols.clone()),
        handshake_status: existing.as_ref().map_or_else(
            || "contact_material".to_owned(),
            |entry| entry.handshake_status.clone(),
        ),
        last_error: None,
        contact_material: serde_json::from_str(&contact_material.material_json).ok(),
        contact_material_signature: contact_material.signature.clone(),
        contact_material_updated_at: Some(contact_material.generated_at),
        first_identified_at: existing
            .as_ref()
            .map_or(now, |entry| entry.first_identified_at),
        last_identified_at: existing
            .as_ref()
            .map_or(now, |entry| entry.last_identified_at),
    };
    crate::control::save_peer_metadata_record_state(state_dir, &record)
}

pub(super) fn configured_iroh_relay_urls() -> Vec<String> {
    let Ok(raw) = env::var(wattswarm_network_transport_iroh::ENV_IROH_RELAY_URLS) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for value in raw
        .split(|ch| matches!(ch, ',' | '\n' | '\r'))
        .map(str::trim)
        .map(|value| value.trim_end_matches('/'))
        .filter(|value| !value.is_empty())
    {
        if seen.insert(value.to_owned()) {
            urls.push(value.to_owned());
        }
    }
    urls
}

pub(super) fn relay_contact_material_for_peer(
    peer: &NetworkNodeId,
    relay_urls: &[String],
) -> Result<(RawContactMaterial, TransportContactMaterial)> {
    if relay_urls.is_empty() {
        bail!("configured Iroh relay URLs are required");
    }
    let generated_at = observed_at_ms();
    let peer_id = NetworkNodeId::new(peer.to_string())?.to_string();
    let contact = TransportContactMaterial {
        transport: DataTransportRoute::IrohDirect.as_str().to_owned(),
        peer_id: peer_id.clone(),
        metadata: TransportMetadata {
            route: DataTransportRoute::IrohDirect,
            generated_at,
            endpoint_id: Some(peer_id.clone()),
            alpn: Some(wattswarm_network_transport_iroh::DEFAULT_IROH_ALPN.to_owned()),
            listen_addrs: Vec::new(),
            capabilities: PeerTransportCapabilities::iroh_direct_default(),
        },
        extra: json!({
            "endpoint_id": peer_id,
            "alpn": wattswarm_network_transport_iroh::DEFAULT_IROH_ALPN,
            "direct_addrs": [],
            "relay_urls": relay_urls,
        }),
    };
    let material = startup_contact_material_from_transport(contact.clone());
    Ok((
        RawContactMaterial {
            material_json: serde_json::to_string(&material)?,
            signature: None,
            generated_at,
        },
        contact,
    ))
}

impl NetworkBridgeService {
    pub(super) fn ensure_peer_relay_contact_material(
        &mut self,
        state_dir: &Path,
        peer: &NetworkNodeId,
    ) -> Result<bool> {
        if self.runtime.allows_outbound_backfill_to(peer) {
            return Ok(false);
        }
        let relay_urls = configured_iroh_relay_urls();
        if relay_urls.is_empty() {
            return Ok(false);
        }
        let (raw_contact, contact) = relay_contact_material_for_peer(peer, &relay_urls)?;
        upsert_contact_material_for_peer(state_dir, peer.as_str(), &raw_contact)?;
        self.runtime
            .upsert_remote_contact_material(peer.to_string(), contact)?;
        Ok(true)
    }
}

pub(super) fn candidate_peer_addrs(
    state_dir: &Path,
    remote_node_id: &str,
) -> Result<Vec<NetworkAddress>> {
    let mut addrs = Vec::new();
    for record in crate::control::load_peer_metadata_records_state(state_dir)? {
        if record.node_id != remote_node_id {
            continue;
        }
        for contact in record.transport_contact_materials() {
            for direct_addr in transport_contact_direct_network_addrs(&contact) {
                if let Ok(addr) = direct_addr.parse::<NetworkAddress>() {
                    addrs.push(addr);
                }
            }
        }
    }
    addrs.sort();
    addrs.dedup();
    Ok(addrs)
}

fn transport_contact_direct_network_addrs(contact: &TransportContactMaterial) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut addrs = Vec::new();
    if let Some(items) = contact.extra.get("direct_addrs").and_then(Value::as_array) {
        for item in items {
            let Some(raw) = item.as_str().map(str::trim) else {
                continue;
            };
            if raw.parse::<SocketAddr>().is_ok() && seen.insert(raw.to_owned()) {
                addrs.push(raw.to_owned());
            }
        }
    }
    for raw in &contact.metadata.listen_addrs {
        let trimmed = raw.trim();
        if trimmed.parse::<SocketAddr>().is_ok() && seen.insert(trimmed.to_owned()) {
            addrs.push(trimmed.to_owned());
        }
    }
    addrs
}

impl NetworkBridgeService {
    pub(super) fn load_startup_bootstrap_contacts(&mut self, state_dir: &Path) -> Result<()> {
        for raw_contact in load_bootstrap_contacts_from_startup_config(state_dir) {
            let (remote_node_id, material, contacts) =
                match parse_startup_bootstrap_contact(&raw_contact) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        eprintln!("skip invalid startup Iroh bootstrap contact: {error}");
                        continue;
                    }
                };
            upsert_startup_bootstrap_contact_material(state_dir, &remote_node_id, material)
                .with_context(|| {
                    format!("persist startup bootstrap contact material for {remote_node_id}")
                })?;
            for contact in contacts {
                if contact.transport == DataTransportRoute::IrohDirect.as_str() {
                    let remote_network_peer_id = iroh_contact_network_peer_id(&contact)?;
                    self.remember_peer_address_from_contact(&remote_network_peer_id, &contact);
                    self.runtime
                        .upsert_remote_contact_material(remote_network_peer_id.clone(), contact)
                        .with_context(|| {
                            format!(
                                "register startup bootstrap contact material for {remote_node_id}"
                            )
                        })?;
                    if let Ok(peer) = NetworkNodeId::new(remote_network_peer_id) {
                        self.remember_global_backfill_provider(peer.clone());
                        self.schedule_peer_reconnect(peer);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn load_iroh_contact_material(&mut self, state_dir: &Path) -> Result<()> {
        for record in crate::control::load_peer_metadata_records_state(state_dir)? {
            for contact in record.transport_contact_materials() {
                if contact.transport == DataTransportRoute::IrohDirect.as_str() {
                    let remote_network_peer_id = match iroh_contact_network_peer_id(&contact) {
                        Ok(peer) => peer,
                        Err(error) => {
                            eprintln!(
                                "skip invalid persisted Iroh contact material for {}: {error:#}",
                                record.node_id
                            );
                            continue;
                        }
                    };
                    self.remember_peer_address_from_contact(&remote_network_peer_id, &contact);
                    let _ = self
                        .runtime
                        .upsert_remote_contact_material(remote_network_peer_id.clone(), contact)?;
                    if let Ok(peer) = NetworkNodeId::new(remote_network_peer_id) {
                        self.schedule_peer_reconnect(peer);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Default)]
struct StartupBootstrapConfig {
    #[serde(default)]
    bootstrap_contacts: Vec<String>,
}

fn load_bootstrap_contacts_from_startup_config(state_dir: &Path) -> Vec<String> {
    let path = state_dir.join(STARTUP_CONFIG_FILE);
    let Ok(bytes) = fs::read(&path) else {
        return Vec::new();
    };
    match serde_json::from_slice::<StartupBootstrapConfig>(&bytes) {
        Ok(config) => config
            .bootstrap_contacts
            .into_iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect(),
        Err(error) => {
            eprintln!(
                "failed to parse startup bootstrap contacts from {}: {error}",
                path.display()
            );
            Vec::new()
        }
    }
}

fn parse_startup_bootstrap_contact(
    raw_contact: &str,
) -> Result<(String, Value, Vec<TransportContactMaterial>)> {
    let raw_contact = raw_contact.trim();
    if !raw_contact.starts_with('{') {
        let contact = transport_contact_from_short_bootstrap_contact(raw_contact)?;
        let remote_network_peer_id = iroh_contact_network_peer_id(&contact)?;
        return Ok((
            remote_network_peer_id,
            startup_contact_material_from_transport(contact.clone()),
            vec![contact],
        ));
    }
    let raw_value: Value = serde_json::from_str(raw_contact)
        .context("startup bootstrap contact must be JSON contact material")?;
    let material = if raw_value.get("transports").is_some() {
        raw_value
    } else {
        let contact: TransportContactMaterial =
            serde_json::from_value(raw_value).context("decode Iroh transport contact material")?;
        startup_contact_material_from_transport(contact)
    };
    let transports = material
        .get("transports")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("bootstrap contact missing transports"))?
        .iter()
        .map(|entry| {
            serde_json::from_value::<TransportContactMaterial>(entry.clone())
                .context("decode bootstrap transport contact")
        })
        .collect::<Result<Vec<_>>>()?;
    let mut remote_network_peer_id = None;
    for contact in &transports {
        if contact.transport != DataTransportRoute::IrohDirect.as_str() {
            continue;
        }
        let contact_peer = iroh_contact_network_peer_id(contact)?;
        if let Some(existing) = &remote_network_peer_id
            && existing != &contact_peer
        {
            bail!(
                "bootstrap contact has multiple Iroh endpoint ids: {existing} and {contact_peer}"
            );
        }
        remote_network_peer_id = Some(contact_peer);
    }
    let remote_network_peer_id = remote_network_peer_id
        .ok_or_else(|| anyhow!("bootstrap contact missing Iroh transport"))?;
    Ok((remote_network_peer_id, material, transports))
}

fn transport_contact_from_short_bootstrap_contact(
    raw_contact: &str,
) -> Result<TransportContactMaterial> {
    let (endpoint_id, raw_addr) = raw_contact
        .rsplit_once('@')
        .ok_or_else(|| anyhow!("bootstrap contact must be <iroh-node-id>@<host:port>"))?;
    let endpoint_id = NetworkNodeId::new(endpoint_id.to_owned())
        .context("bootstrap contact peer must be an iroh NodeId / EndpointId")?
        .to_string();
    let raw_addr = raw_addr.trim();
    if raw_addr.is_empty() {
        bail!("bootstrap contact address is empty");
    }
    let _: SocketAddr = raw_addr
        .parse()
        .with_context(|| format!("parse bootstrap contact address {raw_addr}"))?;
    let generated_at = observed_at_ms();
    let capabilities = PeerTransportCapabilities::iroh_direct_default();
    Ok(TransportContactMaterial {
        transport: DataTransportRoute::IrohDirect.as_str().to_owned(),
        peer_id: endpoint_id.clone(),
        metadata: TransportMetadata {
            route: DataTransportRoute::IrohDirect,
            generated_at,
            endpoint_id: Some(endpoint_id.clone()),
            alpn: Some(wattswarm_network_transport_iroh::DEFAULT_IROH_ALPN.to_owned()),
            listen_addrs: vec![raw_addr.to_owned()],
            capabilities,
        },
        extra: json!({
            "endpoint_id": endpoint_id,
            "alpn": wattswarm_network_transport_iroh::DEFAULT_IROH_ALPN,
            "direct_addrs": [raw_addr],
            "relay_urls": []
        }),
    })
}

fn startup_contact_material_from_transport(contact: TransportContactMaterial) -> Value {
    json!({
        "node_id": contact.peer_id.clone(),
        "peer_id": contact.peer_id.clone(),
        "listen_addrs": contact.metadata.listen_addrs.clone(),
        "generated_at": contact.metadata.generated_at,
        "transports": [contact.clone()],
        "recommended_routes": crate::control::recommended_data_routes(Some(&contact.metadata.capabilities)),
    })
}

pub(super) fn iroh_contact_network_peer_id(contact: &TransportContactMaterial) -> Result<String> {
    if contact.transport != DataTransportRoute::IrohDirect.as_str() {
        bail!("transport contact is not iroh_direct");
    }
    let metadata_endpoint = contact.metadata.endpoint_id.as_deref().map(str::trim);
    let extra_endpoint = contact
        .extra
        .get("endpoint_id")
        .and_then(Value::as_str)
        .map(str::trim);
    let endpoint_id = extra_endpoint
        .or(metadata_endpoint)
        .unwrap_or(contact.peer_id.as_str())
        .trim();
    let endpoint_id = NetworkNodeId::new(endpoint_id.to_owned())
        .context("iroh contact endpoint_id must be an iroh NodeId / EndpointId")?
        .to_string();
    if let Some(metadata_endpoint) = metadata_endpoint
        && metadata_endpoint != endpoint_id
    {
        bail!(
            "iroh contact metadata endpoint_id {metadata_endpoint} does not match endpoint_id {endpoint_id}"
        );
    }
    if contact.peer_id != endpoint_id {
        bail!(
            "iroh contact peer_id {} must match endpoint_id {}",
            contact.peer_id,
            endpoint_id
        );
    }
    Ok(endpoint_id)
}

fn upsert_startup_bootstrap_contact_material(
    state_dir: &Path,
    remote_node_id: &str,
    material: Value,
) -> Result<()> {
    let now = observed_at_ms();
    let existing = crate::control::load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.node_id == remote_node_id);
    let listen_addrs = material
        .get("listen_addrs")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_else(|| {
            existing
                .as_ref()
                .map_or_else(Vec::new, |record| record.listen_addrs.clone())
        });
    let record = crate::control::PeerMetadataRecord {
        node_id: remote_node_id.to_owned(),
        network_id: material
            .get("network_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| existing.as_ref().and_then(|entry| entry.network_id.clone())),
        params_version: existing.as_ref().and_then(|entry| entry.params_version),
        params_hash: existing
            .as_ref()
            .and_then(|entry| entry.params_hash.clone()),
        agent_version_raw: existing
            .as_ref()
            .and_then(|entry| entry.agent_version_raw.clone()),
        agent_version_prefix: existing
            .as_ref()
            .and_then(|entry| entry.agent_version_prefix.clone()),
        protocol_version: existing
            .as_ref()
            .and_then(|entry| entry.protocol_version.clone()),
        observed_addr: existing
            .as_ref()
            .and_then(|entry| entry.observed_addr.clone()),
        listen_addrs,
        protocols: existing
            .as_ref()
            .map_or_else(Vec::new, |entry| entry.protocols.clone()),
        handshake_status: "startup_contact".to_owned(),
        last_error: None,
        contact_material: Some(material),
        contact_material_signature: existing
            .as_ref()
            .and_then(|entry| entry.contact_material_signature.clone()),
        contact_material_updated_at: Some(now),
        first_identified_at: existing
            .as_ref()
            .map_or(now, |entry| entry.first_identified_at),
        last_identified_at: now,
    };
    crate::control::save_peer_metadata_record_state(state_dir, &record)
}
