use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct DiscoveryBootnodeSettings {
    pub(super) enabled: bool,
    latitude: Option<f64>,
    longitude: Option<f64>,
    pub(super) radius_km: f64,
    pub(super) interval: Duration,
}

#[derive(Debug, Deserialize, Default)]
struct StartupDiscoveryConfig {
    #[serde(default)]
    latitude: Option<f64>,
    #[serde(default)]
    longitude: Option<f64>,
    #[serde(default)]
    nearby_radius_km: Option<f64>,
    #[serde(default)]
    network_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryBootnodeNearbyResponse {
    records: Vec<DiscoveryBootnodeRecordItem>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DiscoveryBootnodeRecordItem {
    Direct(SignedDiscoveryNodeRecord),
    Nearby {
        record: SignedDiscoveryNodeRecord,
        #[serde(default, rename = "distance_km")]
        _distance_km: Option<f64>,
    },
}

impl DiscoveryBootnodeRecordItem {
    fn into_record(self) -> SignedDiscoveryNodeRecord {
        match self {
            Self::Direct(record) | Self::Nearby { record, .. } => record,
        }
    }
}

impl DiscoveryBootnodeSettings {
    pub(super) fn has_local_geo(&self) -> bool {
        self.latitude.is_some() && self.longitude.is_some()
    }
}

pub(super) fn discovery_bootnode_settings_from_state_dir(
    state_dir: &Path,
) -> Result<DiscoveryBootnodeSettings> {
    let startup = load_discovery_startup_config(state_dir);
    let network_mode_allows = startup
        .network_mode
        .as_deref()
        .map(str::trim)
        .map(|value| !value.eq_ignore_ascii_case("local"))
        .unwrap_or(true);
    let radius_km = startup
        .nearby_radius_km
        .unwrap_or(DEFAULT_DISCOVERY_GEO_RADIUS_KM);
    let radius_km = if radius_km.is_finite() && radius_km > 0.0 {
        radius_km
    } else {
        DEFAULT_DISCOVERY_GEO_RADIUS_KM
    };
    let interval = DEFAULT_DISCOVERY_BOOTNODE_QUERY_INTERVAL;
    let latitude = startup.latitude.filter(valid_latitude);
    let longitude = startup.longitude.filter(valid_longitude);
    Ok(DiscoveryBootnodeSettings {
        enabled: network_mode_allows,
        latitude,
        longitude,
        radius_km,
        interval,
    })
}

fn load_discovery_startup_config(state_dir: &Path) -> StartupDiscoveryConfig {
    let path = state_dir.join(STARTUP_CONFIG_FILE);
    let Ok(bytes) = fs::read(&path) else {
        return StartupDiscoveryConfig::default();
    };
    serde_json::from_slice::<StartupDiscoveryConfig>(&bytes).unwrap_or_else(|error| {
        eprintln!(
            "failed to parse discovery config from {}: {error}",
            path.display()
        );
        StartupDiscoveryConfig::default()
    })
}

fn valid_latitude(value: &f64) -> bool {
    value.is_finite() && (-90.0..=90.0).contains(value)
}

fn valid_longitude(value: &f64) -> bool {
    value.is_finite() && (-180.0..=180.0).contains(value)
}

fn distance_km_between(
    local_latitude: f64,
    local_longitude: f64,
    remote_latitude: f64,
    remote_longitude: f64,
) -> f64 {
    let earth_radius_km = 6371.0088_f64;
    let local_latitude = local_latitude.to_radians();
    let remote_latitude = remote_latitude.to_radians();
    let delta_latitude = remote_latitude - local_latitude;
    let delta_longitude = (remote_longitude - local_longitude).to_radians();
    let a = (delta_latitude / 2.0).sin().powi(2)
        + local_latitude.cos() * remote_latitude.cos() * (delta_longitude / 2.0).sin().powi(2);
    2.0 * earth_radius_km * a.sqrt().asin()
}

pub(super) fn query_discovery_bootnodes_for_candidate_records(
    state_dir: &Path,
    network_id: &str,
    settings: &DiscoveryBootnodeSettings,
    now_ms: u64,
) -> Result<Vec<SignedDiscoveryNodeRecord>> {
    if !settings.enabled {
        return Ok(Vec::new());
    }
    let discovery_urls = crate::control::load_discovery_bootnode_urls_state(state_dir)?;
    if discovery_urls.is_empty() {
        return Ok(Vec::new());
    }
    let client = reqwest::blocking::Client::builder()
        .timeout(DISCOVERY_BOOTNODE_QUERY_TIMEOUT)
        .build()
        .context("build discovery bootnode query HTTP client")?;
    let mut records = Vec::new();
    let mut seen = HashSet::new();
    for discovery_url in discovery_urls {
        let endpoint = match discovery_bootnode_query_endpoint(&discovery_url, network_id, settings)
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                eprintln!("discovery bootnode URL skipped {discovery_url}: {error}");
                continue;
            }
        };
        match client.get(endpoint.clone()).send() {
            Ok(response) => match response.error_for_status() {
                Ok(response) => match response.json::<DiscoveryBootnodeNearbyResponse>() {
                    Ok(payload) => {
                        for item in payload.records {
                            let record = item.into_record();
                            if record.verify_fresh_at(now_ms).is_ok()
                                && record.body.network_id == network_id
                                && seen.insert(record.body.node_id.clone())
                            {
                                records.push(record);
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("discovery bootnode response decode failed {endpoint}: {error:#}")
                    }
                },
                Err(error) => eprintln!("discovery bootnode query failed {endpoint}: {error:#}"),
            },
            Err(error) => eprintln!("discovery bootnode query failed {endpoint}: {error:#}"),
        }
    }
    Ok(records)
}

fn discovery_bootnode_query_endpoint(
    base_url: &str,
    network_id: &str,
    settings: &DiscoveryBootnodeSettings,
) -> Result<String> {
    let base_url = base_url.trim().trim_end_matches('/');
    let endpoint = if settings.has_local_geo() {
        if base_url.ends_with("/api/network/discovery/nearby") {
            base_url.to_owned()
        } else if base_url.ends_with("/api/network/discovery") {
            format!("{base_url}/nearby")
        } else {
            format!("{base_url}/api/network/discovery/nearby")
        }
    } else if base_url.ends_with("/api/network/discovery/capability") {
        base_url.to_owned()
    } else if base_url.ends_with("/api/network/discovery") {
        format!("{base_url}/capability")
    } else {
        format!("{base_url}/api/network/discovery/capability")
    };
    let mut url = reqwest::Url::parse(&endpoint)
        .with_context(|| format!("parse discovery bootnode URL {endpoint}"))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("network_id", network_id);
        if let (Some(latitude), Some(longitude)) = (settings.latitude, settings.longitude) {
            pairs
                .append_pair("latitude", &latitude.to_string())
                .append_pair("longitude", &longitude.to_string())
                .append_pair("radius_km", &settings.radius_km.to_string());
        } else {
            pairs.append_pair("capability", DISCOVERY_NODE_CAPABILITY);
        }
        pairs.append_pair("limit", &DISCOVERY_BOOTNODE_QUERY_LIMIT.to_string());
    }
    Ok(url.to_string())
}

pub(super) fn apply_discovery_bootnode_record(
    service: &mut NetworkBridgeService,
    node: &mut Node,
    state_dir: &Path,
    local_peer_id: &str,
    network_id: &str,
    settings: &DiscoveryBootnodeSettings,
    record: SignedDiscoveryNodeRecord,
    now_ms: u64,
) -> Result<bool> {
    record.verify_fresh_at(now_ms)?;
    if record.body.network_id != network_id {
        return Ok(false);
    }
    if record.body.node_id == local_peer_id {
        return Ok(false);
    }
    let distance_km = if settings.has_local_geo() {
        let Some(remote_geo) = &record.body.geo else {
            return Ok(false);
        };
        let Some(distance_km) = discovery_record_distance_km(settings, remote_geo) else {
            return Ok(false);
        };
        if distance_km > settings.radius_km.min(remote_geo.radius_km) {
            return Ok(false);
        }
        Some(distance_km)
    } else {
        None
    };
    let Some(contact) = record.body.transport_contact.clone() else {
        return Ok(false);
    };
    if contact.transport != DataTransportRoute::IrohDirect.as_str() {
        return Ok(false);
    }
    let remote_network_peer_id = iroh_contact_network_peer_id(&contact)?;
    if remote_network_peer_id != record.body.node_id {
        bail!(
            "discovery record node_id {} does not match Iroh contact peer {}",
            record.body.node_id,
            remote_network_peer_id
        );
    }
    let registered = service
        .runtime
        .upsert_remote_contact_material(remote_network_peer_id.clone(), contact)?;
    if !registered {
        return Ok(false);
    }
    upsert_contact_material_for_peer(
        state_dir,
        &record.body.node_id,
        &raw_contact_material_from_discovery_record(&record)?,
    )?;
    mark_peer_metadata_discovery_v1(state_dir, &record.body.node_id, &record.body.network_id)?;
    node.discover_peer(record.body.node_id.clone());
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new(
            "info",
            "discovery",
            "discovery_v1.peer.accepted",
            "ok",
            format!("discovery v1 peer accepted: {}", record.body.node_id),
        )
        .object("peer", Some(record.body.node_id.clone()))
        .source_node_id(Some(record.body.node_id.clone()))
        .details(json!({
            "distance_km": distance_km,
            "registered_contact": registered,
            "radius_km": settings.radius_km,
        })),
    );
    Ok(registered)
}

fn discovery_record_distance_km(
    settings: &DiscoveryBootnodeSettings,
    remote_geo: &DiscoveryGeo,
) -> Option<f64> {
    let local_latitude = settings.latitude?;
    let local_longitude = settings.longitude?;
    if !valid_latitude(&remote_geo.latitude) || !valid_longitude(&remote_geo.longitude) {
        return None;
    }
    Some(distance_km_between(
        local_latitude,
        local_longitude,
        remote_geo.latitude,
        remote_geo.longitude,
    ))
}

fn raw_contact_material_from_discovery_record(
    record: &SignedDiscoveryNodeRecord,
) -> Result<RawContactMaterial> {
    let contact = record
        .body
        .transport_contact
        .clone()
        .ok_or_else(|| anyhow!("discovery record missing transport contact"))?;
    let recommended_routes =
        crate::control::recommended_data_routes(Some(&contact.metadata.capabilities));
    let node_id = record.body.node_id.clone();
    let protocol_version = record.body.protocol_version.clone();
    let material = json!({
        "node_id": node_id,
        "peer_id": contact.peer_id,
        "listen_addrs": contact.metadata.listen_addrs.clone(),
        "generated_at": record.body.updated_at_ms,
        "transports": [contact],
        "recommended_routes": recommended_routes,
        "discovery_protocol": protocol_version,
    });
    Ok(RawContactMaterial {
        material_json: serde_json::to_string(&material)?,
        signature: Some(record.signature_hex.clone()),
        generated_at: record.body.updated_at_ms,
    })
}

fn mark_peer_metadata_discovery_v1(
    state_dir: &Path,
    remote_node_id: &str,
    network_id: &str,
) -> Result<()> {
    let Some(mut record) = crate::control::load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.node_id == remote_node_id)
    else {
        return Ok(());
    };
    record.network_id = Some(network_id.to_owned());
    if record.handshake_status == "unknown" || record.handshake_status == "contact_material" {
        record.handshake_status = "discovery_v1".to_owned();
    }
    record.last_error = None;
    crate::control::save_peer_metadata_record_state(state_dir, &record)
}
