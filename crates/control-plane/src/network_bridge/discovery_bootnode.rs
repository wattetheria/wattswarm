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

#[derive(Debug, Serialize)]
struct DiscoveryBootnodeTopicProviderBatchRequest<'a> {
    network_id: &'a str,
    queries: Vec<DiscoveryBootnodeTopicProviderBatchQuery<'a>>,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct DiscoveryBootnodeTopicProviderBatchQuery<'a> {
    feed_key: &'a str,
    scope_hint: &'a str,
}

#[derive(Debug, Default)]
struct DiscoveryBootnodeFailureLogState {
    consecutive_failures: u64,
    suppressed_failures: u64,
}

type DiscoveryBootnodeFailureLogStates = HashMap<String, DiscoveryBootnodeFailureLogState>;

static DISCOVERY_BOOTNODE_FAILURE_LOG_STATES: OnceLock<Mutex<DiscoveryBootnodeFailureLogStates>> =
    OnceLock::new();

#[derive(Debug)]
struct DiscoveryBootnodeRecordItem(SignedDiscoveryNodeRecord);

impl<'de> Deserialize<'de> for DiscoveryBootnodeRecordItem {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let record = match value {
            Value::Object(mut object) => match object.remove("record") {
                Some(record) => record,
                None => Value::Object(object),
            },
            value => value,
        };
        serde_json::from_value(record)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl DiscoveryBootnodeRecordItem {
    fn into_record(self) -> SignedDiscoveryNodeRecord {
        self.0
    }
}

impl DiscoveryBootnodeSettings {
    pub(super) fn has_local_geo(&self) -> bool {
        self.latitude.is_some() && self.longitude.is_some()
    }
}

fn discovery_bootnode_failure_log_states() -> &'static Mutex<DiscoveryBootnodeFailureLogStates> {
    DISCOVERY_BOOTNODE_FAILURE_LOG_STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn discovery_bootnode_failure_log_decision(endpoint: &str) -> Option<u64> {
    let mut states = discovery_bootnode_failure_log_states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = states.entry(endpoint.to_owned()).or_default();
    state.consecutive_failures = state.consecutive_failures.saturating_add(1);
    if state.consecutive_failures == 1
        || state.consecutive_failures % DISCOVERY_BOOTNODE_FAILURE_LOG_EVERY == 0
    {
        let suppressed = state.suppressed_failures;
        state.suppressed_failures = 0;
        Some(suppressed)
    } else {
        state.suppressed_failures = state.suppressed_failures.saturating_add(1);
        None
    }
}

pub(super) fn reset_discovery_bootnode_failure_log_state(endpoint: &str) {
    let mut states = discovery_bootnode_failure_log_states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    states.remove(endpoint);
}

fn log_discovery_bootnode_query_failure(endpoint: &str, error: &reqwest::Error) {
    let Some(suppressed) = discovery_bootnode_failure_log_decision(endpoint) else {
        return;
    };
    if suppressed > 0 {
        eprintln!(
            "discovery bootnode query failed {endpoint}: {error:#} (suppressed {suppressed} repeated failures)"
        );
    } else {
        eprintln!("discovery bootnode query failed {endpoint}: {error:#}");
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
    node: &Node,
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
    for discovery_url in &discovery_urls {
        let endpoint = match discovery_bootnode_query_endpoint(&discovery_url, network_id, settings)
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                eprintln!("discovery bootnode URL skipped {discovery_url}: {error}");
                continue;
            }
        };
        fetch_discovery_bootnode_records(
            &client,
            endpoint,
            network_id,
            now_ms,
            &mut seen,
            &mut records,
        );
    }
    let subscriptions = match node
        .store
        .list_active_feed_subscriptions(network_id, &node.node_id())
    {
        Ok(subscriptions) => subscriptions,
        Err(error) => {
            eprintln!("discovery bootnode topic provider query skipped: {error:#}");
            return Ok(records);
        }
    };
    let subscriptions = subscriptions
        .into_iter()
        .filter(|subscription| subscription.feed_key.trim() != crate::control::PRIVATE_DM_FEED_KEY)
        .collect::<Vec<_>>();
    if subscriptions.is_empty() {
        return Ok(records);
    }
    for discovery_url in &discovery_urls {
        let batch_endpoint =
            match discovery_bootnode_topic_provider_batch_endpoint(discovery_url, network_id) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    eprintln!("discovery bootnode URL skipped {discovery_url}: {error}");
                    continue;
                }
            };
        if fetch_discovery_bootnode_topic_provider_batch_records(
            &client,
            batch_endpoint,
            network_id,
            &subscriptions,
            now_ms,
            &mut seen,
            &mut records,
        ) {
            continue;
        }
        for subscription in &subscriptions {
            let endpoint = match discovery_bootnode_topic_provider_endpoint(
                discovery_url,
                network_id,
                &subscription.feed_key,
                &subscription.scope_hint,
            ) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    eprintln!("discovery bootnode URL skipped {discovery_url}: {error}");
                    continue;
                }
            };
            fetch_discovery_bootnode_records(
                &client,
                endpoint,
                network_id,
                now_ms,
                &mut seen,
                &mut records,
            );
        }
    }
    Ok(records)
}

fn fetch_discovery_bootnode_records(
    client: &reqwest::blocking::Client,
    endpoint: String,
    network_id: &str,
    now_ms: u64,
    seen: &mut HashSet<String>,
    records: &mut Vec<SignedDiscoveryNodeRecord>,
) {
    match client.get(endpoint.clone()).send() {
        Ok(response) => match response.error_for_status() {
            Ok(response) => match response.json::<DiscoveryBootnodeNearbyResponse>() {
                Ok(payload) => {
                    reset_discovery_bootnode_failure_log_state(&endpoint);
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
            Err(error) => log_discovery_bootnode_query_failure(&endpoint, &error),
        },
        Err(error) => log_discovery_bootnode_query_failure(&endpoint, &error),
    }
}

fn fetch_discovery_bootnode_topic_provider_batch_records(
    client: &reqwest::blocking::Client,
    endpoint: String,
    network_id: &str,
    subscriptions: &[crate::storage::FeedSubscriptionRow],
    now_ms: u64,
    seen: &mut HashSet<String>,
    records: &mut Vec<SignedDiscoveryNodeRecord>,
) -> bool {
    let request = DiscoveryBootnodeTopicProviderBatchRequest {
        network_id,
        queries: subscriptions
            .iter()
            .map(|subscription| DiscoveryBootnodeTopicProviderBatchQuery {
                feed_key: &subscription.feed_key,
                scope_hint: &subscription.scope_hint,
            })
            .collect(),
        limit: DISCOVERY_BOOTNODE_QUERY_LIMIT,
    };
    match client.post(endpoint.clone()).json(&request).send() {
        Ok(response) => {
            let status = response.status();
            if status == reqwest::StatusCode::NOT_FOUND
                || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
            {
                return false;
            }
            match response.error_for_status() {
                Ok(response) => match response.json::<DiscoveryBootnodeNearbyResponse>() {
                    Ok(payload) => {
                        reset_discovery_bootnode_failure_log_state(&endpoint);
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
                        eprintln!(
                            "discovery bootnode response decode failed {endpoint}: {error:#}"
                        );
                    }
                },
                Err(error) => log_discovery_bootnode_query_failure(&endpoint, &error),
            }
        }
        Err(error) => log_discovery_bootnode_query_failure(&endpoint, &error),
    }
    true
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

fn discovery_bootnode_topic_provider_batch_endpoint(
    base_url: &str,
    network_id: &str,
) -> Result<String> {
    let base_url = base_url.trim().trim_end_matches('/');
    let endpoint = if base_url.ends_with("/api/network/discovery/topic-providers/batch") {
        base_url.to_owned()
    } else if base_url.ends_with("/api/network/discovery/topic-providers") {
        format!("{base_url}/batch")
    } else if base_url.ends_with("/api/network/discovery") {
        format!("{base_url}/topic-providers/batch")
    } else {
        format!("{base_url}/api/network/discovery/topic-providers/batch")
    };
    let mut url = reqwest::Url::parse(&endpoint)
        .with_context(|| format!("parse discovery bootnode URL {endpoint}"))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("network_id", network_id);
    }
    Ok(url.to_string())
}

fn discovery_bootnode_topic_provider_endpoint(
    base_url: &str,
    network_id: &str,
    feed_key: &str,
    scope_hint: &str,
) -> Result<String> {
    let base_url = base_url.trim().trim_end_matches('/');
    let endpoint = if base_url.ends_with("/api/network/discovery/topic-providers") {
        base_url.to_owned()
    } else if base_url.ends_with("/api/network/discovery") {
        format!("{base_url}/topic-providers")
    } else {
        format!("{base_url}/api/network/discovery/topic-providers")
    };
    let mut url = reqwest::Url::parse(&endpoint)
        .with_context(|| format!("parse discovery bootnode URL {endpoint}"))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs
            .append_pair("network_id", network_id)
            .append_pair("feed_key", feed_key)
            .append_pair("scope_hint", scope_hint)
            .append_pair("limit", &DISCOVERY_BOOTNODE_QUERY_LIMIT.to_string());
    }
    Ok(url.to_string())
}

pub(super) fn record_discovery_signature_verification_diagnostic(
    state_dir: &Path,
    record: &SignedDiscoveryNodeRecord,
    verification: &Result<()>,
) {
    if !diagnostics::debug_diagnostics_enabled() {
        return;
    }
    let source_agent_id = record
        .body
        .source_agent_card
        .as_ref()
        .map(|card| card.agent_id.as_str());
    let error = verification
        .as_ref()
        .err()
        .map(|error| format!("{error:#}"));
    diagnostics::record_diagnostic(
        Some(state_dir),
        diagnostics::DiagnosticEvent::new(
            if verification.is_ok() { "info" } else { "warn" },
            "identity",
            "discovery_v1.signature.verify",
            if verification.is_ok() {
                "success"
            } else {
                "failed"
            },
            format!(
                "discovery record signature verification {} peer={}",
                if verification.is_ok() {
                    "succeeded"
                } else {
                    "failed"
                },
                record.body.node_id
            ),
        )
        .object("peer", Some(record.body.node_id.clone()))
        .source_node_id(Some(record.body.node_id.clone()))
        .details(json!({
            "peer_id": record.body.node_id,
            "source_agent_id": source_agent_id,
            "has_source_agent_card": record.body.source_agent_card.is_some(),
            "has_source_agent_card_signature": record
                .body
                .source_agent_card
                .as_ref()
                .is_some_and(|card| card.signature.is_some()),
            "verification_scope": "node_signature_and_source_agent_card_hash",
            "did_agent_signature_checked": false,
            "error": error,
        })),
    );
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
    let verification = record.verify_fresh_at(now_ms);
    record_discovery_signature_verification_diagnostic(state_dir, &record, &verification);
    verification?;
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
    let remote_peer = NetworkNodeId::new(remote_network_peer_id.clone())?;
    let registered =
        service.upsert_remote_contact_material(remote_network_peer_id.clone(), contact)?;
    service.reactivate_discovered_peer_reconnect(remote_peer, registered);
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
    let mut material = json!({
        "node_id": node_id,
        "peer_id": contact.peer_id,
        "listen_addrs": contact.metadata.listen_addrs.clone(),
        "generated_at": record.body.updated_at_ms,
        "transports": [contact],
        "recommended_routes": recommended_routes,
        "discovery_protocol": protocol_version,
    });
    if let Some(source_agent_card) = &record.body.source_agent_card {
        material["source_agent_card"] = serde_json::to_value(source_agent_card)?;
    }
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
