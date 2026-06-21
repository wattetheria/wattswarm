use crate::control::{load_discovery_bootnode_urls_state, local_peer_id, open_configured_node};
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::startup_config::{load_startup_config, startup_config_path};
use crate::storage::PgStore;
use crate::types::TopicProviderCapabilities;
use anyhow::{Context, Result};
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::time::Duration;
use wattswarm_crypto::NodeIdentity;
use wattswarm_network_discovery::{
    DEFAULT_RECORD_TTL_MS, DiscoveryGeo, DiscoveryNodeRecordBody, DiscoveryRecordCapabilities,
    DiscoveryRecordUpsert, DiscoveryRoutingTable, DiscoveryTopicProvider,
    DiscoveryTopicProviderCapabilities, SignedDiscoveryNodeRecord,
};
use wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id;
use wattswarm_protocol::types::SourceAgentCard;

const DISCOVERY_RECORDS_FILE: &str = "discovery_records_v1.json";
const DEFAULT_DISCOVERY_QUERY_LIMIT: usize = 50;
const MAX_DISCOVERY_QUERY_LIMIT: usize = 200;
const DISCOVERY_RECORDS_ROUTE: &str = "/api/network/discovery/records";
const DEFAULT_DISCOVERY_RECORD_RADIUS_KM: f64 = 1000.0;
const DISCOVERY_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(3);
const DISCOVERY_AGENT_CARD_ENABLED_ENV: &str = "WATTSWARM_DISCOVERY_AGENT_CARD_ENABLED";
const DISCOVERY_AGENT_CARD_PATH_ENV: &str = "WATTSWARM_DISCOVERY_AGENT_CARD_PATH";
const DEFAULT_DISCOVERY_AGENT_CARD_PATH: &str =
    "/var/lib/wattetheria/.agent-participation/agent-card.json";

#[derive(Debug, Deserialize)]
pub(crate) struct NearbyDiscoveryQuery {
    network_id: String,
    latitude: f64,
    longitude: f64,
    #[serde(default)]
    radius_km: Option<f64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CapabilityDiscoveryQuery {
    network_id: String,
    capability: String,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopicProviderDiscoveryQuery {
    network_id: String,
    feed_key: String,
    scope_hint: String,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoveryAnnounceReport {
    pub attempted: usize,
    pub succeeded: usize,
    pub failed: usize,
}

pub fn maybe_announce_local_record_to_discovery_bootnodes(
    state_dir: &FsPath,
    db_path: &FsPath,
) -> Result<DiscoveryAnnounceReport> {
    let discovery_urls = load_discovery_bootnode_urls_state(state_dir)?;
    if discovery_urls.is_empty() {
        return Ok(DiscoveryAnnounceReport {
            attempted: 0,
            succeeded: 0,
            failed: 0,
        });
    }
    let record = build_local_discovery_record(state_dir, db_path)?;
    let client = reqwest::blocking::Client::builder()
        .timeout(DISCOVERY_ANNOUNCE_TIMEOUT)
        .build()
        .context("build discovery announce HTTP client")?;
    let mut report = DiscoveryAnnounceReport {
        attempted: discovery_urls.len(),
        succeeded: 0,
        failed: 0,
    };
    for discovery_url in discovery_urls {
        let endpoint = discovery_records_endpoint(&discovery_url);
        let response = client.post(&endpoint).json(&record).send();
        match response.and_then(reqwest::blocking::Response::error_for_status) {
            Ok(_) => report.succeeded += 1,
            Err(error) => {
                report.failed += 1;
                eprintln!("wattswarm discovery announce failed endpoint={endpoint}: {error}");
            }
        }
    }
    Ok(report)
}

pub(crate) async fn discovery_announce_record(
    State(state): State<UiServerState>,
    Json(record): Json<SignedDiscoveryNodeRecord>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let now_ms = now_ms();
        let mut table = load_discovery_records(&state_clone.state_dir, now_ms)?;
        let status = table.announce_record(record.clone(), now_ms)?;
        save_discovery_records(&state_clone.state_dir, &table, now_ms)?;
        Ok(json!({
            "ok": true,
            "status": upsert_status(status),
            "node_id": record.body.node_id,
            "network_id": record.body.network_id,
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn discovery_find_node(
    State(state): State<UiServerState>,
    Path(node_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let now_ms = now_ms();
        let table = load_discovery_records(&state_clone.state_dir, now_ms)?;
        Ok(json!({
            "ok": true,
            "record": table.find_node(&node_id, now_ms),
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn discovery_find_nearby(
    State(state): State<UiServerState>,
    Query(query): Query<NearbyDiscoveryQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let origin = DiscoveryGeo {
            latitude: query.latitude,
            longitude: query.longitude,
            radius_km: query.radius_km.unwrap_or(0.0),
        };
        origin.validate()?;
        let now_ms = now_ms();
        let table = load_discovery_records(&state_clone.state_dir, now_ms)?;
        let records = table.find_nearby(
            &query.network_id,
            &origin,
            now_ms,
            normalize_limit(query.limit),
        );
        Ok(json!({
            "ok": true,
            "records": records,
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn discovery_find_capability(
    State(state): State<UiServerState>,
    Query(query): Query<CapabilityDiscoveryQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let now_ms = now_ms();
        let table = load_discovery_records(&state_clone.state_dir, now_ms)?;
        let records = table.find_capability(
            &query.network_id,
            &query.capability,
            now_ms,
            normalize_limit(query.limit),
        );
        Ok(json!({
            "ok": true,
            "records": records,
        }))
    })
    .await?;
    Ok(Json(response))
}

pub(crate) async fn discovery_find_topic_providers(
    State(state): State<UiServerState>,
    Query(query): Query<TopicProviderDiscoveryQuery>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let response = run_blocking(move || -> Result<Value> {
        let now_ms = now_ms();
        let table = load_discovery_records(&state_clone.state_dir, now_ms)?;
        let records = table.find_topic_providers(
            &query.network_id,
            &query.feed_key,
            &query.scope_hint,
            now_ms,
            normalize_limit(query.limit),
        );
        Ok(json!({
            "ok": true,
            "records": records,
        }))
    })
    .await?;
    Ok(Json(response))
}

fn load_discovery_records(state_dir: &FsPath, now_ms: u64) -> Result<DiscoveryRoutingTable> {
    let path = discovery_records_path(state_dir);
    let mut table = DiscoveryRoutingTable::new();
    let Ok(bytes) = fs::read(&path) else {
        return Ok(table);
    };
    let records: Vec<SignedDiscoveryNodeRecord> = match serde_json::from_slice(&bytes) {
        Ok(records) => records,
        Err(error) => {
            quarantine_corrupt_discovery_records(&path, &error);
            return Ok(table);
        }
    };
    for record in records {
        if record.verify_fresh_at(now_ms).is_ok() {
            let _ = table.announce_record(record, now_ms)?;
        }
    }
    Ok(table)
}

fn quarantine_corrupt_discovery_records(path: &FsPath, error: &serde_json::Error) {
    let mut quarantine_path = path.as_os_str().to_owned();
    quarantine_path.push(".corrupt");
    eprintln!(
        "wattswarm discovery records at {} are corrupt ({error}); quarantining to {} and starting with an empty table",
        path.display(),
        std::path::Path::new(&quarantine_path).display()
    );
    if let Err(rename_error) = fs::rename(path, &quarantine_path) {
        eprintln!(
            "wattswarm failed to quarantine corrupt discovery records at {}: {rename_error}",
            path.display()
        );
    }
}

fn save_discovery_records(
    state_dir: &FsPath,
    table: &DiscoveryRoutingTable,
    now_ms: u64,
) -> Result<()> {
    fs::create_dir_all(state_dir)?;
    let path = discovery_records_path(state_dir);
    let records = table.active_records(now_ms, usize::MAX);
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_path = state_dir.join(format!(
        ".{DISCOVERY_RECORDS_FILE}.{}.{nonce}.tmp",
        std::process::id()
    ));
    fs::write(&tmp_path, serde_json::to_vec_pretty(&records)?)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn discovery_records_path(state_dir: &FsPath) -> std::path::PathBuf {
    state_dir.join(DISCOVERY_RECORDS_FILE)
}

fn build_local_discovery_record(
    state_dir: &FsPath,
    db_path: &FsPath,
) -> Result<SignedDiscoveryNodeRecord> {
    let network_id = load_current_network_id(state_dir, db_path)?;
    let identity = load_identity_from_state_dir(state_dir)?;
    let now_ms = now_ms();
    let mut body = DiscoveryNodeRecordBody::new(
        network_id,
        identity.node_id(),
        identity.node_id(),
        now_ms,
        now_ms,
    );
    body.ttl_ms = DEFAULT_RECORD_TTL_MS;
    body.capabilities = DiscoveryRecordCapabilities::new(["wattswarm.node"])?;
    body.topic_providers =
        match local_topic_provider_records(state_dir, db_path, &body.network_id, &identity) {
            Ok(providers) => providers,
            Err(error) => {
                eprintln!("wattswarm discovery topic provider export skipped: {error:#}");
                Vec::new()
            }
        };
    body.geo = local_discovery_geo(state_dir)?;
    let peer_id = local_peer_id(state_dir)?;
    if let Ok(contact) =
        export_local_contact_material_for_network_peer_id(state_dir, &peer_id, now_ms)
    {
        body.transport_contact = Some(contact);
    }
    if discovery_agent_card_enabled() {
        match load_discovery_source_agent_card(&body.node_id) {
            Ok(Some(card)) => {
                body.source_agent_card = Some(card);
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!("wattswarm discovery source agent card export skipped: {error:#}");
            }
        }
    }
    sign_discovery_record_with_agent_card_fallback(body, &identity)
}

fn sign_discovery_record_with_agent_card_fallback(
    mut body: DiscoveryNodeRecordBody,
    identity: &NodeIdentity,
) -> Result<SignedDiscoveryNodeRecord> {
    match SignedDiscoveryNodeRecord::sign(body.clone(), identity) {
        Ok(record) => Ok(record),
        Err(error) if body.source_agent_card.is_some() => {
            eprintln!(
                "wattswarm discovery source agent card invalid; signing without card: {error:#}"
            );
            body.source_agent_card = None;
            SignedDiscoveryNodeRecord::sign(body, identity)
        }
        Err(error) => Err(error),
    }
}

fn discovery_agent_card_enabled() -> bool {
    std::env::var(DISCOVERY_AGENT_CARD_ENABLED_ENV)
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true)
}

fn load_discovery_source_agent_card(local_node_id: &str) -> Result<Option<SourceAgentCard>> {
    let path = discovery_agent_card_path();
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read(&path)
        .with_context(|| format!("read discovery source agent card at {}", path.display()))?;
    let card: SourceAgentCard = serde_json::from_slice(&raw)
        .with_context(|| format!("parse discovery source agent card at {}", path.display()))?;
    if let Some(card_node_id) = card.node_id.as_deref()
        && card_node_id != local_node_id
    {
        anyhow::bail!(
            "source_agent_card node_id {} does not match local node_id {}",
            card_node_id,
            local_node_id
        );
    }
    Ok(Some(card))
}

fn discovery_agent_card_path() -> PathBuf {
    std::env::var(DISCOVERY_AGENT_CARD_PATH_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DISCOVERY_AGENT_CARD_PATH))
}

fn local_topic_provider_records(
    state_dir: &FsPath,
    db_path: &FsPath,
    network_id: &str,
    identity: &NodeIdentity,
) -> Result<Vec<DiscoveryTopicProvider>> {
    let node = open_configured_node(state_dir, db_path)?;
    let subscriptions = node
        .store
        .list_active_feed_subscriptions(network_id, &identity.node_id())?;
    subscriptions
        .into_iter()
        .map(|subscription| {
            Ok(DiscoveryTopicProvider {
                feed_key: subscription.feed_key,
                scope_hint: subscription.scope_hint,
                capabilities: discovery_provider_capabilities(
                    subscription.provider_capabilities.as_ref(),
                ),
                updated_at_ms: subscription.updated_at,
            })
        })
        .collect()
}

fn discovery_provider_capabilities(
    capabilities: Option<&TopicProviderCapabilities>,
) -> DiscoveryTopicProviderCapabilities {
    capabilities
        .map(|capabilities| DiscoveryTopicProviderCapabilities {
            live_gossip: capabilities.live_gossip,
            history_backfill: capabilities.history_backfill,
            local_store: capabilities.local_store,
            retention_ms: capabilities.retention_ms,
        })
        .unwrap_or_else(DiscoveryTopicProviderCapabilities::local_history_provider)
}

fn load_current_network_id(state_dir: &FsPath, db_path: &FsPath) -> Result<String> {
    let store = PgStore::open(db_path)?;
    if let Ok(verified) = store.load_verified_network_protocol_params() {
        return Ok(verified.network_id);
    }
    Ok(open_configured_node(state_dir, db_path)?
        .store
        .load_verified_network_protocol_params()?
        .network_id)
}

fn load_identity_from_state_dir(state_dir: &FsPath) -> Result<NodeIdentity> {
    let path = state_dir.join("node_seed.hex");
    let hex_seed = fs::read_to_string(&path)
        .with_context(|| format!("read node seed at {}", path.display()))?;
    let bytes = hex::decode(hex_seed.trim()).context("decode node seed hex")?;
    let seed: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("node seed must be 32 bytes"))?;
    Ok(NodeIdentity::from_seed(seed))
}

fn local_discovery_geo(state_dir: &FsPath) -> Result<Option<DiscoveryGeo>> {
    let config = load_startup_config(&startup_config_path(state_dir))?;
    let (Some(latitude), Some(longitude)) = (config.latitude, config.longitude) else {
        return Ok(None);
    };
    let geo = DiscoveryGeo {
        latitude,
        longitude,
        radius_km: DEFAULT_DISCOVERY_RECORD_RADIUS_KM,
    };
    geo.validate()?;
    Ok(Some(geo))
}

fn discovery_records_endpoint(base_url: &str) -> String {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.ends_with(DISCOVERY_RECORDS_ROUTE) {
        base_url.to_owned()
    } else if base_url.ends_with("/api/network/discovery") {
        format!("{base_url}/records")
    } else {
        format!("{base_url}{DISCOVERY_RECORDS_ROUTE}")
    }
}

fn normalize_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_DISCOVERY_QUERY_LIMIT)
        .clamp(1, MAX_DISCOVERY_QUERY_LIMIT)
}

fn upsert_status(status: DiscoveryRecordUpsert) -> &'static str {
    match status {
        DiscoveryRecordUpsert::Inserted => "inserted",
        DiscoveryRecordUpsert::Updated => "updated",
        DiscoveryRecordUpsert::IgnoredStale => "ignored_stale",
    }
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}
