use crate::control::{load_discovery_bootnode_urls_state, local_peer_id, open_configured_node};
use crate::http::{ApiError, UiServerState, run_blocking};
use crate::startup_config::{load_startup_config, startup_config_path};
use crate::storage::PgStore;
use anyhow::{Context, Result};
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::Path as FsPath;
use std::time::Duration;
use wattswarm_crypto::NodeIdentity;
use wattswarm_network_discovery::{
    DEFAULT_RECORD_TTL_MS, DiscoveryGeo, DiscoveryNodeRecordBody, DiscoveryRecordCapabilities,
    DiscoveryRecordUpsert, DiscoveryRoutingTable, SignedDiscoveryNodeRecord,
};
use wattswarm_network_transport_iroh::export_local_contact_material_for_network_peer_id;

const DISCOVERY_RECORDS_FILE: &str = "discovery_records_v1.json";
const DEFAULT_DISCOVERY_QUERY_LIMIT: usize = 50;
const MAX_DISCOVERY_QUERY_LIMIT: usize = 200;
const DISCOVERY_RECORDS_ROUTE: &str = "/api/network/discovery/records";
const DEFAULT_DISCOVERY_RECORD_RADIUS_KM: f64 = 1000.0;
const DISCOVERY_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(3);

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

fn load_discovery_records(state_dir: &FsPath, now_ms: u64) -> Result<DiscoveryRoutingTable> {
    let path = discovery_records_path(state_dir);
    let mut table = DiscoveryRoutingTable::new();
    let Ok(bytes) = fs::read(&path) else {
        return Ok(table);
    };
    let records: Vec<SignedDiscoveryNodeRecord> = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse discovery records at {}", path.display()))?;
    for record in records {
        if record.verify_fresh_at(now_ms).is_ok() {
            let _ = table.announce_record(record, now_ms)?;
        }
    }
    Ok(table)
}

fn save_discovery_records(
    state_dir: &FsPath,
    table: &DiscoveryRoutingTable,
    now_ms: u64,
) -> Result<()> {
    fs::create_dir_all(state_dir)?;
    let path = discovery_records_path(state_dir);
    let records = table.active_records(now_ms, usize::MAX);
    fs::write(path, serde_json::to_vec_pretty(&records)?)?;
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
    body.geo = local_discovery_geo(state_dir)?;
    let peer_id = local_peer_id(state_dir)?;
    if let Ok(contact) =
        export_local_contact_material_for_network_peer_id(state_dir, &peer_id, now_ms)
    {
        body.transport_contact = Some(contact);
    }
    SignedDiscoveryNodeRecord::sign(body, &identity)
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
