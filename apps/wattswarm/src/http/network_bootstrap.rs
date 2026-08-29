use crate::control::{local_peer_id, open_configured_node};
use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::{Result, bail};
use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use wattswarm_network_p2p::NetworkNodeId;
use wattswarm_network_transport_iroh::IrohRouteProbeResult;

const ENV_PUBLIC_BOOTSTRAP_URLS: &str = "WATTSWARM_PUBLIC_BOOTSTRAP_URLS";
const ENV_PUBLIC_BOOTSTRAP_CONTACTS: &str = "WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS";
const ENV_PUBLIC_GATEWAY_URLS: &str = "WATTSWARM_PUBLIC_GATEWAY_URLS";
const ENV_PUBLIC_DISCOVERY_URLS: &str = "WATTSWARM_PUBLIC_DISCOVERY_URLS";
const ENV_IROH_RELAY_URLS: &str = "WATTSWARM_IROH_RELAY_URLS";
const ENV_IROH_PUBLISH_DIRECT_ADDRS: &str = "WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS";
const DIAGNOSTIC_LOG_RELATIVE_PATH: &str = "diagnostics/wattswarm_node.jsonl";
const IROH_ROUTE_SELECTED_PHASE: &str = "iroh.route.selected";

#[derive(Debug, Deserialize)]
pub(crate) struct IrohProbeRequest {
    endpoint_id: String,
    #[serde(default = "default_iroh_probe_hold_ms")]
    hold_ms: u64,
    #[serde(default = "default_iroh_probe_timeout_ms")]
    timeout_ms: u64,
}

fn default_iroh_probe_hold_ms() -> u64 {
    10_000
}

fn default_iroh_probe_timeout_ms() -> u64 {
    30_000
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

fn record_iroh_probe_route_diagnostic(
    state_dir: &Path,
    local_peer_id: &str,
    result: &IrohRouteProbeResult,
) -> Result<()> {
    let path = state_dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let timestamp_ms = unix_timestamp_millis();
    let entry = json!({
        "id": format!("iroh-route-{timestamp_ms}-{}", result.peer_id),
        "timestamp_ms": timestamp_ms,
        "level": "info",
        "component": "wattswarm.network_transport_iroh",
        "category": "transport",
        "phase": IROH_ROUTE_SELECTED_PHASE,
        "status": "ok",
        "message": format!("iroh selected route: {}", result.selected_route),
        "object_kind": "peer",
        "object_id": result.peer_id,
        "source_node_id": local_peer_id,
        "details": {
            "peer_id": result.peer_id,
            "endpoint_id": result.endpoint_id,
            "selected_route": result.selected_route,
            "path_count": result.path_count,
            "stage": result.stage,
            "runtime": result.runtime,
            "direct_capable": result.direct_capable,
            "fallback_reason": result.fallback_reason,
        },
    });
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(serde_json::to_string(&entry)?.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

pub(crate) async fn network_local(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let (peer_id, listen_addrs, public_bootstrap) =
        run_blocking(move || -> Result<(String, Vec<String>, bool)> {
            let peer_id = local_peer_id(&state_clone.state_dir)?;
            let listen_addrs =
                crate::network_bridge::network_config_from_state_dir(&state_clone.state_dir)
                    .listen_addrs;
            let public_bootstrap =
                is_public_bootstrap_node(&state_clone.state_dir, &state_clone.db_path, &peer_id);
            Ok((peer_id, listen_addrs, public_bootstrap))
        })
        .await?;
    Ok(Json(json!({
        "ok": true,
        "network_enabled": crate::network_bridge::network_enabled_from_env(),
        "local_peer_id": peer_id,
        "listen_addrs": listen_addrs,
        "public_bootstrap": public_bootstrap,
    })))
}

fn is_public_bootstrap_node(state_dir: &Path, db_path: &Path, peer_id: &str) -> bool {
    if public_bootstrap_contact_node_ids().contains(peer_id) {
        return true;
    }
    let Ok(node) = open_configured_node(state_dir, db_path) else {
        return false;
    };
    let Ok(bundle) = node.store.load_network_bootstrap_bundle() else {
        return false;
    };
    bundle.topology.network.genesis_node_id == peer_id
}

fn public_bootstrap_contact_node_ids() -> HashSet<String> {
    let Some(raw) = std::env::var(ENV_PUBLIC_BOOTSTRAP_CONTACTS).ok() else {
        return HashSet::new();
    };
    parse_public_bootstrap_contact_node_ids(&raw)
}

fn parse_public_bootstrap_contact_node_ids(raw: &str) -> HashSet<String> {
    let mut node_ids = HashSet::new();
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            collect_bootstrap_contact_node_ids(&value, &mut node_ids);
        } else {
            for value in line
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                insert_bootstrap_contact_node_id(value, &mut node_ids);
            }
        }
    }
    node_ids
}

fn collect_bootstrap_contact_node_ids(value: &Value, node_ids: &mut HashSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_bootstrap_contact_node_ids(value, node_ids);
            }
        }
        Value::Object(object) => {
            for key in ["node_id", "peer_id"] {
                if let Some(value) = object.get(key).and_then(Value::as_str) {
                    insert_bootstrap_contact_node_id(value, node_ids);
                }
            }
        }
        Value::String(value) => {
            if let Ok(nested) = serde_json::from_str::<Value>(value) {
                collect_bootstrap_contact_node_ids(&nested, node_ids);
            } else {
                insert_bootstrap_contact_node_id(value, node_ids);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn insert_bootstrap_contact_node_id(value: &str, node_ids: &mut HashSet<String>) {
    let candidate = value.split('@').next().map(str::trim).unwrap_or_default();
    if let Ok(node_id) = NetworkNodeId::new(candidate) {
        node_ids.insert(node_id.to_string());
    }
}

pub(crate) async fn network_iroh_probe(
    State(state): State<UiServerState>,
    Json(req): Json<IrohProbeRequest>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let result = run_blocking(move || {
        let endpoint_id = req.endpoint_id.trim();
        if endpoint_id.is_empty() {
            bail!("iroh probe endpoint id is required");
        }
        let peer_id = local_peer_id(&state_clone.state_dir)?;
        let result =
            wattswarm_network_transport_iroh::probe_iroh_route_endpoint_id_for_network_peer_id(
                &state_clone.state_dir,
                &peer_id,
                endpoint_id,
                Duration::from_millis(req.hold_ms),
                Duration::from_millis(req.timeout_ms),
            )?;
        record_iroh_probe_route_diagnostic(&state_clone.state_dir, &peer_id, &result)?;
        Ok(result)
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "result": result,
    })))
}

pub(crate) async fn network_local_contact_material(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let contact_material = run_blocking(move || {
        crate::network_bridge::export_local_contact_material(&state_clone.state_dir)
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "contact_material": contact_material,
    })))
}

pub(crate) async fn network_bootstrap(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
    let state_clone = state.clone();
    let bundle = run_blocking(move || -> Result<crate::types::NetworkBootstrapBundle> {
        let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
        node.store.load_network_bootstrap_bundle()
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "bundle": bundle,
    })))
}

pub(crate) async fn network_join_manifest(
    State(state): State<UiServerState>,
) -> Result<Json<crate::types::NetworkJoinManifest>, ApiError> {
    let state_clone = state.clone();
    let manifest = run_blocking(move || -> Result<crate::types::NetworkJoinManifest> {
        let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
        let bundle = node.store.load_network_bootstrap_bundle()?;
        let relay_urls = split_public_manifest_values(ENV_IROH_RELAY_URLS);
        let mut bootstrap_contacts =
            public_manifest_bootstrap_contacts(ENV_PUBLIC_BOOTSTRAP_CONTACTS, &relay_urls)?;
        if bootstrap_contacts.is_empty() {
            if public_iroh_direct_addrs_disabled() && !relay_urls.is_empty() {
                bootstrap_contacts.push(relay_only_bootstrap_contact_json(
                    &state_clone.state_dir,
                    &relay_urls,
                )?);
            } else if let Ok(contact) =
                crate::network_bridge::export_local_bootstrap_contact_json(&state_clone.state_dir)
            {
                bootstrap_contacts.push(contact);
            }
        }
        Ok(crate::types::NetworkJoinManifest {
            network_id: bundle.topology.network.network_id,
            genesis_node_id: bundle.topology.network.genesis_node_id,
            params_hash: bundle.signed_params.params_hash,
            bootstrap_urls: split_public_manifest_values(ENV_PUBLIC_BOOTSTRAP_URLS),
            bootstrap_contacts,
            gateway_urls: split_public_manifest_values(ENV_PUBLIC_GATEWAY_URLS),
            discovery_urls: split_public_manifest_values(ENV_PUBLIC_DISCOVERY_URLS),
            relay_urls,
        })
    })
    .await?;
    Ok(Json(manifest))
}

fn split_public_manifest_values(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn public_manifest_bootstrap_contacts(key: &str, relay_urls: &[String]) -> Result<Vec<String>> {
    let Some(raw) = std::env::var(key).ok() else {
        return Ok(Vec::new());
    };
    raw.lines()
        .flat_map(|line| {
            let value = line.trim();
            if value.starts_with('{') {
                vec![value.to_owned()]
            } else {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            }
        })
        .map(|value| public_manifest_bootstrap_contact(value, relay_urls))
        .collect()
}

fn public_manifest_bootstrap_contact(value: String, relay_urls: &[String]) -> Result<String> {
    if value.starts_with('{') || value.contains('@') {
        return Ok(value);
    }
    if relay_urls.is_empty() {
        bail!("bare bootstrap node id requires WATTSWARM_IROH_RELAY_URLS");
    }
    let peer_id = NetworkNodeId::new(value)?.to_string();
    relay_only_bootstrap_contact_json_for_peer_id(peer_id, relay_urls)
}

fn public_iroh_direct_addrs_disabled() -> bool {
    std::env::var(ENV_IROH_PUBLISH_DIRECT_ADDRS)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(false)
}

fn relay_only_bootstrap_contact_json(state_dir: &Path, relay_urls: &[String]) -> Result<String> {
    let peer_id = local_peer_id(state_dir)?;
    relay_only_bootstrap_contact_json_for_peer_id(peer_id, relay_urls)
}

fn relay_only_bootstrap_contact_json_for_peer_id(
    peer_id: String,
    relay_urls: &[String],
) -> Result<String> {
    let generated_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
    Ok(json!({
        "node_id": peer_id.clone(),
        "peer_id": peer_id.clone(),
        "listen_addrs": [],
        "generated_at": generated_at,
        "transports": [{
            "transport": "iroh_direct",
            "peer_id": peer_id.clone(),
            "metadata": {
                "route": "iroh_direct",
                "generated_at": generated_at,
                "endpoint_id": peer_id.clone(),
                "alpn": "/wattswarm/iroh/1",
                "listen_addrs": [],
                "capabilities": {
                    "supports_iroh_direct": true,
                    "supports_streaming": true,
                    "max_recommended_inline_bytes": 16384,
                    "preferred_data_route": "iroh_direct"
                }
            },
            "extra": {
                "endpoint_id": peer_id,
                "alpn": "/wattswarm/iroh/1",
                "direct_addrs": [],
                "relay_urls": relay_urls
            }
        }],
        "recommended_routes": {
            "artifact_blob": "iroh_direct",
            "backfill_chunk": "iroh_direct",
            "checkpoint_snapshot": "iroh_direct",
            "direct_message": "iroh_direct",
            "evidence_blob": "iroh_direct",
            "task_sync": "iroh_direct",
            "topic_sync": "iroh_direct"
        }
    })
    .to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOTSTRAP_NODE_ID: &str =
        "83393ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956";
    const SECOND_BOOTSTRAP_NODE_ID: &str =
        "9b196ed13c0ec849dd7b8bf5add0b07ad2c88c1bf5d79e8591f6681ab1803258";

    #[test]
    fn record_iroh_probe_route_diagnostic_writes_selected_route() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = IrohRouteProbeResult {
            peer_id: "peer-1".to_owned(),
            endpoint_id: "endpoint-1".to_owned(),
            selected_route: "relay".to_owned(),
            path_count: 1,
            stage: "probe".to_owned(),
            runtime: "in_process".to_owned(),
            direct_capable: false,
            fallback_reason: Some("iroh selected relay route".to_owned()),
        };

        record_iroh_probe_route_diagnostic(dir.path(), "local-peer", &result)
            .expect("record diagnostic");

        let raw = fs::read_to_string(dir.path().join(DIAGNOSTIC_LOG_RELATIVE_PATH))
            .expect("read diagnostics log");
        let saved: Value = serde_json::from_str(raw.trim()).expect("diagnostic json");
        assert_eq!(saved["phase"], IROH_ROUTE_SELECTED_PHASE);
        assert_eq!(saved["source_node_id"], "local-peer");
        assert_eq!(saved["details"]["selected_route"], "relay");
        assert_eq!(saved["details"]["stage"], "probe");
        assert_eq!(saved["details"]["direct_capable"], false);
    }

    #[test]
    fn public_bootstrap_contact_node_ids_accept_ids_and_contact_json() {
        let raw = format!(
            "{BOOTSTRAP_NODE_ID}, {SECOND_BOOTSTRAP_NODE_ID}@127.0.0.1:4010\n{}",
            json!({
                "node_id": BOOTSTRAP_NODE_ID,
                "peer_id": BOOTSTRAP_NODE_ID,
                "transports": []
            })
        );

        let node_ids = parse_public_bootstrap_contact_node_ids(&raw);

        assert!(node_ids.contains(BOOTSTRAP_NODE_ID));
        assert!(node_ids.contains(SECOND_BOOTSTRAP_NODE_ID));
    }
}
