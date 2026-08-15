use crate::control::{load_local_identity, local_peer_id, open_configured_node};
use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::{Context, Result, bail};
use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use wattswarm_network_client_server::{AutoRegistrationRequest, AutoRegistrationResponse};
use wattswarm_network_p2p::NetworkNodeId;
use wattswarm_network_transport_iroh::IrohRouteProbeResult;
use wattswarm_protocol::types::{NETWORK_MEMBERSHIP_GRANT_VERSION, UnsignedNetworkMembershipGrant};

const ENV_PUBLIC_BOOTSTRAP_URLS: &str = "WATTSWARM_PUBLIC_BOOTSTRAP_URLS";
const ENV_PUBLIC_BOOTSTRAP_CONTACTS: &str = "WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS";
const ENV_PUBLIC_GATEWAY_URLS: &str = "WATTSWARM_PUBLIC_GATEWAY_URLS";
const ENV_NETWORK_GRANT_TTL_SECONDS: &str = "WATTSWARM_NETWORK_GRANT_TTL_SECONDS";
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
    let network_enabled = crate::network_bridge::network_enabled_from_state_dir(&state.state_dir);
    let state_clone = state.clone();
    let (peer_id, listen_addrs) = run_blocking(move || -> Result<(String, Vec<String>)> {
        let peer_id = local_peer_id(&state_clone.state_dir)?;
        let listen_addrs =
            crate::network_bridge::network_config_from_state_dir(&state_clone.state_dir)
                .listen_addrs;
        Ok((peer_id, listen_addrs))
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "network_enabled": network_enabled,
        "local_peer_id": peer_id,
        "listen_addrs": listen_addrs
    })))
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

pub(crate) async fn network_auto_registration(
    State(state): State<UiServerState>,
    Json(request): Json<AutoRegistrationRequest>,
) -> Result<Json<AutoRegistrationResponse>, ApiError> {
    let state_clone = state.clone();
    let response =
        run_blocking(move || issue_network_membership_grant(&state_clone, &request)).await?;
    Ok(Json(response))
}

fn issue_network_membership_grant(
    state: &UiServerState,
    request: &AutoRegistrationRequest,
) -> Result<AutoRegistrationResponse> {
    validate_registration_request(request)?;
    if !crate::control::network_service::client_server_auto_registration_enabled(&state.state_dir)?
    {
        bail!("automatic network registration is disabled by the Genesis policy");
    }
    let node = open_configured_node(&state.state_dir, &state.db_path)?;
    let identity = load_local_identity(&state.state_dir)?;
    let topology = node
        .store
        .load_network_topology_for_org(node.store.org_id())?;
    if topology.network.network_id != request.network_id {
        bail!(
            "registration network mismatch expected={} got={}",
            topology.network.network_id,
            request.network_id
        );
    }
    if topology.network.genesis_node_id != identity.node_id() {
        bail!("only the Genesis node can issue network membership grants");
    }
    let signing_message = request.signing_message()?;
    wattswarm_crypto::verify_signature(
        &request.public_key_hex,
        &signing_message,
        &request.signature_hex,
    )?;
    let issued_at = unix_timestamp_millis();
    let grant = wattswarm_crypto::sign_network_membership_grant(
        &UnsignedNetworkMembershipGrant {
            version: NETWORK_MEMBERSHIP_GRANT_VERSION,
            network_id: request.network_id.clone(),
            principal_id: request.principal_id.clone(),
            public_key_hex: request.public_key_hex.clone(),
            issuer_genesis_id: identity.node_id(),
            issued_at,
            expires_at: network_grant_expiry(issued_at)?,
        },
        &identity,
    )?;
    node.store.join_node_to_network_topology(
        &request.network_id,
        &request.principal_id,
        &request.public_key_hex,
        issued_at,
    )?;
    node.store.put_network_membership_grant(&grant, issued_at)?;
    Ok(AutoRegistrationResponse {
        network_id: request.network_id.clone(),
        principal_id: request.principal_id.clone(),
        grant,
        status: "active".to_owned(),
    })
}

fn validate_registration_request(request: &AutoRegistrationRequest) -> Result<()> {
    if request.network_id.trim().is_empty() || request.network_id.len() > 256 {
        bail!("registration network id is invalid");
    }
    if request.nonce.trim().is_empty() || request.nonce.len() > 256 {
        bail!("registration nonce is invalid");
    }
    if request.principal_id != request.public_key_hex
        || !matches!(hex::decode(&request.public_key_hex), Ok(bytes) if bytes.len() == 32)
    {
        bail!("registration principal identity is invalid");
    }
    if request
        .tenant_instance_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
    {
        bail!("registration tenant instance is invalid");
    }
    Ok(())
}

fn network_grant_expiry(issued_at: u64) -> Result<Option<u64>> {
    let Some(raw) = std::env::var_os(ENV_NETWORK_GRANT_TTL_SECONDS) else {
        return Ok(None);
    };
    let ttl_seconds: u64 = raw
        .to_string_lossy()
        .trim()
        .parse()
        .context("parse WATTSWARM_NETWORK_GRANT_TTL_SECONDS")?;
    if ttl_seconds == 0 {
        return Ok(None);
    }
    let ttl_ms = ttl_seconds
        .checked_mul(1_000)
        .context("network grant TTL is too large")?;
    issued_at
        .checked_add(ttl_ms)
        .map(Some)
        .context("network grant expiry overflow")
}

pub(crate) async fn network_join_manifest(
    State(state): State<UiServerState>,
) -> Result<Json<crate::types::NetworkJoinManifest>, ApiError> {
    let state_clone = state.clone();
    let manifest = run_blocking(move || -> Result<crate::types::NetworkJoinManifest> {
        let node = open_configured_node(&state_clone.state_dir, &state_clone.db_path)?;
        let bundle = node.store.load_network_bootstrap_bundle()?;
        let transport_config =
            crate::control::network_service::network_transport_config_from_env_or_startup_config(
                &state_clone.state_dir,
            )?;
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
            network_backend: transport_config.network_backend,
            client_server_url: transport_config.client_server_url,
            cs_auto_register: Some(transport_config.cs_auto_register.unwrap_or(true)),
            registration_urls: transport_config
                .network_registration_url
                .into_iter()
                .collect(),
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
}
