use crate::control::{local_peer_id, open_configured_node};
use crate::http::{ApiError, UiServerState, run_blocking};
use anyhow::{Result, bail};
use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};
use std::path::Path;
use wattswarm_network_p2p::NetworkNodeId;

const ENV_PUBLIC_BOOTSTRAP_URLS: &str = "WATTSWARM_PUBLIC_BOOTSTRAP_URLS";
const ENV_PUBLIC_BOOTSTRAP_CONTACTS: &str = "WATTSWARM_PUBLIC_BOOTSTRAP_CONTACTS";
const ENV_PUBLIC_GATEWAY_URLS: &str = "WATTSWARM_PUBLIC_GATEWAY_URLS";
const ENV_PUBLIC_DISCOVERY_URLS: &str = "WATTSWARM_PUBLIC_DISCOVERY_URLS";
const ENV_IROH_RELAY_URLS: &str = "WATTSWARM_IROH_RELAY_URLS";
const ENV_IROH_PUBLISH_DIRECT_ADDRS: &str = "WATTSWARM_IROH_PUBLISH_DIRECT_ADDRS";

pub(crate) async fn network_local(
    State(state): State<UiServerState>,
) -> Result<Json<Value>, ApiError> {
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
        "network_enabled": crate::network_bridge::network_enabled_from_env(),
        "local_peer_id": peer_id,
        "listen_addrs": listen_addrs
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
