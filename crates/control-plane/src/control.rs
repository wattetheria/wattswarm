use crate::crypto::{NodeIdentity, candidate_hash, sha256_hex, vote_commit_hash};
use crate::node::{Node, finality_sign};
use crate::runtime::{HttpRuntimeClient, RuntimeCapabilities, RuntimeClient};
use crate::storage::{PgStore, local_control_scope_id, local_control_store};
use crate::task_template::sample_contract;
use crate::types::{
    AgentEnvelope, Candidate, ClaimRole, EventPayload, ExecutionIntentDeclaredPayload,
    ExecutionSetConfirmedPayload, ExecutionSetMember, FinalityProof, Membership,
    NetworkBootstrapBundle, NetworkJoinManifest, Role, TaskContract, VerificationStatus,
    VerifierResult, VoteChoice, VoteCommitPayload, VoteRevealPayload,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use uuid::Uuid;
use wattswarm_artifact_store::{
    ArtifactAvailabilityManifest, ArtifactAvailabilityStatus, ArtifactKind, ArtifactStore,
};
use wattswarm_network_transport_core::{
    DirectDataFetchRequest, DirectDataObjectKind, PeerTransportCapabilities, TransferIntent,
    TransferKind, TransportContactMaterial, TransportRoute as DataTransportRoute, TransportRouter,
};
use wattswarm_network_transport_iroh::fetch_direct_data_for_network_peer_id;

#[path = "control_state.rs"]
mod control_state;
pub use control_state::*;

mod artifacts;
mod task_flow;

pub use artifacts::{
    emit_topic_message_with_content, emit_topic_message_with_content_and_agent_envelope,
    fetch_checkpoint_artifact_json, fetch_evidence_artifact, fetch_json_content_artifact_via_iroh,
    fetch_json_content_artifact_via_iroh_with_local_peer_id,
    fetch_latest_round_checkpoint_artifact, fetch_round_checkpoint_artifact,
    fetch_snapshot_artifact_json, fetch_task_detail_artifact, list_artifacts_needing_repair,
    load_peer_metadata_record_for_transport_peer_id_state, materialize_candidate_output_artifact,
    materialize_checkpoint_artifact_json, materialize_evidence_artifact,
    materialize_json_content_artifact, materialize_round_checkpoint_artifact,
    materialize_snapshot_artifact_json, materialize_task_detail_artifact, recommended_data_routes,
    recommended_transfer_route_for_remote_node, round_checkpoint_artifact_id,
};
use artifacts::{open_local_artifact_store, transport_contact_direct_addrs};
use task_flow::retry_runtime_probe;
pub use task_flow::{bridge_remote_task_into_local_execution, run_real_task_flow};
pub(crate) use task_flow::{prepare_runtime_for_executor, run_existing_task_with_runtime};

pub const PRIVATE_DM_FEED_KEY: &str = "wattswarm.dm";

pub fn private_dm_pair_digest(local_node_id: &str, remote_node_id: &str) -> String {
    let mut members = [
        local_node_id.trim().to_owned(),
        remote_node_id.trim().to_owned(),
    ];
    members.sort();
    sha256_hex(format!("dm-v1\0{}\0{}", members[0], members[1]).as_bytes())
}

pub fn private_dm_group_id(local_node_id: &str, remote_node_id: &str) -> String {
    let digest = private_dm_pair_digest(local_node_id, remote_node_id);
    format!("dm-{}", &digest[..24])
}

pub fn private_dm_scope_hint(local_node_id: &str, remote_node_id: &str) -> String {
    format!(
        "group:{}",
        private_dm_group_id(local_node_id, remote_node_id)
    )
}

pub fn private_dm_thread_id(local_node_id: &str, remote_node_id: &str) -> String {
    let digest = private_dm_pair_digest(local_node_id, remote_node_id);
    format!("dm:{}", &digest[..24])
}

pub fn private_dm_encryption_aad(
    sender_node_id: &str,
    recipient_node_id: &str,
    thread_id: &str,
    message_id: &str,
) -> Vec<u8> {
    format!(
        "wattswarm-private-dm-v1\0{}\0{}\0{}\0{}",
        sender_node_id.trim(),
        recipient_node_id.trim(),
        thread_id.trim(),
        message_id.trim()
    )
    .into_bytes()
}

pub fn is_private_hive_route(feed_key: &str, scope_hint: &str) -> bool {
    feed_key.trim() != PRIVATE_DM_FEED_KEY && scope_hint.trim().starts_with("group:dm-")
}

pub fn private_hive_group_id(feed_key: &str, scope_hint: &str) -> String {
    let scope = scope_hint
        .trim()
        .strip_prefix("group:")
        .unwrap_or(scope_hint.trim());
    format!("{}:{}", feed_key.trim(), scope)
}

pub fn private_hive_encryption_aad(feed_key: &str, scope_hint: &str, message_id: &str) -> Vec<u8> {
    format!(
        "wattswarm-private-hive-gss-v1\0{}\0{}\0{}",
        feed_key.trim(),
        scope_hint.trim(),
        message_id.trim()
    )
    .into_bytes()
}

pub fn private_hive_plaintext_payload(
    content: Value,
    agent_envelope: Option<AgentEnvelope>,
) -> Value {
    json!({
        "kind": "private_hive_plaintext_v1",
        "content": content,
        "agent_envelope": agent_envelope,
    })
}

pub fn decode_private_hive_plaintext_payload(
    value: Value,
) -> Result<(Value, Option<AgentEnvelope>)> {
    let Some(kind) = value.get("kind").and_then(Value::as_str) else {
        return Ok((value, None));
    };
    if kind != "private_hive_plaintext_v1" {
        return Ok((value, None));
    }
    let content = value.get("content").cloned().unwrap_or(Value::Null);
    let agent_envelope = value
        .get("agent_envelope")
        .filter(|value| !value.is_null())
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("decode private hive plaintext agent envelope")?;
    Ok((content, agent_envelope))
}

pub fn executor_registry_path(state_dir: &Path) -> PathBuf {
    state_dir.join("executors.json")
}

pub fn node_state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("node_state.json")
}

pub fn discovered_peers_path(state_dir: &Path) -> PathBuf {
    state_dir.join("discovered_peers.json")
}

pub fn private_message_keypair_path(state_dir: &Path) -> PathBuf {
    state_dir.join("private_message_keypair.json")
}

pub fn private_hive_keys_path(state_dir: &Path) -> PathBuf {
    state_dir.join("private_hive_keys.json")
}

pub fn discovery_bootnode_urls_path(state_dir: &Path) -> PathBuf {
    state_dir.join("discovery_bootnode_urls_v1.json")
}

pub fn load_discovery_bootnode_urls_state(state_dir: &Path) -> Result<Vec<String>> {
    let path = discovery_bootnode_urls_path(state_dir);
    let Ok(bytes) = fs::read(&path) else {
        return Ok(Vec::new());
    };
    let urls = serde_json::from_slice::<Vec<String>>(&bytes)
        .with_context(|| format!("parse discovery bootnode URLs at {}", path.display()))?;
    Ok(normalize_discovery_urls(&urls))
}

pub fn save_discovery_bootnode_urls_state(state_dir: &Path, urls: &[String]) -> Result<bool> {
    let urls = normalize_discovery_urls(urls);
    let existing = load_discovery_bootnode_urls_state(state_dir)?;
    if existing == urls {
        return Ok(false);
    }
    fs::create_dir_all(state_dir)?;
    fs::write(
        discovery_bootnode_urls_path(state_dir),
        serde_json::to_vec_pretty(&urls)?,
    )?;
    Ok(true)
}

pub fn artifact_store_path(state_dir: &Path) -> PathBuf {
    state_dir.join("artifacts")
}

pub fn remote_task_bridge_registry_path(state_dir: &Path) -> PathBuf {
    state_dir.join("remote_task_bridge_registry.json")
}

fn normalize_bootstrap_bundle_endpoint(value: &str) -> String {
    if value.contains("/api/") {
        value.to_owned()
    } else {
        format!("{}/api/network/bootstrap", value.trim_end_matches('/'))
    }
}

fn comma_separated_env_values(key: &str) -> Vec<String> {
    env::var(key)
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

fn bootstrap_bundle_endpoint_candidates(_state_dir: &Path) -> Result<Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut endpoints = Vec::new();
    for value in comma_separated_env_values(ENV_NETWORK_BOOTSTRAP_HTTP_URLS) {
        let endpoint = normalize_bootstrap_bundle_endpoint(&value);
        if seen.insert(endpoint.clone()) {
            endpoints.push(endpoint);
        }
    }
    if !endpoints.is_empty() {
        return Ok(endpoints);
    }

    Ok(endpoints)
}

const DEFAULT_WAN_JOIN_MANIFEST_BASE_URL: &str = "https://bootstrap.wattetheria.com";

fn join_manifest_endpoint_candidates(include_default_wan_endpoint: bool) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut endpoints = Vec::new();
    for value in comma_separated_env_values(ENV_NETWORK_JOIN_MANIFEST_URLS) {
        let endpoint = if value.ends_with(".json") || value.contains("/.well-known/") {
            value
        } else {
            format!(
                "{}{}",
                value.trim_end_matches('/'),
                NETWORK_JOIN_MANIFEST_ROUTE
            )
        };
        if seen.insert(endpoint.clone()) {
            endpoints.push(endpoint);
        }
    }
    if endpoints.is_empty() && include_default_wan_endpoint {
        endpoints.push(format!(
            "{}{}",
            DEFAULT_WAN_JOIN_MANIFEST_BASE_URL, NETWORK_JOIN_MANIFEST_ROUTE
        ));
    }
    endpoints
}

fn fallback_bundle_endpoint_from_manifest_url(endpoint: &str) -> Option<String> {
    let Ok(mut url) = reqwest::Url::parse(endpoint) else {
        return None;
    };
    url.set_path(NETWORK_BOOTSTRAP_ROUTE);
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

fn bootstrap_bundle_endpoints_from_manifest(
    manifest: &NetworkJoinManifest,
    manifest_endpoint: &str,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut endpoints = Vec::new();
    for value in &manifest.bootstrap_urls {
        let endpoint = normalize_bootstrap_bundle_endpoint(value.trim());
        if seen.insert(endpoint.clone()) {
            endpoints.push(endpoint);
        }
    }
    if endpoints.is_empty()
        && let Some(endpoint) = fallback_bundle_endpoint_from_manifest_url(manifest_endpoint)
        && seen.insert(endpoint.clone())
    {
        endpoints.push(endpoint);
    }
    endpoints
}

fn fetch_network_bootstrap_bundle(endpoint: &str) -> Result<NetworkBootstrapBundle> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .context("build bootstrap bundle HTTP client")?;
    let response = client
        .get(endpoint)
        .send()
        .with_context(|| format!("request bootstrap bundle from {endpoint}"))?
        .error_for_status()
        .with_context(|| format!("bootstrap bundle endpoint returned error: {endpoint}"))?;
    let payload: NetworkBootstrapBundleResponse = response
        .json()
        .with_context(|| format!("decode bootstrap bundle response from {endpoint}"))?;
    if !payload.ok {
        anyhow::bail!("bootstrap bundle endpoint reported ok=false: {endpoint}");
    }
    Ok(payload.bundle)
}

fn fetch_network_join_manifest(endpoint: &str) -> Result<NetworkJoinManifest> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .context("build join manifest HTTP client")?;
    client
        .get(endpoint)
        .send()
        .with_context(|| format!("request join manifest from {endpoint}"))?
        .error_for_status()
        .with_context(|| format!("join manifest endpoint returned error: {endpoint}"))?
        .json()
        .with_context(|| format!("decode join manifest from {endpoint}"))
}

fn validate_bundle_against_join_manifest(
    manifest: &NetworkJoinManifest,
    bundle: &NetworkBootstrapBundle,
) -> Result<()> {
    let network = &bundle.topology.network;
    if network.network_id != manifest.network_id {
        bail!(
            "join manifest network_id mismatch expected={} got={}",
            manifest.network_id,
            network.network_id
        );
    }
    if network.genesis_node_id != manifest.genesis_node_id {
        bail!(
            "join manifest genesis_node_id mismatch expected={} got={}",
            manifest.genesis_node_id,
            network.genesis_node_id
        );
    }
    if bundle.signed_params.params_hash != manifest.params_hash {
        bail!(
            "join manifest params_hash mismatch expected={} got={}",
            manifest.params_hash,
            bundle.signed_params.params_hash
        );
    }
    Ok(())
}

fn replace_manifest_values(
    value: &mut Value,
    field: &str,
    manifest_values: &[String],
    trim_trailing_slash: bool,
) -> bool {
    let mut values = Vec::new();
    let mut seen = BTreeSet::new();
    for manifest_value in manifest_values {
        let trimmed = manifest_value.trim();
        let manifest_value = if trim_trailing_slash {
            trimmed.trim_end_matches('/')
        } else {
            trimmed
        };
        if !manifest_value.is_empty() && seen.insert(manifest_value.to_owned()) {
            values.push(manifest_value.to_owned());
        }
    }
    let replacement = Value::Array(values.into_iter().map(Value::String).collect());
    if value.get(field) == Some(&replacement) {
        return false;
    }
    value[field] = replacement;
    true
}

fn normalize_discovery_urls(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let value = value.trim().trim_end_matches('/').to_owned();
        if !value.is_empty() && seen.insert(value.clone()) {
            normalized.push(value);
        }
    }
    normalized
}

fn replace_manifest_bootstrap_contacts(value: &mut Value, manifest_values: &[String]) -> bool {
    let mut values = Vec::<(String, String)>::new();
    let mut indexes = BTreeMap::<String, usize>::new();
    for raw in manifest_values.iter().cloned() {
        let Some((key, normalized)) = normalize_manifest_bootstrap_contact(&raw) else {
            continue;
        };
        if let Some(index) = indexes.get(&key) {
            values[*index].1 = normalized;
        } else {
            indexes.insert(key.clone(), values.len());
            values.push((key, normalized));
        }
    }

    let replacement = Value::Array(
        values
            .into_iter()
            .map(|(_, raw)| Value::String(raw))
            .collect(),
    );
    if value.get("bootstrap_contacts") == Some(&replacement) {
        return false;
    }
    value["bootstrap_contacts"] = replacement;
    true
}

fn normalize_manifest_bootstrap_contact(raw: &str) -> Option<(String, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !trimmed.starts_with('{') {
        let key = trimmed
            .rsplit_once('@')
            .map(|(endpoint_id, _)| endpoint_id.trim())
            .filter(|endpoint_id| !endpoint_id.is_empty())
            .unwrap_or(trimmed);
        return Some((format!("raw:{key}"), trimmed.to_owned()));
    }

    let mut contact = serde_json::from_str::<Value>(trimmed).ok()?;
    sanitize_manifest_bootstrap_contact(&mut contact);
    if !manifest_contact_has_iroh_identity_or_transport_address(&contact) {
        return None;
    }
    let key = manifest_bootstrap_contact_key(&contact).unwrap_or_else(|| trimmed.to_owned());
    let normalized = serde_json::to_string(&contact).ok()?;
    Some((format!("json:{key}"), normalized))
}

fn sanitize_manifest_bootstrap_contact(contact: &mut Value) {
    sanitize_socket_addr_array(contact, "listen_addrs");
    if let Some(transports) = contact.get_mut("transports").and_then(Value::as_array_mut) {
        for transport in transports {
            sanitize_transport_contact(transport);
        }
    } else {
        sanitize_transport_contact(contact);
    }
}

fn sanitize_transport_contact(contact: &mut Value) {
    if let Some(metadata) = contact.get_mut("metadata") {
        sanitize_socket_addr_array(metadata, "listen_addrs");
    }
    if let Some(extra) = contact.get_mut("extra") {
        sanitize_socket_addr_array(extra, "direct_addrs");
        sanitize_trimmed_string_array(extra, "relay_urls");
    }
}

fn sanitize_socket_addr_array(value: &mut Value, field: &str) {
    if let Some(items) = value.get_mut(field).and_then(Value::as_array_mut) {
        let mut values = Vec::new();
        let mut seen = BTreeSet::new();
        for item in items.iter() {
            let Some(raw) = item.as_str().map(str::trim) else {
                continue;
            };
            if raw.parse::<SocketAddr>().is_ok() && seen.insert(raw.to_owned()) {
                values.push(Value::String(raw.to_owned()));
            }
        }
        *items = values;
    }
}

fn sanitize_trimmed_string_array(value: &mut Value, field: &str) {
    if let Some(items) = value.get_mut(field).and_then(Value::as_array_mut) {
        let mut values = Vec::new();
        let mut seen = BTreeSet::new();
        for item in items.iter() {
            let Some(raw) = item.as_str().map(str::trim).filter(|raw| !raw.is_empty()) else {
                continue;
            };
            let normalized = raw.trim_end_matches('/').to_owned();
            if seen.insert(normalized.clone()) {
                values.push(Value::String(normalized));
            }
        }
        *items = values;
    }
}

fn manifest_bootstrap_contact_key(contact: &Value) -> Option<String> {
    contact
        .get("transports")
        .and_then(Value::as_array)
        .and_then(|transports| transports.iter().find_map(transport_contact_key))
        .or_else(|| transport_contact_key(contact))
        .or_else(|| string_field(contact, "node_id"))
}

fn transport_contact_key(contact: &Value) -> Option<String> {
    contact
        .get("extra")
        .and_then(|extra| string_field(extra, "endpoint_id"))
        .or_else(|| {
            contact
                .get("metadata")
                .and_then(|metadata| string_field(metadata, "endpoint_id"))
        })
        .or_else(|| string_field(contact, "peer_id"))
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn manifest_contact_has_iroh_identity_or_transport_address(contact: &Value) -> bool {
    contact
        .get("transports")
        .and_then(Value::as_array)
        .is_some_and(|transports| {
            transports
                .iter()
                .any(transport_has_iroh_identity_or_transport_address)
        })
        || transport_has_iroh_identity_or_transport_address(contact)
}

fn transport_has_iroh_identity_or_transport_address(contact: &Value) -> bool {
    contact.get("extra").is_some_and(|extra| {
        string_field(extra, "endpoint_id").is_some()
            || value_array_has_strings(extra, "direct_addrs")
            || value_array_has_strings(extra, "relay_urls")
    }) || contact.get("metadata").is_some_and(|metadata| {
        string_field(metadata, "endpoint_id").is_some()
            || value_array_has_strings(metadata, "listen_addrs")
    })
}

fn value_array_has_strings(value: &Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.as_str().is_some_and(|raw| !raw.trim().is_empty()))
        })
}

fn persist_join_manifest_startup_config(
    state_dir: &Path,
    manifest: &NetworkJoinManifest,
) -> Result<()> {
    if !manifest.discovery_urls.is_empty() {
        let _ = save_discovery_bootnode_urls_state(state_dir, &manifest.discovery_urls)?;
    }
    if manifest.bootstrap_contacts.is_empty()
        && manifest.gateway_urls.is_empty()
        && manifest.relay_urls.is_empty()
    {
        return Ok(());
    }
    fs::create_dir_all(state_dir)?;
    let path = state_dir.join("startup_config.json");
    let mut value = if path.exists() {
        serde_json::from_slice::<Value>(&fs::read(&path)?)
            .with_context(|| format!("parse startup config at {}", path.display()))?
    } else {
        json!({})
    };
    if !value.is_object() {
        value = json!({});
    }
    let mut changed = false;
    if !value
        .get("network_mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| !mode.trim().is_empty())
    {
        value["network_mode"] = Value::String("wan".to_owned());
        changed = true;
    }
    changed |= replace_manifest_bootstrap_contacts(&mut value, &manifest.bootstrap_contacts);
    changed |= replace_manifest_values(&mut value, "gateway_urls", &manifest.gateway_urls, true);
    if !manifest.relay_urls.is_empty() {
        changed |= replace_manifest_values(&mut value, "relay_urls", &manifest.relay_urls, true);
    }
    if changed {
        fs::write(&path, serde_json::to_vec_pretty(&value)?)?;
    }
    Ok(())
}

/// Refresh the locally persisted relay URLs from the network join manifest.
///
/// Runs once per process startup so an already-joined node picks up the
/// network's current relay set on restart. The fetched list overwrites the
/// `relay_urls` field in `startup_config.json`; fetch failures leave the
/// local value untouched and must not block node startup.
pub fn refresh_startup_config_relay_urls_from_join_manifest(state_dir: &Path) -> Result<bool> {
    let mode = configured_node_mode(state_dir)?;
    if !matches!(mode, Some(NodeMode::Network) | Some(NodeMode::Lan)) {
        return Ok(false);
    }
    let endpoints = join_manifest_endpoint_candidates(matches!(mode, Some(NodeMode::Network)));
    if endpoints.is_empty() {
        return Ok(false);
    }
    let mut last_err = None;
    for endpoint in endpoints {
        match retry_runtime_probe(|| fetch_network_join_manifest(&endpoint)) {
            Ok(manifest) => {
                if manifest.relay_urls.is_empty() {
                    // The manifest endpoint reports no relay set (for example an
                    // older genesis version); keep the local value.
                    return Ok(false);
                }
                return persist_startup_config_relay_urls(state_dir, &manifest.relay_urls);
            }
            Err(err) => {
                last_err = Some(err.context(format!("fetch join manifest from {endpoint}")));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("no reachable join manifest endpoint")))
}

fn persist_startup_config_relay_urls(state_dir: &Path, relay_urls: &[String]) -> Result<bool> {
    let mut seen = BTreeSet::new();
    let sanitized: Vec<String> = relay_urls
        .iter()
        .map(|url| url.trim().trim_end_matches('/').to_owned())
        .filter(|url| !url.is_empty())
        .filter(|url| seen.insert(url.clone()))
        .collect();
    if sanitized.is_empty() {
        return Ok(false);
    }
    fs::create_dir_all(state_dir)?;
    let path = state_dir.join("startup_config.json");
    let mut value = if path.exists() {
        serde_json::from_slice::<Value>(&fs::read(&path)?)
            .with_context(|| format!("parse startup config at {}", path.display()))?
    } else {
        json!({})
    };
    if !value.is_object() {
        value = json!({});
    }
    let next = json!(sanitized);
    if value.get("relay_urls") == Some(&next) {
        return Ok(false);
    }
    value["relay_urls"] = next;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_path = state_dir.join(format!(
        ".startup_config.json.{}.{nonce}.tmp",
        std::process::id()
    ));
    fs::write(&tmp_path, serde_json::to_vec_pretty(&value)?)?;
    fs::rename(&tmp_path, &path)?;
    Ok(true)
}

fn try_sync_network_bootstrap_bundle_from_join_manifest(
    state_dir: &Path,
    store: &PgStore,
    include_default_wan_endpoint: bool,
) -> Result<bool> {
    let endpoints = join_manifest_endpoint_candidates(include_default_wan_endpoint);
    if endpoints.is_empty() {
        return Ok(false);
    }
    let mut last_err = None;
    for manifest_endpoint in endpoints {
        match retry_runtime_probe(|| fetch_network_join_manifest(&manifest_endpoint)) {
            Ok(manifest) => {
                let bundle_endpoints =
                    bootstrap_bundle_endpoints_from_manifest(&manifest, &manifest_endpoint);
                for bundle_endpoint in bundle_endpoints {
                    match retry_runtime_probe(|| fetch_network_bootstrap_bundle(&bundle_endpoint)) {
                        Ok(bundle) => {
                            validate_bundle_against_join_manifest(&manifest, &bundle)?;
                            store.import_network_bootstrap_bundle(&bundle)?;
                            persist_join_manifest_startup_config(state_dir, &manifest)?;
                            return Ok(true);
                        }
                        Err(err) => {
                            last_err =
                                Some(err.context(format!(
                                    "fetch bootstrap bundle via {bundle_endpoint}"
                                )));
                        }
                    }
                }
            }
            Err(err) => {
                last_err =
                    Some(err.context(format!("fetch join manifest via {manifest_endpoint}")));
            }
        }
    }
    if let Some(err) = last_err {
        return Err(err);
    }
    Ok(false)
}

fn maybe_sync_network_bootstrap_bundle(
    state_dir: &Path,
    store: &PgStore,
    self_node_id: &str,
    now: u64,
) -> Result<bool> {
    let local_topology =
        store.resolve_network_bootstrap_topology_descriptor(self_node_id, self_node_id, now);
    let local_bundle_ready = match &local_topology {
        Ok(topology) => store
            .for_org(&topology.org.org_id)
            .load_verified_network_protocol_params()
            .is_ok(),
        Err(_) => false,
    };
    if local_bundle_ready {
        return Ok(false);
    }

    let endpoints = bootstrap_bundle_endpoint_candidates(state_dir)?;
    let include_default_wan_endpoint = local_topology.is_err() && endpoints.is_empty();
    if try_sync_network_bootstrap_bundle_from_join_manifest(
        state_dir,
        store,
        include_default_wan_endpoint,
    )
    .context("sync network bootstrap bundle from join manifest")?
    {
        return Ok(true);
    }

    if endpoints.is_empty() {
        return Ok(false);
    }

    let mut last_err = None;
    for endpoint in endpoints {
        match retry_runtime_probe(|| fetch_network_bootstrap_bundle(&endpoint)) {
            Ok(bundle) => {
                store.import_network_bootstrap_bundle(&bundle)?;
                return Ok(true);
            }
            Err(err) => {
                last_err = Some(err.context(format!("fetch bootstrap bundle via {endpoint}")))
            }
        }
    }

    if let Some(err) = last_err {
        return Err(err);
    }
    Ok(false)
}

fn observed_at_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

pub fn accept_task_result_locally(
    node: &mut Node,
    task_id: &str,
    candidate_id: &str,
) -> Result<Value> {
    accept_task_result_locally_with_agent_envelope(node, task_id, candidate_id, None)
}

pub fn accept_task_result_locally_with_agent_envelope(
    node: &mut Node,
    task_id: &str,
    candidate_id: &str,
    agent_envelope: Option<crate::types::AgentEnvelope>,
) -> Result<Value> {
    let task = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task not found: {task_id}"))?;
    if task.terminal_state == crate::types::TaskTerminalState::Finalized {
        if task.finalized_candidate_id.as_deref() == Some(candidate_id) {
            return Ok(json!({
                "ok": true,
                "status": "finalized",
                "task_id": task_id,
                "candidate_id": candidate_id,
                "already_finalized": true,
            }));
        }
        bail!(
            "task {task_id} already finalized with different candidate: {:?}",
            task.finalized_candidate_id
        );
    }
    let candidate = node
        .store
        .get_candidate_by_id(task_id, candidate_id)?
        .ok_or_else(|| anyhow!("candidate not found for task {task_id}: {candidate_id}"))?;
    let candidate_hash = candidate_hash(&candidate)?;
    let now = observed_at_ms();
    let verify_execution_id = format!("verify-{}", Uuid::new_v4());
    let lease_until = now.saturating_add(task.contract.assignment.claim.lease_ms);
    node.claim_task_with_agent_envelope(
        task_id,
        ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
        agent_envelope.clone(),
        task.epoch,
        now.saturating_add(1),
    )?;
    let verifier_result_hash = format!(
        "accept-result:{}:{}:{}",
        task_id, candidate_id, verify_execution_id
    );
    node.submit_verifier_result(
        task_id,
        VerifierResult {
            candidate_id: candidate_id.to_owned(),
            execution_id: verify_execution_id.clone(),
            verification_status: VerificationStatus::Passed,
            passed: true,
            score: 1.0,
            reason_codes: vec![wattswarm_protocol::reason_codes::REASON_SCHEMA_OK],
            verifier_result_hash: verifier_result_hash.clone(),
            provider_family: "agent_event_bus".to_owned(),
            model_id: "local-agent-approval".to_owned(),
            policy_id: task.contract.acceptance.verifier_policy.policy_id.clone(),
            policy_version: task
                .contract
                .acceptance
                .verifier_policy
                .policy_version
                .clone(),
            policy_hash: task.contract.acceptance.verifier_policy.policy_hash.clone(),
        },
        task.epoch,
        now.saturating_add(2),
    )?;
    let salt = Uuid::new_v4().to_string();
    let commit_hash = vote_commit_hash(VoteChoice::Approve, &salt, &verifier_result_hash);
    node.submit_vote_commit(
        VoteCommitPayload {
            task_id: task_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
            candidate_hash: candidate_hash.clone(),
            execution_id: verify_execution_id.clone(),
            verifier_result_hash: verifier_result_hash.clone(),
            commit_hash,
        },
        task.epoch,
        now.saturating_add(3),
    )?;
    node.submit_vote_reveal(
        VoteRevealPayload {
            task_id: task_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
            candidate_hash,
            execution_id: verify_execution_id,
            verifier_result_hash,
            vote: VoteChoice::Approve,
            salt,
        },
        task.epoch,
        now.saturating_add(4),
    )?;
    node.commit_decision(task_id, task.epoch, candidate_id, now.saturating_add(5))?;
    node.finalize_decision_with_agent_envelope(
        task_id,
        task.epoch,
        candidate_id,
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(
                &node.identity,
                task_id,
                task.epoch,
                candidate_id,
            )],
        },
        agent_envelope,
        now.saturating_add(6),
    )?;
    Ok(json!({
        "ok": true,
        "status": "finalized",
        "task_id": task_id,
        "candidate_id": candidate_id,
    }))
}

fn current_network_context_id(node: &Node) -> String {
    if node.store.is_org_configured()
        && let Ok(topology) = node
            .store
            .load_network_topology_for_org(node.store.org_id())
    {
        return topology.network.network_id;
    }
    node.store
        .load_verified_network_protocol_params()
        .map(|verified| verified.network_id)
        .unwrap_or_else(|_| "default".to_owned())
}

fn current_network_topology(node: &Node) -> Result<crate::types::NetworkTopology> {
    node.store
        .load_network_topology_for_org(node.store.org_id())
}

pub fn load_network_directory_snapshot(
    node: &Node,
    state_dir: &Path,
    limit: usize,
) -> Result<NetworkDirectorySnapshot> {
    let current_topology = current_network_topology(node)?;
    let current_network_id = current_topology.network.network_id.clone();

    let mut networks = node
        .store
        .list_discoverable_network_topologies(limit)?
        .into_iter()
        .map(|topology| DirectoryNetworkInstance {
            is_current: topology.network.network_id == current_network_id,
            topology,
        })
        .collect::<Vec<_>>();
    networks.sort_by(|left, right| {
        right.is_current.cmp(&left.is_current).then_with(|| {
            left.topology
                .network
                .network_id
                .cmp(&right.topology.network.network_id)
        })
    });

    let active_dissemination_domains = node.store.list_active_dissemination_domains(limit)?;
    let feeds = node
        .store
        .list_discoverable_feed_sources(limit)?
        .into_iter()
        .map(|row| DirectoryFeedSource {
            feed_key: row.feed_key,
            scope_hint: row.scope_hint,
            subscriber_count: row.subscriber_count,
            latest_announcement_id: row.latest_announcement_id,
            latest_task_id: row.latest_task_id,
            latest_source_node_id: row.latest_source_node_id,
            latest_announced_at: row.latest_announced_at,
        })
        .collect::<Vec<_>>();

    let peer_metadata = load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .map(|record| (record.node_id.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let mut sync_endpoints = BTreeMap::<(String, String), DirectorySyncEndpoint>::new();
    for (node_id, record) in &peer_metadata {
        let transports = record.advertised_transports();
        let recommended_routes = record.recommended_data_routes();
        for endpoint in record
            .transport_contact_materials()
            .iter()
            .flat_map(transport_contact_direct_addrs)
        {
            sync_endpoints.insert(
                (node_id.clone(), endpoint.clone()),
                DirectorySyncEndpoint {
                    network_id: current_network_id.clone(),
                    node_id: node_id.clone(),
                    listen_addr: endpoint,
                    source_kind: record.handshake_status.clone(),
                    transports: transports.clone(),
                    recommended_routes: recommended_routes.clone(),
                },
            );
        }
    }

    Ok(NetworkDirectorySnapshot {
        current_topology,
        networks,
        active_dissemination_domains,
        feeds,
        sync_endpoints: sync_endpoints.into_values().take(limit).collect(),
    })
}

fn load_or_create_identity(seed_file: &Path) -> Result<NodeIdentity> {
    if seed_file.exists() {
        let hex_seed = fs::read_to_string(seed_file)?;
        let bytes = hex::decode(hex_seed.trim())?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("seed must be 32 bytes"))?;
        return Ok(NodeIdentity::from_seed(arr));
    }

    let random_seed: [u8; 32] = rand::random();
    fs::write(seed_file, hex::encode(random_seed))?;
    Ok(NodeIdentity::from_seed(random_seed))
}

#[cfg(test)]
mod tests {
    use super::{
        ENV_NETWORK_BOOTSTRAP_HTTP_URLS, ENV_NETWORK_JOIN_MANIFEST_URLS,
        NETWORK_JOIN_MANIFEST_ROUTE, accept_task_result_locally,
        bootstrap_bundle_endpoint_candidates, join_manifest_endpoint_candidates,
        peer_dm_content_from_envelope, replace_manifest_bootstrap_contacts,
        replace_manifest_values, synthesize_peer_dm_envelope,
    };
    use crate::node::Node;
    use crate::task_template::sample_contract;
    use crate::types::{Candidate, Role};
    use serde_json::json;
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use uuid::Uuid;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct EnvVarGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: test mutates process env and restores it on drop.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }

        fn remove(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: test mutates process env and restores it on drop.
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: test restores process env to its prior state.
            unsafe {
                if let Some(value) = &self.prev {
                    std::env::set_var(self.key, value);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    fn temp_test_dir(prefix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "wattswarm-control-{prefix}-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn bootstrap_bundle_candidates_do_not_treat_startup_contacts_as_http_sources() {
        let _guard = env_lock();
        let state_dir = temp_test_dir("bootstrap-candidates");
        let startup_config_path = state_dir.join("startup_config.json");
        fs::write(
            &startup_config_path,
            serde_json::to_vec(&serde_json::json!({
                "network_mode": "wan",
                "bootstrap_contacts": [
                    "{\"transport\":\"iroh_direct\",\"peer_id\":\"node-a\",\"metadata\":{\"route\":\"iroh_direct\",\"generated_at\":1,\"endpoint_id\":\"node-a\",\"alpn\":\"/wattswarm/iroh/1\",\"listen_addrs\":[\"13.55.201.222:4001\"],\"capabilities\":{\"supports_iroh_direct\":true,\"supports_streaming\":true,\"max_recommended_inline_bytes\":16384,\"preferred_data_route\":\"iroh_direct\"}},\"extra\":{\"endpoint_id\":\"node-a\",\"alpn\":\"/wattswarm/iroh/1\",\"direct_addrs\":[\"13.55.201.222:4001\"],\"relay_urls\":[]}}"
                ],
                "core_agent": {
                    "mode": "local_url",
                    "base_url": "http://127.0.0.1:8787",
                    "provider": "openai-compatible",
                    "model": "",
                    "api_key": ""
                }
            }))
            .expect("serialize startup config"),
        )
        .expect("write startup config");

        let _http_guard = EnvVarGuard::remove(ENV_NETWORK_BOOTSTRAP_HTTP_URLS);
        let _peers_guard = EnvVarGuard::remove("WATTSWARM_P2P_BOOTSTRAP_PEERS");

        let endpoints =
            bootstrap_bundle_endpoint_candidates(&state_dir).expect("load bootstrap candidates");
        assert!(endpoints.is_empty());
        let _ = fs::remove_dir_all(state_dir);
    }

    #[test]
    fn join_manifest_candidates_default_to_wattetheria_for_fresh_wan() {
        let _guard = env_lock();
        let _manifest_guard = EnvVarGuard::remove(ENV_NETWORK_JOIN_MANIFEST_URLS);
        assert!(join_manifest_endpoint_candidates(false).is_empty());
        assert_eq!(
            join_manifest_endpoint_candidates(true),
            vec![format!(
                "https://bootstrap.wattetheria.com{NETWORK_JOIN_MANIFEST_ROUTE}"
            )]
        );
    }

    #[test]
    fn join_manifest_candidates_use_explicit_env_without_default() {
        let _guard = env_lock();
        let _manifest_guard = EnvVarGuard::set(
            ENV_NETWORK_JOIN_MANIFEST_URLS,
            "https://custom.example.net,https://seed.example.net/.well-known/wattswarm/join.json",
        );
        assert_eq!(
            join_manifest_endpoint_candidates(true),
            vec![
                format!("https://custom.example.net{NETWORK_JOIN_MANIFEST_ROUTE}"),
                "https://seed.example.net/.well-known/wattswarm/join.json".to_owned(),
            ]
        );
    }

    #[test]
    fn manifest_bootstrap_contacts_replace_existing_and_sanitize_invalid_direct_addrs() {
        let stale_endpoint_id = "11113ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956";
        let endpoint_id = "83393ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956";
        let contact = |endpoint_id: &str, generated_at: u64| {
            json!({
                "generated_at": generated_at,
                "listen_addrs": [endpoint_id],
                "node_id": endpoint_id,
                "peer_id": endpoint_id,
                "recommended_routes": ["iroh_direct"],
                "transports": [{
                    "transport": "iroh_direct",
                    "peer_id": endpoint_id,
                    "metadata": {
                        "route": "iroh_direct",
                        "generated_at": generated_at,
                        "endpoint_id": endpoint_id,
                        "alpn": "/wattswarm/iroh/1",
                        "listen_addrs": [endpoint_id],
                        "capabilities": {
                            "supports_iroh_direct": true,
                            "supports_streaming": true,
                            "max_recommended_inline_bytes": 16384,
                            "preferred_data_route": "iroh_direct"
                        }
                    },
                    "extra": {
                        "endpoint_id": endpoint_id,
                        "alpn": "/wattswarm/iroh/1",
                        "direct_addrs": [endpoint_id],
                        "relay_urls": [
                            "https://relay.wattetheria.com/",
                            "https://relay.wattetheria.com"
                        ]
                    }
                }]
            })
            .to_string()
        };
        let mut value = json!({
            "network_mode": "wan",
            "bootstrap_contacts": [contact(stale_endpoint_id, 1)]
        });

        assert!(replace_manifest_bootstrap_contacts(
            &mut value,
            &[contact(endpoint_id, 2), contact(endpoint_id, 3)],
        ));

        let contacts = value["bootstrap_contacts"].as_array().expect("contacts");
        assert_eq!(contacts.len(), 1);
        let saved: serde_json::Value =
            serde_json::from_str(contacts[0].as_str().expect("contact string")).unwrap();
        assert_eq!(saved["node_id"].as_str(), Some(endpoint_id));
        assert_eq!(saved["generated_at"].as_u64(), Some(3));
        assert!(
            saved["listen_addrs"]
                .as_array()
                .expect("top-level listen_addrs")
                .is_empty()
        );
        let transport = &saved["transports"][0];
        assert!(
            transport["metadata"]["listen_addrs"]
                .as_array()
                .expect("metadata listen_addrs")
                .is_empty()
        );
        assert!(
            transport["extra"]["direct_addrs"]
                .as_array()
                .expect("direct_addrs")
                .is_empty()
        );
        assert_eq!(
            transport["extra"]["relay_urls"],
            json!(["https://relay.wattetheria.com"])
        );
        assert!(!replace_manifest_bootstrap_contacts(
            &mut value,
            &[contact(endpoint_id, 3)]
        ));
    }

    #[test]
    fn manifest_bootstrap_contacts_keep_endpoint_id_only_iroh_contact() {
        let endpoint_id = "83393ad000151bc41e686a1fc892e07a440a2a53110bbaeae3d13e5978599956";
        let contact = json!({
            "generated_at": 1,
            "listen_addrs": [],
            "node_id": endpoint_id,
            "peer_id": endpoint_id,
            "recommended_routes": ["iroh_direct"],
            "transports": [{
                "transport": "iroh_direct",
                "peer_id": endpoint_id,
                "metadata": {
                    "route": "iroh_direct",
                    "generated_at": 1,
                    "endpoint_id": endpoint_id,
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
                    "endpoint_id": endpoint_id,
                    "alpn": "/wattswarm/iroh/1",
                    "direct_addrs": [],
                    "relay_urls": []
                }
            }]
        })
        .to_string();
        let mut value = json!({
            "network_mode": "wan",
            "bootstrap_contacts": []
        });

        assert!(replace_manifest_bootstrap_contacts(&mut value, &[contact],));

        let contacts = value["bootstrap_contacts"].as_array().expect("contacts");
        assert_eq!(contacts.len(), 1);
        let saved: serde_json::Value =
            serde_json::from_str(contacts[0].as_str().expect("contact string")).unwrap();
        assert_eq!(
            saved["transports"][0]["extra"]["endpoint_id"].as_str(),
            Some(endpoint_id)
        );
        assert!(
            saved["transports"][0]["extra"]["direct_addrs"]
                .as_array()
                .expect("direct addrs")
                .is_empty()
        );
        assert!(
            saved["transports"][0]["extra"]["relay_urls"]
                .as_array()
                .expect("relay urls")
                .is_empty()
        );
    }

    #[test]
    fn manifest_gateway_urls_replace_existing_and_normalize_latest_values() {
        let mut value = json!({
            "network_mode": "wan",
            "gateway_urls": ["https://old-gateway.wattetheria.com"]
        });

        assert!(replace_manifest_values(
            &mut value,
            "gateway_urls",
            &[
                "https://gateway.wattetheria.com/".to_owned(),
                "https://gateway.wattetheria.com".to_owned(),
            ],
            true,
        ));

        assert_eq!(
            value["gateway_urls"],
            json!(["https://gateway.wattetheria.com"])
        );
        assert!(!replace_manifest_values(
            &mut value,
            "gateway_urls",
            &["https://gateway.wattetheria.com/".to_owned()],
            true,
        ));
    }

    #[test]
    fn synthesized_dm_envelope_round_trips_content_projection() {
        let content = serde_json::json!({"text":"hello"});
        let envelope = synthesize_peer_dm_envelope("google_a2a", &content);
        assert_eq!(envelope.protocol, "google_a2a");
        assert_eq!(peer_dm_content_from_envelope(&envelope), content);
    }

    #[test]
    fn accept_task_result_locally_is_idempotent_for_same_finalized_candidate() {
        let mut node = Node::open_in_memory_with_roles(&[
            Role::Proposer,
            Role::Verifier,
            Role::Committer,
            Role::Finalizer,
        ])
        .expect("node");
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("policy binding")
            .policy_hash;
        let contract = sample_contract("task-finalized-idempotent", policy_hash);
        node.submit_task(contract, 1, 10).expect("submit task");

        let candidate = Candidate {
            candidate_id: "cand-finalized".to_owned(),
            execution_id: "exec-finalized".to_owned(),
            output_ref: crate::types::ArtifactRef {
                uri: "artifact://reference/cand-finalized".to_owned(),
                digest: "sha256:cand-finalized".to_owned(),
                size_bytes: 64,
                mime: "application/json".to_owned(),
                created_at: 12,
                producer: node.node_id(),
            },
            output: json!({
                "answer": "ok",
                "confidence": 1.0,
                "check_summary": "accepted"
            }),
            evidence_inline: vec![],
            evidence_refs: vec![],
        };
        node.store
            .put_candidate("task-finalized-idempotent", &node.node_id(), &candidate)
            .expect("put candidate");

        let first =
            accept_task_result_locally(&mut node, "task-finalized-idempotent", "cand-finalized")
                .expect("first finalize");
        assert_eq!(first["status"].as_str(), Some("finalized"));

        let second =
            accept_task_result_locally(&mut node, "task-finalized-idempotent", "cand-finalized")
                .expect("second finalize");
        assert_eq!(second["status"].as_str(), Some("finalized"));
        assert_eq!(second["already_finalized"].as_bool(), Some(true));
    }
}
