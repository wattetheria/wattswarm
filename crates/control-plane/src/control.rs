use crate::crypto::{NodeIdentity, candidate_hash, sha256_hex, vote_commit_hash};
use crate::node::{Node, finality_sign};
use crate::runtime::{HttpRuntimeClient, RuntimeCapabilities, RuntimeClient};
use crate::storage::{PgStore, local_control_scope_id, local_control_store};
use crate::task_template::sample_contract;
use crate::types::{
    Candidate, ClaimRole, EventPayload, ExecutionIntentDeclaredPayload,
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

pub fn executor_registry_path(state_dir: &Path) -> PathBuf {
    state_dir.join("executors.json")
}

pub fn node_state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("node_state.json")
}

pub fn discovered_peers_path(state_dir: &Path) -> PathBuf {
    state_dir.join("discovered_peers.json")
}

pub fn artifact_store_path(state_dir: &Path) -> PathBuf {
    state_dir.join("artifacts")
}

pub fn remote_task_bridge_registry_path(state_dir: &Path) -> PathBuf {
    state_dir.join("remote_task_bridge_registry.json")
}

fn open_local_artifact_store(state_dir: &Path) -> Result<ArtifactStore> {
    let store = ArtifactStore::new(artifact_store_path(state_dir));
    store.ensure_layout()?;
    Ok(store)
}

fn content_artifact_uri(kind: ArtifactKind, digest: &str) -> String {
    let kind_str = match kind {
        ArtifactKind::TopicMessage => "topic-message",
        ArtifactKind::DirectMessage => "direct-message",
        ArtifactKind::Reference => "reference",
        ArtifactKind::Evidence => "evidence",
        ArtifactKind::Checkpoint => "checkpoint",
        ArtifactKind::Snapshot => "snapshot",
        ArtifactKind::EventBatch => "event-batch",
        ArtifactKind::Availability => "availability",
    };
    format!("artifact://{kind_str}/{digest}")
}

pub fn materialize_json_content_artifact(
    state_dir: &Path,
    kind: ArtifactKind,
    producer: &str,
    content: &Value,
    created_at: u64,
) -> Result<crate::types::ArtifactRef> {
    let bytes = serde_json::to_vec(content)?;
    let digest = format!("sha256:{}", sha256_hex(&bytes));
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.write_validated_bytes(
        kind,
        &digest,
        None,
        &bytes,
        Some(&digest),
        Some(bytes.len() as u64),
    )?;
    let manifest = availability_manifest(
        kind,
        &digest,
        None,
        Some(&content_artifact_uri(kind, &digest)),
        Some(&digest),
        Some("application/json"),
        Some(bytes.len() as u64),
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        created_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(crate::types::ArtifactRef {
        uri: content_artifact_uri(kind, &digest),
        digest,
        size_bytes: bytes.len() as u64,
        mime: "application/json".to_owned(),
        created_at,
        producer: producer.to_owned(),
    })
}

pub fn fetch_json_content_artifact_via_iroh(
    state_dir: &Path,
    remote_node_id: &str,
    kind: ArtifactKind,
    reference: &crate::types::ArtifactRef,
) -> Result<Value> {
    let local_peer_id: crate::network_p2p::NetworkNodeId = local_peer_id(state_dir)?
        .parse()
        .map_err(|err| anyhow!("parse local peer id: {err}"))?;
    fetch_json_content_artifact_via_iroh_with_local_peer_id(
        state_dir,
        &local_peer_id,
        remote_node_id,
        kind,
        reference,
    )
}

pub fn fetch_json_content_artifact_via_iroh_with_local_peer_id(
    state_dir: &Path,
    local_peer_id: &crate::network_p2p::NetworkNodeId,
    remote_node_id: &str,
    kind: ArtifactKind,
    reference: &crate::types::ArtifactRef,
) -> Result<Value> {
    let object_kind = match kind {
        ArtifactKind::TopicMessage => DirectDataObjectKind::TopicMessageJson,
        ArtifactKind::DirectMessage => DirectDataObjectKind::DirectMessageJson,
        ArtifactKind::Reference => DirectDataObjectKind::ReferenceArtifact,
        ArtifactKind::Evidence => DirectDataObjectKind::EvidenceArtifact,
        ArtifactKind::Checkpoint => DirectDataObjectKind::CheckpointJson,
        ArtifactKind::Snapshot => DirectDataObjectKind::SnapshotJson,
        ArtifactKind::EventBatch | ArtifactKind::Availability => {
            bail!("unsupported JSON content artifact kind {:?}", kind)
        }
    };
    let metadata = load_peer_metadata_record_for_remote_node_state(state_dir, remote_node_id)?
        .ok_or_else(|| anyhow!("missing peer metadata for {remote_node_id}"))?;
    let contact = metadata
        .transport_contact_material(DataTransportRoute::IrohDirect)
        .ok_or_else(|| anyhow!("missing iroh_direct contact material for {remote_node_id}"))?;
    let bytes = fetch_direct_data_for_network_peer_id(
        state_dir,
        local_peer_id.as_str(),
        &contact,
        &DirectDataFetchRequest {
            object_kind,
            object_id: reference.digest.clone(),
            scope: None,
            source_uri: Some(reference.uri.clone()),
            expected_digest: Some(reference.digest.clone()),
            expected_size: Some(reference.size_bytes),
        },
    )?
    .bytes;
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.write_validated_bytes(
        kind,
        &reference.digest,
        None,
        &bytes,
        Some(&reference.digest),
        Some(reference.size_bytes),
    )?;
    let manifest = availability_manifest(
        kind,
        &reference.digest,
        None,
        Some(&reference.uri),
        Some(&reference.digest),
        Some(&reference.mime),
        Some(reference.size_bytes),
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at_ms(),
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn emit_topic_message_with_content(
    node: &mut Node,
    state_dir: &Path,
    network_id: &str,
    feed_key: &str,
    scope_hint: &str,
    content: Value,
    reply_to_message_id: Option<String>,
    created_at: u64,
) -> Result<crate::types::Event> {
    let content_ref = materialize_json_content_artifact(
        state_dir,
        ArtifactKind::TopicMessage,
        &node.node_id(),
        &content,
        created_at,
    )?;
    let event = node.emit_at(
        1,
        crate::types::EventPayload::TopicMessagePosted(crate::types::TopicMessagePostedPayload {
            network_id: network_id.to_owned(),
            feed_key: feed_key.to_owned(),
            scope_hint: scope_hint.to_owned(),
            content_ref: content_ref.clone(),
            local_content_cache: Some(content.clone()),
            reply_to_message_id,
        }),
        created_at,
    )?;
    node.store
        .update_topic_message_content(&event.event_id, &content, created_at)?;
    Ok(event)
}

pub fn materialize_candidate_output_artifact(
    state_dir: &Path,
    producer: &str,
    output: &Value,
    created_at: u64,
) -> Result<crate::types::ArtifactRef> {
    materialize_json_content_artifact(
        state_dir,
        ArtifactKind::Reference,
        producer,
        output,
        created_at,
    )
}

fn load_remote_task_bridge_registry_file(path: &Path) -> Result<RemoteTaskBridgeRegistry> {
    if !path.exists() {
        return Ok(RemoteTaskBridgeRegistry::default());
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn load_remote_task_bridge_registry(state_dir: &Path) -> Result<RemoteTaskBridgeRegistry> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    let entries = store.list_local_remote_task_bridges(&scope_id)?;
    if !entries.is_empty() {
        return Ok(RemoteTaskBridgeRegistry {
            entries: entries
                .into_iter()
                .map(|entry| RemoteTaskBridgeRecord {
                    task_id: entry.task_id,
                    announcement_id: entry.announcement_id,
                    network_id: entry.network_id,
                    source_node_id: entry.source_node_id,
                    source_scope_hint: entry.source_scope_hint,
                    detail_ref_digest: entry.detail_ref_digest,
                    executor: entry.executor,
                    profile: entry.profile,
                    candidate_id: entry.candidate_id,
                    terminal_state: entry.terminal_state,
                    bridged_at: entry.bridged_at,
                })
                .collect(),
        });
    }
    let path = remote_task_bridge_registry_path(state_dir);
    let legacy = load_remote_task_bridge_registry_file(&path)?;
    if !legacy.entries.is_empty() {
        for entry in &legacy.entries {
            store.upsert_local_remote_task_bridge(
                &scope_id,
                &crate::storage::LocalRemoteTaskBridgeRow {
                    task_id: entry.task_id.clone(),
                    announcement_id: entry.announcement_id.clone(),
                    network_id: entry.network_id.clone(),
                    source_node_id: entry.source_node_id.clone(),
                    source_scope_hint: entry.source_scope_hint.clone(),
                    detail_ref_digest: entry.detail_ref_digest.clone(),
                    executor: entry.executor.clone(),
                    profile: entry.profile.clone(),
                    candidate_id: entry.candidate_id.clone(),
                    terminal_state: entry.terminal_state.clone(),
                    bridged_at: entry.bridged_at,
                },
            )?;
        }
    }
    Ok(legacy)
}

fn save_remote_task_bridge_registry(
    state_dir: &Path,
    reg: &RemoteTaskBridgeRegistry,
) -> Result<()> {
    let store = local_control_store(state_dir)?;
    let scope_id = local_control_scope_id(state_dir);
    store.replace_local_remote_task_bridges(
        &scope_id,
        &reg.entries
            .iter()
            .map(|entry| crate::storage::LocalRemoteTaskBridgeRow {
                task_id: entry.task_id.clone(),
                announcement_id: entry.announcement_id.clone(),
                network_id: entry.network_id.clone(),
                source_node_id: entry.source_node_id.clone(),
                source_scope_hint: entry.source_scope_hint.clone(),
                detail_ref_digest: entry.detail_ref_digest.clone(),
                executor: entry.executor.clone(),
                profile: entry.profile.clone(),
                candidate_id: entry.candidate_id.clone(),
                terminal_state: entry.terminal_state.clone(),
                bridged_at: entry.bridged_at,
            })
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn prepare_runtime_for_executor(
    state_dir: &Path,
    executor: &str,
    profile: &str,
) -> Result<PreparedRuntime> {
    let reg = load_executor_registry_state(state_dir)?;
    let executor = normalize_executor_name(executor);
    let entry = reg
        .entries
        .iter()
        .find(|e| e.name == executor)
        .ok_or_else(|| anyhow!("executor not found: {executor}"))?;

    let runtime = HttpRuntimeClient::new(entry.base_url.clone());
    retry_runtime_probe(|| runtime.health()).with_context(|| {
        format!(
            "runtime /health failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    let capabilities = retry_runtime_probe(|| runtime.capabilities()).with_context(|| {
        format!(
            "runtime /capabilities failed (executor='{}', base_url='{}')",
            executor, entry.base_url
        )
    })?;
    if !capabilities
        .profiles
        .iter()
        .any(|candidate| candidate == profile)
    {
        return Err(anyhow!(
            "profile '{}' not supported by executor '{}'",
            profile,
            executor
        ));
    }
    Ok(PreparedRuntime {
        runtime,
        capabilities,
    })
}

fn retry_runtime_probe<T, F>(mut op: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    const MAX_ATTEMPTS: usize = 5;
    const BASE_BACKOFF_MS: u64 = 50;

    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_err = Some(err);
                if attempt + 1 < MAX_ATTEMPTS {
                    thread::sleep(Duration::from_millis(
                        BASE_BACKOFF_MS * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("runtime probe failed without an error")))
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
    if !manifest_contact_has_transport_address(&contact) {
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

fn manifest_contact_has_transport_address(contact: &Value) -> bool {
    contact
        .get("transports")
        .and_then(Value::as_array)
        .is_some_and(|transports| transports.iter().any(transport_has_transport_address))
        || transport_has_transport_address(contact)
}

fn transport_has_transport_address(contact: &Value) -> bool {
    contact.get("extra").is_some_and(|extra| {
        value_array_has_strings(extra, "direct_addrs")
            || value_array_has_strings(extra, "relay_urls")
    }) || contact
        .get("metadata")
        .is_some_and(|metadata| value_array_has_strings(metadata, "listen_addrs"))
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
    if manifest.bootstrap_contacts.is_empty() && manifest.gateway_urls.is_empty() {
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
    if changed {
        fs::write(&path, serde_json::to_vec_pretty(&value)?)?;
    }
    Ok(())
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
    )? {
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

fn default_artifact_retry_after_ms() -> u64 {
    30_000
}

fn transport_intents() -> Vec<TransferIntent> {
    vec![
        TransferIntent {
            kind: TransferKind::DirectMessage,
            payload_bytes: 4 * 1024,
            requires_streaming: false,
        },
        TransferIntent {
            kind: TransferKind::TopicSync,
            payload_bytes: 8 * 1024,
            requires_streaming: false,
        },
        TransferIntent {
            kind: TransferKind::TaskSync,
            payload_bytes: 8 * 1024,
            requires_streaming: false,
        },
        TransferIntent {
            kind: TransferKind::BackfillChunk,
            payload_bytes: 64 * 1024,
            requires_streaming: true,
        },
        TransferIntent {
            kind: TransferKind::ArtifactBlob,
            payload_bytes: 256 * 1024,
            requires_streaming: true,
        },
        TransferIntent {
            kind: TransferKind::EvidenceBlob,
            payload_bytes: 256 * 1024,
            requires_streaming: true,
        },
        TransferIntent {
            kind: TransferKind::CheckpointSnapshot,
            payload_bytes: 256 * 1024,
            requires_streaming: true,
        },
    ]
}

pub fn recommended_data_routes(
    capabilities: Option<&PeerTransportCapabilities>,
) -> BTreeMap<String, String> {
    transport_intents()
        .into_iter()
        .map(|intent| {
            (
                intent.kind.as_str().to_owned(),
                TransportRouter::select(&intent, capabilities)
                    .as_str()
                    .to_owned(),
            )
        })
        .collect()
}

pub fn recommended_transfer_route_for_remote_node(
    state_dir: &Path,
    remote_node_id: &str,
    intent: &TransferIntent,
) -> Result<DataTransportRoute> {
    let capabilities = load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.node_id == remote_node_id)
        .and_then(|record| record.transport_capabilities());
    Ok(TransportRouter::select(intent, capabilities.as_ref()))
}

pub fn load_peer_metadata_record_for_transport_peer_id_state(
    state_dir: &Path,
    peer_id: &str,
) -> Result<Option<PeerMetadataRecord>> {
    Ok(load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.contact_material_transport_peer_id().as_deref() == Some(peer_id)))
}

fn load_peer_metadata_record_for_remote_node_state(
    state_dir: &Path,
    remote_node_id: &str,
) -> Result<Option<PeerMetadataRecord>> {
    Ok(load_peer_metadata_records_state(state_dir)?
        .into_iter()
        .find(|record| record.node_id == remote_node_id))
}

fn append_transport_decision_entry(state_dir: &Path, entry: &Value) {
    let path = state_dir.join("transport_decisions.jsonl");
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = std::io::Write::write_all(&mut file, entry.to_string().as_bytes());
        let _ = std::io::Write::write_all(&mut file, b"\n");
    }
}

fn task_detail_binding_key(task_id: &str) -> String {
    task_id.to_owned()
}

fn evidence_binding_key(task_id: &str, candidate_id: &str, evidence_digest: &str) -> String {
    format!("{task_id}:{candidate_id}:{evidence_digest}")
}

fn checkpoint_binding_key(checkpoint_id: &str) -> String {
    checkpoint_id.to_owned()
}

fn snapshot_binding_key(snapshot_id: &str) -> String {
    snapshot_id.to_owned()
}

fn record_transport_decision_for_binding(
    state_dir: &Path,
    binding: Option<&DataSourceBindingRecord>,
    intent: &TransferIntent,
    route: DataTransportRoute,
    subject: Value,
) {
    append_transport_decision_entry(
        state_dir,
        &json!({
            "kind": intent.kind.as_str(),
            "route": route.as_str(),
            "source_node_id": binding.map(|row| row.source_node_id.clone()),
            "binding_kind": binding.map(|row| row.binding_kind.as_str().to_owned()),
            "binding_scope": binding.and_then(|row| row.binding_scope.clone()),
            "binding_key": binding.map(|row| row.binding_key.clone()),
            "source_uri": binding.and_then(|row| row.source_uri.clone()),
            "decided_at": observed_at_ms(),
            "subject": subject,
        }),
    );
}

fn record_missing_manifest_with_binding(
    artifact_store: &ArtifactStore,
    kind: ArtifactKind,
    artifact_id: &str,
    scope: Option<&str>,
    source_uri: Option<&str>,
    expected_digest: Option<&str>,
    mime: Option<&str>,
    size_bytes: Option<u64>,
    observed_at: u64,
    binding: Option<&DataSourceBindingRecord>,
    route: DataTransportRoute,
    error: anyhow::Error,
) -> Result<()> {
    let detail = match binding {
        Some(binding) => format!(
            "{}; source_node_id={}; route={}",
            error,
            binding.source_node_id,
            route.as_str()
        ),
        None => format!("{error}; route={}", route.as_str()),
    };
    write_missing_manifest(
        artifact_store,
        kind,
        artifact_id,
        scope,
        source_uri,
        expected_digest,
        mime,
        size_bytes,
        observed_at,
        anyhow!(detail),
    )
}

fn resolve_route_for_binding(
    state_dir: &Path,
    binding: Option<&DataSourceBindingRecord>,
    intent: &TransferIntent,
) -> Result<DataTransportRoute> {
    let Some(binding) = binding else {
        return Ok(DataTransportRoute::IrohControl);
    };
    recommended_transfer_route_for_remote_node(state_dir, &binding.source_node_id, intent)
}

fn fetch_via_iroh_route(
    state_dir: &Path,
    remote_node_id: &str,
    request: &DirectDataFetchRequest,
) -> Result<Vec<u8>> {
    let metadata = load_peer_metadata_record_for_remote_node_state(state_dir, remote_node_id)?
        .ok_or_else(|| anyhow!("missing peer metadata for {remote_node_id}"))?;
    let contact = metadata
        .transport_contact_material(DataTransportRoute::IrohDirect)
        .ok_or_else(|| anyhow!("missing iroh_direct contact material for {remote_node_id}"))?;
    let local_peer_id: crate::network_p2p::NetworkNodeId = local_peer_id(state_dir)?
        .parse()
        .map_err(|err| anyhow!("parse local peer id: {err}"))?;
    let response = fetch_direct_data_for_network_peer_id(
        state_dir,
        local_peer_id.as_str(),
        &contact,
        request,
    )?;
    Ok(response.bytes)
}

fn maybe_fetch_reference_artifact_via_transport(
    state_dir: &Path,
    kind: ArtifactKind,
    artifact_id: &str,
    source_uri: &str,
    expected_digest: &str,
    mime: &str,
    size_bytes: u64,
    observed_at: u64,
    binding: Option<&DataSourceBindingRecord>,
    route: DataTransportRoute,
) -> Result<Option<Vec<u8>>> {
    if route != DataTransportRoute::IrohDirect {
        return Ok(None);
    }
    let Some(binding) = binding else {
        return Ok(None);
    };
    let object_kind = match kind {
        ArtifactKind::Reference => DirectDataObjectKind::ReferenceArtifact,
        ArtifactKind::Evidence => DirectDataObjectKind::EvidenceArtifact,
        ArtifactKind::TopicMessage => DirectDataObjectKind::TopicMessageJson,
        ArtifactKind::DirectMessage => DirectDataObjectKind::DirectMessageJson,
        ArtifactKind::Checkpoint => DirectDataObjectKind::CheckpointJson,
        ArtifactKind::Snapshot => DirectDataObjectKind::SnapshotJson,
        _ => bail!("unsupported reference artifact kind {:?}", kind),
    };
    let bytes = fetch_via_iroh_route(
        state_dir,
        &binding.source_node_id,
        &DirectDataFetchRequest {
            object_kind,
            object_id: artifact_id.to_owned(),
            scope: None,
            source_uri: Some(source_uri.to_owned()),
            expected_digest: Some(expected_digest.to_owned()),
            expected_size: Some(size_bytes),
        },
    )?;
    let _ = materialize_reference_artifact(
        state_dir,
        kind,
        artifact_id,
        source_uri,
        expected_digest,
        mime,
        size_bytes,
        &bytes,
        observed_at,
    )?;
    Ok(Some(bytes))
}

fn maybe_fetch_json_artifact_via_transport(
    state_dir: &Path,
    kind: ArtifactKind,
    scope_key: &str,
    artifact_id: &str,
    source_uri: Option<&str>,
    observed_at: u64,
    binding: Option<&DataSourceBindingRecord>,
    route: DataTransportRoute,
) -> Result<Option<Vec<u8>>> {
    if route != DataTransportRoute::IrohDirect {
        return Ok(None);
    }
    let Some(binding) = binding else {
        return Ok(None);
    };
    let object_kind = match kind {
        ArtifactKind::Checkpoint => DirectDataObjectKind::CheckpointJson,
        ArtifactKind::Snapshot => DirectDataObjectKind::SnapshotJson,
        _ => bail!("unsupported json artifact kind {:?}", kind),
    };
    let bytes = fetch_via_iroh_route(
        state_dir,
        &binding.source_node_id,
        &DirectDataFetchRequest {
            object_kind,
            object_id: artifact_id.to_owned(),
            scope: Some(scope_key.to_owned()),
            source_uri: source_uri.map(str::to_owned),
            expected_digest: None,
            expected_size: None,
        },
    )?;
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = match kind {
        ArtifactKind::Checkpoint => artifact_store.checkpoint_path(artifact_id)?,
        ArtifactKind::Snapshot => artifact_store.snapshot_path(scope_key, artifact_id)?,
        _ => unreachable!("validated above"),
    };
    artifact_store.write_bytes(&path, &bytes)?;
    let manifest = availability_manifest(
        kind,
        artifact_id,
        Some(scope_key),
        source_uri,
        None,
        Some("application/json"),
        None,
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(Some(bytes))
}

fn observed_at_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

pub fn accept_task_result_locally(
    node: &mut Node,
    task_id: &str,
    candidate_id: &str,
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
    node.claim_task(
        task_id,
        ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
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
    node.finalize_decision(
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

fn transport_contact_direct_addrs(contact: &TransportContactMaterial) -> Vec<String> {
    let mut seen = BTreeSet::new();
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

fn availability_manifest(
    kind: ArtifactKind,
    artifact_id: &str,
    scope: Option<&str>,
    source_uri: Option<&str>,
    expected_digest: Option<&str>,
    mime: Option<&str>,
    size_bytes: Option<u64>,
    local_path: Option<&Path>,
    status: ArtifactAvailabilityStatus,
    observed_at: u64,
    repair_attempts: u32,
    next_retry_at: Option<u64>,
    last_error: Option<String>,
) -> ArtifactAvailabilityManifest {
    ArtifactAvailabilityManifest {
        artifact_kind: kind,
        artifact_id: artifact_id.to_owned(),
        scope: scope.map(ToOwned::to_owned),
        source_uri: source_uri.map(ToOwned::to_owned),
        expected_digest: expected_digest.map(ToOwned::to_owned),
        mime: mime.map(ToOwned::to_owned),
        size_bytes,
        local_path: local_path.map(|path| path.display().to_string()),
        status,
        observed_at,
        last_checked_at: Some(observed_at),
        missing_since: (status == ArtifactAvailabilityStatus::Missing).then_some(observed_at),
        repair_attempts,
        next_retry_at,
        last_error,
    }
}

fn load_task_detail_reference(node: &Node, task_id: &str) -> Result<crate::types::ArtifactRef> {
    let detail = node
        .store
        .get_task_announcement_detail_for_task(task_id)?
        .ok_or_else(|| anyhow!("task announcement missing for task {task_id}"))?;
    detail
        .detail_ref()
        .cloned()
        .ok_or_else(|| anyhow!("task announcement detail_ref missing for task {task_id}"))
}

fn task_detail_data_source_binding(
    task_id: &str,
    reference: &crate::types::ArtifactRef,
    observed_at: u64,
) -> Option<DataSourceBindingRecord> {
    let source_node_id = reference.producer.trim();
    if source_node_id.is_empty() {
        return None;
    }
    Some(DataSourceBindingRecord {
        binding_kind: DataSourceBindingKind::TaskDetail,
        binding_scope: None,
        binding_key: task_detail_binding_key(task_id),
        source_node_id: source_node_id.to_owned(),
        source_uri: Some(reference.uri.clone()),
        updated_at: observed_at,
    })
}

fn load_evidence_reference(
    node: &Node,
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
) -> Result<crate::types::ArtifactRef> {
    if let Some(reference) =
        node.store
            .get_evidence_reference(task_id, candidate_id, evidence_digest)?
    {
        return Ok(reference);
    }
    let candidate = node
        .store
        .get_candidate_by_id(task_id, candidate_id)?
        .ok_or_else(|| anyhow!("candidate missing for evidence {candidate_id}"))?;
    candidate
        .evidence_refs
        .into_iter()
        .find(|reference| reference.digest == evidence_digest)
        .ok_or_else(|| {
            anyhow!("evidence ref {evidence_digest} missing for candidate {candidate_id}")
        })
}

fn evidence_data_source_binding(
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
    reference: &crate::types::ArtifactRef,
    observed_at: u64,
) -> Option<DataSourceBindingRecord> {
    let source_node_id = reference.producer.trim();
    if source_node_id.is_empty() {
        return None;
    }
    Some(DataSourceBindingRecord {
        binding_kind: DataSourceBindingKind::Evidence,
        binding_scope: None,
        binding_key: evidence_binding_key(task_id, candidate_id, evidence_digest),
        source_node_id: source_node_id.to_owned(),
        source_uri: Some(reference.uri.clone()),
        updated_at: observed_at,
    })
}

fn write_missing_manifest(
    artifact_store: &ArtifactStore,
    kind: ArtifactKind,
    artifact_id: &str,
    scope: Option<&str>,
    source_uri: Option<&str>,
    expected_digest: Option<&str>,
    mime: Option<&str>,
    size_bytes: Option<u64>,
    observed_at: u64,
    error: anyhow::Error,
) -> Result<()> {
    let repair_attempts = artifact_store
        .read_availability_manifest(kind, artifact_id, scope)?
        .map(|manifest| manifest.repair_attempts.saturating_add(1))
        .unwrap_or(1);
    artifact_store.write_availability_manifest(&availability_manifest(
        kind,
        artifact_id,
        scope,
        source_uri,
        expected_digest,
        mime,
        size_bytes,
        None,
        ArtifactAvailabilityStatus::Missing,
        observed_at,
        repair_attempts,
        Some(observed_at.saturating_add(default_artifact_retry_after_ms())),
        Some(error.to_string()),
    ))?;
    Ok(())
}

fn materialize_reference_artifact(
    state_dir: &Path,
    kind: ArtifactKind,
    artifact_id: &str,
    source_uri: &str,
    expected_digest: &str,
    mime: &str,
    size_bytes: u64,
    bytes: &[u8],
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.write_validated_bytes(
        kind,
        artifact_id,
        None,
        bytes,
        Some(expected_digest),
        Some(size_bytes),
    )?;
    let manifest = availability_manifest(
        kind,
        artifact_id,
        None,
        Some(source_uri),
        Some(expected_digest),
        Some(mime),
        Some(size_bytes),
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(manifest)
}

fn fetch_reference_artifact(
    state_dir: &Path,
    kind: ArtifactKind,
    artifact_id: &str,
    source_uri: &str,
    expected_digest: &str,
    mime: &str,
    size_bytes: u64,
    observed_at: u64,
    binding: Option<&DataSourceBindingRecord>,
    intent: &TransferIntent,
    subject: Value,
) -> Result<Vec<u8>> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    match artifact_store.read_validated_bytes(
        kind,
        artifact_id,
        None,
        Some(expected_digest),
        Some(size_bytes),
    ) {
        Ok(bytes) => {
            let path = match kind {
                ArtifactKind::Reference => artifact_store.reference_path(artifact_id)?,
                ArtifactKind::Evidence => artifact_store.evidence_path(artifact_id)?,
                _ => unreachable!("reference artifact kind"),
            };
            artifact_store.write_availability_manifest(&availability_manifest(
                kind,
                artifact_id,
                None,
                Some(source_uri),
                Some(expected_digest),
                Some(mime),
                Some(size_bytes),
                Some(&path),
                ArtifactAvailabilityStatus::Available,
                observed_at,
                artifact_store
                    .read_availability_manifest(kind, artifact_id, None)?
                    .map(|manifest| manifest.repair_attempts)
                    .unwrap_or(0),
                None,
                None,
            ))?;
            Ok(bytes)
        }
        Err(err) => {
            let route = resolve_route_for_binding(state_dir, binding, intent)?;
            let transport_fetch_error = match maybe_fetch_reference_artifact_via_transport(
                state_dir,
                kind,
                artifact_id,
                source_uri,
                expected_digest,
                mime,
                size_bytes,
                observed_at,
                binding,
                route,
            ) {
                Ok(Some(bytes)) => return Ok(bytes),
                Ok(None) => None,
                Err(fetch_err) => Some(fetch_err),
            };
            record_transport_decision_for_binding(state_dir, binding, intent, route, subject);
            record_missing_manifest_with_binding(
                &artifact_store,
                kind,
                artifact_id,
                None,
                Some(source_uri),
                Some(expected_digest),
                Some(mime),
                Some(size_bytes),
                observed_at,
                binding,
                route,
                match transport_fetch_error {
                    Some(fetch_err) => {
                        err.context(format!("iroh fetch reference artifact: {fetch_err}"))
                    }
                    None => err.context("fetch reference artifact"),
                },
            )?;
            Err(anyhow!("artifact {} is not locally available", artifact_id))
        }
    }
}

pub fn materialize_task_detail_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    bytes: &[u8],
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let reference = load_task_detail_reference(node, task_id)?;
    materialize_reference_artifact(
        state_dir,
        ArtifactKind::Reference,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        bytes,
        observed_at,
    )
}

pub fn fetch_task_detail_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    observed_at: u64,
) -> Result<Vec<u8>> {
    let reference = load_task_detail_reference(node, task_id)?;
    let binding = task_detail_data_source_binding(task_id, &reference, observed_at);
    if let Some(binding) = binding.as_ref() {
        save_data_source_binding_record_state(state_dir, binding)?;
    }
    fetch_reference_artifact(
        state_dir,
        ArtifactKind::Reference,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        observed_at,
        binding.as_ref(),
        &TransferIntent {
            kind: TransferKind::ArtifactBlob,
            payload_bytes: reference.size_bytes as usize,
            requires_streaming: reference.size_bytes > 16 * 1024,
        },
        json!({
            "artifact_kind": "task_detail",
            "task_id": task_id,
            "artifact_id": reference.digest,
        }),
    )
}

pub fn materialize_evidence_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
    bytes: &[u8],
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let reference = load_evidence_reference(node, task_id, candidate_id, evidence_digest)?;
    materialize_reference_artifact(
        state_dir,
        ArtifactKind::Evidence,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        bytes,
        observed_at,
    )
}

pub fn fetch_evidence_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    candidate_id: &str,
    evidence_digest: &str,
    observed_at: u64,
) -> Result<Vec<u8>> {
    let reference = load_evidence_reference(node, task_id, candidate_id, evidence_digest)?;
    let binding = evidence_data_source_binding(
        task_id,
        candidate_id,
        evidence_digest,
        &reference,
        observed_at,
    );
    if let Some(binding) = binding.as_ref() {
        save_data_source_binding_record_state(state_dir, binding)?;
    }
    fetch_reference_artifact(
        state_dir,
        ArtifactKind::Evidence,
        &reference.digest,
        &reference.uri,
        &reference.digest,
        &reference.mime,
        reference.size_bytes,
        observed_at,
        binding.as_ref(),
        &TransferIntent {
            kind: TransferKind::EvidenceBlob,
            payload_bytes: reference.size_bytes as usize,
            requires_streaming: reference.size_bytes > 16 * 1024,
        },
        json!({
            "artifact_kind": "evidence",
            "task_id": task_id,
            "candidate_id": candidate_id,
            "artifact_id": reference.digest,
        }),
    )
}

pub fn materialize_checkpoint_artifact_json<T: Serialize>(
    state_dir: &Path,
    node: &Node,
    scope_key: &str,
    checkpoint_id: &str,
    value: &T,
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let checkpoint = node
        .store
        .get_checkpoint_announcement(scope_key, checkpoint_id)?
        .ok_or_else(|| {
            anyhow!("checkpoint announcement missing for {scope_key}/{checkpoint_id}")
        })?;
    let path = artifact_store.checkpoint_path(checkpoint_id)?;
    artifact_store.write_json(&path, value)?;
    let manifest = availability_manifest(
        ArtifactKind::Checkpoint,
        checkpoint_id,
        Some(scope_key),
        Some(&checkpoint.artifact_path),
        None,
        Some("application/json"),
        None,
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(manifest)
}

fn round_checkpoint_scope_key(contract: &TaskContract) -> String {
    if let Some(scope_hint) = contract
        .inputs
        .get("scope_hint")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return crate::types::normalized_scope_hint(scope_hint);
    }
    if let Some(route) = contract.transport_route()
        && let Some(group_id) = route.group_id.as_deref()
        && !group_id.trim().is_empty()
    {
        return crate::types::normalized_scope_hint(&format!("group:{group_id}"));
    }
    "global".to_owned()
}

fn round_checkpoint_phase_label(phase: crate::types::RoundCheckpointPhase) -> &'static str {
    match phase {
        crate::types::RoundCheckpointPhase::Opening => "opening",
        crate::types::RoundCheckpointPhase::Active => "active",
        crate::types::RoundCheckpointPhase::Closing => "closing",
        crate::types::RoundCheckpointPhase::Closed => "closed",
        crate::types::RoundCheckpointPhase::Finalized => "finalized",
        crate::types::RoundCheckpointPhase::Takeover => "takeover",
    }
}

pub fn round_checkpoint_artifact_id(
    task_id: &str,
    round_index: u32,
    phase: crate::types::RoundCheckpointPhase,
) -> Result<String> {
    Ok(sha256_hex(&serde_json::to_vec(&json!({
        "task_id": task_id,
        "round_index": round_index,
        "phase": round_checkpoint_phase_label(phase),
    }))?))
}

fn round_checkpoint_id(checkpoint: &crate::types::RoundCheckpoint) -> Result<String> {
    round_checkpoint_artifact_id(
        &checkpoint.task_id,
        checkpoint.round_index,
        checkpoint.phase,
    )
}

pub fn materialize_round_checkpoint_artifact(
    state_dir: &Path,
    node: &mut Node,
    contract: &TaskContract,
    round_index: u32,
    steward_node_id: &str,
    phase: crate::types::RoundCheckpointPhase,
    open_participant_ids: Vec<String>,
    close_reason: Option<String>,
    next_round_index: Option<u32>,
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let checkpoint = crate::types::RoundCheckpoint {
        task_id: contract.task_id.clone(),
        round_index,
        phase,
        steward_node_id: steward_node_id.to_owned(),
        open_participant_ids,
        close_reason,
        next_round_index,
        created_at: observed_at,
    };
    let checkpoint_id = round_checkpoint_id(&checkpoint)?;
    let scope_key = round_checkpoint_scope_key(contract);
    let artifact_path = format!(
        "task://{}/{}/round/{}/{}",
        contract.task_type,
        contract.task_id,
        round_index,
        round_checkpoint_phase_label(phase)
    );
    node.store.put_checkpoint_announcement(
        &scope_key,
        &checkpoint_id,
        &artifact_path,
        observed_at,
    )?;
    let epoch = node
        .task_view(&contract.task_id)?
        .map(|task| task.epoch)
        .unwrap_or(1);
    let _ = node.create_checkpoint(checkpoint_id.clone(), epoch, observed_at)?;
    Ok(materialize_checkpoint_artifact_json(
        state_dir,
        node,
        &scope_key,
        &checkpoint_id,
        &checkpoint,
        observed_at,
    )?)
}

pub fn fetch_checkpoint_artifact_json<T: DeserializeOwned>(
    state_dir: &Path,
    node: &Node,
    scope_key: &str,
    checkpoint_id: &str,
    observed_at: u64,
) -> Result<T> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let checkpoint = node
        .store
        .get_checkpoint_announcement(scope_key, checkpoint_id)?
        .ok_or_else(|| {
            anyhow!("checkpoint announcement missing for {scope_key}/{checkpoint_id}")
        })?;
    let path = artifact_store.checkpoint_path(checkpoint_id)?;
    match artifact_store.read_json::<T>(&path) {
        Ok(value) => {
            artifact_store.write_availability_manifest(&availability_manifest(
                ArtifactKind::Checkpoint,
                checkpoint_id,
                Some(scope_key),
                Some(&checkpoint.artifact_path),
                None,
                Some("application/json"),
                None,
                Some(&path),
                ArtifactAvailabilityStatus::Available,
                observed_at,
                artifact_store
                    .read_availability_manifest(
                        ArtifactKind::Checkpoint,
                        checkpoint_id,
                        Some(scope_key),
                    )?
                    .map(|manifest| manifest.repair_attempts)
                    .unwrap_or(0),
                None,
                None,
            ))?;
            Ok(value)
        }
        Err(err) => {
            let binding = load_data_source_binding_record_state(
                state_dir,
                DataSourceBindingKind::Checkpoint,
                Some(scope_key),
                &checkpoint_binding_key(checkpoint_id),
            )?;
            let intent = TransferIntent {
                kind: TransferKind::CheckpointSnapshot,
                payload_bytes: 256 * 1024,
                requires_streaming: true,
            };
            let route = resolve_route_for_binding(state_dir, binding.as_ref(), &intent)?;
            let transport_fetch_error = match maybe_fetch_json_artifact_via_transport(
                state_dir,
                ArtifactKind::Checkpoint,
                scope_key,
                checkpoint_id,
                Some(&checkpoint.artifact_path),
                observed_at,
                binding.as_ref(),
                route,
            ) {
                Ok(Some(bytes)) => return Ok(serde_json::from_slice(&bytes)?),
                Ok(None) => None,
                Err(fetch_err) => Some(fetch_err),
            };
            record_transport_decision_for_binding(
                state_dir,
                binding.as_ref(),
                &intent,
                route,
                json!({
                    "artifact_kind": "checkpoint",
                    "scope_key": scope_key,
                    "checkpoint_id": checkpoint_id,
                }),
            );
            record_missing_manifest_with_binding(
                &artifact_store,
                ArtifactKind::Checkpoint,
                checkpoint_id,
                Some(scope_key),
                Some(&checkpoint.artifact_path),
                None,
                Some("application/json"),
                None,
                observed_at,
                binding.as_ref(),
                route,
                match transport_fetch_error {
                    Some(fetch_err) => {
                        err.context(format!("iroh fetch checkpoint artifact: {fetch_err}"))
                    }
                    None => err.context("fetch checkpoint artifact"),
                },
            )?;
            Err(anyhow!(
                "checkpoint artifact {checkpoint_id} is not locally available"
            ))
        }
    }
}

pub fn fetch_round_checkpoint_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    round_index: u32,
    phase: crate::types::RoundCheckpointPhase,
    observed_at: u64,
) -> Result<Option<crate::types::RoundCheckpoint>> {
    let checkpoint_id = round_checkpoint_artifact_id(task_id, round_index, phase)?;
    let Some(row) = node.store.find_checkpoint_announcement(&checkpoint_id)? else {
        return Ok(None);
    };
    Ok(Some(fetch_checkpoint_artifact_json(
        state_dir,
        node,
        &row.scope_key,
        &checkpoint_id,
        observed_at,
    )?))
}

pub fn fetch_latest_round_checkpoint_artifact(
    state_dir: &Path,
    node: &Node,
    task_id: &str,
    round_index: u32,
    observed_at: u64,
) -> Result<Option<crate::types::RoundCheckpoint>> {
    for phase in [
        crate::types::RoundCheckpointPhase::Takeover,
        crate::types::RoundCheckpointPhase::Finalized,
        crate::types::RoundCheckpointPhase::Closed,
        crate::types::RoundCheckpointPhase::Closing,
        crate::types::RoundCheckpointPhase::Active,
        crate::types::RoundCheckpointPhase::Opening,
    ] {
        if let Some(checkpoint) = fetch_round_checkpoint_artifact(
            state_dir,
            node,
            task_id,
            round_index,
            phase,
            observed_at,
        )? {
            return Ok(Some(checkpoint));
        }
    }
    Ok(None)
}

pub fn materialize_snapshot_artifact_json<T: Serialize>(
    state_dir: &Path,
    scope_key: &str,
    snapshot_id: &str,
    source_uri: Option<&str>,
    value: &T,
    observed_at: u64,
) -> Result<ArtifactAvailabilityManifest> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.snapshot_path(scope_key, snapshot_id)?;
    artifact_store.write_json(&path, value)?;
    let manifest = availability_manifest(
        ArtifactKind::Snapshot,
        snapshot_id,
        Some(scope_key),
        source_uri,
        None,
        Some("application/json"),
        None,
        Some(&path),
        ArtifactAvailabilityStatus::Available,
        observed_at,
        0,
        None,
        None,
    );
    artifact_store.write_availability_manifest(&manifest)?;
    Ok(manifest)
}

pub fn fetch_snapshot_artifact_json<T: DeserializeOwned>(
    state_dir: &Path,
    scope_key: &str,
    snapshot_id: &str,
    observed_at: u64,
) -> Result<T> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    let path = artifact_store.snapshot_path(scope_key, snapshot_id)?;
    match artifact_store.read_json::<T>(&path) {
        Ok(value) => {
            artifact_store.write_availability_manifest(&availability_manifest(
                ArtifactKind::Snapshot,
                snapshot_id,
                Some(scope_key),
                None,
                None,
                Some("application/json"),
                None,
                Some(&path),
                ArtifactAvailabilityStatus::Available,
                observed_at,
                artifact_store
                    .read_availability_manifest(
                        ArtifactKind::Snapshot,
                        snapshot_id,
                        Some(scope_key),
                    )?
                    .map(|manifest| manifest.repair_attempts)
                    .unwrap_or(0),
                None,
                None,
            ))?;
            Ok(value)
        }
        Err(err) => {
            let binding = load_data_source_binding_record_state(
                state_dir,
                DataSourceBindingKind::Snapshot,
                Some(scope_key),
                &snapshot_binding_key(snapshot_id),
            )?;
            let intent = TransferIntent {
                kind: TransferKind::CheckpointSnapshot,
                payload_bytes: 256 * 1024,
                requires_streaming: true,
            };
            let route = resolve_route_for_binding(state_dir, binding.as_ref(), &intent)?;
            let transport_fetch_error = match maybe_fetch_json_artifact_via_transport(
                state_dir,
                ArtifactKind::Snapshot,
                scope_key,
                snapshot_id,
                None,
                observed_at,
                binding.as_ref(),
                route,
            ) {
                Ok(Some(bytes)) => return Ok(serde_json::from_slice(&bytes)?),
                Ok(None) => None,
                Err(fetch_err) => Some(fetch_err),
            };
            record_transport_decision_for_binding(
                state_dir,
                binding.as_ref(),
                &intent,
                route,
                json!({
                    "artifact_kind": "snapshot",
                    "scope_key": scope_key,
                    "snapshot_id": snapshot_id,
                }),
            );
            record_missing_manifest_with_binding(
                &artifact_store,
                ArtifactKind::Snapshot,
                snapshot_id,
                Some(scope_key),
                None,
                None,
                Some("application/json"),
                None,
                observed_at,
                binding.as_ref(),
                route,
                match transport_fetch_error {
                    Some(fetch_err) => {
                        err.context(format!("iroh fetch snapshot artifact: {fetch_err}"))
                    }
                    None => err.context("fetch snapshot artifact"),
                },
            )?;
            Err(anyhow!(
                "snapshot artifact {snapshot_id} is not locally available"
            ))
        }
    }
}

pub fn list_artifacts_needing_repair(
    state_dir: &Path,
    now_ms: u64,
) -> Result<Vec<ArtifactAvailabilityManifest>> {
    let artifact_store = open_local_artifact_store(state_dir)?;
    artifact_store.list_manifests_needing_repair(now_ms)
}

pub(crate) fn run_existing_task_with_runtime(
    node: &mut Node,
    state_dir: &Path,
    runtime: &dyn RuntimeClient,
    capabilities: &RuntimeCapabilities,
    executor: &str,
    profile: &str,
    task_id: &str,
    now: u64,
) -> Result<Value> {
    let task = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task view missing for {task_id}"))?;
    let propose_execution_id = format!("exec-p-{}", Uuid::new_v4());
    let verify_execution_id = format!("exec-v-{}", Uuid::new_v4());
    let lease_until = now.saturating_add(task.contract.assignment.claim.lease_ms);

    node.claim_task(
        task_id,
        ClaimRole::Propose,
        &propose_execution_id,
        lease_until,
        1,
        now.saturating_add(1),
    )?;
    let execute_req = node.build_execute_request_for_runtime(
        task_id,
        profile,
        &propose_execution_id,
        1,
        now.saturating_add(2),
    )?;
    let execute_res = runtime.execute(&execute_req)?;
    let candidate_output_ref = materialize_candidate_output_artifact(
        state_dir,
        &node.node_id(),
        &execute_res.candidate_output,
        now.saturating_add(2),
    )?;
    let candidate = Candidate {
        candidate_id: format!("cand-{}", propose_execution_id),
        execution_id: propose_execution_id.clone(),
        output_ref: candidate_output_ref,
        output: execute_res.candidate_output,
        evidence_inline: execute_res.evidence_inline,
        evidence_refs: execute_res.evidence_refs,
    };
    node.propose_candidate(task_id, candidate, 1, now.saturating_add(2))?;

    let candidate_id = format!("cand-{propose_execution_id}");
    node.claim_task(
        task_id,
        ClaimRole::Verify,
        &verify_execution_id,
        lease_until,
        1,
        now.saturating_add(3),
    )?;
    node.auto_verify_candidate_with_runtime(
        runtime,
        task_id,
        &candidate_id,
        &verify_execution_id,
        1,
        now.saturating_add(4),
    )?;

    let verifier_result = node
        .store
        .list_verifier_results_for_candidate(task_id, &candidate_id)?
        .into_iter()
        .find(|row| row.execution_id == verify_execution_id)
        .ok_or_else(|| {
            anyhow!(
                "missing verifier result for execution {}",
                verify_execution_id
            )
        })?;
    if !verifier_result.passed {
        return Err(anyhow!(
            "verifier did not pass candidate; reason_codes={:?}",
            verifier_result.reason_codes
        ));
    }
    let candidate = node
        .store
        .get_candidate_by_id(task_id, &candidate_id)?
        .ok_or_else(|| anyhow!("candidate not found after execute: {candidate_id}"))?;
    let candidate_hash = candidate_hash(&candidate)?;

    let salt = Uuid::new_v4().to_string();
    let commit_hash = vote_commit_hash(
        VoteChoice::Approve,
        &salt,
        &verifier_result.verifier_result_hash,
    );
    node.submit_vote_commit(
        VoteCommitPayload {
            task_id: task_id.to_owned(),
            candidate_id: candidate_id.clone(),
            candidate_hash: candidate_hash.clone(),
            execution_id: verify_execution_id.clone(),
            verifier_result_hash: verifier_result.verifier_result_hash.clone(),
            commit_hash,
        },
        1,
        now.saturating_add(5),
    )?;
    node.submit_vote_reveal(
        VoteRevealPayload {
            task_id: task_id.to_owned(),
            candidate_id: candidate_id.clone(),
            candidate_hash,
            execution_id: verify_execution_id,
            verifier_result_hash: verifier_result.verifier_result_hash.clone(),
            vote: VoteChoice::Approve,
            salt,
        },
        1,
        now.saturating_add(6),
    )?;
    node.commit_decision(task_id, 1, &candidate_id, now.saturating_add(7))?;
    node.finalize_decision(
        task_id,
        1,
        &candidate_id,
        FinalityProof {
            threshold: 1,
            signatures: vec![finality_sign(&node.identity, task_id, 1, &candidate_id)],
        },
        now.saturating_add(8),
    )?;

    let view = node
        .task_view(task_id)?
        .ok_or_else(|| anyhow!("task view not found after run"))?;
    let final_decision = candidate
        .output
        .get("decision")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let final_answer = candidate
        .output
        .get("answer")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let _ = materialize_round_checkpoint_artifact(
        state_dir,
        node,
        &task.contract,
        1,
        &node.node_id(),
        crate::types::RoundCheckpointPhase::Finalized,
        Vec::new(),
        final_decision.clone(),
        None,
        now.saturating_add(9),
    )?;
    let evidence_digests = candidate
        .evidence_refs
        .iter()
        .map(|r| r.digest.clone())
        .collect::<Vec<_>>();
    Ok(json!({
        "task_id": task_id,
        "candidate_id": candidate_id,
        "executor": executor,
        "profile": profile,
        "provider_family": capabilities.provider_family,
        "model_id": capabilities.model_id,
        "candidate_output": candidate.output,
        "evidence_digests": evidence_digests,
        "final_decision": final_decision,
        "final_answer": final_answer,
        "terminal_state": format!("{:?}", view.terminal_state),
        "committed_candidate_id": view.committed_candidate_id,
        "finalized_candidate_id": view.finalized_candidate_id
    }))
}

fn remote_bridge_scope_hint(raw: &str) -> String {
    crate::types::normalized_scope_hint(raw)
}

fn remote_task_bridge_allowed_for_scope(
    node_id: &str,
    scope_hint: &str,
    route: Option<&crate::types::TransportRoute>,
) -> bool {
    match crate::types::ScopeHint::parse_with_prefix_fallback(scope_hint) {
        Some(crate::types::ScopeHint::Node(target)) => target == node_id,
        Some(crate::types::ScopeHint::Group(group_id)) => {
            route.is_some_and(|route| route.matches_group(&group_id) && route.allows_node(node_id))
        }
        Some(crate::types::ScopeHint::Global | crate::types::ScopeHint::Region(_)) => {
            route.is_none_or(|route| route.allows_node(node_id))
        }
        None => scope_hint.trim().is_empty(),
    }
}

fn remote_bridge_execution_set_id(task_id: &str) -> String {
    format!("remote-bridge:{task_id}")
}

fn ensure_remote_execution_participation(
    node: &mut Node,
    task_id: &str,
    network_id: &str,
    scope_hint: &str,
    created_at: u64,
) -> Result<()> {
    let execution_set_id = remote_bridge_execution_set_id(task_id);
    let local_node_id = node.node_id();
    let existing = node
        .store
        .list_execution_set_members(task_id, &execution_set_id)?
        .into_iter()
        .any(|member| member.participant_node_id == local_node_id);
    if existing {
        return Ok(());
    }

    node.emit_at(
        1,
        EventPayload::ExecutionIntentDeclared(ExecutionIntentDeclaredPayload {
            network_id: network_id.to_owned(),
            task_id: task_id.to_owned(),
            execution_set_id: execution_set_id.clone(),
            participant_node_id: local_node_id.clone(),
            role_hint: "executor".to_owned(),
            scope_hint: scope_hint.to_owned(),
            intent: "accepted".to_owned(),
        }),
        created_at,
    )?;
    node.emit_at(
        1,
        EventPayload::ExecutionSetConfirmed(ExecutionSetConfirmedPayload {
            network_id: network_id.to_owned(),
            task_id: task_id.to_owned(),
            execution_set_id,
            confirmed_by_node_id: local_node_id.clone(),
            scope_hint: scope_hint.to_owned(),
            members: vec![ExecutionSetMember {
                participant_node_id: local_node_id,
                role_hint: "executor".to_owned(),
            }],
        }),
        created_at.saturating_add(1),
    )?;
    Ok(())
}

fn bridged_task_contract(node: &Node, state_dir: &Path, task_id: &str) -> Result<TaskContract> {
    if let Some(task) = node.task_view(task_id)? {
        return Ok(task.contract);
    }
    let detail = node
        .store
        .get_task_announcement_detail_for_task(task_id)?
        .ok_or_else(|| anyhow!("task announcement missing for task {task_id}"))?;
    if let Some(contract) = detail.contract {
        return Ok(contract);
    }
    let bytes = fetch_task_detail_artifact(state_dir, node, task_id, observed_at_ms())?;
    let contract = serde_json::from_slice::<TaskContract>(&bytes)
        .with_context(|| format!("parse bridged task detail for {task_id}"))?;
    if contract.task_id != task_id {
        return Err(anyhow!(
            "bridged task detail task_id mismatch: expected {}, got {}",
            task_id,
            contract.task_id
        ));
    }
    Ok(contract)
}

fn bridge_origin_payload(
    task_id: &str,
    deduped: bool,
    record: &RemoteTaskBridgeRecord,
    run: Value,
) -> Value {
    let mut value = run;
    let bridge = json!({
        "deduped": deduped,
        "announcement_id": record.announcement_id,
        "network_id": record.network_id,
        "source_node_id": record.source_node_id,
        "source_scope_hint": record.source_scope_hint,
        "detail_ref_digest": record.detail_ref_digest,
        "bridged_at": record.bridged_at
    });
    if let Some(obj) = value.as_object_mut() {
        obj.insert("task_id".to_owned(), json!(task_id));
        obj.insert("bridge".to_owned(), bridge);
    }
    value
}

pub fn bridge_remote_task_into_local_execution(
    node: &mut Node,
    state_dir: &Path,
    req: RemoteTaskBridgeRequest,
) -> Result<Value> {
    let RemoteTaskBridgeRequest {
        executor,
        profile,
        task_id,
    } = req;
    let executor = normalize_executor_name(&executor).to_owned();
    let detail = node
        .store
        .get_task_announcement_detail_for_task(&task_id)?
        .ok_or_else(|| anyhow!("remote task announcement missing for task {}", task_id))?;
    let announcement = detail.announcement.clone();

    // Coordinator-only constraint: the node that announced the task must not
    // execute it locally. The initiator coordinates; remote nodes execute.
    if announcement.announced_by_node_id == node.node_id() {
        return Err(anyhow!(
            "coordinator-only constraint: node {} announced task {} and cannot also execute it",
            node.node_id(),
            task_id
        ));
    }

    let network_id = current_network_context_id(node);
    let contract = bridged_task_contract(node, state_dir, &task_id)?;
    let route = contract.transport_route();
    let scope_hint = remote_bridge_scope_hint(&announcement.scope_hint);
    if !remote_task_bridge_allowed_for_scope(&node.node_id(), &scope_hint, route.as_ref()) {
        return Err(anyhow!(
            "remote task {} is not eligible for local node {} under scope {}",
            task_id,
            node.node_id(),
            scope_hint
        ));
    }
    let mut registry = load_remote_task_bridge_registry(state_dir)?;

    if let Some(existing) = registry.entries.iter().find(|entry| {
        entry.task_id == task_id
            && entry.announcement_id == announcement.announcement_id
            && entry.executor == executor
            && entry.profile == profile
    }) {
        return Ok(bridge_origin_payload(
            &task_id,
            true,
            existing,
            json!({
                "task_id": task_id,
                "candidate_id": existing.candidate_id,
                "executor": existing.executor,
                "profile": existing.profile,
                "terminal_state": existing.terminal_state
            }),
        ));
    }

    let prepared = prepare_runtime_for_executor(state_dir, &executor, &profile)?;
    if node.task_view(&task_id)?.is_none() {
        node.submit_task(contract, 1, observed_at_ms())?;
    }
    ensure_remote_execution_participation(
        node,
        &task_id,
        &network_id,
        &scope_hint,
        observed_at_ms().saturating_add(1),
    )?;
    let run = run_existing_task_with_runtime(
        node,
        state_dir,
        &prepared.runtime,
        &prepared.capabilities,
        &executor,
        &profile,
        &task_id,
        observed_at_ms().saturating_add(2),
    )?;
    let candidate_id = run
        .get("candidate_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing candidate_id after bridged run"))?
        .to_owned();
    let terminal_state = run
        .get("terminal_state")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing terminal_state after bridged run"))?
        .to_owned();

    let record = RemoteTaskBridgeRecord {
        task_id: task_id.clone(),
        announcement_id: announcement.announcement_id,
        network_id,
        source_node_id: announcement.announced_by_node_id,
        source_scope_hint: scope_hint,
        detail_ref_digest: announcement.detail_ref.map(|reference| reference.digest),
        executor: executor.clone(),
        profile: profile.clone(),
        candidate_id,
        terminal_state,
        bridged_at: observed_at_ms(),
    };
    registry.entries.retain(|entry| {
        !(entry.task_id == record.task_id
            && entry.executor == record.executor
            && entry.profile == record.profile)
    });
    registry.entries.push(record.clone());
    registry.entries.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.executor.cmp(&right.executor))
            .then_with(|| left.profile.cmp(&right.profile))
    });
    save_remote_task_bridge_registry(state_dir, &registry)?;

    Ok(bridge_origin_payload(&task_id, false, &record, run))
}

pub fn run_real_task_flow(
    node: &mut Node,
    state_dir: &Path,
    req: RealTaskRunRequest,
) -> Result<Value> {
    let RealTaskRunRequest {
        executor,
        profile,
        task_id,
        task_file,
        task_contract,
    } = req;
    let prepared = prepare_runtime_for_executor(state_dir, &executor, &profile)?;

    let mut contract = if let Some(contract) = task_contract {
        contract
    } else if let Some(file) = task_file {
        let raw = fs::read(&file)?;
        serde_json::from_slice::<TaskContract>(&raw)
            .with_context(|| format!("parse task contract from {}", file.display()))?
    } else {
        let real_task_id = task_id
            .clone()
            .unwrap_or_else(|| format!("task-{}", Uuid::new_v4()));
        let policy_hash = node
            .policy_registry()
            .binding_for("vp.schema_only.v1", json!({}))?
            .policy_hash;
        sample_contract(&real_task_id, policy_hash)
    };

    if let Some(task_id) = task_id {
        contract.task_id = task_id;
    }

    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    node.submit_task(contract.clone(), 1, now)?;
    let _ = materialize_round_checkpoint_artifact(
        state_dir,
        node,
        &contract,
        1,
        &node.node_id(),
        crate::types::RoundCheckpointPhase::Opening,
        Vec::new(),
        None,
        None,
        now,
    )?;
    run_existing_task_with_runtime(
        node,
        state_dir,
        &prepared.runtime,
        &prepared.capabilities,
        &executor,
        &profile,
        &contract.task_id,
        now,
    )
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
                "display_name": "Node Agent",
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
