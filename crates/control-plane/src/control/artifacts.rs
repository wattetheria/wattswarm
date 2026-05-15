use super::*;

pub(super) fn open_local_artifact_store(state_dir: &Path) -> Result<ArtifactStore> {
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

pub(super) fn transport_contact_direct_addrs(contact: &TransportContactMaterial) -> Vec<String> {
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
