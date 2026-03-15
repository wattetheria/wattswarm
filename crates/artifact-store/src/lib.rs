use anyhow::{Result, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};
use wattswarm_crypto::sha256_hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Reference,
    Evidence,
    Checkpoint,
    Snapshot,
    EventBatch,
    Availability,
}

impl ArtifactKind {
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Reference => "references",
            Self::Evidence => "evidence",
            Self::Checkpoint => "checkpoints",
            Self::Snapshot => "snapshots",
            Self::EventBatch => "event-batches",
            Self::Availability => "availability",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactAvailabilityStatus {
    Referenced,
    Available,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactAvailabilityManifest {
    pub artifact_kind: ArtifactKind,
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
    pub status: ArtifactAvailabilityStatus,
    pub observed_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_since: Option<u64>,
    #[serde(default)]
    pub repair_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn ensure_layout(&self) -> Result<()> {
        for kind in [
            ArtifactKind::Reference,
            ArtifactKind::Evidence,
            ArtifactKind::Checkpoint,
            ArtifactKind::Snapshot,
            ArtifactKind::EventBatch,
            ArtifactKind::Availability,
        ] {
            fs::create_dir_all(self.kind_root(kind))?;
        }
        Ok(())
    }

    pub fn kind_root(&self, kind: ArtifactKind) -> PathBuf {
        self.root.join(kind.dir_name())
    }

    pub fn evidence_path(&self, digest: &str) -> Result<PathBuf> {
        self.sharded_digest_path(ArtifactKind::Evidence, digest)
    }

    pub fn reference_path(&self, digest: &str) -> Result<PathBuf> {
        self.sharded_digest_path(ArtifactKind::Reference, digest)
    }

    fn sharded_digest_path(&self, kind: ArtifactKind, digest: &str) -> Result<PathBuf> {
        let cleaned = sanitize_component(digest)?;
        let shard = cleaned.get(0..2).unwrap_or("00");
        Ok(self.kind_root(kind).join(shard).join(cleaned))
    }

    pub fn checkpoint_path(&self, checkpoint_id: &str) -> Result<PathBuf> {
        Ok(self
            .kind_root(ArtifactKind::Checkpoint)
            .join(format!("{}.json", sanitize_component(checkpoint_id)?)))
    }

    pub fn snapshot_path(&self, scope: &str, snapshot_id: &str) -> Result<PathBuf> {
        Ok(self
            .kind_root(ArtifactKind::Snapshot)
            .join(sanitize_component(scope)?)
            .join(format!("{}.json", sanitize_component(snapshot_id)?)))
    }

    pub fn event_batch_path(&self, scope: &str, batch_id: &str) -> Result<PathBuf> {
        Ok(self
            .kind_root(ArtifactKind::EventBatch)
            .join(sanitize_component(scope)?)
            .join(format!("{}.jsonl", sanitize_component(batch_id)?)))
    }

    pub fn availability_path(
        &self,
        kind: ArtifactKind,
        artifact_id: &str,
        scope: Option<&str>,
    ) -> Result<PathBuf> {
        let mut path = self
            .kind_root(ArtifactKind::Availability)
            .join(kind.dir_name());
        if let Some(scope) = scope {
            path = path.join(sanitize_component(scope)?);
        }
        Ok(path.join(format!("{}.json", sanitize_component(artifact_id)?)))
    }

    pub fn write_availability_manifest(
        &self,
        manifest: &ArtifactAvailabilityManifest,
    ) -> Result<PathBuf> {
        let path = self.availability_path(
            manifest.artifact_kind,
            &manifest.artifact_id,
            manifest.scope.as_deref(),
        )?;
        self.write_json(&path, manifest)?;
        Ok(path)
    }

    pub fn read_availability_manifest(
        &self,
        kind: ArtifactKind,
        artifact_id: &str,
        scope: Option<&str>,
    ) -> Result<Option<ArtifactAvailabilityManifest>> {
        let path = self.availability_path(kind, artifact_id, scope)?;
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(self.read_json(&path)?))
    }

    pub fn list_manifests_needing_repair(
        &self,
        now_ms: u64,
    ) -> Result<Vec<ArtifactAvailabilityManifest>> {
        let root = self.kind_root(ArtifactKind::Availability);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut pending = Vec::new();
        let mut stack = vec![root];
        while let Some(path) = stack.pop() {
            for entry in fs::read_dir(&path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let manifest: ArtifactAvailabilityManifest = self.read_json(&path)?;
                if manifest.status == ArtifactAvailabilityStatus::Missing
                    && manifest.next_retry_at.is_some_and(|next| next <= now_ms)
                {
                    pending.push(manifest);
                }
            }
        }
        Ok(pending)
    }

    pub fn write_validated_bytes(
        &self,
        kind: ArtifactKind,
        artifact_id: &str,
        scope: Option<&str>,
        bytes: &[u8],
        expected_digest: Option<&str>,
        expected_size: Option<u64>,
    ) -> Result<PathBuf> {
        self.validate_bytes(bytes, expected_digest, expected_size)?;
        let path = self.content_path(kind, artifact_id, scope)?;
        self.write_bytes(&path, bytes)?;
        Ok(path)
    }

    pub fn read_validated_bytes(
        &self,
        kind: ArtifactKind,
        artifact_id: &str,
        scope: Option<&str>,
        expected_digest: Option<&str>,
        expected_size: Option<u64>,
    ) -> Result<Vec<u8>> {
        let path = self.content_path(kind, artifact_id, scope)?;
        let bytes = self.read_bytes(&path)?;
        self.validate_bytes(&bytes, expected_digest, expected_size)?;
        Ok(bytes)
    }

    pub fn write_bytes(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        ensure_parent_dir(path)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    pub fn read_bytes(&self, path: &Path) -> Result<Vec<u8>> {
        Ok(fs::read(path)?)
    }

    pub fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(value)?;
        self.write_bytes(path, &bytes)
    }

    pub fn read_json<T: DeserializeOwned>(&self, path: &Path) -> Result<T> {
        let bytes = self.read_bytes(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn content_path(
        &self,
        kind: ArtifactKind,
        artifact_id: &str,
        scope: Option<&str>,
    ) -> Result<PathBuf> {
        match kind {
            ArtifactKind::Reference => self.reference_path(artifact_id),
            ArtifactKind::Evidence => self.evidence_path(artifact_id),
            ArtifactKind::Checkpoint => self.checkpoint_path(artifact_id),
            ArtifactKind::Snapshot => self.snapshot_path(
                scope.ok_or_else(|| anyhow::anyhow!("scope required"))?,
                artifact_id,
            ),
            ArtifactKind::EventBatch => self.event_batch_path(
                scope.ok_or_else(|| anyhow::anyhow!("scope required"))?,
                artifact_id,
            ),
            ArtifactKind::Availability => bail!("availability manifests are not content objects"),
        }
    }

    fn validate_bytes(
        &self,
        bytes: &[u8],
        expected_digest: Option<&str>,
        expected_size: Option<u64>,
    ) -> Result<()> {
        if let Some(expected_size) = expected_size
            && expected_size != bytes.len() as u64
        {
            bail!(
                "artifact size mismatch: expected {}, got {}",
                expected_size,
                bytes.len()
            );
        }
        if let Some(expected_hash) = normalized_sha256_digest(expected_digest)
            && sha256_hex(bytes) != expected_hash
        {
            bail!("artifact digest mismatch");
        }
        Ok(())
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        bail!("artifact path must have a parent directory");
    };
    fs::create_dir_all(parent)?;
    Ok(())
}

fn sanitize_component(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("artifact path component cannot be empty");
    }

    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out == "." || out == ".." {
        bail!("artifact path component cannot be a traversal marker");
    }
    Ok(out)
}

fn normalized_sha256_digest(raw: Option<&str>) -> Option<String> {
    let raw = raw?.trim();
    let candidate = raw.strip_prefix("sha256:").unwrap_or(raw);
    if candidate.len() == 64 && candidate.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(candidate.to_ascii_lowercase())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SampleManifest {
        id: String,
        revision: u64,
    }

    #[test]
    fn ensure_layout_creates_known_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ArtifactStore::new(dir.path());
        store.ensure_layout().expect("layout");

        for kind in [
            ArtifactKind::Reference,
            ArtifactKind::Evidence,
            ArtifactKind::Checkpoint,
            ArtifactKind::Snapshot,
            ArtifactKind::EventBatch,
            ArtifactKind::Availability,
        ] {
            assert!(store.kind_root(kind).exists(), "missing {:?}", kind);
        }
    }

    #[test]
    fn reference_path_uses_digest_shard() {
        let store = ArtifactStore::new("/tmp/ws-artifacts");
        let path = store
            .reference_path("sha256:abcdef123456")
            .expect("reference path");
        assert_eq!(
            path,
            PathBuf::from("/tmp/ws-artifacts/references/sh/sha256_abcdef123456")
        );
    }

    #[test]
    fn evidence_path_uses_digest_shard() {
        let store = ArtifactStore::new("/tmp/ws-artifacts");
        let path = store
            .evidence_path("sha256:abcdef123456")
            .expect("evidence path");
        assert_eq!(
            path,
            PathBuf::from("/tmp/ws-artifacts/evidence/sh/sha256_abcdef123456")
        );
    }

    #[test]
    fn snapshot_and_batch_paths_are_scope_partitioned() {
        let store = ArtifactStore::new("/tmp/ws-artifacts");
        let snapshot = store
            .snapshot_path("region:sol-1", "epoch-42")
            .expect("snapshot path");
        let batch = store
            .event_batch_path("global", "batch-7")
            .expect("batch path");

        assert_eq!(
            snapshot,
            PathBuf::from("/tmp/ws-artifacts/snapshots/region_sol-1/epoch-42.json")
        );
        assert_eq!(
            batch,
            PathBuf::from("/tmp/ws-artifacts/event-batches/global/batch-7.jsonl")
        );
    }

    #[test]
    fn traversal_markers_are_rejected() {
        let store = ArtifactStore::new("/tmp/ws-artifacts");
        assert!(store.checkpoint_path("..").is_err());
        assert!(store.snapshot_path(".", "snap-1").is_err());
    }

    #[test]
    fn write_and_read_json_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ArtifactStore::new(dir.path());
        store.ensure_layout().expect("layout");

        let path = store.checkpoint_path("cp-1").expect("checkpoint path");
        let manifest = SampleManifest {
            id: "cp-1".to_owned(),
            revision: 7,
        };
        store.write_json(&path, &manifest).expect("write json");

        let loaded: SampleManifest = store.read_json(&path).expect("read json");
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn validated_reference_bytes_roundtrip_and_repair_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ArtifactStore::new(dir.path());
        store.ensure_layout().expect("layout");

        let bytes = br#"{"hello":"artifact"}"#;
        let digest = format!("sha256:{}", sha256_hex(bytes));
        let path = store
            .write_validated_bytes(
                ArtifactKind::Reference,
                &digest,
                None,
                bytes,
                Some(&digest),
                Some(bytes.len() as u64),
            )
            .expect("write validated bytes");
        assert!(path.exists());

        let loaded = store
            .read_validated_bytes(
                ArtifactKind::Reference,
                &digest,
                None,
                Some(&digest),
                Some(bytes.len() as u64),
            )
            .expect("read validated bytes");
        assert_eq!(loaded, bytes);

        let manifest = ArtifactAvailabilityManifest {
            artifact_kind: ArtifactKind::Reference,
            artifact_id: digest.clone(),
            scope: None,
            source_uri: Some("ipfs://detail".to_owned()),
            expected_digest: Some(digest.clone()),
            mime: Some("application/json".to_owned()),
            size_bytes: Some(bytes.len() as u64),
            local_path: Some(path.display().to_string()),
            status: ArtifactAvailabilityStatus::Missing,
            observed_at: 10,
            last_checked_at: Some(20),
            missing_since: Some(20),
            repair_attempts: 1,
            next_retry_at: Some(30),
            last_error: Some("missing".to_owned()),
        };
        store
            .write_availability_manifest(&manifest)
            .expect("write availability");
        let pending = store
            .list_manifests_needing_repair(30)
            .expect("list pending repairs");
        assert_eq!(pending, vec![manifest]);
    }
}
