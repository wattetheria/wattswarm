use anyhow::{Result, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    Evidence,
    Checkpoint,
    Snapshot,
    EventBatch,
}

impl ArtifactKind {
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Evidence => "evidence",
            Self::Checkpoint => "checkpoints",
            Self::Snapshot => "snapshots",
            Self::EventBatch => "event-batches",
        }
    }
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
            ArtifactKind::Evidence,
            ArtifactKind::Checkpoint,
            ArtifactKind::Snapshot,
            ArtifactKind::EventBatch,
        ] {
            fs::create_dir_all(self.kind_root(kind))?;
        }
        Ok(())
    }

    pub fn kind_root(&self, kind: ArtifactKind) -> PathBuf {
        self.root.join(kind.dir_name())
    }

    pub fn evidence_path(&self, digest: &str) -> Result<PathBuf> {
        let cleaned = sanitize_component(digest)?;
        let shard = cleaned.get(0..2).unwrap_or("00");
        Ok(self
            .kind_root(ArtifactKind::Evidence)
            .join(shard)
            .join(cleaned))
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
            ArtifactKind::Evidence,
            ArtifactKind::Checkpoint,
            ArtifactKind::Snapshot,
            ArtifactKind::EventBatch,
        ] {
            assert!(store.kind_root(kind).exists(), "missing {:?}", kind);
        }
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
}
