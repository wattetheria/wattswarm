use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use uuid::Uuid;

use crate::network_p2p::SwarmScope;
pub(super) use crate::network_p2p::diagnostic_error_is_timeout;

const DIAGNOSTIC_LOG_RELATIVE_PATH: &str = "diagnostics/wattswarm_node.jsonl";
pub(super) const ENV_NETWORK_DEBUG_DIAGNOSTICS: &str = "WATTSWARM_NETWORK_DEBUG_DIAGNOSTICS";

const ANOMALY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
pub(super) const RUNTIME_DIAGNOSTIC_QUEUE_CAPACITY: usize = 256;

#[derive(Serialize, Deserialize)]
struct PublishCursorRecord {
    last_published_seq: u64,
    updated_at_ms: u64,
    clean_shutdown: bool,
}

pub(super) struct PublishCursorMarker {
    path: std::path::PathBuf,
    pub(super) marker_seq: Option<u64>,
    pub(super) marker_updated_at_ms: Option<u64>,
    pub(super) previous_clean_shutdown: Option<bool>,
    last_published_seq: u64,
    saved_seq: Option<u64>,
    last_write: Option<std::time::Instant>,
}

impl PublishCursorMarker {
    pub(super) fn new(state_dir: &Path, head_seq: u64) -> Self {
        let path = state_dir.join("diagnostics/publish_cursor.json");
        let record = fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<PublishCursorRecord>(&raw).ok());
        let marker_seq = record.as_ref().map(|record| record.last_published_seq);
        Self {
            path,
            marker_seq,
            marker_updated_at_ms: record.as_ref().map(|record| record.updated_at_ms),
            previous_clean_shutdown: record.as_ref().map(|record| record.clean_shutdown),
            last_published_seq: head_seq,
            saved_seq: marker_seq,
            last_write: None,
        }
    }

    pub(super) fn advance(&mut self, seq: u64, now: std::time::Instant) {
        if seq > self.last_published_seq {
            self.last_published_seq = seq;
            self.persist(now, false);
        }
    }

    fn persist(&mut self, now: std::time::Instant, clean_shutdown: bool) {
        if !clean_shutdown
            && (self.saved_seq == Some(self.last_published_seq)
                || self.last_write.is_some_and(|last| {
                    now.duration_since(last) < std::time::Duration::from_secs(5)
                }))
        {
            return;
        }
        self.last_write = Some(now);
        let result = (|| -> Result<()> {
            fs::create_dir_all(self.path.parent().expect("diagnostics marker parent"))?;
            fs::write(
                &self.path,
                serde_json::to_vec(&PublishCursorRecord {
                    last_published_seq: self.last_published_seq,
                    updated_at_ms: super::observed_at_ms(),
                    clean_shutdown,
                })?,
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => self.saved_seq = Some(self.last_published_seq),
            Err(error) => eprintln!("persist diagnostic publish cursor failed: {error:#}"),
        }
    }
}

impl Drop for PublishCursorMarker {
    fn drop(&mut self) {
        self.persist(std::time::Instant::now(), true);
    }
}

#[derive(Default)]
struct AnomalyLimiter {
    recent: std::collections::HashMap<String, (std::time::Instant, u64)>,
}

static ANOMALY_LIMITER: std::sync::OnceLock<std::sync::Mutex<AnomalyLimiter>> =
    std::sync::OnceLock::new();

impl AnomalyLimiter {
    fn allow(&mut self, key: String, now: std::time::Instant) -> Option<u64> {
        if let Some((last, suppressed)) = self.recent.get_mut(&key) {
            if now.duration_since(*last) < ANOMALY_INTERVAL {
                *suppressed = suppressed.saturating_add(1);
                return None;
            }
            *last = now;
            return Some(std::mem::take(suppressed));
        }
        if self.recent.len() >= 4096 {
            self.recent
                .retain(|_, (last, _)| now.duration_since(*last) < ANOMALY_INTERVAL);
            if self.recent.len() >= 4096 {
                return None;
            }
        }
        self.recent.insert(key, (now, 0));
        Some(0)
    }
}

fn record_monitoring(
    state_dir: Option<&Path>,
    build_event: impl FnOnce() -> DiagnosticEvent,
    anomaly: bool,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    let Some(state_dir) = state_dir else {
        return;
    };
    let mut event = build_event();
    if let Some(envelope) = event
        .details
        .as_object_mut()
        .and_then(|details| details.remove("agent_envelope"))
        && let Some(message) = envelope.get("message_json").and_then(Value::as_str)
        && let Ok(message) = serde_json::from_str::<Value>(message)
    {
        for key in ["request_id", "message_id"] {
            if let Some(value) = message.get(key).filter(|value| value.is_string()) {
                event.details[format!("agent_{key}")] = value.clone();
            }
        }
    }
    if anomaly {
        let key = format!(
            "{}|{}|{}|{:?}|{:?}|{}|{}",
            state_dir.display(),
            event.phase,
            event.status,
            event.source_node_id,
            event.scope_hint,
            event.details.get("kind").unwrap_or(&Value::Null),
            event.details.get("reason").unwrap_or(&Value::Null)
        );
        let suppressed_count = ANOMALY_LIMITER
            .get_or_init(Default::default)
            .lock()
            .ok()
            .and_then(|mut limiter| limiter.allow(key, std::time::Instant::now()));
        let Some(suppressed_count) = suppressed_count else {
            return;
        };
        event.details["suppressed_count"] = json!(suppressed_count);
    }
    event.details["monitoring"] = json!(true);
    if let Err(err) = append_diagnostic_entry(state_dir, event, false) {
        eprintln!("append Wattswarm monitoring diagnostic failed: {err:#}");
    }
}

pub(super) fn record_anomaly(
    state_dir: Option<&Path>,
    enabled: bool,
    build_event: impl FnOnce() -> DiagnosticEvent,
) {
    record_monitoring(state_dir, build_event, true, enabled);
}

pub(super) fn record_debug(
    state_dir: Option<&Path>,
    enabled: bool,
    build_event: impl FnOnce() -> DiagnosticEvent,
) {
    record_monitoring(
        state_dir,
        || {
            let mut event = build_event();
            event.level = "debug";
            event
        },
        false,
        enabled,
    );
}

pub(super) fn record_runtime_diagnostic(
    state_dir: &Path,
    enabled: bool,
    observation: crate::network_p2p::RuntimeDiagnostic,
) {
    if !enabled {
        return;
    }
    let mut event = DiagnosticEvent::new(
        if observation.anomaly { "warn" } else { "debug" },
        "runtime",
        observation.phase,
        observation.status,
        "network runtime observation",
    )
    .source_node_id(observation.peer)
    .details(observation.details);
    event.event_id = event
        .details
        .get("event_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(scope) = event
        .details
        .get("scope")
        .and_then(|scope| serde_json::from_value::<SwarmScope>(scope.clone()).ok())
    {
        event = event.scope(&scope);
    }
    if observation.anomaly {
        record_anomaly(Some(state_dir), enabled, || event);
    } else {
        record_debug(Some(state_dir), enabled, || event);
    }
}

#[cfg(test)]
mod monitoring_tests {
    use super::*;

    fn state_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("wattswarm-monitoring-{}", Uuid::new_v4()))
    }

    fn observation(level: &'static str) -> DiagnosticEvent {
        DiagnosticEvent::new(
            level,
            "runtime",
            "control.outbound",
            "timeout",
            "request observation",
        )
        .event_id("event-1")
        .source_node_id(Some("peer-1".to_owned()))
        .scope(&SwarmScope::Global)
        .details(json!({"request_id": "request-1", "message_id": "message-1", "kind": "test.v1"}))
    }

    #[test]
    fn monitoring_switch_off_skips_all_records_and_metadata_builders() {
        let dir = state_dir();
        record_debug(Some(&dir), false, || {
            panic!("debug metadata built with switch off")
        });
        record_anomaly(Some(&dir), false, || {
            panic!("anomaly metadata built with switch off")
        });
        assert!(!dir.exists());
        record_monitoring(Some(&dir), || observation("debug"), false, false);
        assert!(!dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH).exists());
        record_monitoring(Some(&dir), || observation("warn"), true, true);
        record_monitoring(Some(&dir), || observation("debug"), false, true);
        let entries = list_diagnostics(&dir, &DiagnosticFilter::default()).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| entry.level == "warn"));
        assert!(entries.iter().any(|entry| entry.level == "debug"));
        for entry in entries {
            assert_eq!(entry.event_id.as_deref(), Some("event-1"));
            assert_eq!(entry.source_node_id.as_deref(), Some("peer-1"));
            assert_eq!(entry.scope_hint.as_deref(), Some("global"));
            assert_eq!(entry.details["request_id"], "request-1");
            assert_eq!(entry.details["message_id"], "message-1");
            assert_eq!(entry.details["kind"], "test.v1");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn monitoring_append_skips_dedupe_and_keeps_clean_repeated_messages() {
        let dir = state_dir();
        fs::create_dir_all(dir.join("diagnostics")).unwrap();
        fs::write(dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH), [0xff, b'\n']).unwrap();
        let event = observation("info");
        assert!(append_diagnostic(&dir, event).is_err());
        append_diagnostic_entry(&dir, observation("info"), false).unwrap();
        fs::remove_file(dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH)).unwrap();
        record_monitoring(Some(&dir), || observation("debug"), false, true);
        record_monitoring(Some(&dir), || observation("debug"), false, true);
        let entries = list_diagnostics(&dir, &DiagnosticFilter::default()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_ne!(entries[0].id, entries[1].id);
        assert_eq!(entries[0].message, "request observation");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn monitoring_log_rotates_to_one_backup_and_lists_current_file() {
        assert_eq!(MAX_DIAGNOSTIC_LOG_BYTES, 5 * 1024 * 1024);
        let dir = state_dir();
        fs::create_dir_all(dir.join("diagnostics")).unwrap();
        for _ in 0..2 {
            let file = fs::File::create(dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH)).unwrap();
            file.set_len(MAX_DIAGNOSTIC_LOG_BYTES + 1).unwrap();
            record_monitoring(Some(&dir), || observation("debug"), false, true);
            assert_eq!(
                fs::metadata(dir.join("diagnostics/wattswarm_node.jsonl.1"))
                    .unwrap()
                    .len(),
                MAX_DIAGNOSTIC_LOG_BYTES + 1
            );
            assert_eq!(
                list_diagnostics(&dir, &DiagnosticFilter::default())
                    .unwrap()
                    .len(),
                1
            );
        }
        assert_eq!(fs::read_dir(dir.join("diagnostics")).unwrap().count(), 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn monitoring_off_leaves_legacy_append_without_rotation_work() {
        let dir = state_dir();
        fs::create_dir_all(dir.join("diagnostics")).unwrap();
        let file = fs::File::create(dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH)).unwrap();
        file.set_len(MAX_DIAGNOSTIC_LOG_BYTES + 1).unwrap();
        append_diagnostic(&dir, observation("info")).unwrap();
        assert!(!dir.join("diagnostics/wattswarm_node.jsonl.1").exists());
        assert!(
            fs::metadata(dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH))
                .unwrap()
                .len()
                > MAX_DIAGNOSTIC_LOG_BYTES
        );
        record_monitoring(Some(&dir), || observation("debug"), false, true);
        assert!(dir.join("diagnostics/wattswarm_node.jsonl.1").exists());
        assert_eq!(
            list_diagnostics(&dir, &DiagnosticFilter::default())
                .unwrap()
                .len(),
            1
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn monitoring_cursor_marker_throttles_and_flushes_on_exit() {
        let dir = state_dir();
        let now = std::time::Instant::now();
        let mut marker = PublishCursorMarker::new(&dir, 10);
        assert_eq!(marker.marker_seq, None);
        assert_eq!(marker.marker_updated_at_ms, None);
        assert_eq!(marker.previous_clean_shutdown, None);
        let read_marker = || -> PublishCursorRecord {
            serde_json::from_slice(&fs::read(dir.join("diagnostics/publish_cursor.json")).unwrap())
                .unwrap()
        };
        marker.advance(11, now);
        assert!(!read_marker().clean_shutdown);
        marker.advance(12, now + std::time::Duration::from_secs(1));
        assert_eq!(read_marker().last_published_seq, 11);
        assert!(!read_marker().clean_shutdown);
        marker.advance(13, now + std::time::Duration::from_secs(5));
        assert_eq!(read_marker().last_published_seq, 13);
        marker.advance(14, now + std::time::Duration::from_secs(6));
        drop(marker);
        let record = read_marker();
        assert_eq!(record.last_published_seq, 14);
        assert!(record.clean_shutdown);
        assert!(record.updated_at_ms > 0);
        let marker = PublishCursorMarker::new(&dir, 14);
        assert_eq!(marker.marker_updated_at_ms, Some(record.updated_at_ms));
        assert_eq!(marker.previous_clean_shutdown, Some(true));
        drop(marker);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn stuck_peer_is_throttled_even_when_request_ids_change() {
        let dir = state_dir();
        record_anomaly(Some(&dir), true, || observation("warn"));
        record_anomaly(Some(&dir), true, || observation("warn").event_id("event-2"));
        record_anomaly(Some(&dir), true, || observation("warn").event_id("event-3"));
        assert_eq!(
            list_diagnostics(&dir, &DiagnosticFilter::default())
                .unwrap()
                .len(),
            1
        );
        {
            let mut limiter = ANOMALY_LIMITER.get().unwrap().lock().unwrap();
            let key_prefix = format!("{}|", dir.display());
            let (_, (last, _)) = limiter
                .recent
                .iter_mut()
                .find(|(key, _)| key.starts_with(&key_prefix))
                .unwrap();
            *last -= ANOMALY_INTERVAL;
        }
        record_anomaly(Some(&dir), true, || observation("warn").event_id("event-4"));
        let entries = list_diagnostics(&dir, &DiagnosticFilter::default()).unwrap();
        assert_eq!(entries[0].details["suppressed_count"], 2);
        assert_eq!(entries[0].event_id.as_deref(), Some("event-4"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn anomaly_limiter_renews_and_is_bounded() {
        let mut limiter = AnomalyLimiter::default();
        let now = std::time::Instant::now();
        assert_eq!(limiter.allow("peer-1".to_owned(), now), Some(0));
        assert_eq!(limiter.allow("peer-1".to_owned(), now), None);
        assert_eq!(limiter.allow("peer-1".to_owned(), now), None);
        assert_eq!(
            limiter.allow("peer-1".to_owned(), now + ANOMALY_INTERVAL),
            Some(2)
        );
        assert_eq!(
            limiter.allow("peer-1".to_owned(), now + ANOMALY_INTERVAL * 2),
            Some(0)
        );
        for index in 0..4095 {
            assert_eq!(
                limiter.allow(index.to_string(), now + ANOMALY_INTERVAL * 2),
                Some(0)
            );
        }
        assert_eq!(
            limiter.allow("overflow".to_owned(), now + ANOMALY_INTERVAL * 2),
            None
        );
        assert_eq!(limiter.recent.len(), 4096);
        assert_eq!(
            limiter.allow("new-window".to_owned(), now + ANOMALY_INTERVAL * 3),
            Some(0)
        );
    }

    #[test]
    fn runtime_anomaly_preserves_correlation_without_payload() {
        let dir = state_dir();
        record_runtime_diagnostic(
            &dir,
            true,
            crate::network_p2p::RuntimeDiagnostic {
                phase: "gossip.publish",
                status: "zero_neighbors",
                anomaly: true,
                peer: Some("peer-1".to_owned()),
                details: json!({"event_id": "event-1", "message_id": "message-1", "topic": "topic-1", "scope": "global", "kind": "messages", "neighbor_count": 0}),
            },
        );
        let entry = list_diagnostics(&dir, &DiagnosticFilter::default())
            .unwrap()
            .remove(0);
        assert_eq!(entry.event_id.as_deref(), Some("event-1"));
        assert_eq!(entry.source_node_id.as_deref(), Some("peer-1"));
        assert_eq!(entry.details["topic"], "topic-1");
        assert_eq!(entry.details["neighbor_count"], 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn legacy_envelopes_are_stripped_by_the_new_wrapper() {
        let dir = state_dir();
        record_monitoring(
            Some(&dir),
            || {
                DiagnosticEvent::new("debug", "gossip", "publish.summary", "ok", "summary")
            .object("summary", Some("summary-1".to_owned())).details(json!({
                "agent_envelope": {"message_json": json!({"request_id": "request-1", "message_id": "message-1", "text": "private text"}).to_string(), "signature": "secret"},
                "summary_id": "summary-1",
            }))
            },
            false,
            true,
        );
        let entries = list_diagnostics(&dir, &DiagnosticFilter::default()).unwrap();
        assert_eq!(entries[0].object_id.as_deref(), Some("summary-1"));
        assert_eq!(entries[0].details["agent_request_id"], "request-1");
        assert_eq!(entries[0].details["agent_message_id"], "message-1");
        let raw = serde_json::to_string(&entries[0]).unwrap();
        assert!(!raw.contains("private text"));
        assert!(!raw.contains("secret"));
        assert!(!raw.contains("agent_envelope"));
        fs::remove_dir_all(dir).unwrap();
    }
}

pub(super) fn debug_diagnostics_enabled() -> bool {
    env::var(ENV_NETWORK_DEBUG_DIAGNOSTICS)
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "on" | "ON"))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiagnosticEntry {
    pub id: String,
    pub timestamp_ms: u64,
    pub level: String,
    pub component: String,
    pub category: String,
    pub phase: String,
    pub status: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_hint: Option<String>,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DiagnosticDedupeKey {
    level: String,
    component: String,
    category: String,
    phase: String,
    status: String,
    message: String,
    event_id: Option<String>,
    object_kind: Option<String>,
    object_id: Option<String>,
    source_node_id: Option<String>,
    scope_hint: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DiagnosticFilter {
    pub limit: Option<usize>,
    pub level: Option<String>,
    pub component: Option<String>,
    pub category: Option<String>,
    pub mode: Option<String>,
    pub phase: Option<String>,
    pub event_id: Option<String>,
    pub object_id: Option<String>,
    pub source_node_id: Option<String>,
    pub search: Option<String>,
}

pub(super) fn record_diagnostic(state_dir: Option<&Path>, event: DiagnosticEvent) {
    let Some(state_dir) = state_dir else {
        return;
    };
    if let Err(error) = append_diagnostic(state_dir, event) {
        eprintln!("wattswarm diagnostic append failed: {error:#}");
    }
}

#[derive(Debug)]
pub(super) struct DiagnosticEvent {
    level: &'static str,
    category: &'static str,
    phase: &'static str,
    status: &'static str,
    message: String,
    event_id: Option<String>,
    object_kind: Option<&'static str>,
    object_id: Option<String>,
    source_node_id: Option<String>,
    scope_hint: Option<String>,
    details: Value,
}

impl DiagnosticEvent {
    pub(super) fn new(
        level: &'static str,
        category: &'static str,
        phase: &'static str,
        status: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            level,
            category,
            phase,
            status,
            message: message.into(),
            event_id: None,
            object_kind: None,
            object_id: None,
            source_node_id: None,
            scope_hint: None,
            details: json!({}),
        }
    }

    pub(super) fn event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = Some(event_id.into());
        self
    }

    pub(super) fn object(mut self, object_kind: &'static str, object_id: Option<String>) -> Self {
        self.object_kind = Some(object_kind);
        self.object_id = object_id;
        self
    }

    pub(super) fn source_node_id(mut self, source_node_id: Option<String>) -> Self {
        self.source_node_id = source_node_id;
        self
    }

    pub(super) fn scope(mut self, scope: &SwarmScope) -> Self {
        self.scope_hint = Some(super::scope_hint_label(scope));
        self
    }

    pub(super) fn details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

fn append_diagnostic(state_dir: &Path, event: DiagnosticEvent) -> Result<()> {
    append_diagnostic_entry(state_dir, event, true)
}

const MAX_DIAGNOSTIC_LOG_BYTES: u64 = 5 * 1024 * 1024;

fn append_diagnostic_entry(state_dir: &Path, event: DiagnosticEvent, dedupe: bool) -> Result<()> {
    static APPEND_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = APPEND_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("diagnostic append lock poisoned"))?;
    let path = state_dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH);
    let entry = DiagnosticEntry {
        id: Uuid::new_v4().to_string(),
        timestamp_ms: super::observed_at_ms(),
        level: event.level.to_owned(),
        component: "wattswarm.network_bridge".to_owned(),
        category: event.category.to_owned(),
        phase: event.phase.to_owned(),
        status: event.status.to_owned(),
        message: event.message,
        event_id: event.event_id,
        object_kind: event.object_kind.map(ToOwned::to_owned),
        object_id: event.object_id,
        source_node_id: event.source_node_id,
        scope_hint: event.scope_hint,
        details: event.details,
    };
    if !dedupe
        && fs::metadata(&path).is_ok_and(|metadata| metadata.len() > MAX_DIAGNOSTIC_LOG_BYTES)
    {
        fs::rename(&path, state_dir.join("diagnostics/wattswarm_node.jsonl.1"))
            .context("rotate Wattswarm diagnostics log")?;
    }
    if dedupe && is_recent_duplicate(&path, &entry)? {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("create Wattswarm diagnostics directory")?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open Wattswarm diagnostics log {}", path.display()))?;
    let mut line = serde_json::to_vec(&entry)?;
    line.push(b'\n');
    file.write_all(&line)?;
    Ok(())
}

fn is_recent_duplicate(path: &Path, entry: &DiagnosticEntry) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if entry.phase.starts_with("startup.") {
        return Ok(false);
    }
    let key = diagnostic_dedupe_key(entry);
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read Wattswarm diagnostics log {}", path.display()))?;
    for line in raw.lines().rev().filter(|line| !line.trim().is_empty()) {
        let existing: DiagnosticEntry = match serde_json::from_str(line) {
            Ok(existing) => existing,
            Err(_) => continue,
        };
        if diagnostic_dedupe_key(&existing) == key {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn list_diagnostics(
    state_dir: &Path,
    filter: &DiagnosticFilter,
) -> Result<Vec<DiagnosticEntry>> {
    let path = state_dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let limit = filter.limit.unwrap_or(100).clamp(1, 1_000);
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("read Wattswarm diagnostics log {}", path.display()))?;
    let mut entries = Vec::with_capacity(limit);
    let mut seen: HashSet<DiagnosticDedupeKey> = HashSet::new();
    for line in raw.lines().rev() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: DiagnosticEntry = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if matches_filter(&entry, filter) {
            let key = diagnostic_dedupe_key(&entry);
            if entry.details.get("monitoring") != Some(&Value::Bool(true)) {
                if seen.contains(&key) {
                    continue;
                }
                seen.insert(key);
            }
            entries.push(entry);
            if entries.len() >= limit {
                break;
            }
        }
    }
    Ok(entries)
}

fn diagnostic_dedupe_key(entry: &DiagnosticEntry) -> DiagnosticDedupeKey {
    DiagnosticDedupeKey {
        level: entry.level.clone(),
        component: entry.component.clone(),
        category: entry.category.clone(),
        phase: entry.phase.clone(),
        status: entry.status.clone(),
        message: entry.message.clone(),
        event_id: entry.event_id.clone(),
        object_kind: entry.object_kind.clone(),
        object_id: entry.object_id.clone(),
        source_node_id: entry.source_node_id.clone(),
        scope_hint: entry.scope_hint.clone(),
    }
}

fn matches_filter(entry: &DiagnosticEntry, filter: &DiagnosticFilter) -> bool {
    matches_text(&entry.level, filter.level.as_deref())
        && matches_text(&entry.component, filter.component.as_deref())
        && matches_text(&entry.category, filter.category.as_deref())
        && matches_mode(entry, filter.mode.as_deref())
        && matches_text(&entry.phase, filter.phase.as_deref())
        && matches_optional(entry.event_id.as_deref(), filter.event_id.as_deref())
        && matches_optional(entry.object_id.as_deref(), filter.object_id.as_deref())
        && matches_optional(
            entry.source_node_id.as_deref(),
            filter.source_node_id.as_deref(),
        )
        && matches_search(entry, filter.search.as_deref())
}

fn matches_text(value: &str, expected: Option<&str>) -> bool {
    expected
        .map(str::trim)
        .filter(|expected| !expected.is_empty())
        .is_none_or(|expected| value.eq_ignore_ascii_case(expected))
}

fn matches_mode(entry: &DiagnosticEntry, mode: Option<&str>) -> bool {
    let Some(mode) = mode.map(str::trim).filter(|mode| !mode.is_empty()) else {
        return true;
    };
    match mode.to_lowercase().as_str() {
        "all" => true,
        "errors" => diagnostic_is_error(entry),
        "transport" => entry.category.eq_ignore_ascii_case("transport"),
        "gossip" => entry.category.eq_ignore_ascii_case("gossip"),
        "agent-events" => diagnostic_is_agent_event(entry),
        "backfill" => diagnostic_text(entry).contains("backfill") && !diagnostic_is_error(entry),
        "callback" => diagnostic_text(entry).contains("callback") && !diagnostic_is_error(entry),
        other => diagnostic_text(entry).contains(other),
    }
}

fn diagnostic_is_error(entry: &DiagnosticEntry) -> bool {
    let text = format!("{} {}", entry.level, entry.status).to_lowercase();
    text.contains("error") || text.contains("fail") || text.contains("warn")
}

fn diagnostic_is_agent_event(entry: &DiagnosticEntry) -> bool {
    entry.category.eq_ignore_ascii_case("agent_event")
        || entry
            .object_kind
            .as_deref()
            .is_some_and(|kind| kind.eq_ignore_ascii_case("agent_event"))
}

fn diagnostic_text(entry: &DiagnosticEntry) -> String {
    let details = entry.details.as_object();
    [
        Some(entry.category.as_str()),
        Some(entry.phase.as_str()),
        Some(entry.component.as_str()),
        entry.object_kind.as_deref(),
        Some(entry.message.as_str()),
        details.and_then(|details| details.get("event_type").and_then(Value::as_str)),
        details.and_then(|details| details.get("feed_key").and_then(Value::as_str)),
        details.and_then(|details| details.get("payload_kind").and_then(Value::as_str)),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ")
    .to_lowercase()
}

fn matches_optional(value: Option<&str>, expected: Option<&str>) -> bool {
    expected
        .map(str::trim)
        .filter(|expected| !expected.is_empty())
        .is_none_or(|expected| value.is_some_and(|value| value.contains(expected)))
}

fn matches_search(entry: &DiagnosticEntry, search: Option<&str>) -> bool {
    let Some(search) = search.map(str::trim).filter(|search| !search.is_empty()) else {
        return true;
    };
    let haystack = serde_json::to_string(entry)
        .unwrap_or_default()
        .to_lowercase();
    haystack.contains(&search.to_lowercase())
}
