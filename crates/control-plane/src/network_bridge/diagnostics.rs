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

const DIAGNOSTIC_LOG_RELATIVE_PATH: &str = "diagnostics/wattswarm_node.jsonl";
pub(super) const ENV_NETWORK_DEBUG_DIAGNOSTICS: &str = "WATTSWARM_NETWORK_DEBUG_DIAGNOSTICS";

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
    if is_recent_duplicate(&path, &entry)? {
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
    file.write_all(serde_json::to_string(&entry)?.as_bytes())?;
    file.write_all(b"\n")?;
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
            if seen.contains(&key) {
                continue;
            }
            seen.insert(key);
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
