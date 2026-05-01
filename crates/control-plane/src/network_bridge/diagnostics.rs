use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use uuid::Uuid;

use crate::network_p2p::SwarmScope;

const DIAGNOSTIC_LOG_RELATIVE_PATH: &str = "diagnostics/wattswarm_node.jsonl";

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

#[derive(Debug, Clone, Default)]
pub struct DiagnosticFilter {
    pub limit: Option<usize>,
    pub level: Option<String>,
    pub component: Option<String>,
    pub category: Option<String>,
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
    let path = state_dir.join(DIAGNOSTIC_LOG_RELATIVE_PATH);
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
    for line in raw.lines().rev() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: DiagnosticEntry = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if matches_filter(&entry, filter) {
            entries.push(entry);
            if entries.len() >= limit {
                break;
            }
        }
    }
    Ok(entries)
}

fn matches_filter(entry: &DiagnosticEntry, filter: &DiagnosticFilter) -> bool {
    matches_text(&entry.level, filter.level.as_deref())
        && matches_text(&entry.component, filter.component.as_deref())
        && matches_text(&entry.category, filter.category.as_deref())
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
