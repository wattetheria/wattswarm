pub mod crypto {
    pub use wattswarm_crypto::*;
}

pub mod reason_codes {
    pub use wattswarm_protocol::reason_codes::*;
}

pub mod types {
    pub use wattswarm_protocol::types::*;
}

use crate::crypto::sha256_hex;
use crate::reason_codes::{
    REASON_CHECK_SUMMARY_MISSING, REASON_CONFIDENCE_TOO_LOW, REASON_EVIDENCE_AUTH_DENIED,
    REASON_EVIDENCE_DIGEST_MISMATCH, REASON_EVIDENCE_TIMEOUT, REASON_EVIDENCE_UNREACHABLE,
    REASON_OUTPUT_TOO_LARGE, REASON_SCHEMA_INVALID, REASON_SCHEMA_OK, REASON_SCORE_TOO_LOW,
};
use crate::types::{Candidate, PolicyBinding};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

const EVIDENCE_REASON_CODES: &[u16] = &[
    REASON_EVIDENCE_UNREACHABLE,
    REASON_EVIDENCE_TIMEOUT,
    REASON_EVIDENCE_AUTH_DENIED,
    REASON_EVIDENCE_DIGEST_MISMATCH,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicySpec {
    pub policy_id: String,
    pub policy_version: String,
    pub params_schema: Value,
    pub reason_codes: Vec<u16>,
    pub io_contract: String,
}

#[derive(Debug, Clone)]
pub struct PolicyResult {
    pub passed: bool,
    pub score: f64,
    pub reason_codes: Vec<u16>,
}

pub trait Policy: Send + Sync {
    fn id(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn spec(&self) -> PolicySpec;
    fn evaluate(
        &self,
        candidate: &Candidate,
        output_schema: &Value,
        params: &Value,
    ) -> PolicyResult;

    fn hash(&self) -> String {
        let spec = self.spec();
        let bytes = serde_json::to_vec(&spec).expect("policy spec to json");
        sha256_hex(&bytes)
    }
}

pub struct PolicyRegistry {
    policies: HashMap<String, Box<dyn Policy>>,
    compatible_hashes: HashMap<(String, String), HashSet<String>>,
}

impl PolicyRegistry {
    pub fn with_builtin() -> Self {
        let mut reg = Self {
            policies: HashMap::new(),
            compatible_hashes: HashMap::new(),
        };
        reg.register(Box::new(SchemaOnlyV1));
        reg.register(Box::new(SchemaThresholdsV1));
        reg.register(Box::new(CrossCheckV1));
        reg
    }

    pub fn register(&mut self, policy: Box<dyn Policy>) {
        let policy_id = policy.id().to_owned();
        let policy_version = policy.version().to_owned();
        let policy_hash = policy.hash();
        self.policies.insert(policy_id.clone(), policy);
        self.compatible_hashes
            .entry((policy_id, policy_version))
            .or_default()
            .insert(policy_hash);
    }

    pub fn get(&self, policy_id: &str) -> Option<&dyn Policy> {
        self.policies.get(policy_id).map(|b| b.as_ref())
    }

    pub fn allow_compatible_hash(
        &mut self,
        policy_id: &str,
        policy_version: &str,
        policy_hash: &str,
    ) {
        self.compatible_hashes
            .entry((policy_id.to_owned(), policy_version.to_owned()))
            .or_default()
            .insert(policy_hash.to_owned());
    }

    pub fn require_binding(&self, binding: &PolicyBinding) -> Result<&dyn Policy> {
        let policy = self
            .get(&binding.policy_id)
            .ok_or_else(|| anyhow!("policy {} missing in registry", binding.policy_id))?;
        if policy.version() != binding.policy_version {
            return Err(anyhow!("policy version mismatch"));
        }
        let accepted = self
            .compatible_hashes
            .get(&(binding.policy_id.clone(), binding.policy_version.clone()))
            .map(|allowed| allowed.contains(&binding.policy_hash))
            .unwrap_or(false);
        if !accepted {
            return Err(anyhow!("policy hash mismatch"));
        }
        Ok(policy)
    }

    pub fn binding_for(&self, policy_id: &str, params: Value) -> Result<PolicyBinding> {
        let policy = self
            .get(policy_id)
            .ok_or_else(|| anyhow!("policy {} missing", policy_id))?;
        Ok(PolicyBinding {
            policy_id: policy.id().to_owned(),
            policy_version: policy.version().to_owned(),
            policy_hash: policy.hash(),
            policy_params: params,
        })
    }
}

fn with_evidence_reason_codes(base: Vec<u16>) -> Vec<u16> {
    let mut merged = base;
    for code in EVIDENCE_REASON_CODES {
        if !merged.iter().any(|existing| existing == code) {
            merged.push((*code).to_owned());
        }
    }
    merged
}

pub fn validate_schema_minimal(schema: &Value, value: &Value) -> Result<()> {
    let schema_obj = schema
        .as_object()
        .ok_or_else(|| anyhow!("output_schema must be a JSON object"))?;
    if let Some(type_name) = schema_obj.get("type").and_then(Value::as_str)
        && !matches_type(type_name, value)
    {
        return Err(anyhow!("schema type mismatch: expected {type_name}"));
    }

    if let Some(required) = schema_obj.get("required").and_then(Value::as_array) {
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("required fields need object output"))?;
        for key in required.iter().filter_map(Value::as_str) {
            if !obj.contains_key(key) {
                return Err(anyhow!("missing required key: {key}"));
            }
        }
    }

    if let Some(properties) = schema_obj.get("properties").and_then(Value::as_object)
        && let Some(obj) = value.as_object()
    {
        for (k, v_schema) in properties {
            if let Some(v) = obj.get(k)
                && let Some(t) = v_schema.get("type").and_then(Value::as_str)
                && !matches_type(t, v)
            {
                return Err(anyhow!("property type mismatch for key {k}"));
            }
        }
    }

    Ok(())
}

fn matches_type(type_name: &str, value: &Value) -> bool {
    match type_name {
        "object" => value.is_object(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => false,
    }
}

fn confidence_from(candidate: &Candidate) -> Option<f64> {
    candidate
        .output
        .as_object()
        .and_then(|m| m.get("confidence"))
        .and_then(Value::as_f64)
}

fn get_number(params: &Map<String, Value>, key: &str) -> Option<f64> {
    params.get(key).and_then(Value::as_f64)
}

fn get_u64(params: &Map<String, Value>, key: &str) -> Option<u64> {
    params.get(key).and_then(Value::as_u64)
}

pub struct SchemaOnlyV1;

impl Policy for SchemaOnlyV1 {
    fn id(&self) -> &'static str {
        "vp.schema_only.v1"
    }

    fn version(&self) -> &'static str {
        "1"
    }

    fn spec(&self) -> PolicySpec {
        PolicySpec {
            policy_id: self.id().to_owned(),
            policy_version: self.version().to_owned(),
            params_schema: serde_json::json!({"type":"object","properties":{}}),
            reason_codes: with_evidence_reason_codes(vec![REASON_SCHEMA_OK, REASON_SCHEMA_INVALID]),
            io_contract: "validate candidate output against output_schema".to_owned(),
        }
    }

    fn evaluate(
        &self,
        candidate: &Candidate,
        output_schema: &Value,
        _params: &Value,
    ) -> PolicyResult {
        match validate_schema_minimal(output_schema, &candidate.output) {
            Ok(()) => PolicyResult {
                passed: true,
                score: 1.0,
                reason_codes: vec![REASON_SCHEMA_OK],
            },
            Err(_) => PolicyResult {
                passed: false,
                score: 0.0,
                reason_codes: vec![REASON_SCHEMA_INVALID],
            },
        }
    }
}

pub struct SchemaThresholdsV1;

impl Policy for SchemaThresholdsV1 {
    fn id(&self) -> &'static str {
        "vp.schema_thresholds.v1"
    }

    fn version(&self) -> &'static str {
        "1"
    }

    fn spec(&self) -> PolicySpec {
        PolicySpec {
            policy_id: self.id().to_owned(),
            policy_version: self.version().to_owned(),
            params_schema: serde_json::json!({
              "type":"object",
              "properties":{"min_confidence":{"type":"number"},"max_len":{"type":"integer"}}
            }),
            reason_codes: with_evidence_reason_codes(vec![
                REASON_SCHEMA_OK,
                REASON_SCHEMA_INVALID,
                REASON_CONFIDENCE_TOO_LOW,
                REASON_OUTPUT_TOO_LARGE,
            ]),
            io_contract: "schema + generic thresholds".to_owned(),
        }
    }

    fn evaluate(
        &self,
        candidate: &Candidate,
        output_schema: &Value,
        params: &Value,
    ) -> PolicyResult {
        let mut reason_codes = Vec::new();
        let mut passed = true;

        if validate_schema_minimal(output_schema, &candidate.output).is_err() {
            passed = false;
            reason_codes.push(REASON_SCHEMA_INVALID);
        } else {
            reason_codes.push(REASON_SCHEMA_OK);
        }

        if let Some(params_obj) = params.as_object() {
            if let Some(min_confidence) = get_number(params_obj, "min_confidence")
                && confidence_from(candidate).unwrap_or(0.0) < min_confidence
            {
                passed = false;
                reason_codes.push(REASON_CONFIDENCE_TOO_LOW);
            }

            if let Some(max_len) = get_u64(params_obj, "max_len") {
                let output_len =
                    serde_json::to_vec(&candidate.output).map_or(u64::MAX, |v| v.len() as u64);
                if output_len > max_len {
                    passed = false;
                    reason_codes.push(REASON_OUTPUT_TOO_LARGE);
                }
            }
        }

        let score = if passed { 1.0 } else { 0.2 };
        PolicyResult {
            passed,
            score,
            reason_codes,
        }
    }
}

pub struct CrossCheckV1;

impl Policy for CrossCheckV1 {
    fn id(&self) -> &'static str {
        "vp.crosscheck.v1"
    }

    fn version(&self) -> &'static str {
        "1"
    }

    fn spec(&self) -> PolicySpec {
        PolicySpec {
            policy_id: self.id().to_owned(),
            policy_version: self.version().to_owned(),
            params_schema: serde_json::json!({
              "type":"object",
              "properties":{"min_score":{"type":"number"}}
            }),
            reason_codes: with_evidence_reason_codes(vec![
                REASON_SCHEMA_OK,
                REASON_SCHEMA_INVALID,
                REASON_CHECK_SUMMARY_MISSING,
                REASON_SCORE_TOO_LOW,
            ]),
            io_contract: "cross-check output and enforce check_summary".to_owned(),
        }
    }

    fn evaluate(
        &self,
        candidate: &Candidate,
        output_schema: &Value,
        params: &Value,
    ) -> PolicyResult {
        let mut reason_codes = Vec::new();
        let mut passed = true;

        if validate_schema_minimal(output_schema, &candidate.output).is_err() {
            passed = false;
            reason_codes.push(REASON_SCHEMA_INVALID);
        } else {
            reason_codes.push(REASON_SCHEMA_OK);
        }

        let check_summary_ok = candidate
            .output
            .as_object()
            .and_then(|o| o.get("check_summary"))
            .and_then(Value::as_str)
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);

        if !check_summary_ok {
            passed = false;
            reason_codes.push(REASON_CHECK_SUMMARY_MISSING);
        }

        let score = if passed { 0.9 } else { 0.3 };
        if let Some(min_score) = params
            .as_object()
            .and_then(|o| o.get("min_score"))
            .and_then(Value::as_f64)
            && score < min_score
        {
            passed = false;
            reason_codes.push(REASON_SCORE_TOO_LOW);
        }

        PolicyResult {
            passed,
            score,
            reason_codes,
        }
    }
}
