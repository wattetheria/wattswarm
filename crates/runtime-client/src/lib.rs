pub mod reason_codes {
    pub use wattswarm_protocol::reason_codes::*;
}

pub mod types {
    pub use wattswarm_protocol::types::*;
}

use crate::types::{
    ArtifactRef, Candidate, PolicyBinding, SeedBundle, TaskContract, VerificationStatus,
    VerifierResult,
};
use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilities {
    pub task_types: Vec<String>,
    pub profiles: Vec<String>,
    pub provider_family: String,
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub task_id: String,
    pub execution_id: String,
    pub task_type: String,
    pub inputs: Value,
    pub profile: String,
    pub task_contract: TaskContract,
    pub stage: String,
    pub attempt_id: String,
    pub seed_bundle: Option<SeedBundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteResponse {
    pub candidate_output: Value,
    pub evidence_inline: Vec<crate::types::InlineEvidence>,
    pub evidence_refs: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyRequest {
    pub candidate: Candidate,
    pub output_schema: Value,
    pub policy: PolicyBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResponse {
    #[serde(default)]
    pub verification_status: Option<VerificationStatus>,
    pub passed: bool,
    pub score: f64,
    pub reason_codes: Vec<u16>,
    pub verifier_result_hash: String,
    pub provider_family: String,
    pub model_id: String,
}

pub trait RuntimeClient: Send + Sync {
    fn health(&self) -> Result<()>;
    fn capabilities(&self) -> Result<RuntimeCapabilities>;
    fn execute(&self, req: &ExecuteRequest) -> Result<ExecuteResponse>;
    fn verify(&self, req: &VerifyRequest) -> Result<VerifyResponse>;
}

#[derive(Clone)]
pub struct HttpRuntimeClient {
    base_url: String,
    client: Client,
}

impl HttpRuntimeClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

impl RuntimeClient for HttpRuntimeClient {
    fn health(&self) -> Result<()> {
        self.client
            .get(self.url("/health"))
            .send()
            .context("runtime /health request")?
            .error_for_status()
            .context("runtime /health status")?;
        Ok(())
    }

    fn capabilities(&self) -> Result<RuntimeCapabilities> {
        self.client
            .get(self.url("/capabilities"))
            .send()
            .context("runtime /capabilities request")?
            .error_for_status()
            .context("runtime /capabilities status")?
            .json()
            .context("runtime /capabilities decode")
    }

    fn execute(&self, req: &ExecuteRequest) -> Result<ExecuteResponse> {
        self.client
            .post(self.url("/execute"))
            .json(req)
            .send()
            .context("runtime /execute request")?
            .error_for_status()
            .context("runtime /execute status")?
            .json()
            .context("runtime /execute decode")
    }

    fn verify(&self, req: &VerifyRequest) -> Result<VerifyResponse> {
        self.client
            .post(self.url("/verify"))
            .json(req)
            .send()
            .context("runtime /verify request")?
            .error_for_status()
            .context("runtime /verify status")?
            .json()
            .context("runtime /verify decode")
    }
}

pub fn verifier_result_from_response(
    response: VerifyResponse,
    candidate_id: String,
    execution_id: String,
    policy: &PolicyBinding,
) -> VerifierResult {
    let verification_status = response.verification_status.unwrap_or_else(|| {
        if response.passed {
            VerificationStatus::Passed
        } else if response.reason_codes.iter().any(|code| {
            matches!(
                *code,
                crate::reason_codes::REASON_EVIDENCE_UNREACHABLE
                    | crate::reason_codes::REASON_EVIDENCE_TIMEOUT
                    | crate::reason_codes::REASON_EVIDENCE_AUTH_DENIED
            )
        }) {
            VerificationStatus::Inconclusive
        } else {
            VerificationStatus::Failed
        }
    });

    VerifierResult {
        candidate_id,
        execution_id,
        verification_status,
        passed: response.passed,
        score: response.score,
        reason_codes: response.reason_codes,
        verifier_result_hash: response.verifier_result_hash,
        provider_family: response.provider_family,
        model_id: response.model_id,
        policy_id: policy.policy_id.clone(),
        policy_version: policy.policy_version.clone(),
        policy_hash: policy.policy_hash.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reason_codes::{
        REASON_EVIDENCE_TIMEOUT, REASON_EVIDENCE_UNREACHABLE, REASON_SCHEMA_INVALID,
    };
    use serde_json::json;

    fn sample_policy() -> PolicyBinding {
        PolicyBinding {
            policy_id: "vp.schema_only.v1".to_owned(),
            policy_version: "1".to_owned(),
            policy_hash: "hash-1".to_owned(),
            policy_params: json!({}),
        }
    }

    #[test]
    fn http_runtime_client_url_joins_without_double_slash() {
        let client = HttpRuntimeClient::new("http://127.0.0.1:8787/");
        assert_eq!(client.url("/health"), "http://127.0.0.1:8787/health");
        assert_eq!(
            client.url("/capabilities"),
            "http://127.0.0.1:8787/capabilities"
        );
    }

    #[test]
    fn verifier_result_uses_explicit_status_when_present() {
        let response = VerifyResponse {
            verification_status: Some(VerificationStatus::Inconclusive),
            passed: true,
            score: 0.9,
            reason_codes: vec![],
            verifier_result_hash: "vr-hash".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m1".to_owned(),
        };
        let result = verifier_result_from_response(
            response,
            "c1".to_owned(),
            "e1".to_owned(),
            &sample_policy(),
        );
        assert_eq!(result.verification_status, VerificationStatus::Inconclusive);
        assert!(result.passed);
    }

    #[test]
    fn verifier_result_infers_passed_status() {
        let response = VerifyResponse {
            verification_status: None,
            passed: true,
            score: 1.0,
            reason_codes: vec![],
            verifier_result_hash: "vr-hash".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m1".to_owned(),
        };
        let result = verifier_result_from_response(
            response,
            "c1".to_owned(),
            "e1".to_owned(),
            &sample_policy(),
        );
        assert_eq!(result.verification_status, VerificationStatus::Passed);
    }

    #[test]
    fn verifier_result_infers_inconclusive_for_evidence_reasons() {
        let response = VerifyResponse {
            verification_status: None,
            passed: false,
            score: 0.2,
            reason_codes: vec![REASON_EVIDENCE_UNREACHABLE, REASON_EVIDENCE_TIMEOUT],
            verifier_result_hash: "vr-hash".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m1".to_owned(),
        };
        let result = verifier_result_from_response(
            response,
            "c1".to_owned(),
            "e1".to_owned(),
            &sample_policy(),
        );
        assert_eq!(result.verification_status, VerificationStatus::Inconclusive);
    }

    #[test]
    fn verifier_result_infers_failed_for_non_evidence_reasons() {
        let response = VerifyResponse {
            verification_status: None,
            passed: false,
            score: 0.2,
            reason_codes: vec![REASON_SCHEMA_INVALID],
            verifier_result_hash: "vr-hash".to_owned(),
            provider_family: "pf".to_owned(),
            model_id: "m1".to_owned(),
        };
        let result = verifier_result_from_response(
            response,
            "c1".to_owned(),
            "e1".to_owned(),
            &sample_policy(),
        );
        assert_eq!(result.verification_status, VerificationStatus::Failed);
    }
}
