use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use serde_json::json;
use std::sync::Arc;
use wattswarm::crypto::sha256_hex;
use wattswarm::policy::PolicyRegistry;
use wattswarm::runtime::{
    ExecuteRequest, ExecuteResponse, RuntimeCapabilities, VerifyRequest, VerifyResponse,
};
use wattswarm::types::{ArtifactRef, InlineEvidence, VerificationStatus};

#[derive(Parser, Debug)]
#[command(name = "wattswarm-runtime")]
#[command(about = "Reference HTTP runtime for WattSwarm real-flow runs")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8787")]
    listen: String,
    #[arg(long, default_value = "swarm-runtime")]
    provider_family: String,
    #[arg(long, default_value = "swarm-model-v1")]
    model_id: String,
    #[arg(long, default_value = "default")]
    profiles: String,
    #[arg(long, default_value = "swarm")]
    task_types: String,
}

#[derive(Clone)]
struct AppState {
    capabilities: RuntimeCapabilities,
    policies: Arc<PolicyRegistry>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let capabilities = RuntimeCapabilities {
        task_types: split_csv(&args.task_types),
        profiles: split_csv(&args.profiles),
        provider_family: args.provider_family.clone(),
        model_id: args.model_id.clone(),
    };
    let state = AppState {
        capabilities,
        policies: Arc::new(PolicyRegistry::with_builtin()),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/capabilities", get(capabilities_endpoint))
        .route("/execute", post(execute))
        .route("/verify", post(verify))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    println!("wattswarm-runtime listening on {}", args.listen);
    axum::serve(listener, app).await.context("serve runtime")
}

fn split_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn capabilities_endpoint(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.capabilities)
}

async fn execute(
    State(state): State<AppState>,
    Json(req): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    let prompt = req
        .inputs
        .get("prompt")
        .and_then(|v| v.as_str())
        .unwrap_or("no-prompt");
    let answer = format!("{}::{}", req.profile, prompt);
    let evidence_payload = format!("{}|{}|{}", req.task_id, req.execution_id, answer);
    let evidence_digest = format!("sha256:{}", sha256_hex(evidence_payload.as_bytes()));
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;

    let response = ExecuteResponse {
        candidate_output: json!({
            "answer": answer,
            "confidence": 0.93,
            "check_summary": format!("stage={} attempt={}", req.stage, req.attempt_id),
        }),
        evidence_inline: vec![InlineEvidence {
            mime: "text/plain".to_owned(),
            content: format!("trace:{}", req.attempt_id),
        }],
        evidence_refs: vec![ArtifactRef {
            uri: format!("https://runtime.local/{}/{}", req.task_id, req.execution_id),
            digest: evidence_digest,
            size_bytes: evidence_payload.len() as u64,
            mime: "application/json".to_owned(),
            created_at: now_ms,
            producer: format!(
                "{}/{}",
                state.capabilities.provider_family, state.capabilities.model_id
            ),
        }],
    };

    Ok(Json(response))
}

async fn verify(
    State(state): State<AppState>,
    Json(req): Json<VerifyRequest>,
) -> Result<Json<VerifyResponse>, (StatusCode, String)> {
    let policy = state.policies.require_binding(&req.policy).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("policy binding invalid: {err}"),
        )
    })?;
    let result = policy.evaluate(
        &req.candidate,
        &req.output_schema,
        &req.policy.policy_params,
    );

    let verifier_result_hash = sha256_hex(
        serde_json::to_vec(&json!({
            "candidate_id": req.candidate.candidate_id,
            "execution_id": req.candidate.execution_id,
            "passed": result.passed,
            "score": result.score,
            "reason_codes": result.reason_codes,
            "provider_family": state.capabilities.provider_family,
            "model_id": state.capabilities.model_id,
            "policy_hash": req.policy.policy_hash,
        }))
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
        .as_slice(),
    );

    let response = VerifyResponse {
        verification_status: Some(if result.passed {
            VerificationStatus::Passed
        } else {
            VerificationStatus::Failed
        }),
        passed: result.passed,
        score: result.score,
        reason_codes: result.reason_codes,
        verifier_result_hash,
        provider_family: state.capabilities.provider_family.clone(),
        model_id: state.capabilities.model_id.clone(),
    };
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wattswarm::types::{Candidate, PolicyBinding};

    fn sample_state() -> AppState {
        AppState {
            capabilities: RuntimeCapabilities {
                task_types: vec!["swarm".to_owned()],
                profiles: vec!["default".to_owned()],
                provider_family: "swarm-runtime".to_owned(),
                model_id: "swarm-model-v1".to_owned(),
            },
            policies: Arc::new(PolicyRegistry::with_builtin()),
        }
    }

    fn sample_policy_binding(state: &AppState) -> PolicyBinding {
        state
            .policies
            .binding_for("vp.schema_only.v1", json!({}))
            .expect("builtin policy binding")
    }

    fn sample_execute_request(policy_hash: String) -> ExecuteRequest {
        let mut contract = wattswarm::task_template::sample_contract("task-1", policy_hash);
        contract.output_schema = json!({
            "type": "object",
            "required": ["answer"],
            "properties": {
                "answer": {"type": "string"}
            }
        });
        ExecuteRequest {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            task_type: "swarm".to_owned(),
            inputs: json!({"prompt": "hello"}),
            profile: "default".to_owned(),
            task_contract: contract,
            stage: "explore".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            seed_bundle: None,
        }
    }

    #[test]
    fn split_csv_trims_and_drops_empty_items() {
        assert_eq!(
            split_csv(" swarm , , default,verify "),
            vec![
                "swarm".to_owned(),
                "default".to_owned(),
                "verify".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn execute_builds_candidate_output_and_evidence() {
        let state = sample_state();
        let policy = sample_policy_binding(&state);
        let req = sample_execute_request(policy.policy_hash);

        let Json(resp) = execute(State(state), Json(req)).await.expect("execute ok");
        assert_eq!(resp.candidate_output["answer"], json!("default::hello"));
        assert_eq!(resp.evidence_inline.len(), 1);
        assert_eq!(resp.evidence_refs.len(), 1);
        assert_eq!(resp.evidence_refs[0].mime, "application/json");
        assert!(
            resp.evidence_refs[0]
                .producer
                .contains("swarm-runtime/swarm-model-v1")
        );
    }

    #[tokio::test]
    async fn verify_returns_bad_request_for_invalid_policy_binding() {
        let state = sample_state();
        let req = VerifyRequest {
            candidate: Candidate {
                candidate_id: "c1".to_owned(),
                execution_id: "e1".to_owned(),
                output: json!({"answer":"ok"}),
                evidence_inline: vec![],
                evidence_refs: vec![],
            },
            output_schema: json!({
                "type":"object",
                "required":["answer"],
                "properties":{"answer":{"type":"string"}}
            }),
            policy: PolicyBinding {
                policy_id: "vp.schema_only.v1".to_owned(),
                policy_version: "1".to_owned(),
                policy_hash: "invalid-hash".to_owned(),
                policy_params: json!({}),
            },
        };

        let err = verify(State(state), Json(req))
            .await
            .expect_err("should fail");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("policy binding invalid"));
    }

    #[tokio::test]
    async fn verify_returns_passed_status_for_valid_candidate() {
        let state = sample_state();
        let policy = sample_policy_binding(&state);
        let req = VerifyRequest {
            candidate: Candidate {
                candidate_id: "c1".to_owned(),
                execution_id: "e1".to_owned(),
                output: json!({"answer":"ok"}),
                evidence_inline: vec![],
                evidence_refs: vec![],
            },
            output_schema: json!({
                "type":"object",
                "required":["answer"],
                "properties":{"answer":{"type":"string"}}
            }),
            policy,
        };

        let Json(resp) = verify(State(state), Json(req)).await.expect("verify ok");
        assert_eq!(resp.verification_status, Some(VerificationStatus::Passed));
        assert!(resp.passed);
        assert_eq!(resp.score, 1.0);
    }
}
