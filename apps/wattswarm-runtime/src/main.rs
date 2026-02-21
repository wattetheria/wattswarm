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
