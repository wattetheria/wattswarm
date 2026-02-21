use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use wattswarm_runtime_client::types::{
    Acceptance, Assignment, Budget, BudgetMode, Candidate, ClaimPolicy, EvidencePolicy,
    ExploreAssignment, ExploreStopPolicy, FeedbackCapabilityPolicy, FinalizeAssignment,
    MaxConcurrency, PolicyBinding, SettlementBadPenalty, SettlementDiminishingReturns,
    SettlementPolicy, TaskContract, TaskMode, VerificationStatus, VerifyAssignment, VotePolicy,
};
use wattswarm_runtime_client::{
    ExecuteRequest, HttpRuntimeClient, RuntimeClient, VerifyRequest, verifier_result_from_response,
};

#[derive(Clone)]
struct StubConfig {
    health_status: u16,
    health_body: String,
    capabilities_status: u16,
    capabilities_body: String,
    execute_status: u16,
    execute_body: String,
    verify_status: u16,
    verify_body: String,
}

struct StubServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl StubServer {
    fn start(cfg: StubConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking listener");
        let addr = listener.local_addr().expect("listener local addr");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        handle_conn(stream, &cfg);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            stop,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Status",
    }
}

fn write_response(mut stream: TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        status_text(status),
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn handle_conn(mut stream: TcpStream, cfg: &StubConfig) {
    let mut buf = [0_u8; 8192];
    let n = stream.read(&mut buf).unwrap_or(0);
    if n == 0 {
        return;
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let line = req.lines().next().unwrap_or_default();

    if line.starts_with("GET /health ") {
        return write_response(stream, cfg.health_status, &cfg.health_body);
    }
    if line.starts_with("GET /capabilities ") {
        return write_response(stream, cfg.capabilities_status, &cfg.capabilities_body);
    }
    if line.starts_with("POST /execute ") {
        return write_response(stream, cfg.execute_status, &cfg.execute_body);
    }
    if line.starts_with("POST /verify ") {
        return write_response(stream, cfg.verify_status, &cfg.verify_body);
    }
    write_response(stream, 404, "{}")
}

fn sample_policy() -> PolicyBinding {
    PolicyBinding {
        policy_id: "vp.schema_only.v1".to_owned(),
        policy_version: "1".to_owned(),
        policy_hash: "hash-1".to_owned(),
        policy_params: serde_json::json!({}),
    }
}

fn sample_contract(task_id: &str) -> TaskContract {
    TaskContract {
        protocol_version: "v0.1".to_owned(),
        task_id: task_id.to_owned(),
        task_type: "resume_review".to_owned(),
        inputs: serde_json::json!({"prompt":"hello"}),
        output_schema: serde_json::json!({"type":"object"}),
        budget: Budget {
            time_ms: 30_000,
            max_steps: 10,
            cost_units: 1_000,
            mode: BudgetMode::Lifetime,
            explore_cost_units: 350,
            verify_cost_units: 450,
            finalize_cost_units: 200,
            reuse_verify_time_ms: 20_000,
            reuse_verify_cost_units: 100,
            reuse_max_attempts: 1,
        },
        assignment: Assignment {
            mode: "CLAIM".to_owned(),
            claim: ClaimPolicy {
                lease_ms: 5_000,
                max_concurrency: MaxConcurrency {
                    propose: 1,
                    verify: 1,
                },
            },
            explore: ExploreAssignment {
                max_proposers: 1,
                topk: 3,
                stop: ExploreStopPolicy {
                    no_new_evidence_rounds: 3,
                },
            },
            verify: VerifyAssignment { max_verifiers: 1 },
            finalize: FinalizeAssignment { max_finalizers: 1 },
        },
        acceptance: Acceptance {
            quorum_threshold: 1,
            verifier_policy: sample_policy(),
            vote: VotePolicy {
                commit_reveal: true,
                reveal_deadline_ms: 10_000,
            },
            settlement: SettlementPolicy {
                window_ms: 86_400_000,
                implicit_weight: 0.1,
                implicit_diminishing_returns: SettlementDiminishingReturns { w: 10, k: 50 },
                bad_penalty: SettlementBadPenalty { p: 3 },
                feedback: FeedbackCapabilityPolicy {
                    mode: "CAPABILITY".to_owned(),
                    authority_pubkey: "ed25519:placeholder".to_owned(),
                },
            },
            da_quorum_threshold: 1,
        },
        task_mode: TaskMode::OneShot,
        expiry_ms: 2_000_000_000_000,
        evidence_policy: EvidencePolicy {
            max_inline_evidence_bytes: 65_536,
            max_inline_media_bytes: 0,
            inline_mime_allowlist: vec!["application/json".to_owned()],
            max_snippet_bytes: 8_192,
            max_snippet_tokens: 2_048,
        },
    }
}

#[test]
fn http_runtime_client_covers_success_and_error_paths() {
    let server = StubServer::start(StubConfig {
        health_status: 200,
        health_body: "{}".to_owned(),
        capabilities_status: 200,
        capabilities_body: serde_json::json!({
            "task_types": ["resume_review"],
            "profiles": ["default"],
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
        execute_status: 200,
        execute_body: serde_json::json!({
            "candidate_output": {"answer": "ok"},
            "evidence_inline": [],
            "evidence_refs": []
        })
        .to_string(),
        verify_status: 200,
        verify_body: serde_json::json!({
            "passed": true,
            "score": 1.0,
            "reason_codes": [],
            "verifier_result_hash": "vrh-1",
            "provider_family": "stub",
            "model_id": "stub-1"
        })
        .to_string(),
    });

    let client = HttpRuntimeClient::new(server.base_url());
    client.health().expect("health ok");

    let caps = client.capabilities().expect("capabilities ok");
    assert_eq!(caps.profiles, vec!["default".to_owned()]);

    let exec = client
        .execute(&ExecuteRequest {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            task_type: "resume_review".to_owned(),
            inputs: serde_json::json!({"prompt":"hello"}),
            profile: "default".to_owned(),
            task_contract: sample_contract("task-1"),
            stage: "EXECUTE".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            seed_bundle: None,
        })
        .expect("execute ok");
    assert_eq!(exec.candidate_output["answer"], "ok");

    let verify = client
        .verify(&VerifyRequest {
            candidate: Candidate {
                candidate_id: "cand-1".to_owned(),
                execution_id: "exec-1".to_owned(),
                output: serde_json::json!({"answer":"ok"}),
                evidence_inline: vec![],
                evidence_refs: vec![],
            },
            output_schema: serde_json::json!({"type":"object"}),
            policy: sample_policy(),
        })
        .expect("verify ok");
    assert!(verify.passed);
}

#[test]
fn http_runtime_client_reports_status_and_decode_failures() {
    let status_server = StubServer::start(StubConfig {
        health_status: 500,
        health_body: "{}".to_owned(),
        capabilities_status: 500,
        capabilities_body: "{}".to_owned(),
        execute_status: 400,
        execute_body: "{}".to_owned(),
        verify_status: 500,
        verify_body: "{}".to_owned(),
    });
    let client = HttpRuntimeClient::new(status_server.base_url());

    assert!(
        client
            .health()
            .expect_err("health should fail")
            .to_string()
            .contains("runtime /health status")
    );
    assert!(
        client
            .capabilities()
            .expect_err("capabilities should fail")
            .to_string()
            .contains("runtime /capabilities status")
    );

    let execute_err = client
        .execute(&ExecuteRequest {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            task_type: "resume_review".to_owned(),
            inputs: serde_json::json!({}),
            profile: "default".to_owned(),
            task_contract: sample_contract("task-1"),
            stage: "EXECUTE".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            seed_bundle: None,
        })
        .expect_err("execute should fail on status");
    assert!(execute_err.to_string().contains("runtime /execute status"));

    let verify_err = client
        .verify(&VerifyRequest {
            candidate: Candidate {
                candidate_id: "cand-1".to_owned(),
                execution_id: "exec-1".to_owned(),
                output: serde_json::json!({}),
                evidence_inline: vec![],
                evidence_refs: vec![],
            },
            output_schema: serde_json::json!({}),
            policy: sample_policy(),
        })
        .expect_err("verify should fail on status");
    assert!(verify_err.to_string().contains("runtime /verify status"));

    let decode_server = StubServer::start(StubConfig {
        health_status: 200,
        health_body: "{}".to_owned(),
        capabilities_status: 200,
        capabilities_body: "not-json".to_owned(),
        execute_status: 200,
        execute_body: "not-json".to_owned(),
        verify_status: 200,
        verify_body: "not-json".to_owned(),
    });
    let decode_client = HttpRuntimeClient::new(decode_server.base_url());

    assert!(
        decode_client
            .capabilities()
            .expect_err("capabilities decode should fail")
            .to_string()
            .contains("runtime /capabilities decode")
    );

    assert!(
        decode_client
            .execute(&ExecuteRequest {
                task_id: "task-1".to_owned(),
                execution_id: "exec-1".to_owned(),
                task_type: "resume_review".to_owned(),
                inputs: serde_json::json!({}),
                profile: "default".to_owned(),
                task_contract: sample_contract("task-1"),
                stage: "EXECUTE".to_owned(),
                attempt_id: "attempt-1".to_owned(),
                seed_bundle: None,
            })
            .expect_err("execute decode should fail")
            .to_string()
            .contains("runtime /execute decode")
    );

    assert!(
        decode_client
            .verify(&VerifyRequest {
                candidate: Candidate {
                    candidate_id: "cand-1".to_owned(),
                    execution_id: "exec-1".to_owned(),
                    output: serde_json::json!({}),
                    evidence_inline: vec![],
                    evidence_refs: vec![],
                },
                output_schema: serde_json::json!({}),
                policy: sample_policy(),
            })
            .expect_err("verify decode should fail")
            .to_string()
            .contains("runtime /verify decode")
    );
}

#[test]
fn verifier_result_from_response_copies_policy_fields() {
    let policy = sample_policy();
    let result = verifier_result_from_response(
        wattswarm_runtime_client::VerifyResponse {
            verification_status: Some(VerificationStatus::Passed),
            passed: true,
            score: 0.9,
            reason_codes: vec![],
            verifier_result_hash: "vrh-9".to_owned(),
            provider_family: "stub".to_owned(),
            model_id: "stub-1".to_owned(),
        },
        "cand-1".to_owned(),
        "exec-1".to_owned(),
        &policy,
    );

    assert_eq!(result.policy_id, policy.policy_id);
    assert_eq!(result.policy_version, policy.policy_version);
    assert_eq!(result.policy_hash, policy.policy_hash);
}
