use crate::control::{RealTaskRunRequest, open_node, run_real_task_flow};
use crate::egress_agent::{EgressAgentConfig, EgressAgentMode};
use crate::run_control;
use crate::run_queue::RunSubmitSpec;
use crate::task_template::sample_contract;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum InvocationProtocol {
    #[default]
    GoogleA2a,
}

impl InvocationProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GoogleA2a => "google_a2a",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvocationSkill {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl InvocationSkill {
    pub fn normalized(mut self) -> Self {
        self.id = self.id.trim().to_owned();
        self.name = self.name.trim().to_owned();
        self.description = self.description.trim().to_owned();
        self.tags = self
            .tags
            .into_iter()
            .map(|tag| tag.trim().to_owned())
            .filter(|tag| !tag.is_empty())
            .collect();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InvocationRequestEnvelope {
    pub request_id: String,
    pub target_agent_id: String,
    pub capability: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub auth_proof: Option<Value>,
    #[serde(default)]
    pub payment_proof: Option<Value>,
    #[serde(default)]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InvocationResponseEnvelope {
    pub request_id: String,
    pub status: String,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub receipt: Option<Value>,
    #[serde(default)]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GoogleA2aSendRequest {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    pub id: Value,
    pub method: String,
    pub params: GoogleA2aSendParams,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GoogleA2aSendParams {
    #[serde(default)]
    #[serde(alias = "target_agent_id")]
    pub target_agent_id: Option<String>,
    pub capability: String,
    pub message: GoogleA2aMessage,
    #[serde(default)]
    pub extensions: Option<GoogleA2aExtensions>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GoogleA2aMessage {
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub parts: Vec<GoogleA2aPart>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GoogleA2aPart {
    #[serde(rename = "type")]
    pub part_type: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GoogleA2aExtensions {
    #[serde(default)]
    pub auth_proof: Option<Value>,
    #[serde(default)]
    pub payment_proof: Option<Value>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub run_spec: Option<RunSubmitSpec>,
    #[serde(default)]
    pub kickoff: Option<bool>,
}

pub trait InvocationAdapter {
    fn protocol(&self) -> InvocationProtocol;

    fn build_agent_card(&self, config: &EgressAgentConfig, node_id: &str) -> Result<Value>;

    fn wrap_invocation_request(&self, request: &InvocationRequestEnvelope) -> Result<Value>;

    fn wrap_invocation_response(&self, response: &InvocationResponseEnvelope) -> Result<Value>;
}

#[derive(Debug, Default)]
pub struct GoogleA2aAdapter;

impl GoogleA2aAdapter {
    fn public_base_url(config: &EgressAgentConfig) -> String {
        if config.public_base_url.is_empty() {
            "http://127.0.0.1:7788".to_owned()
        } else {
            config.public_base_url.clone()
        }
    }

    fn public_agent_url(config: &EgressAgentConfig) -> String {
        format!("{}/a2a/google", Self::public_base_url(config))
    }
}

impl InvocationAdapter for GoogleA2aAdapter {
    fn protocol(&self) -> InvocationProtocol {
        InvocationProtocol::GoogleA2a
    }

    fn build_agent_card(&self, config: &EgressAgentConfig, node_id: &str) -> Result<Value> {
        let mode = match config.mode {
            EgressAgentMode::GroupRepresentative => "group_representative",
            EgressAgentMode::DirectGateway => "direct_gateway",
        };
        let skills = config
            .skills
            .iter()
            .map(|skill| {
                json!({
                    "id": skill.id,
                    "name": skill.name,
                    "description": skill.description,
                    "tags": skill.tags,
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "protocol": "google_a2a",
            "preferred_transport": "https+jsonrpc",
            "name": config.display_name,
            "description": config.description,
            "url": Self::public_agent_url(config),
            "version": "0.1.0",
            "provider": {
                "organization": "WattSwarm Node",
                "node_id": node_id,
            },
            "default_input_modes": ["application/json"],
            "default_output_modes": ["application/json"],
            "capabilities": {
                "streaming": false,
                "push_notifications": false,
                "state_transition_history": true,
            },
            "authentication": {
                "schemes": ["did_signature", "wallet_proof"],
            },
            "security": {
                "transport": "libp2p_tunnel_or_https",
                "signing": "node_identity",
            },
            "skills": skills,
            "metadata": {
                "agent_id": config.agent_id,
                "protocol_adapter": self.protocol().as_str(),
                "mode": mode,
                "executor": config.executor,
                "profile": config.profile,
                "publish_to_network": config.publish_to_network,
                "accept_inbound_invocations": config.accept_inbound_invocations,
            }
        }))
    }

    fn wrap_invocation_request(&self, request: &InvocationRequestEnvelope) -> Result<Value> {
        Ok(json!({
            "jsonrpc": "2.0",
            "id": request.request_id,
            "method": "message/send",
            "params": {
                "targetAgentId": request.target_agent_id,
                "capability": request.capability,
                "message": {
                    "role": "user",
                    "parts": [
                        {
                            "type": "data",
                            "data": request.payload
                        }
                    ]
                },
                "extensions": {
                    "auth_proof": request.auth_proof,
                    "payment_proof": request.payment_proof,
                    "signature": request.signature,
                }
            }
        }))
    }

    fn wrap_invocation_response(&self, response: &InvocationResponseEnvelope) -> Result<Value> {
        Ok(json!({
            "jsonrpc": "2.0",
            "id": response.request_id,
            "result": {
                "status": response.status,
                "artifacts": response.result.clone().into_iter().collect::<Vec<_>>(),
                "extensions": {
                    "receipt": response.receipt,
                    "error": response.error,
                }
            }
        }))
    }
}

pub fn adapter_for(protocol: InvocationProtocol) -> Box<dyn InvocationAdapter> {
    match protocol {
        InvocationProtocol::GoogleA2a => Box::<GoogleA2aAdapter>::default(),
    }
}

pub fn build_agent_card(config: &EgressAgentConfig, node_id: &str) -> Result<Value> {
    adapter_for(config.protocol).build_agent_card(config, node_id)
}

pub fn handle_google_message_send(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    config: &EgressAgentConfig,
    request: GoogleA2aSendRequest,
) -> Result<Value> {
    let request_id = request_id_string(&request.id);
    let target_agent_id = request
        .params
        .target_agent_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config.agent_id.clone());
    let payload = first_payload_part(&request.params.message);
    let auth_proof = request
        .params
        .extensions
        .as_ref()
        .and_then(|ext| ext.auth_proof.clone());
    let payment_proof = request
        .params
        .extensions
        .as_ref()
        .and_then(|ext| ext.payment_proof.clone());
    let signature = request
        .params
        .extensions
        .as_ref()
        .and_then(|ext| ext.signature.clone());

    let invocation = InvocationRequestEnvelope {
        request_id: request_id.clone(),
        target_agent_id,
        capability: request.params.capability.clone(),
        payload,
        auth_proof,
        payment_proof,
        signature,
    };

    let response = match config.mode {
        EgressAgentMode::DirectGateway => {
            execute_direct_gateway(state_dir, db_path, config, &invocation)?
        }
        EgressAgentMode::GroupRepresentative => execute_group_representative(
            state_dir,
            db_path,
            pg_url,
            request.params.extensions.as_ref(),
            &invocation,
        )?,
    };
    adapter_for(InvocationProtocol::GoogleA2a).wrap_invocation_response(&response)
}

fn request_id_string(id: &Value) -> String {
    match id {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => Uuid::new_v4().to_string(),
    }
}

fn first_payload_part(message: &GoogleA2aMessage) -> Value {
    for part in &message.parts {
        match part.part_type.as_str() {
            "data" => {
                if let Some(data) = &part.data {
                    return data.clone();
                }
            }
            "text" => {
                if let Some(text) = &part.text {
                    return json!({ "text": text });
                }
            }
            _ => {}
        }
    }
    json!({})
}

fn execute_direct_gateway(
    state_dir: &Path,
    db_path: &Path,
    config: &EgressAgentConfig,
    invocation: &InvocationRequestEnvelope,
) -> Result<InvocationResponseEnvelope> {
    let executor = config
        .executor
        .clone()
        .ok_or_else(|| anyhow::anyhow!("egress agent executor is required for direct_gateway"))?;
    let mut node = open_node(state_dir, db_path)?;
    let policy_hash = node
        .policy_registry()
        .binding_for("vp.schema_only.v1", json!({}))?
        .policy_hash;
    let task_id = format!("a2a-{}-{}", invocation.capability, Uuid::new_v4().simple());
    let mut contract = sample_contract(&task_id, policy_hash);
    contract.task_type = "a2a_invoke".to_owned();
    contract.inputs = json!({
        "request_id": invocation.request_id,
        "target_agent_id": invocation.target_agent_id,
        "capability": invocation.capability,
        "payload": invocation.payload,
        "auth_proof": invocation.auth_proof,
        "payment_proof": invocation.payment_proof,
        "signature": invocation.signature,
        "protocol": "google_a2a",
    });
    let result = run_real_task_flow(
        &mut node,
        state_dir,
        RealTaskRunRequest {
            executor: executor.clone(),
            profile: config.profile.clone(),
            task_id: Some(task_id.clone()),
            task_file: None,
            task_contract: Some(contract),
        },
    )?;
    Ok(InvocationResponseEnvelope {
        request_id: invocation.request_id.clone(),
        status: "completed".to_owned(),
        result: Some(result),
        receipt: Some(json!({
            "mode": "direct_gateway",
            "executor": executor,
            "profile": config.profile,
            "task_id": task_id,
        })),
        error: None,
    })
}

fn execute_group_representative(
    state_dir: &Path,
    db_path: &Path,
    pg_url: &str,
    extensions: Option<&GoogleA2aExtensions>,
    invocation: &InvocationRequestEnvelope,
) -> Result<InvocationResponseEnvelope> {
    let ext = extensions.ok_or_else(|| {
        anyhow::anyhow!("google a2a group_representative requires extensions.run_spec")
    })?;
    let spec = ext
        .run_spec
        .clone()
        .ok_or_else(|| anyhow::anyhow!("extensions.run_spec is required"))?;
    let kickoff = ext.kickoff.unwrap_or(true);
    let submit = run_control::submit_run(state_dir, db_path, pg_url, spec, kickoff)?;
    Ok(InvocationResponseEnvelope {
        request_id: invocation.request_id.clone(),
        status: "accepted".to_owned(),
        result: Some(submit.clone()),
        receipt: Some(json!({
            "mode": "group_representative",
            "run_id": submit.get("run_id").cloned().unwrap_or(Value::Null),
            "kicked_off": submit.get("kicked_off").cloned().unwrap_or(Value::Bool(kickoff)),
        })),
        error: None,
    })
}
