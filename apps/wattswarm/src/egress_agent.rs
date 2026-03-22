use crate::a2a::{InvocationProtocol, InvocationSkill};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EgressAgentMode {
    #[default]
    GroupRepresentative,
    DirectGateway,
}

impl EgressAgentMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GroupRepresentative => "group_representative",
            Self::DirectGateway => "direct_gateway",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EgressAgentConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_agent_id")]
    pub agent_id: String,
    #[serde(default = "default_display_name")]
    pub display_name: String,
    #[serde(default = "default_description")]
    pub description: String,
    #[serde(default)]
    pub protocol: InvocationProtocol,
    #[serde(default)]
    pub mode: EgressAgentMode,
    #[serde(default)]
    pub executor: Option<String>,
    #[serde(default = "default_profile")]
    pub profile: String,
    #[serde(default)]
    pub public_base_url: String,
    #[serde(default)]
    pub publish_to_network: bool,
    #[serde(default)]
    pub accept_inbound_invocations: bool,
    #[serde(default)]
    pub skills: Vec<InvocationSkill>,
}

fn default_agent_id() -> String {
    "egress-agent".to_owned()
}

fn default_display_name() -> String {
    "WattSwarm Egress Agent".to_owned()
}

fn default_description() -> String {
    "Node-facing gateway agent for external invocation and outbound result publishing.".to_owned()
}

fn default_profile() -> String {
    "default".to_owned()
}

impl Default for EgressAgentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            agent_id: default_agent_id(),
            display_name: default_display_name(),
            description: default_description(),
            protocol: InvocationProtocol::default(),
            mode: EgressAgentMode::default(),
            executor: None,
            profile: default_profile(),
            public_base_url: String::new(),
            publish_to_network: false,
            accept_inbound_invocations: false,
            skills: Vec::new(),
        }
    }
}

impl EgressAgentConfig {
    pub fn normalized(mut self) -> Self {
        self.agent_id = self.agent_id.trim().to_owned();
        self.display_name = self.display_name.trim().to_owned();
        self.description = self.description.trim().to_owned();
        self.public_base_url = self.public_base_url.trim().trim_end_matches('/').to_owned();
        self.profile = self.profile.trim().to_owned();
        self.executor = self
            .executor
            .take()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        self.skills = self
            .skills
            .into_iter()
            .filter_map(|skill| {
                let normalized = skill.normalized();
                (!normalized.id.is_empty() && !normalized.name.is_empty()).then_some(normalized)
            })
            .collect();
        if self.agent_id.is_empty() {
            self.agent_id = default_agent_id();
        }
        if self.display_name.is_empty() {
            self.display_name = default_display_name();
        }
        if self.description.is_empty() {
            self.description = default_description();
        }
        if self.profile.is_empty() {
            self.profile = default_profile();
        }
        self
    }
}

pub fn egress_agent_config_path(state_dir: &Path) -> PathBuf {
    state_dir.join("egress_agent.json")
}

pub fn load_egress_agent_config(path: &Path) -> Result<EgressAgentConfig> {
    if !path.exists() {
        return Ok(EgressAgentConfig::default());
    }
    Ok(serde_json::from_slice::<EgressAgentConfig>(&fs::read(path)?)?.normalized())
}

pub fn save_egress_agent_config(path: &Path, config: &EgressAgentConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(&config.clone().normalized())?,
    )?;
    Ok(())
}
