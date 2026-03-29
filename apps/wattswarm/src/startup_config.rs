use crate::control::{
    ExecutorRegistryEntry, load_executor_registry_state, save_executor_registry_state,
};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const CORE_AGENT_EXECUTOR_NAME: &str = "core-agent";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    #[default]
    Local,
    Lan,
    Wan,
}

impl NetworkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Lan => "lan",
            Self::Wan => "wan",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CoreAgentMode {
    #[default]
    LocalUrl,
    RemoteUrl,
    CloudApiKey,
}

impl CoreAgentMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalUrl => "local_url",
            Self::RemoteUrl => "remote_url",
            Self::CloudApiKey => "cloud_api_key",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreAgentConfig {
    #[serde(default)]
    pub mode: CoreAgentMode,
    #[serde(default = "default_core_agent_provider")]
    pub provider: String,
    #[serde(default = "default_core_agent_base_url")]
    pub base_url: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub api_key: String,
}

impl Default for CoreAgentConfig {
    fn default() -> Self {
        Self {
            mode: CoreAgentMode::default(),
            provider: default_core_agent_provider(),
            base_url: default_core_agent_base_url(),
            model: String::new(),
            api_key: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartupConfig {
    #[serde(default = "default_display_name")]
    pub display_name: String,
    #[serde(default)]
    pub network_mode: NetworkMode,
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    #[serde(default)]
    pub core_agent: CoreAgentConfig,
}

impl Default for StartupConfig {
    fn default() -> Self {
        Self {
            display_name: default_display_name(),
            network_mode: NetworkMode::default(),
            bootstrap_peers: Vec::new(),
            core_agent: CoreAgentConfig::default(),
        }
    }
}

fn default_display_name() -> String {
    "Node Agent".to_owned()
}

fn default_core_agent_provider() -> String {
    "openai-compatible".to_owned()
}

fn default_core_agent_base_url() -> String {
    "http://127.0.0.1:8787".to_owned()
}

impl StartupConfig {
    pub fn normalized(mut self) -> Self {
        self.display_name = self.display_name.trim().to_owned();
        self.bootstrap_peers = normalize_bootstrap_peers(&self.bootstrap_peers);
        if matches!(self.network_mode, NetworkMode::Local) {
            self.bootstrap_peers.clear();
        }
        self.core_agent.provider = self.core_agent.provider.trim().to_owned();
        self.core_agent.base_url = self
            .core_agent
            .base_url
            .trim()
            .trim_end_matches('/')
            .to_owned();
        self.core_agent.model = self.core_agent.model.trim().to_owned();
        self.core_agent.api_key = self.core_agent.api_key.trim().to_owned();
        if self.display_name.is_empty() {
            self.display_name = default_display_name();
        }
        if self.core_agent.provider.is_empty() {
            self.core_agent.provider = default_core_agent_provider();
        }
        if matches!(self.core_agent.mode, CoreAgentMode::LocalUrl)
            && self.core_agent.base_url.is_empty()
        {
            self.core_agent.base_url = default_core_agent_base_url();
        }
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.display_name.trim().is_empty() {
            bail!("display_name is required");
        }
        match self.core_agent.mode {
            CoreAgentMode::LocalUrl | CoreAgentMode::RemoteUrl => {
                if self.core_agent.base_url.trim().is_empty() {
                    bail!("core_agent.base_url is required for url modes");
                }
            }
            CoreAgentMode::CloudApiKey => {
                if self.core_agent.provider.trim().is_empty() {
                    bail!("core_agent.provider is required for cloud_api_key mode");
                }
                if self.core_agent.model.trim().is_empty() {
                    bail!("core_agent.model is required for cloud_api_key mode");
                }
                if self.core_agent.api_key.trim().is_empty() {
                    bail!("core_agent.api_key is required for cloud_api_key mode");
                }
            }
        }
        Ok(())
    }
}

fn normalize_bootstrap_peers(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || normalized.iter().any(|existing| existing == trimmed) {
            continue;
        }
        normalized.push(trimmed.to_owned());
    }
    normalized
}

pub fn startup_config_path(state_dir: &Path) -> PathBuf {
    state_dir.join("startup_config.json")
}

pub fn load_startup_config(path: &Path) -> Result<StartupConfig> {
    if !path.exists() {
        return Ok(StartupConfig::default());
    }
    Ok(serde_json::from_slice::<StartupConfig>(&fs::read(path)?)?.normalized())
}

pub fn save_startup_config(path: &Path, config: &StartupConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(&config.clone().normalized())?,
    )?;
    Ok(())
}

pub fn sync_core_agent_executor(state_dir: &Path, config: &StartupConfig) -> Result<bool> {
    if !matches!(
        config.core_agent.mode,
        CoreAgentMode::LocalUrl | CoreAgentMode::RemoteUrl
    ) {
        return Ok(false);
    }

    let mut reg = load_executor_registry_state(state_dir)?;
    reg.entries
        .retain(|entry| entry.name != CORE_AGENT_EXECUTOR_NAME);
    reg.entries.push(ExecutorRegistryEntry {
        name: CORE_AGENT_EXECUTOR_NAME.to_owned(),
        base_url: config.core_agent.base_url.clone(),
    });
    save_executor_registry_state(state_dir, &reg)?;
    Ok(true)
}

pub fn core_agent_executor_name() -> &'static str {
    CORE_AGENT_EXECUTOR_NAME
}

#[cfg(test)]
mod tests {
    use super::{
        CoreAgentConfig, CoreAgentMode, NetworkMode, StartupConfig, core_agent_executor_name,
    };

    #[test]
    fn validates_url_modes_require_base_url() {
        let config = StartupConfig {
            display_name: "Node A".to_owned(),
            network_mode: NetworkMode::Lan,
            core_agent: CoreAgentConfig {
                mode: CoreAgentMode::LocalUrl,
                base_url: String::new(),
                ..CoreAgentConfig::default()
            },
            ..StartupConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validates_cloud_mode_requires_provider_model_and_key() {
        let config = StartupConfig {
            display_name: "Node A".to_owned(),
            network_mode: NetworkMode::Wan,
            core_agent: CoreAgentConfig {
                mode: CoreAgentMode::CloudApiKey,
                provider: "openclaw".to_owned(),
                ..CoreAgentConfig::default()
            },
            ..StartupConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validates_without_public_id_field() {
        let config = StartupConfig {
            display_name: "Node A".to_owned(),
            network_mode: NetworkMode::Local,
            core_agent: CoreAgentConfig::default(),
            ..StartupConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn core_agent_executor_name_is_stable() {
        assert_eq!(core_agent_executor_name(), "core-agent");
    }

    #[test]
    fn normalizes_bootstrap_peers_trim_and_dedup() {
        let config = StartupConfig {
            network_mode: NetworkMode::Lan,
            bootstrap_peers: vec![
                " /ip4/127.0.0.1/tcp/4001/p2p/peer-a ".to_owned(),
                String::new(),
                "/ip4/127.0.0.1/tcp/4001/p2p/peer-a".to_owned(),
                "/ip4/127.0.0.1/tcp/4002/p2p/peer-b".to_owned(),
            ],
            ..StartupConfig::default()
        }
        .normalized();

        assert_eq!(
            config.bootstrap_peers,
            vec![
                "/ip4/127.0.0.1/tcp/4001/p2p/peer-a".to_owned(),
                "/ip4/127.0.0.1/tcp/4002/p2p/peer-b".to_owned()
            ]
        );
    }

    #[test]
    fn clears_bootstrap_peers_for_local_mode() {
        let config = StartupConfig {
            network_mode: NetworkMode::Local,
            bootstrap_peers: vec!["/ip4/127.0.0.1/tcp/4001/p2p/peer-a".to_owned()],
            ..StartupConfig::default()
        }
        .normalized();

        assert!(config.bootstrap_peers.is_empty());
    }
}
