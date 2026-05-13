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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StartupConfig {
    #[serde(default = "default_display_name")]
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub network_mode: NetworkMode,
    #[serde(default)]
    pub bootstrap_contacts: Vec<String>,
    #[serde(default)]
    pub gateway_urls: Vec<String>,
    #[serde(default)]
    pub core_agent: CoreAgentConfig,
}

impl Default for StartupConfig {
    fn default() -> Self {
        Self {
            display_name: default_display_name(),
            latitude: None,
            longitude: None,
            network_mode: NetworkMode::default(),
            bootstrap_contacts: Vec::new(),
            gateway_urls: Vec::new(),
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
        self.latitude = normalize_latitude(self.latitude);
        self.longitude = normalize_longitude(self.longitude);
        self.bootstrap_contacts = normalize_bootstrap_contacts(&self.bootstrap_contacts);
        self.gateway_urls = normalize_gateway_urls(&self.gateway_urls);
        if matches!(self.network_mode, NetworkMode::Local) {
            self.bootstrap_contacts.clear();
            self.gateway_urls.clear();
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
        if let Some(latitude) = self.latitude
            && !(-90.0..=90.0).contains(&latitude)
        {
            bail!("latitude must be between -90 and 90");
        }
        if let Some(longitude) = self.longitude
            && !(-180.0..=180.0).contains(&longitude)
        {
            bail!("longitude must be between -180 and 180");
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

fn normalize_bootstrap_contacts(values: &[String]) -> Vec<String> {
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

fn normalize_gateway_urls(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        let trimmed = value.trim().trim_end_matches('/');
        if trimmed.is_empty() || normalized.iter().any(|existing| existing == trimmed) {
            continue;
        }
        normalized.push(trimmed.to_owned());
    }
    normalized
}

fn normalize_latitude(value: Option<f64>) -> Option<f64> {
    value.filter(|latitude| latitude.is_finite() && (-90.0..=90.0).contains(latitude))
}

fn normalize_longitude(value: Option<f64>) -> Option<f64> {
    value.filter(|longitude| longitude.is_finite() && (-180.0..=180.0).contains(longitude))
}

pub fn startup_config_path(state_dir: &Path) -> PathBuf {
    state_dir.join("startup_config.json")
}

pub fn ensure_default_wan_startup_config(state_dir: &Path) -> Result<StartupConfig> {
    let path = startup_config_path(state_dir);
    if path.exists() {
        return load_startup_config(&path);
    }
    let config = StartupConfig {
        network_mode: NetworkMode::Wan,
        ..StartupConfig::default()
    }
    .normalized();
    save_startup_config(&path, &config)?;
    Ok(config)
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

pub fn update_startup_config_geo(path: &Path, latitude: f64, longitude: f64) -> Result<bool> {
    validate_geo_coordinate(latitude, -90.0, 90.0, "latitude")?;
    validate_geo_coordinate(longitude, -180.0, 180.0, "longitude")?;

    let mut config = load_startup_config(path)?;
    if config.latitude == Some(latitude) && config.longitude == Some(longitude) {
        return Ok(false);
    }
    config.latitude = Some(latitude);
    config.longitude = Some(longitude);
    config.validate()?;
    save_startup_config(path, &config)?;
    Ok(true)
}

fn validate_geo_coordinate(value: f64, min: f64, max: f64, label: &str) -> Result<()> {
    if !value.is_finite() || !(min..=max).contains(&value) {
        bail!("{label} must be between {min} and {max}");
    }
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
        agent_event_callback_base_url: None,
        kind: Default::default(),
        target_node_id: None,
        scope_hint: None,
        commit_plane_endpoint: None,
        commit_plane_token_file: None,
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
        ensure_default_wan_startup_config, load_startup_config, save_startup_config,
        startup_config_path, update_startup_config_geo,
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
    fn normalizes_bootstrap_contacts_trim_and_dedup() {
        let contact_a = r#"{"contact":"a"}"#.to_owned();
        let contact_b = r#"{"contact":"b"}"#.to_owned();
        let config = StartupConfig {
            network_mode: NetworkMode::Lan,
            bootstrap_contacts: vec![
                format!(" {contact_a} "),
                String::new(),
                contact_a.clone(),
                contact_b.clone(),
            ],
            ..StartupConfig::default()
        }
        .normalized();

        assert_eq!(config.bootstrap_contacts, vec![contact_a, contact_b]);
    }

    #[test]
    fn clears_bootstrap_contacts_for_local_mode() {
        let config = StartupConfig {
            network_mode: NetworkMode::Local,
            bootstrap_contacts: vec!["iroh-contact-a".to_owned()],
            gateway_urls: vec!["https://gw.example.com".to_owned()],
            ..StartupConfig::default()
        }
        .normalized();

        assert!(config.bootstrap_contacts.is_empty());
        assert!(config.gateway_urls.is_empty());
    }

    #[test]
    fn normalizes_gateway_urls_trim_and_dedup() {
        let config = StartupConfig {
            network_mode: NetworkMode::Wan,
            gateway_urls: vec![
                " https://gw.example.com/ ".to_owned(),
                String::new(),
                "https://gw.example.com".to_owned(),
                "http://gateway.example.com:8080/".to_owned(),
            ],
            ..StartupConfig::default()
        }
        .normalized();

        assert_eq!(
            config.gateway_urls,
            vec![
                "https://gw.example.com".to_owned(),
                "http://gateway.example.com:8080".to_owned(),
            ]
        );
    }

    #[test]
    fn normalizes_invalid_geo_coordinates_to_none() {
        let config = StartupConfig {
            latitude: Some(91.0),
            longitude: Some(181.0),
            ..StartupConfig::default()
        }
        .normalized();

        assert_eq!(config.latitude, None);
        assert_eq!(config.longitude, None);
    }

    #[test]
    fn validates_geo_coordinate_ranges() {
        let invalid_latitude = StartupConfig {
            latitude: Some(-91.0),
            ..StartupConfig::default()
        };
        assert!(invalid_latitude.validate().is_err());

        let invalid_longitude = StartupConfig {
            longitude: Some(181.0),
            ..StartupConfig::default()
        };
        assert!(invalid_longitude.validate().is_err());
    }

    #[test]
    fn ensure_default_wan_startup_config_creates_missing_file() {
        let dir = tempfile::tempdir().unwrap();

        let config = ensure_default_wan_startup_config(dir.path()).unwrap();

        assert_eq!(config.network_mode, NetworkMode::Wan);
        let saved = load_startup_config(&startup_config_path(dir.path())).unwrap();
        assert_eq!(saved.network_mode, NetworkMode::Wan);
    }

    #[test]
    fn ensure_default_wan_startup_config_preserves_existing_config() {
        let dir = tempfile::tempdir().unwrap();
        let existing = StartupConfig {
            network_mode: NetworkMode::Lan,
            bootstrap_contacts: vec!["iroh-contact-a".to_owned()],
            gateway_urls: vec!["https://gw.example.com".to_owned()],
            ..StartupConfig::default()
        };
        save_startup_config(&startup_config_path(dir.path()), &existing).unwrap();

        let config = ensure_default_wan_startup_config(dir.path()).unwrap();

        assert_eq!(config.network_mode, NetworkMode::Lan);
        assert_eq!(config.bootstrap_contacts, vec!["iroh-contact-a"]);
        assert_eq!(config.gateway_urls, vec!["https://gw.example.com"]);
    }

    #[test]
    fn update_startup_config_geo_preserves_other_startup_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = startup_config_path(dir.path());
        let existing = StartupConfig {
            display_name: "Sydney Node".to_owned(),
            network_mode: NetworkMode::Wan,
            gateway_urls: vec!["https://gateway.example.com".to_owned()],
            latitude: Some(1.0),
            longitude: Some(2.0),
            ..StartupConfig::default()
        };
        save_startup_config(&path, &existing).unwrap();

        let updated = update_startup_config_geo(&path, -33.8399, 151.0583).unwrap();

        assert!(updated);
        let saved = load_startup_config(&path).unwrap();
        assert_eq!(saved.display_name, "Sydney Node");
        assert_eq!(saved.network_mode, NetworkMode::Wan);
        assert_eq!(
            saved.gateway_urls,
            vec!["https://gateway.example.com".to_owned()]
        );
        assert!(
            saved
                .latitude
                .is_some_and(|value| (value - -33.8399).abs() < f64::EPSILON)
        );
        assert!(
            saved
                .longitude
                .is_some_and(|value| (value - 151.0583).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn update_startup_config_geo_rejects_invalid_coordinates() {
        let dir = tempfile::tempdir().unwrap();
        let path = startup_config_path(dir.path());

        assert!(update_startup_config_geo(&path, 91.0, 0.0).is_err());
        assert!(update_startup_config_geo(&path, 0.0, f64::NAN).is_err());
    }
}
