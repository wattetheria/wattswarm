use anyhow::{Result, bail};
use std::time::Duration;

pub const DELIVERY_POLICY_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientServerConfig {
    pub gateway_url: String,
    pub delivery_policy_version: u64,
    pub request_timeout: Duration,
    pub delivery_page_size: usize,
}

impl ClientServerConfig {
    pub fn from_gateway_url(gateway_url: impl Into<String>) -> Self {
        Self {
            gateway_url: gateway_url.into(),
            delivery_policy_version: DELIVERY_POLICY_VERSION,
            request_timeout: Duration::from_secs(30),
            delivery_page_size: 64,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let parsed = reqwest::Url::parse(&self.gateway_url)?;
        if !matches!(parsed.scheme(), "https" | "http") {
            bail!("ClientServer Gateway URL must use http or https");
        }
        if parsed.host_str().is_none() {
            bail!("ClientServer Gateway URL must include a host");
        }
        if self.delivery_policy_version == 0 {
            bail!("delivery policy version must be greater than zero");
        }
        if self.delivery_page_size == 0 {
            bail!("delivery page size must be greater than zero");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_gateway_configuration_without_broker_credentials() {
        ClientServerConfig::from_gateway_url("https://network.example.com")
            .validate()
            .unwrap();
        assert!(
            ClientServerConfig::from_gateway_url("amqps://broker")
                .validate()
                .is_err()
        );
    }
}
