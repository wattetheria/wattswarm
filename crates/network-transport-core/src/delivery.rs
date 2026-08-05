use crate::EventTransportRoute;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryClass {
    Interactive,
    Bulk,
}

impl DeliveryClass {
    pub const ALL: [Self; 2] = [Self::Interactive, Self::Bulk];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Bulk => "bulk",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxBinding {
    pub network_id: String,
    pub recipient_principal_id: String,
    pub delivery_class: DeliveryClass,
}

impl MailboxBinding {
    pub fn validate(&self) -> Result<()> {
        require_non_empty("mailbox network_id", &self.network_id)?;
        require_non_empty(
            "mailbox recipient_principal_id",
            &self.recipient_principal_id,
        )
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueSignedRecord(Vec<u8>);

impl OpaqueSignedRecord {
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.is_empty() {
            bail!("signed record cannot be empty");
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for OpaqueSignedRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueSignedRecord")
            .field("bytes", &self.0.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueCommitToken(String);

impl OpaqueCommitToken {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        require_non_empty("commit token", &value)?;
        Ok(Self(value))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OpaqueCommitToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueCommitToken([redacted])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxDelivery {
    pub delivery_id: String,
    pub record_id: String,
    pub route: EventTransportRoute,
    pub record: OpaqueSignedRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership_version: Option<String>,
    pub enqueued_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxControlDelivery {
    pub delivery_id: String,
    pub correlation_id: String,
    pub source_principal_id: String,
    pub target_principal_id: String,
    pub control_kind: String,
    pub payload: OpaqueSignedRecord,
    pub enqueued_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailboxGapReason {
    Expired,
    DeliveryLimitExceeded,
    AdministrativeRemoval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxGap {
    pub gap_id: String,
    pub delivery_class: DeliveryClass,
    pub delivery_policy_version: u64,
    pub route: EventTransportRoute,
    pub reason: MailboxGapReason,
    pub first_affected_at: u64,
    pub last_affected_at: u64,
    pub approximate_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryPage {
    pub page_id: String,
    pub binding: MailboxBinding,
    #[serde(default)]
    pub deliveries: Vec<MailboxDelivery>,
    #[serde(default)]
    pub controls: Vec<MailboxControlDelivery>,
    #[serde(default)]
    pub gaps: Vec<MailboxGap>,
    pub commit_token: OpaqueCommitToken,
}

impl DeliveryPage {
    pub fn validate(&self) -> Result<()> {
        require_non_empty("delivery page_id", &self.page_id)?;
        self.binding.validate()?;
        if self.deliveries.is_empty() && self.controls.is_empty() && self.gaps.is_empty() {
            bail!("delivery page cannot be empty");
        }
        for delivery in &self.deliveries {
            require_non_empty("delivery_id", &delivery.delivery_id)?;
            if delivery
                .expires_at
                .is_some_and(|expires_at| expires_at < delivery.enqueued_at)
            {
                bail!("delivery expires_at cannot precede enqueued_at");
            }
        }
        for control in &self.controls {
            require_non_empty("control delivery_id", &control.delivery_id)?;
            require_non_empty("control correlation_id", &control.correlation_id)?;
            require_non_empty("control source principal", &control.source_principal_id)?;
            require_non_empty("control target principal", &control.target_principal_id)?;
            require_non_empty("control kind", &control.control_kind)?;
            if control.target_principal_id != self.binding.recipient_principal_id {
                bail!("control target does not match page recipient");
            }
        }
        for gap in &self.gaps {
            require_non_empty("mailbox gap_id", &gap.gap_id)?;
            if gap.delivery_class != self.binding.delivery_class {
                bail!("mailbox gap delivery class does not match page binding");
            }
            if gap.delivery_policy_version == 0 {
                bail!("mailbox gap delivery_policy_version must be > 0");
            }
            if gap.first_affected_at > gap.last_affected_at {
                bail!("mailbox gap affected time range is invalid");
            }
            if gap.approximate_count == 0 {
                bail!("mailbox gap approximate_count must be > 0");
            }
        }
        Ok(())
    }
}

pub fn stable_delivery_id(
    network_id: &str,
    record_id: &str,
    recipient_principal_id: &str,
    membership_version: Option<&str>,
) -> Result<String> {
    require_non_empty("delivery network_id", network_id)?;
    require_non_empty("delivery record_id", record_id)?;
    require_non_empty("delivery recipient_principal_id", recipient_principal_id)?;

    let mut hasher = Sha256::new();
    hasher.update(b"wattswarm:client-server-delivery:v1\0");
    for component in [
        network_id,
        record_id,
        recipient_principal_id,
        membership_version.unwrap_or_default(),
    ] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

fn require_non_empty(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{label} cannot be empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PropagationLane, SwarmScope};

    fn route(lane: PropagationLane) -> EventTransportRoute {
        EventTransportRoute::from_kind_label(
            SwarmScope::Group("hive-1".to_owned()),
            lane,
            "TopicMessagePosted",
            false,
        )
        .unwrap()
    }

    #[test]
    fn delivery_identity_is_stable_and_recipient_scoped() {
        let first = stable_delivery_id("net", "record", "agent-a", Some("v40")).unwrap();
        let retry = stable_delivery_id("net", "record", "agent-a", Some("v40")).unwrap();
        let other_recipient = stable_delivery_id("net", "record", "agent-b", Some("v40")).unwrap();
        assert_eq!(first, retry);
        assert_ne!(first, other_recipient);
    }

    #[test]
    fn gap_only_page_is_valid_for_the_same_delivery_class() {
        let page = DeliveryPage {
            page_id: "page-1".to_owned(),
            binding: MailboxBinding {
                network_id: "net".to_owned(),
                recipient_principal_id: "agent-a".to_owned(),
                delivery_class: DeliveryClass::Bulk,
            },
            deliveries: Vec::new(),
            controls: Vec::new(),
            gaps: vec![MailboxGap {
                gap_id: "gap-1".to_owned(),
                delivery_class: DeliveryClass::Bulk,
                delivery_policy_version: 1,
                route: route(PropagationLane::Summaries),
                reason: MailboxGapReason::Expired,
                first_affected_at: 10,
                last_affected_at: 20,
                approximate_count: 2,
            }],
            commit_token: OpaqueCommitToken::new("opaque").unwrap(),
        };
        page.validate().unwrap();
    }

    #[test]
    fn page_rejects_cross_class_gap_acknowledgement() {
        let page = DeliveryPage {
            page_id: "page-1".to_owned(),
            binding: MailboxBinding {
                network_id: "net".to_owned(),
                recipient_principal_id: "agent-a".to_owned(),
                delivery_class: DeliveryClass::Interactive,
            },
            deliveries: Vec::new(),
            controls: Vec::new(),
            gaps: vec![MailboxGap {
                gap_id: "gap-1".to_owned(),
                delivery_class: DeliveryClass::Bulk,
                delivery_policy_version: 1,
                route: route(PropagationLane::Summaries),
                reason: MailboxGapReason::Expired,
                first_affected_at: 10,
                last_affected_at: 20,
                approximate_count: 1,
            }],
            commit_token: OpaqueCommitToken::new("opaque").unwrap(),
        };
        assert!(page.validate().is_err());
    }
}
