use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmScope {
    Global,
    Region(String),
    Node(String),
    Group(String),
}

impl SwarmScope {
    pub fn label(&self) -> Result<String> {
        match self {
            Self::Global => Ok("global".to_owned()),
            Self::Region(region_id) => Ok(format!("region.{}", sanitize_segment(region_id)?)),
            Self::Node(node_id) => Ok(format!("node.{}", sanitize_segment(node_id)?)),
            Self::Group(group_id) => Ok(format!("group.{}", sanitize_segment(group_id)?)),
        }
    }
}

pub fn sanitize_segment(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("network segment cannot be empty");
    }

    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropagationLane {
    Events,
    Messages,
    Rules,
    Checkpoints,
    Summaries,
}

impl PropagationLane {
    pub const ALL: [Self; 5] = [
        Self::Events,
        Self::Messages,
        Self::Rules,
        Self::Checkpoints,
        Self::Summaries,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Messages => "messages",
            Self::Rules => "rules",
            Self::Checkpoints => "checkpoints",
            Self::Summaries => "summaries",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTransportRoute {
    pub scope: SwarmScope,
    pub lane: PropagationLane,
    pub address: String,
    pub public_global_control: bool,
}

impl EventTransportRoute {
    pub fn from_kind_label(
        scope: SwarmScope,
        lane: PropagationLane,
        kind_label: &str,
        public_global_control: bool,
    ) -> Result<Self> {
        let address = format!("ws.{}.{}", scope.label()?, kind_label);
        Ok(Self {
            scope,
            lane,
            address,
            public_global_control,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizer_and_scope_labels_preserve_the_p2p_contract() {
        let cases = [
            (" Main/Alpha ", "main-alpha"),
            ("A_B-c", "a_b-c"),
            ("sol.1/alpha", "sol-1-alpha"),
            ("UPPER", "upper"),
        ];
        for (raw, expected) in cases {
            assert_eq!(sanitize_segment(raw).unwrap(), expected);
        }
        assert!(sanitize_segment("  ").is_err());
        assert_eq!(
            SwarmScope::Region("sol-1/alpha".to_owned())
                .label()
                .unwrap(),
            "region.sol-1-alpha"
        );
    }

    #[test]
    fn propagation_lane_serde_is_stable() {
        let encoded = serde_json::to_string(&PropagationLane::ALL).unwrap();
        assert_eq!(
            encoded,
            "[\"events\",\"messages\",\"rules\",\"checkpoints\",\"summaries\"]"
        );
    }

    #[test]
    fn event_transport_route_keeps_the_existing_address_shape() {
        let route = EventTransportRoute::from_kind_label(
            SwarmScope::Group("hive/123".to_owned()),
            PropagationLane::Messages,
            "TopicMessagePosted",
            false,
        )
        .unwrap();
        assert_eq!(route.address, "ws.group.hive-123.TopicMessagePosted");
    }
}
