use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SettlementLayer {
    Web2,
    #[default]
    Web3,
}

impl SettlementLayer {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Web2 => "web2",
            Self::Web3 => "web3",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SettlementRequest {
    #[serde(default)]
    pub layer: SettlementLayer,
    pub rail: String,
    #[serde(default)]
    pub request: Value,
}

impl SettlementRequest {
    pub fn normalized(mut self) -> Self {
        self.rail = self.rail.trim().to_ascii_lowercase();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NormalizedSettlementRequest {
    pub layer: SettlementLayer,
    pub rail: String,
    #[serde(default)]
    pub request: Value,
}

pub trait PaymentRailAdapter {
    fn rail_id(&self) -> &'static str;

    fn layer(&self) -> SettlementLayer;

    fn normalize_request(&self, request: &Value) -> Result<Value>;
}

#[derive(Debug, Default)]
pub struct X402PaymentRailAdapter;

impl PaymentRailAdapter for X402PaymentRailAdapter {
    fn rail_id(&self) -> &'static str {
        "x402"
    }

    fn layer(&self) -> SettlementLayer {
        SettlementLayer::Web3
    }

    fn normalize_request(&self, request: &Value) -> Result<Value> {
        let mut normalized = match request {
            Value::Object(map) => Value::Object(map.clone()),
            Value::Null => json!({}),
            _ => return Err(anyhow!("x402 settlement request must be a JSON object")),
        };
        if normalized.get("protocol").is_none() {
            normalized["protocol"] = json!("x402");
        }
        Ok(normalized)
    }
}

fn payment_rail_adapter(settlement: &SettlementRequest) -> Result<Box<dyn PaymentRailAdapter>> {
    match (
        settlement.layer,
        settlement.rail.trim().to_ascii_lowercase().as_str(),
    ) {
        (SettlementLayer::Web3, "x402") => Ok(Box::<X402PaymentRailAdapter>::default()),
        (SettlementLayer::Web3, rail) => Err(anyhow!("unsupported web3 settlement rail: {rail}")),
        (SettlementLayer::Web2, rail) => Err(anyhow!(
            "web2 settlement rail is not implemented yet: {rail}"
        )),
    }
}

pub fn supported_settlement_rails() -> Vec<Value> {
    vec![json!({
        "layer": "web3",
        "rail": "x402",
        "default": true,
        "status": "active",
    })]
}

pub fn normalize_settlement_request(
    settlement: Option<SettlementRequest>,
) -> Result<Option<NormalizedSettlementRequest>> {
    settlement
        .map(|value| value.normalized())
        .map(|value| -> Result<NormalizedSettlementRequest> {
            let adapter = payment_rail_adapter(&value)?;
            Ok(NormalizedSettlementRequest {
                layer: adapter.layer(),
                rail: adapter.rail_id().to_owned(),
                request: adapter.normalize_request(&value.request)?,
            })
        })
        .transpose()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_settlement_request_infers_x402_protocol() {
        let normalized = normalize_settlement_request(Some(SettlementRequest {
            layer: SettlementLayer::Web3,
            rail: "X402".to_owned(),
            request: json!({
                "pay_to": "0xabc123"
            }),
        }))
        .expect("normalize settlement")
        .expect("settlement exists");
        assert_eq!(normalized.rail.as_str(), "x402");
        assert_eq!(
            normalized.request.get("protocol").and_then(Value::as_str),
            Some("x402")
        );
    }
}
