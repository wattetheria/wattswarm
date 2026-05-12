use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use wattswarm_crypto::{NodeIdentity, verify_signature};
use wattswarm_network_transport_core::TransportContactMaterial;

pub const DISCOVERY_PROTOCOL_VERSION: &str = "wattswarm-discovery/1";
pub const DEFAULT_RECORD_TTL_MS: u64 = 5 * 60 * 1000;
const EARTH_RADIUS_KM: f64 = 6_371.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryGeo {
    pub latitude: f64,
    pub longitude: f64,
    pub radius_km: f64,
}

impl DiscoveryGeo {
    pub fn validate(&self) -> Result<()> {
        if !self.latitude.is_finite() || self.latitude < -90.0 || self.latitude > 90.0 {
            bail!("latitude must be between -90 and 90");
        }
        if !self.longitude.is_finite() || self.longitude < -180.0 || self.longitude > 180.0 {
            bail!("longitude must be between -180 and 180");
        }
        if !self.radius_km.is_finite() || self.radius_km < 0.0 {
            bail!("radius_km must be a non-negative finite number");
        }
        Ok(())
    }

    pub fn distance_km_to(&self, other: &Self) -> f64 {
        haversine_km(
            self.latitude,
            self.longitude,
            other.latitude,
            other.longitude,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryRecordCapabilities {
    #[serde(default)]
    pub services: BTreeSet<String>,
}

impl DiscoveryRecordCapabilities {
    pub fn new(services: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        let mut normalized = BTreeSet::new();
        for service in services {
            let service = service.into();
            normalized.insert(normalize_capability(&service)?);
        }
        Ok(Self {
            services: normalized,
        })
    }

    pub fn contains(&self, service: &str) -> bool {
        normalize_capability(service)
            .map(|service| self.services.contains(&service))
            .unwrap_or(false)
    }
}

impl Default for DiscoveryRecordCapabilities {
    fn default() -> Self {
        Self {
            services: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryNodeRecordBody {
    pub protocol_version: String,
    pub network_id: String,
    pub node_id: String,
    pub signing_public_key_hex: String,
    pub seq: u64,
    pub updated_at_ms: u64,
    pub ttl_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geo: Option<DiscoveryGeo>,
    #[serde(default)]
    pub capabilities: DiscoveryRecordCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_contact: Option<TransportContactMaterial>,
}

impl DiscoveryNodeRecordBody {
    pub fn new(
        network_id: impl Into<String>,
        node_id: impl Into<String>,
        signing_public_key_hex: impl Into<String>,
        seq: u64,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            protocol_version: DISCOVERY_PROTOCOL_VERSION.to_owned(),
            network_id: network_id.into(),
            node_id: node_id.into(),
            signing_public_key_hex: signing_public_key_hex.into(),
            seq,
            updated_at_ms,
            ttl_ms: DEFAULT_RECORD_TTL_MS,
            geo: None,
            capabilities: DiscoveryRecordCapabilities::default(),
            transport_contact: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != DISCOVERY_PROTOCOL_VERSION {
            bail!(
                "unsupported discovery protocol version: {}",
                self.protocol_version
            );
        }
        validate_segment("network_id", &self.network_id)?;
        validate_segment("node_id", &self.node_id)?;
        validate_hex_public_key(&self.signing_public_key_hex)?;
        if self.node_id != self.signing_public_key_hex {
            bail!("node_id must match signing_public_key_hex");
        }
        if self.ttl_ms == 0 {
            bail!("ttl_ms must be > 0");
        }
        if let Some(geo) = &self.geo {
            geo.validate()?;
        }
        if let Some(contact) = &self.transport_contact {
            if contact.peer_id != self.node_id {
                bail!("transport contact peer_id must match node_id");
            }
        }
        Ok(())
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.updated_at_ms.saturating_add(self.ttl_ms)
    }

    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms() <= now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedDiscoveryNodeRecord {
    pub body: DiscoveryNodeRecordBody,
    pub signature_hex: String,
}

impl SignedDiscoveryNodeRecord {
    pub fn sign(body: DiscoveryNodeRecordBody, identity: &NodeIdentity) -> Result<Self> {
        body.validate()?;
        if body.signing_public_key_hex != identity.node_id() {
            bail!("discovery record signer does not match body public key");
        }
        let signing_bytes = canonical_body_bytes(&body)?;
        Ok(Self {
            body,
            signature_hex: identity.sign_bytes(&signing_bytes),
        })
    }

    pub fn verify(&self) -> Result<()> {
        self.body.validate()?;
        let signing_bytes = canonical_body_bytes(&self.body)?;
        verify_signature(
            &self.body.signing_public_key_hex,
            &signing_bytes,
            &self.signature_hex,
        )
        .context("verify discovery record signature")
    }

    pub fn verify_fresh_at(&self, now_ms: u64) -> Result<()> {
        self.verify()?;
        if self.body.is_expired_at(now_ms) {
            bail!("discovery record is expired");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryCandidate {
    pub record: SignedDiscoveryNodeRecord,
    pub distance_km: Option<f64>,
}

#[derive(Debug, Default)]
pub struct DiscoveryRoutingTable {
    records: BTreeMap<String, SignedDiscoveryNodeRecord>,
}

impl DiscoveryRoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn announce_record(
        &mut self,
        record: SignedDiscoveryNodeRecord,
        now_ms: u64,
    ) -> Result<DiscoveryRecordUpsert> {
        record.verify_fresh_at(now_ms)?;
        let node_id = record.body.node_id.clone();
        let incoming_seq = record.body.seq;
        let incoming_updated_at_ms = record.body.updated_at_ms;
        match self.records.get(&node_id) {
            None => {
                self.records.insert(node_id, record);
                Ok(DiscoveryRecordUpsert::Inserted)
            }
            Some(existing)
                if should_replace_record(existing, incoming_seq, incoming_updated_at_ms) =>
            {
                self.records.insert(node_id, record);
                Ok(DiscoveryRecordUpsert::Updated)
            }
            Some(_) => Ok(DiscoveryRecordUpsert::IgnoredStale),
        }
    }

    pub fn find_node(&self, node_id: &str, now_ms: u64) -> Option<SignedDiscoveryNodeRecord> {
        self.records
            .get(node_id)
            .filter(|record| !record.body.is_expired_at(now_ms))
            .cloned()
    }

    pub fn active_records(&self, now_ms: u64, limit: usize) -> Vec<SignedDiscoveryNodeRecord> {
        self.records
            .values()
            .filter(|record| !record.body.is_expired_at(now_ms))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn find_capability(
        &self,
        network_id: &str,
        capability: &str,
        now_ms: u64,
        limit: usize,
    ) -> Vec<SignedDiscoveryNodeRecord> {
        self.records
            .values()
            .filter(|record| {
                record.body.network_id == network_id
                    && !record.body.is_expired_at(now_ms)
                    && record.body.capabilities.contains(capability)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn find_nearby(
        &self,
        network_id: &str,
        origin: &DiscoveryGeo,
        now_ms: u64,
        limit: usize,
    ) -> Vec<DiscoveryCandidate> {
        let mut candidates = self
            .records
            .values()
            .filter_map(|record| {
                if record.body.network_id != network_id || record.body.is_expired_at(now_ms) {
                    return None;
                }
                let remote_geo = record.body.geo.as_ref()?;
                let distance_km = origin.distance_km_to(remote_geo);
                let allowed_radius_km = origin.radius_km.min(remote_geo.radius_km);
                if distance_km > allowed_radius_km {
                    return None;
                }
                Some(DiscoveryCandidate {
                    record: record.clone(),
                    distance_km: Some(distance_km),
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.distance_km
                .partial_cmp(&right.distance_km)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.record.body.node_id.cmp(&right.record.body.node_id))
        });
        candidates.truncate(limit);
        candidates
    }

    pub fn prune_expired(&mut self, now_ms: u64) -> usize {
        let before = self.records.len();
        self.records
            .retain(|_, record| !record.body.is_expired_at(now_ms));
        before - self.records.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryRecordUpsert {
    Inserted,
    Updated,
    IgnoredStale,
}

#[derive(Debug, Default)]
pub struct DiscoveryBootnode {
    routing_table: DiscoveryRoutingTable,
}

impl DiscoveryBootnode {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn announce_record(
        &mut self,
        record: SignedDiscoveryNodeRecord,
        now_ms: u64,
    ) -> Result<DiscoveryRecordUpsert> {
        self.routing_table.announce_record(record, now_ms)
    }

    pub fn find_node(&self, node_id: &str, now_ms: u64) -> Option<SignedDiscoveryNodeRecord> {
        self.routing_table.find_node(node_id, now_ms)
    }

    pub fn active_records(&self, now_ms: u64, limit: usize) -> Vec<SignedDiscoveryNodeRecord> {
        self.routing_table.active_records(now_ms, limit)
    }

    pub fn find_nearby(
        &self,
        network_id: &str,
        origin: &DiscoveryGeo,
        now_ms: u64,
        limit: usize,
    ) -> Vec<DiscoveryCandidate> {
        self.routing_table
            .find_nearby(network_id, origin, now_ms, limit)
    }

    pub fn find_capability(
        &self,
        network_id: &str,
        capability: &str,
        now_ms: u64,
        limit: usize,
    ) -> Vec<SignedDiscoveryNodeRecord> {
        self.routing_table
            .find_capability(network_id, capability, now_ms, limit)
    }
}

fn canonical_body_bytes(body: &DiscoveryNodeRecordBody) -> Result<Vec<u8>> {
    serde_jcs::to_vec(body).map_err(|err| anyhow!("canonicalize discovery record: {err}"))
}

fn should_replace_record(
    existing: &SignedDiscoveryNodeRecord,
    incoming_seq: u64,
    incoming_updated_at_ms: u64,
) -> bool {
    incoming_seq > existing.body.seq
        || (incoming_seq == existing.body.seq
            && incoming_updated_at_ms > existing.body.updated_at_ms)
}

fn validate_segment(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{label} cannot be empty");
    }
    if trimmed != value {
        bail!("{label} cannot contain leading or trailing whitespace");
    }
    if value.chars().any(char::is_whitespace) {
        bail!("{label} cannot contain whitespace");
    }
    Ok(())
}

fn validate_hex_public_key(value: &str) -> Result<()> {
    validate_segment("signing_public_key_hex", value)?;
    let bytes = hex::decode(value).context("decode signing_public_key_hex")?;
    if bytes.len() != 32 {
        bail!("signing_public_key_hex must decode to 32 bytes");
    }
    Ok(())
}

fn normalize_capability(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("capability cannot be empty");
    }
    if trimmed.chars().any(char::is_whitespace) {
        bail!("capability cannot contain whitespace");
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn haversine_km(lat_a: f64, lon_a: f64, lat_b: f64, lon_b: f64) -> f64 {
    let lat_a = lat_a.to_radians();
    let lat_b = lat_b.to_radians();
    let delta_lat = lat_b - lat_a;
    let delta_lon = (lon_b - lon_a).to_radians();
    let sin_lat = (delta_lat / 2.0).sin();
    let sin_lon = (delta_lon / 2.0).sin();
    let a = sin_lat * sin_lat + lat_a.cos() * lat_b.cos() * sin_lon * sin_lon;
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_KM * c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        identity: &NodeIdentity,
        network_id: &str,
        seq: u64,
        updated_at_ms: u64,
        lat: f64,
        lon: f64,
        radius_km: f64,
        capabilities: &[&str],
    ) -> SignedDiscoveryNodeRecord {
        let node_id = identity.node_id();
        let mut body =
            DiscoveryNodeRecordBody::new(network_id, node_id.clone(), node_id, seq, updated_at_ms);
        body.geo = Some(DiscoveryGeo {
            latitude: lat,
            longitude: lon,
            radius_km,
        });
        body.capabilities = DiscoveryRecordCapabilities::new(capabilities.iter().copied()).unwrap();
        SignedDiscoveryNodeRecord::sign(body, identity).unwrap()
    }

    #[test]
    fn signed_record_verifies_and_rejects_tampering() {
        let identity = NodeIdentity::from_seed([7; 32]);
        let mut signed = record(
            &identity,
            "mainnet:wattetheria",
            1,
            1_000,
            37.0,
            -122.0,
            100.0,
            &["wattswarm.node"],
        );
        signed.verify_fresh_at(1_500).unwrap();

        signed.body.network_id = "other-network".to_owned();
        assert!(signed.verify().is_err());
    }

    #[test]
    fn routing_table_uses_seq_and_timestamp_for_idempotent_replacement() {
        let identity = NodeIdentity::from_seed([8; 32]);
        let mut table = DiscoveryRoutingTable::new();
        let first = record(&identity, "net", 1, 1_000, 0.0, 0.0, 10.0, &[]);
        let older = record(&identity, "net", 1, 900, 0.0, 0.0, 10.0, &[]);
        let newer = record(&identity, "net", 2, 1_100, 0.0, 0.0, 10.0, &[]);

        assert_eq!(
            table.announce_record(first, 1_001).unwrap(),
            DiscoveryRecordUpsert::Inserted
        );
        assert_eq!(
            table.announce_record(older, 1_001).unwrap(),
            DiscoveryRecordUpsert::IgnoredStale
        );
        assert_eq!(
            table.announce_record(newer, 1_101).unwrap(),
            DiscoveryRecordUpsert::Updated
        );
        assert_eq!(table.len(), 1);
        assert_eq!(
            table
                .find_node(&identity.node_id(), 1_101)
                .unwrap()
                .body
                .seq,
            2
        );
    }

    #[test]
    fn nearby_query_filters_network_ttl_and_radius() {
        let near_identity = NodeIdentity::from_seed([1; 32]);
        let far_identity = NodeIdentity::from_seed([2; 32]);
        let other_network_identity = NodeIdentity::from_seed([3; 32]);
        let mut table = DiscoveryRoutingTable::new();
        table
            .announce_record(
                record(
                    &near_identity,
                    "net",
                    1,
                    1_000,
                    37.78,
                    -122.41,
                    50.0,
                    &["wattswarm.node"],
                ),
                1_001,
            )
            .unwrap();
        table
            .announce_record(
                record(
                    &far_identity,
                    "net",
                    1,
                    1_000,
                    40.71,
                    -74.00,
                    50.0,
                    &["wattswarm.node"],
                ),
                1_001,
            )
            .unwrap();
        table
            .announce_record(
                record(
                    &other_network_identity,
                    "other",
                    1,
                    1_000,
                    37.78,
                    -122.41,
                    50.0,
                    &["wattswarm.node"],
                ),
                1_001,
            )
            .unwrap();

        let origin = DiscoveryGeo {
            latitude: 37.77,
            longitude: -122.42,
            radius_km: 50.0,
        };
        let nearby = table.find_nearby("net", &origin, 2_000, 10);
        assert_eq!(nearby.len(), 1);
        assert_eq!(nearby[0].record.body.node_id, near_identity.node_id());

        assert!(table.find_nearby("net", &origin, 1_000_000, 10).is_empty());
    }

    #[test]
    fn capability_query_returns_only_matching_network_capability() {
        let gateway_identity = NodeIdentity::from_seed([4; 32]);
        let node_identity = NodeIdentity::from_seed([5; 32]);
        let mut bootnode = DiscoveryBootnode::new();
        bootnode
            .announce_record(
                record(
                    &gateway_identity,
                    "net",
                    1,
                    1_000,
                    0.0,
                    0.0,
                    1_000.0,
                    &["gateway.node", "discovery.bootnode"],
                ),
                1_001,
            )
            .unwrap();
        bootnode
            .announce_record(
                record(
                    &node_identity,
                    "net",
                    1,
                    1_000,
                    0.0,
                    0.0,
                    1_000.0,
                    &["wattswarm.node"],
                ),
                1_001,
            )
            .unwrap();

        let gateways = bootnode.find_capability("net", "GATEWAY.NODE", 2_000, 10);
        assert_eq!(gateways.len(), 1);
        assert_eq!(gateways[0].body.node_id, gateway_identity.node_id());
    }

    #[test]
    fn expired_records_are_pruned() {
        let identity = NodeIdentity::from_seed([6; 32]);
        let mut table = DiscoveryRoutingTable::new();
        table
            .announce_record(
                record(&identity, "net", 1, 1_000, 0.0, 0.0, 10.0, &[]),
                1_001,
            )
            .unwrap();
        assert_eq!(table.prune_expired(1_100), 0);
        assert_eq!(table.prune_expired(DEFAULT_RECORD_TTL_MS + 1_001), 1);
        assert!(table.is_empty());
    }

    #[test]
    fn active_records_omits_expired_records() {
        let active_identity = NodeIdentity::from_seed([9; 32]);
        let expired_identity = NodeIdentity::from_seed([10; 32]);
        let mut table = DiscoveryRoutingTable::new();
        table
            .announce_record(
                record(&active_identity, "net", 1, 10_000, 0.0, 0.0, 10.0, &[]),
                10_001,
            )
            .unwrap();
        let mut expired_body = record(&expired_identity, "net", 1, 1_000, 0.0, 0.0, 10.0, &[]).body;
        expired_body.ttl_ms = 1_000;
        table
            .announce_record(
                SignedDiscoveryNodeRecord::sign(expired_body, &expired_identity).unwrap(),
                1_001,
            )
            .unwrap();

        let records = table.active_records(10_100, 10);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].body.node_id, active_identity.node_id());
    }
}
