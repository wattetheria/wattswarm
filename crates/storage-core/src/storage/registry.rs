use super::*;
use crate::crypto::{NodeIdentity, sha256_hex, verify_signature};
use wattswarm_protocol::types::{
    NetworkBootstrapBundle, NetworkDescriptor, NetworkKind, NetworkProtocolParams, NetworkTopology,
    OrgDescriptor, SignedNetworkProtocolParamsEnvelope, UnsignedNetworkProtocolParamsEnvelope,
};

pub const DEFAULT_LOCAL_ORG_NAME: &str = "Local Bootstrap";
pub const DEFAULT_LAN_ORG_NAME: &str = "LAN Bootstrap";
pub const DEFAULT_MAINNET_ORG_NAME: &str = "Mainnet Bootstrap";

pub fn local_network_id(node_id: &str) -> String {
    format!("local:{node_id}")
}

pub fn lan_network_id(node_id: &str) -> String {
    format!("lan:{node_id}")
}

pub fn bootstrap_org_id(network_id: &str) -> String {
    format!("{network_id}:bootstrap")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNetworkProtocolParams {
    pub network_id: String,
    pub genesis_node_id: String,
    pub signed: SignedNetworkProtocolParamsEnvelope,
}

impl VerifiedNetworkProtocolParams {
    pub fn params(&self) -> &NetworkProtocolParams {
        &self.signed.params
    }

    pub fn params_hash(&self) -> &str {
        &self.signed.params_hash
    }
}

fn network_params_payload_hash(payload: &UnsignedNetworkProtocolParamsEnvelope) -> Result<String> {
    Ok(sha256_hex(&serde_json::to_vec(payload)?))
}

fn sign_network_protocol_params(
    network_id: &str,
    version: u64,
    prev_hash: Option<String>,
    params: &NetworkProtocolParams,
    signer: &NodeIdentity,
) -> Result<SignedNetworkProtocolParamsEnvelope> {
    let payload = UnsignedNetworkProtocolParamsEnvelope {
        network_id: network_id.to_owned(),
        version,
        prev_hash,
        params: params.clone(),
    };
    let params_hash = network_params_payload_hash(&payload)?;
    Ok(SignedNetworkProtocolParamsEnvelope {
        network_id: payload.network_id,
        version: payload.version,
        prev_hash: payload.prev_hash,
        params_hash: params_hash.clone(),
        params: payload.params,
        signed_by: signer.node_id(),
        signature: signer.sign_bytes(params_hash.as_bytes()),
    })
}

fn verify_network_protocol_params(
    expected_network_id: &str,
    expected_genesis_node_id: &str,
    signed: &SignedNetworkProtocolParamsEnvelope,
) -> Result<()> {
    if signed.network_id != expected_network_id {
        anyhow::bail!(
            "network params network_id mismatch expected={} got={}",
            expected_network_id,
            signed.network_id
        );
    }
    if signed.signed_by != expected_genesis_node_id {
        anyhow::bail!(
            "network params signed_by mismatch expected={} got={}",
            expected_genesis_node_id,
            signed.signed_by
        );
    }
    if signed.version == 0 {
        anyhow::bail!("network params version must be >= 1");
    }
    if signed.version == 1 && signed.prev_hash.is_some() {
        anyhow::bail!("network params version 1 must not include prev_hash");
    }
    if signed.version > 1 && signed.prev_hash.as_deref().unwrap_or("").trim().is_empty() {
        anyhow::bail!("network params version > 1 requires prev_hash");
    }
    let expected_hash = network_params_payload_hash(&signed.unsigned_payload())?;
    if signed.params_hash != expected_hash {
        anyhow::bail!("network params hash mismatch");
    }
    verify_signature(
        expected_genesis_node_id,
        signed.params_hash.as_bytes(),
        &signed.signature,
    )?;
    Ok(())
}

impl PgStore {
    fn parse_network_kind(raw: &str) -> Result<NetworkKind> {
        NetworkKind::parse(raw).ok_or_else(|| anyhow::anyhow!("unsupported network_kind '{raw}'"))
    }

    fn ensure_node_identity_unique(
        conn: &Connection,
        node_id: &str,
        public_key: &str,
    ) -> Result<()> {
        if let Some((existing_node_id, existing_public_key)) = conn
            .query_row(
                "SELECT node_id, public_key FROM node_registry WHERE node_id = $1 OR public_key = $2",
                params![node_id, public_key],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?
        {
            if existing_node_id != node_id || existing_public_key != public_key {
                anyhow::bail!(
                    "node identity conflict: node_id/public_key must be unique; check duplicated node_seed.hex"
                );
            }
        }
        Ok(())
    }

    fn load_network_descriptor_tx(
        conn: &Connection,
        network_id: &str,
    ) -> Result<NetworkDescriptor> {
        let row: Option<(String, String, String, Option<String>, String)> = conn
            .query_row(
                "SELECT network_id, name, network_kind, parent_network_id, genesis_node_id
                 FROM network_registry
                 WHERE network_id = $1",
                params![network_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let Some((network_id, name, network_kind, parent_network_id, genesis_node_id)) = row else {
            anyhow::bail!("missing network_registry row for network {network_id}");
        };
        Ok(NetworkDescriptor {
            network_id,
            network_name: name,
            network_kind: Self::parse_network_kind(&network_kind)?,
            parent_network_id,
            genesis_node_id,
        })
    }

    fn load_org_descriptor_tx(conn: &Connection, org_id: &str) -> Result<OrgDescriptor> {
        let row: Option<(String, String, String, String, bool)> = conn
            .query_row(
                "SELECT org_id, network_id, name, org_kind, is_default
                 FROM org_registry
                 WHERE org_id = $1",
                params![org_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let Some((org_id, network_id, network_org_name, org_kind, is_default)) = row else {
            anyhow::bail!("missing org_registry row for org {org_id}");
        };
        Ok(OrgDescriptor {
            org_id,
            network_id,
            network_org_name,
            org_kind,
            is_default,
        })
    }

    fn load_default_org_descriptor_tx(
        conn: &Connection,
        network_id: &str,
    ) -> Result<OrgDescriptor> {
        let rows = conn
            .prepare(
                "SELECT org_id, network_id, name, org_kind, is_default
                 FROM org_registry
                 WHERE network_id = $1
                   AND is_default = TRUE
                   AND status = 'active'
                 ORDER BY created_at ASC
                 LIMIT 2",
            )?
            .query_map(params![network_id], |r| {
                Ok(OrgDescriptor {
                    org_id: r.get(0)?,
                    network_id: r.get(1)?,
                    network_org_name: r.get(2)?,
                    org_kind: r.get(3)?,
                    is_default: r.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        match rows.as_slice() {
            [] => anyhow::bail!("missing default org for network {network_id}"),
            [org] => Ok(org.clone()),
            _ => anyhow::bail!(
                "multiple active default orgs found for network {network_id}; keep exactly one"
            ),
        }
    }

    fn upsert_node_registry_membership_tx(
        conn: &Connection,
        node_id: &str,
        public_key: &str,
        home_network_id: &str,
        joined_network_ids: &[&str],
        now: u64,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO node_registry(node_id, public_key, home_network_id, status, created_at, last_seen_at)
             VALUES ($1, $2, $3, 'active', TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(node_id) DO UPDATE SET
               status = excluded.status,
               last_seen_at = excluded.last_seen_at",
            params![node_id, public_key, home_network_id, now as i64],
        )?;
        for joined_network_id in joined_network_ids {
            conn.execute(
                "INSERT INTO node_network_membership(node_id, network_id, joined_at)
                 VALUES ($1, $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))
                 ON CONFLICT(node_id, network_id) DO NOTHING",
                params![node_id, joined_network_id, now as i64],
            )?;
        }
        Ok(())
    }

    fn ensure_bootstrap_topology(
        &self,
        network_id: &str,
        network_kind: &str,
        parent_network_id: Option<&str>,
        network_name: &str,
        control_mode: &str,
        org_name: &str,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        let org_id = bootstrap_org_id(network_id);
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;

        Self::ensure_node_identity_unique(&conn, node_id, public_key)?;

        conn.execute(
            "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
             VALUES ($1, $2, $3, $4, 'active', $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(network_id) DO UPDATE SET
               network_kind = excluded.network_kind,
               parent_network_id = excluded.parent_network_id,
               name = excluded.name,
               status = excluded.status,
               genesis_node_id = excluded.genesis_node_id",
            params![
                network_id,
                network_kind,
                parent_network_id,
                network_name,
                node_id,
                now as i64
            ],
        )?;

        conn.execute(
            "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
             VALUES ($1, $2, 1, 1, $4, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(network_id) DO UPDATE SET
               control_mode = excluded.control_mode,
               updated_at = excluded.updated_at",
            params![network_id, control_mode, now as i64, "{}"],
        )?;

        let mut joined_network_ids = vec![network_id];
        if let Some(parent_network_id) = parent_network_id {
            joined_network_ids.push(parent_network_id);
        }
        Self::upsert_node_registry_membership_tx(
            &conn,
            node_id,
            public_key,
            network_id,
            &joined_network_ids,
            now,
        )?;

        conn.execute(
            "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
             VALUES ($1, $2, 'bootstrap', $3, 'active', TRUE, TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id) DO UPDATE SET
               network_id = excluded.network_id,
               name = excluded.name,
               status = excluded.status,
               is_default = excluded.is_default",
            params![
                org_id,
                network_id,
                org_name,
                now as i64
            ],
        )?;

        Ok(NetworkTopology {
            network: Self::load_network_descriptor_tx(&conn, network_id)?,
            org: Self::load_org_descriptor_tx(&conn, &org_id)?,
        })
    }

    pub fn network_id_for_org(&self, org_id: &str) -> Result<String> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.query_row(
            "SELECT network_id FROM org_registry WHERE org_id = $1",
            params![org_id],
            |r| r.get(0),
        )?)
    }

    fn network_genesis_node_id(&self, network_id: &str) -> Result<String> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.query_row(
            "SELECT genesis_node_id FROM network_registry WHERE network_id = $1",
            params![network_id],
            |r| r.get(0),
        )?)
    }

    pub fn ensure_bootstrap_signed_network_protocol_params(
        &self,
        network_id: &str,
        signer: &NodeIdentity,
    ) -> Result<SignedNetworkProtocolParamsEnvelope> {
        let genesis_node_id = self.network_genesis_node_id(network_id)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let existing_json: Option<String> = conn
            .query_row(
                "SELECT params_json FROM network_params WHERE network_id = $1",
                params![network_id],
                |r| r.get(0),
            )
            .optional()?;
        let existing_json = existing_json.unwrap_or_default();
        if !existing_json.trim().is_empty() && existing_json.trim() != "{}" {
            if let Ok(signed) =
                serde_json::from_str::<SignedNetworkProtocolParamsEnvelope>(&existing_json)
            {
                verify_network_protocol_params(network_id, &genesis_node_id, &signed)?;
                return Ok(signed);
            }
        }
        if signer.node_id() != genesis_node_id {
            anyhow::bail!("only the genesis node can sign bootstrap network params");
        }
        let params = if !existing_json.trim().is_empty() && existing_json.trim() != "{}" {
            serde_json::from_str::<NetworkProtocolParams>(&existing_json)?
        } else {
            NetworkProtocolParams::default()
        };
        let signed = sign_network_protocol_params(network_id, 1, None, &params, signer)?;
        conn.execute(
            "UPDATE network_params SET params_json = $1, updated_at = NOW() WHERE network_id = $2",
            params![serde_json::to_string(&signed)?, network_id],
        )?;
        Ok(signed)
    }

    pub fn ensure_local_bootstrap_topology(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<String> {
        Ok(self
            .ensure_local_bootstrap_network_topology(node_id, public_key, now)?
            .org
            .org_id)
    }

    pub fn ensure_local_bootstrap_network_topology(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        let network_id = local_network_id(node_id);
        self.ensure_bootstrap_topology(
            &network_id,
            "local",
            None,
            &format!("Local Network {node_id}"),
            "local_owner",
            DEFAULT_LOCAL_ORG_NAME,
            node_id,
            public_key,
            now,
        )
    }

    pub fn ensure_lan_bootstrap_topology(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<String> {
        Ok(self
            .ensure_lan_bootstrap_network_topology(node_id, public_key, now)?
            .org
            .org_id)
    }

    pub fn ensure_lan_bootstrap_network_topology(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        let network_id = lan_network_id(node_id);
        self.ensure_bootstrap_topology(
            &network_id,
            "lan",
            None,
            &format!("LAN Network {node_id}"),
            "lan_owner",
            DEFAULT_LAN_ORG_NAME,
            node_id,
            public_key,
            now,
        )
    }

    pub fn ensure_mainnet_bootstrap_network_topology(
        &self,
        network_id: &str,
        network_name: &str,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        self.ensure_bootstrap_topology(
            network_id,
            "mainnet",
            None,
            network_name,
            "manual_owner",
            DEFAULT_MAINNET_ORG_NAME,
            node_id,
            public_key,
            now,
        )
    }

    pub fn ensure_subnet_bootstrap_network_topology(
        &self,
        parent_network_id: &str,
        subnet_id: &str,
        subnet_name: &str,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let parent = Self::load_network_descriptor_tx(&conn, parent_network_id)?;
        if !parent.network_kind.is_overlay() {
            anyhow::bail!(
                "subnet parent network must be an overlay; got {}",
                parent.network_kind.as_str()
            );
        }
        drop(conn);
        self.ensure_bootstrap_topology(
            subnet_id,
            "subnet",
            Some(parent_network_id),
            subnet_name,
            "subnet_owner",
            "Subnet Bootstrap",
            node_id,
            public_key,
            now,
        )
    }

    pub fn resolve_network_bootstrap_topology(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<String> {
        Ok(self
            .resolve_network_bootstrap_topology_descriptor(node_id, public_key, now)?
            .org
            .org_id)
    }

    pub fn resolve_network_bootstrap_topology_descriptor(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;

        Self::ensure_node_identity_unique(&conn, node_id, public_key)?;

        let rows = conn
            .prepare(
                "SELECT org.network_id
                 FROM org_registry org
                 JOIN network_registry net ON net.network_id = org.network_id
                 WHERE org.is_default = TRUE
                   AND org.status = 'active'
                   AND net.status = 'active'
                   AND net.network_kind NOT IN ('local', 'lan')
                 ORDER BY org.created_at ASC
                 LIMIT 2",
            )?
            .query_map(params![], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let network_id = match rows.as_slice() {
            [] => anyhow::bail!(
                "network mode requires a preconfigured network_registry/org_registry default bootstrap org"
            ),
            [row] => row.clone(),
            _ => anyhow::bail!(
                "network mode found multiple active default network orgs; keep exactly one default org in org_registry"
            ),
        };
        drop(conn);
        self.join_node_to_network_topology(&network_id, node_id, public_key, now)
    }

    pub fn load_network_topology_for_org(&self, org_id: &str) -> Result<NetworkTopology> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let org = Self::load_org_descriptor_tx(&conn, org_id)?;
        let network = Self::load_network_descriptor_tx(&conn, &org.network_id)?;
        Ok(NetworkTopology { network, org })
    }

    pub fn load_network_topology_for_network(&self, network_id: &str) -> Result<NetworkTopology> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let network = Self::load_network_descriptor_tx(&conn, network_id)?;
        let org = Self::load_default_org_descriptor_tx(&conn, network_id)?;
        Ok(NetworkTopology { network, org })
    }

    pub fn load_parent_network_topology_for_org(
        &self,
        org_id: &str,
    ) -> Result<Option<NetworkTopology>> {
        let topology = self.load_network_topology_for_org(org_id)?;
        let Some(parent_network_id) = topology.network.parent_network_id.as_deref() else {
            return Ok(None);
        };
        self.load_network_topology_for_network(parent_network_id)
            .map(Some)
    }

    pub fn list_discoverable_network_topologies(
        &self,
        limit: usize,
    ) -> Result<Vec<NetworkTopology>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let network_ids = conn
            .prepare(
                "SELECT net.network_id
                 FROM network_registry net
                 JOIN org_registry org ON org.network_id = net.network_id
                 WHERE net.status = 'active'
                   AND org.status = 'active'
                   AND org.is_default = TRUE
                 ORDER BY net.created_at ASC, net.network_id ASC
                 LIMIT $1",
            )?
            .query_map(params![limit as i64], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(conn);

        let mut topologies = Vec::with_capacity(network_ids.len());
        for network_id in network_ids {
            topologies.push(self.load_network_topology_for_network(&network_id)?);
        }
        Ok(topologies)
    }

    pub fn node_has_network_membership(&self, node_id: &str, network_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let exists = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM node_network_membership
                WHERE node_id = $1 AND network_id = $2
            )",
            params![node_id, network_id],
            |r| r.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub fn join_node_to_network_topology(
        &self,
        network_id: &str,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<NetworkTopology> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Self::ensure_node_identity_unique(&conn, node_id, public_key)?;
        let network = Self::load_network_descriptor_tx(&conn, network_id)?;
        let org = Self::load_default_org_descriptor_tx(&conn, network_id)?;
        let mut joined_network_ids = vec![network_id];
        if let Some(parent_network_id) = network.parent_network_id.as_deref() {
            joined_network_ids.push(parent_network_id);
        }
        Self::upsert_node_registry_membership_tx(
            &conn,
            node_id,
            public_key,
            network_id,
            &joined_network_ids,
            now,
        )?;
        Ok(NetworkTopology { network, org })
    }

    pub fn load_verified_network_protocol_params(&self) -> Result<VerifiedNetworkProtocolParams> {
        if !self.is_org_configured() {
            anyhow::bail!("store org is not configured");
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let row: Option<(String, String, String)> = conn
            .query_row(
                "SELECT np.network_id, net.genesis_node_id, np.params_json
                 FROM network_params np
                 JOIN org_registry org ON org.network_id = np.network_id
                 JOIN network_registry net ON net.network_id = np.network_id
                 WHERE org.org_id = $1",
                params![self.org_id()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((network_id, genesis_node_id, json)) = row else {
            anyhow::bail!("missing network_params for org {}", self.org_id());
        };
        if json.trim().is_empty() || json.trim() == "{}" {
            anyhow::bail!("missing signed network params envelope for network {network_id}");
        }
        let signed: SignedNetworkProtocolParamsEnvelope = serde_json::from_str(&json)?;
        verify_network_protocol_params(&network_id, &genesis_node_id, &signed)?;
        Ok(VerifiedNetworkProtocolParams {
            network_id,
            genesis_node_id,
            signed,
        })
    }

    pub fn load_network_bootstrap_bundle(&self) -> Result<NetworkBootstrapBundle> {
        if !self.is_org_configured() {
            anyhow::bail!("store org is not configured");
        }
        let topology = self.load_network_topology_for_org(self.org_id())?;
        let verified = self.load_verified_network_protocol_params()?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let row: Option<(String, i64, i64)> = conn
            .query_row(
                "SELECT control_mode, membership_version, policy_version
                 FROM network_params
                 WHERE network_id = $1",
                params![&verified.network_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((control_mode, membership_version, policy_version)) = row else {
            anyhow::bail!(
                "missing network_params metadata for network {}",
                verified.network_id
            );
        };
        Ok(NetworkBootstrapBundle {
            topology,
            control_mode,
            membership_version: membership_version as u64,
            policy_version: policy_version as u64,
            signed_params: verified.signed,
        })
    }

    /// Read verified `NetworkProtocolParams` for the network that owns this store's org.
    pub fn load_network_protocol_params(&self) -> Result<NetworkProtocolParams> {
        Ok(self.load_verified_network_protocol_params()?.signed.params)
    }

    /// Write a new signed `NetworkProtocolParams` envelope into the
    /// `network_params.params_json` cache for the given network.
    pub fn put_network_protocol_params(
        &self,
        network_id: &str,
        signer: &NodeIdentity,
        params: &NetworkProtocolParams,
    ) -> Result<SignedNetworkProtocolParamsEnvelope> {
        let genesis_node_id = self.network_genesis_node_id(network_id)?;
        if signer.node_id() != genesis_node_id {
            anyhow::bail!("only the genesis node can update network params");
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let current_json: Option<String> = conn
            .query_row(
                "SELECT params_json FROM network_params WHERE network_id = $1",
                params![network_id],
                |r| r.get(0),
            )
            .optional()?;
        let (version, prev_hash) = match current_json {
            Some(json) if !json.trim().is_empty() && json.trim() != "{}" => {
                if let Ok(signed) =
                    serde_json::from_str::<SignedNetworkProtocolParamsEnvelope>(&json)
                {
                    verify_network_protocol_params(network_id, &genesis_node_id, &signed)?;
                    (signed.version + 1, Some(signed.params_hash))
                } else {
                    let _: NetworkProtocolParams = serde_json::from_str(&json)?;
                    (1, None)
                }
            }
            _ => (1, None),
        };
        let signed = sign_network_protocol_params(network_id, version, prev_hash, params, signer)?;
        let json = serde_json::to_string(&signed)?;
        conn.execute(
            "UPDATE network_params SET params_json = $1, updated_at = NOW() WHERE network_id = $2",
            params![json, network_id],
        )?;
        Ok(signed)
    }

    pub fn import_network_bootstrap_bundle(&self, bundle: &NetworkBootstrapBundle) -> Result<()> {
        let network = &bundle.topology.network;
        let org = &bundle.topology.org;
        if network.network_id != bundle.signed_params.network_id {
            anyhow::bail!("bootstrap bundle network_id mismatch");
        }
        if org.network_id != network.network_id {
            anyhow::bail!("bootstrap bundle org/network mismatch");
        }
        if !network.network_kind.is_overlay() {
            anyhow::bail!("bootstrap bundle requires overlay network kind");
        }
        verify_network_protocol_params(
            &network.network_id,
            &network.genesis_node_id,
            &bundle.signed_params,
        )?;

        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
             VALUES ($1, $2, $3, $4, 'active', $5, NOW())
             ON CONFLICT(network_id) DO UPDATE SET
               network_kind = excluded.network_kind,
               parent_network_id = excluded.parent_network_id,
               name = excluded.name,
               status = 'active',
               genesis_node_id = excluded.genesis_node_id",
            params![
                &network.network_id,
                network.network_kind.as_str(),
                &network.parent_network_id,
                &network.network_name,
                &network.genesis_node_id,
            ],
        )?;
        conn.execute(
            "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
             ON CONFLICT(network_id) DO UPDATE SET
               control_mode = excluded.control_mode,
               membership_version = excluded.membership_version,
               policy_version = excluded.policy_version,
               params_json = excluded.params_json,
               updated_at = NOW()",
            params![
                &network.network_id,
                &bundle.control_mode,
                bundle.membership_version as i64,
                bundle.policy_version as i64,
                serde_json::to_string(&bundle.signed_params)?,
            ],
        )?;
        conn.execute(
            "INSERT INTO org_registry(org_id, network_id, org_kind, name, status, is_default, created_at)
             VALUES ($1, $2, $3, $4, 'active', $5, NOW())
             ON CONFLICT(org_id) DO UPDATE SET
               network_id = excluded.network_id,
               org_kind = excluded.org_kind,
               name = excluded.name,
               status = 'active',
               is_default = excluded.is_default",
            params![
                &org.org_id,
                &org.network_id,
                &org.org_kind,
                &org.network_org_name,
                org.is_default,
            ],
        )?;
        Ok(())
    }
}
