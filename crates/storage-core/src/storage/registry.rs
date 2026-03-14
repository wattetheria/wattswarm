use super::*;
use wattswarm_protocol::types::NetworkProtocolParams;

pub const DEFAULT_LOCAL_ORG_NAME: &str = "Local Bootstrap";
pub const DEFAULT_LAN_ORG_NAME: &str = "LAN Bootstrap";

pub fn local_network_id(node_id: &str) -> String {
    format!("local:{node_id}")
}

pub fn lan_network_id(node_id: &str) -> String {
    format!("lan:{node_id}")
}

pub fn bootstrap_org_id(network_id: &str) -> String {
    format!("{network_id}:bootstrap")
}

impl PgStore {
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

    fn ensure_bootstrap_topology(
        &self,
        network_id: &str,
        network_kind: &str,
        network_name: &str,
        control_mode: &str,
        org_name: &str,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<String> {
        let org_id = bootstrap_org_id(network_id);
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;

        Self::ensure_node_identity_unique(&conn, node_id, public_key)?;

        conn.execute(
            "INSERT INTO network_registry(network_id, network_kind, parent_network_id, name, status, genesis_node_id, created_at)
             VALUES ($1, $2, NULL, $3, 'active', $4, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(network_id) DO UPDATE SET
               network_kind = excluded.network_kind,
               name = excluded.name,
               status = excluded.status,
               genesis_node_id = excluded.genesis_node_id",
            params![
                network_id,
                network_kind,
                network_name,
                node_id,
                now as i64
            ],
        )?;

        let default_params_json = serde_json::to_string(&NetworkProtocolParams::default())
            .unwrap_or_else(|_| "{}".into());
        conn.execute(
            "INSERT INTO network_params(network_id, control_mode, membership_version, policy_version, params_json, created_at, updated_at)
             VALUES ($1, $2, 1, 1, $4, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(network_id) DO UPDATE SET
               control_mode = excluded.control_mode,
               updated_at = excluded.updated_at",
            params![network_id, control_mode, now as i64, default_params_json],
        )?;

        conn.execute(
            "INSERT INTO node_registry(node_id, public_key, home_network_id, status, created_at, last_seen_at)
             VALUES ($1, $2, $3, 'active', TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(node_id) DO UPDATE SET
               home_network_id = excluded.home_network_id,
               status = excluded.status,
               last_seen_at = excluded.last_seen_at",
            params![node_id, public_key, network_id, now as i64],
        )?;

        conn.execute(
            "INSERT INTO node_network_membership(node_id, network_id, joined_at)
             VALUES ($1, $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(node_id, network_id) DO NOTHING",
            params![node_id, network_id, now as i64],
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

        Ok(org_id)
    }

    pub fn ensure_local_bootstrap_topology(
        &self,
        node_id: &str,
        public_key: &str,
        now: u64,
    ) -> Result<String> {
        let network_id = local_network_id(node_id);
        self.ensure_bootstrap_topology(
            &network_id,
            "local",
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
        let network_id = lan_network_id(node_id);
        self.ensure_bootstrap_topology(
            &network_id,
            "lan",
            &format!("LAN Network {node_id}"),
            "lan_owner",
            DEFAULT_LAN_ORG_NAME,
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
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;

        Self::ensure_node_identity_unique(&conn, node_id, public_key)?;

        let rows = conn
            .prepare(
                "SELECT org.org_id, org.network_id
                 FROM org_registry org
                 JOIN network_registry net ON net.network_id = org.network_id
                 WHERE org.is_default = TRUE
                   AND org.status = 'active'
                   AND net.status = 'active'
                   AND net.network_kind NOT IN ('local', 'lan')
                 ORDER BY org.created_at ASC
                 LIMIT 2",
            )?
            .query_map(params![], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let (org_id, network_id) = match rows.as_slice() {
            [] => anyhow::bail!(
                "network mode requires a preconfigured network_registry/org_registry default bootstrap org"
            ),
            [row] => row.clone(),
            _ => anyhow::bail!(
                "network mode found multiple active default network orgs; keep exactly one default org in org_registry"
            ),
        };

        conn.execute(
            "INSERT INTO node_registry(node_id, public_key, home_network_id, status, created_at, last_seen_at)
             VALUES ($1, $2, $3, 'active', TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'), TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(node_id) DO UPDATE SET
               home_network_id = excluded.home_network_id,
               status = excluded.status,
               last_seen_at = excluded.last_seen_at",
            params![node_id, public_key, network_id, now as i64],
        )?;

        conn.execute(
            "INSERT INTO node_network_membership(node_id, network_id, joined_at)
             VALUES ($1, $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(node_id, network_id) DO NOTHING",
            params![node_id, network_id, now as i64],
        )?;

        Ok(org_id)
    }

    /// Read `NetworkProtocolParams` from the `network_params` table for the
    /// network that owns this store's org. Returns `Default` when the store
    /// has no org bound or no matching row exists (e.g. in-memory test stores
    /// that skip bootstrap topology).
    pub fn load_network_protocol_params(&self) -> Result<NetworkProtocolParams> {
        if !self.is_org_configured() {
            return Ok(NetworkProtocolParams::default());
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let row: Option<String> = conn
            .query_row(
                "SELECT np.params_json
                 FROM network_params np
                 JOIN org_registry org ON org.network_id = np.network_id
                 WHERE org.org_id = $1",
                params![self.org_id()],
                |r| r.get(0),
            )
            .optional()?;
        match row {
            Some(json) if !json.is_empty() && json != "{}" => Ok(serde_json::from_str(&json)?),
            _ => Ok(NetworkProtocolParams::default()),
        }
    }

    /// Write `NetworkProtocolParams` into the `network_params.params_json`
    /// column for the given network. Only the genesis node or governance
    /// proposals should call this.
    pub fn put_network_protocol_params(
        &self,
        network_id: &str,
        params: &NetworkProtocolParams,
    ) -> Result<()> {
        let json = serde_json::to_string(params)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE network_params SET params_json = $1, updated_at = NOW() WHERE network_id = $2",
            params![json, network_id],
        )?;
        Ok(())
    }
}
