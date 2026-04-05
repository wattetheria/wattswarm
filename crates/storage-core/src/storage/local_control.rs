use super::*;
use std::path::Path;

const LOCAL_CONFIG_KEY_EGRESS_AGENT: &str = "egress_agent";
const LOCAL_CONTROL_SCOPE_SEP: &str = "\u{1f}";

pub fn local_control_store(state_dir: &Path) -> Result<PgStore> {
    PgStore::open(state_dir.join("local-control.state"))
}

pub fn local_control_scope_id(state_dir: &Path) -> String {
    state_dir
        .canonicalize()
        .unwrap_or_else(|_| state_dir.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn scoped_key(scope_id: &str, key: &str) -> String {
    format!("{scope_id}{LOCAL_CONTROL_SCOPE_SEP}{key}")
}

fn escaped_like_prefix(scope_id: &str) -> String {
    let raw = format!("{scope_id}{LOCAL_CONTROL_SCOPE_SEP}");
    let mut escaped = String::with_capacity(raw.len() + 1);
    for ch in raw.chars() {
        match ch {
            '%' | '_' | '\\' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped.push('%');
    escaped
}

fn strip_scoped_key(scope_id: &str, value: &str) -> Option<String> {
    value
        .strip_prefix(&format!("{scope_id}{LOCAL_CONTROL_SCOPE_SEP}"))
        .map(ToOwned::to_owned)
}

impl PgStore {
    pub fn list_local_executors(&self, scope_id: &str) -> Result<Vec<LocalExecutorEntryRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT executor_name, base_url,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
             FROM executor_registry_local
             WHERE scope_id = $1
             ORDER BY executor_name ASC",
        )?;
        let rows = stmt.query_map(params![scope_id], |r| {
            Ok(LocalExecutorEntryRow {
                name: r.get(0)?,
                base_url: r.get(1)?,
                updated_at: r.get::<_, i64>(2)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn replace_local_executors(
        &self,
        scope_id: &str,
        entries: &[LocalExecutorEntryRow],
        updated_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute_batch("BEGIN")?;
        let result: Result<()> = (|| {
            conn.execute(
                "DELETE FROM executor_registry_local WHERE scope_id = $1",
                params![scope_id],
            )?;
            for entry in entries {
                conn.execute(
                    "INSERT INTO executor_registry_local(scope_id, executor_name, base_url, updated_at)
                     VALUES (
                        $1,
                        $2,
                        $3,
                        TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                     )",
                    params![
                        scope_id,
                        &entry.name,
                        entry.base_url,
                        updated_at as i64
                    ],
                )?;
            }
            Ok(())
        })();
        if result.is_ok() {
            conn.execute_batch("COMMIT")?;
        } else {
            let _ = conn.execute_batch("ROLLBACK");
        }
        result
    }

    pub fn list_local_discovered_peers(
        &self,
        scope_id: &str,
    ) -> Result<Vec<LocalDiscoveredPeerRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT node_id,
                    listen_addr,
                    source_kind,
                    (EXTRACT(EPOCH FROM discovered_at) * 1000)::BIGINT AS discovered_at_ms,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
             FROM discovered_peers_local
             WHERE scope_id = $1
             ORDER BY node_id ASC",
        )?;
        let rows = stmt.query_map(params![scope_id], |r| {
            Ok(LocalDiscoveredPeerRow {
                node_id: r.get(0)?,
                listen_addr: r.get(1)?,
                source_kind: r.get(2)?,
                discovered_at: r.get::<_, i64>(3)? as u64,
                updated_at: r.get::<_, i64>(4)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn replace_local_discovered_peers(
        &self,
        scope_id: &str,
        peers: &[LocalDiscoveredPeerRow],
        now: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute_batch("BEGIN")?;
        let result: Result<()> = (|| {
            conn.execute(
                "DELETE FROM discovered_peers_local WHERE scope_id = $1",
                params![scope_id],
            )?;
            for peer in peers {
                conn.execute(
                    "INSERT INTO discovered_peers_local(scope_id, node_id, listen_addr, source_kind, discovered_at, updated_at)
                     VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                        TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
                     )",
                    params![
                        scope_id,
                        &peer.node_id,
                        peer.listen_addr,
                        &peer.source_kind,
                        peer.discovered_at as i64,
                        now as i64
                    ],
                )?;
            }
            Ok(())
        })();
        if result.is_ok() {
            conn.execute_batch("COMMIT")?;
        } else {
            let _ = conn.execute_batch("ROLLBACK");
        }
        result
    }

    pub fn upsert_local_discovered_peer(
        &self,
        scope_id: &str,
        node_id: &str,
        listen_addr: Option<&str>,
        source_kind: &str,
        now: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let existing = conn
            .query_row(
                "SELECT listen_addr, source_kind
                 FROM discovered_peers_local
                 WHERE scope_id = $1 AND node_id = $2",
                params![scope_id, node_id],
                |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        let normalized = listen_addr
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let normalized_source = source_kind.trim();
        let normalized_source = if normalized_source.is_empty() {
            "unknown"
        } else {
            normalized_source
        };
        match existing {
            Some((current_addr, current_source))
                if (normalized.is_none() || current_addr == normalized)
                    && current_source == normalized_source =>
            {
                Ok(false)
            }
            Some(_) => {
                conn.execute(
                    "UPDATE discovered_peers_local
                     SET listen_addr = $2,
                         source_kind = $3,
                         updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                     WHERE scope_id = $1 AND node_id = $5",
                    params![scope_id, normalized, normalized_source, now as i64, node_id],
                )?;
                Ok(true)
            }
            None => {
                conn.execute(
                    "INSERT INTO discovered_peers_local(scope_id, node_id, listen_addr, source_kind, discovered_at, updated_at)
                     VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                        TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
                     )",
                    params![scope_id, node_id, normalized, normalized_source, now as i64],
                )?;
                Ok(true)
            }
        }
    }

    pub fn list_local_peer_metadata(&self, scope_id: &str) -> Result<Vec<LocalPeerMetadataRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT node_id,
                    network_id,
                    params_version,
                    params_hash,
                    agent_version_raw,
                    agent_version_prefix,
                    protocol_version,
                    observed_addr,
                    listen_addrs_json,
                    protocols_json,
                    handshake_status,
                    last_error,
                    contact_material_json,
                    contact_material_signature,
                    (EXTRACT(EPOCH FROM contact_material_updated_at) * 1000)::BIGINT AS contact_material_updated_at_ms,
                    (EXTRACT(EPOCH FROM first_identified_at) * 1000)::BIGINT AS first_identified_at_ms,
                    (EXTRACT(EPOCH FROM last_identified_at) * 1000)::BIGINT AS last_identified_at_ms
             FROM peer_metadata_local
             WHERE scope_id = $1
             ORDER BY node_id ASC",
        )?;
        let rows = stmt.query_map(params![scope_id], |r| {
            Ok(LocalPeerMetadataRow {
                node_id: r.get(0)?,
                network_id: r.get(1)?,
                params_version: r.get::<_, Option<i64>>(2)?.map(|value| value as u64),
                params_hash: r.get(3)?,
                agent_version_raw: r.get(4)?,
                agent_version_prefix: r.get(5)?,
                protocol_version: r.get(6)?,
                observed_addr: r.get(7)?,
                listen_addrs_json: r.get(8)?,
                protocols_json: r.get(9)?,
                handshake_status: r.get(10)?,
                last_error: r.get(11)?,
                contact_material_json: r.get(12)?,
                contact_material_signature: r.get(13)?,
                contact_material_updated_at: r.get::<_, Option<i64>>(14)?.map(|value| value as u64),
                first_identified_at: r.get::<_, i64>(15)? as u64,
                last_identified_at: r.get::<_, i64>(16)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn upsert_local_peer_metadata(
        &self,
        scope_id: &str,
        row: &LocalPeerMetadataRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO peer_metadata_local(
                scope_id, node_id, network_id, params_version, params_hash,
                agent_version_raw, agent_version_prefix, protocol_version, observed_addr,
                listen_addrs_json, protocols_json, handshake_status, last_error,
                contact_material_json, contact_material_signature, contact_material_updated_at,
                first_identified_at, last_identified_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15,
                CASE WHEN $16::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($16::bigint * INTERVAL '1 millisecond') END,
                TIMESTAMPTZ 'epoch' + ($17::bigint * INTERVAL '1 millisecond'),
                TIMESTAMPTZ 'epoch' + ($18::bigint * INTERVAL '1 millisecond')
             )
             ON CONFLICT(scope_id, node_id) DO UPDATE SET
                network_id = excluded.network_id,
                params_version = excluded.params_version,
                params_hash = excluded.params_hash,
                agent_version_raw = excluded.agent_version_raw,
                agent_version_prefix = excluded.agent_version_prefix,
                protocol_version = excluded.protocol_version,
                observed_addr = excluded.observed_addr,
                listen_addrs_json = excluded.listen_addrs_json,
                protocols_json = excluded.protocols_json,
                handshake_status = excluded.handshake_status,
                last_error = excluded.last_error,
                contact_material_json = excluded.contact_material_json,
                contact_material_signature = excluded.contact_material_signature,
                contact_material_updated_at = excluded.contact_material_updated_at,
                last_identified_at = excluded.last_identified_at",
            params![
                scope_id,
                &row.node_id,
                row.network_id,
                row.params_version.map(|value| value as i64),
                row.params_hash,
                row.agent_version_raw,
                row.agent_version_prefix,
                row.protocol_version,
                row.observed_addr,
                row.listen_addrs_json,
                row.protocols_json,
                row.handshake_status,
                row.last_error,
                row.contact_material_json,
                row.contact_material_signature,
                row.contact_material_updated_at.map(|value| value as i64).unwrap_or(-1),
                row.first_identified_at as i64,
                row.last_identified_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_local_peer_relationships(
        &self,
        scope_id: &str,
    ) -> Result<Vec<LocalPeerRelationshipRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT remote_node_id,
                    relationship_state,
                    last_action,
                    initiated_by,
                    (EXTRACT(EPOCH FROM requested_at) * 1000)::BIGINT AS requested_at_ms,
                    (EXTRACT(EPOCH FROM responded_at) * 1000)::BIGINT AS responded_at_ms,
                    (EXTRACT(EPOCH FROM blocked_at) * 1000)::BIGINT AS blocked_at_ms,
                    (EXTRACT(EPOCH FROM cleared_at) * 1000)::BIGINT AS cleared_at_ms,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
             FROM peer_relationships_local
             WHERE scope_id = $1
             ORDER BY remote_node_id ASC",
        )?;
        let rows = stmt.query_map(params![scope_id], |r| {
            Ok(LocalPeerRelationshipRow {
                remote_node_id: r.get(0)?,
                relationship_state: r.get(1)?,
                last_action: r.get(2)?,
                initiated_by: r.get(3)?,
                requested_at: r.get::<_, Option<i64>>(4)?.map(|value| value as u64),
                responded_at: r.get::<_, Option<i64>>(5)?.map(|value| value as u64),
                blocked_at: r.get::<_, Option<i64>>(6)?.map(|value| value as u64),
                cleared_at: r.get::<_, Option<i64>>(7)?.map(|value| value as u64),
                updated_at: r.get::<_, i64>(8)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn upsert_local_peer_relationship(
        &self,
        scope_id: &str,
        row: &LocalPeerRelationshipRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let requested_at_ms = row.requested_at.map(|value| value as i64).unwrap_or(-1);
        let responded_at_ms = row.responded_at.map(|value| value as i64).unwrap_or(-1);
        let blocked_at_ms = row.blocked_at.map(|value| value as i64).unwrap_or(-1);
        let cleared_at_ms = row.cleared_at.map(|value| value as i64).unwrap_or(-1);
        conn.execute(
            "INSERT INTO peer_relationships_local(
                scope_id, remote_node_id, relationship_state, last_action, initiated_by,
                requested_at, responded_at, blocked_at, cleared_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                CASE WHEN $6::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond') END,
                CASE WHEN $7::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond') END,
                CASE WHEN $8::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond') END,
                CASE WHEN $9::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($9::bigint * INTERVAL '1 millisecond') END,
                TIMESTAMPTZ 'epoch' + ($10::bigint * INTERVAL '1 millisecond')
             )
             ON CONFLICT(scope_id, remote_node_id) DO UPDATE SET
                relationship_state = excluded.relationship_state,
                last_action = excluded.last_action,
                initiated_by = excluded.initiated_by,
                requested_at = excluded.requested_at,
                responded_at = excluded.responded_at,
                blocked_at = excluded.blocked_at,
                cleared_at = excluded.cleared_at,
                updated_at = excluded.updated_at",
            params![
                scope_id,
                &row.remote_node_id,
                row.relationship_state.as_str(),
                row.last_action.as_str(),
                row.initiated_by.as_str(),
                requested_at_ms,
                responded_at_ms,
                blocked_at_ms,
                cleared_at_ms,
                row.updated_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_local_peer_dm_threads(&self, scope_id: &str) -> Result<Vec<LocalPeerDmThreadRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT remote_node_id,
                    thread_id,
                    thread_kind,
                    session_state,
                    (EXTRACT(EPOCH FROM relationship_established_at) * 1000)::BIGINT AS relationship_established_at_ms,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms,
                    (EXTRACT(EPOCH FROM last_message_at) * 1000)::BIGINT AS last_message_at_ms
             FROM peer_dm_threads_local
             WHERE scope_id = $1
             ORDER BY remote_node_id ASC, updated_at DESC, thread_id ASC",
        )?;
        let rows = stmt.query_map(params![scope_id], |r| {
            Ok(LocalPeerDmThreadRow {
                remote_node_id: r.get(0)?,
                thread_id: r.get(1)?,
                thread_kind: r.get(2)?,
                session_state: r.get(3)?,
                relationship_established_at: r.get::<_, Option<i64>>(4)?.map(|value| value as u64),
                created_at: r.get::<_, i64>(5)? as u64,
                updated_at: r.get::<_, i64>(6)? as u64,
                last_message_at: r.get::<_, Option<i64>>(7)?.map(|value| value as u64),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn upsert_local_peer_dm_thread(
        &self,
        scope_id: &str,
        row: &LocalPeerDmThreadRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO peer_dm_threads_local(
                scope_id, remote_node_id, thread_id, thread_kind, session_state,
                relationship_established_at, created_at, updated_at, last_message_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                CASE WHEN $6::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond') END,
                TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'),
                TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'),
                CASE WHEN $9::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($9::bigint * INTERVAL '1 millisecond') END
             )
             ON CONFLICT(scope_id, thread_id) DO UPDATE SET
                remote_node_id = excluded.remote_node_id,
                thread_kind = excluded.thread_kind,
                session_state = excluded.session_state,
                relationship_established_at = excluded.relationship_established_at,
                updated_at = excluded.updated_at,
                last_message_at = excluded.last_message_at",
            params![
                scope_id,
                &row.remote_node_id,
                &row.thread_id,
                &row.thread_kind,
                &row.session_state,
                row.relationship_established_at.map(|value| value as i64).unwrap_or(-1),
                row.created_at as i64,
                row.updated_at as i64,
                row.last_message_at.map(|value| value as i64).unwrap_or(-1)
            ],
        )?;
        Ok(())
    }

    pub fn list_local_peer_dm_messages(
        &self,
        scope_id: &str,
        thread_id: &str,
    ) -> Result<Vec<LocalPeerDmMessageRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT thread_id,
                    message_id,
                    remote_node_id,
                    message_kind,
                    direction,
                    delivery_state,
                    a2a_protocol,
                    content_json,
                    encrypted_body,
                    content_encoding,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,
                    (EXTRACT(EPOCH FROM acknowledged_at) * 1000)::BIGINT AS acknowledged_at_ms
             FROM peer_dm_messages_local
             WHERE scope_id = $1 AND thread_id = $2
             ORDER BY created_at ASC, message_id ASC",
        )?;
        let rows = stmt.query_map(params![scope_id, thread_id], |r| {
            Ok(LocalPeerDmMessageRow {
                thread_id: r.get(0)?,
                message_id: r.get(1)?,
                remote_node_id: r.get(2)?,
                message_kind: r.get(3)?,
                direction: r.get(4)?,
                delivery_state: r.get(5)?,
                a2a_protocol: r.get(6)?,
                content_json: r.get(7)?,
                encrypted_body: r.get(8)?,
                content_encoding: r.get(9)?,
                created_at: r.get::<_, i64>(10)? as u64,
                acknowledged_at: r.get::<_, Option<i64>>(11)?.map(|value| value as u64),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn upsert_local_peer_dm_message(
        &self,
        scope_id: &str,
        row: &LocalPeerDmMessageRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO peer_dm_messages_local(
                scope_id, thread_id, message_id, remote_node_id, message_kind, direction,
                delivery_state, a2a_protocol, content_json, encrypted_body, content_encoding,
                created_at, acknowledged_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11,
                TIMESTAMPTZ 'epoch' + ($12::bigint * INTERVAL '1 millisecond'),
                CASE WHEN $13::bigint < 0::bigint THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($13::bigint * INTERVAL '1 millisecond') END
             )
             ON CONFLICT(scope_id, message_id) DO UPDATE SET
                thread_id = excluded.thread_id,
                remote_node_id = excluded.remote_node_id,
                message_kind = excluded.message_kind,
                direction = excluded.direction,
                delivery_state = excluded.delivery_state,
                a2a_protocol = excluded.a2a_protocol,
                content_json = excluded.content_json,
                encrypted_body = excluded.encrypted_body,
                content_encoding = excluded.content_encoding,
                acknowledged_at = excluded.acknowledged_at",
            params![
                scope_id,
                &row.thread_id,
                &row.message_id,
                &row.remote_node_id,
                &row.message_kind,
                &row.direction,
                &row.delivery_state,
                &row.a2a_protocol,
                &row.content_json,
                row.encrypted_body,
                row.content_encoding,
                row.created_at as i64,
                row.acknowledged_at.map(|value| value as i64).unwrap_or(-1)
            ],
        )?;
        Ok(())
    }

    pub fn load_local_config_json<T: DeserializeOwned>(
        &self,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<T>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let raw = conn
            .query_row(
                "SELECT config_json
                 FROM local_config_json
                 WHERE config_key = $1",
                params![scoped_key(scope_id, key)],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        raw.map(|json| serde_json::from_str::<T>(&json).map_err(Into::into))
            .transpose()
    }

    pub fn save_local_config_json<T: Serialize>(
        &self,
        scope_id: &str,
        key: &str,
        value: &T,
        now: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let json = serde_json::to_string(value)?;
        conn.execute(
            "INSERT INTO local_config_json(config_key, config_json, updated_at)
             VALUES (
                $1,
                $2,
                TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             )
             ON CONFLICT(config_key) DO UPDATE SET
               config_json = excluded.config_json,
               updated_at = excluded.updated_at",
            params![scoped_key(scope_id, key), json, now as i64],
        )?;
        Ok(())
    }

    pub fn load_local_egress_agent_config<T: DeserializeOwned>(
        &self,
        scope_id: &str,
    ) -> Result<Option<T>> {
        self.load_local_config_json(scope_id, LOCAL_CONFIG_KEY_EGRESS_AGENT)
    }

    pub fn save_local_egress_agent_config<T: Serialize>(
        &self,
        scope_id: &str,
        value: &T,
        now: u64,
    ) -> Result<()> {
        self.save_local_config_json(scope_id, LOCAL_CONFIG_KEY_EGRESS_AGENT, value, now)
    }

    pub fn list_local_remote_task_bridges(
        &self,
        scope_id: &str,
    ) -> Result<Vec<LocalRemoteTaskBridgeRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT task_id, announcement_id, network_id, source_node_id, source_scope_hint,
                    detail_ref_digest, executor, profile, candidate_id, terminal_state,
                    (EXTRACT(EPOCH FROM bridged_at) * 1000)::BIGINT AS bridged_at_ms
             FROM remote_task_bridge_registry_local
             WHERE task_id LIKE $1 ESCAPE '\\'
             ORDER BY task_id ASC, executor ASC, profile ASC",
        )?;
        let rows = stmt.query_map(params![escaped_like_prefix(scope_id)], |r| {
            Ok(LocalRemoteTaskBridgeRow {
                task_id: r.get(0)?,
                announcement_id: r.get(1)?,
                network_id: r.get(2)?,
                source_node_id: r.get(3)?,
                source_scope_hint: r.get(4)?,
                detail_ref_digest: r.get(5)?,
                executor: r.get(6)?,
                profile: r.get(7)?,
                candidate_id: r.get(8)?,
                terminal_state: r.get(9)?,
                bridged_at: r.get::<_, i64>(10)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map(|rows| {
                rows.into_iter()
                    .filter_map(|row| {
                        strip_scoped_key(scope_id, &row.task_id).map(|task_id| {
                            LocalRemoteTaskBridgeRow {
                                task_id,
                                announcement_id: row.announcement_id,
                                network_id: row.network_id,
                                source_node_id: row.source_node_id,
                                source_scope_hint: row.source_scope_hint,
                                detail_ref_digest: row.detail_ref_digest,
                                executor: row.executor,
                                profile: row.profile,
                                candidate_id: row.candidate_id,
                                terminal_state: row.terminal_state,
                                bridged_at: row.bridged_at,
                            }
                        })
                    })
                    .collect()
            })
            .map_err(Into::into)
    }

    pub fn upsert_local_remote_task_bridge(
        &self,
        scope_id: &str,
        row: &LocalRemoteTaskBridgeRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO remote_task_bridge_registry_local(
                task_id, announcement_id, network_id, source_node_id, source_scope_hint,
                detail_ref_digest, executor, profile, candidate_id, terminal_state, bridged_at
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond')
             )
             ON CONFLICT(task_id, executor, profile) DO UPDATE SET
                announcement_id = excluded.announcement_id,
                network_id = excluded.network_id,
                source_node_id = excluded.source_node_id,
                source_scope_hint = excluded.source_scope_hint,
                detail_ref_digest = excluded.detail_ref_digest,
                candidate_id = excluded.candidate_id,
                terminal_state = excluded.terminal_state,
                bridged_at = excluded.bridged_at",
            params![
                scoped_key(scope_id, &row.task_id),
                row.announcement_id,
                row.network_id,
                row.source_node_id,
                row.source_scope_hint,
                row.detail_ref_digest,
                row.executor,
                row.profile,
                row.candidate_id,
                row.terminal_state,
                row.bridged_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn replace_local_remote_task_bridges(
        &self,
        scope_id: &str,
        rows: &[LocalRemoteTaskBridgeRow],
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute_batch("BEGIN")?;
        let result: Result<()> = (|| {
            conn.execute(
                "DELETE FROM remote_task_bridge_registry_local WHERE task_id LIKE $1 ESCAPE '\\'",
                params![escaped_like_prefix(scope_id)],
            )?;
            for row in rows {
                conn.execute(
                    "INSERT INTO remote_task_bridge_registry_local(
                        task_id, announcement_id, network_id, source_node_id, source_scope_hint,
                        detail_ref_digest, executor, profile, candidate_id, terminal_state, bridged_at
                     ) VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9, $10,
                        TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond')
                     )",
                    params![
                        scoped_key(scope_id, &row.task_id),
                        row.announcement_id,
                        row.network_id,
                        row.source_node_id,
                        row.source_scope_hint,
                        row.detail_ref_digest,
                        row.executor,
                        row.profile,
                        row.candidate_id,
                        row.terminal_state,
                        row.bridged_at as i64
                    ],
                )?;
            }
            Ok(())
        })();
        if result.is_ok() {
            conn.execute_batch("COMMIT")?;
        } else {
            let _ = conn.execute_batch("ROLLBACK");
        }
        result
    }

    pub fn get_local_data_source_binding(
        &self,
        scope_id: &str,
        binding_kind: &str,
        binding_scope: &str,
        binding_key: &str,
    ) -> Result<Option<LocalDataSourceBindingRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT binding_kind, binding_scope, binding_key, source_node_id, source_uri,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
             FROM data_source_bindings_local
             WHERE scope_id = $1 AND binding_kind = $2 AND binding_scope = $3 AND binding_key = $4",
            params![scope_id, binding_kind, binding_scope, binding_key],
            |r| {
                Ok(LocalDataSourceBindingRow {
                    binding_kind: r.get(0)?,
                    binding_scope: r.get(1)?,
                    binding_key: r.get(2)?,
                    source_node_id: r.get(3)?,
                    source_uri: r.get(4)?,
                    updated_at: r.get::<_, i64>(5)? as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_local_data_source_bindings(
        &self,
        scope_id: &str,
    ) -> Result<Vec<LocalDataSourceBindingRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT binding_kind, binding_scope, binding_key, source_node_id, source_uri,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
             FROM data_source_bindings_local
             WHERE scope_id = $1
             ORDER BY binding_kind ASC, binding_scope ASC, binding_key ASC",
        )?;
        let rows = stmt.query_map(params![scope_id], |r| {
            Ok(LocalDataSourceBindingRow {
                binding_kind: r.get(0)?,
                binding_scope: r.get(1)?,
                binding_key: r.get(2)?,
                source_node_id: r.get(3)?,
                source_uri: r.get(4)?,
                updated_at: r.get::<_, i64>(5)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn upsert_local_data_source_binding(
        &self,
        scope_id: &str,
        row: &LocalDataSourceBindingRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO data_source_bindings_local(
                scope_id, binding_kind, binding_scope, binding_key, source_node_id, source_uri, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6,
                TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
             )
             ON CONFLICT(scope_id, binding_kind, binding_scope, binding_key) DO UPDATE SET
                source_node_id = excluded.source_node_id,
                source_uri = excluded.source_uri,
                updated_at = excluded.updated_at",
            params![
                scope_id,
                row.binding_kind,
                row.binding_scope,
                row.binding_key,
                row.source_node_id,
                row.source_uri,
                row.updated_at as i64
            ],
        )?;
        Ok(())
    }
}
