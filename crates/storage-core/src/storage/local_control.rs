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
                discovered_at: r.get::<_, i64>(2)? as u64,
                updated_at: r.get::<_, i64>(3)? as u64,
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
                    "INSERT INTO discovered_peers_local(scope_id, node_id, listen_addr, discovered_at, updated_at)
                     VALUES (
                        $1,
                        $2,
                        $3,
                        TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                        TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
                     )",
                    params![
                        scope_id,
                        &peer.node_id,
                        peer.listen_addr,
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
        now: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let existing = conn
            .query_row(
                "SELECT listen_addr
                 FROM discovered_peers_local
                 WHERE scope_id = $1 AND node_id = $2",
                params![scope_id, node_id],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?;
        let normalized = listen_addr
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        match existing {
            Some(current) if normalized.is_none() || current == normalized => Ok(false),
            Some(_) => {
                conn.execute(
                    "UPDATE discovered_peers_local
                     SET listen_addr = $2,
                         updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
                     WHERE scope_id = $1 AND node_id = $4",
                    params![scope_id, normalized, now as i64, node_id],
                )?;
                Ok(true)
            }
            None => {
                conn.execute(
                    "INSERT INTO discovered_peers_local(scope_id, node_id, listen_addr, discovered_at, updated_at)
                     VALUES (
                        $1,
                        $2,
                        $3,
                        TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                        TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                     )",
                    params![scope_id, node_id, normalized, now as i64],
                )?;
                Ok(true)
            }
        }
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
}
