use super::PgStore;
use crate::error::SwarmError;
use crate::storage::pg::{DbParam, OptionalExtension};
use anyhow::{Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsOutboundProgressRow {
    pub source_id: String,
    pub outbound_partition: String,
    pub scanned_sequence: u64,
    pub cutover_sequence: u64,
    pub delivery_policy_version: u64,
    pub retry_attempts: u32,
    pub next_retry_at: Option<u64>,
    pub last_error: Option<String>,
    pub updated_at: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CsMailboxDeliveryState {
    pub delivery_id: String,
    pub record_id: String,
    pub delivery_class: String,
    pub delivery_policy_version: u64,
    pub result_status: String,
    pub page_id: Option<String>,
    pub pending_commit_token: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CsMailboxPendingCommitRow {
    pub page_id: String,
    pub delivery_class: String,
    pub commit_token: String,
    pub attempts: u32,
    pub last_error: Option<String>,
}

impl std::fmt::Debug for CsMailboxPendingCommitRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CsMailboxPendingCommitRow")
            .field("page_id", &self.page_id)
            .field("delivery_class", &self.delivery_class)
            .field("commit_token", &"[redacted]")
            .field("attempts", &self.attempts)
            .field("last_error", &self.last_error)
            .finish()
    }
}

impl std::fmt::Debug for CsMailboxDeliveryState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CsMailboxDeliveryState")
            .field("delivery_id", &self.delivery_id)
            .field("record_id", &self.record_id)
            .field("delivery_class", &self.delivery_class)
            .field("delivery_policy_version", &self.delivery_policy_version)
            .field("result_status", &self.result_status)
            .field("page_id", &self.page_id)
            .field(
                "pending_commit_token",
                &self.pending_commit_token.as_ref().map(|_| "[redacted]"),
            )
            .field("last_error", &self.last_error)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkBackendStatusRow {
    pub backend: Option<String>,
    pub status: String,
    pub reason: Option<String>,
    pub published: u64,
    pub received: u64,
    pub retries: u64,
    pub backend_details_json: String,
    pub updated_at: u64,
}

impl PgStore {
    pub fn load_or_create_cs_tenant_instance_id(
        &self,
        scope_id: &str,
        now_ms: u64,
    ) -> Result<String> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let instance_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO cs_tenant_instance_local(scope_id, instance_id, created_at)
             VALUES ($1, $2, TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(scope_id) DO NOTHING",
            crate::params![scope_id, instance_id, now_ms as i64],
        )?;
        conn.query_row(
            "SELECT instance_id FROM cs_tenant_instance_local WHERE scope_id = $1",
            crate::params![scope_id],
            |row| row.get(0),
        )
        .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn initialize_cs_outbound_progress(
        &self,
        scope_id: &str,
        source_id: &str,
        outbound_partitions: &[&str],
        cutover_sequence: u64,
        delivery_policy_version: u64,
        source_head: u64,
        now_ms: u64,
    ) -> Result<()> {
        validate_partitions(outbound_partitions)?;
        if delivery_policy_version == 0 {
            bail!("ClientServer delivery policy version must be positive");
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let existing = {
            let mut statement = conn.prepare(
                "SELECT outbound_partition, scanned_sequence, delivery_policy_version
                 FROM cs_outbound_progress_local
                 WHERE scope_id = $1 AND source_id = $2",
            )?;
            let rows = statement.query_map(crate::params![scope_id, source_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                ))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        if !existing.is_empty() && existing.len() != outbound_partitions.len() {
            bail!("ClientServer outbound progress is only partially initialized");
        }
        if existing.iter().any(|(_, sequence, version)| {
            *version != delivery_policy_version && *sequence < source_head
        }) {
            bail!("ClientServer delivery policy cannot change before every partition is drained");
        }
        conn.with_transaction(|tx| {
            if !existing.is_empty() {
                let delivery_policy_version = delivery_policy_version as i64;
                let now_ms = now_ms as i64;
                let params: [&(dyn DbParam + Sync); 4] =
                    [&scope_id, &source_id, &delivery_policy_version, &now_ms];
                tx.execute(
                    "UPDATE cs_outbound_progress_local
                     SET delivery_policy_version = $3,
                         updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
                     WHERE scope_id = $1 AND source_id = $2",
                    &params,
                )?;
                return Ok(());
            }
            for partition in outbound_partitions {
                let partition = *partition;
                let cutover_sequence = cutover_sequence as i64;
                let delivery_policy_version = delivery_policy_version as i64;
                let now_ms = now_ms as i64;
                let params: [&(dyn DbParam + Sync); 6] = [
                    &scope_id,
                    &source_id,
                    &partition,
                    &cutover_sequence,
                    &delivery_policy_version,
                    &now_ms,
                ];
                tx.execute(
                    "INSERT INTO cs_outbound_progress_local(
                         scope_id, source_id, outbound_partition, scanned_sequence,
                         cutover_sequence, delivery_policy_version, retry_attempts, updated_at
                     ) VALUES (
                         $1, $2, $3, $4, $4, $5, 0,
                         TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
                     ) ON CONFLICT(scope_id, source_id, outbound_partition) DO NOTHING",
                    &params,
                )?;
            }
            Ok(())
        })?;
        Ok(())
    }

    pub fn load_cs_outbound_progress(
        &self,
        scope_id: &str,
        source_id: &str,
        outbound_partition: &str,
    ) -> Result<Option<CsOutboundProgressRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn
            .query_row(
                "SELECT source_id, outbound_partition, scanned_sequence, cutover_sequence,
                        delivery_policy_version, retry_attempts,
                        (EXTRACT(EPOCH FROM next_retry_at) * 1000)::BIGINT,
                        last_error,
                        (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT
                 FROM cs_outbound_progress_local
                 WHERE scope_id = $1 AND source_id = $2 AND outbound_partition = $3",
                crate::params![scope_id, source_id, outbound_partition],
                |row| {
                    Ok(CsOutboundProgressRow {
                        source_id: row.get(0)?,
                        outbound_partition: row.get(1)?,
                        scanned_sequence: row.get::<_, i64>(2)? as u64,
                        cutover_sequence: row.get::<_, i64>(3)? as u64,
                        delivery_policy_version: row.get::<_, i64>(4)? as u64,
                        retry_attempts: row.get::<_, i64>(5)? as u32,
                        next_retry_at: row.get::<_, Option<i64>>(6)?.map(|value| value as u64),
                        last_error: row.get(7)?,
                        updated_at: row.get::<_, i64>(8)? as u64,
                    })
                },
            )
            .optional()?)
    }

    pub fn advance_cs_outbound_progress(
        &self,
        scope_id: &str,
        source_id: &str,
        outbound_partition: &str,
        expected_sequence: u64,
        accepted_sequence: u64,
        now_ms: u64,
    ) -> Result<bool> {
        if accepted_sequence < expected_sequence {
            bail!("accepted sequence cannot move outbound progress backwards");
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE cs_outbound_progress_local
             SET scanned_sequence = $5, retry_attempts = 0, next_retry_at = NULL,
                 last_error = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND source_id = $2 AND outbound_partition = $3
               AND scanned_sequence = $4 AND $5 >= scanned_sequence",
            crate::params![
                scope_id,
                source_id,
                outbound_partition,
                expected_sequence as i64,
                accepted_sequence as i64,
                now_ms as i64,
            ],
        )? > 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn retry_cs_outbound_progress(
        &self,
        scope_id: &str,
        source_id: &str,
        outbound_partition: &str,
        expected_sequence: u64,
        next_retry_at: u64,
        error: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE cs_outbound_progress_local
             SET retry_attempts = retry_attempts + 1,
                 next_retry_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                 last_error = $6,
                 updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND source_id = $2 AND outbound_partition = $3
               AND scanned_sequence = $4",
            crate::params![
                scope_id,
                source_id,
                outbound_partition,
                expected_sequence as i64,
                next_retry_at as i64,
                error,
                now_ms as i64,
            ],
        )? > 0)
    }

    pub fn save_cs_mailbox_delivery_state(
        &self,
        scope_id: &str,
        state: &CsMailboxDeliveryState,
        now_ms: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO cs_mailbox_delivery_state_local(
                 scope_id, delivery_id, record_id, delivery_class, delivery_policy_version,
                 result_status, page_id, pending_commit_token, last_error, applied_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9,
                 TIMESTAMPTZ 'epoch' + ($10::bigint * INTERVAL '1 millisecond'),
                 TIMESTAMPTZ 'epoch' + ($10::bigint * INTERVAL '1 millisecond')
             ) ON CONFLICT(scope_id, delivery_id) DO UPDATE SET
                 result_status = excluded.result_status,
                 page_id = excluded.page_id,
                 pending_commit_token = excluded.pending_commit_token,
                 last_error = excluded.last_error,
                 updated_at = excluded.updated_at",
            crate::params![
                scope_id,
                &state.delivery_id,
                &state.record_id,
                &state.delivery_class,
                state.delivery_policy_version as i64,
                &state.result_status,
                &state.page_id,
                &state.pending_commit_token,
                &state.last_error,
                now_ms as i64,
            ],
        )?;
        Ok(())
    }

    pub fn load_cs_mailbox_delivery_state(
        &self,
        scope_id: &str,
        delivery_id: &str,
    ) -> Result<Option<CsMailboxDeliveryState>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn
            .query_row(
                "SELECT delivery_id, record_id, delivery_class, delivery_policy_version,
                        result_status, page_id, pending_commit_token, last_error
                 FROM cs_mailbox_delivery_state_local
                 WHERE scope_id = $1 AND delivery_id = $2",
                crate::params![scope_id, delivery_id],
                |row| {
                    Ok(CsMailboxDeliveryState {
                        delivery_id: row.get(0)?,
                        record_id: row.get(1)?,
                        delivery_class: row.get(2)?,
                        delivery_policy_version: row.get::<_, i64>(3)? as u64,
                        result_status: row.get(4)?,
                        page_id: row.get(5)?,
                        pending_commit_token: row.get(6)?,
                        last_error: row.get(7)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn mark_cs_mailbox_page_committed(
        &self,
        scope_id: &str,
        page_id: &str,
        delivery_class: &str,
        now_ms: u64,
    ) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE cs_mailbox_delivery_state_local
             SET result_status = 'committed', pending_commit_token = NULL,
                 committed_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND page_id = $2 AND delivery_class = $3
               AND result_status = 'commit_pending'",
            crate::params![scope_id, page_id, delivery_class, now_ms as i64],
        )? as u64)
    }

    pub fn save_cs_mailbox_pending_commit(
        &self,
        scope_id: &str,
        page_id: &str,
        delivery_class: &str,
        commit_token: &str,
        now_ms: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO cs_mailbox_pending_commit_local(
                 scope_id, page_id, delivery_class, commit_token, attempts,
                 created_at, updated_at
             ) VALUES ($1, $2, $3, $4, 0,
                 TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                 TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(scope_id, page_id, delivery_class) DO UPDATE SET
                 commit_token = excluded.commit_token,
                 updated_at = excluded.updated_at",
            crate::params![
                scope_id,
                page_id,
                delivery_class,
                commit_token,
                now_ms as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_cs_mailbox_pending_commits(
        &self,
        scope_id: &str,
    ) -> Result<Vec<CsMailboxPendingCommitRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut statement = conn.prepare(
            "SELECT page_id, delivery_class, commit_token, attempts, last_error
             FROM cs_mailbox_pending_commit_local
             WHERE scope_id = $1
             ORDER BY created_at, page_id, delivery_class",
        )?;
        let rows = statement.query_map(crate::params![scope_id], |row| {
            Ok(CsMailboxPendingCommitRow {
                page_id: row.get(0)?,
                delivery_class: row.get(1)?,
                commit_token: row.get(2)?,
                attempts: row.get::<_, i64>(3)? as u32,
                last_error: row.get(4)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn record_cs_mailbox_pending_commit_error(
        &self,
        scope_id: &str,
        page_id: &str,
        delivery_class: &str,
        error: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE cs_mailbox_pending_commit_local
             SET attempts = attempts + 1, last_error = $4,
                 updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND page_id = $2 AND delivery_class = $3",
            crate::params![scope_id, page_id, delivery_class, error, now_ms as i64],
        )? > 0)
    }

    pub fn clear_cs_mailbox_pending_commit(
        &self,
        scope_id: &str,
        page_id: &str,
        delivery_class: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "DELETE FROM cs_mailbox_pending_commit_local
             WHERE scope_id = $1 AND page_id = $2 AND delivery_class = $3",
            crate::params![scope_id, page_id, delivery_class],
        )? > 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_cs_mailbox_gap(
        &self,
        scope_id: &str,
        gap_id: &str,
        delivery_class: &str,
        delivery_policy_version: u64,
        route_json: &str,
        reason: &str,
        first_affected_at: u64,
        last_affected_at: u64,
        approximate_count: u64,
        page_id: &str,
        now_ms: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO cs_mailbox_gap_local(
                 scope_id, gap_id, delivery_class, delivery_policy_version, route_json,
                 reason, first_affected_at, last_affected_at, approximate_count, page_id, updated_at
             ) VALUES (
                 $1, $2, $3, $4, $5, $6,
                 TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'),
                 TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'), $9, $10,
                 TIMESTAMPTZ 'epoch' + ($11::bigint * INTERVAL '1 millisecond')
             ) ON CONFLICT(scope_id, gap_id) DO UPDATE SET
                 page_id = excluded.page_id,
                 updated_at = excluded.updated_at",
            crate::params![
                scope_id,
                gap_id,
                delivery_class,
                delivery_policy_version as i64,
                route_json,
                reason,
                first_affected_at as i64,
                last_affected_at as i64,
                approximate_count as i64,
                page_id,
                now_ms as i64,
            ],
        )?;
        Ok(())
    }

    pub fn mark_cs_mailbox_gaps_acknowledged(
        &self,
        scope_id: &str,
        page_id: &str,
        delivery_class: &str,
        now_ms: u64,
    ) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE cs_mailbox_gap_local
             SET acknowledged_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND page_id = $2 AND delivery_class = $3
               AND acknowledged_at IS NULL",
            crate::params![scope_id, page_id, delivery_class, now_ms as i64],
        )? as u64)
    }

    pub fn unacknowledged_cs_mailbox_gap_count(
        &self,
        scope_id: &str,
        delivery_class: &str,
    ) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM cs_mailbox_gap_local
             WHERE scope_id = $1 AND delivery_class = $2 AND acknowledged_at IS NULL",
            crate::params![scope_id, delivery_class],
            |row| Ok(row.get::<_, i64>(0)? as u64),
        )?)
    }

    pub fn cs_mailbox_gap_count(&self, scope_id: &str, delivery_class: &str) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM cs_mailbox_gap_local
             WHERE scope_id = $1 AND delivery_class = $2",
            crate::params![scope_id, delivery_class],
            |row| Ok(row.get::<_, i64>(0)? as u64),
        )?)
    }

    pub fn store_network_backend_status(
        &self,
        scope_id: &str,
        status: &NetworkBackendStatusRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO network_backend_status_local(
                 scope_id, backend, status, reason, published, received, retries,
                 backend_details_json, updated_at
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8,
                 TIMESTAMPTZ 'epoch' + ($9::bigint * INTERVAL '1 millisecond')
             ) ON CONFLICT(scope_id) DO UPDATE SET
                 backend = excluded.backend, status = excluded.status, reason = excluded.reason,
                 published = excluded.published, received = excluded.received,
                 retries = excluded.retries, backend_details_json = excluded.backend_details_json,
                 updated_at = excluded.updated_at",
            crate::params![
                scope_id,
                &status.backend,
                &status.status,
                &status.reason,
                status.published as i64,
                status.received as i64,
                status.retries as i64,
                &status.backend_details_json,
                status.updated_at as i64,
            ],
        )?;
        Ok(())
    }

    pub fn load_network_backend_status(
        &self,
        scope_id: &str,
    ) -> Result<Option<NetworkBackendStatusRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn
            .query_row(
                "SELECT backend, status, reason, published, received, retries,
                        backend_details_json,
                        (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT
                 FROM network_backend_status_local WHERE scope_id = $1",
                crate::params![scope_id],
                |row| {
                    Ok(NetworkBackendStatusRow {
                        backend: row.get(0)?,
                        status: row.get(1)?,
                        reason: row.get(2)?,
                        published: row.get::<_, i64>(3)? as u64,
                        received: row.get::<_, i64>(4)? as u64,
                        retries: row.get::<_, i64>(5)? as u64,
                        backend_details_json: row.get(6)?,
                        updated_at: row.get::<_, i64>(7)? as u64,
                    })
                },
            )
            .optional()?)
    }
}

fn validate_partitions(partitions: &[&str]) -> Result<()> {
    if partitions.is_empty() {
        bail!("at least one ClientServer outbound partition is required");
    }
    for partition in partitions {
        if !matches!(
            *partition,
            "global_interactive" | "global_bulk" | "non_global_interactive" | "non_global_bulk"
        ) {
            bail!("invalid ClientServer outbound partition: {partition}");
        }
    }
    Ok(())
}
