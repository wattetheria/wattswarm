use super::{LocalAgentEventRow, PgStore};
use crate::error::SwarmError;
use crate::storage::pg::{DbParam, OptionalExtension};
use anyhow::Result;
use uuid::Uuid;

const COMMAND_STATUS_QUEUED: &str = "queued";
const COMMAND_STATUS_IN_FLIGHT: &str = "in_flight";
const COMMAND_STATUS_AWAITING_ACK: &str = "awaiting_ack";
const COMMAND_STATUS_FAILED: &str = "failed";
const AGENT_EVENT_STATUS_PENDING: &str = "pending";
const AGENT_EVENT_STATUS_CLAIMED: &str = "claimed";
const AGENT_EVENT_STATUS_RETRYING: &str = "retrying";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNetworkCommandInsert {
    pub command_id: String,
    pub dedup_key: Option<String>,
    pub command_kind: String,
    pub payload_json: String,
    pub attempts: u32,
    pub next_retry_at: Option<u64>,
    pub last_error: Option<String>,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedPendingNetworkCommandRow {
    pub command_id: String,
    pub dedup_key: Option<String>,
    pub command_kind: String,
    pub payload_json: String,
    pub attempts: u32,
    pub lease_token: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNetworkCommandStateRow {
    pub command_id: String,
    pub dedup_key: Option<String>,
    pub command_kind: String,
    pub payload_json: String,
    pub status: String,
    pub attempts: u32,
    pub next_retry_at: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedLocalAgentEventRow {
    pub event: LocalAgentEventRow,
    pub attempts: u32,
    pub lease_token: String,
}

impl PgStore {
    pub fn list_pending_network_commands(
        &self,
        scope_id: &str,
    ) -> Result<Vec<PendingNetworkCommandStateRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut statement = conn.prepare(
            "SELECT command_id, dedup_key, command_kind, payload_json, status, attempts,
                    CASE WHEN next_retry_at IS NULL THEN -1
                         ELSE (EXTRACT(EPOCH FROM next_retry_at) * 1000)::BIGINT END,
                    last_error
             FROM pending_network_commands_local
             WHERE scope_id = $1
             ORDER BY created_at, command_id",
        )?;
        let rows = statement.query_map(crate::params![scope_id], |row| {
            let next_retry_at = row.get::<_, i64>(6)?;
            Ok(PendingNetworkCommandStateRow {
                command_id: row.get(0)?,
                dedup_key: row.get(1)?,
                command_kind: row.get(2)?,
                payload_json: row.get(3)?,
                status: row.get(4)?,
                attempts: row.get::<_, i64>(5)? as u32,
                next_retry_at: (next_retry_at >= 0).then_some(next_retry_at as u64),
                last_error: row.get(7)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn enqueue_pending_network_command(
        &self,
        scope_id: &str,
        command: &PendingNetworkCommandInsert,
    ) -> Result<bool> {
        let next_retry_at = command.next_retry_at.map(|value| value.to_string());
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let inserted = conn.execute(
            "INSERT INTO pending_network_commands_local(
                 scope_id, command_id, dedup_key, command_kind, payload_json, status,
                 attempts, next_retry_at, last_error, created_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7,
                 CASE WHEN $8::TEXT IS NULL THEN NULL
                      ELSE TIMESTAMPTZ 'epoch' + (($8::text)::bigint * INTERVAL '1 millisecond') END,
                 $9,
                 TIMESTAMPTZ 'epoch' + ($10::bigint * INTERVAL '1 millisecond'),
                 TIMESTAMPTZ 'epoch' + ($10::bigint * INTERVAL '1 millisecond')
             )
             ON CONFLICT(scope_id, command_id) DO UPDATE SET
                 dedup_key = excluded.dedup_key,
                 command_kind = excluded.command_kind,
                 payload_json = excluded.payload_json,
                 updated_at = excluded.updated_at",
            crate::params![
                scope_id,
                &command.command_id,
                &command.dedup_key,
                &command.command_kind,
                &command.payload_json,
                COMMAND_STATUS_QUEUED,
                i64::from(command.attempts),
                &next_retry_at,
                &command.last_error,
                command.created_at as i64,
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn remove_pending_network_command_by_dedup_key(
        &self,
        scope_id: &str,
        dedup_key: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "DELETE FROM pending_network_commands_local
             WHERE scope_id = $1 AND dedup_key = $2",
            crate::params![scope_id, dedup_key],
        )? > 0)
    }

    pub fn acknowledge_pending_network_command(
        &self,
        scope_id: &str,
        command_id: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "DELETE FROM pending_network_commands_local
             WHERE scope_id = $1 AND command_id = $2",
            crate::params![scope_id, command_id],
        )? > 0)
    }

    pub fn fail_pending_network_command(
        &self,
        scope_id: &str,
        command_id: &str,
        lease_token: &str,
        error: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = $4, next_retry_at = NULL, last_error = $5,
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND command_id = $2 AND lease_token = $3
               AND status = 'in_flight'",
            crate::params![
                scope_id,
                command_id,
                lease_token,
                COMMAND_STATUS_FAILED,
                error,
                now_ms as i64,
            ],
        )? > 0)
    }

    pub fn requeue_pending_network_command(
        &self,
        scope_id: &str,
        command_id: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = $3, attempts = 0, next_retry_at = NULL, last_error = NULL,
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND command_id = $2 AND status = 'failed'",
            crate::params![scope_id, command_id, COMMAND_STATUS_QUEUED, now_ms as i64],
        )? > 0)
    }

    pub fn claim_due_pending_network_commands(
        &self,
        scope_id: &str,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<ClaimedPendingNetworkCommandRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let now = now_ms as i64;
        let lease_expires_at = now_ms.saturating_add(lease_ms) as i64;
        Ok(conn.with_transaction(|tx| {
        let recover_params: [&(dyn DbParam + Sync); 4] = [
            &scope_id,
            &COMMAND_STATUS_QUEUED,
            &now,
            &COMMAND_STATUS_IN_FLIGHT,
        ];
        tx.execute(
            "UPDATE pending_network_commands_local
             SET status = $2,
                 lease_owner = NULL,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1
               AND status = $4
               AND lease_expires_at <= TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')",
            &recover_params,
        )?;
        let select_params: [&(dyn DbParam + Sync); 5] = [
            &scope_id,
            &COMMAND_STATUS_QUEUED,
            &COMMAND_STATUS_AWAITING_ACK,
            &now,
            &(limit as i64),
        ];
        let rows = tx.query(
            "SELECT command_id, dedup_key, command_kind, payload_json, attempts,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms
             FROM pending_network_commands_local
             WHERE scope_id = $1
               AND status IN ($2, $3)
               AND (next_retry_at IS NULL OR next_retry_at <= TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
               AND (lease_expires_at IS NULL OR lease_expires_at <= TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
             ORDER BY COALESCE(next_retry_at, created_at), created_at, command_id
             LIMIT $5
             FOR UPDATE SKIP LOCKED",
            &select_params,
        )?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let command_id: String = row.get(0);
            let lease_token = Uuid::new_v4().to_string();
            let update_params: [&(dyn DbParam + Sync); 7] = [
                &scope_id,
                &command_id,
                &COMMAND_STATUS_IN_FLIGHT,
                &owner,
                &lease_token,
                &lease_expires_at,
                &now,
            ];
            let updated = tx.execute(
                "UPDATE pending_network_commands_local
                 SET status = $3,
                     attempts = attempts + 1,
                     lease_owner = $4,
                     lease_token = $5,
                     lease_expires_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'),
                     updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
                 WHERE scope_id = $1 AND command_id = $2",
                &update_params,
            )?;
            if updated == 0 {
                continue;
            }
            claimed.push(ClaimedPendingNetworkCommandRow {
                command_id,
                dedup_key: row.get(1),
                command_kind: row.get(2),
                payload_json: row.get(3),
                attempts: (row.get::<_, i64>(4).saturating_add(1)) as u32,
                lease_token,
                created_at: row.get::<_, i64>(5) as u64,
            });
        }
        Ok(claimed)
        })?)
    }

    pub fn complete_pending_network_command(
        &self,
        scope_id: &str,
        command_id: &str,
        lease_token: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "DELETE FROM pending_network_commands_local
             WHERE scope_id = $1 AND command_id = $2 AND status = $3 AND lease_token = $4",
            crate::params![scope_id, command_id, COMMAND_STATUS_IN_FLIGHT, lease_token],
        )? > 0)
    }

    pub fn await_pending_network_command_remote_ack(
        &self,
        scope_id: &str,
        command_id: &str,
        lease_token: &str,
        next_retry_at: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = 'awaiting_ack', next_retry_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                 last_error = NULL, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND command_id = $2 AND status = $3 AND lease_token = $4",
            crate::params![
                scope_id,
                command_id,
                COMMAND_STATUS_IN_FLIGHT,
                lease_token,
                next_retry_at as i64,
            ],
        )? > 0)
    }

    pub fn retry_pending_network_command(
        &self,
        scope_id: &str,
        command_id: &str,
        lease_token: &str,
        next_retry_at: u64,
        error: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = 'queued', next_retry_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
                 last_error = $6, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND command_id = $2 AND status = $3 AND lease_token = $4",
            crate::params![
                scope_id,
                command_id,
                COMMAND_STATUS_IN_FLIGHT,
                lease_token,
                next_retry_at as i64,
                error,
            ],
        )? > 0)
    }

    pub fn retry_pending_network_command_by_dedup_key(
        &self,
        scope_id: &str,
        dedup_key: &str,
        next_retry_at: u64,
        error: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = 'queued', attempts = attempts + 1,
                 next_retry_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                 last_error = $4, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND dedup_key = $2",
            crate::params![scope_id, dedup_key, next_retry_at as i64, error],
        )? > 0)
    }

    pub fn schedule_pending_network_command_by_dedup_key(
        &self,
        scope_id: &str,
        dedup_key: &str,
        next_retry_at: u64,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = 'queued',
                 next_retry_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond'),
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND dedup_key = $2",
            crate::params![scope_id, dedup_key, next_retry_at as i64, now_ms as i64],
        )? > 0)
    }

    pub fn recover_expired_pending_network_command_leases(
        &self,
        scope_id: &str,
        now_ms: u64,
    ) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE pending_network_commands_local
             SET status = 'queued', lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND status = 'in_flight'
               AND lease_expires_at <= TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')",
            crate::params![scope_id, now_ms as i64],
        )? as u64)
    }

    pub fn enqueue_local_agent_event(
        &self,
        scope_id: &str,
        row: &LocalAgentEventRow,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "INSERT INTO agent_event_bus_local(
                scope_id, event_id, event_type, source_kind, source_node_id, target_agent_id,
                target_executor, agent_envelope_json, payload_json, allowed_actions_json,
                requires_commit, status, dedupe_key, correlation_id, attempts, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 0,
                TIMESTAMPTZ 'epoch' + ($15::bigint * INTERVAL '1 millisecond'),
                TIMESTAMPTZ 'epoch' + ($16::bigint * INTERVAL '1 millisecond')
             ) ON CONFLICT(scope_id, event_id) DO NOTHING",
            crate::params![
                scope_id,
                &row.event_id,
                &row.event_type,
                &row.source_kind,
                &row.source_node_id,
                &row.target_agent_id,
                &row.target_executor,
                &row.agent_envelope_json,
                &row.payload_json,
                &row.allowed_actions_json,
                row.requires_commit,
                AGENT_EVENT_STATUS_PENDING,
                &row.dedupe_key,
                &row.correlation_id,
                row.created_at as i64,
                row.updated_at as i64,
            ],
        )? > 0)
    }

    pub fn claim_due_local_agent_events(
        &self,
        scope_id: &str,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<ClaimedLocalAgentEventRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let now = now_ms as i64;
        let lease_expires_at = now_ms.saturating_add(lease_ms) as i64;
        Ok(conn.with_transaction(|tx| {
        let recover_params: [&(dyn DbParam + Sync); 4] = [
            &scope_id,
            &AGENT_EVENT_STATUS_RETRYING,
            &now,
            &AGENT_EVENT_STATUS_CLAIMED,
        ];
        tx.execute(
            "UPDATE agent_event_bus_local
             SET status = $2, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND status = $4
               AND lease_expires_at <= TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')",
            &recover_params,
        )?;
        let select_params: [&(dyn DbParam + Sync); 5] = [
            &scope_id,
            &AGENT_EVENT_STATUS_PENDING,
            &AGENT_EVENT_STATUS_RETRYING,
            &now,
            &(limit as i64),
        ];
        let rows = tx.query(
            "SELECT event_id, event_type, source_kind, source_node_id, target_agent_id,
                    target_executor, agent_envelope_json, payload_json, allowed_actions_json,
                    requires_commit, status, dedupe_key, correlation_id, attempts,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
             FROM agent_event_bus_local
             WHERE scope_id = $1 AND status IN ($2, $3)
               AND (next_retry_at IS NULL OR next_retry_at <= TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
               AND (lease_expires_at IS NULL OR lease_expires_at <= TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'))
             ORDER BY COALESCE(next_retry_at, created_at), created_at, event_id
             LIMIT $5
             FOR UPDATE SKIP LOCKED",
            &select_params,
        )?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id: String = row.get(0);
            let lease_token = Uuid::new_v4().to_string();
            let update_params: [&(dyn DbParam + Sync); 7] = [
                &scope_id,
                &event_id,
                &AGENT_EVENT_STATUS_CLAIMED,
                &owner,
                &lease_token,
                &lease_expires_at,
                &now,
            ];
            let updated = tx.execute(
                "UPDATE agent_event_bus_local
                 SET status = $3, attempts = attempts + 1, lease_owner = $4, lease_token = $5,
                     lease_expires_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'),
                     updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
                 WHERE scope_id = $1 AND event_id = $2",
                &update_params,
            )?;
            if updated == 0 {
                continue;
            }
            claimed.push(ClaimedLocalAgentEventRow {
                event: LocalAgentEventRow {
                    event_id,
                    event_type: row.get(1),
                    source_kind: row.get(2),
                    source_node_id: row.get(3),
                    target_agent_id: row.get(4),
                    target_executor: row.get(5),
                    agent_envelope_json: row.get(6),
                    payload_json: row.get(7),
                    allowed_actions_json: row.get(8),
                    requires_commit: row.get(9),
                    status: AGENT_EVENT_STATUS_CLAIMED.to_owned(),
                    dedupe_key: row.get(11),
                    correlation_id: row.get(12),
                    created_at: row.get::<_, i64>(14) as u64,
                    updated_at: now_ms,
                },
                attempts: (row.get::<_, i64>(13).saturating_add(1)) as u32,
                lease_token,
            });
        }
        Ok(claimed)
        })?)
    }

    pub fn finish_local_agent_event_claim(
        &self,
        scope_id: &str,
        event_id: &str,
        lease_token: &str,
        status: &str,
        error: Option<&str>,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE agent_event_bus_local
             SET status = $5, last_error = $6, next_retry_at = NULL,
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND event_id = $2 AND status = $3 AND lease_token = $4",
            crate::params![
                scope_id,
                event_id,
                AGENT_EVENT_STATUS_CLAIMED,
                lease_token,
                status,
                error,
                now_ms as i64,
            ],
        )? > 0)
    }

    pub fn retry_local_agent_event_claim(
        &self,
        scope_id: &str,
        event_id: &str,
        lease_token: &str,
        next_retry_at: u64,
        error: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE agent_event_bus_local
             SET status = $5,
                 next_retry_at = TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'),
                 last_error = $7, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND event_id = $2 AND status = $3 AND lease_token = $4",
            crate::params![
                scope_id,
                event_id,
                AGENT_EVENT_STATUS_CLAIMED,
                lease_token,
                AGENT_EVENT_STATUS_RETRYING,
                next_retry_at as i64,
                error,
                now_ms as i64,
            ],
        )? > 0)
    }

    pub fn requeue_local_agent_event(
        &self,
        scope_id: &str,
        event_id: &str,
        now_ms: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE agent_event_bus_local
             SET status = 'pending', attempts = 0, next_retry_at = NULL, last_error = NULL,
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($3::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND event_id = $2 AND status = 'failed'",
            crate::params![scope_id, event_id, now_ms as i64],
        )? > 0)
    }

    pub fn recover_expired_local_agent_event_leases(
        &self,
        scope_id: &str,
        now_ms: u64,
    ) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn.execute(
            "UPDATE agent_event_bus_local
             SET status = 'retrying', lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                 updated_at = TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')
             WHERE scope_id = $1 AND status = 'claimed'
               AND lease_expires_at <= TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')",
            crate::params![scope_id, now_ms as i64],
        )? as u64)
    }

    pub fn pending_network_command_count(&self, scope_id: &str) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        Ok(conn
            .query_row(
                "SELECT COUNT(*) FROM pending_network_commands_local WHERE scope_id = $1",
                crate::params![scope_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or_default() as u64)
    }
}
