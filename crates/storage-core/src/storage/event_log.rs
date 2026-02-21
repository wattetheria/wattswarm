use super::*;

impl PgStore {
    pub fn append_event(&self, event: &Event) -> Result<u64> {
        let event_json = serde_json::to_string(event)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let seq: i64 = conn
            .query_row(
                "INSERT INTO events(event_id, protocol_version, task_id, epoch, event_kind, author_node_id, created_at, event_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, TIMESTAMPTZ 'epoch' + (?7::bigint * INTERVAL '1 millisecond'), ?8)
                 RETURNING seq",
                params![
                    event.event_id,
                    event.protocol_version,
                    event.task_id,
                    event.epoch as i64,
                    format!("{:?}", event.event_kind),
                    event.author_node_id,
                    event.created_at as i64,
                    event_json,
                ],
                |r| r.get(0),
            )
            .context("insert event")?;
        Ok(seq as u64)
    }

    pub fn head_seq(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let seq = conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", params![], |r| {
            r.get::<_, i64>(0)
        })?;
        Ok(seq as u64)
    }

    pub fn peer_protocol_version_distribution(
        &self,
        local_node_id: &str,
    ) -> Result<Vec<(String, u32)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT protocol_version, COUNT(DISTINCT author_node_id) as node_count
             FROM events
             WHERE author_node_id != ?1
             GROUP BY protocol_version
             ORDER BY protocol_version ASC",
        )?;
        let rows = stmt.query_map(params![local_node_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u32))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn load_events_from(&self, from_exclusive: u64) -> Result<Vec<(u64, Event)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt =
            conn.prepare("SELECT seq, event_json FROM events WHERE seq > ?1 ORDER BY seq ASC")?;
        let rows = stmt.query_map(params![from_exclusive as i64], |row| {
            let seq: i64 = row.get(0)?;
            let json: String = row.get(1)?;
            let event: Event = serde_json::from_str(&json).map_err(|e| {
                pg::Error::FromSqlConversionFailure(1, pg::types::Type::Text, Box::new(e))
            })?;
            Ok((seq as u64, event))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn load_events_page(&self, from_exclusive: u64, limit: usize) -> Result<Vec<(u64, Event)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT seq, event_json FROM events
             WHERE seq > ?1
             ORDER BY seq ASC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![from_exclusive as i64, limit as i64], |row| {
            let seq: i64 = row.get(0)?;
            let json: String = row.get(1)?;
            let event: Event = serde_json::from_str(&json).map_err(|e| {
                pg::Error::FromSqlConversionFailure(1, pg::types::Type::Text, Box::new(e))
            })?;
            Ok((seq as u64, event))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn load_all_events(&self) -> Result<Vec<(u64, Event)>> {
        self.load_events_from(0)
    }

    pub fn clear_projection(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute_batch(
            "
            DELETE FROM task_projection;
            DELETE FROM leases;
            DELETE FROM candidates;
            DELETE FROM verifier_results;
            DELETE FROM evidence_added;
            DELETE FROM evidence_available;
            DELETE FROM vote_commits;
            DELETE FROM vote_reveals;
            DELETE FROM finalizations;
            DELETE FROM checkpoints;
            DELETE FROM membership_projection;
            DELETE FROM decision_memory;
            DELETE FROM evidence_summary;
            DELETE FROM runtime_metrics;
            DELETE FROM task_settlement;
            DELETE FROM task_stage_usage;
            DELETE FROM task_cost_reports;
            DELETE FROM reputation_state;
            DELETE FROM knowledge_lookups;
            DELETE FROM reuse_blacklist;
            DELETE FROM advisory_state;
            DELETE FROM unknown_reason_observations;
            ",
        )?;
        Ok(())
    }
}
