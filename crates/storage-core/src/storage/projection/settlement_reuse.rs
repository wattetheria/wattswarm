use super::*;

impl PgStore {
    pub fn put_task_settlement(
        &self,
        task_id: &str,
        epoch: u64,
        finalized_at: u64,
        window_end_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO task_settlement(
                org_id, task_id, epoch, finalized_at, window_end_at, bad_feedback_exists, bad_feedback_at,
                implicit_settled, implicit_settled_at
             )
             VALUES (
               $1,
               $2,
               $3,
               TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond'),
               TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'),
               COALESCE((SELECT bad_feedback_exists FROM task_settlement WHERE org_id = $1 AND task_id = $2 AND epoch = $3), FALSE),
               COALESCE((SELECT bad_feedback_at FROM task_settlement WHERE org_id = $1 AND task_id = $2 AND epoch = $3), NULL),
               COALESCE((SELECT implicit_settled FROM task_settlement WHERE org_id = $1 AND task_id = $2 AND epoch = $3), FALSE),
               COALESCE((SELECT implicit_settled_at FROM task_settlement WHERE org_id = $1 AND task_id = $2 AND epoch = $3), NULL)
             )
             ON CONFLICT(org_id, task_id, epoch) DO UPDATE SET
               finalized_at = excluded.finalized_at,
               window_end_at = excluded.window_end_at,
               bad_feedback_exists = excluded.bad_feedback_exists,
               bad_feedback_at = excluded.bad_feedback_at,
               implicit_settled = excluded.implicit_settled,
               implicit_settled_at = excluded.implicit_settled_at",
            params![
                self.org_id(),
                task_id,
                epoch as i64,
                finalized_at as i64,
                window_end_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn mark_task_bad_feedback(
        &self,
        task_id: &str,
        epoch: u64,
        bad_feedback_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_settlement
             SET bad_feedback_exists = TRUE,
                 bad_feedback_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND task_id = $2 AND epoch = $3 AND bad_feedback_exists = FALSE",
            params![self.org_id(), task_id, epoch as i64, bad_feedback_at as i64],
        )?;
        Ok(())
    }

    pub fn get_task_settlement(&self, task_id: &str) -> Result<Option<TaskSettlementRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT task_id, epoch,
                    (EXTRACT(EPOCH FROM finalized_at) * 1000)::BIGINT AS finalized_at,
                    (EXTRACT(EPOCH FROM window_end_at) * 1000)::BIGINT AS window_end_at,
                    bad_feedback_exists,
                    CASE WHEN bad_feedback_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM bad_feedback_at) * 1000)::BIGINT END AS bad_feedback_at,
                    implicit_settled,
                    CASE WHEN implicit_settled_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM implicit_settled_at) * 1000)::BIGINT END AS implicit_settled_at
             FROM task_settlement
             WHERE org_id = $1 AND task_id = $2
             ORDER BY epoch DESC
             LIMIT 1",
            params![self.org_id(), task_id],
            |r| {
                Ok(TaskSettlementRow {
                    task_id: r.get(0)?,
                    epoch: r.get::<_, i64>(1)? as u64,
                    finalized_at: r.get::<_, i64>(2)? as u64,
                    window_end_at: r.get::<_, i64>(3)? as u64,
                    bad_feedback_exists: r.get::<_, bool>(4)?,
                    bad_feedback_at: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    implicit_settled: r.get::<_, bool>(6)?,
                    implicit_settled_at: r.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_due_implicit_settlements(
        &self,
        now: u64,
        limit: u32,
    ) -> Result<Vec<(String, u64)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT task_id, epoch
             FROM task_settlement
             WHERE org_id = $1
               AND bad_feedback_exists = FALSE
               AND implicit_settled = FALSE
               AND window_end_at <= TIMESTAMPTZ 'epoch' + ($2::bigint * INTERVAL '1 millisecond')
             ORDER BY window_end_at ASC
             LIMIT $3",
        )?;
        let rows = stmt.query_map(params![self.org_id(), now as i64, limit as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn mark_task_implicit_settled(
        &self,
        task_id: &str,
        epoch: u64,
        settled_at: u64,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let changed = conn.execute(
            "UPDATE task_settlement
             SET implicit_settled = TRUE,
                 implicit_settled_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND task_id = $2 AND epoch = $3 AND implicit_settled = FALSE",
            params![self.org_id(), task_id, epoch as i64, settled_at as i64],
        )?;
        Ok(changed > 0)
    }

    pub fn finalized_candidate_id_at_epoch(
        &self,
        task_id: &str,
        epoch: u64,
    ) -> Result<Option<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT candidate_id FROM finalizations WHERE org_id = $1 AND task_id = $2 AND epoch = $3",
            params![self.org_id(), task_id, epoch as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn count_reuse_applied_lookups(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM knowledge_lookups WHERE org_id = $1 AND task_id = $2 AND reuse_applied = TRUE",
            params![self.org_id(), task_id],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn reuse_applied_lookup_stats(&self, task_id: &str) -> Result<(u32, Option<u64>)> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let (count, first_at) = conn.query_row(
            "SELECT COUNT(1) AS reuse_count,
                    CASE
                      WHEN MIN(lookup_time) IS NULL THEN NULL
                      ELSE (EXTRACT(EPOCH FROM MIN(lookup_time)) * 1000)::BIGINT
                    END AS first_lookup_at
             FROM knowledge_lookups
             WHERE org_id = $1 AND task_id = $2 AND reuse_applied = TRUE",
            params![self.org_id(), task_id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?)),
        )?;
        Ok((count as u32, first_at.map(|v| v as u64)))
    }

    pub fn lookup_hit_profile(&self, task_id: &str) -> Result<(bool, bool, bool)> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let (exact_hits, similar_hits, reuse_applied) = conn.query_row(
            "SELECT
                COALESCE(SUM(exact_hit_count), 0)::BIGINT,
                COALESCE(SUM(similar_hit_count), 0)::BIGINT,
                COALESCE(SUM(CASE WHEN reuse_applied THEN 1 ELSE 0 END), 0)::BIGINT
             FROM knowledge_lookups
             WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            },
        )?;
        Ok((exact_hits > 0, similar_hits > 0, reuse_applied > 0))
    }

    pub fn find_reject_candidate_hash_with_quorum(
        &self,
        task_id: &str,
        quorum: u32,
    ) -> Result<Option<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT candidate_hash, COUNT(1) AS cnt
             FROM vote_reveals
             WHERE org_id = $1
               AND task_id = $2
               AND vote = 'reject'
               AND valid = TRUE
               AND candidate_hash <> ''
             GROUP BY candidate_hash",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u32))
        })?;
        let pairs = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        let max_count = pairs.iter().map(|(_, count)| *count).max();
        let Some(max_count) = max_count else {
            return Ok(None);
        };
        if max_count < quorum {
            return Ok(None);
        }
        let mut winners = pairs
            .into_iter()
            .filter(|(_, count)| *count == max_count)
            .map(|(hash, _)| hash);
        let winner = winners.next();
        if winners.next().is_some() {
            return Ok(None);
        }
        Ok(winner)
    }

    pub fn list_reject_voters_for_candidate_hash(
        &self,
        task_id: &str,
        candidate_hash: &str,
    ) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT voter_node_id
             FROM vote_reveals
             WHERE org_id = $1
               AND task_id = $2
               AND candidate_hash = $3
               AND vote = 'reject'
               AND valid = TRUE
             ORDER BY voter_node_id ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id, candidate_hash], |r| {
            r.get::<_, String>(0)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn list_reason_codes_for_candidate_hash(
        &self,
        task_id: &str,
        candidate_hash: &str,
    ) -> Result<Vec<u16>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT vr.result_json
             FROM verifier_results vr
             JOIN candidates c
               ON c.org_id = vr.org_id
              AND c.task_id = vr.task_id
              AND c.candidate_id = vr.candidate_id
             WHERE vr.org_id = $1
               AND vr.task_id = $2
               AND c.candidate_hash = $3",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id, candidate_hash], |r| {
            r.get::<_, String>(0)
        })?;
        let mut set = std::collections::BTreeSet::new();
        for row in rows {
            let raw = row?;
            if let Ok(parsed) = serde_json::from_str::<VerifierResult>(&raw) {
                for code in parsed.reason_codes {
                    set.insert(code);
                }
            }
        }
        Ok(set.into_iter().collect())
    }

    pub fn add_reuse_blacklist(
        &self,
        task_id: &str,
        epoch: u64,
        candidate_hash: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO reuse_blacklist(org_id, task_id, epoch, candidate_hash)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(org_id, task_id, epoch, candidate_hash) DO NOTHING",
            params![self.org_id(), task_id, epoch as i64, candidate_hash],
        )?;
        Ok(())
    }

    pub fn is_reuse_blacklisted(
        &self,
        task_id: &str,
        epoch: u64,
        candidate_hash: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let exists = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM reuse_blacklist
                WHERE org_id = $1 AND task_id = $2 AND epoch = $3 AND candidate_hash = $4
            )",
            params![self.org_id(), task_id, epoch as i64, candidate_hash],
            |r| r.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub fn list_reuse_blacklist(&self, task_id: &str, epoch: u64) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT candidate_hash FROM reuse_blacklist
             WHERE org_id = $1 AND task_id = $2 AND epoch = $3
             ORDER BY candidate_hash ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id, epoch as i64], |r| {
            r.get::<_, String>(0)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_knowledge_lookup(
        &self,
        task_id: &str,
        task_type: &str,
        input_digest: &str,
        lookup_time: u64,
        hit_count: u32,
        exact_hit_count: u32,
        similar_hit_count: u32,
        hits_digest: &str,
        reuse_applied: bool,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO knowledge_lookups(org_id, task_id, task_type, input_digest, lookup_time, hit_count, exact_hit_count, similar_hit_count, hits_digest, reuse_applied)
             VALUES ($1, $2, $3, $4, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'), $6, $7, $8, $9, $10)",
            params![
                self.org_id(),
                task_id,
                task_type,
                input_digest,
                lookup_time as i64,
                hit_count as i64,
                exact_hit_count as i64,
                similar_hit_count as i64,
                hits_digest,
                reuse_applied
            ],
        )?;
        Ok(())
    }

    pub fn upsert_evidence_summary(
        &self,
        cid: &str,
        mime: &str,
        size_bytes: u64,
        source_hint_digest: &str,
        added_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO evidence_summary(org_id, cid, mime, size_bytes, source_hint_digest, added_at, availability_confirmations_count)
             VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'), 1)
             ON CONFLICT(org_id, cid) DO UPDATE SET
               mime = excluded.mime,
               size_bytes = excluded.size_bytes,
               source_hint_digest = excluded.source_hint_digest,
               added_at = excluded.added_at,
               availability_confirmations_count = evidence_summary.availability_confirmations_count + 1",
            params![
                self.org_id(),
                cid,
                mime,
                size_bytes as i64,
                source_hint_digest,
                added_at as i64
            ],
        )?;
        Ok(())
    }
}
