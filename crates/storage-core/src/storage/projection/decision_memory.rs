use super::*;

impl PgStore {
    #[allow(clippy::too_many_arguments)]
    pub fn put_decision_memory(
        &self,
        task_id: &str,
        epoch: u64,
        final_commit_hash: &str,
        finalized_at: u64,
        winning_candidate_hash: &str,
        output_digest: &str,
        result_summary: &serde_json::Value,
        quorum_result: &serde_json::Value,
        reason_codes: &[u16],
        reason_details: &serde_json::Value,
        policy_snapshot_digest: &str,
        task_type: &str,
        input_digest: &str,
        output_schema_digest: &str,
        policy_id: &str,
        policy_params_digest: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO decision_memory(org_id, task_id, epoch, final_commit_hash, finalized_at, winning_candidate_hash, output_digest, result_summary, quorum_result_json, reason_codes_json, reason_details, policy_snapshot_digest, task_type, input_digest, output_schema_digest, policy_id, policy_params_digest, deprecated_as_exact)
             VALUES ($1, $2, $3, $4, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'), $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, COALESCE((SELECT deprecated_as_exact FROM decision_memory WHERE org_id = $1 AND task_id = $2 AND epoch = $3), FALSE))
             ON CONFLICT(org_id, task_id, epoch) DO UPDATE SET
               final_commit_hash = excluded.final_commit_hash,
               finalized_at = excluded.finalized_at,
               winning_candidate_hash = excluded.winning_candidate_hash,
               output_digest = excluded.output_digest,
               result_summary = excluded.result_summary,
               quorum_result_json = excluded.quorum_result_json,
               reason_codes_json = excluded.reason_codes_json,
               reason_details = excluded.reason_details,
               policy_snapshot_digest = excluded.policy_snapshot_digest,
               task_type = excluded.task_type,
               input_digest = excluded.input_digest,
               output_schema_digest = excluded.output_schema_digest,
               policy_id = excluded.policy_id,
               policy_params_digest = excluded.policy_params_digest,
               deprecated_as_exact = excluded.deprecated_as_exact",
            params![
                self.org_id(),
                task_id,
                epoch as i64,
                final_commit_hash,
                finalized_at as i64,
                winning_candidate_hash,
                output_digest,
                serde_json::to_string(result_summary)?,
                serde_json::to_string(quorum_result)?,
                serde_json::to_string(reason_codes)?,
                serde_json::to_string(reason_details)?,
                policy_snapshot_digest,
                task_type,
                input_digest,
                output_schema_digest,
                policy_id,
                policy_params_digest
            ],
        )?;
        Ok(())
    }

    pub fn mark_decision_deprecated_by_hash(&self, final_commit_hash: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE decision_memory SET deprecated_as_exact = TRUE WHERE org_id = $1 AND final_commit_hash = $2",
            params![self.org_id(), final_commit_hash],
        )?;
        Ok(())
    }

    pub fn put_imported_decision_memory(
        &self,
        summary_id: &str,
        source_node_id: &str,
        decision: &DecisionMemoryHitRow,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO imported_decision_memory(org_id, summary_id, source_node_id, task_id, epoch, final_commit_hash, finalized_at,
                                                  winning_candidate_hash, output_digest, result_summary, quorum_result_json,
                                                  reason_codes_json, reason_details, policy_snapshot_digest, task_type,
                                                  input_digest, output_schema_digest, policy_id, policy_params_digest,
                                                  deprecated_as_exact, revoked)
             VALUES ($1, $2, $3, $4, $5, $6, TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'),
                     $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, FALSE)
             ON CONFLICT(org_id, summary_id, task_id, epoch) DO UPDATE SET
               source_node_id = excluded.source_node_id,
               final_commit_hash = excluded.final_commit_hash,
               finalized_at = excluded.finalized_at,
               winning_candidate_hash = excluded.winning_candidate_hash,
               output_digest = excluded.output_digest,
               result_summary = excluded.result_summary,
               quorum_result_json = excluded.quorum_result_json,
               reason_codes_json = excluded.reason_codes_json,
               reason_details = excluded.reason_details,
               policy_snapshot_digest = excluded.policy_snapshot_digest,
               task_type = excluded.task_type,
               input_digest = excluded.input_digest,
               output_schema_digest = excluded.output_schema_digest,
               policy_id = excluded.policy_id,
               policy_params_digest = excluded.policy_params_digest,
               deprecated_as_exact = excluded.deprecated_as_exact,
               revoked = FALSE",
            params![
                self.org_id(),
                summary_id,
                source_node_id,
                decision.task_id,
                decision.epoch as i64,
                decision.final_commit_hash,
                decision.finalized_at as i64,
                decision.winning_candidate_hash,
                decision.output_digest,
                serde_json::to_string(&decision.result_summary)?,
                serde_json::to_string(&decision.quorum_result)?,
                serde_json::to_string(&decision.reason_codes)?,
                serde_json::to_string(&decision.reason_details)?,
                decision.policy_snapshot_digest,
                decision.task_type,
                decision.input_digest,
                decision.output_schema_digest,
                decision.policy_id,
                decision.policy_params_digest,
                decision.deprecated_as_exact,
            ],
        )?;
        Ok(())
    }

    pub fn revoke_imported_decision_memory_by_summary(&self, summary_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE imported_decision_memory SET revoked = TRUE WHERE org_id = $1 AND summary_id = $2",
            params![self.org_id(), summary_id],
        )?;
        Ok(())
    }

    pub fn revoke_imported_decision_memory_by_source(&self, source_node_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE imported_decision_memory SET revoked = TRUE WHERE org_id = $1 AND source_node_id = $2",
            params![self.org_id(), source_node_id],
        )?;
        Ok(())
    }

    pub fn put_imported_task_outcome(&self, outcome: &ImportedTaskOutcomeRow) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO imported_task_outcomes(org_id, summary_id, source_node_id, scope_hint, task_id, task_type,
                                                candidate_id, output_digest, result_summary_json, evidence_digest_count,
                                                checkpoint_id, proof_artifact_path, finalized_at, revoked)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                     TIMESTAMPTZ 'epoch' + ($13::bigint * INTERVAL '1 millisecond'), FALSE)
             ON CONFLICT(org_id, summary_id, task_id) DO UPDATE SET
               source_node_id = excluded.source_node_id,
               scope_hint = excluded.scope_hint,
               task_type = excluded.task_type,
               candidate_id = excluded.candidate_id,
               output_digest = excluded.output_digest,
               result_summary_json = excluded.result_summary_json,
               evidence_digest_count = excluded.evidence_digest_count,
               checkpoint_id = excluded.checkpoint_id,
               proof_artifact_path = excluded.proof_artifact_path,
               finalized_at = excluded.finalized_at,
               revoked = FALSE",
            params![
                self.org_id(),
                outcome.summary_id,
                outcome.source_node_id,
                outcome.scope_hint,
                outcome.task_id,
                outcome.task_type,
                outcome.candidate_id,
                outcome.output_digest,
                serde_json::to_string(&outcome.result_summary)?,
                outcome.evidence_digest_count as i64,
                outcome.checkpoint_id,
                outcome.proof_artifact_path,
                outcome.finalized_at as i64,
            ],
        )?;
        Ok(())
    }

    pub fn list_imported_task_outcomes_by_task_type(
        &self,
        task_type: &str,
        limit: usize,
    ) -> Result<Vec<ImportedTaskOutcomeRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT summary_id, source_node_id, scope_hint, task_id, task_type, candidate_id, output_digest,
                    result_summary_json, evidence_digest_count, checkpoint_id, proof_artifact_path,
                    CAST(EXTRACT(EPOCH FROM finalized_at) * 1000 AS BIGINT), revoked
             FROM imported_task_outcomes
             WHERE org_id = $1 AND task_type = $2 AND revoked = FALSE
             ORDER BY finalized_at DESC, task_id DESC
             LIMIT $3",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_type, limit as i64], |r| {
            let result_summary_json: String = r.get(7)?;
            let finalized_at_ms: i64 = r.get(11)?;
            Ok(ImportedTaskOutcomeRow {
                summary_id: r.get(0)?,
                source_node_id: r.get(1)?,
                scope_hint: Self::canonical_scope_hint_or_original(r.get(2)?),
                task_id: r.get(3)?,
                task_type: r.get(4)?,
                candidate_id: r.get(5)?,
                output_digest: r.get(6)?,
                result_summary: serde_json::from_str(&result_summary_json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })?,
                evidence_digest_count: r.get::<_, i64>(8)? as u32,
                checkpoint_id: r.get(9)?,
                proof_artifact_path: r.get(10)?,
                finalized_at: finalized_at_ms as u64,
                revoked: r.get(12)?,
            })
        })?;
        let mut outcomes = Vec::new();
        for row in rows {
            outcomes.push(row?);
        }
        Ok(outcomes)
    }

    pub fn revoke_imported_task_outcomes_by_summary(&self, summary_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE imported_task_outcomes SET revoked = TRUE WHERE org_id = $1 AND summary_id = $2",
            params![self.org_id(), summary_id],
        )?;
        Ok(())
    }

    pub fn revoke_imported_task_outcomes_by_source(&self, source_node_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE imported_task_outcomes SET revoked = TRUE WHERE org_id = $1 AND source_node_id = $2",
            params![self.org_id(), source_node_id],
        )?;
        Ok(())
    }

    pub fn count_imported_decision_memory(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1)
             FROM imported_decision_memory
             WHERE org_id = $1 AND revoked = FALSE",
            params![self.org_id()],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u64)
    }

    pub fn count_imported_reputation_snapshots(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1)
             FROM imported_reputation_state
             WHERE org_id = $1 AND revoked = FALSE",
            params![self.org_id()],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u64)
    }

    pub fn count_imported_task_outcomes(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1)
             FROM imported_task_outcomes
             WHERE org_id = $1 AND revoked = FALSE",
            params![self.org_id()],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u64)
    }

    pub fn has_decision_by_final_commit_hash(&self, final_commit_hash: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let exists = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM decision_memory WHERE org_id = $1 AND final_commit_hash = $2)",
            params![self.org_id(), final_commit_hash],
            |r| r.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub fn has_decision_reference(
        &self,
        task_id: &str,
        epoch: u64,
        final_commit_hash: &str,
    ) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let exists = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM decision_memory
                WHERE org_id = $1 AND task_id = $2 AND epoch = $3 AND final_commit_hash = $4
            )",
            params![self.org_id(), task_id, epoch as i64, final_commit_hash],
            |r| r.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub fn list_local_decision_memory_hits_by_task_type(
        &self,
        task_type: &str,
        limit: u32,
    ) -> Result<Vec<DecisionMemoryHitRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT task_id, epoch, final_commit_hash, winning_candidate_hash, output_digest, result_summary,
                    quorum_result_json, reason_codes_json, reason_details, policy_snapshot_digest,
                    input_digest, output_schema_digest, policy_id, task_type,
                    policy_params_digest, deprecated_as_exact,
                    (EXTRACT(EPOCH FROM finalized_at) * 1000)::BIGINT AS finalized_at
             FROM decision_memory
             WHERE org_id = $1
               AND task_type = $2
               AND winning_candidate_hash <> ''
                   AND NOT EXISTS (
                        SELECT 1 FROM event_revocations revoked
                        WHERE revoked.org_id = $1
                          AND revoked.event_id = decision_memory.final_commit_hash
                   )
             ORDER BY finalized_at DESC
             LIMIT $3",
        )?;
        let rows = stmt.query_map(
            params![self.org_id(), task_type, limit as i64],
            parse_decision_memory_row,
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn list_decision_memory_hits_by_task_type(
        &self,
        task_type: &str,
        limit: u32,
    ) -> Result<Vec<DecisionMemoryHitRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT task_id, epoch, final_commit_hash, winning_candidate_hash, output_digest, result_summary,
                    quorum_result_json, reason_codes_json, reason_details, policy_snapshot_digest,
                    input_digest, output_schema_digest, policy_id, task_type,
                    policy_params_digest, deprecated_as_exact,
                    (EXTRACT(EPOCH FROM finalized_at) * 1000)::BIGINT AS finalized_at
             FROM (
                SELECT task_id, epoch, final_commit_hash, winning_candidate_hash, output_digest, result_summary,
                       quorum_result_json, reason_codes_json, reason_details, policy_snapshot_digest,
                       input_digest, output_schema_digest, policy_id, task_type,
                       policy_params_digest, deprecated_as_exact, finalized_at
                 FROM decision_memory local_dm
                 WHERE org_id = $1
                   AND task_type = $2
                   AND winning_candidate_hash <> ''
                   AND NOT EXISTS (
                        SELECT 1 FROM event_revocations revoked
                        WHERE revoked.org_id = $1
                          AND revoked.event_id = local_dm.final_commit_hash
                   )
                UNION ALL
                SELECT task_id, epoch, final_commit_hash, winning_candidate_hash, output_digest, result_summary,
                       quorum_result_json, reason_codes_json, reason_details, policy_snapshot_digest,
                       input_digest, output_schema_digest, policy_id, task_type,
                       policy_params_digest, deprecated_as_exact, finalized_at
                  FROM imported_decision_memory imported_dm
                 WHERE org_id = $1
                   AND task_type = $2
                   AND winning_candidate_hash <> ''
                   AND revoked = FALSE
                   AND NOT EXISTS (
                        SELECT 1 FROM summary_revocations revoked_summary
                        WHERE revoked_summary.org_id = $1
                          AND revoked_summary.summary_id = imported_dm.summary_id
                   )
                   AND NOT EXISTS (
                        SELECT 1 FROM penalized_nodes penalized
                        WHERE penalized.node_id = imported_dm.source_node_id
                          AND penalized.block_summaries = TRUE
                   )
             ) merged
             ORDER BY finalized_at DESC
             LIMIT $3",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_type, limit as i64], |r| {
            parse_decision_memory_row(r)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}
