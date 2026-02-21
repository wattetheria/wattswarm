use super::*;

impl PgStore {
    pub fn export_knowledge_by_task(&self, task_id: &str) -> Result<serde_json::Value> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;

        let decision_memory = query_table_json(
            &conn,
            "SELECT task_id, epoch, final_commit_hash,
                    (EXTRACT(EPOCH FROM finalized_at) * 1000)::BIGINT AS finalized_at,
                    winning_candidate_hash, output_digest, result_summary, reason_codes_json, policy_snapshot_digest, task_type, input_digest, output_schema_digest, policy_id, policy_params_digest
             FROM decision_memory WHERE task_id = ?1 ORDER BY epoch ASC",
            params![task_id],
        )?;
        let evidence_summary = query_table_json(
            &conn,
            "SELECT cid, mime, size_bytes, source_hint_digest,
                    (EXTRACT(EPOCH FROM added_at) * 1000)::BIGINT AS added_at,
                    availability_confirmations_count
             FROM evidence_summary ORDER BY added_at DESC LIMIT 200",
            params![],
        )?;
        let runtime_metrics = query_table_json(
            &conn,
            "SELECT runtime_id, profile_id, task_type,
                    (EXTRACT(EPOCH FROM window_start) * 1000)::BIGINT AS window_start,
                    (EXTRACT(EPOCH FROM window_end) * 1000)::BIGINT AS window_end,
                    finalize_rate, timeout_rate, crash_rate, invalid_output_rate, median_latency_ms, cost_units, reject_reason_distribution, sample_count, finalize_count, timeout_count, crash_count, invalid_output_count, reuse_hit_rate_exact, reuse_hit_rate_similar, reuse_candidate_accept_rate, time_to_finality_p50, time_to_finality_p95, expired_rate, cost_units_per_finalized_task_p50, cost_units_per_finalized_task_p95, verify_cost_ratio, invalid_event_reject_count, fork_prevented_count, da_fetch_fail_rate
             FROM runtime_metrics ORDER BY window_end DESC LIMIT 200",
            params![],
        )?;
        let task_settlement = query_table_json(
            &conn,
            "SELECT task_id, epoch,
                    (EXTRACT(EPOCH FROM finalized_at) * 1000)::BIGINT AS finalized_at,
                    (EXTRACT(EPOCH FROM window_end_at) * 1000)::BIGINT AS window_end_at,
                    bad_feedback_exists,
                    CASE WHEN bad_feedback_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM bad_feedback_at) * 1000)::BIGINT END AS bad_feedback_at
             FROM task_settlement WHERE task_id = ?1 ORDER BY epoch ASC",
            params![task_id],
        )?;
        let task_cost_reports = query_table_json(
            &conn,
            "SELECT task_id, epoch, cost_units_by_stage_json, latency_by_stage_json, evidence_fetch_bytes, events_emitted_count, cache_hit_rate
             FROM task_cost_reports WHERE task_id = ?1 ORDER BY epoch ASC",
            params![task_id],
        )?;
        let reputation_state_raw = query_table_json(
            &conn,
            "SELECT runtime_id, profile_id, stability_reputation, quality_reputation,
                    (EXTRACT(EPOCH FROM last_updated_at) * 1000)::BIGINT AS last_updated_at
             FROM reputation_state ORDER BY last_updated_at DESC LIMIT 200",
            params![],
        )?;
        let reputation_state = with_reputation_decimal(reputation_state_raw);
        let knowledge_lookups = query_table_json(
            &conn,
            "SELECT task_id, task_type, input_digest,
                    (EXTRACT(EPOCH FROM lookup_time) * 1000)::BIGINT AS lookup_time,
                    hit_count, hits_digest, reuse_applied
             FROM knowledge_lookups WHERE task_id = ?1 ORDER BY lookup_time DESC",
            params![task_id],
        )?;
        let advisory_state = query_table_json(
            &conn,
            "SELECT advisory_id, policy_id, suggested_policy_hash, status,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at,
                    approved_by,
                    CASE WHEN approved_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM approved_at) * 1000)::BIGINT END AS approved_at,
                    applied_policy_hash,
                    CASE WHEN applied_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM applied_at) * 1000)::BIGINT END AS applied_at
             FROM advisory_state ORDER BY created_at DESC",
            params![],
        )?;
        let unknown_reason_observations = query_table_json(
            &conn,
            "SELECT task_id, unknown_reason_code, peer_protocol_version, local_protocol_version, author_node_id,
                    (EXTRACT(EPOCH FROM observed_at) * 1000)::BIGINT AS observed_at
             FROM unknown_reason_observations WHERE task_id = ?1 ORDER BY observed_at DESC",
            params![task_id],
        )?;

        Ok(serde_json::json!({
            "decision_memory": decision_memory,
            "evidence_summary": evidence_summary,
            "runtime_metrics": runtime_metrics,
            "task_settlement": task_settlement,
            "task_cost_reports": task_cost_reports,
            "reputation_state": reputation_state,
            "knowledge_lookups": knowledge_lookups,
            "advisory_state": advisory_state,
            "unknown_reason_observations": unknown_reason_observations
        }))
    }

    pub fn export_knowledge_by_task_type(&self, task_type: &str) -> Result<serde_json::Value> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let knowledge_lookups = query_table_json(
            &conn,
            "SELECT task_id, task_type, input_digest,
                    (EXTRACT(EPOCH FROM lookup_time) * 1000)::BIGINT AS lookup_time,
                    hit_count, hits_digest, reuse_applied
             FROM knowledge_lookups WHERE task_type = ?1 ORDER BY lookup_time DESC",
            params![task_type],
        )?;
        let runtime_metrics = query_table_json(
            &conn,
            "SELECT runtime_id, profile_id, task_type,
                    (EXTRACT(EPOCH FROM window_start) * 1000)::BIGINT AS window_start,
                    (EXTRACT(EPOCH FROM window_end) * 1000)::BIGINT AS window_end,
                    finalize_rate, timeout_rate, crash_rate, invalid_output_rate, median_latency_ms, cost_units, reject_reason_distribution, sample_count, finalize_count, timeout_count, crash_count, invalid_output_count, reuse_hit_rate_exact, reuse_hit_rate_similar, reuse_candidate_accept_rate, time_to_finality_p50, time_to_finality_p95, expired_rate, cost_units_per_finalized_task_p50, cost_units_per_finalized_task_p95, verify_cost_ratio, invalid_event_reject_count, fork_prevented_count, da_fetch_fail_rate
             FROM runtime_metrics WHERE task_type = ?1 ORDER BY window_end DESC",
            params![task_type],
        )?;
        let unknown_reason_observations = query_table_json(
            &conn,
            "SELECT task_id, unknown_reason_code, peer_protocol_version, local_protocol_version, author_node_id,
                    (EXTRACT(EPOCH FROM observed_at) * 1000)::BIGINT AS observed_at
             FROM unknown_reason_observations
             WHERE task_id IN (SELECT task_id FROM task_projection WHERE json_extract(contract_json, '$.task_type') = ?1)
             ORDER BY observed_at DESC LIMIT 200",
            params![task_type],
        )?;
        Ok(serde_json::json!({
            "task_type": task_type,
            "knowledge_lookups": knowledge_lookups,
            "runtime_metrics": runtime_metrics,
            "unknown_reason_observations": unknown_reason_observations
        }))
    }

    pub fn get_vote_commit(
        &self,
        task_id: &str,
        voter_node_id: &str,
    ) -> Result<Option<VoteCommitRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT commit_hash, candidate_hash, verifier_result_hash, execution_id,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at
             FROM vote_commits WHERE task_id = ?1 AND voter_node_id = ?2",
            params![task_id, voter_node_id],
            |r| {
                Ok(VoteCommitRow {
                    commit_hash: r.get(0)?,
                    candidate_hash: r.get(1)?,
                    verifier_result_hash: r.get(2)?,
                    execution_id: r.get(3)?,
                    created_at: r.get::<_, i64>(4)? as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn put_vote_reveal(&self, row: &VoteRevealRow) -> Result<()> {
        let vote_str = match row.vote {
            VoteChoice::Approve => "approve",
            VoteChoice::Reject => "reject",
        };
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO vote_reveals(task_id, voter_node_id, candidate_id, candidate_hash, vote, salt, verifier_result_hash, valid, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, TIMESTAMPTZ 'epoch' + (?9::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(task_id, voter_node_id) DO UPDATE SET
               candidate_id = excluded.candidate_id,
               candidate_hash = excluded.candidate_hash,
               vote = excluded.vote,
               salt = excluded.salt,
               verifier_result_hash = excluded.verifier_result_hash,
               valid = excluded.valid,
               created_at = excluded.created_at",
            params![
                row.task_id,
                row.voter_node_id,
                row.candidate_id,
                row.candidate_hash,
                vote_str,
                row.salt,
                row.verifier_result_hash,
                if row.valid { 1 } else { 0 },
                row.created_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn count_valid_votes_for_candidate(
        &self,
        task_id: &str,
        candidate_id: &str,
        vote: VoteChoice,
    ) -> Result<u32> {
        let vote_str = match vote {
            VoteChoice::Approve => "approve",
            VoteChoice::Reject => "reject",
        };
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM vote_reveals
             WHERE task_id = ?1 AND candidate_id = ?2 AND vote = ?3 AND valid = 1",
            params![task_id, candidate_id, vote_str],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn count_valid_votes_for_candidate_hash(
        &self,
        task_id: &str,
        candidate_hash: &str,
        vote: VoteChoice,
    ) -> Result<u32> {
        let vote_str = match vote {
            VoteChoice::Approve => "approve",
            VoteChoice::Reject => "reject",
        };
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM vote_reveals
             WHERE task_id = ?1 AND candidate_hash = ?2 AND vote = ?3 AND valid = 1",
            params![task_id, candidate_hash, vote_str],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn put_finalization(
        &self,
        task_id: &str,
        epoch: u64,
        candidate_id: &str,
        finality_proof_json: &str,
        event_id: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO finalizations(task_id, epoch, candidate_id, finality_proof_json, event_id)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(task_id, epoch) DO UPDATE SET
               candidate_id = excluded.candidate_id,
               finality_proof_json = excluded.finality_proof_json,
               event_id = excluded.event_id",
            params![
                task_id,
                epoch as i64,
                candidate_id,
                finality_proof_json,
                event_id
            ],
        )?;
        Ok(())
    }

    pub fn get_finalization_candidate(&self, task_id: &str, epoch: u64) -> Result<Option<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT candidate_id FROM finalizations WHERE task_id = ?1 AND epoch = ?2",
            params![task_id, epoch as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn put_checkpoint(
        &self,
        checkpoint_id: &str,
        up_to_seq: u64,
        event_id: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO checkpoints(checkpoint_id, up_to_seq, event_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(checkpoint_id) DO UPDATE SET
               up_to_seq = excluded.up_to_seq,
               event_id = excluded.event_id",
            params![checkpoint_id, up_to_seq as i64, event_id],
        )?;
        Ok(())
    }

    pub fn latest_checkpoint_seq(&self) -> Result<Option<u64>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT up_to_seq FROM checkpoints ORDER BY up_to_seq DESC LIMIT 1",
            params![],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map(|opt| opt.map(|v| v as u64))
        .map_err(Into::into)
    }

    pub fn put_membership(&self, membership_json: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO membership_projection(singleton, membership_json)
             VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET membership_json = excluded.membership_json",
            params![membership_json],
        )?;
        Ok(())
    }

    pub fn load_membership(&self) -> Result<Option<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT membership_json FROM membership_projection WHERE singleton = 1",
            params![],
            |r| r.get(0),
        )
        .optional()
        .map_err(Into::into)
    }
}
