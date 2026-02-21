use super::*;

impl PgStore {
    pub fn upsert_task_contract(&self, contract: &TaskContract, epoch: u64) -> Result<()> {
        let contract_json = serde_json::to_string(contract)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO task_projection(task_id, epoch, contract_json, terminal_state, committed_candidate_id, finalized_candidate_id)
             VALUES (?1, ?2, ?3, 'open', NULL, NULL)
             ON CONFLICT(task_id) DO UPDATE SET epoch=excluded.epoch, contract_json=excluded.contract_json",
            params![contract.task_id, epoch as i64, contract_json],
        )?;
        Ok(())
    }

    pub fn task_projection(&self, task_id: &str) -> Result<Option<TaskProjectionRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT epoch, contract_json, terminal_state, committed_candidate_id, finalized_candidate_id, retry_attempt
             FROM task_projection WHERE task_id = ?1",
            params![task_id],
            |r| {
                let epoch: i64 = r.get(0)?;
                let contract_json: String = r.get(1)?;
                let terminal_state: String = r.get(2)?;
                let committed_candidate_id: Option<String> = r.get(3)?;
                let finalized_candidate_id: Option<String> = r.get(4)?;
                let retry_attempt: i64 = r.get(5)?;
                let contract: TaskContract = serde_json::from_str(&contract_json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(
                        0,
                        pg::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                let terminal_state = match terminal_state.as_str() {
                    "expired" => TaskTerminalState::Expired,
                    "finalized" => TaskTerminalState::Finalized,
                    "stopped" => TaskTerminalState::Stopped,
                    "suspended" => TaskTerminalState::Suspended,
                    "killed" => TaskTerminalState::Killed,
                    _ => TaskTerminalState::Open,
                };
                Ok(TaskProjectionRow {
                    epoch: epoch as u64,
                    contract,
                    terminal_state,
                    committed_candidate_id,
                    finalized_candidate_id,
                    retry_attempt: retry_attempt as u32,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn set_task_terminal_state(&self, task_id: &str, state: TaskTerminalState) -> Result<()> {
        let state_str = match state {
            TaskTerminalState::Open => "open",
            TaskTerminalState::Expired => "expired",
            TaskTerminalState::Finalized => "finalized",
            TaskTerminalState::Stopped => "stopped",
            TaskTerminalState::Suspended => "suspended",
            TaskTerminalState::Killed => "killed",
        };
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection SET terminal_state = ?2 WHERE task_id = ?1",
            params![task_id, state_str],
        )?;
        Ok(())
    }

    pub fn set_task_committed(&self, task_id: &str, candidate_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection SET committed_candidate_id = ?2 WHERE task_id = ?1",
            params![task_id, candidate_id],
        )?;
        Ok(())
    }

    pub fn set_task_finalized(&self, task_id: &str, candidate_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection
             SET finalized_candidate_id = ?2, terminal_state = 'finalized'
             WHERE task_id = ?1",
            params![task_id, candidate_id],
        )?;
        Ok(())
    }

    /// Sets `finalized_candidate_id` without marking the task terminal.
    /// Used for Continuous tasks so settlement feedback can find the winner.
    pub fn set_task_finalized_candidate_only(
        &self,
        task_id: &str,
        candidate_id: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection SET finalized_candidate_id = ?2 WHERE task_id = ?1",
            params![task_id, candidate_id],
        )?;
        Ok(())
    }

    pub fn set_task_retry_attempt(&self, task_id: &str, attempt: u32) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection
             SET retry_attempt = CASE WHEN retry_attempt < ?2 THEN ?2 ELSE retry_attempt END
             WHERE task_id = ?1",
            params![task_id, attempt as i64],
        )?;
        Ok(())
    }

    pub fn advance_task_epoch(&self, task_id: &str, next_epoch: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection
             SET epoch = ?2,
                 terminal_state = 'open',
                 committed_candidate_id = NULL,
                 finalized_candidate_id = NULL,
                 retry_attempt = 0
             WHERE task_id = ?1",
            params![task_id, next_epoch as i64],
        )?;
        conn.execute("DELETE FROM leases WHERE task_id = ?1", params![task_id])?;
        conn.execute(
            "DELETE FROM vote_commits WHERE task_id = ?1",
            params![task_id],
        )?;
        conn.execute(
            "DELETE FROM vote_reveals WHERE task_id = ?1",
            params![task_id],
        )?;
        Ok(())
    }

    pub fn put_evidence_added(
        &self,
        task_id: &str,
        candidate_id: &str,
        evidence: &crate::types::ArtifactRef,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO evidence_added(task_id, candidate_id, evidence_digest, evidence_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(task_id, candidate_id, evidence_digest)
             DO UPDATE SET evidence_json = excluded.evidence_json",
            params![
                task_id,
                candidate_id,
                evidence.digest,
                serde_json::to_string(evidence)?
            ],
        )?;
        Ok(())
    }

    pub fn put_evidence_available(
        &self,
        task_id: &str,
        candidate_id: &str,
        verifier_node_id: &str,
        evidence_digest: &str,
        created_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO evidence_available(task_id, candidate_id, verifier_node_id, evidence_digest, created_at)
             VALUES (?1, ?2, ?3, ?4, TIMESTAMPTZ 'epoch' + (?5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(task_id, candidate_id, verifier_node_id, evidence_digest)
             DO UPDATE SET created_at = excluded.created_at",
            params![
                task_id,
                candidate_id,
                verifier_node_id,
                evidence_digest,
                created_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn count_evidence_available(
        &self,
        task_id: &str,
        candidate_id: &str,
        evidence_digest: &str,
    ) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM evidence_available
             WHERE task_id = ?1 AND candidate_id = ?2 AND evidence_digest = ?3",
            params![task_id, candidate_id, evidence_digest],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn mark_stage_cost(
        &self,
        task_id: &str,
        epoch: u64,
        stage: &str,
        delta: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO task_stage_usage(task_id, epoch, explore_used, verify_used, finalize_used)
             VALUES (?1, ?2, 0, 0, 0)
             ON CONFLICT(task_id, epoch) DO NOTHING",
            params![task_id, epoch as i64],
        )?;
        let (column, value) = match stage {
            "explore" => ("explore_used", delta as i64),
            "verify" => ("verify_used", delta as i64),
            "finalize" => ("finalize_used", delta as i64),
            _ => return Ok(()),
        };
        let sql = format!(
            "UPDATE task_stage_usage SET {column} = {column} + ?3 WHERE task_id = ?1 AND epoch = ?2"
        );
        conn.execute(&sql, params![task_id, epoch as i64, value])?;
        Ok(())
    }

    pub fn get_stage_usage(&self, task_id: &str, epoch: u64) -> Result<Option<TaskStageUsageRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT task_id, epoch, explore_used, verify_used, finalize_used
             FROM task_stage_usage WHERE task_id = ?1 AND epoch = ?2",
            params![task_id, epoch as i64],
            |r| {
                Ok(TaskStageUsageRow {
                    task_id: r.get(0)?,
                    epoch: r.get::<_, i64>(1)? as u64,
                    explore_used: r.get::<_, i64>(2)? as u64,
                    verify_used: r.get::<_, i64>(3)? as u64,
                    finalize_used: r.get::<_, i64>(4)? as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_task_cost_report(
        &self,
        task_id: &str,
        epoch: u64,
        cost_units_by_stage: &serde_json::Value,
        latency_by_stage: &serde_json::Value,
        evidence_fetch_bytes: u64,
        events_emitted_count: u64,
        cache_hit_rate: f64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO task_cost_reports(task_id, epoch, cost_units_by_stage_json, latency_by_stage_json, evidence_fetch_bytes, events_emitted_count, cache_hit_rate)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(task_id, epoch) DO UPDATE SET
               cost_units_by_stage_json = excluded.cost_units_by_stage_json,
               latency_by_stage_json = excluded.latency_by_stage_json,
               evidence_fetch_bytes = excluded.evidence_fetch_bytes,
               events_emitted_count = excluded.events_emitted_count,
               cache_hit_rate = excluded.cache_hit_rate",
            params![
                task_id,
                epoch as i64,
                serde_json::to_string(cost_units_by_stage)?,
                serde_json::to_string(latency_by_stage)?,
                evidence_fetch_bytes as i64,
                events_emitted_count as i64,
                cache_hit_rate
            ],
        )?;
        Ok(())
    }

    pub fn upsert_lease(
        &self,
        task_id: &str,
        role: &str,
        claimer_node_id: &str,
        execution_id: &str,
        lease_until: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO leases(task_id, role, claimer_node_id, execution_id, lease_until)
             VALUES (?1, ?2, ?3, ?4, TIMESTAMPTZ 'epoch' + (?5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(task_id, role) DO UPDATE SET
              claimer_node_id = excluded.claimer_node_id,
              execution_id = excluded.execution_id,
              lease_until = excluded.lease_until",
            params![
                task_id,
                role,
                claimer_node_id,
                execution_id,
                lease_until as i64
            ],
        )?;
        Ok(())
    }

    pub fn get_lease(&self, task_id: &str, role: &str) -> Result<Option<LeaseRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT task_id, role, claimer_node_id, execution_id,
                    (EXTRACT(EPOCH FROM lease_until) * 1000)::BIGINT AS lease_until
             FROM leases WHERE task_id = ?1 AND role = ?2",
            params![task_id, role],
            |r| {
                Ok(LeaseRow {
                    task_id: r.get(0)?,
                    role: r.get(1)?,
                    claimer_node_id: r.get(2)?,
                    execution_id: r.get(3)?,
                    lease_until: r.get::<_, i64>(4)? as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn release_lease(
        &self,
        task_id: &str,
        role: &str,
        claimer_node_id: &str,
        execution_id: &str,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "DELETE FROM leases WHERE task_id = ?1 AND role = ?2 AND claimer_node_id = ?3 AND execution_id = ?4",
            params![task_id, role, claimer_node_id, execution_id],
        )?;
        Ok(())
    }

    pub fn put_candidate(
        &self,
        task_id: &str,
        proposer_node_id: &str,
        candidate: &Candidate,
    ) -> Result<()> {
        let candidate_json = serde_json::to_string(candidate)?;
        let hash = candidate_hash(candidate)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO candidates(task_id, candidate_id, candidate_hash, execution_id, proposer_node_id, candidate_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(task_id, candidate_id) DO UPDATE SET
               candidate_hash = excluded.candidate_hash,
               execution_id = excluded.execution_id,
               proposer_node_id = excluded.proposer_node_id,
               candidate_json = excluded.candidate_json",
            params![
                task_id,
                candidate.candidate_id,
                hash,
                candidate.execution_id,
                proposer_node_id,
                candidate_json
            ],
        )?;
        Ok(())
    }

    pub fn get_candidate_by_id(
        &self,
        task_id: &str,
        candidate_id: &str,
    ) -> Result<Option<Candidate>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT candidate_json FROM candidates WHERE task_id = ?1 AND candidate_id = ?2",
            params![task_id, candidate_id],
            |r| {
                let json: String = r.get(0)?;
                let c: Candidate = serde_json::from_str(&json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })?;
                Ok(c)
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_candidate_by_execution(
        &self,
        task_id: &str,
        execution_id: &str,
    ) -> Result<Option<Candidate>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT candidate_json FROM candidates WHERE task_id = ?1 AND execution_id = ?2",
            params![task_id, execution_id],
            |r| {
                let json: String = r.get(0)?;
                let c: Candidate = serde_json::from_str(&json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })?;
                Ok(c)
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn count_candidates(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM candidates WHERE task_id = ?1",
            params![task_id],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn count_passed_verifier_results(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM verifier_results WHERE task_id = ?1 AND passed = 1",
            params![task_id],
            |r| r.get(0),
        )?;
        Ok(count as u32)
    }

    pub fn count_distinct_evidence_digests(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare("SELECT candidate_json FROM candidates WHERE task_id = ?1")?;
        let rows = stmt.query_map(params![task_id], |r| r.get::<_, String>(0))?;
        let mut set = std::collections::HashSet::new();
        for row in rows {
            let raw = row?;
            if let Ok(candidate) = serde_json::from_str::<Candidate>(&raw) {
                for evidence in candidate.evidence_refs {
                    if !evidence.digest.is_empty() {
                        set.insert(evidence.digest);
                    }
                }
            }
        }
        Ok(set.len() as u32)
    }

    pub fn put_verifier_result(
        &self,
        task_id: &str,
        verifier_node_id: &str,
        result: &VerifierResult,
    ) -> Result<()> {
        let result_json = serde_json::to_string(result)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO verifier_results(task_id, candidate_id, verifier_node_id, result_json, passed)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(task_id, candidate_id, verifier_node_id) DO UPDATE SET
               result_json = excluded.result_json,
               passed = excluded.passed",
            params![
                task_id,
                result.candidate_id,
                verifier_node_id,
                result_json,
                i64::from(result.passed)
            ],
        )?;
        Ok(())
    }

    pub fn list_verifier_results_for_candidate(
        &self,
        task_id: &str,
        candidate_id: &str,
    ) -> Result<Vec<VerifierResult>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT result_json FROM verifier_results WHERE task_id = ?1 AND candidate_id = ?2",
        )?;
        let rows = stmt.query_map(params![task_id, candidate_id], |r| {
            let json: String = r.get(0)?;
            let parsed: VerifierResult = serde_json::from_str(&json).map_err(|e| {
                pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
            })?;
            Ok(parsed)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_vote_commit(
        &self,
        task_id: &str,
        voter_node_id: &str,
        candidate_hash: &str,
        commit_hash: &str,
        verifier_result_hash: &str,
        execution_id: &str,
        created_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO vote_commits(task_id, voter_node_id, candidate_hash, commit_hash, verifier_result_hash, execution_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, TIMESTAMPTZ 'epoch' + (?7::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(task_id, voter_node_id) DO UPDATE SET
               candidate_hash = excluded.candidate_hash,
               commit_hash = excluded.commit_hash,
               verifier_result_hash = excluded.verifier_result_hash,
               execution_id = excluded.execution_id,
               created_at = excluded.created_at",
            params![
                task_id,
                voter_node_id,
                candidate_hash,
                commit_hash,
                verifier_result_hash,
                execution_id,
                created_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_vote_commits_meta(&self, task_id: &str) -> Result<Vec<VoteCommitMetaRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT voter_node_id, (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at
             FROM vote_commits WHERE task_id = ?1",
        )?;
        let rows = stmt.query_map(params![task_id], |r| {
            Ok(VoteCommitMetaRow {
                voter_node_id: r.get(0)?,
                created_at: r.get::<_, i64>(1)? as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn list_vote_reveal_voters(&self, task_id: &str) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn
            .prepare("SELECT voter_node_id FROM vote_reveals WHERE task_id = ?1 AND valid = 1")?;
        let rows = stmt.query_map(params![task_id], |r| r.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn max_valid_approve_reveals(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let max_count: i64 = conn.query_row(
            "SELECT COALESCE(MAX(cnt), 0) FROM (
                SELECT COUNT(1) AS cnt
                FROM vote_reveals
                WHERE task_id = ?1 AND vote = 'approve' AND valid = 1
                GROUP BY candidate_hash
            ) AS reveal_counts",
            params![task_id],
            |r| r.get(0),
        )?;
        Ok(max_count as u32)
    }

    pub fn clear_votes_for_task(&self, task_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "DELETE FROM vote_commits WHERE task_id = ?1",
            params![task_id],
        )?;
        conn.execute(
            "DELETE FROM vote_reveals WHERE task_id = ?1",
            params![task_id],
        )?;
        Ok(())
    }

    pub fn list_open_task_ids(&self) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt =
            conn.prepare("SELECT task_id FROM task_projection WHERE terminal_state = 'open'")?;
        let rows = stmt.query_map(params![], |r| r.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn task_created_at(&self, task_id: &str) -> Result<Option<u64>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT CASE WHEN MIN(created_at) IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM MIN(created_at)) * 1000)::BIGINT END
             FROM events WHERE task_id = ?1 AND event_kind = 'TaskCreated'",
            params![task_id],
            |r| r.get::<_, Option<i64>>(0),
        )
        .optional()
        .map(|v| v.flatten().map(|x| x as u64))
        .map_err(Into::into)
    }

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
            "INSERT INTO task_settlement(task_id, epoch, finalized_at, window_end_at, bad_feedback_exists, bad_feedback_at)
             VALUES (
               ?1,
               ?2,
               TIMESTAMPTZ 'epoch' + (?3::bigint * INTERVAL '1 millisecond'),
               TIMESTAMPTZ 'epoch' + (?4::bigint * INTERVAL '1 millisecond'),
               COALESCE((SELECT bad_feedback_exists FROM task_settlement WHERE task_id = ?1 AND epoch = ?2), 0),
               COALESCE((SELECT bad_feedback_at FROM task_settlement WHERE task_id = ?1 AND epoch = ?2), NULL)
             )
             ON CONFLICT(task_id, epoch) DO UPDATE SET
               finalized_at = excluded.finalized_at,
               window_end_at = excluded.window_end_at,
               bad_feedback_exists = excluded.bad_feedback_exists,
               bad_feedback_at = excluded.bad_feedback_at",
            params![task_id, epoch as i64, finalized_at as i64, window_end_at as i64],
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
             SET bad_feedback_exists = 1,
                 bad_feedback_at = TIMESTAMPTZ 'epoch' + (?3::bigint * INTERVAL '1 millisecond')
             WHERE task_id = ?1 AND epoch = ?2 AND bad_feedback_exists = 0",
            params![task_id, epoch as i64, bad_feedback_at as i64],
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
                    CASE WHEN bad_feedback_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM bad_feedback_at) * 1000)::BIGINT END AS bad_feedback_at
             FROM task_settlement
             WHERE task_id = ?1
             ORDER BY epoch DESC
             LIMIT 1",
            params![task_id],
            |r| {
                Ok(TaskSettlementRow {
                    task_id: r.get(0)?,
                    epoch: r.get::<_, i64>(1)? as u64,
                    finalized_at: r.get::<_, i64>(2)? as u64,
                    window_end_at: r.get::<_, i64>(3)? as u64,
                    bad_feedback_exists: r.get::<_, i64>(4)? != 0,
                    bad_feedback_at: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                })
            },
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
            "SELECT COUNT(1) FROM knowledge_lookups WHERE task_id = ?1 AND reuse_applied = 1",
            params![task_id],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
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
            "INSERT INTO reuse_blacklist(task_id, epoch, candidate_hash)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(task_id, epoch, candidate_hash) DO NOTHING",
            params![task_id, epoch as i64, candidate_hash],
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
                WHERE task_id = ?1 AND epoch = ?2 AND candidate_hash = ?3
            )",
            params![task_id, epoch as i64, candidate_hash],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }

    pub fn list_reuse_blacklist(&self, task_id: &str, epoch: u64) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT candidate_hash FROM reuse_blacklist
             WHERE task_id = ?1 AND epoch = ?2
             ORDER BY candidate_hash ASC",
        )?;
        let rows = stmt.query_map(params![task_id, epoch as i64], |r| r.get::<_, String>(0))?;
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
        hits_digest: &str,
        reuse_applied: bool,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO knowledge_lookups(task_id, task_type, input_digest, lookup_time, hit_count, hits_digest, reuse_applied)
             VALUES (?1, ?2, ?3, TIMESTAMPTZ 'epoch' + (?4::bigint * INTERVAL '1 millisecond'), ?5, ?6, ?7)",
            params![
                task_id,
                task_type,
                input_digest,
                lookup_time as i64,
                hit_count as i64,
                hits_digest,
                if reuse_applied { 1 } else { 0 }
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
            "INSERT INTO evidence_summary(cid, mime, size_bytes, source_hint_digest, added_at, availability_confirmations_count)
             VALUES (?1, ?2, ?3, ?4, TIMESTAMPTZ 'epoch' + (?5::bigint * INTERVAL '1 millisecond'), 1)
             ON CONFLICT(cid) DO UPDATE SET
               mime = excluded.mime,
               size_bytes = excluded.size_bytes,
               source_hint_digest = excluded.source_hint_digest,
               added_at = excluded.added_at,
               availability_confirmations_count = evidence_summary.availability_confirmations_count + 1",
            params![cid, mime, size_bytes as i64, source_hint_digest, added_at as i64],
        )?;
        Ok(())
    }

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
        reason_codes: &[u16],
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
            "INSERT INTO decision_memory(task_id, epoch, final_commit_hash, finalized_at, winning_candidate_hash, output_digest, result_summary, reason_codes_json, policy_snapshot_digest, task_type, input_digest, output_schema_digest, policy_id, policy_params_digest, deprecated_as_exact)
             VALUES (?1, ?2, ?3, TIMESTAMPTZ 'epoch' + (?4::bigint * INTERVAL '1 millisecond'), ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, COALESCE((SELECT deprecated_as_exact FROM decision_memory WHERE task_id = ?1 AND epoch = ?2), 0))
             ON CONFLICT(task_id, epoch) DO UPDATE SET
               final_commit_hash = excluded.final_commit_hash,
               finalized_at = excluded.finalized_at,
               winning_candidate_hash = excluded.winning_candidate_hash,
               output_digest = excluded.output_digest,
               result_summary = excluded.result_summary,
               reason_codes_json = excluded.reason_codes_json,
               policy_snapshot_digest = excluded.policy_snapshot_digest,
               task_type = excluded.task_type,
               input_digest = excluded.input_digest,
               output_schema_digest = excluded.output_schema_digest,
               policy_id = excluded.policy_id,
               policy_params_digest = excluded.policy_params_digest,
               deprecated_as_exact = excluded.deprecated_as_exact",
            params![
                task_id,
                epoch as i64,
                final_commit_hash,
                finalized_at as i64,
                winning_candidate_hash,
                output_digest,
                serde_json::to_string(result_summary)?,
                serde_json::to_string(reason_codes)?,
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
            "UPDATE decision_memory SET deprecated_as_exact = 1 WHERE final_commit_hash = ?1",
            params![final_commit_hash],
        )?;
        Ok(())
    }

    pub fn has_decision_by_final_commit_hash(&self, final_commit_hash: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let exists = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM decision_memory WHERE final_commit_hash = ?1)",
            params![final_commit_hash],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
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
                WHERE task_id = ?1 AND epoch = ?2 AND final_commit_hash = ?3
            )",
            params![task_id, epoch as i64, final_commit_hash],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
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
            "SELECT task_id, epoch, final_commit_hash, winning_candidate_hash, result_summary,
                    reason_codes_json, input_digest, output_schema_digest, policy_id,
                    policy_params_digest, deprecated_as_exact,
                    (EXTRACT(EPOCH FROM finalized_at) * 1000)::BIGINT AS finalized_at
             FROM decision_memory
             WHERE task_type = ?1
               AND winning_candidate_hash <> ''
             ORDER BY finalized_at DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![task_type, limit as i64], |r| {
            let summary_raw: String = r.get(4)?;
            let summary =
                serde_json::from_str(&summary_raw).unwrap_or_else(|_| serde_json::json!({}));
            let reason_codes_raw: String = r.get(5)?;
            let reason_codes = serde_json::from_str(&reason_codes_raw).unwrap_or_default();
            Ok(DecisionMemoryHitRow {
                task_id: r.get(0)?,
                epoch: r.get::<_, i64>(1)? as u64,
                final_commit_hash: r.get(2)?,
                winning_candidate_hash: r.get(3)?,
                result_summary: summary,
                reason_codes,
                input_digest: r.get(6)?,
                output_schema_digest: r.get(7)?,
                policy_id: r.get(8)?,
                policy_params_digest: r.get(9)?,
                deprecated_as_exact: r.get::<_, i64>(10)? != 0,
                finalized_at: r.get::<_, i64>(11)? as u64,
                confidence_hint: 0.5,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}
