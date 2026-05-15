use super::*;

impl PgStore {
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
            "INSERT INTO task_stage_usage(org_id, task_id, epoch, explore_used, verify_used, finalize_used)
             VALUES ($1, $2, $3, 0, 0, 0)
             ON CONFLICT(org_id, task_id, epoch) DO NOTHING",
            params![self.org_id(), task_id, epoch as i64],
        )?;
        let (column, value) = match stage {
            "explore" => ("explore_used", delta as i64),
            "verify" => ("verify_used", delta as i64),
            "finalize" => ("finalize_used", delta as i64),
            _ => return Ok(()),
        };
        let sql = format!(
            "UPDATE task_stage_usage SET {column} = {column} + $4 WHERE org_id = $1 AND task_id = $2 AND epoch = $3"
        );
        conn.execute(&sql, params![self.org_id(), task_id, epoch as i64, value])?;
        Ok(())
    }

    pub fn get_stage_usage(&self, task_id: &str, epoch: u64) -> Result<Option<TaskStageUsageRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT task_id, epoch, explore_used, verify_used, finalize_used
             FROM task_stage_usage WHERE org_id = $1 AND task_id = $2 AND epoch = $3",
            params![self.org_id(), task_id, epoch as i64],
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
            "INSERT INTO task_cost_reports(org_id, task_id, epoch, cost_units_by_stage_json, latency_by_stage_json, evidence_fetch_bytes, events_emitted_count, cache_hit_rate)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT(org_id, task_id, epoch) DO UPDATE SET
               cost_units_by_stage_json = excluded.cost_units_by_stage_json,
               latency_by_stage_json = excluded.latency_by_stage_json,
               evidence_fetch_bytes = excluded.evidence_fetch_bytes,
               events_emitted_count = excluded.events_emitted_count,
               cache_hit_rate = excluded.cache_hit_rate",
            params![
                self.org_id(),
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
            "INSERT INTO leases(org_id, task_id, role, claimer_node_id, execution_id, lease_until)
             VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, task_id, role) DO UPDATE SET
              claimer_node_id = excluded.claimer_node_id,
              execution_id = excluded.execution_id,
              lease_until = excluded.lease_until",
            params![
                self.org_id(),
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
             FROM leases WHERE org_id = $1 AND task_id = $2 AND role = $3",
            params![self.org_id(), task_id, role],
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
            "DELETE FROM leases WHERE org_id = $1 AND task_id = $2 AND role = $3 AND claimer_node_id = $4 AND execution_id = $5",
            params![self.org_id(), task_id, role, claimer_node_id, execution_id],
        )?;
        Ok(())
    }

    pub fn put_candidate(
        &self,
        task_id: &str,
        proposer_node_id: &str,
        candidate: &Candidate,
    ) -> Result<()> {
        let candidate_json = candidate.control_json()?;
        let hash = candidate_hash(candidate)?;
        let output_ref_json = serde_json::to_string(&candidate.output_ref)?;
        let output_json = serde_json::to_string(&candidate.output)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO candidates(org_id, task_id, candidate_id, candidate_hash, execution_id, proposer_node_id, candidate_json, output_ref_json, output_json, output_resolved_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CASE WHEN $9 = 'null' THEN NULL ELSE NOW() END)
             ON CONFLICT(org_id, task_id, candidate_id) DO UPDATE SET
               candidate_hash = excluded.candidate_hash,
               execution_id = excluded.execution_id,
               proposer_node_id = excluded.proposer_node_id,
               candidate_json = excluded.candidate_json,
               output_ref_json = excluded.output_ref_json,
               output_json = excluded.output_json,
               output_resolved_at = excluded.output_resolved_at",
            params![
                self.org_id(),
                task_id,
                candidate.candidate_id,
                hash,
                candidate.execution_id,
                proposer_node_id,
                candidate_json,
                output_ref_json,
                output_json,
            ],
        )?;
        if candidate.has_resolved_output() {
            conn.execute(
                "INSERT INTO candidate_output_cache(org_id, task_id, candidate_id, output_json, updated_at)
                 VALUES ($1, $2, $3, $4, NOW())
                 ON CONFLICT(org_id, task_id, candidate_id) DO UPDATE SET
                   output_json = excluded.output_json,
                   updated_at = excluded.updated_at",
                params![self.org_id(), task_id, candidate.candidate_id, output_json],
            )?;
        }
        Ok(())
    }

    pub fn update_candidate_output(
        &self,
        task_id: &str,
        candidate_id: &str,
        output: &Value,
        resolved_at: u64,
    ) -> Result<()> {
        let output_json = serde_json::to_string(output)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE candidates
             SET output_json = $4,
                 output_resolved_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3",
            params![
                self.org_id(),
                task_id,
                candidate_id,
                output_json,
                resolved_at as i64
            ],
        )?;
        conn.execute(
            "INSERT INTO candidate_output_cache(org_id, task_id, candidate_id, output_json, updated_at)
             VALUES ($1, $2, $3, $4, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, task_id, candidate_id) DO UPDATE SET
               output_json = excluded.output_json,
               updated_at = excluded.updated_at",
            params![
                self.org_id(),
                task_id,
                candidate_id,
                output_json,
                resolved_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn cached_candidate_output(
        &self,
        task_id: &str,
        candidate_id: &str,
    ) -> Result<Option<Value>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT output_json
             FROM candidate_output_cache
             WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3",
            params![self.org_id(), task_id, candidate_id],
            |r| {
                let raw: String = r.get(0)?;
                serde_json::from_str::<Value>(&raw).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })
            },
        )
        .optional()
        .map_err(Into::into)
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
            "SELECT candidate_json, output_json FROM candidates WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3",
            params![self.org_id(), task_id, candidate_id],
            |r| {
                let json: String = r.get(0)?;
                let output_json: String = r.get(1)?;
                let mut c: Candidate = serde_json::from_str(&json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })?;
                c.output = serde_json::from_str(&output_json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(1, pg::types::Type::Text, Box::new(e))
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
            "SELECT candidate_json, output_json FROM candidates WHERE org_id = $1 AND task_id = $2 AND execution_id = $3",
            params![self.org_id(), task_id, execution_id],
            |r| {
                let json: String = r.get(0)?;
                let output_json: String = r.get(1)?;
                let mut c: Candidate = serde_json::from_str(&json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })?;
                c.output = serde_json::from_str(&output_json).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(1, pg::types::Type::Text, Box::new(e))
                })?;
                Ok(c)
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_candidates_for_task(&self, task_id: &str) -> Result<Vec<TaskCandidateRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT proposer_node_id, candidate_hash, candidate_json, output_json,
                    CASE WHEN output_resolved_at IS NULL THEN NULL ELSE CAST(EXTRACT(EPOCH FROM output_resolved_at) * 1000 AS BIGINT) END
             FROM candidates
             WHERE org_id = $1 AND task_id = $2
             ORDER BY proposer_node_id ASC, candidate_hash ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| {
            let candidate_json: String = r.get(2)?;
            let output_json: String = r.get(3)?;
            let mut candidate: Candidate = serde_json::from_str(&candidate_json).map_err(|e| {
                pg::Error::FromSqlConversionFailure(2, pg::types::Type::Text, Box::new(e))
            })?;
            candidate.output = serde_json::from_str(&output_json).map_err(|e| {
                pg::Error::FromSqlConversionFailure(3, pg::types::Type::Text, Box::new(e))
            })?;
            Ok(TaskCandidateRow {
                task_id: task_id.to_owned(),
                proposer_node_id: r.get(0)?,
                candidate_hash: r.get(1)?,
                output_resolved_at: r.get::<_, Option<i64>>(4)?.map(|value| value as u64),
                candidate,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn count_candidates(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM candidates WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id],
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
            "SELECT COUNT(1) FROM verifier_results WHERE org_id = $1 AND task_id = $2 AND passed = TRUE",
            params![self.org_id(), task_id],
            |r| r.get(0),
        )?;
        Ok(count as u32)
    }

    pub fn count_distinct_evidence_digests(&self, task_id: &str) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn
            .prepare("SELECT candidate_json FROM candidates WHERE org_id = $1 AND task_id = $2")?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| r.get::<_, String>(0))?;
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
            "INSERT INTO verifier_results(org_id, task_id, candidate_id, verifier_node_id, result_json, passed)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT(org_id, task_id, candidate_id, verifier_node_id) DO UPDATE SET
               result_json = excluded.result_json,
               passed = excluded.passed",
            params![
                self.org_id(),
                task_id,
                result.candidate_id,
                verifier_node_id,
                result_json,
                result.passed
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
            "SELECT result_json FROM verifier_results WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id, candidate_id], |r| {
            let json: String = r.get(0)?;
            let parsed: VerifierResult = serde_json::from_str(&json).map_err(|e| {
                pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
            })?;
            Ok(parsed)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn list_verifier_results_for_task(&self, task_id: &str) -> Result<Vec<VerifierResultRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT candidate_id, verifier_node_id, result_json
             FROM verifier_results
             WHERE org_id = $1 AND task_id = $2
             ORDER BY candidate_id ASC, verifier_node_id ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| {
            let raw: String = r.get(2)?;
            let result: VerifierResult = serde_json::from_str(&raw).map_err(|e| {
                pg::Error::FromSqlConversionFailure(2, pg::types::Type::Text, Box::new(e))
            })?;
            Ok(VerifierResultRow {
                task_id: task_id.to_owned(),
                candidate_id: r.get(0)?,
                verifier_node_id: r.get(1)?,
                result,
            })
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
            "INSERT INTO vote_commits(org_id, task_id, voter_node_id, candidate_hash, commit_hash, verifier_result_hash, execution_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, TIMESTAMPTZ 'epoch' + ($8::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, task_id, voter_node_id) DO UPDATE SET
               candidate_hash = excluded.candidate_hash,
               commit_hash = excluded.commit_hash,
               verifier_result_hash = excluded.verifier_result_hash,
               execution_id = excluded.execution_id,
               created_at = excluded.created_at",
            params![
                self.org_id(),
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
             FROM vote_commits WHERE org_id = $1 AND task_id = $2",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| {
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
        let mut stmt = conn.prepare(
            "SELECT voter_node_id FROM vote_reveals WHERE org_id = $1 AND task_id = $2 AND valid = TRUE",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| r.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn list_vote_reveals(&self, task_id: &str) -> Result<Vec<VoteRevealRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT voter_node_id, candidate_id, candidate_hash, vote, salt, verifier_result_hash, valid,
                    CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT)
             FROM vote_reveals
             WHERE org_id = $1 AND task_id = $2
             ORDER BY voter_node_id ASC, candidate_id ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| {
            let created_at_ms: i64 = r.get(7)?;
            let vote_raw: String = r.get(3)?;
            let vote = match vote_raw.as_str() {
                "approve" => VoteChoice::Approve,
                "reject" => VoteChoice::Reject,
                other => {
                    return Err(pg::Error::FromSqlConversionFailure(
                        3,
                        pg::types::Type::Text,
                        format!("unsupported vote choice '{other}'").into(),
                    ));
                }
            };
            Ok(VoteRevealRow {
                task_id: task_id.to_owned(),
                voter_node_id: r.get(0)?,
                candidate_id: r.get(1)?,
                candidate_hash: r.get(2)?,
                vote,
                salt: r.get(4)?,
                verifier_result_hash: r.get(5)?,
                valid: r.get(6)?,
                created_at: created_at_ms as u64,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn list_checkpoint_announcements_for_task(
        &self,
        task_id: &str,
        limit: usize,
    ) -> Result<Vec<CheckpointAnnouncementRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let escaped_task_id = task_id
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("task://%/{escaped_task_id}/round/%");
        let mut stmt = conn.prepare(
            "SELECT scope_key, checkpoint_id, artifact_path,
                    CAST(EXTRACT(EPOCH FROM observed_at) * 1000 AS BIGINT)
             FROM network_checkpoint_announcements
             WHERE org_id = $1 AND artifact_path LIKE $2 ESCAPE '\\'
             ORDER BY observed_at DESC, checkpoint_id DESC
             LIMIT $3",
        )?;
        let rows = stmt.query_map(params![self.org_id(), pattern, limit as i64], |r| {
            let observed_at_ms: i64 = r.get(3)?;
            Ok(CheckpointAnnouncementRow {
                scope_key: r.get(0)?,
                checkpoint_id: r.get(1)?,
                artifact_path: r.get(2)?,
                observed_at: observed_at_ms as u64,
            })
        })?;
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
                WHERE org_id = $1 AND task_id = $2 AND vote = 'approve' AND valid = TRUE
                GROUP BY candidate_hash
            ) AS reveal_counts",
            params![self.org_id(), task_id],
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
            "DELETE FROM vote_commits WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id],
        )?;
        conn.execute(
            "DELETE FROM vote_reveals WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id],
        )?;
        Ok(())
    }

    pub fn list_open_task_ids(&self) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT task_id FROM task_projection WHERE org_id = $1 AND terminal_state = 'open'",
        )?;
        let rows = stmt.query_map(params![self.org_id()], |r| r.get(0))?;
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
             FROM events WHERE org_id = $1 AND task_id = $2 AND event_kind = 'TaskCreated'",
            params![self.org_id(), task_id],
            |r| r.get::<_, Option<i64>>(0),
        )
        .optional()
        .map(|v| v.flatten().map(|x| x as u64))
        .map_err(Into::into)
    }
}
