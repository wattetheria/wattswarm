use super::*;

impl PgStore {
    fn canonical_scope_hint_or_original(raw: String) -> String {
        wattswarm_protocol::types::normalized_scope_hint(&raw)
    }

    pub fn upsert_task_contract(&self, contract: &TaskContract, epoch: u64) -> Result<()> {
        let contract_json = serde_json::to_string(contract)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO task_projection(org_id, task_id, epoch, contract_json, terminal_state, committed_candidate_id, finalized_candidate_id)
             VALUES ($1, $2, $3, $4, 'open', NULL, NULL)
             ON CONFLICT(org_id, task_id) DO UPDATE SET epoch=excluded.epoch, contract_json=excluded.contract_json",
            params![self.org_id(), contract.task_id, epoch as i64, contract_json],
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
             FROM task_projection WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id],
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
            "UPDATE task_projection SET terminal_state = $3 WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id, state_str],
        )?;
        Ok(())
    }

    pub fn set_task_committed(&self, task_id: &str, candidate_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE task_projection SET committed_candidate_id = $3 WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id, candidate_id],
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
             SET finalized_candidate_id = $3, terminal_state = 'finalized'
             WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id, candidate_id],
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
            "UPDATE task_projection SET finalized_candidate_id = $3 WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id, candidate_id],
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
             SET retry_attempt = CASE WHEN retry_attempt < $3 THEN $3 ELSE retry_attempt END
             WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id, attempt as i64],
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
             SET epoch = $3,
                 terminal_state = 'open',
                 committed_candidate_id = NULL,
                 finalized_candidate_id = NULL,
                 retry_attempt = 0
             WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id, next_epoch as i64],
        )?;
        conn.execute(
            "DELETE FROM leases WHERE org_id = $1 AND task_id = $2",
            params![self.org_id(), task_id],
        )?;
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

    pub fn upsert_feed_subscription(
        &self,
        subscriber_node_id: &str,
        feed_key: &str,
        scope_hint: &str,
        active: bool,
        updated_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO feed_subscriptions(org_id, subscriber_node_id, feed_key, scope_hint, active, updated_at)
             VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, subscriber_node_id, feed_key) DO UPDATE SET
               scope_hint = excluded.scope_hint,
               active = excluded.active,
               updated_at = excluded.updated_at",
            params![
                self.org_id(),
                subscriber_node_id,
                feed_key,
                scope_hint,
                active,
                updated_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_active_feed_subscription_scope_hints(
        &self,
        subscriber_node_id: &str,
    ) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT scope_hint
             FROM feed_subscriptions
             WHERE org_id = $1 AND subscriber_node_id = $2 AND active = TRUE
             ORDER BY updated_at DESC, feed_key ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), subscriber_node_id], |r| r.get(0))?;
        let mut hints = Vec::new();
        for row in rows {
            let hint = Self::canonical_scope_hint_or_original(row?);
            if !hints.contains(&hint) {
                hints.push(hint);
            }
        }
        Ok(hints)
    }

    pub fn list_active_feed_subscription_scopes(
        &self,
        subscriber_node_id: &str,
    ) -> Result<Vec<ProjectionScope>> {
        let mut scopes = Vec::new();
        for hint in self.list_active_feed_subscription_scope_hints(subscriber_node_id)? {
            if let Some(scope) = ProjectionScope::parse(&hint)
                && !scopes.contains(&scope)
            {
                scopes.push(scope);
            }
        }
        Ok(scopes)
    }

    pub fn get_feed_subscription(
        &self,
        subscriber_node_id: &str,
        feed_key: &str,
    ) -> Result<Option<FeedSubscriptionRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT scope_hint, active, CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM feed_subscriptions
             WHERE org_id = $1 AND subscriber_node_id = $2 AND feed_key = $3",
            params![self.org_id(), subscriber_node_id, feed_key],
            |r| {
                let updated_at_ms: i64 = r.get(2)?;
                Ok(FeedSubscriptionRow {
                    subscriber_node_id: subscriber_node_id.to_owned(),
                    feed_key: feed_key.to_owned(),
                    scope_hint: Self::canonical_scope_hint_or_original(r.get(0)?),
                    active: r.get(1)?,
                    updated_at: updated_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn put_task_announcement(
        &self,
        task_id: &str,
        announcement_id: &str,
        feed_key: &str,
        scope_hint: &str,
        summary: &serde_json::Value,
        detail_ref: Option<&crate::types::ArtifactRef>,
        announced_by_node_id: &str,
        created_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let detail_ref_json = detail_ref.map(serde_json::to_string).transpose()?;
        conn.execute(
            "INSERT INTO task_announcements(org_id, task_id, announcement_id, feed_key, scope_hint, summary_json, detail_ref_json, announced_by_node_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TIMESTAMPTZ 'epoch' + ($9::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, announcement_id) DO UPDATE SET
               task_id = excluded.task_id,
               feed_key = excluded.feed_key,
               scope_hint = excluded.scope_hint,
               summary_json = excluded.summary_json,
               detail_ref_json = excluded.detail_ref_json,
               announced_by_node_id = excluded.announced_by_node_id,
               created_at = excluded.created_at",
            params![
                self.org_id(),
                task_id,
                announcement_id,
                feed_key,
                scope_hint,
                serde_json::to_string(summary)?,
                detail_ref_json,
                announced_by_node_id,
                created_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn get_task_announcement(
        &self,
        announcement_id: &str,
    ) -> Result<Option<TaskAnnouncementRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT task_id, feed_key, scope_hint, summary_json, detail_ref_json, announced_by_node_id,
                    CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT)
             FROM task_announcements
             WHERE org_id = $1 AND announcement_id = $2",
            params![self.org_id(), announcement_id],
            |r| {
                let summary_json: String = r.get(3)?;
                let detail_ref_json: Option<String> = r.get(4)?;
                let created_at_ms: i64 = r.get(6)?;
                Ok(TaskAnnouncementRow {
                    task_id: r.get(0)?,
                    announcement_id: announcement_id.to_owned(),
                    feed_key: r.get(1)?,
                    scope_hint: Self::canonical_scope_hint_or_original(r.get(2)?),
                    summary: serde_json::from_str(&summary_json).map_err(|e| {
                        pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                    })?,
                    detail_ref: detail_ref_json
                        .map(|raw| {
                            serde_json::from_str(&raw).map_err(|e| {
                                pg::Error::FromSqlConversionFailure(
                                    0,
                                    pg::types::Type::Text,
                                    Box::new(e),
                                )
                            })
                        })
                        .transpose()?,
                    announced_by_node_id: r.get(5)?,
                    created_at: created_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_task_announcement_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskAnnouncementRow>> {
        let announcement_id = {
            let conn = self
                .conn
                .lock()
                .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
            conn.query_row(
                "SELECT announcement_id
                 FROM task_announcements
                 WHERE org_id = $1 AND task_id = $2
                 ORDER BY created_at DESC, announcement_id DESC
                 LIMIT 1",
                params![self.org_id(), task_id],
                |r| r.get::<_, String>(0),
            )
            .optional()?
        };
        match announcement_id {
            Some(announcement_id) => self.get_task_announcement(&announcement_id),
            None => Ok(None),
        }
    }

    pub fn get_task_announcement_detail(
        &self,
        announcement_id: &str,
    ) -> Result<Option<TaskAnnouncementDetailRow>> {
        let Some(announcement) = self.get_task_announcement(announcement_id)? else {
            return Ok(None);
        };
        let contract = self
            .task_projection(&announcement.task_id)?
            .map(|task| task.contract);
        Ok(Some(TaskAnnouncementDetailRow {
            announcement,
            contract,
        }))
    }

    pub fn get_task_announcement_detail_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskAnnouncementDetailRow>> {
        let Some(announcement) = self.get_task_announcement_for_task(task_id)? else {
            return Ok(None);
        };
        let contract = self.task_projection(task_id)?.map(|task| task.contract);
        Ok(Some(TaskAnnouncementDetailRow {
            announcement,
            contract,
        }))
    }

    pub fn upsert_execution_set_member(
        &self,
        task_id: &str,
        execution_set_id: &str,
        participant_node_id: &str,
        role_hint: &str,
        scope_hint: &str,
        status: &str,
        confirmed_by_node_id: Option<&str>,
        updated_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO execution_set_projection(org_id, task_id, execution_set_id, participant_node_id, role_hint, scope_hint, status, confirmed_by_node_id, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TIMESTAMPTZ 'epoch' + ($9::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, task_id, execution_set_id, participant_node_id) DO UPDATE SET
               role_hint = excluded.role_hint,
               scope_hint = excluded.scope_hint,
               status = excluded.status,
               confirmed_by_node_id = excluded.confirmed_by_node_id,
               updated_at = excluded.updated_at",
            params![
                self.org_id(),
                task_id,
                execution_set_id,
                participant_node_id,
                role_hint,
                scope_hint,
                status,
                confirmed_by_node_id,
                updated_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_execution_set_members(
        &self,
        task_id: &str,
        execution_set_id: &str,
    ) -> Result<Vec<ExecutionSetMemberRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT participant_node_id, role_hint, scope_hint, status, confirmed_by_node_id,
                    CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM execution_set_projection
             WHERE org_id = $1 AND task_id = $2 AND execution_set_id = $3
             ORDER BY participant_node_id ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id, execution_set_id], |r| {
            let updated_at_ms: i64 = r.get(5)?;
            Ok(ExecutionSetMemberRow {
                task_id: task_id.to_owned(),
                execution_set_id: execution_set_id.to_owned(),
                participant_node_id: r.get(0)?,
                role_hint: r.get(1)?,
                scope_hint: Self::canonical_scope_hint_or_original(r.get(2)?),
                status: r.get(3)?,
                confirmed_by_node_id: r.get(4)?,
                updated_at: updated_at_ms as u64,
            })
        })?;
        let mut members = Vec::new();
        for row in rows {
            members.push(row?);
        }
        Ok(members)
    }

    pub fn put_rule_announcement(
        &self,
        scope_key: &str,
        rule_set: &str,
        rule_version: u64,
        activation_epoch: Option<u64>,
        observed_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO network_rule_announcements(org_id, scope_key, rule_set, rule_version, activation_epoch, observed_at)
             VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, scope_key, rule_set, rule_version) DO UPDATE SET
               activation_epoch = excluded.activation_epoch,
               observed_at = excluded.observed_at",
            params![
                self.org_id(),
                scope_key,
                rule_set,
                rule_version as i64,
                activation_epoch.map(|value| value as i64),
                observed_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn latest_rule_announcement(
        &self,
        scope_key: &str,
        rule_set: &str,
    ) -> Result<Option<RuleAnnouncementRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT rule_version, activation_epoch, CAST(EXTRACT(EPOCH FROM observed_at) * 1000 AS BIGINT)
             FROM network_rule_announcements
             WHERE org_id = $1 AND scope_key = $2 AND rule_set = $3
             ORDER BY rule_version DESC
             LIMIT 1",
            params![self.org_id(), scope_key, rule_set],
            |r| {
                let observed_at_ms: i64 = r.get(2)?;
                let activation_epoch: Option<i64> = r.get(1)?;
                Ok(RuleAnnouncementRow {
                    scope_key: scope_key.to_owned(),
                    rule_set: rule_set.to_owned(),
                    rule_version: r.get::<_, i64>(0)? as u64,
                    activation_epoch: activation_epoch.map(|value| value as u64),
                    observed_at: observed_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn put_checkpoint_announcement(
        &self,
        scope_key: &str,
        checkpoint_id: &str,
        artifact_path: &str,
        observed_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO network_checkpoint_announcements(org_id, scope_key, checkpoint_id, artifact_path, observed_at)
             VALUES ($1, $2, $3, $4, TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, scope_key, checkpoint_id) DO UPDATE SET
               artifact_path = excluded.artifact_path,
               observed_at = excluded.observed_at",
            params![
                self.org_id(),
                scope_key,
                checkpoint_id,
                artifact_path,
                observed_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn get_checkpoint_announcement(
        &self,
        scope_key: &str,
        checkpoint_id: &str,
    ) -> Result<Option<CheckpointAnnouncementRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT artifact_path, CAST(EXTRACT(EPOCH FROM observed_at) * 1000 AS BIGINT)
             FROM network_checkpoint_announcements
             WHERE org_id = $1 AND scope_key = $2 AND checkpoint_id = $3",
            params![self.org_id(), scope_key, checkpoint_id],
            |r| {
                let observed_at_ms: i64 = r.get(1)?;
                Ok(CheckpointAnnouncementRow {
                    scope_key: scope_key.to_owned(),
                    checkpoint_id: checkpoint_id.to_owned(),
                    artifact_path: r.get(0)?,
                    observed_at: observed_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
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
            "INSERT INTO evidence_added(org_id, task_id, candidate_id, evidence_digest, evidence_json)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(org_id, task_id, candidate_id, evidence_digest)
             DO UPDATE SET evidence_json = excluded.evidence_json",
            params![
                self.org_id(),
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
            "INSERT INTO evidence_available(org_id, task_id, candidate_id, verifier_node_id, evidence_digest, created_at)
             VALUES ($1, $2, $3, $4, $5, TIMESTAMPTZ 'epoch' + ($6::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, task_id, candidate_id, verifier_node_id, evidence_digest)
             DO UPDATE SET created_at = excluded.created_at",
            params![
                self.org_id(),
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
             WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3 AND evidence_digest = $4",
            params![self.org_id(), task_id, candidate_id, evidence_digest],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn get_evidence_reference(
        &self,
        task_id: &str,
        candidate_id: &str,
        evidence_digest: &str,
    ) -> Result<Option<crate::types::ArtifactRef>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT evidence_json
             FROM evidence_added
             WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3 AND evidence_digest = $4",
            params![self.org_id(), task_id, candidate_id, evidence_digest],
            |r| {
                let raw: String = r.get(0)?;
                serde_json::from_str(&raw).map_err(|e| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_evidence_references(
        &self,
        task_id: &str,
        candidate_id: &str,
    ) -> Result<Vec<crate::types::ArtifactRef>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT evidence_json
             FROM evidence_added
             WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3
             ORDER BY evidence_digest ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id, candidate_id], |r| {
            let raw: String = r.get(0)?;
            serde_json::from_str(&raw).map_err(|e| {
                pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
            })
        })?;
        let mut references = Vec::new();
        for row in rows {
            references.push(row?);
        }
        Ok(references)
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
        let candidate_json = serde_json::to_string(candidate)?;
        let hash = candidate_hash(candidate)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO candidates(org_id, task_id, candidate_id, candidate_hash, execution_id, proposer_node_id, candidate_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT(org_id, task_id, candidate_id) DO UPDATE SET
               candidate_hash = excluded.candidate_hash,
               execution_id = excluded.execution_id,
               proposer_node_id = excluded.proposer_node_id,
               candidate_json = excluded.candidate_json",
            params![
                self.org_id(),
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
            "SELECT candidate_json FROM candidates WHERE org_id = $1 AND task_id = $2 AND candidate_id = $3",
            params![self.org_id(), task_id, candidate_id],
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
            "SELECT candidate_json FROM candidates WHERE org_id = $1 AND task_id = $2 AND execution_id = $3",
            params![self.org_id(), task_id, execution_id],
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

fn parse_decision_memory_row(r: &pg::Row) -> std::result::Result<DecisionMemoryHitRow, pg::Error> {
    let summary_raw: String = r.get(5)?;
    let summary = serde_json::from_str(&summary_raw).unwrap_or_else(|_| serde_json::json!({}));
    let quorum_result_raw: String = r.get(6)?;
    let quorum_result =
        serde_json::from_str(&quorum_result_raw).unwrap_or_else(|_| serde_json::json!({}));
    let reason_codes_raw: String = r.get(7)?;
    let reason_codes = serde_json::from_str(&reason_codes_raw).unwrap_or_default();
    let reason_details_raw: String = r.get(8)?;
    let reason_details =
        serde_json::from_str(&reason_details_raw).unwrap_or_else(|_| serde_json::json!({}));
    Ok(DecisionMemoryHitRow {
        task_id: r.get(0)?,
        epoch: r.get::<_, i64>(1)? as u64,
        final_commit_hash: r.get(2)?,
        winning_candidate_hash: r.get(3)?,
        output_digest: r.get(4)?,
        result_summary: summary,
        quorum_result,
        reason_codes,
        reason_details,
        policy_snapshot_digest: r.get(9)?,
        input_digest: r.get(10)?,
        output_schema_digest: r.get(11)?,
        policy_id: r.get(12)?,
        task_type: r.get(13)?,
        policy_params_digest: r.get(14)?,
        deprecated_as_exact: r.get::<_, bool>(15)?,
        finalized_at: r.get::<_, i64>(16)? as u64,
        confidence_hint: 0.5,
    })
}
