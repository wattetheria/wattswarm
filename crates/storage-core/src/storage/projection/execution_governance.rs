use super::*;

impl PgStore {
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

    pub fn list_execution_set_members_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<ExecutionSetMemberRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT execution_set_id, participant_node_id, role_hint, scope_hint, status, confirmed_by_node_id,
                    CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM execution_set_projection
             WHERE org_id = $1 AND task_id = $2
             ORDER BY execution_set_id ASC, participant_node_id ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), task_id], |r| {
            let updated_at_ms: i64 = r.get(6)?;
            Ok(ExecutionSetMemberRow {
                task_id: task_id.to_owned(),
                execution_set_id: r.get(0)?,
                participant_node_id: r.get(1)?,
                role_hint: r.get(2)?,
                scope_hint: Self::canonical_scope_hint_or_original(r.get(3)?),
                status: r.get(4)?,
                confirmed_by_node_id: r.get(5)?,
                updated_at: updated_at_ms as u64,
            })
        })?;
        let mut members = Vec::new();
        for row in rows {
            members.push(row?);
        }
        Ok(members)
    }

    pub fn count_execution_set_members(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM execution_set_projection WHERE org_id = $1",
            params![self.org_id()],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u64)
    }

    pub fn count_distinct_execution_sets(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(DISTINCT task_id || ':' || execution_set_id)
             FROM execution_set_projection
             WHERE org_id = $1",
            params![self.org_id()],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u64)
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

    pub fn find_checkpoint_announcement(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<CheckpointAnnouncementRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT scope_key, artifact_path, CAST(EXTRACT(EPOCH FROM observed_at) * 1000 AS BIGINT)
             FROM network_checkpoint_announcements
             WHERE org_id = $1 AND checkpoint_id = $2
             ORDER BY observed_at DESC
             LIMIT 1",
            params![self.org_id(), checkpoint_id],
            |r| {
                let observed_at_ms: i64 = r.get(2)?;
                Ok(CheckpointAnnouncementRow {
                    scope_key: r.get(0)?,
                    checkpoint_id: checkpoint_id.to_owned(),
                    artifact_path: r.get(1)?,
                    observed_at: observed_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn count_checkpoint_announcements(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM network_checkpoint_announcements WHERE org_id = $1",
            params![self.org_id()],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u64)
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
}
