use super::*;

impl PgStore {
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
        network_id: &str,
        subscriber_node_id: &str,
        feed_key: &str,
        scope_hint: &str,
        gossip_kinds: &[String],
        active: bool,
        updated_at: u64,
    ) -> Result<()> {
        self.upsert_feed_subscription_with_provider_capabilities(FeedSubscriptionUpsert {
            network_id,
            subscriber_node_id,
            feed_key,
            scope_hint,
            gossip_kinds,
            provider_capabilities: None,
            active,
            updated_at,
        })
    }

    pub fn upsert_feed_subscription_with_provider_capabilities(
        &self,
        upsert: FeedSubscriptionUpsert<'_>,
    ) -> Result<()> {
        let gossip_kinds_json = serde_json::to_string(upsert.gossip_kinds)?;
        let provider_capabilities_json = upsert
            .provider_capabilities
            .map(serde_json::to_string)
            .transpose()?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO feed_subscriptions(org_id, network_id, subscriber_node_id, feed_key, scope_hint, gossip_kinds_json, provider_capabilities_json, active, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TIMESTAMPTZ 'epoch' + ($9::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, network_id, subscriber_node_id, feed_key) DO UPDATE SET
               scope_hint = excluded.scope_hint,
               gossip_kinds_json = excluded.gossip_kinds_json,
               provider_capabilities_json = excluded.provider_capabilities_json,
               active = excluded.active,
               updated_at = excluded.updated_at",
            params![
                self.org_id(),
                upsert.network_id,
                upsert.subscriber_node_id,
                upsert.feed_key,
                upsert.scope_hint,
                gossip_kinds_json,
                provider_capabilities_json,
                upsert.active,
                upsert.updated_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_active_feed_subscription_scope_hints(
        &self,
        network_id: &str,
        subscriber_node_id: &str,
    ) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT scope_hint
             FROM feed_subscriptions
             WHERE org_id = $1 AND network_id = $2 AND subscriber_node_id = $3 AND active = TRUE
             ORDER BY updated_at DESC, feed_key ASC",
        )?;
        let rows = stmt.query_map(
            params![self.org_id(), network_id, subscriber_node_id],
            |r| r.get(0),
        )?;
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
        network_id: &str,
        subscriber_node_id: &str,
    ) -> Result<Vec<ProjectionScope>> {
        let mut scopes = Vec::new();
        for hint in
            self.list_active_feed_subscription_scope_hints(network_id, subscriber_node_id)?
        {
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
        network_id: &str,
        subscriber_node_id: &str,
        feed_key: &str,
    ) -> Result<Option<FeedSubscriptionRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT scope_hint, gossip_kinds_json, provider_capabilities_json, active, CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM feed_subscriptions
             WHERE org_id = $1 AND network_id = $2 AND subscriber_node_id = $3 AND feed_key = $4",
            params![self.org_id(), network_id, subscriber_node_id, feed_key],
            |r| {
                let updated_at_ms: i64 = r.get(4)?;
                Ok(FeedSubscriptionRow {
                    network_id: network_id.to_owned(),
                    subscriber_node_id: subscriber_node_id.to_owned(),
                    feed_key: feed_key.to_owned(),
                    scope_hint: Self::canonical_scope_hint_or_original(r.get(0)?),
                    gossip_kinds: Self::decode_gossip_kinds_json(r.get(1)?),
                    provider_capabilities: Self::decode_topic_provider_capabilities_json(r.get(2)?),
                    active: r.get(3)?,
                    updated_at: updated_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_active_feed_subscriptions(
        &self,
        network_id: &str,
        subscriber_node_id: &str,
    ) -> Result<Vec<FeedSubscriptionRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT feed_key, scope_hint, gossip_kinds_json, provider_capabilities_json, active, CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM feed_subscriptions
             WHERE org_id = $1 AND network_id = $2 AND subscriber_node_id = $3 AND active = TRUE
             ORDER BY updated_at DESC, feed_key ASC",
        )?;
        let rows = stmt.query_map(
            params![self.org_id(), network_id, subscriber_node_id],
            |r| {
                let updated_at_ms: i64 = r.get(5)?;
                Ok(FeedSubscriptionRow {
                    network_id: network_id.to_owned(),
                    subscriber_node_id: subscriber_node_id.to_owned(),
                    feed_key: r.get(0)?,
                    scope_hint: Self::canonical_scope_hint_or_original(r.get(1)?),
                    gossip_kinds: Self::decode_gossip_kinds_json(r.get(2)?),
                    provider_capabilities: Self::decode_topic_provider_capabilities_json(r.get(3)?),
                    active: r.get(4)?,
                    updated_at: updated_at_ms as u64,
                })
            },
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn list_active_feed_subscriptions_for_network(
        &self,
        network_id: &str,
    ) -> Result<Vec<FeedSubscriptionRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT subscriber_node_id, feed_key, scope_hint, gossip_kinds_json, provider_capabilities_json, active, CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM feed_subscriptions
             WHERE org_id = $1 AND network_id = $2 AND active = TRUE
             ORDER BY updated_at DESC, subscriber_node_id ASC, feed_key ASC",
        )?;
        let rows = stmt.query_map(params![self.org_id(), network_id], |r| {
            let updated_at_ms: i64 = r.get(6)?;
            Ok(FeedSubscriptionRow {
                network_id: network_id.to_owned(),
                subscriber_node_id: r.get(0)?,
                feed_key: r.get(1)?,
                scope_hint: Self::canonical_scope_hint_or_original(r.get(2)?),
                gossip_kinds: Self::decode_gossip_kinds_json(r.get(3)?),
                provider_capabilities: Self::decode_topic_provider_capabilities_json(r.get(4)?),
                active: r.get(5)?,
                updated_at: updated_at_ms as u64,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
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

    pub fn put_topic_message(
        &self,
        message_id: &str,
        network_id: &str,
        feed_key: &str,
        scope_hint: &str,
        author_node_id: &str,
        agent_envelope: Option<&AgentEnvelope>,
        content_ref: &ArtifactRef,
        content: Option<&serde_json::Value>,
        reply_to_message_id: Option<&str>,
        created_at: u64,
    ) -> Result<()> {
        let canonical_scope_hint = Self::canonical_scope_hint_or_original(scope_hint.to_owned());
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO topic_messages(org_id, message_id, network_id, feed_key, scope_hint, author_node_id, agent_envelope_json, content_ref_json, content_json, content_resolved_at, reply_to_message_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                     CASE WHEN $10::bigint < 0 THEN NULL ELSE TIMESTAMPTZ 'epoch' + ($10::bigint * INTERVAL '1 millisecond') END,
                     $11,
                     TIMESTAMPTZ 'epoch' + ($12::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, message_id) DO UPDATE SET
               network_id = excluded.network_id,
               feed_key = excluded.feed_key,
               scope_hint = excluded.scope_hint,
               author_node_id = excluded.author_node_id,
               agent_envelope_json = excluded.agent_envelope_json,
               content_ref_json = excluded.content_ref_json,
               content_json = excluded.content_json,
               content_resolved_at = excluded.content_resolved_at,
               reply_to_message_id = excluded.reply_to_message_id,
               created_at = excluded.created_at",
            params![
                self.org_id(),
                message_id,
                network_id,
                feed_key,
                canonical_scope_hint,
                author_node_id,
                agent_envelope.map(serde_json::to_string).transpose()?,
                serde_json::to_string(content_ref)?,
                serde_json::to_string(content.unwrap_or(&serde_json::Value::Null))?,
                content.map(|_| created_at as i64).unwrap_or(-1),
                reply_to_message_id,
                created_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn list_topic_messages_page(
        &self,
        network_id: &str,
        feed_key: &str,
        scope_hint: &str,
        before_created_at: Option<u64>,
        before_message_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TopicMessageRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let canonical_scope_hint = Self::canonical_scope_hint_or_original(scope_hint.to_owned());
        let has_anchor = before_created_at.is_some();
        let before_created_at = before_created_at.unwrap_or(u64::MAX) as i64;
        let mut stmt = conn.prepare(
            "SELECT message_id, network_id, author_node_id, agent_envelope_json, content_ref_json, content_json, reply_to_message_id,
                    CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT),
                    CASE WHEN content_resolved_at IS NULL THEN NULL ELSE CAST(EXTRACT(EPOCH FROM content_resolved_at) * 1000 AS BIGINT) END
             FROM topic_messages
             WHERE org_id = $1 AND network_id = $2 AND feed_key = $3 AND scope_hint = $4
               AND (
                    $5 = FALSE
                    OR CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT) < $6
                    OR (
                        CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT) = $6
                        AND $7::text IS NOT NULL
                        AND message_id < $7
                    )
               )
             ORDER BY created_at DESC, message_id DESC
             LIMIT $8",
        )?;
        let rows = stmt.query_map(
            params![
                self.org_id(),
                network_id,
                feed_key,
                canonical_scope_hint,
                has_anchor,
                before_created_at,
                before_message_id,
                limit as i64
            ],
            |r| {
                let content_ref_json: String = r.get(4)?;
                let content_json: String = r.get(5)?;
                let agent_envelope_json: Option<String> = r.get(3)?;
                let created_at_ms: i64 = r.get(7)?;
                Ok(TopicMessageRow {
                    message_id: r.get(0)?,
                    network_id: r.get(1)?,
                    feed_key: feed_key.to_owned(),
                    scope_hint: canonical_scope_hint.clone(),
                    author_node_id: r.get(2)?,
                    agent_envelope: agent_envelope_json
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|e| {
                            pg::Error::FromSqlConversionFailure(
                                0,
                                pg::types::Type::Text,
                                Box::new(e),
                            )
                        })?,
                    content_ref: serde_json::from_str(&content_ref_json).map_err(|e| {
                        pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                    })?,
                    content: serde_json::from_str(&content_json).map_err(|e| {
                        pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(e))
                    })?,
                    content_resolved_at: r.get::<_, Option<i64>>(8)?.map(|value| value as u64),
                    reply_to_message_id: r.get(6)?,
                    created_at: created_at_ms as u64,
                })
            },
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn list_topic_messages(
        &self,
        network_id: &str,
        feed_key: &str,
        scope_hint: &str,
        limit: usize,
    ) -> Result<Vec<TopicMessageRow>> {
        self.list_topic_messages_page(network_id, feed_key, scope_hint, None, None, limit)
    }

    pub fn get_topic_message(&self, message_id: &str) -> Result<Option<TopicMessageRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT network_id, feed_key, scope_hint, author_node_id, agent_envelope_json, content_ref_json, content_json,
                    reply_to_message_id, CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT),
                    CASE WHEN content_resolved_at IS NULL THEN NULL ELSE CAST(EXTRACT(EPOCH FROM content_resolved_at) * 1000 AS BIGINT) END
             FROM topic_messages
             WHERE org_id = $1 AND message_id = $2",
        )?;
        let mut rows = stmt.query_map(params![self.org_id(), message_id], |row| {
            let agent_envelope_json: Option<String> = row.get(4)?;
            let content_ref_json: String = row.get(5)?;
            let content_json: String = row.get(6)?;
            let scope_hint: String = row.get(2)?;
            let feed_key: String = row.get(1)?;
            Ok(TopicMessageRow {
                message_id: message_id.to_owned(),
                network_id: row.get(0)?,
                feed_key,
                scope_hint,
                author_node_id: row.get(3)?,
                agent_envelope: agent_envelope_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| {
                        pg::Error::FromSqlConversionFailure(
                            0,
                            pg::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                content_ref: serde_json::from_str(&content_ref_json).map_err(|error| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(error))
                })?,
                content: serde_json::from_str(&content_json).map_err(|error| {
                    pg::Error::FromSqlConversionFailure(0, pg::types::Type::Text, Box::new(error))
                })?,
                content_resolved_at: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
                reply_to_message_id: row.get(7)?,
                created_at: row.get::<_, i64>(8)? as u64,
            })
        })?;
        let Some(row) = rows.next() else {
            return Ok(None);
        };
        Ok(Some(row?))
    }

    pub fn update_topic_message_content(
        &self,
        message_id: &str,
        content: &serde_json::Value,
        resolved_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE topic_messages
             SET content_json = $3,
                 content_resolved_at = TIMESTAMPTZ 'epoch' + ($4::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND message_id = $2",
            params![
                self.org_id(),
                message_id,
                serde_json::to_string(content)?,
                resolved_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn update_topic_message_content_and_agent_envelope(
        &self,
        message_id: &str,
        content: &serde_json::Value,
        agent_envelope: Option<&AgentEnvelope>,
        resolved_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let updated = conn.execute(
            "UPDATE topic_messages
             SET content_json = $3,
                 agent_envelope_json = $4,
                 content_resolved_at = TIMESTAMPTZ 'epoch' + ($5::bigint * INTERVAL '1 millisecond')
             WHERE org_id = $1 AND message_id = $2",
            params![
                self.org_id(),
                message_id,
                serde_json::to_string(content)?,
                agent_envelope.map(serde_json::to_string).transpose()?,
                resolved_at as i64
            ],
        )?;
        if updated == 0 {
            return Err(SwarmError::Storage(format!(
                "topic message {message_id} not found for content update"
            ))
            .into());
        }
        Ok(())
    }

    pub fn upsert_topic_cursor(
        &self,
        network_id: &str,
        subscriber_node_id: &str,
        feed_key: &str,
        scope_hint: &str,
        last_event_seq: u64,
        updated_at: u64,
    ) -> Result<()> {
        let canonical_scope_hint = Self::canonical_scope_hint_or_original(scope_hint.to_owned());
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO topic_cursors(org_id, network_id, subscriber_node_id, feed_key, scope_hint, last_event_seq, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, TIMESTAMPTZ 'epoch' + ($7::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(org_id, network_id, subscriber_node_id, feed_key) DO UPDATE SET
               scope_hint = excluded.scope_hint,
               last_event_seq = GREATEST(topic_cursors.last_event_seq, excluded.last_event_seq),
               updated_at = excluded.updated_at",
            params![
                self.org_id(),
                network_id,
                subscriber_node_id,
                feed_key,
                canonical_scope_hint,
                last_event_seq as i64,
                updated_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn get_topic_cursor(
        &self,
        network_id: &str,
        subscriber_node_id: &str,
        feed_key: &str,
    ) -> Result<Option<TopicCursorRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT scope_hint, last_event_seq, CAST(EXTRACT(EPOCH FROM updated_at) * 1000 AS BIGINT)
             FROM topic_cursors
             WHERE org_id = $1 AND network_id = $2 AND subscriber_node_id = $3 AND feed_key = $4",
            params![self.org_id(), network_id, subscriber_node_id, feed_key],
            |r| {
                let updated_at_ms: i64 = r.get(2)?;
                let last_event_seq: i64 = r.get(1)?;
                Ok(TopicCursorRow {
                    network_id: network_id.to_owned(),
                    subscriber_node_id: subscriber_node_id.to_owned(),
                    feed_key: feed_key.to_owned(),
                    scope_hint: Self::canonical_scope_hint_or_original(r.get(0)?),
                    last_event_seq: last_event_seq as u64,
                    updated_at: updated_at_ms as u64,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_discoverable_feed_sources(&self, limit: usize) -> Result<Vec<DiscoverableFeedRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let subscription_rows = conn
            .prepare(
                "SELECT subscriber_node_id, feed_key, scope_hint
                 FROM feed_subscriptions
                 WHERE org_id = $1 AND active = TRUE",
            )?
            .query_map(params![self.org_id()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let announcement_rows = conn
            .prepare(
                "SELECT announcement_id, task_id, feed_key, scope_hint, announced_by_node_id,
                        CAST(EXTRACT(EPOCH FROM created_at) * 1000 AS BIGINT)
                 FROM task_announcements
                 WHERE org_id = $1",
            )?
            .query_map(params![self.org_id()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)? as u64,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut subscribers_by_feed = std::collections::BTreeMap::<
            (String, String),
            std::collections::BTreeSet<String>,
        >::new();
        for (subscriber_node_id, feed_key, raw_scope_hint) in subscription_rows {
            subscribers_by_feed
                .entry((
                    feed_key,
                    Self::canonical_scope_hint_or_original(raw_scope_hint),
                ))
                .or_default()
                .insert(subscriber_node_id);
        }

        let mut latest_by_feed =
            std::collections::BTreeMap::<(String, String), (String, String, String, u64)>::new();
        for (
            announcement_id,
            task_id,
            feed_key,
            raw_scope_hint,
            announced_by_node_id,
            created_at,
        ) in announcement_rows
        {
            let key = (
                feed_key,
                Self::canonical_scope_hint_or_original(raw_scope_hint),
            );
            let replace = match latest_by_feed.get(&key) {
                Some((existing_announcement_id, _, _, existing_created_at)) => {
                    created_at > *existing_created_at
                        || (created_at == *existing_created_at
                            && announcement_id > *existing_announcement_id)
                }
                None => true,
            };
            if replace {
                latest_by_feed.insert(
                    key,
                    (announcement_id, task_id, announced_by_node_id, created_at),
                );
            }
        }

        let mut feed_keys = subscribers_by_feed
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        feed_keys.extend(latest_by_feed.keys().cloned());

        let mut rows = Vec::with_capacity(feed_keys.len());
        for (feed_key, scope_hint) in feed_keys {
            let key = (feed_key.clone(), scope_hint.clone());
            let latest = latest_by_feed.get(&key);
            rows.push(DiscoverableFeedRow {
                feed_key,
                scope_hint,
                subscriber_count: subscribers_by_feed
                    .get(&key)
                    .map(|subscribers| subscribers.len() as u32)
                    .unwrap_or(0),
                latest_announcement_id: latest.map(|row| row.0.clone()),
                latest_task_id: latest.map(|row| row.1.clone()),
                latest_source_node_id: latest.map(|row| row.2.clone()),
                latest_announced_at: latest.map(|row| row.3),
            });
        }
        rows.sort_by(|left, right| {
            right
                .latest_announced_at
                .cmp(&left.latest_announced_at)
                .then_with(|| left.feed_key.cmp(&right.feed_key))
                .then_with(|| left.scope_hint.cmp(&right.scope_hint))
        });
        rows.truncate(limit);
        Ok(rows)
    }

    pub fn list_active_dissemination_domains(&self, limit: usize) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut domains = std::collections::BTreeSet::new();
        for sql in [
            "SELECT scope_hint FROM feed_subscriptions WHERE org_id = $1 AND active = TRUE",
            "SELECT scope_hint FROM task_announcements WHERE org_id = $1",
            "SELECT scope_hint FROM execution_set_projection WHERE org_id = $1",
        ] {
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map(params![self.org_id()], |r| r.get::<_, String>(0))?;
            for row in rows {
                domains.insert(Self::canonical_scope_hint_or_original(row?));
            }
        }
        Ok(domains.into_iter().take(limit).collect())
    }
}
