use super::*;

impl Node {
    pub(crate) fn record_knowledge_lookup(
        &self,
        contract: &TaskContract,
        lookup_time: u64,
        hit_count: u32,
        reuse_applied: bool,
        hits: &[KnowledgeHit],
    ) -> Result<()> {
        let input_digest = sha256_hex(&serde_json::to_vec(&contract.inputs)?);
        let hit_refs: Vec<_> = hits
            .iter()
            .take(5)
            .map(|h| {
                format!(
                    "{}:{}:{}:{:?}",
                    h.decision_ref.task_id,
                    h.decision_ref.epoch,
                    h.decision_ref.final_commit_hash,
                    h.hit_type
                )
            })
            .collect();
        let hits_digest = sha256_hex(&serde_json::to_vec(&hit_refs)?);
        self.store.put_knowledge_lookup(
            &contract.task_id,
            &contract.task_type,
            &input_digest,
            lookup_time,
            hit_count,
            &hits_digest,
            reuse_applied,
        )?;
        Ok(())
    }

    pub(crate) fn lookup_knowledge_hits(
        &self,
        contract: &TaskContract,
    ) -> Result<Vec<KnowledgeHit>> {
        let input_digest = sha256_hex(&serde_json::to_vec(&contract.inputs)?);
        let output_schema_digest = sha256_hex(&serde_json::to_vec(&contract.output_schema)?);
        let policy_params_digest = sha256_hex(&serde_json::to_vec(
            &contract.acceptance.verifier_policy.policy_params,
        )?);

        let rows = self
            .store
            .list_decision_memory_hits_by_task_type(&contract.task_type, 32)?;

        let mut hits = rows
            .into_iter()
            .map(|row| {
                let exact_match = row.input_digest == input_digest
                    && row.output_schema_digest == output_schema_digest
                    && row.policy_id == contract.acceptance.verifier_policy.policy_id
                    && row.policy_params_digest == policy_params_digest
                    && !row.deprecated_as_exact;
                let hit_type = if exact_match {
                    KnowledgeHitType::Exact
                } else {
                    KnowledgeHitType::Similar
                };
                KnowledgeHit {
                    hit_type,
                    decision_ref: crate::types::DecisionReference {
                        task_id: row.task_id,
                        epoch: row.epoch,
                        final_commit_hash: row.final_commit_hash,
                    },
                    reuse_payload: row.result_summary,
                    evidence_digests: Vec::new(),
                    reason_codes_summary: serde_json::json!({
                        "reason_codes": row.reason_codes
                    }),
                    confidence_hint: if exact_match {
                        1.0
                    } else {
                        row.confidence_hint
                    },
                }
            })
            .collect::<Vec<_>>();

        hits.sort_by(|a, b| match (&a.hit_type, &b.hit_type) {
            (KnowledgeHitType::Exact, KnowledgeHitType::Similar) => std::cmp::Ordering::Less,
            (KnowledgeHitType::Similar, KnowledgeHitType::Exact) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        });
        if hits.len() > 5 {
            hits.truncate(5);
        }
        Ok(hits)
    }

    pub(crate) fn seed_bundle_from_hits(
        &self,
        task_id: &str,
        epoch: u64,
        hits: &[KnowledgeHit],
        contract: &TaskContract,
        reuse_applied: bool,
    ) -> Result<Option<SeedBundle>> {
        let mut selected = Vec::new();
        if reuse_applied {
            selected.extend(
                hits.iter()
                    .filter(|h| h.hit_type == KnowledgeHitType::Exact)
                    .take(1)
                    .cloned(),
            );
        }
        selected.extend(
            hits.iter()
                .filter(|h| h.hit_type == KnowledgeHitType::Similar)
                .take(5)
                .cloned(),
        );
        if selected.is_empty() {
            return Ok(None);
        }

        let blacklist = self.store.list_reuse_blacklist(task_id, epoch)?;
        Ok(Some(SeedBundle {
            seed_type: "SIMILAR_HITS_V1".to_owned(),
            hits: selected,
            constraints: SeedConstraints {
                must_consider_claims: vec![
                    format!(
                        "policy:{}:{}",
                        contract.acceptance.verifier_policy.policy_id,
                        contract.acceptance.verifier_policy.policy_version
                    ),
                    format!("task_type:{}", contract.task_type),
                ],
                must_avoid_blacklist_candidate_hashes: blacklist,
            },
        }))
    }

    pub(crate) fn record_unknown_reason_observations(
        &self,
        task_id: Option<&str>,
        reason_codes: &[u16],
        author_node_id: &str,
        peer_protocol_version: &str,
        observed_at: u64,
    ) -> Result<()> {
        for code in reason_codes {
            if is_protocol_reason_code(*code) {
                continue;
            }
            self.store.record_unknown_reason_observation(
                task_id,
                *code,
                peer_protocol_version,
                LOCAL_PROTOCOL_VERSION,
                author_node_id,
                observed_at,
            )?;
        }
        Ok(())
    }

    pub(crate) fn update_runtime_metrics_for_verifier_result(
        &self,
        task_id: &str,
        result: &VerifierResult,
        created_at: u64,
    ) -> Result<()> {
        let task = self.require_task(task_id)?;
        let created = self.store.task_created_at(task_id)?.unwrap_or(created_at);
        let latency_ms = created_at.saturating_sub(created);
        let (window_start, window_end) = daily_window(created_at);
        let timeout = result
            .reason_codes
            .iter()
            .any(|code| *code == REASON_TASK_TIMEOUT || *code == REASON_EVIDENCE_TIMEOUT);
        let crash = result.reason_codes.contains(&REASON_RUNTIME_CRASH);
        let invalid_output = result.reason_codes.contains(&REASON_INVALID_OUTPUT);
        let reject_reason_codes: Vec<u16> = if result.passed {
            Vec::new()
        } else {
            result.reason_codes.clone()
        };
        let observation = RuntimeMetricObservation {
            runtime_id: &result.provider_family,
            profile_id: &result.model_id,
            task_type: &task.contract.task_type,
            window_start,
            window_end,
            finalized: result.passed,
            timeout,
            crash,
            invalid_output,
            latency_ms,
            cost_units: task.contract.budget.cost_units,
            reject_reason_codes: &reject_reason_codes,
        };
        self.store.upsert_runtime_metric_observation(&observation)
    }

    pub(crate) fn update_reputation_for_verifier_result(
        &self,
        result: &VerifierResult,
        created_at: u64,
    ) -> Result<()> {
        let mut stability_delta = 0_i64;
        let mut quality_delta = 0_i64;
        if result.passed {
            quality_delta += REPUTATION_SCALE / 10;
        } else if result.verification_status == VerificationStatus::Failed {
            quality_delta -= REPUTATION_SCALE / 10;
        }
        if result.reason_codes.contains(&REASON_TASK_TIMEOUT)
            || result.reason_codes.contains(&REASON_EVIDENCE_TIMEOUT)
            || result.reason_codes.contains(&REASON_RUNTIME_CRASH)
            || result.reason_codes.contains(&REASON_INVALID_OUTPUT)
        {
            stability_delta -= REPUTATION_SCALE / 10;
        }
        if stability_delta != 0 || quality_delta != 0 {
            self.store.adjust_reputation(
                &result.provider_family,
                &result.model_id,
                stability_delta,
                quality_delta,
                created_at,
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_bad_feedback_penalty(
        &self,
        task_id: &str,
        epoch: u64,
        now: u64,
    ) -> Result<()> {
        let task = self.require_task(task_id)?;
        let finalization_epoch = self
            .store
            .get_task_settlement(task_id)?
            .map(|row| row.epoch)
            .unwrap_or(epoch);
        if finalization_epoch != epoch {
            return Ok(());
        }
        let penalty_units = task
            .contract
            .acceptance
            .settlement
            .bad_penalty
            .p
            .saturating_mul(REPUTATION_SCALE);
        let Some(final_candidate_id) = task.finalized_candidate_id else {
            return Ok(());
        };
        let results = self
            .store
            .list_verifier_results_for_candidate(task_id, &final_candidate_id)?;
        for result in results {
            self.store.adjust_reputation(
                &result.provider_family,
                &result.model_id,
                -penalty_units,
                -penalty_units,
                now,
            )?;
        }
        Ok(())
    }

    pub(crate) fn refresh_task_cost_report(&self, task_id: &str, epoch: u64) -> Result<()> {
        let usage = self.store.get_stage_usage(task_id, epoch)?.unwrap_or(
            crate::storage::TaskStageUsageRow {
                task_id: task_id.to_owned(),
                epoch,
                explore_used: 0,
                verify_used: 0,
                finalize_used: 0,
            },
        );
        let cost_units_by_stage = serde_json::json!({
            "explore": usage.explore_used,
            "verify": usage.verify_used,
            "finalize": usage.finalize_used
        });
        let latency_by_stage = serde_json::json!({
            "explore_ms": usage.explore_used.saturating_mul(10),
            "verify_ms": usage.verify_used.saturating_mul(20),
            "finalize_ms": usage.finalize_used.saturating_mul(5)
        });
        let events_emitted_count = usage
            .explore_used
            .saturating_add(usage.verify_used)
            .saturating_add(usage.finalize_used);
        let task = match self.require_task(task_id) {
            Ok(t) => t,
            Err(_) => return Ok(()),
        };
        let lookups = self.store.count_reuse_applied_lookups(task_id)?;
        let cache_hit_rate = if task.contract.assignment.explore.topk == 0 {
            0.0
        } else {
            f64::from(lookups) / f64::from(task.contract.assignment.explore.topk)
        };
        let evidence_fetch_bytes = task
            .committed_candidate_id
            .as_deref()
            .and_then(|cid| self.store.get_candidate_by_id(task_id, cid).ok().flatten())
            .map(|candidate| {
                candidate
                    .evidence_refs
                    .iter()
                    .map(|r| r.size_bytes)
                    .fold(0_u64, |a, b| a.saturating_add(b))
            })
            .unwrap_or(0);
        self.store.upsert_task_cost_report(
            task_id,
            epoch,
            &cost_units_by_stage,
            &latency_by_stage,
            evidence_fetch_bytes,
            events_emitted_count,
            cache_hit_rate.clamp(0.0, 1.0),
        )?;
        Ok(())
    }

    pub(crate) fn require_task(&self, task_id: &str) -> Result<TaskProjectionRow> {
        self.store
            .task_projection(task_id)?
            .ok_or_else(|| SwarmError::NotFound(format!("task {task_id} missing")).into())
    }

    pub(crate) fn assert_valid_lease(
        &self,
        task_id: &str,
        role: ClaimRole,
        author_node_id: &str,
        execution_id: &str,
        now: u64,
    ) -> Result<()> {
        let role_str = claim_role_str(role);
        let lease = self
            .store
            .get_lease(task_id, role_str)?
            .ok_or_else(|| SwarmError::NotFound("lease missing".into()))?;

        if lease.claimer_node_id != author_node_id {
            return Err(SwarmError::InvalidEvent("lease claimer mismatch".into()).into());
        }
        if lease.execution_id != execution_id {
            return Err(SwarmError::InvalidEvent("lease execution_id mismatch".into()).into());
        }
        if is_deadline_expired(lease.lease_until, now) {
            return Err(SwarmError::InvalidEvent("lease expired".into()).into());
        }
        Ok(())
    }
}
