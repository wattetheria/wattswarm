use super::*;

impl Node {
    pub(crate) fn apply_to_projection(&mut self, event: &Event) -> Result<()> {
        self.store.begin_tx()?;
        let result = self.apply_to_projection_inner(event);
        if result.is_ok() {
            self.store.commit_tx()?;
        } else {
            self.store.rollback_tx()?;
        }
        result
    }

    fn apply_to_projection_inner(&mut self, event: &Event) -> Result<()> {
        match &event.payload {
            EventPayload::TaskCreated(contract) => {
                self.store.upsert_task_contract(contract, event.epoch)?;
                let hits = self.lookup_knowledge_hits(contract)?;
                self.record_knowledge_lookup(
                    contract,
                    event.created_at,
                    usize::min(hits.len(), 5) as u32,
                    false,
                    &hits,
                )?;
            }
            EventPayload::TaskClaimed(claim) => {
                self.store.upsert_lease(
                    &claim.task_id,
                    claim_role_str(claim.role),
                    &claim.claimer_node_id,
                    &claim.execution_id,
                    claim.lease_until,
                )?;
            }
            EventPayload::TaskClaimRenewed(claim) => {
                self.store.upsert_lease(
                    &claim.task_id,
                    claim_role_str(claim.role),
                    &claim.claimer_node_id,
                    &claim.execution_id,
                    claim.lease_until,
                )?;
            }
            EventPayload::TaskClaimReleased(claim) => {
                self.store.release_lease(
                    &claim.task_id,
                    claim_role_str(claim.role),
                    &claim.claimer_node_id,
                    &claim.execution_id,
                )?;
            }
            EventPayload::CandidateProposed(payload) => {
                self.store.put_candidate(
                    &payload.task_id,
                    &event.author_node_id,
                    &payload.candidate,
                )?;
                for reference in &payload.candidate.evidence_refs {
                    self.store.upsert_evidence_summary(
                        &reference.digest,
                        &reference.mime,
                        reference.size_bytes,
                        &sha256_hex(reference.uri.as_bytes()),
                        event.created_at,
                    )?;
                }
            }
            EventPayload::EvidenceAdded(payload) => {
                for reference in &payload.evidence_refs {
                    self.store.put_evidence_added(
                        &payload.task_id,
                        &payload.candidate_id,
                        reference,
                    )?;
                    self.store.upsert_evidence_summary(
                        &reference.digest,
                        &reference.mime,
                        reference.size_bytes,
                        &sha256_hex(reference.uri.as_bytes()),
                        event.created_at,
                    )?;
                }
            }
            EventPayload::EvidenceAvailable(payload) => {
                self.store.put_evidence_available(
                    &payload.task_id,
                    &payload.candidate_id,
                    &event.author_node_id,
                    &payload.evidence_digest,
                    event.created_at,
                )?;
            }
            EventPayload::VerifierResultSubmitted(payload) => {
                self.store.put_verifier_result(
                    &payload.task_id,
                    &event.author_node_id,
                    &payload.result,
                )?;
                self.record_unknown_reason_observations(
                    Some(&payload.task_id),
                    &payload.result.reason_codes,
                    &event.author_node_id,
                    &event.protocol_version,
                    event.created_at,
                )?;
                self.update_runtime_metrics_for_verifier_result(
                    &payload.task_id,
                    &payload.result,
                    event.created_at,
                )?;
                self.update_reputation_for_verifier_result(&payload.result, event.created_at)?;
            }
            EventPayload::VoteCommit(payload) => {
                self.store.put_vote_commit(
                    &payload.task_id,
                    &event.author_node_id,
                    &payload.candidate_hash,
                    &payload.commit_hash,
                    &payload.verifier_result_hash,
                    &payload.execution_id,
                    event.created_at,
                )?;
            }
            EventPayload::VoteReveal(payload) => {
                self.store.put_vote_reveal(&VoteRevealRow {
                    task_id: payload.task_id.clone(),
                    voter_node_id: event.author_node_id.clone(),
                    candidate_id: payload.candidate_id.clone(),
                    candidate_hash: payload.candidate_hash.clone(),
                    vote: payload.vote.clone(),
                    salt: payload.salt.clone(),
                    verifier_result_hash: payload.verifier_result_hash.clone(),
                    valid: true,
                    created_at: event.created_at,
                })?;
            }
            EventPayload::DecisionCommitted(payload) => {
                self.store
                    .set_task_committed(&payload.task_id, &payload.candidate_id)?;
            }
            EventPayload::DecisionFinalized(payload) => {
                let proof_json = serde_json::to_string(&payload.finality_proof)?;
                self.store.put_finalization(
                    &payload.task_id,
                    payload.epoch,
                    &payload.candidate_id,
                    &proof_json,
                    &event.event_id,
                )?;
                let task_for_mode = self.require_task(&payload.task_id)?;
                if matches!(
                    task_for_mode.contract.task_mode,
                    crate::types::TaskMode::OneShot
                ) {
                    self.store
                        .set_task_finalized(&payload.task_id, &payload.candidate_id)?;
                } else {
                    // Continuous mode: record both committed and finalized so
                    // settlement feedback penalty can locate the winning candidate.
                    self.store
                        .set_task_committed(&payload.task_id, &payload.candidate_id)?;
                    self.store.set_task_finalized_candidate_only(
                        &payload.task_id,
                        &payload.candidate_id,
                    )?;
                }
                let task = self.require_task(&payload.task_id)?;
                let candidate = self
                    .store
                    .get_candidate_by_id(&payload.task_id, &payload.candidate_id)?
                    .ok_or_else(|| {
                        SwarmError::NotFound("candidate missing for finalization".into())
                    })?;
                let output_digest = sha256_hex(&serde_json::to_vec(&candidate.output)?);
                let policy_snapshot_digest = sha256_hex(&serde_json::to_vec(
                    &task.contract.acceptance.verifier_policy,
                )?);
                let reason_codes: Vec<u16> = vec![REASON_SCHEMA_OK];
                self.store.put_decision_memory(
                    &payload.task_id,
                    payload.epoch,
                    &event.event_id,
                    event.created_at,
                    &payload.winning_candidate_hash,
                    &output_digest,
                    &candidate.output,
                    &reason_codes,
                    &policy_snapshot_digest,
                    &task.contract.task_type,
                    &sha256_hex(&serde_json::to_vec(&task.contract.inputs)?),
                    &sha256_hex(&serde_json::to_vec(&task.contract.output_schema)?),
                    &task.contract.acceptance.verifier_policy.policy_id,
                    &sha256_hex(&serde_json::to_vec(
                        &task.contract.acceptance.verifier_policy.policy_params,
                    )?),
                )?;
                let window_end = event
                    .created_at
                    .saturating_add(task.contract.acceptance.settlement.window_ms);
                self.store.put_task_settlement(
                    &payload.task_id,
                    payload.epoch,
                    event.created_at,
                    window_end,
                )?;
            }
            EventPayload::TaskError(payload) => {
                let task = self.require_task(&payload.task_id)?;
                let policy_snapshot_digest = sha256_hex(&serde_json::to_vec(
                    &task.contract.acceptance.verifier_policy,
                )?);
                self.store.put_decision_memory(
                    &payload.task_id,
                    event.epoch,
                    &event.event_id,
                    event.created_at,
                    "",
                    "",
                    &serde_json::json!({"task_error": payload.message}),
                    &payload.reason_codes,
                    &policy_snapshot_digest,
                    &task.contract.task_type,
                    &sha256_hex(&serde_json::to_vec(&task.contract.inputs)?),
                    &sha256_hex(&serde_json::to_vec(&task.contract.output_schema)?),
                    &task.contract.acceptance.verifier_policy.policy_id,
                    &sha256_hex(&serde_json::to_vec(
                        &task.contract.acceptance.verifier_policy.policy_params,
                    )?),
                )?;
                self.record_unknown_reason_observations(
                    Some(&payload.task_id),
                    &payload.reason_codes,
                    &event.author_node_id,
                    &event.protocol_version,
                    event.created_at,
                )?;
            }
            EventPayload::TaskRetryScheduled(payload) => {
                self.store
                    .set_task_retry_attempt(&payload.task_id, payload.attempt)?;
                self.store.clear_votes_for_task(&payload.task_id)?;
            }
            EventPayload::TaskExpired(payload) => {
                self.store
                    .set_task_terminal_state(&payload.task_id, TaskTerminalState::Expired)?;
                if let Ok(task) = self.require_task(&payload.task_id) {
                    let policy_snapshot_digest = sha256_hex(&serde_json::to_vec(
                        &task.contract.acceptance.verifier_policy,
                    )?);
                    self.store.put_decision_memory(
                        &payload.task_id,
                        event.epoch,
                        &event.event_id,
                        event.created_at,
                        "",
                        "",
                        &serde_json::json!({"expired": true}),
                        &[REASON_TASK_EXPIRED],
                        &policy_snapshot_digest,
                        &task.contract.task_type,
                        &sha256_hex(&serde_json::to_vec(&task.contract.inputs)?),
                        &sha256_hex(&serde_json::to_vec(&task.contract.output_schema)?),
                        &task.contract.acceptance.verifier_policy.policy_id,
                        &sha256_hex(&serde_json::to_vec(
                            &task.contract.acceptance.verifier_policy.policy_params,
                        )?),
                    )?;
                }
            }
            EventPayload::EpochEnded(payload) => {
                let task = self.require_task(&payload.task_id)?;
                if matches!(task.contract.task_mode, crate::types::TaskMode::Continuous) {
                    self.store
                        .advance_task_epoch(&payload.task_id, payload.epoch.saturating_add(1))?;
                }
            }
            EventPayload::TaskStopped(payload) => {
                self.store
                    .set_task_terminal_state(&payload.task_id, TaskTerminalState::Stopped)?;
            }
            EventPayload::TaskSuspended(payload) => {
                self.store
                    .set_task_terminal_state(&payload.task_id, TaskTerminalState::Suspended)?;
            }
            EventPayload::TaskKilled(payload) => {
                self.store
                    .set_task_terminal_state(&payload.task_id, TaskTerminalState::Killed)?;
            }
            EventPayload::CheckpointCreated(payload) => {
                self.store.put_checkpoint(
                    &payload.checkpoint_id,
                    payload.up_to_seq,
                    &event.event_id,
                )?;
            }
            EventPayload::MembershipUpdated(payload) => {
                self.store
                    .put_membership(&serde_json::to_string(&payload.new_membership)?)?;
            }
            EventPayload::PolicyTuned(payload) => {
                if let Some(version) = self
                    .policy_registry
                    .get(&payload.policy_id)
                    .map(|policy| policy.version().to_owned())
                {
                    self.policy_registry.allow_compatible_hash(
                        &payload.policy_id,
                        &version,
                        &payload.to_policy_hash,
                    );
                }
            }
            EventPayload::AdvisoryCreated(payload) => {
                self.store.put_advisory_created(
                    &payload.advisory_id,
                    &payload.policy_id,
                    &payload.suggested_policy_hash,
                    event.created_at,
                )?;
            }
            EventPayload::AdvisoryApproved(payload) => {
                self.store.mark_advisory_approved(
                    &payload.advisory_id,
                    &payload.admin_node_id,
                    event.created_at,
                )?;
            }
            EventPayload::AdvisoryApplied(payload) => {
                self.store.mark_advisory_applied(
                    &payload.advisory_id,
                    &payload.applied_policy_hash,
                    event.created_at,
                )?;
            }
            EventPayload::TaskFeedbackReported(payload) => {
                self.store.mark_task_bad_feedback(
                    &payload.task_id,
                    payload.epoch,
                    payload.timestamp,
                )?;
                self.apply_bad_feedback_penalty(
                    &payload.task_id,
                    payload.epoch,
                    payload.timestamp,
                )?;
            }
            EventPayload::ReuseRejectRecorded(payload) => {
                self.store
                    .mark_decision_deprecated_by_hash(&payload.decision_ref.final_commit_hash)?;
                self.store.add_reuse_blacklist(
                    &payload.task_id,
                    event.epoch,
                    &payload.candidate_hash,
                )?;
            }
        }
        if let Some(task_id) = event.task_id.as_deref() {
            if let Some((stage, cost)) = stage_cost_for_payload(&event.payload) {
                self.store
                    .mark_stage_cost(task_id, event.epoch, stage, cost)?;
            }
            self.refresh_task_cost_report(task_id, event.epoch)?;
        }
        Ok(())
    }

    pub(crate) fn load_membership(&self) -> Result<Membership> {
        if let Some(raw) = self.store.load_membership()? {
            let parsed = serde_json::from_str::<Membership>(&raw)?;
            Ok(parsed)
        } else {
            Ok(self.genesis_membership.clone())
        }
    }
}
