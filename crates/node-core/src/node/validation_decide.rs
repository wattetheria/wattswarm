use super::*;

impl Node {
    pub(crate) fn validate_vote_commit(
        &self,
        event: &Event,
        payload: &VoteCommitPayload,
    ) -> Result<()> {
        self.assert_valid_lease(
            &payload.task_id,
            ClaimRole::Verify,
            &event.author_node_id,
            &payload.execution_id,
            event.created_at,
        )?;

        let candidate = self
            .store
            .get_candidate_by_id(&payload.task_id, &payload.candidate_id)?
            .ok_or_else(|| SwarmError::NotFound("candidate missing for vote commit".into()))?;
        let expected_candidate_hash = candidate_hash(&candidate)?;
        if expected_candidate_hash != payload.candidate_hash {
            return Err(
                SwarmError::InvalidEvent("vote commit candidate_hash mismatch".into()).into(),
            );
        }

        if payload.commit_hash.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("commit hash required".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_vote_reveal(
        &self,
        event: &Event,
        payload: &VoteRevealPayload,
    ) -> Result<()> {
        self.assert_valid_lease(
            &payload.task_id,
            ClaimRole::Verify,
            &event.author_node_id,
            &payload.execution_id,
            event.created_at,
        )?;

        let task = self.require_task(&payload.task_id)?;
        let commit = self
            .store
            .get_vote_commit(&payload.task_id, &event.author_node_id)?
            .ok_or_else(|| SwarmError::NotFound("vote commit missing".into()))?;

        if commit.execution_id != payload.execution_id {
            return Err(SwarmError::InvalidEvent("vote reveal execution mismatch".into()).into());
        }
        if commit.verifier_result_hash != payload.verifier_result_hash {
            return Err(SwarmError::InvalidEvent(
                "vote reveal verifier_result_hash mismatch".into(),
            )
            .into());
        }
        if commit.candidate_hash != payload.candidate_hash {
            return Err(
                SwarmError::InvalidEvent("vote reveal candidate_hash mismatch".into()).into(),
            );
        }

        let reveal_deadline = commit.created_at + task.contract.acceptance.vote.reveal_deadline_ms;
        if event.created_at > reveal_deadline {
            return Err(SwarmError::InvalidEvent("vote reveal passed deadline".into()).into());
        }

        let expected_commit_hash = vote_commit_hash(
            payload.vote.clone(),
            &payload.salt,
            &payload.verifier_result_hash,
        );
        if expected_commit_hash != commit.commit_hash {
            return Err(SwarmError::InvalidEvent("vote reveal commit mismatch".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_decision_committed(
        &self,
        payload: &DecisionCommittedPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;
        if task.terminal_state != TaskTerminalState::Open {
            return Err(SwarmError::InvalidEvent("cannot commit on closed task".into()).into());
        }
        let candidate = self
            .store
            .get_candidate_by_id(&payload.task_id, &payload.candidate_id)?
            .ok_or_else(|| SwarmError::NotFound("candidate missing for commit".into()))?;
        let expected_hash = candidate_hash(&candidate)?;
        if payload.candidate_hash != expected_hash {
            return Err(
                SwarmError::InvalidEvent("decision commit candidate_hash mismatch".into()).into(),
            );
        }

        let approvals = self.store.count_valid_votes_for_candidate_hash(
            &payload.task_id,
            &payload.candidate_hash,
            VoteChoice::Approve,
        )?;
        if approvals < task.contract.acceptance.quorum_threshold {
            return Err(
                SwarmError::InvalidEvent("insufficient reveal votes for commit".into()).into(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate_decision_finalized(
        &self,
        payload: &DecisionFinalizedPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;

        if task.terminal_state == TaskTerminalState::Expired {
            return Err(SwarmError::InvalidEvent("cannot finalize expired task".into()).into());
        }

        if let Some(existing) = self
            .store
            .get_finalization_candidate(&payload.task_id, payload.epoch)?
            && existing != payload.candidate_id
        {
            return Err(SwarmError::InvalidEvent("forked finalize detected".into()).into());
        }

        if task.committed_candidate_id.as_deref() != Some(payload.candidate_id.as_str()) {
            return Err(
                SwarmError::InvalidEvent("finalize candidate must be committed".into()).into(),
            );
        }
        let candidate = self
            .store
            .get_candidate_by_id(&payload.task_id, &payload.candidate_id)?
            .ok_or_else(|| SwarmError::NotFound("candidate missing for finalize".into()))?;
        let expected_hash = candidate_hash(&candidate)?;
        if payload.winning_candidate_hash != expected_hash {
            return Err(SwarmError::InvalidEvent(
                "finalize winning_candidate_hash mismatch".into(),
            )
            .into());
        }
        for reference in &candidate.evidence_refs {
            let available = self.store.count_evidence_available(
                &payload.task_id,
                &payload.candidate_id,
                &reference.digest,
            )?;
            if available < task.contract.acceptance.da_quorum_threshold {
                return Err(SwarmError::InvalidEvent(format!(
                    "insufficient DA confirmations for evidence digest {}",
                    reference.digest
                ))
                .into());
            }
        }

        if payload.finality_proof.signatures.len() < payload.finality_proof.threshold as usize {
            return Err(SwarmError::InvalidEvent("insufficient finality signatures".into()).into());
        }

        let membership = self.load_membership()?;
        let message = finality_message(&payload.task_id, payload.epoch, &payload.candidate_id);
        let mut seen = HashSet::new();
        let mut valid_count = 0u32;
        for sig in &payload.finality_proof.signatures {
            if !seen.insert(sig.signer_node_id.clone()) {
                continue;
            }
            if !membership.has_role(&sig.signer_node_id, Role::Finalizer) {
                continue;
            }
            if verify_signature(&sig.signer_node_id, message.as_bytes(), &sig.signature_hex).is_ok()
            {
                valid_count += 1;
            }
        }
        if valid_count < payload.finality_proof.threshold {
            return Err(
                SwarmError::InvalidEvent("finality proof signature verify failed".into()).into(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate_task_expired(
        &self,
        event: &Event,
        payload: &crate::types::TaskExpiredPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;
        if task.terminal_state == TaskTerminalState::Finalized {
            return Err(SwarmError::InvalidEvent("cannot expire finalized task".into()).into());
        }
        if event.created_at < task.contract.expiry_ms {
            return Err(SwarmError::InvalidEvent("task not reached expiry_ms".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_task_error(&self, payload: &TaskErrorPayload) -> Result<()> {
        if payload.reason_codes.is_empty() {
            return Err(SwarmError::InvalidEvent("task_error reason_codes required".into()).into());
        }
        for code in &payload.reason_codes {
            if *code == REASON_CUSTOM_ERROR {
                let ns = payload
                    .custom_reason_namespace
                    .as_deref()
                    .unwrap_or_default();
                let c = payload.custom_reason_code.as_deref().unwrap_or_default();
                let msg = payload.custom_reason_message.as_deref().unwrap_or_default();
                if ns.is_empty() || c.is_empty() {
                    return Err(SwarmError::InvalidEvent(
                        "custom reason requires namespace and reason code".into(),
                    )
                    .into());
                }
                if ns.len() > 64 || c.len() > 64 || msg.len() > 4096 {
                    return Err(SwarmError::InvalidEvent(
                        "custom reason fields exceed limits".into(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_membership_update(
        &self,
        payload: &MembershipUpdatedPayload,
    ) -> Result<()> {
        if payload.quorum_signatures.len() < payload.quorum_threshold as usize {
            return Err(SwarmError::InvalidEvent(
                "membership quorum signatures insufficient".into(),
            )
            .into());
        }

        let current = self.load_membership()?;
        let message = serde_json::to_vec(&payload.new_membership)?;
        let mut unique = HashSet::new();
        let mut valid = 0u32;
        for sig in &payload.quorum_signatures {
            if !unique.insert(sig.signer_node_id.clone()) {
                continue;
            }
            if !current.has_role(&sig.signer_node_id, Role::Finalizer) {
                continue;
            }
            if verify_signature(&sig.signer_node_id, &message, &sig.signature_hex).is_ok() {
                valid += 1;
            }
        }
        if valid < payload.quorum_threshold {
            return Err(SwarmError::InvalidEvent("membership signatures invalid".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_advisory_created(
        &self,
        payload: &crate::types::AdvisoryCreatedPayload,
    ) -> Result<()> {
        if payload.advisory_id.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("advisory_id required".into()).into());
        }
        if payload.reason.trim().is_empty() || payload.reason.len() > MAX_INLINE_EVIDENCE_BYTES {
            return Err(SwarmError::InvalidEvent("advisory reason must be 1..64KB".into()).into());
        }
        if self
            .store
            .get_advisory_state(&payload.advisory_id)?
            .is_some()
        {
            return Err(SwarmError::Conflict("advisory already exists".into()).into());
        }
        if self.policy_registry.get(&payload.policy_id).is_none() {
            return Err(SwarmError::InvalidEvent("advisory policy_id missing".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_advisory_approved(
        &self,
        event: &Event,
        payload: &crate::types::AdvisoryApprovedPayload,
    ) -> Result<()> {
        let state = self
            .store
            .get_advisory_state(&payload.advisory_id)?
            .ok_or_else(|| SwarmError::NotFound("advisory missing".into()))?;
        if payload.admin_node_id.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("admin_node_id required".into()).into());
        }
        if payload.admin_node_id != event.author_node_id {
            return Err(SwarmError::InvalidEvent("admin_node_id must equal author".into()).into());
        }
        if state.status != "created" {
            return Err(
                SwarmError::InvalidEvent("advisory must be in created state".into()).into(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate_advisory_applied(
        &self,
        payload: &crate::types::AdvisoryAppliedPayload,
    ) -> Result<()> {
        let state = self
            .store
            .get_advisory_state(&payload.advisory_id)?
            .ok_or_else(|| SwarmError::NotFound("advisory missing".into()))?;
        if payload.applied_policy_hash.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("applied_policy_hash required".into()).into());
        }
        if state.status != "approved" {
            return Err(
                SwarmError::InvalidEvent("advisory must be approved before apply".into()).into(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate_policy_tuned(
        &self,
        payload: &crate::types::PolicyTunedPayload,
    ) -> Result<()> {
        if payload.policy_id.trim().is_empty()
            || payload.from_policy_hash.trim().is_empty()
            || payload.to_policy_hash.trim().is_empty()
        {
            return Err(SwarmError::InvalidEvent("policy_tuned fields required".into()).into());
        }
        if payload.from_policy_hash == payload.to_policy_hash {
            return Err(
                SwarmError::InvalidEvent("policy_tuned requires hash change".into()).into(),
            );
        }
        let advisory = self
            .store
            .get_advisory_state(&payload.advisory_id)?
            .ok_or_else(|| SwarmError::NotFound("policy_tuned advisory missing".into()))?;
        if advisory.policy_id != payload.policy_id {
            return Err(SwarmError::InvalidEvent(
                "policy_tuned policy_id mismatch advisory".into(),
            )
            .into());
        }
        if advisory.status != "applied" {
            return Err(
                SwarmError::InvalidEvent("policy_tuned requires applied advisory".into()).into(),
            );
        }
        if advisory.applied_policy_hash.as_deref() != Some(payload.to_policy_hash.as_str()) {
            return Err(SwarmError::InvalidEvent(
                "policy_tuned target hash mismatch advisory".into(),
            )
            .into());
        }
        if self.policy_registry.get(&payload.policy_id).is_none() {
            return Err(SwarmError::InvalidEvent(
                "policy_tuned policy_id missing in registry".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_task_feedback_reported(
        &self,
        _event: &Event,
        payload: &crate::types::TaskFeedbackReportedPayload,
    ) -> Result<()> {
        if payload.outcome != "BAD" {
            return Err(SwarmError::InvalidEvent("feedback outcome must be BAD".into()).into());
        }
        if payload.reason.len() > MAX_INLINE_EVIDENCE_BYTES {
            return Err(SwarmError::InvalidEvent("feedback reason exceeds 64KB".into()).into());
        }
        let task = self.require_task(&payload.task_id)?;
        if task.terminal_state != TaskTerminalState::Finalized {
            return Err(
                SwarmError::InvalidEvent("feedback requires finalized task state".into()).into(),
            );
        }
        let authority = task
            .contract
            .acceptance
            .settlement
            .feedback
            .authority_pubkey
            .trim();
        if authority.is_empty() {
            return Err(SwarmError::InvalidEvent("feedback authority missing".into()).into());
        }
        if payload.signed_by != authority {
            return Err(SwarmError::InvalidEvent("feedback signed_by mismatch".into()).into());
        }
        let public_key = payload.signed_by.trim_start_matches("ed25519:");
        let message = feedback_message(
            &payload.task_id,
            payload.epoch,
            &payload.outcome,
            &payload.reason,
            payload.timestamp,
        );
        verify_signature(public_key, message.as_bytes(), &payload.signature)
            .context("feedback signature verify failed")?;

        let settlement = self
            .store
            .get_task_settlement(&payload.task_id)?
            .ok_or_else(|| SwarmError::InvalidEvent("task settlement missing".into()))?;
        if settlement.epoch != payload.epoch {
            return Err(SwarmError::InvalidEvent(
                "feedback epoch does not match active settlement".into(),
            )
            .into());
        }
        if payload.timestamp < settlement.finalized_at
            || payload.timestamp > settlement.window_end_at
        {
            return Err(
                SwarmError::InvalidEvent("feedback timestamp outside window".into()).into(),
            );
        }
        if settlement.bad_feedback_exists {
            return Err(SwarmError::InvalidEvent("duplicate BAD feedback for task".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_reuse_reject_recorded(
        &self,
        payload: &crate::types::ReuseRejectRecordedPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;
        if payload.reject_quorum_proof.is_empty() {
            return Err(
                SwarmError::InvalidEvent("reuse reject quorum proof required".into()).into(),
            );
        }
        if !self.store.has_decision_reference(
            &payload.decision_ref.task_id,
            payload.decision_ref.epoch,
            &payload.decision_ref.final_commit_hash,
        )? {
            return Err(
                SwarmError::InvalidEvent("reuse reject decision_ref not found".into()).into(),
            );
        }
        if payload.reason_codes.is_empty() {
            return Err(
                SwarmError::InvalidEvent("reuse reject reason_codes required".into()).into(),
            );
        }
        let rejects = self.store.count_valid_votes_for_candidate_hash(
            &payload.task_id,
            &payload.candidate_hash,
            VoteChoice::Reject,
        )?;
        if rejects < task.contract.acceptance.quorum_threshold {
            return Err(SwarmError::InvalidEvent(
                "reuse reject record requires reject quorum".into(),
            )
            .into());
        }
        Ok(())
    }
}
