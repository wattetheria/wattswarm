use super::*;

impl Node {
    pub(crate) fn validate_event(&self, event: &Event) -> Result<()> {
        verify_event_signature(event).context("event signature verify")?;

        let bytes = serde_json::to_vec(event)?;
        if bytes.len() > MAX_EVENT_BYTES {
            return Err(SwarmError::InvalidEvent("event bytes > 128KB".into()).into());
        }
        let payload_bytes = serde_json::to_vec(&event.payload)?;
        if payload_bytes.len() > MAX_EVENT_PAYLOAD_BYTES {
            return Err(SwarmError::InvalidEvent("event payload bytes > limit".into()).into());
        }

        if event.payload.kind() != event.event_kind {
            return Err(SwarmError::InvalidEvent("event_kind mismatch".into()).into());
        }
        if event.protocol_version.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("protocol_version required".into()).into());
        }
        if event.payload.task_id().map(ToOwned::to_owned) != event.task_id {
            return Err(SwarmError::InvalidEvent("task_id mismatch".into()).into());
        }

        self.validate_role_permission(event)?;

        if let Some(task_id) = &event.task_id
            && let Some(task) = self.store.task_projection(task_id)?
        {
            if matches!(
                task.terminal_state,
                TaskTerminalState::Expired
                    | TaskTerminalState::Finalized
                    | TaskTerminalState::Stopped
                    | TaskTerminalState::Suspended
                    | TaskTerminalState::Killed
            ) {
                let allow = matches!(
                    event.payload,
                    EventPayload::CheckpointCreated(_)
                        | EventPayload::MembershipUpdated(_)
                        | EventPayload::TaskFeedbackReported(_)
                );
                if !allow {
                    return Err(SwarmError::InvalidEvent("task already closed".into()).into());
                }
            }

            if event.created_at > task.contract.expiry_ms {
                match event.payload {
                    EventPayload::TaskExpired(_) => {}
                    EventPayload::DecisionCommitted(_)
                    | EventPayload::DecisionFinalized(_)
                    | EventPayload::EpochEnded(_) => {
                        return Err(SwarmError::InvalidEvent(
                            "cannot commit/finalize after expiry".into(),
                        )
                        .into());
                    }
                    _ => {}
                }
            }

            self.validate_stage_budget(&task, event)?;
        }

        match &event.payload {
            EventPayload::TaskCreated(contract) => self.validate_task_created(contract),
            EventPayload::TaskClaimed(claim) => self.validate_claimed(event, claim),
            EventPayload::TaskClaimRenewed(claim) => self.validate_claim_renewed(event, claim),
            EventPayload::TaskClaimReleased(claim) => self.validate_claim_released(claim),
            EventPayload::CandidateProposed(payload) => {
                self.validate_candidate_proposed(event, payload)
            }
            EventPayload::EvidenceAdded(payload) => self.validate_evidence_added(event, payload),
            EventPayload::EvidenceAvailable(payload) => {
                self.validate_evidence_available(event, payload)
            }
            EventPayload::VerifierResultSubmitted(payload) => {
                self.validate_verifier_result_submitted(event, payload)
            }
            EventPayload::VoteCommit(payload) => self.validate_vote_commit(event, payload),
            EventPayload::VoteReveal(payload) => self.validate_vote_reveal(event, payload),
            EventPayload::DecisionCommitted(payload) => self.validate_decision_committed(payload),
            EventPayload::DecisionFinalized(payload) => self.validate_decision_finalized(payload),
            EventPayload::TaskError(payload) => self.validate_task_error(payload),
            EventPayload::TaskRetryScheduled(payload) => {
                let task = self.require_task(&payload.task_id)?;
                if task.terminal_state != TaskTerminalState::Open {
                    return Err(SwarmError::InvalidEvent("retry on closed task".into()).into());
                }
                if payload.attempt <= task.retry_attempt {
                    return Err(
                        SwarmError::InvalidEvent("retry attempt must increase".into()).into(),
                    );
                }
                Ok(())
            }
            EventPayload::TaskExpired(payload) => self.validate_task_expired(event, payload),
            EventPayload::EpochEnded(payload) => self.validate_epoch_ended(payload),
            EventPayload::TaskStopped(payload) => {
                self.validate_task_stop_like(&payload.task_id, payload.epoch)
            }
            EventPayload::TaskSuspended(payload) => {
                self.validate_task_stop_like(&payload.task_id, payload.epoch)
            }
            EventPayload::TaskKilled(payload) => {
                self.validate_task_stop_like(&payload.task_id, payload.epoch)
            }
            EventPayload::CheckpointCreated(payload) => {
                if payload.up_to_seq > self.store.head_seq()? {
                    return Err(SwarmError::InvalidEvent("checkpoint beyond head".into()).into());
                }
                Ok(())
            }
            EventPayload::MembershipUpdated(payload) => self.validate_membership_update(payload),
            EventPayload::PolicyTuned(payload) => self.validate_policy_tuned(payload),
            EventPayload::AdvisoryCreated(payload) => self.validate_advisory_created(payload),
            EventPayload::AdvisoryApproved(payload) => {
                self.validate_advisory_approved(event, payload)
            }
            EventPayload::AdvisoryApplied(payload) => self.validate_advisory_applied(payload),
            EventPayload::TaskFeedbackReported(payload) => {
                self.validate_task_feedback_reported(event, payload)
            }
            EventPayload::ReuseRejectRecorded(payload) => {
                self.validate_reuse_reject_recorded(payload)
            }
        }
    }

    pub(crate) fn validate_role_permission(&self, event: &Event) -> Result<()> {
        let membership = self.load_membership()?;
        let (required_role, check_author_match): (Option<Role>, Option<&str>) = match &event.payload
        {
            EventPayload::TaskCreated(_) => (Some(Role::Proposer), None),
            EventPayload::TaskClaimed(p) => (
                Some(claim_role_to_permission(p.role)),
                Some(&p.claimer_node_id),
            ),
            EventPayload::TaskClaimRenewed(p) => (
                Some(claim_role_to_permission(p.role)),
                Some(&p.claimer_node_id),
            ),
            EventPayload::TaskClaimReleased(p) => (
                Some(claim_role_to_permission(p.role)),
                Some(&p.claimer_node_id),
            ),
            EventPayload::CandidateProposed(_) => (Some(Role::Proposer), None),
            EventPayload::EvidenceAdded(_) => (Some(Role::Proposer), None),
            EventPayload::EvidenceAvailable(_) => (Some(Role::Verifier), None),
            EventPayload::VerifierResultSubmitted(_) => (Some(Role::Verifier), None),
            EventPayload::VoteCommit(_) => (Some(Role::Verifier), None),
            EventPayload::VoteReveal(_) => (Some(Role::Verifier), None),
            EventPayload::DecisionCommitted(_) => (Some(Role::Finalizer), None),
            EventPayload::DecisionFinalized(_) => (Some(Role::Finalizer), None),
            EventPayload::TaskError(_) => (None, None),
            EventPayload::TaskRetryScheduled(_) => (Some(Role::Committer), None),
            EventPayload::TaskExpired(_) => (Some(Role::Finalizer), None),
            EventPayload::EpochEnded(_) => (Some(Role::Finalizer), None),
            EventPayload::TaskStopped(_) => (Some(Role::Finalizer), None),
            EventPayload::TaskSuspended(_) => (Some(Role::Finalizer), None),
            EventPayload::TaskKilled(_) => (Some(Role::Finalizer), None),
            EventPayload::CheckpointCreated(_) => (Some(Role::Committer), None),
            EventPayload::MembershipUpdated(_) => (Some(Role::Finalizer), None),
            EventPayload::PolicyTuned(_) => (Some(Role::Finalizer), None),
            EventPayload::AdvisoryCreated(_) => (Some(Role::Committer), None),
            EventPayload::AdvisoryApproved(_) => (Some(Role::Finalizer), None),
            EventPayload::AdvisoryApplied(_) => (Some(Role::Committer), None),
            EventPayload::TaskFeedbackReported(_) => (None, None),
            EventPayload::ReuseRejectRecorded(_) => (Some(Role::Committer), None),
        };

        if let Some(expected_author) = check_author_match
            && expected_author != event.author_node_id
        {
            return Err(SwarmError::Unauthorized("claimer author mismatch".into()).into());
        }

        if let Some(required_role) = required_role
            && !membership.has_role(&event.author_node_id, required_role)
        {
            return Err(
                SwarmError::Unauthorized(format!("author lacks role {:?}", required_role)).into(),
            );
        }
        if matches!(event.payload, EventPayload::TaskError(_))
            && !membership.has_role(&event.author_node_id, Role::Proposer)
            && !membership.has_role(&event.author_node_id, Role::Verifier)
            && !membership.has_role(&event.author_node_id, Role::Committer)
        {
            return Err(SwarmError::Unauthorized(
                "task_error author lacks proposer/verifier/committer role".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_task_created(&self, contract: &TaskContract) -> Result<()> {
        if contract.assignment.mode != "CLAIM" {
            return Err(SwarmError::InvalidEvent("assignment.mode must be CLAIM".into()).into());
        }
        if contract.protocol_version.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("task protocol_version required".into()).into());
        }
        if !contract.acceptance.vote.commit_reveal {
            return Err(SwarmError::InvalidEvent("vote.commit_reveal must be true".into()).into());
        }
        if contract.budget.reuse_max_attempts == 0 {
            return Err(
                SwarmError::InvalidEvent("budget.reuse_max_attempts must be > 0".into()).into(),
            );
        }
        if contract.budget.reuse_verify_time_ms > contract.budget.time_ms {
            return Err(SwarmError::InvalidEvent(
                "reuse_verify_time_ms must be <= budget.time_ms".into(),
            )
            .into());
        }
        if contract.budget.reuse_verify_cost_units > contract.budget.cost_units {
            return Err(SwarmError::InvalidEvent(
                "reuse_verify_cost_units must be <= budget.cost_units".into(),
            )
            .into());
        }
        if contract.budget.explore_cost_units == 0
            || contract.budget.verify_cost_units == 0
            || contract.budget.finalize_cost_units == 0
        {
            return Err(SwarmError::InvalidEvent("stage budget buckets must be > 0".into()).into());
        }
        if contract.budget.explore_cost_units
            + contract.budget.verify_cost_units
            + contract.budget.finalize_cost_units
            > contract.budget.cost_units
        {
            return Err(SwarmError::InvalidEvent(
                "sum(stage budget buckets) must be <= budget.cost_units".into(),
            )
            .into());
        }
        if matches!(contract.task_mode, crate::types::TaskMode::Continuous)
            && !matches!(contract.budget.mode, crate::types::BudgetMode::EpochRenew)
        {
            return Err(SwarmError::InvalidEvent(
                "CONTINUOUS task_mode requires budget.mode=EPOCH_RENEW".into(),
            )
            .into());
        }
        if contract.assignment.explore.max_proposers == 0
            || contract.assignment.verify.max_verifiers == 0
            || contract.assignment.finalize.max_finalizers == 0
        {
            return Err(
                SwarmError::InvalidEvent("stage max concurrency must be > 0".into()).into(),
            );
        }
        if contract.assignment.explore.topk == 0 {
            return Err(SwarmError::InvalidEvent("explore.topk must be > 0".into()).into());
        }
        if contract.assignment.explore.stop.no_new_evidence_rounds == 0 {
            return Err(SwarmError::InvalidEvent(
                "explore.stop.no_new_evidence_rounds must be > 0".into(),
            )
            .into());
        }
        if contract.acceptance.da_quorum_threshold == 0 {
            return Err(SwarmError::InvalidEvent(
                "acceptance.da_quorum_threshold must be > 0".into(),
            )
            .into());
        }
        if contract.evidence_policy.max_snippet_bytes == 0
            || contract.evidence_policy.max_snippet_tokens == 0
        {
            return Err(
                SwarmError::InvalidEvent("evidence snippet limits must be > 0".into()).into(),
            );
        }
        let settlement = &contract.acceptance.settlement;
        if !(0.0..=1.0).contains(&settlement.implicit_weight) {
            return Err(SwarmError::InvalidEvent(
                "settlement.implicit_weight must be within [0,1]".into(),
            )
            .into());
        }
        if settlement.implicit_diminishing_returns.k == 0 {
            return Err(SwarmError::InvalidEvent("settlement.K must be >= 1".into()).into());
        }
        if settlement.feedback.mode != "CAPABILITY" {
            return Err(SwarmError::InvalidEvent(
                "settlement.feedback.mode must be CAPABILITY".into(),
            )
            .into());
        }
        if settlement.feedback.authority_pubkey.trim().is_empty() {
            return Err(SwarmError::InvalidEvent(
                "settlement.feedback.authority_pubkey required".into(),
            )
            .into());
        }
        if contract.evidence_policy.max_inline_evidence_bytes as usize != MAX_INLINE_EVIDENCE_BYTES
        {
            return Err(
                SwarmError::InvalidEvent("max_inline_evidence_bytes mismatch".into()).into(),
            );
        }
        if contract.evidence_policy.max_inline_media_bytes as usize != MAX_INLINE_MEDIA_BYTES {
            return Err(SwarmError::InvalidEvent("max_inline_media_bytes must be 0".into()).into());
        }
        for mime in INLINE_MIME_ALLOWLIST {
            if !contract
                .evidence_policy
                .inline_mime_allowlist
                .iter()
                .any(|m| m == mime)
            {
                return Err(SwarmError::InvalidEvent(format!(
                    "missing inline mime allowlist: {mime}"
                ))
                .into());
            }
        }
        self.policy_registry
            .require_binding(&contract.acceptance.verifier_policy)
            .context("task policy binding invalid")?;

        if self.store.task_projection(&contract.task_id)?.is_some() {
            return Err(SwarmError::Conflict("task already exists".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_stage_budget(
        &self,
        task: &TaskProjectionRow,
        event: &Event,
    ) -> Result<()> {
        let Some((stage, cost_delta)) = stage_cost_for_payload(&event.payload) else {
            return Ok(());
        };
        let usage = self
            .store
            .get_stage_usage(&task.contract.task_id, event.epoch)?
            .unwrap_or(crate::storage::TaskStageUsageRow {
                task_id: task.contract.task_id.clone(),
                epoch: event.epoch,
                explore_used: 0,
                verify_used: 0,
                finalize_used: 0,
            });
        let (used, limit) = match stage {
            "explore" => (usage.explore_used, task.contract.budget.explore_cost_units),
            "verify" => (usage.verify_used, task.contract.budget.verify_cost_units),
            "finalize" => (
                usage.finalize_used,
                task.contract.budget.finalize_cost_units,
            ),
            _ => return Ok(()),
        };
        if used.saturating_add(cost_delta) > limit {
            return Err(SwarmError::InvalidEvent(format!(
                "{stage} budget exhausted for task {} epoch {}",
                usage.task_id, usage.epoch
            ))
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_evidence_added(
        &self,
        event: &Event,
        payload: &EvidenceAddedPayload,
    ) -> Result<()> {
        self.assert_valid_lease(
            &payload.task_id,
            ClaimRole::Propose,
            &event.author_node_id,
            &payload.execution_id,
            event.created_at,
        )?;
        let candidate = self
            .store
            .get_candidate_by_id(&payload.task_id, &payload.candidate_id)?
            .ok_or_else(|| SwarmError::NotFound("candidate missing for evidence".into()))?;
        if candidate.execution_id != payload.execution_id {
            return Err(SwarmError::InvalidEvent("evidence execution_id mismatch".into()).into());
        }
        if payload.evidence_refs.is_empty() {
            return Err(SwarmError::InvalidEvent("evidence_refs required".into()).into());
        }
        for reference in &payload.evidence_refs {
            if reference.digest.trim().is_empty() {
                return Err(SwarmError::InvalidEvent("evidence digest required".into()).into());
            }
        }
        Ok(())
    }

    pub(crate) fn validate_evidence_available(
        &self,
        event: &Event,
        payload: &EvidenceAvailablePayload,
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
            .ok_or_else(|| {
                SwarmError::NotFound("candidate missing for evidence availability".into())
            })?;
        if !candidate
            .evidence_refs
            .iter()
            .any(|r| r.digest == payload.evidence_digest)
        {
            return Err(SwarmError::InvalidEvent(
                "evidence availability digest not found in candidate".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_epoch_ended(
        &self,
        payload: &crate::types::EpochEndedPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;
        if payload.epoch != task.epoch {
            return Err(SwarmError::InvalidEvent("epoch_ended epoch mismatch".into()).into());
        }
        if !matches!(task.contract.task_mode, crate::types::TaskMode::Continuous) {
            return Err(
                SwarmError::InvalidEvent("epoch_ended only valid for CONTINUOUS".into()).into(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate_task_stop_like(&self, task_id: &str, epoch: u64) -> Result<()> {
        if epoch == u64::MAX {
            return Err(SwarmError::InvalidEvent("invalid epoch".into()).into());
        }
        let task = self.require_task(task_id)?;
        if task.terminal_state != TaskTerminalState::Open {
            return Err(SwarmError::InvalidEvent("task already closed".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_claimed(&self, event: &Event, claim: &ClaimPayload) -> Result<()> {
        let task = self.require_task(&claim.task_id)?;
        if task.terminal_state != TaskTerminalState::Open {
            return Err(SwarmError::InvalidEvent("claim on closed task".into()).into());
        }
        if claim.execution_id.trim().is_empty() {
            return Err(SwarmError::InvalidEvent("execution_id required".into()).into());
        }

        let role_str = claim_role_str(claim.role);
        if let Some(current) = self.store.get_lease(&claim.task_id, role_str)?
            && !is_deadline_expired(current.lease_until, event.created_at)
        {
            let winner = std::cmp::min(current.execution_id.clone(), claim.execution_id.clone());
            if winner != claim.execution_id {
                return Err(SwarmError::Conflict("lease conflict (tie-break loser)".into()).into());
            }
        }

        if claim.lease_until <= event.created_at {
            return Err(
                SwarmError::InvalidEvent("lease_until must be in the future".into()).into(),
            );
        }
        let max_lease_until = event
            .created_at
            .saturating_add(task.contract.assignment.claim.lease_ms)
            .saturating_add(CLOCK_SKEW_TOLERANCE_MS);
        if claim.lease_until > max_lease_until {
            return Err(SwarmError::InvalidEvent(
                "lease_until exceeds contract lease window".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_claim_renewed(
        &self,
        event: &Event,
        claim: &ClaimRenewPayload,
    ) -> Result<()> {
        let task = self.require_task(&claim.task_id)?;
        let role_str = claim_role_str(claim.role);
        let current = self
            .store
            .get_lease(&claim.task_id, role_str)?
            .ok_or_else(|| SwarmError::NotFound("lease missing".into()))?;
        if current.claimer_node_id != claim.claimer_node_id
            || current.execution_id != claim.execution_id
        {
            return Err(SwarmError::InvalidEvent("renew must match active lease".into()).into());
        }
        if is_deadline_expired(current.lease_until, event.created_at) {
            return Err(SwarmError::InvalidEvent("cannot renew expired lease".into()).into());
        }
        if claim.lease_until <= current.lease_until {
            return Err(SwarmError::InvalidEvent("renew lease_until must extend".into()).into());
        }
        if claim.lease_until <= event.created_at {
            return Err(
                SwarmError::InvalidEvent("renew lease_until must be in the future".into()).into(),
            );
        }
        let max_lease_until = event
            .created_at
            .saturating_add(task.contract.assignment.claim.lease_ms)
            .saturating_add(CLOCK_SKEW_TOLERANCE_MS);
        if claim.lease_until > max_lease_until {
            return Err(SwarmError::InvalidEvent(
                "renew lease_until exceeds contract lease window".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_claim_released(&self, claim: &ClaimReleasePayload) -> Result<()> {
        let role_str = claim_role_str(claim.role);
        let current = self
            .store
            .get_lease(&claim.task_id, role_str)?
            .ok_or_else(|| SwarmError::NotFound("lease missing".into()))?;
        if current.claimer_node_id != claim.claimer_node_id
            || current.execution_id != claim.execution_id
        {
            return Err(SwarmError::InvalidEvent("release must match active lease".into()).into());
        }
        Ok(())
    }

    pub(crate) fn validate_candidate_proposed(
        &self,
        event: &Event,
        payload: &CandidateProposedPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;
        if task.terminal_state != TaskTerminalState::Open {
            return Err(SwarmError::InvalidEvent("candidate on closed task".into()).into());
        }
        let output_bytes = serde_json::to_vec(&payload.candidate.output)?;
        if output_bytes.len() > MAX_STRUCTURED_SUMMARY_BYTES {
            return Err(SwarmError::InvalidEvent(
                "candidate structured summary exceeds limit".into(),
            )
            .into());
        }
        let candidate_count = self.store.count_candidates(&payload.task_id)?;
        let passed_verifies = self.store.count_passed_verifier_results(&payload.task_id)?;
        if candidate_count >= task.contract.assignment.explore.topk && passed_verifies > 0 {
            return Err(SwarmError::InvalidEvent(
                "explore early-stop: topk reached and verified candidate exists".into(),
            )
            .into());
        }
        let distinct_evidence = self
            .store
            .count_distinct_evidence_digests(&payload.task_id)?;
        if candidate_count
            >= distinct_evidence
                .saturating_add(task.contract.assignment.explore.stop.no_new_evidence_rounds)
            && payload.candidate.evidence_refs.is_empty()
        {
            return Err(SwarmError::InvalidEvent(
                "explore early-stop: no new evidence rounds threshold reached".into(),
            )
            .into());
        }

        self.assert_valid_lease(
            &payload.task_id,
            ClaimRole::Propose,
            &event.author_node_id,
            &payload.candidate.execution_id,
            event.created_at,
        )?;
        let c_hash = candidate_hash(&payload.candidate)?;
        if self
            .store
            .is_reuse_blacklisted(&payload.task_id, event.epoch, &c_hash)?
        {
            return Err(SwarmError::InvalidEvent(
                "candidate_hash is blacklisted for this task epoch".into(),
            )
            .into());
        }

        let max_inline = task.contract.evidence_policy.max_inline_evidence_bytes as usize;
        let allowlist = &task.contract.evidence_policy.inline_mime_allowlist;
        for inline in &payload.candidate.evidence_inline {
            if inline.mime.starts_with("image/")
                || inline.mime.starts_with("video/")
                || inline.mime.starts_with("audio/")
            {
                return Err(SwarmError::InvalidEvent("inline media is forbidden".into()).into());
            }
            if !allowlist.iter().any(|m| m == &inline.mime) {
                return Err(SwarmError::InvalidEvent("inline mime not allowlisted".into()).into());
            }
            if inline.content.len() > max_inline || inline.content.len() > MAX_INLINE_EVIDENCE_BYTES
            {
                return Err(
                    SwarmError::InvalidEvent("inline evidence exceeds max bytes".into()).into(),
                );
            }
            if inline.mime == "application/json"
                && serde_json::from_str::<Value>(&inline.content).is_err()
            {
                return Err(SwarmError::InvalidEvent(
                    "application/json inline evidence must be valid json".into(),
                )
                .into());
            }
            if inline_contains_forbidden_media(&inline.content) {
                return Err(SwarmError::InvalidEvent(
                    "inline media payload detected (including base64/data-uri)".into(),
                )
                .into());
            }
        }

        for reference in &payload.candidate.evidence_refs {
            if reference.digest.trim().is_empty() {
                return Err(SwarmError::InvalidEvent("artifact_ref.digest required".into()).into());
            }
        }

        if let Some(existing) = self
            .store
            .get_candidate_by_execution(&payload.task_id, &payload.candidate.execution_id)?
            && existing.output != payload.candidate.output
        {
            return Err(SwarmError::Conflict(
                "execution_id idempotency violated for candidate output".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn validate_verifier_result_submitted(
        &self,
        event: &Event,
        payload: &VerifierResultSubmittedPayload,
    ) -> Result<()> {
        let task = self.require_task(&payload.task_id)?;
        if task.terminal_state != TaskTerminalState::Open {
            return Err(SwarmError::InvalidEvent("verifier result on closed task".into()).into());
        }
        let result_bytes = serde_json::to_vec(&payload.result)?;
        if result_bytes.len() > MAX_STRUCTURED_SUMMARY_BYTES {
            return Err(SwarmError::InvalidEvent(
                "verifier structured summary exceeds limit".into(),
            )
            .into());
        }

        self.assert_valid_lease(
            &payload.task_id,
            ClaimRole::Verify,
            &event.author_node_id,
            &payload.result.execution_id,
            event.created_at,
        )?;

        let candidate = self
            .store
            .get_candidate_by_id(&payload.task_id, &payload.result.candidate_id)?
            .ok_or_else(|| SwarmError::NotFound("candidate missing for verify".into()))?;

        if payload.result.provider_family.trim().is_empty()
            || payload.result.model_id.trim().is_empty()
        {
            return Err(
                SwarmError::InvalidEvent("provider_family/model_id required".into()).into(),
            );
        }

        if !(0.0..=1.0).contains(&payload.result.score) {
            return Err(SwarmError::InvalidEvent("verify score must be 0..1".into()).into());
        }

        match payload.result.verification_status {
            VerificationStatus::Passed if !payload.result.passed => {
                return Err(SwarmError::InvalidEvent(
                    "verification_status passed requires passed=true".into(),
                )
                .into());
            }
            VerificationStatus::Failed if payload.result.passed => {
                return Err(SwarmError::InvalidEvent(
                    "verification_status failed requires passed=false".into(),
                )
                .into());
            }
            VerificationStatus::Inconclusive if payload.result.passed => {
                return Err(SwarmError::InvalidEvent(
                    "verification_status inconclusive requires passed=false".into(),
                )
                .into());
            }
            _ => {}
        }

        if payload.result.policy_hash != task.contract.acceptance.verifier_policy.policy_hash
            || payload.result.policy_id != task.contract.acceptance.verifier_policy.policy_id
            || payload.result.policy_version
                != task.contract.acceptance.verifier_policy.policy_version
        {
            return Err(SwarmError::InvalidEvent(
                "policy binding mismatch in verifier result".into(),
            )
            .into());
        }

        let policy = self
            .policy_registry
            .require_binding(&task.contract.acceptance.verifier_policy)?;
        let allowed_reason_codes = policy.spec().reason_codes;
        if payload.result.reason_codes.is_empty() {
            return Err(SwarmError::InvalidEvent("reason_codes cannot be empty".into()).into());
        }
        for code in &payload.result.reason_codes {
            if !allowed_reason_codes.iter().any(|c| c == code)
                && *code != REASON_UNKNOWN
                && *code != REASON_CUSTOM_ERROR
                && is_protocol_reason_code(*code)
            {
                return Err(
                    SwarmError::InvalidEvent(format!("unknown reason code: {code}")).into(),
                );
            }
        }
        if payload.result.verification_status == VerificationStatus::Inconclusive
            && !payload.result.reason_codes.iter().any(|code| {
                matches!(
                    *code,
                    REASON_EVIDENCE_UNREACHABLE
                        | REASON_EVIDENCE_TIMEOUT
                        | REASON_EVIDENCE_AUTH_DENIED
                )
            })
        {
            return Err(SwarmError::InvalidEvent(
                "inconclusive verification requires evidence reachability reason code".into(),
            )
            .into());
        }

        let recomputed = policy.evaluate(
            &candidate,
            &task.contract.output_schema,
            &task.contract.acceptance.verifier_policy.policy_params,
        );
        if payload.result.verification_status == VerificationStatus::Passed && !recomputed.passed {
            return Err(SwarmError::InvalidEvent("verifier passed flag mismatch".into()).into());
        }

        Ok(())
    }
}
