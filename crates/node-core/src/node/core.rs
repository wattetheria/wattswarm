use super::*;

impl Node {
    pub fn new(
        identity: NodeIdentity,
        store: SqliteStore,
        genesis_membership: Membership,
    ) -> Result<Self> {
        let policy_registry = PolicyRegistry::with_builtin();
        let this = Self {
            identity,
            store,
            policy_registry,
            peers: HashSet::new(),
            genesis_membership: genesis_membership.clone(),
        };
        this.store
            .put_membership(&serde_json::to_string(&genesis_membership)?)?;
        Ok(this)
    }

    pub fn open_in_memory_with_roles(roles: &[Role]) -> Result<Self> {
        let identity = NodeIdentity::random();
        let mut membership = Membership::new();
        for role in roles {
            membership.grant(&identity.node_id(), *role);
        }
        Self::new(identity, SqliteStore::open_in_memory()?, membership)
    }

    pub fn node_id(&self) -> String {
        self.identity.node_id()
    }

    pub fn policy_registry(&self) -> &PolicyRegistry {
        &self.policy_registry
    }

    pub fn policy_registry_mut(&mut self) -> &mut PolicyRegistry {
        &mut self.policy_registry
    }

    pub fn discover_peer(&mut self, peer_node_id: String) {
        self.peers.insert(peer_node_id);
    }

    pub fn peers(&self) -> Vec<String> {
        self.peers.iter().cloned().collect()
    }

    pub fn emit(&mut self, epoch: u64, payload: EventPayload) -> Result<Event> {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        self.emit_at(epoch, payload, now)
    }

    pub fn emit_at(&mut self, epoch: u64, payload: EventPayload, created_at: u64) -> Result<Event> {
        let unsigned = UnsignedEvent::from_payload(
            LOCAL_PROTOCOL_VERSION.to_owned(),
            self.node_id(),
            epoch,
            created_at,
            payload,
        );
        let event = self.identity.sign_unsigned_event(&unsigned)?;
        self.ingest(event.clone())?;
        Ok(event)
    }

    pub fn ingest(&mut self, event: Event) -> Result<()> {
        self.validate_event(&event)?;
        if let Err(err) = self.store.append_event(&event) {
            if is_duplicate_event_error(&err) {
                return Ok(());
            }
            return Err(err.context("append event"));
        }
        self.apply_to_projection(&event)?;
        Ok(())
    }

    pub fn ingest_remote(&mut self, event: Event) -> Result<()> {
        self.ingest(event)
    }

    pub fn head_seq(&self) -> Result<u64> {
        self.store.head_seq()
    }

    pub fn replay_rebuild_projection(&mut self) -> Result<()> {
        self.store.clear_projection()?;
        self.store
            .put_membership(&serde_json::to_string(&self.genesis_membership)?)?;
        let events = self.store.load_all_events()?;
        for (_, event) in events {
            self.apply_to_projection(&event)?;
        }
        Ok(())
    }

    pub fn sync_from_peer(&mut self, peer: &Node) -> Result<usize> {
        self.sync_from_peer_batched(peer, BACKFILL_BATCH_EVENTS)
    }

    pub fn sync_from_peer_batched(&mut self, peer: &Node, max_events: usize) -> Result<usize> {
        if max_events == 0 {
            return Ok(0);
        }
        let local_head = self.head_seq()?;
        let missing = peer.store.load_events_page(local_head, max_events)?;
        let mut applied = 0usize;
        for (_, event) in missing {
            if self.ingest_remote(event).is_ok() {
                applied += 1;
            }
        }
        Ok(applied)
    }

    pub fn sync_from_peer_until_head(&mut self, peer: &Node) -> Result<usize> {
        let mut total = 0usize;
        loop {
            let applied = self.sync_from_peer(peer)?;
            if applied == 0 {
                break;
            }
            total += applied;
        }
        Ok(total)
    }

    pub fn anti_entropy_with(&mut self, peer: &mut Node) -> Result<(usize, usize)> {
        let a = self.sync_from_peer(peer)?;
        let b = peer.sync_from_peer(self)?;
        Ok((a, b))
    }

    pub fn create_checkpoint(
        &mut self,
        checkpoint_id: String,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        let up_to_seq = self.head_seq()?;
        self.emit_at(
            epoch,
            EventPayload::CheckpointCreated(crate::types::CheckpointCreatedPayload {
                checkpoint_id,
                up_to_seq,
            }),
            created_at,
        )
    }

    pub fn task_view(&self, task_id: &str) -> Result<Option<TaskProjectionRow>> {
        self.store.task_projection(task_id)
    }

    pub fn verify_log(&self) -> Result<()> {
        for (_, event) in self.store.load_all_events()? {
            verify_event_signature(&event)?;
            let serialized = serde_json::to_vec(&event)?;
            if serialized.len() > MAX_EVENT_BYTES {
                return Err(anyhow!("event {} exceeds max bytes", event.event_id));
            }
        }
        Ok(())
    }

    pub fn submit_task(
        &mut self,
        contract: TaskContract,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(epoch, EventPayload::TaskCreated(contract), created_at)
    }

    pub fn claim_task(
        &mut self,
        task_id: &str,
        role: ClaimRole,
        execution_id: &str,
        lease_until: u64,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskClaimed(ClaimPayload {
                task_id: task_id.to_owned(),
                role,
                claimer_node_id: self.node_id(),
                execution_id: execution_id.to_owned(),
                lease_until,
            }),
            created_at,
        )
    }

    pub fn renew_claim(
        &mut self,
        task_id: &str,
        role: ClaimRole,
        execution_id: &str,
        lease_until: u64,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskClaimRenewed(ClaimRenewPayload {
                task_id: task_id.to_owned(),
                role,
                claimer_node_id: self.node_id(),
                execution_id: execution_id.to_owned(),
                lease_until,
            }),
            created_at,
        )
    }

    pub fn release_claim(
        &mut self,
        task_id: &str,
        role: ClaimRole,
        execution_id: &str,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskClaimReleased(ClaimReleasePayload {
                task_id: task_id.to_owned(),
                role,
                claimer_node_id: self.node_id(),
                execution_id: execution_id.to_owned(),
            }),
            created_at,
        )
    }

    pub fn propose_candidate(
        &mut self,
        task_id: &str,
        candidate: Candidate,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::CandidateProposed(CandidateProposedPayload {
                task_id: task_id.to_owned(),
                candidate,
            }),
            created_at,
        )
    }

    pub fn add_evidence(
        &mut self,
        task_id: &str,
        candidate_id: &str,
        execution_id: &str,
        evidence_refs: Vec<crate::types::ArtifactRef>,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::EvidenceAdded(EvidenceAddedPayload {
                task_id: task_id.to_owned(),
                candidate_id: candidate_id.to_owned(),
                execution_id: execution_id.to_owned(),
                evidence_refs,
            }),
            created_at,
        )
    }

    pub fn evidence_available(
        &mut self,
        task_id: &str,
        candidate_id: &str,
        execution_id: &str,
        evidence_digest: &str,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::EvidenceAvailable(EvidenceAvailablePayload {
                task_id: task_id.to_owned(),
                candidate_id: candidate_id.to_owned(),
                execution_id: execution_id.to_owned(),
                evidence_digest: evidence_digest.to_owned(),
            }),
            created_at,
        )
    }

    pub fn submit_verifier_result(
        &mut self,
        task_id: &str,
        result: crate::types::VerifierResult,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::VerifierResultSubmitted(VerifierResultSubmittedPayload {
                task_id: task_id.to_owned(),
                result,
            }),
            created_at,
        )
    }

    pub fn submit_vote_commit(
        &mut self,
        payload: VoteCommitPayload,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(epoch, EventPayload::VoteCommit(payload), created_at)
    }

    pub fn submit_vote_reveal(
        &mut self,
        payload: VoteRevealPayload,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(epoch, EventPayload::VoteReveal(payload), created_at)
    }

    pub fn commit_decision(
        &mut self,
        task_id: &str,
        epoch: u64,
        candidate_id: &str,
        created_at: u64,
    ) -> Result<Event> {
        let candidate = self
            .store
            .get_candidate_by_id(task_id, candidate_id)?
            .ok_or_else(|| anyhow!("candidate {candidate_id} missing"))?;
        let winning_hash = candidate_hash(&candidate)?;
        self.emit_at(
            epoch,
            EventPayload::DecisionCommitted(DecisionCommittedPayload {
                task_id: task_id.to_owned(),
                epoch,
                candidate_id: candidate_id.to_owned(),
                candidate_hash: winning_hash,
            }),
            created_at,
        )
    }

    pub fn finalize_decision(
        &mut self,
        task_id: &str,
        epoch: u64,
        candidate_id: &str,
        finality_proof: crate::types::FinalityProof,
        created_at: u64,
    ) -> Result<Event> {
        let candidate = self
            .store
            .get_candidate_by_id(task_id, candidate_id)?
            .ok_or_else(|| anyhow!("candidate {candidate_id} missing"))?;
        let winning_hash = candidate_hash(&candidate)?;
        self.emit_at(
            epoch,
            EventPayload::DecisionFinalized(DecisionFinalizedPayload {
                task_id: task_id.to_owned(),
                epoch,
                candidate_id: candidate_id.to_owned(),
                winning_candidate_hash: winning_hash,
                finality_proof,
            }),
            created_at,
        )
    }

    pub fn task_error(
        &mut self,
        task_id: &str,
        reason: crate::types::TaskErrorReason,
        message: &str,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskError(TaskErrorPayload {
                task_id: task_id.to_owned(),
                reason,
                reason_codes: task_error_reason_codes(reason),
                custom_reason_namespace: None,
                custom_reason_code: None,
                custom_reason_message: None,
                message: message.to_owned(),
            }),
            created_at,
        )
    }

    pub fn schedule_retry(
        &mut self,
        task_id: &str,
        attempt: u32,
        run_at: u64,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskRetryScheduled(TaskRetryScheduledPayload {
                task_id: task_id.to_owned(),
                attempt,
                run_at,
            }),
            created_at,
        )
    }

    pub fn expire_task(&mut self, task_id: &str, epoch: u64, created_at: u64) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskExpired(crate::types::TaskExpiredPayload {
                task_id: task_id.to_owned(),
            }),
            created_at,
        )
    }

    pub fn end_epoch(
        &mut self,
        task_id: &str,
        epoch: u64,
        reason: EpochEndReason,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::EpochEnded(crate::types::EpochEndedPayload {
                task_id: task_id.to_owned(),
                epoch,
                reason,
            }),
            created_at,
        )
    }

    pub fn stop_task(
        &mut self,
        task_id: &str,
        epoch: u64,
        reason: &str,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskStopped(crate::types::TaskStoppedPayload {
                task_id: task_id.to_owned(),
                epoch,
                reason: reason.to_owned(),
            }),
            created_at,
        )
    }

    pub fn suspend_task(
        &mut self,
        task_id: &str,
        epoch: u64,
        reason: &str,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskSuspended(crate::types::TaskSuspendedPayload {
                task_id: task_id.to_owned(),
                epoch,
                reason: reason.to_owned(),
            }),
            created_at,
        )
    }

    pub fn kill_task(
        &mut self,
        task_id: &str,
        epoch: u64,
        reason: &str,
        created_at: u64,
    ) -> Result<Event> {
        self.emit_at(
            epoch,
            EventPayload::TaskKilled(crate::types::TaskKilledPayload {
                task_id: task_id.to_owned(),
                epoch,
                reason: reason.to_owned(),
            }),
            created_at,
        )
    }

    pub fn reconcile_timeouts(&mut self, epoch: u64, now: u64) -> Result<Vec<Event>> {
        let task_ids = self.store.list_open_task_ids()?;
        let mut emitted = Vec::new();
        for task_id in task_ids {
            let task = match self.store.task_projection(&task_id)? {
                Some(task) => task,
                None => continue,
            };

            if now >= task.contract.expiry_ms {
                if let Ok(event) = self.expire_task(&task_id, epoch, now) {
                    emitted.push(event);
                }
                continue;
            }

            if task.committed_candidate_id.is_some() {
                continue;
            }

            let commits = self.store.list_vote_commits_meta(&task_id)?;
            if commits.is_empty() {
                continue;
            }
            let reveal_voters: HashSet<String> = self
                .store
                .list_vote_reveal_voters(&task_id)?
                .into_iter()
                .collect();
            let unresolved: Vec<_> = commits
                .iter()
                .filter(|row| !reveal_voters.contains(&row.voter_node_id))
                .collect();
            let unresolved_expired = unresolved
                .iter()
                .all(|row| now > row.created_at + task.contract.acceptance.vote.reveal_deadline_ms);
            let all_commits_resolved = unresolved.is_empty();
            if !unresolved_expired && !all_commits_resolved {
                continue;
            }

            let max_approvals = self.store.max_valid_approve_reveals(&task_id)?;
            if max_approvals >= task.contract.acceptance.quorum_threshold {
                continue;
            }

            let next_attempt = task.retry_attempt.saturating_add(1);
            let run_at = now.saturating_add(task.contract.acceptance.vote.reveal_deadline_ms);
            if let Ok(event) = self.schedule_retry(&task_id, next_attempt, run_at, epoch, now) {
                emitted.push(event);
            }
        }
        Ok(emitted)
    }

    pub fn auto_execute_with_runtime(
        &mut self,
        runtime: &dyn RuntimeClient,
        task_id: &str,
        profile: &str,
        execution_id: &str,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        let task = self
            .store
            .task_projection(task_id)?
            .ok_or_else(|| anyhow!("task {task_id} missing"))?;
        let lookup_hits = self.lookup_knowledge_hits(&task.contract)?;
        let exact_hit_exists = lookup_hits
            .iter()
            .any(|hit| hit.hit_type == KnowledgeHitType::Exact);
        let reuse_attempts = self.store.count_reuse_applied_lookups(task_id)?;
        let reuse_allowed =
            exact_hit_exists && reuse_attempts < task.contract.budget.reuse_max_attempts;
        let seed_bundle = self.seed_bundle_from_hits(
            task_id,
            epoch,
            &lookup_hits,
            &task.contract,
            reuse_allowed,
        )?;
        self.record_knowledge_lookup(
            &task.contract,
            created_at,
            usize::min(lookup_hits.len(), 5) as u32,
            reuse_allowed,
            &lookup_hits,
        )?;
        let blacklist_non_empty = self
            .store
            .list_reuse_blacklist(task_id, epoch)?
            .iter()
            .any(|_| true);
        let stage = if reuse_allowed {
            "DECIDE".to_owned()
        } else if blacklist_non_empty {
            "DIVERGE_MODE".to_owned()
        } else {
            "EXPLORE".to_owned()
        };

        let req = ExecuteRequest {
            task_id: task_id.to_owned(),
            execution_id: execution_id.to_owned(),
            task_type: task.contract.task_type.clone(),
            inputs: task.contract.inputs.clone(),
            profile: profile.to_owned(),
            task_contract: task.contract.clone(),
            stage,
            attempt_id: execution_id.to_owned(),
            seed_bundle,
        };
        let execute_res = match runtime.execute(&req) {
            Ok(res) => res,
            Err(err) => {
                let reason = if is_timeout_error(&err) {
                    crate::types::TaskErrorReason::Timeout
                } else {
                    crate::types::TaskErrorReason::Other
                };
                let _ = self.task_error(
                    task_id,
                    reason,
                    &format!("runtime /execute failed: {err}"),
                    epoch,
                    created_at,
                );
                return Err(anyhow!("runtime execute failed: {err}"));
            }
        };

        if validate_schema_minimal(&task.contract.output_schema, &execute_res.candidate_output)
            .is_err()
        {
            let _ = self.task_error(
                task_id,
                crate::types::TaskErrorReason::InvalidOutput,
                "runtime /execute output does not match output_schema",
                epoch,
                created_at,
            );
            return Err(anyhow!("runtime output schema invalid"));
        }

        let candidate = Candidate {
            candidate_id: format!("cand-{}", execution_id),
            execution_id: execution_id.to_owned(),
            output: execute_res.candidate_output,
            evidence_inline: execute_res.evidence_inline,
            evidence_refs: execute_res.evidence_refs,
        };
        self.propose_candidate(task_id, candidate, epoch, created_at)
    }

    pub fn auto_verify_candidate_with_runtime(
        &mut self,
        runtime: &dyn RuntimeClient,
        task_id: &str,
        candidate_id: &str,
        execution_id: &str,
        epoch: u64,
        created_at: u64,
    ) -> Result<Event> {
        let task = self
            .store
            .task_projection(task_id)?
            .ok_or_else(|| anyhow!("task {task_id} missing"))?;
        let candidate = self
            .store
            .get_candidate_by_id(task_id, candidate_id)?
            .ok_or_else(|| anyhow!("candidate {candidate_id} missing"))?;

        let req = VerifyRequest {
            candidate: candidate.clone(),
            output_schema: task.contract.output_schema.clone(),
            policy: task.contract.acceptance.verifier_policy.clone(),
        };
        let response = match runtime.verify(&req) {
            Ok(res) => res,
            Err(err) => {
                let reason_code = if is_timeout_error(&err) {
                    REASON_EVIDENCE_TIMEOUT
                } else {
                    REASON_EVIDENCE_UNREACHABLE
                };
                let caps = runtime.capabilities().ok();
                let inconclusive_hash = sha256_hex(
                    format!(
                        "{task_id}|{candidate_id}|{execution_id}|{reason_code}|{}",
                        created_at
                    )
                    .as_bytes(),
                );
                let inconclusive = VerifierResult {
                    candidate_id: candidate_id.to_owned(),
                    execution_id: execution_id.to_owned(),
                    verification_status: VerificationStatus::Inconclusive,
                    passed: false,
                    score: 0.0,
                    reason_codes: vec![reason_code],
                    verifier_result_hash: inconclusive_hash,
                    provider_family: caps
                        .as_ref()
                        .map(|c| c.provider_family.clone())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    model_id: caps
                        .as_ref()
                        .map(|c| c.model_id.clone())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    policy_id: task.contract.acceptance.verifier_policy.policy_id.clone(),
                    policy_version: task
                        .contract
                        .acceptance
                        .verifier_policy
                        .policy_version
                        .clone(),
                    policy_hash: task.contract.acceptance.verifier_policy.policy_hash.clone(),
                };
                return match self.submit_verifier_result(task_id, inconclusive, epoch, created_at) {
                    Ok(event) => Ok(event),
                    Err(submit_err) => {
                        let _ = self.task_error(
                            task_id,
                            crate::types::TaskErrorReason::Other,
                            &format!("runtime /verify failed and inconclusive result rejected: {submit_err}"),
                            epoch,
                            created_at,
                        );
                        Err(anyhow!("runtime verify failed: {err}"))
                    }
                };
            }
        };
        let result = verifier_result_from_response(
            response,
            candidate_id.to_owned(),
            execution_id.to_owned(),
            &task.contract.acceptance.verifier_policy,
        );
        for reference in &candidate.evidence_refs {
            self.evidence_available(
                task_id,
                candidate_id,
                execution_id,
                &reference.digest,
                epoch,
                created_at,
            )?;
        }
        match self.submit_verifier_result(task_id, result, epoch, created_at) {
            Ok(event) => Ok(event),
            Err(err) => {
                let _ = self.task_error(
                    task_id,
                    crate::types::TaskErrorReason::InvalidOutput,
                    "runtime /verify output rejected by validity rules",
                    epoch,
                    created_at,
                );
                Err(err)
            }
        }
    }
}
