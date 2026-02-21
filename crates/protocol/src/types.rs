use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Proposer,
    Verifier,
    Committer,
    Finalizer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub members: HashMap<String, HashSet<Role>>,
}

impl Membership {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
        }
    }

    pub fn grant(&mut self, node_id: &str, role: Role) {
        self.members
            .entry(node_id.to_owned())
            .or_default()
            .insert(role);
    }

    pub fn has_role(&self, node_id: &str, role: Role) -> bool {
        self.members
            .get(node_id)
            .map(|roles| roles.contains(&role))
            .unwrap_or(false)
    }

    pub fn holders(&self, role: Role) -> Vec<String> {
        self.members
            .iter()
            .filter(|(_, roles)| roles.contains(&role))
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }
}

impl Default for Membership {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskContract {
    pub protocol_version: String,
    pub task_id: String,
    pub task_type: String,
    pub inputs: Value,
    pub output_schema: Value,
    pub budget: Budget,
    pub assignment: Assignment,
    pub acceptance: Acceptance,
    #[serde(default)]
    pub task_mode: TaskMode,
    pub expiry_ms: u64,
    pub evidence_policy: EvidencePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub time_ms: u64,
    pub max_steps: u32,
    pub cost_units: u64,
    #[serde(default)]
    pub mode: BudgetMode,
    #[serde(default)]
    pub explore_cost_units: u64,
    #[serde(default)]
    pub verify_cost_units: u64,
    #[serde(default)]
    pub finalize_cost_units: u64,
    pub reuse_verify_time_ms: u64,
    pub reuse_verify_cost_units: u64,
    pub reuse_max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub mode: String,
    pub claim: ClaimPolicy,
    #[serde(default)]
    pub explore: ExploreAssignment,
    #[serde(default)]
    pub verify: VerifyAssignment,
    #[serde(default)]
    pub finalize: FinalizeAssignment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimPolicy {
    pub lease_ms: u64,
    pub max_concurrency: MaxConcurrency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaxConcurrency {
    pub propose: u32,
    pub verify: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExploreAssignment {
    #[serde(default = "default_one")]
    pub max_proposers: u32,
    #[serde(default = "default_topk")]
    pub topk: u32,
    #[serde(default)]
    pub stop: ExploreStopPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExploreStopPolicy {
    #[serde(default = "default_stop_rounds")]
    pub no_new_evidence_rounds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyAssignment {
    #[serde(default = "default_one")]
    pub max_verifiers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeAssignment {
    #[serde(default = "default_one")]
    pub max_finalizers: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Acceptance {
    pub quorum_threshold: u32,
    pub verifier_policy: PolicyBinding,
    pub vote: VotePolicy,
    pub settlement: SettlementPolicy,
    #[serde(default = "default_one")]
    pub da_quorum_threshold: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotePolicy {
    pub commit_reveal: bool,
    pub reveal_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementPolicy {
    pub window_ms: u64,
    pub implicit_weight: f64,
    pub implicit_diminishing_returns: SettlementDiminishingReturns,
    pub bad_penalty: SettlementBadPenalty,
    pub feedback: FeedbackCapabilityPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementDiminishingReturns {
    #[serde(rename = "W")]
    pub w: u32,
    #[serde(rename = "K")]
    pub k: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementBadPenalty {
    #[serde(rename = "P")]
    pub p: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedbackCapabilityPolicy {
    pub mode: String,
    pub authority_pubkey: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidencePolicy {
    pub max_inline_evidence_bytes: u64,
    pub max_inline_media_bytes: u64,
    pub inline_mime_allowlist: Vec<String>,
    #[serde(default = "default_max_snippet_bytes")]
    pub max_snippet_bytes: u64,
    #[serde(default = "default_max_snippet_tokens")]
    pub max_snippet_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskMode {
    #[default]
    OneShot,
    Continuous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BudgetMode {
    #[default]
    Lifetime,
    EpochRenew,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyBinding {
    pub policy_id: String,
    pub policy_version: String,
    pub policy_hash: String,
    pub policy_params: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineEvidence {
    pub mime: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub uri: String,
    pub digest: String,
    pub size_bytes: u64,
    pub mime: String,
    pub created_at: u64,
    pub producer: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub candidate_id: String,
    pub execution_id: String,
    pub output: Value,
    pub evidence_inline: Vec<InlineEvidence>,
    pub evidence_refs: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifierResult {
    pub candidate_id: String,
    pub execution_id: String,
    pub verification_status: VerificationStatus,
    pub passed: bool,
    pub score: f64,
    pub reason_codes: Vec<u16>,
    pub verifier_result_hash: String,
    pub provider_family: String,
    pub model_id: String,
    pub policy_id: String,
    pub policy_version: String,
    pub policy_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Passed,
    Failed,
    Inconclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskErrorReason {
    Timeout,
    InvalidOutput,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoteChoice {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteCommitPayload {
    pub task_id: String,
    pub candidate_id: String,
    pub candidate_hash: String,
    pub execution_id: String,
    pub verifier_result_hash: String,
    pub commit_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRevealPayload {
    pub task_id: String,
    pub candidate_id: String,
    pub candidate_hash: String,
    pub execution_id: String,
    pub verifier_result_hash: String,
    pub vote: VoteChoice,
    pub salt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureEnvelope {
    pub signer_node_id: String,
    pub signature_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalityProof {
    pub threshold: u32,
    pub signatures: Vec<SignatureEnvelope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimRole {
    Propose,
    Verify,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimPayload {
    pub task_id: String,
    pub role: ClaimRole,
    pub claimer_node_id: String,
    pub execution_id: String,
    pub lease_until: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRenewPayload {
    pub task_id: String,
    pub role: ClaimRole,
    pub claimer_node_id: String,
    pub execution_id: String,
    pub lease_until: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimReleasePayload {
    pub task_id: String,
    pub role: ClaimRole,
    pub claimer_node_id: String,
    pub execution_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateProposedPayload {
    pub task_id: String,
    pub candidate: Candidate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceAddedPayload {
    pub task_id: String,
    pub candidate_id: String,
    pub execution_id: String,
    pub evidence_refs: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAvailablePayload {
    pub task_id: String,
    pub candidate_id: String,
    pub execution_id: String,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifierResultSubmittedPayload {
    pub task_id: String,
    pub result: VerifierResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionCommittedPayload {
    pub task_id: String,
    pub epoch: u64,
    pub candidate_id: String,
    pub candidate_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionFinalizedPayload {
    pub task_id: String,
    pub epoch: u64,
    pub candidate_id: String,
    pub winning_candidate_hash: String,
    pub finality_proof: FinalityProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskErrorPayload {
    pub task_id: String,
    pub reason: TaskErrorReason,
    pub reason_codes: Vec<u16>,
    pub custom_reason_namespace: Option<String>,
    pub custom_reason_code: Option<String>,
    pub custom_reason_message: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRetryScheduledPayload {
    pub task_id: String,
    pub attempt: u32,
    pub run_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskExpiredPayload {
    pub task_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EpochEndReason {
    BudgetExhausted,
    TimeboxReached,
    Finalized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochEndedPayload {
    pub task_id: String,
    pub epoch: u64,
    pub reason: EpochEndReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStoppedPayload {
    pub task_id: String,
    pub epoch: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSuspendedPayload {
    pub task_id: String,
    pub epoch: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskKilledPayload {
    pub task_id: String,
    pub epoch: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointCreatedPayload {
    pub checkpoint_id: String,
    pub up_to_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdatedPayload {
    pub new_membership: Membership,
    pub quorum_threshold: u32,
    pub quorum_signatures: Vec<SignatureEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyTunedPayload {
    pub policy_id: String,
    pub from_policy_hash: String,
    pub to_policy_hash: String,
    pub advisory_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisoryCreatedPayload {
    pub advisory_id: String,
    pub policy_id: String,
    pub suggested_policy_hash: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisoryApprovedPayload {
    pub advisory_id: String,
    pub admin_node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisoryAppliedPayload {
    pub advisory_id: String,
    pub applied_policy_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFeedbackReportedPayload {
    pub task_id: String,
    pub epoch: u64,
    pub outcome: String,
    pub reason: String,
    pub signed_by: String,
    pub signature: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionReference {
    pub task_id: String,
    pub epoch: u64,
    pub final_commit_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseRejectRecordedPayload {
    pub task_id: String,
    pub decision_ref: DecisionReference,
    pub candidate_hash: String,
    pub reject_quorum_proof: Vec<String>,
    pub reason_codes: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnowledgeHitType {
    Exact,
    Similar,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeHit {
    pub hit_type: KnowledgeHitType,
    pub decision_ref: DecisionReference,
    pub reuse_payload: Value,
    pub evidence_digests: Vec<ArtifactRef>,
    pub reason_codes_summary: Value,
    pub confidence_hint: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeedConstraints {
    pub must_consider_claims: Vec<String>,
    pub must_avoid_blacklist_candidate_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeedBundle {
    pub seed_type: String,
    pub hits: Vec<KnowledgeHit>,
    pub constraints: SeedConstraints,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
#[allow(clippy::large_enum_variant)]
pub enum EventPayload {
    TaskCreated(TaskContract),
    TaskClaimed(ClaimPayload),
    TaskClaimRenewed(ClaimRenewPayload),
    TaskClaimReleased(ClaimReleasePayload),
    CandidateProposed(CandidateProposedPayload),
    EvidenceAdded(EvidenceAddedPayload),
    EvidenceAvailable(EvidenceAvailablePayload),
    VerifierResultSubmitted(VerifierResultSubmittedPayload),
    VoteCommit(VoteCommitPayload),
    VoteReveal(VoteRevealPayload),
    DecisionCommitted(DecisionCommittedPayload),
    DecisionFinalized(DecisionFinalizedPayload),
    TaskError(TaskErrorPayload),
    TaskRetryScheduled(TaskRetryScheduledPayload),
    TaskExpired(TaskExpiredPayload),
    EpochEnded(EpochEndedPayload),
    TaskStopped(TaskStoppedPayload),
    TaskSuspended(TaskSuspendedPayload),
    TaskKilled(TaskKilledPayload),
    CheckpointCreated(CheckpointCreatedPayload),
    MembershipUpdated(MembershipUpdatedPayload),
    PolicyTuned(PolicyTunedPayload),
    AdvisoryCreated(AdvisoryCreatedPayload),
    AdvisoryApproved(AdvisoryApprovedPayload),
    AdvisoryApplied(AdvisoryAppliedPayload),
    TaskFeedbackReported(TaskFeedbackReportedPayload),
    ReuseRejectRecorded(ReuseRejectRecordedPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    TaskCreated,
    TaskClaimed,
    TaskClaimRenewed,
    TaskClaimReleased,
    CandidateProposed,
    EvidenceAdded,
    EvidenceAvailable,
    VerifierResultSubmitted,
    VoteCommit,
    VoteReveal,
    DecisionCommitted,
    DecisionFinalized,
    TaskError,
    TaskRetryScheduled,
    TaskExpired,
    EpochEnded,
    TaskStopped,
    TaskSuspended,
    TaskKilled,
    CheckpointCreated,
    MembershipUpdated,
    PolicyTuned,
    AdvisoryCreated,
    AdvisoryApproved,
    AdvisoryApplied,
    TaskFeedbackReported,
    ReuseRejectRecorded,
}

impl EventPayload {
    pub fn kind(&self) -> EventKind {
        match self {
            Self::TaskCreated(_) => EventKind::TaskCreated,
            Self::TaskClaimed(_) => EventKind::TaskClaimed,
            Self::TaskClaimRenewed(_) => EventKind::TaskClaimRenewed,
            Self::TaskClaimReleased(_) => EventKind::TaskClaimReleased,
            Self::CandidateProposed(_) => EventKind::CandidateProposed,
            Self::EvidenceAdded(_) => EventKind::EvidenceAdded,
            Self::EvidenceAvailable(_) => EventKind::EvidenceAvailable,
            Self::VerifierResultSubmitted(_) => EventKind::VerifierResultSubmitted,
            Self::VoteCommit(_) => EventKind::VoteCommit,
            Self::VoteReveal(_) => EventKind::VoteReveal,
            Self::DecisionCommitted(_) => EventKind::DecisionCommitted,
            Self::DecisionFinalized(_) => EventKind::DecisionFinalized,
            Self::TaskError(_) => EventKind::TaskError,
            Self::TaskRetryScheduled(_) => EventKind::TaskRetryScheduled,
            Self::TaskExpired(_) => EventKind::TaskExpired,
            Self::EpochEnded(_) => EventKind::EpochEnded,
            Self::TaskStopped(_) => EventKind::TaskStopped,
            Self::TaskSuspended(_) => EventKind::TaskSuspended,
            Self::TaskKilled(_) => EventKind::TaskKilled,
            Self::CheckpointCreated(_) => EventKind::CheckpointCreated,
            Self::MembershipUpdated(_) => EventKind::MembershipUpdated,
            Self::PolicyTuned(_) => EventKind::PolicyTuned,
            Self::AdvisoryCreated(_) => EventKind::AdvisoryCreated,
            Self::AdvisoryApproved(_) => EventKind::AdvisoryApproved,
            Self::AdvisoryApplied(_) => EventKind::AdvisoryApplied,
            Self::TaskFeedbackReported(_) => EventKind::TaskFeedbackReported,
            Self::ReuseRejectRecorded(_) => EventKind::ReuseRejectRecorded,
        }
    }

    pub fn task_id(&self) -> Option<&str> {
        match self {
            Self::TaskCreated(contract) => Some(&contract.task_id),
            Self::TaskClaimed(p) => Some(&p.task_id),
            Self::TaskClaimRenewed(p) => Some(&p.task_id),
            Self::TaskClaimReleased(p) => Some(&p.task_id),
            Self::CandidateProposed(p) => Some(&p.task_id),
            Self::EvidenceAdded(p) => Some(&p.task_id),
            Self::EvidenceAvailable(p) => Some(&p.task_id),
            Self::VerifierResultSubmitted(p) => Some(&p.task_id),
            Self::VoteCommit(p) => Some(&p.task_id),
            Self::VoteReveal(p) => Some(&p.task_id),
            Self::DecisionCommitted(p) => Some(&p.task_id),
            Self::DecisionFinalized(p) => Some(&p.task_id),
            Self::TaskError(p) => Some(&p.task_id),
            Self::TaskRetryScheduled(p) => Some(&p.task_id),
            Self::TaskExpired(p) => Some(&p.task_id),
            Self::EpochEnded(p) => Some(&p.task_id),
            Self::TaskStopped(p) => Some(&p.task_id),
            Self::TaskSuspended(p) => Some(&p.task_id),
            Self::TaskKilled(p) => Some(&p.task_id),
            Self::CheckpointCreated(_) => None,
            Self::MembershipUpdated(_) => None,
            Self::PolicyTuned(_) => None,
            Self::AdvisoryCreated(_) => None,
            Self::AdvisoryApproved(_) => None,
            Self::AdvisoryApplied(_) => None,
            Self::TaskFeedbackReported(p) => Some(&p.task_id),
            Self::ReuseRejectRecorded(p) => Some(&p.task_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub protocol_version: String,
    pub event_kind: EventKind,
    pub task_id: Option<String>,
    pub epoch: u64,
    pub author_node_id: String,
    pub created_at: u64,
    pub payload: EventPayload,
    pub signature_hex: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnsignedEvent {
    pub protocol_version: String,
    pub event_kind: EventKind,
    pub task_id: Option<String>,
    pub epoch: u64,
    pub author_node_id: String,
    pub created_at: u64,
    pub payload: EventPayload,
}

impl UnsignedEvent {
    pub fn from_payload(
        protocol_version: String,
        author_node_id: String,
        epoch: u64,
        created_at: u64,
        payload: EventPayload,
    ) -> Self {
        Self {
            protocol_version,
            event_kind: payload.kind(),
            task_id: payload.task_id().map(ToOwned::to_owned),
            epoch,
            author_node_id,
            created_at,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskTerminalState {
    Open,
    Expired,
    Finalized,
    Stopped,
    Suspended,
    Killed,
}

#[derive(Debug, Clone)]
pub struct TaskView {
    pub contract: TaskContract,
    pub epoch: u64,
    pub terminal_state: TaskTerminalState,
    pub committed_candidate_id: Option<String>,
    pub finalized_candidate_id: Option<String>,
}

fn default_one() -> u32 {
    1
}

fn default_topk() -> u32 {
    3
}

fn default_stop_rounds() -> u32 {
    3
}

fn default_max_snippet_bytes() -> u64 {
    8_192
}

fn default_max_snippet_tokens() -> u64 {
    2_048
}

impl Default for ExploreStopPolicy {
    fn default() -> Self {
        Self {
            no_new_evidence_rounds: default_stop_rounds(),
        }
    }
}

impl Default for ExploreAssignment {
    fn default() -> Self {
        Self {
            max_proposers: default_one(),
            topk: default_topk(),
            stop: ExploreStopPolicy::default(),
        }
    }
}

impl Default for VerifyAssignment {
    fn default() -> Self {
        Self {
            max_verifiers: default_one(),
        }
    }
}

impl Default for FinalizeAssignment {
    fn default() -> Self {
        Self {
            max_finalizers: default_one(),
        }
    }
}
