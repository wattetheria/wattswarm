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
pub struct FeedSubscriptionUpdatedPayload {
    #[serde(default = "default_network_context_id")]
    pub network_id: String,
    pub subscriber_node_id: String,
    pub feed_key: String,
    pub scope_hint: String,
    pub active: bool,
}

impl FeedSubscriptionUpdatedPayload {
    pub fn scope(&self) -> Option<ScopeHint> {
        ScopeHint::parse(&self.scope_hint)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskAnnouncedPayload {
    #[serde(default = "default_network_context_id")]
    pub network_id: String,
    pub task_id: String,
    pub announcement_id: String,
    pub feed_key: String,
    pub scope_hint: String,
    pub summary: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_ref: Option<ArtifactRef>,
}

impl TaskAnnouncedPayload {
    pub fn scope(&self) -> Option<ScopeHint> {
        ScopeHint::parse(&self.scope_hint)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionIntentDeclaredPayload {
    #[serde(default = "default_network_context_id")]
    pub network_id: String,
    pub task_id: String,
    pub execution_set_id: String,
    pub participant_node_id: String,
    pub role_hint: String,
    pub scope_hint: String,
    pub intent: String,
}

impl ExecutionIntentDeclaredPayload {
    pub fn scope(&self) -> Option<ScopeHint> {
        ScopeHint::parse(&self.scope_hint)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSetMember {
    pub participant_node_id: String,
    pub role_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSetConfirmedPayload {
    #[serde(default = "default_network_context_id")]
    pub network_id: String,
    pub task_id: String,
    pub execution_set_id: String,
    pub confirmed_by_node_id: String,
    pub scope_hint: String,
    pub members: Vec<ExecutionSetMember>,
}

impl ExecutionSetConfirmedPayload {
    pub fn scope(&self) -> Option<ScopeHint> {
        ScopeHint::parse(&self.scope_hint)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NetworkKind {
    Local,
    Lan,
    Mainnet,
    Subnet,
}

impl NetworkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Lan => "lan",
            Self::Mainnet => "mainnet",
            Self::Subnet => "subnet",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "local" => Some(Self::Local),
            "lan" => Some(Self::Lan),
            "mainnet" => Some(Self::Mainnet),
            "subnet" => Some(Self::Subnet),
            _ => None,
        }
    }

    pub fn is_overlay(self) -> bool {
        matches!(self, Self::Mainnet | Self::Subnet)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkDescriptor {
    pub network_id: String,
    pub network_kind: NetworkKind,
    pub parent_network_id: Option<String>,
    pub genesis_node_id: String,
}

impl NetworkDescriptor {
    pub fn is_subnet(&self) -> bool {
        self.network_kind == NetworkKind::Subnet
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrgDescriptor {
    pub org_id: String,
    pub network_id: String,
    pub org_kind: String,
    pub is_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkTopology {
    pub network: NetworkDescriptor,
    pub org: OrgDescriptor,
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
pub struct EventRevokedPayload {
    pub target_event_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryRevokedPayload {
    pub target_summary_id: String,
    pub summary_kind: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePenalizedPayload {
    pub penalized_node_id: String,
    pub reason: String,
    #[serde(default)]
    pub revoked_event_ids: Vec<String>,
    #[serde(default)]
    pub revoked_summary_ids: Vec<String>,
    #[serde(default = "default_true")]
    pub block_summaries: bool,
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
    FeedSubscriptionUpdated(FeedSubscriptionUpdatedPayload),
    TaskAnnounced(TaskAnnouncedPayload),
    ExecutionIntentDeclared(ExecutionIntentDeclaredPayload),
    ExecutionSetConfirmed(ExecutionSetConfirmedPayload),
    MembershipUpdated(MembershipUpdatedPayload),
    PolicyTuned(PolicyTunedPayload),
    AdvisoryCreated(AdvisoryCreatedPayload),
    AdvisoryApproved(AdvisoryApprovedPayload),
    AdvisoryApplied(AdvisoryAppliedPayload),
    TaskFeedbackReported(TaskFeedbackReportedPayload),
    ReuseRejectRecorded(ReuseRejectRecordedPayload),
    EventRevoked(EventRevokedPayload),
    SummaryRevoked(SummaryRevokedPayload),
    NodePenalized(NodePenalizedPayload),
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
    FeedSubscriptionUpdated,
    TaskAnnounced,
    ExecutionIntentDeclared,
    ExecutionSetConfirmed,
    MembershipUpdated,
    PolicyTuned,
    AdvisoryCreated,
    AdvisoryApproved,
    AdvisoryApplied,
    TaskFeedbackReported,
    ReuseRejectRecorded,
    EventRevoked,
    SummaryRevoked,
    NodePenalized,
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
            Self::FeedSubscriptionUpdated(_) => EventKind::FeedSubscriptionUpdated,
            Self::TaskAnnounced(_) => EventKind::TaskAnnounced,
            Self::ExecutionIntentDeclared(_) => EventKind::ExecutionIntentDeclared,
            Self::ExecutionSetConfirmed(_) => EventKind::ExecutionSetConfirmed,
            Self::MembershipUpdated(_) => EventKind::MembershipUpdated,
            Self::PolicyTuned(_) => EventKind::PolicyTuned,
            Self::AdvisoryCreated(_) => EventKind::AdvisoryCreated,
            Self::AdvisoryApproved(_) => EventKind::AdvisoryApproved,
            Self::AdvisoryApplied(_) => EventKind::AdvisoryApplied,
            Self::TaskFeedbackReported(_) => EventKind::TaskFeedbackReported,
            Self::ReuseRejectRecorded(_) => EventKind::ReuseRejectRecorded,
            Self::EventRevoked(_) => EventKind::EventRevoked,
            Self::SummaryRevoked(_) => EventKind::SummaryRevoked,
            Self::NodePenalized(_) => EventKind::NodePenalized,
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
            Self::FeedSubscriptionUpdated(_) => None,
            Self::TaskAnnounced(p) => Some(&p.task_id),
            Self::ExecutionIntentDeclared(p) => Some(&p.task_id),
            Self::ExecutionSetConfirmed(p) => Some(&p.task_id),
            Self::MembershipUpdated(_) => None,
            Self::PolicyTuned(_) => None,
            Self::AdvisoryCreated(_) => None,
            Self::AdvisoryApproved(_) => None,
            Self::AdvisoryApplied(_) => None,
            Self::TaskFeedbackReported(p) => Some(&p.task_id),
            Self::ReuseRejectRecorded(p) => Some(&p.task_id),
            Self::EventRevoked(_) => None,
            Self::SummaryRevoked(_) => None,
            Self::NodePenalized(_) => None,
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

fn default_true() -> bool {
    true
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

/// Network-wide protocol parameters set by the genesis node.
///
/// These values are persisted in `network_params.params_json` and govern
/// P2P behaviour for every node in the network. Individual nodes MUST NOT
/// override them; only governance proposals can change them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkProtocolParams {
    // ── Network identity / compatibility ────────────────────────────
    /// Shared topic namespace prefix for this network.
    #[serde(default = "default_namespace_network")]
    pub namespace_network: String,

    /// Shared libp2p identify protocol version for this network.
    #[serde(default = "default_network_protocol_version")]
    pub protocol_version: String,

    /// Maximum number of simultaneously established connections allowed
    /// to the same peer.
    #[serde(default = "default_max_established_per_peer")]
    pub max_established_per_peer: u32,

    // ── Anti-entropy timing ──────────────────────────────────────────
    /// Minimum seconds between successful anti-entropy backfill requests
    /// to the same peer.
    #[serde(default = "default_anti_entropy_interval_secs")]
    pub anti_entropy_interval_secs: u64,

    /// Seconds to wait before retrying a failed backfill request.
    #[serde(default = "default_backfill_retry_after_secs")]
    pub backfill_retry_after_secs: u64,

    // ── Gossipsub mesh ───────────────────────────────────────────────
    /// Target number of mesh peers per topic.
    #[serde(default = "default_gossipsub_d")]
    pub gossipsub_d: usize,

    /// Minimum mesh peers before attempting to graft more.
    #[serde(default = "default_gossipsub_d_low")]
    pub gossipsub_d_low: usize,

    /// Maximum mesh peers before pruning excess.
    #[serde(default = "default_gossipsub_d_high")]
    pub gossipsub_d_high: usize,

    /// Gossipsub heartbeat interval in milliseconds.
    #[serde(default = "default_gossipsub_heartbeat_ms")]
    pub gossipsub_heartbeat_ms: u64,

    /// Maximum single gossip message size in bytes.
    #[serde(default = "default_gossipsub_max_transmit_size")]
    pub gossipsub_max_transmit_size: usize,

    // ── Backfill protocol ────────────────────────────────────────────
    /// Default number of events returned per backfill response.
    #[serde(default = "default_max_backfill_events")]
    pub default_max_backfill_events: usize,

    /// Hard upper limit on events per backfill response.
    #[serde(default = "default_max_backfill_events_hard_limit")]
    pub max_backfill_events_hard_limit: usize,

    // ── Summary propagation ──────────────────────────────────────────
    /// Maximum reputation snapshot entries per summary announcement.
    #[serde(default = "default_summary_reputation_limit")]
    pub summary_reputation_limit: usize,

    /// Maximum decision-memory hits per knowledge summary.
    #[serde(default = "default_summary_decision_memory_limit")]
    pub summary_decision_memory_limit: u32,
}

fn default_namespace_network() -> String {
    "wattswarm".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeHint {
    Global,
    Region(String),
    Node(String),
}

impl ScopeHint {
    pub fn from_kind_id(kind: &str, id: &str) -> Option<Self> {
        let kind = kind.trim();
        if kind.eq_ignore_ascii_case("global") {
            return Some(Self::Global);
        }
        let id = id.trim();
        if id.is_empty() {
            return None;
        }
        Self::parse(&format!("{kind}:{id}"))
    }

    pub fn parse(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("global") {
            return Some(Self::Global);
        }
        let (kind, rest) = trimmed.split_once(':')?;
        let id = rest.trim();
        if id.is_empty() {
            return None;
        }
        match kind.trim().to_ascii_lowercase().as_str() {
            "region" => Some(Self::Region(id.to_owned())),
            "local" | "node" => Some(Self::Node(id.to_owned())),
            _ => None,
        }
    }

    pub fn canonical(&self) -> String {
        match self {
            Self::Global => "global".to_owned(),
            Self::Region(id) => format!("region:{id}"),
            Self::Node(id) => format!("node:{id}"),
        }
    }

    pub fn parse_prefix(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("global") {
            return Some(Self::Global);
        }
        let (kind, rest) = trimmed.split_once(':')?;
        let id = rest
            .split([':', '/'])
            .next()
            .map(str::trim)
            .unwrap_or_default();
        if id.is_empty() {
            return None;
        }
        Self::parse(&format!("{}:{id}", kind.trim()))
    }

    pub fn parse_with_prefix_fallback(raw: &str) -> Option<Self> {
        Self::parse_prefix(raw).or_else(|| Self::parse(raw))
    }
}

pub fn canonical_scope_hint(raw: &str) -> Option<String> {
    ScopeHint::parse(raw).map(|scope| scope.canonical())
}

pub fn normalized_scope_hint(raw: &str) -> String {
    canonical_scope_hint(raw).unwrap_or_else(|| raw.trim().to_owned())
}

fn default_network_protocol_version() -> String {
    "/wattswarm/0.1.0".to_owned()
}
fn default_network_context_id() -> String {
    "default".to_owned()
}
fn default_max_established_per_peer() -> u32 {
    2
}
fn default_anti_entropy_interval_secs() -> u64 {
    15
}
fn default_backfill_retry_after_secs() -> u64 {
    5
}
fn default_gossipsub_d() -> usize {
    6
}
fn default_gossipsub_d_low() -> usize {
    4
}
fn default_gossipsub_d_high() -> usize {
    12
}
fn default_gossipsub_heartbeat_ms() -> u64 {
    1_000
}
fn default_gossipsub_max_transmit_size() -> usize {
    512 * 1024
}
fn default_max_backfill_events() -> usize {
    512
}
fn default_max_backfill_events_hard_limit() -> usize {
    8_192
}
fn default_summary_reputation_limit() -> usize {
    64
}
fn default_summary_decision_memory_limit() -> u32 {
    16
}

impl Default for NetworkProtocolParams {
    fn default() -> Self {
        Self {
            namespace_network: default_namespace_network(),
            protocol_version: default_network_protocol_version(),
            max_established_per_peer: default_max_established_per_peer(),
            anti_entropy_interval_secs: default_anti_entropy_interval_secs(),
            backfill_retry_after_secs: default_backfill_retry_after_secs(),
            gossipsub_d: default_gossipsub_d(),
            gossipsub_d_low: default_gossipsub_d_low(),
            gossipsub_d_high: default_gossipsub_d_high(),
            gossipsub_heartbeat_ms: default_gossipsub_heartbeat_ms(),
            gossipsub_max_transmit_size: default_gossipsub_max_transmit_size(),
            default_max_backfill_events: default_max_backfill_events(),
            max_backfill_events_hard_limit: default_max_backfill_events_hard_limit(),
            summary_reputation_limit: default_summary_reputation_limit(),
            summary_decision_memory_limit: default_summary_decision_memory_limit(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsignedNetworkProtocolParamsEnvelope {
    pub network_id: String,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    pub params: NetworkProtocolParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedNetworkProtocolParamsEnvelope {
    pub network_id: String,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    pub params_hash: String,
    pub params: NetworkProtocolParams,
    pub signed_by: String,
    pub signature: String,
}

impl SignedNetworkProtocolParamsEnvelope {
    pub fn unsigned_payload(&self) -> UnsignedNetworkProtocolParamsEnvelope {
        UnsignedNetworkProtocolParamsEnvelope {
            network_id: self.network_id.clone(),
            version: self.version,
            prev_hash: self.prev_hash.clone(),
            params: self.params.clone(),
        }
    }
}
