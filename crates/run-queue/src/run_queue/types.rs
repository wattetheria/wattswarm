use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: default_backoff_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationPolicy {
    #[serde(default = "default_aggregation_mode")]
    pub mode: String,
    pub quorum: Option<u32>,
    #[serde(default)]
    pub tie_policy: TiePolicy,
    #[serde(default)]
    pub null_policy: NullPolicy,
}

impl Default for AggregationPolicy {
    fn default() -> Self {
        Self {
            mode: default_aggregation_mode(),
            quorum: None,
            tie_policy: TiePolicy::default(),
            null_policy: NullPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TieTrigger {
    QuorumNull,
    Tie,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TieResolverMode {
    #[serde(rename = "REEXPLORE")]
    ReExplore,
    ConfidenceWeighted,
    ReputationWeighted,
    Stochastic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TiePolicy {
    #[serde(default = "default_tie_enabled_on")]
    pub enabled_on: Vec<TieTrigger>,
    #[serde(default = "default_tie_chain")]
    pub chain: Vec<TieResolverMode>,
    #[serde(default)]
    pub reexplore: ReExplorePolicy,
    #[serde(default)]
    pub confidence_weighted: ConfidenceWeightedPolicy,
    #[serde(default)]
    pub reputation_weighted: ReputationWeightedPolicy,
    #[serde(default)]
    pub stochastic: StochasticPolicy,
}

impl Default for TiePolicy {
    fn default() -> Self {
        Self {
            enabled_on: default_tie_enabled_on(),
            chain: default_tie_chain(),
            reexplore: ReExplorePolicy::default(),
            confidence_weighted: ConfidenceWeightedPolicy::default(),
            reputation_weighted: ReputationWeightedPolicy::default(),
            stochastic: StochasticPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NullTrigger {
    Empty,
    QuorumNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NullResolverMode {
    #[serde(rename = "REEXPLORE")]
    ReExplore,
    FinalizeNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullPolicy {
    #[serde(default = "default_null_enabled_on")]
    pub enabled_on: Vec<NullTrigger>,
    #[serde(default = "default_null_chain")]
    pub chain: Vec<NullResolverMode>,
    #[serde(default)]
    pub reexplore: ReExplorePolicy,
}

impl Default for NullPolicy {
    fn default() -> Self {
        Self {
            enabled_on: default_null_enabled_on(),
            chain: default_null_chain(),
            reexplore: ReExplorePolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReExplorePolicy {
    #[serde(default = "default_reexplore_max_tie_iterations")]
    pub max_tie_iterations: u32,
    #[serde(default = "default_reexplore_no_new_evidence_rounds")]
    pub no_new_evidence_rounds: u32,
    #[serde(default = "default_true")]
    pub require_new_evidence: bool,
}

impl Default for ReExplorePolicy {
    fn default() -> Self {
        Self {
            max_tie_iterations: default_reexplore_max_tie_iterations(),
            no_new_evidence_rounds: default_reexplore_no_new_evidence_rounds(),
            require_new_evidence: default_true(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceWeightedPolicy {
    #[serde(default = "default_decision_confidence_field")]
    pub decision_confidence_field: String,
    #[serde(default = "default_answer_confidence_field")]
    pub answer_confidence_field: String,
    #[serde(default = "default_confidence_field")]
    pub confidence_field: String,
    #[serde(default = "default_confidence_fallback")]
    pub default_confidence: f64,
}

impl Default for ConfidenceWeightedPolicy {
    fn default() -> Self {
        Self {
            decision_confidence_field: default_decision_confidence_field(),
            answer_confidence_field: default_answer_confidence_field(),
            confidence_field: default_confidence_field(),
            default_confidence: default_confidence_fallback(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MissingReputationPolicy {
    Skip,
    Uniform,
}

impl Default for MissingReputationPolicy {
    fn default() -> Self {
        Self::Skip
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationWeightedPolicy {
    #[serde(default)]
    pub agent_reputation_units: HashMap<String, i64>,
    #[serde(default)]
    pub missing_reputation: MissingReputationPolicy,
}

impl Default for ReputationWeightedPolicy {
    fn default() -> Self {
        Self {
            agent_reputation_units: HashMap::new(),
            missing_reputation: MissingReputationPolicy::Skip,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StochasticPolicy {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAgentSpec {
    pub agent_id: String,
    pub executor: String,
    #[serde(default = "default_profile")]
    pub profile: String,
    pub prompt: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSubmitSpec {
    pub run_id: String,
    #[serde(default = "default_task_type")]
    pub task_type: String,
    #[serde(default = "default_shared_inputs")]
    pub shared_inputs: Value,
    pub agents: Vec<RunAgentSpec>,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub aggregation: AggregationPolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunView {
    pub run_id: String,
    pub status: String,
    pub task_type: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub counts: RunStepCounts,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RunStepCounts {
    pub created: i64,
    pub queued: i64,
    pub leased: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub retry_wait: i64,
    pub cancelled: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunEvent {
    pub id: i64,
    pub run_id: String,
    pub event_type: String,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    pub worker_id: String,
    pub concurrency: usize,
    pub poll_ms: u64,
    pub lease_ms: u64,
    pub once: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimedStep {
    pub(crate) step_id: String,
    pub(crate) run_id: String,
    pub(crate) agent_id: String,
    pub(crate) executor: String,
    pub(crate) profile: String,
    pub(crate) prompt: String,
    pub(crate) attempt: u32,
    pub(crate) max_attempts: u32,
    pub(crate) task_type: String,
    pub(crate) shared_inputs: Value,
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) run_status: String,
    pub(crate) lease_id: String,
}

pub(crate) fn default_max_attempts() -> u32 {
    2
}

pub(crate) fn default_backoff_ms() -> u64 {
    1_500
}

pub(crate) fn default_aggregation_mode() -> String {
    "all_done".to_owned()
}

fn default_tie_enabled_on() -> Vec<TieTrigger> {
    vec![TieTrigger::Tie]
}

fn default_tie_chain() -> Vec<TieResolverMode> {
    vec![TieResolverMode::Stochastic]
}

fn default_null_enabled_on() -> Vec<NullTrigger> {
    vec![NullTrigger::Empty, NullTrigger::QuorumNull]
}

fn default_null_chain() -> Vec<NullResolverMode> {
    vec![NullResolverMode::ReExplore, NullResolverMode::FinalizeNull]
}

fn default_reexplore_max_tie_iterations() -> u32 {
    1
}

fn default_reexplore_no_new_evidence_rounds() -> u32 {
    1
}

fn default_true() -> bool {
    true
}

fn default_decision_confidence_field() -> String {
    "decision_confidence".to_owned()
}

fn default_answer_confidence_field() -> String {
    "answer_confidence".to_owned()
}

fn default_confidence_field() -> String {
    "confidence".to_owned()
}

fn default_confidence_fallback() -> f64 {
    0.5
}

pub(crate) fn default_profile() -> String {
    "default".to_owned()
}

pub(crate) fn default_weight() -> f64 {
    1.0
}

pub(crate) fn default_task_type() -> String {
    "swarm".to_owned()
}

pub(crate) fn default_shared_inputs() -> Value {
    Value::Object(Map::new())
}
