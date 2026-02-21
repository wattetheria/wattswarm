use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
}

impl Default for AggregationPolicy {
    fn default() -> Self {
        Self {
            mode: default_aggregation_mode(),
            quorum: None,
        }
    }
}

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
