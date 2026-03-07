use anyhow::Result;
use postgres::Transaction;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use wattswarm_crypto::sha256_hex;

use super::types::{
    AggregationPolicy, ConfidenceWeightedPolicy, MissingReputationPolicy, NullResolverMode,
    NullTrigger, ReExplorePolicy, TieResolverMode, TieTrigger, default_aggregation_mode,
};

const RUN_TIE_REEXPLORE_TRIGGERED: &str = "RUN_TIE_REEXPLORE_TRIGGERED";
const RUN_NULL_REEXPLORE_TRIGGERED: &str = "RUN_NULL_REEXPLORE_TRIGGERED";

#[derive(Debug, Clone)]
pub(crate) struct RunSummaryOutput {
    pub(crate) result_payload: Value,
    pub(crate) event_payload: Value,
    pub(crate) action: AggregationNextAction,
}

#[derive(Debug, Clone)]
pub(crate) enum AggregationNextAction {
    Finalize,
    ReExplore(ReExploreDirective),
}

#[derive(Debug, Clone)]
pub(crate) struct ReExploreDirective {
    pub(crate) next_iteration: u32,
    pub(crate) event_type: String,
    pub(crate) signal_type: String,
    pub(crate) resolution_path: String,
    pub(crate) no_new_evidence_rounds: u32,
    pub(crate) require_new_evidence: bool,
    pub(crate) missing_new_evidence_agents: Vec<String>,
    pub(crate) evidence_by_agent: HashMap<String, Vec<String>>,
    pub(crate) unresolved_buckets: Vec<String>,
    pub(crate) null_primary_kind: Option<String>,
    pub(crate) null_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnresolvedKind {
    QuorumNull,
    Tie,
    Empty,
}

#[derive(Debug, Clone)]
struct MajorityOutcome {
    winner: Option<String>,
    unresolved_kind: Option<UnresolvedKind>,
    candidates: Vec<String>,
}

#[derive(Debug, Clone)]
struct VoteEntry {
    agent_id: String,
    decision: Option<String>,
    answer: Option<String>,
    decision_confidence: f64,
    answer_confidence: f64,
}

#[derive(Debug, Clone)]
struct ReExploreState {
    iteration: u32,
    no_new_evidence_rounds: u32,
    evidence_by_agent: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionPath {
    Winner,
    Tie,
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullPrimaryKind {
    Empty,
    QuorumNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullReason {
    NoValidDecision,
    ExecutionIncompleteOrFailed,
    ValidVotesBelowQuorum,
    Unknown,
}

impl ResolutionPath {
    fn as_str(self) -> &'static str {
        match self {
            ResolutionPath::Winner => "WINNER_PATH",
            ResolutionPath::Tie => "TIE_PATH",
            ResolutionPath::Null => "NULL_PATH",
        }
    }
}

impl NullPrimaryKind {
    fn as_str(self) -> &'static str {
        match self {
            NullPrimaryKind::Empty => "EMPTY",
            NullPrimaryKind::QuorumNull => "QUORUM_NULL",
        }
    }
}

impl NullReason {
    fn as_str(self) -> &'static str {
        match self {
            NullReason::NoValidDecision => "NO_VALID_DECISION",
            NullReason::ExecutionIncompleteOrFailed => "EXECUTION_INCOMPLETE_OR_FAILED",
            NullReason::ValidVotesBelowQuorum => "VALID_VOTES_BELOW_QUORUM",
            NullReason::Unknown => "UNKNOWN",
        }
    }
}

pub(crate) fn build_run_summary_tx(
    tx: &mut Transaction<'_>,
    run_id: &str,
    final_status: &str,
    counts: &super::types::RunStepCounts,
) -> Result<RunSummaryOutput> {
    let aggregation_policy = load_aggregation_policy(tx, run_id)?;
    let tie_reexplore_state = load_reexplore_state(tx, run_id, RUN_TIE_REEXPLORE_TRIGGERED)?;
    let null_reexplore_state = load_reexplore_state(tx, run_id, RUN_NULL_REEXPLORE_TRIGGERED)?;

    let step_rows = tx.query(
        "SELECT step_id, agent_id, executor, profile, status, attempt, task_id, result_json, error_text
         FROM run_steps
         WHERE run_id = $1
         ORDER BY priority DESC, step_id ASC",
        &[&run_id],
    )?;

    let mut entries = Vec::with_capacity(step_rows.len());
    let mut steps = Vec::with_capacity(step_rows.len());
    let mut current_evidence_by_agent: HashMap<String, HashSet<String>> = HashMap::new();
    let mut has_incomplete_or_failed_steps = false;

    for row in step_rows {
        let agent_id: String = row.get(1);
        let step_status: String = row.get(4);
        if step_status != "SUCCEEDED" {
            has_incomplete_or_failed_steps = true;
        }
        let result_raw: Option<String> = row.get(7);
        let result = parse_json_value(result_raw.as_deref());
        let conclusion = extract_step_conclusion(&result);
        let evidence_digests = extract_evidence_digests(&result, &conclusion);
        current_evidence_by_agent
            .entry(agent_id.clone())
            .or_default()
            .extend(evidence_digests.iter().cloned());

        let decision = conclusion
            .get("decision")
            .and_then(Value::as_str)
            .and_then(normalize_text);
        let answer = conclusion
            .get("answer")
            .and_then(Value::as_str)
            .and_then(normalize_text);
        entries.push(VoteEntry {
            agent_id: agent_id.clone(),
            decision,
            answer,
            decision_confidence: confidence_for_bucket(
                &conclusion,
                &aggregation_policy.tie_policy.confidence_weighted,
                true,
            ),
            answer_confidence: confidence_for_bucket(
                &conclusion,
                &aggregation_policy.tie_policy.confidence_weighted,
                false,
            ),
        });

        steps.push(json!({
            "step_id": row.get::<_, String>(0),
            "agent_id": agent_id,
            "executor": row.get::<_, String>(2),
            "profile": row.get::<_, String>(3),
            "status": step_status,
            "attempt": row.get::<_, i32>(5),
            "task_id": row.get::<_, Option<String>>(6),
            "error": row.get::<_, Option<String>>(8),
            "result": result,
            "conclusion": conclusion,
            "evidence_digests": evidence_digests.into_iter().collect::<Vec<_>>(),
        }));
    }

    let tie_reexplore_analysis = analyze_reexplore(
        &aggregation_policy.tie_policy.reexplore,
        &tie_reexplore_state,
        &current_evidence_by_agent,
    );
    let null_reexplore_analysis = analyze_reexplore(
        &aggregation_policy.null_policy.reexplore,
        &null_reexplore_state,
        &current_evidence_by_agent,
    );
    let tie_eligible_agents = if aggregation_policy.tie_policy.reexplore.require_new_evidence
        && tie_reexplore_state.iteration > 0
    {
        tie_reexplore_analysis.eligible_agents.clone()
    } else {
        HashSet::new()
    };
    let null_eligible_agents = if aggregation_policy
        .null_policy
        .reexplore
        .require_new_evidence
        && null_reexplore_state.iteration > 0
    {
        null_reexplore_analysis.eligible_agents.clone()
    } else {
        HashSet::new()
    };
    let eligible_agents = if !tie_eligible_agents.is_empty() {
        tie_eligible_agents
    } else {
        null_eligible_agents
    };

    let mut decision_votes: HashMap<String, u32> = HashMap::new();
    let mut answer_votes: HashMap<String, u32> = HashMap::new();
    for entry in &entries {
        if !eligible_agents.is_empty() && !eligible_agents.contains(entry.agent_id.as_str()) {
            continue;
        }
        if let Some(decision) = &entry.decision {
            bump_vote(&mut decision_votes, decision);
        }
        if let Some(answer) = &entry.answer {
            bump_vote(&mut answer_votes, answer);
        }
    }

    let decision_majority = pick_majority(&decision_votes, aggregation_policy.quorum);
    let answer_majority = pick_majority(&answer_votes, aggregation_policy.quorum);
    let mut tie_resolution_trace = Vec::new();
    let mut null_resolution_trace = Vec::new();

    let decision_outcome = resolve_bucket(
        run_id,
        "decision",
        &decision_majority,
        &decision_votes,
        &entries,
        has_incomplete_or_failed_steps,
        &aggregation_policy,
        &mut tie_resolution_trace,
        &mut null_resolution_trace,
    );
    let answer_outcome = resolve_bucket(
        run_id,
        "answer",
        &answer_majority,
        &answer_votes,
        &entries,
        has_incomplete_or_failed_steps,
        &aggregation_policy,
        &mut tie_resolution_trace,
        &mut null_resolution_trace,
    );

    let final_decision = decision_outcome.value.clone();
    let final_answer = answer_outcome.value.clone();

    let tie_unresolved_buckets = [("decision", &decision_outcome), ("answer", &answer_outcome)]
        .iter()
        .filter_map(|(bucket, out)| {
            if out.path == ResolutionPath::Tie {
                Some((*bucket).to_owned())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let null_unresolved_buckets = [("decision", &decision_outcome), ("answer", &answer_outcome)]
        .iter()
        .filter_map(|(bucket, out)| {
            if out.path == ResolutionPath::Null {
                Some((*bucket).to_owned())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let need_tie_reexplore = !tie_unresolved_buckets.is_empty()
        && tie_unresolved_buckets.iter().any(|bucket| {
            let kind = if bucket == "decision" {
                decision_majority.unresolved_kind
            } else {
                answer_majority.unresolved_kind
            };
            should_apply_tie_policy(&aggregation_policy, kind)
        })
        && aggregation_policy
            .tie_policy
            .chain
            .iter()
            .any(|mode| *mode == TieResolverMode::ReExplore)
        && tie_reexplore_analysis.allow_reexplore;

    let need_null_reexplore = !null_unresolved_buckets.is_empty()
        && null_unresolved_buckets.iter().any(|bucket| {
            let kind = if bucket == "decision" {
                decision_majority.unresolved_kind
            } else {
                answer_majority.unresolved_kind
            };
            should_apply_null_policy(&aggregation_policy, kind)
        })
        && aggregation_policy
            .null_policy
            .chain
            .iter()
            .any(|mode| *mode == NullResolverMode::ReExplore)
        && null_reexplore_analysis.allow_reexplore;

    let decision_path = decision_outcome.path.as_str();
    let answer_path = answer_outcome.path.as_str();

    let aggregation_overview = json!({
        "mode": aggregation_policy.mode,
        "final_decision": final_decision.clone(),
        "final_answer": final_answer.clone(),
        "decision_votes": decision_votes,
        "answer_votes": answer_votes,
        "resolution_paths": {
            "decision": decision_path,
            "answer": answer_path
        },
        "tie_policy": {
            "enabled_on": aggregation_policy.tie_policy.enabled_on,
            "chain": aggregation_policy.tie_policy.chain,
            "trace": tie_resolution_trace,
            "reexplore": {
                "tie_iteration": tie_reexplore_state.iteration,
                "next_tie_iteration": tie_reexplore_state.iteration.saturating_add(1),
                "allow_reexplore": tie_reexplore_analysis.allow_reexplore,
                "no_new_evidence_rounds": tie_reexplore_analysis.no_new_evidence_rounds,
                "max_tie_iterations": aggregation_policy.tie_policy.reexplore.max_tie_iterations,
                "require_new_evidence": aggregation_policy.tie_policy.reexplore.require_new_evidence,
                "missing_new_evidence_agents": tie_reexplore_analysis.missing_new_evidence_agents,
                "unresolved_buckets": tie_unresolved_buckets
            }
        },
        "null_policy": {
            "enabled_on": aggregation_policy.null_policy.enabled_on,
            "chain": aggregation_policy.null_policy.chain,
            "trace": null_resolution_trace,
            "reexplore": {
                "null_iteration": null_reexplore_state.iteration,
                "next_null_iteration": null_reexplore_state.iteration.saturating_add(1),
                "allow_reexplore": null_reexplore_analysis.allow_reexplore,
                "no_new_evidence_rounds": null_reexplore_analysis.no_new_evidence_rounds,
                "max_tie_iterations": aggregation_policy.null_policy.reexplore.max_tie_iterations,
                "require_new_evidence": aggregation_policy.null_policy.reexplore.require_new_evidence,
                "missing_new_evidence_agents": null_reexplore_analysis.missing_new_evidence_agents,
                "unresolved_buckets": null_unresolved_buckets
            }
        },
        "null_resolution": {
            "decision": {
                "primary_kind": decision_outcome.null_primary_kind.map(|v| v.as_str()),
                "reason": decision_outcome.null_reason.map(|v| v.as_str())
            },
            "answer": {
                "primary_kind": answer_outcome.null_primary_kind.map(|v| v.as_str()),
                "reason": answer_outcome.null_reason.map(|v| v.as_str())
            }
        }
    });

    let result_payload = json!({
        "run_id": run_id,
        "status": final_status,
        "counts": counts,
        "aggregation": {
            "mode": aggregation_policy.mode,
            "final_decision": final_decision,
            "final_answer": final_answer,
            "decision_votes": aggregation_overview["decision_votes"].clone(),
            "answer_votes": aggregation_overview["answer_votes"].clone(),
            "tie_policy": aggregation_overview["tie_policy"].clone(),
            "steps": steps
        }
    });

    if need_tie_reexplore {
        let directive = ReExploreDirective {
            next_iteration: tie_reexplore_state.iteration.saturating_add(1),
            event_type: RUN_TIE_REEXPLORE_TRIGGERED.to_owned(),
            signal_type: "NEED_MORE_EVIDENCE".to_owned(),
            resolution_path: ResolutionPath::Tie.as_str().to_owned(),
            no_new_evidence_rounds: tie_reexplore_analysis.no_new_evidence_rounds,
            require_new_evidence: aggregation_policy.tie_policy.reexplore.require_new_evidence,
            missing_new_evidence_agents: tie_reexplore_analysis.missing_new_evidence_agents.clone(),
            evidence_by_agent: map_sets_to_sorted_vec(&current_evidence_by_agent),
            unresolved_buckets: tie_unresolved_buckets.clone(),
            null_primary_kind: None,
            null_reason: None,
        };
        let event_payload = json!({
            "run_id": run_id,
            "action": directive.signal_type,
            "path": directive.resolution_path,
            "aggregation": aggregation_overview,
            "reexplore": {
                "tie_iteration": directive.next_iteration,
                "no_new_evidence_rounds": directive.no_new_evidence_rounds,
                "require_new_evidence": directive.require_new_evidence,
                "missing_new_evidence_agents": directive.missing_new_evidence_agents,
                "evidence_by_agent": directive.evidence_by_agent,
                "unresolved_buckets": directive.unresolved_buckets
            }
        });
        return Ok(RunSummaryOutput {
            result_payload,
            event_payload,
            action: AggregationNextAction::ReExplore(directive),
        });
    }

    if need_null_reexplore {
        let null_focus = if decision_outcome.path == ResolutionPath::Null {
            Some(&decision_outcome)
        } else if answer_outcome.path == ResolutionPath::Null {
            Some(&answer_outcome)
        } else {
            None
        };
        let null_primary_kind = null_focus
            .and_then(|out| out.null_primary_kind)
            .map(|v| v.as_str().to_owned());
        let null_reason = null_focus
            .and_then(|out| out.null_reason)
            .map(|v| v.as_str().to_owned());
        let signal_type = if matches!(
            null_focus.and_then(|out| out.null_reason),
            Some(NullReason::ExecutionIncompleteOrFailed)
        ) {
            "NEED_STEP_RECOVERY"
        } else if matches!(
            null_focus.and_then(|out| out.null_primary_kind),
            Some(NullPrimaryKind::Empty)
        ) {
            "NEED_DECISION"
        } else {
            "NEED_MORE_EVIDENCE"
        };
        let directive = ReExploreDirective {
            next_iteration: null_reexplore_state.iteration.saturating_add(1),
            event_type: RUN_NULL_REEXPLORE_TRIGGERED.to_owned(),
            signal_type: signal_type.to_owned(),
            resolution_path: ResolutionPath::Null.as_str().to_owned(),
            no_new_evidence_rounds: null_reexplore_analysis.no_new_evidence_rounds,
            require_new_evidence: aggregation_policy
                .null_policy
                .reexplore
                .require_new_evidence,
            missing_new_evidence_agents: null_reexplore_analysis
                .missing_new_evidence_agents
                .clone(),
            evidence_by_agent: map_sets_to_sorted_vec(&current_evidence_by_agent),
            unresolved_buckets: null_unresolved_buckets.clone(),
            null_primary_kind,
            null_reason,
        };
        let event_payload = json!({
            "run_id": run_id,
            "action": directive.signal_type,
            "path": directive.resolution_path,
            "aggregation": aggregation_overview,
            "null_resolution": {
                "primary_kind": directive.null_primary_kind,
                "reason": directive.null_reason
            },
            "reexplore": {
                "null_iteration": directive.next_iteration,
                "no_new_evidence_rounds": directive.no_new_evidence_rounds,
                "require_new_evidence": directive.require_new_evidence,
                "missing_new_evidence_agents": directive.missing_new_evidence_agents,
                "evidence_by_agent": directive.evidence_by_agent,
                "unresolved_buckets": directive.unresolved_buckets
            }
        });
        return Ok(RunSummaryOutput {
            result_payload,
            event_payload,
            action: AggregationNextAction::ReExplore(directive),
        });
    }

    let event_payload = json!({
        "run_id": run_id,
        "status": final_status,
        "counts": counts,
        "aggregation": aggregation_overview
    });
    Ok(RunSummaryOutput {
        result_payload,
        event_payload,
        action: AggregationNextAction::Finalize,
    })
}

fn load_aggregation_policy(tx: &mut Transaction<'_>, run_id: &str) -> Result<AggregationPolicy> {
    let policy = tx
        .query_opt(
            "SELECT aggregation_policy_json FROM runs WHERE run_id = $1",
            &[&run_id],
        )?
        .and_then(|row| {
            let raw: String = row.get(0);
            serde_json::from_str::<AggregationPolicy>(&raw).ok()
        })
        .unwrap_or_default();
    let mode = if policy.mode.trim().is_empty() {
        default_aggregation_mode()
    } else {
        policy.mode
    };
    Ok(AggregationPolicy {
        mode,
        quorum: policy.quorum.filter(|v| *v > 0),
        tie_policy: policy.tie_policy,
        null_policy: policy.null_policy,
    })
}

fn load_reexplore_state(
    tx: &mut Transaction<'_>,
    run_id: &str,
    event_type: &str,
) -> Result<ReExploreState> {
    let iteration = tx.query_one(
        "SELECT COUNT(1)
         FROM run_events
         WHERE run_id = $1 AND event_type = $2",
        &[&run_id, &event_type],
    )?;
    let iteration = iteration.get::<_, i64>(0).max(0) as u32;

    let mut state = ReExploreState {
        iteration,
        no_new_evidence_rounds: 0,
        evidence_by_agent: HashMap::new(),
    };
    let latest_payload = tx.query_opt(
        "SELECT payload_json
         FROM run_events
         WHERE run_id = $1 AND event_type = $2
         ORDER BY id DESC
         LIMIT 1",
        &[&run_id, &event_type],
    )?;
    let Some(row) = latest_payload else {
        return Ok(state);
    };
    let raw: String = row.get(0);
    let payload = serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| json!({}));
    state.no_new_evidence_rounds = payload
        .get("reexplore")
        .and_then(|v| v.get("no_new_evidence_rounds"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    state.evidence_by_agent = payload
        .get("reexplore")
        .and_then(|v| v.get("evidence_by_agent"))
        .and_then(Value::as_object)
        .map(|obj| {
            obj.iter()
                .map(|(agent, value)| {
                    let set = value
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<HashSet<_>>();
                    (agent.clone(), set)
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    Ok(state)
}

#[derive(Debug, Clone)]
struct ReExploreAnalysis {
    allow_reexplore: bool,
    no_new_evidence_rounds: u32,
    missing_new_evidence_agents: Vec<String>,
    eligible_agents: HashSet<String>,
}

#[derive(Debug, Clone)]
struct BucketOutcome {
    value: Option<String>,
    path: ResolutionPath,
    null_primary_kind: Option<NullPrimaryKind>,
    null_reason: Option<NullReason>,
}

fn analyze_reexplore(
    policy: &ReExplorePolicy,
    state: &ReExploreState,
    current: &HashMap<String, HashSet<String>>,
) -> ReExploreAnalysis {
    let mut eligible_agents = HashSet::new();
    let mut missing_agents = Vec::new();
    let mut any_new = state.evidence_by_agent.is_empty();
    for (agent, digests) in current {
        let has_new = state
            .evidence_by_agent
            .get(agent)
            .map(|prev| digests.iter().any(|d| !prev.contains(d)))
            .unwrap_or(!digests.is_empty());
        if has_new {
            any_new = true;
            eligible_agents.insert(agent.clone());
        } else {
            missing_agents.push(agent.clone());
        }
    }
    missing_agents.sort();
    let no_new_rounds = if any_new {
        0
    } else {
        state.no_new_evidence_rounds.saturating_add(1)
    };
    let allow_reexplore = state.iteration < policy.max_tie_iterations
        && no_new_rounds <= policy.no_new_evidence_rounds;
    ReExploreAnalysis {
        allow_reexplore,
        no_new_evidence_rounds: no_new_rounds,
        missing_new_evidence_agents: missing_agents,
        eligible_agents,
    }
}

fn map_sets_to_sorted_vec(map: &HashMap<String, HashSet<String>>) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    for (agent, set) in map {
        let mut values = set.iter().cloned().collect::<Vec<_>>();
        values.sort();
        out.insert(agent.clone(), values);
    }
    out
}

fn should_apply_tie_policy(
    policy: &AggregationPolicy,
    unresolved_kind: Option<UnresolvedKind>,
) -> bool {
    matches!(unresolved_kind, Some(UnresolvedKind::Tie))
        && policy.tie_policy.enabled_on.contains(&TieTrigger::Tie)
}

fn should_apply_null_policy(
    policy: &AggregationPolicy,
    unresolved_kind: Option<UnresolvedKind>,
) -> bool {
    match unresolved_kind {
        Some(UnresolvedKind::Empty) => policy.null_policy.enabled_on.contains(&NullTrigger::Empty),
        Some(UnresolvedKind::QuorumNull) => policy
            .null_policy
            .enabled_on
            .contains(&NullTrigger::QuorumNull),
        _ => false,
    }
}

fn classify_null_reason(
    unresolved_kind: UnresolvedKind,
    has_incomplete_or_failed_steps: bool,
) -> (NullPrimaryKind, NullReason) {
    match unresolved_kind {
        UnresolvedKind::Empty => {
            if has_incomplete_or_failed_steps {
                (
                    NullPrimaryKind::Empty,
                    NullReason::ExecutionIncompleteOrFailed,
                )
            } else {
                (NullPrimaryKind::Empty, NullReason::NoValidDecision)
            }
        }
        UnresolvedKind::QuorumNull => (
            NullPrimaryKind::QuorumNull,
            NullReason::ValidVotesBelowQuorum,
        ),
        _ => (NullPrimaryKind::Empty, NullReason::Unknown),
    }
}

fn resolve_bucket(
    run_id: &str,
    bucket: &str,
    majority: &MajorityOutcome,
    votes: &HashMap<String, u32>,
    entries: &[VoteEntry],
    has_incomplete_or_failed_steps: bool,
    policy: &AggregationPolicy,
    tie_trace: &mut Vec<Value>,
    null_trace: &mut Vec<Value>,
) -> BucketOutcome {
    if let Some(winner) = majority.winner.clone() {
        return BucketOutcome {
            value: Some(winner),
            path: ResolutionPath::Winner,
            null_primary_kind: None,
            null_reason: None,
        };
    }

    let Some(unresolved_kind) = majority.unresolved_kind else {
        return BucketOutcome {
            value: None,
            path: ResolutionPath::Null,
            null_primary_kind: Some(NullPrimaryKind::Empty),
            null_reason: Some(NullReason::Unknown),
        };
    };

    if unresolved_kind == UnresolvedKind::Tie {
        let winner =
            resolve_tie_bucket(run_id, bucket, majority, votes, entries, policy, tie_trace);
        return BucketOutcome {
            value: winner,
            path: ResolutionPath::Tie,
            null_primary_kind: None,
            null_reason: None,
        };
    }

    let (primary_kind, null_reason) =
        classify_null_reason(unresolved_kind, has_incomplete_or_failed_steps);
    let value = resolve_null_bucket(bucket, unresolved_kind, policy, null_trace);
    BucketOutcome {
        value,
        path: ResolutionPath::Null,
        null_primary_kind: Some(primary_kind),
        null_reason: Some(null_reason),
    }
}

fn resolve_tie_bucket(
    run_id: &str,
    bucket: &str,
    majority: &MajorityOutcome,
    votes: &HashMap<String, u32>,
    entries: &[VoteEntry],
    policy: &AggregationPolicy,
    trace: &mut Vec<Value>,
) -> Option<String> {
    if !should_apply_tie_policy(policy, majority.unresolved_kind) {
        return None;
    }

    let mut candidates = if !majority.candidates.is_empty() {
        majority.candidates.clone()
    } else {
        votes.keys().cloned().collect::<Vec<_>>()
    };
    candidates.sort();
    candidates.dedup();

    for mode in &policy.tie_policy.chain {
        match mode {
            TieResolverMode::ReExplore => {
                trace.push(json!({
                    "bucket": bucket,
                    "mode": "REEXPLORE",
                    "outcome": "PENDING_REEXPLORE"
                }));
            }
            TieResolverMode::ConfidenceWeighted => {
                if candidates.is_empty() {
                    continue;
                }
                if let Some(winner) = resolve_by_confidence(bucket, &candidates, entries) {
                    trace.push(json!({
                        "bucket": bucket,
                        "mode": "CONFIDENCE_WEIGHTED",
                        "winner": winner
                    }));
                    return Some(winner);
                }
                trace.push(json!({
                    "bucket": bucket,
                    "mode": "CONFIDENCE_WEIGHTED",
                    "outcome": "UNRESOLVED"
                }));
            }
            TieResolverMode::ReputationWeighted => {
                if candidates.is_empty() {
                    continue;
                }
                if let Some(winner) = resolve_by_reputation(
                    bucket,
                    &candidates,
                    entries,
                    &policy.tie_policy.reputation_weighted,
                ) {
                    trace.push(json!({
                        "bucket": bucket,
                        "mode": "REPUTATION_WEIGHTED",
                        "winner": winner
                    }));
                    return Some(winner);
                }
                trace.push(json!({
                    "bucket": bucket,
                    "mode": "REPUTATION_WEIGHTED",
                    "outcome": "UNRESOLVED"
                }));
            }
            TieResolverMode::Stochastic => {
                if candidates.is_empty() {
                    continue;
                }
                let winner = resolve_by_stochastic(run_id, bucket, &candidates);
                trace.push(json!({
                    "bucket": bucket,
                    "mode": "STOCHASTIC",
                    "winner": winner
                }));
                return Some(winner);
            }
        }
    }
    None
}

fn resolve_null_bucket(
    bucket: &str,
    unresolved_kind: UnresolvedKind,
    policy: &AggregationPolicy,
    trace: &mut Vec<Value>,
) -> Option<String> {
    if !should_apply_null_policy(policy, Some(unresolved_kind)) {
        trace.push(json!({
            "bucket": bucket,
            "mode": "NULL_POLICY",
            "outcome": "DISABLED"
        }));
        return None;
    }
    for mode in &policy.null_policy.chain {
        match mode {
            NullResolverMode::ReExplore => {
                trace.push(json!({
                    "bucket": bucket,
                    "mode": "REEXPLORE",
                    "outcome": "PENDING_REEXPLORE"
                }));
            }
            NullResolverMode::FinalizeNull => {
                trace.push(json!({
                    "bucket": bucket,
                    "mode": "FINALIZE_NULL",
                    "outcome": "FINALIZED_NULL"
                }));
                return None;
            }
        }
    }
    None
}

fn resolve_by_confidence(
    bucket: &str,
    candidates: &[String],
    entries: &[VoteEntry],
) -> Option<String> {
    let mut sums: HashMap<String, f64> = HashMap::new();
    for key in candidates {
        sums.insert(key.clone(), 0.0);
    }
    for entry in entries {
        let key = if bucket == "decision" {
            entry.decision.as_deref()
        } else {
            entry.answer.as_deref()
        };
        let Some(key) = key else {
            continue;
        };
        if !sums.contains_key(key) {
            continue;
        }
        let confidence = if bucket == "decision" {
            entry.decision_confidence
        } else {
            entry.answer_confidence
        };
        let slot = sums.get_mut(key).expect("existing confidence bucket");
        *slot += confidence;
    }
    pick_unique_max_f64(&sums)
}

fn resolve_by_reputation(
    bucket: &str,
    candidates: &[String],
    entries: &[VoteEntry],
    cfg: &super::types::ReputationWeightedPolicy,
) -> Option<String> {
    if cfg.agent_reputation_units.is_empty()
        && cfg.missing_reputation == MissingReputationPolicy::Skip
    {
        return None;
    }
    let mut sums: HashMap<String, i64> = HashMap::new();
    for key in candidates {
        sums.insert(key.clone(), 0);
    }
    for entry in entries {
        let key = if bucket == "decision" {
            entry.decision.as_deref()
        } else {
            entry.answer.as_deref()
        };
        let Some(key) = key else {
            continue;
        };
        if !sums.contains_key(key) {
            continue;
        }
        let reputation = if let Some(rep) = cfg.agent_reputation_units.get(&entry.agent_id) {
            *rep
        } else if cfg.missing_reputation == MissingReputationPolicy::Uniform {
            1
        } else {
            continue;
        };
        let slot = sums.get_mut(key).expect("existing reputation bucket");
        *slot = slot.saturating_add(reputation);
    }
    pick_unique_max_i64(&sums)
}

fn resolve_by_stochastic(run_id: &str, bucket: &str, candidates: &[String]) -> String {
    let mut sorted = candidates.to_vec();
    sorted.sort();
    let seed = format!("{}:{}:{}", run_id, bucket, sorted.join("|"));
    let digest = sha256_hex(seed.as_bytes());
    let idx = u64::from_str_radix(&digest[..16], 16).unwrap_or(0) % (sorted.len() as u64);
    sorted[idx as usize].clone()
}

fn pick_unique_max_f64(counter: &HashMap<String, f64>) -> Option<String> {
    let mut max_value = f64::MIN;
    let mut winner: Option<&String> = None;
    let mut tie = false;
    for (key, value) in counter {
        if *value > max_value {
            max_value = *value;
            winner = Some(key);
            tie = false;
        } else if (*value - max_value).abs() <= f64::EPSILON {
            tie = true;
        }
    }
    if tie {
        return None;
    }
    winner.cloned()
}

fn pick_unique_max_i64(counter: &HashMap<String, i64>) -> Option<String> {
    let mut max_value = i64::MIN;
    let mut winner: Option<&String> = None;
    let mut tie = false;
    for (key, value) in counter {
        if *value > max_value {
            max_value = *value;
            winner = Some(key);
            tie = false;
        } else if *value == max_value {
            tie = true;
        }
    }
    if tie {
        return None;
    }
    winner.cloned()
}

fn parse_json_value(raw: Option<&str>) -> Value {
    raw.and_then(|v| serde_json::from_str::<Value>(v).ok())
        .unwrap_or_else(|| json!({}))
}

fn extract_step_conclusion(step_result: &Value) -> Value {
    if let Some(candidate_output) = step_result.get("candidate_output") {
        return candidate_output.clone();
    }
    if let Some(summary) = step_result.get("result_summary") {
        if let Some(raw) = summary.as_str()
            && let Ok(parsed) = serde_json::from_str::<Value>(raw)
        {
            return parsed;
        }
        return summary.clone();
    }
    if step_result.get("decision").is_some() || step_result.get("answer").is_some() {
        return step_result.clone();
    }
    Value::Null
}

fn extract_evidence_digests(step_result: &Value, conclusion: &Value) -> HashSet<String> {
    let mut out = HashSet::new();
    for container in [step_result, conclusion] {
        if let Some(list) = container.get("evidence_digests").and_then(Value::as_array) {
            for item in list {
                if let Some(digest) = item.as_str()
                    && !digest.trim().is_empty()
                {
                    out.insert(digest.trim().to_owned());
                }
            }
        }
        if let Some(list) = container.get("evidence_refs").and_then(Value::as_array) {
            for item in list {
                if let Some(digest) = item.get("digest").and_then(Value::as_str)
                    && !digest.trim().is_empty()
                {
                    out.insert(digest.trim().to_owned());
                }
            }
        }
    }
    out
}

fn confidence_for_bucket(
    conclusion: &Value,
    cfg: &ConfidenceWeightedPolicy,
    decision_bucket: bool,
) -> f64 {
    let field = if decision_bucket {
        cfg.decision_confidence_field.as_str()
    } else {
        cfg.answer_confidence_field.as_str()
    };
    conclusion
        .get(field)
        .and_then(Value::as_f64)
        .or_else(|| {
            conclusion
                .get(&cfg.confidence_field)
                .and_then(Value::as_f64)
        })
        .unwrap_or(cfg.default_confidence)
        .clamp(0.0, 1.0)
}

fn normalize_text(raw: &str) -> Option<String> {
    let text = raw.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_uppercase())
    }
}

fn bump_vote(counter: &mut HashMap<String, u32>, key: &str) {
    *counter.entry(key.to_owned()).or_insert(0) += 1;
}

fn pick_majority(counter: &HashMap<String, u32>, quorum: Option<u32>) -> MajorityOutcome {
    if counter.is_empty() {
        return MajorityOutcome {
            winner: None,
            unresolved_kind: Some(UnresolvedKind::Empty),
            candidates: Vec::new(),
        };
    }
    let max_count = counter.values().copied().max().unwrap_or(0);
    if let Some(threshold) = quorum
        && max_count < threshold
    {
        let mut candidates = counter.keys().cloned().collect::<Vec<_>>();
        candidates.sort();
        return MajorityOutcome {
            winner: None,
            unresolved_kind: Some(UnresolvedKind::QuorumNull),
            candidates,
        };
    }
    let mut winners = counter
        .iter()
        .filter(|(_, count)| **count == max_count)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    winners.sort();
    if winners.len() > 1 {
        return MajorityOutcome {
            winner: None,
            unresolved_kind: Some(UnresolvedKind::Tie),
            candidates: winners,
        };
    }
    MajorityOutcome {
        winner: winners.into_iter().next(),
        unresolved_kind: None,
        candidates: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TieResolverMode, bump_vote, extract_step_conclusion, normalize_text, parse_json_value,
        pick_majority, resolve_by_confidence, resolve_by_reputation, resolve_by_stochastic,
    };
    use crate::run_queue::types::{MissingReputationPolicy, ReputationWeightedPolicy};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn parse_json_value_defaults_to_empty_object() {
        assert_eq!(parse_json_value(None), json!({}));
        assert_eq!(parse_json_value(Some("not-json")), json!({}));
    }

    #[test]
    fn parse_json_value_parses_valid_json() {
        assert_eq!(
            parse_json_value(Some(r#"{"x":1,"y":"ok"}"#)),
            json!({"x": 1, "y": "ok"})
        );
    }

    #[test]
    fn extract_step_conclusion_prefers_candidate_output() {
        let result = json!({
            "candidate_output": {"decision":"approve","answer":"yes"},
            "decision": "reject"
        });
        assert_eq!(
            extract_step_conclusion(&result),
            json!({"decision":"approve","answer":"yes"})
        );
    }

    #[test]
    fn extract_step_conclusion_parses_json_result_summary() {
        let result = json!({
            "result_summary": "{\"decision\":\"accept\",\"answer\":\"done\"}"
        });
        assert_eq!(
            extract_step_conclusion(&result),
            json!({"decision":"accept","answer":"done"})
        );
    }

    #[test]
    fn normalize_text_trims_and_uppercases() {
        assert_eq!(normalize_text("  hello  "), Some("HELLO".to_owned()));
        assert_eq!(normalize_text("\n\t "), None);
    }

    #[test]
    fn bump_vote_accumulates_counts() {
        let mut votes = HashMap::new();
        bump_vote(&mut votes, "ACCEPT");
        bump_vote(&mut votes, "ACCEPT");
        bump_vote(&mut votes, "REJECT");
        assert_eq!(votes.get("ACCEPT"), Some(&2));
        assert_eq!(votes.get("REJECT"), Some(&1));
    }

    #[test]
    fn pick_majority_returns_most_voted_key() {
        let mut votes = HashMap::new();
        votes.insert("APPROVE".to_owned(), 3);
        votes.insert("REJECT".to_owned(), 1);
        assert_eq!(
            pick_majority(&votes, None).winner,
            Some("APPROVE".to_owned())
        );
        assert_eq!(pick_majority(&HashMap::new(), None).winner, None);
    }

    #[test]
    fn pick_majority_returns_none_on_tie() {
        let mut votes = HashMap::new();
        votes.insert("APPROVE".to_owned(), 2);
        votes.insert("REJECT".to_owned(), 2);
        let out = pick_majority(&votes, None);
        assert_eq!(out.winner, None);
        assert_eq!(
            out.candidates,
            vec!["APPROVE".to_owned(), "REJECT".to_owned()]
        );
    }

    #[test]
    fn pick_majority_honors_quorum_threshold() {
        let mut votes = HashMap::new();
        votes.insert("APPROVE".to_owned(), 2);
        votes.insert("REJECT".to_owned(), 1);

        assert_eq!(pick_majority(&votes, Some(3)).winner, None);
        assert_eq!(
            pick_majority(&votes, Some(2)).winner,
            Some("APPROVE".to_owned())
        );
    }

    #[test]
    fn confidence_weighted_can_break_tie() {
        let entries = vec![
            super::VoteEntry {
                agent_id: "a".to_owned(),
                decision: Some("APPROVE".to_owned()),
                answer: None,
                decision_confidence: 0.9,
                answer_confidence: 0.0,
            },
            super::VoteEntry {
                agent_id: "b".to_owned(),
                decision: Some("REJECT".to_owned()),
                answer: None,
                decision_confidence: 0.4,
                answer_confidence: 0.0,
            },
        ];
        let winner = resolve_by_confidence(
            "decision",
            &["APPROVE".to_owned(), "REJECT".to_owned()],
            &entries,
        );
        assert_eq!(winner, Some("APPROVE".to_owned()));
    }

    #[test]
    fn reputation_weighted_can_break_tie() {
        let entries = vec![
            super::VoteEntry {
                agent_id: "a".to_owned(),
                decision: Some("APPROVE".to_owned()),
                answer: None,
                decision_confidence: 0.1,
                answer_confidence: 0.0,
            },
            super::VoteEntry {
                agent_id: "b".to_owned(),
                decision: Some("REJECT".to_owned()),
                answer: None,
                decision_confidence: 0.9,
                answer_confidence: 0.0,
            },
        ];
        let cfg = ReputationWeightedPolicy {
            agent_reputation_units: HashMap::from([
                ("a".to_owned(), 100_i64),
                ("b".to_owned(), 50_i64),
            ]),
            missing_reputation: MissingReputationPolicy::Skip,
        };
        let winner = resolve_by_reputation(
            "decision",
            &["APPROVE".to_owned(), "REJECT".to_owned()],
            &entries,
            &cfg,
        );
        assert_eq!(winner, Some("APPROVE".to_owned()));
    }

    #[test]
    fn stochastic_is_deterministic_for_same_inputs() {
        let candidates = vec!["A".to_owned(), "B".to_owned(), "C".to_owned()];
        let one = resolve_by_stochastic("run-1", "decision", &candidates);
        let two = resolve_by_stochastic("run-1", "decision", &candidates);
        assert_eq!(one, two);
    }

    #[test]
    fn tie_policy_mode_enum_serialization_is_stable() {
        let modes = vec![
            TieResolverMode::ReExplore,
            TieResolverMode::ConfidenceWeighted,
            TieResolverMode::ReputationWeighted,
            TieResolverMode::Stochastic,
        ];
        let raw = serde_json::to_string(&modes).expect("serialize modes");
        assert!(raw.contains("REEXPLORE"));
        assert!(raw.contains("CONFIDENCE_WEIGHTED"));
        assert!(raw.contains("REPUTATION_WEIGHTED"));
        assert!(raw.contains("STOCHASTIC"));
    }
}
