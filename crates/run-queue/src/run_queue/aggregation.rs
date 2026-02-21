use anyhow::Result;
use postgres::Transaction;
use serde_json::{Value, json};
use std::collections::HashMap;

use super::types::{RunStepCounts, default_aggregation_mode};

pub(crate) fn build_run_summary_tx(
    tx: &mut Transaction<'_>,
    run_id: &str,
    final_status: &str,
    counts: &RunStepCounts,
) -> Result<(Value, Value)> {
    let aggregation_mode = tx
        .query_opt(
            "SELECT aggregation_policy_json FROM runs WHERE run_id = $1",
            &[&run_id],
        )?
        .and_then(|row| {
            let raw: String = row.get(0);
            serde_json::from_str::<Value>(&raw).ok()
        })
        .and_then(|policy| {
            policy
                .get("mode")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(default_aggregation_mode);

    let step_rows = tx.query(
        "SELECT step_id, agent_id, executor, profile, status, attempt, task_id, result_json, error_text
         FROM run_steps
         WHERE run_id = $1
         ORDER BY priority DESC, step_id ASC",
        &[&run_id],
    )?;

    let mut decision_votes: HashMap<String, u32> = HashMap::new();
    let mut answer_votes: HashMap<String, u32> = HashMap::new();
    let mut steps = Vec::with_capacity(step_rows.len());

    for row in step_rows {
        let result_raw: Option<String> = row.get(7);
        let result = parse_json_value(result_raw.as_deref());
        let conclusion = extract_step_conclusion(&result);

        if let Some(decision) = conclusion
            .get("decision")
            .and_then(Value::as_str)
            .and_then(normalize_text)
        {
            bump_vote(&mut decision_votes, &decision);
        }
        if let Some(answer) = conclusion
            .get("answer")
            .and_then(Value::as_str)
            .and_then(normalize_text)
        {
            bump_vote(&mut answer_votes, &answer);
        }

        steps.push(json!({
            "step_id": row.get::<_, String>(0),
            "agent_id": row.get::<_, String>(1),
            "executor": row.get::<_, String>(2),
            "profile": row.get::<_, String>(3),
            "status": row.get::<_, String>(4),
            "attempt": row.get::<_, i32>(5),
            "task_id": row.get::<_, Option<String>>(6),
            "error": row.get::<_, Option<String>>(8),
            "result": result,
            "conclusion": conclusion
        }));
    }

    let final_decision = pick_majority(&decision_votes);
    let final_answer = pick_majority(&answer_votes);
    let aggregation_overview = json!({
        "mode": aggregation_mode,
        "final_decision": final_decision.clone(),
        "final_answer": final_answer.clone(),
        "decision_votes": decision_votes,
        "answer_votes": answer_votes
    });
    let result_payload = json!({
        "run_id": run_id,
        "status": final_status,
        "counts": counts,
        "aggregation": {
            "mode": aggregation_mode,
            "final_decision": final_decision,
            "final_answer": final_answer,
            "decision_votes": aggregation_overview["decision_votes"].clone(),
            "answer_votes": aggregation_overview["answer_votes"].clone(),
            "steps": steps
        }
    });
    let final_event_payload = json!({
        "run_id": run_id,
        "status": final_status,
        "counts": counts,
        "aggregation": aggregation_overview
    });
    Ok((result_payload, final_event_payload))
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
        if let Some(raw) = summary.as_str() {
            if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
                return parsed;
            }
        }
        return summary.clone();
    }
    if step_result.get("decision").is_some() || step_result.get("answer").is_some() {
        return step_result.clone();
    }
    Value::Null
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

fn pick_majority(counter: &HashMap<String, u32>) -> Option<String> {
    let mut pairs: Vec<(&String, &u32)> = counter.iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    pairs.first().map(|(key, _)| (*key).clone())
}

#[cfg(test)]
mod tests {
    use super::{
        bump_vote, extract_step_conclusion, normalize_text, parse_json_value, pick_majority,
    };
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
    fn extract_step_conclusion_falls_back_to_raw_summary_or_null() {
        let plain_summary = json!({"result_summary":"summary text"});
        assert_eq!(
            extract_step_conclusion(&plain_summary),
            json!("summary text")
        );

        let bare = json!({"decision":"accept","answer":"ok"});
        assert_eq!(extract_step_conclusion(&bare), bare);

        let empty = json!({"x":1});
        assert_eq!(extract_step_conclusion(&empty), serde_json::Value::Null);
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
        assert_eq!(pick_majority(&votes), Some("APPROVE".to_owned()));
        assert_eq!(pick_majority(&HashMap::new()), None);
    }
}
