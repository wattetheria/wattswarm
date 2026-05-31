use super::*;

mod candidates_votes;
mod decision_memory;
mod execution_governance;
mod settlement_reuse;
mod tasks;

impl PgStore {
    fn canonical_scope_hint_or_original(raw: String) -> String {
        wattswarm_protocol::types::normalized_scope_hint(&raw)
    }

    fn decode_gossip_kinds_json(raw: String) -> Vec<String> {
        serde_json::from_str(&raw).unwrap_or_default()
    }

    fn decode_topic_provider_capabilities_json(
        raw: Option<String>,
    ) -> Option<TopicProviderCapabilities> {
        raw.and_then(|value| serde_json::from_str(&value).ok())
    }
}

fn parse_decision_memory_row(r: &pg::Row) -> std::result::Result<DecisionMemoryHitRow, pg::Error> {
    let summary_raw: String = r.get(5)?;
    let summary = serde_json::from_str(&summary_raw).unwrap_or_else(|_| serde_json::json!({}));
    let quorum_result_raw: String = r.get(6)?;
    let quorum_result =
        serde_json::from_str(&quorum_result_raw).unwrap_or_else(|_| serde_json::json!({}));
    let reason_codes_raw: String = r.get(7)?;
    let reason_codes = serde_json::from_str(&reason_codes_raw).unwrap_or_default();
    let reason_details_raw: String = r.get(8)?;
    let reason_details =
        serde_json::from_str(&reason_details_raw).unwrap_or_else(|_| serde_json::json!({}));
    Ok(DecisionMemoryHitRow {
        task_id: r.get(0)?,
        epoch: r.get::<_, i64>(1)? as u64,
        final_commit_hash: r.get(2)?,
        winning_candidate_hash: r.get(3)?,
        output_digest: r.get(4)?,
        result_summary: summary,
        quorum_result,
        reason_codes,
        reason_details,
        policy_snapshot_digest: r.get(9)?,
        input_digest: r.get(10)?,
        output_schema_digest: r.get(11)?,
        policy_id: r.get(12)?,
        task_type: r.get(13)?,
        policy_params_digest: r.get(14)?,
        deprecated_as_exact: r.get::<_, bool>(15)?,
        finalized_at: r.get::<_, i64>(16)? as u64,
        confidence_hint: 0.5,
    })
}
