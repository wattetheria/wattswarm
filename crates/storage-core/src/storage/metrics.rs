use super::*;

impl PgStore {
    pub fn get_advisory_state(&self, advisory_id: &str) -> Result<Option<AdvisoryStateRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT advisory_id, policy_id, suggested_policy_hash, status,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at,
                    approved_by,
                    CASE WHEN approved_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM approved_at) * 1000)::BIGINT END AS approved_at,
                    applied_policy_hash,
                    CASE WHEN applied_at IS NULL THEN NULL ELSE (EXTRACT(EPOCH FROM applied_at) * 1000)::BIGINT END AS applied_at
             FROM advisory_state WHERE advisory_id = ?1",
            params![advisory_id],
            |r| {
                Ok(AdvisoryStateRow {
                    advisory_id: r.get(0)?,
                    policy_id: r.get(1)?,
                    suggested_policy_hash: r.get(2)?,
                    status: r.get(3)?,
                    created_at: r.get::<_, i64>(4)? as u64,
                    approved_by: r.get(5)?,
                    approved_at: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                    applied_policy_hash: r.get(7)?,
                    applied_at: r.get::<_, Option<i64>>(8)?.map(|v| v as u64),
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn put_advisory_created(
        &self,
        advisory_id: &str,
        policy_id: &str,
        suggested_policy_hash: &str,
        created_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO advisory_state(advisory_id, policy_id, suggested_policy_hash, status, created_at)
             VALUES (?1, ?2, ?3, 'created', TIMESTAMPTZ 'epoch' + (?4::bigint * INTERVAL '1 millisecond'))",
            params![
                advisory_id,
                policy_id,
                suggested_policy_hash,
                created_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn mark_advisory_approved(
        &self,
        advisory_id: &str,
        admin_node_id: &str,
        approved_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE advisory_state
             SET status = 'approved',
                 approved_by = ?2,
                 approved_at = TIMESTAMPTZ 'epoch' + (?3::bigint * INTERVAL '1 millisecond')
             WHERE advisory_id = ?1",
            params![advisory_id, admin_node_id, approved_at as i64],
        )?;
        Ok(())
    }

    pub fn mark_advisory_applied(
        &self,
        advisory_id: &str,
        applied_policy_hash: &str,
        applied_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "UPDATE advisory_state
             SET status = 'applied',
                 applied_policy_hash = ?2,
                 applied_at = TIMESTAMPTZ 'epoch' + (?3::bigint * INTERVAL '1 millisecond')
             WHERE advisory_id = ?1",
            params![advisory_id, applied_policy_hash, applied_at as i64],
        )?;
        Ok(())
    }

    pub fn record_unknown_reason_observation(
        &self,
        task_id: Option<&str>,
        unknown_reason_code: u16,
        peer_protocol_version: &str,
        local_protocol_version: &str,
        author_node_id: &str,
        observed_at: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.execute(
            "INSERT INTO unknown_reason_observations(task_id, unknown_reason_code, peer_protocol_version, local_protocol_version, author_node_id, observed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, TIMESTAMPTZ 'epoch' + (?6::bigint * INTERVAL '1 millisecond'))",
            params![
                task_id,
                unknown_reason_code as i64,
                peer_protocol_version,
                local_protocol_version,
                author_node_id,
                observed_at as i64
            ],
        )?;
        Ok(())
    }

    pub fn upsert_runtime_metric_observation(
        &self,
        obs: &RuntimeMetricObservation<'_>,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let existing = conn
            .query_row(
                "SELECT sample_count, finalize_count, timeout_count, crash_count, invalid_output_count,
                        latency_samples_json, reject_reason_distribution, cost_units
                 FROM runtime_metrics
                 WHERE runtime_id = ?1 AND profile_id = ?2 AND task_type = ?3
                   AND window_start = TIMESTAMPTZ 'epoch' + (?4::bigint * INTERVAL '1 millisecond')
                   AND window_end = TIMESTAMPTZ 'epoch' + (?5::bigint * INTERVAL '1 millisecond')",
                params![
                    obs.runtime_id,
                    obs.profile_id,
                    obs.task_type,
                    obs.window_start as i64,
                    obs.window_end as i64
                ],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;

        let (
            mut sample_count,
            mut finalize_count,
            mut timeout_count,
            mut crash_count,
            mut invalid_output_count,
            mut latency_samples,
            mut reject_dist,
            mut total_cost_units,
        ) = match existing {
            Some((a, b, c, d, e, latency_json, reject_json, cost_units)) => (
                a,
                b,
                c,
                d,
                e,
                serde_json::from_str::<Vec<u64>>(&latency_json).unwrap_or_default(),
                serde_json::from_str::<BTreeMap<String, u64>>(&reject_json).unwrap_or_default(),
                cost_units,
            ),
            None => (0, 0, 0, 0, 0, Vec::new(), BTreeMap::new(), 0),
        };

        sample_count += 1;
        if obs.finalized {
            finalize_count += 1;
        }
        if obs.timeout {
            timeout_count += 1;
        }
        if obs.crash {
            crash_count += 1;
        }
        if obs.invalid_output {
            invalid_output_count += 1;
        }

        if obs.latency_ms > 0 {
            latency_samples.push(obs.latency_ms);
            if latency_samples.len() > 101 {
                let drop_n = latency_samples.len() - 101;
                latency_samples.drain(0..drop_n);
            }
        }
        for code in obs.reject_reason_codes {
            let key = code.to_string();
            *reject_dist.entry(key).or_insert(0) += 1;
        }
        total_cost_units = total_cost_units.saturating_add(obs.cost_units as i64);

        let median_latency_ms = median_u64(&latency_samples).unwrap_or(0) as i64;
        let denom = sample_count.max(1) as f64;
        let finalize_rate = finalize_count as f64 / denom;
        let timeout_rate = timeout_count as f64 / denom;
        let crash_rate = crash_count as f64 / denom;
        let invalid_output_rate = invalid_output_count as f64 / denom;
        let expired_rate = (1.0 - finalize_rate).clamp(0.0, 1.0);
        let finalized_non_zero = finalize_count.max(1) as f64;
        let cost_units_per_finalized = total_cost_units as f64 / finalized_non_zero;
        let verify_cost_ratio = if total_cost_units == 0 {
            0.0
        } else {
            (timeout_count as f64 + invalid_output_count as f64) / denom
        };
        let time_to_finality_p50 = median_latency_ms;
        let time_to_finality_p95 = median_latency_ms.saturating_mul(2);
        let reuse_candidate_accept_rate = finalize_rate;
        let da_fetch_fail_rate = timeout_rate;

        conn.execute(
            "INSERT INTO runtime_metrics(
                runtime_id, profile_id, task_type, window_start, window_end,
                finalize_rate, timeout_rate, crash_rate, invalid_output_rate,
                median_latency_ms, cost_units, reject_reason_distribution,
                sample_count, finalize_count, timeout_count, crash_count, invalid_output_count,
                latency_samples_json,
                reuse_hit_rate_exact, reuse_hit_rate_similar, reuse_candidate_accept_rate,
                time_to_finality_p50, time_to_finality_p95, expired_rate,
                cost_units_per_finalized_task_p50, cost_units_per_finalized_task_p95,
                verify_cost_ratio, invalid_event_reject_count, fork_prevented_count, da_fetch_fail_rate
             ) VALUES (
                ?1, ?2, ?3,
                TIMESTAMPTZ 'epoch' + (?4::bigint * INTERVAL '1 millisecond'),
                TIMESTAMPTZ 'epoch' + (?5::bigint * INTERVAL '1 millisecond'),
                ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18,
                ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30
             )
             ON CONFLICT(runtime_id, profile_id, task_type, window_start, window_end) DO UPDATE SET
                finalize_rate = excluded.finalize_rate,
                timeout_rate = excluded.timeout_rate,
                crash_rate = excluded.crash_rate,
                invalid_output_rate = excluded.invalid_output_rate,
                median_latency_ms = excluded.median_latency_ms,
                cost_units = excluded.cost_units,
                reject_reason_distribution = excluded.reject_reason_distribution,
                sample_count = excluded.sample_count,
                finalize_count = excluded.finalize_count,
                timeout_count = excluded.timeout_count,
                crash_count = excluded.crash_count,
                invalid_output_count = excluded.invalid_output_count,
                latency_samples_json = excluded.latency_samples_json,
                reuse_hit_rate_exact = excluded.reuse_hit_rate_exact,
                reuse_hit_rate_similar = excluded.reuse_hit_rate_similar,
                reuse_candidate_accept_rate = excluded.reuse_candidate_accept_rate,
                time_to_finality_p50 = excluded.time_to_finality_p50,
                time_to_finality_p95 = excluded.time_to_finality_p95,
                expired_rate = excluded.expired_rate,
                cost_units_per_finalized_task_p50 = excluded.cost_units_per_finalized_task_p50,
                cost_units_per_finalized_task_p95 = excluded.cost_units_per_finalized_task_p95,
                verify_cost_ratio = excluded.verify_cost_ratio,
                invalid_event_reject_count = excluded.invalid_event_reject_count,
                fork_prevented_count = excluded.fork_prevented_count,
                da_fetch_fail_rate = excluded.da_fetch_fail_rate",
            params![
                obs.runtime_id,
                obs.profile_id,
                obs.task_type,
                obs.window_start as i64,
                obs.window_end as i64,
                finalize_rate,
                timeout_rate,
                crash_rate,
                invalid_output_rate,
                median_latency_ms,
                total_cost_units,
                serde_json::to_string(&reject_dist)?,
                sample_count,
                finalize_count,
                timeout_count,
                crash_count,
                invalid_output_count,
                serde_json::to_string(&latency_samples)?,
                0.0_f64,
                0.0_f64,
                reuse_candidate_accept_rate,
                time_to_finality_p50,
                time_to_finality_p95,
                expired_rate,
                cost_units_per_finalized,
                cost_units_per_finalized,
                verify_cost_ratio,
                0_i64,
                0_i64,
                da_fetch_fail_rate
            ],
        )?;
        Ok(())
    }

    pub fn adjust_reputation(
        &self,
        runtime_id: &str,
        profile_id: &str,
        stability_delta_units: i64,
        quality_delta_units: i64,
        now: u64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let existing = conn
            .query_row(
                "SELECT stability_reputation, quality_reputation FROM reputation_state
                 WHERE runtime_id = ?1 AND profile_id = ?2",
                params![runtime_id, profile_id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;
        let (stability, quality) = match existing {
            Some((s, q)) => (s, q),
            None => (0, 0),
        };
        conn.execute(
            "INSERT INTO reputation_state(runtime_id, profile_id, stability_reputation, quality_reputation, last_updated_at)
             VALUES (?1, ?2, ?3, ?4, TIMESTAMPTZ 'epoch' + (?5::bigint * INTERVAL '1 millisecond'))
             ON CONFLICT(runtime_id, profile_id) DO UPDATE SET
               stability_reputation = excluded.stability_reputation,
               quality_reputation = excluded.quality_reputation,
               last_updated_at = excluded.last_updated_at",
            params![
                runtime_id,
                profile_id,
                stability.saturating_add(stability_delta_units),
                quality.saturating_add(quality_delta_units),
                now as i64
            ],
        )?;
        Ok(())
    }
}
