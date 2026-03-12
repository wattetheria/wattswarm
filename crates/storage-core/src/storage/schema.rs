use super::*;

fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
    conn.query_row(
        "SELECT 1
         FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = $1
           AND column_name = $2
         LIMIT 1",
        params![table, column],
        |_| Ok(1_i64),
    )
    .optional()
    .map(|v| v.is_some())
    .unwrap_or(false)
}

fn column_data_type(conn: &Connection, table: &str, column: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT data_type
         FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = $1
           AND column_name = $2
         LIMIT 1",
        params![table, column],
        |r| r.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn ensure_timestamp_column(conn: &Connection, table: &str, column: &str) -> Result<()> {
    let Some(data_type) = column_data_type(conn, table, column)? else {
        return Ok(());
    };
    if data_type == "timestamp with time zone" {
        return Ok(());
    }
    if data_type != "bigint" {
        return Err(SwarmError::Storage(format!(
            "unsupported time column type {}.{}: {}",
            table, column, data_type
        ))
        .into());
    }
    let sql = format!(
        "ALTER TABLE {table}
         ALTER COLUMN {column}
         TYPE TIMESTAMPTZ
         USING TIMESTAMPTZ 'epoch' + ({column} * INTERVAL '1 millisecond')"
    );
    conn.execute_batch(&sql)?;
    Ok(())
}

fn ensure_boolean_column(
    conn: &Connection,
    table: &str,
    column: &str,
    default_value: Option<&str>,
) -> Result<()> {
    let Some(data_type) = column_data_type(conn, table, column)? else {
        return Ok(());
    };
    if data_type == "boolean" {
        return Ok(());
    }
    if data_type != "bigint" && data_type != "integer" && data_type != "smallint" {
        return Err(SwarmError::Storage(format!(
            "unsupported bool column type {}.{}: {}",
            table, column, data_type
        ))
        .into());
    }
    let mut sql = format!(
        "ALTER TABLE {table}
         ALTER COLUMN {column}
         DROP DEFAULT;
         ALTER TABLE {table}
         ALTER COLUMN {column}
         TYPE BOOLEAN
         USING ({column} <> 0)"
    );
    if let Some(default_value) = default_value {
        sql.push_str(&format!(
            ";
             ALTER TABLE {table}
             ALTER COLUMN {column}
             SET DEFAULT {default_value}"
        ));
    }
    conn.execute_batch(&sql)?;
    Ok(())
}

impl PgStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::initialize(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::initialize(conn)
    }

    fn initialize(conn: Connection) -> Result<Self> {
        const INIT_LOCK_KEY: i64 = 0x7773_696e_6974; // "wsinit"

        conn.query_row(
            "SELECT pg_advisory_lock($1)",
            params![INIT_LOCK_KEY],
            |_| Ok(()),
        )?;

        let init_result: Result<()> = (|| {
            conn.execute_batch(
                "
            CREATE TABLE IF NOT EXISTS events (
                seq BIGSERIAL PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                protocol_version TEXT NOT NULL DEFAULT '0.1.0',
                task_id TEXT,
                epoch BIGINT NOT NULL,
                event_kind TEXT NOT NULL,
                author_node_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                event_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_projection (
                task_id TEXT PRIMARY KEY,
                epoch BIGINT NOT NULL,
                contract_json TEXT NOT NULL,
                terminal_state TEXT NOT NULL,
                committed_candidate_id TEXT,
                finalized_candidate_id TEXT,
                retry_attempt BIGINT NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS leases (
                task_id TEXT NOT NULL,
                role TEXT NOT NULL,
                claimer_node_id TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                lease_until TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(task_id, role)
            );

            CREATE TABLE IF NOT EXISTS candidates (
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                proposer_node_id TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                PRIMARY KEY(task_id, candidate_id)
            );

            CREATE TABLE IF NOT EXISTS verifier_results (
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                verifier_node_id TEXT NOT NULL,
                result_json TEXT NOT NULL,
                passed BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(task_id, candidate_id, verifier_node_id)
            );

            CREATE TABLE IF NOT EXISTS evidence_added (
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                PRIMARY KEY(task_id, candidate_id, evidence_digest)
            );

            CREATE TABLE IF NOT EXISTS evidence_available (
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                verifier_node_id TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(task_id, candidate_id, verifier_node_id, evidence_digest)
            );

            CREATE TABLE IF NOT EXISTS vote_commits (
                task_id TEXT NOT NULL,
                voter_node_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL DEFAULT '',
                commit_hash TEXT NOT NULL,
                verifier_result_hash TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(task_id, voter_node_id)
            );

            CREATE TABLE IF NOT EXISTS vote_reveals (
                task_id TEXT NOT NULL,
                voter_node_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL DEFAULT '',
                vote TEXT NOT NULL,
                salt TEXT NOT NULL,
                verifier_result_hash TEXT NOT NULL,
                valid BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(task_id, voter_node_id)
            );

            CREATE TABLE IF NOT EXISTS finalizations (
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                candidate_id TEXT NOT NULL,
                finality_proof_json TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY(task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                up_to_seq BIGINT NOT NULL,
                event_id TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS membership_projection (
                singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
                membership_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS decision_memory (
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                final_commit_hash TEXT NOT NULL,
                finalized_at TIMESTAMPTZ NOT NULL,
                winning_candidate_hash TEXT NOT NULL,
                output_digest TEXT NOT NULL,
                result_summary TEXT NOT NULL,
                quorum_result_json TEXT NOT NULL DEFAULT '{}',
                reason_codes_json TEXT NOT NULL,
                reason_details TEXT NOT NULL DEFAULT '{}',
                policy_snapshot_digest TEXT NOT NULL,
                task_type TEXT NOT NULL DEFAULT '',
                input_digest TEXT NOT NULL DEFAULT '',
                output_schema_digest TEXT NOT NULL DEFAULT '',
                policy_id TEXT NOT NULL DEFAULT '',
                policy_params_digest TEXT NOT NULL DEFAULT '',
                deprecated_as_exact BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS evidence_summary (
                cid TEXT PRIMARY KEY,
                mime TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                source_hint_digest TEXT NOT NULL,
                added_at TIMESTAMPTZ NOT NULL,
                availability_confirmations_count BIGINT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_metrics (
                runtime_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                finalize_rate DOUBLE PRECISION NOT NULL,
                timeout_rate DOUBLE PRECISION NOT NULL,
                crash_rate DOUBLE PRECISION NOT NULL,
                invalid_output_rate DOUBLE PRECISION NOT NULL,
                median_latency_ms BIGINT NOT NULL,
                cost_units BIGINT NOT NULL,
                reject_reason_distribution TEXT NOT NULL,
                sample_count BIGINT NOT NULL DEFAULT 0,
                finalize_count BIGINT NOT NULL DEFAULT 0,
                timeout_count BIGINT NOT NULL DEFAULT 0,
                crash_count BIGINT NOT NULL DEFAULT 0,
                invalid_output_count BIGINT NOT NULL DEFAULT 0,
                latency_samples_json TEXT NOT NULL DEFAULT '[]',
                reuse_hit_rate_exact DOUBLE PRECISION NOT NULL DEFAULT 0,
                reuse_hit_rate_similar DOUBLE PRECISION NOT NULL DEFAULT 0,
                reuse_candidate_accept_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
                time_to_finality_p50 BIGINT NOT NULL DEFAULT 0,
                time_to_finality_p95 BIGINT NOT NULL DEFAULT 0,
                expired_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
                cost_units_per_finalized_task_p50 DOUBLE PRECISION NOT NULL DEFAULT 0,
                cost_units_per_finalized_task_p95 DOUBLE PRECISION NOT NULL DEFAULT 0,
                verify_cost_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
                invalid_event_reject_count BIGINT NOT NULL DEFAULT 0,
                fork_prevented_count BIGINT NOT NULL DEFAULT 0,
                da_fetch_fail_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
                PRIMARY KEY(runtime_id, profile_id, task_type, window_start, window_end)
            );

            CREATE TABLE IF NOT EXISTS task_settlement (
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                finalized_at TIMESTAMPTZ NOT NULL,
                window_end_at TIMESTAMPTZ NOT NULL,
                bad_feedback_exists BOOLEAN NOT NULL DEFAULT FALSE,
                bad_feedback_at TIMESTAMPTZ,
                implicit_settled BOOLEAN NOT NULL DEFAULT FALSE,
                implicit_settled_at TIMESTAMPTZ,
                PRIMARY KEY(task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS task_stage_usage (
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                explore_used BIGINT NOT NULL DEFAULT 0,
                verify_used BIGINT NOT NULL DEFAULT 0,
                finalize_used BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS task_cost_reports (
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                cost_units_by_stage_json TEXT NOT NULL,
                latency_by_stage_json TEXT NOT NULL,
                evidence_fetch_bytes BIGINT NOT NULL DEFAULT 0,
                events_emitted_count BIGINT NOT NULL DEFAULT 0,
                cache_hit_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
                PRIMARY KEY(task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS reputation_state (
                runtime_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                stability_reputation BIGINT NOT NULL,
                quality_reputation BIGINT NOT NULL,
                last_updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(runtime_id, profile_id)
            );

            CREATE TABLE IF NOT EXISTS knowledge_lookups (
                task_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                lookup_time TIMESTAMPTZ NOT NULL,
                hit_count BIGINT NOT NULL,
                exact_hit_count BIGINT NOT NULL DEFAULT 0,
                similar_hit_count BIGINT NOT NULL DEFAULT 0,
                hits_digest TEXT NOT NULL,
                reuse_applied BOOLEAN NOT NULL DEFAULT FALSE
            );

            CREATE TABLE IF NOT EXISTS reuse_blacklist (
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                candidate_hash TEXT NOT NULL,
                PRIMARY KEY(task_id, epoch, candidate_hash)
            );

            CREATE TABLE IF NOT EXISTS advisory_state (
                advisory_id TEXT PRIMARY KEY,
                policy_id TEXT NOT NULL,
                suggested_policy_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                approved_by TEXT,
                approved_at TIMESTAMPTZ,
                applied_policy_hash TEXT,
                applied_at TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS unknown_reason_observations (
                id BIGSERIAL PRIMARY KEY,
                task_id TEXT,
                unknown_reason_code BIGINT NOT NULL,
                peer_protocol_version TEXT NOT NULL,
                local_protocol_version TEXT NOT NULL,
                author_node_id TEXT NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_revocations (
                event_id TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                revoked_by_node_id TEXT NOT NULL,
                revoked_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS summary_revocations (
                summary_id TEXT PRIMARY KEY,
                summary_kind TEXT NOT NULL,
                reason TEXT NOT NULL,
                revoked_by_node_id TEXT NOT NULL,
                revoked_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS penalized_nodes (
                node_id TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                block_summaries BOOLEAN NOT NULL DEFAULT TRUE,
                penalized_by_node_id TEXT NOT NULL,
                penalized_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS imported_decision_memory (
                summary_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                final_commit_hash TEXT NOT NULL,
                finalized_at TIMESTAMPTZ NOT NULL,
                winning_candidate_hash TEXT NOT NULL,
                output_digest TEXT NOT NULL,
                result_summary TEXT NOT NULL,
                quorum_result_json TEXT NOT NULL DEFAULT '{}',
                reason_codes_json TEXT NOT NULL DEFAULT '[]',
                reason_details TEXT NOT NULL DEFAULT '{}',
                policy_snapshot_digest TEXT NOT NULL DEFAULT '',
                task_type TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                output_schema_digest TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                policy_params_digest TEXT NOT NULL,
                deprecated_as_exact BOOLEAN NOT NULL DEFAULT FALSE,
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(summary_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS imported_reputation_state (
                summary_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                runtime_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                stability_reputation BIGINT NOT NULL,
                quality_reputation BIGINT NOT NULL,
                last_updated_at TIMESTAMPTZ NOT NULL,
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(summary_id, runtime_id, profile_id)
            );
            ",
            )?;

            // Backward-compatible migration for pre-existing local db files.
            // (table, column, ALTER TABLE statement)
            let migrations: &[(&str, &str, &str)] = &[
                (
                    "task_projection",
                    "retry_attempt",
                    "ALTER TABLE task_projection ADD COLUMN retry_attempt BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "events",
                    "protocol_version",
                    "ALTER TABLE events ADD COLUMN protocol_version TEXT NOT NULL DEFAULT '0.1.0'",
                ),
                (
                    "candidates",
                    "candidate_hash",
                    "ALTER TABLE candidates ADD COLUMN candidate_hash TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "vote_commits",
                    "candidate_hash",
                    "ALTER TABLE vote_commits ADD COLUMN candidate_hash TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "vote_reveals",
                    "candidate_hash",
                    "ALTER TABLE vote_reveals ADD COLUMN candidate_hash TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "decision_memory",
                    "task_type",
                    "ALTER TABLE decision_memory ADD COLUMN task_type TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "decision_memory",
                    "input_digest",
                    "ALTER TABLE decision_memory ADD COLUMN input_digest TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "decision_memory",
                    "output_schema_digest",
                    "ALTER TABLE decision_memory ADD COLUMN output_schema_digest TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "decision_memory",
                    "policy_id",
                    "ALTER TABLE decision_memory ADD COLUMN policy_id TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "decision_memory",
                    "policy_params_digest",
                    "ALTER TABLE decision_memory ADD COLUMN policy_params_digest TEXT NOT NULL DEFAULT ''",
                ),
                (
                    "decision_memory",
                    "deprecated_as_exact",
                    "ALTER TABLE decision_memory ADD COLUMN deprecated_as_exact BOOLEAN NOT NULL DEFAULT FALSE",
                ),
                (
                    "decision_memory",
                    "quorum_result_json",
                    "ALTER TABLE decision_memory ADD COLUMN quorum_result_json TEXT NOT NULL DEFAULT '{}'",
                ),
                (
                    "decision_memory",
                    "reason_details",
                    "ALTER TABLE decision_memory ADD COLUMN reason_details TEXT NOT NULL DEFAULT '{}'",
                ),
                (
                    "runtime_metrics",
                    "sample_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN sample_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "finalize_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN finalize_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "timeout_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN timeout_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "crash_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN crash_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "invalid_output_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN invalid_output_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "latency_samples_json",
                    "ALTER TABLE runtime_metrics ADD COLUMN latency_samples_json TEXT NOT NULL DEFAULT '[]'",
                ),
                (
                    "runtime_metrics",
                    "reuse_hit_rate_exact",
                    "ALTER TABLE runtime_metrics ADD COLUMN reuse_hit_rate_exact DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "reuse_hit_rate_similar",
                    "ALTER TABLE runtime_metrics ADD COLUMN reuse_hit_rate_similar DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "reuse_candidate_accept_rate",
                    "ALTER TABLE runtime_metrics ADD COLUMN reuse_candidate_accept_rate DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "time_to_finality_p50",
                    "ALTER TABLE runtime_metrics ADD COLUMN time_to_finality_p50 BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "time_to_finality_p95",
                    "ALTER TABLE runtime_metrics ADD COLUMN time_to_finality_p95 BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "expired_rate",
                    "ALTER TABLE runtime_metrics ADD COLUMN expired_rate DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "cost_units_per_finalized_task_p50",
                    "ALTER TABLE runtime_metrics ADD COLUMN cost_units_per_finalized_task_p50 DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "cost_units_per_finalized_task_p95",
                    "ALTER TABLE runtime_metrics ADD COLUMN cost_units_per_finalized_task_p95 DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "verify_cost_ratio",
                    "ALTER TABLE runtime_metrics ADD COLUMN verify_cost_ratio DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "invalid_event_reject_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN invalid_event_reject_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "fork_prevented_count",
                    "ALTER TABLE runtime_metrics ADD COLUMN fork_prevented_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "runtime_metrics",
                    "da_fetch_fail_rate",
                    "ALTER TABLE runtime_metrics ADD COLUMN da_fetch_fail_rate DOUBLE PRECISION NOT NULL DEFAULT 0",
                ),
                (
                    "task_settlement",
                    "bad_feedback_exists",
                    "ALTER TABLE task_settlement ADD COLUMN bad_feedback_exists BOOLEAN NOT NULL DEFAULT FALSE",
                ),
                (
                    "task_settlement",
                    "implicit_settled",
                    "ALTER TABLE task_settlement ADD COLUMN implicit_settled BOOLEAN NOT NULL DEFAULT FALSE",
                ),
                (
                    "task_settlement",
                    "implicit_settled_at",
                    "ALTER TABLE task_settlement ADD COLUMN implicit_settled_at TIMESTAMPTZ",
                ),
                (
                    "verifier_results",
                    "passed",
                    "ALTER TABLE verifier_results ADD COLUMN passed BOOLEAN NOT NULL DEFAULT FALSE",
                ),
                (
                    "knowledge_lookups",
                    "exact_hit_count",
                    "ALTER TABLE knowledge_lookups ADD COLUMN exact_hit_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "knowledge_lookups",
                    "similar_hit_count",
                    "ALTER TABLE knowledge_lookups ADD COLUMN similar_hit_count BIGINT NOT NULL DEFAULT 0",
                ),
                (
                    "knowledge_lookups",
                    "reuse_applied",
                    "ALTER TABLE knowledge_lookups ADD COLUMN reuse_applied BOOLEAN NOT NULL DEFAULT FALSE",
                ),
            ];
            for (table, column, alter_stmt) in migrations {
                if !column_exists(&conn, table, column) {
                    conn.execute(alter_stmt, params![])?;
                }
            }

            let timestamp_columns: &[(&str, &str)] = &[
                ("events", "created_at"),
                ("leases", "lease_until"),
                ("evidence_available", "created_at"),
                ("vote_commits", "created_at"),
                ("vote_reveals", "created_at"),
                ("decision_memory", "finalized_at"),
                ("evidence_summary", "added_at"),
                ("runtime_metrics", "window_start"),
                ("runtime_metrics", "window_end"),
                ("task_settlement", "finalized_at"),
                ("task_settlement", "window_end_at"),
                ("task_settlement", "bad_feedback_at"),
                ("task_settlement", "implicit_settled_at"),
                ("reputation_state", "last_updated_at"),
                ("knowledge_lookups", "lookup_time"),
                ("advisory_state", "created_at"),
                ("advisory_state", "approved_at"),
                ("advisory_state", "applied_at"),
                ("unknown_reason_observations", "observed_at"),
            ];
            for (table, column) in timestamp_columns {
                ensure_timestamp_column(&conn, table, column)?;
            }

            ensure_boolean_column(&conn, "vote_reveals", "valid", None)?;
            ensure_boolean_column(&conn, "verifier_results", "passed", Some("FALSE"))?;
            ensure_boolean_column(
                &conn,
                "decision_memory",
                "deprecated_as_exact",
                Some("FALSE"),
            )?;
            ensure_boolean_column(
                &conn,
                "task_settlement",
                "bad_feedback_exists",
                Some("FALSE"),
            )?;
            ensure_boolean_column(&conn, "task_settlement", "implicit_settled", Some("FALSE"))?;
            ensure_boolean_column(&conn, "knowledge_lookups", "reuse_applied", Some("FALSE"))?;
            Ok(())
        })();

        let unlock_result = conn.query_row(
            "SELECT pg_advisory_unlock($1)",
            params![INIT_LOCK_KEY],
            |_| Ok(()),
        );

        init_result?;
        unlock_result?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}
