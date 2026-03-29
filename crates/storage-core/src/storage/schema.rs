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

fn migrate_discovered_peers_local_scope_schema(conn: &Connection) -> Result<()> {
    if column_exists(conn, "discovered_peers_local", "scope_id") {
        return Ok(());
    }

    const LEGACY_SCOPE_SEP: char = '\u{1f}';

    conn.execute_batch(
        "
        ALTER TABLE discovered_peers_local
        ADD COLUMN scope_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE discovered_peers_local
        RENAME COLUMN node_id TO legacy_node_id;
        ALTER TABLE discovered_peers_local
        ADD COLUMN node_id TEXT;
        ",
    )?;

    let mut stmt = conn.prepare(
        "SELECT legacy_node_id
         FROM discovered_peers_local",
    )?;
    let rows = stmt.query_map(params![], |r| r.get::<_, String>(0))?;
    let legacy_node_ids = rows.collect::<std::result::Result<Vec<_>, _>>()?;

    for legacy_node_id in legacy_node_ids {
        let (scope_id, node_id) = legacy_node_id
            .split_once(LEGACY_SCOPE_SEP)
            .map(|(scope, node)| (scope.to_owned(), node.to_owned()))
            .unwrap_or_else(|| ("".to_owned(), legacy_node_id.clone()));
        conn.execute(
            "UPDATE discovered_peers_local
             SET scope_id = $2,
                 node_id = $3
             WHERE legacy_node_id = $1",
            params![legacy_node_id, scope_id, node_id],
        )?;
    }

    conn.execute_batch(
        "
        ALTER TABLE discovered_peers_local
        DROP CONSTRAINT IF EXISTS discovered_peers_local_pkey;
        ALTER TABLE discovered_peers_local
        ALTER COLUMN node_id SET NOT NULL;
        ALTER TABLE discovered_peers_local
        DROP COLUMN legacy_node_id;
        ALTER TABLE discovered_peers_local
        ADD CONSTRAINT discovered_peers_local_pkey PRIMARY KEY (scope_id, node_id);
        DROP INDEX IF EXISTS idx_discovered_peers_local_updated;
        CREATE INDEX IF NOT EXISTS idx_discovered_peers_local_updated
            ON discovered_peers_local(scope_id, updated_at DESC, node_id ASC);
        ",
    )?;
    Ok(())
}

fn migrate_executor_registry_local_scope_schema(conn: &Connection) -> Result<()> {
    if column_exists(conn, "executor_registry_local", "scope_id") {
        return Ok(());
    }

    const LEGACY_SCOPE_SEP: char = '\u{1f}';

    conn.execute_batch(
        "
        ALTER TABLE executor_registry_local
        ADD COLUMN scope_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE executor_registry_local
        RENAME COLUMN executor_name TO legacy_executor_name;
        ALTER TABLE executor_registry_local
        ADD COLUMN executor_name TEXT;
        ",
    )?;

    let mut stmt = conn.prepare(
        "SELECT legacy_executor_name
         FROM executor_registry_local",
    )?;
    let rows = stmt.query_map(params![], |r| r.get::<_, String>(0))?;
    let legacy_names = rows.collect::<std::result::Result<Vec<_>, _>>()?;

    for legacy_name in legacy_names {
        let (scope_id, executor_name) = legacy_name
            .split_once(LEGACY_SCOPE_SEP)
            .map(|(scope, name)| (scope.to_owned(), name.to_owned()))
            .unwrap_or_else(|| ("".to_owned(), legacy_name.clone()));
        conn.execute(
            "UPDATE executor_registry_local
             SET scope_id = $2,
                 executor_name = $3
             WHERE legacy_executor_name = $1",
            params![legacy_name, scope_id, executor_name],
        )?;
    }

    conn.execute_batch(
        "
        ALTER TABLE executor_registry_local
        DROP CONSTRAINT IF EXISTS executor_registry_local_pkey;
        ALTER TABLE executor_registry_local
        ALTER COLUMN executor_name SET NOT NULL;
        ALTER TABLE executor_registry_local
        DROP COLUMN legacy_executor_name;
        ALTER TABLE executor_registry_local
        ADD CONSTRAINT executor_registry_local_pkey PRIMARY KEY (scope_id, executor_name);
        ",
    )?;
    Ok(())
}

fn ensure_discovered_peers_local_scope_index(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_discovered_peers_local_updated
            ON discovered_peers_local(scope_id, updated_at DESC, node_id ASC);
        ",
    )?;
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
            CREATE TABLE IF NOT EXISTS network_registry (
                network_id TEXT PRIMARY KEY,
                network_kind TEXT NOT NULL,
                parent_network_id TEXT,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                genesis_node_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS network_params (
                network_id TEXT PRIMARY KEY,
                control_mode TEXT NOT NULL,
                membership_version BIGINT NOT NULL DEFAULT 1,
                policy_version BIGINT NOT NULL DEFAULT 1,
                params_json TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS node_registry (
                node_id TEXT PRIMARY KEY,
                public_key TEXT NOT NULL UNIQUE,
                home_network_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                last_seen_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS node_network_membership (
                node_id TEXT NOT NULL,
                network_id TEXT NOT NULL,
                joined_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(node_id, network_id)
            );

            CREATE TABLE IF NOT EXISTS org_registry (
                org_id TEXT PRIMARY KEY,
                network_id TEXT NOT NULL,
                org_kind TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                is_default BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS local_config_json (
                config_key TEXT PRIMARY KEY,
                config_json TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS executor_registry_local (
                scope_id TEXT NOT NULL DEFAULT '',
                executor_name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(scope_id, executor_name)
            );

            CREATE TABLE IF NOT EXISTS discovered_peers_local (
                scope_id TEXT NOT NULL DEFAULT '',
                node_id TEXT NOT NULL,
                listen_addr TEXT,
                discovered_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(scope_id, node_id)
            );

            CREATE TABLE IF NOT EXISTS remote_task_bridge_registry_local (
                task_id TEXT NOT NULL,
                executor TEXT NOT NULL,
                profile TEXT NOT NULL,
                announcement_id TEXT NOT NULL,
                network_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                source_scope_hint TEXT NOT NULL,
                detail_ref_digest TEXT,
                candidate_id TEXT NOT NULL,
                terminal_state TEXT NOT NULL,
                bridged_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(task_id, executor, profile)
            );

            CREATE TABLE IF NOT EXISTS events (
                seq BIGSERIAL PRIMARY KEY,
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
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
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                contract_json TEXT NOT NULL,
                terminal_state TEXT NOT NULL,
                committed_candidate_id TEXT,
                finalized_candidate_id TEXT,
                retry_attempt BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(org_id, task_id)
            );

            CREATE TABLE IF NOT EXISTS leases (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                role TEXT NOT NULL,
                claimer_node_id TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                lease_until TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, task_id, role)
            );

            CREATE TABLE IF NOT EXISTS candidates (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                proposer_node_id TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                PRIMARY KEY(org_id, task_id, candidate_id)
            );

            CREATE TABLE IF NOT EXISTS verifier_results (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                verifier_node_id TEXT NOT NULL,
                result_json TEXT NOT NULL,
                passed BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(org_id, task_id, candidate_id, verifier_node_id)
            );

            CREATE TABLE IF NOT EXISTS evidence_added (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                PRIMARY KEY(org_id, task_id, candidate_id, evidence_digest)
            );

            CREATE TABLE IF NOT EXISTS evidence_available (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                verifier_node_id TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, task_id, candidate_id, verifier_node_id, evidence_digest)
            );

            CREATE TABLE IF NOT EXISTS vote_commits (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                voter_node_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL DEFAULT '',
                commit_hash TEXT NOT NULL,
                verifier_result_hash TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, task_id, voter_node_id)
            );

            CREATE TABLE IF NOT EXISTS vote_reveals (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                voter_node_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL DEFAULT '',
                vote TEXT NOT NULL,
                salt TEXT NOT NULL,
                verifier_result_hash TEXT NOT NULL,
                valid BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, task_id, voter_node_id)
            );

            CREATE TABLE IF NOT EXISTS finalizations (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                candidate_id TEXT NOT NULL,
                finality_proof_json TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY(org_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                checkpoint_id TEXT NOT NULL,
                up_to_seq BIGINT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY(org_id, checkpoint_id)
            );

            CREATE TABLE IF NOT EXISTS feed_subscriptions (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                subscriber_node_id TEXT NOT NULL,
                feed_key TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, subscriber_node_id, feed_key)
            );

            CREATE TABLE IF NOT EXISTS task_announcements (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                announcement_id TEXT NOT NULL,
                feed_key TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                detail_ref_json TEXT,
                announced_by_node_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, announcement_id)
            );

            CREATE TABLE IF NOT EXISTS topic_messages (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                message_id TEXT NOT NULL,
                network_id TEXT NOT NULL,
                feed_key TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                author_node_id TEXT NOT NULL,
                content_json TEXT NOT NULL,
                reply_to_message_id TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, message_id)
            );
            CREATE INDEX IF NOT EXISTS idx_topic_messages_feed_scope_created
                ON topic_messages(org_id, feed_key, scope_hint, created_at DESC, message_id DESC);

            CREATE TABLE IF NOT EXISTS topic_cursors (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                subscriber_node_id TEXT NOT NULL,
                feed_key TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                last_event_seq BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, subscriber_node_id, feed_key)
            );
            CREATE INDEX IF NOT EXISTS idx_topic_cursors_scope_updated
                ON topic_cursors(org_id, scope_hint, updated_at DESC);

            CREATE TABLE IF NOT EXISTS execution_set_projection (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                execution_set_id TEXT NOT NULL,
                participant_node_id TEXT NOT NULL,
                role_hint TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                status TEXT NOT NULL,
                confirmed_by_node_id TEXT,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, task_id, execution_set_id, participant_node_id)
            );

            CREATE TABLE IF NOT EXISTS network_rule_announcements (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                scope_key TEXT NOT NULL,
                rule_set TEXT NOT NULL,
                rule_version BIGINT NOT NULL,
                activation_epoch BIGINT,
                observed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, scope_key, rule_set, rule_version)
            );

            CREATE TABLE IF NOT EXISTS network_checkpoint_announcements (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                scope_key TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, scope_key, checkpoint_id)
            );

            CREATE TABLE IF NOT EXISTS membership_projection (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                singleton BIGINT NOT NULL DEFAULT 1 CHECK (singleton = 1),
                membership_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS decision_memory (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
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
                PRIMARY KEY(org_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS evidence_summary (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                cid TEXT NOT NULL,
                mime TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                source_hint_digest TEXT NOT NULL,
                added_at TIMESTAMPTZ NOT NULL,
                availability_confirmations_count BIGINT NOT NULL,
                PRIMARY KEY(org_id, cid)
            );

            CREATE TABLE IF NOT EXISTS runtime_metrics (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
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
                PRIMARY KEY(org_id, runtime_id, profile_id, task_type, window_start, window_end)
            );

            CREATE TABLE IF NOT EXISTS task_settlement (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                finalized_at TIMESTAMPTZ NOT NULL,
                window_end_at TIMESTAMPTZ NOT NULL,
                bad_feedback_exists BOOLEAN NOT NULL DEFAULT FALSE,
                bad_feedback_at TIMESTAMPTZ,
                implicit_settled BOOLEAN NOT NULL DEFAULT FALSE,
                implicit_settled_at TIMESTAMPTZ,
                PRIMARY KEY(org_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS task_stage_usage (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                explore_used BIGINT NOT NULL DEFAULT 0,
                verify_used BIGINT NOT NULL DEFAULT 0,
                finalize_used BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(org_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS task_cost_reports (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                cost_units_by_stage_json TEXT NOT NULL,
                latency_by_stage_json TEXT NOT NULL,
                evidence_fetch_bytes BIGINT NOT NULL DEFAULT 0,
                events_emitted_count BIGINT NOT NULL DEFAULT 0,
                cache_hit_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
                PRIMARY KEY(org_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS reputation_state (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                runtime_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                stability_reputation BIGINT NOT NULL,
                quality_reputation BIGINT NOT NULL,
                last_updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(org_id, runtime_id, profile_id)
            );

            CREATE TABLE IF NOT EXISTS knowledge_lookups (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
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
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT NOT NULL,
                epoch BIGINT NOT NULL,
                candidate_hash TEXT NOT NULL,
                PRIMARY KEY(org_id, task_id, epoch, candidate_hash)
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
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                task_id TEXT,
                unknown_reason_code BIGINT NOT NULL,
                peer_protocol_version TEXT NOT NULL,
                local_protocol_version TEXT NOT NULL,
                author_node_id TEXT NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_revocations (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                event_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                revoked_by_node_id TEXT NOT NULL,
                revoked_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS summary_revocations (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                summary_id TEXT NOT NULL,
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
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
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
                PRIMARY KEY(org_id, summary_id, task_id, epoch)
            );

            CREATE TABLE IF NOT EXISTS imported_reputation_state (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                summary_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                runtime_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                stability_reputation BIGINT NOT NULL,
                quality_reputation BIGINT NOT NULL,
                last_updated_at TIMESTAMPTZ NOT NULL,
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(org_id, summary_id, runtime_id, profile_id)
            );

            CREATE TABLE IF NOT EXISTS imported_task_outcomes (
                org_id TEXT NOT NULL DEFAULT '__unset_org__',
                summary_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                scope_hint TEXT NOT NULL,
                task_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                output_digest TEXT NOT NULL,
                result_summary_json TEXT NOT NULL,
                evidence_digest_count BIGINT NOT NULL DEFAULT 0,
                checkpoint_id TEXT NOT NULL,
                proof_artifact_path TEXT NOT NULL,
                finalized_at TIMESTAMPTZ NOT NULL,
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY(org_id, summary_id, task_id)
            );
            ",
            )?;

            // Backward-compatible migration for pre-existing local db files.
            // (table, column, ALTER TABLE statement)
            let migrations: &[(&str, &str, &str)] = &[
                (
                    "events",
                    "org_id",
                    "ALTER TABLE events ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "task_projection",
                    "org_id",
                    "ALTER TABLE task_projection ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "leases",
                    "org_id",
                    "ALTER TABLE leases ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "candidates",
                    "org_id",
                    "ALTER TABLE candidates ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "verifier_results",
                    "org_id",
                    "ALTER TABLE verifier_results ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "evidence_added",
                    "org_id",
                    "ALTER TABLE evidence_added ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "evidence_available",
                    "org_id",
                    "ALTER TABLE evidence_available ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "vote_commits",
                    "org_id",
                    "ALTER TABLE vote_commits ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "vote_reveals",
                    "org_id",
                    "ALTER TABLE vote_reveals ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "finalizations",
                    "org_id",
                    "ALTER TABLE finalizations ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "checkpoints",
                    "org_id",
                    "ALTER TABLE checkpoints ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "decision_memory",
                    "org_id",
                    "ALTER TABLE decision_memory ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "evidence_summary",
                    "org_id",
                    "ALTER TABLE evidence_summary ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "runtime_metrics",
                    "org_id",
                    "ALTER TABLE runtime_metrics ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "task_settlement",
                    "org_id",
                    "ALTER TABLE task_settlement ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "task_stage_usage",
                    "org_id",
                    "ALTER TABLE task_stage_usage ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "task_cost_reports",
                    "org_id",
                    "ALTER TABLE task_cost_reports ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "reputation_state",
                    "org_id",
                    "ALTER TABLE reputation_state ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "knowledge_lookups",
                    "org_id",
                    "ALTER TABLE knowledge_lookups ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "reuse_blacklist",
                    "org_id",
                    "ALTER TABLE reuse_blacklist ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "unknown_reason_observations",
                    "org_id",
                    "ALTER TABLE unknown_reason_observations ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "membership_projection",
                    "org_id",
                    "ALTER TABLE membership_projection ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "event_revocations",
                    "org_id",
                    "ALTER TABLE event_revocations ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "summary_revocations",
                    "org_id",
                    "ALTER TABLE summary_revocations ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "imported_decision_memory",
                    "org_id",
                    "ALTER TABLE imported_decision_memory ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "imported_reputation_state",
                    "org_id",
                    "ALTER TABLE imported_reputation_state ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
                (
                    "imported_task_outcomes",
                    "org_id",
                    "ALTER TABLE imported_task_outcomes ADD COLUMN org_id TEXT NOT NULL DEFAULT '__unset_org__'",
                ),
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
                ("network_registry", "created_at"),
                ("network_params", "created_at"),
                ("network_params", "updated_at"),
                ("node_registry", "created_at"),
                ("node_registry", "last_seen_at"),
                ("node_network_membership", "joined_at"),
                ("org_registry", "created_at"),
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
                ("event_revocations", "revoked_at"),
                ("summary_revocations", "revoked_at"),
                ("imported_decision_memory", "finalized_at"),
                ("imported_reputation_state", "last_updated_at"),
                ("imported_task_outcomes", "finalized_at"),
            ];
            for (table, column) in timestamp_columns {
                ensure_timestamp_column(&conn, table, column)?;
            }

            for table in [
                "events",
                "task_projection",
                "leases",
                "candidates",
                "verifier_results",
                "evidence_added",
                "evidence_available",
                "vote_commits",
                "vote_reveals",
                "finalizations",
                "checkpoints",
                "decision_memory",
                "evidence_summary",
                "runtime_metrics",
                "task_settlement",
                "task_stage_usage",
                "task_cost_reports",
                "reputation_state",
                "knowledge_lookups",
                "reuse_blacklist",
                "unknown_reason_observations",
                "membership_projection",
                "event_revocations",
                "summary_revocations",
                "imported_decision_memory",
                "imported_reputation_state",
                "imported_task_outcomes",
            ] {
                conn.execute(
                    &format!("ALTER TABLE {table} ALTER COLUMN org_id SET DEFAULT '__unset_org__'"),
                    params![],
                )?;
            }

            conn.execute_batch(
                "
                ALTER TABLE task_projection DROP CONSTRAINT IF EXISTS task_projection_pkey;
                ALTER TABLE task_projection ADD CONSTRAINT task_projection_pkey PRIMARY KEY (org_id, task_id);

                ALTER TABLE leases DROP CONSTRAINT IF EXISTS leases_pkey;
                ALTER TABLE leases ADD CONSTRAINT leases_pkey PRIMARY KEY (org_id, task_id, role);

                ALTER TABLE candidates DROP CONSTRAINT IF EXISTS candidates_pkey;
                ALTER TABLE candidates ADD CONSTRAINT candidates_pkey PRIMARY KEY (org_id, task_id, candidate_id);

                ALTER TABLE verifier_results DROP CONSTRAINT IF EXISTS verifier_results_pkey;
                ALTER TABLE verifier_results ADD CONSTRAINT verifier_results_pkey PRIMARY KEY (org_id, task_id, candidate_id, verifier_node_id);

                ALTER TABLE evidence_added DROP CONSTRAINT IF EXISTS evidence_added_pkey;
                ALTER TABLE evidence_added ADD CONSTRAINT evidence_added_pkey PRIMARY KEY (org_id, task_id, candidate_id, evidence_digest);

                ALTER TABLE evidence_available DROP CONSTRAINT IF EXISTS evidence_available_pkey;
                ALTER TABLE evidence_available ADD CONSTRAINT evidence_available_pkey PRIMARY KEY (org_id, task_id, candidate_id, verifier_node_id, evidence_digest);

                ALTER TABLE vote_commits DROP CONSTRAINT IF EXISTS vote_commits_pkey;
                ALTER TABLE vote_commits ADD CONSTRAINT vote_commits_pkey PRIMARY KEY (org_id, task_id, voter_node_id);

                ALTER TABLE vote_reveals DROP CONSTRAINT IF EXISTS vote_reveals_pkey;
                ALTER TABLE vote_reveals ADD CONSTRAINT vote_reveals_pkey PRIMARY KEY (org_id, task_id, voter_node_id);

                ALTER TABLE finalizations DROP CONSTRAINT IF EXISTS finalizations_pkey;
                ALTER TABLE finalizations ADD CONSTRAINT finalizations_pkey PRIMARY KEY (org_id, task_id, epoch);

                ALTER TABLE checkpoints DROP CONSTRAINT IF EXISTS checkpoints_pkey;
                ALTER TABLE checkpoints ADD CONSTRAINT checkpoints_pkey PRIMARY KEY (org_id, checkpoint_id);

                ALTER TABLE decision_memory DROP CONSTRAINT IF EXISTS decision_memory_pkey;
                ALTER TABLE decision_memory ADD CONSTRAINT decision_memory_pkey PRIMARY KEY (org_id, task_id, epoch);

                ALTER TABLE evidence_summary DROP CONSTRAINT IF EXISTS evidence_summary_pkey;
                ALTER TABLE evidence_summary ADD CONSTRAINT evidence_summary_pkey PRIMARY KEY (org_id, cid);

                ALTER TABLE runtime_metrics DROP CONSTRAINT IF EXISTS runtime_metrics_pkey;
                ALTER TABLE runtime_metrics ADD CONSTRAINT runtime_metrics_pkey PRIMARY KEY (org_id, runtime_id, profile_id, task_type, window_start, window_end);

                ALTER TABLE task_settlement DROP CONSTRAINT IF EXISTS task_settlement_pkey;
                ALTER TABLE task_settlement ADD CONSTRAINT task_settlement_pkey PRIMARY KEY (org_id, task_id, epoch);

                ALTER TABLE task_stage_usage DROP CONSTRAINT IF EXISTS task_stage_usage_pkey;
                ALTER TABLE task_stage_usage ADD CONSTRAINT task_stage_usage_pkey PRIMARY KEY (org_id, task_id, epoch);

                ALTER TABLE task_cost_reports DROP CONSTRAINT IF EXISTS task_cost_reports_pkey;
                ALTER TABLE task_cost_reports ADD CONSTRAINT task_cost_reports_pkey PRIMARY KEY (org_id, task_id, epoch);

                ALTER TABLE reputation_state DROP CONSTRAINT IF EXISTS reputation_state_pkey;
                ALTER TABLE reputation_state ADD CONSTRAINT reputation_state_pkey PRIMARY KEY (org_id, runtime_id, profile_id);

                ALTER TABLE reuse_blacklist DROP CONSTRAINT IF EXISTS reuse_blacklist_pkey;
                ALTER TABLE reuse_blacklist ADD CONSTRAINT reuse_blacklist_pkey PRIMARY KEY (org_id, task_id, epoch, candidate_hash);

                ALTER TABLE membership_projection DROP CONSTRAINT IF EXISTS membership_projection_pkey;
                ALTER TABLE membership_projection ADD CONSTRAINT membership_projection_pkey PRIMARY KEY (org_id, singleton);

                ALTER TABLE event_revocations DROP CONSTRAINT IF EXISTS event_revocations_pkey;
                ALTER TABLE event_revocations ADD CONSTRAINT event_revocations_pkey PRIMARY KEY (org_id, event_id);

                ALTER TABLE summary_revocations DROP CONSTRAINT IF EXISTS summary_revocations_pkey;
                ALTER TABLE summary_revocations ADD CONSTRAINT summary_revocations_pkey PRIMARY KEY (org_id, summary_id);

                ALTER TABLE imported_decision_memory DROP CONSTRAINT IF EXISTS imported_decision_memory_pkey;
                ALTER TABLE imported_decision_memory ADD CONSTRAINT imported_decision_memory_pkey PRIMARY KEY (org_id, summary_id, task_id, epoch);

                ALTER TABLE imported_reputation_state DROP CONSTRAINT IF EXISTS imported_reputation_state_pkey;
                ALTER TABLE imported_reputation_state ADD CONSTRAINT imported_reputation_state_pkey PRIMARY KEY (org_id, summary_id, runtime_id, profile_id);

                ALTER TABLE imported_task_outcomes DROP CONSTRAINT IF EXISTS imported_task_outcomes_pkey;
                ALTER TABLE imported_task_outcomes ADD CONSTRAINT imported_task_outcomes_pkey PRIMARY KEY (org_id, summary_id, task_id);
                ",
            )?;

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
            migrate_executor_registry_local_scope_schema(&conn)?;
            migrate_discovered_peers_local_scope_schema(&conn)?;
            ensure_discovered_peers_local_scope_index(&conn)?;
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
            org_id: Arc::new(UNSET_ORG_ID.to_owned()),
        })
    }
}
