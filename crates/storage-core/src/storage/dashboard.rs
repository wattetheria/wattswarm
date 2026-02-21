use super::*;

impl SqliteStore {
    pub fn list_task_ids_recent(&self, limit: usize) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let mut stmt = conn.prepare(
            "SELECT task_id FROM task_projection
             ORDER BY epoch DESC, task_id DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| r.get::<_, String>(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn latest_candidate_for_task(&self, task_id: &str) -> Result<Option<Candidate>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        conn.query_row(
            "SELECT candidate_json FROM candidates
             WHERE task_id = ?1
             ORDER BY candidate_id DESC
             LIMIT 1",
            params![task_id],
            |r| {
                let json: String = r.get(0)?;
                let candidate: Candidate = serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                Ok(candidate)
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn count_verifier_results_for_candidate(
        &self,
        task_id: &str,
        candidate_id: &str,
    ) -> Result<u32> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let count = conn.query_row(
            "SELECT COUNT(1) FROM verifier_results
             WHERE task_id = ?1 AND candidate_id = ?2",
            params![task_id, candidate_id],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(count as u32)
    }

    pub fn has_vote_commit_for_execution(&self, task_id: &str, execution_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| SwarmError::Storage("mutex poisoned".into()))?;
        let exists = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM vote_commits
                WHERE task_id = ?1 AND execution_id = ?2
            )",
            params![task_id, execution_id],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }
}
