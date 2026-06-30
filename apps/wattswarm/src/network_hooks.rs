use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

const SLOW_POST_TICK_INTERVAL: Duration = Duration::from_secs(5);

static SLOW_POST_TICK_DUE: OnceLock<Mutex<HashMap<PathBuf, Instant>>> = OnceLock::new();

fn slow_post_tick_due(state_dir: &Path, now: Instant) -> bool {
    let Ok(mut due_by_state_dir) = SLOW_POST_TICK_DUE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
    else {
        return true;
    };
    let next_due = due_by_state_dir
        .entry(state_dir.to_path_buf())
        .or_insert(now);
    if now < *next_due {
        return false;
    }
    *next_due = now + SLOW_POST_TICK_INTERVAL;
    true
}

pub(crate) fn run_background_post_tick(node: &mut crate::control::node::Node, state_dir: &Path) {
    let _ = crate::run_queue::network_bridge::process_pending_bridge_tasks(node, state_dir);
    let _ = crate::run_queue::network_bridge::process_pending_run_queue_results(state_dir);
    let _ = crate::run_queue::network_bridge::process_pending_stigmergy_contributions(state_dir);
    if !slow_post_tick_due(state_dir, Instant::now()) {
        return;
    }
    let _ = crate::run_queue::network_bridge::evaluate_open_stigmergy_rounds_for_state(state_dir);
    let _ = crate::control::topic_interpretation::process_topic_interpretation(node, state_dir);
    let _ = crate::control::topic_consensus::process_structured_topic_consensus(node, state_dir);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_post_tick_due_is_scoped_by_state_dir_and_interval() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let state_dir = PathBuf::from(format!("/tmp/wattswarm-post-tick-test-{}", unique));
        let other_state_dir =
            PathBuf::from(format!("/tmp/wattswarm-post-tick-test-{}-other", unique));
        let now = Instant::now();

        assert!(slow_post_tick_due(&state_dir, now));
        assert!(!slow_post_tick_due(
            &state_dir,
            now + Duration::from_secs(1)
        ));
        assert!(slow_post_tick_due(&other_state_dir, now));
        assert!(slow_post_tick_due(
            &state_dir,
            now + SLOW_POST_TICK_INTERVAL + Duration::from_millis(1)
        ));
    }
}
