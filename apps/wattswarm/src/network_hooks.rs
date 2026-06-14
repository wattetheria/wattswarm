use std::path::Path;

pub(crate) fn run_background_post_tick(node: &mut crate::control::node::Node, state_dir: &Path) {
    let _ = crate::run_queue::network_bridge::process_pending_bridge_tasks(node, state_dir);
    let _ = crate::run_queue::network_bridge::process_pending_run_queue_results(state_dir);
    let _ = crate::run_queue::network_bridge::process_pending_stigmergy_contributions(state_dir);
    let _ = crate::run_queue::network_bridge::evaluate_open_stigmergy_rounds_for_state(state_dir);
    let _ = crate::control::topic_interpretation::process_topic_interpretation(node, state_dir);
    let _ = crate::control::topic_consensus::process_structured_topic_consensus(node, state_dir);
}
