mod support;

use std::sync::{Mutex, MutexGuard, OnceLock};

fn network_bridge_local_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn two_nodes_sync_global_control_event_over_iroh() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::two_nodes_sync_global_control_event_over_iroh();
}

#[test]
fn global_publish_excludes_task_and_process_firehose() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::global_publish_excludes_task_and_process_firehose();
}

#[test]
fn two_nodes_backfill_missing_events_over_request_response() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::two_nodes_backfill_missing_events_over_request_response();
}

#[test]
fn two_nodes_execute_peer_relationship_request_and_accept_over_network() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::two_nodes_execute_peer_relationship_request_and_accept_over_network();
}

#[test]
fn two_nodes_execute_peer_relationship_request_and_block_over_network() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::two_nodes_execute_peer_relationship_request_and_block_over_network();
}

#[test]
fn two_nodes_sync_topic_message_content_over_iroh() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::two_nodes_sync_topic_message_content_over_iroh();
}

#[test]
fn region_scoped_backfill_only_reaches_region_subscribers() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::region_scoped_backfill_only_reaches_region_subscribers();
}

#[test]
fn node_scoped_live_sync_only_reaches_matching_node_scope() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::node_scoped_live_sync_only_reaches_matching_node_scope();
}

#[test]
fn group_scoped_live_sync_only_reaches_matching_group_scope() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::group_scoped_live_sync_only_reaches_matching_group_scope();
}

#[test]
fn group_subscribed_middle_peer_relays_group_events() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::group_subscribed_middle_peer_relays_group_events();
}

#[test]
fn subnet_nodes_sync_and_mainnet_overlay_stays_isolated() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::subnet_nodes_sync_and_mainnet_overlay_stays_isolated();
}

#[test]
fn summary_gossip_imports_knowledge_and_reputation_into_remote_store() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::summary_gossip_imports_knowledge_and_reputation_into_remote_store();
}

#[test]
fn revoked_event_propagates_and_removes_remote_projection_state() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::revoked_event_propagates_and_removes_remote_projection_state();
}

#[test]
fn revoked_summary_event_removes_remote_imported_state() {
    let _guard = network_bridge_local_test_guard();
    support::network_bridge::revoked_summary_event_removes_remote_imported_state();
}
