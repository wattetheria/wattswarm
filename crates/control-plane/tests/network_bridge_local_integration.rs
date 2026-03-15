mod support;

#[test]
fn two_nodes_sync_global_event_over_libp2p() {
    support::network_bridge::two_nodes_sync_global_event_over_libp2p();
}

#[test]
fn global_task_detail_sync_excludes_process_firehose() {
    support::network_bridge::global_task_detail_sync_excludes_process_firehose();
}

#[test]
fn two_nodes_backfill_missing_events_over_request_response() {
    support::network_bridge::two_nodes_backfill_missing_events_over_request_response();
}

#[test]
fn region_scoped_backfill_only_reaches_region_subscribers() {
    support::network_bridge::region_scoped_backfill_only_reaches_region_subscribers();
}

#[test]
fn node_scoped_live_sync_only_reaches_matching_node_scope() {
    support::network_bridge::node_scoped_live_sync_only_reaches_matching_node_scope();
}

#[test]
fn group_scoped_live_sync_only_reaches_matching_group_scope() {
    support::network_bridge::group_scoped_live_sync_only_reaches_matching_group_scope();
}

#[test]
fn subnet_nodes_sync_and_mainnet_overlay_stays_isolated() {
    support::network_bridge::subnet_nodes_sync_and_mainnet_overlay_stays_isolated();
}

#[test]
fn summary_gossip_imports_knowledge_and_reputation_into_remote_store() {
    support::network_bridge::summary_gossip_imports_knowledge_and_reputation_into_remote_store();
}

#[test]
fn revoked_event_propagates_and_removes_remote_projection_state() {
    support::network_bridge::revoked_event_propagates_and_removes_remote_projection_state();
}

#[test]
fn revoked_summary_event_removes_remote_imported_state() {
    support::network_bridge::revoked_summary_event_removes_remote_imported_state();
}
