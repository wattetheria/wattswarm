mod support;

#[test]
fn anti_entropy_syncs_missed_event_without_live_publish() {
    support::network_bridge::anti_entropy_syncs_missed_event_without_live_publish();
}

#[test]
fn anti_entropy_uses_scope_specific_cursor_for_recovery() {
    support::network_bridge::anti_entropy_uses_scope_specific_cursor_for_recovery();
}

#[test]
fn reconnect_recovers_missing_events_after_partition_like_disconnect() {
    support::network_bridge::reconnect_recovers_missing_events_after_partition_like_disconnect();
}
