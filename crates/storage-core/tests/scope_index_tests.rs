//! Tests for the `events.swarm_scope` index column and scope-filtered reads
//! (`load_scope_events_before` / `load_scope_events_page`) that decouple backfill
//! from a full event-log scan.

use wattswarm_storage_core::PgStore;
use wattswarm_storage_core::types::{Event, EventPayload, TaskExpiredPayload};

fn open_store() -> PgStore {
    PgStore::open_in_memory()
        .expect("open store")
        .for_org("local:test-scope-index:bootstrap")
}

fn scoped_event(event_id: &str, created_at: u64, swarm_scope: &str) -> Event {
    let payload = EventPayload::TaskExpired(TaskExpiredPayload {
        task_id: format!("task-{event_id}"),
    });
    Event {
        event_id: event_id.to_owned(),
        protocol_version: "0.1.0".to_owned(),
        event_kind: payload.kind(),
        task_id: payload.task_id().map(ToOwned::to_owned),
        swarm_scope: swarm_scope.to_owned(),
        epoch: 1,
        author_node_id: "node-local".to_owned(),
        created_at,
        payload,
        signature_hex: "deadbeef".to_owned(),
    }
}

#[test]
fn scope_reads_return_only_matching_scope_in_order() {
    let store = open_store();
    store
        .append_event(&scoped_event("g1", 10, "group:alpha"))
        .expect("g1");
    store
        .append_event(&scoped_event("b1", 11, "group:beta"))
        .expect("b1");
    store
        .append_event(&scoped_event("g2", 12, "group:alpha"))
        .expect("g2");
    store
        .append_event(&scoped_event("glob", 13, "global"))
        .expect("glob");

    // Oldest-first page returns only group:alpha events.
    let page: Vec<String> = store
        .load_scope_events_page("group:alpha", 0, 10)
        .expect("page")
        .into_iter()
        .map(|(_, e)| e.event_id)
        .collect();
    assert_eq!(page, vec!["g1".to_owned(), "g2".to_owned()]);

    // Newest-first recent returns only group:alpha events.
    let recent: Vec<String> = store
        .load_scope_events_before("group:alpha", None, 10)
        .expect("recent")
        .into_iter()
        .map(|(_, e)| e.event_id)
        .collect();
    assert_eq!(recent, vec!["g2".to_owned(), "g1".to_owned()]);

    // Other scopes stay isolated.
    let beta = store
        .load_scope_events_page("group:beta", 0, 10)
        .expect("beta");
    assert_eq!(beta.len(), 1);
    assert_eq!(beta[0].1.event_id, "b1");

    let global = store
        .load_scope_events_page("global", 0, 10)
        .expect("global");
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].1.event_id, "glob");
}

#[test]
fn scope_page_respects_cursor_and_limit() {
    let store = open_store();
    for i in 0..5 {
        store
            .append_event(&scoped_event(
                &format!("a{i}"),
                100 + i as u64,
                "group:alpha",
            ))
            .expect("append");
    }

    let first = store
        .load_scope_events_page("group:alpha", 0, 2)
        .expect("first");
    let first_ids: Vec<&str> = first.iter().map(|(_, e)| e.event_id.as_str()).collect();
    assert_eq!(first_ids, vec!["a0", "a1"]);

    let cursor = first.last().expect("cursor").0;
    let second: Vec<String> = store
        .load_scope_events_page("group:alpha", cursor, 2)
        .expect("second")
        .into_iter()
        .map(|(_, e)| e.event_id)
        .collect();
    assert_eq!(second, vec!["a2".to_owned(), "a3".to_owned()]);
}

#[test]
fn non_canonical_scope_is_canonicalized_for_lookup() {
    let store = open_store();
    // Prefix-fallback parse: "group:alpha/thread" -> Group("alpha") -> "group:alpha".
    store
        .append_event(&scoped_event("p1", 10, "group:alpha/thread"))
        .expect("p1");
    let page = store
        .load_scope_events_page("group:alpha", 0, 10)
        .expect("page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].1.event_id, "p1");
}

#[test]
fn unparseable_scope_is_excluded_from_scope_reads() {
    let store = open_store();
    store
        .append_event(&scoped_event("u1", 10, "not-a-scope"))
        .expect("u1");
    // Unparseable scope -> NULL column -> excluded from scope reads, but the row
    // still exists in the global log.
    assert!(
        store
            .load_scope_events_page("not-a-scope", 0, 10)
            .expect("page")
            .is_empty()
    );
    assert!(
        store
            .load_scope_events_before("not-a-scope", None, 10)
            .expect("recent")
            .is_empty()
    );
    assert_eq!(store.load_all_events().expect("all").len(), 1);
}
