use wattswarm_storage_core::types::NetworkKind;
use wattswarm_storage_core::types::{
    Acceptance, ArtifactRef, Assignment, Budget, BudgetMode, Candidate, CheckpointCreatedPayload,
    ClaimPolicy, DecisionCommittedPayload, Event, EventPayload, EvidencePolicy, ExploreAssignment,
    ExploreStopPolicy, FeedbackCapabilityPolicy, FinalizeAssignment, MaxConcurrency, PolicyBinding,
    SettlementBadPenalty, SettlementDiminishingReturns, SettlementPolicy, TaskContract,
    TaskExpiredPayload, TaskMode, VerificationStatus, VerifierResult, VerifyAssignment, VoteChoice,
    VotePolicy,
};
use wattswarm_storage_core::{AdvisoryStateRow, ImportedTaskOutcomeRow, PgStore, ProjectionScope};

fn open_test_store() -> PgStore {
    PgStore::open_in_memory()
        .expect("open store")
        .for_org("local:test-storage:bootstrap")
}

#[test]
fn network_substrate_reads_canonicalize_scope_hints() {
    let store = open_test_store();
    store
        .upsert_feed_subscription("node-a", "feed-1", " local:lab-9 ", true, 100)
        .expect("upsert subscription");

    let subscription = store
        .get_feed_subscription("node-a", "feed-1")
        .expect("load subscription")
        .expect("subscription exists");
    assert_eq!(subscription.scope_hint, "node:lab-9");
    assert_eq!(
        subscription.scope(),
        Some(ProjectionScope::Node("lab-9".to_owned()))
    );

    let hints = store
        .list_active_feed_subscription_scope_hints("node-a")
        .expect("load hints");
    assert_eq!(hints, vec!["node:lab-9".to_owned()]);

    let scopes = store
        .list_active_feed_subscription_scopes("node-a")
        .expect("load scopes");
    assert_eq!(scopes, vec![ProjectionScope::Node("lab-9".to_owned())]);

    store
        .upsert_feed_subscription("node-a", "feed-group", " group:crew-7 ", true, 101)
        .expect("upsert group subscription");
    let group_subscription = store
        .get_feed_subscription("node-a", "feed-group")
        .expect("load group subscription")
        .expect("group subscription exists");
    assert_eq!(group_subscription.scope_hint, "group:crew-7");
    assert_eq!(
        group_subscription.scope(),
        Some(ProjectionScope::Group("crew-7".to_owned()))
    );
}

#[test]
fn task_announcement_detail_fetch_returns_latest_announcement_and_task_contract() {
    let store = open_test_store();
    let contract = sample_contract("task-announced-detail", "region:sol-1:swarm");
    store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    store
        .put_task_announcement(
            &contract.task_id,
            "announce-1",
            "feed-market",
            "region:sol-1",
            &serde_json::json!({"headline":"discovery"}),
            Some(&ArtifactRef {
                uri: "ipfs://detail-ref".to_owned(),
                digest: "digest-detail-ref".to_owned(),
                size_bytes: 256,
                mime: "application/json".to_owned(),
                created_at: 100,
                producer: "node-a".to_owned(),
            }),
            "node-a",
            100,
        )
        .expect("put task announcement");

    let detail = store
        .get_task_announcement_detail_for_task(&contract.task_id)
        .expect("load announcement detail")
        .expect("announcement detail exists");

    assert_eq!(detail.task_id(), contract.task_id);
    assert_eq!(
        detail.announcement.scope(),
        Some(ProjectionScope::Region("sol-1".to_owned()))
    );
    assert_eq!(
        detail.contract.as_ref().expect("task contract").task_type,
        contract.task_type
    );
    assert_eq!(
        detail.detail_ref().expect("detail ref").digest,
        "digest-detail-ref"
    );
}

#[test]
fn topic_message_roundtrip_reads_scope_and_content() {
    let store = open_test_store();
    store
        .put_topic_message(
            "msg-1",
            "default",
            "crew.chat",
            " group:crew-7 ",
            "node-a",
            &serde_json::json!({"text":"hello crew"}),
            None,
            123,
        )
        .expect("put topic message");
    store
        .put_topic_message(
            "msg-2",
            "default",
            "crew.chat",
            "group:crew-7",
            "node-b",
            &serde_json::json!({"text":"reply"}),
            Some("msg-1"),
            124,
        )
        .expect("put reply topic message");

    let messages = store
        .list_topic_messages("crew.chat", " group:crew-7 ", 10)
        .expect("list topic messages");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].message_id, "msg-2");
    assert_eq!(messages[0].reply_to_message_id.as_deref(), Some("msg-1"));
    assert_eq!(
        messages[0].scope(),
        Some(ProjectionScope::Group("crew-7".to_owned()))
    );
    assert_eq!(messages[1].content["text"], serde_json::json!("hello crew"));
}

#[test]
fn topic_cursor_roundtrip_advances_only_forward() {
    let store = open_test_store();
    store
        .upsert_topic_cursor("node-a", "crew.chat", " group:crew-7 ", 11, 200)
        .expect("put topic cursor");
    store
        .upsert_topic_cursor("node-a", "crew.chat", "group:crew-7", 7, 201)
        .expect("attempt backwards cursor");

    let cursor = store
        .get_topic_cursor("node-a", "crew.chat")
        .expect("get topic cursor")
        .expect("cursor exists");
    assert_eq!(cursor.last_event_seq, 11);
    assert_eq!(
        cursor.scope(),
        Some(ProjectionScope::Group("crew-7".to_owned()))
    );
}

#[test]
fn topic_message_pagination_uses_created_at_and_message_id_anchor() {
    let store = open_test_store();
    store
        .put_topic_message(
            "msg-1",
            "default",
            "crew.chat",
            "group:crew-7",
            "node-a",
            &serde_json::json!({"text":"first"}),
            None,
            124,
        )
        .expect("put first topic message");
    store
        .put_topic_message(
            "msg-2",
            "default",
            "crew.chat",
            "group:crew-7",
            "node-b",
            &serde_json::json!({"text":"second"}),
            None,
            124,
        )
        .expect("put second topic message");
    store
        .put_topic_message(
            "msg-3",
            "default",
            "crew.chat",
            "group:crew-7",
            "node-c",
            &serde_json::json!({"text":"third"}),
            None,
            125,
        )
        .expect("put third topic message");

    let page = store
        .list_topic_messages_page("crew.chat", "group:crew-7", Some(125), Some("msg-3"), 10)
        .expect("load paged topic messages");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].message_id, "msg-2");
    assert_eq!(page[1].message_id, "msg-1");

    let tie_page = store
        .list_topic_messages_page("crew.chat", "group:crew-7", Some(124), Some("msg-2"), 10)
        .expect("load tie-broken topic messages");
    assert_eq!(tie_page.len(), 1);
    assert_eq!(tie_page[0].message_id, "msg-1");
}

#[test]
fn evidence_reference_reads_return_added_artifacts() {
    let store = open_test_store();
    let reference = ArtifactRef {
        uri: "ipfs://evidence-ref".to_owned(),
        digest: "sha256:evidence-ref".to_owned(),
        size_bytes: 512,
        mime: "application/json".to_owned(),
        created_at: 123,
        producer: "node-a".to_owned(),
    };
    store
        .put_evidence_added("task-evidence", "cand-1", &reference)
        .expect("put evidence");

    let loaded = store
        .get_evidence_reference("task-evidence", "cand-1", &reference.digest)
        .expect("load evidence")
        .expect("evidence exists");
    assert_eq!(loaded.digest, reference.digest);
    assert_eq!(loaded.uri, reference.uri);

    let listed = store
        .list_evidence_references("task-evidence", "cand-1")
        .expect("list evidence");
    assert_eq!(listed, vec![reference]);
}

#[test]
fn bootstrap_topology_returns_typed_network_and_org_descriptors() {
    let store = PgStore::open_in_memory().expect("open store");
    let topology = store
        .ensure_local_bootstrap_network_topology("node-a", "pub-a", 1_700_000_000_000)
        .expect("bootstrap topology");

    assert_eq!(topology.network.network_id, "local:node-a");
    assert_eq!(topology.network.network_name, "Local Network node-a");
    assert_eq!(topology.network.network_kind, NetworkKind::Local);
    assert_eq!(topology.network.parent_network_id, None);
    assert_eq!(topology.network.genesis_node_id, "node-a");
    assert_eq!(topology.org.org_id, "local:node-a:bootstrap");
    assert_eq!(topology.org.network_id, topology.network.network_id);
    assert_eq!(topology.org.network_org_name, "Local Bootstrap");
    assert_eq!(topology.org.org_kind, "bootstrap");
    assert!(topology.org.is_default);

    let loaded = store
        .load_network_topology_for_org(&topology.org.org_id)
        .expect("load topology");
    assert_eq!(loaded.network.network_name, "Local Network node-a");
    assert_eq!(loaded.org.network_org_name, "Local Bootstrap");
    assert_eq!(loaded, topology);
}

#[test]
fn subnet_topology_creation_and_joining_attach_parent_membership() {
    let store = PgStore::open_in_memory().expect("open store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            "genesis-a",
            "genesis-a",
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            "genesis-a",
            "genesis-a",
            1_700_000_000_100,
        )
        .expect("subnet topology");

    assert_eq!(subnet.network.network_kind, NetworkKind::Subnet);
    assert_eq!(
        subnet.network.parent_network_id.as_deref(),
        Some(mainnet.network.network_id.as_str())
    );
    assert!(
        store
            .node_has_network_membership("genesis-a", &mainnet.network.network_id)
            .expect("genesis parent membership")
    );
    assert!(
        store
            .node_has_network_membership("genesis-a", &subnet.network.network_id)
            .expect("genesis subnet membership")
    );

    let joined = store
        .join_node_to_network_topology("subnet:alpha", "node-b", "node-b", 1_700_000_000_200)
        .expect("join subnet");
    assert_eq!(joined.network.network_kind, NetworkKind::Subnet);
    assert_eq!(joined.org.org_id, "subnet:alpha:bootstrap");
    assert!(
        store
            .node_has_network_membership("node-b", "subnet:alpha")
            .expect("joined subnet membership")
    );
    assert!(
        store
            .node_has_network_membership("node-b", "mainnet:watt-galaxy")
            .expect("joined parent membership")
    );

    let parent = store
        .load_parent_network_topology_for_org(&subnet.org.org_id)
        .expect("load parent topology")
        .expect("parent topology");
    assert_eq!(parent.network.network_id, mainnet.network.network_id);
    assert_eq!(parent.network.network_kind, NetworkKind::Mainnet);
}

#[test]
fn discoverable_network_topologies_list_active_default_networks() {
    let store = PgStore::open_in_memory().expect("open store");
    let mainnet = store
        .ensure_mainnet_bootstrap_network_topology(
            "mainnet:watt-galaxy",
            "Watt Galaxy",
            "genesis-a",
            "genesis-a",
            1_700_000_000_000,
        )
        .expect("mainnet topology");
    let subnet = store
        .ensure_subnet_bootstrap_network_topology(
            &mainnet.network.network_id,
            "subnet:alpha",
            "Subnet Alpha",
            "genesis-a",
            "genesis-a",
            1_700_000_000_100,
        )
        .expect("subnet topology");

    let discoverable = store
        .list_discoverable_network_topologies(10)
        .expect("list discoverable networks");

    assert_eq!(discoverable.len(), 2);
    assert_eq!(discoverable[0], mainnet);
    assert_eq!(discoverable[1], subnet);
}

#[test]
fn discoverable_feed_sources_and_domains_capture_active_network_surfaces() {
    let store = open_test_store();
    let contract = sample_contract("task-feed-directory", "resume_review");
    store
        .upsert_task_contract(&contract, 1)
        .expect("upsert task contract");
    store
        .upsert_feed_subscription("node-a", "feed-market", " region:sol-1 ", true, 10)
        .expect("active feed subscription");
    store
        .upsert_feed_subscription("node-b", "feed-market", "region:sol-1", true, 11)
        .expect("second feed subscription");
    store
        .upsert_execution_set_member(
            &contract.task_id,
            "exec-1",
            "node-a",
            "worker",
            "node:lab-7",
            "active",
            Some("node-a"),
            12,
        )
        .expect("execution set member");
    store
        .put_task_announcement(
            &contract.task_id,
            "announce-directory",
            "feed-market",
            "region:sol-1",
            &serde_json::json!({"headline":"directory"}),
            None,
            "node-source",
            20,
        )
        .expect("task announcement");

    let feeds = store
        .list_discoverable_feed_sources(10)
        .expect("list discoverable feeds");
    assert_eq!(feeds.len(), 1);
    assert_eq!(feeds[0].feed_key, "feed-market");
    assert_eq!(feeds[0].scope_hint, "region:sol-1");
    assert_eq!(feeds[0].subscriber_count, 2);
    assert_eq!(
        feeds[0].latest_announcement_id.as_deref(),
        Some("announce-directory")
    );
    assert_eq!(
        feeds[0].latest_task_id.as_deref(),
        Some(contract.task_id.as_str())
    );
    assert_eq!(
        feeds[0].latest_source_node_id.as_deref(),
        Some("node-source")
    );
    assert_eq!(feeds[0].latest_announced_at, Some(20));

    let domains = store
        .list_active_dissemination_domains(10)
        .expect("list domains");
    assert_eq!(
        domains,
        vec!["node:lab-7".to_owned(), "region:sol-1".to_owned()]
    );
}

fn sample_contract(task_id: &str, task_type: &str) -> TaskContract {
    TaskContract {
        protocol_version: "v0.1".to_owned(),
        task_id: task_id.to_owned(),
        task_type: task_type.to_owned(),
        inputs: serde_json::json!({"prompt":"hello"}),
        output_schema: serde_json::json!({"type":"object"}),
        budget: Budget {
            time_ms: 30_000,
            max_steps: 10,
            cost_units: 1_000,
            mode: BudgetMode::Lifetime,
            explore_cost_units: 350,
            verify_cost_units: 450,
            finalize_cost_units: 200,
            reuse_verify_time_ms: 20_000,
            reuse_verify_cost_units: 200,
            reuse_max_attempts: 1,
        },
        assignment: Assignment {
            mode: "CLAIM".to_owned(),
            claim: ClaimPolicy {
                lease_ms: 5_000,
                max_concurrency: MaxConcurrency {
                    propose: 1,
                    verify: 1,
                },
            },
            explore: ExploreAssignment {
                max_proposers: 1,
                topk: 3,
                stop: ExploreStopPolicy {
                    no_new_evidence_rounds: 3,
                },
            },
            verify: VerifyAssignment { max_verifiers: 1 },
            finalize: FinalizeAssignment { max_finalizers: 1 },
        },
        acceptance: Acceptance {
            quorum_threshold: 1,
            verifier_policy: PolicyBinding {
                policy_id: "vp.schema_only.v1".to_owned(),
                policy_version: "1".to_owned(),
                policy_hash: "policy-hash".to_owned(),
                policy_params: serde_json::json!({}),
            },
            vote: VotePolicy {
                commit_reveal: true,
                reveal_deadline_ms: 10_000,
            },
            settlement: SettlementPolicy {
                window_ms: 86_400_000,
                implicit_weight: 0.1,
                implicit_diminishing_returns: SettlementDiminishingReturns { w: 10, k: 50 },
                bad_penalty: SettlementBadPenalty { p: 3 },
                feedback: FeedbackCapabilityPolicy {
                    mode: "CAPABILITY".to_owned(),
                    authority_pubkey: "ed25519:placeholder".to_owned(),
                },
            },
            da_quorum_threshold: 1,
        },
        task_mode: TaskMode::OneShot,
        expiry_ms: 2_000_000_000_000,
        evidence_policy: EvidencePolicy {
            max_inline_evidence_bytes: 65_536,
            max_inline_media_bytes: 0,
            inline_mime_allowlist: vec!["application/json".to_owned(), "text/plain".to_owned()],
            max_snippet_bytes: 8_192,
            max_snippet_tokens: 2_048,
        },
    }
}

fn sample_event(
    event_id: &str,
    protocol_version: &str,
    author_node_id: &str,
    epoch: u64,
    created_at: u64,
    payload: EventPayload,
) -> Event {
    Event {
        event_id: event_id.to_owned(),
        protocol_version: protocol_version.to_owned(),
        event_kind: payload.kind(),
        task_id: payload.task_id().map(ToOwned::to_owned),
        epoch,
        author_node_id: author_node_id.to_owned(),
        created_at,
        payload,
        signature_hex: "deadbeef".to_owned(),
    }
}

fn sample_candidate(candidate_id: &str, execution_id: &str) -> Candidate {
    Candidate {
        candidate_id: candidate_id.to_owned(),
        execution_id: execution_id.to_owned(),
        output: serde_json::json!({"answer":"ok"}),
        evidence_inline: vec![],
        evidence_refs: vec![ArtifactRef {
            uri: "https://example.com/artifacts/1".to_owned(),
            digest: "sha256:abc123".to_owned(),
            size_bytes: 123,
            mime: "application/json".to_owned(),
            created_at: 1_700_000_000_000,
            producer: "runtime/profile".to_owned(),
        }],
    }
}

fn sample_verifier_result(candidate_id: &str, execution_id: &str, passed: bool) -> VerifierResult {
    VerifierResult {
        candidate_id: candidate_id.to_owned(),
        execution_id: execution_id.to_owned(),
        verification_status: if passed {
            VerificationStatus::Passed
        } else {
            VerificationStatus::Failed
        },
        passed,
        score: if passed { 0.99 } else { 0.2 },
        reason_codes: vec![1001],
        verifier_result_hash: format!("vrh-{candidate_id}-{execution_id}"),
        provider_family: "openai".to_owned(),
        model_id: "gpt-5".to_owned(),
        policy_id: "vp.schema_only.v1".to_owned(),
        policy_version: "1".to_owned(),
        policy_hash: "policy-hash".to_owned(),
    }
}

#[test]
fn event_log_round_trip_and_clear_projection_keeps_events() {
    let store = open_test_store();
    let task_id = "task-event-log";
    let contract = sample_contract(task_id, "resume_review");
    store
        .upsert_task_contract(&contract, 1)
        .expect("upsert contract");

    let created = sample_event(
        "evt-1",
        "0.1.0",
        "node-local",
        1,
        1_700_000_000_100,
        EventPayload::TaskCreated(contract.clone()),
    );
    let expired = sample_event(
        "evt-2",
        "0.2.0",
        "node-peer",
        1,
        1_700_000_000_200,
        EventPayload::TaskExpired(TaskExpiredPayload {
            task_id: task_id.to_owned(),
        }),
    );

    let seq1 = store.append_event(&created).expect("append evt-1");
    let seq2 = store.append_event(&expired).expect("append evt-2");
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);
    assert_eq!(store.head_seq().expect("head seq"), 2);

    let from_1 = store.load_events_from(1).expect("events after 1");
    assert_eq!(from_1.len(), 1);
    assert_eq!(from_1[0].0, 2);
    assert_eq!(from_1[0].1.event_id, "evt-2");

    let first_page = store.load_events_page(0, 1).expect("page");
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].1.event_id, "evt-1");

    let all = store.load_all_events().expect("all events");
    assert_eq!(all.len(), 2);

    let created_at = store.task_created_at(task_id).expect("task created at");
    assert_eq!(created_at, Some(1_700_000_000_100));

    let peer_dist = store
        .peer_protocol_version_distribution("node-local")
        .expect("peer version dist");
    assert_eq!(peer_dist, vec![("0.2.0".to_owned(), 1)]);

    assert_eq!(
        store
            .task_projection(task_id)
            .expect("projection")
            .expect("projection exists")
            .contract
            .task_id,
        task_id
    );

    store.clear_projection().expect("clear projection");
    assert!(
        store
            .task_projection(task_id)
            .expect("projection after clear")
            .is_none()
    );
    assert_eq!(store.head_seq().expect("head seq after clear"), 2);
}

#[test]
fn export_and_vote_checkpoint_membership_paths_work() {
    let store = open_test_store();
    let task_id = "task-export";
    let task_type = "resume_review";
    store
        .upsert_task_contract(&sample_contract(task_id, task_type), 1)
        .expect("upsert task");

    store
        .put_decision_memory(
            task_id,
            1,
            "final-commit-1",
            1_700_000_000_300,
            "winner-hash",
            "output-digest",
            &serde_json::json!({"winner":"cand-1"}),
            &serde_json::json!({"approvals":1,"rejects":0,"quorum_threshold":1}),
            &[101, 202],
            &serde_json::json!({"reason":"ok"}),
            "policy-snapshot",
            task_type,
            "input-digest",
            "output-schema-digest",
            "policy-id",
            "policy-params-digest",
        )
        .expect("put decision memory");
    store
        .upsert_evidence_summary(
            "cid-1",
            "application/json",
            111,
            "source-hint",
            1_700_000_000_250,
        )
        .expect("upsert evidence summary");

    let reject_codes = vec![7_u16, 8_u16];
    store
        .upsert_runtime_metric_observation(&wattswarm_storage_core::RuntimeMetricObservation {
            runtime_id: "rt",
            profile_id: "default",
            task_type,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
            finalized: true,
            timeout: false,
            crash: false,
            invalid_output: false,
            latency_ms: 120,
            cost_units: 9,
            reject_reason_codes: &reject_codes,
            reuse_hit_exact: true,
            reuse_hit_similar: false,
            reuse_applied: true,
        })
        .expect("upsert runtime metric");

    store
        .put_task_settlement(task_id, 1, 1_700_000_001_200, 1_700_000_001_900)
        .expect("put settlement");
    store
        .mark_task_bad_feedback(task_id, 1, 1_700_000_001_500)
        .expect("mark bad feedback");
    store
        .upsert_task_cost_report(
            task_id,
            1,
            &serde_json::json!({"explore": 1, "verify": 2}),
            &serde_json::json!({"explore_ms": 30, "verify_ms": 50}),
            10,
            5,
            0.25,
        )
        .expect("upsert task cost");
    store
        .adjust_reputation("rt", "default", 12_345, -600, 1_700_000_002_000)
        .expect("adjust reputation");
    let reputation = store
        .get_reputation_snapshot("rt", "default")
        .expect("get reputation snapshot")
        .expect("reputation row exists");
    assert_eq!(reputation.stability_reputation, 12_345);
    assert_eq!(reputation.quality_reputation, -600);
    store
        .put_reputation_snapshot("rt", "default", 9_999, 9_999, 1_700_000_001_000)
        .expect("put older reputation snapshot");
    let reputation_after_old = store
        .get_reputation_snapshot("rt", "default")
        .expect("get reputation after old snapshot")
        .expect("reputation row exists");
    assert_eq!(reputation_after_old.stability_reputation, 12_345);
    assert_eq!(reputation_after_old.quality_reputation, -600);
    store
        .put_reputation_snapshot("rt", "default", 20_000, 30_000, 1_700_000_003_000)
        .expect("put newer reputation snapshot");
    let reputation_after_new = store
        .get_reputation_snapshot("rt", "default")
        .expect("get reputation after new snapshot")
        .expect("reputation row exists");
    assert_eq!(reputation_after_new.stability_reputation, 20_000);
    assert_eq!(reputation_after_new.quality_reputation, 30_000);
    store
        .put_knowledge_lookup(
            task_id,
            task_type,
            "input-digest",
            1_700_000_002_500,
            1,
            1,
            0,
            "hits-digest",
            true,
        )
        .expect("put knowledge lookup");

    store
        .put_advisory_created("adv-1", "policy-id", "hash-a", 1_700_000_003_000)
        .expect("advisory created");
    store
        .mark_advisory_approved("adv-1", "admin-1", 1_700_000_003_500)
        .expect("advisory approved");
    store
        .mark_advisory_applied("adv-1", "hash-b", 1_700_000_004_000)
        .expect("advisory applied");

    let advisory: AdvisoryStateRow = store
        .get_advisory_state("adv-1")
        .expect("get advisory state")
        .expect("advisory exists");
    assert_eq!(advisory.status, "applied");
    assert_eq!(advisory.approved_by.as_deref(), Some("admin-1"));

    store
        .record_unknown_reason_observation(
            Some(task_id),
            901,
            "0.0.9",
            "0.1.0",
            "peer-2",
            1_700_000_004_500,
        )
        .expect("record unknown reason");

    assert!(
        store
            .get_vote_commit(task_id, "voter-a")
            .expect("empty vote commit")
            .is_none()
    );
    store
        .put_vote_commit(
            task_id,
            "voter-a",
            "cand-hash",
            "commit-hash",
            "vrh",
            "exec-1",
            1_700_000_005_000,
        )
        .expect("put vote commit");
    let commit = store
        .get_vote_commit(task_id, "voter-a")
        .expect("get vote commit")
        .expect("vote commit exists");
    assert_eq!(commit.execution_id, "exec-1");

    let reveal = wattswarm_storage_core::VoteRevealRow {
        task_id: task_id.to_owned(),
        voter_node_id: "voter-a".to_owned(),
        candidate_id: "cand-1".to_owned(),
        candidate_hash: "cand-hash".to_owned(),
        vote: VoteChoice::Approve,
        salt: "salt-1".to_owned(),
        verifier_result_hash: "vrh".to_owned(),
        valid: true,
        created_at: 1_700_000_005_100,
    };
    store.put_vote_reveal(&reveal).expect("put vote reveal");

    assert_eq!(
        store
            .count_valid_votes_for_candidate(task_id, "cand-1", VoteChoice::Approve)
            .expect("count valid approve"),
        1
    );
    assert_eq!(
        store
            .count_valid_votes_for_candidate(task_id, "cand-1", VoteChoice::Reject)
            .expect("count valid reject"),
        0
    );
    assert_eq!(
        store
            .count_valid_votes_for_candidate_hash(task_id, "cand-hash", VoteChoice::Approve)
            .expect("count by hash"),
        1
    );

    store
        .put_finalization(task_id, 1, "cand-1", "{\"sig\":\"ok\"}", "evt-final-1")
        .expect("put finalization");
    assert_eq!(
        store
            .get_finalization_candidate(task_id, 1)
            .expect("get finalization candidate")
            .as_deref(),
        Some("cand-1")
    );

    assert_eq!(
        store.latest_checkpoint_seq().expect("empty checkpoint"),
        None
    );
    store
        .put_checkpoint("cp-1", 10, "evt-10")
        .expect("put checkpoint 1");
    store
        .put_checkpoint("cp-2", 20, "evt-20")
        .expect("put checkpoint 2");
    assert_eq!(
        store.latest_checkpoint_seq().expect("latest checkpoint"),
        Some(20)
    );

    assert!(
        store
            .load_membership()
            .expect("load membership empty")
            .is_none()
    );
    store
        .put_membership("{\"members\":{}}")
        .expect("put membership");
    assert_eq!(
        store.load_membership().expect("load membership").as_deref(),
        Some("{\"members\":{}}")
    );

    let export_task = store
        .export_knowledge_by_task(task_id)
        .expect("export by task");
    assert_eq!(
        export_task["decision_memory"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["runtime_metrics"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["task_settlement"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["task_cost_reports"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["knowledge_lookups"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["unknown_reason_observations"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["advisory_state"].as_array().map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task["reputation_state"][0]["stability_reputation_decimal"],
        "2.0000"
    );

    let export_task_type = store
        .export_knowledge_by_task_type(task_type)
        .expect("export by task type");
    assert_eq!(export_task_type["task_type"], task_type);
    assert_eq!(
        export_task_type["knowledge_lookups"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task_type["runtime_metrics"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );
    assert_eq!(
        export_task_type["unknown_reason_observations"]
            .as_array()
            .map(|a| a.len()),
        Some(1)
    );

    let decision_event = sample_event(
        "evt-decision",
        "0.1.0",
        "node-a",
        1,
        1_700_000_000_400,
        EventPayload::DecisionCommitted(DecisionCommittedPayload {
            task_id: task_id.to_owned(),
            epoch: 1,
            candidate_id: "cand-1".to_owned(),
            candidate_hash: "cand-hash".to_owned(),
        }),
    );
    let checkpoint_event = sample_event(
        "evt-checkpoint",
        "0.1.0",
        "node-a",
        1,
        1_700_000_000_500,
        EventPayload::CheckpointCreated(CheckpointCreatedPayload {
            checkpoint_id: "cp-2".to_owned(),
            up_to_seq: 20,
        }),
    );
    store
        .append_event(&decision_event)
        .expect("append decision event");
    store
        .append_event(&checkpoint_event)
        .expect("append checkpoint event");

    let candidate = sample_candidate("cand-1", "exec-1");
    store
        .put_candidate(task_id, "node-a", &candidate)
        .expect("put candidate");
    store
        .put_verifier_result(
            task_id,
            "verifier-a",
            &sample_verifier_result("cand-1", "exec-1", true),
        )
        .expect("put verifier result");
}

#[test]
fn ensure_local_bootstrap_topology_scopes_default_org_and_rejects_duplicate_identity() {
    let store = open_test_store();
    let org_id = store
        .ensure_local_bootstrap_topology("node-a", "pub-a", 1_700_000_000_000)
        .expect("bootstrap local topology");
    assert_eq!(org_id, "local:node-a:bootstrap");
    assert_eq!(store.for_org(&org_id).org_id(), "local:node-a:bootstrap");

    let err = store
        .ensure_local_bootstrap_topology("node-b", "pub-a", 1_700_000_000_100)
        .expect_err("duplicate public key should be rejected");
    assert!(
        err.to_string()
            .contains("node identity conflict: node_id/public_key must be unique")
    );
    assert!(err.to_string().contains("node_seed.hex"));
}

#[test]
fn membership_and_revocations_are_scoped_by_org() {
    let store = open_test_store();
    let local_org = store
        .ensure_local_bootstrap_topology("node-a", "pub-a", 1_700_000_000_000)
        .expect("bootstrap local topology");
    let org_a = store.for_org(&local_org);
    let org_b = store.for_org("local:node-a:other");

    org_a
        .put_membership("{\"members\":{\"node-a\":[\"FINALIZER\"]}}")
        .expect("put membership org a");
    assert_eq!(
        org_a
            .load_membership()
            .expect("load membership org a")
            .as_deref(),
        Some("{\"members\":{\"node-a\":[\"FINALIZER\"]}}")
    );
    assert!(
        org_b
            .load_membership()
            .expect("load membership org b")
            .is_none(),
        "membership should not bleed across orgs"
    );

    org_a
        .put_event_revocation("evt-1", "bad event", "node-a", 1_700_000_000_100)
        .expect("put event revocation org a");
    assert!(org_a.is_event_revoked("evt-1").expect("revoked in org a"));
    assert!(
        !org_b
            .is_event_revoked("evt-1")
            .expect("not revoked in org b"),
        "event revocation should be org-scoped"
    );

    org_a
        .put_summary_revocation(
            "summary-1",
            "decision_memory",
            "bad summary",
            "node-a",
            1_700_000_000_200,
        )
        .expect("put summary revocation org a");
    assert!(
        org_a
            .is_summary_revoked("summary-1")
            .expect("revoked in org a")
    );
    assert!(
        !org_b
            .is_summary_revoked("summary-1")
            .expect("not revoked in org b"),
        "summary revocation should be org-scoped"
    );
}

#[test]
fn imported_task_outcomes_roundtrip_and_revoke() {
    let store = open_test_store();
    store
        .put_imported_task_outcome(&ImportedTaskOutcomeRow {
            summary_id: "summary-outcome-1".to_owned(),
            source_node_id: "node-remote".to_owned(),
            scope_hint: "region:sol-1".to_owned(),
            task_id: "task-outcome-1".to_owned(),
            task_type: "summary-type".to_owned(),
            candidate_id: "cand-1".to_owned(),
            output_digest: "sha256:abc".to_owned(),
            result_summary: serde_json::json!({"answer":"compressed"}),
            evidence_digest_count: 2,
            checkpoint_id: "cp-1".to_owned(),
            proof_artifact_path: "finality://summary-type/task-outcome-1/1/cand-1".to_owned(),
            finalized_at: 1_700_000_000_300,
            revoked: false,
        })
        .expect("put imported outcome");

    let outcomes = store
        .list_imported_task_outcomes_by_task_type("summary-type", 8)
        .expect("list imported outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].task_id, "task-outcome-1");
    assert_eq!(
        outcomes[0].result_summary["answer"],
        serde_json::json!("compressed")
    );
    assert_eq!(outcomes[0].checkpoint_id, "cp-1");

    store
        .revoke_imported_task_outcomes_by_summary("summary-outcome-1")
        .expect("revoke imported outcome");
    assert!(
        store
            .list_imported_task_outcomes_by_task_type("summary-type", 8)
            .expect("list outcomes after revoke")
            .is_empty()
    );
}
