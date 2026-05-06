use super::*;

#[test]
fn ui_exposes_run_queue_http_apis() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _mode_guard = EnvVarGuard::set("WATTSWARM_NODE_MODE", "local");
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
    let run_id = "run-ui-http";
    let retry_run_id = "run-ui-http-retry";

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"rt-ui","base_url":"http://127.0.0.1:8787"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/run/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "kickoff": true,
                            "spec": sample_run_spec(run_id),
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_res.status(), StatusCode::OK);
        let submit_json = json_from(submit_res).await;
        assert_eq!(submit_json["run_id"].as_str(), Some(run_id));
        assert_eq!(submit_json["kicked_off"].as_bool(), Some(true));

        let watch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/watch/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(watch_res.status(), StatusCode::OK);
        let watch_json = json_from(watch_res).await;
        assert_eq!(watch_json["watch"]["run_id"].as_str(), Some(run_id));
        assert_eq!(watch_json["watch"]["status"].as_str(), Some("QUEUED"));
        assert_eq!(watch_json["watch"]["counts"]["queued"].as_i64(), Some(2));

        let result_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/result/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result_res.status(), StatusCode::OK);
        let result_json = json_from(result_res).await;
        assert_eq!(result_json["result"]["run_id"].as_str(), Some(run_id));
        assert_eq!(result_json["result"]["status"].as_str(), Some("QUEUED"));

        let events_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/events/{run_id}?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events_res.status(), StatusCode::OK);
        let events_json = json_from(events_res).await;
        let event_types: Vec<_> = events_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["event_type"].as_str())
            .collect();
        assert!(event_types.contains(&"RUN_CREATED"));
        assert!(event_types.contains(&"RUN_KICKOFF"));

        let cancel_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/run/cancel/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel_res.status(), StatusCode::OK);

        let cancelled_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/result/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cancelled_json = json_from(cancelled_res).await;
        assert_eq!(
            cancelled_json["result"]["status"].as_str(),
            Some("CANCELLED")
        );

        let retry_submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/run/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&sample_run_spec(retry_run_id)).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_submit_res.status(), StatusCode::OK);

        let retry_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/run/retry/{retry_run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_res.status(), StatusCode::OK);

        let retry_events_res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/run/events/{retry_run_id}?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let retry_events_json = json_from(retry_events_res).await;
        let retry_event_types: Vec<_> = retry_events_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["event_type"].as_str())
            .collect();
        assert!(retry_event_types.contains(&"RUN_RETRY_REQUESTED"));
    });
}

#[test]
fn ui_rejects_lan_run_submit_when_only_local_executors_are_available() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let save_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/startup-config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "display_name": "Captain Aurora",
                            "network_mode": "lan",
                            "bootstrap_contacts": []
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);

        let add_exec_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/executors/add")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"rt-ui","base_url":"http://127.0.0.1:8787"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_exec_res.status(), StatusCode::OK);

        let submit_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/run/submit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "kickoff": true,
                            "spec": sample_run_spec("run-lan-only-local"),
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit_res.status(), StatusCode::BAD_REQUEST);
        let submit_json = json_from(submit_res).await;
        assert!(
            submit_json["error"]
                .as_str()
                .is_some_and(|error| error.contains("no non-coordinator executors")),
            "unexpected submit response: {submit_json:?}"
        );
    });
}
