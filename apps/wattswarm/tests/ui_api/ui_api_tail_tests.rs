use super::*;

#[test]
fn ui_wattetheria_snapshot_does_not_initialize_local_topology_when_unconfigured() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = build_app(UiServerState::new(state_dir.clone(), db_path.clone()));
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/wattetheria/network/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let json = json_from(res).await;
        assert_eq!(json["ok"].as_bool(), Some(false));
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|value| value.contains("node mode is not configured yet"))
        );
    });

    assert_eq!(count_projection_rows("network_registry"), 0);
    assert_eq!(count_projection_rows("node_registry"), 0);
}
