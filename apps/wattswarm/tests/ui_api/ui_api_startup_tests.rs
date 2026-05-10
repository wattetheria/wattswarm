use super::*;

#[test]
fn ui_startup_config_roundtrips_network_settings_without_agent_binding() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let _p2p_guard = EnvVarGuard::set("WATTSWARM_P2P_ENABLED", "0");
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
        let lan_contact = "iroh-contact-a";

        let default_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/startup-config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(default_res.status(), StatusCode::OK);
        let default_json = json_from(default_res).await;
        assert_eq!(
            default_json["config"]["network_mode"].as_str(),
            Some("local")
        );
        assert_eq!(
            default_json["config"]["bootstrap_contacts"]
                .as_array()
                .map(|items| items.len()),
            Some(0)
        );
        assert_eq!(
            default_json["config"]["gateway_urls"]
                .as_array()
                .map(|items| items.len()),
            Some(0)
        );
        assert_eq!(
            default_json["core_agent_executor"].as_str(),
            Some("core-agent")
        );

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
                            "bootstrap_contacts": [
                                lan_contact
                            ],
                            "gateway_urls": [
                                " http://gateway.example.com:8080/ ",
                                "http://gateway.example.com:8080"
                            ]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);
        let save_json = json_from(save_res).await;
        assert_eq!(save_json["executor_registered"].as_bool(), Some(false));

        let get_saved_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/startup-config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_saved_res.status(), StatusCode::OK);
        let get_saved_json = json_from(get_saved_res).await;
        assert_eq!(
            get_saved_json["config"]["display_name"].as_str(),
            Some("Captain Aurora")
        );
        assert_eq!(
            get_saved_json["config"]["bootstrap_contacts"][0].as_str(),
            Some(lan_contact)
        );
        assert_eq!(
            get_saved_json["config"]["gateway_urls"][0].as_str(),
            Some("http://gateway.example.com:8080")
        );
        assert_eq!(
            get_saved_json["config"]["core_agent"]["base_url"].as_str(),
            Some("http://127.0.0.1:8787")
        );

        let save_wan_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/startup-config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "display_name": "Captain Aurora",
                            "network_mode": "wan",
                            "bootstrap_contacts": [
                                "iroh-contact-should-be-automatic"
                            ],
                            "gateway_urls": [
                                "https://gateway-should-be-automatic.example.com"
                            ]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_wan_res.status(), StatusCode::OK);
        let save_wan_json = json_from(save_wan_res).await;
        assert_eq!(
            save_wan_json["config"]["network_mode"].as_str(),
            Some("wan")
        );
        assert_eq!(
            save_wan_json["config"]["bootstrap_contacts"][0].as_str(),
            Some(lan_contact)
        );
        assert_eq!(
            save_wan_json["config"]["gateway_urls"][0].as_str(),
            Some("http://gateway.example.com:8080")
        );

        let save_local_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/startup-config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "display_name": "Captain Aurora",
                            "network_mode": "local",
                            "bootstrap_contacts": [
                                "iroh-contact-should-be-cleared"
                            ],
                            "gateway_urls": [
                                "https://gw.example.com"
                            ]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_local_res.status(), StatusCode::OK);
        let save_local_json = json_from(save_local_res).await;
        assert_eq!(
            save_local_json["config"]["bootstrap_contacts"]
                .as_array()
                .map(|items| items.len()),
            Some(0)
        );
        assert_eq!(
            save_local_json["config"]["gateway_urls"]
                .as_array()
                .map(|items| items.len()),
            Some(0)
        );

        let executors_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/executors/list")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executors_res.status(), StatusCode::OK);
        let executors_json = json_from(executors_res).await;
        assert!(
            executors_json["executors"]
                .as_array()
                .unwrap()
                .iter()
                .all(|entry| entry["name"].as_str() != Some("core-agent"))
        );

        let get_local_saved_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/startup-config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_local_saved_res.status(), StatusCode::OK);
        let get_local_saved_json = json_from(get_local_saved_res).await;
        assert_eq!(
            get_local_saved_json["config"]["network_mode"].as_str(),
            Some("local")
        );
        assert_eq!(
            get_local_saved_json["config"]["bootstrap_contacts"]
                .as_array()
                .map(|items| items.len()),
            Some(0)
        );
        assert_eq!(
            get_local_saved_json["config"]["gateway_urls"]
                .as_array()
                .map(|items| items.len()),
            Some(0)
        );

        let status_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/node/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let status_json = json_from(status_res).await;
        assert_eq!(status_json["mode"].as_str(), Some("local"));
    });

    let conn =
        Connection::open(state_dir.join("local-control.state")).expect("open local control db");
    let mut stmt = conn
        .prepare(
            "SELECT scope_id, executor_name, base_url
             FROM executor_registry_local
             ORDER BY executor_name ASC",
        )
        .expect("prepare executor raw query");
    let raw_rows = stmt
        .query_map(wattswarm_storage_core::params![], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .expect("query executor raw rows")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect executor raw rows");
    assert!(raw_rows.is_empty());
}

#[test]
fn ui_startup_config_save_updates_existing_runtime_node_mode() {
    let _guard = env_lock();
    let _db_lock = DbTestLock::acquire();
    let schema = reset_test_schema("test");
    let _schema_guard = EnvVarGuard::set("WATTSWARM_PG_SCHEMA", &schema);
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let db_path = state_dir.join("ui.state");
    std::fs::write(
        state_dir.join("node_state.json"),
        serde_json::to_vec_pretty(&json!({
            "running": true,
            "mode": "network"
        }))
        .unwrap(),
    )
    .unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = build_app(UiServerState::new(state_dir.clone(), db_path));
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
                            "network_mode": "local",
                            "bootstrap_contacts": []
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);

        let status_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/node/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let status_json = json_from(status_res).await;
        assert_eq!(status_json["mode"].as_str(), Some("local"));
        assert_eq!(status_json["running"].as_bool(), Some(true));
    });

    let runtime_state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state_dir.join("node_state.json")).unwrap()).unwrap();
    assert_eq!(runtime_state["mode"].as_str(), Some("local"));
    assert_eq!(runtime_state["running"].as_bool(), Some(true));
}

#[test]
fn ui_startup_config_still_accepts_legacy_core_agent_binding_payload() {
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
        let save_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/startup-config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "display_name": "Legacy Node",
                            "network_mode": "local",
                            "bootstrap_contacts": [],
                            "core_agent": {
                                "mode": "remote_url",
                                "base_url": "http://127.0.0.1:9999",
                                "provider": "openclaw",
                                "model": "",
                                "api_key": ""
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save_res.status(), StatusCode::OK);
        let save_json = json_from(save_res).await;
        assert_eq!(save_json["executor_registered"].as_bool(), Some(true));
    });
}
