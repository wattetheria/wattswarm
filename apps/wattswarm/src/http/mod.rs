use anyhow::{Result, anyhow};
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::path::PathBuf;

pub(crate) mod background;
pub(crate) mod diagnostics;
pub(crate) mod egress;
pub(crate) mod executors;
pub(crate) mod helpers;
pub(crate) mod network_bootstrap;
pub(crate) mod node;
pub(crate) mod pages;
pub(crate) mod peers;
pub(crate) mod runs;
pub(crate) mod server;
pub(crate) mod startup;
pub(crate) mod swarm;
pub(crate) mod tasks;
pub(crate) mod topics;

#[derive(Clone)]
pub struct UiServerState {
    pub(crate) state_dir: PathBuf,
    pub(crate) db_path: PathBuf,
}

impl UiServerState {
    pub fn new(state_dir: PathBuf, db_path: PathBuf) -> Self {
        Self { state_dir, db_path }
    }
}

#[derive(Debug)]
pub(crate) struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": self.0.to_string()
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

pub(crate) async fn run_blocking<T, F>(f: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| anyhow!("task join error: {err}"))?
        .map_err(Into::into)
}
