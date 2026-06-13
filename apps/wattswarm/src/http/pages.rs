use crate::startup_template::STARTUP_HTML;
use crate::swarm_dashboard_template::SWARM_DASHBOARD_HTML;
use crate::ui_template::INDEX_HTML;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, Redirect};

const FAVICON_PNG: &[u8] = include_bytes!("../../../../ui/public/favicon.png");

pub(crate) async fn index() -> Html<&'static str> {
    Html(STARTUP_HTML)
}

pub(crate) async fn diagnostics_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

pub(crate) async fn legacy_console_redirect() -> Redirect {
    Redirect::permanent("/diagnostics")
}

pub(crate) async fn swarm_page() -> Html<&'static str> {
    Html(SWARM_DASHBOARD_HTML)
}

pub(crate) async fn favicon_png() -> impl axum::response::IntoResponse {
    ([(CONTENT_TYPE, "image/png")], FAVICON_PNG)
}
