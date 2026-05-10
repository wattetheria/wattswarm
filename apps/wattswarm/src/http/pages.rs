use crate::startup_template::STARTUP_HTML;
use crate::swarm_dashboard_template::SWARM_DASHBOARD_HTML;
use crate::ui_template::INDEX_HTML;
use axum::response::{Html, Redirect};

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
