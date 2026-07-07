use std::sync::LazyLock;

use crate::startup_template::STARTUP_HTML;
use crate::swarm_dashboard_template::SWARM_DASHBOARD_HTML;
use crate::theme_template::{SWARM_THEME_CSS, SWARM_THEME_PLACEHOLDER};
use crate::ui_template::INDEX_HTML;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse, Redirect, Response};

const FAVICON_PNG: &[u8] = include_bytes!("../../../../ui/public/favicon.png");
const FONT_FILES: &[(&str, &[u8])] = &[
    (
        "albert-sans-v4-latin-regular.woff2",
        include_bytes!("../../../../ui/public/fonts/albert-sans-v4-latin-regular.woff2"),
    ),
    (
        "albert-sans-v4-latin-500.woff2",
        include_bytes!("../../../../ui/public/fonts/albert-sans-v4-latin-500.woff2"),
    ),
    (
        "albert-sans-v4-latin-600.woff2",
        include_bytes!("../../../../ui/public/fonts/albert-sans-v4-latin-600.woff2"),
    ),
    (
        "dm-sans-v17-latin-regular.woff2",
        include_bytes!("../../../../ui/public/fonts/dm-sans-v17-latin-regular.woff2"),
    ),
    (
        "dm-sans-v17-latin-500.woff2",
        include_bytes!("../../../../ui/public/fonts/dm-sans-v17-latin-500.woff2"),
    ),
    (
        "dm-sans-v17-latin-600.woff2",
        include_bytes!("../../../../ui/public/fonts/dm-sans-v17-latin-600.woff2"),
    ),
    (
        "fraunces-v38-latin-600.woff2",
        include_bytes!("../../../../ui/public/fonts/fraunces-v38-latin-600.woff2"),
    ),
    (
        "outfit-v15-latin-regular.woff2",
        include_bytes!("../../../../ui/public/fonts/outfit-v15-latin-regular.woff2"),
    ),
    (
        "outfit-v15-latin-500.woff2",
        include_bytes!("../../../../ui/public/fonts/outfit-v15-latin-500.woff2"),
    ),
    (
        "outfit-v15-latin-600.woff2",
        include_bytes!("../../../../ui/public/fonts/outfit-v15-latin-600.woff2"),
    ),
    (
        "playwrite-us-trad-v11-latin-regular.woff2",
        include_bytes!("../../../../ui/public/fonts/playwrite-us-trad-v11-latin-regular.woff2"),
    ),
    (
        "OFL.txt",
        include_bytes!("../../../../ui/public/fonts/OFL.txt"),
    ),
];

static STARTUP_PAGE: LazyLock<String> = LazyLock::new(|| render_with_theme(STARTUP_HTML));
static DIAGNOSTICS_PAGE: LazyLock<String> = LazyLock::new(|| render_with_theme(INDEX_HTML));
static SWARM_PAGE: LazyLock<String> = LazyLock::new(|| render_with_theme(SWARM_DASHBOARD_HTML));

fn render_with_theme(template: &str) -> String {
    template.replace(SWARM_THEME_PLACEHOLDER, SWARM_THEME_CSS)
}

pub(crate) async fn index() -> Html<&'static str> {
    Html(STARTUP_PAGE.as_str())
}

pub(crate) async fn diagnostics_page() -> Html<&'static str> {
    Html(DIAGNOSTICS_PAGE.as_str())
}

pub(crate) async fn legacy_console_redirect() -> Redirect {
    Redirect::permanent("/diagnostics")
}

pub(crate) async fn swarm_page() -> Html<&'static str> {
    Html(SWARM_PAGE.as_str())
}

pub(crate) async fn favicon_png() -> impl axum::response::IntoResponse {
    ([(CONTENT_TYPE, "image/png")], FAVICON_PNG)
}

pub(crate) async fn font_file(Path(file): Path<String>) -> Response {
    for (name, bytes) in FONT_FILES {
        if *name == file {
            let content_type = if name.ends_with(".woff2") {
                "font/woff2"
            } else {
                "text/plain; charset=utf-8"
            };
            return ([(CONTENT_TYPE, content_type)], *bytes).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_pages_embed_theme_css() {
        for page in [
            STARTUP_PAGE.as_str(),
            DIAGNOSTICS_PAGE.as_str(),
            SWARM_PAGE.as_str(),
        ] {
            assert!(!page.contains(SWARM_THEME_PLACEHOLDER));
            for theme in ["matcha", "butter", "chocolate"] {
                assert!(
                    page.contains(&format!("[data-theme=\"{theme}\"]")),
                    "page missing theme block for {theme}"
                );
            }
        }
    }

    #[test]
    fn theme_pickers_offer_the_four_supported_themes() {
        for page in [STARTUP_PAGE.as_str(), DIAGNOSTICS_PAGE.as_str()] {
            for theme in ["forest", "matcha", "butter", "chocolate"] {
                assert!(
                    page.contains(&format!("data-theme-swatch=\"{theme}\"")),
                    "picker missing swatch for {theme}"
                );
            }
            for removed in ["teal", "emerald", "blue-royal", "blue-sky", "indigo"] {
                assert!(
                    !page.contains(&format!("data-theme-swatch=\"{removed}\"")),
                    "picker still offers removed theme {removed}"
                );
            }
        }
    }

    #[tokio::test]
    async fn font_file_serves_known_fonts_and_rejects_unknown() {
        let ok = font_file(Path("dm-sans-v17-latin-regular.woff2".to_string())).await;
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(
            ok.headers().get(CONTENT_TYPE).map(|v| v.to_str().unwrap()),
            Some("font/woff2")
        );

        let license = font_file(Path("OFL.txt".to_string())).await;
        assert_eq!(license.status(), StatusCode::OK);
        assert_eq!(
            license
                .headers()
                .get(CONTENT_TYPE)
                .map(|v| v.to_str().unwrap()),
            Some("text/plain; charset=utf-8")
        );

        let missing = font_file(Path("../secret".to_string())).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
}
