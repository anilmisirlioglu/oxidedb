use axum::response::Html;

/// In debug builds, read the HTML from disk so changes don't require a rebuild.
/// In release builds, the file is embedded at compile time for zero-dependency deployment.
pub async fn serve_ui() -> Html<String> {
    #[cfg(debug_assertions)]
    {
        // Dev mode: read from filesystem for hot-reload
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/static/index.html");
        match std::fs::read_to_string(path) {
            Ok(contents) => Html(contents),
            Err(_) => Html(include_str!("../../static/index.html").to_string()),
        }
    }
    #[cfg(not(debug_assertions))]
    {
        // Release mode: use embedded file
        Html(include_str!("../../static/index.html").to_string())
    }
}
