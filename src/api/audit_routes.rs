use crate::api::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct EventQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub bucket: Option<String>,
}

/// List audit events
pub async fn list_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EventQuery>,
) -> Json<serde_json::Value> {
    let limit = params.limit.unwrap_or(100);
    let offset = params.offset.unwrap_or(0);

    let events = if let Some(bucket) = params.bucket {
        state.audit_logger.get_events_for_bucket(&bucket, limit)
    } else {
        state.audit_logger.get_events(limit, offset)
    };

    let count = events.len();
    Json(serde_json::json!({
        "total": state.audit_logger.event_count(),
        "count": count,
        "events": events,
    }))
}

/// Clear all audit events
pub async fn clear_events(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    state.audit_logger.clear();
    (StatusCode::OK, Json(serde_json::json!({
        "status": "ok",
        "message": "All audit events cleared",
    })))
}

/// Get audit config
pub async fn get_config(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "enabled": state.audit_logger.is_enabled(),
        "max_events": 10000,
    }))
}

#[derive(Deserialize)]
pub struct AuditConfigUpdate {
    pub enabled: Option<bool>,
}

/// Update audit config
pub async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AuditConfigUpdate>,
) -> Json<serde_json::Value> {
    if let Some(enabled) = body.enabled {
        state.audit_logger.set_enabled(enabled);
    }
    Json(serde_json::json!({
        "status": "ok",
        "enabled": state.audit_logger.is_enabled(),
    }))
}

/// Get audit stats
pub async fn get_stats(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "total_events": state.audit_logger.event_count(),
        "enabled": state.audit_logger.is_enabled(),
    }))
}
