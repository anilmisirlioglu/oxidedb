//! DCP (Database Change Protocol) REST API Routes
//!
//! Endpoints:
//!   POST   /api/v1/dcp/streams            → Create a DCP stream
//!   GET    /api/v1/dcp/streams            → List all streams
//!   GET    /api/v1/dcp/streams/:id        → Get stream info
//!   DELETE /api/v1/dcp/streams/:id        → Close/delete a stream
//!   POST   /api/v1/dcp/streams/:id/pause  → Pause a stream
//!   POST   /api/v1/dcp/streams/:id/resume → Resume a stream
//!   GET    /api/v1/dcp/streams/:id/events → Poll for recent events
//!   GET    /api/v1/dcp/streams/:id/sse    → SSE real-time event stream
//!   POST   /api/v1/dcp/backfill           → One-shot backfill of all documents

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures::stream::Stream;
use std::convert::Infallible;
use std::sync::Arc;

use super::AppState;
use crate::dcp::stream::CreateStreamRequest;

/// POST /api/v1/dcp/streams — Create a DCP stream
pub async fn create_dcp_stream(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateStreamRequest>,
) -> Json<serde_json::Value> {
    match state.dcp_engine.create_stream(&req) {
        Ok(stream) => Json(serde_json::json!({
            "status": "ok",
            "stream": stream
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// GET /api/v1/dcp/streams — List all DCP streams
pub async fn list_dcp_streams(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let streams = state.dcp_engine.list_streams();
    let count = streams.len();
    Json(serde_json::json!({
        "status": "ok",
        "streams": streams,
        "count": count
    }))
}

/// GET /api/v1/dcp/streams/:id — Get DCP stream info
pub async fn get_dcp_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.dcp_engine.get_stream(&id) {
        Some(stream) => Json(serde_json::json!({
            "status": "ok",
            "stream": stream
        })),
        None => Json(serde_json::json!({
            "status": "error",
            "error": format!("Stream '{}' not found", id)
        })),
    }
}

/// DELETE /api/v1/dcp/streams/:id — Close/delete a stream
pub async fn close_dcp_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.dcp_engine.close_stream(&id) {
        Ok(()) => Json(serde_json::json!({
            "status": "ok",
            "message": format!("Stream '{}' closed", id)
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// POST /api/v1/dcp/streams/:id/pause — Pause a stream
pub async fn pause_dcp_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.dcp_engine.pause_stream(&id) {
        Ok(()) => Json(serde_json::json!({
            "status": "ok",
            "message": format!("Stream '{}' paused", id)
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// POST /api/v1/dcp/streams/:id/resume — Resume a stream
pub async fn resume_dcp_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.dcp_engine.resume_stream(&id) {
        Ok(()) => Json(serde_json::json!({
            "status": "ok",
            "message": format!("Stream '{}' resumed", id)
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// GET /api/v1/dcp/streams/:id/events — Poll recent events from the stream
/// Returns buffered events since the last poll (via broadcast channel)
pub async fn poll_dcp_events(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    let stream_info = match state.dcp_engine.get_stream(&id) {
        Some(s) => s,
        None => return Json(serde_json::json!({
            "status": "error",
            "error": format!("Stream '{}' not found", id)
        })),
    };

    // Get recent events from the broadcast channel (non-blocking)
    let mut rx = state.dcp_engine.subscribe();
    let mut events = Vec::new();
    let timeout = tokio::time::Duration::from_millis(100);

    // Drain available events with a short timeout
    loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Ok(event)) => {
                // Apply stream filters
                if event.bucket != stream_info.bucket {
                    continue;
                }
                if let Some(ref sf) = stream_info.scope_filter {
                    if event.scope != *sf {
                        continue;
                    }
                }
                if let Some(ref cf) = stream_info.collection_filter {
                    if event.collection != *cf {
                        continue;
                    }
                }
                events.push(event);
                if events.len() >= 1000 {
                    break; // cap at 1000 per poll
                }
            }
            _ => break, // timeout or error
        }
    }

    let count = events.len();
    Json(serde_json::json!({
        "status": "ok",
        "stream_id": id,
        "events": events,
        "count": count
    }))
}

/// POST /api/v1/dcp/backfill — One-shot backfill
#[derive(serde::Deserialize)]
pub struct BackfillRequest {
    pub bucket: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub collection: Option<String>,
}

pub async fn dcp_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillRequest>,
) -> Json<serde_json::Value> {
    match state.dcp_engine.backfill(
        &req.bucket,
        req.scope.as_deref(),
        req.collection.as_deref(),
    ) {
        Ok(events) => {
            let count = events.len();
            Json(serde_json::json!({
                "status": "ok",
                "events": events,
                "count": count
            }))
        }
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// GET /api/v1/dcp/streams/:id/sse — Server-Sent Events stream for real-time DCP events
pub async fn dcp_sse_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream_info = state.dcp_engine.get_stream(&id);
    let mut rx = state.dcp_engine.subscribe();

    let stream = async_stream::stream! {
        // Send initial connection event
        yield Ok(Event::default()
            .event("connected")
            .data(serde_json::json!({
                "stream_id": id,
                "message": "DCP SSE stream connected"
            }).to_string()));

        loop {
            match rx.recv().await {
                Ok(event) => {
                    // Apply stream filters if stream exists
                    if let Some(ref info) = stream_info {
                        if event.bucket != info.bucket {
                            continue;
                        }
                        if let Some(ref sf) = info.scope_filter {
                            if event.scope != *sf {
                                continue;
                            }
                        }
                        if let Some(ref cf) = info.collection_filter {
                            if event.collection != *cf {
                                continue;
                            }
                        }
                    }

                    let event_type = format!("{:?}", event.event_type).to_lowercase();
                    if let Ok(data) = serde_json::to_string(&event) {
                        yield Ok(Event::default()
                            .event(&event_type)
                            .data(data));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(Event::default()
                        .event("lagged")
                        .data(format!("{{\"skipped\":{}}}", n)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    yield Ok(Event::default()
                        .event("closed")
                        .data("{\"message\":\"DCP stream channel closed\"}".to_string()));
                    break;
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}
