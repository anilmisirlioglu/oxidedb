//! FTS (Full-Text Search) REST API Routes
//!
//! Endpoints:
//!   POST   /api/v1/fts/indexes              → Create FTS index
//!   GET    /api/v1/fts/indexes              → List all FTS indexes
//!   GET    /api/v1/fts/indexes/:name        → Get FTS index info
//!   DELETE /api/v1/fts/indexes/:name        → Drop FTS index
//!   POST   /api/v1/fts/indexes/:name/build  → Build/rebuild FTS index
//!   POST   /api/v1/fts/indexes/:name/search → Search FTS index
//!   POST   /api/v1/fts/search               → Search (Couchbase-compat)

use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

use super::AppState;
use crate::fts::engine::{AnalyzerConfig, FtsFieldMapping, FtsSearchRequest};

/// Request body for creating an FTS index
#[derive(Debug, Deserialize)]
pub struct CreateFtsIndexRequest {
    pub name: String,
    pub bucket: String,
    #[serde(default)]
    pub fields: Vec<FtsFieldMapping>,
    pub analyzer: Option<AnalyzerConfig>,
}

/// POST /api/v1/fts/indexes — Create an FTS index
pub async fn create_fts_index(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateFtsIndexRequest>,
) -> Json<serde_json::Value> {
    match state.fts_engine.create_index(req.name, req.bucket, req.fields, req.analyzer) {
        Ok(def) => {
            // Auto-build the index
            let idx_name = def.name.clone();
            let fts_engine = state.fts_engine.clone();
            tokio::spawn(async move {
                match fts_engine.build_index(&idx_name) {
                    Ok(count) => {
                        tracing::info!("FTS index '{}' built with {} docs", idx_name, count);
                        // Persist definitions
                        if let Some(data_dir) = fts_engine.storage.data_dir() {
                            let _ = fts_engine.save_definitions(&data_dir);
                        }
                    }
                    Err(e) => tracing::warn!("FTS index '{}' build failed: {}", idx_name, e),
                }
            });

            Json(serde_json::json!({
                "status": "ok",
                "index": def,
                "message": "Index created, building in background"
            }))
        }
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// GET /api/v1/fts/indexes — List all FTS indexes
pub async fn list_fts_indexes(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let indexes = state.fts_engine.list_indexes();
    let count = indexes.len();
    Json(serde_json::json!({
        "status": "ok",
        "indexes": indexes,
        "count": count
    }))
}

/// GET /api/v1/fts/indexes/:name — Get FTS index info
pub async fn get_fts_index(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    match state.fts_engine.get_index(&name) {
        Some(def) => Json(serde_json::json!({
            "status": "ok",
            "index": def
        })),
        None => Json(serde_json::json!({
            "status": "error",
            "error": format!("FTS index '{}' not found", name)
        })),
    }
}

/// DELETE /api/v1/fts/indexes/:name — Drop FTS index
pub async fn drop_fts_index(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    match state.fts_engine.drop_index(&name) {
        Ok(()) => {
            // Persist definitions
            if let Some(data_dir) = state.fts_engine.storage.data_dir() {
                let _ = state.fts_engine.save_definitions(&data_dir);
            }
            Json(serde_json::json!({
                "status": "ok",
                "message": format!("FTS index '{}' dropped", name)
            }))
        }
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// POST /api/v1/fts/indexes/:name/build — Build/rebuild FTS index
pub async fn build_fts_index(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    match state.fts_engine.build_index(&name) {
        Ok(count) => {
            // Persist definitions
            if let Some(data_dir) = state.fts_engine.storage.data_dir() {
                let _ = state.fts_engine.save_definitions(&data_dir);
            }
            Json(serde_json::json!({
                "status": "ok",
                "message": format!("FTS index '{}' rebuilt with {} documents", name, count),
                "doc_count": count
            }))
        }
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "error": e
        })),
    }
}

/// POST /api/v1/fts/indexes/:name/search — Search FTS index
pub async fn search_fts_index(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(mut req): Json<FtsSearchRequest>,
) -> Json<serde_json::Value> {
    req.index = name;
    match state.fts_engine.search(&req) {
        Ok(result) => Json(serde_json::json!(result)),
        Err(e) => Json(serde_json::json!({
            "status": "errors",
            "errors": [{"msg": e}],
            "total_hits": 0,
            "hits": [],
            "took_ms": 0
        })),
    }
}

/// POST /api/v1/fts/search — Couchbase-compatible FTS search endpoint
pub async fn search_fts(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FtsSearchRequest>,
) -> Json<serde_json::Value> {
    match state.fts_engine.search(&req) {
        Ok(result) => Json(serde_json::json!(result)),
        Err(e) => Json(serde_json::json!({
            "status": "errors",
            "errors": [{"msg": e}],
            "total_hits": 0,
            "hits": [],
            "took_ms": 0
        })),
    }
}
