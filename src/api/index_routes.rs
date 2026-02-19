use crate::error::{NosqlError, Result};
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::AppState;

// ---- Request/Response types ----

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub bucket: String,
    pub fields: Vec<String>,
    #[serde(default)]
    pub condition: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexResponse {
    pub name: String,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    pub fields: Vec<String>,
    pub index_type: String,
    pub state: String,
    pub num_entries: usize,
    pub created_at: String,
    pub condition: Option<String>,
}

impl From<crate::storage::index::IndexDefinition> for IndexResponse {
    fn from(def: crate::storage::index::IndexDefinition) -> Self {
        Self {
            name: def.name,
            bucket: def.bucket,
            scope: def.scope,
            collection: def.collection,
            fields: def.fields,
            index_type: format!("{:?}", def.index_type),
            state: format!("{:?}", def.state),
            num_entries: def.num_entries,
            created_at: def.created_at,
            condition: def.condition,
        }
    }
}

// ---- Handlers ----

/// POST /api/v1/indexes - Create a new index
pub async fn create_index(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateIndexRequest>,
) -> Result<Json<serde_json::Value>> {
    // Validate bucket exists
    let bucket = state.storage.get_bucket(&req.bucket)?;

    // Create the index definition
    let def = state
        .index_manager
        .create_index(
            req.name.clone(),
            req.bucket.clone(),
            req.fields.clone(),
            req.condition.clone(),
        )
        .map_err(|e| NosqlError::InvalidRequest(e))?;

    // Build the index immediately by scanning all documents
    let all_docs = bucket.scan_all_documents();
    let num_entries = state
        .index_manager
        .build_index(&req.bucket, &req.name, &all_docs)
        .map_err(|e| NosqlError::Internal(e))?;

    let resp: IndexResponse = def.into();

    Ok(Json(serde_json::json!({
        "status": "created",
        "index": resp,
        "entries_indexed": num_entries,
        "message": format!("Index '{}' created and built with {} entries", req.name, num_entries)
    })))
}

/// GET /api/v1/indexes - List all indexes
pub async fn list_indexes(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<IndexResponse>>> {
    let indexes = state.index_manager.list_indexes(None);
    let responses: Vec<IndexResponse> = indexes.into_iter().map(|d| d.into()).collect();
    Ok(Json(responses))
}

/// GET /api/v1/indexes/:bucket - List indexes for a bucket
pub async fn list_bucket_indexes(
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> Result<Json<Vec<IndexResponse>>> {
    // Validate bucket exists
    let _ = state.storage.get_bucket(&bucket_name)?;

    let indexes = state.index_manager.list_indexes(Some(&bucket_name));
    let responses: Vec<IndexResponse> = indexes.into_iter().map(|d| d.into()).collect();
    Ok(Json(responses))
}

/// GET /api/v1/indexes/:bucket/:index_name - Get index details
pub async fn get_index(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, index_name)): Path<(String, String)>,
) -> Result<Json<IndexResponse>> {
    let def = state
        .index_manager
        .get_index(&bucket_name, &index_name)
        .ok_or_else(|| {
            NosqlError::InvalidRequest(format!(
                "Index '{}' not found on bucket '{}'",
                index_name, bucket_name
            ))
        })?;

    Ok(Json(def.into()))
}

/// DELETE /api/v1/indexes/:bucket/:index_name - Drop an index
pub async fn drop_index(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, index_name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>> {
    let def = state
        .index_manager
        .drop_index(&bucket_name, &index_name)
        .map_err(|e| NosqlError::InvalidRequest(e))?;

    Ok(Json(serde_json::json!({
        "status": "dropped",
        "index": def.name,
        "bucket": def.bucket,
        "message": format!("Index '{}' dropped from bucket '{}'", index_name, bucket_name)
    })))
}

/// POST /api/v1/indexes/:bucket/:index_name/rebuild - Rebuild an index
pub async fn rebuild_index(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, index_name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    let all_docs = bucket.scan_all_documents();

    let num_entries = state
        .index_manager
        .build_index(&bucket_name, &index_name, &all_docs)
        .map_err(|e| NosqlError::Internal(e))?;

    Ok(Json(serde_json::json!({
        "status": "rebuilt",
        "index": index_name,
        "bucket": bucket_name,
        "entries_indexed": num_entries,
        "message": format!("Index '{}' rebuilt with {} entries", index_name, num_entries)
    })))
}
