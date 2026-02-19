use crate::audit::logger::AuditEventType;
use crate::cluster::chronicle::{ConfigChangeType, ConfigProposal};
use crate::cluster::node::NodeStatus;
use crate::error::{NosqlError, Result};
use crate::storage::engine::{
    BucketConfig, BucketType, CompressionMode, ConflictResolutionType, EvictionPolicy,
};
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

use super::AppState;

/// Replicate a bucket/scope/collection change to all other healthy nodes in the cluster.
/// `method` is "POST" or "DELETE", `path` is the REST path, `body` is optional JSON payload.
async fn replicate_to_cluster(
    state: &Arc<AppState>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) {
    let nodes = state.cluster.list_nodes().await;
    let self_name = &state.config.node_name;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    for node in &nodes {
        if node.name == *self_name {
            continue;
        }
        if node.status != NodeStatus::Healthy && node.status != NodeStatus::Warmup {
            continue;
        }

        let url = format!("{}{}", node.base_url(), path);
        let req = match method {
            "DELETE" => client.delete(&url),
            _ => {
                let mut r = client.post(&url);
                if let Some(ref b) = body {
                    r = r.json(b);
                }
                r
            }
        };

        // Add internal header so remote nodes don't re-replicate
        let req = req.header("X-Internal-Replicate", "true");

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                info!("Replicated {} {} to node '{}'", method, path, node.name);
            }
            Ok(resp) => {
                warn!(
                    "Replication {} {} to node '{}' failed: HTTP {}",
                    method, path, node.name, resp.status()
                );
            }
            Err(e) => {
                warn!(
                    "Replication {} {} to node '{}' error: {}",
                    method, path, node.name, e
                );
            }
        }
    }
}

// ---- Request/Response types ----

#[derive(Debug, Deserialize)]
pub struct CreateBucketRequest {
    pub name: String,
    #[serde(default = "default_bucket_type")]
    pub bucket_type: String,
    #[serde(default = "default_ram_quota")]
    pub ram_quota_mb: u64,
    #[serde(default = "default_replicas")]
    pub num_replicas: u8,
    #[serde(default)]
    pub flush_enabled: bool,
    #[serde(default = "default_conflict_resolution")]
    pub conflict_resolution: String,
    pub max_ttl: Option<u64>,
}

fn default_bucket_type() -> String { "couchbase".to_string() }
fn default_ram_quota() -> u64 { 256 }
fn default_replicas() -> u8 { 1 }
fn default_conflict_resolution() -> String { "seqno".to_string() }

#[derive(Debug, Serialize)]
pub struct BucketResponse {
    pub name: String,
    pub bucket_type: String,
    pub ram_quota_mb: u64,
    pub num_replicas: u8,
    pub num_vbuckets: u16,
    pub flush_enabled: bool,
    pub conflict_resolution: String,
    pub max_ttl: Option<u64>,
    pub document_count: usize,
    pub size_bytes: usize,
    pub scopes: Vec<ScopeResponse>,
}

#[derive(Debug, Serialize)]
pub struct ScopeResponse {
    pub name: String,
    pub collections: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScopeRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub max_ttl: Option<u64>,
}

// ---- Handlers ----

/// POST /api/v1/buckets - Create a bucket
pub async fn create_bucket(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateBucketRequest>,
) -> Result<Json<BucketResponse>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    let bucket_type = match req.bucket_type.to_lowercase().as_str() {
        "couchbase" => BucketType::Couchbase,
        "ephemeral" => BucketType::Ephemeral,
        "memcached" => BucketType::Memcached,
        _ => return Err(NosqlError::InvalidRequest(format!("Invalid bucket type: {}", req.bucket_type))),
    };

    let conflict_resolution = match req.conflict_resolution.to_lowercase().as_str() {
        "seqno" | "sequence" | "sequencenumber" => ConflictResolutionType::SequenceNumber,
        "timestamp" | "lww" => ConflictResolutionType::Timestamp,
        _ => return Err(NosqlError::InvalidRequest(format!("Invalid conflict resolution: {}", req.conflict_resolution))),
    };

    let config = BucketConfig {
        name: req.name.clone(),
        bucket_type,
        ram_quota_mb: req.ram_quota_mb,
        num_replicas: req.num_replicas,
        num_vbuckets: state.config.num_vbuckets,
        flush_enabled: req.flush_enabled,
        conflict_resolution,
        max_ttl: req.max_ttl,
        compression_mode: CompressionMode::Passive,
        eviction_policy: EvictionPolicy::ValueOnly,
    };

    state.storage.create_bucket(config)?;

    state.audit_logger.log_full(
        AuditEventType::BucketCreated,
        format!("Bucket '{}' created", req.name),
        None, None, Some(req.name.clone()), None,
    );

    // Record bucket creation in Chronicle (metadata consensus)
    let _ = state.cluster.get_chronicle().propose(ConfigProposal {
        change_type: ConfigChangeType::BucketCreate,
        payload: serde_json::json!({
            "name": req.name,
            "bucket_type": req.bucket_type,
            "ram_quota_mb": req.ram_quota_mb,
            "num_replicas": req.num_replicas,
        }),
        proposed_by: state.config.node_name.clone(),
    });

    // Replicate bucket creation to all other nodes in the cluster
    let bucket_name = req.name.clone();
    if !is_internal {
        let replicate_body = serde_json::json!({
            "name": &bucket_name,
            "bucket_type": &req.bucket_type,
            "ram_quota_mb": req.ram_quota_mb,
            "num_replicas": req.num_replicas,
            "flush_enabled": req.flush_enabled,
            "conflict_resolution": &req.conflict_resolution,
            "max_ttl": req.max_ttl,
        });
        replicate_to_cluster(&state, "POST", "/api/v1/buckets", Some(replicate_body)).await;
    }

    get_bucket(State(state), Path(bucket_name)).await
}

/// GET /api/v1/buckets - List all buckets
pub async fn list_buckets(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<BucketResponse>>> {
    let configs = state.storage.list_buckets();
    let mut responses = Vec::new();

    for config in configs {
        if let Ok(bucket) = state.storage.get_bucket(&config.name) {
            let scopes = bucket.list_scopes();
            responses.push(BucketResponse {
                name: config.name,
                bucket_type: format!("{:?}", config.bucket_type),
                ram_quota_mb: config.ram_quota_mb,
                num_replicas: config.num_replicas,
                num_vbuckets: config.num_vbuckets,
                flush_enabled: config.flush_enabled,
                conflict_resolution: format!("{:?}", config.conflict_resolution),
                max_ttl: config.max_ttl,
                document_count: bucket.document_count(),
                size_bytes: bucket.total_size_bytes(),
                scopes: scopes.iter().map(|s| ScopeResponse {
                    name: s.name.clone(),
                    collections: s.collections.clone(),
                }).collect(),
            });
        }
    }

    Ok(Json(responses))
}

/// GET /api/v1/buckets/:name - Get bucket details
pub async fn get_bucket(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<BucketResponse>> {
    let bucket = state.storage.get_bucket(&name)?;
    let config = &bucket.config;
    let scopes = bucket.list_scopes();

    Ok(Json(BucketResponse {
        name: config.name.clone(),
        bucket_type: format!("{:?}", config.bucket_type),
        ram_quota_mb: config.ram_quota_mb,
        num_replicas: config.num_replicas,
        num_vbuckets: config.num_vbuckets,
        flush_enabled: config.flush_enabled,
        conflict_resolution: format!("{:?}", config.conflict_resolution),
        max_ttl: config.max_ttl,
        document_count: bucket.document_count(),
        size_bytes: bucket.total_size_bytes(),
        scopes: scopes.iter().map(|s| ScopeResponse {
            name: s.name.clone(),
            collections: s.collections.clone(),
        }).collect(),
    }))
}

/// DELETE /api/v1/buckets/:name - Delete a bucket
pub async fn delete_bucket(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    state.storage.delete_bucket(&name)?;
    state.audit_logger.log_full(
        AuditEventType::BucketDeleted,
        format!("Bucket '{}' deleted", name),
        None, None, Some(name.clone()), None,
    );

    // Record bucket deletion in Chronicle
    let _ = state.cluster.get_chronicle().propose(ConfigProposal {
        change_type: ConfigChangeType::BucketDelete,
        payload: serde_json::json!({"name": name}),
        proposed_by: state.config.node_name.clone(),
    });

    if !is_internal {
        let path = format!("/api/v1/buckets/{}", name);
        replicate_to_cluster(&state, "DELETE", &path, None).await;
    }

    Ok(Json(serde_json::json!({"status": "ok", "message": format!("Bucket '{}' deleted", name)})))
}

/// POST /api/v1/buckets/:name/flush - Flush a bucket
pub async fn flush_bucket(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    let bucket = state.storage.get_bucket(&name)?;
    bucket.flush()?;
    state.audit_logger.log_full(
        AuditEventType::BucketFlushed,
        format!("Bucket '{}' flushed", name),
        None, None, Some(name.clone()), None,
    );

    if !is_internal {
        let path = format!("/api/v1/buckets/{}/flush", name);
        replicate_to_cluster(&state, "POST", &path, None).await;
    }

    Ok(Json(serde_json::json!({"status": "ok", "message": format!("Bucket '{}' flushed", name)})))
}

/// POST /api/v1/buckets/:bucket/scopes - Create a scope
pub async fn create_scope(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
    Json(req): Json<CreateScopeRequest>,
) -> Result<Json<serde_json::Value>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    let bucket = state.storage.get_bucket(&bucket_name)?;
    bucket.create_scope(req.name.clone())?;

    // Record in Chronicle
    let _ = state.cluster.get_chronicle().propose(ConfigProposal {
        change_type: ConfigChangeType::ScopeCreate,
        payload: serde_json::json!({"bucket": bucket_name, "scope": req.name}),
        proposed_by: state.config.node_name.clone(),
    });

    if !is_internal {
        let path = format!("/api/v1/buckets/{}/scopes", bucket_name);
        let body = serde_json::json!({"name": req.name});
        replicate_to_cluster(&state, "POST", &path, Some(body)).await;
    }

    Ok(Json(serde_json::json!({"status": "ok", "message": format!("Scope '{}' created", req.name)})))
}

/// GET /api/v1/buckets/:bucket/scopes - List scopes
pub async fn list_scopes(
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> Result<Json<Vec<ScopeResponse>>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    let scopes = bucket.list_scopes();
    Ok(Json(scopes.iter().map(|s| ScopeResponse {
        name: s.name.clone(),
        collections: s.collections.clone(),
    }).collect()))
}

/// DELETE /api/v1/buckets/:bucket/scopes/:scope - Delete a scope
pub async fn delete_scope(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    let bucket = state.storage.get_bucket(&bucket_name)?;
    bucket.delete_scope(&scope_name)?;

    let _ = state.cluster.get_chronicle().propose(ConfigProposal {
        change_type: ConfigChangeType::ScopeDelete,
        payload: serde_json::json!({"bucket": bucket_name, "scope": scope_name}),
        proposed_by: state.config.node_name.clone(),
    });

    if !is_internal {
        let path = format!("/api/v1/buckets/{}/scopes/{}", bucket_name, scope_name);
        replicate_to_cluster(&state, "DELETE", &path, None).await;
    }

    Ok(Json(serde_json::json!({"status": "ok", "message": format!("Scope '{}' deleted", scope_name)})))
}

/// POST /api/v1/buckets/:bucket/scopes/:scope/collections - Create a collection
pub async fn create_collection(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name)): Path<(String, String)>,
    Json(req): Json<CreateCollectionRequest>,
) -> Result<Json<serde_json::Value>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    let bucket = state.storage.get_bucket(&bucket_name)?;
    bucket.create_collection(&scope_name, req.name.clone())?;

    let _ = state.cluster.get_chronicle().propose(ConfigProposal {
        change_type: ConfigChangeType::CollectionCreate,
        payload: serde_json::json!({"bucket": bucket_name, "scope": scope_name, "collection": req.name}),
        proposed_by: state.config.node_name.clone(),
    });

    if !is_internal {
        let path = format!("/api/v1/buckets/{}/scopes/{}/collections", bucket_name, scope_name);
        let body = serde_json::json!({"name": req.name, "max_ttl": req.max_ttl});
        replicate_to_cluster(&state, "POST", &path, Some(body)).await;
    }

    Ok(Json(serde_json::json!({"status": "ok", "message": format!("Collection '{}' created in scope '{}'", req.name, scope_name)})))
}

/// DELETE /api/v1/buckets/:bucket/scopes/:scope/collections/:collection - Delete a collection
pub async fn delete_collection(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>> {
    let is_internal = headers.get("X-Internal-Replicate").is_some();
    let bucket = state.storage.get_bucket(&bucket_name)?;
    bucket.delete_collection(&scope_name, &collection_name)?;

    let _ = state.cluster.get_chronicle().propose(ConfigProposal {
        change_type: ConfigChangeType::CollectionDelete,
        payload: serde_json::json!({"bucket": bucket_name, "scope": scope_name, "collection": collection_name}),
        proposed_by: state.config.node_name.clone(),
    });

    if !is_internal {
        let path = format!("/api/v1/buckets/{}/scopes/{}/collections/{}", bucket_name, scope_name, collection_name);
        replicate_to_cluster(&state, "DELETE", &path, None).await;
    }

    Ok(Json(serde_json::json!({"status": "ok", "message": format!("Collection '{}' deleted", collection_name)})))
}
