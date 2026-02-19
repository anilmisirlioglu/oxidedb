use crate::auth::middleware::{AuthUser, require_permission};
use crate::auth::rbac::Permission;
use crate::cluster::durability::{DurabilityLevel, DurabilityRequirement, DurabilityToken};
use crate::error::{NosqlError, Result};
#[allow(unused_imports)]
use crate::storage::vbucket::hash_to_vbucket;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};

use super::AppState;

// ---- Request/Response types ----

#[derive(Debug, Serialize, Deserialize)]
pub struct UpsertDocumentRequest {
    pub value: serde_json::Value,
    pub expiry: Option<u64>,
    pub flags: Option<u32>,
    pub cas: Option<u64>,
    /// Durability level for this write (Couchbase-style).
    /// Options: "none" (default), "majority", "majority_and_persist_to_active", "persist_to_majority"
    #[serde(default)]
    pub durability_level: Option<DurabilityLevel>,
    /// Durability timeout in milliseconds (default: 2500ms)
    #[serde(default)]
    pub durability_timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TouchDocumentRequest {
    pub expiry: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DocumentResponse {
    pub key: String,
    pub value: serde_json::Value,
    pub cas: u64,
    pub rev_id: u64,
    pub seq_no: u64,
    pub expiry: Option<String>,
    pub flags: u32,
    pub vbucket_id: u16,
    pub created_at: String,
    pub updated_at: String,
    pub served_by: String,
}

#[derive(Debug, Deserialize)]
pub struct DocQueryParams {
    pub cas: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ListDocsParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub prefix: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListDocsResponse {
    pub documents: Vec<DocumentSummary>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
    pub has_more: bool,
}

#[derive(Debug, Serialize)]
pub struct DocumentSummary {
    pub key: String,
    pub cas: u64,
    pub rev_id: u64,
    pub seq_no: u64,
    pub vbucket_id: u16,
    pub size_bytes: usize,
    pub updated_at: String,
    pub expiry: Option<String>,
    pub value_preview: serde_json::Value,
}

impl DocumentResponse {
    pub fn from_doc(doc: crate::storage::document::Document, node_name: String) -> Self {
        Self {
            key: doc.key,
            value: doc.value,
            cas: doc.cas,
            rev_id: doc.rev_id,
            seq_no: doc.seq_no,
            expiry: doc.expiry.map(|e| e.to_rfc3339()),
            flags: doc.flags,
            vbucket_id: doc.vbucket_id,
            created_at: doc.created_at.to_rfc3339(),
            updated_at: doc.updated_at.to_rfc3339(),
            served_by: node_name,
        }
    }
}

// For backward compat with From<Document>
impl From<crate::storage::document::Document> for DocumentResponse {
    fn from(doc: crate::storage::document::Document) -> Self {
        Self {
            key: doc.key,
            value: doc.value,
            cas: doc.cas,
            rev_id: doc.rev_id,
            seq_no: doc.seq_no,
            expiry: doc.expiry.map(|e| e.to_rfc3339()),
            flags: doc.flags,
            vbucket_id: doc.vbucket_id,
            created_at: doc.created_at.to_rfc3339(),
            updated_at: doc.updated_at.to_rfc3339(),
            served_by: "local".to_string(),
        }
    }
}

// ---- Partition-aware helpers ----

/// Check if a key's vBucket is local to this node.
/// If not, return the URL of the node that owns it.
async fn check_partition(
    state: &AppState,
    bucket_name: &str,
    key: &str,
) -> Result<Option<String>> {
    let bucket = state.storage.get_bucket(bucket_name)?;
    let vb_id = bucket.get_vbucket_id(key);
    let is_local = state.cluster.is_vbucket_local(vb_id).await;

    if is_local {
        Ok(None)
    } else {
        // Get the node that owns this vBucket
        let owner = state
            .cluster
            .get_active_node_for_vbucket(vb_id)
            .await
            .ok_or_else(|| {
                NosqlError::Internal(format!("No active node for vBucket {}", vb_id))
            })?;
        let url = state.cluster.get_node_url(&owner).await.ok_or_else(|| {
            NosqlError::Internal(format!("Node '{}' URL not found", owner))
        })?;
        debug!(
            "Key '{}' → vBucket {} → forwarding to node '{}' at {}",
            key, vb_id, owner, url
        );
        Ok(Some(url))
    }
}

/// Forward a GET request to a remote node
async fn forward_get(
    base_url: &str,
    bucket: &str,
    scope: &str,
    collection: &str,
    key: &str,
) -> Result<DocumentResponse> {
    let url = format!(
        "{}/api/v1/docs/{}/scopes/{}/collections/{}/docs/{}",
        base_url, bucket, scope, collection, key
    );
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("X-Forwarded", "true")
        .send()
        .await
        .map_err(|e| NosqlError::Internal(format!("Forward GET failed: {}", e)))?;

    if resp.status().is_success() {
        let doc: DocumentResponse = resp
            .json()
            .await
            .map_err(|e| NosqlError::Internal(format!("Forward response parse error: {}", e)))?;
        Ok(doc)
    } else {
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body["error"].as_str().unwrap_or("Unknown error");
        if status == 404 {
            Err(NosqlError::DocumentNotFound(key.to_string()))
        } else {
            Err(NosqlError::Internal(format!("Remote node error: {}", msg)))
        }
    }
}

/// Forward a PUT request to a remote node
async fn forward_put(
    base_url: &str,
    bucket: &str,
    scope: &str,
    collection: &str,
    key: &str,
    body: &UpsertDocumentRequest,
) -> Result<DocumentResponse> {
    let url = format!(
        "{}/api/v1/docs/{}/scopes/{}/collections/{}/docs/{}",
        base_url, bucket, scope, collection, key
    );
    let client = reqwest::Client::new();
    let resp = client
        .put(&url)
        .header("X-Forwarded", "true")
        .json(body)
        .send()
        .await
        .map_err(|e| NosqlError::Internal(format!("Forward PUT failed: {}", e)))?;

    if resp.status().is_success() {
        let doc: DocumentResponse = resp
            .json()
            .await
            .map_err(|e| NosqlError::Internal(format!("Forward response parse error: {}", e)))?;
        Ok(doc)
    } else {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body["error"].as_str().unwrap_or("Unknown error");
        Err(NosqlError::Internal(format!("Remote node error: {}", msg)))
    }
}

/// Forward a DELETE request to a remote node
async fn forward_delete(
    base_url: &str,
    bucket: &str,
    scope: &str,
    collection: &str,
    key: &str,
    cas: Option<u64>,
) -> Result<serde_json::Value> {
    let mut url = format!(
        "{}/api/v1/docs/{}/scopes/{}/collections/{}/docs/{}",
        base_url, bucket, scope, collection, key
    );
    if let Some(cas_val) = cas {
        url = format!("{}?cas={}", url, cas_val);
    }
    let client = reqwest::Client::new();
    let resp = client
        .delete(&url)
        .header("X-Forwarded", "true")
        .send()
        .await
        .map_err(|e| NosqlError::Internal(format!("Forward DELETE failed: {}", e)))?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| NosqlError::Internal(format!("Forward response parse error: {}", e)))?;
        Ok(body)
    } else {
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body["error"].as_str().unwrap_or("Unknown error");
        if status == 404 {
            Err(NosqlError::DocumentNotFound(key.to_string()))
        } else {
            Err(NosqlError::Internal(format!("Remote node error: {}", msg)))
        }
    }
}

// ---- Handlers ----

/// GET /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs
/// List documents with pagination, optional prefix filter.
/// Only returns documents from vBuckets owned by this node (partition-aware).
pub async fn list_documents(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name)): Path<(String, String, String)>,
    Query(params): Query<ListDocsParams>,
) -> Result<Json<ListDocsResponse>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    bucket.validate_path_public(&scope_name, &collection_name)?;

    let limit = params.limit.unwrap_or(50).min(500);
    let offset = params.offset.unwrap_or(0);

    // Scan all documents from the bucket
    let mut all_docs = bucket.scan_all_documents();

    // On multi-node clusters, filter to only locally-owned vBuckets
    if !state.cluster.is_single_node().await {
        let local_vbuckets = state.cluster.get_partition_map().await
            .active_vbuckets_for(&state.config.node_name);
        all_docs.retain(|doc| local_vbuckets.contains(&doc.vbucket_id));
    }

    // Filter by prefix if provided
    if let Some(ref prefix) = params.prefix {
        all_docs.retain(|doc| doc.key.starts_with(prefix.as_str()));
    }

    // Sort by key for consistent pagination
    all_docs.sort_by(|a, b| a.key.cmp(&b.key));

    let total = all_docs.len();
    let has_more = offset + limit < total;

    // Apply pagination
    let page: Vec<_> = all_docs
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|doc| {
            // Create a preview of the value (truncate large objects)
            let value_preview = truncate_json_preview(&doc.value, 3);
            let size_bytes = serde_json::to_string(&doc.value)
                .map(|s| s.len())
                .unwrap_or(0);
            DocumentSummary {
                key: doc.key,
                cas: doc.cas,
                rev_id: doc.rev_id,
                seq_no: doc.seq_no,
                vbucket_id: doc.vbucket_id,
                size_bytes,
                updated_at: doc.updated_at.to_rfc3339(),
                expiry: doc.expiry.map(|e| e.to_rfc3339()),
                value_preview,
            }
        })
        .collect();

    Ok(Json(ListDocsResponse {
        documents: page,
        total,
        limit,
        offset,
        has_more,
    }))
}

/// Truncate a JSON value to show a preview (limit number of keys shown)
fn truncate_json_preview(value: &serde_json::Value, max_keys: usize) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut preview = serde_json::Map::new();
            for (i, (k, v)) in map.iter().enumerate() {
                if i >= max_keys {
                    preview.insert(
                        "...".to_string(),
                        serde_json::json!(format!("{} more fields", map.len() - max_keys)),
                    );
                    break;
                }
                // Truncate string values
                let truncated = match v {
                    serde_json::Value::String(s) if s.len() > 60 => {
                        serde_json::Value::String(format!("{}...", &s[..57]))
                    }
                    serde_json::Value::Object(_) => serde_json::json!("{...}"),
                    serde_json::Value::Array(arr) => {
                        serde_json::json!(format!("[{} items]", arr.len()))
                    }
                    other => other.clone(),
                };
                preview.insert(k.clone(), truncated);
            }
            serde_json::Value::Object(preview)
        }
        other => other.clone(),
    }
}

/// GET /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key
pub async fn get_document(
    State(state): State<Arc<AppState>>,
    auth_user: AuthUser,
    Path((bucket_name, scope_name, collection_name, key)): Path<(String, String, String, String)>,
) -> Result<Json<DocumentResponse>> {
    // RBAC: check BucketRead permission
    require_permission(&auth_user, &state.rbac, &Permission::BucketRead(bucket_name.clone()))
        .map_err(|e| NosqlError::InvalidRequest(e.message))?;

    // On multi-node clusters, check if vBucket is on this node
    if !state.cluster.is_single_node().await {
    if let Some(remote_url) = check_partition(&state, &bucket_name, &key).await? {
        let doc = forward_get(&remote_url, &bucket_name, &scope_name, &collection_name, &key).await?;
        return Ok(Json(doc));
        }
    }

    let bucket = state.storage.get_bucket(&bucket_name)?;
    let doc = bucket.get(&scope_name, &collection_name, &key)?;
    let node_name = state.config.node_name.clone();
    Ok(Json(DocumentResponse::from_doc(doc, node_name)))
}

/// PUT /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key
pub async fn upsert_document(
    State(state): State<Arc<AppState>>,
    auth_user: AuthUser,
    Path((bucket_name, scope_name, collection_name, key)): Path<(String, String, String, String)>,
    Json(req): Json<UpsertDocumentRequest>,
) -> Result<Json<DocumentResponse>> {
    // RBAC: check BucketWrite permission
    require_permission(&auth_user, &state.rbac, &Permission::BucketWrite(bucket_name.clone()))
        .map_err(|e| NosqlError::InvalidRequest(e.message))?;

    // On multi-node clusters, check if vBucket is on this node
    if !state.cluster.is_single_node().await {
    if let Some(remote_url) = check_partition(&state, &bucket_name, &key).await? {
        let doc = forward_put(&remote_url, &bucket_name, &scope_name, &collection_name, &key, &req).await?;
        return Ok(Json(doc));
    }
    }

    let durability_level = req.durability_level.unwrap_or(DurabilityLevel::None);
    let durability_timeout = req.durability_timeout_ms.unwrap_or(2500);

    let bucket = state.storage.get_bucket(&bucket_name)?;
    let doc = if let Some(cas) = req.cas {
        bucket.replace(&scope_name, &collection_name, &key, req.value, Some(cas))?
    } else {
        bucket.upsert(&scope_name, &collection_name, key, req.value, req.expiry)?
    };

    // Buffer mutation for WAL persistence
    let needs_flush = state.storage.buffer_mutation(&bucket_name, &doc);
    if needs_flush {
        let _ = state.storage.flush_wal_buffer();
    }

    // Update secondary indexes
    state.index_manager.on_document_upsert(&bucket_name, &doc);

    // Publish DCP mutation event (this triggers intra-cluster replication)
    state.dcp_engine.publish_mutation(
        &bucket_name,
        &scope_name,
        &collection_name,
        &doc.key,
        Some(&doc.value),
        doc.cas,
        doc.vbucket_id,
        doc.expiry.map(|e| {
            let now = chrono::Utc::now();
            if e > now { (e - now).num_seconds().max(0) as u32 } else { 0 }
        }).unwrap_or(0),
    );

    // Update FTS indexes
    state.fts_engine.on_document_upsert(&bucket_name, &doc.key, &doc.value);

    // Register durability token if durable write requested
    if durability_level != DurabilityLevel::None {
        let pmap = state.cluster.get_partition_map().await;
        let replica_nodes: Vec<String> = pmap
            .map
            .get(doc.vbucket_id as usize)
            .map(|entry| entry.replica_nodes.clone())
            .unwrap_or_default();

        let requirement = DurabilityRequirement {
            level: durability_level,
            timeout_ms: durability_timeout,
        };

        let token = DurabilityToken::new(
            doc.key.clone(),
            doc.vbucket_id,
            doc.cas,
            doc.seq_no,
            requirement,
            replica_nodes.clone(),
        );

        state.durability.register(token);

        // For MajorityAndPersistToActive, the active persist is satisfied
        // immediately since we already wrote to WAL
        if durability_level == DurabilityLevel::MajorityAndPersistToActive
            || durability_level == DurabilityLevel::PersistToMajority
        {
            state.durability.ack_persist(doc.vbucket_id, doc.cas);
        }

        // Wait for durability to be satisfied (with timeout)
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(durability_timeout);
        loop {
            if start.elapsed() >= timeout {
                warn!(
                    "Durability timeout for key '{}' (level={:?}, timeout={}ms)",
                    doc.key, durability_level, durability_timeout
                );
                // Durability timeout is ambiguous — write succeeded but durability not guaranteed
                break;
            }

            // Check if token was satisfied (removed by ACK processing)
            if state.durability.pending_count() == 0 {
                break;
            }

            // If single node, no replicas to wait for
            if replica_nodes.is_empty() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    let node_name = state.config.node_name.clone();
    Ok(Json(DocumentResponse::from_doc(doc, node_name)))
}

/// DELETE /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key
pub async fn delete_document(
    State(state): State<Arc<AppState>>,
    auth_user: AuthUser,
    Path((bucket_name, scope_name, collection_name, key)): Path<(String, String, String, String)>,
    Query(params): Query<DocQueryParams>,
) -> Result<Json<serde_json::Value>> {
    // RBAC: check BucketWrite permission
    require_permission(&auth_user, &state.rbac, &Permission::BucketWrite(bucket_name.clone()))
        .map_err(|e| NosqlError::InvalidRequest(e.message))?;

    // On multi-node clusters, check if vBucket is on this node
    if !state.cluster.is_single_node().await {
    if let Some(remote_url) = check_partition(&state, &bucket_name, &key).await? {
        let result = forward_delete(&remote_url, &bucket_name, &scope_name, &collection_name, &key, params.cas).await?;
        return Ok(Json(result));
        }
    }

    let bucket = state.storage.get_bucket(&bucket_name)?;
    let doc = bucket.delete(&scope_name, &collection_name, &key, params.cas)?;

    // Buffer delete mutation for WAL persistence
    let needs_flush = state.storage.buffer_mutation(&bucket_name, &doc);
    if needs_flush {
        let _ = state.storage.flush_wal_buffer();
    }

    // Update secondary indexes
    state.index_manager.on_document_delete(&bucket_name, &key);

    // Publish DCP deletion event
    state.dcp_engine.publish_deletion(
        &bucket_name,
        &scope_name,
        &collection_name,
        &key,
        doc.cas,
        doc.vbucket_id,
    );

    // Update FTS indexes
    state.fts_engine.on_document_delete(&bucket_name, &key);

    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Document '{}' deleted", key),
        "served_by": state.config.node_name
    })))
}

/// POST /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key/touch
pub async fn touch_document(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name, key)): Path<(String, String, String, String)>,
    Json(req): Json<TouchDocumentRequest>,
) -> Result<Json<DocumentResponse>> {
    // For touch, also check partition on multi-node clusters
    if !state.cluster.is_single_node().await {
    if let Some(remote_url) = check_partition(&state, &bucket_name, &key).await? {
        // Forward as a touch - we'll forward as a POST
        let url = format!(
            "{}/api/v1/docs/{}/scopes/{}/collections/{}/docs/{}/touch",
            remote_url, bucket_name, scope_name, collection_name, key
        );
        let client = reqwest::Client::new();
        let resp = client
            .post(&url)
            .header("X-Forwarded", "true")
            .json(&req)
            .send()
            .await
            .map_err(|e| NosqlError::Internal(format!("Forward TOUCH failed: {}", e)))?;

        if resp.status().is_success() {
            let doc: DocumentResponse = resp
                .json()
                .await
                .map_err(|e| NosqlError::Internal(format!("Forward response error: {}", e)))?;
            return Ok(Json(doc));
        } else {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let msg = body["error"].as_str().unwrap_or("Unknown error");
            return Err(NosqlError::Internal(format!("Remote node error: {}", msg)));
            }
        }
    }

    let bucket = state.storage.get_bucket(&bucket_name)?;
    let doc = bucket.touch(&scope_name, &collection_name, &key, req.expiry)?;
    let node_name = state.config.node_name.clone();
    Ok(Json(DocumentResponse::from_doc(doc, node_name)))
}

// =====================================================================
// Extended Attributes (XATTRs)
// =====================================================================

#[derive(Debug, Deserialize)]
pub struct XattrUpsertRequest {
    pub value: serde_json::Value,
    pub cas: Option<u64>,
}

/// GET /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key/xattrs
pub async fn list_xattrs(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name, key)): Path<(String, String, String, String)>,
) -> Result<Json<serde_json::Value>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    let xattrs = bucket.xattr_list(&scope_name, &collection_name, &key)?;
    Ok(Json(serde_json::json!({
        "key": key,
        "xattrs": xattrs,
    })))
}

/// GET /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key/xattrs/:path
pub async fn get_xattr(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name, key, xattr_path)): Path<(String, String, String, String, String)>,
) -> Result<Json<serde_json::Value>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    let value = bucket.xattr_get(&scope_name, &collection_name, &key, &xattr_path)?;
    Ok(Json(serde_json::json!({
        "key": key,
        "path": xattr_path,
        "value": value,
    })))
}

/// PUT /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key/xattrs/:path
pub async fn upsert_xattr(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name, key, xattr_path)): Path<(String, String, String, String, String)>,
    Json(req): Json<XattrUpsertRequest>,
) -> Result<Json<serde_json::Value>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    let doc = bucket.xattr_upsert(&scope_name, &collection_name, &key, &xattr_path, req.value, req.cas)?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "key": key,
        "path": xattr_path,
        "cas": doc.cas,
    })))
}

/// DELETE /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key/xattrs/:path
pub async fn delete_xattr(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, scope_name, collection_name, key, xattr_path)): Path<(String, String, String, String, String)>,
    Query(params): Query<DocQueryParams>,
) -> Result<Json<serde_json::Value>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;
    let doc = bucket.xattr_remove(&scope_name, &collection_name, &key, &xattr_path, params.cas)?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "key": key,
        "path": xattr_path,
        "cas": doc.cas,
    })))
}

/// GET /api/v1/persistence/stats - Get persistence layer statistics
pub async fn persistence_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    if let Some(summary) = state.storage.persistence_stats() {
        Ok(Json(serde_json::json!(summary)))
    } else {
        Ok(Json(serde_json::json!({
            "status": "disabled",
            "message": "Persistence is not enabled"
        })))
    }
}

/// GET /api/v1/buckets/:bucket/stats - Get bucket statistics
pub async fn bucket_stats(
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let bucket = state.storage.get_bucket(&bucket_name)?;

    let high_seq_nos = bucket.get_high_seq_nos();
    let max_seq = high_seq_nos.iter().map(|(_, s)| s).max().copied().unwrap_or(0);
    let vb_doc_counts = bucket.get_vbucket_doc_counts();

    // Partition info
    let pmap = state.cluster.get_partition_map().await;
    let local_vbuckets = pmap.active_vbuckets_for(&state.config.node_name);
    let local_doc_count: usize = vb_doc_counts
        .iter()
        .filter(|(id, _)| local_vbuckets.contains(id))
        .map(|(_, c)| c)
        .sum();

    Ok(Json(serde_json::json!({
        "bucket": bucket_name,
        "document_count": bucket.document_count(),
        "local_document_count": local_doc_count,
        "size_bytes": bucket.total_size_bytes(),
        "num_vbuckets": bucket.config.num_vbuckets,
        "local_active_vbuckets": local_vbuckets.len(),
        "max_sequence_number": max_seq,
        "partition_map_revision": pmap.revision,
        "vbucket_seq_nos": high_seq_nos.iter()
            .filter(|(_, s)| *s > 0)
            .map(|(id, s)| serde_json::json!({"vbucket": id, "seq_no": s}))
            .collect::<Vec<_>>(),
    })))
}
