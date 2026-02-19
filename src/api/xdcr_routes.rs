use crate::error::{NosqlError, Result};
use crate::xdcr::replicator::{
    RemoteClusterRef, ReplicationConfig, XdcrBatchRequest,
};
use axum::extract::{Path, State};
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::AppState;
use crate::storage::engine::ConflictResolutionType;

// ---- Request/Response types ----

#[derive(Debug, Deserialize)]
pub struct AddRemoteClusterRequest {
    pub name: String,
    pub hostname: String,
    pub port: u16,
    pub username: Option<String>,
    #[serde(default)]
    pub secure: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateReplicationRequest {
    pub source_bucket: String,
    pub target_cluster: String,
    pub target_bucket: String,
    #[serde(default = "default_conflict_resolution")]
    pub conflict_resolution: String,
    pub filter_expression: Option<String>,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub bidirectional: bool,
}

fn default_conflict_resolution() -> String {
    "seqno".to_string()
}
fn default_batch_size() -> usize {
    500
}

#[derive(Debug, Serialize)]
pub struct ReplicationResponse {
    pub id: String,
    pub source_bucket: String,
    pub target_cluster: String,
    pub target_bucket: String,
    pub status: String,
    pub conflict_resolution: String,
    pub filter_expression: Option<String>,
    pub bidirectional: bool,
    pub docs_replicated: u64,
    pub docs_failed: u64,
    pub total_conflicts: u64,
    pub last_replicated_at: Option<String>,
    pub error_message: Option<String>,
    pub created_at: String,
}

// ---- Handlers ----

/// POST /api/v1/xdcr/clusters - Add remote cluster reference
pub async fn add_remote_cluster(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddRemoteClusterRequest>,
) -> Result<Json<serde_json::Value>> {
    let cluster_ref = RemoteClusterRef {
        name: req.name.clone(),
        hostname: req.hostname,
        port: req.port,
        username: req.username,
        secure: req.secure,
        created_at: Utc::now(),
    };

    state.xdcr.add_remote_cluster(cluster_ref).await?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Remote cluster '{}' added", req.name)
    })))
}

/// GET /api/v1/xdcr/clusters - List remote clusters
pub async fn list_remote_clusters(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<RemoteClusterRef>>> {
    let clusters = state.xdcr.list_remote_clusters().await;
    Ok(Json(clusters))
}

/// DELETE /api/v1/xdcr/clusters/:name - Remove remote cluster
pub async fn remove_remote_cluster(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    state.xdcr.remove_remote_cluster(&name).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Remote cluster '{}' removed", name)
    })))
}

/// POST /api/v1/xdcr/replications - Create XDCR replication
pub async fn create_replication(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateReplicationRequest>,
) -> Result<Json<ReplicationResponse>> {
    let conflict_resolution = match req.conflict_resolution.to_lowercase().as_str() {
        "seqno" | "sequence" | "sequencenumber" => ConflictResolutionType::SequenceNumber,
        "timestamp" | "lww" => ConflictResolutionType::Timestamp,
        _ => {
            return Err(NosqlError::InvalidRequest(format!(
                "Invalid conflict resolution: {}",
                req.conflict_resolution
            )));
        }
    };

    let id = format!(
        "{}/{}/{}",
        req.source_bucket, req.target_cluster, req.target_bucket
    );

    let config = ReplicationConfig {
        id: id.clone(),
        source_bucket: req.source_bucket,
        target_cluster: req.target_cluster,
        target_bucket: req.target_bucket,
        conflict_resolution,
        filter_expression: req.filter_expression,
        batch_size: req.batch_size,
        bidirectional: req.bidirectional,
        created_at: Utc::now(),
    };

    state.xdcr.create_replication(config).await?;

    let replication = state.xdcr.get_replication(&id).await?;
    Ok(Json(replication_to_response(&replication)))
}

/// GET /api/v1/xdcr/replications - List replications
pub async fn list_replications(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ReplicationResponse>>> {
    let replications = state.xdcr.list_replications().await;
    Ok(Json(
        replications.iter().map(replication_to_response).collect(),
    ))
}

/// GET /api/v1/xdcr/replications/:id - Get replication details
pub async fn get_replication(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ReplicationResponse>> {
    let replication = state.xdcr.get_replication(&id).await?;
    Ok(Json(replication_to_response(&replication)))
}

/// DELETE /api/v1/xdcr/replications/:id - Delete replication
pub async fn delete_replication(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    state.xdcr.delete_replication(&id).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Replication '{}' deleted", id)
    })))
}

/// POST /api/v1/xdcr/replications/:id/pause - Pause replication
pub async fn pause_replication(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    state.xdcr.pause_replication(&id).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Replication '{}' paused", id)
    })))
}

/// POST /api/v1/xdcr/replications/:id/resume - Resume replication
pub async fn resume_replication(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    state.xdcr.resume_replication(&id).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Replication '{}' resumed", id)
    })))
}

/// POST /api/v1/xdcr/receive - Receive mutations from remote cluster (incoming XDCR)
pub async fn receive_mutations(
    State(state): State<Arc<AppState>>,
    Json(req): Json<XdcrBatchRequest>,
) -> Result<Json<crate::xdcr::replicator::XdcrBatchResponse>> {
    let response = state.xdcr.receive_batch(req).await?;
    Ok(Json(response))
}

fn replication_to_response(
    state: &crate::xdcr::replicator::ReplicationState,
) -> ReplicationResponse {
    ReplicationResponse {
        id: state.config.id.clone(),
        source_bucket: state.config.source_bucket.clone(),
        target_cluster: state.config.target_cluster.clone(),
        target_bucket: state.config.target_bucket.clone(),
        status: format!("{:?}", state.status),
        conflict_resolution: format!("{:?}", state.config.conflict_resolution),
        filter_expression: state.config.filter_expression.clone(),
        bidirectional: state.config.bidirectional,
        docs_replicated: state.stats.docs_replicated,
        docs_failed: state.stats.docs_failed,
        total_conflicts: state.stats.total_conflicts,
        last_replicated_at: state.last_replicated_at.map(|t| t.to_rfc3339()),
        error_message: state.error_message.clone(),
        created_at: state.config.created_at.to_rfc3339(),
    }
}
