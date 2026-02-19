use crate::cluster::failover::{FailoverConfig, FailoverState};
use crate::cluster::node::ClusterNode;
use crate::cluster::partition::VBucketData;
#[allow(unused_imports)]
use crate::error::{NosqlError, Result};
use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

use super::AppState;

#[derive(Debug, Deserialize)]
pub struct AddNodeRequest {
    pub name: String,
    pub hostname: String,
    pub port: u16,
}

/// GET /api/v1/cluster - Get cluster info
pub async fn get_cluster_info(
    State(state): State<Arc<AppState>>,
) -> Result<Json<crate::cluster::ClusterInfo>> {
    let bucket_count = state.storage.list_buckets().len();
    let info = state.cluster.get_cluster_info(bucket_count).await;
    Ok(Json(info))
}

/// GET /api/v1/cluster/nodes - List nodes
pub async fn list_nodes(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ClusterNode>>> {
    let nodes = state.cluster.list_nodes().await;
    Ok(Json(nodes))
}

/// POST /api/v1/cluster/nodes - Add a node
pub async fn add_node(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddNodeRequest>,
) -> Result<Json<serde_json::Value>> {
    let node = ClusterNode::new_self(req.name.clone(), req.hostname.clone(), req.port);
    let transfers = state.cluster.add_node(node).await?;

    // Sync existing buckets/scopes/collections to the new node
    let new_node_url = format!("http://{}:{}", req.hostname, req.port);
    tokio::spawn({
        let storage = state.storage.clone();
        let node_url = new_node_url.clone();
        let node_name = req.name.clone();
        async move {
            sync_buckets_to_node(&storage, &node_url, &node_name).await;
        }
    });

    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Node '{}' added to cluster", req.name),
        "rebalance": {
            "transfers_needed": transfers.len(),
            "transfers": transfers
        }
    })))
}

/// Sync all existing buckets, scopes, and collections to a newly joined node.
async fn sync_buckets_to_node(
    storage: &crate::storage::engine::StorageEngine,
    node_url: &str,
    node_name: &str,
) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let buckets = storage.list_buckets();
    for bucket_info in &buckets {
        // Create bucket on the new node
        let create_url = format!("{}/api/v1/buckets", node_url);
        let body = serde_json::json!({
            "name": bucket_info.name,
            "bucket_type": format!("{:?}", bucket_info.bucket_type).to_lowercase(),
            "ram_quota_mb": bucket_info.ram_quota_mb,
            "num_replicas": bucket_info.num_replicas,
            "flush_enabled": bucket_info.flush_enabled,
            "conflict_resolution": format!("{:?}", bucket_info.conflict_resolution).to_lowercase(),
        });

        match client
            .post(&create_url)
            .header("X-Internal-Replicate", "true")
            .json(&body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("Synced bucket '{}' to node '{}'", bucket_info.name, node_name);
            }
            Ok(resp) => {
                tracing::warn!(
                    "Failed to sync bucket '{}' to '{}': HTTP {}",
                    bucket_info.name, node_name, resp.status()
                );
                continue;
            }
            Err(e) => {
                tracing::warn!("Failed to sync bucket '{}' to '{}': {}", bucket_info.name, node_name, e);
                continue;
            }
        }

        // Sync scopes and collections
        if let Ok(bucket) = storage.get_bucket(&bucket_info.name) {
            let scopes = bucket.list_scopes();
            for scope in &scopes {
                if scope.name == "_default" {
                    // Default scope is auto-created; just sync non-default collections
                    for coll in &scope.collections {
                        if *coll != "_default" {
                            let coll_url = format!(
                                "{}/api/v1/buckets/{}/scopes/_default/collections",
                                node_url, bucket_info.name
                            );
                            let _ = client
                                .post(&coll_url)
                                .header("X-Internal-Replicate", "true")
                                .json(&serde_json::json!({"name": coll}))
                                .send()
                                .await;
                        }
                    }
                    continue;
                }
                // Create scope
                let scope_url = format!(
                    "{}/api/v1/buckets/{}/scopes",
                    node_url, bucket_info.name
                );
                let _ = client
                    .post(&scope_url)
                    .header("X-Internal-Replicate", "true")
                    .json(&serde_json::json!({"name": scope.name}))
                    .send()
                    .await;

                // Create collections in scope
                for coll in &scope.collections {
                    if *coll == "_default" { continue; }
                    let coll_url = format!(
                        "{}/api/v1/buckets/{}/scopes/{}/collections",
                        node_url, bucket_info.name, scope.name
                    );
                    let _ = client
                        .post(&coll_url)
                        .header("X-Internal-Replicate", "true")
                        .json(&serde_json::json!({"name": coll}))
                        .send()
                        .await;
                }
            }
        }
    }

    if !buckets.is_empty() {
        tracing::info!(
            "Bucket sync to node '{}' complete: {} buckets synced",
            node_name, buckets.len()
        );
    }
}

/// DELETE /api/v1/cluster/nodes/:name - Remove a node
pub async fn remove_node(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let transfers = state.cluster.remove_node(&name).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Node '{}' removed from cluster", name),
        "rebalance": {
            "transfers_needed": transfers.len(),
            "transfers": transfers
        }
    })))
}

/// POST /api/v1/cluster/nodes/:name/heartbeat - Node heartbeat
pub async fn node_heartbeat(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    state.cluster.heartbeat(&name).await?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

// =========================================================================
// Partition management endpoints
// =========================================================================

/// GET /api/v1/cluster/partitions - Get the full partition map
pub async fn get_partition_map(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let pmap = state.cluster.get_partition_map().await;
    let node_info = state.cluster.get_node_partition_info().await;
    Ok(Json(serde_json::json!({
        "revision": pmap.revision,
        "num_vbuckets": pmap.num_vbuckets,
        "num_replicas": pmap.num_replicas,
        "nodes": node_info,
        "map": pmap.map,
    })))
}

/// GET /api/v1/cluster/partitions/summary - Get per-node summary
pub async fn get_partition_summary(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let pmap = state.cluster.get_partition_map().await;
    let node_info = state.cluster.get_node_partition_info().await;
    let rebalance = state.cluster.get_rebalance_status().await;

    Ok(Json(serde_json::json!({
        "revision": pmap.revision,
        "num_vbuckets": pmap.num_vbuckets,
        "rebalance_status": rebalance,
        "nodes": node_info.iter().map(|n| serde_json::json!({
            "node_name": n.node_name,
            "active_count": n.active_count,
            "replica_count": n.replica_count,
            "active_vbucket_range": format_vbucket_ranges(&n.active_vbuckets),
        })).collect::<Vec<_>>(),
    })))
}

/// POST /api/v1/cluster/rebalance - Trigger a manual rebalance
pub async fn trigger_rebalance(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let transfers = state.cluster.trigger_rebalance().await;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "Rebalance triggered",
        "transfers_needed": transfers.len(),
        "transfers": transfers,
    })))
}

/// GET /api/v1/cluster/rebalance - Get rebalance status
pub async fn get_rebalance_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let status = state.cluster.get_rebalance_status().await;
    let pending = state.cluster.get_pending_transfers().await;
    Ok(Json(serde_json::json!({
        "status": status,
        "pending_transfers": pending,
    })))
}

/// POST /api/v1/cluster/vbuckets/transfer - Receive vBucket data from another node
pub async fn receive_vbucket_data(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VBucketTransferPayload>,
) -> Result<Json<serde_json::Value>> {
    let vb_id = payload.data.vbucket_id;
    state
        .storage
        .import_vbucket(&payload.bucket, payload.data)?;

    // Mark transfer complete
    state.cluster.complete_transfer(vb_id).await;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("vBucket {} imported into bucket '{}'", vb_id, payload.bucket),
    })))
}

/// GET /api/v1/cluster/vbuckets/:bucket/:vbucket_id - Export vBucket data
pub async fn export_vbucket_data(
    State(state): State<Arc<AppState>>,
    Path((bucket_name, vbucket_id)): Path<(String, u16)>,
) -> Result<Json<VBucketData>> {
    let data = state.storage.export_vbucket(&bucket_name, vbucket_id)?;
    Ok(Json(data))
}

#[derive(Debug, Deserialize)]
pub struct VBucketTransferPayload {
    pub bucket: String,
    pub data: VBucketData,
}

// =========================================================================
// Failover endpoints
// =========================================================================

/// GET /api/v1/cluster/failover - Get failover state (config + events + status)
pub async fn get_failover_state(
    State(state): State<Arc<AppState>>,
) -> Result<Json<FailoverState>> {
    let fo_state = state.cluster.get_failover_state().await;
    Ok(Json(fo_state))
}

/// POST /api/v1/cluster/failover/config - Update failover configuration
pub async fn update_failover_config(
    State(state): State<Arc<AppState>>,
    Json(config): Json<FailoverConfig>,
) -> Result<Json<serde_json::Value>> {
    state.cluster.update_failover_config(config.clone()).await;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "Failover configuration updated",
        "config": config
    })))
}

/// POST /api/v1/cluster/failover/reset - Reset auto-failover counter
pub async fn reset_failover_quota(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    state.cluster.reset_failover_quota().await;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "Auto-failover quota reset"
    })))
}

#[derive(Debug, Deserialize)]
pub struct FailoverRequest {
    /// "graceful" or "hard"
    #[serde(default = "default_failover_type")]
    pub failover_type: String,
}

fn default_failover_type() -> String {
    "graceful".to_string()
}

/// POST /api/v1/cluster/failover/:node_name - Trigger manual failover for a node
pub async fn failover_node(
    State(state): State<Arc<AppState>>,
    Path(node_name): Path<String>,
    Json(req): Json<FailoverRequest>,
) -> Result<Json<serde_json::Value>> {
    let event = if req.failover_type == "hard" {
        state.cluster.hard_failover(&node_name).await?
    } else {
        state.cluster.graceful_failover(&node_name).await?
    };

    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Node '{}' failed over successfully", node_name),
        "event": event
    })))
}

/// POST /api/v1/cluster/failover/:node_name/recover - Recover a failed-over node
pub async fn recover_node(
    State(state): State<Arc<AppState>>,
    Path(node_name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let transfers = state.cluster.recover_node(&node_name).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Node '{}' recovered, rebalance triggered", node_name),
        "rebalance": {
            "transfers_needed": transfers.len(),
            "transfers": transfers
        }
    })))
}

// =================================================================
// Server Group Routes (Rack/Zone Awareness)
// =================================================================

#[derive(Debug, Deserialize)]
pub struct CreateServerGroupRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct MoveNodeToGroupRequest {
    pub node_name: String,
    pub group_name: String,
}

/// GET /api/v1/cluster/server-groups - List all server groups
pub async fn list_server_groups(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let groups = state.cluster.list_server_groups().await;
    Ok(Json(serde_json::json!({
        "groups": groups
    })))
}

/// POST /api/v1/cluster/server-groups - Create a new server group
pub async fn create_server_group(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateServerGroupRequest>,
) -> Result<Json<serde_json::Value>> {
    let group = state.cluster.create_server_group(&req.name).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Server group '{}' created", req.name),
        "group": group
    })))
}

/// DELETE /api/v1/cluster/server-groups/:name - Delete a server group
pub async fn delete_server_group(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    state.cluster.delete_server_group(&name).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Server group '{}' deleted", name)
    })))
}

/// POST /api/v1/cluster/server-groups/move - Move a node to a different group
pub async fn move_node_to_group(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MoveNodeToGroupRequest>,
) -> Result<Json<serde_json::Value>> {
    state.cluster.move_node_to_group(&req.node_name, &req.group_name).await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Node '{}' moved to group '{}'", req.node_name, req.group_name)
    })))
}

/// POST /api/v1/cluster/rebalance-groups - Rebalance with server group awareness
pub async fn rebalance_with_groups(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let transfers = state.cluster.rebalance_with_groups().await;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "Group-aware rebalance completed",
        "transfers": transfers.len(),
        "details": transfers
    })))
}

// ---- helpers ----

/// Format vBucket IDs into human-readable ranges like "0-255, 512-767"
fn format_vbucket_ranges(vbuckets: &[u16]) -> String {
    if vbuckets.is_empty() {
        return "none".to_string();
    }
    let mut sorted = vbuckets.to_vec();
    sorted.sort();

    let mut ranges = Vec::new();
    let mut start = sorted[0];
    let mut end = sorted[0];

    for &vb in &sorted[1..] {
        if vb == end + 1 {
            end = vb;
        } else {
            if start == end {
                ranges.push(format!("{}", start));
            } else {
                ranges.push(format!("{}-{}", start, end));
            }
            start = vb;
            end = vb;
        }
    }
    if start == end {
        ranges.push(format!("{}", start));
    } else {
        ranges.push(format!("{}-{}", start, end));
    }

    ranges.join(", ")
}
