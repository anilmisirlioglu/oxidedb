//! Internal replication & consensus API endpoints
//!
//! These endpoints are used for intra-cluster communication:
//! - Receiving replicated mutations from active nodes (DCP)
//! - Chronicle metadata consensus (Prepare/ACK/Commit)
//! - Replication status and health monitoring
//! - Durability ACK processing

use crate::cluster::chronicle::{
    ChronicleSnapshot, CommitNotification, ConfigProposal, PrepareRequest, PrepareResponse,
};
use crate::dcp::replicator::{ReplicationAck, ReplicationBatch, ReplicationStatus};
use crate::error::Result;
use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

use super::AppState;

// ═══════════════════════════════════════════════════════════════════════
// DCP Replication endpoints
// ═══════════════════════════════════════════════════════════════════════

/// POST /api/v1/internal/replicate — Receive replicated mutations from an active node
pub async fn receive_replication(
    State(state): State<Arc<AppState>>,
    Json(batch): Json<ReplicationBatch>,
) -> Result<Json<ReplicationAck>> {
    let ack = state.replicator.receive_batch(&batch);
    Ok(Json(ack))
}

/// GET /api/v1/internal/replication/status — Get replication status
pub async fn get_replication_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ReplicationStatus>> {
    let status = state.replicator.get_status().await;
    Ok(Json(status))
}

/// POST /api/v1/internal/replication/pause — Pause replication
pub async fn pause_replication(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    state.replicator.pause();
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "Intra-cluster replication paused"
    })))
}

/// POST /api/v1/internal/replication/resume — Resume replication
pub async fn resume_replication(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    state.replicator.resume();
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "Intra-cluster replication resumed"
    })))
}

/// GET /api/v1/internal/orchestrator — Get orchestrator state
pub async fn get_orchestrator_state(
    State(state): State<Arc<AppState>>,
) -> Result<Json<crate::cluster::orchestrator::OrchestratorState>> {
    let orch = state.cluster.get_orchestrator_state().await;
    Ok(Json(orch))
}

/// GET /api/v1/internal/durability/stats — Get durability manager stats
pub async fn get_durability_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let pending = state.durability.pending_count();
    Ok(Json(serde_json::json!({
        "pending_durable_writes": pending,
    })))
}

// ═══════════════════════════════════════════════════════════════════════
// Chronicle Metadata Consensus endpoints
// ═══════════════════════════════════════════════════════════════════════

/// GET /api/v1/internal/chronicle — Get Chronicle state snapshot
pub async fn get_chronicle_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ChronicleSnapshot>> {
    let chronicle = state.cluster.get_chronicle();
    Ok(Json(chronicle.get_snapshot()))
}

/// POST /api/v1/internal/chronicle/propose — Propose a config change
/// If this node is the orchestrator, it will be committed directly (or prepared for multi-node).
/// If this node is a follower, the request should be forwarded to the orchestrator.
pub async fn propose_config_change(
    State(state): State<Arc<AppState>>,
    Json(proposal): Json<ConfigProposal>,
) -> Result<Json<serde_json::Value>> {
    let chronicle = state.cluster.get_chronicle();

    match chronicle.propose(proposal) {
        Ok(entry) => Ok(Json(serde_json::json!({
            "status": "ok",
            "entry_index": entry.index,
            "entry_status": entry.status,
            "term": entry.term,
        }))),
        Err(e) => {
            // Not the leader — return the orchestrator URL for forwarding
            let orch = state.cluster.get_orchestrator_state().await;
            Ok(Json(serde_json::json!({
                "status": "error",
                "message": e,
                "forward_to": orch.orchestrator_url,
                "orchestrator": orch.orchestrator_node,
            })))
        }
    }
}

/// POST /api/v1/internal/chronicle/prepare — Receive a Prepare from the orchestrator
/// Called on follower nodes to replicate a config entry.
pub async fn handle_chronicle_prepare(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PrepareRequest>,
) -> Result<Json<PrepareResponse>> {
    let chronicle = state.cluster.get_chronicle();
    let resp = chronicle.handle_prepare(&req);
    Ok(Json(resp))
}

/// POST /api/v1/internal/chronicle/ack — Process ACK from a follower (called on orchestrator)
#[derive(Debug, Deserialize)]
pub struct AckRequest {
    pub index: u64,
    pub node: String,
}

pub async fn handle_chronicle_ack(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AckRequest>,
) -> Result<Json<serde_json::Value>> {
    let chronicle = state.cluster.get_chronicle();

    match chronicle.process_ack(req.index, &req.node) {
        Some(entry) => Ok(Json(serde_json::json!({
            "status": "committed",
            "entry_index": entry.index,
            "change_type": entry.change_type,
            "acks": entry.acks,
        }))),
        None => Ok(Json(serde_json::json!({
            "status": "acknowledged",
            "message": "ACK recorded, awaiting majority"
        }))),
    }
}

/// POST /api/v1/internal/chronicle/commit — Notify follower of a committed entry
pub async fn handle_chronicle_commit(
    State(state): State<Arc<AppState>>,
    Json(notification): Json<CommitNotification>,
) -> Result<Json<serde_json::Value>> {
    let chronicle = state.cluster.get_chronicle();
    chronicle.apply_commit(&notification);
    Ok(Json(serde_json::json!({
        "status": "ok",
        "committed_index": notification.index,
    })))
}

/// GET /api/v1/internal/chronicle/log — Get committed config entries since a given index
#[derive(Debug, Deserialize)]
pub struct LogQuery {
    pub since: Option<u64>,
}

pub async fn get_chronicle_log(
    State(state): State<Arc<AppState>>,
    Query(params): Query<LogQuery>,
) -> Result<Json<serde_json::Value>> {
    let chronicle = state.cluster.get_chronicle();
    let since = params.since.unwrap_or(0);
    let entries = chronicle.get_committed_since(since);
    Ok(Json(serde_json::json!({
        "since_index": since,
        "entries": entries,
        "count": entries.len(),
        "current_commit_index": chronicle.get_commit_index(),
    })))
}
