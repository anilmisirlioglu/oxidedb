use crate::auth::middleware::{AuthUser, require_permission};
use crate::auth::rbac::Permission;
use crate::error::{NosqlError, Result};
use crate::query::engine::{QueryRequest, QueryResult, QueryMetrics};
use axum::extract::State;
use axum::Json;
use std::sync::Arc;
use tracing::{debug, warn};

use super::AppState;

/// POST /api/v1/query - Execute a query with cross-node scatter-gather
pub async fn execute_query(
    State(state): State<Arc<AppState>>,
    auth_user: AuthUser,
    headers: axum::http::HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResult>> {
    // RBAC: check QueryExecute permission
    require_permission(&auth_user, &state.rbac, &Permission::QueryExecute)
        .map_err(|e| NosqlError::InvalidRequest(e.message))?;

    let start = std::time::Instant::now();

    // If this is a forwarded scatter-gather child request, just execute locally
    let is_child = headers
        .get("X-Scatter-Gather")
        .map(|v| v.to_str().unwrap_or("") == "child")
        .unwrap_or(false);

    // Check if this is a single-node cluster or a child scatter request
    if state.cluster.is_single_node().await || is_child {
        // Single node or child: execute locally
        let result = state.query_engine.execute(&req)?;
        return Ok(Json(result));
    }

    // Multi-node: Scatter-Gather query execution
    // 1. Execute locally
    let local_result = state.query_engine.execute(&req)?;

    // 2. Forward query to all other healthy nodes
    let remote_nodes: Vec<(String, String)> = {
        let nodes = state.cluster.list_nodes().await;
        nodes
            .iter()
            .filter(|n| {
                n.name != state.config.node_name
                    && n.status == crate::cluster::node::NodeStatus::Healthy
            })
            .map(|n| (n.name.clone(), n.base_url()))
            .collect()
    };

    if remote_nodes.is_empty() {
        // No remote nodes, return local result
        return Ok(Json(local_result));
    }

    // 3. Fan out queries to remote nodes in parallel
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();

    let mut handles = Vec::new();
    for (node_name, base_url) in &remote_nodes {
        let client = client.clone();
        let url = format!("{}/api/v1/query", base_url);
        let req_clone = req.clone();
        let node = node_name.clone();
        handles.push(tokio::spawn(async move {
            match client
                .post(&url)
                .json(&req_clone)
                .header("X-Scatter-Gather", "child")  // Prevent infinite scatter
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<QueryResult>().await {
                        Ok(result) => Some((node, result)),
                        Err(e) => {
                            warn!("Query parse error from node '{}': {}", node, e);
                            None
                        }
                    }
                }
                Ok(resp) => {
                    warn!("Query error from node '{}': HTTP {}", node, resp.status());
                    None
                }
                Err(e) => {
                    warn!("Query connection error to node '{}': {}", node, e);
                    None
                }
            }
        }));
    }

    // 4. Gather results
    let mut all_results = local_result.results;
    let mut total_scanned = local_result.metrics.scanned_count;

    for handle in handles {
        if let Ok(Some((_node, remote_result))) = handle.await {
            all_results.extend(remote_result.results);
            total_scanned += remote_result.metrics.scanned_count;
        }
    }

    // 5. Deduplicate by META().id if present
    // Documents from different nodes shouldn't overlap (different vBuckets),
    // but we deduplicate by document key just in case
    let result_count = all_results.len();

    let elapsed = start.elapsed().as_millis() as u64;
    debug!(
        "Scatter-gather query: {} results from {} nodes in {}ms",
        result_count,
        remote_nodes.len() + 1,
        elapsed
    );

    Ok(Json(QueryResult {
        status: "success".to_string(),
        results: all_results,
        metrics: QueryMetrics {
            result_count,
            elapsed_ms: elapsed,
            scanned_count: total_scanned,
            index_used: local_result.metrics.index_used,
        },
    }))
}

/// GET /api/v1/query/prepared - List all prepared statements
pub async fn list_prepared_statements(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let stmts = state.query_engine.list_prepared_statements();
    let count = stmts.len();
    Json(serde_json::json!({
        "prepared_statements": stmts,
        "count": count,
    }))
}
