//! Couchbase-compatible REST API endpoints
//!
//! These endpoints enable Couchbase SDKs to bootstrap by discovering
//! cluster topology, bucket configurations, and service ports.
//!
//! Key endpoints:
//!   GET /pools                        → Cluster entry point
//!   GET /pools/default                → Default pool info
//!   GET /pools/default/buckets        → List buckets with vBucket maps
//!   GET /pools/default/buckets/:name  → Single bucket config
//!   GET /pools/default/nodeServices   → Service port discovery

use axum::extract::{Path, State};
use axum::Json;
use std::sync::Arc;

use super::AppState;

/// GET /pools — Cluster entry point (SDK bootstrap starts here)
pub async fn pools(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let hostname = effective_hostname(&state.config.host);
    Json(serde_json::json!({
        "isAdminCreds": true,
        "isROAdminCreds": false,
        "isEnterprise": true,
        "allowedServices": ["kv", "n1ql", "index", "fts", "cbas"],
        "pools": [{
            "name": "default",
            "uri": "/pools/default?uuid=1",
            "streamingUri": "/poolsStreaming/default?uuid=1"
        }],
        "settings": {
            "maxParallelIndexers": "/settings/maxParallelIndexers?uuid=1",
            "viewUpdateDaemon": "/settings/viewUpdateDaemon?uuid=1"
        },
        "uuid": state.cluster.cluster_uuid.clone(),
        "implementationVersion": "0.3.0-oxidedb",
        "componentsVersion": {
            "oxidedb": "0.3.0",
            "rust": "1.76+"
        },
        "hostname": format!("{}:{}", hostname, state.config.port)
    }))
}

/// GET /pools/default — Default pool information
pub async fn pools_default(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let nodes = state.cluster.list_nodes().await;
    let bucket_count = state.storage.list_buckets().len();

    let nodes_json: Vec<serde_json::Value> = nodes
        .iter()
        .map(|n| build_node_json(n, &state))
        .collect();

    Json(serde_json::json!({
        "name": "default",
        "nodes": nodes_json,
        "buckets": {
            "uri": "/pools/default/buckets?v=1",
            "terseBucketsBase": "/pools/default/b/",
            "terseClusterInfo": "/pools/default/terseClusterInfo"
        },
        "controllers": {
            "addNode": { "uri": "/controller/addNode" },
            "rebalance": { "uri": "/controller/rebalance" },
            "failOver": { "uri": "/controller/failOver" },
            "reAddNode": { "uri": "/controller/reAddNode" },
            "ejectNode": { "uri": "/controller/ejectNode" }
        },
        "rebalanceStatus": "none",
        "rebalanceProgressUri": "/pools/default/rebalanceProgress",
        "storageTotals": {
            "ram": {
                "total": 1073741824u64,
                "quotaTotal": 536870912u64,
                "quotaUsed": 268435456u64,
                "used": 268435456u64,
                "usedByData": 134217728u64
            },
            "hdd": {
                "total": 10737418240u64,
                "quotaTotal": 5368709120u64,
                "used": 1073741824u64,
                "usedByData": 536870912u64,
                "free": 4294967296u64
            }
        },
        "maxBucketCount": 30,
        "clusterName": state.cluster.cluster_name.clone(),
        "balanced": true,
        "failoverWarnings": [],
        "alerts": [],
        "alertsSilenceURL": "/controller/resetAlerts?uuid=1",
        "serverGroupsUri": "/pools/default/serverGroups?v=1",
        "bucketCount": bucket_count
    }))
}

/// GET /pools/default/buckets — List all buckets with full config
pub async fn pools_default_buckets(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<serde_json::Value>> {
    let buckets = state.storage.list_buckets();
    let mut result = Vec::new();

    for bucket_info in &buckets {
        let config = build_bucket_config(&bucket_info.name, &state).await;
        result.push(config);
    }

    Json(result)
}

/// GET /pools/default/buckets/:name — Single bucket configuration
pub async fn pools_default_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> Json<serde_json::Value> {
    Json(build_bucket_config(&bucket_name, &state).await)
}

/// GET /pools/default/b/:name — Terse bucket config (CCCP compatible)
pub async fn pools_default_bucket_terse(
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> Json<serde_json::Value> {
    Json(build_bucket_config(&bucket_name, &state).await)
}

/// GET /pools/default/nodeServices — Service port discovery
pub async fn node_services(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let nodes = state.cluster.list_nodes().await;
    let pmap = state.cluster.get_partition_map().await;

    let nodes_ext: Vec<serde_json::Value> = nodes
        .iter()
        .map(|n| {
            let hostname = effective_hostname(&n.hostname);
            let kv_port = if n.name == state.config.node_name {
                state.config.memcached_port
            } else {
                n.port + (state.config.memcached_port - state.config.port)
            };
            serde_json::json!({
                "services": {
                    "mgmt": n.port,
                    "kv": kv_port,
                    "n1ql": n.port,
                    "fts": n.port,
                    "capi": n.port,
                    "indexAdmin": n.port,
                    "indexScan": n.port,
                    "indexHttp": n.port
                },
                "thisNode": n.name == state.config.node_name,
                "hostname": format!("{}:{}", hostname, n.port)
            })
        })
        .collect();

    Json(serde_json::json!({
        "rev": pmap.revision,
        "nodesExt": nodes_ext,
        "clusterCapabilitiesVer": [1, 0],
        "clusterCapabilities": {
            "n1ql": ["enhancedPreparedStatements"]
        }
    }))
}

/// POST /query/service — N1QL query endpoint (Couchbase SDK compatible)
pub async fn query_service(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Json<serde_json::Value> {
    // Parse the statement and params from form-encoded or JSON body
    let (statement, params) = if body.starts_with('{') {
        // JSON body
        let parsed = serde_json::from_str::<serde_json::Value>(&body).ok();
        let stmt = parsed
            .as_ref()
            .and_then(|v| v["statement"].as_str().map(|s| s.to_string()))
            .unwrap_or_default();
        // Extract named_params or args (positional)
        let params = parsed
            .as_ref()
            .and_then(|v| {
                if let Some(named) = v.get("named_params") {
                    Some(named.clone())
                } else if let Some(args) = v.get("args") {
                    Some(args.clone())
                } else {
                    None
                }
            });
        (stmt, params)
    } else {
        // Form-encoded
        let stmt = body
            .split('&')
            .find(|p| p.starts_with("statement="))
            .map(|p| urlencoding_decode(&p[10..]))
            .unwrap_or_default();
        (stmt, None)
    };

    if statement.is_empty() {
        return Json(serde_json::json!({
            "status": "fatal",
            "errors": [{"code": 1000, "msg": "No statement provided"}]
        }));
    }

    let request = crate::query::engine::QueryRequest { statement, params };
    match state.query_engine.execute(&request) {
        Ok(result) => Json(serde_json::json!(result)),
        Err(e) => Json(serde_json::json!({
            "status": "fatal",
            "errors": [{"code": 3000, "msg": e.to_string()}]
        })),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn effective_hostname(hostname: &str) -> &str {
    if hostname == "0.0.0.0" {
        "127.0.0.1"
    } else {
        hostname
    }
}

fn build_node_json(
    node: &crate::cluster::node::ClusterNode,
    state: &AppState,
) -> serde_json::Value {
    let hostname = effective_hostname(&node.hostname);
    let kv_port = if node.name == state.config.node_name {
        state.config.memcached_port
    } else {
        node.port + (state.config.memcached_port - state.config.port)
    };

    serde_json::json!({
        "couchApiBase": format!("http://{}:{}/", hostname, node.port),
        "hostname": format!("{}:{}", hostname, node.port),
        "status": match node.status {
            crate::cluster::node::NodeStatus::Healthy => "healthy",
            crate::cluster::node::NodeStatus::Warmup => "warmup",
            _ => "unhealthy"
        },
        "clusterMembership": "active",
        "thisNode": node.name == state.config.node_name,
        "ports": {
            "httpsMgmt": 18091,
            "httpsCAPI": 18092,
            "direct": kv_port
        },
        "services": ["kv", "n1ql", "index"],
        "nodeUUID": node.name,
        "otpNode": format!("ns_1@{}", hostname),
        "recoveryType": "none",
        "clusterCompatibility": 458752,
        "version": "0.3.0-oxidedb",
        "os": "linux-amd64",
        "cpuCount": 4,
        "memoryTotal": 1073741824u64,
        "memoryFree": 536870912u64,
        "uptime": node.uptime_seconds.to_string(),
        "interestingStats": {
            "curr_items": 0,
            "curr_items_tot": 0,
            "vb_replica_curr_items": 0,
            "cmd_get": 0,
            "get_hits": 0,
            "ops": 0
        }
    })
}

async fn build_bucket_config(
    bucket_name: &str,
    state: &AppState,
) -> serde_json::Value {
    let pmap = state.cluster.get_partition_map().await;
    let nodes = state.cluster.list_nodes().await;

    // Build server list
    let mut server_list: Vec<String> = Vec::new();
    let mut node_index_map: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for node in &nodes {
        let hostname = effective_hostname(&node.hostname);
        let kv_port = if node.name == state.config.node_name {
            state.config.memcached_port
        } else {
            node.port + (state.config.memcached_port - state.config.port)
        };
        let idx = server_list.len();
        server_list.push(format!("{}:{}", hostname, kv_port));
        node_index_map.insert(node.name.clone(), idx);
    }

    // Build vBucket map
    let mut vbucket_map: Vec<Vec<i32>> = Vec::with_capacity(pmap.num_vbuckets as usize);
    for entry in &pmap.map {
        let mut mapping = Vec::new();
        let active_idx = node_index_map
            .get(&entry.active_node)
            .copied()
            .map(|i| i as i32)
            .unwrap_or(-1);
        mapping.push(active_idx);
        for replica in &entry.replica_nodes {
            let replica_idx = node_index_map
                .get(replica)
                .copied()
                .map(|i| i as i32)
                .unwrap_or(-1);
            mapping.push(replica_idx);
        }
        while mapping.len() < 2 {
            mapping.push(-1);
        }
        vbucket_map.push(mapping);
    }

    let nodes_json: Vec<serde_json::Value> = nodes
        .iter()
        .map(|n| build_node_json(n, state))
        .collect();

    let nodes_ext: Vec<serde_json::Value> = nodes
        .iter()
        .map(|n| {
            let hostname = effective_hostname(&n.hostname);
            let kv_port = if n.name == state.config.node_name {
                state.config.memcached_port
            } else {
                n.port + (state.config.memcached_port - state.config.port)
            };
            serde_json::json!({
                "services": {
                    "mgmt": n.port,
                    "kv": kv_port,
                    "n1ql": n.port,
                    "capi": n.port
                },
                "thisNode": n.name == state.config.node_name,
                "hostname": format!("{}:{}", hostname, n.port)
            })
        })
        .collect();

    let (doc_count, ram_used) = state
        .storage
        .get_bucket(bucket_name)
        .map(|b| (b.document_count(), b.total_size_bytes()))
        .unwrap_or((0, 0));

    serde_json::json!({
        "name": bucket_name,
        "bucketType": "membase",
        "authType": "sasl",
        "saslPassword": "",
        "proxyPort": 0,
        "replicaIndex": false,
        "uri": format!("/pools/default/buckets/{}", bucket_name),
        "streamingUri": format!("/pools/default/bucketsStreaming/{}", bucket_name),
        "localRandomKeyUri": format!("/pools/default/buckets/{}/localRandomKey", bucket_name),
        "controllers": {
            "compactAll": format!("/pools/default/buckets/{}/controller/compactBucket", bucket_name),
            "compactDB": format!("/pools/default/buckets/{}/controller/compactDatabases", bucket_name),
            "purgeDeletes": format!("/pools/default/buckets/{}/controller/unsafePurgeBucket", bucket_name),
            "startRecovery": format!("/pools/default/buckets/{}/controller/startRecovery", bucket_name)
        },
        "nodes": nodes_json,
        "nodesExt": nodes_ext,
        "stats": {
            "uri": format!("/pools/default/buckets/{}/stats", bucket_name),
            "directoryURI": format!("/pools/default/buckets/{}/stats/Directory", bucket_name),
            "nodeStatsListURI": format!("/pools/default/buckets/{}/nodes", bucket_name)
        },
        "nodeLocator": "vbucket",
        "uuid": state.cluster.cluster_uuid.clone(),
        "ddocs": { "uri": format!("/pools/default/buckets/{}/ddocs", bucket_name) },
        "vBucketServerMap": {
            "hashAlgorithm": "CRC",
            "numReplicas": pmap.num_replicas,
            "serverList": server_list,
            "vBucketMap": vbucket_map
        },
        "bucketCapabilitiesVer": "",
        "bucketCapabilities": [
            "collections", "durableWrite", "tombstonedUserXAttrs",
            "couchapi", "dcp", "cbhello", "touch", "cccp", "nodesExt",
            "xdcrCheckpointing", "xattr"
        ],
        "clusterCapabilitiesVer": [1, 0],
        "clusterCapabilities": {
            "n1ql": ["enhancedPreparedStatements"]
        },
        "collectionsManifestUid": "0",
        "maxTTL": 0,
        "compressionMode": "passive",
        "replicaNumber": pmap.num_replicas,
        "threadsNumber": 3,
        "quota": {
            "ram": 268435456u64,
            "rawRAM": 268435456u64
        },
        "basicStats": {
            "quotaPercentUsed": 10.0,
            "opsPerSec": 0,
            "diskFetches": 0,
            "itemCount": doc_count,
            "diskUsed": ram_used,
            "dataUsed": ram_used,
            "memUsed": ram_used,
            "vbActiveNumNonResident": 0
        },
        "evictionPolicy": "valueOnly",
        "conflictResolutionType": "seqno",
        "durabilityMinLevel": "none",
        "pitrEnabled": false,
        "pitrGranularity": 600,
        "pitrMaxHistoryAge": 86400,
        "autoCompactionSettings": false,
        "storageBackend": "couchstore"
    })
}

/// GET /pools/default/bucketsStreaming/:name — CCCP Streaming config endpoint
/// SDKs use this to receive real-time config updates via chunked HTTP response.
/// Each chunk is a complete bucket config JSON followed by "\n\n\n\n".
/// The SDK reads config, then waits for the next one (long-polling style).
pub async fn bucket_streaming(
    State(state): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> axum::response::Response {
    use axum::body::Body;
    #[allow(unused_imports)]
    use tokio_stream::StreamExt;

    let state_clone = state.clone();
    let bucket = bucket_name.clone();

    let stream = async_stream::stream! {
        let mut last_rev: u64;

        // Send initial config immediately
        let config = build_bucket_config(&bucket, &state_clone).await;
        let config_str = serde_json::to_string(&config).unwrap_or_default();
        last_rev = state_clone.cluster.get_partition_map().await.revision;
        yield Ok::<_, std::convert::Infallible>(format!("{}\n\n\n\n", config_str));

        // Then poll for changes every 2 seconds
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            let current_rev = state_clone.cluster.get_partition_map().await.revision;
            if current_rev != last_rev {
                let config = build_bucket_config(&bucket, &state_clone).await;
                let config_str = serde_json::to_string(&config).unwrap_or_default();
                last_rev = current_rev;
                yield Ok::<_, std::convert::Infallible>(format!("{}\n\n\n\n", config_str));
            }
        }
    };

    let body = Body::from_stream(stream);

    axum::response::Response::builder()
        .header("Content-Type", "application/json")
        .header("Transfer-Encoding", "chunked")
        .header("Connection", "close")
        .body(body)
        .unwrap_or_else(|_| {
            axum::response::Response::new(Body::empty())
        })
}

/// GET /whoami — Current user info (Go SDK uses this for readiness check)
pub async fn whoami(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let hostname = effective_hostname(&state.config.host);
    Json(serde_json::json!({
        "id": "admin",
        "domain": "local",
        "roles": [
            {"role": "admin"},
            {"role": "bucket_full_access", "bucket_name": "*"},
            {"role": "cluster_admin"}
        ],
        "name": "Administrator",
        "password_change_date": "2026-01-01T00:00:00.000000000+00:00",
        "uuid": state.cluster.cluster_uuid.clone(),
        "hostname": format!("{}:{}", hostname, state.config.port)
    }))
}

/// GET /admin/ping — Simple health ping (Go SDK readiness check)
pub async fn admin_ping() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "revision": 1
    }))
}

/// Simple URL decoding (no external dependency)
fn urlencoding_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '+' {
            result.push(' ');
        } else if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            }
        } else {
            result.push(c);
        }
    }
    result
}
