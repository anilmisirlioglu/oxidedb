pub mod audit_routes;
pub mod backup_routes;
pub mod bucket_routes;
pub mod cluster_routes;
pub mod couchbase_compat;
pub mod dcp_routes;
pub mod document_routes;
pub mod fts_routes;
pub mod index_routes;
pub mod query_routes;
pub mod rbac_routes;
pub mod replication_routes;
pub mod transaction_routes;
pub mod web_ui;
pub mod xdcr_routes;

use crate::audit::logger::AuditLogger;
use crate::auth::rbac::RbacManager;
use crate::cluster::durability::DurabilityManager;
use crate::cluster::ClusterManager;
use crate::config::ServerConfig;
use crate::dcp::replicator::IntraClusterReplicator;
use crate::dcp::stream::DcpEngine;
use crate::fts::engine::FtsEngine;
use crate::query::engine::QueryEngine;
use crate::storage::engine::StorageEngine;
use crate::storage::index::IndexManager;
use crate::transactions::engine::TransactionEngine;
use crate::xdcr::replicator::XdcrManager;
use axum::routing::{delete, get, post, put};
use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Shared application state
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub xdcr: Arc<XdcrManager>,
    pub cluster: Arc<ClusterManager>,
    pub query_engine: Arc<QueryEngine>,
    pub index_manager: Arc<IndexManager>,
    pub fts_engine: Arc<FtsEngine>,
    pub dcp_engine: Arc<DcpEngine>,
    pub audit_logger: Arc<AuditLogger>,
    pub rbac: Arc<RbacManager>,
    pub txn_engine: Arc<TransactionEngine>,
    pub replicator: Arc<IntraClusterReplicator>,
    pub durability: Arc<DurabilityManager>,
    pub config: ServerConfig,
}

/// Build the API router
pub fn build_router(state: Arc<AppState>) -> Router {
    let bucket_routes = Router::new()
        .route("/", get(bucket_routes::list_buckets).post(bucket_routes::create_bucket))
        .route("/:name", get(bucket_routes::get_bucket).delete(bucket_routes::delete_bucket))
        .route("/:name/flush", post(bucket_routes::flush_bucket))
        .route("/:name/stats", get(document_routes::bucket_stats))
        .route("/:bucket/scopes", get(bucket_routes::list_scopes).post(bucket_routes::create_scope))
        .route("/:bucket/scopes/:scope", delete(bucket_routes::delete_scope))
        .route(
            "/:bucket/scopes/:scope/collections",
            post(bucket_routes::create_collection),
        )
        .route(
            "/:bucket/scopes/:scope/collections/:collection",
            delete(bucket_routes::delete_collection),
        );

    let document_routes = Router::new()
        .route(
            "/:bucket/scopes/:scope/collections/:collection/docs",
            get(document_routes::list_documents),
        )
        .route(
            "/:bucket/scopes/:scope/collections/:collection/docs/:key",
            get(document_routes::get_document)
                .put(document_routes::upsert_document)
                .delete(document_routes::delete_document),
        )
        .route(
            "/:bucket/scopes/:scope/collections/:collection/docs/:key/touch",
            post(document_routes::touch_document),
        )
        .route(
            "/:bucket/scopes/:scope/collections/:collection/docs/:key/xattrs",
            get(document_routes::list_xattrs),
        )
        .route(
            "/:bucket/scopes/:scope/collections/:collection/docs/:key/xattrs/:path",
            get(document_routes::get_xattr)
                .put(document_routes::upsert_xattr)
                .delete(document_routes::delete_xattr),
        );

    let xdcr_routes = Router::new()
        .route("/clusters", get(xdcr_routes::list_remote_clusters).post(xdcr_routes::add_remote_cluster))
        .route("/clusters/:name", delete(xdcr_routes::remove_remote_cluster))
        .route("/replications", get(xdcr_routes::list_replications).post(xdcr_routes::create_replication))
        .route("/replications/:id", get(xdcr_routes::get_replication).delete(xdcr_routes::delete_replication))
        .route("/replications/:id/pause", post(xdcr_routes::pause_replication))
        .route("/replications/:id/resume", post(xdcr_routes::resume_replication))
        .route("/receive", post(xdcr_routes::receive_mutations));

    let cluster_routes = Router::new()
        .route("/", get(cluster_routes::get_cluster_info))
        .route("/nodes", get(cluster_routes::list_nodes).post(cluster_routes::add_node))
        .route("/nodes/:name", delete(cluster_routes::remove_node))
        .route("/nodes/:name/heartbeat", post(cluster_routes::node_heartbeat))
        .route("/partitions", get(cluster_routes::get_partition_map))
        .route("/partitions/summary", get(cluster_routes::get_partition_summary))
        .route("/rebalance", get(cluster_routes::get_rebalance_status).post(cluster_routes::trigger_rebalance))
        .route("/vbuckets/transfer", post(cluster_routes::receive_vbucket_data))
        .route("/vbuckets/:bucket/:vbucket_id", get(cluster_routes::export_vbucket_data))
        .route("/failover", get(cluster_routes::get_failover_state))
        .route("/failover/config", post(cluster_routes::update_failover_config))
        .route("/failover/reset", post(cluster_routes::reset_failover_quota))
        .route("/failover/:node_name", post(cluster_routes::failover_node))
        .route("/failover/:node_name/recover", post(cluster_routes::recover_node))
        // Server Groups (rack/zone awareness)
        .route("/server-groups", get(cluster_routes::list_server_groups).post(cluster_routes::create_server_group))
        .route("/server-groups/:name", delete(cluster_routes::delete_server_group))
        .route("/server-groups/move", post(cluster_routes::move_node_to_group))
        .route("/rebalance-groups", post(cluster_routes::rebalance_with_groups));

    let query_routes = Router::new()
        .route("/", post(query_routes::execute_query))
        .route("/prepared", get(query_routes::list_prepared_statements));

    let index_routes = Router::new()
        .route("/", get(index_routes::list_indexes).post(index_routes::create_index))
        .route("/:bucket", get(index_routes::list_bucket_indexes))
        .route("/:bucket/:index_name", get(index_routes::get_index).delete(index_routes::drop_index))
        .route("/:bucket/:index_name/rebuild", post(index_routes::rebuild_index));

    let fts_routes = Router::new()
        .route("/indexes", get(fts_routes::list_fts_indexes).post(fts_routes::create_fts_index))
        .route("/indexes/:name", get(fts_routes::get_fts_index).delete(fts_routes::drop_fts_index))
        .route("/indexes/:name/build", post(fts_routes::build_fts_index))
        .route("/indexes/:name/search", post(fts_routes::search_fts_index))
        .route("/search", post(fts_routes::search_fts));

    let dcp_routes = Router::new()
        .route("/streams", get(dcp_routes::list_dcp_streams).post(dcp_routes::create_dcp_stream))
        .route("/streams/:id", get(dcp_routes::get_dcp_stream).delete(dcp_routes::close_dcp_stream))
        .route("/streams/:id/pause", post(dcp_routes::pause_dcp_stream))
        .route("/streams/:id/resume", post(dcp_routes::resume_dcp_stream))
        .route("/streams/:id/events", get(dcp_routes::poll_dcp_events))
        .route("/streams/:id/sse", get(dcp_routes::dcp_sse_stream))
        .route("/backfill", post(dcp_routes::dcp_backfill));

    let audit_routes = Router::new()
        .route("/events", get(audit_routes::list_events))
        .route("/events/clear", post(audit_routes::clear_events))
        .route("/config", get(audit_routes::get_config).post(audit_routes::update_config))
        .route("/stats", get(audit_routes::get_stats));

    let rbac_routes = Router::new()
        .route("/users", get(rbac_routes::list_users).post(rbac_routes::create_user))
        .route("/users/:username", get(rbac_routes::get_user).delete(rbac_routes::delete_user))
        .route("/users/:username/roles", put(rbac_routes::update_user_roles))
        .route("/users/:username/password", put(rbac_routes::change_password))
        .route("/roles", get(rbac_routes::list_roles));

    let backup_routes = Router::new()
        .route("/", get(backup_routes::list_backups).post(backup_routes::create_backup))
        .route("/:name", get(backup_routes::get_backup).delete(backup_routes::delete_backup))
        .route("/:name/restore", post(backup_routes::restore_backup));

    let transaction_routes = Router::new()
        .route("/", get(transaction_routes::list_transactions).post(transaction_routes::begin_transaction))
        .route("/:txn_id", get(transaction_routes::get_transaction))
        .route("/:txn_id/get", post(transaction_routes::transaction_get))
        .route("/:txn_id/insert", post(transaction_routes::transaction_insert))
        .route("/:txn_id/replace", post(transaction_routes::transaction_replace))
        .route("/:txn_id/remove", post(transaction_routes::transaction_remove))
        .route("/:txn_id/commit", post(transaction_routes::commit_transaction))
        .route("/:txn_id/rollback", post(transaction_routes::rollback_transaction));

    // Internal intra-cluster replication & consensus routes
    let internal_routes = Router::new()
        // DCP replication
        .route("/replicate", post(replication_routes::receive_replication))
        .route("/replication/status", get(replication_routes::get_replication_status))
        .route("/replication/pause", post(replication_routes::pause_replication))
        .route("/replication/resume", post(replication_routes::resume_replication))
        // Orchestrator
        .route("/orchestrator", get(replication_routes::get_orchestrator_state))
        // Durability
        .route("/durability/stats", get(replication_routes::get_durability_stats))
        // Chronicle metadata consensus
        .route("/chronicle", get(replication_routes::get_chronicle_status))
        .route("/chronicle/propose", post(replication_routes::propose_config_change))
        .route("/chronicle/prepare", post(replication_routes::handle_chronicle_prepare))
        .route("/chronicle/ack", post(replication_routes::handle_chronicle_ack))
        .route("/chronicle/commit", post(replication_routes::handle_chronicle_commit))
        .route("/chronicle/log", get(replication_routes::get_chronicle_log));

    // Couchbase SDK compatible routes
    let couchbase_routes = Router::new()
        .route("/pools", get(couchbase_compat::pools))
        .route("/pools/default", get(couchbase_compat::pools_default))
        .route("/pools/default/buckets", get(couchbase_compat::pools_default_buckets))
        .route("/pools/default/buckets/:name", get(couchbase_compat::pools_default_bucket))
        .route("/pools/default/bucketsStreaming/:name", get(couchbase_compat::bucket_streaming))
        .route("/pools/default/b/:name", get(couchbase_compat::pools_default_bucket_terse))
        .route("/pools/default/nodeServices", get(couchbase_compat::node_services))
        .route("/query/service", post(couchbase_compat::query_service))
        // FTS service endpoint (Couchbase SDK compatible)
        .route("/api/index", get(fts_routes::list_fts_indexes))
        .route("/api/index/:name", get(fts_routes::get_fts_index).delete(fts_routes::drop_fts_index))
        .route("/api/index/:name/query", post(fts_routes::search_fts_index))
        // Go SDK readiness endpoints
        .route("/whoami", get(couchbase_compat::whoami))
        .route("/admin/ping", get(couchbase_compat::admin_ping));

    Router::new()
        .nest("/api/v1/buckets", bucket_routes)
        .nest("/api/v1/docs", document_routes)
        .nest("/api/v1/xdcr", xdcr_routes)
        .nest("/api/v1/cluster", cluster_routes)
        .nest("/api/v1/query", query_routes)
        .nest("/api/v1/indexes", index_routes)
        .nest("/api/v1/fts", fts_routes)
        .nest("/api/v1/dcp", dcp_routes)
        .nest("/api/v1/audit", audit_routes)
        .nest("/api/v1/rbac", rbac_routes)
        .nest("/api/v1/backups", backup_routes)
        .nest("/api/v1/transactions", transaction_routes)
        .nest("/api/v1/internal", internal_routes)
        .route("/api/v1/persistence/stats", get(document_routes::persistence_stats))
        // Couchbase SDK bootstrap endpoints
        .merge(couchbase_routes)
        .route("/ui", get(web_ui::serve_ui))
        .route("/", get(root_handler))
        .route("/health", get(health_check))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn root_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "name": "OxideDB",
        "version": "0.5.0",
        "description": "Couchbase-like NoSQL DB with XDCR, B+ tree storage, secondary indexes, FTS",
        "storage_format": "B+ Tree (4KB pages, binary)",
        "wal": "Buffered WAL with dual-trigger flush (ops/bytes/interval)",
        "indexes": "GSI-like secondary indexes with composite key support",
        "endpoints": {
            "buckets": "/api/v1/buckets",
            "documents": "/api/v1/docs/{bucket}/scopes/{scope}/collections/{collection}/docs/{key}",
            "indexes": "/api/v1/indexes",
            "xdcr": "/api/v1/xdcr",
            "cluster": "/api/v1/cluster",
            "partitions": "/api/v1/cluster/partitions",
            "rebalance": "/api/v1/cluster/rebalance",
            "query": "/api/v1/query",
            "fts": "/api/v1/fts",
            "dcp": "/api/v1/dcp",
            "audit": "/api/v1/audit",
            "rbac": "/api/v1/rbac",
            "backups": "/api/v1/backups",
            "transactions": "/api/v1/transactions",
            "persistence": "/api/v1/persistence/stats",
            "replication": "/api/v1/internal/replication/status",
            "orchestrator": "/api/v1/internal/orchestrator",
            "chronicle": "/api/v1/internal/chronicle",
            "chronicle_log": "/api/v1/internal/chronicle/log",
            "server_groups": "/api/v1/cluster/server-groups",
            "rebalance_groups": "/api/v1/cluster/rebalance-groups",
            "health": "/health",
            "ui": "/ui"
        }
    }))
}

async fn health_check() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}
