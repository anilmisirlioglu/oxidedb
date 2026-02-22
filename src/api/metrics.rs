//! Prometheus metrics endpoint for OxideDB
//!
//! Exposes detailed metrics in Prometheus text exposition format at GET /metrics.
//!
//! Metric families:
//!   - oxidedb_info                    (gauge)  — build/version info label
//!   - oxidedb_up                      (gauge)  — always 1 when server is running
//!   - oxidedb_uptime_seconds          (gauge)  — seconds since server start
//!
//!   # Storage / Buckets
//!   - oxidedb_buckets_total           (gauge)
//!   - oxidedb_bucket_documents_total  (gauge, per bucket)
//!   - oxidedb_bucket_size_bytes       (gauge, per bucket)
//!   - oxidedb_bucket_ram_quota_bytes  (gauge, per bucket)
//!   - oxidedb_bucket_vbuckets_total   (gauge, per bucket)
//!   - oxidedb_bucket_evicted_total    (gauge, per bucket)
//!
//!   # Cluster
//!   - oxidedb_cluster_nodes_total     (gauge)
//!   - oxidedb_cluster_node_status     (gauge, per node, status label)
//!   - oxidedb_cluster_rebalance_in_progress (gauge)
//!   - oxidedb_cluster_partition_map_revision (gauge)
//!
//!   # Indexes
//!   - oxidedb_indexes_total           (gauge)
//!   - oxidedb_index_entries           (gauge, per index)
//!   - oxidedb_fts_indexes_total       (gauge)
//!   - oxidedb_fts_index_documents     (gauge, per fts index)
//!   - oxidedb_fts_index_terms         (gauge, per fts index)
//!
//!   # WAL / Persistence
//!   - oxidedb_wal_buffer_pending_ops  (gauge)
//!   - oxidedb_wal_buffer_pending_bytes(gauge)
//!   - oxidedb_wal_buffer_total_buffered (counter)
//!   - oxidedb_wal_buffer_total_flushes  (counter)
//!   - oxidedb_wal_ms_since_flush      (gauge)
//!
//!   # XDCR
//!   - oxidedb_xdcr_replications_total (gauge)
//!   - oxidedb_xdcr_replication_status (gauge, per replication)
//!   - oxidedb_xdcr_docs_replicated_total (counter, per replication)
//!
//!   # DCP
//!   - oxidedb_dcp_streams_total       (gauge)
//!   - oxidedb_dcp_stream_events_total (counter, per stream)
//!
//!   # Transactions
//!   - oxidedb_transactions_active     (gauge)
//!
//!   # Audit
//!   - oxidedb_audit_events_total      (gauge)
//!   - oxidedb_audit_enabled           (gauge)
//!
//!   # Query
//!   - oxidedb_prepared_statements_total (gauge)

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;

use super::AppState;

/// Server start time — initialised on first call and cached.
static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Initialise (or retrieve) the server boot instant.
pub fn init_start_time() {
    START_TIME.get_or_init(Instant::now);
}

/// GET /metrics — Prometheus text exposition format
pub async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let mut out = String::with_capacity(8192);

    // ── Server info ─────────────────────────────────────────────
    write_help_type(&mut out, "oxidedb_info", "OxideDB build information", "gauge");
    writeln!(out, "oxidedb_info{{version=\"0.5.0\",edition=\"2021\"}} 1").ok();

    write_help_type(&mut out, "oxidedb_up", "Whether OxideDB is up (always 1)", "gauge");
    writeln!(out, "oxidedb_up 1").ok();

    let uptime = START_TIME.get().map(|t| t.elapsed().as_secs()).unwrap_or(0);
    write_help_type(&mut out, "oxidedb_uptime_seconds", "Seconds since server start", "gauge");
    writeln!(out, "oxidedb_uptime_seconds {}", uptime).ok();

    // ── Buckets ─────────────────────────────────────────────────
    let buckets = state.storage.list_buckets();

    write_help_type(&mut out, "oxidedb_buckets_total", "Total number of buckets", "gauge");
    writeln!(out, "oxidedb_buckets_total {}", buckets.len()).ok();

    write_help_type(
        &mut out,
        "oxidedb_bucket_documents_total",
        "Number of live documents in a bucket",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_bucket_size_bytes",
        "Approximate in-memory size of a bucket in bytes",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_bucket_ram_quota_bytes",
        "RAM quota for a bucket in bytes",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_bucket_vbuckets_total",
        "Number of vBuckets in a bucket",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_bucket_evicted_total",
        "Total items evicted from a bucket",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_bucket_scopes_total",
        "Number of scopes in a bucket",
        "gauge",
    );

    for cfg in &buckets {
        let name = &cfg.name;
        let lbl = format!("bucket=\"{}\"", escape_label(name));

        if let Ok(bucket) = state.storage.get_bucket(name) {
            let doc_count = bucket.document_count();
            let size = bucket.total_size_bytes();
            let quota = (cfg.ram_quota_mb as u64) * 1024 * 1024;
            let eviction = bucket.eviction_stats();
            let scopes = bucket.list_scopes();

            writeln!(out, "oxidedb_bucket_documents_total{{{}}} {}", lbl, doc_count).ok();
            writeln!(out, "oxidedb_bucket_size_bytes{{{}}} {}", lbl, size).ok();
            writeln!(out, "oxidedb_bucket_ram_quota_bytes{{{}}} {}", lbl, quota).ok();
            writeln!(out, "oxidedb_bucket_vbuckets_total{{{}}} {}", lbl, cfg.num_vbuckets).ok();
            writeln!(out, "oxidedb_bucket_evicted_total{{{}}} {}", lbl, eviction.total_evicted_items).ok();
            writeln!(out, "oxidedb_bucket_scopes_total{{{}}} {}", lbl, scopes.len()).ok();
        }
    }

    // ── Cluster ─────────────────────────────────────────────────
    let nodes = state.cluster.list_nodes().await;

    write_help_type(&mut out, "oxidedb_cluster_nodes_total", "Total number of cluster nodes", "gauge");
    writeln!(out, "oxidedb_cluster_nodes_total {}", nodes.len()).ok();

    write_help_type(
        &mut out,
        "oxidedb_cluster_node_status",
        "Node status (1=healthy, 0=unhealthy/warmup/inactive)",
        "gauge",
    );
    for node in &nodes {
        let status_str = format!("{:?}", node.status);
        let healthy: u8 = if status_str == "Healthy" { 1 } else { 0 };
        writeln!(
            out,
            "oxidedb_cluster_node_status{{node=\"{}\",status=\"{}\"}} {}",
            escape_label(&node.name),
            escape_label(&status_str),
            healthy,
        )
        .ok();
    }

    let rebalance = state.cluster.get_rebalance_status().await;
    write_help_type(
        &mut out,
        "oxidedb_cluster_rebalance_in_progress",
        "Whether a rebalance is currently in progress (1/0)",
        "gauge",
    );
    writeln!(out, "oxidedb_cluster_rebalance_in_progress {}", if rebalance.in_progress { 1 } else { 0 }).ok();

    write_help_type(
        &mut out,
        "oxidedb_cluster_rebalance_progress_percent",
        "Current rebalance progress percentage",
        "gauge",
    );
    writeln!(out, "oxidedb_cluster_rebalance_progress_percent {:.2}", rebalance.progress_percent).ok();

    let pmap = state.cluster.get_partition_map().await;
    write_help_type(
        &mut out,
        "oxidedb_cluster_partition_map_revision",
        "Current partition map revision",
        "gauge",
    );
    writeln!(out, "oxidedb_cluster_partition_map_revision {}", pmap.revision).ok();

    write_help_type(
        &mut out,
        "oxidedb_cluster_vbuckets_total",
        "Total number of vBuckets in the partition map",
        "gauge",
    );
    writeln!(out, "oxidedb_cluster_vbuckets_total {}", pmap.num_vbuckets).ok();

    // ── Secondary Indexes (GSI) ─────────────────────────────────
    let indexes = state.index_manager.list_indexes(None);

    write_help_type(&mut out, "oxidedb_indexes_total", "Total number of GSI indexes", "gauge");
    writeln!(out, "oxidedb_indexes_total {}", indexes.len()).ok();

    write_help_type(
        &mut out,
        "oxidedb_index_entries",
        "Number of entries in a secondary index",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_index_state",
        "Index state (1=Online, 0=other)",
        "gauge",
    );
    for idx in &indexes {
        let lbl = format!(
            "bucket=\"{}\",index=\"{}\"",
            escape_label(&idx.bucket),
            escape_label(&idx.name),
        );
        writeln!(out, "oxidedb_index_entries{{{}}} {}", lbl, idx.num_entries).ok();
        let online: u8 = if idx.state == crate::storage::index::IndexState::Online { 1 } else { 0 };
        writeln!(out, "oxidedb_index_state{{{}}} {}", lbl, online).ok();
    }

    // ── Full-Text Search Indexes ────────────────────────────────
    let fts_indexes = state.fts_engine.list_indexes();

    write_help_type(&mut out, "oxidedb_fts_indexes_total", "Total number of FTS indexes", "gauge");
    writeln!(out, "oxidedb_fts_indexes_total {}", fts_indexes.len()).ok();

    write_help_type(
        &mut out,
        "oxidedb_fts_index_documents",
        "Number of documents in an FTS index",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_fts_index_terms",
        "Number of unique terms in an FTS index",
        "gauge",
    );
    for fi in &fts_indexes {
        let lbl = format!(
            "bucket=\"{}\",index=\"{}\"",
            escape_label(&fi.bucket),
            escape_label(&fi.name),
        );
        writeln!(out, "oxidedb_fts_index_documents{{{}}} {}", lbl, fi.doc_count).ok();
        writeln!(out, "oxidedb_fts_index_terms{{{}}} {}", lbl, fi.term_count).ok();
    }

    // ── WAL / Persistence ───────────────────────────────────────
    if let Some(ref persistence) = state.storage.persistence {
        let buf_stats = persistence.buffer_stats();

        write_help_type(&mut out, "oxidedb_wal_buffer_pending_ops", "Number of ops pending in WAL buffer", "gauge");
        writeln!(out, "oxidedb_wal_buffer_pending_ops {}", buf_stats.pending_ops).ok();

        write_help_type(&mut out, "oxidedb_wal_buffer_pending_bytes", "Bytes pending in WAL buffer", "gauge");
        writeln!(out, "oxidedb_wal_buffer_pending_bytes {}", buf_stats.pending_bytes).ok();

        write_help_type(
            &mut out,
            "oxidedb_wal_buffer_total_buffered",
            "Total operations buffered (lifetime)",
            "counter",
        );
        writeln!(out, "oxidedb_wal_buffer_total_buffered {}", buf_stats.total_buffered).ok();

        write_help_type(
            &mut out,
            "oxidedb_wal_buffer_total_flushes",
            "Total WAL flushes performed (lifetime)",
            "counter",
        );
        writeln!(out, "oxidedb_wal_buffer_total_flushes {}", buf_stats.total_flushes).ok();

        write_help_type(
            &mut out,
            "oxidedb_wal_ms_since_flush",
            "Milliseconds since last WAL flush",
            "gauge",
        );
        writeln!(out, "oxidedb_wal_ms_since_flush {}", buf_stats.ms_since_flush).ok();

        write_help_type(
            &mut out,
            "oxidedb_wal_config_max_ops",
            "WAL flush trigger: max ops threshold",
            "gauge",
        );
        writeln!(out, "oxidedb_wal_config_max_ops {}", buf_stats.config_max_ops).ok();

        write_help_type(
            &mut out,
            "oxidedb_wal_config_max_bytes",
            "WAL flush trigger: max bytes threshold",
            "gauge",
        );
        writeln!(out, "oxidedb_wal_config_max_bytes {}", buf_stats.config_max_bytes).ok();

        // Per-bucket persistence stats
        let summary = persistence.summary();

        write_help_type(
            &mut out,
            "oxidedb_persistence_wal_file_size_bytes",
            "WAL file size per bucket in bytes",
            "gauge",
        );
        for bs in &summary.buckets {
            writeln!(
                out,
                "oxidedb_persistence_wal_file_size_bytes{{bucket=\"{}\"}} {}",
                escape_label(&bs.bucket_name),
                bs.wal_file_size_bytes,
            )
            .ok();
        }

        write_help_type(
            &mut out,
            "oxidedb_persistence_btree_file_size_bytes",
            "B+ tree file size per bucket in bytes",
            "gauge",
        );
        write_help_type(
            &mut out,
            "oxidedb_persistence_btree_pages",
            "B+ tree page count per bucket",
            "gauge",
        );
        write_help_type(
            &mut out,
            "oxidedb_persistence_btree_records",
            "B+ tree record count per bucket",
            "gauge",
        );
        write_help_type(
            &mut out,
            "oxidedb_persistence_btree_height",
            "B+ tree height per bucket",
            "gauge",
        );
        for bs in &summary.buckets {
            let lbl = format!("bucket=\"{}\"", escape_label(&bs.bucket_name));
            writeln!(out, "oxidedb_persistence_btree_file_size_bytes{{{}}} {}", lbl, bs.btree.file_size_bytes).ok();
            writeln!(out, "oxidedb_persistence_btree_pages{{{}}} {}", lbl, bs.btree.page_count).ok();
            writeln!(out, "oxidedb_persistence_btree_records{{{}}} {}", lbl, bs.btree.record_count).ok();
            writeln!(out, "oxidedb_persistence_btree_height{{{}}} {}", lbl, bs.btree.tree_height).ok();
        }
    }

    // ── XDCR ────────────────────────────────────────────────────
    let replications = state.xdcr.list_replications().await;

    write_help_type(&mut out, "oxidedb_xdcr_replications_total", "Total number of XDCR replications", "gauge");
    writeln!(out, "oxidedb_xdcr_replications_total {}", replications.len()).ok();

    write_help_type(
        &mut out,
        "oxidedb_xdcr_replication_status",
        "XDCR replication status (1=Running, 0=other)",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_xdcr_docs_replicated_total",
        "Total documents replicated per replication",
        "counter",
    );
    write_help_type(
        &mut out,
        "oxidedb_xdcr_docs_failed_total",
        "Total documents that failed replication",
        "counter",
    );
    write_help_type(
        &mut out,
        "oxidedb_xdcr_conflicts_total",
        "Total XDCR conflicts per replication",
        "counter",
    );
    for repl in &replications {
        let lbl = format!(
            "id=\"{}\",source=\"{}\",target_cluster=\"{}\",target_bucket=\"{}\"",
            escape_label(&repl.config.id),
            escape_label(&repl.config.source_bucket),
            escape_label(&repl.config.target_cluster),
            escape_label(&repl.config.target_bucket),
        );
        let running: u8 = if repl.status == crate::xdcr::replicator::ReplicationStatus::Running { 1 } else { 0 };
        writeln!(out, "oxidedb_xdcr_replication_status{{{}}} {}", lbl, running).ok();
        writeln!(out, "oxidedb_xdcr_docs_replicated_total{{{}}} {}", lbl, repl.stats.docs_replicated).ok();
        writeln!(out, "oxidedb_xdcr_docs_failed_total{{{}}} {}", lbl, repl.stats.docs_failed).ok();
        writeln!(out, "oxidedb_xdcr_conflicts_total{{{}}} {}", lbl, repl.stats.total_conflicts).ok();
    }

    // ── DCP Streams ─────────────────────────────────────────────
    let streams = state.dcp_engine.list_streams();

    write_help_type(&mut out, "oxidedb_dcp_streams_total", "Total number of DCP streams", "gauge");
    writeln!(out, "oxidedb_dcp_streams_total {}", streams.len()).ok();

    write_help_type(
        &mut out,
        "oxidedb_dcp_stream_active",
        "Whether a DCP stream is active (1/0)",
        "gauge",
    );
    write_help_type(
        &mut out,
        "oxidedb_dcp_stream_events_total",
        "Total events streamed per DCP stream",
        "counter",
    );
    for stream in &streams {
        let lbl = format!(
            "id=\"{}\",name=\"{}\",bucket=\"{}\"",
            escape_label(&stream.id),
            escape_label(&stream.name),
            escape_label(&stream.bucket),
        );
        writeln!(out, "oxidedb_dcp_stream_active{{{}}} {}", lbl, if stream.active { 1 } else { 0 }).ok();
        writeln!(out, "oxidedb_dcp_stream_events_total{{{}}} {}", lbl, stream.events_streamed).ok();
    }

    // ── Transactions ────────────────────────────────────────────
    let active_txns = state.txn_engine.list_active();

    write_help_type(
        &mut out,
        "oxidedb_transactions_active",
        "Number of currently active transactions",
        "gauge",
    );
    writeln!(out, "oxidedb_transactions_active {}", active_txns.len()).ok();

    // ── Audit ───────────────────────────────────────────────────
    write_help_type(&mut out, "oxidedb_audit_events_total", "Total audit events in ring buffer", "gauge");
    writeln!(out, "oxidedb_audit_events_total {}", state.audit_logger.event_count()).ok();

    write_help_type(&mut out, "oxidedb_audit_enabled", "Whether audit logging is enabled (1/0)", "gauge");
    writeln!(
        out,
        "oxidedb_audit_enabled {}",
        if state.audit_logger.is_enabled() { 1 } else { 0 }
    )
    .ok();

    // ── Query Engine ────────────────────────────────────────────
    let prepared = state.query_engine.list_prepared_statements();

    write_help_type(
        &mut out,
        "oxidedb_prepared_statements_total",
        "Number of cached prepared statements",
        "gauge",
    );
    writeln!(out, "oxidedb_prepared_statements_total {}", prepared.len()).ok();

    // ── RBAC ────────────────────────────────────────────────────
    let users = state.rbac.list_users();

    write_help_type(&mut out, "oxidedb_rbac_users_total", "Number of RBAC users", "gauge");
    writeln!(out, "oxidedb_rbac_users_total {}", users.len()).ok();

    // Return as Prometheus text format
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        out,
    )
}

// ── Helpers ─────────────────────────────────────────────────────

/// Write a HELP + TYPE block for a metric family.
fn write_help_type(out: &mut String, name: &str, help: &str, mtype: &str) {
    writeln!(out, "# HELP {} {}", name, help).ok();
    writeln!(out, "# TYPE {} {}", name, mtype).ok();
}

/// Escape a label value per the Prometheus exposition format.
fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
