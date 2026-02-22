mod api;
mod audit;
mod auth;
mod cluster;
mod config;
mod dcp;
mod error;
mod fts;
mod memcached;
mod query;
mod storage;
mod tls;
mod transactions;
mod xdcr;

use crate::api::AppState;
use crate::audit::logger::{AuditEventType, AuditLogger};
use crate::auth::rbac::RbacManager;
use crate::cluster::durability::DurabilityManager;
use crate::cluster::ClusterManager;
use crate::config::{CliArgs, ServerConfig};
use crate::dcp::replicator::IntraClusterReplicator;
use crate::dcp::stream::DcpEngine;
use crate::fts::engine::FtsEngine;
use crate::memcached::server::MemcachedServer;
use crate::query::engine::QueryEngine;
use crate::storage::engine::StorageEngine;
use crate::storage::index::IndexManager;
use crate::storage::wal::WriteBufferConfig;
use crate::transactions::engine::TransactionEngine;
use crate::xdcr::replicator::XdcrManager;
use clap::Parser;
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(true)
        .init();

    let args = CliArgs::parse();
    let config: ServerConfig = args.into();

    print_banner(&config);

    // Build write buffer config from CLI args
    let buffer_config = WriteBufferConfig {
        max_buffer_ops: config.wal_buffer_max_ops,
        max_buffer_bytes: config.wal_buffer_max_bytes,
        flush_interval_ms: config.wal_flush_interval_ms,
    };

    // Initialize storage engine with buffer config
    let data_dir = if config.enable_persistence {
        Some(config.data_dir.clone())
    } else {
        None
    };
    let storage = Arc::new(StorageEngine::new(
        config.num_vbuckets,
        data_dir,
        Some(buffer_config),
    ));

    // Load persisted data (B+ tree + WAL replay)
    if config.enable_persistence {
        if let Err(e) = storage.load_from_persistence() {
            warn!("Failed to load persisted data: {}", e);
        }
    }

    // Initialize cluster manager
    let cluster = Arc::new(ClusterManager::new(&config));

    // Initialize XDCR manager
    let xdcr = Arc::new(XdcrManager::new(
        storage.clone(),
        config.node_name.clone(),
    ));

    // Initialize index manager
    let index_manager = Arc::new(IndexManager::new());

    // Load persisted index definitions and rebuild indexes
    if config.enable_persistence {
        let storage_ref = storage.clone();
        match index_manager.load_definitions(&config.data_dir, |bucket_name| {
            match storage_ref.get_bucket(bucket_name) {
                Ok(bucket) => bucket.scan_all_documents(),
                Err(_) => Vec::new(),
            }
        }) {
            Ok(count) => {
                if count > 0 {
                    info!("Loaded {} index definitions from disk", count);
                }
            }
            Err(e) => warn!("Failed to load index definitions: {}", e),
        }
    }

    // Initialize query engine (with index support)
    let query_engine = Arc::new(QueryEngine::new(
        storage.clone(),
        index_manager.clone(),
    ));

    // Initialize FTS engine
    let fts_engine = Arc::new(FtsEngine::new(storage.clone()));

    // Load persisted FTS index definitions and rebuild
    if config.enable_persistence {
        match fts_engine.load_definitions(&config.data_dir) {
            Ok(count) => {
                if count > 0 {
                    info!("Loaded {} FTS index definitions from disk", count);
                }
            }
            Err(e) => warn!("Failed to load FTS index definitions: {}", e),
        }
    }

    // Initialize DCP engine
    let dcp_engine = Arc::new(DcpEngine::new(storage.clone()));
    info!("DCP (Database Change Protocol) engine initialized");

    // Initialize Audit Logger
    let audit_logger = Arc::new(AuditLogger::new());
    audit_logger.log(AuditEventType::ServerStarted, "OxideDB server starting".to_string());
    info!("Audit logging engine initialized");

    // Initialize RBAC Manager
    let rbac = Arc::new(RbacManager::new());
    info!("RBAC (Role-Based Access Control) manager initialized");

    // Initialize Transaction Engine
    let txn_engine = Arc::new(TransactionEngine::new(storage.clone()));
    info!("Multi-document ACID Transaction engine initialized");

    // Initialize Intra-Cluster Replicator (Couchbase-style DCP replication)
    let replicator = Arc::new(IntraClusterReplicator::new(
        storage.clone(),
        cluster.clone(),
        dcp_engine.clone(),
        config.node_name.clone(),
    ));
    info!("Intra-cluster DCP replicator initialized (Couchbase-style)");

    // Initialize Durability Manager
    let durability = Arc::new(DurabilityManager::new());
    info!("Durability manager initialized (levels: None, Majority, MajorityAndPersistToActive, PersistToMajority)");

    // Chronicle is already initialized inside ClusterManager
    info!("Chronicle metadata consensus engine initialized (Couchbase-style)");

    // Build app state
    let state = Arc::new(AppState {
        storage: storage.clone(),
        xdcr: xdcr.clone(),
        cluster: cluster.clone(),
        query_engine,
        index_manager: index_manager.clone(),
        fts_engine: fts_engine.clone(),
        dcp_engine: dcp_engine.clone(),
        audit_logger: audit_logger.clone(),
        rbac: rbac.clone(),
        txn_engine: txn_engine.clone(),
        replicator: replicator.clone(),
        durability: durability.clone(),
        config: config.clone(),
    });

    // Start background tasks
    start_background_tasks(
        storage.clone(),
        xdcr.clone(),
        cluster.clone(),
        txn_engine.clone(),
        replicator.clone(),
        durability.clone(),
        &config,
    );

    // ── TLS Setup ────────────────────────────────────────────────
    let tls_state = if config.tls_enabled {
        if config.tls_cert_path.is_empty() || config.tls_key_path.is_empty() {
            warn!("TLS enabled but cert/key paths not provided. Running without TLS.");
            tls::TlsState::disabled()
        } else {
            match tls::load_tls_config(&config.tls_cert_path, &config.tls_key_path) {
                Ok(acceptor) => {
                    info!("TLS loaded: cert={}, key={}", config.tls_cert_path, config.tls_key_path);
                    tls::TlsState::new(Some(acceptor))
                }
                Err(e) => {
                    warn!("TLS setup failed: {}. Running without TLS.", e);
                    tls::TlsState::disabled()
                }
            }
        }
    } else {
        tls::TlsState::disabled()
    };
    let tls_acceptor = tls_state.acceptor.clone();

    // Initialize Prometheus uptime clock
    api::metrics::init_start_time();

    // Build router
    let app = api::build_router(state);

    // Start Memcached binary protocol server (Couchbase SDK compatible)
    let mc_server = Arc::new(MemcachedServer::new(
        storage.clone(),
        cluster.clone(),
        index_manager.clone(),
        dcp_engine.clone(),
        config.clone(),
    ));

    // Plain Memcached server
    let mc_server_clone = mc_server.clone();
    tokio::spawn(async move {
        if let Err(e) = mc_server_clone.start().await {
            warn!("Memcached server error: {}", e);
        }
    });

    // TLS Memcached server (if TLS enabled, on separate port)
    if let Some(ref acceptor) = tls_acceptor {
        let mc_server_tls = mc_server.clone();
        let tls_mc_port = config.tls_memcached_port;
        let tls_host = config.host.clone();
        let tls_accept = acceptor.clone();
        info!("Starting TLS Memcached server on {}:{}", tls_host, tls_mc_port);
        tokio::spawn(async move {
            if let Err(e) = mc_server_tls.start_tls(&tls_host, tls_mc_port, tls_accept).await {
                warn!("TLS Memcached server error: {}", e);
            }
        });
    }

    // Start HTTP server
    let bind_addr = config.bind_address();
    if tls_state.enabled {
        info!("TLS enabled — Memcached TLS on port {}, HTTP on {} (use nginx/haproxy for HTTPS)", config.tls_memcached_port, bind_addr);
    }
    info!("Starting OxideDB server on {}", bind_addr);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Start background tasks
fn start_background_tasks(
    storage: Arc<StorageEngine>,
    xdcr: Arc<XdcrManager>,
    cluster: Arc<ClusterManager>,
    txn_engine: Arc<TransactionEngine>,
    replicator: Arc<IntraClusterReplicator>,
    durability: Arc<DurabilityManager>,
    config: &ServerConfig,
) {
    // TTL expiry task
    let ttl_interval = config.ttl_check_interval_secs;
    let storage_ttl = storage.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(ttl_interval));
        loop {
            interval.tick().await;
            storage_ttl.run_ttl_expiry();
        }
    });

    // XDCR replication task
    let xdcr_interval = config.xdcr_replication_interval_ms;
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(xdcr_interval));
        loop {
            interval.tick().await;
            xdcr.run_replication_cycle().await;
        }
    });

    // ── WAL Write Buffer flush task (dual-trigger) ──────────────────
    // This is the main persistence hot path. Checks every 50ms if
    // ANY flush trigger has been hit (ops, bytes, or interval).
    if config.enable_persistence {
        let storage_wal = storage.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(50));
            loop {
                interval.tick().await;
                if storage_wal.should_flush_wal() {
                    if let Err(e) = storage_wal.flush_wal_buffer() {
                        warn!("WAL buffer flush error: {}", e);
                    }
                }
            }
        });
    }

    // ── B+ tree compaction task ─────────────────────────────────────
    // Periodically merges WAL entries into the B+ tree data files
    // and truncates the WAL.
    if config.enable_persistence {
        let storage_compact = storage.clone();
        let compact_interval = config.btree_compact_interval_secs;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(compact_interval));
            loop {
                interval.tick().await;
                if let Err(e) = storage_compact.persist_all() {
                    warn!("B+ tree compaction error: {}", e);
                }
            }
        });
    }

    // Cluster health check + automatic failover task
    let cluster_for_chronicle = cluster.clone();
    let cluster_for_transfer = cluster.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            // First, actively ping all remote nodes to refresh heartbeat timestamps
            cluster.send_heartbeats().await;
            // Then, evaluate health based on the updated timestamps
            let events = cluster.check_node_health().await;
            for event in &events {
                info!(
                    "AUTO-FAILOVER EVENT: node='{}', type={:?}, vBuckets={}, promoted={}",
                    event.node_name, event.failover_type,
                    event.vbuckets_affected, event.replicas_promoted
                );
            }
        }
    });

    // Transaction cleanup task — expire abandoned transactions
    {
        let txn_cleanup = txn_engine.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                txn_cleanup.cleanup_expired();
                txn_cleanup.purge_old(300); // Purge completed txns older than 5 minutes
            }
        });
    }

    // Eviction background task — periodically checks all buckets
    // with eviction policies and frees memory when over quota
    {
        let storage_evict = storage.clone();
        let eviction_interval = config.eviction_check_interval_secs;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(eviction_interval));
            loop {
                interval.tick().await;
                for entry in storage_evict.buckets.iter() {
                    let bucket = entry.value();
                    let quota_bytes = (bucket.config.ram_quota_mb as usize) * 1024 * 1024;
                    if quota_bytes == 0 {
                        continue; // unlimited
                    }
                    let used = bucket.total_size_bytes();
                    if used > quota_bytes * 90 / 100 {
                        // Over 90% — try eviction
                        let target = quota_bytes * 80 / 100;
                        let evicted = bucket.run_eviction(target);
                        if evicted > 0 {
                            info!(
                                "Background eviction: freed {} items from bucket '{}' (policy: {:?})",
                                evicted,
                                bucket.config.name,
                                bucket.config.eviction_policy,
                            );
                        }
                    }
                }
            }
        });
    }

    // Intra-cluster DCP replication task (Couchbase-style active → replica)
    {
        let repl = replicator.clone();
        tokio::spawn(async move {
            repl.run().await;
        });
    }

    // vBucket data transfer execution task — actually moves data between nodes
    // after rebalance computes the transfer plan
    {
        let transfer_cluster = cluster_for_transfer.clone();
        let transfer_storage = storage.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(2));
            loop {
                interval.tick().await;
                transfer_cluster.execute_pending_transfers(&transfer_storage).await;
            }
        });
    }

    // Durability manager cleanup task — expire timed-out durable write tokens
    {
        let dur = durability.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(100));
            loop {
                interval.tick().await;
                dur.cleanup_timed_out();
            }
        });
    }

    // Chronicle metadata consensus maintenance tasks:
    // 1. GC old committed entries to prevent unbounded log growth
    // 2. Fail timed-out prepared entries
    // 3. (Future) Retry sending uncommitted entries to followers
    {
        let chronicle = cluster_for_chronicle.get_chronicle();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                // GC: keep last 1000 committed entries
                chronicle.gc_old_entries(1000);
                // Fail entries that haven't reached majority in 30 seconds
                chronicle.fail_timed_out_entries(30);
            }
        });
    }

    info!(
        "Background tasks started (TTL: {}s, XDCR: {}ms, WAL flush: {}ms/{}ops/{}B, B+tree compact: {}s, Eviction: {}s, DCP replication: active)",
        ttl_interval,
        xdcr_interval,
        config.wal_flush_interval_ms,
        config.wal_buffer_max_ops,
        config.wal_buffer_max_bytes,
        config.btree_compact_interval_secs,
        config.eviction_check_interval_secs,
    );
}

fn print_banner(config: &ServerConfig) {
    println!(r#"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║      ██████╗ ██╗  ██╗██╗██████╗ ███████╗██████╗ ██████╗     ║
║     ██╔═══██╗╚██╗██╔╝██║██╔══██╗██╔════╝██╔══██╗██╔══██╗    ║
║     ██║   ██║ ╚███╔╝ ██║██║  ██║█████╗  ██║  ██║██████╔╝    ║
║     ██║   ██║ ██╔██╗ ██║██║  ██║██╔══╝  ██║  ██║██╔══██╗    ║
║     ╚██████╔╝██╔╝ ██╗██║██████╔╝███████╗██████╔╝██████╔╝    ║
║      ╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝ ╚══════╝╚═════╝ ╚═════╝     ║
║                                                               ║
║         OxideDB — Couchbase-compatible NoSQL Database         ║
║               Written in Rust with ❤️                          ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

  Node:        {}
  REST API:    {}:{}
  KV (MC):     {}:{}
  Data Dir:    {}
  vBuckets:    {}
  Persistence: {}
  Storage:     B+ Tree (4KB pages, binary)
  WAL Buffer:  {}ops / {}B / {}ms (dual-trigger)
  SDK:         Couchbase SDK compatible (port {})
  Replication: DCP intra-cluster (active → replica)
  Consensus:   Chronicle (metadata) + DCP (data)
  Durability:  None | Majority | MajorityAndPersist | PersistToMajority
  Groups:      Server Groups (rack/zone awareness)
  CertAuth:    {} 
  ClusterEnc:  {}
  TLS:         {} {}
"#,
        config.node_name,
        config.host,
        config.port,
        config.host,
        config.memcached_port,
        config.data_dir,
        config.num_vbuckets,
        if config.enable_persistence { "enabled" } else { "disabled" },
        config.wal_buffer_max_ops,
        config.wal_buffer_max_bytes,
        config.wal_flush_interval_ms,
        config.memcached_port,
        if config.client_cert_auth { "x509 cert auth enabled" } else { "password auth" },
        if config.cluster_encryption { "enabled (node-to-node TLS)" } else { "disabled" },
        if config.tls_enabled { "enabled" } else { "disabled" },
        if config.tls_enabled { format!("(MC TLS port: {})", config.tls_memcached_port) } else { String::new() },
    );
}
