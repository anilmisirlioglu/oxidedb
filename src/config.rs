use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone)]
#[command(name = "oxidedb", about = "OxideDB — A Couchbase-compatible NoSQL database written in Rust")]
pub struct CliArgs {
    /// Host address to bind
    #[arg(long, default_value = "0.0.0.0")]
    pub host: String,

    /// Port to listen on
    #[arg(short, long, default_value_t = 8091)]
    pub port: u16,

    /// Data directory for persistence
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// Node name for cluster identification
    #[arg(long, default_value = "node-1")]
    pub node_name: String,

    /// Number of vBuckets per bucket
    #[arg(long, default_value_t = 1024)]
    pub num_vbuckets: u16,

    /// TTL check interval in seconds
    #[arg(long, default_value_t = 1)]
    pub ttl_check_interval_secs: u64,

    /// XDCR replication interval in milliseconds
    #[arg(long, default_value_t = 500)]
    pub xdcr_replication_interval_ms: u64,

    /// Enable WAL persistence
    #[arg(long, default_value_t = true)]
    pub enable_persistence: bool,

    /// WAL sync interval in milliseconds (legacy, kept for compat)
    #[arg(long, default_value_t = 1000)]
    pub wal_sync_interval_ms: u64,

    // ── Write buffer dual-trigger flush ──────────────────────────

    /// Max buffered ops before WAL flush (0 = no op-count trigger)
    #[arg(long, default_value_t = 5000)]
    pub wal_buffer_max_ops: usize,

    /// Max buffered bytes before WAL flush (0 = no byte-size trigger)
    #[arg(long, default_value_t = 4194304)] // 4 MB
    pub wal_buffer_max_bytes: usize,

    /// Max ms between WAL flushes (interval trigger)
    #[arg(long, default_value_t = 1000)]
    pub wal_flush_interval_ms: u64,

    /// B+ tree compaction interval in seconds
    #[arg(long, default_value_t = 30)]
    pub btree_compact_interval_secs: u64,

    /// Memcached binary protocol port (Couchbase SDK compatible)
    #[arg(long, default_value_t = 11210)]
    pub memcached_port: u16,

    /// Eviction check interval in seconds (for background NRU eviction)
    #[arg(long, default_value_t = 5)]
    pub eviction_check_interval_secs: u64,

    /// Enable TLS/SSL for all connections (HTTP + Memcached)
    #[arg(long, default_value_t = false)]
    pub tls_enabled: bool,

    /// Path to TLS certificate file (PEM format)
    #[arg(long, default_value = "")]
    pub tls_cert_path: String,

    /// Path to TLS private key file (PEM format)
    #[arg(long, default_value = "")]
    pub tls_key_path: String,

    /// TLS port for Memcached (if different from memcached_port, 0 = same port)
    #[arg(long, default_value_t = 11207)]
    pub tls_memcached_port: u16,

    /// Enable client certificate authentication (mTLS)
    #[arg(long, default_value_t = false)]
    pub client_cert_auth: bool,

    /// Path to CA certificate for client cert verification (PEM)
    #[arg(long, default_value = "")]
    pub client_ca_cert_path: String,

    /// Enable intra-cluster encryption (node-to-node TLS)
    #[arg(long, default_value_t = false)]
    pub cluster_encryption: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub node_name: String,
    pub num_vbuckets: u16,
    pub ttl_check_interval_secs: u64,
    pub xdcr_replication_interval_ms: u64,
    pub enable_persistence: bool,
    pub wal_sync_interval_ms: u64,
    // Write buffer / flush config
    pub wal_buffer_max_ops: usize,
    pub wal_buffer_max_bytes: usize,
    pub wal_flush_interval_ms: u64,
    pub btree_compact_interval_secs: u64,
    pub memcached_port: u16,
    pub eviction_check_interval_secs: u64,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_memcached_port: u16,
    pub client_cert_auth: bool,
    pub client_ca_cert_path: String,
    pub cluster_encryption: bool,
}

impl From<CliArgs> for ServerConfig {
    fn from(args: CliArgs) -> Self {
        Self {
            host: args.host,
            port: args.port,
            data_dir: args.data_dir,
            node_name: args.node_name,
            num_vbuckets: args.num_vbuckets,
            ttl_check_interval_secs: args.ttl_check_interval_secs,
            xdcr_replication_interval_ms: args.xdcr_replication_interval_ms,
            enable_persistence: args.enable_persistence,
            wal_sync_interval_ms: args.wal_sync_interval_ms,
            wal_buffer_max_ops: args.wal_buffer_max_ops,
            wal_buffer_max_bytes: args.wal_buffer_max_bytes,
            wal_flush_interval_ms: args.wal_flush_interval_ms,
            btree_compact_interval_secs: args.btree_compact_interval_secs,
            memcached_port: args.memcached_port,
            eviction_check_interval_secs: args.eviction_check_interval_secs,
            tls_enabled: args.tls_enabled,
            tls_cert_path: args.tls_cert_path,
            tls_key_path: args.tls_key_path,
            tls_memcached_port: args.tls_memcached_port,
            client_cert_auth: args.client_cert_auth,
            client_ca_cert_path: args.client_ca_cert_path,
            cluster_encryption: args.cluster_encryption,
        }
    }
}

impl ServerConfig {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
