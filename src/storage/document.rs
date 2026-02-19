use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

static CAS_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_cas() -> u64 {
    CAS_COUNTER.fetch_add(1, Ordering::SeqCst)
}

static SEQ_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_seq() -> u64 {
    SEQ_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// A document stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Document key
    pub key: String,

    /// JSON value
    pub value: serde_json::Value,

    /// Compare-And-Swap value for optimistic concurrency
    pub cas: u64,

    /// Sequence number for change tracking (used by XDCR)
    pub seq_no: u64,

    /// Revision ID for conflict resolution
    pub rev_id: u64,

    /// Expiry timestamp (None = no expiry)
    pub expiry: Option<DateTime<Utc>>,

    /// Flags for client SDK metadata
    pub flags: u32,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last modification timestamp
    pub updated_at: DateTime<Utc>,

    /// Whether this document is a deletion tombstone
    pub deleted: bool,

    /// Source cluster for XDCR tracking
    pub source_cluster: Option<String>,

    /// vBucket ID this document belongs to
    pub vbucket_id: u16,

    /// Extended Attributes (XATTRs)
    /// System xattrs start with '_' (e.g., "_sync", "_mou")
    /// User xattrs are any other namespace
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub xattrs: HashMap<String, serde_json::Value>,

    /// Last accessed timestamp for NRU eviction
    #[serde(default = "Utc::now")]
    pub last_accessed: DateTime<Utc>,

    /// Whether this document's value has been evicted from memory
    #[serde(default)]
    pub evicted: bool,
}

impl Document {
    pub fn new(key: String, value: serde_json::Value, vbucket_id: u16) -> Self {
        let now = Utc::now();
        Self {
            key,
            value,
            cas: next_cas(),
            seq_no: next_seq(),
            rev_id: 1,
            expiry: None,
            flags: 0,
            created_at: now,
            updated_at: now,
            deleted: false,
            source_cluster: None,
            vbucket_id,
            xattrs: HashMap::new(),
            last_accessed: now,
            evicted: false,
        }
    }

    #[allow(dead_code)]
    pub fn with_expiry(mut self, seconds: u64) -> Self {
        self.expiry = Some(Utc::now() + chrono::Duration::seconds(seconds as i64));
        self
    }

    #[allow(dead_code)]
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            Utc::now() > expiry
        } else {
            false
        }
    }

    /// Update document value, incrementing CAS and revision
    pub fn update(&mut self, value: serde_json::Value) {
        self.value = value;
        self.cas = next_cas();
        self.seq_no = next_seq();
        self.rev_id += 1;
        self.updated_at = Utc::now();
    }

    /// Mark as deleted (tombstone for XDCR)
    pub fn mark_deleted(&mut self) {
        self.deleted = true;
        self.value = serde_json::Value::Null;
        self.cas = next_cas();
        self.seq_no = next_seq();
        self.rev_id += 1;
        self.updated_at = Utc::now();
    }

    /// Mark document value as evicted (value-only eviction)
    pub fn evict_value(&mut self) {
        self.evicted = true;
        self.value = serde_json::Value::Null;
    }

    /// Restore evicted document value
    #[allow(dead_code)]
    pub fn restore_value(&mut self, value: serde_json::Value) {
        self.evicted = false;
        self.value = value;
        self.last_accessed = Utc::now();
    }

    /// Create a mutation entry for XDCR replication
    pub fn to_mutation(&self) -> Mutation {
        Mutation {
            key: self.key.clone(),
            value: self.value.clone(),
            cas: self.cas,
            seq_no: self.seq_no,
            rev_id: self.rev_id,
            expiry: self.expiry,
            flags: self.flags,
            updated_at: self.updated_at,
            deleted: self.deleted,
            source_cluster: self.source_cluster.clone(),
            vbucket_id: self.vbucket_id,
            xattrs: self.xattrs.clone(),
        }
    }
}

/// Represents a mutation for XDCR replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mutation {
    pub key: String,
    pub value: serde_json::Value,
    pub cas: u64,
    pub seq_no: u64,
    pub rev_id: u64,
    pub expiry: Option<DateTime<Utc>>,
    pub flags: u32,
    pub updated_at: DateTime<Utc>,
    pub deleted: bool,
    pub source_cluster: Option<String>,
    pub vbucket_id: u16,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub xattrs: HashMap<String, serde_json::Value>,
}
