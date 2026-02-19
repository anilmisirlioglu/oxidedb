//! DCP (Database Change Protocol) Streaming Engine
//!
//! Implements a simplified Couchbase DCP-like change stream service:
//! - Mutation events (insert, update, delete, expiry)
//! - Per-vBucket sequencing with failover logs
//! - Multi-stream support with independent cursors
//! - REST API for stream management
//! - SSE (Server-Sent Events) for real-time streaming
//! - Backfill from current state + live mutations
//! - Persistent cursor checkpoints

use crate::storage::engine::StorageEngine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info};

// ═══════════════════════════════════════════════════════════════════════
// DCP Event types
// ═══════════════════════════════════════════════════════════════════════

/// Type of mutation event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DcpEventType {
    Mutation,
    Deletion,
    Expiration,
}

/// A single DCP event (mutation, deletion, or expiration)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DcpEvent {
    /// Event type
    pub event_type: DcpEventType,
    /// Document key
    pub key: String,
    /// Document value (None for deletions/expirations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// CAS value
    pub cas: u64,
    /// Revision sequence number
    pub seq_no: u64,
    /// vBucket ID
    pub vbucket_id: u16,
    /// Bucket name
    pub bucket: String,
    /// Scope name
    pub scope: String,
    /// Collection name
    pub collection: String,
    /// Timestamp of the event
    pub timestamp: String,
    /// Document expiry (0 = no expiry)
    pub expiry: u32,
    /// Flags
    pub flags: u32,
    /// Datatype
    pub datatype: u8,
}

/// Stream state per vBucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VBucketStreamState {
    /// Last sequence number consumed
    pub start_seq_no: u64,
    /// End sequence number (0 = stream forever)
    pub end_seq_no: u64,
    /// vBucket UUID for failover detection
    pub vbucket_uuid: u64,
}

/// A DCP stream definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DcpStream {
    /// Unique stream ID
    pub id: String,
    /// Descriptive name
    pub name: String,
    /// Bucket to stream from
    pub bucket: String,
    /// Scope filter (empty = all scopes)
    pub scope_filter: Option<String>,
    /// Collection filter (empty = all collections)
    pub collection_filter: Option<String>,
    /// Created timestamp
    pub created_at: String,
    /// Is the stream active?
    pub active: bool,
    /// Total events streamed
    pub events_streamed: u64,
    /// Per-vBucket cursor positions
    pub cursors: HashMap<u16, u64>,
}

/// Request to create a DCP stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateStreamRequest {
    pub name: String,
    pub bucket: String,
    #[serde(default)]
    pub scope_filter: Option<String>,
    #[serde(default)]
    pub collection_filter: Option<String>,
    /// Start from sequence number (0 = beginning)
    #[serde(default)]
    pub from_seq_no: u64,
    /// Whether to include current documents (backfill)
    #[serde(default = "default_true")]
    pub include_backfill: bool,
}

fn default_true() -> bool {
    true
}

/// DCP stream status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatus {
    pub stream: DcpStream,
    pub buffer_size: usize,
}

// ═══════════════════════════════════════════════════════════════════════
// DCP Engine
// ═══════════════════════════════════════════════════════════════════════

pub struct DcpEngine {
    storage: Arc<StorageEngine>,
    /// Active streams: stream_id → DcpStream
    streams: std::sync::RwLock<HashMap<String, DcpStream>>,
    /// Broadcast channel for live events
    event_tx: broadcast::Sender<DcpEvent>,
    /// Global sequence counter
    global_seq: AtomicU64,
}

impl DcpEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        let (event_tx, _) = broadcast::channel(10_000); // buffer 10k events
        Self {
            storage,
            streams: std::sync::RwLock::new(HashMap::new()),
            event_tx,
            global_seq: AtomicU64::new(1),
        }
    }

    /// Get the broadcast receiver for live events
    pub fn subscribe(&self) -> broadcast::Receiver<DcpEvent> {
        self.event_tx.subscribe()
    }

    /// Publish a mutation event (called by storage engine on writes)
    pub fn publish_mutation(
        &self,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
        value: Option<&serde_json::Value>,
        cas: u64,
        vbucket_id: u16,
        expiry: u32,
    ) {
        let seq = self.global_seq.fetch_add(1, Ordering::SeqCst);
        let event = DcpEvent {
            event_type: DcpEventType::Mutation,
            key: key.to_string(),
            value: value.cloned(),
            cas,
            seq_no: seq,
            vbucket_id,
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            timestamp: Utc::now().to_rfc3339(),
            expiry,
            flags: 0,
            datatype: if value.is_some() { 1 } else { 0 }, // 1 = JSON
        };

        // Update stream cursors
        if let Ok(mut streams) = self.streams.write() {
            for stream in streams.values_mut() {
                if stream.active && stream.bucket == bucket {
                    if let Some(sf) = &stream.scope_filter {
                        if sf != scope {
                            continue;
                        }
                    }
                    if let Some(cf) = &stream.collection_filter {
                        if cf != collection {
                            continue;
                        }
                    }
                    stream.events_streamed += 1;
                    stream.cursors.insert(vbucket_id, seq);
                }
            }
        }

        // Broadcast (ignore errors - means no active receivers)
        let _ = self.event_tx.send(event);
    }

    /// Publish a deletion event
    pub fn publish_deletion(
        &self,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
        cas: u64,
        vbucket_id: u16,
    ) {
        let seq = self.global_seq.fetch_add(1, Ordering::SeqCst);
        let event = DcpEvent {
            event_type: DcpEventType::Deletion,
            key: key.to_string(),
            value: None,
            cas,
            seq_no: seq,
            vbucket_id,
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            timestamp: Utc::now().to_rfc3339(),
            expiry: 0,
            flags: 0,
            datatype: 0,
        };

        if let Ok(mut streams) = self.streams.write() {
            for stream in streams.values_mut() {
                if stream.active && stream.bucket == bucket {
                    stream.events_streamed += 1;
                    stream.cursors.insert(vbucket_id, seq);
                }
            }
        }

        let _ = self.event_tx.send(event);
    }

    /// Publish an expiration event
    #[allow(dead_code)]
    pub fn publish_expiration(
        &self,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
        cas: u64,
        vbucket_id: u16,
    ) {
        let seq = self.global_seq.fetch_add(1, Ordering::SeqCst);
        let event = DcpEvent {
            event_type: DcpEventType::Expiration,
            key: key.to_string(),
            value: None,
            cas,
            seq_no: seq,
            vbucket_id,
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            timestamp: Utc::now().to_rfc3339(),
            expiry: 0,
            flags: 0,
            datatype: 0,
        };

        let _ = self.event_tx.send(event);
    }

    // =================================================================
    // Stream management
    // =================================================================

    /// Create a new DCP stream
    pub fn create_stream(&self, req: &CreateStreamRequest) -> Result<DcpStream, String> {
        // Verify bucket exists
        self.storage
            .get_bucket(&req.bucket)
            .map_err(|e| format!("Bucket error: {}", e))?;

        let stream_id = generate_stream_id();

        let stream = DcpStream {
            id: stream_id.clone(),
            name: req.name.clone(),
            bucket: req.bucket.clone(),
            scope_filter: req.scope_filter.clone(),
            collection_filter: req.collection_filter.clone(),
            created_at: Utc::now().to_rfc3339(),
            active: true,
            events_streamed: 0,
            cursors: HashMap::new(),
        };

        let mut streams = self.streams.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        streams.insert(stream_id.clone(), stream.clone());

        info!("DCP stream '{}' ({}) created for bucket '{}'", req.name, stream_id, req.bucket);
        Ok(stream)
    }

    /// Get stream info
    pub fn get_stream(&self, stream_id: &str) -> Option<DcpStream> {
        let streams = self.streams.read().ok()?;
        streams.get(stream_id).cloned()
    }

    /// List all streams
    pub fn list_streams(&self) -> Vec<DcpStream> {
        self.streams.read().map(|s| s.values().cloned().collect()).unwrap_or_default()
    }

    /// Close/delete a stream
    pub fn close_stream(&self, stream_id: &str) -> Result<(), String> {
        let mut streams = self.streams.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        if streams.remove(stream_id).is_none() {
            return Err(format!("Stream '{}' not found", stream_id));
        }
        info!("DCP stream '{}' closed", stream_id);
        Ok(())
    }

    /// Pause a stream
    pub fn pause_stream(&self, stream_id: &str) -> Result<(), String> {
        let mut streams = self.streams.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        let stream = streams
            .get_mut(stream_id)
            .ok_or_else(|| format!("Stream '{}' not found", stream_id))?;
        stream.active = false;
        info!("DCP stream '{}' paused", stream_id);
        Ok(())
    }

    /// Resume a stream
    pub fn resume_stream(&self, stream_id: &str) -> Result<(), String> {
        let mut streams = self.streams.write().map_err(|e| format!("Lock poisoned: {}", e))?;
        let stream = streams
            .get_mut(stream_id)
            .ok_or_else(|| format!("Stream '{}' not found", stream_id))?;
        stream.active = true;
        info!("DCP stream '{}' resumed", stream_id);
        Ok(())
    }

    /// Perform backfill: scan all existing documents and return them as events
    pub fn backfill(
        &self,
        bucket_name: &str,
        scope_filter: Option<&str>,
        collection_filter: Option<&str>,
    ) -> Result<Vec<DcpEvent>, String> {
        let bucket = self
            .storage
            .get_bucket(bucket_name)
            .map_err(|e| format!("Bucket error: {}", e))?;

        let docs = bucket.scan_all_documents();
        let mut events = Vec::new();

        // Default scope/collection for scan_all_documents
        let scope_name = scope_filter.unwrap_or("_default");
        let collection_name = collection_filter.unwrap_or("_default");

        for doc in &docs {
            if doc.deleted {
                continue;
            }

            let value = Some(doc.value.clone());
            let seq = self.global_seq.fetch_add(1, Ordering::SeqCst);

            events.push(DcpEvent {
                event_type: DcpEventType::Mutation,
                key: doc.key.clone(),
                value,
                cas: doc.cas,
                seq_no: seq,
                vbucket_id: doc.vbucket_id,
                bucket: bucket_name.to_string(),
                scope: scope_name.to_string(),
                collection: collection_name.to_string(),
                timestamp: doc.updated_at.to_rfc3339(),
                expiry: doc.expiry.map(|e| {
                    let now = Utc::now();
                    if e > now {
                        (e - now).num_seconds().max(0) as u32
                    } else {
                        0
                    }
                }).unwrap_or(0),
                flags: 0,
                datatype: 1, // JSON
            });
        }

        debug!("DCP backfill for '{}': {} events", bucket_name, events.len());
        Ok(events)
    }

    /// Get failover log for a vBucket
    #[allow(dead_code)]
    pub fn get_failover_log(&self, _bucket: &str, _vbucket_id: u16) -> Vec<(u64, u64)> {
        // Simplified: return a single entry with uuid=0, seq=0
        vec![(0, 0)]
    }

    /// Get current high sequence number for a vBucket
    #[allow(dead_code)]
    pub fn get_high_seq_no(&self, _bucket: &str, _vbucket_id: u16) -> u64 {
        self.global_seq.load(Ordering::SeqCst)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Utilities
// ═══════════════════════════════════════════════════════════════════════

fn generate_stream_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;

    let mut h = DefaultHasher::new();
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut h);
    format!("dcp-{:016x}", h.finish())
}
