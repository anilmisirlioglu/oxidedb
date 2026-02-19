//! Intra-Cluster Replication via DCP (Couchbase-style)
//!
//! Couchbase's approach to data replication:
//! 1. When a document is written to an active vBucket, the mutation is streamed
//!    to all replica nodes for that vBucket via DCP.
//! 2. Replica nodes apply the mutation to their local copy of the vBucket.
//! 3. For durable writes, the replica sends an ACK back to the active node.
//!
//! This is NOT Raft log replication. Key differences:
//! - No leader election (orchestrator is deterministic)
//! - No consensus required for writes (write to active, async replicate)
//! - Durability is optional (DurabilityLevel::None is default for max perf)
//! - Replication is per-vBucket, not per-cluster
//!
//! Architecture:
//!   Client → Active vBucket → ACK → (async) DCP → Replica vBucket
//!                                                  ↓
//!                                           ACK back (if durable)

use crate::cluster::ClusterManager;
use crate::dcp::stream::{DcpEngine, DcpEvent, DcpEventType};
use crate::storage::document::Mutation;
use crate::storage::engine::StorageEngine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// A replication mutation sent from active → replica
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationMutation {
    /// Bucket name
    pub bucket: String,
    /// vBucket ID
    pub vbucket_id: u16,
    /// Document key
    pub key: String,
    /// Document value (None for deletions)
    pub value: Option<serde_json::Value>,
    /// CAS value
    pub cas: u64,
    /// Sequence number
    pub seq_no: u64,
    /// Revision ID
    pub rev_id: u64,
    /// Expiry (epoch seconds, 0 = no expiry)
    pub expiry: u64,
    /// Flags
    pub flags: u32,
    /// Whether this is a deletion
    pub deleted: bool,
    /// Source node
    pub source_node: String,
}

/// Batch of replication mutations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationBatch {
    /// Source node name
    pub source_node: String,
    /// Mutations in this batch
    pub mutations: Vec<ReplicationMutation>,
}

/// Response from a replica node after receiving mutations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationAck {
    /// Node that received the mutations
    pub node: String,
    /// vBucket ACKs: vbucket_id → highest seq_no received
    pub vbucket_acks: HashMap<u16, u64>,
    /// Number of mutations applied
    pub applied: usize,
    /// Number of mutations rejected (e.g., CAS conflict with higher local)
    pub rejected: usize,
}

/// Per-vBucket replication progress tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VBucketReplicationProgress {
    /// Highest sequence number sent to each replica
    pub sent_seq: HashMap<String, u64>,
    /// Highest sequence number ACKed by each replica
    pub acked_seq: HashMap<String, u64>,
    /// Pending (in-flight) mutations count
    pub pending_count: u64,
    /// Total mutations replicated
    pub total_replicated: u64,
    /// Total replication errors
    pub total_errors: u64,
}

/// Overall replication status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    /// Is replication active?
    pub active: bool,
    /// Node name
    pub node_name: String,
    /// Per-vBucket progress
    pub vbucket_progress: HashMap<u16, VBucketReplicationProgress>,
    /// Total mutations replicated across all vBuckets
    pub total_replicated: u64,
    /// Total replication errors
    pub total_errors: u64,
    /// Replication lag (estimated, in seq numbers)
    pub estimated_lag: u64,
}

/// The intra-cluster replicator.
/// Subscribes to local DCP events and forwards mutations to replica nodes.
pub struct IntraClusterReplicator {
    storage: Arc<StorageEngine>,
    cluster: Arc<ClusterManager>,
    dcp_engine: Arc<DcpEngine>,
    node_name: String,

    /// Whether replication is active
    active: AtomicBool,
    /// Total mutations replicated
    total_replicated: AtomicU64,
    /// Total replication errors
    total_errors: AtomicU64,
    /// Per-vBucket replication progress
    progress: RwLock<HashMap<u16, VBucketReplicationProgress>>,
}

impl IntraClusterReplicator {
    pub fn new(
        storage: Arc<StorageEngine>,
        cluster: Arc<ClusterManager>,
        dcp_engine: Arc<DcpEngine>,
        node_name: String,
    ) -> Self {
        Self {
            storage,
            cluster,
            dcp_engine,
            node_name,
            active: AtomicBool::new(true),
            total_replicated: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            progress: RwLock::new(HashMap::new()),
        }
    }

    /// Start the replication loop.
    /// This subscribes to DCP events and forwards them to replica nodes.
    pub async fn run(&self) {
        info!(
            "Intra-cluster replicator started on node '{}'",
            self.node_name
        );

        let mut rx = self.dcp_engine.subscribe();

        loop {
            if !self.active.load(Ordering::Relaxed) {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            match rx.recv().await {
                Ok(event) => {
                    self.handle_dcp_event(event).await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Replicator lagged behind by {} events", n);
                    // Continue receiving — we'll catch up
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    info!("DCP channel closed, replicator stopping");
                    break;
                }
            }
        }
    }

    /// Handle a single DCP event by forwarding to replica nodes
    async fn handle_dcp_event(&self, event: DcpEvent) {
        // Only replicate events for vBuckets we are active owner of
        if !self.cluster.is_vbucket_local(event.vbucket_id).await {
            return;
        }

        // Get replica nodes for this vBucket
        let pmap = self.cluster.get_partition_map().await;
        let replica_nodes: Vec<String> = pmap
            .map
            .get(event.vbucket_id as usize)
            .map(|o| o.replica_nodes.clone())
            .unwrap_or_default();

        if replica_nodes.is_empty() {
            return; // No replicas configured
        }

        // Build replication mutation
        let mutation = ReplicationMutation {
            bucket: event.bucket.clone(),
            vbucket_id: event.vbucket_id,
            key: event.key.clone(),
            value: event.value.clone(),
            cas: event.cas,
            seq_no: event.seq_no,
            rev_id: 0, // Will be set by the storage engine
            expiry: event.expiry as u64,
            flags: event.flags,
            deleted: event.event_type == DcpEventType::Deletion,
            source_node: self.node_name.clone(),
        };

        // Send to each replica node
        for replica_node in &replica_nodes {
            if let Some(url) = self.cluster.get_node_url(replica_node).await {
                let mutation = mutation.clone();
                let url = url.clone();
                let replica = replica_node.clone();
                let total_replicated = &self.total_replicated;
                let total_errors = &self.total_errors;

                match self.send_to_replica(&url, &mutation).await {
                    Ok(_) => {
                        total_replicated.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "Replicated {}:{} to {} (seq={})",
                            event.bucket, event.key, replica, event.seq_no
                        );

                        // Update progress
                        {
                            let mut progress = self.progress.write().await;
                            let vb_progress = progress
                                .entry(event.vbucket_id)
                                .or_insert_with(|| VBucketReplicationProgress {
                                    sent_seq: HashMap::new(),
                                    acked_seq: HashMap::new(),
                                    pending_count: 0,
                                    total_replicated: 0,
                                    total_errors: 0,
                                });
                            vb_progress
                                .sent_seq
                                .insert(replica.clone(), event.seq_no);
                            vb_progress.total_replicated += 1;
                        }
                    }
                    Err(e) => {
                        total_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "Failed to replicate {}:{} to {}: {}",
                            event.bucket, event.key, replica, e
                        );

                        {
                            let mut progress = self.progress.write().await;
                            let vb_progress = progress
                                .entry(event.vbucket_id)
                                .or_insert_with(|| VBucketReplicationProgress {
                                    sent_seq: HashMap::new(),
                                    acked_seq: HashMap::new(),
                                    pending_count: 0,
                                    total_replicated: 0,
                                    total_errors: 0,
                                });
                            vb_progress.total_errors += 1;
                        }
                    }
                }
            }
        }
    }

    /// Send a single mutation to a replica node via HTTP
    async fn send_to_replica(
        &self,
        base_url: &str,
        mutation: &ReplicationMutation,
    ) -> Result<ReplicationAck, String> {
        let url = format!("{}/api/v1/internal/replicate", base_url);
        let batch = ReplicationBatch {
            source_node: self.node_name.clone(),
            mutations: vec![mutation.clone()],
        };

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| format!("HTTP client error: {}", e))?;

        let resp = client
            .post(&url)
            .json(&batch)
            .send()
            .await
            .map_err(|e| format!("Send failed: {}", e))?;

        if resp.status().is_success() {
            let ack: ReplicationAck = resp
                .json()
                .await
                .map_err(|e| format!("Response parse error: {}", e))?;
            Ok(ack)
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(format!("Replica returned {}: {}", status, body))
        }
    }

    /// Receive a batch of mutations from an active node (called on replica side)
    pub fn receive_batch(&self, batch: &ReplicationBatch) -> ReplicationAck {
        let mut applied = 0;
        let mut rejected = 0;
        let mut vbucket_acks: HashMap<u16, u64> = HashMap::new();

        for mutation in &batch.mutations {
            match self.apply_replica_mutation(mutation) {
                Ok(_) => {
                    applied += 1;
                    let entry = vbucket_acks
                        .entry(mutation.vbucket_id)
                        .or_insert(0);
                    if mutation.seq_no > *entry {
                        *entry = mutation.seq_no;
                    }
                }
                Err(e) => {
                    rejected += 1;
                    debug!(
                        "Rejected replica mutation for {}:{}: {}",
                        mutation.bucket, mutation.key, e
                    );
                }
            }
        }

        if applied > 0 {
            debug!(
                "Applied {} mutations from node '{}' ({} rejected)",
                applied, batch.source_node, rejected
            );
        }

        ReplicationAck {
            node: self.node_name.clone(),
            vbucket_acks,
            applied,
            rejected,
        }
    }

    /// Apply a single mutation on the replica side
    fn apply_replica_mutation(&self, mutation: &ReplicationMutation) -> Result<(), String> {
        let bucket = self
            .storage
            .get_bucket(&mutation.bucket)
            .map_err(|e| format!("Bucket error: {}", e))?;

        // Build a storage Mutation
        let storage_mutation = Mutation {
            key: mutation.key.clone(),
            value: mutation.value.clone().unwrap_or(serde_json::Value::Null),
            cas: mutation.cas,
            seq_no: mutation.seq_no,
            rev_id: mutation.rev_id,
            expiry: if mutation.expiry > 0 {
                Some(Utc::now() + chrono::Duration::seconds(mutation.expiry as i64))
            } else {
                None
            },
            flags: mutation.flags,
            updated_at: Utc::now(),
            deleted: mutation.deleted,
            source_cluster: Some(mutation.source_node.clone()),
            vbucket_id: mutation.vbucket_id,
            xattrs: HashMap::new(),
        };

        bucket
            .apply_mutation(&storage_mutation)
            .map_err(|e| format!("Apply error: {}", e))
    }

    /// Get replication status
    pub async fn get_status(&self) -> ReplicationStatus {
        let progress = self.progress.read().await;
        let mut estimated_lag: u64 = 0;

        for (_, vb_progress) in progress.iter() {
            for (node, sent) in &vb_progress.sent_seq {
                let acked = vb_progress.acked_seq.get(node).copied().unwrap_or(0);
                estimated_lag += sent.saturating_sub(acked);
            }
        }

        ReplicationStatus {
            active: self.active.load(Ordering::Relaxed),
            node_name: self.node_name.clone(),
            vbucket_progress: progress.clone(),
            total_replicated: self.total_replicated.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            estimated_lag,
        }
    }

    /// Pause replication
    pub fn pause(&self) {
        self.active.store(false, Ordering::Relaxed);
        info!("Intra-cluster replication paused");
    }

    /// Resume replication
    pub fn resume(&self) {
        self.active.store(true, Ordering::Relaxed);
        info!("Intra-cluster replication resumed");
    }
}
