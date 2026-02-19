use crate::error::{NosqlError, Result};
use crate::storage::document::Mutation;
use crate::storage::engine::{ConflictResolutionType, StorageEngine};
use crate::xdcr::conflict::{ConflictResolver, ConflictStats, ConflictWinner};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Remote cluster reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteClusterRef {
    pub name: String,
    pub hostname: String,
    pub port: u16,
    pub username: Option<String>,
    pub secure: bool,
    pub created_at: DateTime<Utc>,
}

impl RemoteClusterRef {
    pub fn base_url(&self) -> String {
        let scheme = if self.secure { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.hostname, self.port)
    }
}

/// Replication status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStatus {
    Running,
    Paused,
    Error,
    Initializing,
}

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub id: String,
    pub source_bucket: String,
    pub target_cluster: String,
    pub target_bucket: String,
    pub conflict_resolution: ConflictResolutionType,
    pub filter_expression: Option<String>,
    pub batch_size: usize,
    pub bidirectional: bool,
    pub created_at: DateTime<Utc>,
}

/// Replication state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationState {
    pub config: ReplicationConfig,
    pub status: ReplicationStatus,
    /// Checkpoint: per-vBucket sequence numbers
    pub checkpoints: HashMap<u16, u64>,
    pub stats: ConflictStats,
    pub last_replicated_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

/// XDCR batch request for sending mutations to remote cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdcrBatchRequest {
    pub source_cluster: String,
    pub source_bucket: String,
    pub target_bucket: String,
    pub mutations: Vec<Mutation>,
}

/// XDCR batch response from remote cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdcrBatchResponse {
    pub accepted: u64,
    pub rejected: u64,
    pub conflicts: u64,
}

/// The main XDCR manager
pub struct XdcrManager {
    /// Remote cluster references
    pub remote_clusters: RwLock<HashMap<String, RemoteClusterRef>>,
    /// Active replications
    pub replications: RwLock<HashMap<String, ReplicationState>>,
    /// Reference to the storage engine
    storage: Arc<StorageEngine>,
    /// This node's cluster name
    pub local_cluster_name: String,
    /// HTTP client for replication
    http_client: reqwest::Client,
}

impl XdcrManager {
    pub fn new(storage: Arc<StorageEngine>, local_cluster_name: String) -> Self {
        Self {
            remote_clusters: RwLock::new(HashMap::new()),
            replications: RwLock::new(HashMap::new()),
            storage,
            local_cluster_name,
            http_client: reqwest::Client::new(),
        }
    }

    // ---- Remote Cluster Management ----

    pub async fn add_remote_cluster(&self, cluster: RemoteClusterRef) -> Result<()> {
        let mut clusters = self.remote_clusters.write().await;
        if clusters.contains_key(&cluster.name) {
            return Err(NosqlError::RemoteClusterAlreadyExists(cluster.name));
        }
        info!("Added remote cluster reference: {}", cluster.name);
        clusters.insert(cluster.name.clone(), cluster);
        Ok(())
    }

    pub async fn remove_remote_cluster(&self, name: &str) -> Result<()> {
        let mut clusters = self.remote_clusters.write().await;
        clusters
            .remove(name)
            .ok_or_else(|| NosqlError::RemoteClusterNotFound(name.to_string()))?;
        info!("Removed remote cluster reference: {}", name);
        Ok(())
    }

    pub async fn list_remote_clusters(&self) -> Vec<RemoteClusterRef> {
        let clusters = self.remote_clusters.read().await;
        clusters.values().cloned().collect()
    }

    pub async fn get_remote_cluster(&self, name: &str) -> Result<RemoteClusterRef> {
        let clusters = self.remote_clusters.read().await;
        clusters
            .get(name)
            .cloned()
            .ok_or_else(|| NosqlError::RemoteClusterNotFound(name.to_string()))
    }

    // ---- Replication Management ----

    pub async fn create_replication(&self, config: ReplicationConfig) -> Result<()> {
        // Verify source bucket exists
        self.storage.get_bucket(&config.source_bucket)?;

        // Verify remote cluster exists
        let _cluster = self.get_remote_cluster(&config.target_cluster).await?;

        let mut replications = self.replications.write().await;
        if replications.contains_key(&config.id) {
            return Err(NosqlError::ReplicationAlreadyExists(config.id));
        }

        let state = ReplicationState {
            config: config.clone(),
            status: ReplicationStatus::Initializing,
            checkpoints: HashMap::new(),
            stats: ConflictStats::default(),
            last_replicated_at: None,
            error_message: None,
        };

        info!(
            "Created XDCR replication '{}': {} -> {}/{}",
            config.id, config.source_bucket, config.target_cluster, config.target_bucket
        );
        replications.insert(config.id.clone(), state);
        Ok(())
    }

    pub async fn delete_replication(&self, id: &str) -> Result<()> {
        let mut replications = self.replications.write().await;
        replications
            .remove(id)
            .ok_or_else(|| NosqlError::ReplicationNotFound(id.to_string()))?;
        info!("Deleted XDCR replication '{}'", id);
        Ok(())
    }

    pub async fn pause_replication(&self, id: &str) -> Result<()> {
        let mut replications = self.replications.write().await;
        let state = replications
            .get_mut(id)
            .ok_or_else(|| NosqlError::ReplicationNotFound(id.to_string()))?;
        state.status = ReplicationStatus::Paused;
        info!("Paused XDCR replication '{}'", id);
        Ok(())
    }

    pub async fn resume_replication(&self, id: &str) -> Result<()> {
        let mut replications = self.replications.write().await;
        let state = replications
            .get_mut(id)
            .ok_or_else(|| NosqlError::ReplicationNotFound(id.to_string()))?;
        state.status = ReplicationStatus::Running;
        state.error_message = None;
        info!("Resumed XDCR replication '{}'", id);
        Ok(())
    }

    pub async fn list_replications(&self) -> Vec<ReplicationState> {
        let replications = self.replications.read().await;
        replications.values().cloned().collect()
    }

    pub async fn get_replication(&self, id: &str) -> Result<ReplicationState> {
        let replications = self.replications.read().await;
        replications
            .get(id)
            .cloned()
            .ok_or_else(|| NosqlError::ReplicationNotFound(id.to_string()))
    }

    // ---- Replication Engine ----

    /// Run one replication cycle for all active replications
    pub async fn run_replication_cycle(&self) {
        let replication_ids: Vec<String> = {
            let replications = self.replications.read().await;
            replications
                .iter()
                .filter(|(_, state)| state.status == ReplicationStatus::Running || state.status == ReplicationStatus::Initializing)
                .map(|(id, _)| id.clone())
                .collect()
        };

        for id in replication_ids {
            if let Err(e) = self.replicate_one(&id).await {
                error!("XDCR replication '{}' error: {}", id, e);
                let mut replications = self.replications.write().await;
                if let Some(state) = replications.get_mut(&id) {
                    state.status = ReplicationStatus::Error;
                    state.error_message = Some(e.to_string());
                }
            }
        }
    }

    /// Run one replication for a specific replication config
    async fn replicate_one(&self, replication_id: &str) -> Result<()> {
        let (config, checkpoints) = {
            let replications = self.replications.read().await;
            let state = replications
                .get(replication_id)
                .ok_or_else(|| NosqlError::ReplicationNotFound(replication_id.to_string()))?;
            (state.config.clone(), state.checkpoints.clone())
        };

        // Get the source bucket
        let bucket = self.storage.get_bucket(&config.source_bucket)?;

        // Get the remote cluster
        let remote_cluster = self.get_remote_cluster(&config.target_cluster).await?;

        // Collect mutations from all vBuckets since last checkpoint
        let mut all_mutations: Vec<Mutation> = Vec::new();
        let mut new_checkpoints: HashMap<u16, u64> = checkpoints.clone();

        for vb_id in 0..bucket.config.num_vbuckets {
            let since_seq = checkpoints.get(&vb_id).copied().unwrap_or(0);
            if let Ok(mutations) = bucket.get_mutations_since(vb_id, since_seq) {
                for mut mutation in mutations {
                    // Tag with source cluster
                    mutation.source_cluster = Some(self.local_cluster_name.clone());

                    // Apply filter if configured
                    if let Some(ref filter) = config.filter_expression {
                        if !self.matches_filter(&mutation, filter) {
                            continue;
                        }
                    }

                    // Update checkpoint
                    let current = new_checkpoints.get(&vb_id).copied().unwrap_or(0);
                    if mutation.seq_no > current {
                        new_checkpoints.insert(vb_id, mutation.seq_no);
                    }

                    all_mutations.push(mutation);
                }
            }
        }

        if all_mutations.is_empty() {
            // Update status to running if initializing
            let mut replications = self.replications.write().await;
            if let Some(state) = replications.get_mut(replication_id) {
                if state.status == ReplicationStatus::Initializing {
                    state.status = ReplicationStatus::Running;
                }
            }
            return Ok(());
        }

        // Send mutations in batches
        let batch_size = config.batch_size.max(1);
        let mut total_accepted = 0u64;
        let mut total_rejected = 0u64;
        let mut total_conflicts = 0u64;

        for batch in all_mutations.chunks(batch_size) {
            let request = XdcrBatchRequest {
                source_cluster: self.local_cluster_name.clone(),
                source_bucket: config.source_bucket.clone(),
                target_bucket: config.target_bucket.clone(),
                mutations: batch.to_vec(),
            };

            match self.send_batch(&remote_cluster, &request).await {
                Ok(response) => {
                    total_accepted += response.accepted;
                    total_rejected += response.rejected;
                    total_conflicts += response.conflicts;
                }
                Err(e) => {
                    warn!(
                        "Failed to send XDCR batch to {}: {}",
                        remote_cluster.name, e
                    );
                    return Err(NosqlError::XdcrConnectionError(e.to_string()));
                }
            }
        }

        // Update replication state
        let mut replications = self.replications.write().await;
        if let Some(state) = replications.get_mut(replication_id) {
            state.status = ReplicationStatus::Running;
            state.checkpoints = new_checkpoints;
            state.stats.docs_replicated += total_accepted;
            state.stats.docs_failed += total_rejected;
            state.stats.total_conflicts += total_conflicts;
            state.last_replicated_at = Some(Utc::now());
            state.error_message = None;

            debug!(
                "XDCR '{}': replicated {} docs, {} conflicts, {} failed",
                replication_id, total_accepted, total_conflicts, total_rejected
            );
        }

        Ok(())
    }

    /// Send a batch of mutations to a remote cluster
    async fn send_batch(
        &self,
        remote: &RemoteClusterRef,
        request: &XdcrBatchRequest,
    ) -> std::result::Result<XdcrBatchResponse, String> {
        let url = format!("{}/api/v1/xdcr/receive", remote.base_url());

        match self.http_client.post(&url).json(request).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    response
                        .json::<XdcrBatchResponse>()
                        .await
                        .map_err(|e| format!("Failed to parse response: {}", e))
                } else {
                    Err(format!("Remote returned status: {}", response.status()))
                }
            }
            Err(e) => Err(format!("Connection error: {}", e)),
        }
    }

    /// Receive a batch of mutations from a remote cluster (incoming XDCR)
    pub async fn receive_batch(&self, request: XdcrBatchRequest) -> Result<XdcrBatchResponse> {
        let bucket = self.storage.get_bucket(&request.target_bucket)?;
        let resolver = ConflictResolver::new(bucket.config.conflict_resolution);

        let mut accepted = 0u64;
        let mut rejected = 0u64;
        let mut conflicts = 0u64;

        for mutation in &request.mutations {
            // Skip mutations that originated from this cluster (prevent loops)
            if mutation.source_cluster.as_deref() == Some(&self.local_cluster_name) {
                continue;
            }

            // Check for conflicts
            let key = &mutation.key;
            let vb_id = crate::storage::vbucket::hash_to_vbucket(key, bucket.config.num_vbuckets);

            let should_apply = {
                let vb = bucket.vbuckets[vb_id as usize]
                    .read()
                    .map_err(|e| NosqlError::Internal(e.to_string()))?;

                match vb.get(key) {
                    Ok(local_doc) => {
                        // Conflict! Resolve it.
                        conflicts += 1;
                        let winner = resolver.resolve(local_doc, mutation);
                        winner == ConflictWinner::Remote
                    }
                    Err(_) => {
                        // No local document, always accept
                        true
                    }
                }
            };

            if should_apply {
                match bucket.apply_mutation(mutation) {
                    Ok(_) => accepted += 1,
                    Err(e) => {
                        warn!("Failed to apply XDCR mutation for key '{}': {}", key, e);
                        rejected += 1;
                    }
                }
            } else {
                rejected += 1;
            }
        }

        info!(
            "XDCR receive from '{}': {} accepted, {} rejected, {} conflicts",
            request.source_cluster, accepted, rejected, conflicts
        );

        Ok(XdcrBatchResponse {
            accepted,
            rejected,
            conflicts,
        })
    }

    /// Simple filter matching (key prefix or regex-like)
    fn matches_filter(&self, mutation: &Mutation, filter: &str) -> bool {
        // Simple key prefix filter for now
        if filter.starts_with("key:") {
            let prefix = &filter[4..];
            return mutation.key.starts_with(prefix);
        }

        // JSON field filter: field:value
        if filter.contains('=') {
            let parts: Vec<&str> = filter.splitn(2, '=').collect();
            if parts.len() == 2 {
                let field = parts[0];
                let expected = parts[1];
                if let Some(val) = mutation.value.get(field) {
                    return val.to_string().trim_matches('"') == expected;
                }
                return false;
            }
        }

        // Default: match all
        true
    }
}
