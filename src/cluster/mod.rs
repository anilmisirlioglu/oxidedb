pub mod chronicle;
pub mod durability;
pub mod failover;
pub mod node;
pub mod orchestrator;
pub mod partition;

use crate::cluster::chronicle::Chronicle;
use crate::cluster::failover::{FailoverConfig, FailoverEvent, FailoverManager, FailoverState, FailoverType};
use crate::cluster::node::{ClusterNode, NodeStatus};
use crate::cluster::orchestrator::{OrchestratorRole, OrchestratorState};
#[allow(unused_imports)]
use crate::cluster::partition::{PartitionMap, VBucketTransfer, RebalanceStatus, NodePartitionInfo, VBucketData};
use crate::config::ServerConfig;
use crate::error::{NosqlError, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

/// Cluster information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub name: String,
    pub uuid: String,
    pub nodes: Vec<ClusterNode>,
    pub bucket_count: usize,
    pub partition_map_revision: u64,
    pub total_vbuckets: u16,
    pub orchestrator: OrchestratorState,
}

/// Manages the cluster state, including partition map, failover, orchestrator election,
/// and Chronicle metadata consensus.
pub struct ClusterManager {
    pub cluster_name: String,
    pub cluster_uuid: String,
    pub nodes: RwLock<HashMap<String, ClusterNode>>,
    pub self_node: ClusterNode,
    pub partition_map: RwLock<PartitionMap>,
    pub rebalance_status: RwLock<RebalanceStatus>,
    pub pending_transfers: RwLock<Vec<VBucketTransfer>>,
    pub num_vbuckets: u16,
    pub failover_manager: RwLock<FailoverManager>,
    /// Current orchestrator state (Couchbase-style deterministic leader)
    pub orchestrator_state: RwLock<OrchestratorState>,
    /// Chronicle: replicated metadata consensus log (Couchbase-style)
    pub chronicle: Arc<Chronicle>,
    /// Server groups for rack/zone awareness
    pub server_groups: RwLock<HashMap<String, node::ServerGroup>>,
    /// Whether intra-cluster encryption is enabled
    pub cluster_encryption: bool,
}

impl ClusterManager {
    pub fn new(config: &ServerConfig) -> Self {
        let self_node = ClusterNode::new_self(
            config.node_name.clone(),
            config.host.clone(),
            config.port,
        );

        let cluster_uuid = uuid::Uuid::new_v4().to_string();
        let mut nodes = HashMap::new();
        nodes.insert(config.node_name.clone(), self_node.clone());

        // Initialize partition map with self as the only node
        let node_names = vec![config.node_name.clone()];
        let partition_map = PartitionMap::new(config.num_vbuckets, 1, &node_names);

        let rebalance_status = RebalanceStatus {
            in_progress: false,
            progress_percent: 0.0,
            transfers_total: 0,
            transfers_completed: 0,
            source_map_revision: 0,
            target_map_revision: 0,
        };

        let failover_config = FailoverConfig::default();

        // Initialize orchestrator state — single node starts as orchestrator
        let orchestrator_state = OrchestratorState {
            orchestrator_node: config.node_name.clone(),
            role: OrchestratorRole::Orchestrator,
            orchestrator_url: Some(format!("http://{}:{}", config.host, config.port)),
            participating_nodes: 1,
            revision: 1,
        };

        // Initialize Chronicle for metadata consensus
        let chronicle = Arc::new(Chronicle::new(config.node_name.clone()));

        // Initialize default server group
        let default_group = node::ServerGroup {
            name: "Group 1".to_string(),
            uuid: uuid::Uuid::new_v4().to_string(),
            nodes: vec![config.node_name.clone()],
        };
        let mut server_groups = HashMap::new();
        server_groups.insert("Group 1".to_string(), default_group);

        Self {
            cluster_name: format!("oxidedb-cluster"),
            cluster_uuid,
            nodes: RwLock::new(nodes),
            self_node,
            partition_map: RwLock::new(partition_map),
            rebalance_status: RwLock::new(rebalance_status),
            pending_transfers: RwLock::new(Vec::new()),
            num_vbuckets: config.num_vbuckets,
            failover_manager: RwLock::new(FailoverManager::new(failover_config)),
            orchestrator_state: RwLock::new(orchestrator_state),
            chronicle,
            server_groups: RwLock::new(server_groups),
            cluster_encryption: config.tls_enabled,
        }
    }

    /// Add a node to the cluster and trigger rebalance
    pub async fn add_node(&self, node: ClusterNode) -> Result<Vec<VBucketTransfer>> {
        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&node.name) {
            return Err(NosqlError::NodeAlreadyExists(node.name));
        }
        info!("Node '{}' joined the cluster", node.name);
        let node_name = node.name.clone();
        nodes.insert(node.name.clone(), node);

        // Update Chronicle cluster size
        let healthy_count = nodes.values()
            .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
            .count();
        self.chronicle.update_cluster_size(healthy_count);

        // Record in Chronicle
        let _ = self.chronicle.propose(chronicle::ConfigProposal {
            change_type: chronicle::ConfigChangeType::NodeAdd,
            payload: serde_json::json!({"node": node_name}),
            proposed_by: self.self_node.name.clone(),
        });

        // Trigger automatic rebalance
        let node_names: Vec<String> = nodes
            .values()
            .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
            .map(|n| n.name.clone())
            .collect();
        drop(nodes); // Release lock before rebalance

        let transfers = self.do_rebalance(node_names).await;

        // Re-evaluate orchestrator after membership change
        self.evaluate_orchestrator().await;

        Ok(transfers)
    }

    /// Remove a node from the cluster and trigger rebalance
    pub async fn remove_node(&self, name: &str) -> Result<Vec<VBucketTransfer>> {
        if name == self.self_node.name {
            return Err(NosqlError::InvalidRequest(
                "Cannot remove self node".to_string(),
            ));
        }
        let mut nodes = self.nodes.write().await;
        nodes
            .remove(name)
            .ok_or_else(|| NosqlError::NodeNotFound(name.to_string()))?;
        info!("Node '{}' removed from the cluster", name);

        // Clean up failover state
        let mut fm = self.failover_manager.write().await;
        fm.recover_node(name);
        fm.clear_failure_timer(name);
        drop(fm);

        // Update Chronicle cluster size
        let healthy_count = nodes.values()
            .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
            .count();
        self.chronicle.update_cluster_size(healthy_count);

        // Record in Chronicle
        let _ = self.chronicle.propose(chronicle::ConfigProposal {
            change_type: chronicle::ConfigChangeType::NodeRemove,
            payload: serde_json::json!({"node": name}),
            proposed_by: self.self_node.name.clone(),
        });

        // Trigger rebalance with remaining nodes
        let node_names: Vec<String> = nodes
            .values()
            .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
            .map(|n| n.name.clone())
            .collect();
        drop(nodes);

        let transfers = self.do_rebalance(node_names).await;

        // Re-evaluate orchestrator after membership change
        self.evaluate_orchestrator().await;

        Ok(transfers)
    }

    /// Perform the rebalance operation — uses group-aware placement if groups exist
    async fn do_rebalance(&self, node_names: Vec<String>) -> Vec<VBucketTransfer> {
        let node_groups = self.get_node_group_map().await;
        let has_multiple_groups = {
            let mut groups: std::collections::HashSet<&String> = std::collections::HashSet::new();
            for g in node_groups.values() {
                groups.insert(g);
            }
            groups.len() > 1
        };

        let mut pmap = self.partition_map.write().await;
        let old_rev = pmap.revision;
        let transfers = if has_multiple_groups {
            pmap.rebalance_with_groups(&node_names, &node_groups)
        } else {
            pmap.rebalance(&node_names)
        };
        let new_rev = pmap.revision;

        // Update rebalance status
        let mut status = self.rebalance_status.write().await;
        if !transfers.is_empty() {
            status.in_progress = true;
            status.transfers_total = transfers.len();
            status.transfers_completed = 0;
            status.progress_percent = 0.0;
            status.source_map_revision = old_rev;
            status.target_map_revision = new_rev;

            // Store pending transfers
            let mut pending = self.pending_transfers.write().await;
            *pending = transfers.clone();

            info!(
                "Rebalance initiated: {} vBucket transfers, rev {} → {}",
                transfers.len(),
                old_rev,
                new_rev
            );

            // Record partition map update in Chronicle
            let _ = self.chronicle.propose(chronicle::ConfigProposal {
                change_type: chronicle::ConfigChangeType::PartitionMapUpdate,
                payload: serde_json::json!({
                    "old_revision": old_rev,
                    "new_revision": new_rev,
                    "transfers": transfers.len(),
                    "nodes": node_names,
                }),
                proposed_by: self.self_node.name.clone(),
            });
        } else {
            status.in_progress = false;
            status.progress_percent = 100.0;
        }

        transfers
    }

    /// Promote replicas for vBuckets owned by a failed node.
    /// Returns (vbuckets_affected, replicas_promoted).
    async fn promote_replicas_for_node(&self, failed_node: &str) -> (usize, usize) {
        let mut pmap = self.partition_map.write().await;
        let mut vbuckets_affected = 0;
        let mut replicas_promoted = 0;

        for entry in pmap.map.iter_mut() {
            if entry.active_node == failed_node {
                vbuckets_affected += 1;
                // Promote the first replica to active
                if let Some(replica) = entry.replica_nodes.first().cloned() {
                    entry.active_node = replica.clone();
                    entry.replica_nodes.retain(|r| r != &replica);
                    replicas_promoted += 1;
                }
                // Remove failed node from replicas too
                entry.replica_nodes.retain(|r| r != failed_node);
            } else {
                // Remove failed node from replica lists
                entry.replica_nodes.retain(|r| r != failed_node);
            }
        }

        if replicas_promoted > 0 {
            pmap.revision += 1;
            info!(
                "Replica promotion complete for failed node '{}': {} vBuckets affected, {} replicas promoted (rev {})",
                failed_node, vbuckets_affected, replicas_promoted, pmap.revision
            );
        }

        (vbuckets_affected, replicas_promoted)
    }

    /// Execute failover for a specific node
    async fn execute_failover(
        &self,
        node_name: &str,
        failover_type: FailoverType,
        reason: String,
    ) -> FailoverEvent {
        // Mark node as FailedOver
        {
            let mut nodes = self.nodes.write().await;
            if let Some(node) = nodes.get_mut(node_name) {
                node.status = NodeStatus::FailedOver;
            }
        }

        // Step 1: Promote replicas (immediate — no data loss if replicas exist)
        let (vbuckets_affected, replicas_promoted) =
            self.promote_replicas_for_node(node_name).await;

        // Step 2: Rebalance to redistribute among healthy nodes
        let healthy_nodes: Vec<String> = {
            let nodes = self.nodes.read().await;
            nodes
                .values()
                .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
                .map(|n| n.name.clone())
                .collect()
        };

        let rebalance_triggered = !healthy_nodes.is_empty();
        if rebalance_triggered {
            self.do_rebalance(healthy_nodes).await;
        }

        // Step 3: Record the event
        let mut fm = self.failover_manager.write().await;
        let event = fm.record_failover(
            node_name,
            failover_type,
            reason,
            vbuckets_affected,
            replicas_promoted,
            rebalance_triggered,
        );

        info!(
            "Failover complete: node='{}', type={:?}, vBuckets={}, promoted={}, rebalanced={}",
            node_name, failover_type, vbuckets_affected, replicas_promoted, rebalance_triggered
        );

        // Record failover in Chronicle
        let _ = self.chronicle.propose(chronicle::ConfigProposal {
            change_type: chronicle::ConfigChangeType::NodeFailover,
            payload: serde_json::json!({
                "node": node_name,
                "failover_type": format!("{:?}", failover_type),
                "vbuckets_affected": vbuckets_affected,
                "replicas_promoted": replicas_promoted,
            }),
            proposed_by: self.self_node.name.clone(),
        });

        // Re-evaluate orchestrator after failover
        self.evaluate_orchestrator().await;

        event
    }

    /// Manual graceful failover — operator triggers this
    pub async fn graceful_failover(&self, node_name: &str) -> Result<FailoverEvent> {
        // Validate node exists and is not self
        if node_name == self.self_node.name {
            return Err(NosqlError::InvalidRequest(
                "Cannot failover self node".to_string(),
            ));
        }

        {
            let nodes = self.nodes.read().await;
            if !nodes.contains_key(node_name) {
                return Err(NosqlError::NodeNotFound(node_name.to_string()));
            }
        }

        // Check if already failed over
        {
            let fm = self.failover_manager.read().await;
            if fm.is_failed_over(node_name) {
                return Err(NosqlError::InvalidRequest(format!(
                    "Node '{}' is already in failover state",
                    node_name
                )));
            }
        }

        let event = self
            .execute_failover(
                node_name,
                FailoverType::Graceful,
                format!("Graceful failover initiated by operator"),
            )
            .await;

        Ok(event)
    }

    /// Manual hard failover — immediate, no waiting
    pub async fn hard_failover(&self, node_name: &str) -> Result<FailoverEvent> {
        if node_name == self.self_node.name {
            return Err(NosqlError::InvalidRequest(
                "Cannot failover self node".to_string(),
            ));
        }

        {
            let nodes = self.nodes.read().await;
            if !nodes.contains_key(node_name) {
                return Err(NosqlError::NodeNotFound(node_name.to_string()));
            }
        }

        let event = self
            .execute_failover(
                node_name,
                FailoverType::Hard,
                format!("Hard failover initiated by operator"),
            )
            .await;

        Ok(event)
    }

    /// Recover a failed-over node — add it back and trigger rebalance
    pub async fn recover_node(&self, node_name: &str) -> Result<Vec<VBucketTransfer>> {
        // Check node exists and is in FailedOver state
        {
            let nodes = self.nodes.read().await;
            let node = nodes
                .get(node_name)
                .ok_or_else(|| NosqlError::NodeNotFound(node_name.to_string()))?;
            if node.status != NodeStatus::FailedOver && node.status != NodeStatus::Failed {
                return Err(NosqlError::InvalidRequest(format!(
                    "Node '{}' is not in failed/failedover state (current: {:?})",
                    node_name, node.status
                )));
            }
        }

        // Mark as healthy
        {
            let mut nodes = self.nodes.write().await;
            if let Some(node) = nodes.get_mut(node_name) {
                node.status = NodeStatus::Healthy;
                node.last_heartbeat = Utc::now();
            }
        }

        // Clear failover state
        {
            let mut fm = self.failover_manager.write().await;
            fm.recover_node(node_name);
        }

        // Trigger rebalance to include the recovered node
        let node_names: Vec<String> = {
            let nodes = self.nodes.read().await;
            nodes
                .values()
                .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
                .map(|n| n.name.clone())
                .collect()
        };

        let transfers = self.do_rebalance(node_names).await;
        info!(
            "Node '{}' recovered and rebalance triggered ({} transfers)",
            node_name,
            transfers.len()
        );

        // Re-evaluate orchestrator after recovery
        self.evaluate_orchestrator().await;

        Ok(transfers)
    }

    /// Manual rebalance trigger
    pub async fn trigger_rebalance(&self) -> Vec<VBucketTransfer> {
        let nodes = self.nodes.read().await;
        let node_names: Vec<String> = nodes
            .values()
            .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
            .map(|n| n.name.clone())
            .collect();
        drop(nodes);
        self.do_rebalance(node_names).await
    }

    /// Mark a vBucket transfer as completed
    pub async fn complete_transfer(&self, vbucket_id: u16) {
        let mut pending = self.pending_transfers.write().await;
        if let Some(t) = pending.iter_mut().find(|t| t.vbucket_id == vbucket_id) {
            t.status = partition::TransferStatus::Completed;
        }
        let completed = pending
            .iter()
            .filter(|t| t.status == partition::TransferStatus::Completed)
            .count();
        let total = pending.len();

        let mut status = self.rebalance_status.write().await;
        status.transfers_completed = completed;
        if total > 0 {
            status.progress_percent = (completed as f32 / total as f32) * 100.0;
        }
        if completed >= total {
            status.in_progress = false;
            status.progress_percent = 100.0;
            info!("Rebalance complete: all {} transfers finished", total);
        }
    }

    /// Get the active node for a vBucket
    pub async fn get_active_node_for_vbucket(&self, vbucket_id: u16) -> Option<String> {
        let pmap = self.partition_map.read().await;
        pmap.get_active_node(vbucket_id).map(|s| s.to_string())
    }

    /// Check if a vBucket is active on this node
    pub async fn is_vbucket_local(&self, vbucket_id: u16) -> bool {
        let pmap = self.partition_map.read().await;
        pmap.is_active_on(vbucket_id, &self.self_node.name)
    }

    /// Check if this is a single-node cluster (no forwarding needed)
    pub async fn is_single_node(&self) -> bool {
        let nodes = self.nodes.read().await;
        nodes.len() <= 1
    }

    /// Execute pending vBucket transfers by actually moving data between nodes.
    /// This is called periodically by a background task after rebalance.
    pub async fn execute_pending_transfers(&self, storage: &crate::storage::engine::StorageEngine) {
        // Only the orchestrator coordinates all transfers
        if !self.is_orchestrator().await {
            return;
        }

        // Snapshot pending transfers
        let transfers: Vec<VBucketTransfer> = {
            let pending = self.pending_transfers.read().await;
            pending
                .iter()
                .filter(|t| t.status == partition::TransferStatus::Pending)
                .cloned()
                .collect()
        };

        if transfers.is_empty() {
            return;
        }

        let self_name = &self.self_node.name;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        // Pre-check which nodes are reachable
        let node_urls: std::collections::HashMap<String, String> = {
            let nodes = self.nodes.read().await;
            nodes.values().map(|n| (n.name.clone(), n.base_url())).collect()
        };

        // Check node health — skip transfers involving unreachable nodes
        let unhealthy_nodes: std::collections::HashSet<String> = {
            let nodes = self.nodes.read().await;
            nodes
                .values()
                .filter(|n| {
                    n.status == NodeStatus::Failed
                        || n.status == NodeStatus::FailedOver
                        || n.status == NodeStatus::Unhealthy
                })
                .map(|n| n.name.clone())
                .collect()
        };

        // Process up to 32 transfers per tick to avoid blocking too long
        let batch_size = 32.min(transfers.len());

        for transfer in transfers.iter().take(batch_size) {
            // Skip transfers involving unhealthy nodes — mark as failed
            if unhealthy_nodes.contains(&transfer.from_node)
                || unhealthy_nodes.contains(&transfer.to_node)
            {
                warn!(
                    "vBucket {} transfer skipped: node unhealthy ({} → {})",
                    transfer.vbucket_id, transfer.from_node, transfer.to_node
                );
                self.fail_transfer(transfer.vbucket_id).await;
                continue;
            }

            let target_url = match node_urls.get(&transfer.to_node) {
                Some(url) => url.clone(),
                None => {
                    warn!(
                        "Transfer vBucket {}: target '{}' not found, failing",
                        transfer.vbucket_id, transfer.to_node
                    );
                    self.fail_transfer(transfer.vbucket_id).await;
                    continue;
                }
            };

            let bucket_names: Vec<String> = storage
                .buckets
                .iter()
                .map(|e| e.key().clone())
                .collect();

            let mut transfer_ok = true;

            if transfer.from_node == *self_name {
                // ── LOCAL export → remote import ──
                for bucket_name in &bucket_names {
                    match storage.export_vbucket(bucket_name, transfer.vbucket_id) {
                        Ok(data) if !data.documents.is_empty() => {
                            let doc_count = data.documents.len();
                            let send_url = format!(
                                "{}/api/v1/cluster/vbuckets/transfer",
                                target_url
                            );
                            let payload = serde_json::json!({
                                "bucket": bucket_name,
                                "data": data,
                            });

                            match client.post(&send_url).json(&payload).send().await {
                                Ok(resp) if resp.status().is_success() => {
                                    debug!(
                                        "vBucket {} transferred: {} docs '{}' ({} → {})",
                                        transfer.vbucket_id, doc_count, bucket_name,
                                        transfer.from_node, transfer.to_node
                                    );
                                }
                                Ok(resp) => {
                                    warn!(
                                        "vBucket {} transfer failed (HTTP {}): '{}' → {}",
                                        transfer.vbucket_id, resp.status(), bucket_name, transfer.to_node
                                    );
                                    transfer_ok = false;
                                }
                                Err(e) => {
                                    warn!(
                                        "vBucket {} transfer error: '{}' → {}: {}",
                                        transfer.vbucket_id, bucket_name, transfer.to_node, e
                                    );
                                    transfer_ok = false;
                                }
                            }
                        }
                        _ => {} // No data — nothing to move
                    }
                }
            } else {
                // ── REMOTE export (pull from source) → remote import (push to target) ──
                let source_url = match node_urls.get(&transfer.from_node) {
                    Some(url) => url.clone(),
                    None => {
                        self.fail_transfer(transfer.vbucket_id).await;
                        continue;
                    }
                };

                for bucket_name in &bucket_names {
                    // Pull vBucket data from source node
                    let export_url = format!(
                        "{}/api/v1/cluster/vbuckets/{}/{}",
                        source_url, bucket_name, transfer.vbucket_id
                    );

                    let export_resp = match client.get(&export_url).send().await {
                        Ok(resp) if resp.status().is_success() => resp,
                        Ok(resp) if resp.status().as_u16() == 404 => {
                            continue; // No data for this bucket/vbucket on source — fine
                        }
                        Ok(resp) => {
                            warn!(
                                "vBucket {} export failed (HTTP {}): {} from {}",
                                transfer.vbucket_id, resp.status(), bucket_name, transfer.from_node
                            );
                            transfer_ok = false;
                            continue;
                        }
                        Err(e) => {
                            warn!(
                                "vBucket {} export error: {} from {}: {}",
                                transfer.vbucket_id, bucket_name, transfer.from_node, e
                            );
                            transfer_ok = false;
                            continue;
                        }
                    };

                    // Parse exported data
                    let data: VBucketData = match export_resp.json().await {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(
                                "vBucket {} export parse error: {}: {}",
                                transfer.vbucket_id, bucket_name, e
                            );
                            transfer_ok = false;
                            continue;
                        }
                    };

                    if data.documents.is_empty() {
                        continue; // No data to move
                    }

                    // Push to target node
                    let send_url = format!(
                        "{}/api/v1/cluster/vbuckets/transfer",
                        target_url
                    );
                    let payload = serde_json::json!({
                        "bucket": bucket_name,
                        "data": data,
                    });

                    match client.post(&send_url).json(&payload).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            debug!(
                                "vBucket {} relayed: '{}' ({} → {})",
                                transfer.vbucket_id, bucket_name,
                                transfer.from_node, transfer.to_node
                            );
                        }
                        Ok(resp) => {
                            warn!(
                                "vBucket {} relay failed (HTTP {}): '{}' → {}",
                                transfer.vbucket_id, resp.status(), bucket_name, transfer.to_node
                            );
                            transfer_ok = false;
                        }
                        Err(e) => {
                            warn!(
                                "vBucket {} relay error: '{}' → {}: {}",
                                transfer.vbucket_id, bucket_name, transfer.to_node, e
                            );
                            transfer_ok = false;
                        }
                    }
                }
            }

            if transfer_ok {
                self.complete_transfer(transfer.vbucket_id).await;
            } else {
                self.fail_transfer(transfer.vbucket_id).await;
            }
        }
    }

    /// Mark a vBucket transfer as failed
    pub async fn fail_transfer(&self, vbucket_id: u16) {
        let mut pending = self.pending_transfers.write().await;
        if let Some(t) = pending.iter_mut().find(|t| t.vbucket_id == vbucket_id && t.status == partition::TransferStatus::Pending) {
            t.status = partition::TransferStatus::Failed;
        }
        let completed = pending
            .iter()
            .filter(|t| t.status == partition::TransferStatus::Completed || t.status == partition::TransferStatus::Failed)
            .count();
        let total = pending.len();

        let mut status = self.rebalance_status.write().await;
        status.transfers_completed = completed;
        if total > 0 {
            status.progress_percent = (completed as f32 / total as f32) * 100.0;
        }
        if completed >= total {
            status.in_progress = false;
            status.progress_percent = 100.0;
            let failed_count = pending.iter().filter(|t| t.status == partition::TransferStatus::Failed).count();
            if failed_count > 0 {
                warn!("Rebalance finished: {} completed, {} failed out of {}", completed - failed_count, failed_count, total);
            } else {
                info!("Rebalance complete: all {} transfers finished", total);
            }
        }
    }

    /// Get the base URL for a node
    pub async fn get_node_url(&self, node_name: &str) -> Option<String> {
        let nodes = self.nodes.read().await;
        nodes.get(node_name).map(|n| n.base_url())
    }

    /// Get partition map snapshot
    pub async fn get_partition_map(&self) -> PartitionMap {
        let pmap = self.partition_map.read().await;
        pmap.clone()
    }

    /// Get per-node partition info
    pub async fn get_node_partition_info(&self) -> Vec<NodePartitionInfo> {
        let nodes = self.nodes.read().await;
        let node_names: Vec<String> = nodes.keys().cloned().collect();
        drop(nodes);

        let pmap = self.partition_map.read().await;
        pmap.get_node_partition_info(&node_names)
    }

    /// Get rebalance status
    pub async fn get_rebalance_status(&self) -> RebalanceStatus {
        let status = self.rebalance_status.read().await;
        status.clone()
    }

    /// Get pending transfers
    pub async fn get_pending_transfers(&self) -> Vec<VBucketTransfer> {
        let pending = self.pending_transfers.read().await;
        pending.clone()
    }

    /// List all nodes
    pub async fn list_nodes(&self) -> Vec<ClusterNode> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Update heartbeat for a node
    pub async fn heartbeat(&self, node_name: &str) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let node = nodes
            .get_mut(node_name)
            .ok_or_else(|| NosqlError::NodeNotFound(node_name.to_string()))?;
        node.last_heartbeat = Utc::now();
        if node.status == NodeStatus::Unhealthy {
            node.status = NodeStatus::Healthy;
        }
        drop(nodes);

        // Clear any pending failure timer
        let mut fm = self.failover_manager.write().await;
        fm.clear_failure_timer(node_name);

        Ok(())
    }

    /// Get cluster info
    pub async fn get_cluster_info(&self, bucket_count: usize) -> ClusterInfo {
        let nodes = self.nodes.read().await;
        let pmap = self.partition_map.read().await;
        let orch = self.orchestrator_state.read().await;
        ClusterInfo {
            name: self.cluster_name.clone(),
            uuid: self.cluster_uuid.clone(),
            nodes: nodes.values().cloned().collect(),
            bucket_count,
            partition_map_revision: pmap.revision,
            total_vbuckets: pmap.num_vbuckets,
            orchestrator: orch.clone(),
        }
    }

    /// Get failover state
    pub async fn get_failover_state(&self) -> FailoverState {
        let fm = self.failover_manager.read().await;
        fm.get_state()
    }

    /// Update failover configuration
    pub async fn update_failover_config(&self, config: FailoverConfig) {
        let mut fm = self.failover_manager.write().await;
        fm.update_config(config);
    }

    /// Reset auto-failover quota counter
    pub async fn reset_failover_quota(&self) {
        let mut fm = self.failover_manager.write().await;
        fm.reset_quota();
    }

    // =========================================================================
    // Orchestrator Election (Couchbase-style)
    // =========================================================================

    /// Re-evaluate orchestrator election based on current healthy nodes.
    /// Called after any node membership change (add, remove, failover, recover).
    /// Also syncs Chronicle leadership state with orchestrator.
    pub async fn evaluate_orchestrator(&self) {
        let nodes = self.nodes.read().await;
        let healthy_nodes: Vec<(String, String)> = nodes
            .values()
            .filter(|n| n.status == NodeStatus::Healthy || n.status == NodeStatus::Warmup)
            .map(|n| (n.name.clone(), n.base_url()))
            .collect();
        drop(nodes);

        if let Some((leader_name, leader_url)) = orchestrator::elect_orchestrator(&healthy_nodes) {
            let mut state = self.orchestrator_state.write().await;
            let old_leader = state.orchestrator_node.clone();

            state.orchestrator_node = leader_name.clone();
            state.orchestrator_url = Some(leader_url);
            state.participating_nodes = healthy_nodes.len();

            let am_i_leader = leader_name == self.self_node.name;
            state.role = if am_i_leader {
                OrchestratorRole::Orchestrator
            } else {
                OrchestratorRole::Follower
            };

            // Sync Chronicle leadership with orchestrator state
            self.chronicle.set_leader(am_i_leader);
            self.chronicle.update_cluster_size(healthy_nodes.len());

            if old_leader != leader_name {
                state.revision += 1;
                self.chronicle.new_term(); // Increment Chronicle term on leader change
                info!(
                    "Orchestrator changed: {} → {} (rev {})",
                    old_leader, leader_name, state.revision
                );
            }
        }
    }

    /// Check if this node is the orchestrator
    pub async fn is_orchestrator(&self) -> bool {
        let state = self.orchestrator_state.read().await;
        state.role == OrchestratorRole::Orchestrator
    }

    /// Get current orchestrator state
    pub async fn get_orchestrator_state(&self) -> OrchestratorState {
        self.orchestrator_state.read().await.clone()
    }

    /// Get the Chronicle engine (for API routes and background tasks)
    pub fn get_chronicle(&self) -> Arc<Chronicle> {
        self.chronicle.clone()
    }

    /// Actively send heartbeat pings to all remote nodes.
    /// For each node that responds, update its `last_heartbeat` timestamp.
    /// This ensures that the health check has fresh data to work with.
    pub async fn send_heartbeats(&self) {
        let remote_nodes: Vec<(String, String)> = {
            let nodes = self.nodes.read().await;
            nodes
                .values()
                .filter(|n| n.name != self.self_node.name && n.status != NodeStatus::FailedOver)
                .map(|n| (n.name.clone(), n.base_url()))
                .collect()
        };

        if remote_nodes.is_empty() {
            return;
        }

        let self_name = self.self_node.name.clone();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap_or_default();

        // Ping all remote nodes in parallel
        let mut handles = Vec::new();
        for (node_name, base_url) in &remote_nodes {
            let client = client.clone();
            let url = format!("{}/health", base_url);
            let hb_url = format!(
                "{}/api/v1/cluster/nodes/{}/heartbeat",
                base_url, self_name
            );
            let name = node_name.clone();
            handles.push(tokio::spawn(async move {
                // First, check if the remote node is alive
                let alive = client.get(&url).send().await.is_ok();
                if alive {
                    // Also register ourselves on the remote node so it knows about us
                    let _ = client.post(&hb_url).send().await;
                }
                (name, alive)
            }));
        }

        // Collect results and update heartbeats
        for handle in handles {
            if let Ok((node_name, alive)) = handle.await {
                if alive {
                    let mut nodes = self.nodes.write().await;
                    if let Some(node) = nodes.get_mut(&node_name) {
                        node.last_heartbeat = Utc::now();
                        if node.status == NodeStatus::Unhealthy {
                            debug!("Node '{}' is reachable again, marking Healthy", node_name);
                            node.status = NodeStatus::Healthy;
                        }
                    }
                } else {
                    warn!("Heartbeat ping to '{}' failed", node_name);
                }
            }
        }
    }

    /// Check for failed nodes — the heart of automatic failover.
    /// Called periodically by the background health check task.
    pub async fn check_node_health(&self) -> Vec<FailoverEvent> {
        let mut failover_candidates: Vec<String> = Vec::new();
        let mut recovered_nodes: Vec<String> = Vec::new();

        // Phase 1: Update node statuses based on heartbeat
        {
            let mut nodes = self.nodes.write().await;
            let now = Utc::now();
            for node in nodes.values_mut() {
                if node.name == self.self_node.name {
                    node.last_heartbeat = now;
                    continue;
                }
                // Skip already failed-over nodes
                if node.status == NodeStatus::FailedOver {
                    continue;
                }
                let elapsed = (now - node.last_heartbeat).num_seconds();
                if elapsed > 30 {
                    if node.status == NodeStatus::Healthy || node.status == NodeStatus::Warmup {
                        node.status = NodeStatus::Unhealthy;
                    }
                    failover_candidates.push(node.name.clone());
                } else if elapsed > 10 {
                    if node.status == NodeStatus::Healthy {
                        node.status = NodeStatus::Unhealthy;
                    }
                    // Still unhealthy but recovering — start timer
                    failover_candidates.push(node.name.clone());
                } else {
                    // Node is responsive
                    if node.status == NodeStatus::Unhealthy {
                        node.status = NodeStatus::Healthy;
                        recovered_nodes.push(node.name.clone());
                    }
                }
            }
        }

        // Phase 2: Update failure timers and check for recoveries
        {
            let mut fm = self.failover_manager.write().await;
            for node_name in &recovered_nodes {
                fm.clear_failure_timer(node_name);
            }
            for node_name in &failover_candidates {
                fm.start_failure_timer(node_name);
            }
        }

        // Phase 3: Check which nodes have exceeded the failover timeout
        let mut nodes_to_failover: Vec<String> = Vec::new();
        {
            let fm = self.failover_manager.read().await;
            let nodes = self.nodes.read().await;
            let healthy_count = nodes
                .values()
                .filter(|n| n.status == NodeStatus::Healthy)
                .count();

            for node_name in &failover_candidates {
                if fm.is_failure_timeout_reached(node_name) {
                    let (can, reason) = fm.can_auto_failover(node_name, healthy_count);
                    if can {
                        nodes_to_failover.push(node_name.clone());
                    } else {
                        let elapsed = fm.failure_timer_elapsed(node_name).unwrap_or(0);
                        info!(
                            "Auto-failover blocked for '{}' ({}s elapsed): {}",
                            node_name, elapsed, reason
                        );
                    }
                }
            }
        }

        // Phase 4: Execute auto-failovers
        let mut events = Vec::new();
        for node_name in nodes_to_failover {
            let elapsed = {
                let fm = self.failover_manager.read().await;
                fm.failure_timer_elapsed(&node_name).unwrap_or(0)
            };

            info!(
                "AUTO-FAILOVER: Node '{}' failed (no heartbeat for {}s, timeout={}s)",
                node_name,
                elapsed,
                {
                    let fm = self.failover_manager.read().await;
                    fm.config.timeout_secs
                }
            );

            let event = self
                .execute_failover(
                    &node_name,
                    FailoverType::Automatic,
                    format!(
                        "No heartbeat for {}s (timeout: {}s)",
                        elapsed,
                        {
                            let fm = self.failover_manager.read().await;
                            fm.config.timeout_secs
                        }
                    ),
                )
                .await;

            events.push(event);
        }

        // Re-evaluate orchestrator after any health changes
        if !failover_candidates.is_empty() || !recovered_nodes.is_empty() {
            self.evaluate_orchestrator().await;
        }

        events
    }

    // =================================================================
    // Server Group Management (Rack/Zone Awareness)
    // =================================================================

    /// Create a new server group
    pub async fn create_server_group(&self, name: &str) -> Result<node::ServerGroup> {
        let mut groups = self.server_groups.write().await;
        if groups.contains_key(name) {
            return Err(NosqlError::InvalidRequest(format!(
                "Server group '{}' already exists", name
            )));
        }
        let group = node::ServerGroup {
            name: name.to_string(),
            uuid: uuid::Uuid::new_v4().to_string(),
            nodes: Vec::new(),
        };
        groups.insert(name.to_string(), group.clone());
        info!("Server group '{}' created", name);

        // Record in Chronicle
        let _ = self.chronicle.propose(chronicle::ConfigProposal {
            change_type: chronicle::ConfigChangeType::ServerGroupAdd,
            payload: serde_json::json!({"group": name}),
            proposed_by: self.self_node.name.clone(),
        });

        Ok(group)
    }

    /// Delete a server group (must be empty)
    pub async fn delete_server_group(&self, name: &str) -> Result<()> {
        let mut groups = self.server_groups.write().await;
        if let Some(group) = groups.get(name) {
            if !group.nodes.is_empty() {
                return Err(NosqlError::InvalidRequest(format!(
                    "Server group '{}' is not empty ({} nodes)", name, group.nodes.len()
                )));
            }
            groups.remove(name);
            info!("Server group '{}' deleted", name);
            Ok(())
        } else {
            Err(NosqlError::InvalidRequest(format!(
                "Server group '{}' not found", name
            )))
        }
    }

    /// Move a node to a different server group
    pub async fn move_node_to_group(&self, node_name: &str, group_name: &str) -> Result<()> {
        let mut groups = self.server_groups.write().await;

        // Verify target group exists
        if !groups.contains_key(group_name) {
            return Err(NosqlError::InvalidRequest(format!(
                "Server group '{}' not found", group_name
            )));
        }

        // Verify node exists
        {
            let nodes = self.nodes.read().await;
            if !nodes.contains_key(node_name) {
                return Err(NosqlError::NodeNotFound(node_name.to_string()));
            }
        }

        // Remove from old group
        for group in groups.values_mut() {
            group.nodes.retain(|n| n != node_name);
        }

        // Add to new group
        if let Some(group) = groups.get_mut(group_name) {
            group.nodes.push(node_name.to_string());
        }

        // Update node's server_group field
        {
            let mut nodes = self.nodes.write().await;
            if let Some(node) = nodes.get_mut(node_name) {
                node.server_group = group_name.to_string();
            }
        }

        info!("Node '{}' moved to server group '{}'", node_name, group_name);
        Ok(())
    }

    /// List all server groups
    pub async fn list_server_groups(&self) -> Vec<node::ServerGroup> {
        let groups = self.server_groups.read().await;
        groups.values().cloned().collect()
    }

    /// Get node-to-group mapping for group-aware rebalance
    pub async fn get_node_group_map(&self) -> HashMap<String, String> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .map(|n| (n.name.clone(), n.server_group.clone()))
            .collect()
    }

    /// Rebalance with server group awareness
    pub async fn rebalance_with_groups(&self) -> Vec<VBucketTransfer> {
        let nodes = self.nodes.read().await;
        let node_names: Vec<String> = nodes
            .values()
            .filter(|n| n.status != NodeStatus::Failed && n.status != NodeStatus::FailedOver)
            .map(|n| n.name.clone())
            .collect();
        let node_groups: HashMap<String, String> = nodes
            .values()
            .map(|n| (n.name.clone(), n.server_group.clone()))
            .collect();
        drop(nodes);

        let mut pmap = self.partition_map.write().await;
        let old_rev = pmap.revision;
        let transfers = pmap.rebalance_with_groups(&node_names, &node_groups);
        let new_rev = pmap.revision;

        // Update rebalance status
        let mut status = self.rebalance_status.write().await;
        if !transfers.is_empty() {
            status.in_progress = true;
            status.transfers_total = transfers.len();
            status.transfers_completed = 0;
            status.progress_percent = 0.0;
            status.source_map_revision = old_rev;
            status.target_map_revision = new_rev;

            let mut pending = self.pending_transfers.write().await;
            *pending = transfers.clone();

            info!(
                "Group-aware rebalance: {} vBucket transfers, rev {} → {}",
                transfers.len(), old_rev, new_rev
            );
        } else {
            status.in_progress = false;
            status.progress_percent = 100.0;
        }

        transfers
    }
}
