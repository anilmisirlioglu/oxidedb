use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

/// Describes the ownership of a single vBucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VBucketOwnership {
    /// The node that owns this vBucket (active copy)
    pub active_node: String,
    /// Nodes holding replica copies
    pub replica_nodes: Vec<String>,
}

/// The partition map: maps every vBucket to its active and replica nodes.
/// This is the heart of multi-node data distribution, similar to
/// Couchbase's vBucket map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMap {
    /// Total number of vBuckets
    pub num_vbuckets: u16,
    /// Number of replicas per vBucket
    pub num_replicas: u8,
    /// Map revision (incremented on every rebalance)
    pub revision: u64,
    /// vBucket ID → ownership info
    pub map: Vec<VBucketOwnership>,
}

/// Summary of which vBuckets a particular node is responsible for
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePartitionInfo {
    pub node_name: String,
    pub active_vbuckets: Vec<u16>,
    pub replica_vbuckets: Vec<u16>,
    pub active_count: usize,
    pub replica_count: usize,
}

/// Rebalance progress tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceStatus {
    pub in_progress: bool,
    pub progress_percent: f32,
    pub transfers_total: usize,
    pub transfers_completed: usize,
    pub source_map_revision: u64,
    pub target_map_revision: u64,
}

impl PartitionMap {
    /// Create a new partition map, distributing vBuckets evenly across the given nodes.
    pub fn new(num_vbuckets: u16, num_replicas: u8, node_names: &[String]) -> Self {
        let mut map = Vec::with_capacity(num_vbuckets as usize);

        if node_names.is_empty() {
            // No nodes — every vBucket is unassigned (shouldn't happen in practice)
            for _ in 0..num_vbuckets {
                map.push(VBucketOwnership {
                    active_node: String::new(),
                    replica_nodes: Vec::new(),
                });
            }
        } else {
            let n = node_names.len();
            for vb_id in 0..num_vbuckets {
                // Round-robin active assignment
                let active_idx = vb_id as usize % n;
                let active_node = node_names[active_idx].clone();

                // Assign replicas to different nodes
                let mut replica_nodes = Vec::new();
                for r in 0..num_replicas as usize {
                    if n > 1 {
                        let replica_idx = (active_idx + 1 + r) % n;
                        if replica_idx != active_idx {
                            replica_nodes.push(node_names[replica_idx].clone());
                        }
                    }
                }
                map.push(VBucketOwnership {
                    active_node,
                    replica_nodes,
                });
            }
        }

        Self {
            num_vbuckets,
            num_replicas,
            revision: 1,
            map,
        }
    }

    /// Rebalance: redistribute vBuckets evenly across a new set of nodes.
    /// Returns a list of (vBucketId, old_owner, new_owner) transfers needed.
    pub fn rebalance(&mut self, node_names: &[String]) -> Vec<VBucketTransfer> {
        if node_names.is_empty() {
            return Vec::new();
        }

        let old_map = self.map.clone();
        let n = node_names.len();
        let mut transfers = Vec::new();

        // Calculate ideal distribution: each node should own roughly num_vbuckets / n
        let base_count = self.num_vbuckets as usize / n;
        let remainder = self.num_vbuckets as usize % n;

        // Build target counts: first `remainder` nodes get base_count+1, rest get base_count
        let mut target_counts: Vec<usize> = Vec::new();
        for i in 0..n {
            if i < remainder {
                target_counts.push(base_count + 1);
            } else {
                target_counts.push(base_count);
            }
        }

        // Current assignment counts per node
        let mut current_counts: HashMap<String, usize> = HashMap::new();
        for name in node_names {
            current_counts.insert(name.clone(), 0);
        }
        for entry in &self.map {
            if let Some(c) = current_counts.get_mut(&entry.active_node) {
                *c += 1;
            }
        }

        // Simple greedy rebalance: go through vBuckets and reassign from overloaded → underloaded
        // Build a new map with round-robin assignment
        let mut new_map = Vec::with_capacity(self.num_vbuckets as usize);
        let mut node_assigned: Vec<usize> = vec![0; n];

        // First pass: keep vBuckets that are already on valid nodes and within target
        let mut unassigned: Vec<u16> = Vec::new();
        let mut node_name_to_idx: HashMap<&String, usize> = HashMap::new();
        for (i, name) in node_names.iter().enumerate() {
            node_name_to_idx.insert(name, i);
        }

        // Initialize new map with empty entries
        for _ in 0..self.num_vbuckets {
            new_map.push(VBucketOwnership {
                active_node: String::new(),
                replica_nodes: Vec::new(),
            });
        }

        // Try to keep existing assignments if the node still exists and is under target
        for (vb_id, old_entry) in old_map.iter().enumerate() {
            if let Some(&idx) = node_name_to_idx.get(&old_entry.active_node) {
                if node_assigned[idx] < target_counts[idx] {
                    new_map[vb_id].active_node = old_entry.active_node.clone();
                    node_assigned[idx] += 1;
                    continue;
                }
            }
            unassigned.push(vb_id as u16);
        }

        // Assign remaining vBuckets to nodes that still have capacity
        let mut assign_idx = 0;
        for vb_id in unassigned {
            // Find next node with capacity
            loop {
                if node_assigned[assign_idx] < target_counts[assign_idx] {
                    break;
                }
                assign_idx = (assign_idx + 1) % n;
            }
            new_map[vb_id as usize].active_node = node_names[assign_idx].clone();
            node_assigned[assign_idx] += 1;
        }

        // Assign replicas (basic round-robin; use rebalance_with_groups for rack awareness)
        for vb_id in 0..self.num_vbuckets as usize {
            let active_node = &new_map[vb_id].active_node;
            let active_idx = node_name_to_idx.get(active_node).copied().unwrap_or(0);
            let mut replicas = Vec::new();
            for r in 0..self.num_replicas as usize {
                if n > 1 {
                    let replica_idx = (active_idx + 1 + r) % n;
                    if replica_idx != active_idx {
                        replicas.push(node_names[replica_idx].clone());
                    }
                }
            }
            new_map[vb_id].replica_nodes = replicas;
        }

        // Build transfer list
        for (vb_id, (old_entry, new_entry)) in
            old_map.iter().zip(new_map.iter()).enumerate()
        {
            if old_entry.active_node != new_entry.active_node && !old_entry.active_node.is_empty() {
                transfers.push(VBucketTransfer {
                    vbucket_id: vb_id as u16,
                    from_node: old_entry.active_node.clone(),
                    to_node: new_entry.active_node.clone(),
                    status: TransferStatus::Pending,
                });
            }
        }

        self.map = new_map;
        self.revision += 1;

        info!(
            "Rebalance complete: revision={}, {} transfers needed across {} nodes",
            self.revision,
            transfers.len(),
            n
        );

        transfers
    }

    /// Get the active node for a given vBucket
    pub fn get_active_node(&self, vbucket_id: u16) -> Option<&str> {
        self.map.get(vbucket_id as usize).map(|o| o.active_node.as_str())
    }

    /// Get replica nodes for a given vBucket
    #[allow(dead_code)]
    pub fn get_replica_nodes(&self, vbucket_id: u16) -> Vec<&str> {
        self.map
            .get(vbucket_id as usize)
            .map(|o| o.replica_nodes.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Check if a given node is the active owner of a vBucket
    pub fn is_active_on(&self, vbucket_id: u16, node_name: &str) -> bool {
        self.map
            .get(vbucket_id as usize)
            .map(|o| o.active_node == node_name)
            .unwrap_or(false)
    }

    /// Get per-node partition summary
    pub fn get_node_partition_info(&self, node_names: &[String]) -> Vec<NodePartitionInfo> {
        let mut infos: HashMap<&str, NodePartitionInfo> = HashMap::new();

        for name in node_names {
            infos.insert(
                name.as_str(),
                NodePartitionInfo {
                    node_name: name.clone(),
                    active_vbuckets: Vec::new(),
                    replica_vbuckets: Vec::new(),
                    active_count: 0,
                    replica_count: 0,
                },
            );
        }

        for (vb_id, entry) in self.map.iter().enumerate() {
            if let Some(info) = infos.get_mut(entry.active_node.as_str()) {
                info.active_vbuckets.push(vb_id as u16);
                info.active_count += 1;
            }
            for replica in &entry.replica_nodes {
                if let Some(info) = infos.get_mut(replica.as_str()) {
                    info.replica_vbuckets.push(vb_id as u16);
                    info.replica_count += 1;
                }
            }
        }

        node_names
            .iter()
            .filter_map(|n| infos.remove(n.as_str()))
            .collect()
    }

    /// Get all vBuckets that are active on a given node
    pub fn active_vbuckets_for(&self, node_name: &str) -> Vec<u16> {
        self.map
            .iter()
            .enumerate()
            .filter(|(_, o)| o.active_node == node_name)
            .map(|(id, _)| id as u16)
            .collect()
    }

    /// Rebalance with server group (rack/zone) awareness.
    /// Replicas are placed on nodes in different server groups when possible.
    pub fn rebalance_with_groups(
        &mut self,
        node_names: &[String],
        node_groups: &HashMap<String, String>,
    ) -> Vec<VBucketTransfer> {
        if node_names.is_empty() {
            return Vec::new();
        }

        let old_map = self.map.clone();
        let n = node_names.len();
        let mut transfers = Vec::new();

        // Calculate ideal distribution
        let base_count = self.num_vbuckets as usize / n;
        let remainder = self.num_vbuckets as usize % n;
        let target_counts: Vec<usize> = (0..n)
            .map(|i| if i < remainder { base_count + 1 } else { base_count })
            .collect();

        let mut node_name_to_idx: HashMap<&String, usize> = HashMap::new();
        for (i, name) in node_names.iter().enumerate() {
            node_name_to_idx.insert(name, i);
        }

        // Initialize new map
        let mut new_map: Vec<VBucketOwnership> = (0..self.num_vbuckets)
            .map(|_| VBucketOwnership {
                active_node: String::new(),
                replica_nodes: Vec::new(),
            })
            .collect();

        let mut node_assigned: Vec<usize> = vec![0; n];
        let mut unassigned: Vec<u16> = Vec::new();

        // Keep existing assignments if valid
        for (vb_id, old_entry) in old_map.iter().enumerate() {
            if let Some(&idx) = node_name_to_idx.get(&old_entry.active_node) {
                if node_assigned[idx] < target_counts[idx] {
                    new_map[vb_id].active_node = old_entry.active_node.clone();
                    node_assigned[idx] += 1;
                    continue;
                }
            }
            unassigned.push(vb_id as u16);
        }

        // Assign remaining vBuckets
        let mut assign_idx = 0;
        for vb_id in unassigned {
            loop {
                if node_assigned[assign_idx] < target_counts[assign_idx] {
                    break;
                }
                assign_idx = (assign_idx + 1) % n;
            }
            new_map[vb_id as usize].active_node = node_names[assign_idx].clone();
            node_assigned[assign_idx] += 1;
        }

        // Assign replicas with server group awareness
        for vb_id in 0..self.num_vbuckets as usize {
            let active_node = &new_map[vb_id].active_node;
            let active_group = node_groups
                .get(active_node)
                .cloned()
                .unwrap_or_else(|| "Group 1".to_string());

            let mut replicas = Vec::new();
            let mut used_groups: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            used_groups.insert(active_group.clone());

            // First pass: pick replicas from different groups
            for name in node_names {
                if replicas.len() >= self.num_replicas as usize {
                    break;
                }
                if name == active_node {
                    continue;
                }
                let group = node_groups
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| "Group 1".to_string());
                if !used_groups.contains(&group) {
                    replicas.push(name.clone());
                    used_groups.insert(group);
                }
            }

            // Second pass: fill remaining replicas from any available node
            if replicas.len() < self.num_replicas as usize {
                for name in node_names {
                    if replicas.len() >= self.num_replicas as usize {
                        break;
                    }
                    if name == active_node || replicas.contains(name) {
                        continue;
                    }
                    replicas.push(name.clone());
                }
            }

            new_map[vb_id].replica_nodes = replicas;
        }

        // Build transfer list
        for (vb_id, (old_entry, new_entry)) in
            old_map.iter().zip(new_map.iter()).enumerate()
        {
            if old_entry.active_node != new_entry.active_node && !old_entry.active_node.is_empty() {
                transfers.push(VBucketTransfer {
                    vbucket_id: vb_id as u16,
                    from_node: old_entry.active_node.clone(),
                    to_node: new_entry.active_node.clone(),
                    status: TransferStatus::Pending,
                });
            }
        }

        self.map = new_map;
        self.revision += 1;

        info!(
            "Rebalance (group-aware) complete: revision={}, {} transfers across {} nodes",
            self.revision,
            transfers.len(),
            n
        );

        transfers
    }

    /// Get all vBuckets that are replicas on a given node
    #[allow(dead_code)]
    pub fn replica_vbuckets_for(&self, node_name: &str) -> Vec<u16> {
        self.map
            .iter()
            .enumerate()
            .filter(|(_, o)| o.replica_nodes.iter().any(|r| r == node_name))
            .map(|(id, _)| id as u16)
            .collect()
    }
}

/// Describes a vBucket transfer during rebalance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VBucketTransfer {
    pub vbucket_id: u16,
    pub from_node: String,
    pub to_node: String,
    pub status: TransferStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Data payload for transferring a vBucket between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VBucketData {
    pub vbucket_id: u16,
    pub high_seq_no: u64,
    pub documents: Vec<crate::storage::document::Document>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_map_single_node() {
        let nodes = vec!["node-1".to_string()];
        let pmap = PartitionMap::new(8, 1, &nodes);
        assert_eq!(pmap.map.len(), 8);
        for entry in &pmap.map {
            assert_eq!(entry.active_node, "node-1");
            assert!(entry.replica_nodes.is_empty()); // Can't replicate to self
        }
    }

    #[test]
    fn test_partition_map_two_nodes() {
        let nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let pmap = PartitionMap::new(8, 1, &nodes);
        let n1_count = pmap.map.iter().filter(|e| e.active_node == "node-1").count();
        let n2_count = pmap.map.iter().filter(|e| e.active_node == "node-2").count();
        assert_eq!(n1_count, 4);
        assert_eq!(n2_count, 4);
    }

    #[test]
    fn test_rebalance_add_node() {
        let nodes = vec!["node-1".to_string()];
        let mut pmap = PartitionMap::new(12, 1, &nodes);
        // All on node-1
        assert_eq!(pmap.active_vbuckets_for("node-1").len(), 12);

        // Add node-2
        let new_nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let transfers = pmap.rebalance(&new_nodes);

        assert!(!transfers.is_empty());
        // After rebalance, should be roughly 6/6
        let n1 = pmap.active_vbuckets_for("node-1").len();
        let n2 = pmap.active_vbuckets_for("node-2").len();
        assert_eq!(n1 + n2, 12);
        assert_eq!(n1, 6);
        assert_eq!(n2, 6);
    }

    #[test]
    fn test_rebalance_remove_node() {
        let nodes = vec!["node-1".to_string(), "node-2".to_string(), "node-3".to_string()];
        let mut pmap = PartitionMap::new(12, 1, &nodes);

        // Remove node-3
        let new_nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let transfers = pmap.rebalance(&new_nodes);

        let n1 = pmap.active_vbuckets_for("node-1").len();
        let n2 = pmap.active_vbuckets_for("node-2").len();
        assert_eq!(n1 + n2, 12);
        assert_eq!(n1, 6);
        assert_eq!(n2, 6);
        assert!(pmap.active_vbuckets_for("node-3").is_empty());
    }
}
