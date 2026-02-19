//! Orchestrator Election (Couchbase-style)
//!
//! Couchbase uses a simple deterministic leader election:
//! - The node with the lexicographically lowest name becomes the orchestrator
//! - All cluster topology decisions (rebalance, failover) are coordinated by the orchestrator
//! - If the orchestrator fails, the next lowest healthy node takes over
//!
//! This avoids the complexity of Raft leader election while providing
//! a single point of coordination for cluster management operations.

use serde::{Deserialize, Serialize};

/// Orchestrator role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrchestratorRole {
    /// This node is the orchestrator (cluster coordinator)
    Orchestrator,
    /// This node is a follower (defers topology decisions to orchestrator)
    Follower,
}

/// Orchestrator state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorState {
    /// Current orchestrator node name
    pub orchestrator_node: String,
    /// This node's role
    pub role: OrchestratorRole,
    /// Orchestrator base URL
    pub orchestrator_url: Option<String>,
    /// Number of healthy nodes participating in election
    pub participating_nodes: usize,
    /// Revision (incremented on every orchestrator change)
    pub revision: u64,
}

/// Determine who should be the orchestrator.
/// Simple rule: the lexicographically lowest healthy node name wins.
///
/// `healthy_nodes` is a list of `(node_name, base_url)` tuples.
pub fn elect_orchestrator(healthy_nodes: &[(String, String)]) -> Option<(String, String)> {
    healthy_nodes
        .iter()
        .min_by_key(|(name, _)| name.clone())
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elect_single_node() {
        let nodes = vec![("node-1".to_string(), "http://host1:8091".to_string())];
        let (name, url) = elect_orchestrator(&nodes).unwrap();
        assert_eq!(name, "node-1");
        assert_eq!(url, "http://host1:8091");
    }

    #[test]
    fn test_elect_multiple_nodes() {
        let nodes = vec![
            ("node-3".to_string(), "http://host3:8091".to_string()),
            ("node-1".to_string(), "http://host1:8091".to_string()),
            ("node-2".to_string(), "http://host2:8091".to_string()),
        ];
        let (name, _) = elect_orchestrator(&nodes).unwrap();
        assert_eq!(name, "node-1"); // Lowest name wins
    }

    #[test]
    fn test_elect_empty() {
        let nodes: Vec<(String, String)> = vec![];
        assert!(elect_orchestrator(&nodes).is_none());
    }
}
