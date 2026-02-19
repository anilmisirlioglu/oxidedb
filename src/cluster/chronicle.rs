//! Chronicle: Metadata Consensus Engine (Couchbase-style)
//!
//! Implements a replicated configuration log for cluster metadata.
//! Similar to Couchbase's Chronicle (which replaced ns_config in 7.x):
//!
//! Key concepts:
//! - **Config Log**: Ordered sequence of configuration changes
//! - **Orchestrator as Leader**: Only the orchestrator can commit entries
//! - **Majority ACK**: Changes require majority of nodes to acknowledge
//! - **Atomic Application**: Committed entries are applied atomically
//!
//! What gets stored in Chronicle:
//! - Bucket create/delete/update
//! - Node membership changes (add/remove/failover)
//! - Partition map updates (rebalance results)
//! - Scope/collection changes
//!
//! Flow:
//! 1. Any node proposes a config change → forwarded to orchestrator
//! 2. Orchestrator appends to log → sends Prepare to all nodes
//! 3. Nodes persist entry and ACK
//! 4. Once majority ACK → Orchestrator marks Committed
//! 5. Committed entries are applied to cluster state
//!
//! This is NOT full Raft — there is no leader election here.
//! Leader election is handled by the deterministic orchestrator module.
//! Chronicle only handles the replicated config log.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use tracing::{debug, info, warn};

// ═══════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════

/// Type of configuration change tracked by Chronicle
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigChangeType {
    /// Bucket created
    BucketCreate,
    /// Bucket deleted
    BucketDelete,
    /// Bucket config updated (replicas, quota, etc.)
    BucketUpdate,
    /// Scope created
    ScopeCreate,
    /// Scope deleted
    ScopeDelete,
    /// Collection created
    CollectionCreate,
    /// Collection deleted
    CollectionDelete,
    /// Node added to cluster
    NodeAdd,
    /// Node removed from cluster
    NodeRemove,
    /// Node failed over
    NodeFailover,
    /// Node recovered from failover
    NodeRecover,
    /// Partition map updated (after rebalance)
    PartitionMapUpdate,
    /// Cluster settings changed
    ClusterSettingsUpdate,
    /// Server group created
    ServerGroupAdd,
    /// Server group deleted
    ServerGroupDelete,
    /// Node moved between server groups
    ServerGroupNodeMove,
}

/// Status of a config entry in the log
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStatus {
    /// Sent to nodes, waiting for ACKs
    Prepared,
    /// Majority ACKed — committed and safe
    Committed,
    /// Failed (timeout or not enough ACKs)
    Failed,
}

/// A single configuration change entry in the Chronicle log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigEntry {
    /// Term number (increments when orchestrator changes)
    pub term: u64,
    /// Sequential index in the log
    pub index: u64,
    /// Type of change
    pub change_type: ConfigChangeType,
    /// Change payload (JSON — contents depend on change_type)
    pub payload: serde_json::Value,
    /// Node that proposed this change
    pub proposed_by: String,
    /// Current status
    pub status: EntryStatus,
    /// Nodes that have ACKed this entry
    pub acks: Vec<String>,
    /// Required number of ACKs for commit (majority)
    pub required_acks: usize,
    /// Timestamp of creation
    pub timestamp: DateTime<Utc>,
}

/// Proposal for a config change (sent to orchestrator)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigProposal {
    pub change_type: ConfigChangeType,
    pub payload: serde_json::Value,
    pub proposed_by: String,
}

/// Request from orchestrator → follower to replicate an entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareRequest {
    pub entry: ConfigEntry,
    pub leader_term: u64,
    pub leader_commit_index: u64,
}

/// Response from follower → orchestrator after receiving a Prepare
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareResponse {
    pub node: String,
    pub term: u64,
    pub index: u64,
    pub success: bool,
    pub reason: Option<String>,
}

/// Commit notification from orchestrator → followers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitNotification {
    pub index: u64,
    pub term: u64,
}

/// Chronicle state snapshot (for API/debugging)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChronicleSnapshot {
    pub node_name: String,
    pub is_leader: bool,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_length: usize,
    pub cluster_size: usize,
    pub majority_required: usize,
    pub recent_entries: Vec<ConfigEntry>,
}

// ═══════════════════════════════════════════════════════════════════════
// Chronicle Engine
// ═══════════════════════════════════════════════════════════════════════

/// The Chronicle metadata consensus engine.
///
/// Maintains a replicated log of cluster configuration changes.
/// The orchestrator node acts as the leader — proposes, collects ACKs, commits.
/// Follower nodes receive Prepare requests and ACK them.
pub struct Chronicle {
    /// This node's name
    node_name: String,

    /// Whether this node is the orchestrator (leader for consensus)
    is_leader: RwLock<bool>,

    /// Current term (incremented when orchestrator changes)
    current_term: RwLock<u64>,

    /// The replicated config log (append-only)
    log: RwLock<Vec<ConfigEntry>>,

    /// Index of highest committed entry
    commit_index: RwLock<u64>,

    /// Index of highest entry applied to local state
    last_applied: RwLock<u64>,

    /// Next index for new entries
    next_index: RwLock<u64>,

    /// Number of nodes in the cluster (for majority calculation)
    cluster_size: RwLock<usize>,
}

impl Chronicle {
    pub fn new(node_name: String) -> Self {
        Self {
            node_name,
            is_leader: RwLock::new(true), // Single node starts as leader
            current_term: RwLock::new(1),
            log: RwLock::new(Vec::new()),
            commit_index: RwLock::new(0),
            last_applied: RwLock::new(0),
            next_index: RwLock::new(1),
            cluster_size: RwLock::new(1),
        }
    }

    // ── Cluster size & leadership ─────────────────────────────────────

    /// Update cluster size (called when nodes join/leave).
    /// This affects the majority calculation.
    pub fn update_cluster_size(&self, size: usize) {
        if let Ok(mut s) = self.cluster_size.write() {
            *s = size.max(1);
        }
    }

    /// Calculate majority count: floor(n/2) + 1
    fn majority(&self) -> usize {
        let size = self.cluster_size.read().map(|s| *s).unwrap_or(1);
        size / 2 + 1
    }

    /// Set whether this node is the leader (called by orchestrator election)
    pub fn set_leader(&self, is_leader: bool) {
        if let Ok(mut l) = self.is_leader.write() {
            let was_leader = *l;
            *l = is_leader;
            if is_leader && !was_leader {
                info!("Chronicle: this node is now the leader (orchestrator)");
            } else if !is_leader && was_leader {
                info!("Chronicle: this node is now a follower");
            }
        }
    }

    /// Increment term (called when orchestrator changes)
    pub fn new_term(&self) {
        if let Ok(mut term) = self.current_term.write() {
            *term += 1;
            info!("Chronicle: term incremented to {}", *term);
        }
    }

    #[allow(dead_code)]
    pub fn is_leader(&self) -> bool {
        self.is_leader.read().map(|l| *l).unwrap_or(false)
    }

    // ── Proposing changes (orchestrator side) ─────────────────────────

    /// Propose a config change. Only the orchestrator can commit.
    ///
    /// On a single-node cluster, this immediately commits.
    /// On a multi-node cluster, this creates a Prepared entry that needs ACKs.
    ///
    /// Returns the entry (Committed if single-node, Prepared if multi-node).
    pub fn propose(&self, proposal: ConfigProposal) -> Result<ConfigEntry, String> {
        let is_leader = self.is_leader.read().map(|l| *l).unwrap_or(false);
        if !is_leader {
            return Err("Not the orchestrator — proposal must be forwarded to leader".to_string());
        }

        let term = self.current_term.read().map(|t| *t).unwrap_or(1);
        let index = {
            let mut next = self.next_index.write().map_err(|e| e.to_string())?;
            let idx = *next;
            *next += 1;
            idx
        };

        let majority = self.majority();

        let mut entry = ConfigEntry {
            term,
            index,
            change_type: proposal.change_type.clone(),
            payload: proposal.payload,
            proposed_by: proposal.proposed_by,
            status: EntryStatus::Prepared,
            acks: vec![self.node_name.clone()], // Leader self-ACKs
            required_acks: majority,
            timestamp: Utc::now(),
        };

        // Single node → immediate commit (majority of 1, leader already ACKed)
        if majority <= 1 {
            entry.status = EntryStatus::Committed;

            if let Ok(mut log) = self.log.write() {
                log.push(entry.clone());
            }
            if let Ok(mut ci) = self.commit_index.write() {
                *ci = index;
            }
            if let Ok(mut la) = self.last_applied.write() {
                *la = index;
            }

            debug!(
                "Chronicle: entry #{} auto-committed (single-node, type={:?})",
                index, proposal.change_type
            );
            return Ok(entry);
        }

        // Multi-node → add as Prepared, needs follower ACKs
        if let Ok(mut log) = self.log.write() {
            log.push(entry.clone());
        }

        info!(
            "Chronicle: entry #{} prepared (type={:?}, need {}/{} ACKs)",
            index,
            proposal.change_type,
            majority,
            self.cluster_size.read().map(|s| *s).unwrap_or(0)
        );

        Ok(entry)
    }

    /// Build a PrepareRequest to send to a follower for a given entry
    #[allow(dead_code)]
    pub fn build_prepare_request(&self, entry: &ConfigEntry) -> PrepareRequest {
        PrepareRequest {
            entry: entry.clone(),
            leader_term: self.current_term.read().map(|t| *t).unwrap_or(1),
            leader_commit_index: self.commit_index.read().map(|c| *c).unwrap_or(0),
        }
    }

    // ── Receiving entries (follower side) ─────────────────────────────

    /// Handle a PrepareRequest from the orchestrator (called on follower nodes).
    /// Appends the entry to local log and returns an ACK.
    pub fn handle_prepare(&self, req: &PrepareRequest) -> PrepareResponse {
        let my_term = self.current_term.read().map(|t| *t).unwrap_or(0);

        // Accept if leader term >= our term
        if req.leader_term < my_term {
            return PrepareResponse {
                node: self.node_name.clone(),
                term: my_term,
                index: req.entry.index,
                success: false,
                reason: Some(format!(
                    "Stale leader term {} < my term {}",
                    req.leader_term, my_term
                )),
            };
        }

        // Update our term to leader's term
        if let Ok(mut term) = self.current_term.write() {
            if req.leader_term > *term {
                *term = req.leader_term;
            }
        }

        // Append to local log (idempotent)
        if let Ok(mut log) = self.log.write() {
            let exists = log
                .iter()
                .any(|e| e.term == req.entry.term && e.index == req.entry.index);
            if !exists {
                log.push(req.entry.clone());
                debug!(
                    "Chronicle: follower appended entry #{} (type={:?})",
                    req.entry.index, req.entry.change_type
                );
            }
        }

        // Apply any committed entries from leader's commit_index
        if let Ok(mut log) = self.log.write() {
            for entry in log.iter_mut() {
                if entry.index <= req.leader_commit_index
                    && entry.status == EntryStatus::Prepared
                {
                    entry.status = EntryStatus::Committed;
                }
            }
        }
        if let Ok(mut ci) = self.commit_index.write() {
            if req.leader_commit_index > *ci {
                *ci = req.leader_commit_index;
            }
        }

        PrepareResponse {
            node: self.node_name.clone(),
            term: my_term,
            index: req.entry.index,
            success: true,
            reason: None,
        }
    }

    // ── Processing ACKs (orchestrator side) ───────────────────────────

    /// Process a follower's ACK for a Prepared entry.
    /// If this ACK gives us majority, the entry transitions to Committed.
    /// Returns the entry if it was just committed, None otherwise.
    pub fn process_ack(&self, index: u64, node_name: &str) -> Option<ConfigEntry> {
        let mut committed_entry = None;

        if let Ok(mut log) = self.log.write() {
            if let Some(entry) = log.iter_mut().find(|e| e.index == index) {
                // Add ACK if not already present
                if !entry.acks.contains(&node_name.to_string()) {
                    entry.acks.push(node_name.to_string());
                }

                // Check if we now have majority
                if entry.acks.len() >= entry.required_acks
                    && entry.status == EntryStatus::Prepared
                {
                    entry.status = EntryStatus::Committed;
                    committed_entry = Some(entry.clone());

                    info!(
                        "Chronicle: entry #{} committed (type={:?}, acks={}/{})",
                        entry.index,
                        entry.change_type,
                        entry.acks.len(),
                        entry.required_acks
                    );
                }
            }
        }

        // Update commit index
        if let Some(ref entry) = committed_entry {
            if let Ok(mut ci) = self.commit_index.write() {
                if entry.index > *ci {
                    *ci = entry.index;
                }
            }
            if let Ok(mut la) = self.last_applied.write() {
                if entry.index > *la {
                    *la = entry.index;
                }
            }
        }

        committed_entry
    }

    // ── Commit notification (follower side) ───────────────────────────

    /// Handle a commit notification from the orchestrator.
    /// Marks the entry as committed in our local log.
    pub fn apply_commit(&self, notification: &CommitNotification) {
        if let Ok(mut log) = self.log.write() {
            if let Some(entry) = log
                .iter_mut()
                .find(|e| e.index == notification.index && e.term == notification.term)
            {
                entry.status = EntryStatus::Committed;
                debug!(
                    "Chronicle: follower committed entry #{} (type={:?})",
                    entry.index, entry.change_type
                );
            }
        }

        if let Ok(mut ci) = self.commit_index.write() {
            if notification.index > *ci {
                *ci = notification.index;
            }
        }
        if let Ok(mut la) = self.last_applied.write() {
            if notification.index > *la {
                *la = notification.index;
            }
        }
    }

    // ── Query methods ─────────────────────────────────────────────────

    /// Get a snapshot of Chronicle state (for API/monitoring)
    pub fn get_snapshot(&self) -> ChronicleSnapshot {
        let term = self.current_term.read().map(|t| *t).unwrap_or(0);
        let ci = self.commit_index.read().map(|c| *c).unwrap_or(0);
        let la = self.last_applied.read().map(|a| *a).unwrap_or(0);
        let is_leader = self.is_leader.read().map(|l| *l).unwrap_or(false);
        let size = self.cluster_size.read().map(|s| *s).unwrap_or(1);
        let log = self.log.read().map(|l| l.clone()).unwrap_or_default();

        // Return last 50 entries (most recent first)
        let recent: Vec<ConfigEntry> = log.iter().rev().take(50).cloned().collect();

        ChronicleSnapshot {
            node_name: self.node_name.clone(),
            is_leader,
            current_term: term,
            commit_index: ci,
            last_applied: la,
            log_length: log.len(),
            cluster_size: size,
            majority_required: self.majority(),
            recent_entries: recent,
        }
    }

    /// Get all committed entries since a given index (for catch-up)
    pub fn get_committed_since(&self, since_index: u64) -> Vec<ConfigEntry> {
        self.log
            .read()
            .map(|log| {
                log.iter()
                    .filter(|e| e.index > since_index && e.status == EntryStatus::Committed)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get uncommitted (Prepared) entries — for leader to retry sending
    #[allow(dead_code)]
    pub fn get_uncommitted(&self) -> Vec<ConfigEntry> {
        self.log
            .read()
            .map(|log| {
                log.iter()
                    .filter(|e| e.status == EntryStatus::Prepared)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get log length
    #[allow(dead_code)]
    pub fn log_length(&self) -> usize {
        self.log.read().map(|l| l.len()).unwrap_or(0)
    }

    /// Get commit index
    pub fn get_commit_index(&self) -> u64 {
        self.commit_index.read().map(|c| *c).unwrap_or(0)
    }

    /// Get current term
    #[allow(dead_code)]
    pub fn get_term(&self) -> u64 {
        self.current_term.read().map(|t| *t).unwrap_or(0)
    }

    /// Expire old failed entries to prevent unbounded log growth
    pub fn gc_old_entries(&self, keep_committed: usize) {
        if let Ok(mut log) = self.log.write() {
            // Remove failed entries
            log.retain(|e| e.status != EntryStatus::Failed);

            // Keep only the last N committed entries
            let committed_count = log
                .iter()
                .filter(|e| e.status == EntryStatus::Committed)
                .count();
            if committed_count > keep_committed {
                let to_remove = committed_count - keep_committed;
                let mut removed = 0;
                log.retain(|e| {
                    if e.status == EntryStatus::Committed && removed < to_remove {
                        removed += 1;
                        false
                    } else {
                        true
                    }
                });
                if removed > 0 {
                    debug!("Chronicle: GC'd {} old committed entries", removed);
                }
            }
        }
    }

    /// Fail timed-out prepared entries (entries that didn't get enough ACKs)
    pub fn fail_timed_out_entries(&self, timeout_secs: i64) {
        let now = Utc::now();
        if let Ok(mut log) = self.log.write() {
            for entry in log.iter_mut() {
                if entry.status == EntryStatus::Prepared {
                    let elapsed = (now - entry.timestamp).num_seconds();
                    if elapsed > timeout_secs {
                        entry.status = EntryStatus::Failed;
                        warn!(
                            "Chronicle: entry #{} timed out (type={:?}, elapsed={}s, acks={}/{})",
                            entry.index,
                            entry.change_type,
                            elapsed,
                            entry.acks.len(),
                            entry.required_acks
                        );
                    }
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_auto_commit() {
        let chronicle = Chronicle::new("node-1".to_string());

        let proposal = ConfigProposal {
            change_type: ConfigChangeType::BucketCreate,
            payload: serde_json::json!({"name": "test-bucket", "type": "couchbase"}),
            proposed_by: "node-1".to_string(),
        };

        let entry = chronicle.propose(proposal).unwrap();
        assert_eq!(entry.status, EntryStatus::Committed);
        assert_eq!(entry.index, 1);
        assert_eq!(entry.term, 1);
        assert_eq!(chronicle.get_commit_index(), 1);
    }

    #[test]
    fn test_multi_node_needs_majority() {
        let chronicle = Chronicle::new("node-1".to_string());
        chronicle.update_cluster_size(3); // 3 nodes → majority = 2

        let proposal = ConfigProposal {
            change_type: ConfigChangeType::BucketCreate,
            payload: serde_json::json!({"name": "test-bucket"}),
            proposed_by: "node-1".to_string(),
        };

        let entry = chronicle.propose(proposal).unwrap();
        assert_eq!(entry.status, EntryStatus::Prepared); // Not yet committed

        // ACK from node-2 gives us majority (node-1 self-ACK + node-2 = 2 >= 2)
        let committed = chronicle.process_ack(1, "node-2");
        assert!(committed.is_some());
        assert_eq!(committed.unwrap().status, EntryStatus::Committed);
        assert_eq!(chronicle.get_commit_index(), 1);
    }

    #[test]
    fn test_follower_rejects_proposal() {
        let chronicle = Chronicle::new("node-2".to_string());
        chronicle.set_leader(false);

        let proposal = ConfigProposal {
            change_type: ConfigChangeType::BucketCreate,
            payload: serde_json::json!({"name": "test"}),
            proposed_by: "node-2".to_string(),
        };

        let result = chronicle.propose(proposal);
        assert!(result.is_err());
    }

    #[test]
    fn test_follower_handle_prepare() {
        let follower = Chronicle::new("node-2".to_string());
        follower.set_leader(false);

        let entry = ConfigEntry {
            term: 1,
            index: 1,
            change_type: ConfigChangeType::BucketCreate,
            payload: serde_json::json!({"name": "test-bucket"}),
            proposed_by: "node-1".to_string(),
            status: EntryStatus::Prepared,
            acks: vec!["node-1".to_string()],
            required_acks: 2,
            timestamp: Utc::now(),
        };

        let req = PrepareRequest {
            entry,
            leader_term: 1,
            leader_commit_index: 0,
        };

        let resp = follower.handle_prepare(&req);
        assert!(resp.success);
        assert_eq!(resp.node, "node-2");
        assert_eq!(follower.log_length(), 1);
    }

    #[test]
    fn test_commit_notification() {
        let follower = Chronicle::new("node-2".to_string());
        follower.set_leader(false);

        // First, receive a prepare
        let entry = ConfigEntry {
            term: 1,
            index: 1,
            change_type: ConfigChangeType::NodeAdd,
            payload: serde_json::json!({"node": "node-3"}),
            proposed_by: "node-1".to_string(),
            status: EntryStatus::Prepared,
            acks: vec!["node-1".to_string()],
            required_acks: 2,
            timestamp: Utc::now(),
        };

        let req = PrepareRequest {
            entry,
            leader_term: 1,
            leader_commit_index: 0,
        };
        follower.handle_prepare(&req);

        // Then receive commit notification
        let commit = CommitNotification { index: 1, term: 1 };
        follower.apply_commit(&commit);

        assert_eq!(follower.get_commit_index(), 1);
    }

    #[test]
    fn test_sequential_entries() {
        let chronicle = Chronicle::new("node-1".to_string());

        for i in 0..5 {
            let proposal = ConfigProposal {
                change_type: ConfigChangeType::BucketCreate,
                payload: serde_json::json!({"name": format!("bucket-{}", i)}),
                proposed_by: "node-1".to_string(),
            };
            let entry = chronicle.propose(proposal).unwrap();
            assert_eq!(entry.index, (i + 1) as u64);
            assert_eq!(entry.status, EntryStatus::Committed);
        }

        assert_eq!(chronicle.log_length(), 5);
        assert_eq!(chronicle.get_commit_index(), 5);
    }

    #[test]
    fn test_get_committed_since() {
        let chronicle = Chronicle::new("node-1".to_string());

        for i in 0..3 {
            let proposal = ConfigProposal {
                change_type: ConfigChangeType::BucketCreate,
                payload: serde_json::json!({"name": format!("bucket-{}", i)}),
                proposed_by: "node-1".to_string(),
            };
            chronicle.propose(proposal).unwrap();
        }

        let since_1 = chronicle.get_committed_since(1);
        assert_eq!(since_1.len(), 2); // entries 2, 3
    }

    #[test]
    fn test_five_node_majority() {
        let chronicle = Chronicle::new("node-1".to_string());
        chronicle.update_cluster_size(5); // majority = 3

        let proposal = ConfigProposal {
            change_type: ConfigChangeType::PartitionMapUpdate,
            payload: serde_json::json!({"revision": 42}),
            proposed_by: "node-1".to_string(),
        };

        let entry = chronicle.propose(proposal).unwrap();
        assert_eq!(entry.status, EntryStatus::Prepared);

        // One ACK not enough (1 self + 1 = 2 < 3)
        assert!(chronicle.process_ack(1, "node-2").is_none());

        // Two ACKs = majority (1 self + 2 = 3 >= 3)
        let committed = chronicle.process_ack(1, "node-3");
        assert!(committed.is_some());
        assert_eq!(committed.unwrap().status, EntryStatus::Committed);
    }
}
