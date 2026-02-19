//! Durability Levels (Couchbase-style)
//!
//! Couchbase supports different durability guarantees for write operations:
//! - None:                        Write is ACKed after being written to active vBucket memory
//! - Majority:                    Wait for majority of replicas to acknowledge in memory
//! - MajorityAndPersistToActive:  Majority ACK + persist to disk on active node
//! - PersistToMajority:           Persist to disk on majority of nodes (strongest)
//!
//! Higher durability = higher latency but stronger data safety guarantees.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Durability level for write operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityLevel {
    /// No durability guarantee — write to active vBucket memory only (fastest)
    None,
    /// Wait for majority of replicas to acknowledge in memory
    Majority,
    /// Majority ACK in memory + persist to disk on active node
    MajorityAndPersistToActive,
    /// Persist to disk on majority of nodes (strongest, slowest)
    PersistToMajority,
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        DurabilityLevel::None
    }
}

/// Durability requirement for a specific write operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityRequirement {
    pub level: DurabilityLevel,
    /// Timeout for durability acknowledgment (in milliseconds)
    pub timeout_ms: u64,
}

impl Default for DurabilityRequirement {
    fn default() -> Self {
        Self {
            level: DurabilityLevel::None,
            timeout_ms: 2500, // Couchbase default: 2.5s
        }
    }
}

impl DurabilityRequirement {
    /// Calculate how many replica ACKs are needed for the given level
    pub fn required_acks(&self, num_replicas: u8) -> usize {
        match self.level {
            DurabilityLevel::None => 0,
            DurabilityLevel::Majority
            | DurabilityLevel::MajorityAndPersistToActive
            | DurabilityLevel::PersistToMajority => {
                // total_copies = 1 (active) + num_replicas
                // majority = total / 2 + 1
                // acks_needed = majority - 1 (active already has it)
                let total = 1 + num_replicas as usize;
                let majority = total / 2 + 1;
                majority.saturating_sub(1)
            }
        }
    }

    pub fn requires_persist_active(&self) -> bool {
        matches!(
            self.level,
            DurabilityLevel::MajorityAndPersistToActive | DurabilityLevel::PersistToMajority
        )
    }

    #[allow(dead_code)]
    pub fn requires_persist_replicas(&self) -> bool {
        matches!(self.level, DurabilityLevel::PersistToMajority)
    }
}

/// Token tracking the durability state of a pending write
#[derive(Debug, Clone)]
pub struct DurabilityToken {
    /// Document key
    pub key: String,
    /// vBucket ID
    pub vbucket_id: u16,
    /// CAS of the write
    pub cas: u64,
    /// Sequence number
    pub seq_no: u64,
    /// Required durability
    pub requirement: DurabilityRequirement,
    /// Replica ACKs received: node_name → acked
    pub replica_acks: Vec<(String, bool)>,
    /// Whether active node has persisted
    pub active_persisted: bool,
    /// Created timestamp
    pub created_at: Instant,
}

impl DurabilityToken {
    pub fn new(
        key: String,
        vbucket_id: u16,
        cas: u64,
        seq_no: u64,
        requirement: DurabilityRequirement,
        replica_nodes: Vec<String>,
    ) -> Self {
        let replica_acks = replica_nodes.into_iter().map(|n| (n, false)).collect();
        Self {
            key,
            vbucket_id,
            cas,
            seq_no,
            requirement,
            replica_acks,
            active_persisted: false,
            created_at: Instant::now(),
        }
    }

    /// Record a replica ACK
    #[allow(dead_code)]
    pub fn ack_replica(&mut self, node_name: &str) {
        for (name, acked) in &mut self.replica_acks {
            if name == node_name {
                *acked = true;
                break;
            }
        }
    }

    /// Record active persist
    pub fn ack_active_persist(&mut self) {
        self.active_persisted = true;
    }

    /// Check if durability requirement is satisfied
    pub fn is_satisfied(&self) -> bool {
        let required = self
            .requirement
            .required_acks(self.replica_acks.len() as u8);
        let acked = self.replica_acks.iter().filter(|(_, a)| *a).count();

        if acked < required {
            return false;
        }

        if self.requirement.requires_persist_active() && !self.active_persisted {
            return false;
        }

        true
    }

    /// Check if timed out
    pub fn is_timed_out(&self) -> bool {
        self.created_at.elapsed() > Duration::from_millis(self.requirement.timeout_ms)
    }
}

/// Manages pending durability tokens for in-flight writes
pub struct DurabilityManager {
    /// Pending tokens: (vbucket_id, cas) → DurabilityToken
    pending: RwLock<HashMap<(u16, u64), DurabilityToken>>,
}

impl DurabilityManager {
    pub fn new() -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new durable write
    pub fn register(&self, token: DurabilityToken) {
        let key = (token.vbucket_id, token.cas);
        if let Ok(mut pending) = self.pending.write() {
            pending.insert(key, token);
        }
    }

    /// Process a replica ACK
    #[allow(dead_code)]
    pub fn ack_replica(&self, vbucket_id: u16, cas: u64, node_name: &str) -> Option<bool> {
        let mut pending = self.pending.write().ok()?;
        let key = (vbucket_id, cas);
        if let Some(token) = pending.get_mut(&key) {
            token.ack_replica(node_name);
            if token.is_satisfied() {
                pending.remove(&key);
                return Some(true); // Durability satisfied
            }
            return Some(false); // Not yet satisfied
        }
        None // Token not found (non-durable write or already completed)
    }

    /// Process active persist ACK
    pub fn ack_persist(&self, vbucket_id: u16, cas: u64) -> Option<bool> {
        let mut pending = self.pending.write().ok()?;
        let key = (vbucket_id, cas);
        if let Some(token) = pending.get_mut(&key) {
            token.ack_active_persist();
            if token.is_satisfied() {
                pending.remove(&key);
                return Some(true);
            }
            return Some(false);
        }
        None
    }

    /// Clean up timed-out tokens, returns count of timed-out entries
    pub fn cleanup_timed_out(&self) -> usize {
        if let Ok(mut pending) = self.pending.write() {
            let before = pending.len();
            pending.retain(|_, token| !token.is_timed_out());
            before - pending.len()
        } else {
            0
        }
    }

    /// Get number of pending durable writes
    pub fn pending_count(&self) -> usize {
        self.pending.read().map(|p| p.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_required_acks_none() {
        let req = DurabilityRequirement {
            level: DurabilityLevel::None,
            timeout_ms: 2500,
        };
        assert_eq!(req.required_acks(0), 0);
        assert_eq!(req.required_acks(1), 0);
        assert_eq!(req.required_acks(2), 0);
    }

    #[test]
    fn test_required_acks_majority() {
        let req = DurabilityRequirement {
            level: DurabilityLevel::Majority,
            timeout_ms: 2500,
        };
        // 1 active + 1 replica = 2 total → majority = 2 → acks = 1
        assert_eq!(req.required_acks(1), 1);
        // 1 active + 2 replicas = 3 total → majority = 2 → acks = 1
        assert_eq!(req.required_acks(2), 1);
        // 1 active + 3 replicas = 4 total → majority = 3 → acks = 2
        assert_eq!(req.required_acks(3), 2);
    }

    #[test]
    fn test_durability_token_satisfaction() {
        let req = DurabilityRequirement {
            level: DurabilityLevel::Majority,
            timeout_ms: 2500,
        };
        let mut token = DurabilityToken::new(
            "key1".to_string(),
            0,
            100,
            1,
            req,
            vec!["node-2".to_string()],
        );
        assert!(!token.is_satisfied());
        token.ack_replica("node-2");
        assert!(token.is_satisfied());
    }

    #[test]
    fn test_persist_to_active() {
        let req = DurabilityRequirement {
            level: DurabilityLevel::MajorityAndPersistToActive,
            timeout_ms: 2500,
        };
        let mut token = DurabilityToken::new(
            "key1".to_string(),
            0,
            100,
            1,
            req,
            vec!["node-2".to_string()],
        );
        token.ack_replica("node-2");
        assert!(!token.is_satisfied()); // Need persist too
        token.ack_active_persist();
        assert!(token.is_satisfied());
    }
}
