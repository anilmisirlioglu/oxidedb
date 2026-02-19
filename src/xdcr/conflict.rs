use crate::storage::document::{Document, Mutation};
use crate::storage::engine::ConflictResolutionType;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Result of conflict resolution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictWinner {
    /// Local document wins
    Local,
    /// Remote mutation wins
    Remote,
}

/// Conflict resolution strategy
pub struct ConflictResolver {
    pub resolution_type: ConflictResolutionType,
}

impl ConflictResolver {
    pub fn new(resolution_type: ConflictResolutionType) -> Self {
        Self { resolution_type }
    }

    /// Resolve conflict between local document and remote mutation
    pub fn resolve(&self, local: &Document, remote: &Mutation) -> ConflictWinner {
        match self.resolution_type {
            ConflictResolutionType::SequenceNumber => {
                self.resolve_by_sequence(local, remote)
            }
            ConflictResolutionType::Timestamp => {
                self.resolve_by_timestamp(local, remote)
            }
        }
    }

    /// Resolve by revision sequence number (higher wins)
    /// Tie-breaking: higher CAS wins, then lexicographic comparison of values
    fn resolve_by_sequence(&self, local: &Document, remote: &Mutation) -> ConflictWinner {
        if remote.rev_id > local.rev_id {
            debug!(
                "Conflict on key '{}': remote wins (rev {} > {})",
                local.key, remote.rev_id, local.rev_id
            );
            ConflictWinner::Remote
        } else if remote.rev_id < local.rev_id {
            debug!(
                "Conflict on key '{}': local wins (rev {} > {})",
                local.key, local.rev_id, remote.rev_id
            );
            ConflictWinner::Local
        } else {
            // Same revision - tie break by CAS
            if remote.cas > local.cas {
                debug!(
                    "Conflict on key '{}': remote wins (cas {} > {})",
                    local.key, remote.cas, local.cas
                );
                ConflictWinner::Remote
            } else {
                debug!(
                    "Conflict on key '{}': local wins (cas {} >= {})",
                    local.key, local.cas, remote.cas
                );
                ConflictWinner::Local
            }
        }
    }

    /// Resolve by timestamp (last write wins)
    /// Tie-breaking: higher CAS wins
    fn resolve_by_timestamp(&self, local: &Document, remote: &Mutation) -> ConflictWinner {
        if remote.updated_at > local.updated_at {
            debug!(
                "Conflict on key '{}': remote wins (time {} > {})",
                local.key, remote.updated_at, local.updated_at
            );
            ConflictWinner::Remote
        } else if remote.updated_at < local.updated_at {
            debug!(
                "Conflict on key '{}': local wins (time {} > {})",
                local.key, local.updated_at, remote.updated_at
            );
            ConflictWinner::Local
        } else {
            // Same timestamp - tie break by CAS
            if remote.cas > local.cas {
                ConflictWinner::Remote
            } else {
                ConflictWinner::Local
            }
        }
    }
}

/// XDCR conflict statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConflictStats {
    pub total_conflicts: u64,
    pub local_wins: u64,
    pub remote_wins: u64,
    pub docs_replicated: u64,
    pub docs_failed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_sequence_resolution_remote_wins() {
        let resolver = ConflictResolver::new(ConflictResolutionType::SequenceNumber);
        let now = Utc::now();

        let local = Document {
            key: "test".to_string(),
            value: serde_json::json!({"v": 1}),
            cas: 1,
            seq_no: 1,
            rev_id: 1,
            expiry: None,
            flags: 0,
            created_at: now,
            updated_at: now,
            deleted: false,
            source_cluster: None,
            vbucket_id: 0,
            xattrs: std::collections::HashMap::new(),
            last_accessed: now,
            evicted: false,
        };

        let remote = Mutation {
            key: "test".to_string(),
            value: serde_json::json!({"v": 2}),
            cas: 2,
            seq_no: 2,
            rev_id: 2,
            expiry: None,
            flags: 0,
            updated_at: now,
            deleted: false,
            source_cluster: Some("remote".to_string()),
            vbucket_id: 0,
            xattrs: std::collections::HashMap::new(),
        };

        assert_eq!(resolver.resolve(&local, &remote), ConflictWinner::Remote);
    }

    #[test]
    fn test_timestamp_resolution_local_wins() {
        let resolver = ConflictResolver::new(ConflictResolutionType::Timestamp);

        let now = Utc::now();
        let earlier = now - chrono::Duration::seconds(10);

        let local = Document {
            key: "test".to_string(),
            value: serde_json::json!({"v": 1}),
            cas: 1,
            seq_no: 1,
            rev_id: 1,
            expiry: None,
            flags: 0,
            created_at: now,
            updated_at: now,
            deleted: false,
            source_cluster: None,
            vbucket_id: 0,
            xattrs: std::collections::HashMap::new(),
            last_accessed: now,
            evicted: false,
        };

        let remote = Mutation {
            key: "test".to_string(),
            value: serde_json::json!({"v": 2}),
            cas: 2,
            seq_no: 2,
            rev_id: 2,
            expiry: None,
            flags: 0,
            updated_at: earlier,
            deleted: false,
            source_cluster: Some("remote".to_string()),
            vbucket_id: 0,
            xattrs: std::collections::HashMap::new(),
        };

        assert_eq!(resolver.resolve(&local, &remote), ConflictWinner::Local);
    }
}
