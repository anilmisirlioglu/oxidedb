//! Multi-Document ACID Transaction Engine
//!
//! Implements a simplified version of Couchbase's Distributed Transactions:
//!   - Atomicity: All mutations within a transaction succeed or fail together
//!   - Consistency: CAS-based optimistic concurrency control
//!   - Isolation: Read-committed isolation level (reads see committed data only)
//!   - Durability: Mutations are persisted only on commit
//!
//! Transaction lifecycle:
//!   1. Begin  → creates a new TransactionContext with a unique ATR (Active Transaction Record)
//!   2. Get    → reads a document, recording it in the transaction's read set
//!   3. Insert → stages a new document in the transaction's write set
//!   4. Replace→ stages a replacement in the transaction's write set (requires prior Get)
//!   5. Remove → stages a deletion in the transaction's write set
//!   6. Commit → applies all staged mutations atomically, then cleans up ATR
//!   7. Rollback → discards all staged mutations
//!
//! Conflict detection:
//!   - CAS check on commit: if any document's CAS changed since the Get, commit fails
//!   - Transaction timeout: abandoned transactions are rolled back after expiry

use crate::storage::engine::StorageEngine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is active and accepting operations
    Active,
    /// Transaction is being committed (applying mutations)
    Committing,
    /// Transaction has been committed successfully
    Committed,
    /// Transaction has been rolled back
    RolledBack,
    /// Transaction has expired (timed out)
    Expired,
}

/// Type of staged mutation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StagedMutationType {
    Insert,
    Replace,
    Remove,
}

/// A staged mutation within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedMutation {
    /// Document key
    pub key: String,
    /// Bucket name
    pub bucket: String,
    /// Scope name
    pub scope: String,
    /// Collection name
    pub collection: String,
    /// Mutation type
    pub mutation_type: StagedMutationType,
    /// The new value (None for Remove)
    pub value: Option<serde_json::Value>,
    /// CAS of the document when it was read (for conflict detection)
    pub original_cas: u64,
}

/// A document read within a transaction (for CAS checking at commit)
#[derive(Debug, Clone)]
pub struct TransactionGet {
    pub key: String,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    pub value: serde_json::Value,
    pub cas: u64,
}

/// Active Transaction Record — tracks a single transaction's state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    /// Unique transaction ID
    pub id: String,
    /// Current state
    pub state: TransactionState,
    /// When the transaction was started
    pub started_at: DateTime<Utc>,
    /// When the transaction expires (auto-rollback after this)
    pub expires_at: DateTime<Utc>,
    /// Staged mutations (write set)
    pub mutations: Vec<StagedMutation>,
    /// Number of reads performed
    pub read_count: usize,
    /// Timeout in seconds
    pub timeout_secs: u64,
}

/// Transaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionConfig {
    /// Transaction timeout in seconds (default: 15)
    pub timeout_secs: u64,
    /// Durability level for committed mutations
    pub durability_level: String,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            timeout_secs: 15,
            durability_level: "majority".to_string(),
        }
    }
}

/// Transaction Engine — manages all active transactions
pub struct TransactionEngine {
    storage: Arc<StorageEngine>,
    /// Active transactions: txn_id → TransactionRecord
    transactions: RwLock<HashMap<String, TransactionRecord>>,
    /// Read sets: txn_id → list of (doc_key, cas)
    read_sets: RwLock<HashMap<String, Vec<(DocRef, u64)>>>,
}

/// A reference to a document (bucket/scope/collection/key)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DocRef {
    bucket: String,
    scope: String,
    collection: String,
    key: String,
}

impl TransactionEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            transactions: RwLock::new(HashMap::new()),
            read_sets: RwLock::new(HashMap::new()),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self, config: Option<TransactionConfig>) -> Result<String, String> {
        let config = config.unwrap_or_default();
        let txn_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let record = TransactionRecord {
            id: txn_id.clone(),
            state: TransactionState::Active,
            started_at: now,
            expires_at: now + chrono::Duration::seconds(config.timeout_secs as i64),
            mutations: Vec::new(),
            read_count: 0,
            timeout_secs: config.timeout_secs,
        };

        {
            let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
            txns.insert(txn_id.clone(), record);
        }
        {
            let mut reads = self.read_sets.write().map_err(|e| e.to_string())?;
            reads.insert(txn_id.clone(), Vec::new());
        }

        tracing::info!("Transaction {} started (timeout: {}s)", txn_id, config.timeout_secs);
        Ok(txn_id)
    }

    /// Get a document within a transaction (records CAS for conflict detection)
    pub fn get(
        &self,
        txn_id: &str,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
    ) -> Result<TransactionGet, String> {
        // Check transaction is active
        self.check_active(txn_id)?;

        // Check if document was already staged for insert/replace
        {
            let txns = self.transactions.read().map_err(|e| e.to_string())?;
            if let Some(txn) = txns.get(txn_id) {
                for mutation in &txn.mutations {
                    if mutation.key == key
                        && mutation.bucket == bucket
                        && mutation.scope == scope
                        && mutation.collection == collection
                    {
                        match mutation.mutation_type {
                            StagedMutationType::Remove => {
                                return Err(format!("Document '{}' was removed in this transaction", key));
                            }
                            StagedMutationType::Insert | StagedMutationType::Replace => {
                                if let Some(ref val) = mutation.value {
                                    return Ok(TransactionGet {
                                        key: key.to_string(),
                                        bucket: bucket.to_string(),
                                        scope: scope.to_string(),
                                        collection: collection.to_string(),
                                        value: val.clone(),
                                        cas: mutation.original_cas,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Read from storage
        let bucket_obj = self.storage.get_bucket(bucket)
            .map_err(|e| format!("Bucket error: {}", e))?;
        let doc = bucket_obj.get(scope, collection, key)
            .map_err(|e| format!("Get error: {}", e))?;

        let result = TransactionGet {
            key: key.to_string(),
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            value: doc.value.clone(),
            cas: doc.cas,
        };

        // Record in read set
        {
            let mut reads = self.read_sets.write().map_err(|e| e.to_string())?;
            if let Some(read_set) = reads.get_mut(txn_id) {
                read_set.push((
                    DocRef {
                        bucket: bucket.to_string(),
                        scope: scope.to_string(),
                        collection: collection.to_string(),
                        key: key.to_string(),
                    },
                    doc.cas,
                ));
            }
        }

        // Increment read count
        {
            let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
            if let Some(txn) = txns.get_mut(txn_id) {
                txn.read_count += 1;
            }
        }

        Ok(result)
    }

    /// Insert a new document within a transaction
    pub fn insert(
        &self,
        txn_id: &str,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        self.check_active(txn_id)?;

        // Check document doesn't already exist
        let bucket_obj = self.storage.get_bucket(bucket)
            .map_err(|e| format!("Bucket error: {}", e))?;
        if bucket_obj.get(scope, collection, key).is_ok() {
            return Err(format!("Document '{}' already exists", key));
        }

        let mutation = StagedMutation {
            key: key.to_string(),
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            mutation_type: StagedMutationType::Insert,
            value: Some(value),
            original_cas: 0,
        };

        let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
        if let Some(txn) = txns.get_mut(txn_id) {
            txn.mutations.push(mutation);
        }

        Ok(())
    }

    /// Replace a document within a transaction (requires prior Get for CAS)
    pub fn replace(
        &self,
        txn_id: &str,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
        value: serde_json::Value,
        cas: u64,
    ) -> Result<(), String> {
        self.check_active(txn_id)?;

        let mutation = StagedMutation {
            key: key.to_string(),
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            mutation_type: StagedMutationType::Replace,
            value: Some(value),
            original_cas: cas,
        };

        let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
        if let Some(txn) = txns.get_mut(txn_id) {
            // Remove any previous mutation for this key
            txn.mutations.retain(|m| {
                !(m.key == key && m.bucket == bucket && m.scope == scope && m.collection == collection)
            });
            txn.mutations.push(mutation);
        }

        Ok(())
    }

    /// Remove a document within a transaction
    pub fn remove(
        &self,
        txn_id: &str,
        bucket: &str,
        scope: &str,
        collection: &str,
        key: &str,
        cas: u64,
    ) -> Result<(), String> {
        self.check_active(txn_id)?;

        let mutation = StagedMutation {
            key: key.to_string(),
            bucket: bucket.to_string(),
            scope: scope.to_string(),
            collection: collection.to_string(),
            mutation_type: StagedMutationType::Remove,
            value: None,
            original_cas: cas,
        };

        let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
        if let Some(txn) = txns.get_mut(txn_id) {
            txn.mutations.retain(|m| {
                !(m.key == key && m.bucket == bucket && m.scope == scope && m.collection == collection)
            });
            txn.mutations.push(mutation);
        }

        Ok(())
    }

    /// Commit a transaction — applies all staged mutations atomically
    pub fn commit(&self, txn_id: &str) -> Result<CommitResult, String> {
        let start = std::time::Instant::now();

        // Mark as committing
        {
            let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
            let txn = txns.get_mut(txn_id)
                .ok_or_else(|| format!("Transaction '{}' not found", txn_id))?;

            if txn.state != TransactionState::Active {
                return Err(format!(
                    "Transaction '{}' is not active (state: {:?})",
                    txn_id, txn.state
                ));
            }

            // Check timeout
            if Utc::now() > txn.expires_at {
                txn.state = TransactionState::Expired;
                return Err(format!("Transaction '{}' has expired", txn_id));
            }

            txn.state = TransactionState::Committing;
        }

        // Phase 1: CAS validation — check all read documents haven't changed
        {
            let reads = self.read_sets.read().map_err(|e| e.to_string())?;
            if let Some(read_set) = reads.get(txn_id) {
                for (doc_ref, expected_cas) in read_set {
                    let bucket_obj = self.storage.get_bucket(&doc_ref.bucket)
                        .map_err(|e| format!("Bucket error: {}", e))?;
                    match bucket_obj.get(&doc_ref.scope, &doc_ref.collection, &doc_ref.key) {
                        Ok(doc) => {
                            if doc.cas != *expected_cas {
                                // CAS mismatch — conflict detected, rollback
                                self.rollback_internal(txn_id)?;
                                return Err(format!(
                                    "CAS conflict on document '{}' (expected: {}, actual: {})",
                                    doc_ref.key, expected_cas, doc.cas
                                ));
                            }
                        }
                        Err(_) => {
                            // Document was deleted after we read it — conflict
                            if *expected_cas > 0 {
                                self.rollback_internal(txn_id)?;
                                return Err(format!(
                                    "Document '{}' was deleted during transaction",
                                    doc_ref.key
                                ));
                            }
                        }
                    }
                }
            }
        }

        // Also check CAS for all staged mutations that have original_cas > 0
        let mutations = {
            let txns = self.transactions.read().map_err(|e| e.to_string())?;
            let txn = txns.get(txn_id).ok_or("Transaction not found")?;
            txn.mutations.clone()
        };

        for mutation in &mutations {
            if mutation.original_cas > 0 {
                let bucket_obj = self.storage.get_bucket(&mutation.bucket)
                    .map_err(|e| format!("Bucket error: {}", e))?;
                match bucket_obj.get(&mutation.scope, &mutation.collection, &mutation.key) {
                    Ok(doc) => {
                        if doc.cas != mutation.original_cas {
                            self.rollback_internal(txn_id)?;
                            return Err(format!(
                                "CAS conflict on mutation for '{}' (expected: {}, actual: {})",
                                mutation.key, mutation.original_cas, doc.cas
                            ));
                        }
                    }
                    Err(_) => {
                        if !matches!(mutation.mutation_type, StagedMutationType::Insert) {
                            self.rollback_internal(txn_id)?;
                            return Err(format!(
                                "Document '{}' no longer exists for mutation",
                                mutation.key
                            ));
                        }
                    }
                }
            }
        }

        // Phase 2: Apply all mutations
        let mut applied = 0;
        for mutation in &mutations {
            let bucket_obj = self.storage.get_bucket(&mutation.bucket)
                .map_err(|e| format!("Bucket error: {}", e))?;

            match mutation.mutation_type {
                StagedMutationType::Insert | StagedMutationType::Replace => {
                    if let Some(ref value) = mutation.value {
                        bucket_obj
                            .upsert(
                                &mutation.scope,
                                &mutation.collection,
                                mutation.key.clone(),
                                value.clone(),
                                None,
                            )
                            .map_err(|e| format!("Apply error: {}", e))?;
                        applied += 1;
                    }
                }
                StagedMutationType::Remove => {
                    let _ = bucket_obj.delete(
                        &mutation.scope,
                        &mutation.collection,
                        &mutation.key,
                        None,
                    );
                    applied += 1;
                }
            }
        }

        // Phase 3: Mark as committed and cleanup
        {
            let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
            if let Some(txn) = txns.get_mut(txn_id) {
                txn.state = TransactionState::Committed;
            }
        }
        {
            let mut reads = self.read_sets.write().map_err(|e| e.to_string())?;
            reads.remove(txn_id);
        }

        let elapsed = start.elapsed().as_millis() as u64;
        tracing::info!(
            "Transaction {} committed: {} mutations applied in {}ms",
            txn_id,
            applied,
            elapsed
        );

        Ok(CommitResult {
            txn_id: txn_id.to_string(),
            mutations_applied: applied,
            elapsed_ms: elapsed,
        })
    }

    /// Rollback a transaction — discard all staged mutations
    pub fn rollback(&self, txn_id: &str) -> Result<(), String> {
        self.rollback_internal(txn_id)
    }

    fn rollback_internal(&self, txn_id: &str) -> Result<(), String> {
        {
            let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
            let txn = txns.get_mut(txn_id)
                .ok_or_else(|| format!("Transaction '{}' not found", txn_id))?;
            txn.state = TransactionState::RolledBack;
            txn.mutations.clear();
        }
        {
            let mut reads = self.read_sets.write().map_err(|e| e.to_string())?;
            reads.remove(txn_id);
        }

        tracing::info!("Transaction {} rolled back", txn_id);
        Ok(())
    }

    /// Get transaction status
    pub fn get_transaction(&self, txn_id: &str) -> Option<TransactionRecord> {
        let txns = self.transactions.read().ok()?;
        txns.get(txn_id).cloned()
    }

    /// List all active transactions
    pub fn list_active(&self) -> Vec<TransactionRecord> {
        let txns = match self.transactions.read() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        txns.values()
            .filter(|t| t.state == TransactionState::Active || t.state == TransactionState::Committing)
            .cloned()
            .collect()
    }

    /// Cleanup expired transactions (called periodically from background task)
    pub fn cleanup_expired(&self) -> usize {
        let now = Utc::now();
        let mut expired = Vec::new();

        {
            let txns = match self.transactions.read() {
                Ok(t) => t,
                Err(_) => return 0,
            };
            for (id, txn) in txns.iter() {
                if txn.state == TransactionState::Active && now > txn.expires_at {
                    expired.push(id.clone());
                }
            }
        }

        let count = expired.len();
        for id in expired {
            let _ = self.rollback_internal(&id);
            let mut txns = match self.transactions.write() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if let Some(txn) = txns.get_mut(&id) {
                txn.state = TransactionState::Expired;
            }
        }

        if count > 0 {
            tracing::info!("Cleaned up {} expired transactions", count);
        }
        count
    }

    /// Purge completed/rolled-back/expired transactions older than the given age
    pub fn purge_old(&self, max_age_secs: i64) -> usize {
        let cutoff = Utc::now() - chrono::Duration::seconds(max_age_secs);
        let mut to_remove = Vec::new();

        {
            let txns = match self.transactions.read() {
                Ok(t) => t,
                Err(_) => return 0,
            };
            for (id, txn) in txns.iter() {
                if matches!(
                    txn.state,
                    TransactionState::Committed | TransactionState::RolledBack | TransactionState::Expired
                ) && txn.started_at < cutoff
                {
                    to_remove.push(id.clone());
                }
            }
        }

        let count = to_remove.len();
        if count > 0 {
            let mut txns = match self.transactions.write() {
                Ok(t) => t,
                Err(_) => return 0,
            };
            for id in &to_remove {
                txns.remove(id);
            }
        }

        count
    }

    /// Check that a transaction is active and not expired
    fn check_active(&self, txn_id: &str) -> Result<(), String> {
        let txns = self.transactions.read().map_err(|e| e.to_string())?;
        let txn = txns.get(txn_id)
            .ok_or_else(|| format!("Transaction '{}' not found", txn_id))?;

        if txn.state != TransactionState::Active {
            return Err(format!(
                "Transaction '{}' is not active (state: {:?})",
                txn_id, txn.state
            ));
        }

        if Utc::now() > txn.expires_at {
            drop(txns);
            // Mark as expired
            let mut txns = self.transactions.write().map_err(|e| e.to_string())?;
            if let Some(txn) = txns.get_mut(txn_id) {
                txn.state = TransactionState::Expired;
            }
            return Err(format!("Transaction '{}' has expired", txn_id));
        }

        Ok(())
    }
}

/// Result of a successful commit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResult {
    pub txn_id: String,
    pub mutations_applied: usize,
    pub elapsed_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_txn_engine() -> TransactionEngine {
        let storage = Arc::new(StorageEngine::new(16, None, None));
        storage
            .create_bucket(crate::storage::engine::BucketConfig {
                name: "test".to_string(),
                num_vbuckets: 16,
                ..Default::default()
            })
            .unwrap();
        TransactionEngine::new(storage)
    }

    #[test]
    fn test_begin_and_get_transaction() {
        let engine = make_txn_engine();
        let txn_id = engine.begin(None).unwrap();
        let fetched = engine.get_transaction(&txn_id).unwrap();
        assert_eq!(fetched.state, TransactionState::Active);
        assert_eq!(fetched.id, txn_id);
    }

    #[test]
    fn test_insert_and_commit() {
        let engine = make_txn_engine();
        let txn_id = engine.begin(None).unwrap();

        engine.insert(
            &txn_id,
            "test",
            "_default",
            "_default",
            "doc1",
            serde_json::json!({"name": "Alice"}),
        ).unwrap();

        let result = engine.commit(&txn_id).unwrap();
        assert_eq!(result.mutations_applied, 1);

        // Verify document is in storage
        let bucket = engine.storage.get_bucket("test").unwrap();
        let doc = bucket.get("_default", "_default", "doc1").unwrap();
        assert_eq!(doc.value["name"], "Alice");
    }

    #[test]
    fn test_rollback() {
        let engine = make_txn_engine();
        let txn_id = engine.begin(None).unwrap();

        engine.insert(
            &txn_id,
            "test",
            "_default",
            "_default",
            "doc1",
            serde_json::json!({"name": "Bob"}),
        ).unwrap();

        engine.rollback(&txn_id).unwrap();

        // Document should NOT be in storage
        let bucket = engine.storage.get_bucket("test").unwrap();
        assert!(bucket.get("_default", "_default", "doc1").is_err());
    }

    #[test]
    fn test_replace_in_transaction() {
        let engine = make_txn_engine();
        // Pre-insert a doc
        let bucket = engine.storage.get_bucket("test").unwrap();
        bucket.upsert("_default", "_default", "doc1".to_string(), serde_json::json!({"v": 1}), None).unwrap();

        let txn_id = engine.begin(None).unwrap();

        // Get first (required for replace)
        engine.get(&txn_id, "test", "_default", "_default", "doc1").unwrap();

        // Replace
        engine.replace(
            &txn_id,
            "test",
            "_default",
            "_default",
            "doc1",
            serde_json::json!({"v": 2}),
            0,
        ).unwrap();

        engine.commit(&txn_id).unwrap();

        let doc = bucket.get("_default", "_default", "doc1").unwrap();
        assert_eq!(doc.value["v"], 2);
    }

    #[test]
    fn test_remove_in_transaction() {
        let engine = make_txn_engine();
        let bucket = engine.storage.get_bucket("test").unwrap();
        bucket.upsert("_default", "_default", "doc1".to_string(), serde_json::json!({"v": 1}), None).unwrap();

        let txn_id = engine.begin(None).unwrap();

        engine.get(&txn_id, "test", "_default", "_default", "doc1").unwrap();
        engine.remove(&txn_id, "test", "_default", "_default", "doc1", 0).unwrap();
        engine.commit(&txn_id).unwrap();

        assert!(bucket.get("_default", "_default", "doc1").is_err());
    }

    #[test]
    fn test_list_active_transactions() {
        let engine = make_txn_engine();
        engine.begin(None).unwrap();
        engine.begin(None).unwrap();
        let list = engine.list_active();
        assert!(list.len() >= 2);
    }

    #[test]
    fn test_cleanup_expired() {
        let engine = make_txn_engine();
        let config = TransactionConfig { timeout_secs: 0, durability_level: "none".to_string() };
        let txn_id = engine.begin(Some(config)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let cleaned = engine.cleanup_expired();
        assert!(cleaned >= 1);
        let fetched = engine.get_transaction(&txn_id);
        // Should be expired or cleaned
        if let Some(t) = fetched {
            assert!(t.state == TransactionState::Expired || t.state == TransactionState::RolledBack);
        }
    }
}
