use crate::cluster::partition::VBucketData;
use crate::error::{NosqlError, Result};
use crate::storage::document::{Document, Mutation};
use crate::storage::persistence::PersistenceManager;
use crate::storage::vbucket::{hash_to_vbucket, VBucket, VBucketState};
use crate::storage::wal::WriteBufferConfig;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

/// Bucket types (similar to Couchbase)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketType {
    /// Persistent bucket (Couchbase type)
    Couchbase,
    /// Memory-only bucket
    Ephemeral,
    /// Simple key-value (Memcached compatible)
    Memcached,
}

/// Bucket configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketConfig {
    pub name: String,
    pub bucket_type: BucketType,
    pub ram_quota_mb: u64,
    pub num_replicas: u8,
    pub num_vbuckets: u16,
    pub flush_enabled: bool,
    pub conflict_resolution: ConflictResolutionType,
    pub max_ttl: Option<u64>,
    pub compression_mode: CompressionMode,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolutionType {
    /// Last write wins based on revision sequence
    SequenceNumber,
    /// Last write wins based on timestamp
    Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionMode {
    Off,
    Passive,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    ValueOnly,
    FullEviction,
    NoEviction,
    NotRecentlyUsed,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            bucket_type: BucketType::Couchbase,
            ram_quota_mb: 256,
            num_replicas: 1,
            num_vbuckets: 1024,
            flush_enabled: false,
            conflict_resolution: ConflictResolutionType::SequenceNumber,
            max_ttl: None,
            compression_mode: CompressionMode::Passive,
            eviction_policy: EvictionPolicy::ValueOnly,
        }
    }
}

/// Scope within a bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeInfo {
    pub name: String,
    pub collections: Vec<String>,
}

/// A scope containing collections
pub struct Scope {
    pub name: String,
    pub collections: DashMap<String, Collection>,
}

impl Scope {
    pub fn new(name: String) -> Self {
        Self {
            name,
            collections: DashMap::new(),
        }
    }

    pub fn with_default_collection(name: String) -> Self {
        let scope = Self::new(name);
        scope.collections.insert(
            "_default".to_string(),
            Collection::new("_default".to_string()),
        );
        scope
    }
}

/// A collection within a scope - maps to vBuckets
pub struct Collection {
    pub name: String,
    pub max_ttl: Option<u64>,
}

impl Collection {
    pub fn new(name: String) -> Self {
        Self {
            name,
            max_ttl: None,
        }
    }
}

/// A complete bucket with vBuckets, scopes, and collections
pub struct Bucket {
    pub config: BucketConfig,
    pub scopes: DashMap<String, Scope>,
    pub vbuckets: Vec<RwLock<VBucket>>,
}

impl Bucket {
    pub fn new(config: BucketConfig) -> Self {
        let num_vbuckets = config.num_vbuckets;
        let mut vbuckets = Vec::with_capacity(num_vbuckets as usize);
        for i in 0..num_vbuckets {
            vbuckets.push(RwLock::new(VBucket::new(i)));
        }

        let scopes = DashMap::new();
        // Create default scope with default collection
        scopes.insert(
            "_default".to_string(),
            Scope::with_default_collection("_default".to_string()),
        );

        Self {
            config,
            scopes,
            vbuckets,
        }
    }

    /// Get the vBucket for a given key
    pub fn get_vbucket_id(&self, key: &str) -> u16 {
        hash_to_vbucket(key, self.config.num_vbuckets)
    }

    /// Validate scope and collection exist (public version for API layer)
    pub fn validate_path_public(&self, scope: &str, collection: &str) -> Result<()> {
        self.validate_path(scope, collection)
    }

    /// Validate scope and collection exist
    fn validate_path(&self, scope: &str, collection: &str) -> Result<()> {
        let scope_ref = self
            .scopes
            .get(scope)
            .ok_or_else(|| NosqlError::ScopeNotFound(scope.to_string()))?;
        if !scope_ref.collections.contains_key(collection) {
            return Err(NosqlError::CollectionNotFound(collection.to_string()));
        }
        Ok(())
    }

    /// Check if memory quota would be exceeded.
    /// Tries eviction first based on the bucket's eviction policy.
    /// Returns Ok(()) if within quota (possibly after eviction), Err if would exceed.
    fn check_memory_quota(&self) -> Result<()> {
        let quota_bytes = (self.config.ram_quota_mb as usize) * 1024 * 1024;
        if quota_bytes == 0 {
            return Ok(()); // 0 means unlimited
        }

        let current = self.total_size_bytes();
        if current < quota_bytes {
            return Ok(());
        }

        // Try eviction based on policy
        match self.config.eviction_policy {
            EvictionPolicy::NoEviction => {
                // No eviction allowed — reject write
            }
            EvictionPolicy::ValueOnly | EvictionPolicy::FullEviction | EvictionPolicy::NotRecentlyUsed => {
                // Target: free at least 10% of quota
                let target = quota_bytes * 90 / 100;
                let evicted = self.run_eviction(target);
                if evicted > 0 {
                    tracing::info!(
                        "Evicted {} items from bucket '{}' (policy: {:?})",
                        evicted,
                        self.config.name,
                        self.config.eviction_policy
                    );
                }
                // Recheck after eviction
                let new_size = self.total_size_bytes();
                if new_size < quota_bytes {
                    return Ok(());
                }
            }
        }

        let current = self.total_size_bytes();
        Err(NosqlError::MemoryQuotaExceeded(
            self.config.name.clone(),
            (current / (1024 * 1024)) as u64,
            self.config.ram_quota_mb,
        ))
    }

    /// Run eviction across all vBuckets, returns total number of items evicted
    pub fn run_eviction(&self, target_bytes_per_vb: usize) -> usize {
        let per_vb_target = target_bytes_per_vb / self.config.num_vbuckets.max(1) as usize;
        let mut total = 0;
        for vb in &self.vbuckets {
            if let Ok(mut vb) = vb.write() {
                total += vb.run_eviction(&self.config.eviction_policy, per_vb_target);
            }
        }
        total
    }

    /// Get eviction statistics
    #[allow(dead_code)]
    pub fn eviction_stats(&self) -> EvictionStats {
        let mut total_evicted = 0;
        for vb in &self.vbuckets {
            if let Ok(vb) = vb.read() {
                total_evicted += vb.evicted_count();
            }
        }
        EvictionStats {
            policy: self.config.eviction_policy,
            total_evicted_items: total_evicted,
            quota_bytes: (self.config.ram_quota_mb as usize) * 1024 * 1024,
            used_bytes: self.total_size_bytes(),
        }
    }

    /// Get a document
    pub fn get(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize]
            .read()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        match vb.get(key) {
            Ok(doc) => Ok(doc.clone()),
            Err(NosqlError::DocumentNotFound(_)) => {
                // Fallback: scan all vBuckets in case the document ended up in
                // a different vBucket (e.g. after persistence recovery with
                // different num_vbuckets, or data migration).
                drop(vb);
                self.get_fallback(key)
            }
            Err(e) => Err(e),
        }
    }

    /// Fallback document lookup: scan ALL vBuckets for the key.
    /// This handles cases where a document ended up in a vBucket that
    /// doesn't match the current hash (e.g. after num_vbuckets change).
    fn get_fallback(&self, key: &str) -> Result<Document> {
        for vb in &self.vbuckets {
            if let Ok(vb) = vb.read() {
                if let Ok(doc) = vb.get(key) {
                    tracing::warn!(
                        "Document '{}' found via fallback scan in vBucket {} (expected vBucket {})",
                        key, doc.vbucket_id, self.get_vbucket_id(key)
                    );
                    return Ok(doc.clone());
                }
            }
        }
        Err(NosqlError::DocumentNotFound(key.to_string()))
    }

    /// Upsert a document
    pub fn upsert(
        &self,
        scope: &str,
        collection: &str,
        key: String,
        value: serde_json::Value,
        expiry: Option<u64>,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        self.check_memory_quota()?;
        let vb_id = self.get_vbucket_id(&key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;

        let mut doc = vb.upsert(key, value)?;

        // Apply TTL — set on both the returned clone AND the stored document
        if let Some(exp) = expiry.or(self.config.max_ttl) {
            let expiry_time = chrono::Utc::now() + chrono::Duration::seconds(exp as i64);
            doc.expiry = Some(expiry_time);
            vb.set_expiry(&doc.key, Some(expiry_time));
        }

        Ok(doc)
    }

    /// Replace a document with CAS check
    pub fn replace(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        self.check_memory_quota()?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.replace(key, value, cas)
    }

    /// Delete a document
    pub fn delete(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        match vb.delete(key, cas) {
            Ok(doc) => Ok(doc),
            Err(NosqlError::DocumentNotFound(_)) => {
                // Fallback: scan all vBuckets for the key
                drop(vb);
                self.delete_fallback(key, cas)
            }
            Err(e) => Err(e),
        }
    }

    /// Fallback document delete: scan ALL vBuckets for the key.
    fn delete_fallback(&self, key: &str, cas: Option<u64>) -> Result<Document> {
        for vb in &self.vbuckets {
            if let Ok(vb_read) = vb.read() {
                if vb_read.get(key).is_ok() {
                    drop(vb_read);
                    if let Ok(mut vb_write) = vb.write() {
                        tracing::warn!(
                            "Document '{}' deleted via fallback scan in vBucket {} (expected vBucket {})",
                            key, vb_write.id, self.get_vbucket_id(key)
                        );
                        return vb_write.delete(key, cas);
                    }
                }
            }
        }
        Err(NosqlError::DocumentNotFound(key.to_string()))
    }

    /// Touch a document (update expiry)
    pub fn touch(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
        expiry_seconds: u64,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.touch(key, expiry_seconds)
    }

    /// Increment/Decrement a counter document
    /// If the key doesn't exist, create it with `initial` value.
    /// If the key exists, parse its value as u64 and add/subtract `delta`.
    pub fn counter(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
        delta: i64,
        initial: u64,
        expiry: Option<u64>,
        create: bool,
    ) -> Result<(Document, u64)> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;

        let current_val = match vb.get(key) {
            Ok(doc) => {
                // Try to parse existing value as number
                match &doc.value {
                    serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
                    serde_json::Value::String(s) => s.parse::<u64>().unwrap_or(0),
                    _ => 0,
                }
            }
            Err(NosqlError::DocumentNotFound(_)) => {
                if !create {
                    return Err(NosqlError::DocumentNotFound(key.to_string()));
                }
                // Will create with initial value
                initial
            }
            Err(e) => return Err(e),
        };

        let new_val = if delta >= 0 {
            current_val.wrapping_add(delta as u64)
        } else {
            current_val.saturating_sub((-delta) as u64)
        };

        // For existing docs that we're just creating, use initial directly
        let final_val = if vb.get(key).is_err() {
            initial
        } else {
            new_val
        };

        let mut doc = vb.upsert(key.to_string(), serde_json::json!(final_val))?;
        if let Some(exp) = expiry {
            doc.expiry = Some(chrono::Utc::now() + chrono::Duration::seconds(exp as i64));
        }

        Ok((doc, final_val))
    }

    /// Append data to an existing document's value
    pub fn append(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
        data: &[u8],
        cas: Option<u64>,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        self.check_memory_quota()?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;

        let existing = vb.get(key)?.clone();
        if let Some(expected_cas) = cas {
            if existing.cas != expected_cas {
                return Err(NosqlError::CasMismatch {
                    expected: expected_cas,
                    actual: existing.cas,
                });
            }
        }

        // Append to existing value
        let new_value = match &existing.value {
            serde_json::Value::String(s) => {
                let append_str = String::from_utf8_lossy(data);
                serde_json::Value::String(format!("{}{}", s, append_str))
            }
            _ => {
                // Binary append: serialize existing + new data
                let existing_bytes = serde_json::to_string(&existing.value).unwrap_or_default();
                let append_str = String::from_utf8_lossy(data);
                serde_json::Value::String(format!("{}{}", existing_bytes, append_str))
            }
        };

        vb.upsert(key.to_string(), new_value)
    }

    /// Prepend data to an existing document's value
    pub fn prepend(
        &self,
        scope: &str,
        collection: &str,
        key: &str,
        data: &[u8],
        cas: Option<u64>,
    ) -> Result<Document> {
        self.validate_path(scope, collection)?;
        self.check_memory_quota()?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;

        let existing = vb.get(key)?.clone();
        if let Some(expected_cas) = cas {
            if existing.cas != expected_cas {
                return Err(NosqlError::CasMismatch {
                    expected: expected_cas,
                    actual: existing.cas,
                });
            }
        }

        let new_value = match &existing.value {
            serde_json::Value::String(s) => {
                let prepend_str = String::from_utf8_lossy(data);
                serde_json::Value::String(format!("{}{}", prepend_str, s))
            }
            _ => {
                let existing_bytes = serde_json::to_string(&existing.value).unwrap_or_default();
                let prepend_str = String::from_utf8_lossy(data);
                serde_json::Value::String(format!("{}{}", prepend_str, existing_bytes))
            }
        };

        vb.upsert(key.to_string(), new_value)
    }

    /// Update the expiry of a stored document directly in the vBucket.
    /// Used after upsert when flags/expiry need to be set on the actual stored doc.
    #[allow(dead_code)]
    pub fn set_document_expiry(&self, key: &str, expiry: Option<chrono::DateTime<chrono::Utc>>) {
        let vb_id = self.get_vbucket_id(key);
        if let Ok(mut vb) = self.vbuckets[vb_id as usize].write() {
            vb.set_expiry(key, expiry);
        }
    }

    /// Update the flags of a stored document directly in the vBucket.
    pub fn set_document_flags(&self, key: &str, flags: u32) {
        let vb_id = self.get_vbucket_id(key);
        if let Ok(mut vb) = self.vbuckets[vb_id as usize].write() {
            vb.set_flags(key, flags);
        }
    }

    // =====================================================================
    // Exists, Lock/Unlock
    // =====================================================================

    /// Check if a document exists
    pub fn exists(&self, scope: &str, collection: &str, key: &str) -> Result<u64> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.exists(key)
    }

    /// Get and lock a document
    pub fn get_and_lock(&self, scope: &str, collection: &str, key: &str, lock_seconds: u32) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.get_and_lock(key, lock_seconds)
    }

    /// Unlock a document
    pub fn unlock(&self, scope: &str, collection: &str, key: &str, cas: u64) -> Result<()> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.unlock(key, cas)
    }

    // =====================================================================
    // Sub-Document Operations
    // =====================================================================

    pub fn subdoc_get(&self, scope: &str, collection: &str, key: &str, path: &str) -> Result<serde_json::Value> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_get(key, path)
    }

    pub fn subdoc_exists(&self, scope: &str, collection: &str, key: &str, path: &str) -> Result<bool> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_exists(key, path)
    }

    pub fn subdoc_get_count(&self, scope: &str, collection: &str, key: &str, path: &str) -> Result<usize> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_get_count(key, path)
    }

    pub fn subdoc_dict_upsert(&self, scope: &str, collection: &str, key: &str, path: &str, value: serde_json::Value, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_dict_upsert(key, path, value, cas)
    }

    pub fn subdoc_dict_add(&self, scope: &str, collection: &str, key: &str, path: &str, value: serde_json::Value, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_dict_add(key, path, value, cas)
    }

    pub fn subdoc_delete(&self, scope: &str, collection: &str, key: &str, path: &str, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_delete(key, path, cas)
    }

    pub fn subdoc_replace(&self, scope: &str, collection: &str, key: &str, path: &str, value: serde_json::Value, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_replace(key, path, value, cas)
    }

    pub fn subdoc_array_push_last(&self, scope: &str, collection: &str, key: &str, path: &str, value: serde_json::Value, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_array_push_last(key, path, value, cas)
    }

    pub fn subdoc_array_push_first(&self, scope: &str, collection: &str, key: &str, path: &str, value: serde_json::Value, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_array_push_first(key, path, value, cas)
    }

    pub fn subdoc_counter(&self, scope: &str, collection: &str, key: &str, path: &str, delta: i64, cas: Option<u64>) -> Result<(Document, i64)> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.subdoc_counter(key, path, delta, cas)
    }

    // =====================================================================
    // Extended Attributes (XATTRs)
    // =====================================================================

    pub fn xattr_get(&self, scope: &str, collection: &str, key: &str, xattr_path: &str) -> Result<serde_json::Value> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.xattr_get(key, xattr_path)
    }

    pub fn xattr_exists(&self, scope: &str, collection: &str, key: &str, xattr_path: &str) -> Result<bool> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.xattr_exists(key, xattr_path)
    }

    pub fn xattr_upsert(&self, scope: &str, collection: &str, key: &str, xattr_path: &str, value: serde_json::Value, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.xattr_upsert(key, xattr_path, value, cas)
    }

    pub fn xattr_remove(&self, scope: &str, collection: &str, key: &str, xattr_path: &str, cas: Option<u64>) -> Result<Document> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let mut vb = self.vbuckets[vb_id as usize].write().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.xattr_remove(key, xattr_path, cas)
    }

    pub fn xattr_list(&self, scope: &str, collection: &str, key: &str) -> Result<std::collections::HashMap<String, serde_json::Value>> {
        self.validate_path(scope, collection)?;
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize].read().map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.xattr_list(key)
    }

    /// Purge tombstones across all vBuckets
    #[allow(dead_code)]
    pub fn purge_tombstones(&self, max_age: chrono::Duration) -> usize {
        let mut total = 0;
        for vb in &self.vbuckets {
            if let Ok(mut vb) = vb.write() {
                total += vb.purge_tombstones(max_age);
            }
        }
        total
    }

    /// Get a document by key (return the Document struct directly)
    #[allow(dead_code)]
    pub fn get_document(&self, key: &str) -> Result<Option<Document>> {
        let vb_id = self.get_vbucket_id(key);
        let vb = self.vbuckets[vb_id as usize]
            .read()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        match vb.get(key) {
            Ok(doc) => Ok(Some(doc.clone())),
            Err(NosqlError::DocumentNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get mutations since sequence number for a vBucket (XDCR)
    pub fn get_mutations_since(&self, vbucket_id: u16, since_seq_no: u64) -> Result<Vec<Mutation>> {
        if vbucket_id >= self.config.num_vbuckets {
            return Err(NosqlError::VBucketNotFound(vbucket_id));
        }
        let vb = self.vbuckets[vbucket_id as usize]
            .read()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        Ok(vb.get_mutations_since(since_seq_no))
    }

    /// Apply a mutation from XDCR
    pub fn apply_mutation(&self, mutation: &Mutation) -> Result<()> {
        let vb_id = hash_to_vbucket(&mutation.key, self.config.num_vbuckets);
        let mut vb = self.vbuckets[vb_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.apply_mutation(mutation)
    }

    /// Get high sequence numbers for all vBuckets
    pub fn get_high_seq_nos(&self) -> Vec<(u16, u64)> {
        self.vbuckets
            .iter()
            .enumerate()
            .filter_map(|(i, vb)| {
                vb.read().ok().map(|vb| (i as u16, vb.high_seq_no))
            })
            .collect()
    }

    /// Purge expired documents across all vBuckets
    pub fn purge_expired(&self) -> usize {
        let mut total = 0;
        for vb in &self.vbuckets {
            if let Ok(mut vb) = vb.write() {
                total += vb.purge_expired().len();
            }
        }
        total
    }

    /// Get total document count
    pub fn document_count(&self) -> usize {
        self.vbuckets
            .iter()
            .filter_map(|vb| vb.read().ok())
            .map(|vb| vb.document_count())
            .sum()
    }

    /// Get total size in bytes
    pub fn total_size_bytes(&self) -> usize {
        self.vbuckets
            .iter()
            .filter_map(|vb| vb.read().ok())
            .map(|vb| vb.size_bytes())
            .sum()
    }

    /// Create a new scope
    pub fn create_scope(&self, name: String) -> Result<()> {
        if self.scopes.contains_key(&name) {
            return Err(NosqlError::ScopeAlreadyExists(name));
        }
        self.scopes
            .insert(name.clone(), Scope::with_default_collection(name));
        Ok(())
    }

    /// Delete a scope
    pub fn delete_scope(&self, name: &str) -> Result<()> {
        if name == "_default" {
            return Err(NosqlError::InvalidRequest(
                "Cannot delete _default scope".to_string(),
            ));
        }
        self.scopes
            .remove(name)
            .ok_or_else(|| NosqlError::ScopeNotFound(name.to_string()))?;
        Ok(())
    }

    /// Create a collection in a scope
    pub fn create_collection(&self, scope: &str, name: String) -> Result<()> {
        let scope_ref = self
            .scopes
            .get(scope)
            .ok_or_else(|| NosqlError::ScopeNotFound(scope.to_string()))?;
        if scope_ref.collections.contains_key(&name) {
            return Err(NosqlError::CollectionAlreadyExists(name));
        }
        scope_ref
            .collections
            .insert(name.clone(), Collection::new(name));
        Ok(())
    }

    /// Delete a collection from a scope
    pub fn delete_collection(&self, scope: &str, name: &str) -> Result<()> {
        if name == "_default" && scope == "_default" {
            return Err(NosqlError::InvalidRequest(
                "Cannot delete _default collection from _default scope".to_string(),
            ));
        }
        let scope_ref = self
            .scopes
            .get(scope)
            .ok_or_else(|| NosqlError::ScopeNotFound(scope.to_string()))?;
        scope_ref
            .collections
            .remove(name)
            .ok_or_else(|| NosqlError::CollectionNotFound(name.to_string()))?;
        Ok(())
    }

    /// List scopes
    pub fn list_scopes(&self) -> Vec<ScopeInfo> {
        self.scopes
            .iter()
            .map(|entry| {
                let scope = entry.value();
                ScopeInfo {
                    name: scope.name.clone(),
                    collections: scope
                        .collections
                        .iter()
                        .map(|c| c.key().clone())
                        .collect(),
                }
            })
            .collect()
    }

    /// Get all documents matching a basic filter (for query engine)
    pub fn scan_all_documents(&self) -> Vec<Document> {
        let mut docs = Vec::new();
        for vb in &self.vbuckets {
            if let Ok(vb) = vb.read() {
                for doc in vb.get_all_documents() {
                    docs.push(doc.clone());
                }
            }
        }
        docs
    }

    /// Flush all data from the bucket
    pub fn flush(&self) -> Result<()> {
        if !self.config.flush_enabled {
            return Err(NosqlError::InvalidRequest(
                "Flush is not enabled for this bucket".to_string(),
            ));
        }
        for vb in &self.vbuckets {
            if let Ok(mut vb) = vb.write() {
                *vb = VBucket::new(vb.id);
            }
        }
        info!("Bucket '{}' flushed", self.config.name);
        Ok(())
    }

    // =========================================================================
    // Multi-node partitioning: vBucket data export/import
    // =========================================================================

    /// Export a vBucket's data for transfer to another node
    pub fn export_vbucket(&self, vbucket_id: u16) -> Result<VBucketData> {
        if vbucket_id >= self.config.num_vbuckets {
            return Err(NosqlError::VBucketNotFound(vbucket_id));
        }
        let vb = self.vbuckets[vbucket_id as usize]
            .read()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        Ok(VBucketData {
            vbucket_id,
            high_seq_no: vb.high_seq_no,
            documents: vb.get_all_documents().into_iter().cloned().collect(),
        })
    }

    /// Import a vBucket's data received from another node
    pub fn import_vbucket(&self, data: VBucketData) -> Result<()> {
        if data.vbucket_id >= self.config.num_vbuckets {
            return Err(NosqlError::VBucketNotFound(data.vbucket_id));
        }
        let mut vb = self.vbuckets[data.vbucket_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;

        // Clear existing data and import
        *vb = VBucket::new(data.vbucket_id);
        vb.state = VBucketState::Active;
        vb.high_seq_no = data.high_seq_no;
        for doc in data.documents {
            let mutation = doc.to_mutation();
            let _ = vb.apply_mutation(&mutation);
        }
        info!(
            "Imported vBucket {} ({} seq_no: {})",
            data.vbucket_id,
            vb.document_count(),
            data.high_seq_no
        );
        Ok(())
    }

    /// Set a vBucket's state (Active, Replica, Dead)
    #[allow(dead_code)]
    pub fn set_vbucket_state(&self, vbucket_id: u16, state: VBucketState) -> Result<()> {
        if vbucket_id >= self.config.num_vbuckets {
            return Err(NosqlError::VBucketNotFound(vbucket_id));
        }
        let mut vb = self.vbuckets[vbucket_id as usize]
            .write()
            .map_err(|e| NosqlError::Internal(e.to_string()))?;
        vb.state = state;
        Ok(())
    }

    /// Get vBucket states for all vBuckets
    #[allow(dead_code)]
    pub fn get_vbucket_states(&self) -> Vec<(u16, VBucketState)> {
        self.vbuckets
            .iter()
            .enumerate()
            .filter_map(|(i, vb)| vb.read().ok().map(|vb| (i as u16, vb.state)))
            .collect()
    }

    /// Get document count per vBucket (for partition stats)
    pub fn get_vbucket_doc_counts(&self) -> Vec<(u16, usize)> {
        self.vbuckets
            .iter()
            .enumerate()
            .filter_map(|(i, vb)| {
                vb.read().ok().map(|vb| (i as u16, vb.document_count()))
            })
            .collect()
    }
}

/// Eviction statistics for a bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionStats {
    pub policy: EvictionPolicy,
    pub total_evicted_items: usize,
    pub quota_bytes: usize,
    pub used_bytes: usize,
}

/// The main storage engine managing all buckets
pub struct StorageEngine {
    pub buckets: DashMap<String, Arc<Bucket>>,
    pub persistence: Option<Arc<PersistenceManager>>,
    pub num_vbuckets: u16,
}

impl StorageEngine {
    pub fn new(num_vbuckets: u16, data_dir: Option<String>, buffer_config: Option<WriteBufferConfig>) -> Self {
        let persistence = data_dir.map(|dir| {
            Arc::new(PersistenceManager::new(
                dir,
                buffer_config.unwrap_or_default(),
            ))
        });

        Self {
            buckets: DashMap::new(),
            persistence,
            num_vbuckets,
        }
    }

    /// Create a new bucket
    pub fn create_bucket(&self, mut config: BucketConfig) -> Result<()> {
        if self.buckets.contains_key(&config.name) {
            return Err(NosqlError::BucketAlreadyExists(config.name));
        }
        config.num_vbuckets = self.num_vbuckets;
        let name = config.name.clone();

        // Persist bucket config to disk
        if let Some(ref persistence) = self.persistence {
            if let Err(e) = persistence.save_bucket_config(&config) {
                warn!("Failed to persist bucket config for '{}': {}", name, e);
            }
        }

        let bucket = Arc::new(Bucket::new(config));
        self.buckets.insert(name.clone(), bucket);
        info!("Bucket '{}' created", name);
        Ok(())
    }

    /// Get a bucket by name
    pub fn get_bucket(&self, name: &str) -> Result<Arc<Bucket>> {
        self.buckets
            .get(name)
            .map(|b| b.value().clone())
            .ok_or_else(|| NosqlError::BucketNotFound(name.to_string()))
    }

    /// Get the data directory path (if persistence is enabled)
    pub fn data_dir(&self) -> Option<String> {
        self.persistence
            .as_ref()
            .map(|p| p.data_dir().to_string_lossy().to_string())
    }

    /// Persist scope/collection metadata for a bucket (call after scope/collection changes)
    #[allow(dead_code)]
    pub fn persist_scope_metadata(&self, bucket_name: &str) {
        if let Some(ref persistence) = self.persistence {
            if let Some(bucket) = self.buckets.get(bucket_name) {
                let scopes = bucket.list_scopes();
                if let Err(e) = persistence.save_scope_metadata(bucket_name, &scopes) {
                    warn!("Failed to save scope metadata for '{}': {}", bucket_name, e);
                }
            }
        }
    }

    /// Delete a bucket
    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        self.buckets
            .remove(name)
            .ok_or_else(|| NosqlError::BucketNotFound(name.to_string()))?;

        // Clean up persistence data
        if let Some(ref persistence) = self.persistence {
            if let Err(e) = persistence.delete_bucket_data(name) {
                warn!("Failed to delete persistence data for bucket '{}': {}", name, e);
            }
        }

        info!("Bucket '{}' deleted", name);
        Ok(())
    }

    /// List all buckets
    pub fn list_buckets(&self) -> Vec<BucketConfig> {
        self.buckets
            .iter()
            .map(|entry| entry.value().config.clone())
            .collect()
    }

    /// Run TTL expiry across all buckets
    pub fn run_ttl_expiry(&self) {
        for entry in self.buckets.iter() {
            let bucket = entry.value();
            let expired = bucket.purge_expired();
            if expired > 0 {
                info!(
                    "Purged {} expired documents from bucket '{}'",
                    expired, bucket.config.name
                );
            }
        }
    }

    /// Buffer a document mutation for WAL persistence.
    /// Returns true if an immediate flush is needed.
    pub fn buffer_mutation(&self, bucket_name: &str, doc: &Document) -> bool {
        if let Some(ref persistence) = self.persistence {
            persistence.buffer_mutation(bucket_name, doc)
        } else {
            false
        }
    }

    /// Flush the write buffer to WAL (called by background task or on trigger)
    pub fn flush_wal_buffer(&self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            persistence.flush_buffer()
                .map_err(|e| NosqlError::PersistenceError(e.to_string()))?;
        }
        Ok(())
    }

    /// Check if the WAL buffer needs flushing
    pub fn should_flush_wal(&self) -> bool {
        if let Some(ref persistence) = self.persistence {
            persistence.should_flush()
        } else {
            false
        }
    }

    /// Compact WAL entries into B+ tree for all buckets
    #[allow(dead_code)]
    pub fn compact_all(&self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            for entry in self.buckets.iter() {
                let bucket = entry.value();
                persistence.compact_to_btree(&bucket.config.name)
                    .map_err(|e| NosqlError::PersistenceError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Persist all data (full snapshot to B+ tree + bucket configs + scope metadata)
    pub fn persist_all(&self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            for entry in self.buckets.iter() {
                let bucket = entry.value();
                let bucket_name = &bucket.config.name;

                // Persist bucket config
                if let Err(e) = persistence.save_bucket_config(&bucket.config) {
                    warn!("Failed to save config for '{}': {}", bucket_name, e);
                }

                // Persist scope/collection metadata
                let scopes = bucket.list_scopes();
                if let Err(e) = persistence.save_scope_metadata(bucket_name, &scopes) {
                    warn!("Failed to save scope metadata for '{}': {}", bucket_name, e);
                }

                // Persist documents
                let docs = bucket.scan_all_documents();
                persistence
                    .write_snapshot(bucket_name, &docs)
                    .map_err(|e| NosqlError::PersistenceError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Load data from persistence (B+ tree + WAL replay)
    pub fn load_from_persistence(&self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            let bucket_names = persistence
                .list_buckets()
                .map_err(|e| NosqlError::PersistenceError(e.to_string()))?;

            for bucket_name in bucket_names {
                // Auto-create bucket if it doesn't exist in memory yet
                if !self.buckets.contains_key(&bucket_name) {
                    // Try to load persisted config, or use defaults
                    let config = match persistence.load_bucket_config(&bucket_name) {
                        Ok(Some(cfg)) => {
                            info!("Recovery: loaded config for bucket '{}'", bucket_name);
                            cfg
                        }
                        _ => {
                            info!(
                                "Recovery: no config found for '{}', using defaults",
                                bucket_name
                            );
                            let mut cfg = BucketConfig::default();
                            cfg.name = bucket_name.clone();
                            cfg.num_vbuckets = self.num_vbuckets;
                            cfg
                        }
                    };

                    let bucket = Arc::new(Bucket::new(config));

                    // Restore scopes/collections if metadata exists
                    if let Ok(Some(scopes)) = persistence.load_scope_metadata(&bucket_name) {
                        for scope_info in &scopes {
                            if scope_info.name == "_default" {
                                // _default scope already exists; just add non-default collections
                                if let Some(scope_ref) = bucket.scopes.get(&scope_info.name) {
                                    for coll_name in &scope_info.collections {
                                        if coll_name != "_default"
                                            && !scope_ref.collections.contains_key(coll_name)
                                        {
                                            scope_ref.collections.insert(
                                                coll_name.clone(),
                                                Collection::new(coll_name.clone()),
                                            );
                                        }
                                    }
                                }
                            } else {
                                // Create non-default scope
                                let scope = Scope::new(scope_info.name.clone());
                                for coll_name in &scope_info.collections {
                                    scope.collections.insert(
                                        coll_name.clone(),
                                        Collection::new(coll_name.clone()),
                                    );
                                }
                                bucket.scopes.insert(scope_info.name.clone(), scope);
                            }
                        }
                        info!(
                            "Recovery: restored {} scopes for bucket '{}'",
                            scopes.len(),
                            bucket_name
                        );
                    }

                    self.buckets.insert(bucket_name.clone(), bucket);
                    info!("Recovery: auto-created bucket '{}'", bucket_name);
                }

                // Now load documents
                if let Ok(docs) = persistence.load_bucket(&bucket_name) {
                    if let Some(bucket) = self.buckets.get(&bucket_name) {
                        for doc in &docs {
                            let mutation = doc.to_mutation();
                            let _ = bucket.apply_mutation(&mutation);
                        }
                        info!(
                            "Loaded {} documents for bucket '{}' from B+ tree + WAL",
                            docs.len(),
                            bucket_name
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Get persistence statistics
    pub fn persistence_stats(&self) -> Option<crate::storage::persistence::PersistenceSummary> {
        self.persistence.as_ref().map(|p| p.summary())
    }

    /// Export a vBucket from a specific bucket (for inter-node transfer)
    pub fn export_vbucket(&self, bucket_name: &str, vbucket_id: u16) -> Result<VBucketData> {
        let bucket = self.get_bucket(bucket_name)?;
        bucket.export_vbucket(vbucket_id)
    }

    /// Import a vBucket into a specific bucket (for inter-node transfer)
    pub fn import_vbucket(&self, bucket_name: &str, data: VBucketData) -> Result<()> {
        let bucket = self.get_bucket(bucket_name)?;
        bucket.import_vbucket(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage() -> StorageEngine {
        StorageEngine::new(16, None, None)
    }

    fn make_bucket_config(name: &str) -> BucketConfig {
        BucketConfig {
            name: name.to_string(),
            num_vbuckets: 16,
            ..Default::default()
        }
    }

    const S: &str = "_default";
    const C: &str = "_default";

    #[test]
    fn test_create_and_list_buckets() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        storage.create_bucket(make_bucket_config("b2")).unwrap();
        let list = storage.list_buckets();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_create_duplicate_bucket_fails() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let err = storage.create_bucket(make_bucket_config("b1"));
        assert!(err.is_err());
    }

    #[test]
    fn test_delete_bucket() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        storage.delete_bucket("b1").unwrap();
        assert!(storage.get_bucket("b1").is_err());
    }

    #[test]
    fn test_upsert_and_get() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({"x": 1}), None).unwrap();
        let doc = bucket.get(S, C, "k1").unwrap();
        assert_eq!(doc.value["x"], 1);
    }

    #[test]
    fn test_delete_document() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({"x": 1}), None).unwrap();
        bucket.delete(S, C, "k1", None).unwrap();
        assert!(bucket.get(S, C, "k1").is_err());
    }

    #[test]
    fn test_cas_mismatch() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({"x": 1}), None).unwrap();
        // Delete with wrong CAS
        let result = bucket.delete(S, C, "k1", Some(999));
        assert!(result.is_err());
    }

    #[test]
    fn test_scope_and_collection_crud() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();

        // Default scope exists
        let scopes = bucket.list_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].name, "_default");

        // Create scope
        bucket.create_scope("myScope".to_string()).unwrap();
        assert_eq!(bucket.list_scopes().len(), 2);

        // Create collection
        bucket.create_collection("myScope", "myColl".to_string()).unwrap();
        let scopes = bucket.list_scopes();
        let my_scope = scopes.iter().find(|s| s.name == "myScope").unwrap();
        assert!(my_scope.collections.contains(&"myColl".to_string()));

        // Delete collection
        bucket.delete_collection("myScope", "myColl").unwrap();
        let scopes = bucket.list_scopes();
        let my_scope = scopes.iter().find(|s| s.name == "myScope").unwrap();
        assert!(!my_scope.collections.contains(&"myColl".to_string()));

        // Delete scope
        bucket.delete_scope("myScope").unwrap();
        assert_eq!(bucket.list_scopes().len(), 1);
    }

    #[test]
    fn test_scan_all_documents() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        for i in 0..10 {
            bucket.upsert(S, C, format!("k{}", i), serde_json::json!({"i": i}), None).unwrap();
        }
        let docs = bucket.scan_all_documents();
        assert_eq!(docs.len(), 10);
    }

    #[test]
    fn test_upsert_overwrite() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({"x": 1}), None).unwrap();
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({"x": 2}), None).unwrap();
        let doc = bucket.get(S, C, "k1").unwrap();
        assert_eq!(doc.value["x"], 2);
    }

    #[test]
    fn test_flush_bucket() {
        let storage = make_storage();
        let mut cfg = make_bucket_config("b1");
        cfg.flush_enabled = true;
        storage.create_bucket(cfg).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({"x": 1}), None).unwrap();
        bucket.upsert(S, C, "k2".to_string(), serde_json::json!({"x": 2}), None).unwrap();
        bucket.flush().unwrap();
        assert!(bucket.get(S, C, "k1").is_err());
        assert!(bucket.get(S, C, "k2").is_err());
    }

    #[test]
    fn test_document_count() {
        let storage = make_storage();
        storage.create_bucket(make_bucket_config("b1")).unwrap();
        let bucket = storage.get_bucket("b1").unwrap();
        assert_eq!(bucket.document_count(), 0);
        bucket.upsert(S, C, "k1".to_string(), serde_json::json!({}), None).unwrap();
        bucket.upsert(S, C, "k2".to_string(), serde_json::json!({}), None).unwrap();
        assert_eq!(bucket.document_count(), 2);
    }
}
