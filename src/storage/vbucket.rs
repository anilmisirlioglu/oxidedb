use crate::error::{NosqlError, Result};
use crate::storage::document::{Document, Mutation};
use crate::storage::engine::EvictionPolicy;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// VBucket state in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VBucketState {
    Active,
    Replica,
    Pending,
    Dead,
}

/// Lock info for a document
#[derive(Debug, Clone)]
struct LockInfo {
    /// CAS value when locked
    cas: u64,
    /// When the lock expires
    expiry: DateTime<Utc>,
}

/// A virtual bucket - the unit of data partitioning
#[derive(Debug)]
pub struct VBucket {
    pub id: u16,
    pub state: VBucketState,
    /// Documents stored in this vBucket
    documents: HashMap<String, Document>,
    /// High sequence number (for XDCR change tracking)
    pub high_seq_no: u64,
    /// Mutation log for XDCR (bounded circular buffer)
    mutation_log: Vec<Mutation>,
    /// Maximum mutation log size
    max_mutation_log_size: usize,
    /// Document locks (key → lock info)
    locks: HashMap<String, LockInfo>,
}

impl VBucket {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            state: VBucketState::Active,
            documents: HashMap::new(),
            high_seq_no: 0,
            mutation_log: Vec::new(),
            max_mutation_log_size: 10000,
            locks: HashMap::new(),
        }
    }

    /// Get a document by key (read-only — does NOT update last_accessed)
    pub fn get(&self, key: &str) -> Result<&Document> {
        let doc = self
            .documents
            .get(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;

        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }

        Ok(doc)
    }

    /// Get a document and update last_accessed (requires &mut self)
    #[allow(dead_code)]
    pub fn get_and_touch_access(&mut self, key: &str) -> Result<Document> {
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;

        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }

        doc.last_accessed = Utc::now();
        Ok(doc.clone())
    }

    /// Insert or update a document
    pub fn upsert(&mut self, key: String, value: serde_json::Value) -> Result<Document> {
        if let Some(doc) = self.documents.get_mut(&key) {
            doc.update(value);
            let doc_clone = doc.clone();
            self.high_seq_no = doc_clone.seq_no;
            self.append_mutation(doc_clone.to_mutation());
            Ok(doc_clone)
        } else {
            let doc = Document::new(key.clone(), value, self.id);
            self.high_seq_no = doc.seq_no;
            self.append_mutation(doc.to_mutation());
            self.documents.insert(key, doc.clone());
            Ok(doc)
        }
    }

    /// Insert a document, fail if exists
    #[allow(dead_code)]
    pub fn insert(&mut self, key: String, value: serde_json::Value) -> Result<Document> {
        if let Some(existing) = self.documents.get(&key) {
            if !existing.deleted && !existing.is_expired() {
                return Err(NosqlError::DocumentNotFound(format!(
                    "Document '{}' already exists",
                    key
                )));
            }
        }

        let doc = Document::new(key.clone(), value, self.id);
        self.high_seq_no = doc.seq_no;
        self.append_mutation(doc.to_mutation());
        self.documents.insert(key, doc.clone());
        Ok(doc)
    }

    /// Replace a document with CAS check
    pub fn replace(
        &mut self,
        key: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;

        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }

        // CAS check
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch {
                    expected: expected_cas,
                    actual: doc.cas,
                });
            }
        }

        doc.update(value);
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Delete a document (creates tombstone for XDCR)
    pub fn delete(&mut self, key: &str, cas: Option<u64>) -> Result<Document> {
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;

        if doc.deleted {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }

        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch {
                    expected: expected_cas,
                    actual: doc.cas,
                });
            }
        }

        doc.mark_deleted();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Apply a mutation from XDCR replication
    pub fn apply_mutation(&mut self, mutation: &Mutation) -> Result<()> {
        if let Some(existing) = self.documents.get(&mutation.key) {
            // Document exists - this will be handled by conflict resolution
            // at a higher level. Here we just apply it.
            if existing.cas >= mutation.cas && !mutation.deleted {
                // Local is newer, skip (conflict resolution already decided)
                return Ok(());
            }
        }

        let doc = Document {
            key: mutation.key.clone(),
            value: mutation.value.clone(),
            cas: mutation.cas,
            seq_no: mutation.seq_no,
            rev_id: mutation.rev_id,
            expiry: mutation.expiry,
            flags: mutation.flags,
            created_at: mutation.updated_at,
            updated_at: mutation.updated_at,
            deleted: mutation.deleted,
            source_cluster: mutation.source_cluster.clone(),
            vbucket_id: self.id,
            xattrs: mutation.xattrs.clone(),
            last_accessed: mutation.updated_at,
            evicted: false,
        };

        self.high_seq_no = self.high_seq_no.max(mutation.seq_no);
        self.documents.insert(mutation.key.clone(), doc);
        Ok(())
    }

    /// Get mutations since a given sequence number (for XDCR)
    pub fn get_mutations_since(&self, since_seq_no: u64) -> Vec<Mutation> {
        self.mutation_log
            .iter()
            .filter(|m| m.seq_no > since_seq_no)
            .cloned()
            .collect()
    }

    /// Touch a document (update expiry only)
    pub fn touch(&mut self, key: &str, expiry_seconds: u64) -> Result<Document> {
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;

        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }

        doc.expiry = Some(chrono::Utc::now() + chrono::Duration::seconds(expiry_seconds as i64));
        doc.cas = crate::storage::document::next_cas();
        Ok(doc.clone())
    }

    /// Remove expired documents
    pub fn purge_expired(&mut self) -> Vec<String> {
        let expired_keys: Vec<String> = self
            .documents
            .iter()
            .filter(|(_, doc)| doc.is_expired() && !doc.deleted)
            .map(|(key, _)| key.clone())
            .collect();

        for key in &expired_keys {
            let mutation = {
                if let Some(doc) = self.documents.get_mut(key) {
                    doc.mark_deleted();
                    Some(doc.to_mutation())
                } else {
                    None
                }
            };
            if let Some(m) = mutation {
                self.append_mutation(m);
            }
        }

        expired_keys
    }

    // =====================================================================
    // Exists
    // =====================================================================

    /// Check if a document exists (returns CAS without fetching value)
    #[allow(dead_code)]
    pub fn exists(&self, key: &str) -> Result<u64> {
        let doc = self
            .documents
            .get(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        Ok(doc.cas)
    }

    // =====================================================================
    // Get & Lock / Unlock
    // =====================================================================

    /// Check if a document is currently locked
    fn is_locked(&self, key: &str) -> bool {
        if let Some(lock) = self.locks.get(key) {
            Utc::now() < lock.expiry
        } else {
            false
        }
    }

    /// Get and lock a document (pessimistic locking)
    pub fn get_and_lock(&mut self, key: &str, lock_seconds: u32) -> Result<Document> {
        // Clean expired lock
        if let Some(lock) = self.locks.get(key) {
            if Utc::now() >= lock.expiry {
                self.locks.remove(key);
            }
        }

        if self.is_locked(key) {
            return Err(NosqlError::DocumentLocked(key.to_string()));
        }

        let doc = self
            .documents
            .get(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }

        let lock_info = LockInfo {
            cas: doc.cas,
            expiry: Utc::now() + chrono::Duration::seconds(lock_seconds as i64),
        };
        self.locks.insert(key.to_string(), lock_info);

        Ok(doc.clone())
    }

    /// Unlock a document
    pub fn unlock(&mut self, key: &str, cas: u64) -> Result<()> {
        let lock = self
            .locks
            .get(key)
            .ok_or_else(|| NosqlError::Internal(format!("Document '{}' is not locked", key)))?;

        if lock.cas != cas {
            return Err(NosqlError::CasMismatch {
                expected: lock.cas,
                actual: cas,
            });
        }

        self.locks.remove(key);
        Ok(())
    }

    // =====================================================================
    // Sub-Document Operations
    // =====================================================================

    /// Get a value at a JSON path within a document
    pub fn subdoc_get(&self, key: &str, path: &str) -> Result<serde_json::Value> {
        let doc = self.get(key)?;
        let value = json_path_get(&doc.value, path)
            .ok_or_else(|| NosqlError::SubdocPathNotFound(path.to_string()))?;
        Ok(value.clone())
    }

    /// Check if a path exists within a document
    pub fn subdoc_exists(&self, key: &str, path: &str) -> Result<bool> {
        let doc = self.get(key)?;
        Ok(json_path_get(&doc.value, path).is_some())
    }

    /// Get count of elements at a path (array length or object key count)
    pub fn subdoc_get_count(&self, key: &str, path: &str) -> Result<usize> {
        let doc = self.get(key)?;
        let target = if path.is_empty() {
            &doc.value
        } else {
            json_path_get(&doc.value, path)
                .ok_or_else(|| NosqlError::SubdocPathNotFound(path.to_string()))?
        };
        match target {
            serde_json::Value::Array(arr) => Ok(arr.len()),
            serde_json::Value::Object(map) => Ok(map.len()),
            _ => Err(NosqlError::SubdocPathMismatch(path.to_string())),
        }
    }

    /// Dict upsert: set value at path (create intermediate objects if needed)
    pub fn subdoc_dict_upsert(
        &mut self,
        key: &str,
        path: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.check_lock_for_mutation(key)?;
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        json_path_set(&mut doc.value, path, value)
            .map_err(|e| NosqlError::SubdocPathNotFound(e))?;

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Dict add: set value at path only if it doesn't exist
    pub fn subdoc_dict_add(
        &mut self,
        key: &str,
        path: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        // Check if path already exists
        if let Ok(doc) = self.get(key) {
            if json_path_get(&doc.value, path).is_some() {
                return Err(NosqlError::SubdocPathExists(path.to_string()));
            }
        }
        self.subdoc_dict_upsert(key, path, value, cas)
    }

    /// Delete value at path
    pub fn subdoc_delete(
        &mut self,
        key: &str,
        path: &str,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.check_lock_for_mutation(key)?;
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        json_path_delete(&mut doc.value, path)
            .map_err(|e| NosqlError::SubdocPathNotFound(e))?;

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Replace value at path (must exist)
    pub fn subdoc_replace(
        &mut self,
        key: &str,
        path: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        // Check path exists first
        {
            let doc = self.get(key)?;
            if json_path_get(&doc.value, path).is_none() {
                return Err(NosqlError::SubdocPathNotFound(path.to_string()));
            }
        }
        self.subdoc_dict_upsert(key, path, value, cas)
    }

    /// Push value to end of array at path
    pub fn subdoc_array_push_last(
        &mut self,
        key: &str,
        path: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.check_lock_for_mutation(key)?;
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        let target = json_path_get_mut(&mut doc.value, path)
            .ok_or_else(|| NosqlError::SubdocPathNotFound(path.to_string()))?;
        match target {
            serde_json::Value::Array(arr) => arr.push(value),
            _ => return Err(NosqlError::SubdocPathMismatch(path.to_string())),
        }

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Push value to start of array at path
    pub fn subdoc_array_push_first(
        &mut self,
        key: &str,
        path: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.check_lock_for_mutation(key)?;
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        let target = json_path_get_mut(&mut doc.value, path)
            .ok_or_else(|| NosqlError::SubdocPathNotFound(path.to_string()))?;
        match target {
            serde_json::Value::Array(arr) => arr.insert(0, value),
            _ => return Err(NosqlError::SubdocPathMismatch(path.to_string())),
        }

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Sub-document counter: increment/decrement a numeric value at path
    pub fn subdoc_counter(
        &mut self,
        key: &str,
        path: &str,
        delta: i64,
        cas: Option<u64>,
    ) -> Result<(Document, i64)> {
        self.check_lock_for_mutation(key)?;
        let doc = self
            .documents
            .get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        let target = json_path_get_mut(&mut doc.value, path)
            .ok_or_else(|| NosqlError::SubdocPathNotFound(path.to_string()))?;
        let current = target.as_i64()
            .ok_or_else(|| NosqlError::SubdocPathMismatch(path.to_string()))?;
        let new_val = current + delta;
        *target = serde_json::json!(new_val);

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok((doc_clone, new_val))
    }

    // =====================================================================
    // Extended Attributes (XATTRs)
    // =====================================================================

    /// Get an xattr value by namespace and path.
    /// Handles Couchbase virtual XATTRs ($document.*) which return computed
    /// metadata instead of stored XATTR values.
    pub fn xattr_get(&self, key: &str, xattr_path: &str) -> Result<serde_json::Value> {
        let doc = self.get(key)?;

        // Handle virtual XATTR: $document
        if xattr_path.starts_with("$document") {
            return self.virtual_xattr_get(doc, xattr_path);
        }

        // xattr_path format: "namespace" or "namespace.subpath"
        let (ns, sub_path) = split_xattr_path(xattr_path);
        let xattr_val = doc.xattrs.get(ns)
            .ok_or_else(|| NosqlError::SubdocPathNotFound(xattr_path.to_string()))?;
        if let Some(sub) = sub_path {
            json_path_get(xattr_val, sub)
                .cloned()
                .ok_or_else(|| NosqlError::SubdocPathNotFound(xattr_path.to_string()))
        } else {
            Ok(xattr_val.clone())
        }
    }

    /// Handle Couchbase $document virtual XATTR lookups.
    /// Returns computed metadata about the document.
    fn virtual_xattr_get(&self, doc: &Document, xattr_path: &str) -> Result<serde_json::Value> {
        let exptime: u64 = doc.expiry
            .map(|e| e.timestamp().max(0) as u64)
            .unwrap_or(0);

        // Build the full $document virtual XATTR object
        let vxattr = serde_json::json!({
            "CAS": format!("0x{:016x}", doc.cas),
            "vbucket_uuid": doc.vbucket_id,
            "seqno": format!("0x{:016x}", doc.seq_no),
            "exptime": exptime,
            "flags": doc.flags,
            "value_bytes": serde_json::to_string(&doc.value).unwrap_or_default().len(),
            "datatype": ["json"],
            "deleted": doc.deleted,
            "last_modified": doc.updated_at.timestamp().to_string(),
            "revid": format!("{}", doc.rev_id)
        });

        if xattr_path == "$document" {
            return Ok(vxattr);
        }

        // Extract sub-path: "$document.expiry" → "expiry"
        let sub_path = xattr_path.strip_prefix("$document.")
            .ok_or_else(|| NosqlError::SubdocPathNotFound(xattr_path.to_string()))?;

        // Map known sub-paths
        match sub_path {
            "exptime" | "expiry" => Ok(serde_json::json!(exptime)),
            "CAS" => Ok(serde_json::json!(format!("0x{:016x}", doc.cas))),
            "seqno" => Ok(serde_json::json!(format!("0x{:016x}", doc.seq_no))),
            "flags" => Ok(serde_json::json!(doc.flags)),
            "value_bytes" => Ok(serde_json::json!(
                serde_json::to_string(&doc.value).unwrap_or_default().len()
            )),
            "datatype" => Ok(serde_json::json!(["json"])),
            "deleted" => Ok(serde_json::json!(doc.deleted)),
            "last_modified" => Ok(serde_json::json!(doc.updated_at.timestamp().to_string())),
            "revid" => Ok(serde_json::json!(format!("{}", doc.rev_id))),
            _ => Err(NosqlError::SubdocPathNotFound(xattr_path.to_string())),
        }
    }

    /// Check if an xattr path exists
    pub fn xattr_exists(&self, key: &str, xattr_path: &str) -> Result<bool> {
        let doc = self.get(key)?;

        // Virtual XATTR: $document always exists for live documents
        if xattr_path.starts_with("$document") {
            if xattr_path == "$document" {
                return Ok(true);
            }
            return match xattr_path.strip_prefix("$document.") {
                Some("exptime" | "expiry" | "CAS" | "seqno" | "flags" |
                     "value_bytes" | "datatype" | "deleted" | "last_modified" | "revid") => Ok(true),
                _ => Ok(false),
            };
        }

        let (ns, sub_path) = split_xattr_path(xattr_path);
        match doc.xattrs.get(ns) {
            Some(xattr_val) => {
                if let Some(sub) = sub_path {
                    Ok(json_path_get(xattr_val, sub).is_some())
                } else {
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    /// Upsert an xattr value
    pub fn xattr_upsert(
        &mut self,
        key: &str,
        xattr_path: &str,
        value: serde_json::Value,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.check_lock_for_mutation(key)?;
        let doc = self.documents.get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        let (ns, sub_path) = split_xattr_path(xattr_path);
        if let Some(sub) = sub_path {
            // Set a sub-path within the xattr namespace
            let xattr_val = doc.xattrs.entry(ns.to_string()).or_insert_with(|| serde_json::json!({}));
            json_path_set(xattr_val, sub, value)
                .map_err(|e| NosqlError::SubdocPathNotFound(e))?;
        } else {
            // Set the entire namespace
            doc.xattrs.insert(ns.to_string(), value);
        }

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Remove an xattr
    pub fn xattr_remove(
        &mut self,
        key: &str,
        xattr_path: &str,
        cas: Option<u64>,
    ) -> Result<Document> {
        self.check_lock_for_mutation(key)?;
        let doc = self.documents.get_mut(key)
            .ok_or_else(|| NosqlError::DocumentNotFound(key.to_string()))?;
        if doc.deleted || doc.is_expired() {
            return Err(NosqlError::DocumentNotFound(key.to_string()));
        }
        if let Some(expected_cas) = cas {
            if doc.cas != expected_cas {
                return Err(NosqlError::CasMismatch { expected: expected_cas, actual: doc.cas });
            }
        }

        let (ns, sub_path) = split_xattr_path(xattr_path);
        if let Some(sub) = sub_path {
            let xattr_val = doc.xattrs.get_mut(ns)
                .ok_or_else(|| NosqlError::SubdocPathNotFound(xattr_path.to_string()))?;
            json_path_delete(xattr_val, sub)
                .map_err(|e| NosqlError::SubdocPathNotFound(e))?;
        } else {
            doc.xattrs.remove(ns)
                .ok_or_else(|| NosqlError::SubdocPathNotFound(xattr_path.to_string()))?;
        }

        doc.cas = crate::storage::document::next_cas();
        doc.seq_no = crate::storage::document::next_seq();
        doc.rev_id += 1;
        doc.updated_at = Utc::now();
        let doc_clone = doc.clone();
        self.high_seq_no = doc_clone.seq_no;
        self.append_mutation(doc_clone.to_mutation());
        Ok(doc_clone)
    }

    /// Get all xattrs for a document
    pub fn xattr_list(&self, key: &str) -> Result<std::collections::HashMap<String, serde_json::Value>> {
        let doc = self.get(key)?;
        Ok(doc.xattrs.clone())
    }

    /// Purge deletion tombstones older than the given duration
    #[allow(dead_code)]
    pub fn purge_tombstones(&mut self, max_age: chrono::Duration) -> usize {
        let cutoff = Utc::now() - max_age;
        let keys_to_remove: Vec<String> = self
            .documents
            .iter()
            .filter(|(_, doc)| doc.deleted && doc.updated_at < cutoff)
            .map(|(key, _)| key.clone())
            .collect();
        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.documents.remove(&key);
        }
        count
    }

    // =====================================================================
    // Eviction
    // =====================================================================

    /// Run eviction to free memory, returns number of documents evicted
    pub fn run_eviction(&mut self, policy: &EvictionPolicy, target_bytes: usize) -> usize {
        match policy {
            EvictionPolicy::NoEviction => 0,
            EvictionPolicy::ValueOnly => self.evict_value_only(target_bytes),
            EvictionPolicy::FullEviction => self.evict_full(target_bytes),
            EvictionPolicy::NotRecentlyUsed => self.evict_nru(target_bytes),
        }
    }

    /// Value-only eviction: null out document values, keep metadata
    fn evict_value_only(&mut self, target_bytes: usize) -> usize {
        let current_size = self.size_bytes();
        if current_size <= target_bytes {
            return 0;
        }

        // Sort by last_accessed ascending (oldest first)
        let mut sorted: Vec<(String, DateTime<Utc>)> = self.documents.iter()
            .filter(|(_, doc)| !doc.deleted && !doc.evicted && !doc.is_expired())
            .map(|(key, doc)| (key.clone(), doc.last_accessed))
            .collect();
        sorted.sort_by_key(|(_, ts)| *ts);
        let candidates: Vec<String> = sorted.into_iter().map(|(k, _)| k).collect();

        let mut evicted = 0;
        let mut freed_bytes = 0;
        let bytes_to_free = current_size.saturating_sub(target_bytes);

        for key in candidates {
            if freed_bytes >= bytes_to_free {
                break;
            }
            if let Some(doc) = self.documents.get_mut(&key) {
                let val_size = serde_json::to_string(&doc.value).unwrap_or_default().len();
                doc.evict_value();
                freed_bytes += val_size;
                evicted += 1;
            }
        }

        evicted
    }

    /// Full eviction: remove entire documents from memory
    fn evict_full(&mut self, target_bytes: usize) -> usize {
        let current_size = self.size_bytes();
        if current_size <= target_bytes {
            return 0;
        }

        let mut sorted: Vec<(String, DateTime<Utc>)> = self.documents.iter()
            .filter(|(_, doc)| !doc.deleted && !doc.is_expired())
            .map(|(key, doc)| (key.clone(), doc.last_accessed))
            .collect();
        sorted.sort_by_key(|(_, ts)| *ts);

        let mut evicted = 0;
        let mut freed_bytes = 0;
        let bytes_to_free = current_size.saturating_sub(target_bytes);

        for (key, _) in sorted {
            if freed_bytes >= bytes_to_free {
                break;
            }
            if let Some(doc) = self.documents.get(&key) {
                let doc_size = doc.key.len()
                    + serde_json::to_string(&doc.value).unwrap_or_default().len()
                    + 128;
                freed_bytes += doc_size;
            }
            self.documents.remove(&key);
            evicted += 1;
        }

        evicted
    }

    /// NRU (Not Recently Used) eviction: evict values of least recently accessed docs
    fn evict_nru(&mut self, target_bytes: usize) -> usize {
        // NRU is similar to value-only but uses access time more aggressively
        self.evict_value_only(target_bytes)
    }

    /// Count of evicted documents
    #[allow(dead_code)]
    pub fn evicted_count(&self) -> usize {
        self.documents.values().filter(|d| d.evicted).count()
    }

    /// Check if a document is locked before mutation; return error if locked
    fn check_lock_for_mutation(&mut self, key: &str) -> Result<()> {
        // Clean expired locks first
        if let Some(lock) = self.locks.get(key) {
            if Utc::now() >= lock.expiry {
                self.locks.remove(key);
                return Ok(());
            }
            return Err(NosqlError::DocumentLocked(key.to_string()));
        }
        Ok(())
    }

    /// Update the expiry of a document stored in this vBucket.
    /// This modifies the in-memory document directly (not a clone).
    pub fn set_expiry(&mut self, key: &str, expiry: Option<DateTime<Utc>>) {
        if let Some(doc) = self.documents.get_mut(key) {
            doc.expiry = expiry;
        }
    }

    /// Update the flags of a document stored in this vBucket.
    /// This modifies the in-memory document directly (not a clone).
    pub fn set_flags(&mut self, key: &str, flags: u32) {
        if let Some(doc) = self.documents.get_mut(key) {
            doc.flags = flags;
        }
    }

    /// Get all non-deleted, non-expired documents
    pub fn get_all_documents(&self) -> Vec<&Document> {
        self.documents
            .values()
            .filter(|doc| !doc.deleted && !doc.is_expired())
            .collect()
    }

    /// Get document count
    pub fn document_count(&self) -> usize {
        self.documents
            .values()
            .filter(|doc| !doc.deleted && !doc.is_expired())
            .count()
    }

    /// Get total size in bytes (approximate)
    pub fn size_bytes(&self) -> usize {
        self.documents
            .values()
            .filter(|doc| !doc.deleted)
            .map(|doc| {
                doc.key.len() + serde_json::to_string(&doc.value).unwrap_or_default().len() + 128
                // 128 bytes overhead for metadata
            })
            .sum()
    }

    fn append_mutation(&mut self, mutation: Mutation) {
        if self.mutation_log.len() >= self.max_mutation_log_size {
            // Remove oldest 10%
            let remove_count = self.max_mutation_log_size / 10;
            self.mutation_log.drain(0..remove_count);
        }
        self.mutation_log.push(mutation);
    }
}

/// Split an xattr path into namespace and optional sub-path
/// e.g., "_sync" → ("_sync", None)
/// e.g., "_sync.rev" → ("_sync", Some("rev"))
/// e.g., "myapp.data.field" → ("myapp", Some("data.field"))
fn split_xattr_path(path: &str) -> (&str, Option<&str>) {
    if let Some(dot_pos) = path.find('.') {
        (&path[..dot_pos], Some(&path[dot_pos + 1..]))
    } else {
        (path, None)
    }
}

/// Hash a key to determine its vBucket
pub fn hash_to_vbucket(key: &str, num_vbuckets: u16) -> u16 {
    let hash = crc32fast::hash(key.as_bytes());
    (hash % num_vbuckets as u32) as u16
}

// =========================================================================
// JSON Path helpers for sub-document operations
// =========================================================================

/// Get a value at a dot-separated path (e.g. "address.city")
fn json_path_get<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in &parts {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(*part)?;
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = part.parse::<usize>() {
                    current = arr.get(idx)?;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Get a mutable reference to a value at a dot-separated path
fn json_path_get_mut<'a>(
    value: &'a mut serde_json::Value,
    path: &str,
) -> Option<&'a mut serde_json::Value> {
    if path.is_empty() {
        return Some(value);
    }
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in &parts {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get_mut(*part)?;
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = part.parse::<usize>() {
                    current = arr.get_mut(idx)?;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Set a value at a dot-separated path, creating intermediate objects as needed
fn json_path_set(
    root: &mut serde_json::Value,
    path: &str,
    new_value: serde_json::Value,
) -> std::result::Result<(), String> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return Err("Empty path".to_string());
    }

    let mut current = root;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part: set the value
            match current {
                serde_json::Value::Object(map) => {
                    map.insert(part.to_string(), new_value);
                    return Ok(());
                }
                serde_json::Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        if idx < arr.len() {
                            arr[idx] = new_value;
                            return Ok(());
                        }
                    }
                    return Err(format!("Invalid array index: {}", part));
                }
                _ => return Err(format!("Cannot set path '{}' on non-container", path)),
            }
        } else {
            // Intermediate part: navigate or create
            match current {
                serde_json::Value::Object(map) => {
                    if !map.contains_key(*part) {
                        map.insert(part.to_string(), serde_json::json!({}));
                    }
                    current = map.get_mut(*part).unwrap();
                }
                _ => return Err(format!("Path segment '{}' is not an object", part)),
            }
        }
    }
    Err("Unexpected end of path".to_string())
}

/// Delete a value at a dot-separated path
fn json_path_delete(
    root: &mut serde_json::Value,
    path: &str,
) -> std::result::Result<(), String> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return Err("Empty path".to_string());
    }

    if parts.len() == 1 {
        match root {
            serde_json::Value::Object(map) => {
                if map.remove(parts[0]).is_some() {
                    return Ok(());
                }
                return Err(format!("Path '{}' not found", path));
            }
            _ => return Err("Root is not an object".to_string()),
        }
    }

    // Navigate to parent
    let parent_path = &parts[..parts.len() - 1];
    let last_part = parts[parts.len() - 1];
    let mut current = root;
    for part in parent_path {
        match current {
            serde_json::Value::Object(map) => {
                current = map
                    .get_mut(*part)
                    .ok_or_else(|| format!("Path segment '{}' not found", part))?;
            }
            _ => return Err(format!("Path segment '{}' is not an object", part)),
        }
    }

    match current {
        serde_json::Value::Object(map) => {
            if map.remove(last_part).is_some() {
                Ok(())
            } else {
                Err(format!("Path '{}' not found", path))
            }
        }
        serde_json::Value::Array(arr) => {
            if let Ok(idx) = last_part.parse::<usize>() {
                if idx < arr.len() {
                    arr.remove(idx);
                    return Ok(());
                }
            }
            Err(format!("Invalid array index: {}", last_part))
        }
        _ => Err(format!("Parent of '{}' is not a container", path)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_to_vbucket() {
        let vb = hash_to_vbucket("test-key", 1024);
        assert!(vb < 1024);

        // Same key should always map to same vBucket
        let vb2 = hash_to_vbucket("test-key", 1024);
        assert_eq!(vb, vb2);
    }

    #[test]
    fn test_vbucket_crud() {
        let mut vb = VBucket::new(0);

        // Insert
        let doc = vb
            .upsert("key1".to_string(), serde_json::json!({"name": "test"}))
            .unwrap();
        assert_eq!(doc.key, "key1");
        assert_eq!(doc.rev_id, 1);

        // Get
        let doc = vb.get("key1").unwrap();
        assert_eq!(doc.value["name"], "test");

        // Update
        let doc = vb
            .upsert("key1".to_string(), serde_json::json!({"name": "updated"}))
            .unwrap();
        assert_eq!(doc.rev_id, 2);

        // Delete
        vb.delete("key1", None).unwrap();
        assert!(vb.get("key1").is_err());
    }
}
