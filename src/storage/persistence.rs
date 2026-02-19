//! Persistence layer: WAL write buffer + B+ tree data files.
//!
//! Architecture (Couchbase-like):
//!
//! ```text
//! Write:  App → VBucket (memory) → WriteBuffer → WAL file (on flush) → B+ tree (on compact)
//! Read:   App → VBucket (memory, fast path)
//! Recover: B+ tree → replay WAL → fill VBuckets
//! ```
//!
//! Dual-trigger flush:
//!   Buffer flushes to WAL when EITHER:
//!     • ops  >= max_buffer_ops  (default 5000)
//!     • bytes >= max_buffer_bytes (default 4 MB)
//!     • time >= flush_interval_ms (default 1000 ms)

use crate::storage::btree::{BPlusTree, BTreeStats};
use crate::storage::document::Document;
use crate::storage::wal::{WalEntry, WalFile, WriteBuffer, WriteBufferConfig, WriteBufferStats};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::{error, info, warn};

/// Persistence manager integrating WAL + B+ tree storage.
pub struct PersistenceManager {
    data_dir: PathBuf,
    /// Per-bucket WAL files
    wal_files: Mutex<HashMap<String, WalFile>>,
    /// Per-bucket B+ trees
    btrees: Mutex<HashMap<String, BPlusTree>>,
    /// Shared write buffer (all buckets)
    write_buffer: Mutex<WriteBuffer>,
    /// Buffer configuration
    #[allow(dead_code)]
    buffer_config: WriteBufferConfig,
}

impl PersistenceManager {
    pub fn new(data_dir: String, buffer_config: WriteBufferConfig) -> Self {
        let path = PathBuf::from(&data_dir);
        if let Err(e) = fs::create_dir_all(&path) {
            error!("Failed to create data directory: {}", e);
        }

        let write_buffer = WriteBuffer::new(buffer_config.clone());

        Self {
            data_dir: path,
            wal_files: Mutex::new(HashMap::new()),
            btrees: Mutex::new(HashMap::new()),
            write_buffer: Mutex::new(write_buffer),
            buffer_config,
        }
    }

    /// Get the data directory path
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    // =========================================================================
    // Path helpers
    // =========================================================================

    fn wal_path(&self, bucket_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.wal", bucket_name))
    }

    fn btree_path(&self, bucket_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.nqdb", bucket_name))
    }

    fn config_path(&self, bucket_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.config.json", bucket_name))
    }

    /// Legacy snapshot path (for migration)
    fn legacy_snapshot_path(&self, bucket_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.snapshot.json", bucket_name))
    }

    /// Legacy WAL path (for migration)
    fn legacy_wal_path(&self, bucket_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.wal.json", bucket_name))
    }

    // =========================================================================
    // Bucket config persistence
    // =========================================================================

    /// Save a bucket's configuration to disk as JSON.
    pub fn save_bucket_config(&self, config: &crate::storage::engine::BucketConfig) -> io::Result<()> {
        let path = self.config_path(&config.name);
        let json = serde_json::to_string_pretty(config)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&path, json)?;
        info!("Bucket config saved: {}", config.name);
        Ok(())
    }

    /// Save scope and collection metadata for a bucket.
    pub fn save_scope_metadata(
        &self,
        bucket_name: &str,
        scopes: &[crate::storage::engine::ScopeInfo],
    ) -> io::Result<()> {
        let path = self.data_dir.join(format!("{}.scopes.json", bucket_name));
        let json = serde_json::to_string_pretty(scopes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&path, json)?;
        Ok(())
    }

    /// Load a bucket's configuration from disk.
    pub fn load_bucket_config(
        &self,
        bucket_name: &str,
    ) -> io::Result<Option<crate::storage::engine::BucketConfig>> {
        let path = self.config_path(bucket_name);
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&path)?;
        let config: crate::storage::engine::BucketConfig = serde_json::from_str(&content)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(config))
    }

    /// Load scope metadata for a bucket.
    pub fn load_scope_metadata(
        &self,
        bucket_name: &str,
    ) -> io::Result<Option<Vec<crate::storage::engine::ScopeInfo>>> {
        let path = self.data_dir.join(format!("{}.scopes.json", bucket_name));
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&path)?;
        let scopes: Vec<crate::storage::engine::ScopeInfo> = serde_json::from_str(&content)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(scopes))
    }

    // =========================================================================
    // Write buffer: accept mutations
    // =========================================================================

    /// Record a mutation in the write buffer.
    /// Returns true if an immediate flush should be performed.
    pub fn buffer_mutation(&self, bucket_name: &str, doc: &Document) -> bool {
        let entry = WalEntry::from_document(bucket_name, doc);
        let mut buf = self.write_buffer.lock().expect("write_buffer lock poisoned");
        buf.push(entry)
    }

    /// Check if the buffer should be flushed (called from background task)
    pub fn should_flush(&self) -> bool {
        let buf = self.write_buffer.lock().expect("write_buffer lock poisoned");
        buf.should_flush()
    }

    // =========================================================================
    // Flush: write buffer → WAL file
    // =========================================================================

    /// Flush the write buffer to WAL file(s) on disk.
    /// This is the main persistence path, called when any trigger fires.
    pub fn flush_buffer(&self) -> io::Result<usize> {
        // Drain entries from buffer
        let entries = {
            let mut buf = self.write_buffer.lock().expect("write_buffer lock poisoned");
            if buf.pending_count() == 0 {
                return Ok(0);
            }
            buf.drain()
        };

        let count = entries.len();

        // Group entries by bucket
        let mut by_bucket: HashMap<String, Vec<WalEntry>> = HashMap::new();
        for entry in entries {
            by_bucket
                .entry(entry.bucket.clone())
                .or_default()
                .push(entry);
        }

        // Write each bucket's entries to its WAL file
        let mut wal_files = self.wal_files.lock().expect("wal_files lock poisoned");
        for (bucket, entries) in &by_bucket {
            if !wal_files.contains_key(bucket.as_str()) {
                match WalFile::open(&self.wal_path(bucket)) {
                    Ok(wal) => { wal_files.insert(bucket.clone(), wal); }
                    Err(e) => {
                        error!("Failed to open WAL for bucket '{}': {}", bucket, e);
                        continue;
                    }
                }
            }
            let wal = wal_files.get_mut(bucket.as_str()).unwrap(); // safe: just inserted

            if let Err(e) = wal.append_batch(entries) {
                error!("WAL write error for bucket '{}': {}", bucket, e);
                return Err(e);
            }
        }

        info!("WAL flush: {} entries across {} buckets", count, by_bucket.len());
        Ok(count)
    }

    // =========================================================================
    // Compact: WAL → B+ tree
    // =========================================================================

    /// Compact a bucket's WAL entries into its B+ tree data file.
    /// After compaction, the WAL is truncated.
    pub fn compact_to_btree(&self, bucket_name: &str) -> io::Result<()> {
        // Read WAL entries
        let wal_entries = {
            let mut wal_files = self.wal_files.lock().expect("wal_files lock poisoned");
            if !wal_files.contains_key(bucket_name) {
                let wal = WalFile::open(&self.wal_path(bucket_name))?;
                wal_files.insert(bucket_name.to_string(), wal);
            }
            let wal = wal_files.get_mut(bucket_name).unwrap(); // safe: just inserted
            wal.read_all()?
        };

        if wal_entries.is_empty() {
            return Ok(());
        }

        // Open or create B+ tree
        let btree_path = self.btree_path(bucket_name);
        let mut btrees = self.btrees.lock().expect("btrees lock poisoned");
        if !btrees.contains_key(bucket_name) {
            let tree = BPlusTree::open(&btree_path)?;
            btrees.insert(bucket_name.to_string(), tree);
        }
        let btree = btrees.get_mut(bucket_name).unwrap(); // safe: just inserted

        // Apply WAL entries to B+ tree
        let mut applied = 0;
        for entry in &wal_entries {
            let key = entry.key.as_bytes();
            if entry.deleted {
                btree.delete(key)?;
            } else {
                let value = crate::storage::btree::serialize_value(&entry)?;
                btree.put(key, &value)?;
            }
            applied += 1;
        }

        // Flush B+ tree to disk
        btree.flush()?;

        // Truncate WAL
        let mut wal_files = self.wal_files.lock().expect("wal_files lock poisoned");
        if let Some(wal) = wal_files.get_mut(bucket_name) {
            wal.truncate()?;
        }

        info!(
            "Compacted {} WAL entries into B+ tree for bucket '{}'",
            applied, bucket_name
        );

        Ok(())
    }

    // =========================================================================
    // Full snapshot: write all documents to B+ tree (periodic)
    // =========================================================================

    /// Write a full snapshot of all documents into the B+ tree.
    /// This is used for periodic persistence and ensures the B+ tree
    /// has the latest state.
    pub fn write_snapshot(&self, bucket_name: &str, documents: &[Document]) -> io::Result<()> {
        let btree_path = self.btree_path(bucket_name);
        let mut btrees = self.btrees.lock().expect("btrees lock poisoned");
        if !btrees.contains_key(bucket_name) {
            let tree = BPlusTree::open(&btree_path)?;
            btrees.insert(bucket_name.to_string(), tree);
        }
        let btree = btrees.get_mut(bucket_name).unwrap(); // safe: just inserted

        for doc in documents {
            if doc.deleted {
                btree.delete(doc.key.as_bytes())?;
            } else {
                let entry = WalEntry::from_document(bucket_name, doc);
                let value = crate::storage::btree::serialize_value(&entry)?;
                btree.put(doc.key.as_bytes(), &value)?;
            }
        }

        btree.flush()?;

        // Truncate WAL since B+ tree is fully up to date
        let mut wal_files = self.wal_files.lock().expect("wal_files lock poisoned");
        if let Some(wal) = wal_files.get_mut(bucket_name) {
            wal.truncate()?;
        }

        info!(
            "Snapshot: {} documents written to B+ tree for bucket '{}'",
            documents.len(),
            bucket_name
        );
        Ok(())
    }

    // =========================================================================
    // Recovery: B+ tree + WAL → documents
    // =========================================================================

    /// Load all documents for a bucket from B+ tree + WAL replay.
    pub fn load_bucket(&self, bucket_name: &str) -> io::Result<Vec<Document>> {
        let mut docs: HashMap<String, Document> = HashMap::new();

        // Step 1: Load from B+ tree
        let btree_path = self.btree_path(bucket_name);
        if btree_path.exists() {
            let btree = BPlusTree::open(&btree_path)?;
            let entries = btree.scan_all();

            for (_, value_bytes) in &entries {
                match crate::storage::btree::deserialize_value::<WalEntry>(value_bytes) {
                    Ok(entry) => {
                        if !entry.deleted {
                            let doc = wal_entry_to_document(&entry);
                            docs.insert(entry.key.clone(), doc);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to deserialize B+ tree entry: {}", e);
                    }
                }
            }

            info!(
                "Recovery: loaded {} docs from B+ tree for '{}'",
                docs.len(),
                bucket_name
            );

            // Cache the btree
            let mut btrees = self.btrees.lock().expect("btrees lock poisoned");
            btrees.insert(bucket_name.to_string(), btree);
        }

        // Step 2: Replay WAL on top
        let wal_path = self.wal_path(bucket_name);
        if wal_path.exists() {
            let wal = WalFile::open(&wal_path)?;
            let wal_entries = wal.read_all()?;
            let wal_count = wal_entries.len();

            for entry in wal_entries {
                if entry.deleted {
                    docs.remove(&entry.key);
                } else {
                    let doc = wal_entry_to_document(&entry);
                    docs.insert(entry.key.clone(), doc);
                }
            }

            info!(
                "Recovery: replayed {} WAL entries for '{}'",
                wal_count, bucket_name
            );

            // Cache the WAL file
            let mut wal_files = self.wal_files.lock().expect("wal_files lock poisoned");
            wal_files.insert(bucket_name.to_string(), wal);
        }

        // Step 3: Try legacy format migration
        if docs.is_empty() {
            let legacy_docs = self.load_legacy(bucket_name)?;
            if !legacy_docs.is_empty() {
                info!(
                    "Recovery: migrated {} docs from legacy format for '{}'",
                    legacy_docs.len(),
                    bucket_name
                );
                return Ok(legacy_docs);
            }
        }

        Ok(docs.into_values().collect())
    }

    /// Load from legacy JSON format (migration support)
    fn load_legacy(&self, bucket_name: &str) -> io::Result<Vec<Document>> {
        let snapshot_path = self.legacy_snapshot_path(bucket_name);
        if !snapshot_path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&snapshot_path)?;
        let mut documents = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Document>(line) {
                Ok(doc) => documents.push(doc),
                Err(e) => {
                    warn!("Legacy snapshot parse error: {}", e);
                }
            }
        }

        Ok(documents)
    }

    // =========================================================================
    // Bucket lifecycle
    // =========================================================================

    /// List all buckets that have persistence data
    pub fn list_buckets(&self) -> io::Result<Vec<String>> {
        let mut buckets = std::collections::HashSet::new();
        if !self.data_dir.exists() {
            return Ok(Vec::new());
        }

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let filename = entry.file_name().to_string_lossy().to_string();
            if filename.ends_with(".nqdb") {
                buckets.insert(filename.trim_end_matches(".nqdb").to_string());
            } else if filename.ends_with(".wal") && !filename.ends_with(".wal.json") {
                buckets.insert(filename.trim_end_matches(".wal").to_string());
            } else if filename.ends_with(".snapshot.json") {
                // Legacy format
                buckets.insert(filename.trim_end_matches(".snapshot.json").to_string());
            }
        }

        Ok(buckets.into_iter().collect())
    }

    /// Delete all persistence data for a bucket
    pub fn delete_bucket_data(&self, bucket_name: &str) -> io::Result<()> {
        // Remove from caches
        {
            let mut btrees = self.btrees.lock().expect("btrees lock poisoned");
            btrees.remove(bucket_name);
        }
        {
            let mut wal_files = self.wal_files.lock().expect("wal_files lock poisoned");
            wal_files.remove(bucket_name);
        }

        // Delete files
        let paths = [
            self.btree_path(bucket_name),
            self.wal_path(bucket_name),
            self.config_path(bucket_name),
            self.data_dir.join(format!("{}.scopes.json", bucket_name)),
            self.legacy_snapshot_path(bucket_name),
            self.legacy_wal_path(bucket_name),
        ];
        for path in &paths {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    // =========================================================================
    // Stats / Diagnostics
    // =========================================================================

    /// Get write buffer statistics
    pub fn buffer_stats(&self) -> WriteBufferStats {
        let buf = self.write_buffer.lock().expect("write_buffer lock poisoned");
        buf.stats()
    }

    /// Get B+ tree statistics for a bucket
    #[allow(dead_code)]
    pub fn btree_stats(&self, bucket_name: &str) -> Option<BTreeStats> {
        let btrees = self.btrees.lock().expect("btrees lock poisoned");
        btrees.get(bucket_name).map(|bt| bt.stats())
    }

    /// Get persistence summary for all buckets
    pub fn summary(&self) -> PersistenceSummary {
        let buffer = self.buffer_stats();
        let btrees = self.btrees.lock().expect("btrees lock poisoned");
        let wal_files = self.wal_files.lock().expect("wal_files lock poisoned");

        let mut bucket_stats = Vec::new();
        for (name, btree) in btrees.iter() {
            let wal_size = wal_files
                .get(name)
                .map(|w| w.file_size())
                .unwrap_or(0);

            bucket_stats.push(BucketPersistenceInfo {
                bucket_name: name.clone(),
                btree: btree.stats(),
                wal_file_size_bytes: wal_size,
            });
        }

        PersistenceSummary {
            data_dir: self.data_dir.display().to_string(),
            storage_format: "B+ Tree (4KB pages, binary)".to_string(),
            wal_format: "Binary WAL with CRC32 checksums".to_string(),
            write_buffer: buffer,
            buckets: bucket_stats,
        }
    }
}

// =========================================================================
// Helper: Convert WalEntry → Document
// =========================================================================

fn wal_entry_to_document(entry: &WalEntry) -> Document {
    let timestamp = chrono::DateTime::from_timestamp(entry.timestamp, 0)
        .unwrap_or_else(|| chrono::Utc::now());

    Document {
        key: entry.key.clone(),
        value: entry.value.clone().unwrap_or(serde_json::Value::Null),
        cas: entry.cas,
        seq_no: entry.seq_no,
        rev_id: entry.rev_id,
        expiry: None,
        flags: entry.flags,
        created_at: timestamp,
        updated_at: timestamp,
        deleted: entry.deleted,
        source_cluster: None,
        vbucket_id: 0, // Will be recalculated on import
        xattrs: std::collections::HashMap::new(),
        last_accessed: timestamp,
        evicted: false,
    }
}

// =========================================================================
// Stats types
// =========================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketPersistenceInfo {
    pub bucket_name: String,
    pub btree: BTreeStats,
    pub wal_file_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceSummary {
    pub data_dir: String,
    pub storage_format: String,
    pub wal_format: String,
    pub write_buffer: WriteBufferStats,
    pub buckets: Vec<BucketPersistenceInfo>,
}
