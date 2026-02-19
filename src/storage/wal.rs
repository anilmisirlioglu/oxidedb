//! Write-Ahead Log (WAL) with buffered dual-trigger flush.
//!
//! Mutations accumulate in an in-memory buffer. The buffer is flushed to
//! the WAL file on disk when EITHER condition is met (whichever first):
//!
//!   1. Buffer size >= `max_buffer_ops` operations
//!   2. Buffer memory >= `max_buffer_bytes` bytes
//!   3. Time since last flush >= `flush_interval` duration
//!
//! This matches Couchbase's "dequeuer" pattern: high-throughput batching
//! with bounded latency.
//!
//! WAL file format (binary):
//!   [WAL Header: 32 bytes]
//!   [Entry 0: length-prefixed binary]
//!   [Entry 1: ...]
//!   ...
//!
//! Each entry:
//!   [entry_len: u32][entry_type: u8][key_len: u16][key][value_len: u32][value]
//!   [cas: u64][seq_no: u64][rev_id: u64][flags: u32][deleted: u8]
//!   [timestamp: i64][crc32: u32]

use crate::storage::document::Document;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info, warn};

/// WAL file magic
const WAL_MAGIC: &[u8; 4] = b"OXWL"; // OxideDB WAL

/// WAL format version
const WAL_VERSION: u32 = 1;

/// WAL header size
const WAL_HEADER_SIZE: usize = 32;

// =========================================================================
// WAL Entry (binary format)
// =========================================================================

/// Types of WAL entries
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalEntryType {
    Put = 1,
    Delete = 2,
}

/// A single WAL entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub entry_type: WalEntryType,
    pub bucket: String,
    pub key: String,
    pub value: Option<serde_json::Value>,
    pub cas: u64,
    pub seq_no: u64,
    pub rev_id: u64,
    pub flags: u32,
    pub deleted: bool,
    pub timestamp: i64,
}

impl WalEntry {
    /// Create a WAL entry from a document mutation
    pub fn from_document(bucket: &str, doc: &Document) -> Self {
        let entry_type = if doc.deleted {
            WalEntryType::Delete
        } else {
            WalEntryType::Put
        };
        Self {
            entry_type,
            bucket: bucket.to_string(),
            key: doc.key.clone(),
            value: if doc.deleted {
                None
            } else {
                Some(doc.value.clone())
            },
            cas: doc.cas,
            seq_no: doc.seq_no,
            rev_id: doc.rev_id,
            flags: doc.flags,
            deleted: doc.deleted,
            timestamp: doc.updated_at.timestamp(),
        }
    }

    /// Serialize entry to binary format
    fn to_bytes(&self) -> Vec<u8> {
        // Use JSON serialization for the entry (simpler, still efficient)
        let json = serde_json::to_vec(self).unwrap_or_default();
        let len = json.len() as u32;
        let crc = crc32fast::hash(&json);

        let mut buf = Vec::with_capacity(4 + json.len() + 4);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&json);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Deserialize entry from a reader
    fn from_reader<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        // Read length
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        if len == 0 || len > 64 * 1024 * 1024 {
            // Safety: max 64MB per entry
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid WAL entry length: {}", len),
            ));
        }

        // Read JSON data
        let mut json_buf = vec![0u8; len];
        reader.read_exact(&mut json_buf)?;

        // Read and verify CRC
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32fast::hash(&json_buf);

        if stored_crc != computed_crc {
            warn!(
                "WAL entry CRC mismatch: stored={:#x} computed={:#x}",
                stored_crc, computed_crc
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL entry CRC mismatch",
            ));
        }

        // Deserialize
        let entry: WalEntry = serde_json::from_slice(&json_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some(entry))
    }
}

// =========================================================================
// Write Buffer Configuration
// =========================================================================

/// Configuration for the WAL write buffer and flush triggers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBufferConfig {
    /// Max number of operations before triggering flush
    pub max_buffer_ops: usize,
    /// Max buffer memory in bytes before triggering flush
    pub max_buffer_bytes: usize,
    /// Max time between flushes in milliseconds
    pub flush_interval_ms: u64,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_ops: 5000,
            max_buffer_bytes: 4 * 1024 * 1024, // 4 MB
            flush_interval_ms: 1000,            // 1 second
        }
    }
}

// =========================================================================
// Write Buffer (in-memory accumulator)
// =========================================================================

/// In-memory write buffer that accumulates mutations before flushing to WAL.
///
/// The buffer tracks both operation count and byte size to support
/// dual-trigger flush policy.
pub struct WriteBuffer {
    config: WriteBufferConfig,
    /// Buffered entries waiting to be flushed
    entries: Vec<WalEntry>,
    /// Current buffer size in bytes (approximate)
    buffer_bytes: usize,
    /// Time of last flush
    last_flush: Instant,
    /// Total operations buffered (lifetime counter)
    total_buffered: AtomicU64,
    /// Total flushes performed
    total_flushes: AtomicU64,
}

impl WriteBuffer {
    pub fn new(config: WriteBufferConfig) -> Self {
        Self {
            config,
            entries: Vec::with_capacity(1024),
            buffer_bytes: 0,
            last_flush: Instant::now(),
            total_buffered: AtomicU64::new(0),
            total_flushes: AtomicU64::new(0),
        }
    }

    /// Add an entry to the buffer. Returns true if a flush should be triggered.
    pub fn push(&mut self, entry: WalEntry) -> bool {
        // Estimate size: key + value JSON + metadata overhead
        let entry_size = entry.key.len()
            + entry.bucket.len()
            + entry
                .value
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default().len())
                .unwrap_or(0)
            + 128; // overhead for other fields

        self.buffer_bytes += entry_size;
        self.entries.push(entry);
        self.total_buffered.fetch_add(1, Ordering::Relaxed);

        self.should_flush()
    }

    /// Check if the buffer should be flushed (any trigger met)
    pub fn should_flush(&self) -> bool {
        // Trigger 1: operation count
        if self.entries.len() >= self.config.max_buffer_ops {
            debug!(
                "WAL flush trigger: ops ({} >= {})",
                self.entries.len(),
                self.config.max_buffer_ops
            );
            return true;
        }

        // Trigger 2: buffer size
        if self.buffer_bytes >= self.config.max_buffer_bytes {
            debug!(
                "WAL flush trigger: bytes ({} >= {})",
                self.buffer_bytes, self.config.max_buffer_bytes
            );
            return true;
        }

        // Trigger 3: time elapsed
        if self.last_flush.elapsed().as_millis() >= self.config.flush_interval_ms as u128 {
            if !self.entries.is_empty() {
                debug!(
                    "WAL flush trigger: interval ({}ms elapsed)",
                    self.last_flush.elapsed().as_millis()
                );
                return true;
            }
        }

        false
    }

    /// Drain all entries from the buffer (called during flush)
    pub fn drain(&mut self) -> Vec<WalEntry> {
        self.buffer_bytes = 0;
        self.last_flush = Instant::now();
        self.total_flushes.fetch_add(1, Ordering::Relaxed);
        std::mem::take(&mut self.entries)
    }

    /// Get number of pending entries
    pub fn pending_count(&self) -> usize {
        self.entries.len()
    }

    /// Get current buffer size in bytes
    #[allow(dead_code)]
    pub fn pending_bytes(&self) -> usize {
        self.buffer_bytes
    }

    /// Get time since last flush in milliseconds
    #[allow(dead_code)]
    pub fn ms_since_flush(&self) -> u128 {
        self.last_flush.elapsed().as_millis()
    }

    /// Get lifetime stats
    pub fn stats(&self) -> WriteBufferStats {
        WriteBufferStats {
            pending_ops: self.entries.len(),
            pending_bytes: self.buffer_bytes,
            ms_since_flush: self.last_flush.elapsed().as_millis() as u64,
            total_buffered: self.total_buffered.load(Ordering::Relaxed),
            total_flushes: self.total_flushes.load(Ordering::Relaxed),
            config_max_ops: self.config.max_buffer_ops,
            config_max_bytes: self.config.max_buffer_bytes,
            config_flush_interval_ms: self.config.flush_interval_ms,
        }
    }
}

/// Statistics about the write buffer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBufferStats {
    pub pending_ops: usize,
    pub pending_bytes: usize,
    pub ms_since_flush: u64,
    pub total_buffered: u64,
    pub total_flushes: u64,
    pub config_max_ops: usize,
    pub config_max_bytes: usize,
    pub config_flush_interval_ms: u64,
}

// =========================================================================
// WAL File Manager
// =========================================================================

/// Manages WAL files on disk with append-only writes.
pub struct WalFile {
    path: PathBuf,
    /// Current WAL file size
    file_size: u64,
    /// Number of entries in current WAL file
    entry_count: u64,
}

impl WalFile {
    /// Open or create a WAL file
    pub fn open(path: &Path) -> io::Result<Self> {
        if path.exists() {
            let metadata = fs::metadata(path)?;
            Ok(Self {
                path: path.to_path_buf(),
                file_size: metadata.len(),
                entry_count: 0, // Will be counted on load
            })
        } else {
            // Create with header
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = fs::File::create(path)?;
            let header = Self::create_header();
            file.write_all(&header)?;
            file.flush()?;

            Ok(Self {
                path: path.to_path_buf(),
                file_size: WAL_HEADER_SIZE as u64,
                entry_count: 0,
            })
        }
    }

    /// Append a batch of entries to the WAL file (single fsync)
    pub fn append_batch(&mut self, entries: &[WalEntry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut file = fs::OpenOptions::new().append(true).open(&self.path)?;

        {
            let mut writer = BufWriter::with_capacity(64 * 1024, &mut file);
            for entry in entries {
                let bytes = entry.to_bytes();
                writer.write_all(&bytes)?;
                self.entry_count += 1;
            }
            writer.flush()?;
        }
        // BufWriter is dropped here, releasing the mutable borrow on file
        file.sync_all()?; // fsync for durability

        self.file_size = fs::metadata(&self.path)?.len();

        debug!(
            "WAL: appended {} entries (file size: {} bytes)",
            entries.len(),
            self.file_size
        );

        Ok(())
    }

    /// Read all entries from the WAL file
    pub fn read_all(&self) -> io::Result<Vec<WalEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let mut file = fs::File::open(&self.path)?;

        // Verify and skip header
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        match file.read_exact(&mut header_buf) {
            Ok(_) => {
                if &header_buf[0..4] != WAL_MAGIC {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid WAL file magic",
                    ));
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(Vec::new()),
            Err(e) => return Err(e),
        }

        let mut entries = Vec::new();
        loop {
            match WalEntry::from_reader(&mut file) {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => break,
                Err(e) => {
                    warn!("WAL read error (truncated?): {}", e);
                    break; // Stop at first corrupt entry
                }
            }
        }

        info!("WAL: read {} entries from {}", entries.len(), self.path.display());
        Ok(entries)
    }

    /// Truncate the WAL file (after compaction into B+ tree)
    pub fn truncate(&mut self) -> io::Result<()> {
        let mut file = fs::File::create(&self.path)?;
        let header = Self::create_header();
        file.write_all(&header)?;
        file.flush()?;
        self.file_size = WAL_HEADER_SIZE as u64;
        self.entry_count = 0;
        info!("WAL truncated: {}", self.path.display());
        Ok(())
    }

    /// Delete the WAL file
    #[allow(dead_code)]
    pub fn delete(&self) -> io::Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// Get WAL file size
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get path
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn create_header() -> [u8; WAL_HEADER_SIZE] {
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(WAL_MAGIC);
        header[4..8].copy_from_slice(&WAL_VERSION.to_le_bytes());
        // Rest is reserved
        header
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_wal_path() -> std::path::PathBuf {
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("test_wal_{}.wal", id))
    }

    fn make_entry(key: &str, value: &str) -> WalEntry {
        WalEntry {
            entry_type: WalEntryType::Put,
            key: key.to_string(),
            bucket: "test".to_string(),
            value: Some(serde_json::json!(value)),
            cas: 1,
            seq_no: 1,
            rev_id: 1,
            flags: 0,
            deleted: false,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    #[test]
    fn test_write_buffer_push_and_drain() {
        let config = WriteBufferConfig::default();
        let mut buf = WriteBuffer::new(config);

        assert_eq!(buf.pending_count(), 0);
        buf.push(make_entry("k1", "v1"));
        buf.push(make_entry("k2", "v2"));
        assert_eq!(buf.pending_count(), 2);

        let entries = buf.drain();
        assert_eq!(entries.len(), 2);
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_write_buffer_flush_trigger() {
        let config = WriteBufferConfig {
            max_buffer_ops: 2,
            max_buffer_bytes: 1024 * 1024,
            flush_interval_ms: 10_000,
        };
        let mut buf = WriteBuffer::new(config);

        let first = buf.push(make_entry("k1", "v1"));
        assert!(!first); // not full yet
        let second = buf.push(make_entry("k2", "v2"));
        assert!(second); // should trigger flush at 2 ops
    }

    #[test]
    fn test_wal_file_write_and_read() {
        let path = temp_wal_path();
        let mut wal = WalFile::open(&path).unwrap();

        let entries = vec![make_entry("k1", "v1"), make_entry("k2", "v2")];
        wal.append_batch(&entries).unwrap();

        let read_back = wal.read_all().unwrap();
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0].key, "k1");
        assert_eq!(read_back[1].key, "k2");

        // Cleanup
        drop(wal);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_wal_file_truncate() {
        let path = temp_wal_path();
        let mut wal = WalFile::open(&path).unwrap();

        let entries = vec![make_entry("k1", "v1")];
        wal.append_batch(&entries).unwrap();
        assert!(!wal.read_all().unwrap().is_empty());

        wal.truncate().unwrap();
        assert!(wal.read_all().unwrap().is_empty());

        drop(wal);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_wal_entry_from_document() {
        let now = chrono::Utc::now();
        let doc = crate::storage::document::Document {
            key: "mykey".to_string(),
            value: serde_json::json!({"hello": "world"}),
            cas: 42,
            seq_no: 7,
            rev_id: 3,
            expiry: None,
            flags: 0,
            created_at: now,
            updated_at: now,
            deleted: false,
            source_cluster: None,
            vbucket_id: 5,
            xattrs: std::collections::HashMap::new(),
            last_accessed: now,
            evicted: false,
        };

        let entry = WalEntry::from_document("mybucket", &doc);
        assert_eq!(entry.key, "mykey");
        assert_eq!(entry.bucket, "mybucket");
        assert_eq!(entry.cas, 42);
        assert!(!entry.deleted);
    }
}
