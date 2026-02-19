//! On-disk B+ Tree storage engine.
//!
//! Provides a page-based B+ tree that stores key → value pairs.
//! Used as the primary on-disk format for persistent buckets,
//! similar to Couchbase's Couchstore (B-tree backed).
//!
//! File format:
//!   [FileHeader: 64 bytes]
//!   [Page 0: 4096 bytes]
//!   [Page 1: 4096 bytes]
//!   ...
//!   [Page N: 4096 bytes]
//!
//! Page types:
//!   Internal: sorted keys + child page pointers
//!   Leaf: sorted keys + serialized values

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::info;

/// Page size in bytes (4 KB)
pub const PAGE_SIZE: usize = 4096;

/// File header size
const HEADER_SIZE: usize = 64;

/// Magic bytes for the file format
const MAGIC: &[u8; 4] = b"OXBT"; // OxideDB B+Tree

/// File format version
const FORMAT_VERSION: u32 = 1;

/// Maximum key size in bytes
const MAX_KEY_SIZE: usize = 250;

/// Maximum entries per leaf page (calculated to fit in PAGE_SIZE)
/// Each leaf entry: key_len(2) + key(var, avg ~32) + val_len(4) + value(var)
/// Header: 13 bytes; usable: 4083 bytes
/// With average entry ~200 bytes → ~20 entries per leaf
const LEAF_SPLIT_THRESHOLD: usize = 20;

/// Maximum entries per internal page
/// Each internal entry: key_len(2) + key(var, avg ~32) + child_ptr(4) = ~38 bytes
/// ~100 entries per internal page
const INTERNAL_SPLIT_THRESHOLD: usize = 100;

// =========================================================================
// File Header
// =========================================================================

#[derive(Debug, Clone)]
struct FileHeader {
    magic: [u8; 4],
    version: u32,
    root_page: u32,
    page_count: u32,
    record_count: u64,
    tree_height: u32,
}

impl FileHeader {
    fn new() -> Self {
        Self {
            magic: *MAGIC,
            version: FORMAT_VERSION,
            root_page: 0,
            page_count: 0,
            record_count: 0,
            tree_height: 0,
        }
    }

    fn serialize(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.root_page.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        buf[24..28].copy_from_slice(&self.tree_height.to_le_bytes());
        buf
    }

    fn deserialize(buf: &[u8; HEADER_SIZE]) -> io::Result<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);
        if &magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid B+ tree file magic",
            ));
        }
        let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let root_page = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let page_count = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let record_count = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        let tree_height = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]);
        Ok(Self {
            magic,
            version,
            root_page,
            page_count,
            record_count,
            tree_height,
        })
    }
}

// =========================================================================
// Page Types
// =========================================================================

/// Page type tag
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum PageType {
    Internal = 1,
    Leaf = 2,
}

/// An internal (branch) node page
#[derive(Debug, Clone)]
struct InternalPage {
    /// Sorted separator keys
    keys: Vec<Vec<u8>>,
    /// Child page IDs (keys.len() + 1 children)
    children: Vec<u32>,
}

/// A leaf node page
#[derive(Debug, Clone)]
struct LeafPage {
    /// Sorted keys
    keys: Vec<Vec<u8>>,
    /// Values corresponding to each key
    values: Vec<Vec<u8>>,
    /// Next leaf page (linked list for range scans), 0 = none
    next_leaf: u32,
}

/// A B+ tree page
#[derive(Debug, Clone)]
enum BTreePage {
    Internal(InternalPage),
    Leaf(LeafPage),
}

impl BTreePage {
    /// Serialize a page into a fixed-size byte buffer
    fn serialize(&self) -> io::Result<[u8; PAGE_SIZE]> {
        let mut buf = [0u8; PAGE_SIZE];
        let mut pos = 0;

        match self {
            BTreePage::Internal(page) => {
                buf[pos] = PageType::Internal as u8;
                pos += 1;
                // num_keys: u16
                let nk = page.keys.len() as u16;
                buf[pos..pos + 2].copy_from_slice(&nk.to_le_bytes());
                pos += 2;
                // num_children: u16
                let nc = page.children.len() as u16;
                buf[pos..pos + 2].copy_from_slice(&nc.to_le_bytes());
                pos += 2;
                // reserved: 8 bytes
                pos += 8;
                // Header total: 13 bytes

                // Write children first (fixed size section)
                for &child in &page.children {
                    if pos + 4 > PAGE_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Internal page overflow (children)",
                        ));
                    }
                    buf[pos..pos + 4].copy_from_slice(&child.to_le_bytes());
                    pos += 4;
                }

                // Write keys
                for key in &page.keys {
                    let klen = key.len() as u16;
                    if pos + 2 + key.len() > PAGE_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Internal page overflow (keys)",
                        ));
                    }
                    buf[pos..pos + 2].copy_from_slice(&klen.to_le_bytes());
                    pos += 2;
                    buf[pos..pos + key.len()].copy_from_slice(key);
                    pos += key.len();
                }
            }
            BTreePage::Leaf(page) => {
                buf[pos] = PageType::Leaf as u8;
                pos += 1;
                // num_entries: u16
                let ne = page.keys.len() as u16;
                buf[pos..pos + 2].copy_from_slice(&ne.to_le_bytes());
                pos += 2;
                // next_leaf: u32
                buf[pos..pos + 4].copy_from_slice(&page.next_leaf.to_le_bytes());
                pos += 4;
                // reserved: 6 bytes
                pos += 6;
                // Header total: 13 bytes

                // Write entries: key_len(2) + key + value_len(4) + value
                for (key, value) in page.keys.iter().zip(page.values.iter()) {
                    let klen = key.len() as u16;
                    let vlen = value.len() as u32;
                    let entry_size = 2 + key.len() + 4 + value.len();
                    if pos + entry_size > PAGE_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "Leaf page overflow: need {} bytes at pos {}, page size {}",
                                entry_size, pos, PAGE_SIZE
                            ),
                        ));
                    }
                    buf[pos..pos + 2].copy_from_slice(&klen.to_le_bytes());
                    pos += 2;
                    buf[pos..pos + key.len()].copy_from_slice(key);
                    pos += key.len();
                    buf[pos..pos + 4].copy_from_slice(&vlen.to_le_bytes());
                    pos += 4;
                    buf[pos..pos + value.len()].copy_from_slice(value);
                    pos += value.len();
                }
            }
        }

        Ok(buf)
    }

    /// Deserialize a page from a byte buffer
    fn deserialize(buf: &[u8; PAGE_SIZE]) -> io::Result<Self> {
        let page_type = buf[0];
        match page_type {
            1 => {
                // Internal
                let nk = u16::from_le_bytes([buf[1], buf[2]]) as usize;
                let nc = u16::from_le_bytes([buf[3], buf[4]]) as usize;
                let mut pos = 13; // skip header

                let mut children = Vec::with_capacity(nc);
                for _ in 0..nc {
                    let child = u32::from_le_bytes([
                        buf[pos],
                        buf[pos + 1],
                        buf[pos + 2],
                        buf[pos + 3],
                    ]);
                    children.push(child);
                    pos += 4;
                }

                let mut keys = Vec::with_capacity(nk);
                for _ in 0..nk {
                    let klen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                    pos += 2;
                    keys.push(buf[pos..pos + klen].to_vec());
                    pos += klen;
                }

                Ok(BTreePage::Internal(InternalPage { keys, children }))
            }
            2 => {
                // Leaf
                let ne = u16::from_le_bytes([buf[1], buf[2]]) as usize;
                let next_leaf = u32::from_le_bytes([buf[3], buf[4], buf[5], buf[6]]);
                let mut pos = 13;

                let mut keys = Vec::with_capacity(ne);
                let mut values = Vec::with_capacity(ne);

                for _ in 0..ne {
                    let klen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                    pos += 2;
                    keys.push(buf[pos..pos + klen].to_vec());
                    pos += klen;

                    let vlen = u32::from_le_bytes([
                        buf[pos],
                        buf[pos + 1],
                        buf[pos + 2],
                        buf[pos + 3],
                    ]) as usize;
                    pos += 4;
                    values.push(buf[pos..pos + vlen].to_vec());
                    pos += vlen;
                }

                Ok(BTreePage::Leaf(LeafPage {
                    keys,
                    values,
                    next_leaf,
                }))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown page type: {}", page_type),
            )),
        }
    }
}

// =========================================================================
// B+ Tree (on-disk)
// =========================================================================

/// On-disk B+ tree providing ordered key-value storage.
///
/// This is the primary persistent storage format, similar to Couchbase's
/// Couchstore B-tree. Pages are 4KB, keys are sorted, and leaf pages
/// are linked for efficient range scans.
#[allow(dead_code)]
pub struct BPlusTree {
    path: PathBuf,
    header: FileHeader,
    /// Page cache: page_id → page data (in-memory for fast access)
    cache: BTreeMap<u32, BTreePage>,
    dirty_pages: Vec<u32>,
}

#[allow(dead_code)]
impl BPlusTree {
    /// Create a new B+ tree file or open an existing one
    pub fn open(path: &Path) -> io::Result<Self> {
        if path.exists() {
            Self::open_existing(path)
        } else {
            Self::create_new(path)
        }
    }

    /// Create a new empty B+ tree file
    fn create_new(path: &Path) -> io::Result<Self> {
        // Create parent directory
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let header = FileHeader::new();

        // Create empty root leaf page
        let root_leaf = BTreePage::Leaf(LeafPage {
            keys: Vec::new(),
            values: Vec::new(),
            next_leaf: 0,
        });

        let mut tree = Self {
            path: path.to_path_buf(),
            header,
            cache: BTreeMap::new(),
            dirty_pages: Vec::new(),
        };

        tree.header.root_page = 0;
        tree.header.page_count = 1;
        tree.header.tree_height = 1;
        tree.cache.insert(0, root_leaf);
        tree.dirty_pages.push(0);

        // Flush to create the file
        tree.flush()?;

        info!("B+ tree created: {}", path.display());
        Ok(tree)
    }

    /// Open an existing B+ tree file
    fn open_existing(path: &Path) -> io::Result<Self> {
        let mut file = fs::File::open(path)?;

        // Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = FileHeader::deserialize(&header_buf)?;

        // Load all pages into cache
        let mut cache = BTreeMap::new();
        for page_id in 0..header.page_count {
            let offset = HEADER_SIZE as u64 + (page_id as u64 * PAGE_SIZE as u64);
            file.seek(SeekFrom::Start(offset))?;
            let mut page_buf = [0u8; PAGE_SIZE];
            file.read_exact(&mut page_buf)?;
            let page = BTreePage::deserialize(&page_buf)?;
            cache.insert(page_id, page);
        }

        info!(
            "B+ tree opened: {} ({} pages, {} records, height {})",
            path.display(),
            header.page_count,
            header.record_count,
            header.tree_height
        );

        Ok(Self {
            path: path.to_path_buf(),
            header,
            cache,
            dirty_pages: Vec::new(),
        })
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let leaf_page_id = self.find_leaf(key);
        if let Some(BTreePage::Leaf(leaf)) = self.cache.get(&leaf_page_id) {
            // Binary search in sorted keys
            match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => Some(leaf.values[idx].clone()),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    /// Insert or update a key-value pair
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if key.len() > MAX_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Key too large: {} bytes (max {})", key.len(), MAX_KEY_SIZE),
            ));
        }

        let leaf_page_id = self.find_leaf(key);

        // Insert into leaf
        let (need_split, is_new) = {
            let leaf = self.get_leaf_mut(leaf_page_id)?;
            match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => {
                    // Update existing
                    leaf.values[idx] = value.to_vec();
                    (false, false)
                }
                Err(idx) => {
                    // Insert new
                    leaf.keys.insert(idx, key.to_vec());
                    leaf.values.insert(idx, value.to_vec());
                    let should_split = leaf.keys.len() > LEAF_SPLIT_THRESHOLD;
                    (should_split, true)
                }
            }
        };
        if is_new {
            self.header.record_count += 1;
        }
        self.mark_dirty(leaf_page_id);

        if need_split {
            self.split_leaf(leaf_page_id)?;
        }

        Ok(())
    }

    /// Delete a key
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        let leaf_page_id = self.find_leaf(key);
        let found = {
            let leaf = self.get_leaf_mut(leaf_page_id)?;
            match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => {
                    leaf.keys.remove(idx);
                    leaf.values.remove(idx);
                    self.header.record_count = self.header.record_count.saturating_sub(1);
                    true
                }
                Err(_) => false,
            }
        };
        if found {
            self.mark_dirty(leaf_page_id);
        }
        Ok(found)
    }

    /// Scan all key-value pairs (in sorted order)
    pub fn scan_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();
        let first_leaf = self.find_leftmost_leaf();
        let mut current = Some(first_leaf);

        while let Some(page_id) = current {
            if let Some(BTreePage::Leaf(leaf)) = self.cache.get(&page_id) {
                for (k, v) in leaf.keys.iter().zip(leaf.values.iter()) {
                    results.push((k.clone(), v.clone()));
                }
                current = if leaf.next_leaf == 0 {
                    None
                } else {
                    Some(leaf.next_leaf)
                };
            } else {
                break;
            }
        }

        results
    }

    /// Get record count
    pub fn record_count(&self) -> u64 {
        self.header.record_count
    }

    /// Get page count
    pub fn page_count(&self) -> u32 {
        self.header.page_count
    }

    /// Get tree height
    pub fn tree_height(&self) -> u32 {
        self.header.tree_height
    }

    /// Flush all dirty pages to disk
    pub fn flush(&mut self) -> io::Result<()> {
        let temp_path = self.path.with_extension("nqdb.tmp");
        let mut file = fs::File::create(&temp_path)?;

        // Write header
        let header_buf = self.header.serialize();
        file.write_all(&header_buf)?;

        // Write all pages in order
        for page_id in 0..self.header.page_count {
            if let Some(page) = self.cache.get(&page_id) {
                let page_buf = page.serialize()?;
                file.write_all(&page_buf)?;
            } else {
                // Empty page (shouldn't happen)
                file.write_all(&[0u8; PAGE_SIZE])?;
            }
        }

        file.flush()?;
        file.sync_all()?;

        // Atomic rename
        fs::rename(&temp_path, &self.path)?;
        self.dirty_pages.clear();

        Ok(())
    }

    /// Build a B+ tree from a sorted iterator of key-value pairs (bulk load)
    pub fn bulk_load(path: &Path, data: &[(Vec<u8>, Vec<u8>)]) -> io::Result<Self> {
        let mut tree = Self::create_new(path)?;
        for (key, value) in data {
            tree.put(key, value)?;
        }
        tree.flush()?;
        Ok(tree)
    }

    // ---- Internal helpers ----

    /// Find the leaf page that should contain the given key
    fn find_leaf(&self, key: &[u8]) -> u32 {
        let mut current = self.header.root_page;

        loop {
            match self.cache.get(&current) {
                Some(BTreePage::Leaf(_)) => return current,
                Some(BTreePage::Internal(page)) => {
                    // Find the child to descend into
                    let idx = match page.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                        Ok(i) => i + 1,  // Key found: go right
                        Err(i) => i,     // Key not found: insertion point
                    };
                    if idx < page.children.len() {
                        current = page.children[idx];
                    } else {
                        // Shouldn't happen in a well-formed tree
                        return current;
                    }
                }
                None => return current,
            }
        }
    }

    /// Find the leftmost leaf page (for scans)
    fn find_leftmost_leaf(&self) -> u32 {
        let mut current = self.header.root_page;
        loop {
            match self.cache.get(&current) {
                Some(BTreePage::Leaf(_)) => return current,
                Some(BTreePage::Internal(page)) => {
                    if page.children.is_empty() {
                        return current;
                    }
                    current = page.children[0];
                }
                None => return current,
            }
        }
    }

    /// Get a mutable reference to a leaf page
    fn get_leaf_mut(&mut self, page_id: u32) -> io::Result<&mut LeafPage> {
        match self.cache.get_mut(&page_id) {
            Some(BTreePage::Leaf(leaf)) => Ok(leaf),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Leaf page {} not found", page_id),
            )),
        }
    }

    /// Allocate a new page
    fn alloc_page(&mut self) -> u32 {
        let page_id = self.header.page_count;
        self.header.page_count += 1;
        page_id
    }

    /// Mark a page as dirty
    fn mark_dirty(&mut self, page_id: u32) {
        if !self.dirty_pages.contains(&page_id) {
            self.dirty_pages.push(page_id);
        }
    }

    /// Split a leaf page that has exceeded its capacity
    fn split_leaf(&mut self, page_id: u32) -> io::Result<()> {
        // Extract data from the full leaf
        let (left_keys, left_values, right_keys, right_values, split_key, old_next) = {
            let leaf = self.get_leaf_mut(page_id)?;
            let mid = leaf.keys.len() / 2;

            let right_keys = leaf.keys.split_off(mid);
            let right_values = leaf.values.split_off(mid);
            let split_key = right_keys[0].clone();
            let old_next = leaf.next_leaf;

            (
                leaf.keys.clone(),
                leaf.values.clone(),
                right_keys,
                right_values,
                split_key,
                old_next,
            )
        };

        // Create new right leaf page
        let new_page_id = self.alloc_page();
        let new_leaf = BTreePage::Leaf(LeafPage {
            keys: right_keys,
            values: right_values,
            next_leaf: old_next,
        });
        self.cache.insert(new_page_id, new_leaf);
        self.mark_dirty(new_page_id);

        // Update left leaf
        {
            let leaf = self.get_leaf_mut(page_id)?;
            leaf.keys = left_keys;
            leaf.values = left_values;
            leaf.next_leaf = new_page_id;
        }
        self.mark_dirty(page_id);

        // Insert split key into parent
        if page_id == self.header.root_page {
            // Create new root
            let new_root_id = self.alloc_page();
            let new_root = BTreePage::Internal(InternalPage {
                keys: vec![split_key],
                children: vec![page_id, new_page_id],
            });
            self.cache.insert(new_root_id, new_root);
            self.mark_dirty(new_root_id);
            self.header.root_page = new_root_id;
            self.header.tree_height += 1;
        } else {
            self.insert_into_parent(page_id, split_key, new_page_id)?;
        }

        Ok(())
    }

    /// Insert a separator key into a parent internal node after a child split
    fn insert_into_parent(
        &mut self,
        left_child: u32,
        key: Vec<u8>,
        right_child: u32,
    ) -> io::Result<()> {
        // Find parent by searching from root
        let parent_id = self.find_parent(self.header.root_page, left_child);

        if let Some(parent_id) = parent_id {
            let need_split = {
                if let Some(BTreePage::Internal(parent)) = self.cache.get_mut(&parent_id) {
                    // Find position for the new key
                    let idx = match parent
                        .keys
                        .binary_search_by(|k| k.as_slice().cmp(&key))
                    {
                        Ok(i) => i,
                        Err(i) => i,
                    };
                    parent.keys.insert(idx, key.clone());
                    parent.children.insert(idx + 1, right_child);
                    parent.keys.len() > INTERNAL_SPLIT_THRESHOLD
                } else {
                    false
                }
            };
            self.mark_dirty(parent_id);

            if need_split {
                self.split_internal(parent_id)?;
            }
        }

        Ok(())
    }

    /// Split an internal node
    fn split_internal(&mut self, page_id: u32) -> io::Result<()> {
        let (left_keys, left_children, promote_key, right_keys, right_children) = {
            if let Some(BTreePage::Internal(page)) = self.cache.get_mut(&page_id) {
                let mid = page.keys.len() / 2;
                let promote_key = page.keys[mid].clone();

                let right_keys = page.keys.split_off(mid + 1);
                page.keys.pop(); // Remove promoted key
                let right_children = page.children.split_off(mid + 1);

                (
                    page.keys.clone(),
                    page.children.clone(),
                    promote_key,
                    right_keys,
                    right_children,
                )
            } else {
                return Ok(());
            }
        };

        // Create new right internal page
        let new_page_id = self.alloc_page();
        let new_internal = BTreePage::Internal(InternalPage {
            keys: right_keys,
            children: right_children,
        });
        self.cache.insert(new_page_id, new_internal);
        self.mark_dirty(new_page_id);

        // Update left page
        if let Some(BTreePage::Internal(page)) = self.cache.get_mut(&page_id) {
            page.keys = left_keys;
            page.children = left_children;
        }
        self.mark_dirty(page_id);

        // Insert promoted key into parent
        if page_id == self.header.root_page {
            let new_root_id = self.alloc_page();
            let new_root = BTreePage::Internal(InternalPage {
                keys: vec![promote_key],
                children: vec![page_id, new_page_id],
            });
            self.cache.insert(new_root_id, new_root);
            self.mark_dirty(new_root_id);
            self.header.root_page = new_root_id;
            self.header.tree_height += 1;
        } else {
            self.insert_into_parent(page_id, promote_key, new_page_id)?;
        }

        Ok(())
    }

    /// Find the parent of a given page by DFS from root
    fn find_parent(&self, current: u32, target_child: u32) -> Option<u32> {
        if let Some(BTreePage::Internal(page)) = self.cache.get(&current) {
            if page.children.contains(&target_child) {
                return Some(current);
            }
            for &child in &page.children {
                if let Some(parent) = self.find_parent(child, target_child) {
                    return Some(parent);
                }
            }
        }
        None
    }
}

// =========================================================================
// Serialization helpers for documents stored in B+ tree values
// =========================================================================

/// Compression header byte for B+ tree stored values
const COMPRESSION_NONE: u8 = 0x00;
const COMPRESSION_SNAPPY: u8 = 0x01;

/// Minimum value size to apply compression (smaller values don't benefit)
const COMPRESSION_THRESHOLD: usize = 64;

/// Serialize a value for B+ tree storage with optional Snappy compression.
/// Format: [compression_byte] [data...]
///   - 0x00: uncompressed JSON
///   - 0x01: Snappy-compressed JSON
pub fn serialize_value<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let json = serde_json::to_vec(value)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    if json.len() >= COMPRESSION_THRESHOLD {
        // Try Snappy compression
        match snap::raw::Encoder::new().compress_vec(&json) {
            Ok(compressed) if compressed.len() + 1 < json.len() => {
                // Compression is beneficial
                let mut result = Vec::with_capacity(1 + compressed.len());
                result.push(COMPRESSION_SNAPPY);
                result.extend_from_slice(&compressed);
                return Ok(result);
            }
            _ => {} // Compression didn't help or failed — store uncompressed
        }
    }

    // Store uncompressed with header byte
    let mut result = Vec::with_capacity(1 + json.len());
    result.push(COMPRESSION_NONE);
    result.extend_from_slice(&json);
    Ok(result)
}

/// Deserialize a value from B+ tree storage (handles both compressed and legacy formats).
/// Backward-compatible: old data without compression header is treated as raw JSON.
pub fn deserialize_value<T: for<'de> Deserialize<'de>>(data: &[u8]) -> io::Result<T> {
    if data.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty value"));
    }

    match data[0] {
        COMPRESSION_NONE => {
            // Uncompressed with header
            serde_json::from_slice(&data[1..])
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
        }
        COMPRESSION_SNAPPY => {
            // Snappy-compressed with header
            let decompressed = snap::raw::Decoder::new()
                .decompress_vec(&data[1..])
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Snappy decompress: {}", e)))?;
            serde_json::from_slice(&decompressed)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
        }
        _ => {
            // Legacy format: no compression header, raw JSON
            // (backward compat with pre-compression data)
            serde_json::from_slice(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
        }
    }
}

// =========================================================================
// B+ Tree statistics
// =========================================================================

/// Statistics about a B+ tree file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeStats {
    pub file_path: String,
    pub file_size_bytes: u64,
    pub page_count: u32,
    pub record_count: u64,
    pub tree_height: u32,
    pub page_size: usize,
    pub format_version: u32,
}

impl BPlusTree {
    /// Get statistics about this B+ tree
    pub fn stats(&self) -> BTreeStats {
        let file_size = fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0);

        BTreeStats {
            file_path: self.path.display().to_string(),
            file_size_bytes: file_size,
            page_count: self.header.page_count,
            record_count: self.header.record_count,
            tree_height: self.header.tree_height,
            page_size: PAGE_SIZE,
            format_version: self.header.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        PathBuf::from(format!("/tmp/oxidedb_btree_test_{}.nqdb", name))
    }

    #[test]
    fn test_create_and_open() {
        let path = temp_path("create_open");
        let _ = fs::remove_file(&path);

        let tree = BPlusTree::open(&path).unwrap();
        assert_eq!(tree.record_count(), 0);
        assert_eq!(tree.tree_height(), 1);
        drop(tree);

        let tree = BPlusTree::open(&path).unwrap();
        assert_eq!(tree.record_count(), 0);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_put_get() {
        let path = temp_path("put_get");
        let _ = fs::remove_file(&path);

        let mut tree = BPlusTree::open(&path).unwrap();
        tree.put(b"hello", b"world").unwrap();
        tree.put(b"foo", b"bar").unwrap();
        tree.flush().unwrap();

        assert_eq!(tree.get(b"hello"), Some(b"world".to_vec()));
        assert_eq!(tree.get(b"foo"), Some(b"bar".to_vec()));
        assert_eq!(tree.get(b"missing"), None);
        assert_eq!(tree.record_count(), 2);

        // Reopen and verify persistence
        drop(tree);
        let tree = BPlusTree::open(&path).unwrap();
        assert_eq!(tree.get(b"hello"), Some(b"world".to_vec()));
        assert_eq!(tree.get(b"foo"), Some(b"bar".to_vec()));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_update() {
        let path = temp_path("update");
        let _ = fs::remove_file(&path);

        let mut tree = BPlusTree::open(&path).unwrap();
        tree.put(b"key", b"value1").unwrap();
        assert_eq!(tree.get(b"key"), Some(b"value1".to_vec()));

        tree.put(b"key", b"value2").unwrap();
        assert_eq!(tree.get(b"key"), Some(b"value2".to_vec()));
        // Record count should not increase on update
        assert_eq!(tree.record_count(), 1);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_delete() {
        let path = temp_path("delete");
        let _ = fs::remove_file(&path);

        let mut tree = BPlusTree::open(&path).unwrap();
        tree.put(b"key1", b"val1").unwrap();
        tree.put(b"key2", b"val2").unwrap();

        assert!(tree.delete(b"key1").unwrap());
        assert!(!tree.delete(b"missing").unwrap());
        assert_eq!(tree.get(b"key1"), None);
        assert_eq!(tree.get(b"key2"), Some(b"val2".to_vec()));
        assert_eq!(tree.record_count(), 1);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_many_inserts_and_splits() {
        let path = temp_path("splits");
        let _ = fs::remove_file(&path);

        let mut tree = BPlusTree::open(&path).unwrap();

        // Insert enough records to trigger multiple splits
        for i in 0..100 {
            let key = format!("key-{:05}", i);
            let value = format!("value-{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        tree.flush().unwrap();

        assert_eq!(tree.record_count(), 100);
        assert!(tree.tree_height() >= 2, "Expected tree height >= 2, got {}", tree.tree_height());

        // Verify all records
        for i in 0..100 {
            let key = format!("key-{:05}", i);
            let expected = format!("value-{}", i);
            assert_eq!(
                tree.get(key.as_bytes()),
                Some(expected.into_bytes()),
                "Failed to get key: {}",
                key
            );
        }

        // Scan all should return sorted
        let all = tree.scan_all();
        assert_eq!(all.len(), 100);
        for i in 1..all.len() {
            assert!(all[i - 1].0 < all[i].0, "Keys not sorted at index {}", i);
        }

        // Reopen and verify
        drop(tree);
        let tree = BPlusTree::open(&path).unwrap();
        assert_eq!(tree.record_count(), 100);
        assert_eq!(tree.get(b"key-00042"), Some(b"value-42".to_vec()));

        let _ = fs::remove_file(&path);
    }
}
