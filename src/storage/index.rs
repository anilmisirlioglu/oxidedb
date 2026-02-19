//! Secondary Index Engine — GSI-like (Global Secondary Indexes)
//!
//! Supports:
//!   - Secondary indexes on one or more JSON fields
//!   - Composite indexes (multiple fields)
//!   - Partial indexes with WHERE conditions
//!   - **Array indexes** (`ALL ARRAY v FOR v IN field END`)
//!   - **Covering indexes** (INCLUDE extra fields — skip document fetch)
//!   - Index-backed equality and range lookups
//!   - Automatic index maintenance on mutations
//!
//! Syntax (via N1QL):
//!   CREATE INDEX idx_name ON bucket(field1, field2, ...)
//!   CREATE INDEX idx_tags ON bucket(ALL ARRAY v FOR v IN tags END)
//!   CREATE INDEX idx_cov  ON bucket(name) INCLUDE (age, city)
//!   DROP INDEX bucket.idx_name
//!   SELECT ... FROM bucket WHERE indexed_field = value
//!
//! Each index is backed by an in-memory B-tree (std::collections::BTreeMap)
//! that maps encoded field values → set of document keys.

use crate::storage::document::Document;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;
use tracing::info;

// =========================================================================
// Index Definition
// =========================================================================

/// Type of index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// Primary index (key → document, implicit)
    Primary,
    /// Secondary index on one or more fields
    Secondary,
}

/// State of an index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexState {
    /// Index is being built (not yet usable)
    Building,
    /// Index is online and usable
    Online,
    /// Index is deferred (created but not yet built)
    Deferred,
}

/// Array index expression: ALL ARRAY <expr> FOR <var> IN <array_path> END
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayIndexExpr {
    /// Variable name (e.g. "v")
    pub var: String,
    /// Expression to index (e.g. "v", "v.name") — resolved relative to the variable
    pub expr: String,
    /// Array field path in the document (e.g. "tags", "items")
    pub array_path: String,
    /// ALL or DISTINCT
    pub mode: ArrayIndexMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArrayIndexMode {
    All,
    Distinct,
}

/// Definition of an index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Unique name of the index
    pub name: String,
    /// Bucket this index belongs to
    pub bucket: String,
    /// Scope (default: _default)
    pub scope: String,
    /// Collection (default: _default)
    pub collection: String,
    /// Fields to index (supports dot notation for nested: "user.name")
    pub fields: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Current state
    pub state: IndexState,
    /// When the index was created
    pub created_at: String,
    /// Number of entries in the index
    pub num_entries: usize,
    /// Optional WHERE condition (for partial indexes)
    pub condition: Option<String>,
    /// Array index expressions (one per field that is an array expression)
    /// Maps field position → ArrayIndexExpr
    #[serde(default)]
    pub array_exprs: Vec<(usize, ArrayIndexExpr)>,
    /// Extra fields included for covering index (not key fields, just stored)
    #[serde(default)]
    pub include_fields: Vec<String>,
    /// Whether this is a covering index (has include_fields)
    #[serde(default)]
    pub is_covering: bool,
}

// =========================================================================
// Key Encoding (sort-preserving)
// =========================================================================

/// Encode a JSON value into bytes that preserve sort order.
///
/// Encoding scheme:
///   Null:    0x00
///   Bool:    0x01 + 0x00|0x01
///   Number:  0x02 + IEEE754 f64 with sign flip (8 bytes, big-endian)
///   String:  0x03 + UTF-8 bytes + 0x00
///   Other:   0x04 + JSON string + 0x00
fn encode_value(val: &serde_json::Value) -> Vec<u8> {
    match val {
        serde_json::Value::Null => vec![0x00],
        serde_json::Value::Bool(b) => vec![0x01, if *b { 0x01 } else { 0x00 }],
        serde_json::Value::Number(n) => {
            let mut buf = vec![0x02];
            let f = n.as_f64().unwrap_or(0.0);
            let bits = f.to_bits();
            // Flip for sort order: positive numbers get sign bit set,
            // negative numbers get all bits flipped
            let encoded = if f >= 0.0 {
                bits ^ (1u64 << 63)
            } else {
                !bits
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
            buf
        }
        serde_json::Value::String(s) => {
            let mut buf = vec![0x03];
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00);
            buf
        }
        _ => {
            let mut buf = vec![0x04];
            buf.extend_from_slice(
                serde_json::to_string(val)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            buf.push(0x00);
            buf
        }
    }
}

/// Encode multiple field values into a single composite key
fn encode_composite_key(values: &[serde_json::Value]) -> Vec<u8> {
    let mut key = Vec::new();
    for val in values {
        key.extend_from_slice(&encode_value(val));
    }
    key
}

/// Extract a field value from a JSON document, supporting dot notation.
/// e.g. "user.address.city" navigates into nested objects.
pub fn extract_field(doc_value: &serde_json::Value, field_path: &str) -> serde_json::Value {
    let parts: Vec<&str> = field_path.split('.').collect();
    let mut current = doc_value;
    for part in parts {
        match current.get(part) {
            Some(v) => current = v,
            None => return serde_json::Value::Null,
        }
    }
    current.clone()
}

// =========================================================================
// Index Data (in-memory B-tree)
// =========================================================================

/// The actual index data structure.
/// Maps encoded field values → set of document keys that have those values.
struct IndexData {
    /// B-tree: encoded_field_values → { doc_key1, doc_key2, ... }
    tree: BTreeMap<Vec<u8>, BTreeSet<String>>,
    /// Reverse map: doc_key → list of encoded_field_values (for efficient deletes/updates)
    /// A single doc can have multiple entries (array index: one per array element)
    reverse: HashMap<String, Vec<Vec<u8>>>,
    /// Covering index data: doc_key → stored field values (JSON)
    /// Only populated when include_fields is non-empty
    covering_data: HashMap<String, Vec<serde_json::Value>>,
}

impl IndexData {
    fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            reverse: HashMap::new(),
            covering_data: HashMap::new(),
        }
    }

    /// Insert a document into the index (single entry)
    fn insert(&mut self, doc_key: &str, field_values: &[serde_json::Value]) {
        let encoded = encode_composite_key(field_values);

        // Remove old entries if exist (update case)
        self.remove(doc_key);

        // Insert new entry
        self.tree
            .entry(encoded.clone())
            .or_default()
            .insert(doc_key.to_string());
        self.reverse
            .entry(doc_key.to_string())
            .or_default()
            .push(encoded);
    }

    /// Insert multiple entries for a single document (used by array indexes)
    fn insert_multi(&mut self, doc_key: &str, entries: &[Vec<serde_json::Value>]) {
        // Remove old entries
        self.remove(doc_key);

        for field_values in entries {
            let encoded = encode_composite_key(field_values);
            self.tree
                .entry(encoded.clone())
                .or_default()
                .insert(doc_key.to_string());
            self.reverse
                .entry(doc_key.to_string())
                .or_default()
                .push(encoded);
        }
    }

    /// Store covering field values for a document
    fn store_covering(&mut self, doc_key: &str, values: Vec<serde_json::Value>) {
        self.covering_data.insert(doc_key.to_string(), values);
    }

    /// Get covering field values for a document
    fn get_covering(&self, doc_key: &str) -> Option<&Vec<serde_json::Value>> {
        self.covering_data.get(doc_key)
    }

    /// Remove a document from the index
    fn remove(&mut self, doc_key: &str) {
        if let Some(old_encodeds) = self.reverse.remove(doc_key) {
            for old_encoded in old_encodeds {
                if let Some(keys) = self.tree.get_mut(&old_encoded) {
                    keys.remove(doc_key);
                    if keys.is_empty() {
                        self.tree.remove(&old_encoded);
                    }
                }
            }
        }
        self.covering_data.remove(doc_key);
    }

    /// Lookup: exact match on all fields → set of document keys
    fn lookup_eq(&self, field_values: &[serde_json::Value]) -> Vec<String> {
        let encoded = encode_composite_key(field_values);
        self.tree
            .get(&encoded)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Range scan: find all doc keys where the encoded key is in [start, end]
    fn lookup_range(
        &self,
        start: &[serde_json::Value],
        end: &[serde_json::Value],
    ) -> Vec<String> {
        let start_encoded = encode_composite_key(start);
        let end_encoded = encode_composite_key(end);

        let mut results = Vec::new();
        for (_, keys) in self.tree.range(start_encoded..=end_encoded) {
            results.extend(keys.iter().cloned());
        }
        results
    }

    /// Prefix scan: find all doc keys where encoded key starts with the given prefix
    fn lookup_prefix(&self, prefix_values: &[serde_json::Value]) -> Vec<String> {
        let prefix = encode_composite_key(prefix_values);
        let mut results = Vec::new();

        for (key, doc_keys) in self.tree.range(prefix.clone()..) {
            if key.starts_with(&prefix) {
                results.extend(doc_keys.iter().cloned());
            } else {
                break;
            }
        }
        results
    }

    /// Get all doc keys where value > given value (for a single field)
    fn lookup_gt(&self, field_values: &[serde_json::Value]) -> Vec<String> {
        let encoded = encode_composite_key(field_values);
        let mut results = Vec::new();

        // Get everything strictly after the encoded key
        use std::ops::Bound;
        for (_, keys) in self
            .tree
            .range((Bound::Excluded(encoded), Bound::Unbounded))
        {
            results.extend(keys.iter().cloned());
        }
        results
    }

    /// Get all doc keys where value >= given value
    fn lookup_gte(&self, field_values: &[serde_json::Value]) -> Vec<String> {
        let encoded = encode_composite_key(field_values);
        let mut results = Vec::new();
        for (_, keys) in self.tree.range(encoded..) {
            results.extend(keys.iter().cloned());
        }
        results
    }

    /// Get all doc keys where value < given value
    fn lookup_lt(&self, field_values: &[serde_json::Value]) -> Vec<String> {
        let encoded = encode_composite_key(field_values);
        let mut results = Vec::new();
        for (_, keys) in self.tree.range(..encoded) {
            results.extend(keys.iter().cloned());
        }
        results
    }

    /// Get all doc keys where value <= given value
    fn lookup_lte(&self, field_values: &[serde_json::Value]) -> Vec<String> {
        let encoded = encode_composite_key(field_values);
        let mut results = Vec::new();
        use std::ops::Bound;
        for (_, keys) in self
            .tree
            .range((Bound::Unbounded, Bound::Included(encoded)))
        {
            results.extend(keys.iter().cloned());
        }
        results
    }

    /// Number of entries in the index
    fn len(&self) -> usize {
        self.reverse.len()
    }
}

// =========================================================================
// Index Manager
// =========================================================================

/// Manages all indexes across all buckets.
pub struct IndexManager {
    /// Index definitions: (bucket_name, index_name) → IndexDefinition
    definitions: RwLock<HashMap<(String, String), IndexDefinition>>,
    /// Index data: (bucket_name, index_name) → IndexData
    data: RwLock<HashMap<(String, String), IndexData>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            definitions: RwLock::new(HashMap::new()),
            data: RwLock::new(HashMap::new()),
        }
    }

    // =====================================================================
    // Index lifecycle
    // =====================================================================

    /// Create a new secondary index
    pub fn create_index(
        &self,
        name: String,
        bucket: String,
        fields: Vec<String>,
        condition: Option<String>,
    ) -> Result<IndexDefinition, String> {
        self.create_index_ex(name, bucket, fields, condition, vec![], vec![])
    }

    /// Create a new secondary index with array expressions and/or covering fields
    pub fn create_index_ex(
        &self,
        name: String,
        bucket: String,
        fields: Vec<String>,
        condition: Option<String>,
        array_exprs: Vec<(usize, ArrayIndexExpr)>,
        include_fields: Vec<String>,
    ) -> Result<IndexDefinition, String> {
        let key = (bucket.clone(), name.clone());

        // Check if already exists
        {
            let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
            if defs.contains_key(&key) {
                return Err(format!("Index '{}' already exists on bucket '{}'", name, bucket));
            }
        }

        if fields.is_empty() {
            return Err("Index must have at least one field".to_string());
        }

        let is_covering = !include_fields.is_empty();

        let def = IndexDefinition {
            name: name.clone(),
            bucket: bucket.clone(),
            scope: "_default".to_string(),
            collection: "_default".to_string(),
            fields,
            index_type: IndexType::Secondary,
            state: IndexState::Building,
            created_at: Utc::now().to_rfc3339(),
            num_entries: 0,
            condition,
            array_exprs,
            include_fields,
            is_covering,
        };

        // Insert definition and empty data
        {
            let mut defs = self.definitions.write().expect("IndexManager definitions lock poisoned");
            defs.insert(key.clone(), def.clone());
        }
        {
            let mut data = self.data.write().expect("IndexManager data lock poisoned");
            data.insert(key, IndexData::new());
        }

        info!(
            "Index '{}' created on bucket '{}' (fields: {:?}, array_exprs: {}, covering_fields: {:?})",
            name, bucket, def.fields, def.array_exprs.len(), def.include_fields
        );

        Ok(def)
    }

    /// Build an index by scanning all existing documents
    pub fn build_index(
        &self,
        bucket_name: &str,
        index_name: &str,
        documents: &[Document],
    ) -> Result<usize, String> {
        let key = (bucket_name.to_string(), index_name.to_string());

        let (fields, array_exprs, include_fields) = {
            let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
            let def = defs
                .get(&key)
                .ok_or_else(|| format!("Index '{}' not found", index_name))?;
            (def.fields.clone(), def.array_exprs.clone(), def.include_fields.clone())
        };

        let mut count = 0;
        {
            let mut data = self.data.write().expect("IndexManager data lock poisoned");
            let idx_data = data
                .get_mut(&key)
                .ok_or_else(|| format!("Index data '{}' not found", index_name))?;

            for doc in documents {
                let indexed = Self::index_document_fields(
                    &doc.value,
                    &doc.key,
                    &fields,
                    &array_exprs,
                    &include_fields,
                    idx_data,
                );
                if indexed {
                    count += 1;
                }
            }
        }

        // Mark as online
        {
            let mut defs = self.definitions.write().expect("IndexManager definitions lock poisoned");
            if let Some(def) = defs.get_mut(&key) {
                def.state = IndexState::Online;
                def.num_entries = count;
            }
        }

        info!(
            "Index '{}' built on bucket '{}': {} entries",
            index_name, bucket_name, count
        );

        Ok(count)
    }

    /// Index a single document's fields into the index data.
    /// Handles both regular fields and array index expressions.
    /// Returns true if the document was indexed.
    fn index_document_fields(
        doc_value: &serde_json::Value,
        doc_key: &str,
        fields: &[String],
        array_exprs: &[(usize, ArrayIndexExpr)],
        include_fields: &[String],
        idx_data: &mut IndexData,
    ) -> bool {
        // Check if any field is an array expression
        let has_array = !array_exprs.is_empty();

        if has_array {
            // For array indexes, we need to create multiple entries per document
            let entries = Self::expand_array_entries(doc_value, fields, array_exprs);
            if entries.is_empty() {
                return false;
            }
            idx_data.insert_multi(doc_key, &entries);
        } else {
            // Standard index: single entry per document
            let field_values: Vec<serde_json::Value> = fields
                .iter()
                .map(|f| extract_field(doc_value, f))
                .collect();

            // Skip if all fields are null
            if field_values.iter().all(|v| v.is_null()) {
                return false;
            }

            idx_data.insert(doc_key, &field_values);
        }

        // Store covering data if applicable
        if !include_fields.is_empty() {
            let covering_values: Vec<serde_json::Value> = include_fields
                .iter()
                .map(|f| extract_field(doc_value, f))
                .collect();
            idx_data.store_covering(doc_key, covering_values);
        }

        true
    }

    /// Expand array index expressions into multiple entries for a document.
    /// For example, if field 0 is an array expression on "tags",
    /// and the document has tags: ["rust", "db", "nosql"],
    /// this returns 3 entries, one per tag value.
    fn expand_array_entries(
        doc_value: &serde_json::Value,
        fields: &[String],
        array_exprs: &[(usize, ArrayIndexExpr)],
    ) -> Vec<Vec<serde_json::Value>> {
        // Build a map of which field positions are array expressions
        let array_map: HashMap<usize, &ArrayIndexExpr> = array_exprs
            .iter()
            .map(|(pos, expr)| (*pos, expr))
            .collect();

        // Get base values for non-array fields
        let base_values: Vec<(usize, serde_json::Value)> = fields
            .iter()
            .enumerate()
            .filter(|(i, _)| !array_map.contains_key(i))
            .map(|(i, f)| (i, extract_field(doc_value, f)))
            .collect();

        // Get array values for array fields
        let mut array_values: Vec<(usize, Vec<serde_json::Value>)> = Vec::new();
        for (pos, expr) in &array_map {
            let arr = extract_field(doc_value, &expr.array_path);
            if let serde_json::Value::Array(elements) = arr {
                let values: Vec<serde_json::Value> = elements
                    .iter()
                    .map(|elem| {
                        // If expr.expr == expr.var, we index the element itself
                        // If expr.expr is like "v.name", we extract the sub-field
                        if expr.expr == expr.var {
                            elem.clone()
                        } else {
                            // expr.expr might be "v.name" — strip "v." prefix
                            let sub_path = expr.expr.strip_prefix(&format!("{}.", expr.var))
                                .unwrap_or(&expr.expr);
                            extract_field(elem, sub_path)
                        }
                    })
                    .collect();

                // For DISTINCT mode, deduplicate
                let values = if expr.mode == ArrayIndexMode::Distinct {
                    let mut seen = BTreeSet::new();
                    values.into_iter().filter(|v| {
                        let key = serde_json::to_string(v).unwrap_or_default();
                        seen.insert(key)
                    }).collect()
                } else {
                    values
                };

                array_values.push((*pos, values));
            }
        }

        if array_values.is_empty() {
            return Vec::new();
        }

        // Generate cross-product of array values with base values
        // For simplicity, we support one array field for now (most common case)
        let (arr_pos, arr_vals) = &array_values[0];
        let mut entries = Vec::new();

        for arr_val in arr_vals {
            let mut entry = vec![serde_json::Value::Null; fields.len()];
            // Fill base values
            for (pos, val) in &base_values {
                entry[*pos] = val.clone();
            }
            // Fill array value
            entry[*arr_pos] = arr_val.clone();
            entries.push(entry);
        }

        entries
    }

    /// Drop an index
    pub fn drop_index(
        &self,
        bucket_name: &str,
        index_name: &str,
    ) -> Result<IndexDefinition, String> {
        let key = (bucket_name.to_string(), index_name.to_string());

        let def = {
            let mut defs = self.definitions.write().expect("IndexManager definitions lock poisoned");
            defs.remove(&key)
                .ok_or_else(|| format!("Index '{}' not found on bucket '{}'", index_name, bucket_name))?
        };

        {
            let mut data = self.data.write().expect("IndexManager data lock poisoned");
            data.remove(&key);
        }

        info!(
            "Index '{}' dropped from bucket '{}'",
            index_name, bucket_name
        );

        Ok(def)
    }

    /// List all indexes for a bucket (or all buckets if None)
    pub fn list_indexes(&self, bucket: Option<&str>) -> Vec<IndexDefinition> {
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let data = self.data.read().expect("IndexManager data lock poisoned");

        defs.iter()
            .filter(|((b, _), _)| bucket.map_or(true, |bkt| b == bkt))
            .map(|(key, def)| {
                let mut d = def.clone();
                // Update entry count
                if let Some(idx_data) = data.get(key) {
                    d.num_entries = idx_data.len();
                }
                d
            })
            .collect()
    }

    /// Get a specific index definition
    pub fn get_index(
        &self,
        bucket_name: &str,
        index_name: &str,
    ) -> Option<IndexDefinition> {
        let key = (bucket_name.to_string(), index_name.to_string());
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let data = self.data.read().expect("IndexManager data lock poisoned");

        defs.get(&key).map(|def| {
            let mut d = def.clone();
            if let Some(idx_data) = data.get(&key) {
                d.num_entries = idx_data.len();
            }
            d
        })
    }

    // =====================================================================
    // Index maintenance (called on document mutations)
    // =====================================================================

    /// Update indexes when a document is upserted
    pub fn on_document_upsert(&self, bucket_name: &str, doc: &Document) {
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let mut data = self.data.write().expect("IndexManager data lock poisoned");

        for ((b, _), def) in defs.iter() {
            if b != bucket_name || def.state != IndexState::Online {
                continue;
            }

            let key = (b.clone(), def.name.clone());
            if let Some(idx_data) = data.get_mut(&key) {
                Self::index_document_fields(
                    &doc.value,
                    &doc.key,
                    &def.fields,
                    &def.array_exprs,
                    &def.include_fields,
                    idx_data,
                );
            }
        }
    }

    /// Update indexes when a document is deleted
    pub fn on_document_delete(&self, bucket_name: &str, doc_key: &str) {
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let mut data = self.data.write().expect("IndexManager data lock poisoned");

        for ((b, _), def) in defs.iter() {
            if b != bucket_name || def.state != IndexState::Online {
                continue;
            }

            let key = (b.clone(), def.name.clone());
            if let Some(idx_data) = data.get_mut(&key) {
                idx_data.remove(doc_key);
            }
        }
    }

    // =====================================================================
    // Index lookups (used by query engine)
    // =====================================================================

    /// Find the best index for a set of WHERE conditions on a bucket.
    /// Returns (index_name, matching doc keys) if an applicable index is found.
    ///
    /// Selection strategy (similar to Couchbase GSI):
    ///   - An index is only usable if a condition matches its **leading** (first) field
    ///   - Among usable indexes, prefer the one with the most conditions covered
    ///   - For composite indexes, the leading key must have an equality/range predicate
    ///   - Covering indexes are preferred when they can cover the full query
    pub fn find_matching_index(
        &self,
        bucket_name: &str,
        conditions: &[(String, IndexLookupOp, serde_json::Value)],
    ) -> Option<IndexLookupResult> {
        self.find_matching_index_ex(bucket_name, conditions, &[])
    }

    /// Extended find: also checks covering capabilities for the required_fields
    pub fn find_matching_index_ex(
        &self,
        bucket_name: &str,
        conditions: &[(String, IndexLookupOp, serde_json::Value)],
        required_fields: &[String],
    ) -> Option<IndexLookupResult> {
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let data = self.data.read().expect("IndexManager data lock poisoned");

        // Collect candidate indexes: (name, fields, leading_condition_idx, total_covered, is_covering_for_query, include_fields)
        let mut candidates: Vec<(String, Vec<String>, usize, usize, bool, Vec<String>)> = Vec::new();

        for ((b, _), def) in defs.iter() {
            if b != bucket_name || def.state != IndexState::Online {
                continue;
            }

            // Leading field must have a matching condition
            let leading_field = &def.fields[0];
            let leading_cond_idx = conditions
                .iter()
                .position(|(field, _, _)| field == leading_field);

            if leading_cond_idx.is_none() {
                continue; // skip: no predicate on leading key
            }

            let covered: usize = conditions
                .iter()
                .filter(|(field, _, _)| def.fields.contains(field))
                .count();

            // Check if this index covers all required fields
            let all_index_fields: Vec<&String> = def.fields.iter()
                .chain(def.include_fields.iter())
                .collect();
            let covers_query = required_fields.iter().all(|rf| {
                all_index_fields.iter().any(|f| *f == rf) || rf == "META().id"
            });

            candidates.push((
                def.name.clone(),
                def.fields.clone(),
                leading_cond_idx.unwrap(),
                covered,
                covers_query && !required_fields.is_empty(),
                def.include_fields.clone(),
            ));
        }

        if candidates.is_empty() {
            return None;
        }

        // Sort: prefer covering indexes, then more covered conditions, then fewer index fields
        candidates.sort_by(|a, b| {
            b.4.cmp(&a.4) // covering first
                .then(b.3.cmp(&a.3)) // most covered conditions
                .then(a.1.len().cmp(&b.1.len())) // simpler index preferred
        });

        let (index_name, index_fields, leading_idx, _, is_covering, include_fields) = &candidates[0];
        let key = (bucket_name.to_string(), index_name.clone());
        let idx_data = data.get(&key)?;

        let leading_condition = &conditions[*leading_idx];

        let doc_keys = match leading_condition.1 {
            IndexLookupOp::Eq => {
                if index_fields.len() == 1 {
                    idx_data.lookup_eq(&[leading_condition.2.clone()])
                } else {
                    idx_data.lookup_prefix(&[leading_condition.2.clone()])
                }
            }
            IndexLookupOp::Gt => idx_data.lookup_gt(&[leading_condition.2.clone()]),
            IndexLookupOp::Gte => idx_data.lookup_gte(&[leading_condition.2.clone()]),
            IndexLookupOp::Lt => idx_data.lookup_lt(&[leading_condition.2.clone()]),
            IndexLookupOp::Lte => idx_data.lookup_lte(&[leading_condition.2.clone()]),
            IndexLookupOp::Range(ref low, ref high) => {
                idx_data.lookup_range(&[low.clone()], &[high.clone()])
            }
        };

        // Collect covering data if this is a covering scan
        let covering_values = if *is_covering {
            let mut cv = HashMap::new();
            for dk in &doc_keys {
                if let Some(vals) = idx_data.get_covering(dk) {
                    cv.insert(dk.clone(), vals.clone());
                }
            }
            Some(cv)
        } else {
            None
        };

        Some(IndexLookupResult {
            index_name: index_name.clone(),
            index_fields: index_fields.clone(),
            doc_keys,
            lookup_type: format!("{:?}", leading_condition.1),
            is_covering: *is_covering,
            include_fields: include_fields.clone(),
            covering_values,
        })
    }

    /// Find indexes that can serve array lookups (ANY ... IN ... SATISFIES ...)
    #[allow(dead_code)]
    pub fn find_array_index(
        &self,
        bucket_name: &str,
        array_field: &str,
        value: &serde_json::Value,
    ) -> Option<IndexLookupResult> {
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let data = self.data.read().expect("IndexManager data lock poisoned");

        for ((b, _), def) in defs.iter() {
            if b != bucket_name || def.state != IndexState::Online {
                continue;
            }

            // Check if any array expression matches the array_field
            for (_pos, expr) in &def.array_exprs {
                if expr.array_path == array_field {
                    let key = (b.clone(), def.name.clone());
                    if let Some(idx_data) = data.get(&key) {
                        let doc_keys = idx_data.lookup_eq(&[value.clone()]);
                        return Some(IndexLookupResult {
                            index_name: def.name.clone(),
                            index_fields: def.fields.clone(),
                            doc_keys,
                            lookup_type: "ArrayEq".to_string(),
                            is_covering: false,
                            include_fields: vec![],
                            covering_values: None,
                        });
                    }
                }
            }
        }
        None
    }

    /// Drop all indexes for a bucket
    #[allow(dead_code)]
    pub fn drop_all_for_bucket(&self, bucket_name: &str) {
        let keys_to_remove: Vec<(String, String)> = {
            let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
            defs.keys()
                .filter(|(b, _)| b == bucket_name)
                .cloned()
                .collect()
        };

        for key in keys_to_remove {
            let mut defs = self.definitions.write().expect("IndexManager definitions lock poisoned");
            let mut data = self.data.write().expect("IndexManager data lock poisoned");
            defs.remove(&key);
            data.remove(&key);
        }
    }

    // =====================================================================
    // Persistence: save/load index definitions to/from disk
    // =====================================================================

    /// Save all index definitions to a JSON file in the data directory
    pub fn save_definitions(&self, data_dir: &str) -> std::result::Result<(), String> {
        let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
        let all_defs: Vec<IndexDefinition> = defs.values().cloned().collect();

        let path = std::path::Path::new(data_dir).join("indexes.json");
        let json = serde_json::to_string_pretty(&all_defs)
            .map_err(|e| format!("Failed to serialize indexes: {}", e))?;
        std::fs::write(&path, json)
            .map_err(|e| format!("Failed to write indexes to {}: {}", path.display(), e))?;

        info!("Saved {} index definitions to {}", all_defs.len(), path.display());
        Ok(())
    }

    /// Load index definitions from disk and rebuild them using the provided document scanner.
    /// The `doc_scanner` closure takes a bucket name and returns all documents in that bucket.
    pub fn load_definitions<F>(
        &self,
        data_dir: &str,
        doc_scanner: F,
    ) -> std::result::Result<usize, String>
    where
        F: Fn(&str) -> Vec<Document>,
    {
        let path = std::path::Path::new(data_dir).join("indexes.json");
        if !path.exists() {
            return Ok(0);
        }

        let json = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read indexes from {}: {}", path.display(), e))?;
        let saved_defs: Vec<IndexDefinition> = serde_json::from_str(&json)
            .map_err(|e| format!("Failed to parse indexes from {}: {}", path.display(), e))?;

        let mut count = 0;

        for def in saved_defs {
            // Create the index (skip if already exists)
            let key = (def.bucket.clone(), def.name.clone());
            {
                let defs = self.definitions.read().expect("IndexManager definitions lock poisoned");
                if defs.contains_key(&key) {
                    continue;
                }
            }

            // Insert definition
            {
                let mut defs = self.definitions.write().expect("IndexManager definitions lock poisoned");
                defs.insert(key.clone(), def.clone());
            }
            {
                let mut data = self.data.write().expect("IndexManager data lock poisoned");
                data.insert(key.clone(), IndexData::new());
            }

            // Rebuild index data from documents
            let docs = doc_scanner(&def.bucket);
            if !docs.is_empty() {
                let _ = self.build_index(&def.bucket, &def.name, &docs);
            } else {
                // Mark as online even if empty
                let mut defs = self.definitions.write().expect("IndexManager definitions lock poisoned");
                if let Some(d) = defs.get_mut(&key) {
                    d.state = IndexState::Online;
                }
            }

            count += 1;
            info!(
                "Loaded index '{}' on bucket '{}' (fields: {:?})",
                def.name, def.bucket, def.fields
            );
        }

        info!("Loaded {} index definitions from {}", count, path.display());
        Ok(count)
    }
}

// =========================================================================
// Lookup types
// =========================================================================

/// Operations supported for index lookups
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum IndexLookupOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Range(serde_json::Value, serde_json::Value),
}

/// Result of an index lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexLookupResult {
    pub index_name: String,
    pub index_fields: Vec<String>,
    pub doc_keys: Vec<String>,
    pub lookup_type: String,
    /// Whether this index fully covers the query (no document fetch needed)
    #[serde(default)]
    pub is_covering: bool,
    /// Extra fields stored in the covering index
    #[serde(default)]
    pub include_fields: Vec<String>,
    /// Covering field values per doc key (only populated for covering scans)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub covering_values: Option<HashMap<String, Vec<serde_json::Value>>>,
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(key: &str, value: serde_json::Value) -> Document {
        let now = Utc::now();
        Document {
            key: key.to_string(),
            value,
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
        }
    }

    #[test]
    fn test_create_and_build_index() {
        let mgr = IndexManager::new();
        let def = mgr
            .create_index(
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                None,
            )
            .unwrap();
        assert_eq!(def.state, IndexState::Building);

        let docs = vec![
            make_doc("u1", serde_json::json!({"name": "Alice", "age": 30})),
            make_doc("u2", serde_json::json!({"name": "Bob", "age": 25})),
            make_doc("u3", serde_json::json!({"name": "Charlie", "age": 30})),
            make_doc("u4", serde_json::json!({"name": "Diana", "age": 35})),
        ];

        let count = mgr.build_index("users", "idx_age", &docs).unwrap();
        assert_eq!(count, 4);

        let def = mgr.get_index("users", "idx_age").unwrap();
        assert_eq!(def.state, IndexState::Online);
        assert_eq!(def.num_entries, 4);
    }

    #[test]
    fn test_index_eq_lookup() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_age".to_string(),
            "users".to_string(),
            vec!["age".to_string()],
            None,
        )
        .unwrap();

        let docs = vec![
            make_doc("u1", serde_json::json!({"name": "Alice", "age": 30})),
            make_doc("u2", serde_json::json!({"name": "Bob", "age": 25})),
            make_doc("u3", serde_json::json!({"name": "Charlie", "age": 30})),
        ];
        mgr.build_index("users", "idx_age", &docs).unwrap();

        let conditions = vec![(
            "age".to_string(),
            IndexLookupOp::Eq,
            serde_json::json!(30),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert_eq!(result.index_name, "idx_age");
        assert!(result.doc_keys.contains(&"u1".to_string()));
        assert!(result.doc_keys.contains(&"u3".to_string()));
        assert!(!result.doc_keys.contains(&"u2".to_string()));
    }

    #[test]
    fn test_index_range_lookup() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_age".to_string(),
            "users".to_string(),
            vec!["age".to_string()],
            None,
        )
        .unwrap();

        let docs = vec![
            make_doc("u1", serde_json::json!({"age": 20})),
            make_doc("u2", serde_json::json!({"age": 25})),
            make_doc("u3", serde_json::json!({"age": 30})),
            make_doc("u4", serde_json::json!({"age": 35})),
            make_doc("u5", serde_json::json!({"age": 40})),
        ];
        mgr.build_index("users", "idx_age", &docs).unwrap();

        // age > 25
        let conditions = vec![(
            "age".to_string(),
            IndexLookupOp::Gt,
            serde_json::json!(25),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert_eq!(result.doc_keys.len(), 3); // u3, u4, u5

        // age <= 30
        let conditions = vec![(
            "age".to_string(),
            IndexLookupOp::Lte,
            serde_json::json!(30),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert_eq!(result.doc_keys.len(), 3); // u1, u2, u3
    }

    #[test]
    fn test_index_maintenance() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_city".to_string(),
            "users".to_string(),
            vec!["city".to_string()],
            None,
        )
        .unwrap();

        let docs = vec![
            make_doc("u1", serde_json::json!({"city": "NYC"})),
            make_doc("u2", serde_json::json!({"city": "LA"})),
        ];
        mgr.build_index("users", "idx_city", &docs).unwrap();

        // Upsert: update u1's city
        let updated = make_doc("u1", serde_json::json!({"city": "SF"}));
        mgr.on_document_upsert("users", &updated);

        // NYC should now have 0, SF should have 1
        let conditions = vec![(
            "city".to_string(),
            IndexLookupOp::Eq,
            serde_json::json!("NYC"),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert!(result.doc_keys.is_empty());

        let conditions = vec![(
            "city".to_string(),
            IndexLookupOp::Eq,
            serde_json::json!("SF"),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert_eq!(result.doc_keys, vec!["u1"]);

        // Delete u2
        mgr.on_document_delete("users", "u2");
        let conditions = vec![(
            "city".to_string(),
            IndexLookupOp::Eq,
            serde_json::json!("LA"),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert!(result.doc_keys.is_empty());
    }

    #[test]
    fn test_composite_index() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_city_age".to_string(),
            "users".to_string(),
            vec!["city".to_string(), "age".to_string()],
            None,
        )
        .unwrap();

        let docs = vec![
            make_doc("u1", serde_json::json!({"city": "NYC", "age": 30})),
            make_doc("u2", serde_json::json!({"city": "NYC", "age": 25})),
            make_doc("u3", serde_json::json!({"city": "LA", "age": 30})),
            make_doc("u4", serde_json::json!({"city": "LA", "age": 40})),
        ];
        mgr.build_index("users", "idx_city_age", &docs).unwrap();

        // Lookup city = "NYC" (prefix scan on composite)
        let conditions = vec![(
            "city".to_string(),
            IndexLookupOp::Eq,
            serde_json::json!("NYC"),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert_eq!(result.doc_keys.len(), 2); // u1, u2
    }

    #[test]
    fn test_nested_field_index() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_addr_city".to_string(),
            "users".to_string(),
            vec!["address.city".to_string()],
            None,
        )
        .unwrap();

        let docs = vec![
            make_doc(
                "u1",
                serde_json::json!({"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}),
            ),
            make_doc(
                "u2",
                serde_json::json!({"name": "Bob", "address": {"city": "LA", "zip": "90001"}}),
            ),
        ];
        mgr.build_index("users", "idx_addr_city", &docs).unwrap();

        let conditions = vec![(
            "address.city".to_string(),
            IndexLookupOp::Eq,
            serde_json::json!("NYC"),
        )];
        let result = mgr.find_matching_index("users", &conditions).unwrap();
        assert_eq!(result.doc_keys, vec!["u1"]);
    }

    #[test]
    fn test_drop_index() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_test".to_string(),
            "bucket1".to_string(),
            vec!["field".to_string()],
            None,
        )
        .unwrap();

        assert!(mgr.get_index("bucket1", "idx_test").is_some());

        mgr.drop_index("bucket1", "idx_test").unwrap();
        assert!(mgr.get_index("bucket1", "idx_test").is_none());
    }

    #[test]
    fn test_string_sort_order() {
        let mgr = IndexManager::new();
        mgr.create_index(
            "idx_name".to_string(),
            "test".to_string(),
            vec!["name".to_string()],
            None,
        )
        .unwrap();

        let docs = vec![
            make_doc("u1", serde_json::json!({"name": "Charlie"})),
            make_doc("u2", serde_json::json!({"name": "Alice"})),
            make_doc("u3", serde_json::json!({"name": "Bob"})),
        ];
        mgr.build_index("test", "idx_name", &docs).unwrap();

        // name >= "Bob" should return Bob and Charlie
        let conditions = vec![(
            "name".to_string(),
            IndexLookupOp::Gte,
            serde_json::json!("Bob"),
        )];
        let result = mgr.find_matching_index("test", &conditions).unwrap();
        assert_eq!(result.doc_keys.len(), 2);
        assert!(result.doc_keys.contains(&"u1".to_string())); // Charlie
        assert!(result.doc_keys.contains(&"u3".to_string())); // Bob
    }
}
