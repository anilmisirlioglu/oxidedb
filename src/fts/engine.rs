//! Full-Text Search (FTS) Engine
//!
//! Implements a Couchbase-compatible FTS service with:
//! - Inverted index data structure
//! - Multiple tokenizers (standard, whitespace, simple)
//! - TF-IDF scoring with BM25 ranking
//! - Match, match_phrase, term, prefix, wildcard, regexp, boolean queries
//! - Fuzzy matching (Levenshtein distance)
//! - Highlighting (fragment extraction)
//! - FTS index lifecycle (create, build, drop, list)
//! - Persistence of index definitions to disk

use crate::storage::engine::StorageEngine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tracing::{debug, info};

// ═══════════════════════════════════════════════════════════════════════
// Public types
// ═══════════════════════════════════════════════════════════════════════

/// FTS index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsIndexDefinition {
    pub name: String,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    /// Fields to index with their mappings. Empty = index all string fields.
    pub fields: Vec<FtsFieldMapping>,
    /// Analyzer configuration
    pub analyzer: AnalyzerConfig,
    /// Current state
    pub state: FtsIndexState,
    /// Creation time
    pub created_at: String,
    /// Number of documents in the index
    pub doc_count: usize,
    /// Number of unique terms
    pub term_count: usize,
}

/// Field mapping for FTS index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsFieldMapping {
    /// JSON path to the field (e.g. "title", "body", "user.bio")
    pub field: String,
    /// Override analyzer for this field (None = use index default)
    pub analyzer: Option<String>,
    /// Store original value for highlighting
    #[serde(default = "default_true")]
    pub store: bool,
}

fn default_true() -> bool {
    true
}

/// Analyzer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Tokenizer type: "standard", "whitespace", "simple", "keyword"
    #[serde(default = "default_tokenizer")]
    pub tokenizer: String,
    /// Convert to lowercase
    #[serde(default = "default_true")]
    pub lowercase: bool,
    /// Remove stop words
    #[serde(default)]
    pub stop_words: bool,
    /// Minimum token length
    #[serde(default = "default_min_length")]
    pub min_token_length: usize,
}

fn default_tokenizer() -> String {
    "standard".to_string()
}

fn default_min_length() -> usize {
    1
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            tokenizer: "standard".to_string(),
            lowercase: true,
            stop_words: false,
            min_token_length: 1,
        }
    }
}

/// FTS index state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FtsIndexState {
    Building,
    Online,
    Offline,
}

/// FTS search request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsSearchRequest {
    /// Index name to search
    #[serde(default)]
    pub index: String,
    /// The query
    pub query: FtsQuery,
    /// Maximum results to return
    #[serde(default = "default_size")]
    pub size: usize,
    /// Offset for pagination
    #[serde(default)]
    pub from: usize,
    /// Include highlighted fragments
    #[serde(default)]
    pub highlight: bool,
    /// Fields to return (empty = all stored fields)
    #[serde(default)]
    pub fields: Vec<String>,
    /// Sort order: ["_score", "field_name", "-field_name"]
    #[serde(default)]
    pub sort: Vec<String>,
}

fn default_size() -> usize {
    10
}

/// FTS query types (Couchbase-compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FtsQuery {
    /// Match query: analyzes the query text and matches any of the terms
    Match {
        field: String,
        query: String,
        #[serde(default)]
        fuzziness: usize,
        #[serde(default)]
        operator: Option<String>, // "and" or "or" (default)
    },
    /// Match phrase: matches terms in order
    MatchPhrase {
        field: String,
        query: String,
    },
    /// Term query: exact term match (no analysis)
    Term {
        field: String,
        term: String,
    },
    /// Prefix query: matches terms starting with prefix
    Prefix {
        field: String,
        prefix: String,
    },
    /// Wildcard query: * and ? wildcards
    Wildcard {
        field: String,
        wildcard: String,
    },
    /// Regexp query
    Regexp {
        field: String,
        regexp: String,
    },
    /// Numeric range query
    NumericRange {
        field: String,
        min: Option<f64>,
        max: Option<f64>,
        #[serde(default = "default_true")]
        inclusive_min: bool,
        #[serde(default = "default_true")]
        inclusive_max: bool,
    },
    /// Boolean query: must / should / must_not
    Bool {
        #[serde(default)]
        must: Vec<FtsQuery>,
        #[serde(default)]
        should: Vec<FtsQuery>,
        #[serde(default)]
        must_not: Vec<FtsQuery>,
    },
    /// Match all documents
    MatchAll {},
    /// Match no documents
    MatchNone {},
}

/// FTS search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsSearchResult {
    pub status: String,
    pub total_hits: usize,
    pub max_score: f64,
    pub hits: Vec<FtsHit>,
    pub took_ms: u64,
    pub facets: serde_json::Value,
}

/// A single search hit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsHit {
    pub index: String,
    pub id: String,
    pub score: f64,
    pub fields: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragments: Option<HashMap<String, Vec<String>>>,
}

// ═══════════════════════════════════════════════════════════════════════
// Inverted index internals
// ═══════════════════════════════════════════════════════════════════════

/// Per-field inverted index
#[derive(Debug)]
struct FieldIndex {
    /// term → { doc_key → [positions] }
    postings: HashMap<String, BTreeMap<String, Vec<u32>>>,
}

/// Per-document stats for a field
#[derive(Debug, Clone)]
struct DocFieldStats {
    /// Total number of tokens in this field for this document
    total_tokens: u32,
}

/// The inverted index for a single FTS index
#[derive(Debug)]
struct InvertedIndex {
    /// field_name → FieldIndex
    fields: HashMap<String, FieldIndex>,
    /// (doc_key, field_name) → DocFieldStats
    doc_stats: HashMap<(String, String), DocFieldStats>,
    /// field_name → { term → document_frequency }
    doc_freq: HashMap<String, HashMap<String, u32>>,
    /// Stored field values for highlighting: doc_key → field_name → original_text
    stored_fields: HashMap<String, HashMap<String, String>>,
    /// Total unique document IDs
    total_docs: usize,
    /// Average field length per field: field_name → avg_dl
    avg_field_len: HashMap<String, f64>,
}

impl FieldIndex {
    fn new() -> Self {
        Self {
            postings: HashMap::new(),
        }
    }
}

impl InvertedIndex {
    fn new() -> Self {
        Self {
            fields: HashMap::new(),
            doc_stats: HashMap::new(),
            doc_freq: HashMap::new(),
            stored_fields: HashMap::new(),
            total_docs: 0,
            avg_field_len: HashMap::new(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tokenizer
// ═══════════════════════════════════════════════════════════════════════

/// A token with position information
#[derive(Debug, Clone)]
struct Token {
    /// The token text
    text: String,
    /// Position in the token stream (0-based)
    position: u32,
    /// Start byte offset in original text
    #[allow(dead_code)]
    start_offset: usize,
    /// End byte offset in original text
    #[allow(dead_code)]
    end_offset: usize,
}

/// English stop words
const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
    "if", "in", "into", "is", "it", "no", "not", "of", "on", "or",
    "such", "that", "the", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];

fn tokenize(text: &str, config: &AnalyzerConfig) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut position: u32 = 0;

    match config.tokenizer.as_str() {
        "keyword" => {
            // Entire input is one token
            let mut t = text.to_string();
            if config.lowercase {
                t = t.to_lowercase();
            }
            tokens.push(Token {
                text: t,
                position: 0,
                start_offset: 0,
                end_offset: text.len(),
            });
        }
        "whitespace" => {
            // Split on whitespace only
            for (start, word) in find_whitespace_tokens(text) {
                let end = start + word.len();
                let mut t = word.to_string();
                if config.lowercase {
                    t = t.to_lowercase();
                }
                if t.len() >= config.min_token_length {
                    tokens.push(Token {
                        text: t,
                        position,
                        start_offset: start,
                        end_offset: end,
                    });
                    position += 1;
                }
            }
        }
        "simple" => {
            // Split on non-letter characters, lowercase
            for (start, word) in find_letter_tokens(text) {
                let end = start + word.len();
                let t = word.to_lowercase();
                if t.len() >= config.min_token_length {
                    tokens.push(Token {
                        text: t,
                        position,
                        start_offset: start,
                        end_offset: end,
                    });
                    position += 1;
                }
            }
        }
        _ => {
            // "standard" tokenizer: split on word boundaries, remove punctuation
            for (start, word) in find_standard_tokens(text) {
                let end = start + word.len();
                let mut t = word.to_string();

                // Strip leading/trailing punctuation
                t = t.trim_matches(|c: char| !c.is_alphanumeric()).to_string();

                if t.is_empty() {
                    continue;
                }

                if config.lowercase {
                    t = t.to_lowercase();
                }

                if t.len() < config.min_token_length {
                    continue;
                }

                if config.stop_words && STOP_WORDS.contains(&t.as_str()) {
                    continue;
                }

                tokens.push(Token {
                    text: t,
                    position,
                    start_offset: start,
                    end_offset: end,
                });
                position += 1;
            }
        }
    }

    tokens
}

fn find_whitespace_tokens(text: &str) -> Vec<(usize, &str)> {
    let mut result = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        if c.is_whitespace() {
            if let Some(s) = start {
                result.push((s, &text[s..i]));
                start = None;
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        result.push((s, &text[s..]));
    }
    result
}

fn find_letter_tokens(text: &str) -> Vec<(usize, &str)> {
    let mut result = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        if c.is_alphabetic() {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start {
            result.push((s, &text[s..i]));
            start = None;
        }
    }
    if let Some(s) = start {
        result.push((s, &text[s..]));
    }
    result
}

fn find_standard_tokens(text: &str) -> Vec<(usize, &str)> {
    let mut result = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        if c.is_alphanumeric() || c == '_' || c == '-' || c == '\'' {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start {
            result.push((s, &text[s..i]));
            start = None;
        }
    }
    if let Some(s) = start {
        result.push((s, &text[s..]));
    }
    result
}

// ═══════════════════════════════════════════════════════════════════════
// BM25 scoring
// ═══════════════════════════════════════════════════════════════════════

/// BM25 parameters
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Compute BM25 score for a single term in a single document
fn bm25_score(
    tf: f64,        // term frequency in this doc
    df: f64,        // document frequency
    dl: f64,        // document length (tokens in field)
    avgdl: f64,     // average document length
    n: f64,         // total documents
) -> f64 {
    let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
    let tf_norm = (tf * (BM25_K1 + 1.0)) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl));
    idf * tf_norm
}

// ═══════════════════════════════════════════════════════════════════════
// Fuzzy matching (Levenshtein distance)
// ═══════════════════════════════════════════════════════════════════════

fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let mut prev: Vec<usize> = (0..=b_len).collect();
    let mut curr = vec![0usize; b_len + 1];

    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j] + cost)
                .min(prev[j + 1] + 1)
                .min(curr[j] + 1);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b_len]
}

// ═══════════════════════════════════════════════════════════════════════
// FTS Engine
// ═══════════════════════════════════════════════════════════════════════

pub struct FtsEngine {
    pub storage: Arc<StorageEngine>,
    /// Index definitions: index_name → FtsIndexDefinition
    definitions: RwLock<HashMap<String, FtsIndexDefinition>>,
    /// Index data: index_name → InvertedIndex
    indexes: RwLock<HashMap<String, InvertedIndex>>,
}

impl FtsEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            definitions: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
        }
    }

    // =================================================================
    // Index lifecycle
    // =================================================================

    /// Create a new FTS index
    pub fn create_index(
        &self,
        name: String,
        bucket: String,
        fields: Vec<FtsFieldMapping>,
        analyzer: Option<AnalyzerConfig>,
    ) -> Result<FtsIndexDefinition, String> {
        let mut defs = self.definitions.write().expect("FTS definitions lock poisoned");
        if defs.contains_key(&name) {
            return Err(format!("FTS index '{}' already exists", name));
        }

        let def = FtsIndexDefinition {
            name: name.clone(),
            bucket: bucket.clone(),
            scope: "_default".to_string(),
            collection: "_default".to_string(),
            fields,
            analyzer: analyzer.unwrap_or_default(),
            state: FtsIndexState::Building,
            created_at: Utc::now().to_rfc3339(),
            doc_count: 0,
            term_count: 0,
        };

        defs.insert(name.clone(), def.clone());
        drop(defs);

        // Create empty inverted index
        {
            let mut indexes = self.indexes.write().expect("FTS indexes lock poisoned");
            indexes.insert(name.clone(), InvertedIndex::new());
        }

        info!("FTS index '{}' created on bucket '{}' (fields: {:?})", name, bucket,
            def.fields.iter().map(|f| &f.field).collect::<Vec<_>>());

        Ok(def)
    }

    /// Build (or rebuild) an FTS index from existing documents
    pub fn build_index(&self, index_name: &str) -> Result<usize, String> {
        let def = {
            let defs = self.definitions.read().expect("FTS definitions lock poisoned");
            defs.get(index_name)
                .cloned()
                .ok_or_else(|| format!("FTS index '{}' not found", index_name))?
        };

        // Get all documents from the bucket
        let bucket = self
            .storage
            .get_bucket(&def.bucket)
            .map_err(|e| format!("Bucket error: {}", e))?;
        let docs = bucket.scan_all_documents();

        // Build a fresh inverted index
        let mut inv = InvertedIndex::new();
        let mut indexed_count = 0;
        let mut unique_doc_ids: HashSet<String> = HashSet::new();

        for doc in &docs {
            if doc.deleted {
                continue;
            }
            let doc_key = doc.key.clone();
            let value: serde_json::Value = doc.value.clone();

            self.index_document_into(&mut inv, &def, &doc_key, &value);
            unique_doc_ids.insert(doc_key);
            indexed_count += 1;
        }

        inv.total_docs = unique_doc_ids.len();
        self.recompute_avg_field_len(&mut inv);

        // Count unique terms
        let mut total_terms = 0;
        for fi in inv.fields.values() {
            total_terms += fi.postings.len();
        }

        // Update definition
        {
            let mut defs = self.definitions.write().expect("FTS definitions lock poisoned");
            if let Some(d) = defs.get_mut(index_name) {
                d.state = FtsIndexState::Online;
                d.doc_count = indexed_count;
                d.term_count = total_terms;
            }
        }

        // Store the index
        {
            let mut indexes = self.indexes.write().expect("FTS indexes lock poisoned");
            indexes.insert(index_name.to_string(), inv);
        }

        info!(
            "FTS index '{}' built: {} docs, {} terms",
            index_name, indexed_count, total_terms
        );

        Ok(indexed_count)
    }

    /// Drop an FTS index
    pub fn drop_index(&self, index_name: &str) -> Result<(), String> {
        {
            let mut defs = self.definitions.write().expect("FTS definitions lock poisoned");
            if defs.remove(index_name).is_none() {
                return Err(format!("FTS index '{}' not found", index_name));
            }
        }
        {
            let mut indexes = self.indexes.write().expect("FTS indexes lock poisoned");
            indexes.remove(index_name);
        }
        info!("FTS index '{}' dropped", index_name);
        Ok(())
    }

    /// List all FTS indexes
    pub fn list_indexes(&self) -> Vec<FtsIndexDefinition> {
        let defs = self.definitions.read().expect("FTS definitions lock poisoned");
        defs.values().cloned().collect()
    }

    /// Get a specific FTS index definition
    pub fn get_index(&self, name: &str) -> Option<FtsIndexDefinition> {
        let defs = self.definitions.read().expect("FTS definitions lock poisoned");
        defs.get(name).cloned()
    }

    /// Notify the FTS engine that a document was upserted (online indexing)
    pub fn on_document_upsert(&self, bucket: &str, doc_key: &str, value: &serde_json::Value) {
        let defs = self.definitions.read().expect("FTS definitions lock poisoned");
        let matching_indexes: Vec<(String, FtsIndexDefinition)> = defs
            .iter()
            .filter(|(_, d)| d.bucket == bucket && d.state == FtsIndexState::Online)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        drop(defs);

        for (idx_name, def) in matching_indexes {
            let mut indexes = self.indexes.write().expect("FTS indexes lock poisoned");
            if let Some(inv) = indexes.get_mut(&idx_name) {
                // Remove old postings for this doc
                self.remove_document_from(inv, doc_key);
                // Re-index
                self.index_document_into(inv, &def, doc_key, value);
                inv.total_docs = inv.stored_fields.len();
                self.recompute_avg_field_len(inv);
            }
            drop(indexes);

            // Update doc count
            let mut defs = self.definitions.write().expect("FTS definitions lock poisoned");
            if let Some(d) = defs.get_mut(&idx_name) {
                let indexes = self.indexes.read().expect("FTS indexes lock poisoned");
                if let Some(inv) = indexes.get(&idx_name) {
                    d.doc_count = inv.total_docs;
                    d.term_count = inv.fields.values().map(|fi| fi.postings.len()).sum();
                }
            }
        }
    }

    /// Notify the FTS engine that a document was deleted
    pub fn on_document_delete(&self, bucket: &str, doc_key: &str) {
        let defs = self.definitions.read().expect("FTS definitions lock poisoned");
        let matching_indexes: Vec<String> = defs
            .iter()
            .filter(|(_, d)| d.bucket == bucket && d.state == FtsIndexState::Online)
            .map(|(k, _)| k.clone())
            .collect();
        drop(defs);

        for idx_name in matching_indexes {
            let mut indexes = self.indexes.write().expect("FTS indexes lock poisoned");
            if let Some(inv) = indexes.get_mut(&idx_name) {
                self.remove_document_from(inv, doc_key);
                inv.total_docs = inv.stored_fields.len();
                self.recompute_avg_field_len(inv);
            }
        }
    }

    // =================================================================
    // Search
    // =================================================================

    /// Execute an FTS search
    pub fn search(&self, request: &FtsSearchRequest) -> Result<FtsSearchResult, String> {
        let start = std::time::Instant::now();

        let def = {
            let defs = self.definitions.read().expect("FTS definitions lock poisoned");
            defs.get(&request.index)
                .cloned()
                .ok_or_else(|| format!("FTS index '{}' not found", request.index))?
        };

        if def.state != FtsIndexState::Online {
            return Err(format!("FTS index '{}' is not online (state: {:?})", request.index, def.state));
        }

        let indexes = self.indexes.read().expect("FTS indexes lock poisoned");
        let inv = indexes
            .get(&request.index)
            .ok_or_else(|| format!("FTS index '{}' data not found", request.index))?;

        // Execute query to get doc_key → score
        let scored_docs = self.execute_query(inv, &def, &request.query)?;

        // Sort by score descending
        let mut sorted: Vec<(String, f64)> = scored_docs.into_iter().collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let total_hits = sorted.len();
        let max_score = sorted.first().map(|(_, s)| *s).unwrap_or(0.0);

        // Pagination
        let paged: Vec<(String, f64)> = sorted
            .into_iter()
            .skip(request.from)
            .take(request.size)
            .collect();

        // Build hits
        let mut hits = Vec::new();
        for (doc_key, score) in &paged {
            let mut fields = serde_json::json!({});

            // Get stored fields
            if let Some(stored) = inv.stored_fields.get(doc_key) {
                if request.fields.is_empty() {
                    for (fname, fval) in stored {
                        fields[fname] = serde_json::Value::String(fval.clone());
                    }
                } else {
                    for fname in &request.fields {
                        if let Some(fval) = stored.get(fname) {
                            fields[fname] = serde_json::Value::String(fval.clone());
                        }
                    }
                }
            }

            // Highlighting
            let fragments = if request.highlight {
                Some(self.generate_highlights(inv, &def, doc_key, &request.query))
            } else {
                None
            };

            hits.push(FtsHit {
                index: request.index.clone(),
                id: doc_key.clone(),
                score: *score,
                fields,
                fragments,
            });
        }

        let took_ms = start.elapsed().as_millis() as u64;

        Ok(FtsSearchResult {
            status: "ok".to_string(),
            total_hits,
            max_score,
            hits,
            took_ms,
            facets: serde_json::json!({}),
        })
    }

    // =================================================================
    // Query execution
    // =================================================================

    /// If field is "_all", expand the query across all indexed fields and merge results
    fn execute_query(
        &self,
        inv: &InvertedIndex,
        def: &FtsIndexDefinition,
        query: &FtsQuery,
    ) -> Result<HashMap<String, f64>, String> {
        // Handle _all field by expanding across all indexed fields
        if let Some(field) = self.get_query_field(query) {
            if field == "_all" {
                let all_fields: Vec<String> = inv.fields.keys().cloned().collect();
                let mut combined: HashMap<String, f64> = HashMap::new();
                for f in all_fields {
                    let expanded = self.rewrite_query_field(query, &f);
                    if let Ok(scores) = self.execute_query_inner(inv, def, &expanded) {
                        for (k, v) in scores {
                            *combined.entry(k).or_insert(0.0) += v;
                        }
                    }
                }
                return Ok(combined);
            }
        }
        self.execute_query_inner(inv, def, query)
    }

    fn get_query_field<'a>(&self, query: &'a FtsQuery) -> Option<&'a str> {
        match query {
            FtsQuery::Match { field, .. } => Some(field),
            FtsQuery::MatchPhrase { field, .. } => Some(field),
            FtsQuery::Term { field, .. } => Some(field),
            FtsQuery::Prefix { field, .. } => Some(field),
            FtsQuery::Wildcard { field, .. } => Some(field),
            FtsQuery::Regexp { field, .. } => Some(field),
            FtsQuery::NumericRange { field, .. } => Some(field),
            _ => None,
        }
    }

    fn rewrite_query_field(&self, query: &FtsQuery, new_field: &str) -> FtsQuery {
        match query {
            FtsQuery::Match { query: q, fuzziness, operator, .. } => FtsQuery::Match {
                field: new_field.to_string(), query: q.clone(), fuzziness: *fuzziness, operator: operator.clone(),
            },
            FtsQuery::MatchPhrase { query: q, .. } => FtsQuery::MatchPhrase {
                field: new_field.to_string(), query: q.clone(),
            },
            FtsQuery::Term { term, .. } => FtsQuery::Term {
                field: new_field.to_string(), term: term.clone(),
            },
            FtsQuery::Prefix { prefix, .. } => FtsQuery::Prefix {
                field: new_field.to_string(), prefix: prefix.clone(),
            },
            FtsQuery::Wildcard { wildcard, .. } => FtsQuery::Wildcard {
                field: new_field.to_string(), wildcard: wildcard.clone(),
            },
            FtsQuery::Regexp { regexp, .. } => FtsQuery::Regexp {
                field: new_field.to_string(), regexp: regexp.clone(),
            },
            FtsQuery::NumericRange { min, max, inclusive_min, inclusive_max, .. } => FtsQuery::NumericRange {
                field: new_field.to_string(), min: *min, max: *max, inclusive_min: *inclusive_min, inclusive_max: *inclusive_max,
            },
            other => other.clone(),
        }
    }

    fn execute_query_inner(
        &self,
        inv: &InvertedIndex,
        def: &FtsIndexDefinition,
        query: &FtsQuery,
    ) -> Result<HashMap<String, f64>, String> {
        match query {
            FtsQuery::Match { field, query: q, fuzziness, operator } => {
                let analyzer = self.get_field_analyzer(def, field);
                let tokens = tokenize(q, &analyzer);
                let is_and = operator.as_deref() == Some("and");

                if tokens.is_empty() {
                    return Ok(HashMap::new());
                }

                let mut term_results: Vec<HashMap<String, f64>> = Vec::new();

                for token in &tokens {
                    let mut scores: HashMap<String, f64> = HashMap::new();

                    if *fuzziness > 0 {
                        // Fuzzy match: find all terms within edit distance
                        if let Some(fi) = inv.fields.get(field) {
                            for (term, postings) in &fi.postings {
                                if levenshtein_distance(&token.text, term) <= *fuzziness {
                                    let df = inv.doc_freq
                                        .get(field)
                                        .and_then(|m| m.get(term))
                                        .copied()
                                        .unwrap_or(0) as f64;
                                    let avgdl = inv.avg_field_len.get(field).copied().unwrap_or(1.0);
                                    let n = inv.total_docs as f64;

                                    for (doc_key, positions) in postings {
                                        let tf = positions.len() as f64;
                                        let dl = inv.doc_stats
                                            .get(&(doc_key.clone(), field.clone()))
                                            .map(|s| s.total_tokens as f64)
                                            .unwrap_or(1.0);
                                        let score = bm25_score(tf, df, dl, avgdl, n);
                                        *scores.entry(doc_key.clone()).or_insert(0.0) += score;
                                    }
                                }
                            }
                        }
                    } else {
                        // Exact term match
                        self.score_term(inv, field, &token.text, &mut scores);
                    }

                    term_results.push(scores);
                }

                // Combine results
                if is_and {
                    // Intersection: doc must match all terms
                    self.intersect_scores(&term_results)
                } else {
                    // Union: doc can match any term
                    Ok(self.union_scores(&term_results))
                }
            }

            FtsQuery::MatchPhrase { field, query: q } => {
                let analyzer = self.get_field_analyzer(def, field);
                let tokens = tokenize(q, &analyzer);

                if tokens.is_empty() {
                    return Ok(HashMap::new());
                }

                let mut result: HashMap<String, f64> = HashMap::new();

                if let Some(fi) = inv.fields.get(field) {
                    // Get candidates: docs that contain all terms
                    let mut candidate_docs: Option<HashSet<String>> = None;

                    for token in &tokens {
                        if let Some(postings) = fi.postings.get(&token.text) {
                            let doc_set: HashSet<String> = postings.keys().cloned().collect();
                            candidate_docs = Some(match candidate_docs {
                                Some(prev) => prev.intersection(&doc_set).cloned().collect(),
                                None => doc_set,
                            });
                        } else {
                            return Ok(HashMap::new()); // term not found, no phrase match
                        }
                    }

                    // Check position ordering for each candidate
                    if let Some(candidates) = candidate_docs {
                        for doc_key in candidates {
                            if self.check_phrase_match(fi, &doc_key, &tokens) {
                                // Score based on total terms matching
                                let avgdl = inv.avg_field_len.get(field).copied().unwrap_or(1.0);
                                let n = inv.total_docs as f64;
                                let dl = inv.doc_stats
                                    .get(&(doc_key.clone(), field.clone()))
                                    .map(|s| s.total_tokens as f64)
                                    .unwrap_or(1.0);

                                let mut score = 0.0;
                                for token in &tokens {
                                    let df = inv.doc_freq
                                        .get(field)
                                        .and_then(|m| m.get(&token.text))
                                        .copied()
                                        .unwrap_or(0) as f64;
                                    if let Some(postings) = fi.postings.get(&token.text) {
                                        if let Some(positions) = postings.get(&doc_key) {
                                            score += bm25_score(
                                                positions.len() as f64,
                                                df,
                                                dl,
                                                avgdl,
                                                n,
                                            );
                                        }
                                    }
                                }
                                // Phrase match bonus
                                score *= 1.5;
                                result.insert(doc_key, score);
                            }
                        }
                    }
                }

                Ok(result)
            }

            FtsQuery::Term { field, term } => {
                let mut scores: HashMap<String, f64> = HashMap::new();
                self.score_term(inv, field, term, &mut scores);
                Ok(scores)
            }

            FtsQuery::Prefix { field, prefix } => {
                let mut scores: HashMap<String, f64> = HashMap::new();
                let prefix_lower = prefix.to_lowercase();

                if let Some(fi) = inv.fields.get(field) {
                    for (term, postings) in &fi.postings {
                        if term.starts_with(&prefix_lower) {
                            let df = inv.doc_freq
                                .get(field)
                                .and_then(|m| m.get(term))
                                .copied()
                                .unwrap_or(0) as f64;
                            let avgdl = inv.avg_field_len.get(field).copied().unwrap_or(1.0);
                            let n = inv.total_docs as f64;

                            for (doc_key, positions) in postings {
                                let tf = positions.len() as f64;
                                let dl = inv.doc_stats
                                    .get(&(doc_key.clone(), field.clone()))
                                    .map(|s| s.total_tokens as f64)
                                    .unwrap_or(1.0);
                                let score = bm25_score(tf, df, dl, avgdl, n);
                                *scores.entry(doc_key.clone()).or_insert(0.0) += score;
                            }
                        }
                    }
                }

                Ok(scores)
            }

            FtsQuery::Wildcard { field, wildcard } => {
                let pattern = wildcard.to_lowercase();
                let mut scores: HashMap<String, f64> = HashMap::new();

                if let Some(fi) = inv.fields.get(field) {
                    for (term, postings) in &fi.postings {
                        if wildcard_match(&pattern, term) {
                            let df = inv.doc_freq
                                .get(field)
                                .and_then(|m| m.get(term))
                                .copied()
                                .unwrap_or(0) as f64;
                            let avgdl = inv.avg_field_len.get(field).copied().unwrap_or(1.0);
                            let n = inv.total_docs as f64;

                            for (doc_key, positions) in postings {
                                let tf = positions.len() as f64;
                                let dl = inv.doc_stats
                                    .get(&(doc_key.clone(), field.clone()))
                                    .map(|s| s.total_tokens as f64)
                                    .unwrap_or(1.0);
                                let score = bm25_score(tf, df, dl, avgdl, n);
                                *scores.entry(doc_key.clone()).or_insert(0.0) += score;
                            }
                        }
                    }
                }

                Ok(scores)
            }

            FtsQuery::Regexp { field, regexp } => {
                // Simple regex matching (limited)
                let mut scores: HashMap<String, f64> = HashMap::new();

                if let Some(fi) = inv.fields.get(field) {
                    for (term, postings) in &fi.postings {
                        if simple_regex_match(regexp, term) {
                            let df = inv.doc_freq
                                .get(field)
                                .and_then(|m| m.get(term))
                                .copied()
                                .unwrap_or(0) as f64;
                            let avgdl = inv.avg_field_len.get(field).copied().unwrap_or(1.0);
                            let n = inv.total_docs as f64;

                            for (doc_key, positions) in postings {
                                let tf = positions.len() as f64;
                                let dl = inv.doc_stats
                                    .get(&(doc_key.clone(), field.clone()))
                                    .map(|s| s.total_tokens as f64)
                                    .unwrap_or(1.0);
                                let score = bm25_score(tf, df, dl, avgdl, n);
                                *scores.entry(doc_key.clone()).or_insert(0.0) += score;
                            }
                        }
                    }
                }

                Ok(scores)
            }

            FtsQuery::NumericRange { field, min, max, inclusive_min, inclusive_max } => {
                // For numeric range, we look at stored fields
                let mut scores: HashMap<String, f64> = HashMap::new();

                for (doc_key, stored) in &inv.stored_fields {
                    if let Some(val_str) = stored.get(field) {
                        if let Ok(val) = val_str.parse::<f64>() {
                            let above_min = match min {
                                Some(m) => if *inclusive_min { val >= *m } else { val > *m },
                                None => true,
                            };
                            let below_max = match max {
                                Some(m) => if *inclusive_max { val <= *m } else { val < *m },
                                None => true,
                            };
                            if above_min && below_max {
                                scores.insert(doc_key.clone(), 1.0);
                            }
                        }
                    }
                }

                Ok(scores)
            }

            FtsQuery::Bool { must, should, must_not } => {
                let mut result: HashMap<String, f64> = HashMap::new();

                // Must: all sub-queries must match (intersection)
                if !must.is_empty() {
                    let sub_results: Vec<HashMap<String, f64>> = must
                        .iter()
                        .filter_map(|q| self.execute_query(inv, def, q).ok())
                        .collect();
                    result = self.intersect_scores(&sub_results)?;
                }

                // Should: at least one should match (union), boost scores
                if !should.is_empty() {
                    let sub_results: Vec<HashMap<String, f64>> = should
                        .iter()
                        .filter_map(|q| self.execute_query(inv, def, q).ok())
                        .collect();
                    let should_scores = self.union_scores(&sub_results);

                    if must.is_empty() {
                        // If no must clauses, should acts as the primary filter
                        result = should_scores;
                    } else {
                        // Add should scores to must-matched docs
                        for (doc_key, score) in should_scores {
                            if let Some(existing) = result.get_mut(&doc_key) {
                                *existing += score;
                            }
                        }
                    }
                }

                // Must not: exclude matching docs
                if !must_not.is_empty() {
                    let exclude: HashSet<String> = must_not
                        .iter()
                        .filter_map(|q| self.execute_query(inv, def, q).ok())
                        .flat_map(|m| m.into_keys())
                        .collect();
                    result.retain(|k, _| !exclude.contains(k));
                }

                Ok(result)
            }

            FtsQuery::MatchAll {} => {
                // Return all docs with score 1.0
                let mut result: HashMap<String, f64> = HashMap::new();
                for doc_key in inv.stored_fields.keys() {
                    result.insert(doc_key.clone(), 1.0);
                }
                Ok(result)
            }

            FtsQuery::MatchNone {} => Ok(HashMap::new()),
        }
    }

    /// Score a single exact term
    fn score_term(
        &self,
        inv: &InvertedIndex,
        field: &str,
        term: &str,
        scores: &mut HashMap<String, f64>,
    ) {
        if let Some(fi) = inv.fields.get(field) {
            if let Some(postings) = fi.postings.get(term) {
                let df = inv.doc_freq
                    .get(field)
                    .and_then(|m| m.get(term))
                    .copied()
                    .unwrap_or(0) as f64;
                let avgdl = inv.avg_field_len.get(field).copied().unwrap_or(1.0);
                let n = inv.total_docs as f64;

                for (doc_key, positions) in postings {
                    let tf = positions.len() as f64;
                    let dl = inv.doc_stats
                        .get(&(doc_key.clone(), field.to_string()))
                        .map(|s| s.total_tokens as f64)
                        .unwrap_or(1.0);
                    let score = bm25_score(tf, df, dl, avgdl, n);
                    *scores.entry(doc_key.clone()).or_insert(0.0) += score;
                }
            }
        }
    }

    /// Check if tokens appear as a phrase (consecutive positions) in a document
    fn check_phrase_match(
        &self,
        fi: &FieldIndex,
        doc_key: &str,
        tokens: &[Token],
    ) -> bool {
        if tokens.is_empty() {
            return false;
        }

        // Get positions for each term in this doc
        let mut position_lists: Vec<&Vec<u32>> = Vec::new();
        for token in tokens {
            if let Some(postings) = fi.postings.get(&token.text) {
                if let Some(positions) = postings.get(doc_key) {
                    position_lists.push(positions);
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check for consecutive positions
        // For each starting position of the first term
        for &start_pos in position_lists[0] {
            let mut all_match = true;
            for (i, pos_list) in position_lists.iter().enumerate().skip(1) {
                let expected_pos = start_pos + i as u32;
                if !pos_list.contains(&expected_pos) {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                return true;
            }
        }

        false
    }

    /// Intersect multiple score maps (AND logic)
    fn intersect_scores(
        &self,
        results: &[HashMap<String, f64>],
    ) -> Result<HashMap<String, f64>, String> {
        if results.is_empty() {
            return Ok(HashMap::new());
        }

        let mut combined = results[0].clone();
        for other in &results[1..] {
            combined.retain(|k, _| other.contains_key(k));
            for (k, v) in &mut combined {
                if let Some(other_score) = other.get(k) {
                    *v += *other_score;
                }
            }
        }

        Ok(combined)
    }

    /// Union multiple score maps (OR logic)
    fn union_scores(&self, results: &[HashMap<String, f64>]) -> HashMap<String, f64> {
        let mut combined: HashMap<String, f64> = HashMap::new();
        for result in results {
            for (k, v) in result {
                *combined.entry(k.clone()).or_insert(0.0) += v;
            }
        }
        combined
    }

    // =================================================================
    // Highlighting
    // =================================================================

    fn generate_highlights(
        &self,
        inv: &InvertedIndex,
        def: &FtsIndexDefinition,
        doc_key: &str,
        query: &FtsQuery,
    ) -> HashMap<String, Vec<String>> {
        let mut result: HashMap<String, Vec<String>> = HashMap::new();

        // Extract query terms
        let query_terms = self.extract_query_terms(def, query);

        if let Some(stored) = inv.stored_fields.get(doc_key) {
            for (field, original_text) in stored {
                let field_terms: HashSet<&str> = query_terms
                    .iter()
                    .filter(|(f, _)| f == field)
                    .map(|(_, t)| t.as_str())
                    .collect();

                if field_terms.is_empty() {
                    continue;
                }

                let fragments = self.highlight_text(original_text, &field_terms);
                if !fragments.is_empty() {
                    result.insert(field.clone(), fragments);
                }
            }
        }

        result
    }

    /// Extract (field, term) pairs from a query for highlighting
    fn extract_query_terms(
        &self,
        def: &FtsIndexDefinition,
        query: &FtsQuery,
    ) -> Vec<(String, String)> {
        let mut terms = Vec::new();
        match query {
            FtsQuery::Match { field, query: q, .. } => {
                let analyzer = self.get_field_analyzer(def, field);
                for token in tokenize(q, &analyzer) {
                    terms.push((field.clone(), token.text));
                }
            }
            FtsQuery::MatchPhrase { field, query: q } => {
                let analyzer = self.get_field_analyzer(def, field);
                for token in tokenize(q, &analyzer) {
                    terms.push((field.clone(), token.text));
                }
            }
            FtsQuery::Term { field, term } => {
                terms.push((field.clone(), term.clone()));
            }
            FtsQuery::Prefix { field, prefix } => {
                terms.push((field.clone(), prefix.clone()));
            }
            FtsQuery::Bool { must, should, .. } => {
                for q in must.iter().chain(should.iter()) {
                    terms.extend(self.extract_query_terms(def, q));
                }
            }
            _ => {}
        }
        terms
    }

    /// Generate highlighted fragments from text
    fn highlight_text(&self, text: &str, terms: &HashSet<&str>) -> Vec<String> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let mut fragments = Vec::new();
        let window_size = 10; // words around match

        let mut i = 0;
        while i < words.len() {
            let word_lower = words[i].to_lowercase();
            let word_clean: String = word_lower
                .chars()
                .filter(|c| c.is_alphanumeric())
                .collect();

            if terms.contains(word_clean.as_str()) {
                // Build fragment around this match
                let start = i.saturating_sub(window_size / 2);
                let end = (i + window_size / 2 + 1).min(words.len());

                let mut fragment_parts = Vec::new();
                for j in start..end {
                    let w_lower = words[j].to_lowercase();
                    let w_clean: String = w_lower
                        .chars()
                        .filter(|c| c.is_alphanumeric())
                        .collect();

                    if terms.contains(w_clean.as_str()) {
                        fragment_parts.push(format!("<mark>{}</mark>", words[j]));
                    } else {
                        fragment_parts.push(words[j].to_string());
                    }
                }

                let fragment = if start > 0 {
                    format!("...{}", fragment_parts.join(" "))
                } else {
                    fragment_parts.join(" ")
                };
                let fragment = if end < words.len() {
                    format!("{}...", fragment)
                } else {
                    fragment
                };

                fragments.push(fragment);
                i = end; // skip to end of fragment
            } else {
                i += 1;
            }

            if fragments.len() >= 3 {
                break; // max 3 fragments
            }
        }

        fragments
    }

    // =================================================================
    // Indexing helpers
    // =================================================================

    /// Index a single document into the inverted index
    fn index_document_into(
        &self,
        inv: &mut InvertedIndex,
        def: &FtsIndexDefinition,
        doc_key: &str,
        value: &serde_json::Value,
    ) {
        let fields_to_index = if def.fields.is_empty() {
            // Index all string fields
            self.discover_string_fields(value, "")
        } else {
            def.fields.clone()
        };

        let mut doc_stored: HashMap<String, String> = HashMap::new();

        for field_mapping in &fields_to_index {
            let field_value = get_nested_value(value, &field_mapping.field);
            let text = match field_value {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Number(n)) => {
                    // Store number as string for numeric range queries
                    if field_mapping.store {
                        doc_stored.insert(field_mapping.field.clone(), n.to_string());
                    }
                    continue; // Don't tokenize numbers
                }
                Some(serde_json::Value::Array(arr)) => {
                    // Concatenate array elements
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                }
                _ => continue,
            };

            if text.is_empty() {
                continue;
            }

            if field_mapping.store {
                doc_stored.insert(field_mapping.field.clone(), text.clone());
            }

            let analyzer = self.get_field_analyzer(def, &field_mapping.field);
            let tokens = tokenize(&text, &analyzer);

            let field_name = &field_mapping.field;

            // Ensure field index exists
            if !inv.fields.contains_key(field_name) {
                inv.fields.insert(field_name.clone(), FieldIndex::new());
            }
            let fi = inv.fields.get_mut(field_name).unwrap();

            // Record positions
            let mut token_count = 0u32;
            for token in &tokens {
                fi.postings
                    .entry(token.text.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(doc_key.to_string())
                    .or_insert_with(Vec::new)
                    .push(token.position);

                // Update document frequency
                let df_map = inv.doc_freq
                    .entry(field_name.clone())
                    .or_insert_with(HashMap::new);

                // We track df per-term (will need dedup at build time, but for incremental
                // we just note that this doc has this term)
                // Note: for simplicity, we count postings entries
                let posting_count = fi.postings
                    .get(&token.text)
                    .map(|p| p.len() as u32)
                    .unwrap_or(0);
                df_map.insert(token.text.clone(), posting_count);

                token_count += 1;
            }

            // Store doc field stats
            inv.doc_stats.insert(
                (doc_key.to_string(), field_name.clone()),
                DocFieldStats {
                    total_tokens: token_count,
                },
            );
        }

        inv.stored_fields.insert(doc_key.to_string(), doc_stored);
    }

    /// Remove a document from the inverted index
    fn remove_document_from(&self, inv: &mut InvertedIndex, doc_key: &str) {
        // Remove from postings
        for fi in inv.fields.values_mut() {
            for postings in fi.postings.values_mut() {
                postings.remove(doc_key);
            }
            // Clean up empty postings
            fi.postings.retain(|_, v| !v.is_empty());
        }

        // Remove from doc_stats
        inv.doc_stats.retain(|(dk, _), _| dk != doc_key);

        // Remove from stored fields
        inv.stored_fields.remove(doc_key);

        // Recompute doc_freq
        for (field_name, fi) in &inv.fields {
            let df_map = inv.doc_freq.entry(field_name.clone()).or_insert_with(HashMap::new);
            for (term, postings) in &fi.postings {
                df_map.insert(term.clone(), postings.len() as u32);
            }
        }
    }

    /// Discover all string fields in a JSON value recursively
    fn discover_string_fields(&self, value: &serde_json::Value, prefix: &str) -> Vec<FtsFieldMapping> {
        let mut fields = Vec::new();
        if let Some(obj) = value.as_object() {
            for (key, val) in obj {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };

                match val {
                    serde_json::Value::String(_) => {
                        fields.push(FtsFieldMapping {
                            field: path,
                            analyzer: None,
                            store: true,
                        });
                    }
                    serde_json::Value::Object(_) => {
                        fields.extend(self.discover_string_fields(val, &path));
                    }
                    serde_json::Value::Array(arr) => {
                        // Check if array contains strings
                        if arr.iter().any(|v| v.is_string()) {
                            fields.push(FtsFieldMapping {
                                field: path,
                                analyzer: None,
                                store: true,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        fields
    }

    /// Get analyzer config for a specific field
    fn get_field_analyzer(&self, def: &FtsIndexDefinition, field: &str) -> AnalyzerConfig {
        // Check if field has a specific analyzer override
        for fm in &def.fields {
            if fm.field == field {
                if let Some(ref analyzer_name) = fm.analyzer {
                    return AnalyzerConfig {
                        tokenizer: analyzer_name.clone(),
                        ..def.analyzer.clone()
                    };
                }
            }
        }
        def.analyzer.clone()
    }

    /// Recompute average field length
    fn recompute_avg_field_len(&self, inv: &mut InvertedIndex) {
        let mut field_totals: HashMap<String, (f64, usize)> = HashMap::new();
        for ((_, field), stats) in &inv.doc_stats {
            let entry = field_totals.entry(field.clone()).or_insert((0.0, 0));
            entry.0 += stats.total_tokens as f64;
            entry.1 += 1;
        }
        for (field, (total, count)) in field_totals {
            if count > 0 {
                inv.avg_field_len.insert(field, total / count as f64);
            }
        }
    }

    // =================================================================
    // Persistence
    // =================================================================

    /// Save FTS index definitions to disk
    pub fn save_definitions(&self, data_dir: &str) -> Result<(), String> {
        let defs = self.definitions.read().expect("FTS definitions lock poisoned");
        let all_defs: Vec<FtsIndexDefinition> = defs.values().cloned().collect();

        let path = std::path::Path::new(data_dir).join("fts_indexes.json");
        let json = serde_json::to_string_pretty(&all_defs)
            .map_err(|e| format!("Failed to serialize FTS indexes: {}", e))?;
        std::fs::write(&path, json)
            .map_err(|e| format!("Failed to write FTS indexes to {}: {}", path.display(), e))?;

        info!("Saved {} FTS index definitions to {}", all_defs.len(), path.display());
        Ok(())
    }

    /// Load FTS index definitions from disk and rebuild indexes
    pub fn load_definitions(&self, data_dir: &str) -> Result<usize, String> {
        let path = std::path::Path::new(data_dir).join("fts_indexes.json");
        if !path.exists() {
            return Ok(0);
        }

        let json = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read FTS indexes from {}: {}", path.display(), e))?;
        let saved_defs: Vec<FtsIndexDefinition> = serde_json::from_str(&json)
            .map_err(|e| format!("Failed to parse FTS indexes from {}: {}", path.display(), e))?;

        let mut count = 0;
        for def in saved_defs {
            // Create the index definition
            {
                let mut defs = self.definitions.write().expect("FTS definitions lock poisoned");
                if defs.contains_key(&def.name) {
                    continue;
                }
                defs.insert(def.name.clone(), def.clone());
            }
            {
                let mut indexes = self.indexes.write().expect("FTS indexes lock poisoned");
                indexes.insert(def.name.clone(), InvertedIndex::new());
            }

            // Rebuild index from documents
            match self.build_index(&def.name) {
                Ok(doc_count) => {
                    debug!("Rebuilt FTS index '{}': {} docs", def.name, doc_count);
                }
                Err(e) => {
                    debug!("Failed to rebuild FTS index '{}': {}", def.name, e);
                    // Mark as offline
                    let mut defs = self.definitions.write().expect("FTS definitions lock poisoned");
                    if let Some(d) = defs.get_mut(&def.name) {
                        d.state = FtsIndexState::Offline;
                    }
                }
            }

            count += 1;
        }

        info!("Loaded {} FTS index definitions from {}", count, path.display());
        Ok(count)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Utility functions
// ═══════════════════════════════════════════════════════════════════════

/// Get a nested JSON value by dot-notation path
fn get_nested_value<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;

    for part in parts {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

/// Wildcard pattern matching (supports * and ?)
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();

    fn helper(p: &[char], t: &[char]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        if p[0] == '*' {
            // * matches zero or more characters
            return helper(&p[1..], t) || (!t.is_empty() && helper(p, &t[1..]));
        }
        if !t.is_empty() && (p[0] == '?' || p[0] == t[0]) {
            return helper(&p[1..], &t[1..]);
        }
        false
    }

    helper(&p, &t)
}

/// Simple regex-like matching (very basic: supports . for any char, * for repeat)
fn simple_regex_match(pattern: &str, text: &str) -> bool {
    // Very simplified: just use wildcard matching with . → ? mapping
    let wildcard = pattern.replace('.', "?");
    wildcard_match(&wildcard, text)
}

