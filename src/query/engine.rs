use crate::error::{NosqlError, Result};
use crate::storage::document::Document;
use crate::storage::engine::StorageEngine;
use crate::storage::index::{IndexLookupOp, IndexManager};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

// ═══════════════════════════════════════════════════════════════════════
// Public types
// ═══════════════════════════════════════════════════════════════════════

/// Query request (simplified N1QL-like)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub statement: String,
    /// Named parameters for prepared statements: {"$name": value, ...}
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

/// Query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub status: String,
    pub results: Vec<serde_json::Value>,
    pub metrics: QueryMetrics,
}

/// Query execution metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetrics {
    pub result_count: usize,
    pub elapsed_ms: u64,
    pub scanned_count: usize,
    pub index_used: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════
// Internal AST types
// ═══════════════════════════════════════════════════════════════════════

/// Top-level statement
#[derive(Debug)]
enum Statement {
    Select(SelectQuery),
    /// SELECT without FROM clause (e.g. `SELECT 1`, `SELECT 'keep alive'`)
    DualSelect(Vec<SelectExpr>),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    Explain(Box<Statement>),
}

/// JOIN type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinType {
    Inner,
    Left,
    /// NEST — like JOIN but nests the right-side documents into an array
    Nest,
    /// UNNEST — flattens an array field into rows
    Unnest,
    /// LEFT NEST
    LeftNest,
    /// LEFT UNNEST
    LeftUnnest,
}

/// A single JOIN clause
#[derive(Debug, Clone)]
struct JoinClause {
    join_type: JoinType,
    /// For JOIN/NEST: the bucket (and optional scope.collection) to join
    bucket: Option<String>,
    scope: Option<String>,
    collection: Option<String>,
    /// Alias for the joined source
    alias: Option<String>,
    /// For UNNEST: the path expression to unnest (e.g. "b.addresses")
    unnest_path: Option<String>,
    /// ON condition: (left_field, right_field) — simple equi-join
    on_left: Option<String>,
    on_right: Option<String>,
}

/// SELECT query AST
#[derive(Debug, Clone)]
struct SelectQuery {
    select_exprs: Vec<SelectExpr>,
    distinct: bool,
    bucket: String,
    bucket_alias: Option<String>,
    scope: String,
    collection: String,
    joins: Vec<JoinClause>,
    conditions: Vec<Condition>,
    group_by: Vec<String>,
    having: Vec<Condition>,
    order_by: Vec<(Expr, bool)>, // (expr, ascending)
    limit: Option<usize>,
    offset: Option<usize>,
    use_index: Option<String>,
}

/// INSERT statement
#[derive(Debug)]
struct InsertStmt {
    bucket: String,
    scope: String,
    collection: String,
    key_expr: Expr,
    value_expr: Expr,
    /// Optional: batch insert with returning
    returning: Vec<SelectExpr>,
}

/// UPDATE statement
#[derive(Debug)]
struct UpdateStmt {
    bucket: String,
    scope: String,
    collection: String,
    set_clauses: Vec<(String, Expr)>,
    unset_clauses: Vec<String>,
    conditions: Vec<Condition>,
    limit: Option<usize>,
    returning: Vec<SelectExpr>,
}

/// DELETE statement
#[derive(Debug)]
struct DeleteStmt {
    bucket: String,
    scope: String,
    collection: String,
    conditions: Vec<Condition>,
    limit: Option<usize>,
    returning: Vec<SelectExpr>,
}

/// CREATE INDEX statement
#[derive(Debug)]
struct CreateIndexStmt {
    name: String,
    bucket: String,
    fields: Vec<String>,
    condition: Option<String>,
    /// Array index expressions: (field_position, ArrayIndexExpr)
    array_exprs: Vec<(usize, crate::storage::index::ArrayIndexExpr)>,
    /// Extra fields for covering index (INCLUDE clause)
    include_fields: Vec<String>,
}

/// DROP INDEX statement
#[derive(Debug)]
struct DropIndexStmt {
    bucket: String,
    index_name: String,
}

/// Select expression
#[derive(Debug, Clone)]
enum SelectExpr {
    Star,
    Expr { expr: Expr, alias: Option<String> },
}

/// Expression (for function calls, aggregations, field refs, literals)
#[derive(Debug, Clone)]
enum Expr {
    /// A literal value
    Literal(serde_json::Value),
    /// A field reference (could be nested via dot-notation)
    Field(String),
    /// An aggregate function call: COUNT(*), SUM(field), etc.
    Aggregate { func: AggFunc, arg: Box<Expr> },
    /// A scalar function call: LOWER(field), UPPER(field), etc.
    Function { name: String, args: Vec<Expr> },
    /// META().id — special Couchbase meta expression
    MetaId,
    /// A subquery expression: (SELECT ... FROM ...)
    Subquery(Box<SelectQuery>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// A WHERE condition
#[derive(Debug, Clone)]
struct Condition {
    field: String,
    operator: CompareOp,
    value: serde_json::Value,
}

#[derive(Debug, Clone)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    IsNull,
    IsNotNull,
    In,
    Between,
    /// field IN (SELECT ...) — subquery returns a set of values
    InSubquery(Box<SelectQuery>),
    /// field NOT IN (SELECT ...)
    NotInSubquery(Box<SelectQuery>),
    /// EXISTS (SELECT ...)
    Exists(Box<SelectQuery>),
    /// NOT EXISTS (SELECT ...)
    NotExists(Box<SelectQuery>),
}

// ═══════════════════════════════════════════════════════════════════════
// Query Engine
// ═══════════════════════════════════════════════════════════════════════

/// A cached prepared statement
#[derive(Debug, Clone)]
struct PreparedStatement {
    /// The original statement text (with $param placeholders)
    statement: String,
    /// Name/identifier
    name: String,
    /// Auto-generated encoded plan (for Couchbase SDK compatibility)
    encoded_plan: String,
}

pub struct QueryEngine {
    storage: Arc<StorageEngine>,
    index_manager: Arc<IndexManager>,
    /// Prepared statement cache: name → PreparedStatement
    prepared_statements: DashMap<String, PreparedStatement>,
}

impl QueryEngine {
    pub fn new(storage: Arc<StorageEngine>, index_manager: Arc<IndexManager>) -> Self {
        Self {
            storage,
            index_manager,
            prepared_statements: DashMap::new(),
        }
    }

    /// Persist index definitions to disk
    fn persist_indexes(&self) {
        if let Some(data_dir) = self.storage.data_dir() {
            if let Err(e) = self.index_manager.save_definitions(&data_dir) {
                debug!("Failed to persist index definitions: {}", e);
            }
        }
    }

    /// Execute a query
    pub fn execute(&self, request: &QueryRequest) -> Result<QueryResult> {
        let statement = request.statement.trim();
        let upper = statement.to_uppercase();

        // ── PREPARE name AS statement ──
        if upper.starts_with("PREPARE ") {
            return self.handle_prepare(statement);
        }

        // ── EXECUTE name [USING params] ──
        if upper.starts_with("EXECUTE ") {
            return self.handle_execute(statement, &request.params);
        }

        // Substitute named parameters ($param) if provided
        let final_statement = if let Some(ref params) = request.params {
            self.substitute_params(statement, params)
        } else {
            statement.to_string()
        };

        let stmt = self.parse_statement(&final_statement)?;
        self.execute_statement(&stmt)
    }

    /// List all prepared statements
    pub fn list_prepared_statements(&self) -> Vec<serde_json::Value> {
        self.prepared_statements
            .iter()
            .map(|entry| {
                let ps = entry.value();
                serde_json::json!({
                    "name": ps.name,
                    "statement": ps.statement,
                    "encoded_plan": ps.encoded_plan,
                })
            })
            .collect()
    }

    /// Handle PREPARE statement
    /// Syntax: PREPARE [name] AS statement
    ///   or:   PREPARE [name] FROM statement
    fn handle_prepare(&self, statement: &str) -> Result<QueryResult> {
        let rest = statement[8..].trim();
        let _upper_rest = rest.to_uppercase();

        // Find "AS" or "FROM" separator
        let (name, inner_stmt) = if let Some(as_pos) = find_keyword_outside_parens(rest, " AS ") {
            (
                rest[..as_pos].trim().trim_matches('`').trim_matches('"').to_string(),
                rest[as_pos + 4..].trim().to_string(),
            )
        } else if let Some(from_pos) = find_keyword_outside_parens(rest, " FROM ") {
            (
                rest[..from_pos].trim().trim_matches('`').trim_matches('"').to_string(),
                rest[from_pos + 6..].trim().to_string(),
            )
        } else {
            // Auto-generate name
            let name = format!("ps_{:x}", {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                rest.hash(&mut h);
                h.finish()
            });
            (name, rest.to_string())
        };

        // Validate the inner statement parses correctly (dry run)
        // Replace $params with NULL for validation
        let test_stmt = self.substitute_params_with_nulls(&inner_stmt);
        let _ = self.parse_statement(&test_stmt)?;

        // Generate encoded plan (a simple base64 of name:statement for SDK compat)
        let encoded_plan = base64_encode(&format!("{}:{}", name, inner_stmt));

        let ps = PreparedStatement {
            statement: inner_stmt.clone(),
            name: name.clone(),
            encoded_plan: encoded_plan.clone(),
        };

        self.prepared_statements.insert(name.clone(), ps);

        Ok(QueryResult {
            status: "success".to_string(),
            results: vec![serde_json::json!({
                "name": name,
                "statement": inner_stmt,
                "encoded_plan": encoded_plan,
                "text": format!("PREPARE {}", name),
            })],
            metrics: QueryMetrics {
                result_count: 1,
                elapsed_ms: 0,
                scanned_count: 0,
                index_used: None,
            },
        })
    }

    /// Handle EXECUTE statement
    /// Syntax: EXECUTE name [USING params_json]
    fn handle_execute(&self, statement: &str, request_params: &Option<serde_json::Value>) -> Result<QueryResult> {
        let rest = statement[8..].trim();

        // Parse: name [USING {params}]
        let (name, inline_params) = if let Some(using_pos) = find_keyword_outside_parens(rest, " USING ") {
            let name = rest[..using_pos].trim().trim_matches('`').trim_matches('"').to_string();
            let params_str = rest[using_pos + 7..].trim();
            let params: serde_json::Value = serde_json::from_str(params_str)
                .map_err(|e| NosqlError::QueryError(format!("Invalid USING params JSON: {}", e)))?;
            (name, Some(params))
        } else {
            (rest.trim_matches('`').trim_matches('"').to_string(), None)
        };

        // Look up prepared statement
        let ps = self
            .prepared_statements
            .get(&name)
            .ok_or_else(|| NosqlError::QueryError(format!("Prepared statement '{}' not found", name)))?;

        // Merge params: inline USING overrides request-level params
        let effective_params = inline_params
            .or_else(|| request_params.clone());

        // Substitute parameters
        let final_statement = if let Some(ref params) = effective_params {
            self.substitute_params(&ps.statement, params)
        } else {
            ps.statement.clone()
        };

        let stmt = self.parse_statement(&final_statement)?;
        self.execute_statement(&stmt)
    }

    /// Substitute $param placeholders with actual values from params JSON
    fn substitute_params(&self, statement: &str, params: &serde_json::Value) -> String {
        let mut result = statement.to_string();

        if let Some(obj) = params.as_object() {
            // Named params: {"$type": "user"} or {"type": "user"}
            // Sort by key length descending to avoid partial replacement
            let mut keys: Vec<(&String, &serde_json::Value)> = obj.iter().collect();
            keys.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

            for (key, value) in keys {
                let placeholder = if key.starts_with('$') {
                    key.clone()
                } else {
                    format!("${}", key)
                };
                let replacement = param_value_to_sql(value);
                result = result.replace(&placeholder, &replacement);
            }
        } else if let Some(arr) = params.as_array() {
            // Positional params: [$1, $2, ...]
            for (i, value) in arr.iter().enumerate().rev() {
                let placeholder = format!("${}", i + 1);
                let replacement = param_value_to_sql(value);
                result = result.replace(&placeholder, &replacement);
            }
        }

        result
    }

    /// Replace $param placeholders with NULL for validation/dry-run
    fn substitute_params_with_nulls(&self, statement: &str) -> String {
        let mut result = String::new();
        let chars: Vec<char> = statement.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '$' {
                // Skip the parameter name
                i += 1; // skip $
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                result.push_str("NULL");
            } else if chars[i] == '\'' || chars[i] == '"' {
                // Inside a string literal, don't substitute
                let quote = chars[i];
                result.push(chars[i]);
                i += 1;
                while i < chars.len() && chars[i] != quote {
                    result.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    result.push(chars[i]);
                    i += 1;
                }
            } else {
                result.push(chars[i]);
                i += 1;
            }
        }

        result
    }

    // =================================================================
    // Statement parsing
    // =================================================================

    fn parse_statement(&self, statement: &str) -> Result<Statement> {
        let upper = statement.to_uppercase();
        let upper = upper.trim();

        if upper.starts_with("EXPLAIN ") {
            let inner = statement.trim()[8..].trim();
            let inner_stmt = self.parse_statement(inner)?;
            return Ok(Statement::Explain(Box::new(inner_stmt)));
        }
        if upper.starts_with("SELECT") {
            // Check if this is a FROM-less SELECT (e.g. SELECT 1, SELECT 'keep alive')
            // These are valid in N1QL and used by tools like DataGrip as keep-alive pings
            if !upper.contains(" FROM ") {
                let select_start = if upper.starts_with("SELECT DISTINCT ") { 16 } else { 6 };
                let select_part = statement.trim()[select_start..].trim();
                let select_exprs = self.parse_select_exprs(select_part)?;
                return Ok(Statement::DualSelect(select_exprs));
            }
            return Ok(Statement::Select(self.parse_select(statement)?));
        }
        if upper.starts_with("INSERT") {
            return Ok(Statement::Insert(self.parse_insert(statement)?));
        }
        if upper.starts_with("UPDATE") {
            return Ok(Statement::Update(self.parse_update(statement)?));
        }
        if upper.starts_with("DELETE") {
            return Ok(Statement::Delete(self.parse_delete(statement)?));
        }
        if upper.starts_with("CREATE INDEX") || upper.starts_with("CREATE PRIMARY INDEX") {
            return Ok(Statement::CreateIndex(self.parse_create_index(statement)?));
        }
        if upper.starts_with("DROP INDEX") {
            return Ok(Statement::DropIndex(self.parse_drop_index(statement)?));
        }

        Err(NosqlError::QueryError(format!(
            "Unsupported statement: {}",
            &statement[..statement.len().min(40)]
        )))
    }

    // =================================================================
    // Statement execution
    // =================================================================

    fn execute_statement(&self, stmt: &Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(q) => self.execute_select(q),
            Statement::DualSelect(exprs) => self.execute_dual_select(exprs),
            Statement::Insert(ins) => self.execute_insert(ins),
            Statement::Update(upd) => self.execute_update(upd),
            Statement::Delete(del) => self.execute_delete(del),
            Statement::CreateIndex(ci) => self.execute_create_index(ci),
            Statement::DropIndex(di) => self.execute_drop_index(di),
            Statement::Explain(inner) => self.execute_explain(inner),
        }
    }

    /// Execute a FROM-less SELECT (e.g. `SELECT 1`, `SELECT 'keep alive'`, `SELECT NOW_STR()`)
    /// Returns one row with the evaluated expressions, matching Couchbase N1QL behavior.
    fn execute_dual_select(&self, exprs: &[SelectExpr]) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let null_doc = serde_json::Value::Null;
        let mut row = serde_json::Map::new();

        for (i, se) in exprs.iter().enumerate() {
            match se {
                SelectExpr::Star => {
                    // SELECT * without FROM — return empty row
                }
                SelectExpr::Expr { expr, alias } => {
                    let value = self.resolve_expr(expr, &null_doc, "");
                    let key = alias.clone().unwrap_or_else(|| format!("${}", i + 1));
                    row.insert(key, value);
                }
            }
        }

        let elapsed = start.elapsed().as_millis() as u64;
        Ok(QueryResult {
            status: "success".to_string(),
            results: vec![serde_json::Value::Object(row)],
            metrics: QueryMetrics {
                result_count: 1,
                elapsed_ms: elapsed,
                scanned_count: 0,
                index_used: None,
            },
        })
    }

    // =================================================================
    // EXPLAIN
    // =================================================================

    fn execute_explain(&self, stmt: &Statement) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let plan = match stmt {
            Statement::Select(q) => self.build_select_plan(q),
            Statement::DualSelect(_) => serde_json::json!({"plan": "DUAL_SELECT", "strategy": "expression_eval", "scan": "none"}),
            Statement::Insert(_) => serde_json::json!({"plan": "INSERT", "strategy": "direct_kv_write"}),
            Statement::Update(u) => serde_json::json!({
                "plan": "UPDATE",
                "bucket": u.bucket,
                "strategy": if u.conditions.is_empty() { "full_scan" } else { "conditional_scan" },
                "set_fields": u.set_clauses.iter().map(|(f,_)| f.as_str()).collect::<Vec<_>>(),
                "unset_fields": u.unset_clauses,
            }),
            Statement::Delete(d) => serde_json::json!({
                "plan": "DELETE",
                "bucket": d.bucket,
                "strategy": if d.conditions.is_empty() { "full_scan" } else { "conditional_scan" },
            }),
            _ => serde_json::json!({"plan": "DDL"}),
        };

        let elapsed = start.elapsed().as_millis() as u64;
        Ok(QueryResult {
            status: "success".to_string(),
            results: vec![plan],
            metrics: QueryMetrics {
                result_count: 1,
                elapsed_ms: elapsed,
                scanned_count: 0,
                index_used: None,
            },
        })
    }

    fn build_select_plan(&self, q: &SelectQuery) -> serde_json::Value {
        let has_aggregation = q.select_exprs.iter().any(|e| matches!(e, SelectExpr::Expr { expr, .. } if self.expr_has_aggregate(expr)));
        let index_hint = q.use_index.as_deref();

        // Check which index would be used
        let mut index_name = None;
        if !q.conditions.is_empty() {
            let lookup_conditions = self.conditions_to_lookup(&q.conditions);
            if let Some(hint) = index_hint {
                index_name = Some(hint.to_string());
            } else if let Some(result) = self
                .index_manager
                .find_matching_index(&q.bucket, &lookup_conditions)
            {
                index_name = Some(result.index_name.clone());
            }
        }

        let scan_type = if index_name.is_some() {
            "index_scan"
        } else {
            "full_scan"
        };

        let joins_info: Vec<serde_json::Value> = q.joins.iter().map(|j| {
            serde_json::json!({
                "type": format!("{:?}", j.join_type),
                "bucket": j.bucket,
                "alias": j.alias,
                "unnest_path": j.unnest_path,
                "on_left": j.on_left,
                "on_right": j.on_right,
            })
        }).collect();

        serde_json::json!({
            "plan": "SELECT",
            "bucket": q.bucket,
            "bucket_alias": q.bucket_alias,
            "scope": q.scope,
            "collection": q.collection,
            "scan": scan_type,
            "index": index_name,
            "distinct": q.distinct,
            "aggregation": has_aggregation,
            "joins": joins_info,
            "group_by": q.group_by,
            "having": !q.having.is_empty(),
            "order_by": q.order_by.iter().map(|(e, asc)| {
                serde_json::json!({"expr": format!("{:?}", e), "asc": asc})
            }).collect::<Vec<_>>(),
            "limit": q.limit,
            "offset": q.offset,
            "conditions": q.conditions.len(),
        })
    }

    fn expr_has_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Aggregate { .. } => true,
            Expr::Function { args, .. } => args.iter().any(|a| self.expr_has_aggregate(a)),
            Expr::Subquery(_) => false,
            _ => false,
        }
    }

    // =================================================================
    // SELECT execution
    // =================================================================

    fn execute_select(&self, q: &SelectQuery) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let bucket = self.storage.get_bucket(&q.bucket)?;

        // ── Determine if this is an aggregation query ──
        let has_aggregation = q.select_exprs.iter().any(|e| {
            matches!(e, SelectExpr::Expr { expr, .. } if self.expr_has_aggregate(expr))
        });

        // ── Check if we have JOINs ──
        let has_joins = !q.joins.is_empty();

        // ── Index selection ──
        let mut index_used: Option<String> = None;
        let (candidates, scanned) = if !q.conditions.is_empty() && !has_joins {
            let lookup_conditions = self.conditions_to_lookup(&q.conditions);

            // USE INDEX hint
            let index_result = if let Some(ref hint) = q.use_index {
                self.index_manager
                    .find_matching_index(&q.bucket, &lookup_conditions)
                    .filter(|r| r.index_name == *hint)
            } else {
                self.index_manager
                    .find_matching_index(&q.bucket, &lookup_conditions)
            };

            if let Some(result) = index_result {
                debug!(
                    "Index '{}' used for query ({} keys)",
                    result.index_name,
                    result.doc_keys.len()
                );
                index_used = Some(result.index_name.clone());

                let mut docs = Vec::new();
                for key in &result.doc_keys {
                    if let Ok(doc) = bucket.get("_default", "_default", key) {
                        docs.push(doc);
                    }
                }
                let scanned = docs.len();
                (docs, scanned)
            } else {
                let all_docs = bucket.scan_all_documents();
                let scanned = all_docs.len();
                (all_docs, scanned)
            }
        } else {
            let all_docs = bucket.scan_all_documents();
            let scanned = all_docs.len();
            (all_docs, scanned)
        };

        // ── JOIN execution path ──
        if has_joins {
            let joined_rows = self.execute_joins(q, &candidates)?;

            // Apply WHERE on the joined rows
            let filtered: Vec<serde_json::Value> = joined_rows
                .into_iter()
                .filter(|row| self.matches_conditions_on_row(row, &q.conditions, q))
                .collect();

            // Project or aggregate
            if has_aggregation || !q.group_by.is_empty() {
                // For aggregation on joined data we'd need Document wrappers — simplified:
                let results: Vec<serde_json::Value> = filtered;
                let elapsed = start.elapsed().as_millis() as u64;
                let result_count = results.len();
                return Ok(QueryResult {
                    status: "success".to_string(),
                    results,
                    metrics: QueryMetrics {
                        result_count,
                        elapsed_ms: elapsed,
                        scanned_count: scanned,
                        index_used,
                    },
                });
            }

            let mut results = self.project_joined_rows(&filtered, &q.select_exprs, q);

            // DISTINCT
            if q.distinct {
                let mut seen = std::collections::HashSet::new();
                results.retain(|v| {
                    let key = v.to_string();
                    seen.insert(key)
                });
            }

            // ORDER BY
            if !q.order_by.is_empty() {
                results.sort_by(|a, b| {
                    for (expr, ascending) in &q.order_by {
                        let va = self.eval_expr_on_result(expr, a);
                        let vb = self.eval_expr_on_result(expr, b);
                        let cmp = compare_json_values(Some(&va), Some(&vb));
                        if cmp != std::cmp::Ordering::Equal {
                            return if *ascending { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            // OFFSET
            if let Some(offset) = q.offset {
                if offset < results.len() {
                    results = results[offset..].to_vec();
                } else {
                    results.clear();
                }
            }

            // LIMIT
            if let Some(limit) = q.limit {
                results.truncate(limit);
            }

            let elapsed = start.elapsed().as_millis() as u64;
            let result_count = results.len();
            return Ok(QueryResult {
                status: "success".to_string(),
                results,
                metrics: QueryMetrics {
                    result_count,
                    elapsed_ms: elapsed,
                    scanned_count: scanned,
                    index_used,
                },
            });
        }

        // ── Apply WHERE conditions (non-join path) ──
        let filtered: Vec<&Document> = candidates
            .iter()
            .filter(|doc| self.matches_conditions(doc, &q.conditions))
            .collect();

        // ── Aggregation path ──
        if has_aggregation || !q.group_by.is_empty() {
            let results = self.execute_aggregation(q, &filtered)?;
            let elapsed = start.elapsed().as_millis() as u64;
            let result_count = results.len();
            return Ok(QueryResult {
                status: "success".to_string(),
                results,
                metrics: QueryMetrics {
                    result_count,
                    elapsed_ms: elapsed,
                    scanned_count: scanned,
                    index_used,
                },
            });
        }

        // ── ORDER BY (sort documents before projection) ──
        let mut filtered: Vec<&Document> = filtered;
        if !q.order_by.is_empty() {
            filtered.sort_by(|a, b| {
                for (expr, ascending) in &q.order_by {
                    let va = self.eval_expr_on_doc(expr, a);
                    let vb = self.eval_expr_on_doc(expr, b);
                    let cmp = compare_json_values(Some(&va), Some(&vb));
                    if cmp != std::cmp::Ordering::Equal {
                        return if *ascending { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // ── OFFSET ──
        if let Some(offset) = q.offset {
            if offset < filtered.len() {
                filtered = filtered[offset..].to_vec();
            } else {
                filtered.clear();
            }
        }

        // ── LIMIT ──
        if let Some(limit) = q.limit {
            filtered.truncate(limit);
        }

        // ── Non-aggregation: project fields ──
        let mut results: Vec<serde_json::Value> = filtered
            .iter()
            .map(|doc| self.project_row(doc, &q.select_exprs))
            .collect();

        // ── DISTINCT ──
        if q.distinct {
            let mut seen = std::collections::HashSet::new();
            results.retain(|v| {
                let key = v.to_string();
                seen.insert(key)
            });
        }

        let elapsed = start.elapsed().as_millis() as u64;
        let result_count = results.len();

        debug!(
            "Query executed: {} results from {} scanned in {}ms (index: {:?})",
            result_count, scanned, elapsed, index_used
        );

        Ok(QueryResult {
            status: "success".to_string(),
            results,
            metrics: QueryMetrics {
                result_count,
                elapsed_ms: elapsed,
                scanned_count: scanned,
                index_used,
            },
        })
    }

    // =================================================================
    // JOIN execution
    // =================================================================

    /// Execute JOINs and produce a list of "joined rows" (JSON objects)
    /// Each row contains aliased fields from each source.
    fn execute_joins(
        &self,
        q: &SelectQuery,
        left_docs: &[Document],
    ) -> Result<Vec<serde_json::Value>> {
        // The left alias defaults to the bucket name
        let left_alias = q
            .bucket_alias
            .as_deref()
            .unwrap_or(&q.bucket);

        // Start with left-side rows
        let mut rows: Vec<serde_json::Value> = left_docs
            .iter()
            .filter(|d| !d.deleted && !d.is_expired())
            .map(|doc| {
                let mut row = serde_json::Map::new();
                row.insert(
                    left_alias.to_string(),
                    doc.value.clone(),
                );
                row.insert(
                    format!("__{}_key", left_alias),
                    serde_json::Value::String(doc.key.clone()),
                );
                serde_json::Value::Object(row)
            })
            .collect();

        // Process each JOIN clause sequentially
        for join in &q.joins {
            rows = self.apply_join(join, &rows, left_alias)?;
        }

        Ok(rows)
    }

    /// Apply a single JOIN clause to the current row set
    fn apply_join(
        &self,
        join: &JoinClause,
        left_rows: &[serde_json::Value],
        _left_alias: &str,
    ) -> Result<Vec<serde_json::Value>> {
        match join.join_type {
            JoinType::Unnest | JoinType::LeftUnnest => {
                self.apply_unnest(join, left_rows)
            }
            _ => {
                self.apply_bucket_join(join, left_rows)
            }
        }
    }

    /// Apply UNNEST: flatten an array field into rows
    fn apply_unnest(
        &self,
        join: &JoinClause,
        left_rows: &[serde_json::Value],
    ) -> Result<Vec<serde_json::Value>> {
        let path = join
            .unnest_path
            .as_deref()
            .ok_or_else(|| NosqlError::QueryError("UNNEST requires a path".to_string()))?;
        let alias = join
            .alias
            .as_deref()
            .unwrap_or("_unnest");
        let is_left = join.join_type == JoinType::LeftUnnest;

        let mut result = Vec::new();

        for row in left_rows {
            // Resolve the path against the row
            let arr_val = resolve_path_in_row(row, path);

            if let Some(serde_json::Value::Array(items)) = arr_val {
                if items.is_empty() && is_left {
                    // LEFT UNNEST: keep the row with null for the alias
                    let mut new_row = row.as_object().cloned().unwrap_or_default();
                    new_row.insert(alias.to_string(), serde_json::Value::Null);
                    result.push(serde_json::Value::Object(new_row));
                } else {
                    for item in &items {
                        let mut new_row = row.as_object().cloned().unwrap_or_default();
                        new_row.insert(alias.to_string(), item.clone());
                        result.push(serde_json::Value::Object(new_row));
                    }
                }
            } else if is_left {
                let mut new_row = row.as_object().cloned().unwrap_or_default();
                new_row.insert(alias.to_string(), serde_json::Value::Null);
                result.push(serde_json::Value::Object(new_row));
            }
            // For inner UNNEST, if the field is not an array, skip the row
        }

        Ok(result)
    }

    /// Apply JOIN or NEST with another bucket
    fn apply_bucket_join(
        &self,
        join: &JoinClause,
        left_rows: &[serde_json::Value],
    ) -> Result<Vec<serde_json::Value>> {
        let right_bucket_name = join
            .bucket
            .as_deref()
            .ok_or_else(|| NosqlError::QueryError("JOIN requires a bucket name".to_string()))?;
        let right_scope = join.scope.as_deref().unwrap_or("_default");
        let right_collection = join.collection.as_deref().unwrap_or("_default");
        let right_alias = join
            .alias
            .as_deref()
            .unwrap_or(right_bucket_name);
        let is_left = join.join_type == JoinType::Left
            || join.join_type == JoinType::LeftNest;
        let is_nest = join.join_type == JoinType::Nest
            || join.join_type == JoinType::LeftNest;

        let right_bucket = self.storage.get_bucket(right_bucket_name)?;
        let right_docs = right_bucket.scan_all_documents();

        // Build an index of right docs by the right-side ON field value
        let on_right = join
            .on_right
            .as_deref()
            .unwrap_or("");

        // Determine if right side references META().id
        let right_is_meta = on_right.to_uppercase().contains("META(")
            && on_right.to_uppercase().contains(").ID");

        // Build a multimap: right_key_value → Vec<&Document>
        let mut right_index: HashMap<String, Vec<&Document>> = HashMap::new();
        for doc in &right_docs {
            if doc.deleted || doc.is_expired() {
                continue;
            }
            let key_val = if right_is_meta {
                serde_json::Value::String(doc.key.clone())
            } else {
                // Strip alias prefix if present
                let field = strip_alias_prefix(on_right, right_alias);
                extract_field_value(&doc.value, &field)
            };
            let key_str = val_to_group_key(&key_val);
            right_index.entry(key_str).or_default().push(doc);
        }

        let on_left = join.on_left.as_deref().unwrap_or("");

        let mut result = Vec::new();

        for row in left_rows {
            // Evaluate the left ON field against the current row
            let left_val = resolve_path_in_row(row, on_left);
            let left_key = left_val
                .as_ref()
                .map(|v| val_to_group_key(v))
                .unwrap_or_default();

            let matches = right_index.get(&left_key);

            if is_nest {
                // NEST: collect all matching right docs into an array
                let nested_arr: Vec<serde_json::Value> = matches
                    .map(|docs| docs.iter().map(|d| d.value.clone()).collect())
                    .unwrap_or_default();

                if nested_arr.is_empty() && !is_left {
                    continue; // INNER NEST: skip if no match
                }

                let mut new_row = row.as_object().cloned().unwrap_or_default();
                if nested_arr.is_empty() {
                    new_row.insert(right_alias.to_string(), serde_json::Value::Null);
                } else {
                    new_row.insert(
                        right_alias.to_string(),
                        serde_json::Value::Array(nested_arr),
                    );
                }
                result.push(serde_json::Value::Object(new_row));
            } else {
                // JOIN: produce one row per matching right doc
                match matches {
                    Some(right_docs) if !right_docs.is_empty() => {
                        for rdoc in right_docs {
                            let mut new_row = row.as_object().cloned().unwrap_or_default();
                            new_row.insert(right_alias.to_string(), rdoc.value.clone());
                            new_row.insert(
                                format!("__{}_key", right_alias),
                                serde_json::Value::String(rdoc.key.clone()),
                            );
                            // Also validate scope/collection
                            let _ = right_bucket.validate_path_public(right_scope, right_collection);
                            result.push(serde_json::Value::Object(new_row));
                        }
                    }
                    _ => {
                        if is_left {
                            let mut new_row = row.as_object().cloned().unwrap_or_default();
                            new_row.insert(right_alias.to_string(), serde_json::Value::Null);
                            result.push(serde_json::Value::Object(new_row));
                        }
                        // INNER JOIN: skip row if no match
                    }
                }
            }
        }

        Ok(result)
    }

    /// Match WHERE conditions against a joined row (JSON object with aliases)
    fn matches_conditions_on_row(
        &self,
        row: &serde_json::Value,
        conditions: &[Condition],
        q: &SelectQuery,
    ) -> bool {
        conditions.iter().all(|cond| {
            let field_value = self.resolve_field_in_row(row, &cond.field, q);
            match_condition_value(field_value.as_ref(), &cond.operator, &cond.value)
        })
    }

    /// Resolve a field reference against a joined row.
    /// Handles alias.field syntax (e.g. "o.name", "c.city")
    fn resolve_field_in_row(
        &self,
        row: &serde_json::Value,
        field: &str,
        q: &SelectQuery,
    ) -> Option<serde_json::Value> {
        // Try META().id
        if field.to_uppercase() == "META().ID" {
            let alias = q.bucket_alias.as_deref().unwrap_or(&q.bucket);
            return row.get(&format!("__{}_key", alias)).cloned();
        }

        // Try direct field on the row
        if let Some(v) = row.get(field) {
            if !v.is_null() {
                return Some(v.clone());
            }
        }

        // Try alias.field
        if let Some(dot_pos) = field.find('.') {
            let alias = &field[..dot_pos];
            let rest = &field[dot_pos + 1..];

            if let Some(source_val) = row.get(alias) {
                let v = extract_field_value(source_val, rest);
                if !v.is_null() {
                    return Some(v);
                }
            }
        }

        // Try searching in each source in the row
        if let Some(obj) = row.as_object() {
            for (_, source_val) in obj {
                if source_val.is_object() {
                    let v = extract_field_value(source_val, field);
                    if !v.is_null() {
                        return Some(v);
                    }
                }
            }
        }

        None
    }

    /// Project SELECT expressions from joined rows
    fn project_joined_rows(
        &self,
        rows: &[serde_json::Value],
        select_exprs: &[SelectExpr],
        q: &SelectQuery,
    ) -> Vec<serde_json::Value> {
        let is_star = select_exprs
            .iter()
            .any(|e| matches!(e, SelectExpr::Star));

        rows.iter()
            .map(|row| {
                if is_star {
                    // Return all aliased sources merged
                    let mut out = serde_json::Map::new();
                    if let Some(obj) = row.as_object() {
                        for (k, v) in obj {
                            if !k.starts_with("__") {
                                out.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    serde_json::Value::Object(out)
                } else {
                    let mut out = serde_json::Map::new();
                    for sel in select_exprs {
                        if let SelectExpr::Expr { expr, alias } = sel {
                            let val = self.eval_expr_on_joined_row(expr, row, q);
                            let name = alias
                                .clone()
                                .unwrap_or_else(|| expr_display_name(expr));
                            out.insert(name, val);
                        }
                    }
                    serde_json::Value::Object(out)
                }
            })
            .collect()
    }

    /// Evaluate an expression against a joined row
    fn eval_expr_on_joined_row(
        &self,
        expr: &Expr,
        row: &serde_json::Value,
        q: &SelectQuery,
    ) -> serde_json::Value {
        match expr {
            Expr::Literal(v) => v.clone(),
            Expr::Field(f) => {
                self.resolve_field_in_row(row, f, q)
                    .unwrap_or(serde_json::Value::Null)
            }
            Expr::MetaId => {
                let alias = q.bucket_alias.as_deref().unwrap_or(&q.bucket);
                row.get(&format!("__{}_key", alias))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            }
            Expr::Function { name, args } => {
                // Build a merged doc from all sources for function evaluation
                let merged = self.merge_row_sources(row);
                let alias = q.bucket_alias.as_deref().unwrap_or(&q.bucket);
                let doc_key = row
                    .get(&format!("__{}_key", alias))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                self.eval_function(name, args, &merged, doc_key)
            }
            Expr::Aggregate { .. } => serde_json::Value::Null,
            Expr::Subquery(sub) => {
                match self.execute_select(sub) {
                    Ok(result) => result.results.first()
                        .and_then(|r| if let Some(obj) = r.as_object() { obj.values().next().cloned() } else { Some(r.clone()) })
                        .unwrap_or(serde_json::Value::Null),
                    Err(_) => serde_json::Value::Null,
                }
            }
        }
    }

    /// Merge all sources in a joined row into a single flat JSON object
    fn merge_row_sources(&self, row: &serde_json::Value) -> serde_json::Value {
        let mut merged = serde_json::Map::new();
        if let Some(obj) = row.as_object() {
            for (k, v) in obj {
                if k.starts_with("__") {
                    continue; // skip internal keys like __alias_key
                }
                // Keep alias as a top-level key for alias.field resolution
                merged.insert(k.clone(), v.clone());
                // Also merge individual fields from source objects into top-level
                if let Some(inner_obj) = v.as_object() {
                    for (ik, iv) in inner_obj {
                        merged.entry(ik.clone()).or_insert_with(|| iv.clone());
                    }
                }
            }
        }
        serde_json::Value::Object(merged)
    }

    // =================================================================
    // Aggregation
    // =================================================================

    fn execute_aggregation(
        &self,
        q: &SelectQuery,
        docs: &[&Document],
    ) -> Result<Vec<serde_json::Value>> {
        // Group documents
        let groups = if q.group_by.is_empty() {
            // Single group for the entire result set
            let mut map: HashMap<String, Vec<&Document>> = HashMap::new();
            map.insert("__all__".to_string(), docs.to_vec());
            map
        } else {
            let mut map: HashMap<String, Vec<&Document>> = HashMap::new();
            for doc in docs {
                let key = q
                    .group_by
                    .iter()
                    .map(|f| {
                        let v = extract_field_value(&doc.value, f);
                        val_to_group_key(&v)
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                map.entry(key).or_default().push(doc);
            }
            map
        };

        let mut results = Vec::new();

        for (_group_key, group_docs) in &groups {
            let mut row = serde_json::Map::new();

            // Add GROUP BY fields
            if !q.group_by.is_empty() {
                if let Some(first_doc) = group_docs.first() {
                    for field in &q.group_by {
                        let v = extract_field_value(&first_doc.value, field);
                        row.insert(field.clone(), v);
                    }
                }
            }

            // Evaluate SELECT expressions
            for sel in &q.select_exprs {
                match sel {
                    SelectExpr::Star => {
                        // In aggregation with GROUP BY, * doesn't make much sense
                        // but we can return the first doc's fields
                        if let Some(first_doc) = group_docs.first() {
                            if let serde_json::Value::Object(obj) = &first_doc.value {
                                for (k, v) in obj {
                                    row.entry(k.clone()).or_insert_with(|| v.clone());
                                }
                            }
                        }
                    }
                    SelectExpr::Expr { expr, alias } => {
                        let (name, val) = self.eval_aggregate_expr(expr, group_docs, alias)?;
                        row.insert(name, val);
                    }
                }
            }

            let row_val = serde_json::Value::Object(row);

            // Apply HAVING
            if !q.having.is_empty() {
                // Create a dummy doc for condition matching
                let passes = q.having.iter().all(|cond| {
                    let field_val = row_val.get(&cond.field);
                    match_condition_value(field_val, &cond.operator, &cond.value)
                });
                if !passes {
                    continue;
                }
            }

            results.push(row_val);
        }

        // ORDER BY for aggregation results
        if !q.order_by.is_empty() {
            results.sort_by(|a, b| {
                for (expr, ascending) in &q.order_by {
                    let va = self.eval_expr_on_result(expr, a);
                    let vb = self.eval_expr_on_result(expr, b);
                    let cmp = compare_json_values(Some(&va), Some(&vb));
                    if cmp != std::cmp::Ordering::Equal {
                        return if *ascending { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // OFFSET
        if let Some(offset) = q.offset {
            if offset < results.len() {
                results = results[offset..].to_vec();
            } else {
                results.clear();
            }
        }

        // LIMIT
        if let Some(limit) = q.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    fn eval_aggregate_expr(
        &self,
        expr: &Expr,
        docs: &[&Document],
        alias: &Option<String>,
    ) -> Result<(String, serde_json::Value)> {
        match expr {
            Expr::Aggregate { func, arg } => {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| format!("{}({})", agg_func_name(func), expr_name(arg)));
                let val = self.compute_aggregate(func, arg, docs);
                Ok((name, val))
            }
            Expr::Field(f) => {
                let name = alias.clone().unwrap_or_else(|| f.clone());
                // In aggregation context, take the first value
                let val = docs
                    .first()
                    .map(|d| extract_field_value(&d.value, f))
                    .unwrap_or(serde_json::Value::Null);
                Ok((name, val))
            }
            Expr::Function { name: fn_name, args } => {
                let out_name = alias
                    .clone()
                    .unwrap_or_else(|| format!("{}()", fn_name));
                // Evaluate the function on the first doc
                let val = docs
                    .first()
                    .map(|d| self.eval_function(fn_name, args, &d.value, &d.key))
                    .unwrap_or(serde_json::Value::Null);
                Ok((out_name, val))
            }
            Expr::MetaId => {
                let name = alias.clone().unwrap_or_else(|| "META().id".to_string());
                let val = docs
                    .first()
                    .map(|d| serde_json::Value::String(d.key.clone()))
                    .unwrap_or(serde_json::Value::Null);
                Ok((name, val))
            }
            Expr::Literal(v) => {
                let name = alias.clone().unwrap_or_else(|| v.to_string());
                Ok((name, v.clone()))
            }
            Expr::Subquery(sub) => {
                let name = alias.clone().unwrap_or_else(|| "(SUBQUERY)".to_string());
                let val = match self.execute_select(sub) {
                    Ok(result) => result.results.first()
                        .and_then(|r| if let Some(obj) = r.as_object() { obj.values().next().cloned() } else { Some(r.clone()) })
                        .unwrap_or(serde_json::Value::Null),
                    Err(_) => serde_json::Value::Null,
                };
                Ok((name, val))
            }
        }
    }

    fn compute_aggregate(
        &self,
        func: &AggFunc,
        arg: &Expr,
        docs: &[&Document],
    ) -> serde_json::Value {
        match func {
            AggFunc::Count => {
                if matches!(arg, Expr::Field(f) if f == "*") || matches!(arg, Expr::Literal(_)) {
                    serde_json::json!(docs.len())
                } else {
                    // Count non-null values
                    let count = docs
                        .iter()
                        .filter(|d| {
                            let v = self.eval_expr_on_doc(arg, d);
                            !v.is_null()
                        })
                        .count();
                    serde_json::json!(count)
                }
            }
            AggFunc::Sum => {
                let sum: f64 = docs
                    .iter()
                    .filter_map(|d| {
                        let v = self.eval_expr_on_doc(arg, d);
                        v.as_f64()
                    })
                    .sum();
                serde_json::json!(sum)
            }
            AggFunc::Avg => {
                let vals: Vec<f64> = docs
                    .iter()
                    .filter_map(|d| {
                        let v = self.eval_expr_on_doc(arg, d);
                        v.as_f64()
                    })
                    .collect();
                if vals.is_empty() {
                    serde_json::Value::Null
                } else {
                    let sum: f64 = vals.iter().sum();
                    serde_json::json!(sum / vals.len() as f64)
                }
            }
            AggFunc::Min => {
                let mut min: Option<serde_json::Value> = None;
                for d in docs {
                    let v = self.eval_expr_on_doc(arg, d);
                    if v.is_null() {
                        continue;
                    }
                    if let Some(ref current_min) = min {
                        if compare_json_values(Some(&v), Some(current_min))
                            == std::cmp::Ordering::Less
                        {
                            min = Some(v);
                        }
                    } else {
                        min = Some(v);
                    }
                }
                min.unwrap_or(serde_json::Value::Null)
            }
            AggFunc::Max => {
                let mut max: Option<serde_json::Value> = None;
                for d in docs {
                    let v = self.eval_expr_on_doc(arg, d);
                    if v.is_null() {
                        continue;
                    }
                    if let Some(ref current_max) = max {
                        if compare_json_values(Some(&v), Some(current_max))
                            == std::cmp::Ordering::Greater
                        {
                            max = Some(v);
                        }
                    } else {
                        max = Some(v);
                    }
                }
                max.unwrap_or(serde_json::Value::Null)
            }
        }
    }

    fn eval_expr_on_doc(&self, expr: &Expr, doc: &Document) -> serde_json::Value {
        match expr {
            Expr::Literal(v) => v.clone(),
            Expr::Field(f) => {
                if f == "*" {
                    doc.value.clone()
                } else {
                    extract_field_value(&doc.value, f)
                }
            }
            Expr::Aggregate { .. } => serde_json::Value::Null, // should not happen here
            Expr::Function { name, args } => self.eval_function(name, args, &doc.value, &doc.key),
            Expr::MetaId => serde_json::Value::String(doc.key.clone()),
            Expr::Subquery(sub) => {
                // Scalar subquery: return first value of first row
                match self.execute_select(sub) {
                    Ok(result) => {
                        result.results.first()
                            .and_then(|row| {
                                if let Some(obj) = row.as_object() {
                                    obj.values().next().cloned()
                                } else {
                                    Some(row.clone())
                                }
                            })
                            .unwrap_or(serde_json::Value::Null)
                    }
                    Err(_) => serde_json::Value::Null,
                }
            }
        }
    }

    fn eval_expr_on_result(&self, expr: &Expr, row: &serde_json::Value) -> serde_json::Value {
        match expr {
            Expr::Literal(v) => v.clone(),
            Expr::Field(f) => {
                // Try direct field, then nested in "doc"
                row.get(f)
                    .or_else(|| row.get("doc").and_then(|d| d.get(f)))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            }
            Expr::MetaId => row
                .get("_key")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            Expr::Function { name, args } => {
                // Evaluate function on row context
                let doc_val = row
                    .get("doc")
                    .cloned()
                    .unwrap_or_else(|| row.clone());
                let key = row
                    .get("_key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                self.eval_function(name, args, &doc_val, &key)
            }
            Expr::Aggregate { .. } => serde_json::Value::Null,
            Expr::Subquery(sub) => {
                match self.execute_select(sub) {
                    Ok(result) => {
                        result.results.first()
                            .and_then(|row| {
                                if let Some(obj) = row.as_object() {
                                    obj.values().next().cloned()
                                } else {
                                    Some(row.clone())
                                }
                            })
                            .unwrap_or(serde_json::Value::Null)
                    }
                    Err(_) => serde_json::Value::Null,
                }
            }
        }
    }

    // =================================================================
    // N1QL Functions
    // =================================================================

    fn eval_function(
        &self,
        name: &str,
        args: &[Expr],
        doc_value: &serde_json::Value,
        doc_key: &str,
    ) -> serde_json::Value {
        let resolved_args: Vec<serde_json::Value> = args
            .iter()
            .map(|a| self.resolve_expr(a, doc_value, doc_key))
            .collect();

        match name.to_uppercase().as_str() {
            // ── String functions ──
            "LOWER" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::Value::String(s.to_lowercase()))
                .unwrap_or(serde_json::Value::Null),

            "UPPER" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::Value::String(s.to_uppercase()))
                .unwrap_or(serde_json::Value::Null),

            "LENGTH" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::json!(s.len()))
                .unwrap_or(serde_json::Value::Null),

            "TRIM" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::Value::String(s.trim().to_string()))
                .unwrap_or(serde_json::Value::Null),

            "LTRIM" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::Value::String(s.trim_start().to_string()))
                .unwrap_or(serde_json::Value::Null),

            "RTRIM" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::Value::String(s.trim_end().to_string()))
                .unwrap_or(serde_json::Value::Null),

            "SUBSTR" | "SUBSTRING" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                let pos = resolved_args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                let len = resolved_args.get(2).and_then(|v| v.as_i64());
                match s {
                    Some(s) => {
                        if pos >= s.len() {
                            serde_json::Value::String(String::new())
                        } else if let Some(len) = len {
                            let end = (pos + len as usize).min(s.len());
                            serde_json::Value::String(s[pos..end].to_string())
                        } else {
                            serde_json::Value::String(s[pos..].to_string())
                        }
                    }
                    None => serde_json::Value::Null,
                }
            }

            "CONCAT" => {
                let mut result = String::new();
                for arg in &resolved_args {
                    match arg {
                        serde_json::Value::String(s) => result.push_str(s),
                        serde_json::Value::Null => {}
                        other => result.push_str(&other.to_string()),
                    }
                }
                serde_json::Value::String(result)
            }

            "CONTAINS" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                let sub = resolved_args.get(1).and_then(|v| v.as_str());
                match (s, sub) {
                    (Some(s), Some(sub)) => serde_json::Value::Bool(s.contains(sub)),
                    _ => serde_json::Value::Null,
                }
            }

            "REPLACE" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                let from = resolved_args.get(1).and_then(|v| v.as_str());
                let to = resolved_args.get(2).and_then(|v| v.as_str());
                match (s, from, to) {
                    (Some(s), Some(from), Some(to)) => {
                        serde_json::Value::String(s.replace(from, to))
                    }
                    _ => serde_json::Value::Null,
                }
            }

            "SPLIT" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                let sep = resolved_args
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or(",");
                match s {
                    Some(s) => {
                        let parts: Vec<serde_json::Value> = s
                            .split(sep)
                            .map(|p| serde_json::Value::String(p.to_string()))
                            .collect();
                        serde_json::Value::Array(parts)
                    }
                    None => serde_json::Value::Null,
                }
            }

            "REPEAT" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                let n = resolved_args.get(1).and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                match s {
                    Some(s) => serde_json::Value::String(s.repeat(n)),
                    None => serde_json::Value::Null,
                }
            }

            "REVERSE" => resolved_args
                .first()
                .and_then(|v| v.as_str())
                .map(|s| serde_json::Value::String(s.chars().rev().collect()))
                .unwrap_or(serde_json::Value::Null),

            "POSITION" | "POSITION0" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                let sub = resolved_args.get(1).and_then(|v| v.as_str());
                match (s, sub) {
                    (Some(s), Some(sub)) => match s.find(sub) {
                        Some(pos) => serde_json::json!(pos),
                        None => serde_json::json!(-1),
                    },
                    _ => serde_json::Value::Null,
                }
            }

            // ── Math functions ──
            "ABS" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| serde_json::json!(n.abs()))
                .unwrap_or(serde_json::Value::Null),

            "CEIL" | "CEILING" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| serde_json::json!(n.ceil()))
                .unwrap_or(serde_json::Value::Null),

            "FLOOR" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| serde_json::json!(n.floor()))
                .unwrap_or(serde_json::Value::Null),

            "ROUND" => {
                let n = resolved_args.first().and_then(|v| v.as_f64());
                let decimals = resolved_args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                match n {
                    Some(n) => {
                        let factor = 10_f64.powi(decimals as i32);
                        serde_json::json!((n * factor).round() / factor)
                    }
                    None => serde_json::Value::Null,
                }
            }

            "TRUNC" | "TRUNCATE" => {
                let n = resolved_args.first().and_then(|v| v.as_f64());
                let decimals = resolved_args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                match n {
                    Some(n) => {
                        let factor = 10_f64.powi(decimals as i32);
                        serde_json::json!((n * factor).trunc() / factor)
                    }
                    None => serde_json::Value::Null,
                }
            }

            "POWER" | "POW" => {
                let base = resolved_args.first().and_then(|v| v.as_f64());
                let exp = resolved_args.get(1).and_then(|v| v.as_f64());
                match (base, exp) {
                    (Some(b), Some(e)) => serde_json::json!(b.powf(e)),
                    _ => serde_json::Value::Null,
                }
            }

            "SQRT" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| serde_json::json!(n.sqrt()))
                .unwrap_or(serde_json::Value::Null),

            "SIGN" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| {
                    if n > 0.0 {
                        serde_json::json!(1)
                    } else if n < 0.0 {
                        serde_json::json!(-1)
                    } else {
                        serde_json::json!(0)
                    }
                })
                .unwrap_or(serde_json::Value::Null),

            "PI" => serde_json::json!(std::f64::consts::PI),
            "E" => serde_json::json!(std::f64::consts::E),
            "DEGREES" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| serde_json::json!(n.to_degrees()))
                .unwrap_or(serde_json::Value::Null),
            "RADIANS" => resolved_args
                .first()
                .and_then(|v| v.as_f64())
                .map(|n| serde_json::json!(n.to_radians()))
                .unwrap_or(serde_json::Value::Null),

            "RANDOM" => serde_json::json!(rand_simple()),

            // ── Type functions ──
            "TOSTRING" | "TO_STRING" => match resolved_args.first() {
                Some(serde_json::Value::String(s)) => serde_json::Value::String(s.clone()),
                Some(v) => serde_json::Value::String(v.to_string()),
                None => serde_json::Value::Null,
            },

            "TONUMBER" | "TO_NUMBER" | "TONUM" => resolved_args
                .first()
                .and_then(|v| match v {
                    serde_json::Value::Number(n) => Some(serde_json::Value::Number(n.clone())),
                    serde_json::Value::String(s) => s
                        .parse::<f64>()
                        .ok()
                        .and_then(|f| serde_json::Number::from_f64(f))
                        .map(serde_json::Value::Number),
                    _ => None,
                })
                .unwrap_or(serde_json::Value::Null),

            "TOBOOLEAN" | "TO_BOOLEAN" | "TOBOOL" => resolved_args
                .first()
                .map(|v| match v {
                    serde_json::Value::Bool(b) => serde_json::Value::Bool(*b),
                    serde_json::Value::Number(n) => {
                        serde_json::Value::Bool(n.as_f64().unwrap_or(0.0) != 0.0)
                    }
                    serde_json::Value::String(s) => {
                        serde_json::Value::Bool(!s.is_empty() && s != "false" && s != "0")
                    }
                    serde_json::Value::Null => serde_json::Value::Bool(false),
                    _ => serde_json::Value::Bool(true),
                })
                .unwrap_or(serde_json::Value::Null),

            "TOARRAY" | "TO_ARRAY" => resolved_args
                .first()
                .map(|v| match v {
                    serde_json::Value::Array(_) => v.clone(),
                    _ => serde_json::Value::Array(vec![v.clone()]),
                })
                .unwrap_or(serde_json::Value::Null),

            "TYPE" | "TYPENAME" | "TYPE_NAME" => resolved_args
                .first()
                .map(|v| match v {
                    serde_json::Value::Null => serde_json::Value::String("null".to_string()),
                    serde_json::Value::Bool(_) => {
                        serde_json::Value::String("boolean".to_string())
                    }
                    serde_json::Value::Number(_) => {
                        serde_json::Value::String("number".to_string())
                    }
                    serde_json::Value::String(_) => {
                        serde_json::Value::String("string".to_string())
                    }
                    serde_json::Value::Array(_) => {
                        serde_json::Value::String("array".to_string())
                    }
                    serde_json::Value::Object(_) => {
                        serde_json::Value::String("object".to_string())
                    }
                })
                .unwrap_or(serde_json::Value::Null),

            "ISNULL" | "IS_NULL" => resolved_args
                .first()
                .map(|v| serde_json::Value::Bool(v.is_null()))
                .unwrap_or(serde_json::Value::Null),

            "ISSTRING" | "IS_STRING" | "ISSTR" => resolved_args
                .first()
                .map(|v| serde_json::Value::Bool(v.is_string()))
                .unwrap_or(serde_json::Value::Null),

            "ISNUMBER" | "IS_NUMBER" | "ISNUM" => resolved_args
                .first()
                .map(|v| serde_json::Value::Bool(v.is_number()))
                .unwrap_or(serde_json::Value::Null),

            "ISBOOLEAN" | "IS_BOOLEAN" | "ISBOOL" => resolved_args
                .first()
                .map(|v| serde_json::Value::Bool(v.is_boolean()))
                .unwrap_or(serde_json::Value::Null),

            "ISARRAY" | "IS_ARRAY" => resolved_args
                .first()
                .map(|v| serde_json::Value::Bool(v.is_array()))
                .unwrap_or(serde_json::Value::Null),

            "ISOBJECT" | "IS_OBJECT" | "ISOBJ" => resolved_args
                .first()
                .map(|v| serde_json::Value::Bool(v.is_object()))
                .unwrap_or(serde_json::Value::Null),

            // ── Array functions ──
            "ARRAY_LENGTH" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| serde_json::json!(a.len()))
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_APPEND" => {
                let arr = resolved_args.first().and_then(|v| v.as_array()).cloned();
                let val = resolved_args.get(1).cloned();
                match (arr, val) {
                    (Some(mut a), Some(v)) => {
                        a.push(v);
                        serde_json::Value::Array(a)
                    }
                    _ => serde_json::Value::Null,
                }
            }

            "ARRAY_PREPEND" => {
                let val = resolved_args.first().cloned();
                let arr = resolved_args.get(1).and_then(|v| v.as_array()).cloned();
                match (val, arr) {
                    (Some(v), Some(mut a)) => {
                        a.insert(0, v);
                        serde_json::Value::Array(a)
                    }
                    _ => serde_json::Value::Null,
                }
            }

            "ARRAY_CONCAT" => {
                let mut result = Vec::new();
                for arg in &resolved_args {
                    if let Some(arr) = arg.as_array() {
                        result.extend(arr.clone());
                    }
                }
                serde_json::Value::Array(result)
            }

            "ARRAY_CONTAINS" => {
                let arr = resolved_args.first().and_then(|v| v.as_array());
                let val = resolved_args.get(1);
                match (arr, val) {
                    (Some(a), Some(v)) => serde_json::Value::Bool(a.contains(v)),
                    _ => serde_json::Value::Null,
                }
            }

            "ARRAY_DISTINCT" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| {
                    let mut seen = std::collections::HashSet::new();
                    let mut result = Vec::new();
                    for item in a {
                        let key = item.to_string();
                        if seen.insert(key) {
                            result.push(item.clone());
                        }
                    }
                    serde_json::Value::Array(result)
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_FLATTEN" => {
                let arr = resolved_args.first().and_then(|v| v.as_array());
                let depth = resolved_args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
                match arr {
                    Some(a) => serde_json::Value::Array(flatten_array(a, depth as usize)),
                    None => serde_json::Value::Null,
                }
            }

            "ARRAY_REVERSE" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| {
                    let mut r = a.clone();
                    r.reverse();
                    serde_json::Value::Array(r)
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_SORT" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| {
                    let mut r = a.clone();
                    r.sort_by(|a, b| compare_json_values(Some(a), Some(b)));
                    serde_json::Value::Array(r)
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_MIN" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .and_then(|a| {
                    a.iter()
                        .filter(|v| !v.is_null())
                        .min_by(|a, b| compare_json_values(Some(a), Some(b)))
                        .cloned()
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_MAX" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .and_then(|a| {
                    a.iter()
                        .filter(|v| !v.is_null())
                        .max_by(|a, b| compare_json_values(Some(a), Some(b)))
                        .cloned()
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_SUM" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| {
                    let sum: f64 = a.iter().filter_map(|v| v.as_f64()).sum();
                    serde_json::json!(sum)
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_AVG" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| {
                    let vals: Vec<f64> = a.iter().filter_map(|v| v.as_f64()).collect();
                    if vals.is_empty() {
                        serde_json::Value::Null
                    } else {
                        let sum: f64 = vals.iter().sum();
                        serde_json::json!(sum / vals.len() as f64)
                    }
                })
                .unwrap_or(serde_json::Value::Null),

            "ARRAY_COUNT" => resolved_args
                .first()
                .and_then(|v| v.as_array())
                .map(|a| {
                    let count = a.iter().filter(|v| !v.is_null()).count();
                    serde_json::json!(count)
                })
                .unwrap_or(serde_json::Value::Null),

            // ── Date/time functions ──
            "NOW_STR" => {
                let fmt = resolved_args
                    .first()
                    .and_then(|v| v.as_str())
                    .unwrap_or("%Y-%m-%dT%H:%M:%S");
                serde_json::Value::String(chrono::Utc::now().format(fmt).to_string())
            }

            "NOW_MILLIS" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            "NOW_UTC" => {
                serde_json::Value::String(chrono::Utc::now().to_rfc3339())
            }

            "DATE_DIFF_STR" => {
                // DATE_DIFF_STR(date1, date2, part) → difference in part units
                let d1 = resolved_args.first().and_then(|v| v.as_str());
                let d2 = resolved_args.get(1).and_then(|v| v.as_str());
                let part = resolved_args
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("day");
                match (d1, d2) {
                    (Some(d1), Some(d2)) => {
                        if let (Ok(dt1), Ok(dt2)) = (
                            chrono::DateTime::parse_from_rfc3339(d1),
                            chrono::DateTime::parse_from_rfc3339(d2),
                        ) {
                            let diff = dt1.signed_duration_since(dt2);
                            let val = match part.to_lowercase().as_str() {
                                "millisecond" => diff.num_milliseconds(),
                                "second" => diff.num_seconds(),
                                "minute" => diff.num_minutes(),
                                "hour" => diff.num_hours(),
                                "day" => diff.num_days(),
                                _ => diff.num_seconds(),
                            };
                            serde_json::json!(val)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => serde_json::Value::Null,
                }
            }

            // ── Object functions ──
            "OBJECT_LENGTH" => resolved_args
                .first()
                .and_then(|v| v.as_object())
                .map(|o| serde_json::json!(o.len()))
                .unwrap_or(serde_json::Value::Null),

            "OBJECT_KEYS" | "OBJECT_NAMES" => resolved_args
                .first()
                .and_then(|v| v.as_object())
                .map(|o| {
                    let keys: Vec<serde_json::Value> = o
                        .keys()
                        .map(|k| serde_json::Value::String(k.clone()))
                        .collect();
                    serde_json::Value::Array(keys)
                })
                .unwrap_or(serde_json::Value::Null),

            "OBJECT_VALUES" => resolved_args
                .first()
                .and_then(|v| v.as_object())
                .map(|o| serde_json::Value::Array(o.values().cloned().collect()))
                .unwrap_or(serde_json::Value::Null),

            "OBJECT_ADD" | "OBJECT_PUT" => {
                let obj = resolved_args.first().and_then(|v| v.as_object()).cloned();
                let key = resolved_args.get(1).and_then(|v| v.as_str());
                let val = resolved_args.get(2).cloned();
                match (obj, key, val) {
                    (Some(mut o), Some(k), Some(v)) => {
                        o.insert(k.to_string(), v);
                        serde_json::Value::Object(o)
                    }
                    _ => serde_json::Value::Null,
                }
            }

            "OBJECT_REMOVE" => {
                let obj = resolved_args.first().and_then(|v| v.as_object()).cloned();
                let key = resolved_args.get(1).and_then(|v| v.as_str());
                match (obj, key) {
                    (Some(mut o), Some(k)) => {
                        o.remove(k);
                        serde_json::Value::Object(o)
                    }
                    _ => serde_json::Value::Null,
                }
            }

            // ── Conditional functions ──
            "IFNULL" => {
                for arg in &resolved_args {
                    if !arg.is_null() {
                        return arg.clone();
                    }
                }
                serde_json::Value::Null
            }

            "IFMISSING" => resolved_args
                .into_iter()
                .find(|v| !v.is_null())
                .unwrap_or(serde_json::Value::Null),

            "IFMISSINGORNULL" | "COALESCE" | "NVL" => resolved_args
                .into_iter()
                .find(|v| !v.is_null())
                .unwrap_or(serde_json::Value::Null),

            "NULLIF" => {
                let a = resolved_args.first();
                let b = resolved_args.get(1);
                match (a, b) {
                    (Some(a), Some(b)) if a == b => serde_json::Value::Null,
                    (Some(a), _) => a.clone(),
                    _ => serde_json::Value::Null,
                }
            }

            "LEAST" => resolved_args
                .iter()
                .filter(|v| !v.is_null())
                .min_by(|a, b| compare_json_values(Some(a), Some(b)))
                .cloned()
                .unwrap_or(serde_json::Value::Null),

            "GREATEST" => resolved_args
                .iter()
                .filter(|v| !v.is_null())
                .max_by(|a, b| compare_json_values(Some(a), Some(b)))
                .cloned()
                .unwrap_or(serde_json::Value::Null),

            // ── Misc ──
            "UUID" | "UUID()" => {
                // Simple UUID v4-like
                serde_json::Value::String(generate_uuid())
            }

            "MILLIS_TO_STR" => {
                let millis = resolved_args.first().and_then(|v| v.as_i64());
                match millis {
                    Some(ms) => {
                        let dt = chrono::DateTime::from_timestamp_millis(ms);
                        match dt {
                            Some(dt) => serde_json::Value::String(dt.to_rfc3339()),
                            None => serde_json::Value::Null,
                        }
                    }
                    None => serde_json::Value::Null,
                }
            }

            "STR_TO_MILLIS" => {
                let s = resolved_args.first().and_then(|v| v.as_str());
                match s {
                    Some(s) => match chrono::DateTime::parse_from_rfc3339(s) {
                        Ok(dt) => serde_json::json!(dt.timestamp_millis()),
                        Err(_) => serde_json::Value::Null,
                    },
                    None => serde_json::Value::Null,
                }
            }

            "META" => {
                // META() returns an object with id
                serde_json::json!({"id": doc_key})
            }

            _ => serde_json::Value::Null,
        }
    }

    fn resolve_expr(
        &self,
        expr: &Expr,
        doc_value: &serde_json::Value,
        doc_key: &str,
    ) -> serde_json::Value {
        match expr {
            Expr::Literal(v) => v.clone(),
            Expr::Field(f) => extract_field_value(doc_value, f),
            Expr::MetaId => serde_json::Value::String(doc_key.to_string()),
            Expr::Function { name, args } => self.eval_function(name, args, doc_value, doc_key),
            Expr::Aggregate { .. } => serde_json::Value::Null,
            Expr::Subquery(sub) => {
                match self.execute_select(sub) {
                    Ok(result) => result.results.first()
                        .and_then(|r| if let Some(obj) = r.as_object() { obj.values().next().cloned() } else { Some(r.clone()) })
                        .unwrap_or(serde_json::Value::Null),
                    Err(_) => serde_json::Value::Null,
                }
            }
        }
    }

    // =================================================================
    // INSERT execution
    // =================================================================

    fn execute_insert(&self, ins: &InsertStmt) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let bucket = self.storage.get_bucket(&ins.bucket)?;

        // Evaluate key
        let key = match &ins.key_expr {
            Expr::Literal(serde_json::Value::String(s)) => s.clone(),
            _ => {
                return Err(NosqlError::QueryError(
                    "INSERT key must be a string literal".to_string(),
                ))
            }
        };

        // Evaluate value
        let value = match &ins.value_expr {
            Expr::Literal(v) => v.clone(),
            _ => {
                return Err(NosqlError::QueryError(
                    "INSERT value must be a JSON literal".to_string(),
                ))
            }
        };

        let doc = bucket.upsert(&ins.scope, &ins.collection, key.clone(), value, None)?;

        // Update indexes
        self.index_manager.on_document_upsert(&ins.bucket, &doc);

        let elapsed = start.elapsed().as_millis() as u64;

        let result = if ins.returning.is_empty() {
            vec![serde_json::json!({"status": "inserted", "key": key, "cas": doc.cas})]
        } else {
            vec![self.project_row(&doc, &ins.returning)]
        };

        Ok(QueryResult {
            status: "success".to_string(),
            results: result,
            metrics: QueryMetrics {
                result_count: 1,
                elapsed_ms: elapsed,
                scanned_count: 0,
                index_used: None,
            },
        })
    }

    // =================================================================
    // UPDATE execution
    // =================================================================

    fn execute_update(&self, upd: &UpdateStmt) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let bucket = self.storage.get_bucket(&upd.bucket)?;

        // Get all matching documents
        let all_docs = bucket.scan_all_documents();
        let matching: Vec<&Document> = all_docs
            .iter()
            .filter(|doc| self.matches_conditions(doc, &upd.conditions))
            .collect();

        let limit = upd.limit.unwrap_or(matching.len());
        let to_update: Vec<&Document> = matching.into_iter().take(limit).collect();

        let mut updated_count = 0;
        let mut returning_results = Vec::new();

        for doc in &to_update {
            let mut new_value = doc.value.clone();

            // Apply SET clauses
            for (field, expr) in &upd.set_clauses {
                let val = self.resolve_expr(expr, &doc.value, &doc.key);
                set_nested_field(&mut new_value, field, val);
            }

            // Apply UNSET clauses
            for field in &upd.unset_clauses {
                remove_nested_field(&mut new_value, field);
            }

            let updated_doc = bucket.upsert(
                &upd.scope,
                &upd.collection,
                doc.key.clone(),
                new_value,
                None,
            )?;

            self.index_manager
                .on_document_upsert(&upd.bucket, &updated_doc);
            updated_count += 1;

            if !upd.returning.is_empty() {
                returning_results.push(self.project_row(&updated_doc, &upd.returning));
            }
        }

        let elapsed = start.elapsed().as_millis() as u64;

        let results = if upd.returning.is_empty() {
            vec![serde_json::json!({"status": "updated", "mutationCount": updated_count})]
        } else {
            returning_results
        };

        Ok(QueryResult {
            status: "success".to_string(),
            results,
            metrics: QueryMetrics {
                result_count: updated_count,
                elapsed_ms: elapsed,
                scanned_count: all_docs.len(),
                index_used: None,
            },
        })
    }

    // =================================================================
    // DELETE execution
    // =================================================================

    fn execute_delete(&self, del: &DeleteStmt) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let bucket = self.storage.get_bucket(&del.bucket)?;

        let all_docs = bucket.scan_all_documents();
        let matching: Vec<&Document> = all_docs
            .iter()
            .filter(|doc| self.matches_conditions(doc, &del.conditions))
            .collect();

        let limit = del.limit.unwrap_or(matching.len());
        let to_delete: Vec<&Document> = matching.into_iter().take(limit).collect();

        let mut deleted_count = 0;
        let mut returning_results = Vec::new();

        for doc in &to_delete {
            if !del.returning.is_empty() {
                returning_results.push(self.project_row(doc, &del.returning));
            }

            bucket.delete(&del.scope, &del.collection, &doc.key, None)?;
            self.index_manager.on_document_delete(&del.bucket, &doc.key);
            deleted_count += 1;
        }

        let elapsed = start.elapsed().as_millis() as u64;

        let results = if del.returning.is_empty() {
            vec![serde_json::json!({"status": "deleted", "mutationCount": deleted_count})]
        } else {
            returning_results
        };

        Ok(QueryResult {
            status: "success".to_string(),
            results,
            metrics: QueryMetrics {
                result_count: deleted_count,
                elapsed_ms: elapsed,
                scanned_count: all_docs.len(),
                index_used: None,
            },
        })
    }

    // =================================================================
    // CREATE INDEX
    // =================================================================

    fn execute_create_index(&self, ci: &CreateIndexStmt) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let bucket_arc = self.storage.get_bucket(&ci.bucket)?;

        let _def = self
            .index_manager
            .create_index_ex(
                ci.name.clone(),
                ci.bucket.clone(),
                ci.fields.clone(),
                ci.condition.clone(),
                ci.array_exprs.clone(),
                ci.include_fields.clone(),
            )
            .map_err(|e| NosqlError::QueryError(e))?;

        let all_docs = bucket_arc.scan_all_documents();
        let count = self
            .index_manager
            .build_index(&ci.bucket, &ci.name, &all_docs)
            .map_err(|e| NosqlError::QueryError(e))?;

        // Persist index definitions to disk
        self.persist_indexes();

        let elapsed = start.elapsed().as_millis() as u64;

        let mut info = serde_json::json!({
            "status": "created",
            "index": ci.name,
            "bucket": ci.bucket,
            "fields": ci.fields,
            "entries_indexed": count,
        });
        if !ci.array_exprs.is_empty() {
            info["array_index"] = serde_json::json!(true);
        }
        if !ci.include_fields.is_empty() {
            info["covering"] = serde_json::json!(true);
            info["include_fields"] = serde_json::json!(ci.include_fields);
        }

        Ok(QueryResult {
            status: "success".to_string(),
            results: vec![info],
            metrics: QueryMetrics {
                result_count: 1,
                elapsed_ms: elapsed,
                scanned_count: count,
                index_used: None,
            },
        })
    }

    // =================================================================
    // DROP INDEX
    // =================================================================

    fn execute_drop_index(&self, di: &DropIndexStmt) -> Result<QueryResult> {
        let start = std::time::Instant::now();

        let def = self
            .index_manager
            .drop_index(&di.bucket, &di.index_name)
            .map_err(|e| NosqlError::QueryError(e))?;

        // Persist index definitions to disk
        self.persist_indexes();

        let elapsed = start.elapsed().as_millis() as u64;

        Ok(QueryResult {
            status: "success".to_string(),
            results: vec![serde_json::json!({
                "status": "dropped",
                "index": def.name,
                "bucket": def.bucket,
            })],
            metrics: QueryMetrics {
                result_count: 1,
                elapsed_ms: elapsed,
                scanned_count: 0,
                index_used: None,
            },
        })
    }

    // =================================================================
    // Row projection
    // =================================================================

    fn project_row(&self, doc: &Document, select_exprs: &[SelectExpr]) -> serde_json::Value {
        // Check if it's SELECT *
        let is_star = select_exprs
            .iter()
            .any(|e| matches!(e, SelectExpr::Star));

        if is_star {
            return serde_json::json!({
                "_key": doc.key,
                "_cas": doc.cas,
                "_rev": doc.rev_id,
                "_expiry": doc.expiry,
                "_updated_at": doc.updated_at,
                "doc": doc.value,
            });
        }

        let mut result = serde_json::Map::new();

        for sel in select_exprs {
            if let SelectExpr::Expr { expr, alias } = sel {
                let val = self.eval_expr_on_doc(expr, doc);
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                result.insert(name, val);
            }
        }

        serde_json::Value::Object(result)
    }

    // =================================================================
    // Condition helpers
    // =================================================================

    fn conditions_to_lookup(
        &self,
        conditions: &[Condition],
    ) -> Vec<(String, IndexLookupOp, serde_json::Value)> {
        conditions
            .iter()
            .filter_map(|c| {
                let op = match c.operator {
                    CompareOp::Eq => Some(IndexLookupOp::Eq),
                    CompareOp::Gt => Some(IndexLookupOp::Gt),
                    CompareOp::Gte => Some(IndexLookupOp::Gte),
                    CompareOp::Lt => Some(IndexLookupOp::Lt),
                    CompareOp::Lte => Some(IndexLookupOp::Lte),
                    _ => None,
                };
                op.map(|o| (c.field.clone(), o, c.value.clone()))
            })
            .collect()
    }

    fn matches_conditions(&self, doc: &Document, conditions: &[Condition]) -> bool {
        conditions.iter().all(|cond| self.matches_condition(doc, cond))
    }

    fn matches_condition(&self, doc: &Document, condition: &Condition) -> bool {
        // Handle subquery-based operators first
        match &condition.operator {
            CompareOp::InSubquery(subquery) => {
                let field_value = self.resolve_doc_field(doc, &condition.field);
                if let Some(val) = field_value {
                    let sub_values = self.execute_subquery_values(subquery);
                    sub_values.contains(&val)
                } else {
                    false
                }
            }
            CompareOp::NotInSubquery(subquery) => {
                let field_value = self.resolve_doc_field(doc, &condition.field);
                if let Some(val) = field_value {
                    let sub_values = self.execute_subquery_values(subquery);
                    !sub_values.contains(&val)
                } else {
                    true
                }
            }
            CompareOp::Exists(subquery) => {
                // EXISTS: true if subquery returns at least one row
                if let Ok(result) = self.execute_select(subquery) {
                    !result.results.is_empty()
                } else {
                    false
                }
            }
            CompareOp::NotExists(subquery) => {
                if let Ok(result) = self.execute_select(subquery) {
                    result.results.is_empty()
                } else {
                    true
                }
            }
            _ => {
                let field_value = self.resolve_doc_field(doc, &condition.field);
                match_condition_value(field_value.as_ref(), &condition.operator, &condition.value)
            }
        }
    }

    /// Resolve a document field value by name
    fn resolve_doc_field(&self, doc: &Document, field: &str) -> Option<serde_json::Value> {
        if field == "META().id" {
            Some(serde_json::Value::String(doc.key.clone()))
        } else if field.contains('.') {
            let val = crate::storage::index::extract_field(&doc.value, field);
            if val.is_null() { None } else { Some(val) }
        } else {
            doc.value.get(field).cloned()
        }
    }

    /// Execute a subquery and return the first column values as a Vec
    fn execute_subquery_values(&self, subquery: &SelectQuery) -> Vec<serde_json::Value> {
        match self.execute_select(subquery) {
            Ok(result) => {
                result.results.iter().filter_map(|row| {
                    // Return the first (or only) value from each row
                    if let Some(obj) = row.as_object() {
                        obj.values().next().cloned()
                    } else {
                        Some(row.clone())
                    }
                }).collect()
            }
            Err(_) => Vec::new(),
        }
    }

    // =================================================================
    // Parsing: SELECT
    // =================================================================

    fn parse_select(&self, statement: &str) -> Result<SelectQuery> {
        let statement = statement.trim();
        let upper = statement.to_uppercase();

        // Check DISTINCT
        let (select_start, distinct) = if upper.starts_with("SELECT DISTINCT ") {
            (16, true)
        } else if upper.starts_with("SELECT") {
            (6, false)
        } else {
            return Err(NosqlError::QueryError(
                "Query must start with SELECT".to_string(),
            ));
        };

        let from_pos = upper
            .find(" FROM ")
            .ok_or_else(|| NosqlError::QueryError("Missing FROM clause".to_string()))?;

        let select_part = statement[select_start..from_pos].trim();
        let select_exprs = self.parse_select_exprs(select_part)?;

        let after_from = &statement[from_pos + 6..];

        // ── Extract the FROM source + any JOINs before WHERE/ORDER BY/etc. ──
        let (from_and_joins_part, rest) = self.split_at_keyword(after_from);

        // ── Parse the from source, aliases, JOINs ──
        let (bucket_str, bucket_alias, joins, use_index) =
            self.parse_from_clause(from_and_joins_part.trim())?;

        let parts: Vec<&str> = bucket_str.split('.').collect();
        let bucket = parts
            .first()
            .ok_or_else(|| NosqlError::QueryError("Missing bucket name".to_string()))?
            .trim()
            .trim_matches('`')
            .to_string();
        let scope = parts
            .get(1)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());
        let collection = parts
            .get(2)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());

        let mut conditions = Vec::new();
        let mut remaining = rest.to_string();

        // WHERE
        let upper_rest = remaining.to_uppercase();
        if let Some(where_pos) = upper_rest.find("WHERE ") {
            let after_where = &remaining[where_pos + 6..];
            let (where_part, after_where_rest) = self.split_at_keyword(after_where);
            conditions = self.parse_conditions(where_part.trim())?;
            remaining = after_where_rest.to_string();
        }

        // GROUP BY
        let mut group_by = Vec::new();
        let upper_remaining = remaining.to_uppercase();
        if let Some(gb_pos) = upper_remaining.find("GROUP BY ") {
            let after_gb = &remaining[gb_pos + 9..];
            let (gb_part, after_gb_rest) = self.split_at_keyword(after_gb);
            group_by = gb_part
                .split(',')
                .map(|s| s.trim().trim_matches('`').to_string())
                .filter(|s| !s.is_empty())
                .collect();
            remaining = after_gb_rest.to_string();
        }

        // HAVING
        let mut having = Vec::new();
        let upper_remaining = remaining.to_uppercase();
        if let Some(having_pos) = upper_remaining.find("HAVING ") {
            let after_having = &remaining[having_pos + 7..];
            let (having_part, after_having_rest) = self.split_at_keyword(after_having);
            having = self.parse_conditions(having_part.trim())?;
            remaining = after_having_rest.to_string();
        }

        // ORDER BY
        let mut order_by = Vec::new();
        let upper_remaining = remaining.to_uppercase();
        if let Some(order_pos) = upper_remaining.find("ORDER BY ") {
            let after_order = &remaining[order_pos + 9..];
            let (order_part, after_order_rest) = self.split_at_keyword(after_order);
            order_by = self.parse_order_by(order_part.trim());
            remaining = after_order_rest.to_string();
        }

        // LIMIT
        let mut limit = None;
        let upper_remaining = remaining.to_uppercase();
        if let Some(limit_pos) = upper_remaining.find("LIMIT ") {
            let after_limit = &remaining[limit_pos + 6..];
            let (limit_part, after_limit_rest) = self.split_at_keyword(after_limit);
            if let Ok(n) = limit_part.trim().parse::<usize>() {
                limit = Some(n);
            }
            remaining = after_limit_rest.to_string();
        }

        // OFFSET
        let mut offset = None;
        let upper_remaining = remaining.to_uppercase();
        if let Some(offset_pos) = upper_remaining.find("OFFSET ") {
            let after_offset = &remaining[offset_pos + 7..];
            let (offset_part, _) = self.split_at_keyword(after_offset);
            if let Ok(n) = offset_part.trim().parse::<usize>() {
                offset = Some(n);
            }
        }

        Ok(SelectQuery {
            select_exprs,
            distinct,
            bucket,
            bucket_alias,
            scope,
            collection,
            joins,
            conditions,
            group_by,
            having,
            order_by,
            limit,
            offset,
            use_index,
        })
    }

    /// Parse FROM clause: "bucket [AS alias] [JOIN ... ON ...]*"
    /// Returns: (bucket_str, alias, joins, use_index)
    fn parse_from_clause(
        &self,
        from_text: &str,
    ) -> Result<(String, Option<String>, Vec<JoinClause>, Option<String>)> {
        let upper = from_text.to_uppercase();

        // Find first JOIN/NEST/UNNEST keyword position
        let join_keywords = [
            " INNER JOIN ",
            " LEFT OUTER JOIN ",
            " LEFT JOIN ",
            " JOIN ",
            " LEFT OUTER NEST ",
            " LEFT NEST ",
            " NEST ",
            " LEFT OUTER UNNEST ",
            " LEFT UNNEST ",
            " UNNEST ",
        ];

        let mut earliest_join_pos: Option<(usize, &str)> = None;
        for kw in &join_keywords {
            if let Some(pos) = upper.find(kw) {
                if earliest_join_pos.is_none() || pos < earliest_join_pos.unwrap().0 {
                    earliest_join_pos = Some((pos, kw));
                }
            }
        }

        let (source_part, join_part) = if let Some((pos, _)) = earliest_join_pos {
            (&from_text[..pos], &from_text[pos..])
        } else {
            (from_text, "")
        };

        // Parse the source: "bucket.scope.collection [AS alias] [USE INDEX (...)]"
        let source_trimmed = source_part.trim();
        let source_upper = source_trimmed.to_uppercase();

        // Check for USE INDEX
        let (source_no_idx, use_index) = if let Some(use_idx_pos) = source_upper.find(" USE INDEX") {
            let bucket_part = source_trimmed[..use_idx_pos].trim();
            let idx_part = source_trimmed[use_idx_pos + 10..].trim();
            let idx_name = idx_part
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim()
                .trim_matches('`')
                .to_string();
            (bucket_part, Some(idx_name))
        } else {
            (source_trimmed, None)
        };

        // Check for alias: "bucket AS alias" or "bucket alias" (if second token isn't a keyword)
        let (bucket_str, alias) = self.parse_source_with_alias(source_no_idx);

        // Parse JOIN clauses
        let joins = if !join_part.is_empty() {
            self.parse_join_clauses(join_part)?
        } else {
            Vec::new()
        };

        Ok((bucket_str, alias, joins, use_index))
    }

    /// Parse "bucket [AS alias]" or "bucket alias"
    fn parse_source_with_alias(&self, text: &str) -> (String, Option<String>) {
        let text = text.trim();

        if let Some(as_pos) = find_keyword_outside_parens(text, " AS ") {
            let bucket = text[..as_pos].trim().trim_matches('`').to_string();
            let alias = text[as_pos + 4..].trim().trim_matches('`').to_string();
            return (bucket, Some(alias));
        }

        // Check for implicit alias: "bucket b" where b is not a keyword
        let tokens: Vec<&str> = text.split_whitespace().collect();
        if tokens.len() == 2 {
            let second = tokens[1].to_uppercase();
            let reserved = [
                "WHERE", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING", "JOIN",
                "LEFT", "RIGHT", "INNER", "OUTER", "NEST", "UNNEST", "USE",
                "ON", "SET", "UNSET", "RETURNING",
            ];
            if !reserved.contains(&second.as_str()) {
                return (
                    tokens[0].trim_matches('`').to_string(),
                    Some(tokens[1].trim_matches('`').to_string()),
                );
            }
        }

        (text.trim_matches('`').to_string(), None)
    }

    /// Parse one or more JOIN/NEST/UNNEST clauses
    fn parse_join_clauses(&self, text: &str) -> Result<Vec<JoinClause>> {
        let mut joins = Vec::new();
        let mut remaining = text;

        while !remaining.trim().is_empty() {
            let rem_upper = remaining.to_uppercase();
            let rem_trimmed = remaining.trim();

            // Determine the join type and skip past the keyword
            let (join_type, after_kw) = if rem_upper.trim_start().starts_with("INNER JOIN ") {
                (JoinType::Inner, &rem_trimmed[11..])
            } else if rem_upper.trim_start().starts_with("LEFT OUTER JOIN ") {
                (JoinType::Left, &rem_trimmed[16..])
            } else if rem_upper.trim_start().starts_with("LEFT JOIN ") {
                (JoinType::Left, &rem_trimmed[10..])
            } else if rem_upper.trim_start().starts_with("JOIN ") {
                (JoinType::Inner, &rem_trimmed[5..])
            } else if rem_upper.trim_start().starts_with("LEFT OUTER NEST ") {
                (JoinType::LeftNest, &rem_trimmed[16..])
            } else if rem_upper.trim_start().starts_with("LEFT NEST ") {
                (JoinType::LeftNest, &rem_trimmed[10..])
            } else if rem_upper.trim_start().starts_with("NEST ") {
                (JoinType::Nest, &rem_trimmed[5..])
            } else if rem_upper.trim_start().starts_with("LEFT OUTER UNNEST ") {
                (JoinType::LeftUnnest, &rem_trimmed[18..])
            } else if rem_upper.trim_start().starts_with("LEFT UNNEST ") {
                (JoinType::LeftUnnest, &rem_trimmed[12..])
            } else if rem_upper.trim_start().starts_with("UNNEST ") {
                (JoinType::Unnest, &rem_trimmed[7..])
            } else {
                break; // no more join clauses
            };

            if join_type == JoinType::Unnest || join_type == JoinType::LeftUnnest {
                // UNNEST path [AS alias]
                // Find next JOIN keyword or end
                let (unnest_part, next) = self.split_at_join_keyword(after_kw);
                let (unnest_path, alias) = self.parse_source_with_alias(unnest_part.trim());
                joins.push(JoinClause {
                    join_type,
                    bucket: None,
                    scope: None,
                    collection: None,
                    alias,
                    unnest_path: Some(unnest_path),
                    on_left: None,
                    on_right: None,
                });
                remaining = next;
            } else {
                // JOIN/NEST bucket [AS alias] ON left_field = right_field
                let after_kw_upper = after_kw.to_uppercase();
                let on_pos = after_kw_upper.find(" ON ").ok_or_else(|| {
                    NosqlError::QueryError("JOIN requires ON clause".to_string())
                })?;

                let source_part = after_kw[..on_pos].trim();
                let after_on = &after_kw[on_pos + 4..];

                // Parse source with alias
                let (join_bucket_str, join_alias) = self.parse_source_with_alias(source_part);
                let jparts: Vec<&str> = join_bucket_str.split('.').collect();
                let jbucket = jparts[0].trim().trim_matches('`').to_string();
                let jscope = jparts.get(1).map(|s| s.trim().trim_matches('`').to_string());
                let jcoll = jparts.get(2).map(|s| s.trim().trim_matches('`').to_string());

                // Parse ON condition: find next JOIN keyword or end
                let (on_part, next) = self.split_at_join_keyword(after_on);
                let on_trimmed = on_part.trim();

                // Parse simple equi-join: left_field = right_field
                let (on_left, on_right) = if let Some(eq_pos) = on_trimmed.find('=') {
                    (
                        Some(on_trimmed[..eq_pos].trim().trim_matches('`').to_string()),
                        Some(on_trimmed[eq_pos + 1..].trim().trim_matches('`').to_string()),
                    )
                } else {
                    (None, None)
                };

                joins.push(JoinClause {
                    join_type,
                    bucket: Some(jbucket),
                    scope: jscope,
                    collection: jcoll,
                    alias: join_alias,
                    unnest_path: None,
                    on_left,
                    on_right,
                });
                remaining = next;
            }
        }

        Ok(joins)
    }

    /// Split text at the next JOIN/NEST/UNNEST keyword
    fn split_at_join_keyword<'a>(&self, text: &'a str) -> (&'a str, &'a str) {
        let upper = text.to_uppercase();
        let join_keywords = [
            " INNER JOIN ",
            " LEFT OUTER JOIN ",
            " LEFT JOIN ",
            " JOIN ",
            " LEFT OUTER NEST ",
            " LEFT NEST ",
            " NEST ",
            " LEFT OUTER UNNEST ",
            " LEFT UNNEST ",
            " UNNEST ",
        ];

        let mut earliest = text.len();
        for kw in &join_keywords {
            if let Some(pos) = upper.find(kw) {
                if pos < earliest {
                    earliest = pos;
                }
            }
        }

        (&text[..earliest], &text[earliest..])
    }

    fn parse_select_exprs(&self, select_part: &str) -> Result<Vec<SelectExpr>> {
        let trimmed = select_part.trim();
        if trimmed == "*" {
            return Ok(vec![SelectExpr::Star]);
        }

        let mut exprs = Vec::new();
        let tokens = split_top_level_commas(trimmed);

        for token in tokens {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }

            // Check for alias: expr AS alias
            let (expr_str, alias) = if let Some(as_pos) =
                find_keyword_outside_parens(token, " AS ")
            {
                (
                    token[..as_pos].trim(),
                    Some(token[as_pos + 4..].trim().trim_matches('`').to_string()),
                )
            } else {
                (token, None)
            };

            let expr = self.parse_expr(expr_str)?;
            exprs.push(SelectExpr::Expr { expr, alias });
        }

        Ok(exprs)
    }

    fn parse_expr(&self, s: &str) -> Result<Expr> {
        let s = s.trim();
        let upper = s.to_uppercase();

        // Subquery expression: (SELECT ...)
        if s.starts_with('(') && s.ends_with(')') {
            let inner = s[1..s.len() - 1].trim();
            if inner.to_uppercase().starts_with("SELECT") {
                let subquery = self.parse_select(inner)?;
                return Ok(Expr::Subquery(Box::new(subquery)));
            }
        }

        // COUNT(*), COUNT(field), SUM(field), AVG(field), MIN(field), MAX(field)
        for (kw, func) in &[
            ("COUNT(", AggFunc::Count),
            ("SUM(", AggFunc::Sum),
            ("AVG(", AggFunc::Avg),
            ("MIN(", AggFunc::Min),
            ("MAX(", AggFunc::Max),
        ] {
            if upper.starts_with(kw) && s.ends_with(')') {
                let inner = s[kw.len()..s.len() - 1].trim();
                let arg = if inner == "*" || inner == "1" {
                    Expr::Field("*".to_string())
                } else {
                    self.parse_expr(inner)?
                };
                return Ok(Expr::Aggregate {
                    func: *func,
                    arg: Box::new(arg),
                });
            }
        }

        // META().id
        if upper == "META().ID" || upper == "META()" {
            return Ok(Expr::MetaId);
        }

        // Function call: NAME(args...)
        if let Some(paren_pos) = s.find('(') {
            if s.ends_with(')') {
                let fn_name = s[..paren_pos].trim().to_uppercase();
                // Make sure it's not just a field with parens
                if fn_name
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_')
                    && !fn_name.is_empty()
                {
                    let args_str = s[paren_pos + 1..s.len() - 1].trim();
                    let args = if args_str.is_empty() {
                        Vec::new()
                    } else {
                        let arg_tokens = split_top_level_commas(args_str);
                        let mut args = Vec::new();
                        for at in arg_tokens {
                            args.push(self.parse_expr(at.trim())?);
                        }
                        args
                    };
                    return Ok(Expr::Function {
                        name: fn_name,
                        args,
                    });
                }
            }
        }

        // String literal
        if (s.starts_with('"') && s.ends_with('"'))
            || (s.starts_with('\'') && s.ends_with('\''))
        {
            return Ok(Expr::Literal(serde_json::Value::String(
                s[1..s.len() - 1].to_string(),
            )));
        }

        // Numeric literal
        if let Ok(n) = s.parse::<i64>() {
            return Ok(Expr::Literal(serde_json::json!(n)));
        }
        if let Ok(n) = s.parse::<f64>() {
            return Ok(Expr::Literal(serde_json::json!(n)));
        }

        // Boolean/null
        if upper == "TRUE" {
            return Ok(Expr::Literal(serde_json::Value::Bool(true)));
        }
        if upper == "FALSE" {
            return Ok(Expr::Literal(serde_json::Value::Bool(false)));
        }
        if upper == "NULL" {
            return Ok(Expr::Literal(serde_json::Value::Null));
        }

        // JSON object literal { ... }
        if s.starts_with('{') && s.ends_with('}') {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                return Ok(Expr::Literal(v));
            }
        }

        // JSON array literal [ ... ]
        if s.starts_with('[') && s.ends_with(']') {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                return Ok(Expr::Literal(v));
            }
        }

        // Field reference (can include dots for nested access)
        Ok(Expr::Field(s.trim_matches('`').to_string()))
    }

    fn parse_order_by(&self, order_part: &str) -> Vec<(Expr, bool)> {
        let mut result = Vec::new();
        let parts = split_top_level_commas(order_part);
        for part in parts {
            let tokens: Vec<&str> = part.trim().split_whitespace().collect();
            if tokens.is_empty() {
                continue;
            }
            let field = tokens[0].trim_matches('`').to_string();
            let ascending = tokens
                .get(1)
                .map(|d| d.to_uppercase() != "DESC")
                .unwrap_or(true);
            if let Ok(expr) = self.parse_expr(&field) {
                result.push((expr, ascending));
            }
        }
        result
    }

    // =================================================================
    // Parsing: INSERT
    // =================================================================

    fn parse_insert(&self, statement: &str) -> Result<InsertStmt> {
        // INSERT INTO bucket (KEY, VALUE) VALUES ("key", { ... })
        // INSERT INTO bucket.scope.collection (KEY, VALUE) VALUES ("key", { ... })
        let upper = statement.to_uppercase();

        let into_pos = upper.find("INTO ").ok_or_else(|| {
            NosqlError::QueryError("INSERT requires INTO clause".to_string())
        })?;
        let after_into = &statement[into_pos + 5..];

        // Find (KEY, VALUE)
        let paren_pos = after_into.find('(').ok_or_else(|| {
            NosqlError::QueryError("INSERT requires (KEY, VALUE) clause".to_string())
        })?;
        let bucket_str = after_into[..paren_pos].trim();
        let parts: Vec<&str> = bucket_str.split('.').collect();
        let bucket = parts[0].trim().trim_matches('`').to_string();
        let scope = parts
            .get(1)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());
        let collection = parts
            .get(2)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());

        // Find VALUES
        let values_pos = upper.find("VALUES").ok_or_else(|| {
            NosqlError::QueryError("INSERT requires VALUES clause".to_string())
        })?;
        let after_values = statement[values_pos + 6..].trim();

        // Parse VALUES (key, value)
        let after_values = after_values
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();

        // Split key and value — find first comma outside quotes and braces
        let (key_str, val_str) = split_first_value_comma(after_values)?;

        let key_expr = self.parse_expr(key_str.trim())?;
        let value_expr = self.parse_expr(val_str.trim())?;

        // Check for RETURNING
        let returning = if upper.contains("RETURNING") {
            let ret_pos = upper.find("RETURNING").unwrap();
            let ret_part = statement[ret_pos + 9..].trim();
            self.parse_select_exprs(ret_part)?
        } else {
            Vec::new()
        };

        Ok(InsertStmt {
            bucket,
            scope,
            collection,
            key_expr,
            value_expr,
            returning,
        })
    }

    // =================================================================
    // Parsing: UPDATE
    // =================================================================

    fn parse_update(&self, statement: &str) -> Result<UpdateStmt> {
        // UPDATE bucket SET field1 = val1, field2 = val2 [UNSET field3] WHERE ... [LIMIT n] [RETURNING ...]
        let upper = statement.to_uppercase();

        let set_pos = upper.find(" SET ").ok_or_else(|| {
            NosqlError::QueryError("UPDATE requires SET clause".to_string())
        })?;

        let bucket_str = statement[6..set_pos].trim(); // "UPDATE " = 7 chars, but 6 for "UPDATE"
        let parts: Vec<&str> = bucket_str.split('.').collect();
        let bucket = parts[0].trim().trim_matches('`').to_string();
        let scope = parts
            .get(1)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());
        let collection = parts
            .get(2)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());

        let after_set = &statement[set_pos + 5..];

        // Find WHERE, UNSET, LIMIT, RETURNING boundaries
        let (set_part, after_set_rest) = self.split_at_update_keyword(after_set);

        let set_clauses = self.parse_set_clauses(set_part.trim())?;

        let mut remaining = after_set_rest.to_string();

        // UNSET
        let mut unset_clauses = Vec::new();
        let upper_remaining = remaining.to_uppercase();
        if let Some(unset_pos) = upper_remaining.find("UNSET ") {
            let after_unset = &remaining[unset_pos + 6..];
            let (unset_part, after_unset_rest) = self.split_at_update_keyword(after_unset);
            unset_clauses = unset_part
                .split(',')
                .map(|s| s.trim().trim_matches('`').to_string())
                .filter(|s| !s.is_empty())
                .collect();
            remaining = after_unset_rest.to_string();
        }

        // WHERE
        let mut conditions = Vec::new();
        let upper_remaining = remaining.to_uppercase();
        if let Some(where_pos) = upper_remaining.find("WHERE ") {
            let after_where = &remaining[where_pos + 6..];
            let (where_part, after_where_rest) = self.split_at_update_keyword(after_where);
            conditions = self.parse_conditions(where_part.trim())?;
            remaining = after_where_rest.to_string();
        }

        // LIMIT
        let mut limit = None;
        let upper_remaining = remaining.to_uppercase();
        if let Some(limit_pos) = upper_remaining.find("LIMIT ") {
            let after_limit = &remaining[limit_pos + 6..];
            let (limit_part, after_limit_rest) = self.split_at_update_keyword(after_limit);
            if let Ok(n) = limit_part.trim().parse::<usize>() {
                limit = Some(n);
            }
            remaining = after_limit_rest.to_string();
        }

        // RETURNING
        let returning = if remaining.to_uppercase().contains("RETURNING") {
            let ret_pos = remaining.to_uppercase().find("RETURNING").unwrap();
            let ret_part = remaining[ret_pos + 9..].trim();
            self.parse_select_exprs(ret_part)?
        } else {
            Vec::new()
        };

        Ok(UpdateStmt {
            bucket,
            scope,
            collection,
            set_clauses,
            unset_clauses,
            conditions,
            limit,
            returning,
        })
    }

    fn parse_set_clauses(&self, s: &str) -> Result<Vec<(String, Expr)>> {
        let mut clauses = Vec::new();
        let parts = split_top_level_commas(s);
        for part in parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let eq_pos = part.find('=').ok_or_else(|| {
                NosqlError::QueryError(format!("Invalid SET clause: {}", part))
            })?;
            let field = part[..eq_pos].trim().trim_matches('`').to_string();
            let val_str = part[eq_pos + 1..].trim();
            let expr = self.parse_expr(val_str)?;
            clauses.push((field, expr));
        }
        Ok(clauses)
    }

    fn split_at_update_keyword<'a>(&self, text: &'a str) -> (&'a str, &'a str) {
        let upper = text.to_uppercase();
        let keywords = [
            "WHERE ", "UNSET ", "SET ", "LIMIT ", "OFFSET ", "RETURNING ",
        ];

        let mut earliest = text.len();
        for kw in &keywords {
            if let Some(pos) = upper.find(kw) {
                if pos < earliest {
                    earliest = pos;
                }
            }
        }

        (&text[..earliest], &text[earliest..])
    }

    // =================================================================
    // Parsing: DELETE
    // =================================================================

    fn parse_delete(&self, statement: &str) -> Result<DeleteStmt> {
        // DELETE FROM bucket WHERE ... [LIMIT n] [RETURNING ...]
        let upper = statement.to_uppercase();

        let from_pos = upper.find(" FROM ").ok_or_else(|| {
            NosqlError::QueryError("DELETE requires FROM clause".to_string())
        })?;
        let after_from = &statement[from_pos + 6..];

        let (from_part, rest) = self.split_at_keyword(after_from);

        let parts: Vec<&str> = from_part.trim().split('.').collect();
        let bucket = parts[0].trim().trim_matches('`').to_string();
        let scope = parts
            .get(1)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());
        let collection = parts
            .get(2)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or_else(|| "_default".to_string());

        let mut remaining = rest.to_string();

        // WHERE
        let mut conditions = Vec::new();
        let upper_remaining = remaining.to_uppercase();
        if let Some(where_pos) = upper_remaining.find("WHERE ") {
            let after_where = &remaining[where_pos + 6..];
            let (where_part, after_where_rest) = self.split_at_keyword(after_where);
            conditions = self.parse_conditions(where_part.trim())?;
            remaining = after_where_rest.to_string();
        }

        // LIMIT
        let mut limit = None;
        let upper_remaining = remaining.to_uppercase();
        if let Some(limit_pos) = upper_remaining.find("LIMIT ") {
            let after_limit = &remaining[limit_pos + 6..];
            let (limit_part, after_limit_rest) = self.split_at_keyword(after_limit);
            if let Ok(n) = limit_part.trim().parse::<usize>() {
                limit = Some(n);
            }
            remaining = after_limit_rest.to_string();
        }

        // RETURNING
        let returning = if remaining.to_uppercase().contains("RETURNING") {
            let ret_pos = remaining.to_uppercase().find("RETURNING").unwrap();
            let ret_part = remaining[ret_pos + 9..].trim();
            self.parse_select_exprs(ret_part)?
        } else {
            Vec::new()
        };

        Ok(DeleteStmt {
            bucket,
            scope,
            collection,
            conditions,
            limit,
            returning,
        })
    }

    // =================================================================
    // Parsing: CREATE INDEX / DROP INDEX
    // =================================================================

    fn parse_create_index(&self, statement: &str) -> Result<CreateIndexStmt> {
        let upper = statement.to_uppercase();
        let after_create = if upper.starts_with("CREATE INDEX ") {
            &statement[13..]
        } else {
            return Err(NosqlError::QueryError(
                "Expected: CREATE INDEX name ON bucket(fields...)".to_string(),
            ));
        };

        let on_pos = after_create
            .to_uppercase()
            .find(" ON ")
            .ok_or_else(|| {
                NosqlError::QueryError("Missing ON clause in CREATE INDEX".to_string())
            })?;

        let name = after_create[..on_pos]
            .trim()
            .trim_matches('`')
            .to_string();
        let after_on = &after_create[on_pos + 4..];

        let paren_open = after_on
            .find('(')
            .ok_or_else(|| NosqlError::QueryError("Missing ( in CREATE INDEX".to_string()))?;

        // Find matching closing paren (handle nested parens from ARRAY expressions)
        let mut depth = 0;
        let mut paren_close = None;
        for (i, c) in after_on.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        paren_close = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let paren_close = paren_close
            .ok_or_else(|| NosqlError::QueryError("Missing ) in CREATE INDEX".to_string()))?;

        let bucket = after_on[..paren_open]
            .trim()
            .trim_matches('`')
            .to_string();
        let fields_str = &after_on[paren_open + 1..paren_close];

        // Parse fields — may contain ARRAY expressions or plain fields
        let (fields, array_exprs) = Self::parse_index_fields(fields_str)?;

        if fields.is_empty() {
            return Err(NosqlError::QueryError(
                "CREATE INDEX must specify at least one field".to_string(),
            ));
        }

        let after_paren = &after_on[paren_close + 1..];
        let after_paren_upper = after_paren.to_uppercase();

        // Parse INCLUDE clause for covering indexes
        // Syntax: ... INCLUDE (field1, field2, ...) ...
        let mut include_fields = Vec::new();
        let mut remainder_str = after_paren.to_string();
        if let Some(include_pos) = after_paren_upper.find("INCLUDE") {
            let sub = &after_paren[include_pos + 7..];
            if let Some(open) = sub.find('(') {
                if let Some(close) = sub.find(')') {
                    let include_str = &sub[open + 1..close];
                    include_fields = include_str
                        .split(',')
                        .map(|s| s.trim().trim_matches('`').to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    // remainder is everything after the INCLUDE (...) clause
                    remainder_str = format!(
                        "{}{}",
                        &after_paren[..include_pos],
                        &sub[close + 1..]
                    );
                }
            }
        }
        let remainder = &remainder_str;

        // Parse WHERE condition
        let remainder_upper = remainder.to_uppercase();
        let condition = if remainder_upper.contains("WHERE") {
            let where_pos = remainder_upper.find("WHERE").unwrap();
            Some(remainder[where_pos + 5..].trim().to_string())
        } else {
            None
        };

        Ok(CreateIndexStmt {
            name,
            bucket,
            fields,
            condition,
            array_exprs,
            include_fields,
        })
    }

    /// Parse index field expressions, handling both plain fields and ARRAY expressions.
    /// Returns (fields, array_exprs) where array expressions are stored with their position.
    fn parse_index_fields(
        fields_str: &str,
    ) -> Result<(Vec<String>, Vec<(usize, crate::storage::index::ArrayIndexExpr)>)> {
        let mut fields = Vec::new();
        let mut array_exprs = Vec::new();

        // Split on commas, but respect ARRAY ... END blocks
        let mut current = String::new();
        let mut depth = 0;
        let mut in_array = false;

        for c in fields_str.chars() {
            match c {
                ',' if depth == 0 && !in_array => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        Self::process_index_field(&trimmed, &mut fields, &mut array_exprs)?;
                    }
                    current.clear();
                }
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth -= 1;
                    current.push(c);
                }
                _ => {
                    current.push(c);
                    // Check if we're entering/leaving an ARRAY block
                    let upper = current.to_uppercase();
                    if upper.contains("ALL ARRAY") || upper.contains("DISTINCT ARRAY") {
                        in_array = true;
                    }
                    if in_array && upper.trim_end().ends_with(" END") {
                        in_array = false;
                    }
                }
            }
        }

        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            Self::process_index_field(&trimmed, &mut fields, &mut array_exprs)?;
        }

        Ok((fields, array_exprs))
    }

    /// Process a single index field expression (plain field or ARRAY expression)
    fn process_index_field(
        field_str: &str,
        fields: &mut Vec<String>,
        array_exprs: &mut Vec<(usize, crate::storage::index::ArrayIndexExpr)>,
    ) -> Result<()> {
        let trimmed = field_str.trim();
        let upper = trimmed.to_uppercase();

        // Check for ARRAY expression: ALL ARRAY <expr> FOR <var> IN <path> END
        //                          or: DISTINCT ARRAY <expr> FOR <var> IN <path> END
        if (upper.starts_with("ALL ARRAY") || upper.starts_with("DISTINCT ARRAY"))
            && upper.ends_with("END")
        {
            let mode = if upper.starts_with("ALL") {
                crate::storage::index::ArrayIndexMode::All
            } else {
                crate::storage::index::ArrayIndexMode::Distinct
            };

            // Parse: ... ARRAY <expr> FOR <var> IN <path> END
            let array_start = if upper.starts_with("ALL ARRAY") { 9 } else { 14 };
            let inner = &trimmed[array_start..trimmed.len() - 3].trim(); // strip "END"

            // Find "FOR" keyword
            let inner_upper = inner.to_uppercase();
            let for_pos = inner_upper.find(" FOR ").ok_or_else(|| {
                NosqlError::QueryError("ARRAY expression missing FOR keyword".to_string())
            })?;
            let expr = inner[..for_pos].trim().to_string();

            let after_for = &inner[for_pos + 5..];
            let after_for_upper = after_for.to_uppercase();

            // Find "IN" keyword
            let in_pos = after_for_upper.find(" IN ").ok_or_else(|| {
                NosqlError::QueryError("ARRAY expression missing IN keyword".to_string())
            })?;
            let var = after_for[..in_pos].trim().to_string();
            let array_path = after_for[in_pos + 4..].trim().trim_matches('`').to_string();

            let pos = fields.len();
            // Use the array_path as the field name for the index key
            fields.push(format!("__array__{}", array_path));

            array_exprs.push((
                pos,
                crate::storage::index::ArrayIndexExpr {
                    var,
                    expr,
                    array_path,
                    mode,
                },
            ));
        } else {
            // Plain field
            fields.push(trimmed.trim_matches('`').to_string());
        }

        Ok(())
    }

    fn parse_drop_index(&self, statement: &str) -> Result<DropIndexStmt> {
        let after_drop = if statement.to_uppercase().starts_with("DROP INDEX ") {
            &statement[11..]
        } else {
            return Err(NosqlError::QueryError(
                "Expected: DROP INDEX bucket.index_name".to_string(),
            ));
        };

        let parts: Vec<&str> = after_drop.trim().split('.').collect();
        if parts.len() != 2 {
            return Err(NosqlError::QueryError(
                "Expected format: DROP INDEX bucket.index_name".to_string(),
            ));
        }

        let bucket = parts[0].trim().trim_matches('`').to_string();
        let index_name = parts[1].trim().trim_matches('`').to_string();

        Ok(DropIndexStmt { bucket, index_name })
    }

    // =================================================================
    // Parsing: Conditions
    // =================================================================

    fn parse_conditions(&self, where_clause: &str) -> Result<Vec<Condition>> {
        let mut conditions = Vec::new();

        // Split on AND (case-insensitive)
        let parts = split_and(where_clause);

        for part in parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let condition = self.parse_single_condition(part)?;
            conditions.push(condition);
        }

        Ok(conditions)
    }

    fn parse_single_condition(&self, part: &str) -> Result<Condition> {
        let part = part.trim();
        let upper = part.to_uppercase();

        // EXISTS (SELECT ...) / NOT EXISTS (SELECT ...)
        if upper.starts_with("NOT EXISTS") {
            let rest = part[10..].trim();
            let inner = rest.trim_start_matches('(').trim_end_matches(')').trim();
            let subquery = self.parse_select(inner)?;
            return Ok(Condition {
                field: String::new(),
                operator: CompareOp::NotExists(Box::new(subquery)),
                value: serde_json::Value::Null,
            });
        }
        if upper.starts_with("EXISTS") {
            let rest = part[6..].trim();
            let inner = rest.trim_start_matches('(').trim_end_matches(')').trim();
            let subquery = self.parse_select(inner)?;
            return Ok(Condition {
                field: String::new(),
                operator: CompareOp::Exists(Box::new(subquery)),
                value: serde_json::Value::Null,
            });
        }

        // BETWEEN
        if let Some(between_pos) = upper.find(" BETWEEN ") {
            let field = part[..between_pos].trim().to_string();
            let rest = part[between_pos + 9..].trim();
            if let Some(and_pos) = rest.to_uppercase().find(" AND ") {
                let low = self.parse_value(rest[..and_pos].trim())?;
                let high = self.parse_value(rest[and_pos + 5..].trim())?;
                // Encode as two conditions won't work easily; store as a value pair
                return Ok(Condition {
                    field,
                    operator: CompareOp::Between,
                    value: serde_json::json!([low, high]),
                });
            }
        }

        // NOT IN (SELECT ...) — subquery
        if let Some(notin_pos) = find_keyword_outside_parens(part, " NOT IN ") {
            let field = part[..notin_pos].trim().to_string();
            let rest = part[notin_pos + 8..].trim();
            let inner = rest.trim_start_matches('(').trim_end_matches(')').trim();
            let inner_upper = inner.to_uppercase();
            if inner_upper.starts_with("SELECT") {
                let subquery = self.parse_select(inner)?;
                return Ok(Condition {
                    field,
                    operator: CompareOp::NotInSubquery(Box::new(subquery)),
                    value: serde_json::Value::Null,
                });
            }
        }

        // IN (SELECT ...) — subquery  OR  IN (val1, val2, ...)
        if let Some(in_pos) = find_keyword_outside_parens(part, " IN ") {
            let field = part[..in_pos].trim().to_string();
            let rest = part[in_pos + 4..].trim();
            let inner = rest.trim_start_matches('(').trim_end_matches(')').trim();

            // Check if it's a subquery
            let inner_upper = inner.to_uppercase();
            if inner_upper.starts_with("SELECT") {
                let subquery = self.parse_select(inner)?;
                return Ok(Condition {
                    field,
                    operator: CompareOp::InSubquery(Box::new(subquery)),
                    value: serde_json::Value::Null,
                });
            }

            // Regular IN with literal values
            let list_str = inner;
            let items = split_top_level_commas(list_str);
            let values: Vec<serde_json::Value> = items
                .iter()
                .filter_map(|s| self.parse_value(s.trim()).ok())
                .collect();
            return Ok(Condition {
                field,
                operator: CompareOp::In,
                value: serde_json::Value::Array(values),
            });
        }

        // >= <= != > < LIKE IS NOT NULL / IS NULL / = (in order of specificity)
        if let Some(pos) = part.find(">=") {
            let field = part[..pos].trim().to_string();
            let value = self.parse_value(part[pos + 2..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Gte,
                value,
            });
        }
        if let Some(pos) = part.find("<=") {
            let field = part[..pos].trim().to_string();
            let value = self.parse_value(part[pos + 2..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Lte,
                value,
            });
        }
        if let Some(pos) = part.find("!=") {
            let field = part[..pos].trim().to_string();
            let value = self.parse_value(part[pos + 2..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Ne,
                value,
            });
        }
        if let Some(pos) = part.find('>') {
            let field = part[..pos].trim().to_string();
            let value = self.parse_value(part[pos + 1..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Gt,
                value,
            });
        }
        if let Some(pos) = part.find('<') {
            let field = part[..pos].trim().to_string();
            let value = self.parse_value(part[pos + 1..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Lt,
                value,
            });
        }
        if upper.contains(" IS NOT NULL") {
            let field = part[..upper.find(" IS NOT NULL").unwrap()]
                .trim()
                .to_string();
            return Ok(Condition {
                field,
                operator: CompareOp::IsNotNull,
                value: serde_json::Value::Null,
            });
        }
        if upper.contains(" IS NULL") {
            let field = part[..upper.find(" IS NULL").unwrap()]
                .trim()
                .to_string();
            return Ok(Condition {
                field,
                operator: CompareOp::IsNull,
                value: serde_json::Value::Null,
            });
        }
        if upper.contains(" LIKE ") {
            let like_pos = upper.find(" LIKE ").unwrap();
            let field = part[..like_pos].trim().to_string();
            let value = self.parse_value(part[like_pos + 6..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Like,
                value,
            });
        }
        if let Some(pos) = part.find('=') {
            let field = part[..pos].trim().to_string();
            let value = self.parse_value(part[pos + 1..].trim())?;
            return Ok(Condition {
                field,
                operator: CompareOp::Eq,
                value,
            });
        }

        Err(NosqlError::QueryError(format!(
            "Cannot parse condition: {}",
            part
        )))
    }

    // =================================================================
    // Parsing: helpers
    // =================================================================

    fn split_at_keyword<'a>(&self, text: &'a str) -> (&'a str, &'a str) {
        let upper = text.to_uppercase();
        let keywords = [
            "WHERE ",
            "ORDER BY ",
            "LIMIT ",
            "OFFSET ",
            "GROUP BY ",
            "HAVING ",
            "RETURNING ",
        ];

        let mut earliest = text.len();
        for kw in &keywords {
            if let Some(pos) = upper.find(kw) {
                if pos < earliest {
                    earliest = pos;
                }
            }
        }

        (&text[..earliest], &text[earliest..])
    }

    fn parse_value(&self, s: &str) -> Result<serde_json::Value> {
        let s = s.trim();

        if (s.starts_with('"') && s.ends_with('"'))
            || (s.starts_with('\'') && s.ends_with('\''))
        {
            return Ok(serde_json::Value::String(s[1..s.len() - 1].to_string()));
        }

        if s.eq_ignore_ascii_case("true") {
            return Ok(serde_json::Value::Bool(true));
        }
        if s.eq_ignore_ascii_case("false") {
            return Ok(serde_json::Value::Bool(false));
        }
        if s.eq_ignore_ascii_case("null") {
            return Ok(serde_json::Value::Null);
        }

        if let Ok(n) = s.parse::<i64>() {
            return Ok(serde_json::json!(n));
        }
        if let Ok(n) = s.parse::<f64>() {
            return Ok(serde_json::json!(n));
        }

        // JSON object
        if s.starts_with('{') && s.ends_with('}') {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                return Ok(v);
            }
        }

        // JSON array
        if s.starts_with('[') && s.ends_with(']') {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                return Ok(v);
            }
        }

        Ok(serde_json::Value::String(s.to_string()))
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Free functions
// ═══════════════════════════════════════════════════════════════════════

/// Compare two JSON values
fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            if let (Some(na), Some(nb)) = (a.as_f64(), b.as_f64()) {
                return na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal);
            }
            if let (Some(sa), Some(sb)) = (a.as_str(), b.as_str()) {
                return sa.cmp(sb);
            }
            if let (Some(ba), Some(bb)) = (a.as_bool(), b.as_bool()) {
                return ba.cmp(&bb);
            }
            a.to_string().cmp(&b.to_string())
        }
    }
}

/// Match a condition against a field value
fn match_condition_value(
    field_value: Option<&serde_json::Value>,
    operator: &CompareOp,
    cond_value: &serde_json::Value,
) -> bool {
    match operator {
        CompareOp::IsNull => {
            field_value.is_none() || field_value == Some(&serde_json::Value::Null)
        }
        CompareOp::IsNotNull => {
            field_value.is_some() && field_value != Some(&serde_json::Value::Null)
        }
        CompareOp::Eq => field_value.map(|v| v == cond_value).unwrap_or(false),
        CompareOp::Ne => field_value.map(|v| v != cond_value).unwrap_or(true),
        CompareOp::Gt => field_value
            .map(|v| compare_json_values(Some(v), Some(cond_value)) == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        CompareOp::Gte => field_value
            .map(|v| compare_json_values(Some(v), Some(cond_value)) != std::cmp::Ordering::Less)
            .unwrap_or(false),
        CompareOp::Lt => field_value
            .map(|v| compare_json_values(Some(v), Some(cond_value)) == std::cmp::Ordering::Less)
            .unwrap_or(false),
        CompareOp::Lte => field_value
            .map(|v| compare_json_values(Some(v), Some(cond_value)) != std::cmp::Ordering::Greater)
            .unwrap_or(false),
        CompareOp::Like => {
            if let (Some(serde_json::Value::String(val)), serde_json::Value::String(pattern)) =
                (field_value, cond_value)
            {
                simple_like_match(val, pattern)
            } else {
                false
            }
        }
        CompareOp::In => {
            if let serde_json::Value::Array(arr) = cond_value {
                field_value
                    .map(|v| arr.contains(v))
                    .unwrap_or(false)
            } else {
                false
            }
        }
        CompareOp::Between => {
            if let serde_json::Value::Array(arr) = cond_value {
                if arr.len() == 2 {
                    field_value
                        .map(|v| {
                            compare_json_values(Some(v), Some(&arr[0]))
                                != std::cmp::Ordering::Less
                                && compare_json_values(Some(v), Some(&arr[1]))
                                    != std::cmp::Ordering::Greater
                        })
                        .unwrap_or(false)
                } else {
                    false
                }
            } else {
                false
            }
        }
        // Subquery operators are handled in matches_condition, not here
        CompareOp::InSubquery(_) | CompareOp::NotInSubquery(_)
        | CompareOp::Exists(_) | CompareOp::NotExists(_) => false,
    }
}

/// Extract a possibly nested field value from a JSON value using dot-notation
fn extract_field_value(value: &serde_json::Value, field: &str) -> serde_json::Value {
    if field.contains('.') {
        crate::storage::index::extract_field(value, field)
    } else {
        value.get(field).cloned().unwrap_or(serde_json::Value::Null)
    }
}

/// Get the display name for an expression
fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Literal(v) => v.to_string(),
        Expr::Field(f) => f.clone(),
        Expr::MetaId => "META().id".to_string(),
        Expr::Aggregate { func, arg } => format!("{}({})", agg_func_name(func), expr_name(arg)),
        Expr::Function { name, args } => {
            let arg_names: Vec<String> = args.iter().map(expr_name).collect();
            format!("{}({})", name, arg_names.join(", "))
        }
        Expr::Subquery(_) => "(SUBQUERY)".to_string(),
    }
}

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Field(f) => f.clone(),
        Expr::Literal(v) => v.to_string(),
        Expr::MetaId => "META().id".to_string(),
        Expr::Subquery(_) => "(SUBQUERY)".to_string(),
        _ => format!("{:?}", expr),
    }
}

fn agg_func_name(func: &AggFunc) -> &'static str {
    match func {
        AggFunc::Count => "COUNT",
        AggFunc::Sum => "SUM",
        AggFunc::Avg => "AVG",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
    }
}

fn val_to_group_key(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Simple SQL LIKE pattern matching (% and _)
fn simple_like_match(value: &str, pattern: &str) -> bool {
    let pattern = pattern.replace('%', ".*").replace('_', ".");
    regex_lite_match(&pattern, value).unwrap_or(false)
}

fn regex_lite_match(pattern: &str, value: &str) -> std::result::Result<bool, ()> {
    let full_pattern = format!("^{}$", pattern);

    fn match_helper(pattern: &[char], text: &[char]) -> bool {
        if pattern.is_empty() {
            return text.is_empty();
        }
        if pattern.len() >= 2 && pattern[0] == '.' && pattern[1] == '*' {
            for i in 0..=text.len() {
                if match_helper(&pattern[2..], &text[i..]) {
                    return true;
                }
            }
            return false;
        }
        if text.is_empty() {
            return false;
        }
        if pattern[0] == '^' {
            return match_helper(&pattern[1..], text);
        }
        if pattern[0] == '$' {
            return text.is_empty() || (pattern.len() == 1 && text.is_empty());
        }
        if pattern[0] == '.' || pattern[0] == text[0] {
            return match_helper(&pattern[1..], &text[1..]);
        }
        false
    }

    let pattern_chars: Vec<char> = full_pattern.chars().collect();
    let value_chars: Vec<char> = value.chars().collect();
    Ok(match_helper(&pattern_chars, &value_chars))
}

/// Split a string by commas, respecting nested parentheses, braces, and brackets
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    for (i, ch) in s.char_indices() {
        if in_string {
            if ch == string_char {
                in_string = false;
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                in_string = true;
                string_char = ch;
            }
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }

    if start < s.len() {
        parts.push(&s[start..]);
    }

    parts
}

/// Split first comma at top level (for INSERT VALUES parsing)
fn split_first_value_comma(s: &str) -> Result<(&str, &str)> {
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    for (i, ch) in s.char_indices() {
        if in_string {
            if ch == string_char {
                in_string = false;
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                in_string = true;
                string_char = ch;
            }
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ',' if depth == 0 => {
                return Ok((&s[..i], &s[i + 1..]));
            }
            _ => {}
        }
    }

    Err(NosqlError::QueryError(
        "Cannot find comma separator in VALUES clause".to_string(),
    ))
}

/// Find a keyword outside of parentheses (case-insensitive)
fn find_keyword_outside_parens(s: &str, keyword: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let upper_kw = keyword.to_uppercase();
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    for (i, ch) in s.char_indices() {
        if in_string {
            if ch == string_char {
                in_string = false;
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                in_string = true;
                string_char = ch;
            }
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ => {}
        }

        if depth == 0 && i + upper_kw.len() <= upper.len() {
            if upper[i..i + upper_kw.len()] == upper_kw {
                return Some(i);
            }
        }
    }

    None
}

/// Split on " AND " (case-insensitive) respecting parentheses and strings
fn split_and(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let upper = s.to_uppercase();
    let bytes = s.as_bytes();
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = b' ';
    let mut start = 0;

    let mut i = 0;
    while i < bytes.len() {
        if in_string {
            if bytes[i] == string_char {
                in_string = false;
            }
            i += 1;
            continue;
        }

        match bytes[i] {
            b'\'' | b'"' => {
                in_string = true;
                string_char = bytes[i];
            }
            b'(' | b'{' | b'[' => depth += 1,
            b')' | b'}' | b']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ => {}
        }

        if depth == 0 && i + 5 <= upper.len() && &upper[i..i + 5] == " AND " {
            parts.push(&s[start..i]);
            start = i + 5;
            i += 5;
            continue;
        }

        i += 1;
    }

    if start < s.len() {
        parts.push(&s[start..]);
    }

    parts
}

/// Flatten a nested array to a given depth
fn flatten_array(arr: &[serde_json::Value], depth: usize) -> Vec<serde_json::Value> {
    let mut result = Vec::new();
    for item in arr {
        if depth > 0 {
            if let Some(inner) = item.as_array() {
                result.extend(flatten_array(inner, depth - 1));
                continue;
            }
        }
        result.push(item.clone());
    }
    result
}

/// Generate a simple UUID v4-like string
fn generate_uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (now >> 96) as u32,
        (now >> 80) as u16,
        (now >> 64) as u16 & 0x0FFF,
        ((now >> 48) as u16 & 0x3FFF) | 0x8000,
        now as u64 & 0xFFFFFFFFFFFF
    )
}

/// Simple random number (not cryptographically secure)
fn rand_simple() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (nanos as f64) / (u32::MAX as f64)
}

/// Set a possibly nested field using dot-notation
fn set_nested_field(value: &mut serde_json::Value, field: &str, new_val: serde_json::Value) {
    if !field.contains('.') {
        if let Some(obj) = value.as_object_mut() {
            obj.insert(field.to_string(), new_val);
        }
        return;
    }

    let parts: Vec<&str> = field.split('.').collect();
    let mut current = value;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if let Some(obj) = current.as_object_mut() {
                obj.insert(part.to_string(), new_val);
            }
            return;
        }
        match current {
            serde_json::Value::Object(map) => {
                if !map.contains_key(*part) {
                    map.insert(part.to_string(), serde_json::json!({}));
                }
                current = map.get_mut(*part).unwrap();
            }
            _ => return,
        }
    }
}

/// Resolve a dotted path against a joined row.
/// For "b.addresses", look up alias "b" in the row, then field "addresses" in it.
/// For "name", scan all sources in the row.
fn resolve_path_in_row(
    row: &serde_json::Value,
    path: &str,
) -> Option<serde_json::Value> {
    // Try direct key on the row
    if let Some(v) = row.get(path) {
        return Some(v.clone());
    }

    // Try alias.field
    if path.contains('.') {
        let dot_pos = path.find('.').unwrap();
        let alias = &path[..dot_pos];
        let rest = &path[dot_pos + 1..];
        if let Some(source_val) = row.get(alias) {
            let v = extract_field_value(source_val, rest);
            if !v.is_null() {
                return Some(v);
            }
        }
    }

    // Scan all sources
    if let Some(obj) = row.as_object() {
        for (_k, source_val) in obj {
            if source_val.is_object() || source_val.is_array() {
                let v = extract_field_value(source_val, path);
                if !v.is_null() {
                    return Some(v);
                }
            }
        }
    }

    None
}

/// Strip alias prefix from a field reference, e.g. "c.name" with alias "c" → "name"
fn strip_alias_prefix(field: &str, alias: &str) -> String {
    let prefix = format!("{}.", alias);
    if field.starts_with(&prefix) {
        field[prefix.len()..].to_string()
    } else {
        field.to_string()
    }
}

/// Remove a possibly nested field using dot-notation
fn remove_nested_field(value: &mut serde_json::Value, field: &str) {
    if !field.contains('.') {
        if let Some(obj) = value.as_object_mut() {
            obj.remove(field);
        }
        return;
    }

    let parts: Vec<&str> = field.split('.').collect();
    let parent_path = &parts[..parts.len() - 1];
    let last_part = parts[parts.len() - 1];

    let mut current = value;
    for part in parent_path {
        match current {
            serde_json::Value::Object(map) => {
                if let Some(next) = map.get_mut(*part) {
                    current = next;
                } else {
                    return;
                }
            }
            _ => return,
        }
    }

    if let Some(obj) = current.as_object_mut() {
        obj.remove(last_part);
    }
}

/// Convert a JSON parameter value to its SQL literal representation
fn param_value_to_sql(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b { "TRUE".to_string() } else { "FALSE".to_string() }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
        other => other.to_string(),
    }
}

/// Simple base64 encode for prepared statement plans
fn base64_encode(input: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::new();
    let mut i = 0;

    while i < bytes.len() {
        let b0 = bytes[i] as u32;
        let b1 = if i + 1 < bytes.len() { bytes[i + 1] as u32 } else { 0 };
        let b2 = if i + 2 < bytes.len() { bytes[i + 2] as u32 } else { 0 };

        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);

        if i + 1 < bytes.len() {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }

        if i + 2 < bytes.len() {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }

        i += 3;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::BucketConfig;

    fn make_query_engine() -> QueryEngine {
        let storage = Arc::new(StorageEngine::new(16, None, None));
        storage
            .create_bucket(BucketConfig {
                name: "test".to_string(),
                num_vbuckets: 16,
                ..Default::default()
            })
            .unwrap();
        let index_mgr = Arc::new(IndexManager::new());
        QueryEngine::new(storage, index_mgr)
    }

    fn seed_docs(engine: &QueryEngine) {
        let bucket = engine.storage.get_bucket("test").unwrap();
        bucket.upsert("_default", "_default", "u1".to_string(), serde_json::json!({"name": "Alice", "age": 30, "city": "NYC"}), None).unwrap();
        bucket.upsert("_default", "_default", "u2".to_string(), serde_json::json!({"name": "Bob", "age": 25, "city": "LA"}), None).unwrap();
        bucket.upsert("_default", "_default", "u3".to_string(), serde_json::json!({"name": "Charlie", "age": 35, "city": "NYC"}), None).unwrap();
        bucket.upsert("_default", "_default", "u4".to_string(), serde_json::json!({"name": "Diana", "age": 28, "city": "SF"}), None).unwrap();
    }

    #[test]
    fn test_select_all() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest { statement: "SELECT * FROM test".to_string(), params: None };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.status, "success");
        assert_eq!(result.results.len(), 4);
    }

    #[test]
    fn test_select_with_where() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT * FROM test WHERE city = \"NYC\"".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.results.len(), 2);
    }

    #[test]
    fn test_select_with_limit() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT * FROM test LIMIT 2".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.results.len(), 2);
    }

    #[test]
    fn test_select_count() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT COUNT(*) AS cnt FROM test".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0]["cnt"], 4);
    }

    #[test]
    fn test_select_order_by() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT * FROM test ORDER BY age ASC".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.results.len(), 4);
        // Bob (25) should be first — age is in doc.age for SELECT *
        assert_eq!(result.results[0]["doc"]["age"], 25);
    }

    #[test]
    fn test_insert() {
        let engine = make_query_engine();
        let req = QueryRequest {
            statement: "INSERT INTO test (KEY, VALUE) VALUES (\"k1\", {\"x\": 1})".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.status, "success");
        let bucket = engine.storage.get_bucket("test").unwrap();
        let doc = bucket.get("_default", "_default", "k1").unwrap();
        assert_eq!(doc.value["x"], 1);
    }

    #[test]
    fn test_delete() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "DELETE FROM test WHERE name = \"Alice\"".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.status, "success");
        assert!(result.metrics.result_count >= 1);
    }

    #[test]
    fn test_update() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "UPDATE test SET age = 99 WHERE name = \"Bob\"".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.status, "success");
        assert!(result.metrics.result_count >= 1);
    }

    #[test]
    fn test_select_distinct() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT DISTINCT city FROM test".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.results.len(), 3); // NYC, LA, SF
    }

    #[test]
    fn test_explain() {
        let engine = make_query_engine();
        let req = QueryRequest {
            statement: "EXPLAIN SELECT * FROM test WHERE age > 30".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert_eq!(result.status, "success");
        assert!(!result.results.is_empty());
    }

    #[test]
    fn test_select_group_by() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT city, COUNT(*) AS cnt FROM test GROUP BY city".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert!(!result.results.is_empty());
        // NYC should have count 2
        let nyc = result.results.iter().find(|r| r["city"] == "NYC");
        assert!(nyc.is_some());
        assert_eq!(nyc.unwrap()["cnt"], 2);
    }

    #[test]
    fn test_select_specific_fields() {
        let engine = make_query_engine();
        seed_docs(&engine);
        let req = QueryRequest {
            statement: "SELECT name, age FROM test WHERE age > 28".to_string(),
            params: None,
        };
        let result = engine.execute(&req).unwrap();
        assert!(result.results.len() >= 2); // Alice(30), Charlie(35)
        for row in &result.results {
            assert!(row.get("name").is_some());
            assert!(row.get("age").is_some());
        }
    }
}
