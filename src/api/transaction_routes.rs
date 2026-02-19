use crate::api::AppState;
use crate::transactions::engine::TransactionConfig;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

/// Begin a new transaction
pub async fn begin_transaction(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BeginTransactionRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let config = body.config.unwrap_or_default();
    match state.txn_engine.begin(Some(config)) {
        Ok(txn_id) => {
            state.audit_logger.log_full(
                crate::audit::logger::AuditEventType::DocumentWrite,
                format!("Transaction {} started", txn_id),
                None,
                None,
                None,
                None,
            );
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "status": "ok",
                    "txn_id": txn_id,
                })),
            ))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

#[derive(Deserialize)]
pub struct BeginTransactionRequest {
    pub config: Option<TransactionConfig>,
}

/// Get a document within a transaction
pub async fn transaction_get(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(body): Json<TransactionDocRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let scope = body.scope.unwrap_or_else(|| "_default".to_string());
    let collection = body.collection.unwrap_or_else(|| "_default".to_string());

    match state.txn_engine.get(&txn_id, &body.bucket, &scope, &collection, &body.key) {
        Ok(result) => Ok(Json(serde_json::json!({
            "key": result.key,
            "value": result.value,
            "cas": result.cas,
            "bucket": result.bucket,
            "scope": result.scope,
            "collection": result.collection,
        }))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Insert a document within a transaction
pub async fn transaction_insert(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(body): Json<TransactionMutationRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let scope = body.scope.unwrap_or_else(|| "_default".to_string());
    let collection = body.collection.unwrap_or_else(|| "_default".to_string());
    let value = body.value.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "value is required for insert"})),
        )
    })?;

    match state.txn_engine.insert(&txn_id, &body.bucket, &scope, &collection, &body.key, value) {
        Ok(()) => Ok(Json(serde_json::json!({
            "status": "staged",
            "operation": "insert",
            "key": body.key,
        }))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Replace a document within a transaction
pub async fn transaction_replace(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(body): Json<TransactionMutationRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let scope = body.scope.unwrap_or_else(|| "_default".to_string());
    let collection = body.collection.unwrap_or_else(|| "_default".to_string());
    let value = body.value.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "value is required for replace"})),
        )
    })?;
    let cas = body.cas.unwrap_or(0);

    match state.txn_engine.replace(&txn_id, &body.bucket, &scope, &collection, &body.key, value, cas) {
        Ok(()) => Ok(Json(serde_json::json!({
            "status": "staged",
            "operation": "replace",
            "key": body.key,
        }))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Remove a document within a transaction
pub async fn transaction_remove(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(body): Json<TransactionDocRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let scope = body.scope.unwrap_or_else(|| "_default".to_string());
    let collection = body.collection.unwrap_or_else(|| "_default".to_string());
    let cas = body.cas.unwrap_or(0);

    match state.txn_engine.remove(&txn_id, &body.bucket, &scope, &collection, &body.key, cas) {
        Ok(()) => Ok(Json(serde_json::json!({
            "status": "staged",
            "operation": "remove",
            "key": body.key,
        }))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Commit a transaction
pub async fn commit_transaction(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.txn_engine.commit(&txn_id) {
        Ok(result) => {
            state.audit_logger.log_full(
                crate::audit::logger::AuditEventType::DocumentWrite,
                format!(
                    "Transaction {} committed ({} mutations, {}ms)",
                    result.txn_id, result.mutations_applied, result.elapsed_ms
                ),
                None,
                None,
                None,
                None,
            );
            Ok(Json(serde_json::json!({
                "status": "committed",
                "txn_id": result.txn_id,
                "mutations_applied": result.mutations_applied,
                "elapsed_ms": result.elapsed_ms,
            })))
        }
        Err(e) => Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Rollback a transaction
pub async fn rollback_transaction(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.txn_engine.rollback(&txn_id) {
        Ok(()) => {
            state.audit_logger.log_full(
                crate::audit::logger::AuditEventType::DocumentWrite,
                format!("Transaction {} rolled back", txn_id),
                None,
                None,
                None,
                None,
            );
            Ok(Json(serde_json::json!({
                "status": "rolled_back",
                "txn_id": txn_id,
            })))
        }
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Get transaction status
pub async fn get_transaction(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.txn_engine.get_transaction(&txn_id) {
        Some(txn) => Ok(Json(serde_json::json!(txn))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Transaction '{}' not found", txn_id)})),
        )),
    }
}

/// List active transactions
pub async fn list_transactions(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let active = state.txn_engine.list_active();
    let count = active.len();
    Json(serde_json::json!({
        "count": count,
        "transactions": active,
    }))
}

#[derive(Deserialize)]
pub struct TransactionDocRequest {
    pub bucket: String,
    pub scope: Option<String>,
    pub collection: Option<String>,
    pub key: String,
    pub cas: Option<u64>,
}

#[derive(Deserialize)]
pub struct TransactionMutationRequest {
    pub bucket: String,
    pub scope: Option<String>,
    pub collection: Option<String>,
    pub key: String,
    pub value: Option<serde_json::Value>,
    pub cas: Option<u64>,
}
