use crate::api::AppState;
use crate::storage::engine::BucketConfig;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

/// Create a full backup of all (or specific) buckets
pub async fn create_backup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateBackupRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let data_dir = state.config.data_dir.clone();
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let backup_name = body.name.unwrap_or_else(|| format!("backup_{}", timestamp));
    let backup_dir = format!("{}/backups/{}", data_dir, backup_name);

    // Create backup directory
    if let Err(e) = std::fs::create_dir_all(&backup_dir) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to create backup dir: {}", e)})),
        ));
    }

    let buckets_to_backup: Vec<String> = if let Some(ref bucket_names) = body.buckets {
        bucket_names.clone()
    } else {
        state.storage.list_buckets().iter().map(|b| b.name.clone()).collect()
    };

    let mut backed_up = Vec::new();
    let mut total_docs = 0usize;

    for bucket_name in &buckets_to_backup {
        let bucket = match state.storage.get_bucket(bucket_name) {
            Ok(b) => b,
            Err(_) => continue,
        };

        let bucket_backup_dir = format!("{}/{}", backup_dir, bucket_name);
        if let Err(e) = std::fs::create_dir_all(&bucket_backup_dir) {
            tracing::warn!("Failed to create bucket backup dir: {}", e);
            continue;
        }

        // Save bucket config
        let config_json = serde_json::to_string_pretty(&bucket.config).unwrap_or_default();
        if let Err(e) = std::fs::write(format!("{}/config.json", bucket_backup_dir), &config_json) {
            tracing::warn!("Failed to write bucket config: {}", e);
            continue;
        }

        // Save scope metadata
        let scopes = bucket.list_scopes();
        let scopes_json = serde_json::to_string_pretty(&scopes).unwrap_or_default();
        let _ = std::fs::write(format!("{}/scopes.json", bucket_backup_dir), &scopes_json);

        // Save all documents
        let docs = bucket.scan_all_documents();
        let doc_count = docs.len();
        let docs_json = serde_json::to_string_pretty(&docs).unwrap_or_default();
        if let Err(e) = std::fs::write(format!("{}/documents.json", bucket_backup_dir), &docs_json) {
            tracing::warn!("Failed to write documents: {}", e);
            continue;
        }

        total_docs += doc_count;
        backed_up.push(serde_json::json!({
            "bucket": bucket_name,
            "documents": doc_count,
        }));
    }

    // Write backup manifest
    let manifest = serde_json::json!({
        "name": backup_name,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "buckets": backed_up,
        "total_documents": total_docs,
    });
    let _ = std::fs::write(
        format!("{}/manifest.json", backup_dir),
        serde_json::to_string_pretty(&manifest).unwrap_or_default(),
    );

    state.audit_logger.log_full(
        crate::audit::logger::AuditEventType::ConfigChanged,
        format!("Backup '{}' created ({} buckets, {} docs)", backup_name, backed_up.len(), total_docs),
        None,
        None,
        None,
        None,
    );

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "ok",
            "backup": backup_name,
            "path": backup_dir,
            "buckets": backed_up.len(),
            "total_documents": total_docs,
        })),
    ))
}

#[derive(Deserialize)]
pub struct CreateBackupRequest {
    pub name: Option<String>,
    pub buckets: Option<Vec<String>>,
}

/// List available backups
pub async fn list_backups(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let backup_dir = format!("{}/backups", state.config.data_dir);
    let mut backups = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&backup_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                let manifest_path = entry.path().join("manifest.json");
                let manifest = if manifest_path.exists() {
                    std::fs::read_to_string(&manifest_path)
                        .ok()
                        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                } else {
                    None
                };
                backups.push(serde_json::json!({
                    "name": name,
                    "manifest": manifest,
                }));
            }
        }
    }

    let count = backups.len();
    Json(serde_json::json!({
        "count": count,
        "backups": backups,
    }))
}

/// Get backup details
pub async fn get_backup(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let backup_dir = format!("{}/backups/{}", state.config.data_dir, name);
    let manifest_path = format!("{}/manifest.json", backup_dir);

    if !std::path::Path::new(&manifest_path).exists() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Backup '{}' not found", name)})),
        ));
    }

    let manifest_str = std::fs::read_to_string(&manifest_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_str)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;

    Ok(Json(manifest))
}

/// Restore from a backup
pub async fn restore_backup(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<RestoreRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let backup_dir = format!("{}/backups/{}", state.config.data_dir, name);

    if !std::path::Path::new(&backup_dir).exists() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Backup '{}' not found", name)})),
        ));
    }

    // Read manifest
    let manifest_str = std::fs::read_to_string(format!("{}/manifest.json", backup_dir))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;
    let _manifest: serde_json::Value = serde_json::from_str(&manifest_str)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;

    // List bucket directories in backup
    let bucket_dirs: Vec<String> = std::fs::read_dir(&backup_dir)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    let filter_buckets = body.buckets.clone();
    let mut restored = Vec::new();
    let mut total_docs = 0usize;

    for bucket_name in &bucket_dirs {
        // Filter if specific buckets requested
        if let Some(ref filter) = filter_buckets {
            if !filter.contains(bucket_name) {
                continue;
            }
        }

        let bucket_backup_dir = format!("{}/{}", backup_dir, bucket_name);

        // Load bucket config
        let config_path = format!("{}/config.json", bucket_backup_dir);
        let config: BucketConfig = if std::path::Path::new(&config_path).exists() {
            let config_str = std::fs::read_to_string(&config_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;
            serde_json::from_str(&config_str)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?
        } else {
            let mut cfg = BucketConfig::default();
            cfg.name = bucket_name.clone();
            cfg
        };

        // Create bucket if it doesn't exist
        let _ = state.storage.create_bucket(config);

        // Load scope metadata
        let scopes_path = format!("{}/scopes.json", bucket_backup_dir);
        if std::path::Path::new(&scopes_path).exists() {
            if let Ok(scopes_str) = std::fs::read_to_string(&scopes_path) {
                if let Ok(scopes) = serde_json::from_str::<Vec<crate::storage::engine::ScopeInfo>>(&scopes_str) {
                    if let Ok(bucket) = state.storage.get_bucket(bucket_name) {
                        for scope_info in &scopes {
                            if scope_info.name != "_default" {
                                let _ = bucket.create_scope(scope_info.name.clone());
                            }
                            for coll_name in &scope_info.collections {
                                if coll_name != "_default" || scope_info.name != "_default" {
                                    let _ = bucket.create_collection(&scope_info.name, coll_name.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Load documents
        let docs_path = format!("{}/documents.json", bucket_backup_dir);
        if std::path::Path::new(&docs_path).exists() {
            let docs_str = std::fs::read_to_string(&docs_path)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;
            let docs: Vec<crate::storage::document::Document> = serde_json::from_str(&docs_str)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;

            let doc_count = docs.len();
            if let Ok(bucket) = state.storage.get_bucket(bucket_name) {
                for doc in &docs {
                    let mutation = doc.to_mutation();
                    let _ = bucket.apply_mutation(&mutation);
                }
            }
            total_docs += doc_count;
            restored.push(serde_json::json!({
                "bucket": bucket_name,
                "documents": doc_count,
            }));
        }
    }

    state.audit_logger.log_full(
        crate::audit::logger::AuditEventType::ConfigChanged,
        format!("Backup '{}' restored ({} buckets, {} docs)", name, restored.len(), total_docs),
        None,
        None,
        None,
        None,
    );

    Ok(Json(serde_json::json!({
        "status": "ok",
        "backup": name,
        "buckets_restored": restored,
        "total_documents": total_docs,
    })))
}

#[derive(Deserialize)]
pub struct RestoreRequest {
    pub buckets: Option<Vec<String>>,
}

/// Delete a backup
pub async fn delete_backup(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let backup_dir = format!("{}/backups/{}", state.config.data_dir, name);

    if !std::path::Path::new(&backup_dir).exists() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Backup '{}' not found", name)})),
        ));
    }

    std::fs::remove_dir_all(&backup_dir)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;

    state.audit_logger.log_full(
        crate::audit::logger::AuditEventType::ConfigChanged,
        format!("Backup '{}' deleted", name),
        None,
        None,
        None,
        None,
    );

    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Backup '{}' deleted", name),
    })))
}
