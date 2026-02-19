use crate::api::AppState;
use crate::auth::rbac::{list_available_roles, RoleAssignment};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

/// List all users
pub async fn list_users(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let users = state.rbac.list_users();
    let count = users.len();
    Json(serde_json::json!({
        "count": count,
        "users": users,
    }))
}

/// Get a specific user
pub async fn get_user(
    State(state): State<Arc<AppState>>,
    Path(username): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.rbac.get_user(&username) {
        Some(user) => Ok(Json(serde_json::json!(user))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("User '{}' not found", username)})),
        )),
    }
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub display_name: Option<String>,
    pub roles: Option<Vec<RoleAssignment>>,
}

/// Create a new user
pub async fn create_user(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let display_name = body.display_name.unwrap_or_else(|| body.username.clone());
    let roles = body.roles.unwrap_or_default();

    match state.rbac.create_user(body.username, body.password, display_name, roles) {
        Ok(user) => {
            state.audit_logger.log_full(
                crate::audit::logger::AuditEventType::ConfigChanged,
                format!("User '{}' created", user.username),
                None,
                None,
                None,
                None,
            );
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "status": "ok",
                    "username": user.username,
                })),
            ))
        }
        Err(e) => Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// Delete a user
pub async fn delete_user(
    State(state): State<Arc<AppState>>,
    Path(username): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.rbac.delete_user(&username) {
        Ok(()) => {
            state.audit_logger.log_full(
                crate::audit::logger::AuditEventType::ConfigChanged,
                format!("User '{}' deleted", username),
                None,
                None,
                None,
                None,
            );
            Ok(Json(serde_json::json!({
                "status": "ok",
                "message": format!("User '{}' deleted", username),
            })))
        }
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

#[derive(Deserialize)]
pub struct UpdateRolesRequest {
    pub roles: Vec<RoleAssignment>,
}

/// Update user roles
pub async fn update_user_roles(
    State(state): State<Arc<AppState>>,
    Path(username): Path<String>,
    Json(body): Json<UpdateRolesRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.rbac.update_user_roles(&username, body.roles) {
        Ok(()) => Ok(Json(serde_json::json!({
            "status": "ok",
            "message": format!("Roles updated for user '{}'", username),
        }))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    pub password: String,
}

/// Change user password
pub async fn change_password(
    State(state): State<Arc<AppState>>,
    Path(username): Path<String>,
    Json(body): Json<ChangePasswordRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.rbac.change_password(&username, body.password) {
        Ok(()) => Ok(Json(serde_json::json!({
            "status": "ok",
            "message": format!("Password changed for user '{}'", username),
        }))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        )),
    }
}

/// List available roles
pub async fn list_roles() -> Json<serde_json::Value> {
    let roles = list_available_roles();
    Json(serde_json::json!({
        "roles": roles,
    }))
}
