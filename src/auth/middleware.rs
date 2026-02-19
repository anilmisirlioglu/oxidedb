use crate::api::AppState;
use crate::auth::rbac::{Permission, User};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use std::sync::Arc;

/// Authenticated user extracted from the request.
/// If the request has no auth header, falls back to the default admin
/// (backward-compatible for development). In production, this should be
/// changed to require authentication.
#[derive(Debug, Clone)]
pub struct AuthUser(pub User);

impl AuthUser {
    #[allow(dead_code)]
    pub fn username(&self) -> &str {
        &self.0.username
    }

    /// Check if this user has a given permission
    #[allow(dead_code)]
    pub fn has_permission(&self, rbac: &crate::auth::rbac::RbacManager, perm: &Permission) -> bool {
        rbac.check_permission(&self.0.username, perm)
    }
}

/// Error returned when authentication fails
pub struct AuthError {
    pub message: String,
    pub status: StatusCode,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let body = axum::Json(serde_json::json!({
            "error": self.message,
            "status": self.status.as_u16()
        }));
        (self.status, body).into_response()
    }
}

/// Extract Basic authentication credentials from the Authorization header
fn extract_basic_auth(parts: &Parts) -> Option<(String, String)> {
    let auth_header = parts.headers.get("authorization")?.to_str().ok()?;
    if !auth_header.starts_with("Basic ") {
        return None;
    }

    let encoded = &auth_header[6..];
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let decoded_str = String::from_utf8(decoded).ok()?;

    let (username, password) = decoded_str.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

#[axum::async_trait]
impl FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        // Try to extract Basic auth credentials
        if let Some((username, password)) = extract_basic_auth(parts) {
            match state.rbac.authenticate(&username, &password) {
                Some(user) => return Ok(AuthUser(user)),
                None => {
                    return Err(AuthError {
                        message: "Invalid username or password".to_string(),
                        status: StatusCode::UNAUTHORIZED,
                    })
                }
            }
        }

        // No auth header — allow access with default admin for backward
        // compatibility and UI access. In production, remove this fallback.
        if let Some(user) = state.rbac.authenticate("Administrator", "password") {
            return Ok(AuthUser(user));
        }

        // If default admin credentials have been changed, require auth
        Err(AuthError {
            message: "Authentication required. Use Basic auth with valid credentials.".to_string(),
            status: StatusCode::UNAUTHORIZED,
        })
    }
}

/// Permission guard — use in handlers to check a specific permission
pub fn require_permission(
    user: &AuthUser,
    rbac: &crate::auth::rbac::RbacManager,
    permission: &Permission,
) -> Result<(), AuthError> {
    if rbac.check_permission(&user.0.username, permission) {
        Ok(())
    } else {
        Err(AuthError {
            message: format!(
                "User '{}' does not have permission: {:?}",
                user.0.username, permission
            ),
            status: StatusCode::FORBIDDEN,
        })
    }
}
