use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum NosqlError {
    #[error("Bucket '{0}' not found")]
    BucketNotFound(String),

    #[error("Bucket '{0}' already exists")]
    BucketAlreadyExists(String),

    #[error("Scope '{0}' not found")]
    ScopeNotFound(String),

    #[error("Scope '{0}' already exists")]
    ScopeAlreadyExists(String),

    #[error("Collection '{0}' not found")]
    CollectionNotFound(String),

    #[error("Collection '{0}' already exists")]
    CollectionAlreadyExists(String),

    #[error("Document '{0}' not found")]
    DocumentNotFound(String),

    #[error("CAS mismatch: expected {expected}, got {actual}")]
    CasMismatch { expected: u64, actual: u64 },

    #[error("Document has expired")]
    DocumentExpired,

    #[error("XDCR replication '{0}' not found")]
    ReplicationNotFound(String),

    #[error("XDCR replication '{0}' already exists")]
    ReplicationAlreadyExists(String),

    #[error("Remote cluster '{0}' not found")]
    RemoteClusterNotFound(String),

    #[error("Remote cluster '{0}' already exists")]
    RemoteClusterAlreadyExists(String),

    #[error("Node '{0}' not found")]
    NodeNotFound(String),

    #[error("Node '{0}' already exists")]
    NodeAlreadyExists(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Persistence error: {0}")]
    PersistenceError(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("XDCR connection error: {0}")]
    XdcrConnectionError(String),

    #[error("VBucket {0} not found")]
    VBucketNotFound(u16),

    #[error("Document '{0}' is locked")]
    DocumentLocked(String),

    #[error("Sub-document path '{0}' not found")]
    SubdocPathNotFound(String),

    #[error("Sub-document path '{0}' type mismatch")]
    SubdocPathMismatch(String),

    #[error("Sub-document path '{0}' already exists")]
    SubdocPathExists(String),

    #[error("Memory quota exceeded for bucket '{0}': using {1} MB of {2} MB")]
    MemoryQuotaExceeded(String, u64, u64),
}

impl IntoResponse for NosqlError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            NosqlError::BucketNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::BucketAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::ScopeNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::ScopeAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::CollectionNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::CollectionAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::DocumentNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::CasMismatch { .. } => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::DocumentExpired => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::ReplicationNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::ReplicationAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::RemoteClusterNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::RemoteClusterAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::NodeNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::NodeAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::QueryError(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            NosqlError::PersistenceError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
            NosqlError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            NosqlError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            NosqlError::XdcrConnectionError(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, self.to_string())
            }
            NosqlError::VBucketNotFound(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
            NosqlError::DocumentLocked(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::SubdocPathNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            NosqlError::SubdocPathMismatch(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            NosqlError::SubdocPathExists(_) => (StatusCode::CONFLICT, self.to_string()),
            NosqlError::MemoryQuotaExceeded(_, _, _) => {
                (StatusCode::INSUFFICIENT_STORAGE, self.to_string())
            }
        };

        let body = axum::Json(json!({
            "error": message,
            "status": status.as_u16()
        }));

        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, NosqlError>;
