use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::RwLock;
use tracing::info;

/// Maximum number of audit events kept in memory
const MAX_AUDIT_EVENTS: usize = 10000;

/// Audit event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditEventType {
    // Authentication events
    AuthSuccess,
    AuthFailure,

    // Bucket operations
    BucketCreated,
    BucketDeleted,
    BucketFlushed,

    // Scope/Collection operations
    ScopeCreated,
    ScopeDeleted,
    CollectionCreated,
    CollectionDeleted,

    // Document operations
    DocumentRead,
    DocumentWrite,
    DocumentDelete,

    // Index operations
    IndexCreated,
    IndexDropped,

    // FTS operations
    FtsIndexCreated,
    FtsIndexDropped,
    FtsSearch,

    // XDCR operations
    XdcrReplicationCreated,
    XdcrReplicationDeleted,

    // Cluster operations
    NodeAdded,
    NodeRemoved,
    NodeFailover,

    // Admin operations
    ConfigChanged,
    ServerStarted,
    ServerStopped,
}

/// A single audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID
    pub id: u64,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Event type
    pub event_type: AuditEventType,
    /// Description of the event
    pub description: String,
    /// User who triggered the event (if applicable)
    pub user: Option<String>,
    /// Remote address (if applicable)
    pub remote_addr: Option<String>,
    /// Bucket name (if applicable)
    pub bucket: Option<String>,
    /// Additional details as JSON
    pub details: Option<serde_json::Value>,
}

/// The audit logger — thread-safe, in-memory ring buffer
pub struct AuditLogger {
    events: RwLock<VecDeque<AuditEvent>>,
    counter: std::sync::atomic::AtomicU64,
    enabled: RwLock<bool>,
}

impl AuditLogger {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(MAX_AUDIT_EVENTS)),
            counter: std::sync::atomic::AtomicU64::new(1),
            enabled: RwLock::new(true),
        }
    }

    /// Check if audit logging is enabled
    pub fn is_enabled(&self) -> bool {
        *self.enabled.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Enable/disable audit logging
    pub fn set_enabled(&self, enabled: bool) {
        if let Ok(mut e) = self.enabled.write() {
            *e = enabled;
        }
    }

    /// Log an audit event
    pub fn log(&self, event_type: AuditEventType, description: String) {
        self.log_full(event_type, description, None, None, None, None);
    }

    /// Log an audit event with full details
    pub fn log_full(
        &self,
        event_type: AuditEventType,
        description: String,
        user: Option<String>,
        remote_addr: Option<String>,
        bucket: Option<String>,
        details: Option<serde_json::Value>,
    ) {
        if !self.is_enabled() {
            return;
        }

        let event = AuditEvent {
            id: self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            timestamp: Utc::now(),
            event_type: event_type.clone(),
            description: description.clone(),
            user: user.clone(),
            remote_addr,
            bucket,
            details,
        };

        // Log to tracing as well
        info!(
            "AUDIT [{}] {:?}: {} (user={:?})",
            event.id,
            event_type,
            description,
            user,
        );

        if let Ok(mut events) = self.events.write() {
            if events.len() >= MAX_AUDIT_EVENTS {
                events.pop_front();
            }
            events.push_back(event);
        }
    }

    /// Get recent audit events
    pub fn get_events(&self, limit: usize, offset: usize) -> Vec<AuditEvent> {
        if let Ok(events) = self.events.read() {
            events
                .iter()
                .rev()
                .skip(offset)
                .take(limit)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get events filtered by type
    #[allow(dead_code)]
    pub fn get_events_by_type(
        &self,
        event_type: &AuditEventType,
        limit: usize,
    ) -> Vec<AuditEvent> {
        let type_str = serde_json::to_string(event_type).unwrap_or_default();
        if let Ok(events) = self.events.read() {
            events
                .iter()
                .rev()
                .filter(|e| serde_json::to_string(&e.event_type).unwrap_or_default() == type_str)
                .take(limit)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get total event count
    pub fn event_count(&self) -> usize {
        self.events
            .read()
            .map(|e| e.len())
            .unwrap_or(0)
    }

    /// Clear all audit events
    pub fn clear(&self) {
        if let Ok(mut events) = self.events.write() {
            events.clear();
        }
    }

    /// Get events for a specific bucket
    pub fn get_events_for_bucket(&self, bucket_name: &str, limit: usize) -> Vec<AuditEvent> {
        if let Ok(events) = self.events.read() {
            events
                .iter()
                .rev()
                .filter(|e| e.bucket.as_deref() == Some(bucket_name))
                .take(limit)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }
}
