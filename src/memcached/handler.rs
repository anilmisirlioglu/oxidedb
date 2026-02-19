//! Memcached request handler
//!
//! Translates memcached binary protocol operations into storage engine calls.

use crate::error::NosqlError;
use crate::storage::engine::StorageEngine;
use crate::storage::index::IndexManager;
use super::protocol::*;
use std::sync::Arc;
use tracing::debug;

/// XATTR flag in subdoc spec flags byte
const SUBDOC_FLAG_XATTR: u8 = 0x04;

/// A parsed sub-document lookup spec
struct SubdocLookupSpec {
    opcode: u8,
    flags: u8,
    path: String,
}

impl SubdocLookupSpec {
    fn is_xattr(&self) -> bool {
        self.flags & SUBDOC_FLAG_XATTR != 0
    }
}

/// A parsed sub-document mutation spec
struct SubdocMutationSpec {
    opcode: u8,
    flags: u8,
    path: String,
    value: serde_json::Value,
}

impl SubdocMutationSpec {
    fn is_xattr(&self) -> bool {
        self.flags & SUBDOC_FLAG_XATTR != 0
    }
}

/// Parse sub-document lookup specs from the value portion of a multi-lookup request
/// Each spec: 1 byte opcode + 1 byte flags + 2 bytes path_len + path_bytes
fn parse_subdoc_lookup_specs(data: &[u8]) -> Vec<SubdocLookupSpec> {
    let mut specs = Vec::new();
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let opcode = data[offset];
        let flags = data[offset + 1];
        let path_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        if offset + path_len > data.len() { break; }
        let path = String::from_utf8_lossy(&data[offset..offset + path_len]).to_string();
        offset += path_len;
        specs.push(SubdocLookupSpec { opcode, flags, path });
    }
    specs
}

/// Parse sub-document mutation specs from the value portion of a multi-mutation request
/// Each spec: 1 byte opcode + 1 byte flags + 2 bytes path_len + 4 bytes value_len + path_bytes + value_bytes
fn parse_subdoc_mutation_specs(data: &[u8]) -> Vec<SubdocMutationSpec> {
    let mut specs = Vec::new();
    let mut offset = 0;
    while offset + 8 <= data.len() {
        let opcode = data[offset];
        let flags = data[offset + 1];
        let path_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
        let value_len = u32::from_be_bytes([data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]]) as usize;
        offset += 8;
        if offset + path_len + value_len > data.len() { break; }
        let path = String::from_utf8_lossy(&data[offset..offset + path_len]).to_string();
        offset += path_len;
        let value: serde_json::Value = if value_len > 0 {
            serde_json::from_slice(&data[offset..offset + value_len]).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };
        offset += value_len;
        specs.push(SubdocMutationSpec { opcode, flags, path, value });
    }
    specs
}

/// Handles KV operations from the memcached binary protocol
pub struct KvHandler {
    pub storage: Arc<StorageEngine>,
    pub index_manager: Arc<IndexManager>,
    pub dcp_engine: Arc<crate::dcp::stream::DcpEngine>,
}

impl KvHandler {
    pub fn new(
        storage: Arc<StorageEngine>,
        index_manager: Arc<IndexManager>,
        dcp_engine: Arc<crate::dcp::stream::DcpEngine>,
    ) -> Self {
        Self { storage, index_manager, dcp_engine }
    }

    /// Publish a DCP mutation event after a successful KV write
    fn dcp_publish_upsert(&self, bucket: &str, doc: &crate::storage::document::Document) {
        self.dcp_engine.publish_mutation(
            bucket,
            "_default",
            "_default",
            &doc.key,
            Some(&doc.value),
            doc.cas,
            doc.vbucket_id,
            doc.expiry.map(|e| {
                let now = chrono::Utc::now();
                if e > now { (e - now).num_seconds().max(0) as u32 } else { 0 }
            }).unwrap_or(0),
        );
    }

    /// Publish a DCP deletion event after a successful KV delete
    fn dcp_publish_delete(&self, bucket: &str, key: &str, cas: u64, vbucket_id: u16) {
        self.dcp_engine.publish_deletion(bucket, "_default", "_default", key, cas, vbucket_id);
    }

    /// Handle a GET or GETK request
    pub fn handle_get(&self, req: &Request, bucket_name: &str, return_key: bool) -> Response {
        let key = req.key_str();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        debug!("MC GET key={} bucket={}", key, bucket_name);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.get("_default", "_default", key) {
                    Ok(doc) => {
                        if doc.deleted || doc.is_expired() {
                            return Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque);
                        }

                        // Serialize value
                        let value_bytes = serde_json::to_vec(&doc.value).unwrap_or_default();

                        // Extras: 4 bytes flags
                        let mut extras = Vec::with_capacity(4);
                        extras.extend_from_slice(&doc.flags.to_be_bytes());

                        let mut resp = Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_extras(extras)
                            .with_value(value_bytes)
                            .with_datatype(0x01); // JSON datatype

                        if return_key {
                            resp = resp.with_key(req.key.clone());
                        }

                        resp
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => {
                        Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string())
                    }
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle a SET (upsert) request
    pub fn handle_set(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let (flags, expiry_secs) = req.mutation_extras();

        // Parse value - try JSON first, fallback to string
        let value: serde_json::Value = serde_json::from_slice(&req.value)
            .unwrap_or_else(|_| {
                // Store as raw string if not valid JSON
                serde_json::Value::String(String::from_utf8_lossy(&req.value).to_string())
            });

        let expiry = if expiry_secs > 0 {
            // Couchbase: if expiry > 30 days (2592000), it's a Unix timestamp
            if expiry_secs > 2_592_000 {
                let now = chrono::Utc::now().timestamp() as u32;
                if expiry_secs > now {
                    Some((expiry_secs - now) as u64)
                } else {
                    Some(0)
                }
            } else {
                Some(expiry_secs as u64)
            }
        } else {
            None
        };

        debug!("MC SET key={} bucket={} flags={} expiry={:?}", key, bucket_name, flags, expiry);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                // CAS check
                if req.header.cas != 0 {
                    match bucket.replace("_default", "_default", &key, value, Some(req.header.cas)) {
                        Ok(mut doc) => {
                            doc.flags = flags;
                            // Persist flags to the stored document in the vBucket
                            bucket.set_document_flags(&doc.key, flags);
                            // Buffer mutation for WAL
                            let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                            if needs_flush { let _ = self.storage.flush_wal_buffer(); }
                            self.index_manager.on_document_upsert(bucket_name, &doc);
                            self.dcp_publish_upsert(bucket_name, &doc);

                            Response::new(req.header.opcode, Status::Success, req.header.opaque)
                                .with_cas(doc.cas)
                        }
                        Err(NosqlError::CasMismatch { .. }) => {
                            Response::new(req.header.opcode, Status::KeyExists, req.header.opaque)
                        }
                        Err(NosqlError::DocumentNotFound(_)) => {
                            Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                        }
                        Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                    }
                } else {
                    match bucket.upsert("_default", "_default", key, value, expiry) {
                        Ok(mut doc) => {
                            doc.flags = flags;
                            // Persist flags to the stored document in the vBucket
                            bucket.set_document_flags(&doc.key, flags);
                            let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                            if needs_flush { let _ = self.storage.flush_wal_buffer(); }
                            self.index_manager.on_document_upsert(bucket_name, &doc);
                            self.dcp_publish_upsert(bucket_name, &doc);

                            Response::new(req.header.opcode, Status::Success, req.header.opaque)
                                .with_cas(doc.cas)
                        }
                        Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                    }
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle an ADD request (insert only if key doesn't exist)
    pub fn handle_add(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let (flags, expiry_secs) = req.mutation_extras();

        let value: serde_json::Value = serde_json::from_slice(&req.value)
            .unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&req.value).to_string())
            });

        let expiry = if expiry_secs > 0 {
            if expiry_secs > 2_592_000 {
                let now = chrono::Utc::now().timestamp() as u32;
                Some((expiry_secs.saturating_sub(now)) as u64)
            } else {
                Some(expiry_secs as u64)
            }
        } else {
            None
        };

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                // Check if key already exists
                if let Ok(doc) = bucket.get("_default", "_default", &key) {
                    if !doc.deleted && !doc.is_expired() {
                        return Response::new(req.header.opcode, Status::KeyExists, req.header.opaque);
                    }
                }

                match bucket.upsert("_default", "_default", key, value, expiry) {
                    Ok(mut doc) => {
                        doc.flags = flags;
                        // Persist flags to the stored document in the vBucket
                        bucket.set_document_flags(&doc.key, flags);
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }
                        self.index_manager.on_document_upsert(bucket_name, &doc);
                        self.dcp_publish_upsert(bucket_name, &doc);

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle a REPLACE request (update only if key exists)
    pub fn handle_replace(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let (flags, _expiry_secs) = req.mutation_extras();

        let value: serde_json::Value = serde_json::from_slice(&req.value)
            .unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&req.value).to_string())
            });

        let cas = if req.header.cas != 0 { Some(req.header.cas) } else { None };

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.replace("_default", "_default", &key, value, cas) {
                    Ok(mut doc) => {
                        doc.flags = flags;
                        // Persist flags to the stored document in the vBucket
                        bucket.set_document_flags(&doc.key, flags);
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }
                        self.index_manager.on_document_upsert(bucket_name, &doc);
                        self.dcp_publish_upsert(bucket_name, &doc);

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(NosqlError::CasMismatch { .. }) => {
                        Response::new(req.header.opcode, Status::KeyExists, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle a DELETE request
    pub fn handle_delete(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let cas = if req.header.cas != 0 { Some(req.header.cas) } else { None };

        debug!("MC DELETE key={} bucket={}", key, bucket_name);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.delete("_default", "_default", &key, cas) {
                    Ok(doc) => {
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }
                        self.index_manager.on_document_delete(bucket_name, &key);
                        self.dcp_publish_delete(bucket_name, &key, doc.cas, doc.vbucket_id);

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(NosqlError::CasMismatch { .. }) => {
                        Response::new(req.header.opcode, Status::KeyExists, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle a TOUCH request (update expiry only)
    pub fn handle_touch(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        // Touch extras: 4 bytes expiry
        let expiry = if req.extras.len() >= 4 {
            u32::from_be_bytes([req.extras[0], req.extras[1], req.extras[2], req.extras[3]])
        } else {
            0
        };

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.touch("_default", "_default", &key, expiry as u64) {
                    Ok(doc) => {
                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle INCREMENT request
    pub fn handle_increment(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        // Extras: 8 bytes delta + 8 bytes initial + 4 bytes expiry = 20 bytes
        if req.extras.len() < 20 {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Invalid extras");
        }
        let delta = u64::from_be_bytes([
            req.extras[0], req.extras[1], req.extras[2], req.extras[3],
            req.extras[4], req.extras[5], req.extras[6], req.extras[7],
        ]);
        let initial = u64::from_be_bytes([
            req.extras[8], req.extras[9], req.extras[10], req.extras[11],
            req.extras[12], req.extras[13], req.extras[14], req.extras[15],
        ]);
        let expiry_secs = u32::from_be_bytes([
            req.extras[16], req.extras[17], req.extras[18], req.extras[19],
        ]);

        let expiry = if expiry_secs > 0 { Some(expiry_secs as u64) } else { None };
        // 0xFFFFFFFF means "don't create if doesn't exist"
        let create = expiry_secs != 0xFFFFFFFF;

        debug!("MC INCR key={} delta={} initial={} expiry={:?}", key, delta, initial, expiry);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.counter("_default", "_default", &key, delta as i64, initial, expiry, create) {
                    Ok((doc, value)) => {
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }

                        // Response value: 8 bytes counter value
                        let value_bytes = value.to_be_bytes().to_vec();
                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_value(value_bytes)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle DECREMENT request
    pub fn handle_decrement(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        if req.extras.len() < 20 {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Invalid extras");
        }
        let delta = u64::from_be_bytes([
            req.extras[0], req.extras[1], req.extras[2], req.extras[3],
            req.extras[4], req.extras[5], req.extras[6], req.extras[7],
        ]);
        let initial = u64::from_be_bytes([
            req.extras[8], req.extras[9], req.extras[10], req.extras[11],
            req.extras[12], req.extras[13], req.extras[14], req.extras[15],
        ]);
        let expiry_secs = u32::from_be_bytes([
            req.extras[16], req.extras[17], req.extras[18], req.extras[19],
        ]);

        let expiry = if expiry_secs > 0 { Some(expiry_secs as u64) } else { None };
        let create = expiry_secs != 0xFFFFFFFF;

        debug!("MC DECR key={} delta={} initial={} expiry={:?}", key, delta, initial, expiry);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.counter("_default", "_default", &key, -(delta as i64), initial, expiry, create) {
                    Ok((doc, value)) => {
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }

                        let value_bytes = value.to_be_bytes().to_vec();
                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_value(value_bytes)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle APPEND request
    pub fn handle_append(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let cas = if req.header.cas != 0 { Some(req.header.cas) } else { None };

        debug!("MC APPEND key={} value_len={}", key, req.value.len());

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.append("_default", "_default", &key, &req.value, cas) {
                    Ok(doc) => {
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::ItemNotStored, req.header.opaque)
                    }
                    Err(NosqlError::CasMismatch { .. }) => {
                        Response::new(req.header.opcode, Status::KeyExists, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle PREPEND request
    pub fn handle_prepend(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let cas = if req.header.cas != 0 { Some(req.header.cas) } else { None };

        debug!("MC PREPEND key={} value_len={}", key, req.value.len());

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.prepend("_default", "_default", &key, &req.value, cas) {
                    Ok(doc) => {
                        let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                        if needs_flush { let _ = self.storage.flush_wal_buffer(); }

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::ItemNotStored, req.header.opaque)
                    }
                    Err(NosqlError::CasMismatch { .. }) => {
                        Response::new(req.header.opcode, Status::KeyExists, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle EXISTS request (check document existence, return CAS)
    #[allow(dead_code)]
    pub fn handle_exists(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.exists("_default", "_default", &key) {
                    Ok(cas) => {
                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(cas)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle GET_LOCKED request
    pub fn handle_get_locked(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        // Lock duration from extras (4 bytes)
        let lock_seconds = if req.extras.len() >= 4 {
            u32::from_be_bytes([req.extras[0], req.extras[1], req.extras[2], req.extras[3]])
        } else {
            15 // default 15 seconds
        };

        debug!("MC GET_LOCKED key={} lock_secs={}", key, lock_seconds);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.get_and_lock("_default", "_default", &key, lock_seconds) {
                    Ok(doc) => {
                        let value_bytes = serde_json::to_vec(&doc.value).unwrap_or_default();
                        let mut extras = Vec::with_capacity(4);
                        extras.extend_from_slice(&doc.flags.to_be_bytes());

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_extras(extras)
                            .with_value(value_bytes)
                            .with_datatype(0x01)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(NosqlError::DocumentLocked(_)) => {
                        Response::new(req.header.opcode, Status::Locked, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle UNLOCK_KEY request
    pub fn handle_unlock(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let cas = req.header.cas;
        debug!("MC UNLOCK key={} cas={}", key, cas);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.unlock("_default", "_default", &key, cas) {
                    Ok(()) => {
                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                    }
                    Err(NosqlError::CasMismatch { .. }) => {
                        Response::new(req.header.opcode, Status::KeyExists, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::TmpFail, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle SUBDOC_MULTI_LOOKUP request
    pub fn handle_subdoc_multi_lookup(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(Opcode::SubdocMultiLookup, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        // Parse lookup specs from value
        let specs = parse_subdoc_lookup_specs(&req.value);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                // Check document exists first
                match bucket.get("_default", "_default", &key) {
                    Ok(doc) => {
                        let mut response_value = Vec::new();
                        let mut all_ok = true;

                        for spec in &specs {
                            let (status, value) = if spec.is_xattr() {
                                // ── XATTR lookup ──────────────────────────
                                match spec.opcode {
                                    0xC5 => { // SubdocGet on xattr
                                        match bucket.xattr_get("_default", "_default", &key, &spec.path) {
                                            Ok(v) => {
                                                let bytes = serde_json::to_vec(&v).unwrap_or_default();
                                                (Status::Success, bytes)
                                            }
                                            Err(NosqlError::SubdocPathNotFound(_)) => {
                                                all_ok = false;
                                                (Status::SubdocPathNotFound, vec![])
                                            }
                                            Err(_) => {
                                                all_ok = false;
                                                (Status::InternalError, vec![])
                                            }
                                        }
                                    }
                                    0xC6 => { // SubdocExists on xattr
                                        match bucket.xattr_exists("_default", "_default", &key, &spec.path) {
                                            Ok(true) => (Status::Success, vec![]),
                                            Ok(false) => {
                                                all_ok = false;
                                                (Status::SubdocPathNotFound, vec![])
                                            }
                                            Err(_) => {
                                                all_ok = false;
                                                (Status::InternalError, vec![])
                                            }
                                        }
                                    }
                                    _ => {
                                        all_ok = false;
                                        (Status::UnknownCommand, vec![])
                                    }
                                }
                            } else {
                                // ── Normal document body lookup ────────────
                                match spec.opcode {
                                0xC5 => { // SubdocGet
                                    match bucket.subdoc_get("_default", "_default", &key, &spec.path) {
                                        Ok(v) => {
                                            let bytes = serde_json::to_vec(&v).unwrap_or_default();
                                            (Status::Success, bytes)
                                        }
                                        Err(NosqlError::SubdocPathNotFound(_)) => {
                                            all_ok = false;
                                            (Status::SubdocPathNotFound, vec![])
                                        }
                                        Err(_) => {
                                            all_ok = false;
                                            (Status::InternalError, vec![])
                                        }
                                    }
                                }
                                0xC6 => { // SubdocExists
                                    match bucket.subdoc_exists("_default", "_default", &key, &spec.path) {
                                        Ok(true) => (Status::Success, vec![]),
                                        Ok(false) => {
                                            all_ok = false;
                                            (Status::SubdocPathNotFound, vec![])
                                        }
                                        Err(_) => {
                                            all_ok = false;
                                            (Status::InternalError, vec![])
                                        }
                                    }
                                }
                                0xD2 => { // SubdocGetCount
                                    match bucket.subdoc_get_count("_default", "_default", &key, &spec.path) {
                                        Ok(count) => {
                                            let bytes = count.to_string().into_bytes();
                                            (Status::Success, bytes)
                                        }
                                        Err(NosqlError::SubdocPathNotFound(_)) => {
                                            all_ok = false;
                                            (Status::SubdocPathNotFound, vec![])
                                        }
                                        Err(_) => {
                                            all_ok = false;
                                            (Status::InternalError, vec![])
                                        }
                                    }
                                }
                                _ => {
                                    all_ok = false;
                                    (Status::UnknownCommand, vec![])
                                }
                            }
                            };

                            // Each spec result: 2 bytes status + 4 bytes value_len + value
                            response_value.extend_from_slice(&(status as u16).to_be_bytes());
                            response_value.extend_from_slice(&(value.len() as u32).to_be_bytes());
                            response_value.extend_from_slice(&value);
                        }

                        let overall_status = if all_ok {
                            Status::Success
                        } else {
                            Status::SubdocPathNotFound // SDK expects partial success
                        };

                        Response::new(Opcode::SubdocMultiLookup, overall_status, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_value(response_value)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(Opcode::SubdocMultiLookup, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => Response::error(Opcode::SubdocMultiLookup, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(Opcode::SubdocMultiLookup, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle SUBDOC_MULTI_MUTATION request
    pub fn handle_subdoc_multi_mutation(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(Opcode::SubdocMultiMutation, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        let cas = if req.header.cas != 0 { Some(req.header.cas) } else { None };
        let specs = parse_subdoc_mutation_specs(&req.value);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                let mut last_cas = 0u64;

                for (i, spec) in specs.iter().enumerate() {
                    let result = if spec.is_xattr() {
                        // ── XATTR mutation ────────────────────────
                        match spec.opcode {
                            0xC7 | 0xC8 => bucket.xattr_upsert("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                            0xC9 => bucket.xattr_remove("_default", "_default", &key, &spec.path, cas),
                            _ => bucket.xattr_upsert("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                        }
                    } else {
                        // ── Normal document body mutation ──────────
                        match spec.opcode {
                        0xC7 => bucket.subdoc_dict_add("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                        0xC8 => bucket.subdoc_dict_upsert("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                        0xC9 => bucket.subdoc_delete("_default", "_default", &key, &spec.path, cas),
                        0xCA => bucket.subdoc_replace("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                        0xCB => bucket.subdoc_array_push_last("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                        0xCC => bucket.subdoc_array_push_first("_default", "_default", &key, &spec.path, spec.value.clone(), cas),
                        0xCF => {
                            let delta = spec.value.as_i64().unwrap_or(1);
                            bucket.subdoc_counter("_default", "_default", &key, &spec.path, delta, cas)
                                .map(|(doc, _val)| doc)
                        }
                        _ => {
                            return Response::error(Opcode::SubdocMultiMutation, Status::UnknownCommand, req.header.opaque,
                                &format!("Unknown subdoc mutation opcode 0x{:02X}", spec.opcode));
                        }
                    }
                    };

                    match result {
                        Ok(doc) => {
                            last_cas = doc.cas;
                            // Buffer mutation for WAL
                            let needs_flush = self.storage.buffer_mutation(bucket_name, &doc);
                            if needs_flush { let _ = self.storage.flush_wal_buffer(); }
                            self.index_manager.on_document_upsert(bucket_name, &doc);
                            self.dcp_publish_upsert(bucket_name, &doc);
                        }
                        Err(NosqlError::SubdocPathNotFound(_)) => {
                            // Return index of failing spec
                            let mut val = Vec::new();
                            val.push(i as u8);
                            val.extend_from_slice(&(Status::SubdocPathNotFound as u16).to_be_bytes());
                            return Response::new(Opcode::SubdocMultiMutation, Status::SubdocPathNotFound, req.header.opaque)
                                .with_value(val);
                        }
                        Err(NosqlError::SubdocPathExists(_)) => {
                            let mut val = Vec::new();
                            val.push(i as u8);
                            val.extend_from_slice(&(Status::SubdocPathExists as u16).to_be_bytes());
                            return Response::new(Opcode::SubdocMultiMutation, Status::SubdocPathExists, req.header.opaque)
                                .with_value(val);
                        }
                        Err(NosqlError::DocumentNotFound(_)) => {
                            return Response::new(Opcode::SubdocMultiMutation, Status::KeyNotFound, req.header.opaque);
                        }
                        Err(NosqlError::CasMismatch { .. }) => {
                            return Response::new(Opcode::SubdocMultiMutation, Status::KeyExists, req.header.opaque);
                        }
                        Err(e) => {
                            return Response::error(Opcode::SubdocMultiMutation, Status::InternalError, req.header.opaque, &e.to_string());
                        }
                    }
                }

                Response::new(Opcode::SubdocMultiMutation, Status::Success, req.header.opaque)
                    .with_cas(last_cas)
            }
            Err(_) => Response::error(Opcode::SubdocMultiMutation, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle GAT (Get And Touch) request
    pub fn handle_gat(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();

        // GAT extras: 4 bytes expiry
        let expiry = if req.extras.len() >= 4 {
            u32::from_be_bytes([req.extras[0], req.extras[1], req.extras[2], req.extras[3]])
        } else {
            0
        };

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                // First touch
                let _ = bucket.touch("_default", "_default", &key, expiry as u64);
                // Then get
                match bucket.get("_default", "_default", &key) {
                    Ok(doc) if !doc.deleted && !doc.is_expired() => {
                        let value_bytes = serde_json::to_vec(&doc.value).unwrap_or_default();
                        let mut extras = Vec::with_capacity(4);
                        extras.extend_from_slice(&doc.flags.to_be_bytes());

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_extras(extras)
                            .with_value(value_bytes)
                            .with_datatype(0x01)
                    }
                    _ => Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }

    /// Handle GET_META request (returns metadata without the document value)
    pub fn handle_get_meta(&self, req: &Request, bucket_name: &str) -> Response {
        let key = req.key_str().to_string();
        if key.is_empty() {
            return Response::error(req.header.opcode, Status::InvalidArguments, req.header.opaque, "Empty key");
        }

        debug!("MC GET_META key={} bucket={}", key, bucket_name);

        match self.storage.get_bucket(bucket_name) {
            Ok(bucket) => {
                match bucket.get("_default", "_default", &key) {
                    Ok(doc) => {
                        if doc.deleted || doc.is_expired() {
                            return Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque);
                        }

                        // Return metadata in extras:
                        // 4 bytes: deleted (u32)
                        // 4 bytes: flags (u32)
                        // 4 bytes: expiry (u32)
                        // 8 bytes: seqno (u64)
                        let mut extras = Vec::with_capacity(22);
                        extras.extend_from_slice(&0u32.to_be_bytes()); // deleted = 0
                        extras.extend_from_slice(&doc.flags.to_be_bytes()); // flags
                        let expiry_secs: u32 = doc.expiry
                            .map(|e| e.timestamp() as u32)
                            .unwrap_or(0);
                        extras.extend_from_slice(&expiry_secs.to_be_bytes()); // expiry
                        extras.extend_from_slice(&doc.seq_no.to_be_bytes()); // seqno
                        extras.push(0x01); // datatype = JSON

                        Response::new(req.header.opcode, Status::Success, req.header.opaque)
                            .with_cas(doc.cas)
                            .with_extras(extras)
                    }
                    Err(NosqlError::DocumentNotFound(_)) => {
                        Response::new(req.header.opcode, Status::KeyNotFound, req.header.opaque)
                    }
                    Err(e) => Response::error(req.header.opcode, Status::InternalError, req.header.opaque, &e.to_string()),
                }
            }
            Err(_) => Response::error(req.header.opcode, Status::NoBucket, req.header.opaque, "Bucket not found"),
        }
    }
}
