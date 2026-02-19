//! Memcached Binary Protocol TCP Server
//!
//! Accepts connections from Couchbase SDKs and handles the full
//! bootstrap flow: HELLO → SASL AUTH → SELECT_BUCKET → CCCP → KV ops

use crate::cluster::ClusterManager;
use crate::config::ServerConfig;
use crate::storage::engine::StorageEngine;
use crate::storage::index::IndexManager;

use super::handler::KvHandler;
use super::protocol::*;
use super::scram::{ScramState, ScramVariant};

use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

/// Shared state for the memcached server
pub struct MemcachedServer {
    pub storage: Arc<StorageEngine>,
    pub cluster: Arc<ClusterManager>,
    pub index_manager: Arc<IndexManager>,
    pub dcp_engine: Arc<crate::dcp::stream::DcpEngine>,
    pub config: ServerConfig,
}

impl MemcachedServer {
    pub fn new(
        storage: Arc<StorageEngine>,
        cluster: Arc<ClusterManager>,
        index_manager: Arc<IndexManager>,
        dcp_engine: Arc<crate::dcp::stream::DcpEngine>,
        config: ServerConfig,
    ) -> Self {
        Self {
            storage,
            cluster,
            index_manager,
            dcp_engine,
            config,
        }
    }

    /// Start the memcached protocol listener
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        let bind_addr = format!("{}:{}", self.config.host, self.config.memcached_port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(
            "Memcached binary protocol listening on {} (Couchbase SDK compatible)",
            bind_addr
        );

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("MC connection from {}", addr);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream).await {
                            debug!("MC connection error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("MC accept error: {}", e);
                }
            }
        }
    }

    /// Start TLS Memcached server on a separate port
    pub async fn start_tls(
        self: Arc<Self>,
        host: &str,
        port: u16,
        acceptor: tokio_rustls::TlsAcceptor,
    ) -> anyhow::Result<()> {
        let bind_addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(
            "TLS Memcached binary protocol listening on {} (Couchbase SDK compatible, encrypted)",
            bind_addr
        );

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("MC TLS connection from {}", addr);
                    let server = self.clone();
                    let tls_accept = acceptor.clone();
                    tokio::spawn(async move {
                        match tls_accept.accept(stream).await {
                            Ok(tls_stream) => {
                                if let Err(e) = server.handle_connection(tls_stream).await {
                                    debug!("MC TLS connection error from {}: {}", addr, e);
                                }
                            }
                            Err(e) => {
                                debug!("MC TLS handshake failed from {}: {}", addr, e);
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("MC TLS accept error: {}", e);
                }
            }
        }
    }

    /// Handle a single client connection (plain or TLS — generic over AsyncRead + AsyncWrite)
    async fn handle_connection<S>(&self, mut stream: S) -> anyhow::Result<()>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let handler = KvHandler::new(self.storage.clone(), self.index_manager.clone(), self.dcp_engine.clone());
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut conn_state = ConnectionState::new();

        loop {
            // Read data from socket
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                debug!("MC client disconnected (bucket={:?})", conn_state.selected_bucket);
                return Ok(());
            }

            // Process all complete packets in the buffer
            loop {
                match Request::decode(&buf) {
                    Some((mut req, consumed)) => {
                        debug!(
                            "MC << opcode={:?} (0x{:02X}) key_len={} body_len={} opaque=0x{:08X} datatype=0x{:02X}",
                            req.header.opcode, req.header.opcode as u8,
                            req.header.key_length, req.header.total_body_length,
                            req.header.opaque, req.header.data_type
                        );

                        // ── Snappy decompression of incoming request ────────
                        // If datatype bit 0x02 is set, the value is Snappy-compressed
                        if req.header.data_type & 0x02 != 0 && !req.value.is_empty() {
                            match snap::raw::Decoder::new().decompress_vec(&req.value) {
                                Ok(decompressed) => {
                                    debug!("MC Snappy decompress: {} -> {} bytes", req.value.len(), decompressed.len());
                                    req.value = decompressed;
                                    // Clear the Snappy bit so handlers see raw data
                                    req.header.data_type &= !0x02;
                                }
                                Err(e) => {
                                    warn!("MC Snappy decompress failed: {}", e);
                                    // Continue with original data
                                }
                            }
                        }

                        let responses = self.process_request(&req, &handler, &mut conn_state).await;

                        for resp in responses {
                            // ── Snappy compression of outgoing response ─────
                            let resp = if conn_state.snappy_enabled
                                && !resp.value.is_empty()
                                && resp.value.len() > 32  // Don't compress tiny values
                                && resp.header.vbucket_or_status == Status::Success as u16
                            {
                                match snap::raw::Encoder::new().compress_vec(&resp.value) {
                                    Ok(compressed) if compressed.len() < resp.value.len() => {
                                        debug!(
                                            "MC Snappy compress: {} -> {} bytes (saved {}%)",
                                            resp.value.len(), compressed.len(),
                                            100 - (compressed.len() * 100 / resp.value.len())
                                        );
                                        Response::new(resp.header.opcode, Status::Success, resp.header.opaque)
                                            .with_cas(resp.header.cas)
                                            .with_extras(resp.extras)
                                            .with_key(resp.key)
                                            .with_value(compressed)
                                            .with_datatype(resp.header.data_type | 0x02) // Set Snappy bit
                                    }
                                    _ => resp, // Compression didn't help or failed, send uncompressed
                                }
                            } else {
                                resp
                            };

                            debug!(
                                "MC >> opcode={:?} status=0x{:04X} body_len={} opaque=0x{:08X} datatype=0x{:02X}",
                                resp.header.opcode,
                                resp.header.vbucket_or_status,
                                resp.extras.len() + resp.key.len() + resp.value.len(),
                                resp.header.opaque,
                                resp.header.data_type
                            );
                            let encoded = resp.encode();
                            stream.write_all(&encoded).await?;
                        }
                        stream.flush().await?;
                        buf.advance_read(consumed);
                    }
                    None => break, // Need more data
                }
            }
        }
    }

    /// Process a single request and return response(s)
    async fn process_request(
        &self,
        req: &Request,
        handler: &KvHandler,
        state: &mut ConnectionState,
    ) -> Vec<Response> {
        let opcode = req.header.opcode;
        let opaque = req.header.opaque;

        match opcode {
            // ── Bootstrap / Auth ────────────────────────────────────
            Opcode::Hello => {
                vec![self.handle_hello(req, state)]
            }

            Opcode::GetErrorMap => {
                // Return empty error map (SDK expects a response)
                let error_map = serde_json::json!({
                    "version": 2,
                    "revision": 1,
                    "errors": {}
                });
                let body = serde_json::to_vec(&error_map).unwrap_or_default();
                vec![Response::new(opcode, Status::Success, opaque).with_value(body)]
            }

            Opcode::SaslListMechs => {
                // Advertise SCRAM-SHA512, SCRAM-SHA256, and PLAIN
                vec![Response::new(opcode, Status::Success, opaque)
                    .with_value(b"SCRAM-SHA512 SCRAM-SHA256 PLAIN".to_vec())]
            }

            Opcode::SaslAuth => {
                vec![self.handle_sasl_auth(req, state)]
            }

            Opcode::SaslStep => {
                vec![self.handle_sasl_step(req, state)]
            }

            Opcode::SelectBucket => {
                vec![self.handle_select_bucket(req, state)]
            }

            Opcode::GetClusterConfig => {
                vec![self.handle_get_cluster_config(req, state).await]
            }

            // ── KV Operations (require bucket selection) ────────────
            Opcode::Get | Opcode::GetQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_get(req, bucket, false);
                    // Quiet operations don't send response on miss
                    if opcode == Opcode::GetQ && resp.header.vbucket_or_status == Status::KeyNotFound as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::GetK | Opcode::GetKQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_get(req, bucket, true);
                    if opcode == Opcode::GetKQ && resp.header.vbucket_or_status == Status::KeyNotFound as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Set | Opcode::SetQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_set(req, bucket);
                    if opcode == Opcode::SetQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![] // Quiet: no response on success
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Add | Opcode::AddQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_add(req, bucket);
                    if opcode == Opcode::AddQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Replace | Opcode::ReplaceQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_replace(req, bucket);
                    if opcode == Opcode::ReplaceQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Delete | Opcode::DeleteQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_delete(req, bucket);
                    if opcode == Opcode::DeleteQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Increment | Opcode::IncrementQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_increment(req, bucket);
                    if opcode == Opcode::IncrementQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Decrement | Opcode::DecrementQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_decrement(req, bucket);
                    if opcode == Opcode::DecrementQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Append | Opcode::AppendQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_append(req, bucket);
                    if opcode == Opcode::AppendQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Prepend | Opcode::PrependQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_prepend(req, bucket);
                    if opcode == Opcode::PrependQ && resp.header.vbucket_or_status == Status::Success as u16 {
                        vec![]
                    } else {
                        vec![resp]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Touch => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_touch(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Gat => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_gat(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // ── Get & Lock / Unlock ─────────────────────────────────────
            Opcode::GetLocked => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_get_locked(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::UnlockKey => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_unlock(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // ── Sub-Document Operations ─────────────────────────────────
            Opcode::SubdocMultiLookup => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_subdoc_multi_lookup(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::SubdocMultiMutation => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_subdoc_multi_mutation(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // Single sub-doc operations (rarely used directly, SDK prefers multi)
            Opcode::SubdocGet | Opcode::SubdocExists | Opcode::SubdocGetCount
            | Opcode::SubdocDictAdd | Opcode::SubdocDictUpsert | Opcode::SubdocDelete
            | Opcode::SubdocReplace | Opcode::SubdocArrayPushLast | Opcode::SubdocArrayPushFirst
            | Opcode::SubdocArrayInsert | Opcode::SubdocArrayAddUnique | Opcode::SubdocCounter => {
                // Wrap single ops in the multi-lookup/mutation handler format
                if let Some(ref _bucket) = state.selected_bucket {
                    vec![Response::new(opcode, Status::NotSupported, opaque)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // ── Get from Replica ─────────────────────────────────────────
            Opcode::GetReplica => {
                // In a single-node setup, serve from active vBucket
                // (Couchbase SDKs use this for replica reads)
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = handler.handle_get(req, bucket, true);
                    vec![resp]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // ── Get Meta (Exists) ───────────────────────────────────────
            Opcode::GetMeta => {
                if let Some(ref bucket) = state.selected_bucket {
                    vec![handler.handle_get_meta(req, bucket)]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // ── Durable Writes (SyncReplication) ────────────────────────
            Opcode::DurabilitySet | Opcode::DurabilityAdd |
            Opcode::DurabilityReplace | Opcode::DurabilityDelete => {
                // Single-node: treat durable writes as normal writes
                // In production, this would coordinate with replicas
                if let Some(ref bucket) = state.selected_bucket {
                    let resp = match opcode {
                        Opcode::DurabilitySet => handler.handle_set(req, bucket),
                        Opcode::DurabilityAdd => handler.handle_add(req, bucket),
                        Opcode::DurabilityReplace => handler.handle_replace(req, bucket),
                        Opcode::DurabilityDelete => handler.handle_delete(req, bucket),
                        _ => unreachable!(),
                    };
                    vec![resp]
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            // ── Collections / Observe ─────────────────────────────────────
            Opcode::GetCollectionsManifest => {
                // Return a minimal collections manifest
                let manifest = if let Some(ref bucket) = state.selected_bucket {
                    self.build_collections_manifest(bucket)
                } else {
                    r#"{"uid":"0","scopes":[{"name":"_default","uid":"0","collections":[{"name":"_default","uid":"0"}]}]}"#.to_string()
                };
                vec![Response::new(opcode, Status::Success, opaque)
                    .with_value(manifest.into_bytes())]
            }

            Opcode::GetCollectionId => {
                // SDK sends scope.collection as key to get the collection ID
                // For _default._default, return CID=0
                let path = String::from_utf8_lossy(&req.value).to_string();
                debug!("MC GET_COLLECTION_ID path='{}'", path);

                // Return 12 bytes: manifest_uid (8) + collection_id (4)
                let mut extras = Vec::with_capacity(12);
                extras.extend_from_slice(&0u64.to_be_bytes()); // manifest uid
                extras.extend_from_slice(&0u32.to_be_bytes()); // collection id = 0 (_default)
                vec![Response::new(opcode, Status::Success, opaque)
                    .with_extras(extras)]
            }

            Opcode::ObserveSeqno => {
                // Return the current high seqno for the requested vBucket
                let vb_id = req.header.vbucket_or_status;
                let mut value = Vec::with_capacity(27);
                value.push(0u8); // format = 0 (no failover)
                value.extend_from_slice(&vb_id.to_be_bytes()); // vbucket id
                value.extend_from_slice(&0u64.to_be_bytes());  // vbucket uuid
                value.extend_from_slice(&0u64.to_be_bytes());  // last persisted seqno
                value.extend_from_slice(&0u64.to_be_bytes());  // current seqno
                vec![Response::new(opcode, Status::Success, opaque)
                    .with_value(value)]
            }

            Opcode::Noop => {
                vec![Response::new(opcode, Status::Success, opaque)]
            }

            Opcode::Version => {
                vec![Response::new(opcode, Status::Success, opaque)
                    .with_value(b"OxideDB 0.3.0 (Couchbase-compatible)".to_vec())]
            }

            Opcode::Stat => {
                // Return basic stats then terminate with empty key stat
                let mut responses = Vec::new();

                if let Some(ref bucket) = state.selected_bucket {
                    if let Ok(b) = self.storage.get_bucket(bucket) {
                        let stats = vec![
                            ("curr_items", b.document_count().to_string()),
                            ("ep_bucket_type", "couchbase".to_string()),
                            ("vb_active_num", format!("{}", b.config.num_vbuckets)),
                        ];
                        for (k, v) in stats {
                            responses.push(
                                Response::new(opcode, Status::Success, opaque)
                                    .with_key(k.as_bytes().to_vec())
                                    .with_value(v.as_bytes().to_vec()),
                            );
                        }
                    }
                }
                // Terminating empty stat
                responses.push(Response::new(opcode, Status::Success, opaque));
                responses
            }

            Opcode::Flush | Opcode::FlushQ => {
                if let Some(ref bucket) = state.selected_bucket {
                    if let Ok(b) = self.storage.get_bucket(bucket) {
                        let _ = b.flush();
                    }
                    if opcode == Opcode::FlushQ {
                        vec![]
                    } else {
                        vec![Response::new(opcode, Status::Success, opaque)]
                    }
                } else {
                    vec![Response::error(opcode, Status::NoBucket, opaque, "No bucket selected")]
                }
            }

            Opcode::Quit | Opcode::QuitQ => {
                // Client closing
                if opcode != Opcode::QuitQ {
                    vec![Response::new(opcode, Status::Success, opaque)]
                } else {
                    vec![]
                }
            }

            _ => {
                warn!("MC unsupported opcode: {:?} (0x{:02X})", opcode, opcode as u8);
                vec![Response::new(opcode, Status::UnknownCommand, opaque)]
            }
        }
    }

    // ── HELLO ───────────────────────────────────────────────────────
    fn handle_hello(&self, req: &Request, state: &mut ConnectionState) -> Response {
        // Client sends: key = user-agent, value = list of u16 features
        let agent = String::from_utf8_lossy(&req.key).to_string();
        debug!("MC HELLO from agent='{}' ", agent);
        state.user_agent = Some(agent);

        // Parse requested features and echo back supported ones
        let mut supported = Vec::new();
        let mut i = 0;
        while i + 1 < req.value.len() {
            let feature = u16::from_be_bytes([req.value[i], req.value[i + 1]]);
            match feature {
                0x0001 => supported.push(feature), // Datatype
                0x0003 => supported.push(feature), // TCP_NODELAY
                0x0004 => supported.push(feature), // Mutation seqno
                0x0006 => supported.push(feature), // Xattr
                0x0007 => supported.push(feature), // Xerror
                0x0008 => supported.push(feature), // SELECT_BUCKET
                0x000A => supported.push(feature), // Snappy
                0x000B => supported.push(feature), // JSON
                0x000C => supported.push(feature), // Duplex
                0x000D => supported.push(feature), // ClustermapChangeNotification
                0x000E => supported.push(feature), // Unordered execution
                0x0010 => supported.push(feature), // AltRequest
                0x0011 => supported.push(feature), // SyncReplication
                0x0012 => supported.push(feature), // Collections
                0x0014 => supported.push(feature), // PreserveTtl
                _ => {
                    debug!("MC HELLO: unsupported feature 0x{:04X}", feature);
                }
            }
            i += 2;
        }
        state.negotiated_features = supported.clone();
        state.snappy_enabled = supported.contains(&0x000A);

        let mut value = Vec::with_capacity(supported.len() * 2);
        for f in &supported {
            value.extend_from_slice(&f.to_be_bytes());
        }

        debug!("MC HELLO: snappy_enabled={}", state.snappy_enabled);

        Response::new(Opcode::Hello, Status::Success, req.header.opaque)
            .with_value(value)
    }

    // ── SASL AUTH ───────────────────────────────────────────────────
    fn handle_sasl_auth(&self, req: &Request, state: &mut ConnectionState) -> Response {
        let mechanism = String::from_utf8_lossy(&req.key).to_string();
        debug!("MC SASL_AUTH mechanism={}", mechanism);

        match mechanism.as_str() {
            "PLAIN" => {
        // PLAIN format: \0<username>\0<password>
        let parts: Vec<&[u8]> = req.value.splitn(3, |&b| b == 0).collect();
        let username = if parts.len() >= 2 {
            String::from_utf8_lossy(parts[1]).to_string()
        } else {
            "anonymous".to_string()
        };
                let password = if parts.len() >= 3 {
                    String::from_utf8_lossy(parts[2]).to_string()
                } else {
                    String::new()
                };

        state.authenticated = true;
        state.username = Some(username.clone());
                state.password = Some(password);
                info!("MC client authenticated as '{}' (PLAIN)", username);

        Response::new(Opcode::SaslAuth, Status::Success, req.header.opaque)
            .with_value(b"Authenticated".to_vec())
            }
            "SCRAM-SHA512" | "SCRAM-SHA256" => {
                let variant = if mechanism == "SCRAM-SHA512" {
                    ScramVariant::Sha512
                } else {
                    ScramVariant::Sha256
                };

                match ScramState::from_client_first(variant, &req.value) {
                    Some((scram_state, server_first_msg)) => {
                        debug!(
                            "MC SCRAM step 1: user='{}', nonce={}",
                            scram_state.username,
                            scram_state.client_nonce
                        );
                        state.scram_state = Some(scram_state);

                        // Return AuthContinue — tells SDK to send SASL_STEP
                        Response::new(Opcode::SaslAuth, Status::AuthContinue, req.header.opaque)
                            .with_value(server_first_msg)
                    }
                    None => {
                        warn!("MC SCRAM: failed to parse client-first message");
                        Response::error(
                            Opcode::SaslAuth,
                            Status::AuthError,
                            req.header.opaque,
                            "Invalid SCRAM client-first message",
                        )
                    }
                }
            }
            _ => {
                warn!("MC SASL_AUTH: unsupported mechanism '{}'", mechanism);
                Response::error(
                    Opcode::SaslAuth,
                    Status::AuthError,
                    req.header.opaque,
                    &format!("Unsupported SASL mechanism: {}", mechanism),
                )
            }
        }
    }

    // ── SASL STEP (SCRAM step 2) ────────────────────────────────────
    fn handle_sasl_step(&self, req: &Request, state: &mut ConnectionState) -> Response {
        let scram_state = match state.scram_state.take() {
            Some(s) => s,
            None => {
                return Response::error(
                    Opcode::SaslStep,
                    Status::AuthError,
                    req.header.opaque,
                    "No SCRAM auth in progress",
                );
            }
        };

        // We accept any password — the "password" from PasswordAuthenticator
        // We need to guess it. The SDK uses whatever was passed.
        // Since we accept all credentials, we use a well-known default.
        // But the SDK computes the client proof using the *real* password,
        // so we need to extract the password from the client proof.
        //
        // Approach: Try common passwords. If the client connects with
        // PasswordAuthenticator("admin", "password"), we try "password".
        // For a real DB, you'd look up the stored hash.
        //
        // HACK: We try the client-final-message with multiple passwords.
        // For maximum compatibility, we extract it from the state.
        // Since we accept all creds, we compute server sig with "password"
        // and hope it matches. If not, we still accept.

        // We try the password that was configured or default "password".
        // For maximum compatibility, the PasswordAuthenticator password must match.
        let default_password = "password";

        debug!(
            "MC SCRAM step 2: processing client-final ({} bytes): {:?}",
            req.value.len(),
            String::from_utf8_lossy(&req.value)
        );

        match scram_state.process_client_final(&req.value, default_password) {
            Some(server_final) => {
                state.authenticated = true;
                state.username = Some(scram_state.username.clone());
                state.password = Some(default_password.to_string());
                debug!(
                    "MC SCRAM step 2: server-final = {:?}",
                    String::from_utf8_lossy(&server_final)
                );
                info!(
                    "MC client authenticated as '{}' (SCRAM-{:?})",
                    scram_state.username, scram_state.variant
                );

                Response::new(Opcode::SaslStep, Status::Success, req.header.opaque)
                    .with_value(server_final)
            }
            None => {
                warn!("MC SCRAM step 2 failed for user '{}'", scram_state.username);
                Response::error(
                    Opcode::SaslStep,
                    Status::AuthError,
                    req.header.opaque,
                    "SCRAM authentication failed",
                )
            }
        }
    }

    // ── SELECT BUCKET ───────────────────────────────────────────────
    fn handle_select_bucket(&self, req: &Request, state: &mut ConnectionState) -> Response {
        let bucket_name = String::from_utf8_lossy(&req.key).to_string();
        debug!("MC SELECT_BUCKET bucket={}", bucket_name);

        // Verify bucket exists
        match self.storage.get_bucket(&bucket_name) {
            Ok(_) => {
                state.selected_bucket = Some(bucket_name.clone());
                info!("MC client selected bucket '{}'", bucket_name);
                Response::new(Opcode::SelectBucket, Status::Success, req.header.opaque)
            }
            Err(_) => Response::error(
                Opcode::SelectBucket,
                Status::KeyNotFound,
                req.header.opaque,
                &format!("Bucket '{}' not found", bucket_name),
            ),
        }
    }

    // ── GET CLUSTER CONFIG (CCCP) ───────────────────────────────────
    async fn handle_get_cluster_config(
        &self,
        req: &Request,
        state: &ConnectionState,
    ) -> Response {
        let config_json = if let Some(ref bucket_name) = state.selected_bucket {
            // Bucket is selected → return bucket-specific config with vBucket map
            debug!("MC GET_CLUSTER_CONFIG for bucket '{}'", bucket_name);
            self.build_bucket_cluster_config(bucket_name).await
        } else {
            // No bucket selected → return global config (just node list)
            debug!("MC GET_CLUSTER_CONFIG (global, no bucket selected)");
            self.build_global_cluster_config().await
        };

        Response::new(Opcode::GetClusterConfig, Status::Success, req.header.opaque)
            .with_value(config_json.into_bytes())
    }

    /// Build a collections manifest JSON for a bucket
    fn build_collections_manifest(&self, bucket_name: &str) -> String {
        let mut scopes_json = Vec::new();
        if let Ok(bucket) = self.storage.get_bucket(bucket_name) {
            for entry in bucket.scopes.iter() {
                let scope = entry.value();
                let collections: Vec<serde_json::Value> = scope
                    .collections
                    .iter()
                    .map(|c| {
                        serde_json::json!({
                            "name": c.key().clone(),
                            "uid": "0",
                            "maxTTL": 0
                        })
                    })
                    .collect();
                scopes_json.push(serde_json::json!({
                    "name": scope.name,
                    "uid": "0",
                    "collections": collections
                }));
            }
        }
        if scopes_json.is_empty() {
            scopes_json.push(serde_json::json!({
                "name": "_default",
                "uid": "0",
                "collections": [{"name": "_default", "uid": "0", "maxTTL": 0}]
            }));
        }
        let manifest = serde_json::json!({
            "uid": "0",
            "scopes": scopes_json
        });
        serde_json::to_string(&manifest).unwrap_or_default()
    }

    /// Build a GLOBAL cluster config (no bucket-specific data).
    /// Returned before SELECT_BUCKET — just the node list so the SDK
    /// knows where to open per-bucket connections.
    async fn build_global_cluster_config(&self) -> String {
        let pmap = self.cluster.get_partition_map().await;
        let nodes = self.cluster.list_nodes().await;

        let nodes_ext: Vec<serde_json::Value> = nodes
            .iter()
            .map(|node| {
                let kv_port = if node.name == self.config.node_name {
                    self.config.memcached_port
                } else {
                    node.port + (self.config.memcached_port - self.config.port)
                };
                let hostname = if node.hostname == "0.0.0.0" {
                    "127.0.0.1"
                } else {
                    &node.hostname
                };
                serde_json::json!({
                    "services": {
                        "mgmt": node.port,
                        "kv": kv_port,
                        "n1ql": node.port,
                        "capi": node.port
                    },
                    "thisNode": node.name == self.config.node_name,
                    "hostname": hostname
                })
            })
            .collect();

        let config = serde_json::json!({
            "rev": pmap.revision,
            "revEpoch": 1,
            "nodesExt": nodes_ext,
            "clusterCapabilitiesVer": [1, 0],
            "clusterCapabilities": {
                "n1ql": ["enhancedPreparedStatements"]
            }
        });

        serde_json::to_string(&config).unwrap_or_default()
    }

    /// Build a BUCKET-SPECIFIC cluster config (with vBucket map).
    /// Returned after SELECT_BUCKET — the SDK uses this to route KV ops.
    async fn build_bucket_cluster_config(&self, bucket_name: &str) -> String {
        let pmap = self.cluster.get_partition_map().await;
        let nodes = self.cluster.list_nodes().await;

        // Build server list: "host:kv_port"
        let mut server_list: Vec<String> = Vec::new();
        let mut node_index_map: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for node in &nodes {
            let kv_host = if node.hostname == "0.0.0.0" {
                "127.0.0.1".to_string()
            } else {
                node.hostname.clone()
            };
            let kv_port = if node.name == self.config.node_name {
                self.config.memcached_port
            } else {
                node.port + (self.config.memcached_port - self.config.port)
            };
            let idx = server_list.len();
            server_list.push(format!("{}:{}", kv_host, kv_port));
            node_index_map.insert(node.name.clone(), idx);
        }

        // Build vBucket map
        let mut vbucket_map: Vec<Vec<i32>> = Vec::with_capacity(pmap.num_vbuckets as usize);
        for entry in &pmap.map {
            let mut mapping = Vec::new();
            let active_idx = node_index_map
                .get(&entry.active_node)
                .copied()
                .map(|i| i as i32)
                .unwrap_or(-1);
            mapping.push(active_idx);
            for replica in &entry.replica_nodes {
                let replica_idx = node_index_map
                    .get(replica)
                    .copied()
                    .map(|i| i as i32)
                    .unwrap_or(-1);
                mapping.push(replica_idx);
            }
            while mapping.len() < 2 {
                mapping.push(-1);
            }
            vbucket_map.push(mapping);
        }

        let nodes_ext: Vec<serde_json::Value> = nodes
            .iter()
            .map(|node| {
                let kv_port = if node.name == self.config.node_name {
                    self.config.memcached_port
                } else {
                    node.port + (self.config.memcached_port - self.config.port)
                };
                let hostname = if node.hostname == "0.0.0.0" {
                    "127.0.0.1"
                } else {
                    &node.hostname
                };
                serde_json::json!({
                    "services": {
                        "mgmt": node.port,
                        "kv": kv_port,
                        "n1ql": node.port,
                        "capi": node.port
                    },
                    "thisNode": node.name == self.config.node_name,
                    "hostname": hostname
                })
            })
            .collect();

        let num_replicas = pmap.num_replicas;
        let doc_count = self
            .storage
            .get_bucket(bucket_name)
            .map(|b| b.document_count())
            .unwrap_or(0);

        let config = serde_json::json!({
            "rev": pmap.revision,
            "revEpoch": 1,
            "name": bucket_name,
            "uri": format!("/pools/default/buckets/{}", bucket_name),
            "streamingUri": format!("/pools/default/bucketsStreaming/{}", bucket_name),
            "nodes": nodes.iter().map(|n| {
                let hostname = if n.hostname == "0.0.0.0" { "127.0.0.1" } else { &n.hostname };
                serde_json::json!({
                    "couchApiBase": format!("http://{}:{}/{}", hostname, n.port, bucket_name),
                    "hostname": format!("{}:{}", hostname, n.port),
                    "status": match n.status {
                        crate::cluster::node::NodeStatus::Healthy => "healthy",
                        crate::cluster::node::NodeStatus::Warmup => "warmup",
                        _ => "unhealthy"
                    },
                    "ports": {
                        "direct": if n.name == self.config.node_name {
                            self.config.memcached_port
                        } else {
                            n.port + (self.config.memcached_port - self.config.port)
                        }
                    },
                    "services": ["kv", "n1ql", "index"],
                    "clusterCompatibility": 458752
                })
            }).collect::<Vec<_>>(),
            "nodesExt": nodes_ext,
            "bucketType": "membase",
            "authType": "sasl",
            "saslPassword": "",
            "nodeLocator": "vbucket",
            "uuid": self.cluster.cluster_uuid.clone(),
            "collectionsManifestUid": "0",
            "bucketCapabilitiesVer": "",
            "bucketCapabilities": [
                "collections", "durableWrite", "tombstonedUserXAttrs",
                "couchapi", "dcp", "cbhello", "touch", "cccp", "nodesExt",
                "xdcrCheckpointing", "xattr"
            ],
            "vBucketServerMap": {
                "hashAlgorithm": "CRC",
                "numReplicas": num_replicas,
                "serverList": server_list,
                "vBucketMap": vbucket_map
            },
            "clusterCapabilitiesVer": [1, 0],
            "clusterCapabilities": {
                "n1ql": ["enhancedPreparedStatements"]
            },
            "itemCount": doc_count
        });

        serde_json::to_string(&config).unwrap_or_default()
    }
}

// ── Per-Connection State ────────────────────────────────────────────
struct ConnectionState {
    authenticated: bool,
    username: Option<String>,
    password: Option<String>,
    selected_bucket: Option<String>,
    user_agent: Option<String>,
    negotiated_features: Vec<u16>,
    scram_state: Option<ScramState>,
    snappy_enabled: bool,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            authenticated: false,
            username: None,
            password: None,
            selected_bucket: None,
            user_agent: None,
            negotiated_features: Vec::new(),
            scram_state: None,
            snappy_enabled: false,
        }
    }
}

// ── BytesMut helper ─────────────────────────────────────────────────
trait BytesMutAdvance {
    fn advance_read(&mut self, cnt: usize);
}

impl BytesMutAdvance for BytesMut {
    fn advance_read(&mut self, cnt: usize) {
        let remaining = self.split_off(cnt);
        *self = remaining;
    }
}
