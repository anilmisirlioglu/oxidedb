//! TLS/SSL support for OxideDB
//!
//! Provides TLS encrypted connections for both the HTTP REST API
//! and the Memcached binary protocol (Couchbase SDK compatible).
//!
//! Usage:
//!   --tls-enabled --tls-cert-path /path/to/cert.pem --tls-key-path /path/to/key.pem
//!
//! Generates self-signed certs for testing:
//!   openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

use rustls::ServerConfig;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;

/// Load TLS configuration from PEM certificate and key files.
/// Returns a `TlsAcceptor` that can be used to wrap TCP connections.
pub fn load_tls_config(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IoError> {
    // Read certificate chain
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| IoError::new(ErrorKind::NotFound, format!("TLS cert file '{}': {}", cert_path, e)))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(|r| r.ok())
        .collect();

    if certs.is_empty() {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("No certificates found in '{}'", cert_path),
        ));
    }

    // Read private key
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| IoError::new(ErrorKind::NotFound, format!("TLS key file '{}': {}", key_path, e)))?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| IoError::new(ErrorKind::InvalidData, format!("Failed to read TLS key: {}", e)))?
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, format!("No private key found in '{}'", key_path)))?;

    // Build rustls server config
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, rustls::pki_types::PrivateKeyDer::from(key))
        .map_err(|e| IoError::new(ErrorKind::InvalidData, format!("TLS config error: {}", e)))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A helper that provides both plain and TLS-wrapped TCP listeners.
/// The Memcached server and HTTP server use this to optionally encrypt connections.
pub struct TlsState {
    pub acceptor: Option<TlsAcceptor>,
    pub enabled: bool,
}

impl TlsState {
    pub fn new(acceptor: Option<TlsAcceptor>) -> Self {
        let enabled = acceptor.is_some();
        Self { acceptor, enabled }
    }

    pub fn disabled() -> Self {
        Self {
            acceptor: None,
            enabled: false,
        }
    }
}
