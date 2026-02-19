//! SCRAM-SHA512 / SCRAM-SHA256 Authentication
//!
//! Implements RFC 5802 (SCRAM) for Couchbase SDK compatibility.
//! The SDKs prefer SCRAM-SHA512 > SCRAM-SHA256 > PLAIN.
//!
//! We accept any username/password combination (no RBAC enforcement),
//! but we must correctly implement the SCRAM protocol because the SDK
//! verifies the server's signature.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha512};

type HmacSha512 = Hmac<Sha512>;
type HmacSha256 = Hmac<Sha256>;

/// Which SCRAM variant
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramVariant {
    Sha512,
    Sha256,
}

/// Per-connection SCRAM state — lives only during the auth handshake.
#[derive(Debug, Clone)]
pub struct ScramState {
    pub variant: ScramVariant,
    /// Raw "client-first-message-bare": `n=<user>,r=<client_nonce>`
    pub client_first_bare: String,
    /// Combined nonce: client_nonce + server_nonce
    pub combined_nonce: String,
    /// Salt (random bytes, base64 encoded for wire)
    pub salt: Vec<u8>,
    /// Iteration count
    pub iterations: u32,
    /// The server-first-message we sent
    pub server_first: String,
    /// The username the client claimed
    pub username: String,
    /// The client nonce
    pub client_nonce: String,
}

impl ScramState {
    /// Parse the client-first-message and produce the server-first-message.
    ///
    /// Client sends:  `n,,n=<user>,r=<client_nonce>`
    /// We respond:    `r=<combined_nonce>,s=<salt_b64>,i=<iterations>`
    pub fn from_client_first(variant: ScramVariant, client_msg: &[u8]) -> Option<(Self, Vec<u8>)> {
        let msg = std::str::from_utf8(client_msg).ok()?;

        // Strip GS2 header: "n,," (or "n,a=...,")
        let bare = if msg.starts_with("n,,") {
            &msg[3..]
        } else if msg.starts_with("n,") {
            // n,a=<authzid>,<rest>
            let comma_pos = msg[2..].find(',')? + 2;
            &msg[comma_pos + 1..]
        } else {
            return None;
        };

        // Parse bare: "n=<user>,r=<client_nonce>"
        let mut username = String::new();
        let mut client_nonce = String::new();
        for part in bare.split(',') {
            if part.starts_with("n=") {
                username = part[2..].to_string();
            } else if part.starts_with("r=") {
                client_nonce = part[2..].to_string();
            }
        }

        if username.is_empty() || client_nonce.is_empty() {
            return None;
        }

        // Generate server nonce and salt
        let server_nonce = generate_nonce();
        let combined_nonce = format!("{}{}", client_nonce, server_nonce);
        let salt = generate_salt();
        let iterations: u32 = 4096;

        let salt_b64 = B64.encode(&salt);

        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce, salt_b64, iterations
        );

        let state = ScramState {
            variant,
            client_first_bare: bare.to_string(),
            combined_nonce,
            salt,
            iterations,
            server_first: server_first.clone(),
            username,
            client_nonce,
        };

        Some((state, server_first.into_bytes()))
    }

    /// Process the client-final-message and produce server-final.
    ///
    /// Client sends:  `c=<channel_binding_b64>,r=<combined_nonce>,p=<client_proof_b64>`
    /// We respond:    `v=<server_signature_b64>`
    ///
    /// We accept any password since we don't enforce auth. We extract the password
    /// from the proof by reversing the SCRAM math with a fixed "password" assumption.
    /// Actually, since we accept all credentials, we compute the correct server
    /// signature for the claimed credentials to satisfy the SDK's verification.
    pub fn process_client_final(&self, client_msg: &[u8], password: &str) -> Option<Vec<u8>> {
        let msg = std::str::from_utf8(client_msg).ok()?;

        // Parse: "c=<cb>,r=<nonce>,p=<proof>"
        let mut _channel_binding = String::new();
        let mut nonce = String::new();
        let mut _client_proof_b64 = String::new();

        // The client-final-without-proof is everything except ",p=..."
        let proof_pos = msg.rfind(",p=")?;
        let client_final_without_proof = &msg[..proof_pos];

        for part in msg.split(',') {
            if part.starts_with("c=") {
                _channel_binding = part[2..].to_string();
            } else if part.starts_with("r=") {
                nonce = part[2..].to_string();
            } else if part.starts_with("p=") {
                _client_proof_b64 = part[2..].to_string();
            }
        }

        // Verify nonce matches
        if nonce != self.combined_nonce {
            return None;
        }

        // Compute the auth message
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare,
            self.server_first,
            client_final_without_proof
        );

        // Compute server signature based on variant
        let server_signature = match self.variant {
            ScramVariant::Sha512 => {
                compute_server_signature_sha512(
                    password,
                    &self.salt,
                    self.iterations,
                    &auth_message,
                )
            }
            ScramVariant::Sha256 => {
                compute_server_signature_sha256(
                    password,
                    &self.salt,
                    self.iterations,
                    &auth_message,
                )
            }
        };

        let server_final = format!("v={}", B64.encode(&server_signature));
        Some(server_final.into_bytes())
    }
}

// ── SCRAM-SHA-512 Crypto ─────────────────────────────────────────

fn compute_server_signature_sha512(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Vec<u8> {
    // SaltedPassword = Hi(password, salt, iterations)
    let salted_password = pbkdf2_hmac_sha512(password.as_bytes(), salt, iterations);

    // ServerKey = HMAC(SaltedPassword, "Server Key")
    let server_key = hmac_sha512(&salted_password, b"Server Key");

    // ServerSignature = HMAC(ServerKey, AuthMessage)
    hmac_sha512(&server_key, auth_message.as_bytes())
}

fn compute_server_signature_sha256(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Vec<u8> {
    let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
    let server_key = hmac_sha256(&salted_password, b"Server Key");
    hmac_sha256(&server_key, auth_message.as_bytes())
}

fn pbkdf2_hmac_sha512(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut result = vec![0u8; 64]; // SHA-512 output = 64 bytes
    pbkdf2::pbkdf2::<HmacSha512>(password, salt, iterations, &mut result)
        .expect("PBKDF2-HMAC-SHA512 failed");
    result
}

fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut result = vec![0u8; 32]; // SHA-256 output = 32 bytes
    pbkdf2::pbkdf2::<HmacSha256>(password, salt, iterations, &mut result)
        .expect("PBKDF2-HMAC-SHA256 failed");
    result
}

fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha512::new_from_slice(key).expect("HMAC-SHA512 init failed");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 init failed");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..24).map(|_| rng.gen()).collect();
    B64.encode(&bytes)
}

fn generate_salt() -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..16).map(|_| rng.gen()).collect()
}
