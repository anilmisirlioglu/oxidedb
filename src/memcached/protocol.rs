//! Couchbase Memcached Binary Protocol
//!
//! Implements the binary protocol used by Couchbase SDKs for KV operations.
//! Packet format: 24-byte header + extras + key + value
//!
//! Reference: https://github.com/couchbase/memcached/blob/master/docs/BinaryProtocol.md

use bytes::{Buf, BufMut, BytesMut};

// ── Magic bytes ─────────────────────────────────────────────────────
#[allow(dead_code)]
pub const MAGIC_REQUEST: u8 = 0x80;
pub const MAGIC_RESPONSE: u8 = 0x81;

// ── Opcodes ─────────────────────────────────────────────────────────
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)]
pub enum Opcode {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Increment = 0x05,
    Decrement = 0x06,
    Quit = 0x07,
    Flush = 0x08,
    GetQ = 0x09,        // Quiet GET
    Noop = 0x0A,
    Version = 0x0B,
    GetK = 0x0C,        // GET with key
    GetKQ = 0x0D,       // Quiet GET with key
    Append = 0x0E,
    Prepend = 0x0F,
    Stat = 0x10,
    SetQ = 0x11,
    AddQ = 0x12,
    ReplaceQ = 0x13,
    DeleteQ = 0x14,
    IncrementQ = 0x15,
    DecrementQ = 0x16,
    QuitQ = 0x17,
    FlushQ = 0x18,
    AppendQ = 0x19,
    PrependQ = 0x1A,
    Touch = 0x1C,
    Gat = 0x1D,         // GET and TOUCH
    Hello = 0x1F,
    SaslListMechs = 0x20,
    SaslAuth = 0x21,
    SaslStep = 0x22,
    SelectBucket = 0x89,
    GetLocked = 0x94,
    UnlockKey = 0x95,
    ObserveSeqno = 0x91,
    // Sub-document opcodes
    SubdocGet = 0xC5,
    SubdocExists = 0xC6,
    SubdocDictAdd = 0xC7,
    SubdocDictUpsert = 0xC8,
    SubdocDelete = 0xC9,
    SubdocReplace = 0xCA,
    SubdocArrayPushLast = 0xCB,
    SubdocArrayPushFirst = 0xCC,
    SubdocArrayInsert = 0xCD,
    SubdocArrayAddUnique = 0xCE,
    SubdocCounter = 0xCF,
    SubdocMultiLookup = 0xD0,
    SubdocMultiMutation = 0xD1,
    SubdocGetCount = 0xD2,
    GetReplica = 0x83,
    GetMeta = 0xA0,
    GetCollectionsManifest = 0xBA,
    GetCollectionId = 0xBB,
    GetClusterConfig = 0xB5,
    GetErrorMap = 0xFE,
    // Durability-related
    DurabilitySet = 0x35,       // Set with durability
    DurabilityAdd = 0x36,       // Add with durability
    DurabilityReplace = 0x37,   // Replace with durability
    DurabilityDelete = 0x38,    // Delete with durability
    Unknown = 0xFF,
}

impl From<u8> for Opcode {
    fn from(v: u8) -> Self {
        match v {
            0x00 => Opcode::Get,
            0x01 => Opcode::Set,
            0x02 => Opcode::Add,
            0x03 => Opcode::Replace,
            0x04 => Opcode::Delete,
            0x05 => Opcode::Increment,
            0x06 => Opcode::Decrement,
            0x07 => Opcode::Quit,
            0x08 => Opcode::Flush,
            0x09 => Opcode::GetQ,
            0x0A => Opcode::Noop,
            0x0B => Opcode::Version,
            0x0C => Opcode::GetK,
            0x0D => Opcode::GetKQ,
            0x0E => Opcode::Append,
            0x0F => Opcode::Prepend,
            0x10 => Opcode::Stat,
            0x11 => Opcode::SetQ,
            0x12 => Opcode::AddQ,
            0x13 => Opcode::ReplaceQ,
            0x14 => Opcode::DeleteQ,
            0x15 => Opcode::IncrementQ,
            0x16 => Opcode::DecrementQ,
            0x17 => Opcode::QuitQ,
            0x18 => Opcode::FlushQ,
            0x19 => Opcode::AppendQ,
            0x1A => Opcode::PrependQ,
            0x1C => Opcode::Touch,
            0x1D => Opcode::Gat,
            0x1F => Opcode::Hello,
            0x20 => Opcode::SaslListMechs,
            0x21 => Opcode::SaslAuth,
            0x22 => Opcode::SaslStep,
            0x89 => Opcode::SelectBucket,
            0x91 => Opcode::ObserveSeqno,
            0x94 => Opcode::GetLocked,
            0x95 => Opcode::UnlockKey,
            0xC5 => Opcode::SubdocGet,
            0xC6 => Opcode::SubdocExists,
            0xC7 => Opcode::SubdocDictAdd,
            0xC8 => Opcode::SubdocDictUpsert,
            0xC9 => Opcode::SubdocDelete,
            0xCA => Opcode::SubdocReplace,
            0xCB => Opcode::SubdocArrayPushLast,
            0xCC => Opcode::SubdocArrayPushFirst,
            0xCD => Opcode::SubdocArrayInsert,
            0xCE => Opcode::SubdocArrayAddUnique,
            0xCF => Opcode::SubdocCounter,
            0xD0 => Opcode::SubdocMultiLookup,
            0xD1 => Opcode::SubdocMultiMutation,
            0xD2 => Opcode::SubdocGetCount,
            0x83 => Opcode::GetReplica,
            0xA0 => Opcode::GetMeta,
            0xBA => Opcode::GetCollectionsManifest,
            0xBB => Opcode::GetCollectionId,
            0xB5 => Opcode::GetClusterConfig,
            0xFE => Opcode::GetErrorMap,
            0x35 => Opcode::DurabilitySet,
            0x36 => Opcode::DurabilityAdd,
            0x37 => Opcode::DurabilityReplace,
            0x38 => Opcode::DurabilityDelete,
            _ => Opcode::Unknown,
        }
    }
}

// ── Status codes ────────────────────────────────────────────────────
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
#[allow(dead_code)]
pub enum Status {
    Success = 0x0000,
    KeyNotFound = 0x0001,
    KeyExists = 0x0002,
    ValueTooLarge = 0x0003,
    InvalidArguments = 0x0004,
    ItemNotStored = 0x0005,
    IncrDecrNonNumeric = 0x0006,
    NotMyVbucket = 0x0007,
    AuthError = 0x0020,
    AuthContinue = 0x0021,
    InvalidRange = 0x0022,
    UnknownCommand = 0x0081,
    OutOfMemory = 0x0082,
    NotSupported = 0x0083,
    InternalError = 0x0084,
    Busy = 0x0085,
    TmpFail = 0x0086,
    NoBucket = 0x08,
    Locked = 0x0009,
    DurabilityInvalidLevel = 0x00A0,
    DurabilityImpossible = 0x00A1,
    SyncWriteInProgress = 0x00A2,
    SyncWriteAmbiguous = 0x00A3,
    // Sub-document status codes
    SubdocPathNotFound = 0x00C0,
    SubdocPathMismatch = 0x00C1,
    SubdocPathInvalid = 0x00C3,
    SubdocPathExists = 0x00C8,
}

// ── HELLO Features ──────────────────────────────────────────────────
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
#[allow(dead_code)]
pub enum HelloFeature {
    Datatype = 0x0001,
    Tls = 0x0002,
    TcpNodelay = 0x0003,
    MutationSeqno = 0x0004,
    TcpDelay = 0x0005,
    Xattr = 0x0006,
    Xerror = 0x0007,
    SelectBucket = 0x0008,
    Snappy = 0x000A,
    Json = 0x000B,
    Duplex = 0x000C,
    ClustermapNotif = 0x000D,
    UnorderedExec = 0x000E,
    AltRequest = 0x0010,
    SyncReplication = 0x0011,
    Collections = 0x0012,
    PreserveTtl = 0x0014,
}

// ── Packet Header (24 bytes) ────────────────────────────────────────
pub const HEADER_SIZE: usize = 24;

#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub magic: u8,
    pub opcode: Opcode,
    pub key_length: u16,
    pub extras_length: u8,
    pub data_type: u8,
    pub vbucket_or_status: u16,  // vbucket for request, status for response
    pub total_body_length: u32,
    pub opaque: u32,
    pub cas: u64,
}

impl PacketHeader {
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        let mut b = &buf[..HEADER_SIZE];
        let magic = b.get_u8();
        let opcode_byte = b.get_u8();
        let key_length = b.get_u16();
        let extras_length = b.get_u8();
        let data_type = b.get_u8();
        let vbucket_or_status = b.get_u16();
        let total_body_length = b.get_u32();
        let opaque = b.get_u32();
        let cas = b.get_u64();

        Some(PacketHeader {
            magic,
            opcode: Opcode::from(opcode_byte),
            key_length,
            extras_length,
            data_type,
            vbucket_or_status,
            total_body_length,
            opaque,
            cas,
        })
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.magic);
        buf.put_u8(self.opcode as u8);
        buf.put_u16(self.key_length);
        buf.put_u8(self.extras_length);
        buf.put_u8(self.data_type);
        buf.put_u16(self.vbucket_or_status);
        buf.put_u32(self.total_body_length);
        buf.put_u32(self.opaque);
        buf.put_u64(self.cas);
    }
}

// ── Full Request Packet ─────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct Request {
    pub header: PacketHeader,
    pub extras: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Request {
    /// Try to decode a complete request from the buffer.
    /// Returns None if there isn't enough data yet.
    pub fn decode(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < HEADER_SIZE {
            return None;
        }

        let header = PacketHeader::decode(buf)?;
        let total_len = HEADER_SIZE + header.total_body_length as usize;

        if buf.len() < total_len {
            return None; // Need more data
        }

        let body = &buf[HEADER_SIZE..total_len];
        let extras_len = header.extras_length as usize;
        let key_len = header.key_length as usize;

        let extras = body[..extras_len].to_vec();
        let key = body[extras_len..extras_len + key_len].to_vec();
        let value = body[extras_len + key_len..].to_vec();

        Some((
            Request {
                header,
                extras,
                key,
                value,
            },
            total_len,
        ))
    }

    pub fn key_str(&self) -> &str {
        std::str::from_utf8(&self.key).unwrap_or("")
    }

    /// Extract flags and expiry from SET/ADD/REPLACE extras (8 bytes)
    pub fn mutation_extras(&self) -> (u32, u32) {
        if self.extras.len() >= 8 {
            let flags = u32::from_be_bytes([
                self.extras[0], self.extras[1], self.extras[2], self.extras[3],
            ]);
            let expiry = u32::from_be_bytes([
                self.extras[4], self.extras[5], self.extras[6], self.extras[7],
            ]);
            (flags, expiry)
        } else {
            (0, 0)
        }
    }

    #[allow(dead_code)]
    pub fn vbucket_id(&self) -> u16 {
        self.header.vbucket_or_status
    }
}

// ── Response Builder ────────────────────────────────────────────────
#[derive(Debug)]
pub struct Response {
    pub header: PacketHeader,
    pub extras: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Response {
    pub fn new(opcode: Opcode, status: Status, opaque: u32) -> Self {
        Response {
            header: PacketHeader {
                magic: MAGIC_RESPONSE,
                opcode,
                key_length: 0,
                extras_length: 0,
                data_type: 0,
                vbucket_or_status: status as u16,
                total_body_length: 0,
                opaque,
                cas: 0,
            },
            extras: Vec::new(),
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    pub fn with_cas(mut self, cas: u64) -> Self {
        self.header.cas = cas;
        self
    }

    pub fn with_extras(mut self, extras: Vec<u8>) -> Self {
        self.extras = extras;
        self
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = key;
        self
    }

    pub fn with_value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
    }

    pub fn with_datatype(mut self, dt: u8) -> Self {
        self.header.data_type = dt;
        self
    }

    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(
            HEADER_SIZE + self.extras.len() + self.key.len() + self.value.len(),
        );

        let mut header = self.header.clone();
        header.extras_length = self.extras.len() as u8;
        header.key_length = self.key.len() as u16;
        header.total_body_length =
            (self.extras.len() + self.key.len() + self.value.len()) as u32;

        header.encode(&mut buf);
        buf.extend_from_slice(&self.extras);
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }

    /// Quick error response with a message
    pub fn error(opcode: Opcode, status: Status, opaque: u32, msg: &str) -> Self {
        Response::new(opcode, status, opaque)
            .with_value(msg.as_bytes().to_vec())
    }
}
