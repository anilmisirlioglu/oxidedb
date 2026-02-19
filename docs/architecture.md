# OxideDB Architecture

## Overview

OxideDB is a Couchbase-compatible NoSQL database implemented entirely in Rust. It provides a memory-first, disk-backed storage architecture with full Couchbase SDK compatibility.

---

## System Layers

### 1. Client Layer

Three access paths:

| Path | Port | Protocol | Use Case |
|------|------|----------|----------|
| **Couchbase SDK** | 11210 | Memcached Binary Protocol | Application data access (Python, Go, Java, .NET) |
| **REST API** | 8091 | HTTP/JSON | Management operations, queries, monitoring |
| **Web UI** | 8091 | HTTP | Browser-based administration console |

### 2. Protocol Layer

#### Memcached Binary Protocol (port 11210)

Implements the Couchbase KV protocol for SDK compatibility:

- **Connection lifecycle**: HELLO → SASL_LIST_MECHS → SASL_AUTH → (SASL_STEP) → SELECT_BUCKET → KV ops
- **Authentication**: PLAIN, SCRAM-SHA512, SCRAM-SHA256
- **Feature negotiation**: Datatype, TCP_NODELAY, Mutation seqno, Xattr, Xerror, SELECT_BUCKET, Snappy, JSON, Duplex, ClustermapNotif, Unordered execution, AltRequest, SyncReplication, Collections, PreserveTtl
- **Cluster config**: Returns global config (pre-bucket-select) or bucket-specific config (post-bucket-select) with vBucket map

#### REST API (port 8091)

Built on **Axum** framework with:
- Tower middleware (CORS, tracing)
- JSON request/response
- Partition-aware request forwarding (multi-node)
- Couchbase bootstrap endpoints (`/pools`, `/pools/default`, `/pools/default/buckets`, etc.)

### 3. Storage Engine

#### Data Hierarchy

```
StorageEngine
 └── DashMap<String, Arc<Bucket>>
      └── Bucket
           ├── BucketConfig
           ├── DashMap<String, Scope>
           │    └── DashMap<String, Collection>
           └── Vec<RwLock<VBucket>>  [0..1023]
                └── HashMap<String, Document>
```

#### VBucket Partitioning

- **1024 vBuckets** per bucket (configurable)
- **CRC32 hashing**: `key → CRC32(key) % num_vbuckets → vBucket ID`
- Each vBucket has its own `RwLock` for fine-grained concurrency
- vBuckets can be assigned to different cluster nodes

#### Document Model

Each document contains:

| Field | Type | Description |
|-------|------|-------------|
| `key` | String | Unique document identifier |
| `value` | JSON | Document content |
| `cas` | u64 | Compare-and-Swap value (monotonically increasing) |
| `seq_no` | u64 | Sequence number (global, monotonically increasing) |
| `rev_id` | u64 | Revision ID (per-document) |
| `expiry` | Option<DateTime> | TTL expiration time |
| `flags` | u32 | Client-defined flags |
| `vbucket_id` | u16 | Assigned vBucket |
| `created_at` | DateTime | Creation timestamp |
| `updated_at` | DateTime | Last modification timestamp |
| `deleted` | bool | Tombstone flag (for XDCR) |
| `source_cluster` | Option<String> | Origin cluster (for XDCR) |

### 4. Persistence Layer

#### B+ Tree Storage

- **Page size**: 4KB
- **Encoding**: Binary format with big-endian length prefixes
- **File extension**: `.nqdb`
- **Structure**: Multi-level tree with internal nodes (keys + child pointers) and leaf nodes (key-value pairs)
- **Compaction**: Periodic full rewrite merging WAL entries

#### Write-Ahead Log (WAL)

- **File extension**: `.wal`
- **Format**: Length-prefixed JSON entries
- **Buffered writes**: In-memory buffer with configurable flush triggers

**Dual-trigger flush strategy**:

```
Trigger 1: Operation count (default 5000 ops)
Trigger 2: Byte size (default 4MB)
Trigger 3: Time interval (default 1000ms)
→ First trigger to fire causes flush
```

#### Recovery Process

```
Startup:
1. Scan data directory for .nqdb files
2. Load B+ Tree data → reconstruct in-memory documents
3. Scan for .wal files
4. Replay WAL entries on top of B+ Tree data
5. Ready to serve
```

### 5. Secondary Indexes (GSI)

- **Implementation**: In-memory `BTreeMap` for sorted index entries
- **Composite keys**: Sort-preserving key encoding for multi-field indexes
- **Nested fields**: Supports dot-notation (e.g., `address.city`)
- **Automatic maintenance**: Indexes updated on every upsert/delete
- **Query integration**: Query engine automatically selects best index

### 6. Query Engine

**N1QL-like SQL parser** supporting:

- `SELECT [fields | *] FROM bucket`
- `WHERE` with comparison operators (`=`, `!=`, `>`, `<`, `>=`, `<=`)
- `AND` / `OR` logical operators
- `ORDER BY field [ASC|DESC]`
- `LIMIT` / `OFFSET`
- `CREATE INDEX` / `DROP INDEX`
- Nested field access via dot notation

**Query execution flow**:

```
SQL string
  → Parse (tokenize + AST)
  → Index selection (find best matching index)
  → Scan (index scan or full scan)
  → Filter (apply WHERE predicates)
  → Project (select fields)
  → Sort (ORDER BY)
  → Paginate (LIMIT/OFFSET)
  → Return results with metrics
```

### 7. XDCR Replication

- **Uni-directional**: Source bucket → Remote cluster → Target bucket
- **Change tracking**: Per-vBucket mutation log with sequence numbers
- **Conflict resolution**: Sequence Number (higher wins) or Timestamp/LWW
- **Pause/Resume**: Replications can be paused and resumed
- **Filter expressions**: Optional document key/field filters
- **Error handling**: Retry with exponential backoff, error event logging

### 8. Cluster Management

- **Node discovery**: Manual node addition via API
- **Heartbeat**: Periodic health checks (every 5 seconds)
- **Partition map**: Tracks vBucket → node assignment with revision tracking
- **Rebalancing**: Redistributes vBuckets evenly across healthy nodes
- **Auto-failover**: Configurable timeout, quota, cooldown, minimum cluster size
- **Recovery**: Failed-over nodes can be recovered and rebalanced back in

---

## Concurrency Model

| Component | Synchronization | Purpose |
|-----------|----------------|---------|
| StorageEngine.buckets | `DashMap` | Lock-free concurrent bucket access |
| Bucket.scopes | `DashMap` | Lock-free scope management |
| Scope.collections | `DashMap` | Lock-free collection management |
| VBucket | `RwLock` | Multiple readers, exclusive writer |
| IndexManager.indexes | `DashMap` | Lock-free index registry |
| WAL write buffer | `Mutex` | Serialized buffered writes |
| B+ Tree files | `Mutex` | Serialized file I/O |
| Cluster state | `RwLock` | Partition map, node registry |

---

## Background Tasks

| Task | Interval | Purpose |
|------|----------|---------|
| TTL Expiry | 1s | Purge expired documents |
| XDCR Replication | 500ms | Replicate mutations to remote clusters |
| WAL Buffer Check | 50ms | Check if any flush trigger has fired |
| B+ Tree Compaction | 30s | Merge WAL into B+ Tree data files |
| Node Health Check | 5s | Detect failed nodes, trigger auto-failover |

---

## Network Ports

| Port | Protocol | Service |
|------|----------|---------|
| 8091 | HTTP | REST API + Web UI + Couchbase bootstrap |
| 11210 | TCP | Memcached Binary Protocol (Couchbase KV) |
