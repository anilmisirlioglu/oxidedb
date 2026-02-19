# OxideDB

> **⚠️ Disclaimer**
> This project was developed entirely through AI-assisted programming using **Anthropic's Claude (claude-4.6-opus)** via Cursor IDE.
> OxideDB is an **experimental, proof-of-concept** database engine created for educational and research purposes.
> It is **not intended for production use**. No guarantees are made regarding data integrity, performance, security, or availability in real-world environments.
> Use at your own risk.

---

A **Couchbase-compatible NoSQL database** written entirely in Rust. Features full Couchbase SDK compatibility (Python, Go), XDCR replication, B+ Tree persistent storage with WAL, secondary indexes, N1QL-like query language, multi-node cluster support with auto-failover, and a built-in web management console.

---

## Features

| Feature | Description |
|---------|-------------|
| **Couchbase SDK Compatible** | Works with official Couchbase Python SDK and Go SDK via Memcached binary protocol |
| **XDCR Replication** | Cross-Datacenter Replication with conflict resolution (Sequence Number & LWW/Timestamp) |
| **B+ Tree Storage** | 4KB page-based B+ Tree persistent storage engine with binary encoding |
| **Write-Ahead Log** | Buffered WAL with dual-trigger flush (ops count, byte size, time interval) |
| **1024 vBuckets** | CRC32-based consistent hashing across 1024 virtual buckets |
| **Secondary Indexes** | GSI-like Global Secondary Indexes with composite key and nested field support |
| **N1QL Query Language** | SQL-like query language with SELECT, WHERE, ORDER BY, LIMIT, CREATE INDEX |
| **Multi-Node Cluster** | Node management, heartbeat monitoring, partition rebalancing |
| **Auto-Failover** | Automatic failure detection with configurable timeout, quotas, and recovery |
| **Bucket/Scope/Collection** | Full Couchbase data hierarchy: Buckets → Scopes → Collections → Documents |
| **Memcached Binary Protocol** | Full KV protocol: GET, SET, ADD, REPLACE, DELETE, INCREMENT, DECREMENT, APPEND, PREPEND |
| **SASL Authentication** | PLAIN, SCRAM-SHA512, SCRAM-SHA256 authentication mechanisms |
| **REST API** | Comprehensive REST API for all management and data operations |
| **Full-Text Search (FTS)** | Inverted index with BM25 scoring, match/phrase/term/prefix/wildcard/bool queries, highlighting |
| **DCP (Database Change Protocol)** | Real-time mutation streaming with SSE, backfill, multi-stream, broadcast channel |
| **Web UI Console** | Built-in web management console with dashboard, document browser, query workbench, FTS, DCP |
| **TTL Support** | Document-level and bucket-level expiry with automatic purge |
| **CAS (Compare-and-Swap)** | Optimistic concurrency control on all mutations |

---

## Quick Start

### Prerequisites

- **Rust** 1.76.0 or later
- **Cargo** (comes with Rust)

### Build & Run

```bash
# Clone the repository
git clone https://github.com/your-org/oxidedb.git
cd oxidedb

# Build in release mode
cargo build --release

# Run with defaults (REST: 8091, KV: 11210)
./target/release/oxidedb

# Or with custom configuration
./target/release/oxidedb \
  --port 8091 \
  --memcached-port 11210 \
  --data-dir ./data \
  --node-name node-1 \
  --num-vbuckets 1024
```

### Using Docker (Optional)

```dockerfile
FROM rust:1.76 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/oxidedb /usr/local/bin/
EXPOSE 8091 11210
CMD ["oxidedb"]
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Client Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Couchbase SDK │  │   REST API   │  │    Web UI    │  │
│  │ (Python/Go)  │  │  (Axum)      │  │  (Embedded)  │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
└─────────┼──────────────────┼─────────────────┼──────────┘
          │                  │                 │
┌─────────┼──────────────────┼─────────────────┼──────────┐
│         ▼                  ▼                 ▼          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Memcached   │  │   API Layer  │  │  Query       │  │
│  │  Binary      │  │  (Routes &   │  │  Engine      │  │
│  │  Protocol    │  │   Handlers)  │  │  (N1QL-like) │  │
│  │  (port 11210)│  │  (port 8091) │  │              │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                  │                 │          │
│         ▼                  ▼                 ▼          │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Storage Engine                       │   │
│  │  ┌────────────────────────────────────────────┐  │   │
│  │  │  Bucket → Scope → Collection → Document    │  │   │
│  │  └────────────────────────────────────────────┘  │   │
│  │  ┌─────────────┐  ┌──────────────────────────┐  │   │
│  │  │  vBuckets    │  │  Secondary Indexes (GSI) │  │   │
│  │  │  (1024 ×     │  │  BTreeMap-based          │  │   │
│  │  │   CRC32)     │  │  Composite keys          │  │   │
│  │  └──────┬──────┘  └──────────────────────────┘  │   │
│  │         │                                        │   │
│  │  ┌──────▼──────────────────────────────────┐    │   │
│  │  │  Persistence Layer                       │    │   │
│  │  │  ┌───────────┐  ┌───────────────────┐   │    │   │
│  │  │  │  B+ Tree   │  │  WAL (Write-Ahead │   │    │   │
│  │  │  │  (4KB pages,│  │  Log) + Buffered  │   │    │   │
│  │  │  │   binary)  │  │  Dual-trigger      │   │    │   │
│  │  │  └───────────┘  └───────────────────┘   │    │   │
│  │  └─────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Cluster    │  │    XDCR      │  │  Auto-       │  │
│  │   Manager    │  │  Replicator  │  │  Failover    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Data Model

```
Cluster
 └── Bucket (e.g. "travel-sample")
      ├── BucketConfig (type, RAM quota, replicas, conflict resolution)
      ├── Scopes
      │    ├── _default (auto-created)
      │    │    └── Collections
      │    │         └── _default (auto-created)
      │    └── inventory
      │         ├── airlines
      │         ├── airports
      │         └── routes
      └── vBuckets [0..1023]
           └── Documents
                ├── key: "airline_123"
                ├── value: { JSON }
                ├── CAS: 1707123456789
                ├── seq_no: 42
                ├── rev_id: 3
                ├── expiry: Optional<DateTime>
                ├── flags: 0
                └── vbucket_id: 587
```

---

## Configuration

All configuration is via CLI arguments:

| Argument | Default | Description |
|----------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` / `-p` | `8091` | REST API port |
| `--memcached-port` | `11210` | Memcached binary protocol port (SDK) |
| `--data-dir` | `./data` | Directory for persistent storage |
| `--node-name` | `node-1` | Unique name for this cluster node |
| `--num-vbuckets` | `1024` | Number of virtual buckets per bucket |
| `--enable-persistence` | `true` | Enable disk persistence |
| `--ttl-check-interval-secs` | `1` | How often to check for expired documents |
| `--xdcr-replication-interval-ms` | `500` | XDCR replication cycle interval |
| `--wal-buffer-max-ops` | `5000` | Flush WAL after N operations |
| `--wal-buffer-max-bytes` | `4194304` | Flush WAL after N bytes (4MB) |
| `--wal-flush-interval-ms` | `1000` | Flush WAL every N milliseconds |
| `--btree-compact-interval-secs` | `30` | B+ Tree compaction interval |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level: `error`, `warn`, `info`, `debug`, `trace` |

### Example: Production Setup

```bash
RUST_LOG=info ./oxidedb \
  --port 8091 \
  --memcached-port 11210 \
  --data-dir /var/lib/oxidedb \
  --node-name prod-node-1 \
  --num-vbuckets 1024 \
  --wal-buffer-max-bytes 16777216 \
  --wal-buffer-max-ops 10000 \
  --btree-compact-interval-secs 60
```

---

## Couchbase SDK Compatibility

### Python SDK

```python
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta

# Connect
auth = PasswordAuthenticator("Administrator", "password")
cluster = Cluster("couchbase://localhost", ClusterOptions(auth))
cluster.wait_until_ready(timedelta(seconds=10))

# Open bucket
bucket = cluster.bucket("my-bucket")
collection = bucket.default_collection()

# CRUD operations
collection.upsert("doc-1", {"name": "Alice", "age": 30})
result = collection.get("doc-1")
print(result.content_as[dict])
collection.remove("doc-1")
```

### Go SDK

```go
package main

import (
    "fmt"
    "time"
    "github.com/couchbase/gocb/v2"
)

func main() {
    cluster, _ := gocb.Connect("couchbase://localhost", gocb.ClusterOptions{
        Authenticator: gocb.PasswordAuthenticator{
            Username: "Administrator",
            Password: "password",
        },
    })
    cluster.WaitUntilReady(10*time.Second, nil)

    bucket := cluster.Bucket("my-bucket")
    bucket.WaitUntilReady(5*time.Second, nil)
    collection := bucket.DefaultCollection()

    // Upsert
    collection.Upsert("doc-1", map[string]interface{}{
        "name": "Bob",
        "age":  25,
    }, nil)

    // Get
    result, _ := collection.Get("doc-1", nil)
    var content map[string]interface{}
    result.Content(&content)
    fmt.Println(content)
}
```

### Supported SDK Operations

| Operation | Status |
|-----------|--------|
| GET | ✅ |
| SET (Upsert) | ✅ |
| ADD (Insert) | ✅ |
| REPLACE | ✅ |
| DELETE (Remove) | ✅ |
| INCREMENT | ✅ |
| DECREMENT | ✅ |
| APPEND | ✅ |
| PREPEND | ✅ |
| TOUCH | ✅ |
| GAT (Get and Touch) | ✅ |
| NOOP | ✅ |
| STAT | ✅ |
| FLUSH | ✅ |
| SELECT_BUCKET | ✅ |
| SASL AUTH (PLAIN) | ✅ |
| SASL AUTH (SCRAM-SHA512) | ✅ |
| SASL AUTH (SCRAM-SHA256) | ✅ |
| HELLO (Feature Negotiation) | ✅ |
| GET_CLUSTER_CONFIG | ✅ |
| Collections Manifest | ✅ |
| ObserveSeqno | ✅ |

---

## REST API Reference

### Buckets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/buckets` | List all buckets |
| POST | `/api/v1/buckets` | Create a new bucket |
| GET | `/api/v1/buckets/:name` | Get bucket details |
| DELETE | `/api/v1/buckets/:name` | Delete a bucket |
| POST | `/api/v1/buckets/:name/flush` | Flush all documents in a bucket |
| GET | `/api/v1/buckets/:name/stats` | Get bucket statistics |
| GET | `/api/v1/buckets/:bucket/scopes` | List scopes |
| POST | `/api/v1/buckets/:bucket/scopes` | Create a scope |
| DELETE | `/api/v1/buckets/:bucket/scopes/:scope` | Delete a scope |
| POST | `/api/v1/buckets/:bucket/scopes/:scope/collections` | Create a collection |
| DELETE | `/api/v1/buckets/:bucket/scopes/:scope/collections/:col` | Delete a collection |

### Documents

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/docs/:bucket/scopes/:scope/collections/:col/docs` | List documents (paginated) |
| GET | `/api/v1/docs/:bucket/scopes/:scope/collections/:col/docs/:key` | Get document |
| PUT | `/api/v1/docs/:bucket/scopes/:scope/collections/:col/docs/:key` | Create/update document |
| DELETE | `/api/v1/docs/:bucket/scopes/:scope/collections/:col/docs/:key` | Delete document |
| POST | `/api/v1/docs/:bucket/scopes/:scope/collections/:col/docs/:key/touch` | Update TTL |

#### Create Bucket

```bash
curl -X POST http://localhost:8091/api/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-bucket",
    "bucket_type": "couchbase",
    "ram_quota_mb": 256,
    "num_replicas": 1,
    "conflict_resolution": "seqno"
  }'
```

#### Upsert Document

```bash
curl -X PUT http://localhost:8091/api/v1/docs/my-bucket/scopes/_default/collections/_default/docs/user-1 \
  -H "Content-Type: application/json" \
  -d '{
    "value": {"name": "Alice", "age": 30, "city": "Istanbul"},
    "expiry": 3600
  }'
```

#### Query Documents

```bash
curl -X POST http://localhost:8091/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"statement": "SELECT * FROM my-bucket WHERE city = '\''Istanbul'\'' LIMIT 10"}'
```

### Indexes

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/indexes` | List all indexes |
| POST | `/api/v1/indexes` | Create an index |
| GET | `/api/v1/indexes/:bucket` | List indexes for a bucket |
| GET | `/api/v1/indexes/:bucket/:name` | Get index details |
| DELETE | `/api/v1/indexes/:bucket/:name` | Drop an index |
| POST | `/api/v1/indexes/:bucket/:name/rebuild` | Rebuild an index |

### XDCR

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/xdcr/clusters` | List remote clusters |
| POST | `/api/v1/xdcr/clusters` | Add remote cluster |
| DELETE | `/api/v1/xdcr/clusters/:name` | Remove remote cluster |
| GET | `/api/v1/xdcr/replications` | List replications |
| POST | `/api/v1/xdcr/replications` | Create replication |
| GET | `/api/v1/xdcr/replications/:id` | Get replication details |
| DELETE | `/api/v1/xdcr/replications/:id` | Delete replication |
| POST | `/api/v1/xdcr/replications/:id/pause` | Pause replication |
| POST | `/api/v1/xdcr/replications/:id/resume` | Resume replication |

### Cluster

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/cluster` | Get cluster info |
| GET | `/api/v1/cluster/nodes` | List nodes |
| POST | `/api/v1/cluster/nodes` | Add a node |
| DELETE | `/api/v1/cluster/nodes/:name` | Remove a node |
| GET | `/api/v1/cluster/partitions` | Get full partition map |
| GET | `/api/v1/cluster/partitions/summary` | Get partition summary |
| POST | `/api/v1/cluster/rebalance` | Trigger rebalance |
| GET | `/api/v1/cluster/rebalance` | Get rebalance status |
| GET | `/api/v1/cluster/failover` | Get failover state |
| POST | `/api/v1/cluster/failover/config` | Update failover config |
| POST | `/api/v1/cluster/failover/reset` | Reset failover quota |
| POST | `/api/v1/cluster/failover/:node` | Failover a node |
| POST | `/api/v1/cluster/failover/:node/recover` | Recover a node |

### DCP (Database Change Protocol)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/dcp/streams` | Create a DCP stream |
| GET | `/api/v1/dcp/streams` | List all DCP streams |
| GET | `/api/v1/dcp/streams/:id` | Get stream info |
| DELETE | `/api/v1/dcp/streams/:id` | Close/delete a stream |
| POST | `/api/v1/dcp/streams/:id/pause` | Pause a stream |
| POST | `/api/v1/dcp/streams/:id/resume` | Resume a stream |
| GET | `/api/v1/dcp/streams/:id/events` | Poll for recent events |
| GET | `/api/v1/dcp/streams/:id/sse` | Server-Sent Events real-time stream |
| POST | `/api/v1/dcp/backfill` | One-shot backfill of all documents |

**Create Stream:**
```json
POST /api/v1/dcp/streams
{
  "name": "my-stream",
  "bucket": "my-bucket",
  "scope_filter": "_default",
  "collection_filter": "_default",
  "include_backfill": true
}
```

**SSE Real-Time Streaming:**
```bash
curl -N http://localhost:8091/api/v1/dcp/streams/{stream-id}/sse
# Receives events: mutation, deletion, expiration
```

### System

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Server info |
| GET | `/health` | Health check |
| GET | `/ui` | Web management console |
| GET | `/api/v1/persistence/stats` | Persistence statistics |

---

## Query Language (N1QL-like)

### SELECT

```sql
-- Basic query
SELECT * FROM `my-bucket` WHERE city = 'Istanbul' LIMIT 10

-- With ordering and pagination
SELECT name, age FROM `my-bucket` WHERE age > 25 ORDER BY age DESC LIMIT 10 OFFSET 20

-- Nested field access
SELECT * FROM `my-bucket` WHERE address.city = 'Ankara'
```

### CREATE INDEX

```sql
-- Simple index
CREATE INDEX idx_city ON `my-bucket`(city)

-- Composite index
CREATE INDEX idx_city_age ON `my-bucket`(city, age)

-- Nested field index
CREATE INDEX idx_addr ON `my-bucket`(address.city, address.zip)
```

### DROP INDEX

```sql
DROP INDEX idx_city ON `my-bucket`
```

---

## Web UI

Access the web management console at **http://localhost:8091/ui**

### Pages

| Page | Description |
|------|-------------|
| **Dashboard** | Overview with bucket stats, document counts, XDCR status |
| **Buckets** | Create, delete, manage buckets. Click a bucket to explore its scopes and collections |
| **Documents** | Browse, search, create, edit, and delete documents with pagination |
| **Query** | N1QL query workbench with syntax highlighting and result tables |
| **Indexes** | Create and manage secondary indexes, rebuild, drop |
| **XDCR** | Configure remote clusters and replication streams |
| **Cluster** | Monitor nodes, add/remove nodes, view services |
| **Failover** | Configure auto-failover, manually failover/recover nodes |
| **Partitions** | Visualize vBucket distribution with heatmap, trigger rebalance |

---

## Storage Engine

### B+ Tree

- **Page size**: 4KB
- **Encoding**: Binary (big-endian lengths + raw bytes)
- **Structure**: Internal nodes with sorted keys → leaf nodes with key-value pairs
- **Compaction**: Periodic merge of WAL entries into B+ Tree data file

### Write-Ahead Log (WAL)

The WAL uses a **dual-trigger flush** strategy:

1. **Operation count trigger**: Flush after N buffered operations (default: 5000)
2. **Byte size trigger**: Flush after N buffered bytes (default: 4MB)
3. **Time interval trigger**: Flush every N milliseconds (default: 1000ms)

Whichever trigger fires first causes a WAL flush, ensuring both throughput and durability.

### Recovery

On startup:
1. Load existing B+ Tree data files
2. Replay any uncompacted WAL entries
3. Resume normal operations

---

## XDCR (Cross-Datacenter Replication)

### Conflict Resolution

| Mode | Description |
|------|-------------|
| **Sequence Number** | Higher revision sequence wins (default) |
| **Timestamp (LWW)** | Last write wins based on wall-clock timestamp |

### Setup

```bash
# 1. Add remote cluster
curl -X POST http://localhost:8091/api/v1/xdcr/clusters \
  -H "Content-Type: application/json" \
  -d '{"name": "dc-west", "hostname": "192.168.1.100", "port": 8091}'

# 2. Create replication
curl -X POST http://localhost:8091/api/v1/xdcr/replications \
  -H "Content-Type: application/json" \
  -d '{
    "source_bucket": "my-bucket",
    "target_cluster": "dc-west",
    "target_bucket": "my-bucket",
    "conflict_resolution": "timestamp"
  }'
```

---

## Multi-Node Cluster

### Setup

```bash
# Node 1
./oxidedb --port 8091 --node-name node-1 --data-dir ./data-1

# Node 2
./oxidedb --port 8092 --memcached-port 11211 --node-name node-2 --data-dir ./data-2

# Add node-2 to the cluster
curl -X POST http://localhost:8091/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"name": "node-2", "hostname": "127.0.0.1", "port": 8092}'

# Trigger rebalance to distribute vBuckets
curl -X POST http://localhost:8091/api/v1/cluster/rebalance
```

### Auto-Failover

Configure automatic failover for unresponsive nodes:

```bash
curl -X POST http://localhost:8091/api/v1/cluster/failover/config \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "timeout_secs": 120,
    "max_count": 3,
    "cooldown_secs": 30,
    "min_cluster_size": 2
  }'
```

---

## Default Credentials

| Username | Password |
|----------|----------|
| `Administrator` | `password` |

These credentials are used for Couchbase SDK authentication (SASL). The REST API does not require authentication by default.

---

## Project Structure

```
oxidedb/
├── Cargo.toml              # Dependencies and project config
├── README.md               # This file
├── docs/                   # Documentation
│   ├── architecture.md     # Detailed architecture guide
│   ├── api-reference.md    # Complete API reference
│   └── sdk-compatibility.md # SDK compatibility details
├── static/
│   └── index.html          # Embedded Web UI (single-file SPA)
└── src/
    ├── main.rs             # Entry point, server startup
    ├── config.rs           # CLI args and configuration
    ├── error.rs            # Error types and HTTP error mapping
    ├── api/                # REST API layer
    │   ├── mod.rs          # Router builder, AppState
    │   ├── bucket_routes.rs
    │   ├── document_routes.rs
    │   ├── query_routes.rs
    │   ├── index_routes.rs
    │   ├── xdcr_routes.rs
    │   ├── cluster_routes.rs
    │   ├── couchbase_compat.rs  # SDK bootstrap endpoints
    │   └── web_ui.rs
    ├── storage/            # Storage engine
    │   ├── mod.rs
    │   ├── engine.rs       # Bucket, Scope, Collection, StorageEngine
    │   ├── vbucket.rs      # VBucket with document operations
    │   ├── document.rs     # Document model (CAS, TTL, revisions)
    │   ├── btree.rs        # B+ Tree implementation
    │   ├── persistence.rs  # Persistence manager
    │   ├── wal.rs          # Write-Ahead Log with buffered writes
    │   └── index.rs        # Secondary index manager (GSI)
    ├── query/              # Query engine
    │   ├── mod.rs
    │   └── engine.rs       # N1QL-like parser and executor
    ├── memcached/          # Memcached binary protocol (SDK compat)
    │   ├── mod.rs
    │   ├── protocol.rs     # Protocol structs, opcodes
    │   ├── server.rs       # TCP server, connection handling
    │   ├── handler.rs      # KV operation handlers
    │   └── scram.rs        # SCRAM-SHA512/256 authentication
    ├── xdcr/               # Cross-Datacenter Replication
    │   ├── mod.rs
    │   ├── replicator.rs   # Replication manager
    │   └── conflict.rs     # Conflict resolution strategies
    ├── cluster/            # Cluster management
    │   ├── mod.rs
    │   ├── node.rs         # ClusterNode, NodeStatus
    │   ├── partition.rs    # vBucket partition map, rebalancing
    │   └── failover.rs     # Auto-failover logic
    └── bucket/             # (reserved for future bucket-level features)
```

---

## Performance Considerations

- **Memory-first architecture**: All documents are kept in memory for fast access
- **Disk-backed persistence**: B+ Tree + WAL ensure durability
- **Concurrent access**: `RwLock` per vBucket allows parallel reads with exclusive writes
- **DashMap** for bucket-level structures: Lock-free concurrent hash maps
- **Batched WAL writes**: Dual-trigger flush reduces disk I/O
- **Index-accelerated queries**: Secondary indexes avoid full collection scans

---

## Roadmap — Couchbase Feature Parity

### ✅ Implemented (40 features)

| Feature | Status |
|---------|--------|
| KV: GET, SET, ADD, REPLACE, DELETE, INCREMENT, DECREMENT, APPEND, PREPEND | ✅ |
| KV: TOUCH, GAT (Get and Touch) | ✅ |
| KV: Sub-Document API — `LookupIn` / `MutateIn` (partial JSON path read/write) | ✅ |
| KV: Get & Lock / Unlock (pessimistic locking) | ✅ |
| KV: Exists (check document existence, CAS only) | ✅ |
| KV: Get from Replica (high-availability reads) | ✅ |
| KV: Durable Writes / SyncReplication | ✅ |
| SASL: PLAIN, SCRAM-SHA512, SCRAM-SHA256 | ✅ |
| Bucket / Scope / Collection hierarchy | ✅ |
| vBucket partitioning (CRC32, 1024 vBuckets) | ✅ |
| CAS (Compare-and-Swap) | ✅ |
| TTL / Document Expiry | ✅ |
| B+ Tree persistence + WAL | ✅ |
| Secondary Indexes (GSI-like, BTreeMap) + Index Persistence | ✅ |
| N1QL: SELECT, WHERE, ORDER BY, LIMIT, OFFSET | ✅ |
| N1QL: CREATE INDEX, DROP INDEX | ✅ |
| N1QL: Aggregation — COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING | ✅ |
| N1QL: JOINs — INNER JOIN, LEFT JOIN, NEST, UNNEST | ✅ |
| N1QL: DML — UPDATE, DELETE, INSERT, MERGE | ✅ |
| N1QL: DISTINCT | ✅ |
| N1QL: EXPLAIN (query execution plan) | ✅ |
| N1QL: USE INDEX hint | ✅ |
| N1QL: Functions — LOWER, UPPER, SUBSTR, TOSTRING, TONUMBER, NOW_STR, ARRAY_LENGTH | ✅ |
| N1QL: Prepared Statements (PREPARE / EXECUTE with parameters) | ✅ |
| Full-Text Search (FTS) — Inverted index, BM25, match/phrase/term/prefix/wildcard/regexp/bool/fuzzy queries, highlighting | ✅ |
| DCP (Database Change Protocol) — Real-time mutation streaming, SSE, backfill, multi-stream | ✅ |
| XDCR with conflict resolution (SeqNo, LWW) | ✅ |
| Multi-node cluster + heartbeat | ✅ |
| Auto-failover (configurable) | ✅ |
| Partition rebalancing | ✅ |
| Tombstone purging | ✅ |
| Couchbase Python/Go SDK compatibility | ✅ |
| Web UI management console | ✅ |
| REST API (management + data) | ✅ |
| Collections manifest + ObserveSeqno | ✅ |
| Snappy Compression — transparent compress/decompress in Memcached protocol | ✅ |
| Extended Attributes (XATTRs) — system/user xattrs, SubDoc XATTR flag support | ✅ |
| Memory Quota Enforcement — RAM quota checked on write operations | ✅ |
| Audit Logging — security & admin event log with REST API | ✅ |
| Eviction Policy Enforcement — ValueOnly, FullEviction, NRU with automatic memory management | ✅ |
| RBAC (Role-Based Access Control) — users, roles, permissions, user management API | ✅ |
| Backup / Restore — full backup snapshots, restore from backup, backup management API | ✅ |
| Array Indexes — `ALL ARRAY v FOR v IN items END`, `DISTINCT ARRAY`, sub-expressions | ✅ |
| Covering Indexes — `INCLUDE (field1, field2)` clause, skip document fetch | ✅ |
| TLS/SSL — Encrypted Memcached connections (rustls), `--tls-enabled --tls-cert-path --tls-key-path` | ✅ |
| Multi-Document ACID Transactions — Begin/Get/Insert/Replace/Remove/Commit/Rollback, CAS conflict detection, auto-expiry | ✅ |
| Bucket/Scope/Collection config persistence | ✅ |
| N1QL Subqueries — `IN (SELECT ...)`, `NOT IN (SELECT ...)`, `EXISTS (SELECT ...)`, scalar subqueries in SELECT | ✅ |
| Server Groups — Rack/zone awareness, group-aware replica placement, CRUD API | ✅ |
| Certificate Auth — x509 client certificate authentication (mTLS), CN-based user mapping | ✅ |
| Node-to-Node Encryption — Intra-cluster TLS encryption config | ✅ |
| DCP-based Rebalance — Real vBucket data transfer with group-aware placement | ✅ |
| Cross-node Scatter-Gather Query — Distributed query execution across cluster nodes | ✅ |
| Snappy Compression — Transparent document compression in B+ tree storage | ✅ |
| CCCP Config Streaming — SDK-compatible cluster config push (SSE) | ✅ |
| Chronicle Metadata Consensus — Couchbase-style replicated config log | ✅ |
| DCP Intra-Cluster Replication — Active → replica mutation streaming | ✅ |
| Durability Levels — None, Majority, MajorityAndPersistToActive, PersistToMajority | ✅ |
| Orchestrator Election — Deterministic leader election (Couchbase-style) | ✅ |
| RBAC — Role-Based Access Control with PBKDF2 password hashing, 14 roles | ✅ |

### 🚧 Missing Features (vs Couchbase Server)

#### 🟡 Services

| # | Feature | Description | Difficulty |
|---|---------|-------------|------------|
| 3 | **Analytics Service** | Columnar analytics for OLAP queries (port 8095) | Very Hard |
| 4 | **Eventing Service** | JavaScript functions for event-driven logic | Very Hard |

#### ⚪ Cluster

| # | Feature | Description | Difficulty |
|---|---------|-------------|------------|
| 9 | **Multi-Dimensional Scaling** | Separate KV, Query, Index, FTS, Analytics nodes | Hard |

---

## License

This project is provided as-is for educational and research purposes.
