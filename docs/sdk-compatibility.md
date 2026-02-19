# Couchbase SDK Compatibility Guide

OxideDB implements the Couchbase Memcached Binary Protocol and REST bootstrap endpoints, enabling compatibility with official Couchbase SDKs.

---

## Supported SDKs

| SDK | Tested Version | Status |
|-----|---------------|--------|
| **Python** (couchbase) | 4.x | ✅ Fully working |
| **Go** (gocb/v2) | 2.x | ✅ Fully working |
| Java | — | Should work (untested) |
| .NET | — | Should work (untested) |
| Node.js | — | Should work (untested) |

---

## Connection Details

| Setting | Value |
|---------|-------|
| Connection String | `couchbase://localhost` |
| Username | `Administrator` |
| Password | `password` |
| REST Port | 8091 |
| KV Port | 11210 |

---

## SDK Connection Flow

### 1. HTTP Bootstrap

The SDK first contacts the REST API to discover cluster topology:

```
GET /pools                         → Cluster info
GET /pools/default                 → Default pool
GET /pools/default/buckets         → Bucket list
GET /pools/default/buckets/:name   → Bucket config + vBucket map
GET /pools/default/nodeServices    → Service ports per node
```

### 2. Memcached Connection

The SDK connects to port 11210 and performs:

```
HELLO          → Feature negotiation (Datatype, Xerror, JSON, Collections, etc.)
SASL_LIST_MECHS → Get supported auth mechanisms
SASL_AUTH      → Authenticate (SCRAM-SHA512 preferred)
SASL_STEP      → Complete SCRAM challenge-response
GET_CLUSTER_CONFIG → Get global cluster config (JSON)
SELECT_BUCKET  → Select working bucket
GET_CLUSTER_CONFIG → Get bucket-specific config with vBucket map
```

### 3. KV Operations

After authentication and bucket selection, the SDK performs KV operations:

```
SET / GET / DELETE / REPLACE / ADD / INCREMENT / DECREMENT / etc.
```

---

## Authentication

### Supported Mechanisms

| Mechanism | Priority | Description |
|-----------|----------|-------------|
| SCRAM-SHA512 | Highest | Challenge-response with SHA-512 |
| SCRAM-SHA256 | Medium | Challenge-response with SHA-256 |
| PLAIN | Lowest | Simple username/password |

### SCRAM Flow

```
Client → Server: SASL_AUTH(mechanism="SCRAM-SHA512", client-first-message)
Server → Client: SASL_AUTH response(server-first-message with salt, iterations)
Client → Server: SASL_STEP(client-final-message with proof)
Server → Client: SASL_STEP response(server-final-message with signature)
```

### Credentials

The default credentials are:

- **Username**: `Administrator`
- **Password**: `password`

These are hardcoded for development. In production, you would modify `src/memcached/server.rs` to use a proper credential store.

---

## Supported Operations

### Key-Value Operations

| Operation | Opcode | SDK Method | Description |
|-----------|--------|------------|-------------|
| GET | 0x00 | `collection.get()` | Retrieve document |
| SET | 0x01 | `collection.upsert()` | Create or update |
| ADD | 0x02 | `collection.insert()` | Create only (fail if exists) |
| REPLACE | 0x03 | `collection.replace()` | Update only (fail if not exists) |
| DELETE | 0x04 | `collection.remove()` | Delete document |
| INCREMENT | 0x05 | `collection.binary().increment()` | Atomic counter increment |
| DECREMENT | 0x06 | `collection.binary().decrement()` | Atomic counter decrement |
| APPEND | 0x0E | `collection.binary().append()` | Append to value |
| PREPEND | 0x0F | `collection.binary().prepend()` | Prepend to value |
| NOOP | 0x0A | (internal) | Keep-alive |
| FLUSH | 0x08 | `bucket_manager.flush_bucket()` | Flush all documents |
| STAT | 0x10 | (internal) | Get statistics |
| VERSION | 0x0B | (internal) | Get server version |
| TOUCH | 0x1C | `collection.touch()` | Update expiry |
| GAT | 0x1D | `collection.get_and_touch()` | Get and update expiry |

### Control Operations

| Operation | Opcode | Description |
|-----------|--------|-------------|
| HELLO | 0x1F | Feature negotiation |
| SASL_LIST_MECHS | 0x20 | List auth mechanisms |
| SASL_AUTH | 0x21 | Start authentication |
| SASL_STEP | 0x22 | Continue SCRAM auth |
| SELECT_BUCKET | 0x89 | Select active bucket |
| GET_CLUSTER_CONFIG | 0xB5 | Get cluster/bucket config |
| GetCollectionsManifest | 0xBA | Get collection manifest |
| GetCollectionId | 0xBB | Get collection ID |
| ObserveSeqno | 0x91 | Observe vBucket sequence number |

### Negotiated Features

| Feature | Code | Description |
|---------|------|-------------|
| Datatype | 0x0001 | Extended datatype support |
| TCP_NODELAY | 0x0003 | Disable Nagle's algorithm |
| Mutation seqno | 0x0004 | Return mutation sequence numbers |
| Xattr | 0x0006 | Extended attributes |
| Xerror | 0x0007 | Extended error codes |
| SELECT_BUCKET | 0x0008 | Bucket selection |
| Snappy | 0x000A | Snappy compression |
| JSON | 0x000B | JSON datatype |
| Duplex | 0x000C | Duplex mode |
| ClustermapNotif | 0x000D | Cluster map push notifications |
| Unordered execution | 0x000E | Unordered command execution |
| AltRequest | 0x0010 | Alternate request format |
| SyncReplication | 0x0011 | Synchronous replication |
| Collections | 0x0012 | Collection support |
| PreserveTtl | 0x0014 | Preserve TTL on mutation |

---

## Python SDK Example

### Installation

```bash
pip install couchbase
```

### Full Example

```python
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta

# Connect
auth = PasswordAuthenticator("Administrator", "password")
cluster = Cluster("couchbase://localhost", ClusterOptions(auth))
cluster.wait_until_ready(timedelta(seconds=10))

# First, create a bucket via REST API
import requests
requests.post("http://localhost:8091/api/v1/buckets", json={
    "name": "test-bucket",
    "bucket_type": "couchbase",
    "ram_quota_mb": 256
})

# Open bucket
bucket = cluster.bucket("test-bucket")
bucket.wait_until_ready(timedelta(seconds=5))
collection = bucket.default_collection()

# Upsert
collection.upsert("user-1", {
    "name": "Alice",
    "age": 30,
    "city": "Istanbul"
})

# Get
result = collection.get("user-1")
print(result.content_as[dict])
# → {'name': 'Alice', 'age': 30, 'city': 'Istanbul'}

# Replace with CAS
result = collection.replace("user-1", {
    "name": "Alice",
    "age": 31,
    "city": "Ankara"
}, cas=result.cas)

# Remove
collection.remove("user-1")
```

---

## Go SDK Example

### Installation

```bash
go get github.com/couchbase/gocb/v2
```

### Full Example

```go
package main

import (
    "fmt"
    "log"
    "net/http"
    "strings"
    "time"

    "github.com/couchbase/gocb/v2"
)

func main() {
    // Create bucket via REST API first
    body := `{"name":"test-bucket","bucket_type":"couchbase","ram_quota_mb":256}`
    http.Post("http://localhost:8091/api/v1/buckets", "application/json",
        strings.NewReader(body))

    // Connect
    cluster, err := gocb.Connect("couchbase://localhost", gocb.ClusterOptions{
        Authenticator: gocb.PasswordAuthenticator{
            Username: "Administrator",
            Password: "password",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    cluster.WaitUntilReady(10*time.Second, nil)

    // Open bucket
    bucket := cluster.Bucket("test-bucket")
    bucket.WaitUntilReady(5*time.Second, nil)
    collection := bucket.DefaultCollection()

    // Upsert
    _, err = collection.Upsert("user-1", map[string]interface{}{
        "name": "Bob",
        "age":  25,
        "city": "Izmir",
    }, nil)
    if err != nil {
        log.Fatal(err)
    }

    // Get
    result, err := collection.Get("user-1", nil)
    if err != nil {
        log.Fatal(err)
    }
    var content map[string]interface{}
    result.Content(&content)
    fmt.Println(content)

    // Remove
    _, err = collection.Remove("user-1", nil)
    if err != nil {
        log.Fatal(err)
    }
}
```

---

## Cluster Config Format

### Global Config (before bucket select)

```json
{
  "rev": 1,
  "nodesExt": [
    {
      "services": { "kv": 11210, "mgmt": 8091, "n1ql": 8093 },
      "hostname": "127.0.0.1",
      "thisNode": true
    }
  ],
  "clusterCapabilities": {
    "n1ql": ["enhancedPreparedStatements"]
  },
  "clusterCapabilitiesVer": [1, 0]
}
```

### Bucket Config (after SELECT_BUCKET)

```json
{
  "rev": 1,
  "name": "test-bucket",
  "nodeLocator": "vbucket",
  "bucketType": "membase",
  "uuid": "auto-generated",
  "nodesExt": [
    {
      "services": { "kv": 11210, "mgmt": 8091 },
      "hostname": "127.0.0.1",
      "thisNode": true
    }
  ],
  "vBucketServerMap": {
    "hashAlgorithm": "CRC",
    "numReplicas": 0,
    "serverList": ["127.0.0.1:11210"],
    "vBucketMap": [[0], [0], ...]
  },
  "collectionsManifestUid": "0"
}
```

---

## Troubleshooting

### SDK Connection Timeout

1. Ensure server is running on ports 8091 and 11210
2. Create the bucket via REST API before trying to open it from SDK
3. Check server logs: `RUST_LOG=debug ./oxidedb`

### Authentication Errors

1. Use `Administrator` / `password` credentials
2. The server supports SCRAM-SHA512/256 and PLAIN

### "Bucket not found" from SDK

Buckets must be created via REST API before they can be accessed from SDKs:

```bash
curl -X POST http://localhost:8091/api/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{"name":"my-bucket","bucket_type":"couchbase","ram_quota_mb":256}'
```
