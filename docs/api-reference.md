# OxideDB — REST API Reference

Base URL: `http://localhost:8091`

All request/response bodies are JSON. Set `Content-Type: application/json` for requests with bodies.

---

## Buckets

### List Buckets

```
GET /api/v1/buckets
```

**Response** `200 OK`:
```json
[
  {
    "name": "my-bucket",
    "bucket_type": "Couchbase",
    "ram_quota_mb": 256,
    "num_replicas": 1,
    "num_vbuckets": 1024,
    "document_count": 1500,
    "size_bytes": 245760,
    "flush_enabled": false,
    "conflict_resolution": "SequenceNumber",
    "scopes": [
      { "name": "_default", "collections": ["_default"] }
    ]
  }
]
```

### Create Bucket

```
POST /api/v1/buckets
```

**Request Body**:
```json
{
  "name": "travel-sample",
  "bucket_type": "couchbase",
  "ram_quota_mb": 256,
  "num_replicas": 1,
  "conflict_resolution": "seqno",
  "flush_enabled": false
}
```

| Field | Required | Default | Options |
|-------|----------|---------|---------|
| name | Yes | — | Any string |
| bucket_type | No | couchbase | `couchbase`, `ephemeral`, `memcached` |
| ram_quota_mb | No | 256 | Any positive integer |
| num_replicas | No | 1 | 0-3 |
| conflict_resolution | No | seqno | `seqno`, `timestamp` |
| flush_enabled | No | false | true/false |

### Get Bucket

```
GET /api/v1/buckets/:name
```

### Delete Bucket

```
DELETE /api/v1/buckets/:name
```

### Flush Bucket

```
POST /api/v1/buckets/:name/flush
```

Removes all documents from the bucket. Only works if `flush_enabled` is true.

### Bucket Statistics

```
GET /api/v1/buckets/:name/stats
```

**Response** `200 OK`:
```json
{
  "bucket": "my-bucket",
  "document_count": 1500,
  "local_document_count": 1500,
  "size_bytes": 245760,
  "num_vbuckets": 1024,
  "local_active_vbuckets": 1024,
  "max_sequence_number": 4200,
  "partition_map_revision": 1,
  "vbucket_seq_nos": [
    { "vbucket": 0, "seq_no": 12 },
    { "vbucket": 3, "seq_no": 8 }
  ]
}
```

---

## Scopes & Collections

### List Scopes

```
GET /api/v1/buckets/:bucket/scopes
```

**Response** `200 OK`:
```json
[
  {
    "name": "_default",
    "collections": ["_default"]
  },
  {
    "name": "inventory",
    "collections": ["airlines", "airports"]
  }
]
```

### Create Scope

```
POST /api/v1/buckets/:bucket/scopes
```

**Body**: `{"name": "inventory"}`

### Delete Scope

```
DELETE /api/v1/buckets/:bucket/scopes/:scope
```

### Create Collection

```
POST /api/v1/buckets/:bucket/scopes/:scope/collections
```

**Body**: `{"name": "airlines"}`

### Delete Collection

```
DELETE /api/v1/buckets/:bucket/scopes/:scope/collections/:collection
```

---

## Documents

### List Documents

```
GET /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs
```

**Query Parameters**:

| Param | Default | Description |
|-------|---------|-------------|
| limit | 50 | Max documents per page (max 500) |
| offset | 0 | Pagination offset |
| prefix | — | Filter by key prefix |

**Response** `200 OK`:
```json
{
  "documents": [
    {
      "key": "user-1",
      "cas": 1707123456789,
      "rev_id": 1,
      "seq_no": 42,
      "vbucket_id": 587,
      "size_bytes": 128,
      "updated_at": "2026-02-06T12:00:00Z",
      "expiry": null,
      "value_preview": { "name": "Alice", "age": 30, "city": "Istanbul" }
    }
  ],
  "total": 150,
  "limit": 50,
  "offset": 0,
  "has_more": true
}
```

### Get Document

```
GET /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key
```

**Response** `200 OK`:
```json
{
  "key": "user-1",
  "value": { "name": "Alice", "age": 30, "city": "Istanbul" },
  "cas": 1707123456789,
  "rev_id": 1,
  "seq_no": 42,
  "expiry": null,
  "flags": 0,
  "vbucket_id": 587,
  "created_at": "2026-02-06T12:00:00Z",
  "updated_at": "2026-02-06T12:00:00Z",
  "served_by": "node-1"
}
```

### Upsert Document

```
PUT /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key
```

**Request Body**:
```json
{
  "value": { "name": "Alice", "age": 30 },
  "expiry": 3600,
  "cas": 1707123456789
}
```

| Field | Required | Description |
|-------|----------|-------------|
| value | Yes | JSON document content |
| expiry | No | TTL in seconds (0 = no expiry) |
| cas | No | If provided, uses replace-with-CAS |

### Delete Document

```
DELETE /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key
```

**Query Parameters**:

| Param | Description |
|-------|-------------|
| cas | Optional CAS value for conditional delete |

### Touch Document

```
POST /api/v1/docs/:bucket/scopes/:scope/collections/:collection/docs/:key/touch
```

**Body**: `{"expiry": 7200}`

---

## Query

### Execute Query

```
POST /api/v1/query
```

**Request Body**:
```json
{
  "statement": "SELECT name, age FROM my-bucket WHERE city = 'Istanbul' ORDER BY age DESC LIMIT 10"
}
```

**Response** `200 OK`:
```json
{
  "status": "success",
  "results": [
    {
      "_key": "user-1",
      "_cas": 1707123456789,
      "_vbucket": 587,
      "doc": { "name": "Alice", "age": 30 }
    }
  ],
  "metrics": {
    "elapsed_ms": 2,
    "result_count": 1,
    "scanned_count": 150,
    "index_used": "idx_city"
  }
}
```

### Supported SQL Syntax

```sql
SELECT [* | field1, field2, ...] FROM bucket_name
  [WHERE condition [AND|OR condition ...]]
  [ORDER BY field [ASC|DESC]]
  [LIMIT n]
  [OFFSET n]

CREATE INDEX index_name ON bucket_name(field1 [, field2, ...])
DROP INDEX index_name ON bucket_name
```

**WHERE operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`

---

## Indexes

### List All Indexes

```
GET /api/v1/indexes
```

**Response** `200 OK`:
```json
[
  {
    "name": "idx_city",
    "bucket": "my-bucket",
    "fields": ["city"],
    "condition": null,
    "index_type": "GSI",
    "state": "Online",
    "num_entries": 1500,
    "created_at": "2026-02-06T12:00:00Z"
  }
]
```

### List Bucket Indexes

```
GET /api/v1/indexes/:bucket
```

### Create Index

```
POST /api/v1/indexes
```

**Body**:
```json
{
  "name": "idx_city_age",
  "bucket": "my-bucket",
  "fields": ["city", "age"],
  "condition": "type = 'user'"
}
```

### Get Index

```
GET /api/v1/indexes/:bucket/:name
```

### Drop Index

```
DELETE /api/v1/indexes/:bucket/:name
```

### Rebuild Index

```
POST /api/v1/indexes/:bucket/:name/rebuild
```

---

## XDCR

### List Remote Clusters

```
GET /api/v1/xdcr/clusters
```

### Add Remote Cluster

```
POST /api/v1/xdcr/clusters
```

**Body**:
```json
{
  "name": "dc-west",
  "hostname": "192.168.1.100",
  "port": 8091,
  "secure": false
}
```

### Remove Remote Cluster

```
DELETE /api/v1/xdcr/clusters/:name
```

### List Replications

```
GET /api/v1/xdcr/replications
```

### Create Replication

```
POST /api/v1/xdcr/replications
```

**Body**:
```json
{
  "source_bucket": "my-bucket",
  "target_cluster": "dc-west",
  "target_bucket": "my-bucket-replica",
  "conflict_resolution": "timestamp",
  "filter_expression": "type:airline",
  "batch_size": 500
}
```

### Pause / Resume / Delete Replication

```
POST   /api/v1/xdcr/replications/:id/pause
POST   /api/v1/xdcr/replications/:id/resume
DELETE /api/v1/xdcr/replications/:id
```

---

## Cluster

### Get Cluster Info

```
GET /api/v1/cluster
```

### List Nodes

```
GET /api/v1/cluster/nodes
```

### Add Node

```
POST /api/v1/cluster/nodes
```

**Body**:
```json
{
  "name": "node-2",
  "hostname": "192.168.1.101",
  "port": 8092
}
```

### Remove Node

```
DELETE /api/v1/cluster/nodes/:name
```

### Partition Map

```
GET /api/v1/cluster/partitions          # Full map
GET /api/v1/cluster/partitions/summary  # Summary per node
```

### Rebalance

```
GET  /api/v1/cluster/rebalance   # Get rebalance status
POST /api/v1/cluster/rebalance   # Trigger rebalance
```

### Failover

```
GET  /api/v1/cluster/failover                  # Get failover state
POST /api/v1/cluster/failover/config           # Update failover config
POST /api/v1/cluster/failover/reset            # Reset auto-failover quota
POST /api/v1/cluster/failover/:node            # Manual failover
POST /api/v1/cluster/failover/:node/recover    # Recover failed node
```

**Failover Config Body**:
```json
{
  "enabled": true,
  "timeout_secs": 120,
  "max_count": 3,
  "cooldown_secs": 30,
  "min_cluster_size": 2,
  "failover_on_data_loss": false
}
```

**Manual Failover Body**:
```json
{
  "failover_type": "graceful"
}
```

Options: `"graceful"` or `"hard"`

---

## System Endpoints

### Health Check

```
GET /health
```

**Response**: `{"status": "ok", "timestamp": "2026-02-06T12:00:00Z"}`

### Server Info

```
GET /
```

### Persistence Stats

```
GET /api/v1/persistence/stats
```

### Web UI

```
GET /ui
```

---

## Error Responses

All errors return JSON with the following format:

```json
{
  "error": "Document 'user-1' not found",
  "status": 404
}
```

| Status Code | Meaning |
|-------------|---------|
| 400 | Bad Request (invalid query, missing fields) |
| 404 | Not Found (bucket, scope, collection, or document) |
| 409 | Conflict (already exists, CAS mismatch) |
| 500 | Internal Server Error |
| 503 | Service Unavailable (XDCR connection error) |
