# Supported NoSQL Databases

The ASGP Unified DatabaseAgent supports **5 NoSQL databases** with intelligent LLM-based routing.

## Database Overview

| Database | Driver | Use Cases | Query Language |
|----------|--------|-----------|----------------|
| **MongoDB** | `pymongo` | Document queries, aggregations, nested data | JSON filter / aggregation pipeline |
| **Redis** | `redis-py` | Sessions, caching, leaderboards, real-time data | Redis commands (GET, HGETALL, ZRANGE…) |
| **Cassandra** | `cassandra-driver` | Time-series, IoT sensor data, user activity | CQL (SELECT only) |
| **DynamoDB** | `boto3` | User profiles, serverless apps, key lookups | GetItem / Query / Scan JSON |
| **Couchbase** | `couchbase` | Product catalogs, content management | N1QL (SELECT only) |

---

## Query Syntax Examples

### MongoDB
```json
// Filter query
{"_collection": "products", "category": "electronics"}

// Aggregation pipeline
{"_collection": "products", "pipeline": [
  {"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}}}
]}
```

### Redis
```
GET session:123
HGETALL session:12345
SCAN 0 MATCH session:* COUNT 100
ZREVRANGE leaderboard:daily 0 9 WITHSCORES
```

### Cassandra (CQL)
```sql
SELECT * FROM sensor_readings WHERE sensor_id = 'abc'
  AND timestamp > '2024-01-01 10:00:00'
```

### DynamoDB
```json
{"method": "GetItem", "params": {
  "TableName": "Users",
  "Key": {"user_id": {"S": "user-123"}}
}}
```

### Couchbase (N1QL)
```sql
SELECT * FROM products WHERE category = 'Electronics' LIMIT 100
```

---

## When to Use Which Database

| Scenario | Best Database | Why |
|----------|--------------|-----|
| Complex document queries with nested fields | MongoDB | Rich query operators, aggregation framework |
| Session lookups, cache hits | Redis | Sub-millisecond key-value access |
| Time-series sensor data with time-range filters | Cassandra | Partition-key + clustering-key optimized for time ranges |
| Single-item lookups by primary key | DynamoDB | Single-digit ms latency on key-value access |
| SQL-like queries on JSON documents | Couchbase | N1QL provides familiar SQL syntax |
| Aggregations (avg, sum, group by) | MongoDB | Powerful aggregation pipeline |
| Leaderboards and ranked data | Redis | Native sorted set support |
| High-write IoT ingestion | Cassandra | Designed for high write throughput |

---

## Configuration

All databases are configured in `config/db_domain.yaml`:

1. **`source_config`** — Connection credentials (use `${ENV_VAR}` syntax)
2. **`source_details`** — Schema metadata injected into LLM context
3. **`agent_config`** — Agent behavior, model, and source bindings

Environment variables go in `.env` at project root.

---

## Security

Every database tool enforces **read-only access** through multiple layers:

1. **Prompt Intent Analysis** — Blocks natural-language mutation requests
2. **Restricted Collection Guard** — Blocks access to admin/auth tables
3. **Native Query Validation** — Each tool validates its native query format:
   - MongoDB: Recursive operator whitelist
   - Redis: Command whitelist/blocklist
   - Cassandra: CQL keyword scanner (SELECT only)
   - DynamoDB: Operation method whitelist (GetItem/Query/Scan only)
   - Couchbase: N1QL keyword scanner (SELECT only)
4. **JIT Connections** — No persistent connections; open → execute → close
