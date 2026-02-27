
# asgp/agents/database_agent.py

"""
Unified Multi-NoSQL DatabaseAgent with LLM-based routing
and multi-layer security guardrails.

Supports: MongoDB, Redis, Cassandra, DynamoDB, Couchbase
"""

from typing import Dict, Optional, Any, List
import json
import time
import re

from asgp.tools.nosql_tool import NoSQLTool
from asgp.tools.redis_tool import RedisTool
from asgp.tools.cassandra_tool import CassandraTool
from asgp.tools.dynamodb_tool import DynamoDBTool
from asgp.tools.couchbase_tool import CouchbaseTool
from asgp.config.schemas import AgentConfig, SourceConfig, SourceDetails
from asgp.schemas.response import SourceResult, DBRoutingDecision
from asgp.schemas.enums import SourceType, ResultStatus, DBDecision
from asgp.providers.litellm_provider import LiteLLMProvider
from asgp.exceptions import (
    RoutingDecisionError,
    InvalidMongoFilterError,
    SourceBindingError,
    UnsafeQueryError,
    LLMParseError,
    QueryExecutionError
)


class DatabaseAgent:
    """
    Unified NoSQL database agent with LLM-based routing.
    Routes queries to MongoDB, Redis, Cassandra, DynamoDB, or Couchbase
    based on query semantics and available schemas.
    """

    # ==================== Security Constants ====================

    # MongoDB unsafe operators
    UNSAFE_MONGO_OPERATORS = {
        '$where', '$function', '$accumulator',
        '$set', '$unset', '$push', '$pull', '$inc', '$mul',
        '$rename', '$setOnInsert', '$min', '$max',
        '$addToSet', '$pop', '$pullAll', '$bit'
    }

    # Redis unsafe commands
    UNSAFE_REDIS_COMMANDS = {
        'FLUSHDB', 'FLUSHALL', 'DEL', 'SET', 'SETEX',
        'HSET', 'LPUSH', 'RPUSH', 'ZADD', 'CONFIG', 'SHUTDOWN',
        'EVAL', 'SCRIPT', 'SETNX', 'MSET',
    }

    # Cassandra unsafe keywords
    UNSAFE_CASSANDRA_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE',
        'ALTER', 'CREATE', 'GRANT', 'BATCH',
    }

    # DynamoDB unsafe operations
    UNSAFE_DYNAMODB_OPERATIONS = {
        'PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem',
        'CreateTable', 'DeleteTable', 'UpdateTable',
    }

    # Couchbase unsafe keywords
    UNSAFE_COUCHBASE_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'UPSERT', 'MERGE',
        'DROP', 'CREATE', 'ALTER',
    }

    # Mutation keywords in natural language (GUARDRAIL LAYER 1)
    MUTATION_KEYWORDS = {
        'update', 'delete', 'remove', 'insert', 'add', 'modify',
        'change', 'drop', 'create', 'alter', 'set', 'increment'
    }

    # Restricted collections / tables (GUARDRAIL LAYER 2)
    RESTRICTED_COLLECTIONS = {
        'admin', 'system', 'users', 'auth', 'credentials'
    }

    def __init__(
        self,
        agent_cfg: AgentConfig,
        source_cfgs: Dict[str, SourceConfig],
        source_details: Dict[str, SourceDetails]
    ):
        self.agent_cfg = agent_cfg
        self.source_cfgs = source_cfgs
        self.source_details = source_details

        # Initialize all 5 tools (JIT — no persistent connections)
        self.mongo_tool = NoSQLTool()
        self.redis_tool = RedisTool()
        self.cassandra_tool = CassandraTool()
        self.dynamodb_tool = DynamoDBTool()
        self.couchbase_tool = CouchbaseTool()

        print(f"✓ DatabaseAgent initialized")
        print(f"   Agent: {agent_cfg.name}")
        print(f"   Sources: {list(source_cfgs.keys())}")
        print(f"   Model: {agent_cfg.model}")
        print(f"   Security: Multi-layer guardrails enabled")

    # ==================== Main Execution Pipeline ====================

    async def execute(
        self,
        prompt: str,
        trace_id: str
    ) -> SourceResult:
        """
        Main execution pipeline with multi-layer security guardrails.
        Routes to the correct database based on LLM decision.
        """
        t0 = time.time()

        try:
            # GUARDRAIL LAYER 1: Prompt Intent Analysis
            if self._detect_mutation_intent(prompt):
                return self._build_rejection_result(
                    "Modification request detected. This agent only performs read-only queries.",
                    trace_id,
                    int((time.time() - t0) * 1000)
                )

            # GUARDRAIL LAYER 2: Restricted Area Check
            if self._detect_restricted_access(prompt):
                return self._build_rejection_result(
                    "Access to restricted collections is not allowed.",
                    trace_id,
                    int((time.time() - t0) * 1000)
                )

            # Step 1: Build schema context for LLM
            schema_context = self._build_schema_context()

            # Step 2: LLM routing + query generation
            decision = await self._call_llm_router(prompt, schema_context)

            print(f"\n🤖 LLM Routing Decision:")
            print(f"   DB Type: {decision.db_type}")
            print(f"   Source: {decision.source_name}")
            print(f"   Confidence: {decision.confidence:.2f}")
            print(f"   Explanation: {decision.explanation}")

            # Step 3: Validate decision
            self._validate_decision(decision)

            # Step 4–5: Dispatch to correct tool
            data = []
            collection_or_table = "N/A"

            if decision.db_type == DBDecision.mongodb:
                collection_name, query_obj = self._parse_mongo_query(decision.query)
                collection_or_table = collection_name

                print(f"   Collection: {collection_name}")
                if isinstance(query_obj, list):
                    print(f"   Pipeline Stages: {len(query_obj)}")
                else:
                    print(f"   Filter: {json.dumps(query_obj, indent=2)}")

                # GUARDRAIL LAYER 3: Collection Access
                if collection_name in self.RESTRICTED_COLLECTIONS:
                    raise SourceBindingError(f"Collection '{collection_name}' is restricted")

                # GUARDRAIL LAYER 4: Query Safety
                if isinstance(query_obj, list):
                    self._validate_pipeline_safety(query_obj)
                else:
                    self._validate_filter_safety(query_obj)

                data = self._execute_mongodb(collection_name, query_obj, decision.source_name)

            elif decision.db_type == DBDecision.redis:
                print(f"   Command: {decision.query}")
                self._validate_redis_command(decision.query)
                data = self._execute_redis(decision.query, decision.source_name)
                collection_or_table = "redis"

            elif decision.db_type == DBDecision.cassandra:
                print(f"   CQL: {decision.query}")
                self._validate_cql_safety(decision.query)
                data = self._execute_cassandra(decision.query, decision.source_name)
                collection_or_table = "cassandra"

            elif decision.db_type == DBDecision.dynamodb:
                print(f"   Operation: {decision.query[:200]}")
                self._validate_dynamodb_operation(decision.query)
                data = self._execute_dynamodb(decision.query, decision.source_name)
                collection_or_table = "dynamodb"

            elif decision.db_type == DBDecision.couchbase:
                print(f"   N1QL: {decision.query}")
                self._validate_n1ql_safety(decision.query)
                data = self._execute_couchbase(decision.query, decision.source_name)
                collection_or_table = "couchbase"

            else:
                raise RoutingDecisionError(
                    f"Unsupported db_type: {decision.db_type}"
                )

            # Step 6: Build result
            latency_ms = int((time.time() - t0) * 1000)
            return self._build_result(decision, data, latency_ms, collection_or_table)

        except Exception as e:
            latency_ms = int((time.time() - t0) * 1000)
            return self._handle_error(e, trace_id, latency_ms)

    # ==================== Guardrail Methods ====================

    def _detect_mutation_intent(self, prompt: str) -> bool:
        """GUARDRAIL LAYER 1: Detect mutation keywords in prompt"""
        prompt_lower = prompt.lower()

        for keyword in self.MUTATION_KEYWORDS:
            if keyword in prompt_lower:
                if any(word in prompt_lower for word in [
                    'data', 'record', 'document', 'collection',
                    'database', 'table', 'key', 'item', 'bucket'
                ]):
                    print(f"🚫 GUARDRAIL 1 BLOCKED: Mutation keyword '{keyword}' detected")
                    return True

        return False

    def _detect_restricted_access(self, prompt: str) -> bool:
        """GUARDRAIL LAYER 2: Detect restricted collection access attempts"""
        prompt_lower = prompt.lower()

        for restricted in self.RESTRICTED_COLLECTIONS:
            if restricted in prompt_lower:
                print(f"🚫 GUARDRAIL 2 BLOCKED: Restricted collection '{restricted}' mentioned")
                return True

        return False

    # ==================== Schema Context ====================

    def _build_schema_context(self) -> str:
        """Render all source_details as compact JSON for LLM context injection."""
        context = {}

        for source_name, details in self.source_details.items():
            source_cfg = self.source_cfgs.get(source_name)
            driver = source_cfg.driver.value if source_cfg and source_cfg.driver else "unknown"

            source_info = {"type": "nosql", "driver": driver}

            # MongoDB / Couchbase collections
            if details.collections:
                source_info["collections"] = {}
                for collection in details.collections:
                    field_info = []
                    for f in collection.fields:
                        field_str = f"{f.name}:{f.type}"
                        if f.ref:
                            field_str += f"(ref→{f.ref})"
                        if f.range:
                            field_str += f"[{f.range}]"
                        if f.description:
                            field_str += f" - {f.description}"
                        field_info.append(field_str)
                    source_info["collections"][collection.name] = {
                        "description": collection.description,
                        "fields": field_info
                    }

            # Redis key patterns
            if details.key_patterns:
                source_info["key_patterns"] = []
                for kp in details.key_patterns:
                    pattern_info = {
                        "pattern": kp.pattern,
                        "value_type": kp.value_type,
                        "description": kp.description,
                    }
                    if kp.fields:
                        pattern_info["fields"] = kp.fields
                    source_info["key_patterns"].append(pattern_info)

            # Cassandra tables
            if details.cassandra_tables:
                source_info["tables"] = {}
                for table in details.cassandra_tables:
                    col_info = []
                    for c in table.columns:
                        col_str = f"{c.name}:{c.type}"
                        if c.partition_key:
                            col_str += " [PARTITION KEY]"
                        if c.clustering_key:
                            col_str += " [CLUSTERING KEY]"
                        col_info.append(col_str)
                    source_info["tables"][table.name] = {
                        "description": table.description,
                        "columns": col_info
                    }

            # DynamoDB tables
            if details.dynamodb_tables:
                source_info["tables"] = {}
                for table in details.dynamodb_tables:
                    keys_info = {}
                    for key_role, key_meta in table.keys.items():
                        keys_info[key_role] = {
                            "name": key_meta.name,
                            "type": key_meta.type
                        }
                    attr_info = []
                    for a in table.attributes:
                        attr_str = f"{a.name}:{a.type}"
                        if a.values:
                            attr_str += f" values={a.values}"
                        attr_info.append(attr_str)
                    source_info["tables"][table.name] = {
                        "description": table.description,
                        "keys": keys_info,
                        "attributes": attr_info
                    }

            context[source_name] = source_info

        return json.dumps(context, indent=2)

    # ==================== LLM Router ====================

    async def _call_llm_router(
        self,
        prompt: str,
        schema_context: str
    ) -> DBRoutingDecision:
        """
        Call LLM to route query to correct database and generate native query.
        """
        available_sources = list(self.source_cfgs.keys())
        source_drivers = {
            name: cfg.driver.value
            for name, cfg in self.source_cfgs.items()
            if cfg.driver
        }

        system_message = f"""
{self.agent_cfg.system_prompt}

AVAILABLE SOURCES AND SCHEMA:
{schema_context}

SOURCE → DRIVER MAPPING:
{json.dumps(source_drivers, indent=2)}

OUTPUT FORMAT:
You must return a JSON object with these exact fields:
- db_type: one of "mongodb" | "redis" | "cassandra" | "dynamodb" | "couchbase"
- source_name: one of {available_sources}
- query: native query for the chosen database (see format rules below)
- explanation: your reasoning
- confidence: 0.0 to 1.0

DATABASE SELECTION GUIDELINES:
- MongoDB: Complex document queries, nested object filtering, aggregations
- Redis: Key lookups, pattern matching, sorted sets, session/cache data
- Cassandra: Time-series queries, partition key filtering, time-range scans
- DynamoDB: Single-item lookups by key, partition key queries, AWS-native
- Couchbase: N1QL queries, full-text search, JSON document queries

QUERY FORMAT BY DATABASE:

═══ MongoDB ═══
JSON string with _collection field. For filters:
  {{"_collection": "products", "category": "electronics"}}
For aggregations (average, count, sum, group by, top N):
  {{"_collection": "products", "pipeline": [{{"$group": {{"_id": "$category", "avg": {{"$avg": "$price"}}}}}}]}}
Safe operators: $match, $group, $project, $sort, $limit, $count, $sum, $avg, $min, $max, $eq, $gt, $lt, $gte, $lte, $ne, $in, $nin, $and, $or, $not, $regex
NEVER use: $where, $function, $set, $unset, $push, $pull, $inc

═══ Redis ═══
Command string:
  "GET session:123"
  "HGETALL session:12345"
  "SCAN 0 MATCH session:* COUNT 100"
  "ZREVRANGE leaderboard:daily 0 9 WITHSCORES"
  "KEYS cart:*"
Only read commands: GET, MGET, HGETALL, HGET, HMGET, KEYS, SCAN, ZRANGE, ZREVRANGE, LRANGE, SMEMBERS, TYPE, TTL, EXISTS

═══ Cassandra ═══
CQL SELECT statement (always filter on partition key):
  "SELECT * FROM sensor_readings WHERE sensor_id = 'abc' AND timestamp > '2024-01-01 10:00:00'"
  "SELECT * FROM user_activity WHERE user_id = 'xyz' ORDER BY event_time DESC LIMIT 50"
ONLY SELECT, never INSERT/UPDATE/DELETE/DROP

═══ DynamoDB ═══
JSON string with method + params:
  {{"method": "GetItem", "params": {{"TableName": "Users", "Key": {{"user_id": {{"S": "user-123"}}}}}}}}
  {{"method": "Query", "params": {{"TableName": "Orders", "KeyConditionExpression": "user_id = :uid", "ExpressionAttributeValues": {{":uid": {{"S": "user-456"}}}}}}}}
  {{"method": "Scan", "params": {{"TableName": "Users", "FilterExpression": "subscription_tier = :tier", "ExpressionAttributeValues": {{":tier": {{"S": "pro"}}}}}}}}
Only methods: GetItem, Query, Scan

═══ Couchbase ═══
N1QL SELECT statement:
  "SELECT * FROM products WHERE category = 'Electronics' LIMIT 100"
  "SELECT * FROM products WHERE 'premium' IN tags"
ONLY SELECT, never INSERT/UPDATE/DELETE/UPSERT

WHEN TO USE AGGREGATION vs FILTER (MongoDB):
- FILTER: "show me", "find", "get", "list", "retrieve"
- AGGREGATION with pipeline: "average", "sum", "count", "total", "how many", "max", "min", "group by", "top N"

IMPORTANT:
- This is a READ-ONLY agent — NEVER generate mutation queries
- Use field names exactly as shown in schema
- Match db_type to the driver of the chosen source_name
"""

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt}
        ]

        try:
            response_json = await LiteLLMProvider.complete_json(
                messages=messages,
                model=self.agent_cfg.model,
                max_tokens=self.agent_cfg.max_tokens,
                temperature=self.agent_cfg.temperature
            )

            required_fields = ['db_type', 'source_name', 'query', 'explanation', 'confidence']
            missing = [f for f in required_fields if f not in response_json]
            if missing:
                raise LLMParseError(f"LLM response missing fields: {missing}")

            return DBRoutingDecision(
                db_type=DBDecision(response_json['db_type']),
                source_name=response_json['source_name'],
                query=response_json['query'],
                explanation=response_json['explanation'],
                confidence=float(response_json['confidence'])
            )

        except LLMParseError:
            raise
        except Exception as e:
            raise LLMParseError(f"Failed to parse LLM response: {str(e)}")

    # ==================== Decision Validation ====================

    def _validate_decision(self, decision: DBRoutingDecision) -> None:
        """Verify LLM decision is valid for the chosen source."""
        if decision.source_name not in self.source_cfgs:
            raise SourceBindingError(
                f"LLM chose unknown source '{decision.source_name}'. "
                f"Available: {list(self.source_cfgs.keys())}"
            )

        source_cfg = self.source_cfgs[decision.source_name]
        if source_cfg.driver and decision.db_type.value != source_cfg.driver.value:
            raise RoutingDecisionError(
                f"LLM chose db_type={decision.db_type.value} but source "
                f"'{decision.source_name}' has driver={source_cfg.driver.value}"
            )

        if decision.confidence < 0.3:
            print(f"⚠️ Warning: Low confidence decision ({decision.confidence:.2f})")

    # ==================== Query Parsing (MongoDB) ====================

    def _parse_mongo_query(self, query_str: str):
        """Parse MongoDB query string to extract collection and filter/pipeline."""
        try:
            query_obj = json.loads(query_str)
        except json.JSONDecodeError as e:
            raise InvalidMongoFilterError(
                f"Invalid MongoDB query JSON: {query_str[:200]}... Error: {e}"
            )

        # Aggregation pipeline
        if isinstance(query_obj, dict) and "_collection" in query_obj and "pipeline" in query_obj:
            collection_name = query_obj["_collection"]
            pipeline = query_obj["pipeline"]
            if not isinstance(pipeline, list):
                raise InvalidMongoFilterError("Aggregation pipeline must be a list of stages.")
            return collection_name, pipeline

        # Detect misplaced aggregation operators
        AGGREGATION_OPERATORS = {
            "$group", "$sum", "$avg", "$count", "$project",
            "$match", "$sort", "$limit", "$unwind", "$lookup", "$addFields"
        }
        if isinstance(query_obj, dict) and "_collection" in query_obj:
            for key in query_obj:
                if key in AGGREGATION_OPERATORS:
                    raise InvalidMongoFilterError(
                        f"Invalid query: Top-level aggregation operator '{key}' found outside of 'pipeline'. "
                        "Aggregation queries must use the 'pipeline' key with a list of stages."
                    )
            collection_name = query_obj.pop("_collection")
            return collection_name, query_obj

        raise InvalidMongoFilterError(
            f"MongoDB query must include '_collection' field. Got: {query_str[:200]}..."
        )

    # ==================== Safety Validation ====================

    def _validate_pipeline_safety(self, pipeline: list) -> None:
        """GUARDRAIL LAYER 4: Validate aggregation pipeline safety."""
        for stage in pipeline:
            if not isinstance(stage, dict):
                raise UnsafeQueryError(f"Pipeline stage must be a dict, got: {type(stage)}")
            self._validate_filter_safety(stage)
        print(f"✓ GUARDRAIL 4 PASSED: Pipeline safety validated ({len(pipeline)} stages)")

    def _validate_filter_safety(self, filter_dict: Dict[str, Any]) -> None:
        """GUARDRAIL LAYER 4: Recursive MongoDB filter validation."""
        def check_recursive(obj: Any, path: str = "root", depth: int = 0):
            if depth > 10:
                raise UnsafeQueryError(f"Query nesting too deep at {path} (max: 10)")
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in self.UNSAFE_MONGO_OPERATORS:
                        raise UnsafeQueryError(f"Unsafe MongoDB operator '{key}' at {path}")
                    if key == '$regex' and ('eval' in str(value) or 'function' in str(value)):
                        raise UnsafeQueryError(f"Unsafe regex pattern at {path}")
                    check_recursive(value, f"{path}.{key}", depth + 1)
            elif isinstance(obj, list):
                if len(obj) > 100:
                    raise UnsafeQueryError(f"Array too large at {path} (size: {len(obj)}, max: 100)")
                for i, item in enumerate(obj):
                    check_recursive(item, f"{path}[{i}]", depth + 1)

        check_recursive(filter_dict)
        print(f"✓ GUARDRAIL 4 PASSED: MongoDB query safety validated")

    def _validate_redis_command(self, command_str: str) -> None:
        """GUARDRAIL: Validate Redis command is read-only."""
        parts = command_str.strip().split()
        if not parts:
            raise UnsafeQueryError("Empty Redis command")
        cmd = parts[0].upper()
        if cmd in self.UNSAFE_REDIS_COMMANDS:
            raise UnsafeQueryError(f"Unsafe Redis command '{cmd}' blocked")
        print(f"✓ GUARDRAIL PASSED: Redis command '{cmd}' is read-only")

    def _validate_cql_safety(self, cql: str) -> None:
        """GUARDRAIL: Validate CQL is SELECT-only."""
        tokens = re.findall(r'\b([A-Z_]+)\b', cql.upper())
        for token in tokens:
            if token in self.UNSAFE_CASSANDRA_KEYWORDS:
                raise UnsafeQueryError(f"Unsafe CQL keyword '{token}' blocked")
        if tokens and tokens[0] != 'SELECT':
            raise UnsafeQueryError(f"CQL must start with SELECT, got '{tokens[0]}'")
        print(f"✓ GUARDRAIL PASSED: CQL is read-only")

    def _validate_dynamodb_operation(self, operation_json: str) -> None:
        """GUARDRAIL: Validate DynamoDB operation is read-only."""
        try:
            op = json.loads(operation_json)
        except json.JSONDecodeError:
            raise UnsafeQueryError("Invalid DynamoDB operation JSON")
        method = op.get('method', '')
        if method in self.UNSAFE_DYNAMODB_OPERATIONS:
            raise UnsafeQueryError(f"Unsafe DynamoDB operation '{method}' blocked")
        if method not in {'GetItem', 'Query', 'Scan'}:
            raise UnsafeQueryError(f"Unknown DynamoDB operation '{method}'")
        print(f"✓ GUARDRAIL PASSED: DynamoDB operation '{method}' is read-only")

    def _validate_n1ql_safety(self, n1ql: str) -> None:
        """GUARDRAIL: Validate N1QL is SELECT-only."""
        tokens = re.findall(r'\b([A-Z_]+)\b', n1ql.upper())
        for token in tokens:
            if token in self.UNSAFE_COUCHBASE_KEYWORDS:
                raise UnsafeQueryError(f"Unsafe N1QL keyword '{token}' blocked")
        if tokens and tokens[0] != 'SELECT':
            raise UnsafeQueryError(f"N1QL must start with SELECT, got '{tokens[0]}'")
        print(f"✓ GUARDRAIL PASSED: N1QL is read-only")

    # ==================== Execution Methods ====================

    def _execute_mongodb(
        self,
        collection_name: str,
        query_obj: Any,
        source_name: str,
        limit: int = 100
    ) -> list:
        """Execute MongoDB query or aggregation using NoSQLTool."""
        source_cfg = self.source_cfgs[source_name]
        try:
            if isinstance(query_obj, list):
                return self.mongo_tool.aggregate_mongo(
                    pipeline=query_obj,
                    collection_name=collection_name,
                    source_cfg=source_cfg,
                    limit=limit
                )
            return self.mongo_tool.fetch_mongo(
                filter_dict=query_obj,
                collection_name=collection_name,
                source_cfg=source_cfg,
                limit=limit
            )
        except Exception as e:
            raise QueryExecutionError(
                f"MongoDB query failed on collection '{collection_name}': {str(e)}"
            )

    def _execute_redis(self, command_str: str, source_name: str) -> list:
        """Execute Redis command using RedisTool."""
        source_cfg = self.source_cfgs[source_name]
        try:
            return self.redis_tool.fetch_redis(
                command_str=command_str,
                source_cfg=source_cfg
            )
        except Exception as e:
            raise QueryExecutionError(f"Redis command failed: {str(e)}")

    def _execute_cassandra(
        self, cql_query: str, source_name: str, limit: int = 100
    ) -> list:
        """Execute Cassandra CQL query using CassandraTool."""
        source_cfg = self.source_cfgs[source_name]
        try:
            return self.cassandra_tool.fetch_cassandra(
                cql_query=cql_query,
                source_cfg=source_cfg,
                limit=limit
            )
        except Exception as e:
            raise QueryExecutionError(f"Cassandra query failed: {str(e)}")

    def _execute_dynamodb(self, operation_json: str, source_name: str) -> list:
        """Execute DynamoDB operation using DynamoDBTool."""
        source_cfg = self.source_cfgs[source_name]
        try:
            return self.dynamodb_tool.fetch_dynamodb(
                operation_json=operation_json,
                source_cfg=source_cfg
            )
        except Exception as e:
            raise QueryExecutionError(f"DynamoDB operation failed: {str(e)}")

    def _execute_couchbase(
        self, n1ql_query: str, source_name: str, limit: int = 100
    ) -> list:
        """Execute Couchbase N1QL query using CouchbaseTool."""
        source_cfg = self.source_cfgs[source_name]
        try:
            return self.couchbase_tool.fetch_couchbase(
                n1ql_query=n1ql_query,
                source_cfg=source_cfg,
                limit=limit
            )
        except Exception as e:
            raise QueryExecutionError(f"Couchbase query failed: {str(e)}")

    # ==================== Result Builders ====================

    def _build_result(
        self,
        decision: DBRoutingDecision,
        data: list,
        latency_ms: int,
        collection_name: str
    ) -> SourceResult:
        """Build successful SourceResult from execution."""
        return SourceResult(
            source_name=decision.source_name,
            source_type=SourceType.nosql,
            agent_name=self.agent_cfg.name,
            status=ResultStatus.success,
            data=data,
            raw_query=decision.query,
            confidence=decision.confidence,
            latency_ms=latency_ms,
            row_count=len(data),
            metadata={
                "db_type": decision.db_type.value,
                "collection": collection_name,
                "explanation": decision.explanation
            }
        )

    def _build_rejection_result(
        self,
        reason: str,
        trace_id: str,
        latency_ms: int
    ) -> SourceResult:
        """Build result for queries rejected by guardrails."""
        source_name = list(self.source_cfgs.keys())[0] if self.source_cfgs else "unknown"

        return SourceResult(
            source_name=source_name,
            source_type=SourceType.nosql,
            agent_name=self.agent_cfg.name,
            status=ResultStatus.error,
            data=[],
            raw_query=None,
            confidence=0.0,
            latency_ms=latency_ms,
            row_count=0,
            error_detail=f"BLOCKED: {reason}",
            metadata={
                "trace_id": trace_id,
                "blocked_by": "guardrails"
            }
        )

    def _handle_error(
        self,
        error: Exception,
        trace_id: str,
        latency_ms: int
    ) -> SourceResult:
        """Convert exception to error SourceResult."""
        source_name = list(self.source_cfgs.keys())[0] if self.source_cfgs else "unknown"

        error_type = error.__class__.__name__
        error_msg = str(error)

        print(f"\n❌ Error in DatabaseAgent:")
        print(f"   Type: {error_type}")
        print(f"   Message: {error_msg[:200]}")

        return SourceResult(
            source_name=source_name,
            source_type=SourceType.nosql,
            agent_name=self.agent_cfg.name,
            status=ResultStatus.error,
            data=[],
            raw_query=None,
            confidence=0.0,
            latency_ms=latency_ms,
            row_count=0,
            error_detail=f"[{error_type}] {error_msg[:400]}",
            metadata={
                "trace_id": trace_id,
                "error_type": error_type
            }
        )
