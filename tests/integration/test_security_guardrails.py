# tests/integration/test_security_guardrails.py
"""
Tests that security guardrails block mutation operations for all 5 databases.
These tests do NOT require actual database connections — they validate the
agent-level and tool-level security validation logic.
"""
import pytest
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from asgp.tools.redis_tool import RedisTool, RedisSecurityError
from asgp.tools.cassandra_tool import CassandraTool, CassandraSecurityError
from asgp.tools.dynamodb_tool import DynamoDBTool, DynamoDBSecurityError
from asgp.tools.couchbase_tool import CouchbaseTool, CouchbaseSecurityError
from asgp.exceptions import UnsafeQueryError


# ═══════════════════════════════════════════════════════
# Redis Security Tests
# ═══════════════════════════════════════════════════════

class TestRedisGuardrails:
    """Test that RedisTool blocks all write commands."""

    def setup_method(self):
        self.tool = RedisTool()

    @pytest.mark.parametrize("cmd", [
        "FLUSHDB", "FLUSHALL", "DEL key1",
        "SET mykey value", "SETEX key 60 value",
        "HSET hash field value", "LPUSH list val",
        "RPUSH list val", "ZADD zset 1 member",
        "CONFIG SET maxmemory 1gb", "SHUTDOWN",
        "EVAL 'return 1' 0", "SCRIPT FLUSH",
    ])
    def test_blocks_write_commands(self, cmd):
        """Write commands must raise RedisSecurityError."""
        with pytest.raises(RedisSecurityError):
            self.tool._validate_command(cmd.split()[0].upper())

    @pytest.mark.parametrize("cmd", [
        "GET", "MGET", "HGETALL", "HGET", "HMGET",
        "KEYS", "SCAN", "ZRANGE", "ZREVRANGE",
        "LRANGE", "SMEMBERS", "TYPE", "TTL", "EXISTS",
    ])
    def test_allows_read_commands(self, cmd):
        """Read commands must pass validation."""
        self.tool._validate_command(cmd)  # Should not raise


# ═══════════════════════════════════════════════════════
# Cassandra Security Tests
# ═══════════════════════════════════════════════════════

class TestCassandraGuardrails:
    """Test that CassandraTool blocks all mutation CQL."""

    def setup_method(self):
        self.tool = CassandraTool()

    @pytest.mark.parametrize("cql", [
        "INSERT INTO users (id, name) VALUES ('1', 'Alice')",
        "UPDATE users SET name = 'Bob' WHERE id = '1'",
        "DELETE FROM users WHERE id = '1'",
        "DROP TABLE users",
        "TRUNCATE users",
        "ALTER TABLE users ADD email text",
        "CREATE TABLE new_table (id text PRIMARY KEY)",
        "GRANT SELECT ON users TO user1",
        "BATCH BEGIN INSERT INTO users (id) VALUES ('1') APPLY BATCH",
    ])
    def test_blocks_mutation_cql(self, cql):
        """Mutation CQL must raise CassandraSecurityError."""
        with pytest.raises(CassandraSecurityError):
            self.tool._validate_cql(cql)

    def test_allows_select_cql(self):
        """SELECT queries must pass validation."""
        self.tool._validate_cql("SELECT * FROM sensor_readings WHERE sensor_id = 'abc'")

    def test_blocks_empty_cql(self):
        """Empty CQL must raise error."""
        from asgp.tools.cassandra_tool import CassandraQueryError
        with pytest.raises(CassandraQueryError):
            self.tool._validate_cql("")


# ═══════════════════════════════════════════════════════
# DynamoDB Security Tests
# ═══════════════════════════════════════════════════════

class TestDynamoDBGuardrails:
    """Test that DynamoDBTool blocks all write operations."""

    def setup_method(self):
        self.tool = DynamoDBTool()

    @pytest.mark.parametrize("method", [
        "PutItem", "UpdateItem", "DeleteItem",
        "BatchWriteItem", "CreateTable", "DeleteTable", "UpdateTable",
    ])
    def test_blocks_write_operations(self, method):
        """Write operations must raise DynamoDBSecurityError."""
        with pytest.raises(DynamoDBSecurityError):
            self.tool._validate_operation(method)

    @pytest.mark.parametrize("method", ["GetItem", "Query", "Scan"])
    def test_allows_read_operations(self, method):
        """Read operations must pass validation."""
        self.tool._validate_operation(method)  # Should not raise

    def test_dynamodb_deserialization(self):
        """DynamoDB type descriptors should deserialize correctly."""
        item = {
            "user_id": {"S": "user-123"},
            "age": {"N": "30"},
            "active": {"BOOL": True},
            "tags": {"L": [{"S": "admin"}, {"S": "user"}]},
            "metadata": {"M": {"role": {"S": "dev"}}},
            "nothing": {"NULL": True},
        }
        result = self.tool._deserialize_item(item)
        assert result["user_id"] == "user-123"
        assert result["age"] == 30
        assert result["active"] is True
        assert result["tags"] == ["admin", "user"]
        assert result["metadata"] == {"role": "dev"}
        assert result["nothing"] is None


# ═══════════════════════════════════════════════════════
# Couchbase Security Tests
# ═══════════════════════════════════════════════════════

class TestCouchbaseGuardrails:
    """Test that CouchbaseTool blocks all mutation N1QL."""

    def setup_method(self):
        self.tool = CouchbaseTool()

    @pytest.mark.parametrize("n1ql", [
        "INSERT INTO products (KEY, VALUE) VALUES ('p1', {'name': 'Test'})",
        "UPDATE products SET name = 'New' WHERE id = 'p1'",
        "DELETE FROM products WHERE id = 'p1'",
        "UPSERT INTO products (KEY, VALUE) VALUES ('p1', {})",
        "MERGE INTO products USING source ON ...",
        "DROP INDEX idx_name ON products",
        "CREATE INDEX idx_name ON products(name)",
        "ALTER INDEX idx_name ON products",
    ])
    def test_blocks_mutation_n1ql(self, n1ql):
        """Mutation N1QL must raise CouchbaseSecurityError."""
        with pytest.raises(CouchbaseSecurityError):
            self.tool._validate_n1ql(n1ql)

    def test_allows_select_n1ql(self):
        """SELECT queries must pass validation."""
        self.tool._validate_n1ql("SELECT * FROM products WHERE category = 'Electronics' LIMIT 100")


# ═══════════════════════════════════════════════════════
# Enum Tests
# ═══════════════════════════════════════════════════════

class TestEnums:
    """Test that all enum values exist."""

    def test_db_decision_values(self):
        from asgp.schemas.enums import DBDecision
        expected = {'sql', 'mongodb', 'redis', 'cassandra', 'dynamodb', 'couchbase'}
        actual = {e.value for e in DBDecision}
        assert expected == actual

    def test_db_driver_values(self):
        from asgp.schemas.enums import DBDriver
        expected = {'postgresql', 'mysql', 'sqlite', 'mongodb', 'redis',
                    'cassandra', 'dynamodb', 'couchbase'}
        actual = {e.value for e in DBDriver}
        assert expected == actual


# ═══════════════════════════════════════════════════════
# Config Loading Test
# ═══════════════════════════════════════════════════════

class TestConfigLoading:
    """Test YAML config parsing (with mock env vars)."""

    def test_yaml_loads_all_sources(self, monkeypatch):
        """All 5 sources should parse from YAML."""
        # Set required env vars
        env_vars = {
            'MONGO_URI': 'mongodb://localhost:27017/',
            'MONGO_DB': 'test',
            'REDIS_URI': 'redis://localhost:6379/0',
            'CASSANDRA_HOSTS': '127.0.0.1',
            'CASSANDRA_KEYSPACE': 'test_ks',
            'CASSANDRA_USER': 'user',
            'CASSANDRA_PASSWORD': 'pass',
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY': 'testkey',
            'AWS_SECRET_KEY': 'testsecret',
            'DYNAMODB_ENDPOINT': 'http://localhost:8000',
            'COUCHBASE_URI': 'couchbase://localhost',
            'COUCHBASE_USER': 'admin',
            'COUCHBASE_PASSWORD': 'password',
            'COUCHBASE_BUCKET': 'default',
        }
        for k, v in env_vars.items():
            monkeypatch.setenv(k, v)

        from asgp.config.registry import ConfigRegistry
        registry = ConfigRegistry.load(['config/db_domain.yaml'])

        assert 'product_mongo' in registry.sources
        assert 'session_redis' in registry.sources
        assert 'timeseries_cassandra' in registry.sources
        assert 'users_dynamodb' in registry.sources
        assert 'catalog_couchbase' in registry.sources

        assert len(registry.agents) == 1
        agent = registry.agents['database_agent']
        assert len(agent.source_bindings) == 5

    def test_source_details_schemas(self, monkeypatch):
        """Source details should contain correct schema types."""
        env_vars = {
            'MONGO_URI': 'mongodb://localhost:27017/',
            'MONGO_DB': 'test',
            'REDIS_URI': 'redis://localhost:6379/0',
            'CASSANDRA_HOSTS': '127.0.0.1',
            'CASSANDRA_KEYSPACE': 'test_ks',
            'CASSANDRA_USER': 'user',
            'CASSANDRA_PASSWORD': 'pass',
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY': 'testkey',
            'AWS_SECRET_KEY': 'testsecret',
            'DYNAMODB_ENDPOINT': 'http://localhost:8000',
            'COUCHBASE_URI': 'couchbase://localhost',
            'COUCHBASE_USER': 'admin',
            'COUCHBASE_PASSWORD': 'password',
            'COUCHBASE_BUCKET': 'default',
        }
        for k, v in env_vars.items():
            monkeypatch.setenv(k, v)

        from asgp.config.registry import ConfigRegistry
        registry = ConfigRegistry.load(['config/db_domain.yaml'])

        # MongoDB should have collections
        mongo_details = registry.source_details['product_mongo']
        assert len(mongo_details.collections) == 3

        # Redis should have key_patterns
        redis_details = registry.source_details['session_redis']
        assert len(redis_details.key_patterns) == 3

        # Cassandra should have cassandra_tables
        cass_details = registry.source_details['timeseries_cassandra']
        assert len(cass_details.cassandra_tables) == 2

        # DynamoDB should have dynamodb_tables
        ddb_details = registry.source_details['users_dynamodb']
        assert len(ddb_details.dynamodb_tables) == 2

        # Couchbase should have collections
        cb_details = registry.source_details['catalog_couchbase']
        assert len(cb_details.collections) == 1
