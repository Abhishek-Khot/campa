# asgp/exceptions.py
"""
Custom exceptions for ASGP
Multi-NoSQL database error handling
"""


class ASGPException(Exception):
    """Base exception for all ASGP errors"""
    pass


# ==================== Configuration Errors ====================

class ConfigurationError(ASGPException):
    """Base class for configuration-related errors"""
    pass


class SourceNotFoundError(ConfigurationError):
    """Requested source doesn't exist in configuration"""
    pass


class AgentConfigNotFoundError(ConfigurationError):
    """Requested agent config doesn't exist"""
    pass


class DuplicateSourceError(ConfigurationError):
    """Same source name defined in multiple YAML files"""
    pass


class DuplicateAgentError(ConfigurationError):
    """Same agent name defined in multiple YAML files"""
    pass


class InvalidCredentialsError(ConfigurationError):
    """Credentials not using ${ENV_VAR} syntax"""
    pass


class MissingEnvironmentVariableError(ConfigurationError):
    """Required environment variable not set"""
    pass


# ==================== Database Errors ====================

class DatabaseError(ASGPException):
    """Base class for database-related errors"""
    pass


class DatabaseConnectionError(DatabaseError):
    """Failed to connect to database"""
    pass


class QueryExecutionError(DatabaseError):
    """Query execution failed"""
    pass


class QueryTimeoutError(DatabaseError):
    """Query exceeded timeout limit"""
    pass


class InvalidQueryError(DatabaseError):
    """Generated query is malformed or invalid"""
    pass


# ==================== MongoDB-Specific Errors ====================

class MongoDBError(DatabaseError):
    """Base class for MongoDB-specific errors"""
    pass


class InvalidMongoFilterError(MongoDBError):
    """MongoDB filter object is invalid"""
    pass


class CollectionNotFoundError(MongoDBError):
    """Specified collection doesn't exist"""
    pass


class UnsafeOperatorError(MongoDBError):
    """Query contains unsafe MongoDB operators"""
    pass


# ==================== Redis-Specific Errors ====================

class RedisError(DatabaseError):
    """Base class for Redis-specific errors"""
    pass


class UnsafeRedisCommandError(RedisError):
    """Redis command is a write/destructive operation"""
    pass


# ==================== Cassandra-Specific Errors ====================

class CassandraError(DatabaseError):
    """Base class for Cassandra-specific errors"""
    pass


class UnsafeCQLError(CassandraError):
    """CQL statement contains mutation operations"""
    pass


# ==================== DynamoDB-Specific Errors ====================

class DynamoDBError(DatabaseError):
    """Base class for DynamoDB-specific errors"""
    pass


class UnsafeDynamoDBOperationError(DynamoDBError):
    """DynamoDB operation is a write/destructive operation"""
    pass


# ==================== Couchbase-Specific Errors ====================

class CouchbaseError(DatabaseError):
    """Base class for Couchbase-specific errors"""
    pass


class UnsafeN1QLError(CouchbaseError):
    """N1QL statement contains mutation operations"""
    pass


# ==================== Agent Errors ====================

class AgentError(ASGPException):
    """Base class for agent-related errors"""
    pass


class RoutingDecisionError(AgentError):
    """LLM routing decision is invalid"""
    pass


class LLMGenerationError(AgentError):
    """LLM failed to generate valid response"""
    pass


class LLMParseError(AgentError):
    """Failed to parse LLM response as JSON"""
    pass


class SourceBindingError(AgentError):
    """Agent trying to access source not in its bindings"""
    pass


# ==================== Safety Errors ====================

class SafetyError(ASGPException):
    """Base class for safety-related errors"""
    pass


class UnsafeQueryError(SafetyError):
    """Query contains mutation operations"""
    pass


class DMLRejectedError(SafetyError):
    """Query rejected by DML/DDL guard"""
    pass
