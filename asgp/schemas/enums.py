# asgp/schemas/enums.py
"""
Enums for type safety across ASGP
"""
from enum import Enum

class SourceType(str, Enum):
    """Data source categories"""
    relational = "relational"
    nosql = "nosql"
    vector = "vector"
    web = "web"
    filesystem = "filesystem"
    api = "api"

class DBDriver(str, Enum):
    """Database driver types"""
    postgresql = "postgresql"
    mysql = "mysql"
    sqlite = "sqlite"
    mongodb = "mongodb"
    redis = "redis"
    cassandra = "cassandra"
    dynamodb = "dynamodb"
    couchbase = "couchbase"

class DBDecision(str, Enum):
    """LLM routing decision for DatabaseAgent"""
    sql = "sql"
    mongodb = "mongodb"
    redis = "redis"
    cassandra = "cassandra"
    dynamodb = "dynamodb"
    couchbase = "couchbase"

class ResultStatus(str, Enum):
    """Execution result status"""
    success = "success"
    partial = "partial"
    error = "error"

class OutputFormat(str, Enum):
    """Agent output formatting"""
    json = "json"
    text = "text"
    table = "table"
    markdown = "markdown"
