# asgp/config/schemas.py
"""
Pydantic models for YAML configuration
Supports MongoDB, Redis, Cassandra, DynamoDB, Couchbase
"""
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
from asgp.schemas.enums import SourceType, DBDriver, OutputFormat


class SourceConfig(BaseModel):
    """source_config section - connection credentials for all database types"""
    name: str
    type: SourceType
    driver: Optional[DBDriver] = None

    # SQL databases
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # MongoDB / Redis
    uri: Optional[str] = None

    # Redis-specific
    socket_timeout: Optional[int] = None
    db: int = 0

    # Cassandra-specific
    contact_points: Optional[str] = None
    keyspace: Optional[str] = None
    timeout_seconds: Optional[int] = None

    # DynamoDB-specific
    region: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint_url: Optional[str] = None

    # Couchbase-specific
    connection_string: Optional[str] = None
    bucket: Optional[str] = None

    # Common settings
    read_only: bool = True
    pool_size: int = 1
    ssl: bool = False
    timeout_ms: int = 5000

    @validator('password', 'uri', 'access_key', 'secret_key', 'connection_string', pre=True, always=True)
    def must_be_env_ref(cls, v):
        """Enforce ${ENV_VAR} syntax for credentials"""
        if v and not str(v).startswith('${'):
            raise ValueError(
                f'Credentials must use ${{ENV_VAR}} syntax, not raw strings. '
                f'Got: {str(v)[:20]}...'
            )
        return v


# ==================== SQL Metadata ====================

class ColumnMeta(BaseModel):
    """Table column metadata"""
    name: str
    type: str
    pk: bool = False
    fk: Optional[str] = None
    description: Optional[str] = None
    values: Optional[List[str]] = None


class TableMeta(BaseModel):
    """SQL table metadata"""
    name: str
    description: Optional[str] = None
    columns: List[ColumnMeta] = Field(default_factory=list)
    relationships: List[str] = Field(default_factory=list)


# ==================== MongoDB Metadata ====================

class FieldMeta(BaseModel):
    """MongoDB / Couchbase field metadata"""
    name: str
    type: str
    ref: Optional[str] = None
    range: Optional[str] = None
    description: Optional[str] = None


class CollectionMeta(BaseModel):
    """MongoDB / Couchbase collection metadata"""
    name: str
    description: Optional[str] = None
    fields: List[FieldMeta] = Field(default_factory=list)


# ==================== Redis Metadata ====================

class KeyPattern(BaseModel):
    """Redis key pattern metadata"""
    pattern: str
    description: Optional[str] = None
    value_type: str = "string"  # hash | list | string | set | sorted_set
    fields: List[str] = Field(default_factory=list)


# ==================== Cassandra Metadata ====================

class CassandraColumnMeta(BaseModel):
    """Cassandra column metadata with key role"""
    name: str
    type: str
    partition_key: bool = False
    clustering_key: bool = False
    description: Optional[str] = None


class CassandraTableMeta(BaseModel):
    """Cassandra table metadata"""
    name: str
    description: Optional[str] = None
    columns: List[CassandraColumnMeta] = Field(default_factory=list)


# ==================== DynamoDB Metadata ====================

class DynamoDBKeyMeta(BaseModel):
    """DynamoDB key definition"""
    name: str
    type: str  # S, N, B


class DynamoDBAttributeMeta(BaseModel):
    """DynamoDB attribute metadata"""
    name: str
    type: str
    description: Optional[str] = None
    values: Optional[List[str]] = None


class DynamoDBTableMeta(BaseModel):
    """DynamoDB table metadata"""
    name: str
    description: Optional[str] = None
    keys: Dict[str, DynamoDBKeyMeta] = Field(default_factory=dict)
    attributes: List[DynamoDBAttributeMeta] = Field(default_factory=list)


# ==================== Source Details ====================

class SourceDetails(BaseModel):
    """source_details section - schema metadata for LLM context (all DB types)"""
    source_name: str
    # SQL
    tables: List[TableMeta] = Field(default_factory=list)
    # MongoDB / Couchbase
    collections: List[CollectionMeta] = Field(default_factory=list)
    # Redis
    key_patterns: List[KeyPattern] = Field(default_factory=list)
    # Cassandra
    cassandra_tables: List[CassandraTableMeta] = Field(default_factory=list)
    # DynamoDB
    dynamodb_tables: List[DynamoDBTableMeta] = Field(default_factory=list)


# ==================== Agent Config ====================

class AgentConfig(BaseModel):
    """agent_config section - agent behavior definition"""
    name: str
    display_name: str
    source_bindings: List[str] = Field(default_factory=list)
    model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 2048
    system_prompt: str
    output_format: OutputFormat = OutputFormat.json
    output_structure: Dict[str, str] = Field(default_factory=dict)


# ==================== Top-Level Config ====================

class ASGPConfig(BaseModel):
    """Top-level YAML structure"""
    source_config: List[SourceConfig] = Field(default_factory=list)
    source_details: Dict[str, SourceDetails] = Field(default_factory=dict)
    agent_config: List[AgentConfig] = Field(default_factory=list)
