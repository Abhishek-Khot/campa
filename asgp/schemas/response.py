"""
Response schemas for agent execution results
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

from asgp.schemas.enums import SourceType, ResultStatus, DBDecision


class DBRoutingDecision(BaseModel):
    """LLM routing decision for database query"""
    db_type: DBDecision
    source_name: str
    query: str
    explanation: str
    confidence: float


class SourceResult(BaseModel):
    """Result from a single source execution"""
    source_name: str
    source_type: SourceType
    agent_name: str
    status: ResultStatus
    data: List[Dict[str, Any]]
    raw_query: Optional[str] = None
    confidence: float
    latency_ms: int
    row_count: int
    error_detail: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)