"""
MongoDB-only NoSQL Tool with Query Intelligence
Synchronous version using pymongo
"""

import json
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from decimal import Decimal

try:
    from pymongo import MongoClient
    from bson import ObjectId
except ImportError as e:
    raise ImportError(f"Missing MongoDB dependencies: {e}. Install: pip install pymongo")


class NoSQLTool:
    """
    JIT MongoDB connector with intelligent query parsing
    """
    
    # MongoDB operator whitelist
    ALLOWED_OPERATORS = {
        # Query operators
        '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin',
        # Logical operators
        '$and', '$or', '$not', '$nor',
        # Element operators
        '$exists', '$type',
        # Array operators
        '$all', '$elemMatch', '$size',
        # String operators
        '$regex', '$options',
        # Aggregation operators
        '$match', '$group', '$project', '$sort', '$limit', '$skip',
        '$lookup', '$unwind', '$addFields', '$count', '$sum', '$avg'
    }
    
    # Forbidden operators (injection risk)
    FORBIDDEN_OPERATORS = {'$where', '$function', '$accumulator', '$expr'}
    
    def __init__(self):
        """No persistent connections - JIT only"""
        pass
    
    def fetch_mongo(
        self,
        filter_dict: Dict[str, Any],
        collection_name: str,
        source_cfg,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        MongoDB JIT execution with intelligent query parsing
        
        Args:
            filter_dict: MongoDB filter object
            collection_name: Target collection
            source_cfg: Source configuration object
            limit: Max results to return
        
        Returns:
            List of documents
        """
        client = None
        try:
            # Validate filter
            self._validate_filter(filter_dict)
            
            # Get connection details
            uri = source_cfg.uri
            database_name = source_cfg.database
            timeout_ms = getattr(source_cfg, 'timeout_ms', 5000)
            
            # Create JIT connection
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=timeout_ms,
                maxPoolSize=1
            )
            
            db = client[database_name]
            collection = db[collection_name]
            
            # Execute query
            cursor = collection.find(filter_dict).limit(limit)
            results = list(cursor)
            
            # Serialize results
            return [self._serialize_mongo_doc(doc) for doc in results]
        
        except Exception as e:
            raise MongoDBQueryError(f"MongoDB query failed: {self._sanitize_error(e)}")
        
        finally:
            if client:
                client.close()

    def aggregate_mongo(
        self,
        pipeline: List[Dict[str, Any]],
        collection_name: str,
        source_cfg,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        MongoDB aggregation pipeline execution with operator validation

        Args:
            pipeline: List of aggregation pipeline stages
            collection_name: Target collection
            source_cfg: Source configuration object
            limit: Max results to return (applied if not present in pipeline)

        Returns:
            List of aggregation results
        """
        client = None
        try:
            # Validate pipeline
            self._validate_pipeline(pipeline)

            # Get connection details
            uri = source_cfg.uri
            database_name = source_cfg.database
            timeout_ms = getattr(source_cfg, 'timeout_ms', 5000)

            # Create JIT connection
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=timeout_ms,
                maxPoolSize=1
            )

            db = client[database_name]
            collection = db[collection_name]

            # Add limit stage if not present
            has_limit = any('$limit' in stage for stage in pipeline)
            pipeline_to_run = list(pipeline)
            if not has_limit and limit is not None:
                pipeline_to_run.append({'$limit': limit})

            # Execute aggregation
            cursor = collection.aggregate(pipeline_to_run)
            results = list(cursor)

            # Serialize results
            return [self._serialize_mongo_doc(doc) for doc in results]

        except Exception as e:
            raise MongoDBQueryError(f"MongoDB aggregation failed: {self._sanitize_error(e)}")

        finally:
            if client:
                client.close()

    def _validate_pipeline(self, pipeline: List[Dict[str, Any]]) -> None:
        """Validate aggregation pipeline for allowed operators"""
        if not isinstance(pipeline, list):
            raise MongoDBQueryError("Aggregation pipeline must be a list of stages")
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                raise MongoDBQueryError(f"Invalid pipeline stage: {stage}")
            op = next(iter(stage))
            if op in self.FORBIDDEN_OPERATORS:
                raise SecurityError(f"Forbidden operator in pipeline: {op}")
            if op not in self.ALLOWED_OPERATORS:
                raise MongoDBQueryError(f"Unknown/disallowed operator in pipeline: {op}")
            # Recursively validate stage content
            self._validate_filter(stage[op])
    
    def _validate_filter(self, filter_dict: Dict) -> None:
        """Recursive filter validation - block forbidden operators"""
        if not isinstance(filter_dict, dict):
            return
        
        for key, value in filter_dict.items():
            # Check for forbidden operators
            if key in self.FORBIDDEN_OPERATORS:
                raise SecurityError(f"Forbidden operator: {key}")
            
            # Validate operator if present
            if key.startswith('$') and key not in self.ALLOWED_OPERATORS:
                raise MongoDBQueryError(f"Unknown/disallowed operator: {key}")
            
            # Recurse into nested dicts/lists
            if isinstance(value, dict):
                self._validate_filter(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._validate_filter(item)
    
    def _serialize_mongo_doc(self, doc: Dict) -> Dict:
        """Convert MongoDB document to JSON-safe dict"""
        if not doc:
            return {}
        
        serialized = {}
        for key, value in doc.items():
            serialized[key] = self._serialize_value(value)
        return serialized
    
    def _serialize_value(self, value: Any) -> Any:
        """Recursively serialize MongoDB types"""
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='ignore')
        else:
            return value
    
    def _sanitize_error(self, error: Exception) -> str:
        """Remove sensitive info from error messages"""
        msg = str(error)
        # Remove connection strings
        for pattern in ['mongodb://', 'password', 'pwd', 'secret']:
            if pattern in msg.lower():
                msg = msg.split(pattern)[0] + '[REDACTED]'
        return msg


# Custom Exceptions
class MongoDBQueryError(Exception):
    """Invalid MongoDB query"""
    pass


class SecurityError(Exception):
    """Security violation detected"""
    pass
