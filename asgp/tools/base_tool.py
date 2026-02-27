# asgp/tools/base_tool.py
"""
Base tool contract - JIT design
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime, date
from uuid import UUID
from bson import ObjectId
import re

class BaseTool(ABC):
    """
    All tools follow JIT pattern:
    - No module-level connections
    - Open → Fetch → Close in single method call
    - Close in finally block even on error
    """
    
    @abstractmethod
    async def fetch(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """
        JIT contract: open connection inside method → fetch → close in finally
        """
        ...
    
    def _safe_serialize(self, obj: Any) -> Any:
        """
        Convert non-JSON-serializable types to safe equivalents
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, UUID):
            return str(obj)
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        elif isinstance(obj, dict):
            return {k: self._safe_serialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._safe_serialize(item) for item in obj]
        return obj
    
    def _sanitize_error(self, e: Exception) -> str:
        """
        Strip connection strings and credentials from error messages
        """
        msg = str(e)
        # Remove URIs
        msg = re.sub(r'mongodb(\+srv)?://[^@]+@[^\s]+', 'mongodb://***:***@***', msg)
        msg = re.sub(r'redis://[^@]+@[^\s]+', 'redis://***:***@***', msg)
        # Remove password-like patterns
        msg = re.sub(r'password["\']?\s*[:=]\s*["\']?[^\s"\']+', 'password=***', msg, flags=re.IGNORECASE)
        return msg
