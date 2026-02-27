# asgp/tools/dynamodb_tool.py
"""
DynamoDB JIT Tool — Read-only operation execution with security guardrails
Supports: GetItem, Query, Scan operations only.
"""

import json
from typing import Any, Dict, List
from decimal import Decimal

try:
    import boto3
    from botocore.config import Config as BotoConfig
except ImportError as e:
    raise ImportError(
        f"Missing DynamoDB dependency: {e}. Install: pip install boto3"
    )


class DynamoDBTool:
    """
    JIT DynamoDB connector — Open → Execute → Close in single call.
    No persistent connections.
    """

    # Read-only operations allowed
    ALLOWED_OPERATIONS = {'GetItem', 'Query', 'Scan'}

    # Explicitly blocked operations (write / admin)
    BLOCKED_OPERATIONS = {
        'PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem',
        'CreateTable', 'DeleteTable', 'UpdateTable',
        'BatchGetItem',  # allowed in theory but risky at scale
        'TransactWriteItems', 'TransactGetItems',
    }

    def __init__(self):
        """No persistent connections — JIT only"""
        pass

    def fetch_dynamodb(
        self,
        operation_json: str,
        source_cfg,
    ) -> List[Dict[str, Any]]:
        """
        Execute a read-only DynamoDB operation via JIT connection.

        Args:
            operation_json: JSON string with {method, params}
                Example: {"method": "GetItem", "params": {"TableName": "Users",
                          "Key": {"user_id": {"S": "123"}}}}
            source_cfg: SourceConfig with region, access_key, secret_key,
                        endpoint_url (optional)

        Returns:
            List of dicts with results
        """
        client = None
        try:
            # Parse operation
            op = self._parse_operation(operation_json)
            method = op['method']
            params = op['params']

            # Validate read-only
            self._validate_operation(method)

            # Build JIT client
            client_kwargs = {
                'service_name': 'dynamodb',
                'region_name': getattr(source_cfg, 'region', 'us-east-1') or 'us-east-1',
                'config': BotoConfig(
                    connect_timeout=5,
                    read_timeout=5,
                    retries={'max_attempts': 1},
                ),
            }

            access_key = getattr(source_cfg, 'access_key', None)
            secret_key = getattr(source_cfg, 'secret_key', None)
            if access_key and secret_key:
                client_kwargs['aws_access_key_id'] = access_key
                client_kwargs['aws_secret_access_key'] = secret_key

            endpoint_url = getattr(source_cfg, 'endpoint_url', None)
            if endpoint_url:
                client_kwargs['endpoint_url'] = endpoint_url

            client = boto3.client(**client_kwargs)

            # Execute
            result = self._execute_operation(client, method, params)

            # Format to List[Dict]
            return self._format_result(method, result)

        except (DynamoDBSecurityError, DynamoDBQueryError):
            raise
        except Exception as e:
            raise DynamoDBQueryError(
                f"DynamoDB operation failed: {self._sanitize_error(e)}"
            )
        finally:
            if client:
                client.close()

    def _parse_operation(self, operation_json: str) -> Dict[str, Any]:
        """Parse operation JSON string."""
        try:
            op = json.loads(operation_json)
        except json.JSONDecodeError as e:
            raise DynamoDBQueryError(f"Invalid operation JSON: {e}")

        if 'method' not in op:
            raise DynamoDBQueryError(
                "Operation must include 'method' field (GetItem, Query, or Scan)"
            )
        if 'params' not in op or not isinstance(op['params'], dict):
            raise DynamoDBQueryError(
                "Operation must include 'params' dict with at least 'TableName'"
            )
        return op

    def _validate_operation(self, method: str) -> None:
        """Validate that the operation is read-only."""
        if method in self.BLOCKED_OPERATIONS:
            raise DynamoDBSecurityError(
                f"Blocked DynamoDB operation '{method}': "
                "write/destructive operations not allowed"
            )
        if method not in self.ALLOWED_OPERATIONS:
            raise DynamoDBSecurityError(
                f"Unknown or disallowed DynamoDB operation '{method}'. "
                f"Allowed: {sorted(self.ALLOWED_OPERATIONS)}"
            )

    def _execute_operation(
        self, client, method: str, params: Dict
    ) -> Dict[str, Any]:
        """Execute DynamoDB operation and return raw result."""
        executor = getattr(client, self._to_snake_case(method), None)
        if not executor:
            raise DynamoDBQueryError(f"boto3 client has no method '{method}'")
        return executor(**params)

    def _format_result(
        self, method: str, result: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Convert DynamoDB response to List[Dict] with deserialized types."""
        if method == 'GetItem':
            item = result.get('Item')
            if item:
                return [self._deserialize_item(item)]
            return []

        elif method in ('Query', 'Scan'):
            items = result.get('Items', [])
            return [self._deserialize_item(item) for item in items]

        return []

    def _deserialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively convert DynamoDB type descriptors to plain Python types."""
        result = {}
        for key, value in item.items():
            result[key] = self._deserialize_value(value)
        return result

    def _deserialize_value(self, value: Any) -> Any:
        """Convert a single DynamoDB attribute value."""
        if not isinstance(value, dict):
            return value

        if 'S' in value:
            return value['S']
        elif 'N' in value:
            num = value['N']
            return int(num) if '.' not in num else float(num)
        elif 'BOOL' in value:
            return value['BOOL']
        elif 'NULL' in value:
            return None
        elif 'L' in value:
            return [self._deserialize_value(v) for v in value['L']]
        elif 'M' in value:
            return self._deserialize_item(value['M'])
        elif 'SS' in value:
            return list(value['SS'])
        elif 'NS' in value:
            return [int(n) if '.' not in n else float(n) for n in value['NS']]
        elif 'B' in value:
            return str(value['B'])
        elif 'BS' in value:
            return [str(b) for b in value['BS']]
        return value

    @staticmethod
    def _to_snake_case(name: str) -> str:
        """Convert PascalCase to snake_case for boto3 method lookup."""
        import re
        s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def _sanitize_error(self, error: Exception) -> str:
        """Remove sensitive info from error messages."""
        msg = str(error)
        for pattern in ['password', 'secret', 'access_key', 'credential']:
            if pattern in msg.lower():
                msg = msg.split(pattern)[0] + '[REDACTED]'
        return msg


# Custom Exceptions
class DynamoDBQueryError(Exception):
    """Invalid DynamoDB operation"""
    pass


class DynamoDBSecurityError(Exception):
    """Security violation in DynamoDB operation"""
    pass
