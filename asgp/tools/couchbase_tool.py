# asgp/tools/couchbase_tool.py
"""
Couchbase JIT Tool — Read-only N1QL execution with security guardrails
Supports SELECT queries only against a configured bucket.
"""

import re
from typing import Any, Dict, List

try:
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions, QueryOptions
    from couchbase.auth import PasswordAuthenticator
except ImportError as e:
    raise ImportError(
        f"Missing Couchbase dependency: {e}. Install: pip install couchbase"
    )


class CouchbaseTool:
    """
    JIT Couchbase connector — Open → Execute → Close in single call.
    No persistent connections.
    """

    # N1QL keywords that indicate mutation / DDL
    UNSAFE_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'UPSERT', 'MERGE',
        'DROP', 'CREATE', 'ALTER', 'BUILD', 'GRANT', 'REVOKE',
        'INFER', 'PREPARE', 'EXECUTE',
    }

    def __init__(self):
        """No persistent connections — JIT only"""
        pass

    def fetch_couchbase(
        self,
        n1ql_query: str,
        source_cfg,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Execute a read-only N1QL query via JIT connection.

        Args:
            n1ql_query: N1QL SELECT statement
            source_cfg: SourceConfig with connection_string, username,
                        password, bucket, timeout_seconds
            limit: Max rows to return

        Returns:
            List of dicts with query results
        """
        cluster = None
        try:
            # Validate N1QL is read-only
            self._validate_n1ql(n1ql_query)

            # Build JIT connection
            conn_str = source_cfg.connection_string
            username = source_cfg.username
            password = source_cfg.password
            timeout = getattr(source_cfg, 'timeout_seconds', 5) or 5

            auth = PasswordAuthenticator(username, password)
            cluster = Cluster(conn_str, ClusterOptions(auth))

            # Wait for cluster to be ready
            cluster.wait_until_ready(timeout)

            # Add LIMIT if not present
            query_upper = n1ql_query.strip().upper()
            if 'LIMIT' not in query_upper:
                n1ql_query = n1ql_query.rstrip(';').strip() + f' LIMIT {limit}'

            # Execute N1QL query
            result = cluster.query(n1ql_query)

            # Collect rows
            rows = []
            for row in result:
                rows.append(self._serialize_row(row))

            return rows

        except (CouchbaseSecurityError, CouchbaseQueryError):
            raise
        except Exception as e:
            raise CouchbaseQueryError(
                f"Couchbase query failed: {self._sanitize_error(e)}"
            )
        finally:
            if cluster:
                try:
                    cluster.close()
                except Exception:
                    pass  # Best-effort close

    def _validate_n1ql(self, n1ql: str) -> None:
        """Validate N1QL is a SELECT-only statement."""
        stripped = n1ql.strip().rstrip(';').strip()
        if not stripped:
            raise CouchbaseQueryError("Empty N1QL query")

        # First token must be SELECT
        first_word = stripped.split()[0].upper()
        if first_word != 'SELECT':
            raise CouchbaseSecurityError(
                f"Only SELECT queries are allowed. Got: '{first_word}...'"
            )

        # Scan for unsafe keywords as standalone tokens
        tokens = re.findall(r'\b([A-Z_]+)\b', stripped.upper())
        for token in tokens:
            if token in self.UNSAFE_KEYWORDS:
                raise CouchbaseSecurityError(
                    f"Unsafe N1QL keyword '{token}' detected. "
                    "Only read-only SELECT statements are allowed."
                )

    def _serialize_row(self, row: Any) -> Dict[str, Any]:
        """Convert a N1QL result row to a JSON-safe dict."""
        if isinstance(row, dict):
            return {k: self._serialize_value(v) for k, v in row.items()}
        return {"value": row}

    def _serialize_value(self, value: Any) -> Any:
        """Recursively convert types to JSON-safe equivalents."""
        from datetime import datetime, date
        from decimal import Decimal

        if isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='ignore')
        elif isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, (list, tuple)):
            return [self._serialize_value(item) for item in value]
        return value

    def _sanitize_error(self, error: Exception) -> str:
        """Remove sensitive info from error messages."""
        msg = str(error)
        for pattern in ['password', 'pwd', 'secret', 'credential', 'couchbase://']:
            if pattern in msg.lower():
                msg = msg.split(pattern)[0] + '[REDACTED]'
        return msg


# Custom Exceptions
class CouchbaseQueryError(Exception):
    """Invalid Couchbase N1QL query"""
    pass


class CouchbaseSecurityError(Exception):
    """Security violation in N1QL statement"""
    pass
