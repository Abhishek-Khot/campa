# asgp/tools/cassandra_tool.py
"""
Cassandra JIT Tool — Read-only CQL execution with security guardrails
Supports SELECT queries only against a configured keyspace.
"""

import re
from typing import Any, Dict, List

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement
except ImportError as e:
    raise ImportError(
        f"Missing Cassandra dependency: {e}. Install: pip install cassandra-driver"
    )


class CassandraTool:
    """
    JIT Cassandra connector — Open → Execute → Close in single call.
    No persistent connections.
    """

    # CQL keywords that indicate mutation / DDL
    UNSAFE_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE',
        'ALTER', 'CREATE', 'GRANT', 'REVOKE', 'BATCH',
    }

    def __init__(self):
        """No persistent connections — JIT only"""
        pass

    def fetch_cassandra(
        self,
        cql_query: str,
        source_cfg,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Execute a read-only CQL query via JIT connection.

        Args:
            cql_query: CQL SELECT statement
            source_cfg: SourceConfig with contact_points, port, keyspace,
                        username, password, timeout_seconds
            limit: Max rows to return

        Returns:
            List of dicts with query results
        """
        cluster = None
        session = None
        try:
            # Validate CQL is read-only
            self._validate_cql(cql_query)

            # Build connection params
            contact_points = source_cfg.contact_points.split(',')
            port = getattr(source_cfg, 'port', 9042) or 9042
            keyspace = source_cfg.keyspace
            timeout = getattr(source_cfg, 'timeout_seconds', 5) or 5

            auth = None
            username = getattr(source_cfg, 'username', None)
            password = getattr(source_cfg, 'password', None)
            if username and password:
                auth = PlainTextAuthProvider(
                    username=username,
                    password=password,
                )

            # Create JIT connection
            cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth,
                connect_timeout=timeout,
            )
            session = cluster.connect(keyspace)

            # Add LIMIT if not present
            query_upper = cql_query.strip().upper()
            if 'LIMIT' not in query_upper:
                cql_query = cql_query.rstrip(';').strip() + f' LIMIT {limit}'

            # Execute
            statement = SimpleStatement(cql_query)
            result_set = session.execute(statement, timeout=timeout)

            # Convert rows to dicts
            rows = []
            for row in result_set:
                row_dict = {}
                for col_name in row._fields:
                    val = getattr(row, col_name)
                    row_dict[col_name] = self._serialize_value(val)
                rows.append(row_dict)

            return rows

        except (CassandraSecurityError, CassandraQueryError):
            raise
        except Exception as e:
            raise CassandraQueryError(
                f"Cassandra query failed: {self._sanitize_error(e)}"
            )
        finally:
            if session:
                session.shutdown()
            if cluster:
                cluster.shutdown()

    def _validate_cql(self, cql: str) -> None:
        """Validate CQL is a SELECT-only statement."""
        stripped = cql.strip().rstrip(';').strip()
        if not stripped:
            raise CassandraQueryError("Empty CQL query")

        # The first keyword must be SELECT
        first_word = stripped.split()[0].upper()
        if first_word != 'SELECT':
            raise CassandraSecurityError(
                f"Only SELECT queries are allowed. Got: '{first_word}...'"
            )

        # Scan for unsafe keywords appearing as standalone tokens
        tokens = re.findall(r'\b([A-Z_]+)\b', stripped.upper())
        for token in tokens:
            if token in self.UNSAFE_KEYWORDS:
                raise CassandraSecurityError(
                    f"Unsafe CQL keyword '{token}' detected. "
                    "Only read-only SELECT statements are allowed."
                )

    def _serialize_value(self, value: Any) -> Any:
        """Convert Cassandra types to JSON-safe types."""
        from datetime import datetime, date
        from decimal import Decimal
        from uuid import UUID

        if isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, UUID):
            return str(value)
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='ignore')
        elif isinstance(value, dict):
            return {str(k): self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, (list, set, frozenset, tuple)):
            return [self._serialize_value(item) for item in value]
        return value

    def _sanitize_error(self, error: Exception) -> str:
        """Remove sensitive info from error messages."""
        msg = str(error)
        for pattern in ['password', 'pwd', 'secret', 'credential']:
            if pattern in msg.lower():
                msg = msg.split(pattern)[0] + '[REDACTED]'
        return msg


# Custom Exceptions
class CassandraQueryError(Exception):
    """Invalid Cassandra CQL query"""
    pass


class CassandraSecurityError(Exception):
    """Security violation in CQL statement"""
    pass
