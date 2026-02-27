# asgp/tools/redis_tool.py
"""
Redis JIT Tool — Read-only command execution with security guardrails
Supports: GET, MGET, HGETALL, HGET, KEYS, SCAN, ZRANGE, ZREVRANGE, LRANGE,
          SMEMBERS, TYPE, TTL, EXISTS
"""

import re
from typing import Any, Dict, List, Optional

try:
    import redis as redis_lib
except ImportError as e:
    raise ImportError(f"Missing Redis dependency: {e}. Install: pip install redis")


class RedisTool:
    """
    JIT Redis connector — Open → Execute → Close in single call.
    No persistent connections.
    """

    # Read-only commands allowed
    ALLOWED_COMMANDS = {
        'GET', 'MGET', 'HGETALL', 'HGET', 'HMGET',
        'KEYS', 'SCAN',
        'ZRANGE', 'ZREVRANGE', 'ZRANGEBYSCORE', 'ZCARD', 'ZSCORE',
        'LRANGE', 'LLEN', 'LINDEX',
        'SMEMBERS', 'SCARD', 'SISMEMBER',
        'TYPE', 'TTL', 'PTTL', 'EXISTS', 'DBSIZE',
    }

    # Explicitly blocked commands (write / destructive / admin)
    BLOCKED_COMMANDS = {
        'FLUSHDB', 'FLUSHALL', 'DEL', 'UNLINK',
        'SET', 'SETNX', 'SETEX', 'PSETEX', 'MSET', 'MSETNX', 'APPEND',
        'HSET', 'HSETNX', 'HMSET', 'HDEL',
        'LPUSH', 'RPUSH', 'LPOP', 'RPOP', 'LSET', 'LREM', 'LTRIM',
        'SADD', 'SREM', 'SPOP', 'SMOVE',
        'ZADD', 'ZREM', 'ZINCRBY', 'ZREMRANGEBYSCORE', 'ZREMRANGEBYRANK',
        'CONFIG', 'SHUTDOWN', 'BGSAVE', 'BGREWRITEAOF',
        'EVAL', 'EVALSHA', 'SCRIPT',
        'RENAME', 'RENAMENX', 'EXPIRE', 'EXPIREAT', 'PERSIST',
        'MOVE', 'DUMP', 'RESTORE',
    }

    def __init__(self):
        """No persistent connections — JIT only"""
        pass

    def fetch_redis(
        self,
        command_str: str,
        source_cfg,
    ) -> List[Dict[str, Any]]:
        """
        Execute a read-only Redis command via JIT connection.

        Args:
            command_str: Redis command string, e.g. "HGETALL session:123"
            source_cfg: SourceConfig with uri, socket_timeout, db

        Returns:
            List of dicts with results
        """
        client = None
        try:
            # Parse command
            cmd, args = self._parse_command(command_str)

            # Validate read-only
            self._validate_command(cmd)

            # Build JIT connection
            uri = source_cfg.uri
            socket_timeout = getattr(source_cfg, 'socket_timeout', 3) or 3
            db_num = getattr(source_cfg, 'db', 0) or 0

            client = redis_lib.Redis.from_url(
                uri,
                socket_timeout=socket_timeout,
                db=db_num,
                decode_responses=True,
            )

            # Execute command
            result = self._execute_command(client, cmd, args)

            # Format to List[Dict]
            return self._format_result(cmd, args, result)

        except (RedisSecurityError, RedisQueryError):
            raise
        except Exception as e:
            raise RedisQueryError(f"Redis command failed: {self._sanitize_error(e)}")
        finally:
            if client:
                client.close()

    def _parse_command(self, command_str: str) -> tuple:
        """Parse command string into (command, args) tuple."""
        parts = command_str.strip().split()
        if not parts:
            raise RedisQueryError("Empty Redis command")

        cmd = parts[0].upper()
        args = parts[1:]
        return cmd, args

    def _validate_command(self, cmd: str) -> None:
        """Validate that the command is read-only."""
        if cmd in self.BLOCKED_COMMANDS:
            raise RedisSecurityError(
                f"Blocked Redis command '{cmd}': write/destructive operations not allowed"
            )
        if cmd not in self.ALLOWED_COMMANDS:
            raise RedisSecurityError(
                f"Unknown or disallowed Redis command '{cmd}'. "
                f"Allowed: {sorted(self.ALLOWED_COMMANDS)}"
            )

    def _execute_command(self, client, cmd: str, args: list) -> Any:
        """Execute a Redis command and return raw result."""
        cmd_lower = cmd.lower()

        # Map command name to redis-py method
        method_map = {
            'GET': lambda: client.get(args[0]),
            'MGET': lambda: client.mget(args),
            'HGETALL': lambda: client.hgetall(args[0]),
            'HGET': lambda: client.hget(args[0], args[1]),
            'HMGET': lambda: client.hmget(args[0], *args[1:]),
            'KEYS': lambda: client.keys(args[0] if args else '*'),
            'SCAN': lambda: self._exec_scan(client, args),
            'ZRANGE': lambda: client.zrange(
                args[0], int(args[1]), int(args[2]),
                withscores='WITHSCORES' in [a.upper() for a in args]
            ),
            'ZREVRANGE': lambda: client.zrevrange(
                args[0], int(args[1]), int(args[2]),
                withscores='WITHSCORES' in [a.upper() for a in args]
            ),
            'ZRANGEBYSCORE': lambda: client.zrangebyscore(
                args[0], args[1], args[2]
            ),
            'ZCARD': lambda: client.zcard(args[0]),
            'ZSCORE': lambda: client.zscore(args[0], args[1]),
            'LRANGE': lambda: client.lrange(args[0], int(args[1]), int(args[2])),
            'LLEN': lambda: client.llen(args[0]),
            'LINDEX': lambda: client.lindex(args[0], int(args[1])),
            'SMEMBERS': lambda: client.smembers(args[0]),
            'SCARD': lambda: client.scard(args[0]),
            'SISMEMBER': lambda: client.sismember(args[0], args[1]),
            'TYPE': lambda: client.type(args[0]),
            'TTL': lambda: client.ttl(args[0]),
            'PTTL': lambda: client.pttl(args[0]),
            'EXISTS': lambda: client.exists(args[0]),
            'DBSIZE': lambda: client.dbsize(),
        }

        executor = method_map.get(cmd)
        if not executor:
            raise RedisQueryError(f"No executor for command '{cmd}'")

        return executor()

    def _exec_scan(self, client, args: list) -> list:
        """Execute SCAN with cursor, MATCH pattern, and COUNT."""
        cursor = int(args[0]) if args else 0
        match_pattern = None
        count = 100

        i = 1
        while i < len(args):
            arg_upper = args[i].upper()
            if arg_upper == 'MATCH' and i + 1 < len(args):
                match_pattern = args[i + 1]
                i += 2
            elif arg_upper == 'COUNT' and i + 1 < len(args):
                count = int(args[i + 1])
                i += 2
            else:
                i += 1

        _, keys = client.scan(cursor=cursor, match=match_pattern, count=count)
        return keys

    def _format_result(self, cmd: str, args: list, result: Any) -> List[Dict[str, Any]]:
        """Convert Redis result to List[Dict] for unified response."""
        if result is None:
            return [{"key": args[0] if args else None, "value": None}]

        # Hash results
        if isinstance(result, dict):
            return [{"key": args[0], "field": k, "value": v} for k, v in result.items()]

        # List / set results
        if isinstance(result, (list, set)):
            items = list(result)
            # Sorted set with scores → list of tuples
            if items and isinstance(items[0], tuple):
                return [{"member": m, "score": float(s)} for m, s in items]
            # KEYS / SCAN / LRANGE / SMEMBERS
            return [{"value": item} for item in items]

        # Scalar results
        return [{"key": args[0] if args else None, "value": result}]

    def _sanitize_error(self, error: Exception) -> str:
        """Remove sensitive info from error messages."""
        msg = str(error)
        for pattern in ['redis://', 'password', 'pwd', 'secret']:
            if pattern in msg.lower():
                msg = msg.split(pattern)[0] + '[REDACTED]'
        return msg


# Custom Exceptions
class RedisQueryError(Exception):
    """Invalid Redis query"""
    pass


class RedisSecurityError(Exception):
    """Security violation in Redis command"""
    pass
