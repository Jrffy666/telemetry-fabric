"""ClickHouse access for offline analytics.

This client is intentionally separate from the crawler hot path. It reads from
analytical tables and can batch-insert derived features or reports.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar

from analytics.exceptions import OptionalDependencyError, QuerySafetyError


T = TypeVar("T")


@dataclass(frozen=True)
class QuerySafetyConfig:
    """Bounds that keep analytical reads from becoming unbounded loads."""

    default_limit: int = 10_000
    max_limit: int = 1_000_000
    chunk_size: int = 10_000
    max_result_rows: int = 1_000_000
    query_timeout_seconds: int = 60
    require_limit: bool = True

    def __post_init__(self) -> None:
        _require_positive("default_limit", self.default_limit)
        _require_positive("max_limit", self.max_limit)
        _require_positive("chunk_size", self.chunk_size)
        _require_positive("max_result_rows", self.max_result_rows)
        _require_positive("query_timeout_seconds", self.query_timeout_seconds)
        if self.default_limit > self.max_limit:
            raise ValueError("default_limit must not exceed max_limit")

    @classmethod
    def from_env(cls, prefix: str = "ANALYTICS_QUERY_") -> "QuerySafetyConfig":
        return cls(
            default_limit=int(os.getenv(f"{prefix}DEFAULT_LIMIT", str(cls.default_limit))),
            max_limit=int(os.getenv(f"{prefix}MAX_LIMIT", str(cls.max_limit))),
            chunk_size=int(os.getenv(f"{prefix}CHUNK_SIZE", str(cls.chunk_size))),
            max_result_rows=int(os.getenv(f"{prefix}MAX_RESULT_ROWS", str(cls.max_result_rows))),
            query_timeout_seconds=int(
                os.getenv(f"{prefix}TIMEOUT_SECONDS", str(cls.query_timeout_seconds))
            ),
            require_limit=os.getenv(f"{prefix}REQUIRE_LIMIT", "true").lower()
            in {"1", "true", "yes"},
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "QuerySafetyConfig":
        values = values or {}
        return cls(
            default_limit=int(values.get("default_limit", cls.default_limit)),
            max_limit=int(values.get("max_limit", cls.max_limit)),
            chunk_size=int(values.get("chunk_size", cls.chunk_size)),
            max_result_rows=int(values.get("max_result_rows", cls.max_result_rows)),
            query_timeout_seconds=int(
                values.get("query_timeout_seconds", cls.query_timeout_seconds)
            ),
            require_limit=_bool_value(values.get("require_limit", cls.require_limit)),
        )


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str = "localhost"
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "telemetry_fabric"
    secure: bool = False
    connect_timeout_seconds: int = 10
    send_receive_timeout_seconds: int = 60
    retry_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    settings: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("ClickHouse host is required")
        if not self.database:
            raise ValueError("ClickHouse database is required")
        if self.port <= 0 or self.port > 65535:
            raise ValueError("ClickHouse port must be between 1 and 65535")
        _require_positive("connect_timeout_seconds", self.connect_timeout_seconds)
        _require_positive("send_receive_timeout_seconds", self.send_receive_timeout_seconds)
        _require_positive("retry_attempts", self.retry_attempts)
        if self.retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be zero or greater")
        if not isinstance(self.settings, Mapping):
            raise ValueError("ClickHouse settings must be a mapping")

    @classmethod
    def from_env(cls, prefix: str = "CLICKHOUSE_") -> "ClickHouseConfig":
        return cls(
            host=os.getenv(f"{prefix}HOST", cls.host),
            port=int(os.getenv(f"{prefix}PORT", str(cls.port))),
            username=os.getenv(f"{prefix}USER", os.getenv(f"{prefix}USERNAME", cls.username)),
            password=os.getenv(f"{prefix}PASSWORD", cls.password),
            database=os.getenv(f"{prefix}DATABASE", cls.database),
            secure=os.getenv(f"{prefix}SECURE", "false").lower() in {"1", "true", "yes"},
            connect_timeout_seconds=int(
                os.getenv(f"{prefix}CONNECT_TIMEOUT_SECONDS", str(cls.connect_timeout_seconds))
            ),
            send_receive_timeout_seconds=int(
                os.getenv(
                    f"{prefix}SEND_RECEIVE_TIMEOUT_SECONDS",
                    str(cls.send_receive_timeout_seconds),
                )
            ),
            retry_attempts=int(os.getenv(f"{prefix}RETRY_ATTEMPTS", str(cls.retry_attempts))),
            retry_backoff_seconds=float(
                os.getenv(f"{prefix}RETRY_BACKOFF_SECONDS", str(cls.retry_backoff_seconds))
            ),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "ClickHouseConfig":
        values = values or {}
        return cls(
            host=str(values.get("host", cls.host)),
            port=int(values.get("port", cls.port)),
            username=str(values.get("username", values.get("user", cls.username))),
            password=str(values.get("password", cls.password)),
            database=str(values.get("database", cls.database)),
            secure=_bool_value(values.get("secure", cls.secure)),
            connect_timeout_seconds=int(
                values.get("connect_timeout_seconds", cls.connect_timeout_seconds)
            ),
            send_receive_timeout_seconds=int(
                values.get("send_receive_timeout_seconds", cls.send_receive_timeout_seconds)
            ),
            retry_attempts=int(values.get("retry_attempts", cls.retry_attempts)),
            retry_backoff_seconds=float(
                values.get("retry_backoff_seconds", cls.retry_backoff_seconds)
            ),
            settings=dict(values.get("settings", {})),
        )


class ClickHouseClient:
    """Lazy ClickHouse client wrapper using clickhouse-connect."""

    def __init__(
        self,
        config: ClickHouseConfig | None = None,
        safety: QuerySafetyConfig | None = None,
    ) -> None:
        self.config = config or ClickHouseConfig.from_env()
        self.safety = safety or QuerySafetyConfig.from_env()
        self._client: Any | None = None

    def connect(self) -> Any:
        if self._client is None:
            try:
                import clickhouse_connect
            except ModuleNotFoundError as exc:
                raise OptionalDependencyError("clickhouse-connect", "ClickHouse access") from exc

            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
                secure=self.config.secure,
                connect_timeout=self.config.connect_timeout_seconds,
                send_receive_timeout=self.config.send_receive_timeout_seconds,
                settings=dict(self.config.settings),
            )
        return self._client

    def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        safe: bool = True,
    ) -> Any:
        prepared_sql = self._prepare_query(sql, limit=limit, safe=safe)
        return self._with_retries(
            lambda: self.connect().query(
                prepared_sql,
                parameters=parameters or {},
                settings=self._query_settings(),
            )
        )

    def query_dataframe(
        self,
        sql: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        safe: bool = True,
    ) -> Any:
        prepared_sql = self._prepare_query(sql, limit=limit, safe=safe)
        return self._with_retries(
            lambda: self.connect().query_df(
                prepared_sql,
                parameters=parameters or {},
                settings=self._query_settings(),
            )
        )

    def query_dataframe_chunks(
        self,
        sql: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        total_limit: int | None = None,
        chunk_size: int | None = None,
    ) -> Iterable[Any]:
        base_sql = _single_statement_sql(sql)
        if not _is_select(base_sql):
            raise QuerySafetyError("Only SELECT/WITH queries are allowed through safe analytics reads")
        if not _has_stable_order_by(base_sql):
            raise QuerySafetyError(
                "Chunked dataframe queries require an explicit stable top-level ORDER BY"
            )
        total = self._validated_limit(total_limit or self.safety.default_limit)
        size = self._validated_limit(chunk_size or self.safety.chunk_size)
        offset = 0

        while offset < total:
            current_limit = min(size, total - offset)
            page_sql = (
                f"SELECT * FROM (\n{base_sql}\n) AS analytics_page "
                f"LIMIT {current_limit} OFFSET {offset}"
            )
            chunk = self.query_dataframe(page_sql, parameters=parameters, safe=False)
            if _is_empty_dataframe(chunk):
                break
            yield chunk
            offset += current_limit

    def stream_row_blocks(
        self,
        sql: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
    ) -> Iterable[Any]:
        prepared_sql = self._prepare_query(sql, limit=limit, safe=True)
        client = self.connect()
        stream = getattr(client, "query_row_block_stream", None)
        if stream is None:
            yield from self.query_dataframe_chunks(
                sql,
                parameters=parameters,
                total_limit=limit or self.safety.default_limit,
            )
            return

        with stream(
            prepared_sql,
            parameters=parameters or {},
            settings=self._query_settings(),
        ) as row_blocks:
            for block in row_blocks:
                yield block

    def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        column_names: Sequence[str],
    ) -> None:
        self._with_retries(lambda: self.connect().insert(table, rows, column_names=column_names))

    def insert_dataframe(self, table: str, dataframe: Any) -> None:
        self._with_retries(lambda: self.connect().insert_df(table, dataframe))

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _prepare_query(self, sql: str, *, limit: int | None, safe: bool) -> str:
        clean_sql = _single_statement_sql(sql) if safe else self._strip_semicolon(sql)
        if not safe:
            return clean_sql
        if not _is_select(clean_sql):
            raise QuerySafetyError("Only SELECT/WITH queries are allowed through safe analytics reads")

        if _has_limit(clean_sql):
            return clean_sql

        if self.safety.require_limit:
            safe_limit = self._validated_limit(limit or self.safety.default_limit)
            return f"SELECT * FROM (\n{clean_sql}\n) AS analytics_limited LIMIT {safe_limit}"
        return clean_sql

    def _validated_limit(self, value: int) -> int:
        limit = int(value)
        if limit <= 0:
            raise QuerySafetyError("Query limit must be positive")
        if limit > self.safety.max_limit:
            raise QuerySafetyError(
                f"Query limit {limit} exceeds max_limit {self.safety.max_limit}"
            )
        return limit

    def _query_settings(self) -> dict[str, Any]:
        return {
            **dict(self.config.settings),
            "max_execution_time": self.safety.query_timeout_seconds,
            "max_result_rows": self.safety.max_result_rows,
            "result_overflow_mode": "break",
        }

    def _with_retries(self, operation: Callable[[], T]) -> T:
        attempts = max(1, self.config.retry_attempts)
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return operation()
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                time.sleep(self.config.retry_backoff_seconds * attempt)
        assert last_error is not None
        raise last_error

    @staticmethod
    def _strip_semicolon(sql: str) -> str:
        return sql.strip().rstrip(";").strip()


def _is_select(sql: str) -> bool:
    tokens = [token for token in _sql_tokens(sql) if token.depth == 0]
    return bool(tokens and tokens[0].value in {"select", "with"})


@dataclass(frozen=True)
class _SqlToken:
    value: str
    start: int
    end: int
    depth: int


def _has_limit(sql: str) -> bool:
    return any(token.value == "limit" and token.depth == 0 for token in _sql_tokens(sql))


def _has_stable_order_by(sql: str) -> bool:
    order_clause = _top_level_order_by_clause(sql)
    if order_clause is None or not order_clause.strip():
        return False

    return not bool(
        re.search(
            r"\b(rand|rand32|rand64|rand_canonical|random)\s*\(",
            order_clause,
            flags=re.IGNORECASE,
        )
    )


def _top_level_order_by_clause(sql: str) -> str | None:
    tokens = [token for token in _sql_tokens(sql) if token.depth == 0]
    for index, token in enumerate(tokens[:-1]):
        next_token = tokens[index + 1]
        if token.value == "order" and next_token.value == "by":
            end = len(sql)
            for clause_token in tokens[index + 2 :]:
                if clause_token.value in {
                    "limit",
                    "offset",
                    "fetch",
                    "settings",
                    "format",
                    "union",
                    "intersect",
                    "except",
                }:
                    end = clause_token.start
                    break
            return sql[next_token.end : end]
    return None


def _single_statement_sql(sql: str) -> str:
    semicolons = [token for token in _sql_tokens(sql) if token.value == ";"]
    if not semicolons:
        return sql.strip()

    first_semicolon = semicolons[0]
    if len(semicolons) > 1 or _has_non_comment_code_after(sql, first_semicolon.end):
        raise QuerySafetyError("Multiple SQL statements are not allowed through safe analytics reads")
    return sql[: first_semicolon.start].strip()


def _has_non_comment_code_after(sql: str, start: int) -> bool:
    index = start
    length = len(sql)
    while index < length:
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < length else ""
        if char.isspace():
            index += 1
        elif char == "-" and next_char == "-":
            index = _skip_line_comment(sql, index + 2)
        elif char == "#":
            index = _skip_line_comment(sql, index + 1)
        elif char == "/" and next_char == "*":
            index = _skip_block_comment(sql, index + 2)
        else:
            return True
    return False


def _sql_tokens(sql: str) -> list[_SqlToken]:
    tokens: list[_SqlToken] = []
    index = 0
    depth = 0
    length = len(sql)
    while index < length:
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < length else ""

        if char.isspace():
            index += 1
        elif char == "-" and next_char == "-":
            index = _skip_line_comment(sql, index + 2)
        elif char == "#":
            index = _skip_line_comment(sql, index + 1)
        elif char == "/" and next_char == "*":
            index = _skip_block_comment(sql, index + 2)
        elif char in {"'", '"', "`"}:
            index = _skip_quoted(sql, index, char)
        elif char == "(":
            depth += 1
            index += 1
        elif char == ")":
            depth = max(0, depth - 1)
            index += 1
        elif char == ";":
            tokens.append(_SqlToken(";", index, index + 1, depth))
            index += 1
        elif char.isalnum() or char == "_":
            start = index
            index += 1
            while index < length and (sql[index].isalnum() or sql[index] == "_"):
                index += 1
            tokens.append(_SqlToken(sql[start:index].lower(), start, index, depth))
        else:
            index += 1
    return tokens


def _skip_line_comment(sql: str, start: int) -> int:
    newline = sql.find("\n", start)
    return len(sql) if newline == -1 else newline + 1


def _skip_block_comment(sql: str, start: int) -> int:
    end = sql.find("*/", start)
    return len(sql) if end == -1 else end + 2


def _skip_quoted(sql: str, start: int, quote: str) -> int:
    index = start + 1
    length = len(sql)
    while index < length:
        char = sql[index]
        if char == "\\":
            index += 2
        elif char == quote:
            if index + 1 < length and sql[index + 1] == quote:
                index += 2
            else:
                return index + 1
        else:
            index += 1
    return length


def _is_empty_dataframe(dataframe: Any) -> bool:
    if dataframe is None:
        return True
    if hasattr(dataframe, "empty"):
        return bool(dataframe.empty)
    if hasattr(dataframe, "height"):
        return int(dataframe.height) == 0
    try:
        return len(dataframe) == 0
    except TypeError:
        return False


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)


def _require_positive(name: str, value: int | float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
