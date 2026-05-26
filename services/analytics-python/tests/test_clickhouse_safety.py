from __future__ import annotations

import pytest

from analytics.clients.clickhouse import ClickHouseClient, ClickHouseConfig, QuerySafetyConfig
from analytics.exceptions import QuerySafetyError


class FakeClickHouse:
    def __init__(self):
        self.calls = []

    def query_df(self, sql, parameters=None, settings=None):
        self.calls.append({"sql": sql, "parameters": parameters, "settings": settings})
        return []

    def query(self, sql, parameters=None, settings=None):
        self.calls.append({"sql": sql, "parameters": parameters, "settings": settings})
        return {"ok": True}


def client_with_fake(fake: FakeClickHouse) -> ClickHouseClient:
    client = ClickHouseClient(
        ClickHouseConfig(retry_attempts=1),
        QuerySafetyConfig(default_limit=25, max_limit=100, query_timeout_seconds=7),
    )
    client._client = fake
    return client


def test_safe_query_adds_limit_and_timeout_settings():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    client.query_dataframe("SELECT * FROM table_without_limit", {"x": 1})

    call = fake.calls[0]
    assert "LIMIT 25" in call["sql"]
    assert call["settings"]["max_execution_time"] == 7
    assert call["settings"]["result_overflow_mode"] == "break"


def test_safe_query_keeps_explicit_top_level_limit():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    client.query_dataframe("SELECT * FROM table_with_limit LIMIT 5")

    assert fake.calls[0]["sql"] == "SELECT * FROM table_with_limit LIMIT 5"


def test_safe_query_ignores_limit_in_string_comment_and_subquery():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    client.query_dataframe(
        """
        SELECT 'limit 1' AS label
        FROM (SELECT * FROM raw_events LIMIT 1) AS nested
        /* LIMIT 2 */
        -- LIMIT 3
        """
    )

    call_sql = fake.calls[0]["sql"]
    assert "analytics_limited LIMIT 25" in call_sql
    assert "raw_events LIMIT 1" in call_sql


def test_safe_query_rejects_mutation():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    with pytest.raises(QuerySafetyError):
        client.query("ALTER TABLE x DELETE WHERE 1 = 1")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "SELECT ';' AS semicolon_text; DROP TABLE x",
        "SELECT 1; -- comment\nSELECT 2",
        "SELECT 1;;",
    ],
)
def test_safe_query_rejects_multiple_statements(sql):
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    with pytest.raises(QuerySafetyError, match="Multiple SQL statements"):
        client.query(sql)

    assert fake.calls == []


def test_query_limit_cannot_exceed_max_limit():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    with pytest.raises(QuerySafetyError):
        client.query_dataframe("SELECT * FROM x", limit=101)


def test_query_dataframe_chunks_requires_stable_top_level_order_by():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    with pytest.raises(QuerySafetyError, match="ORDER BY"):
        list(client.query_dataframe_chunks("SELECT * FROM x", total_limit=10, chunk_size=5))

    assert fake.calls == []


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM (SELECT * FROM x ORDER BY id) AS nested",
        "SELECT 'ORDER BY id' AS label FROM x",
        "SELECT * FROM x /* ORDER BY id */",
    ],
)
def test_query_dataframe_chunks_ignores_nested_or_comment_order_by(sql):
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    with pytest.raises(QuerySafetyError, match="ORDER BY"):
        list(client.query_dataframe_chunks(sql, total_limit=10, chunk_size=5))

    assert fake.calls == []


def test_query_dataframe_chunks_rejects_random_order_by():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    with pytest.raises(QuerySafetyError, match="stable"):
        list(client.query_dataframe_chunks("SELECT * FROM x ORDER BY rand()", total_limit=10))

    assert fake.calls == []


def test_query_dataframe_chunks_pages_with_explicit_order_by():
    fake = FakeClickHouse()
    client = client_with_fake(fake)

    chunks = list(
        client.query_dataframe_chunks(
            "SELECT * FROM x ORDER BY chain_id, block_hash, tx_hash",
            total_limit=12,
            chunk_size=5,
        )
    )

    assert chunks == []
    call_sql = fake.calls[0]["sql"]
    assert "ORDER BY chain_id, block_hash, tx_hash" in call_sql
    assert "LIMIT 5 OFFSET 0" in call_sql
