from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_duplicate_events_identity_key_uses_canonical_event_coordinates():
    sql = (REPO_ROOT / "storage/clickhouse/queries/duplicate_events.sql").read_text()

    assert "concat(toString(chain_id), ':', block_hash, ':', tx_hash, ':0:0')" in sql
    assert (
        "concat(toString(chain_id), ':', block_hash, ':', tx_hash, ':', "
        "toString(log_index), ':0')"
        in sql
    )
    assert (
        "concat(toString(chain_id), ':', block_hash, ':', tx_hash, ':', "
        "toString(log_index), ':', toString(transfer_index))"
        in sql
    )
    assert "toString(block_number)) AS identity_key" not in sql
