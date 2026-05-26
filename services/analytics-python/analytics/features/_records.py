"""Record helpers shared by feature builders."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any


def iter_records(data: Any) -> Iterable[Mapping[str, Any]]:
    if data is None:
        return []
    if isinstance(data, Mapping):
        return [data]
    if hasattr(data, "to_dicts"):
        return data.to_dicts()
    if hasattr(data, "to_dict"):
        rows = data.to_dict(orient="records")
        if isinstance(rows, list):
            return rows
    return data


def decimal_value(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        text = str(value).strip()
        if not text:
            return default
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return default


def normalize_address(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def canonical_transfer_records(data: Any) -> list[Mapping[str, Any]]:
    """Return transfer rows after applying analytics-safe identity rules."""

    rows: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for row in iter_records(data):
        if _is_reorged(row):
            continue

        key = transfer_identity_key(row)
        if key:
            if key in seen:
                continue
            seen.add(key)

        rows.append(row)
    return rows


def transfer_identity_key(row: Mapping[str, Any]) -> str:
    chain_id = _identity_value(row.get("chain_id"))
    block_hash = _identity_value(row.get("block_hash") or row.get("blockHash"))
    tx_hash = _identity_value(row.get("tx_hash") or row.get("transactionHash"))
    log_index = _integer_identity_value(row.get("log_index", row.get("logIndex")))
    if not all((chain_id, block_hash, tx_hash, log_index)):
        return ""

    parts = [chain_id, block_hash, tx_hash, log_index]
    transfer_index = _integer_identity_value(row.get("transfer_index", row.get("transferIndex")))
    if transfer_index:
        parts.append(transfer_index)
    return "|".join(parts)


def _is_reorged(row: Mapping[str, Any]) -> bool:
    finality = str(row.get("finality_status", row.get("finalityStatus", ""))).strip().upper()
    return (
        _truthy(row.get("reorged"))
        or _truthy(row.get("removed"))
        or finality == "FINALITY_STATUS_REORGED"
    )


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes"}


def _identity_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _integer_identity_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    text = str(value).strip().lower()
    if not text:
        return ""
    try:
        if text.startswith("0x"):
            return str(int(text, 16))
        return str(int(text))
    except ValueError:
        return text
