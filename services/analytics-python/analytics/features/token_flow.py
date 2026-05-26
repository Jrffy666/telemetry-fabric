"""Token flow and large transfer analysis."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from analytics.features._records import canonical_transfer_records, decimal_value, normalize_address


@dataclass(frozen=True)
class TokenFlowSummary:
    address: str
    inbound_usd: Decimal = Decimal("0")
    outbound_usd: Decimal = Decimal("0")
    net_usd: Decimal = Decimal("0")
    transfer_count: int = 0
    per_token_usd: dict[str, Decimal] = field(default_factory=dict)


def summarize_token_flow(
    transfers: Any,
    address: str,
    token_address: str | None = None,
) -> TokenFlowSummary:
    target = normalize_address(address)
    token_filter = normalize_address(token_address)
    inbound = Decimal("0")
    outbound = Decimal("0")
    transfer_count = 0
    per_token: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    for row in canonical_transfer_records(transfers):
        row_token = normalize_address(row.get("token_address"))
        if token_filter and row_token != token_filter:
            continue

        from_address = normalize_address(row.get("from_address"))
        to_address = normalize_address(row.get("to_address"))
        amount_usd = decimal_value(row.get("amount_usd_value", row.get("amount_usd")))
        token_key = row_token or str(row.get("token_symbol", "UNKNOWN"))

        if to_address == target:
            inbound += amount_usd
            per_token[token_key] += amount_usd
            transfer_count += 1
        elif from_address == target:
            outbound += amount_usd
            per_token[token_key] -= amount_usd
            transfer_count += 1

    return TokenFlowSummary(
        address=target,
        inbound_usd=inbound,
        outbound_usd=outbound,
        net_usd=inbound - outbound,
        transfer_count=transfer_count,
        per_token_usd=dict(per_token),
    )


def detect_large_transfers(
    transfers: Any,
    min_amount_usd: Decimal | int | str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    threshold = decimal_value(min_amount_usd)
    matches: list[dict[str, Any]] = []

    for row in canonical_transfer_records(transfers):
        amount_usd = decimal_value(row.get("amount_usd_value", row.get("amount_usd")))
        if amount_usd >= threshold:
            output = dict(row)
            output["amount_usd_decimal"] = amount_usd
            matches.append(output)

    matches.sort(key=lambda item: item["amount_usd_decimal"], reverse=True)
    return matches[:limit] if limit is not None else matches
