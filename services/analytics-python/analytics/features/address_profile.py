"""Address profiling features."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from analytics.features._records import canonical_transfer_records, decimal_value, normalize_address


@dataclass(frozen=True)
class AddressProfile:
    address: str
    tx_count: int = 0
    sent_count: int = 0
    received_count: int = 0
    unique_counterparties: int = 0
    unique_tokens: int = 0
    total_sent_usd: Decimal = Decimal("0")
    total_received_usd: Decimal = Decimal("0")
    net_usd: Decimal = Decimal("0")
    largest_transfer_usd: Decimal = Decimal("0")
    blacklist_counterparty_count: int = 0
    top_counterparties: list[tuple[str, int]] = field(default_factory=list)
    top_tokens: list[tuple[str, int]] = field(default_factory=list)


def profile_address(
    transfers: Any,
    address: str,
    blacklist: set[str] | None = None,
    max_ranked_items: int = 10,
) -> AddressProfile:
    """Build a compact wallet profile from token transfer rows.

    `transfers` may be a list of dicts, a pandas DataFrame, or a Polars
    DataFrame. Rows are expected to use ClickHouse column names from
    `chain_token_transfers`.
    """

    target = normalize_address(address)
    blacklist_normalized = {normalize_address(item) for item in blacklist or set()}

    sent_count = 0
    received_count = 0
    total_sent_usd = Decimal("0")
    total_received_usd = Decimal("0")
    largest_transfer_usd = Decimal("0")
    counterparties: Counter[str] = Counter()
    tokens: Counter[str] = Counter()
    tx_hashes: set[str] = set()

    for row in canonical_transfer_records(transfers):
        from_address = normalize_address(row.get("from_address"))
        to_address = normalize_address(row.get("to_address"))
        if target not in {from_address, to_address}:
            continue

        amount_usd = decimal_value(row.get("amount_usd_value", row.get("amount_usd")))
        largest_transfer_usd = max(largest_transfer_usd, amount_usd)
        token_key = normalize_address(row.get("token_address")) or str(row.get("token_symbol", ""))
        if token_key:
            tokens[token_key] += 1
        if row.get("tx_hash"):
            tx_hashes.add(str(row["tx_hash"]))

        if from_address == target:
            sent_count += 1
            total_sent_usd += amount_usd
            if to_address:
                counterparties[to_address] += 1
        if to_address == target:
            received_count += 1
            total_received_usd += amount_usd
            if from_address:
                counterparties[from_address] += 1

    blacklist_counterparty_count = sum(1 for item in counterparties if item in blacklist_normalized)

    return AddressProfile(
        address=target,
        tx_count=len(tx_hashes),
        sent_count=sent_count,
        received_count=received_count,
        unique_counterparties=len(counterparties),
        unique_tokens=len(tokens),
        total_sent_usd=total_sent_usd,
        total_received_usd=total_received_usd,
        net_usd=total_received_usd - total_sent_usd,
        largest_transfer_usd=largest_transfer_usd,
        blacklist_counterparty_count=blacklist_counterparty_count,
        top_counterparties=counterparties.most_common(max_ranked_items),
        top_tokens=tokens.most_common(max_ranked_items),
    )


def profile_to_dict(profile: AddressProfile) -> dict[str, Any]:
    values = profile.__dict__.copy()
    for key, value in list(values.items()):
        if isinstance(value, Decimal):
            values[key] = str(value)
    return values
