"""Relationship graph builders for token flows."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from analytics.exceptions import OptionalDependencyError
from analytics.features._records import decimal_value, iter_records, normalize_address


def build_flow_graph(transfers: Any, min_amount_usd: Decimal | int | str = 0) -> Any:
    """Build a directed NetworkX graph from transfer rows."""

    try:
        import networkx as nx
    except ModuleNotFoundError as exc:
        raise OptionalDependencyError("networkx", "flow graph analysis") from exc

    threshold = decimal_value(min_amount_usd)
    graph = nx.DiGraph()

    for row in iter_records(transfers):
        from_address = normalize_address(row.get("from_address"))
        to_address = normalize_address(row.get("to_address"))
        if not from_address or not to_address:
            continue

        amount_usd = decimal_value(row.get("amount_usd_value", row.get("amount_usd")))
        if amount_usd < threshold:
            continue

        if graph.has_edge(from_address, to_address):
            edge = graph[from_address][to_address]
            edge["transfer_count"] += 1
            edge["amount_usd"] += amount_usd
        else:
            graph.add_edge(
                from_address,
                to_address,
                transfer_count=1,
                amount_usd=amount_usd,
            )

    return graph


def top_counterparties(graph: Any, address: str, limit: int = 10) -> list[tuple[str, Decimal]]:
    target = normalize_address(address)
    totals: dict[str, Decimal] = {}
    if target in graph:
        for _, counterparty, data in graph.out_edges(target, data=True):
            totals[counterparty] = totals.get(counterparty, Decimal("0")) + data.get(
                "amount_usd", Decimal("0")
            )
        for counterparty, _, data in graph.in_edges(target, data=True):
            totals[counterparty] = totals.get(counterparty, Decimal("0")) + data.get(
                "amount_usd", Decimal("0")
            )

    return sorted(totals.items(), key=lambda item: item[1], reverse=True)[:limit]
