"""Blacklist relationship graph helpers."""

from __future__ import annotations

from typing import Any, Iterable

from analytics.features._records import normalize_address


def blacklist_hop_distance(
    graph: Any,
    address: str,
    blacklist: Iterable[str],
    max_hops: int = 3,
) -> int | None:
    """Return the shortest undirected hop distance to a blacklist address."""

    target = normalize_address(address)
    blacklist_set = {normalize_address(item) for item in blacklist}
    if target in blacklist_set:
        return 0
    if target not in graph:
        return None

    undirected = graph.to_undirected() if hasattr(graph, "to_undirected") else graph
    best: int | None = None
    for blacklisted in blacklist_set:
        if blacklisted not in undirected:
            continue
        try:
            distance = _shortest_path_length(undirected, target, blacklisted)
        except Exception:
            continue
        if distance <= max_hops:
            best = distance if best is None else min(best, distance)
    return best


def blacklist_paths(
    graph: Any,
    address: str,
    blacklist: Iterable[str],
    max_hops: int = 3,
) -> list[list[str]]:
    """Return simple paths from address to blacklist nodes up to max_hops."""

    target = normalize_address(address)
    undirected = graph.to_undirected() if hasattr(graph, "to_undirected") else graph
    paths: list[list[str]] = []

    for blacklisted in {normalize_address(item) for item in blacklist}:
        if target not in undirected or blacklisted not in undirected:
            continue
        try:
            path = _shortest_path(undirected, target, blacklisted)
        except Exception:
            continue
        if len(path) - 1 <= max_hops:
            paths.append(path)
    return paths


def _shortest_path_length(graph: Any, source: str, target: str) -> int:
    try:
        import networkx as nx
    except ModuleNotFoundError:
        if hasattr(graph, "shortest_path_length"):
            return graph.shortest_path_length(source, target)
        raise
    return int(nx.shortest_path_length(graph, source=source, target=target))


def _shortest_path(graph: Any, source: str, target: str) -> list[str]:
    try:
        import networkx as nx
    except ModuleNotFoundError:
        if hasattr(graph, "shortest_path"):
            return list(graph.shortest_path(source, target))
        raise
    return list(nx.shortest_path(graph, source=source, target=target))
