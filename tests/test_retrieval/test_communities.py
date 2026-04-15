"""Community detection via greedy modularity."""

from __future__ import annotations

from src.retrieval.communities import detect_communities


def test_empty_graph_returns_empty() -> None:
    assert detect_communities([], []) == []


def test_singletons_filtered_by_min_size() -> None:
    nodes = ["a", "b", "c"]
    edges: list[tuple[str, str, float]] = []
    # min_size=2 plus zero edges → every node is its own component (size 1)
    # → all filtered.
    assert detect_communities(nodes, edges) == []


def test_two_distinct_clusters() -> None:
    nodes = ["a", "b", "c", "d"]
    edges = [("a", "b", 1.0), ("c", "d", 1.0)]
    clusters = detect_communities(nodes, edges)
    assert len(clusters) == 2
    as_sets = [set(c) for c in clusters]
    assert {"a", "b"} in as_sets
    assert {"c", "d"} in as_sets


def test_clusters_sorted_by_size_desc() -> None:
    nodes = ["a", "b", "c", "d", "e"]
    edges = [
        ("a", "b", 1.0),
        ("b", "c", 1.0),
        ("a", "c", 1.0),
        ("d", "e", 1.0),
    ]
    clusters = detect_communities(nodes, edges)
    assert len(clusters[0]) >= len(clusters[-1])
