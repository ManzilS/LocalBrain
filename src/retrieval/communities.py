"""Community detection over the entity graph.

Uses ``networkx``'s greedy modularity algorithm — close enough to
Leiden for a local vault and available in the stdlib networkx package
without extra deps. Returns a list of entity-id clusters, sorted by
size (largest first).
"""

from __future__ import annotations

import logging
from typing import Iterable

logger = logging.getLogger(__name__)


def detect_communities(
    nodes: Iterable[str],
    edges: Iterable[tuple[str, str, float]],
    *,
    min_size: int = 2,
    max_communities: int = 20,
) -> list[list[str]]:
    """Return entity-ID clusters from the given graph.

    Falls back to a single bucket if ``networkx`` is unavailable, so
    the summarizer still produces *something* without an extra dep.
    """
    node_list = list(nodes)
    edge_list = list(edges)

    if not node_list:
        return []

    try:
        import networkx as nx
        from networkx.algorithms.community import greedy_modularity_communities
    except ImportError:  # pragma: no cover
        logger.info("networkx not installed — using naive single-community fallback")
        return [node_list[:50]] if len(node_list) >= min_size else []

    g = nx.Graph()
    g.add_nodes_from(node_list)
    for src, dst, weight in edge_list:
        g.add_edge(src, dst, weight=max(weight, 0.01))

    try:
        raw = list(greedy_modularity_communities(g, weight="weight"))
    except Exception as exc:  # pragma: no cover — pathological graphs
        logger.warning("Community detection failed: %s", exc)
        return []

    clusters = [sorted(c) for c in raw if len(c) >= min_size]
    clusters.sort(key=len, reverse=True)
    return clusters[:max_communities]
