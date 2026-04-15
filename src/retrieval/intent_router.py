"""Query intent classifier — picks a retrieval lane.

Three lanes:

* ``global_theme``  — MS GraphRAG community summaries
* ``multi_hop``     — HippoRAG-style graph traversal
* ``specific``      — Vector + BM25 hybrid (the default)

We match on whole words only. The previous substring match meant that
"related articles" routed to multi-hop and "overviewed" routed to
global — neither was intended.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_DEFAULT_GLOBAL = frozenset(
    {
        "summarize", "summarise", "summary", "summaries",
        "theme", "themes", "overview", "overall", "general",
        "big-picture",
    }
)

_DEFAULT_MULTIHOP = frozenset(
    {
        "relate", "relates", "related", "relation", "relations",
        "relationship", "relationships",
        "connect", "connects", "connected", "connection", "connections",
        "influence", "influences", "affects",
        "path", "paths", "between",
    }
)

_TOKEN = re.compile(r"[A-Za-z][A-Za-z\-]*")


@dataclass(frozen=True)
class IntentRouter:
    """Keyword-based router. Cheap, deterministic, testable."""

    enable_global: bool = True
    enable_multihop: bool = True
    global_terms: frozenset[str] = _DEFAULT_GLOBAL
    multihop_terms: frozenset[str] = _DEFAULT_MULTIHOP

    def classify(self, query: str) -> str:
        tokens = {t.lower() for t in _TOKEN.findall(query or "")}
        if self.enable_global and tokens & self.global_terms:
            return "global_theme"
        if self.enable_multihop and tokens & self.multihop_terms:
            return "multi_hop"
        return "specific"
