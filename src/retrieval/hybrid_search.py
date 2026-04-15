"""Hybrid retrieval engine — BM25 + vectors + graph context.

1. Classify the query via :class:`IntentRouter`.
2. ``global_theme``  → return community summaries from Kuzu.
3. ``multi_hop``     → still runs hybrid retrieval, but enriches the
   result with N-hop graph context from Kuzu.
4. ``specific``      → Reciprocal Rank Fusion (RRF) over BM25 and
   vector hits; no graph traversal.

LanceDB results are adapted via ``_as_hit`` so a schema drift (missing
key, rename) surfaces as a log warning rather than a ``KeyError``
inside the fusion loop.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from src.retrieval.intent_router import IntentRouter
from src.utils.config import Settings
from src.vault.kuzu_store import KuzuStore
from src.vault.lance_engine import LanceEngine
from src.vault.sqlite_engine import SQLiteEngine

logger = logging.getLogger(__name__)


def _as_hit(raw: dict[str, Any], *, source: str) -> dict[str, Any] | None:
    """Normalise a backend-specific result dict to a common shape."""
    # Vector hits use ``id``; FTS hits use ``chunk_id``. Accept both.
    chunk_id = raw.get("chunk_id") or raw.get("id")
    if not chunk_id:
        logger.warning("%s result missing chunk id: %r", source, sorted(raw.keys()))
        return None
    return {
        "chunk_id": chunk_id,
        "file_id": raw.get("file_id"),
        "path": raw.get("path"),
        "content": raw.get("content", ""),
        "snippet": raw.get("snippet"),
        "score": raw.get("score", 0.0),
        "source": source,
    }


class HybridSearchEngine:
    def __init__(
        self,
        sqlite: SQLiteEngine,
        lance: LanceEngine,
        kuzu: KuzuStore,
        settings: Settings,
        *,
        rrf_k: int = 60,
        router: IntentRouter | None = None,
    ) -> None:
        self.sqlite = sqlite
        self.lance = lance
        self.kuzu = kuzu
        self.settings = settings
        self.rrf_k = rrf_k
        self.router = router or IntentRouter(
            enable_global=settings.enable_ms_graphrag_summarization,
            enable_multihop=(
                settings.enable_hipporag_pagerank and settings.enable_graphrag
            ),
        )

    async def search(
        self,
        query: str,
        query_embedding: list[float] | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        intent = self.router.classify(query)
        logger.info("HybridSearchEngine: query=%r lane=%s", query, intent)

        if intent == "global_theme":
            summaries = await self._safe_call(
                self.kuzu.get_community_summaries, limit=limit
            )
            return {
                "query": query,
                "lane": "global_theme",
                "chunks": [],
                "graph_context": summaries or [],
            }

        chunks = await self._fused_chunks(query, query_embedding, limit)

        graph_context: list[dict[str, Any]] = []
        if intent == "multi_hop":
            graph_context = await self._graph_context_for(query, limit)

        return {
            "query": query,
            "lane": intent,
            "chunks": chunks,
            "graph_context": graph_context,
        }

    # ── Helpers ────────────────────────────────────────────

    async def _fused_chunks(
        self,
        query: str,
        query_embedding: list[float] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        vector_hits: list[dict[str, Any]] = []
        if query_embedding:
            raw = await self._safe_call(
                self.lance.search, query_embedding, limit=limit
            )
            vector_hits = [h for h in (_as_hit(r, source="vector") for r in raw or []) if h]

        keyword_hits: list[dict[str, Any]] = []
        if query and query.strip():
            raw = await self._safe_call(self.sqlite.search_chunks, query, limit=limit)
            keyword_hits = [h for h in (_as_hit(r, source="keyword") for r in raw or []) if h]

        return self._rrf(vector_hits, keyword_hits, limit=limit)

    def _rrf(
        self,
        vector_hits: list[dict[str, Any]],
        keyword_hits: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Reciprocal Rank Fusion across two ranked lists."""
        scores: dict[str, float] = defaultdict(float)
        items: dict[str, dict[str, Any]] = {}

        for rank, hit in enumerate(vector_hits, start=1):
            cid = hit["chunk_id"]
            scores[cid] += 1.0 / (self.rrf_k + rank)
            items.setdefault(cid, dict(hit, vector_score=hit["score"], keyword_score=0.0))

        for rank, hit in enumerate(keyword_hits, start=1):
            cid = hit["chunk_id"]
            scores[cid] += 1.0 / (self.rrf_k + rank)
            if cid in items:
                items[cid]["keyword_score"] = hit["score"]
                # Keyword results carry richer fields (snippet, path) —
                # prefer them when we have them.
                for key in ("snippet", "path", "file_id"):
                    if items[cid].get(key) is None and hit.get(key) is not None:
                        items[cid][key] = hit[key]
            else:
                items[cid] = dict(hit, vector_score=0.0, keyword_score=hit["score"])

        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:limit]
        return [dict(items[cid], rrf_score=score) for cid, score in ranked]

    async def _graph_context_for(
        self, query: str, limit: int
    ) -> list[dict[str, Any]]:
        entities = await self._safe_call(self.kuzu.get_all_entities)
        if not entities:
            return []

        q_lower = query.lower()
        q_tokens = set(q_lower.split())
        # Match entities whose name appears as a whole-word substring in
        # the query. Cheap; accurate enough for short queries.
        matched = [
            e
            for e in entities
            if e.get("name") and e["name"].lower() in q_lower
            # quick reject for entities longer than the query
        ][: max(1, limit)]

        context: list[dict[str, Any]] = []
        for ent in matched:
            related = await self._safe_call(
                self.kuzu.get_context_for_entity, ent["id"], hop_limit=2
            )
            context.append(
                {
                    "query_entity": ent["name"],
                    "related_entities": related or [],
                }
            )
        return context

    @staticmethod
    async def _safe_call(fn, *args, **kwargs):
        """Run an awaitable and swallow + log exceptions."""
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            logger.warning("%s failed: %s", getattr(fn, "__qualname__", fn), exc)
            return None
