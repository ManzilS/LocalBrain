"""Community detection + summary generation worker.

Runs greedy-modularity community detection over the Kuzu entity graph
via :mod:`src.retrieval.communities` and writes one
:class:`CommunitySummary` node per cluster.

The textual summary itself is assembled by a pluggable
``summarizer_fn`` — if you pass a real LLM-backed callable through, you
get GraphRAG-style community summaries. The default is a deterministic
template so the worker produces *something* without an LLM, clearly
labelled as such.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Awaitable, Callable

from src.retrieval.communities import detect_communities
from src.utils.config import Settings
from src.vault.kuzu_store import KuzuStore

logger = logging.getLogger(__name__)

SummarizerFn = Callable[[list[dict]], Awaitable[str]]


async def _default_summarizer(entities: list[dict]) -> str:
    """Deterministic fallback summary — no LLM involvement."""
    names = [e["name"] for e in entities if e.get("name")][:8]
    joined = ", ".join(names) if names else "(unnamed entities)"
    return (
        f"[auto-generated template] Community of {len(entities)} entities "
        f"including {joined}."
    )


def _community_id(entity_ids: list[str]) -> str:
    """Stable ID for a community — hash of its sorted member IDs."""
    h = hashlib.sha1("|".join(sorted(entity_ids)).encode("utf-8")).hexdigest()[:16]
    return f"community_{h}"


class CommunitySummarizer:
    def __init__(
        self,
        kuzu: KuzuStore,
        settings: Settings,
        summarizer_fn: SummarizerFn | None = None,
    ) -> None:
        self.kuzu = kuzu
        self.settings = settings
        self.summarizer_fn: SummarizerFn = summarizer_fn or _default_summarizer
        self._last_entity_count = 0

    async def run_batch(self) -> int:
        if not (
            self.settings.enable_graphrag
            and self.settings.enable_ms_graphrag_summarization
        ):
            return 0

        entity_count = await self.kuzu.count_entities()
        if entity_count < self.settings.graph_summary_min_entities:
            return 0
        # Skip if nothing meaningful has changed since the last run.
        if entity_count == self._last_entity_count:
            return 0

        entities = await self.kuzu.get_all_entities()
        entity_by_id = {e["id"]: e for e in entities}
        edges = await self.kuzu.get_relationships()

        clusters = detect_communities(
            nodes=[e["id"] for e in entities], edges=edges
        )
        if not clusters:
            return 0

        # Rewrite-in-place: drop old summaries, regenerate. Summaries
        # are cheap compared to extraction and avoid stale/orphan
        # clusters from accreting over time.
        await self.kuzu.clear_community_summaries()

        now = time.time()
        written = 0
        for cluster in clusters:
            members = [entity_by_id[eid] for eid in cluster if eid in entity_by_id]
            if len(members) < 2:
                continue
            cid = _community_id(cluster)
            summary_text = await self.summarizer_fn(members)
            await self.kuzu.upsert_community_summary(cid, summary_text, len(members), now)
            for member in members:
                await self.kuzu.link_summary_to_entity(cid, member["id"])
            written += 1

        self._last_entity_count = entity_count
        logger.info(
            "CommunitySummarizer: wrote %d summary(ies) over %d entities",
            written,
            entity_count,
        )
        return written
