"""Entity/relationship extractors for GraphRAG ingestion.

Two implementations ship:

* ``HeuristicExtractor`` — zero-dependency regex stub. It picks
  capitalised noun-phrase candidates and emits co-occurrence edges. It
  is deliberately conservative (stopword filter, length bounds) but
  still noisy; use it only for smoke tests / offline demos.

* ``RouterLLMExtractor`` — delegates to the Router app via the existing
  backpressure queue. Returns structured entities + relations. This is
  the path to enable ``enable_graphrag=True`` in production.

New backends only need to implement the ``EntityExtractor`` Protocol.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# ── Data model ─────────────────────────────────────────────


@dataclass(frozen=True)
class ExtractedEntity:
    name: str
    entity_type: str = "Concept"
    description: str = ""


@dataclass(frozen=True)
class ExtractedRelation:
    source: str  # entity name (not id — extractor-agnostic)
    target: str
    description: str = ""
    weight: float = 1.0


@dataclass
class ExtractionResult:
    entities: list[ExtractedEntity] = field(default_factory=list)
    relations: list[ExtractedRelation] = field(default_factory=list)


@runtime_checkable
class EntityExtractor(Protocol):
    """Strategy interface for pulling structured facts out of a chunk."""

    name: str

    async def extract(self, chunk_id: str, content: str) -> ExtractionResult: ...


# ── Heuristic (offline) backend ────────────────────────────

# Common English words that the capitalised-word heuristic catches as
# false positives (sentence-starters, weekday names, etc.). The list is
# intentionally short — it's a stub, not a real NER model.
_STOPWORDS = frozenset(
    {
        "The", "This", "That", "These", "Those", "Then", "There", "Their",
        "They", "When", "What", "Where", "Which", "While", "With", "Will",
        "Your", "Yours", "About", "Above", "After", "Again", "Also", "Been",
        "Before", "Could", "Does", "During", "Every", "From", "Have", "Here",
        "However", "Into", "Many", "More", "Most", "Much", "Must", "Only",
        "Over", "Said", "Same", "Should", "Some", "Such", "Than", "Through",
        "Under", "Very", "Well", "Were", "What", "Would", "January", "February",
        "March", "April", "June", "July", "August", "September", "October",
        "November", "December", "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday",
    }
)

_CAPS_RUN = re.compile(r"\b[A-Z][a-zA-Z]{3,}(?:\s+[A-Z][a-zA-Z]+){0,2}\b")


class HeuristicExtractor:
    """Offline stub — regex-based capitalised-phrase spotter.

    Not production quality. Entities are capitalised runs of 4+ chars
    minus a small stopword list; relations are co-occurrence edges
    between the first few distinct entities in a chunk.
    """

    name = "heuristic"

    def __init__(self, max_entities_per_chunk: int = 5) -> None:
        self._max = max_entities_per_chunk

    async def extract(self, chunk_id: str, content: str) -> ExtractionResult:
        if not content:
            return ExtractionResult()

        seen: dict[str, ExtractedEntity] = {}
        for match in _CAPS_RUN.finditer(content):
            raw = match.group(0).strip()
            if not raw or raw.split()[0] in _STOPWORDS:
                continue
            key = raw.lower()
            if key in seen:
                continue
            seen[key] = ExtractedEntity(
                name=raw,
                entity_type="Concept",
                description=f"heuristic extraction from chunk {chunk_id}",
            )
            if len(seen) >= self._max:
                break

        entities = list(seen.values())
        relations: list[ExtractedRelation] = []
        # Emit a co-occurrence chain — not a real semantic relation, but
        # at least it's O(n) instead of O(n²) so graphs stay tractable.
        for a, b in zip(entities, entities[1:]):
            relations.append(
                ExtractedRelation(
                    source=a.name,
                    target=b.name,
                    description="co-occurs in chunk",
                    weight=0.5,
                )
            )
        return ExtractionResult(entities=entities, relations=relations)


# ── Router-backed backend ──────────────────────────────────


class RouterLLMExtractor:
    """LLM-backed extractor — delegates to the Router app.

    The Router exposes an ``/extract`` endpoint that accepts chunk
    content and returns a structured entity/relation list. This class
    is a thin HTTP adapter; swap it out to target a different LLM.

    If the Router is unreachable we fall back to an empty result rather
    than raising, so a transient Router outage doesn't wedge the
    janitor loop.
    """

    name = "router_llm"

    def __init__(self, router_url: str, api_key: str = "", timeout: float = 30.0) -> None:
        self._url = router_url.rstrip("/") + "/extract"
        self._api_key = api_key
        self._timeout = timeout

    async def extract(self, chunk_id: str, content: str) -> ExtractionResult:
        if not content.strip():
            return ExtractionResult()
        try:
            import httpx
        except ImportError:
            logger.warning("httpx not installed — RouterLLMExtractor disabled")
            return ExtractionResult()

        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(
                    self._url,
                    headers=headers,
                    json={"chunk_id": chunk_id, "content": content},
                )
                resp.raise_for_status()
                payload = resp.json()
        except Exception as exc:  # pragma: no cover — network path
            logger.warning("RouterLLMExtractor failed for chunk %s: %s", chunk_id, exc)
            return ExtractionResult()

        entities = [
            ExtractedEntity(
                name=e["name"],
                entity_type=e.get("type", "Concept"),
                description=e.get("description", ""),
            )
            for e in payload.get("entities", [])
            if e.get("name")
        ]
        relations = [
            ExtractedRelation(
                source=r["source"],
                target=r["target"],
                description=r.get("description", ""),
                weight=float(r.get("weight", 1.0)),
            )
            for r in payload.get("relations", [])
            if r.get("source") and r.get("target")
        ]
        return ExtractionResult(entities=entities, relations=relations)
