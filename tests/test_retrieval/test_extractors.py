"""Entity extractor heuristics — small but honest contract."""

from __future__ import annotations

import pytest

from src.retrieval.extractors import (
    EntityExtractor,
    HeuristicExtractor,
    ExtractionResult,
)


@pytest.mark.asyncio
async def test_empty_content_returns_empty() -> None:
    ex = HeuristicExtractor()
    res = await ex.extract("c1", "")
    assert res.entities == []
    assert res.relations == []


@pytest.mark.asyncio
async def test_stopwords_are_filtered() -> None:
    ex = HeuristicExtractor()
    res = await ex.extract("c1", "The project started Thursday.")
    names = {e.name.lower() for e in res.entities}
    # Neither sentence-starter "The" nor weekday "Thursday" should show up.
    assert "the" not in names
    assert "thursday" not in names


@pytest.mark.asyncio
async def test_picks_up_proper_nouns() -> None:
    ex = HeuristicExtractor()
    content = "Victor Frankenstein met the creature near Geneva."
    res = await ex.extract("c1", content)
    names = {e.name for e in res.entities}
    # Multi-word proper nouns should come through.
    assert any("Victor" in n for n in names)
    assert "Geneva" in names


@pytest.mark.asyncio
async def test_emits_co_occurrence_relations() -> None:
    ex = HeuristicExtractor()
    res = await ex.extract(
        "c1", "Alice and Bob worked at Acme on Chicago deadlines."
    )
    assert len(res.relations) >= 1
    assert all(r.weight <= 1.0 for r in res.relations)


@pytest.mark.asyncio
async def test_respects_max_entities() -> None:
    ex = HeuristicExtractor(max_entities_per_chunk=2)
    res = await ex.extract(
        "c1", "Alice Bob Carol David Eve Frank Grace Heidi" * 2
    )
    assert len(res.entities) <= 2


def test_protocol_runtime_checkable() -> None:
    assert isinstance(HeuristicExtractor(), EntityExtractor)
