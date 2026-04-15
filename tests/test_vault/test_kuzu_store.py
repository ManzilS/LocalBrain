"""Integration tests for the Kuzu graph store."""

from __future__ import annotations

import pytest

from src.vault.kuzu_store import KuzuStore, entity_id


def test_entity_id_is_stable_and_case_insensitive() -> None:
    assert entity_id("Python") == entity_id("python")
    assert entity_id("Python", "Language") != entity_id("Python", "Concept")
    # Deterministic / bounded width
    assert entity_id("x").startswith("ent_")
    assert len(entity_id("x")) == 4 + 16


@pytest.fixture
async def store(tmp_path):
    s = KuzuStore(str(tmp_path / "kuzu"))
    await s.open()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_upsert_entity_and_read_back(store: KuzuStore) -> None:
    eid = entity_id("Ada")
    await store.upsert_entity(eid, "Ada", "Person", "Mathematician")
    entities = await store.get_all_entities()
    assert any(e["id"] == eid and e["name"] == "Ada" for e in entities)
    assert await store.count_entities() == 1


@pytest.mark.asyncio
async def test_upsert_is_idempotent(store: KuzuStore) -> None:
    eid = entity_id("Ada")
    await store.upsert_entity(eid, "Ada", "Person", "v1")
    await store.upsert_entity(eid, "Ada", "Person", "v2")
    assert await store.count_entities() == 1
    entities = await store.get_all_entities()
    assert entities[0]["description"] == "v2"


@pytest.mark.asyncio
async def test_relationships_and_traversal(store: KuzuStore) -> None:
    a, b, c = entity_id("A"), entity_id("B"), entity_id("C")
    for eid, name in ((a, "A"), (b, "B"), (c, "C")):
        await store.upsert_entity(eid, name, "Concept", "")
    await store.add_relationship(a, b, "ab", 1.0)
    await store.add_relationship(b, c, "bc", 1.0)

    edges = await store.get_relationships()
    assert (a, b, 1.0) in edges
    assert (b, c, 1.0) in edges

    ctx = await store.get_context_for_entity(a, hop_limit=2)
    ids = {x["id"] for x in ctx}
    assert b in ids and c in ids


@pytest.mark.asyncio
async def test_delete_chunks_cascades_to_orphan_entities(store: KuzuStore) -> None:
    await store.upsert_chunk("ch1", "content")
    eid = entity_id("OnlyInChunk1")
    await store.upsert_entity(eid, "OnlyInChunk1", "Concept", "")
    await store.link_entity_to_chunk(eid, "ch1")
    assert await store.count_entities() == 1

    await store.delete_chunks(["ch1"])
    # Orphan entity should be swept.
    assert await store.count_entities() == 0


@pytest.mark.asyncio
async def test_community_summary_roundtrip(store: KuzuStore) -> None:
    await store.upsert_community_summary("c1", "Topic: AI", size=3, created_at=1.0)
    summaries = await store.get_community_summaries()
    assert len(summaries) == 1
    assert summaries[0]["summary"] == "Topic: AI"
    assert summaries[0]["size"] == 3

    await store.clear_community_summaries()
    assert await store.get_community_summaries() == []
