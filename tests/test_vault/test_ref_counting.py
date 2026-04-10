"""Tests for ACID reference counting on shared chunks."""

from __future__ import annotations

import pytest

from src.core.models import Chunk
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.ref_counting import RefCounter


@pytest.fixture
async def engine(tmp_path):
    e = SQLiteEngine(str(tmp_path / "test.db"))
    await e.open()
    yield e
    await e.close()


@pytest.fixture
async def refs(engine):
    return RefCounter(engine.db)


async def _seed_chunk(engine, chunk_id: str):
    c = Chunk(id=chunk_id, content="x", fingerprint=f"fp_{chunk_id}")
    await engine.upsert_chunk(c)


@pytest.mark.asyncio
async def test_increment(engine, refs):
    await _seed_chunk(engine, "c1")
    count = await refs.increment("c1")
    assert count == 1

    count = await refs.increment("c1")
    assert count == 2


@pytest.mark.asyncio
async def test_decrement(engine, refs):
    await _seed_chunk(engine, "c1")
    await refs.increment("c1")
    await refs.increment("c1")

    count = await refs.decrement("c1")
    assert count == 1


@pytest.mark.asyncio
async def test_decrement_floor_zero(engine, refs):
    await _seed_chunk(engine, "c1")
    count = await refs.decrement("c1")
    assert count == 0

    # Should stay at zero
    count = await refs.decrement("c1")
    assert count == 0


@pytest.mark.asyncio
async def test_get_count(engine, refs):
    await _seed_chunk(engine, "c1")
    await refs.increment("c1")
    assert await refs.get_count("c1") == 1


@pytest.mark.asyncio
async def test_get_orphans(engine, refs):
    await _seed_chunk(engine, "c1")
    await _seed_chunk(engine, "c2")
    await refs.increment("c1")
    # c2 has ref_count=0

    orphans = await refs.get_orphans()
    assert "c2" in orphans
    assert "c1" not in orphans


@pytest.mark.asyncio
async def test_bulk_increment(engine, refs):
    await _seed_chunk(engine, "c1")
    await _seed_chunk(engine, "c2")

    await refs.bulk_increment(["c1", "c2"])
    assert await refs.get_count("c1") == 1
    assert await refs.get_count("c2") == 1


@pytest.mark.asyncio
async def test_bulk_decrement_returns_orphans(engine, refs):
    await _seed_chunk(engine, "c1")
    await _seed_chunk(engine, "c2")
    await refs.bulk_increment(["c1", "c2"])

    orphans = await refs.bulk_decrement(["c1", "c2"])
    assert set(orphans) == {"c1", "c2"}


@pytest.mark.asyncio
async def test_bulk_empty_list(engine, refs):
    await refs.bulk_increment([])
    orphans = await refs.bulk_decrement([])
    assert orphans == []
