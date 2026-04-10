"""Tests for the many-to-many document-chunk subscription manager."""

from __future__ import annotations

import pytest

from src.core.models import Chunk, FileIdentity, FileRecord
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.subscriptions import SubscriptionManager


@pytest.fixture
async def engine(tmp_path):
    e = SQLiteEngine(str(tmp_path / "test.db"))
    await e.open()
    yield e
    await e.close()


@pytest.fixture
async def subs(engine):
    return SubscriptionManager(engine.db)


async def _seed_file_and_chunks(engine, file_id: str, chunk_ids: list[str]):
    """Insert a file and chunks into the DB so FK constraints are satisfied."""
    rec = FileRecord(id=file_id, identity=FileIdentity(path=f"/{file_id}.txt"))
    await engine.upsert_file(rec)
    for cid in chunk_ids:
        c = Chunk(id=cid, content="x", fingerprint=f"fp_{cid}")
        await engine.upsert_chunk(c)


@pytest.mark.asyncio
async def test_subscribe_and_get_chunks(engine, subs):
    await _seed_file_and_chunks(engine, "f1", ["c1", "c2", "c3"])
    await subs.subscribe("f1", ["c1", "c2", "c3"])

    chunks = await subs.get_chunks("f1")
    assert chunks == ["c1", "c2", "c3"]


@pytest.mark.asyncio
async def test_get_subscribers(engine, subs):
    await _seed_file_and_chunks(engine, "f1", ["c1"])
    await _seed_file_and_chunks(engine, "f2", ["c1"])  # c1 already exists but that's fine
    await subs.subscribe("f1", ["c1"])
    await subs.subscribe("f2", ["c1"])

    subscribers = await subs.get_subscribers("c1")
    assert set(subscribers) == {"f1", "f2"}


@pytest.mark.asyncio
async def test_unsubscribe(engine, subs):
    await _seed_file_and_chunks(engine, "f1", ["c1", "c2"])
    await subs.subscribe("f1", ["c1", "c2"])

    orphaned = await subs.unsubscribe("f1")
    assert set(orphaned) == {"c1", "c2"}

    chunks = await subs.get_chunks("f1")
    assert chunks == []


@pytest.mark.asyncio
async def test_get_chunk_count(engine, subs):
    await _seed_file_and_chunks(engine, "f1", ["c1", "c2"])
    await subs.subscribe("f1", ["c1", "c2"])

    count = await subs.get_chunk_count("f1")
    assert count == 2


@pytest.mark.asyncio
async def test_empty_file(engine, subs):
    count = await subs.get_chunk_count("nonexistent")
    assert count == 0
