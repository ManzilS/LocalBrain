"""Tests for smart lazy re-indexing."""

from __future__ import annotations

import pytest

from src.core.models import Chunk, FileIdentity, FileRecord
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.subscriptions import SubscriptionManager
from src.janitor.reindex import ReindexManager


@pytest.fixture
async def engine(tmp_path):
    e = SQLiteEngine(str(tmp_path / "test.db"))
    await e.open()
    yield e
    await e.close()


@pytest.fixture
async def reindex(engine):
    subs = SubscriptionManager(engine.db)
    return ReindexManager(engine, subs, threshold=0.20)


async def _seed(engine, file_id: str, chunk_ids: list[str]):
    rec = FileRecord(id=file_id, identity=FileIdentity(path=f"/{file_id}.txt"))
    await engine.upsert_file(rec)
    subs = SubscriptionManager(engine.db)
    for cid in chunk_ids:
        c = Chunk(id=cid, content="x", fingerprint=f"fp_{cid}")
        await engine.upsert_chunk(c)
    await subs.subscribe(file_id, chunk_ids)


@pytest.mark.asyncio
async def test_below_threshold(engine, reindex):
    await _seed(engine, "f1", ["c1", "c2", "c3", "c4", "c5"])
    # 1 out of 5 changed = 20% → exactly at threshold
    result = await reindex.should_reindex("f1", changed_chunks=1)
    assert result is True


@pytest.mark.asyncio
async def test_above_threshold(engine, reindex):
    await _seed(engine, "f1", ["c1", "c2", "c3", "c4", "c5"])
    result = await reindex.should_reindex("f1", changed_chunks=2)
    assert result is True


@pytest.mark.asyncio
async def test_below_threshold_no_reindex(engine, reindex):
    await _seed(engine, "f1", ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"])
    # 1 out of 10 = 10% < 20%
    result = await reindex.should_reindex("f1", changed_chunks=1)
    assert result is False


@pytest.mark.asyncio
async def test_empty_file_always_reindexes(engine, reindex):
    result = await reindex.should_reindex("nonexistent", changed_chunks=0)
    assert result is True


@pytest.mark.asyncio
async def test_pending_tracking(engine, reindex):
    await _seed(engine, "f1", ["c1"])
    await reindex.should_reindex("f1", changed_chunks=1)

    pending = await reindex.get_pending()
    assert "f1" in pending


@pytest.mark.asyncio
async def test_mark_done(engine, reindex):
    await _seed(engine, "f1", ["c1"])
    await reindex.should_reindex("f1", changed_chunks=1)
    await reindex.mark_done("f1")

    pending = await reindex.get_pending()
    assert "f1" not in pending


def test_is_idle():
    # Just verify the method runs without error
    result = ReindexManager.is_idle()
    assert isinstance(result, bool)


def test_is_on_ac_power():
    result = ReindexManager.is_on_ac_power()
    assert isinstance(result, bool)
