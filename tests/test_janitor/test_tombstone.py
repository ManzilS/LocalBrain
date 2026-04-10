"""Tests for the tombstone cascade — soft delete and purge."""

from __future__ import annotations

import pytest

from src.core.models import Chunk, FileIdentity, FileRecord
from src.vault.lance_engine import LanceEngine
from src.vault.ref_counting import RefCounter
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.subscriptions import SubscriptionManager
from src.janitor.tombstone import TombstoneCascade


@pytest.fixture
async def engine(tmp_path):
    e = SQLiteEngine(str(tmp_path / "test.db"))
    await e.open()
    yield e
    await e.close()


@pytest.fixture
async def lance(tmp_path):
    e = LanceEngine(str(tmp_path / "lance_test"), embedding_dim=4)
    await e.open()
    yield e
    await e.close()


@pytest.fixture
async def cascade(engine, lance):
    subs = SubscriptionManager(engine.db)
    refs = RefCounter(engine.db)
    return TombstoneCascade(engine, lance, subs, refs)


async def _seed(engine, file_id: str, chunk_ids: list[str]):
    rec = FileRecord(id=file_id, identity=FileIdentity(path=f"/{file_id}.txt"))
    await engine.upsert_file(rec)

    subs = SubscriptionManager(engine.db)
    refs = RefCounter(engine.db)

    for cid in chunk_ids:
        c = Chunk(id=cid, content="x", fingerprint=f"fp_{cid}")
        await engine.upsert_chunk(c)
    await subs.subscribe(file_id, chunk_ids)
    await refs.bulk_increment(chunk_ids)


@pytest.mark.asyncio
async def test_mark_deleted(engine, cascade):
    await _seed(engine, "f1", ["c1", "c2"])

    await cascade.mark_deleted("f1")

    record = await engine.get_file_by_id("f1")
    assert record is not None
    assert record.status.value == "tombstone"
    assert record.deleted_at is not None


@pytest.mark.asyncio
async def test_purge_removes_old_tombstones(engine, lance, cascade):
    await _seed(engine, "f1", ["c1"])
    await cascade.mark_deleted("f1")

    # With older_than_days=0, should purge immediately
    count = await cascade.purge(older_than_days=0)
    assert count >= 1
