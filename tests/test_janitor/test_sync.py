"""Tests for journal-based filesystem sync."""

from __future__ import annotations

import pytest

from src.core.models import FileIdentity, FileRecord, FileStatus
from src.vault.sqlite_engine import SQLiteEngine
from src.janitor.sync import JournalSync


@pytest.fixture
async def engine(tmp_path):
    e = SQLiteEngine(str(tmp_path / "test.db"))
    await e.open()
    yield e
    await e.close()


@pytest.fixture
async def sync(engine):
    return JournalSync(engine)


@pytest.mark.asyncio
async def test_sync_detects_deleted_file(engine, sync, tmp_path):
    """A tracked file that no longer exists should produce a delete event."""
    rec = FileRecord(
        identity=FileIdentity(path=str(tmp_path / "gone.txt"), mtime=1000, size=10),
        status=FileStatus.indexed,
    )
    await engine.upsert_file(rec)

    events = await sync.sync()
    assert len(events) == 1
    assert events[0].event_type.value == "deleted"


@pytest.mark.asyncio
async def test_sync_detects_modified_file(engine, sync, tmp_path):
    """A tracked file with changed mtime should produce a modified event."""
    f = tmp_path / "exists.txt"
    f.write_text("original")

    rec = FileRecord(
        identity=FileIdentity(
            path=str(f), mtime=0, size=0  # Wrong mtime/size
        ),
        status=FileStatus.indexed,
    )
    await engine.upsert_file(rec)

    events = await sync.sync()
    assert len(events) == 1
    assert events[0].event_type.value == "modified"


@pytest.mark.asyncio
async def test_sync_no_drift(engine, sync, tmp_path):
    """A file that matches the DB should produce no events."""
    f = tmp_path / "stable.txt"
    f.write_text("stable content")
    stat = f.stat()

    rec = FileRecord(
        identity=FileIdentity(
            path=str(f), mtime=stat.st_mtime, size=stat.st_size
        ),
        status=FileStatus.indexed,
    )
    await engine.upsert_file(rec)

    events = await sync.sync()
    assert len(events) == 0
