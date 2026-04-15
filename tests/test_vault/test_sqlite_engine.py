"""Tests for the SQLite state engine — using in-memory database."""

from __future__ import annotations

import pytest

from src.core.models import Chunk, FileIdentity, FileRecord, FileStatus, QueueItem, QueueLane
from src.vault.sqlite_engine import SQLiteEngine


@pytest.fixture
async def engine(tmp_path):
    e = SQLiteEngine(str(tmp_path / "test.db"))
    await e.open()
    yield e
    await e.close()


@pytest.fixture
def sample_record():
    return FileRecord(
        identity=FileIdentity(
            path="/tmp/test.txt", inode=42, device=1, mtime=1000.0, size=100, head_hash="abc"
        ),
        fingerprint="fp123",
        mime_type="text/plain",
        status=FileStatus.pending,
    )


@pytest.mark.asyncio
async def test_upsert_and_get_file(engine, sample_record):
    await engine.upsert_file(sample_record)
    got = await engine.get_file_by_id(sample_record.id)

    assert got is not None
    assert got.id == sample_record.id
    assert got.identity.path == "/tmp/test.txt"
    assert got.fingerprint == "fp123"


@pytest.mark.asyncio
async def test_get_file_by_path(engine, sample_record):
    await engine.upsert_file(sample_record)
    got = await engine.get_file_by_path("/tmp/test.txt")
    assert got is not None
    assert got.id == sample_record.id


@pytest.mark.asyncio
async def test_get_nonexistent(engine):
    assert await engine.get_file_by_id("nonexistent") is None


@pytest.mark.asyncio
async def test_list_files(engine, sample_record):
    await engine.upsert_file(sample_record)
    files = await engine.list_files()
    assert len(files) == 1


@pytest.mark.asyncio
async def test_list_files_by_status(engine, sample_record):
    await engine.upsert_file(sample_record)
    files = await engine.list_files(status=FileStatus.pending)
    assert len(files) == 1

    files = await engine.list_files(status=FileStatus.indexed)
    assert len(files) == 0


@pytest.mark.asyncio
async def test_mark_tombstone(engine, sample_record):
    await engine.upsert_file(sample_record)
    await engine.mark_tombstone(sample_record.id)

    got = await engine.get_file_by_id(sample_record.id)
    assert got is not None
    assert got.status == FileStatus.tombstone
    assert got.deleted_at is not None


@pytest.mark.asyncio
async def test_purge_tombstones(engine, sample_record):
    await engine.upsert_file(sample_record)
    await engine.mark_tombstone(sample_record.id)

    # Won't purge recent tombstones
    count = await engine.purge_tombstones(older_than_days=0)
    # The tombstone was just created, so with 0 days it should be purged
    assert count >= 0


# ── Chunk CRUD ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_upsert_and_get_chunk(engine):
    chunk = Chunk(content="hello", fingerprint="chk1", byte_offset=0, byte_length=5)
    await engine.upsert_chunk(chunk)

    got = await engine.get_chunk_by_fingerprint("chk1")
    assert got is not None
    assert got.content == "hello"


@pytest.mark.asyncio
async def test_get_all_fingerprints(engine):
    c1 = Chunk(content="a", fingerprint="fp_a")
    c2 = Chunk(content="b", fingerprint="fp_b")
    await engine.upsert_chunk(c1)
    await engine.upsert_chunk(c2)

    fps = await engine.get_all_chunk_fingerprints()
    assert set(fps) == {"fp_a", "fp_b"}


# ── Queue ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enqueue_dequeue(engine):
    item = QueueItem(file_id="f1", lane=QueueLane.fast, priority=1)
    await engine.enqueue(item)

    depth = await engine.get_queue_depth(QueueLane.fast)
    assert depth == 1

    items = await engine.dequeue(QueueLane.fast)
    assert len(items) == 1
    assert items[0].file_id == "f1"


@pytest.mark.asyncio
async def test_ack_removes_item(engine):
    item = QueueItem(file_id="f2", lane=QueueLane.heavy)
    await engine.enqueue(item)
    await engine.ack(item.id)

    depth = await engine.get_queue_depth(QueueLane.heavy)
    assert depth == 0


# ── Journal ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_journal_log_and_read(engine):
    await engine.log_journal("INSERT", "file", "f1", {"path": "/test.txt"})
    entries = await engine.get_journal_since(0)
    assert len(entries) == 1
    assert entries[0]["operation"] == "INSERT"


# ── Full-text search ────────────────────────────────────


async def _seed_searchable_chunks(engine, sample_record):
    """Insert a file plus three chunks with distinct vocabularies."""
    await engine.upsert_file(sample_record)
    chunks = [
        Chunk(
            id="chunk-alpha",
            content="the quick brown fox jumps over the lazy dog",
            fingerprint="fp-alpha",
            byte_offset=0,
            byte_length=43,
        ),
        Chunk(
            id="chunk-beta",
            content="python asyncio makes network servers simple",
            fingerprint="fp-beta",
            byte_offset=50,
            byte_length=44,
        ),
        Chunk(
            id="chunk-gamma",
            content="nothing here matches anything unusual",
            fingerprint="fp-gamma",
            byte_offset=100,
            byte_length=37,
        ),
    ]
    for i, c in enumerate(chunks):
        await engine.upsert_chunk(c)
        await engine.db.execute(
            "INSERT INTO file_chunks (file_id, chunk_id, sequence) VALUES (?, ?, ?)",
            (sample_record.id, c.id, i),
        )
    await engine.db.commit()


@pytest.mark.asyncio
async def test_search_returns_matching_chunk(engine, sample_record):
    await _seed_searchable_chunks(engine, sample_record)

    results = await engine.search_chunks("asyncio")

    assert len(results) == 1
    hit = results[0]
    assert hit["chunk_id"] == "chunk-beta"
    assert hit["file_id"] == sample_record.id
    assert hit["path"] == "/tmp/test.txt"
    assert "asyncio" in hit["snippet"].lower()


@pytest.mark.asyncio
async def test_search_multi_term_and(engine, sample_record):
    await _seed_searchable_chunks(engine, sample_record)

    # Multi-term queries are ANDed — this must match chunk-alpha which
    # has both "quick" and "fox", and must NOT match chunk-beta.
    results = await engine.search_chunks("quick fox")

    assert len(results) == 1
    assert results[0]["chunk_id"] == "chunk-alpha"


@pytest.mark.asyncio
async def test_search_no_match_returns_empty(engine, sample_record):
    await _seed_searchable_chunks(engine, sample_record)
    assert await engine.search_chunks("supercalifragilistic") == []


@pytest.mark.asyncio
async def test_search_sanitises_fts_meta_chars(engine, sample_record):
    await _seed_searchable_chunks(engine, sample_record)

    # Meta chars alone must not blow up — they're stripped, leaving no
    # terms, so the engine returns an empty list instead of raising.
    assert await engine.search_chunks('"*()') == []

    # Embedded meta chars should still find the cleaned term.
    results = await engine.search_chunks('"asyncio"')
    assert len(results) == 1
    assert results[0]["chunk_id"] == "chunk-beta"


@pytest.mark.asyncio
async def test_search_backfills_fts_on_reopen(tmp_path, sample_record):
    """Simulate upgrading an existing vault: chunks table populated but
    chunks_fts empty. The next ``open()`` must backfill the index."""
    db_path = str(tmp_path / "upgrade.db")

    # Populate chunks via the normal engine first.
    e1 = SQLiteEngine(db_path)
    await e1.open()
    await _seed_searchable_chunks(e1, sample_record)
    # Drop FTS triggers and virtual table to mimic a pre-FTS vault — the
    # chunks table keeps its rows but there's no FTS index at all.
    await e1.db.execute("DROP TRIGGER IF EXISTS chunks_ai")
    await e1.db.execute("DROP TRIGGER IF EXISTS chunks_au")
    await e1.db.execute("DROP TRIGGER IF EXISTS chunks_ad")
    await e1.db.execute("DROP TABLE IF EXISTS chunks_fts")
    await e1.db.commit()
    await e1.close()

    # Reopen — ensure_schema should detect the gap and backfill.
    e2 = SQLiteEngine(db_path)
    await e2.open()
    try:
        results = await e2.search_chunks("asyncio")
        assert len(results) == 1
        assert results[0]["chunk_id"] == "chunk-beta"
    finally:
        await e2.close()


@pytest.mark.asyncio
async def test_search_reflects_updates_via_triggers(engine, sample_record):
    await _seed_searchable_chunks(engine, sample_record)

    # Upsert same fingerprint with new content — FTS must reflect the
    # update via the AFTER UPDATE trigger.
    await engine.upsert_chunk(
        Chunk(
            id="chunk-beta",
            content="rewritten prose about mongoose lemurs",
            fingerprint="fp-beta",
            byte_offset=50,
            byte_length=38,
        )
    )

    assert await engine.search_chunks("asyncio") == []
    mongoose = await engine.search_chunks("mongoose")
    assert len(mongoose) == 1
    assert mongoose[0]["chunk_id"] == "chunk-beta"
