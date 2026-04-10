"""Tests for the ingestion pipeline with mocked dependencies."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.models import (
    Chunk,
    EventType,
    FileIdentity,
    FileRecord,
    FileStatus,
    IngestEvent,
    ParseResult,
)
from src.core.pipeline import IngestPipeline
from src.core.state import IngestState


@pytest.fixture
def mock_deps(tmp_path):
    """Create a pipeline with all dependencies mocked."""
    gate = MagicMock()
    gate.enforce = MagicMock()

    resolver = MagicMock()
    resolver.resolve.return_value = FileIdentity(
        path=str(tmp_path / "test.txt"), inode=1, device=1, mtime=100, size=50, head_hash="hh"
    )
    resolver.has_changed.return_value = True

    registry = MagicMock()
    mock_parser = AsyncMock()
    mock_parser.can_parse = AsyncMock(return_value=True)
    mock_parser.parse = AsyncMock(
        return_value=ParseResult(content="Hello world content", metadata={"lines": 1})
    )
    registry.find_parser = AsyncMock(return_value=mock_parser)

    dedup = MagicMock()
    dedup.is_duplicate.return_value = False
    dedup.register = MagicMock()

    engine = AsyncMock()
    engine.get_file_by_path = AsyncMock(return_value=None)
    engine.upsert_file = AsyncMock()
    engine.upsert_chunk = AsyncMock()
    engine.get_chunk_by_fingerprint = AsyncMock(return_value=None)
    engine.log_journal = AsyncMock()

    subs = AsyncMock()
    subs.subscribe = AsyncMock()

    refs = AsyncMock()
    refs.bulk_increment = AsyncMock()

    queue = AsyncMock()
    queue.is_full = AsyncMock(return_value=False)
    queue.enqueue = AsyncMock(return_value="item1")

    return {
        "gate": gate,
        "resolver": resolver,
        "registry": registry,
        "dedup": dedup,
        "engine": engine,
        "subs": subs,
        "refs": refs,
        "queue": queue,
        "tmp_path": tmp_path,
    }


@pytest.fixture
def pipeline(mock_deps):
    return IngestPipeline(
        scope_gate=mock_deps["gate"],
        identity_resolver=mock_deps["resolver"],
        registry=mock_deps["registry"],
        deduplicator=mock_deps["dedup"],
        engine=mock_deps["engine"],
        subscriptions=mock_deps["subs"],
        ref_counter=mock_deps["refs"],
        queue=mock_deps["queue"],
    )


def _make_event(tmp_path, event_type=EventType.created) -> IngestEvent:
    return IngestEvent(
        event_type=event_type,
        file_identity=FileIdentity(path=str(tmp_path / "test.txt")),
    )


@pytest.mark.asyncio
async def test_delete_event_marks_tombstone(pipeline, mock_deps):
    tmp_path = mock_deps["tmp_path"]
    existing = FileRecord(id="f1", identity=FileIdentity(path=str(tmp_path / "test.txt")))
    mock_deps["engine"].get_file_by_path = AsyncMock(return_value=existing)

    event = _make_event(tmp_path, EventType.deleted)
    state = IngestState(event=event)
    result = await pipeline.execute(state)

    assert result.early_exit is True
    mock_deps["engine"].mark_tombstone.assert_called_once_with("f1")


@pytest.mark.asyncio
async def test_unchanged_file_skipped(pipeline, mock_deps):
    tmp_path = mock_deps["tmp_path"]
    # File must exist on disk for stat() in ingress phase
    f = tmp_path / "test.txt"
    f.write_text("existing content")

    existing = FileRecord(
        identity=FileIdentity(path=str(tmp_path / "test.txt")),
        status=FileStatus.indexed,
    )
    mock_deps["engine"].get_file_by_path = AsyncMock(return_value=existing)
    mock_deps["resolver"].has_changed.return_value = False

    event = _make_event(tmp_path)
    state = IngestState(event=event)
    result = await pipeline.execute(state)

    assert result.early_exit is True


@pytest.mark.asyncio
async def test_full_pipeline_new_file(pipeline, mock_deps):
    tmp_path = mock_deps["tmp_path"]
    # Create actual file for read_bytes()
    f = tmp_path / "test.txt"
    f.write_text("Hello world content")

    event = _make_event(tmp_path)
    state = IngestState(event=event)
    result = await pipeline.execute(state)

    assert result.error is None
    assert result.phase == "store"
    assert result.file_record is not None
    assert result.file_record.status == FileStatus.indexed
    # Store phase uses no-commit variants inside a transaction
    mock_deps["engine"].upsert_file_no_commit.assert_called_once()
    mock_deps["subs"].subscribe_no_commit.assert_called_once()


@pytest.mark.asyncio
async def test_no_parser_sets_error(pipeline, mock_deps):
    tmp_path = mock_deps["tmp_path"]
    f = tmp_path / "test.txt"
    f.write_text("data")
    mock_deps["registry"].find_parser = AsyncMock(return_value=None)

    event = _make_event(tmp_path)
    state = IngestState(event=event)
    result = await pipeline.execute(state)

    assert result.early_exit is True
    assert result.error is not None
    assert "No parser" in result.error
