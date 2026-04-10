"""Tests for core data models — serialisation, validation, enums."""

from __future__ import annotations

from src.core.models import (
    Chunk,
    EventType,
    FileIdentity,
    FileRecord,
    FileStatus,
    HandoffRequest,
    HandoffResponse,
    IngestEvent,
    ParseResult,
    QueueItem,
    QueueLane,
)


def test_file_identity_creation():
    fi = FileIdentity(path="/tmp/test.txt", inode=123, device=1, mtime=1000.0, size=42)
    assert fi.path == "/tmp/test.txt"
    assert fi.inode == 123
    assert fi.head_hash == ""


def test_file_record_defaults():
    fi = FileIdentity(path="/tmp/x.py")
    rec = FileRecord(identity=fi)
    assert rec.status == FileStatus.pending
    assert rec.mime_type == "application/octet-stream"
    assert rec.deleted_at is None
    assert len(rec.id) == 32  # uuid hex


def test_file_status_enum():
    assert FileStatus.pending.value == "pending"
    assert FileStatus("tombstone") == FileStatus.tombstone


def test_event_type_enum():
    assert EventType.created.value == "created"
    assert EventType("deleted") == EventType.deleted


def test_queue_lane_enum():
    assert QueueLane.fast.value == "fast"
    assert QueueLane("background") == QueueLane.background


def test_ingest_event():
    fi = FileIdentity(path="/a/b.txt")
    ev = IngestEvent(event_type=EventType.created, file_identity=fi)
    assert ev.event_type == EventType.created
    assert ev.timestamp > 0


def test_chunk_defaults():
    c = Chunk(content="hello", fingerprint="abc123")
    assert c.sequence == 0
    assert c.embedding is None
    assert c.byte_offset == 0


def test_queue_item():
    qi = QueueItem(file_id="f1", lane=QueueLane.heavy, priority=5)
    assert qi.lane == QueueLane.heavy
    assert qi.attempts == 0


def test_handoff_request_serialisation():
    c = Chunk(content="test", fingerprint="fp1")
    fi = FileIdentity(path="/test.txt")
    fr = FileRecord(identity=fi)
    req = HandoffRequest(chunks=[c], file_record=fr, action="embed")

    data = req.model_dump()
    assert data["action"] == "embed"
    assert len(data["chunks"]) == 1


def test_handoff_response():
    resp = HandoffResponse(embeddings=[[0.1, 0.2]], summary="A summary")
    assert resp.embeddings is not None
    assert len(resp.embeddings[0]) == 2


def test_parse_result():
    pr = ParseResult(content="hello world", metadata={"lines": 1})
    assert pr.content == "hello world"
    assert pr.sub_files == []
