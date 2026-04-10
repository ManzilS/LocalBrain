"""Universal data models for the LocalBrain ingestion pipeline.

Every component communicates through these Pydantic v2 models — they are
the lingua franca of the system, just as PipelineRequest/Response are for
the Router project.
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Enums ───────────────────────────────────────────────


class FileStatus(str, Enum):
    pending = "pending"
    parsing = "parsing"
    chunked = "chunked"
    indexed = "indexed"
    error = "error"
    tombstone = "tombstone"


class EventType(str, Enum):
    created = "created"
    modified = "modified"
    deleted = "deleted"
    moved = "moved"


class QueueLane(str, Enum):
    fast = "fast"
    heavy = "heavy"
    background = "background"


# ── Identity & Files ───────────────────────────────────


def _new_id() -> str:
    return uuid.uuid4().hex


def _now() -> float:
    return time.time()


class FileIdentity(BaseModel):
    """Composite key that uniquely tracks a physical file across renames."""

    path: str
    inode: int = 0
    device: int = 0
    mtime: float = 0.0
    size: int = 0
    head_hash: str = ""


class FileRecord(BaseModel):
    """Canonical representation of a tracked file in the vault."""

    id: str = Field(default_factory=_new_id)
    identity: FileIdentity
    fingerprint: str = ""
    mime_type: str = "application/octet-stream"
    status: FileStatus = FileStatus.pending
    created_at: float = Field(default_factory=_now)
    updated_at: float = Field(default_factory=_now)
    deleted_at: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Events ──────────────────────────────────────────────


class IngestEvent(BaseModel):
    """A single file-system event entering the pipeline."""

    id: str = Field(default_factory=_new_id)
    event_type: EventType
    file_identity: FileIdentity
    timestamp: float = Field(default_factory=_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Chunks ──────────────────────────────────────────────


class Chunk(BaseModel):
    """An individual semantic chunk extracted from a file."""

    id: str = Field(default_factory=_new_id)
    file_id: str = ""
    sequence: int = 0
    content: str = ""
    fingerprint: str = ""
    byte_offset: int = 0
    byte_length: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None


# ── Queue ───────────────────────────────────────────────


class QueueItem(BaseModel):
    """An item sitting in the persistent backpressure queue."""

    id: str = Field(default_factory=_new_id)
    file_id: str = ""
    lane: QueueLane = QueueLane.fast
    priority: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: float = Field(default_factory=_now)
    attempts: int = 0


# ── Router Handoff ──────────────────────────────────────


class HandoffRequest(BaseModel):
    """Payload sent to the Router app for AI processing."""

    chunks: list[Chunk] = Field(default_factory=list)
    file_record: FileRecord | None = None
    action: str = "embed"  # embed | summarize | classify


class HandoffResponse(BaseModel):
    """Response received from the Router app."""

    embeddings: list[list[float]] | None = None
    summary: str | None = None
    labels: list[str] | None = None


# ── Parse Result ────────────────────────────────────────


class ParseResult(BaseModel):
    """Output from a parser — raw text plus optional sub-files (archives)."""

    content: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    sub_files: list[bytes] = Field(default_factory=list)


# Rebuild forward refs
FileRecord.model_rebuild()
HandoffRequest.model_rebuild()
