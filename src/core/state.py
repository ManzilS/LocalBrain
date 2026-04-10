"""Mutable pipeline state that travels through every ingestion phase.

Mirrors the Router project's ``PipelineState`` — a single container
that each phase reads from and writes to.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from src.core.models import (
    Chunk,
    FileRecord,
    HandoffResponse,
    IngestEvent,
)


class IngestState(BaseModel):
    """State container for one file's journey through the pipeline."""

    # Input
    event: IngestEvent

    # Built up during processing
    file_record: FileRecord | None = None
    raw_content: str = ""
    chunks: list[Chunk] = Field(default_factory=list)
    handoff_response: HandoffResponse | None = None

    # Pipeline control
    phase: str = "ingress"  # ingress -> parse -> chunk -> handoff -> store
    early_exit: bool = False
    error: str | None = None

    # Arbitrary bag for cross-phase communication
    extras: dict[str, Any] = Field(default_factory=dict)
