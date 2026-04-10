"""Abstract base class for file parsers.

Every parser implements two methods:

* ``can_parse`` — quick check (usually MIME type + extension).
* ``parse`` — extract raw text and metadata from file bytes.

The ``lane`` attribute controls which priority queue the file enters.
"""

from __future__ import annotations

import abc

from src.core.models import FileRecord, ParseResult


class ParserBase(abc.ABC):
    """Extension point for adding new file-format support."""

    name: str = "base"
    supported_mimes: list[str] = []
    lane: str = "fast"  # fast | heavy | background

    def __init__(self, **settings: object) -> None:
        """Accept arbitrary settings from ``plugins.yaml``."""
        for key, value in settings.items():
            setattr(self, key, value)

    @abc.abstractmethod
    async def can_parse(self, file_record: FileRecord) -> bool:
        """Return True if this parser can handle the given file."""
        ...

    @abc.abstractmethod
    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        """Extract text content and metadata from *raw_bytes*."""
        ...
