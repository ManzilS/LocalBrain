"""Template for creating a new parser.

Copy this file, rename it to ``<format>_ext.py``, and fill in the
``can_parse`` and ``parse`` methods.  Then add an entry to
``plugins.yaml`` to activate it.
"""

from __future__ import annotations

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase


class TemplateParser(ParserBase):
    name = "template"
    supported_mimes = ["application/x-template"]
    lane = "fast"  # fast | heavy | background

    async def can_parse(self, file_record: FileRecord) -> bool:
        return file_record.mime_type in self.supported_mimes

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        raise NotImplementedError("Replace this with your extraction logic")
