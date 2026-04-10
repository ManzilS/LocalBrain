"""PDF parser — extracts text from PDF documents.

Uses ``pymupdf`` (fitz) when available, falling back to a basic
byte-level text extraction for environments without the C library.
"""

from __future__ import annotations

import logging

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)


class PdfParser(ParserBase):
    name = "pdf"
    supported_mimes = ["application/pdf"]
    lane = "heavy"

    async def can_parse(self, file_record: FileRecord) -> bool:
        if file_record.mime_type == "application/pdf":
            return True
        return file_record.identity.path.lower().endswith(".pdf")

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        try:
            return self._parse_with_pymupdf(raw_bytes)
        except ImportError:
            logger.info("pymupdf not installed — using fallback text extraction")
            return self._parse_fallback(raw_bytes)

    @staticmethod
    def _parse_with_pymupdf(raw_bytes: bytes) -> ParseResult:
        import fitz  # type: ignore[import-untyped]

        doc = fitz.open(stream=raw_bytes, filetype="pdf")
        try:
            pages: list[str] = []
            for page in doc:
                pages.append(page.get_text())
        finally:
            doc.close()

        return ParseResult(
            content="\n\n".join(pages),
            metadata={
                "page_count": len(pages),
                "char_count": sum(len(p) for p in pages),
            },
        )

    @staticmethod
    def _parse_fallback(raw_bytes: bytes) -> ParseResult:
        """Minimal fallback: decode printable ASCII runs from raw PDF bytes."""
        text_runs: list[str] = []
        current: list[str] = []
        for byte in raw_bytes:
            if 32 <= byte < 127 or byte in (9, 10, 13):
                current.append(chr(byte))
            else:
                if len(current) > 4:
                    text_runs.append("".join(current))
                current = []
        if len(current) > 4:
            text_runs.append("".join(current))

        content = "\n".join(text_runs)
        return ParseResult(
            content=content,
            metadata={"fallback": True, "char_count": len(content)},
        )
