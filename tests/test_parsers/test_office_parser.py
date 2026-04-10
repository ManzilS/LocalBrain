"""Tests for the office document parser (DOCX, XLSX, PPTX)."""

from __future__ import annotations

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.office_ext import OfficeParser


@pytest.fixture
def parser():
    return OfficeParser()


@pytest.fixture
def make_record():
    def _make(path: str, mime: str = "application/octet-stream") -> FileRecord:
        return FileRecord(
            identity=FileIdentity(path=path),
            mime_type=mime,
        )

    return _make


# ── can_parse ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_can_parse_docx_mime(parser, make_record):
    r = make_record(
        "/a.docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    assert await parser.can_parse(r)


@pytest.mark.asyncio
async def test_can_parse_xlsx_extension(parser, make_record):
    assert await parser.can_parse(make_record("/data.xlsx"))


@pytest.mark.asyncio
async def test_can_parse_pptx_extension(parser, make_record):
    assert await parser.can_parse(make_record("/slides.pptx"))


@pytest.mark.asyncio
async def test_cannot_parse_txt(parser, make_record):
    assert not await parser.can_parse(make_record("/a.txt", "text/plain"))


# ── parse fallbacks (libraries not installed) ───────────


@pytest.mark.asyncio
async def test_parse_docx_missing_library(parser, make_record):
    """If python-docx is not installed, returns graceful error metadata."""
    r = make_record("/a.docx")
    try:
        from docx import Document  # noqa: F401

        pytest.skip("python-docx is installed — fallback test not applicable")
    except ImportError:
        pass

    result = await parser.parse(r, b"PK\x03\x04fake")
    assert result.metadata.get("error") == "python-docx not installed"


@pytest.mark.asyncio
async def test_parse_xlsx_missing_library(parser, make_record):
    r = make_record("/a.xlsx")
    try:
        from openpyxl import load_workbook  # noqa: F401

        pytest.skip("openpyxl is installed — fallback test not applicable")
    except ImportError:
        pass

    result = await parser.parse(r, b"PK\x03\x04fake")
    assert result.metadata.get("error") == "openpyxl not installed"


@pytest.mark.asyncio
async def test_parse_pptx_missing_library(parser, make_record):
    r = make_record("/a.pptx")
    try:
        from pptx import Presentation  # noqa: F401

        pytest.skip("python-pptx is installed — fallback test not applicable")
    except ImportError:
        pass

    result = await parser.parse(r, b"PK\x03\x04fake")
    assert result.metadata.get("error") == "python-pptx not installed"


@pytest.mark.asyncio
async def test_parse_unknown_office_format(parser, make_record):
    r = make_record("/a.odt", "application/vnd.oasis.opendocument.text")
    result = await parser.parse(r, b"data")
    assert result.metadata.get("error") == "unknown_office_format"
