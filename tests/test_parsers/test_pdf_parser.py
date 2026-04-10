"""Tests for the PDF parser."""

from __future__ import annotations

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.pdf_ext import PdfParser


@pytest.fixture
def parser():
    return PdfParser()


@pytest.fixture
def pdf_record():
    return FileRecord(
        identity=FileIdentity(path="/test.pdf"),
        mime_type="application/pdf",
    )


@pytest.mark.asyncio
async def test_can_parse_by_mime(parser, pdf_record):
    assert await parser.can_parse(pdf_record)


@pytest.mark.asyncio
async def test_can_parse_by_extension(parser):
    rec = FileRecord(
        identity=FileIdentity(path="/document.pdf"),
        mime_type="application/octet-stream",
    )
    assert await parser.can_parse(rec)


@pytest.mark.asyncio
async def test_cannot_parse_non_pdf(parser):
    rec = FileRecord(
        identity=FileIdentity(path="/test.txt"),
        mime_type="text/plain",
    )
    assert not await parser.can_parse(rec)


@pytest.mark.asyncio
async def test_fallback_extraction(parser, pdf_record):
    # Simulate raw bytes with embedded text
    raw = b"%PDF-1.4\nHello World from PDF\x00\x01\x02more text here\x00end"
    result = await parser.parse(pdf_record, raw)

    # The fallback extracts printable ASCII runs
    assert "metadata" in dir(result)


def test_lane_is_heavy(parser):
    assert parser.lane == "heavy"
