"""Tests for the text/markdown/code parser."""

from __future__ import annotations

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.text_ext import TextParser


@pytest.fixture
def parser():
    return TextParser()


@pytest.fixture
def make_record():
    def _make(path: str, mime: str = "text/plain") -> FileRecord:
        return FileRecord(
            identity=FileIdentity(path=path),
            mime_type=mime,
        )

    return _make


@pytest.mark.asyncio
async def test_can_parse_text_mime(parser, make_record):
    assert await parser.can_parse(make_record("/a.txt", "text/plain"))


@pytest.mark.asyncio
async def test_can_parse_markdown_mime(parser, make_record):
    assert await parser.can_parse(make_record("/a.md", "text/markdown"))


@pytest.mark.asyncio
async def test_can_parse_by_extension(parser, make_record):
    assert await parser.can_parse(make_record("/a.py", "application/octet-stream"))
    assert await parser.can_parse(make_record("/a.rs", "application/octet-stream"))
    assert await parser.can_parse(make_record("/a.json", "application/octet-stream"))


@pytest.mark.asyncio
async def test_cannot_parse_binary(parser, make_record):
    assert not await parser.can_parse(make_record("/a.exe", "application/octet-stream"))


@pytest.mark.asyncio
async def test_parse_utf8(parser, make_record):
    record = make_record("/test.txt")
    raw = "Hello, world!\nLine 2".encode("utf-8")
    result = await parser.parse(record, raw)

    assert result.content == "Hello, world!\nLine 2"
    assert result.metadata["line_count"] == 2
    assert result.metadata["char_count"] == 20


@pytest.mark.asyncio
async def test_parse_latin1_fallback(parser, make_record):
    record = make_record("/test.txt")
    raw = "caf\xe9".encode("latin-1")
    result = await parser.parse(record, raw)

    assert "caf" in result.content


@pytest.mark.asyncio
async def test_parse_too_large(parser, make_record):
    parser.max_file_size = 10  # 10 bytes
    record = make_record("/big.txt")
    raw = b"A" * 100

    result = await parser.parse(record, raw)
    assert result.content == ""
    assert result.metadata["error"] == "file_too_large"
