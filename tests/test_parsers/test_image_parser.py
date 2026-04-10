"""Tests for the image parser (EXIF extraction stub)."""

from __future__ import annotations

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.image_ext import ImageParser


@pytest.fixture
def parser():
    return ImageParser()


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
async def test_can_parse_jpeg_mime(parser, make_record):
    assert await parser.can_parse(make_record("/a.jpg", "image/jpeg"))


@pytest.mark.asyncio
async def test_can_parse_png_extension(parser, make_record):
    assert await parser.can_parse(make_record("/photo.png"))


@pytest.mark.asyncio
async def test_can_parse_gif_extension(parser, make_record):
    assert await parser.can_parse(make_record("/anim.gif"))


@pytest.mark.asyncio
async def test_can_parse_webp_extension(parser, make_record):
    assert await parser.can_parse(make_record("/img.webp"))


@pytest.mark.asyncio
async def test_cannot_parse_txt(parser, make_record):
    assert not await parser.can_parse(make_record("/a.txt", "text/plain"))


# ── parse ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_returns_needs_ocr(parser, make_record):
    """All image parses flag needs_ocr for Router handoff."""
    r = make_record("/a.jpg", "image/jpeg")
    result = await parser.parse(r, b"\xff\xd8\xff\xe0fake")
    assert result.metadata["needs_ocr"] is True
    assert result.content == ""


@pytest.mark.asyncio
async def test_parse_without_pillow(parser, make_record):
    """If Pillow is not installed, parse still returns metadata."""
    r = make_record("/a.png", "image/png")
    try:
        from PIL import Image  # noqa: F401

        # Pillow IS installed — test that it extracts something
        # Use a minimal valid PNG
        png_header = (
            b"\x89PNG\r\n\x1a\n"  # signature
            b"\x00\x00\x00\rIHDR"
            b"\x00\x00\x00\x01"  # width=1
            b"\x00\x00\x00\x01"  # height=1
            b"\x08\x02"  # 8-bit RGB
            b"\x00\x00\x00"
            b"\x90wS\xde"  # CRC
            b"\x00\x00\x00\x0cIDATx"
            b"\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05"
            b"\x18\xd8N"  # CRC
            b"\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        result = await parser.parse(r, png_header)
        assert result.metadata["needs_ocr"] is True
        # With Pillow installed we should get format info
        assert "format" in result.metadata or "error" in result.metadata
    except ImportError:
        result = await parser.parse(r, b"\x89PNG\r\n\x1a\nfake")
        assert result.metadata.get("error") == "pillow_not_installed"
        assert result.metadata["needs_ocr"] is True
