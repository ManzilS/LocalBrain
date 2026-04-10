"""Tests for the audio parser stub."""

from __future__ import annotations

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.audio_ext import AudioParser


@pytest.fixture
def parser():
    return AudioParser()


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
async def test_can_parse_mp3_mime(parser, make_record):
    assert await parser.can_parse(make_record("/a.mp3", "audio/mpeg"))


@pytest.mark.asyncio
async def test_can_parse_wav_extension(parser, make_record):
    assert await parser.can_parse(make_record("/audio.wav"))


@pytest.mark.asyncio
async def test_can_parse_flac_extension(parser, make_record):
    assert await parser.can_parse(make_record("/song.flac"))


@pytest.mark.asyncio
async def test_can_parse_ogg_extension(parser, make_record):
    assert await parser.can_parse(make_record("/podcast.ogg"))


@pytest.mark.asyncio
async def test_can_parse_m4a_extension(parser, make_record):
    assert await parser.can_parse(make_record("/voice.m4a"))


@pytest.mark.asyncio
async def test_cannot_parse_txt(parser, make_record):
    assert not await parser.can_parse(make_record("/a.txt", "text/plain"))


@pytest.mark.asyncio
async def test_cannot_parse_pdf(parser, make_record):
    assert not await parser.can_parse(make_record("/doc.pdf", "application/pdf"))


# ── parse ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_returns_transcription_flag(parser, make_record):
    """Audio content is empty — transcription delegated to Router."""
    r = make_record("/a.mp3", "audio/mpeg")
    data = b"\xff\xfb\x90\x00" * 100
    result = await parser.parse(r, data)

    assert result.content == ""
    assert result.metadata["needs_transcription"] is True
    assert result.metadata["size"] == len(data)


@pytest.mark.asyncio
async def test_parse_empty_audio(parser, make_record):
    r = make_record("/empty.wav", "audio/wav")
    result = await parser.parse(r, b"")
    assert result.metadata["size"] == 0
    assert result.metadata["needs_transcription"] is True
