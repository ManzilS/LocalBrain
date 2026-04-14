"""Tests for the Google Gemini conversation parser."""

from __future__ import annotations

import io
import json
import zipfile

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.gemini_ext import GeminiParser


@pytest.fixture
def parser():
    return GeminiParser()


def _record(path: str, mime: str = "application/json") -> FileRecord:
    return FileRecord(identity=FileIdentity(path=path), mime_type=mime)


def _make_conversation(title="Gemini Chat"):
    return {
        "id": "gem-001",
        "title": title,
        "createdTime": "2024-06-01T12:00:00Z",
        "lastModifiedTime": "2024-06-01T12:30:00Z",
        "messages": [
            {"prompt": "What is quantum computing?", "response": "Quantum computing uses qubits..."},
            {"prompt": "Give me an example", "response": "One example is Shor's algorithm..."},
        ],
    }


def _make_takeout_zip(conversations: list[dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for i, conv in enumerate(conversations):
            zf.writestr(
                f"Takeout/Gemini Apps/conversations/conv-{i}.json",
                json.dumps(conv),
            )
    return buf.getvalue()


# ── can_parse ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_can_parse_gemini_json(parser):
    assert await parser.can_parse(_record("/gemini-export/data.json"))


@pytest.mark.asyncio
async def test_can_parse_bard_json(parser):
    assert await parser.can_parse(_record("/bard-conversations.json"))


@pytest.mark.asyncio
async def test_can_parse_takeout_zip(parser):
    assert await parser.can_parse(_record("/Takeout.zip"))


@pytest.mark.asyncio
async def test_can_parse_gemini_html(parser):
    assert await parser.can_parse(_record("/gemini-chat.html"))


@pytest.mark.asyncio
async def test_cannot_parse_random_json(parser):
    assert not await parser.can_parse(_record("/data/config.json"))


# ── parse — JSON (prompt/response format) ──────────────


@pytest.mark.asyncio
async def test_parse_prompt_response_format(parser):
    conv = _make_conversation()
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/gemini/data.json"), data)

    assert result.metadata["platform"] == "gemini"
    assert result.metadata["conversation_count"] == 1
    assert result.metadata["total_messages"] == 4  # 2 prompts + 2 responses
    assert "quantum computing" in result.content.lower()


@pytest.mark.asyncio
async def test_parse_role_content_format(parser):
    """Support standard role/content message format."""
    conv = {
        "title": "Role Format",
        "messages": [
            {"role": "user", "content": "Hello Gemini"},
            {"role": "assistant", "content": "Hello! How can I help?"},
        ],
    }
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/gemini/data.json"), data)

    assert result.metadata["total_messages"] == 2
    assert "Hello Gemini" in result.content


@pytest.mark.asyncio
async def test_parse_date_range(parser):
    conv = _make_conversation()
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/gemini/data.json"), data)

    assert "date_range" in result.metadata
    assert "2024-06-01" in result.metadata["date_range"]["earliest"]


@pytest.mark.asyncio
async def test_parse_empty_array(parser):
    result = await parser.parse(_record("/gemini/data.json"), b"[]")
    assert "error" in result.metadata


@pytest.mark.asyncio
async def test_parse_corrupt_json(parser):
    result = await parser.parse(_record("/gemini/data.json"), b"{{bad")
    assert "error" in result.metadata


# ── parse — Google Activity format ─────────────────────


@pytest.mark.asyncio
async def test_parse_activity_format(parser):
    activity = [
        {
            "header": "Gemini Apps",
            "title": "Asked about weather",
            "time": "2024-06-01T15:00:00Z",
            "products": ["Gemini"],
            "subtitles": [{"name": "It will be sunny today"}],
        }
    ]
    data = json.dumps(activity).encode()
    result = await parser.parse(_record("/gemini/data.json"), data)

    assert result.metadata["conversation_count"] == 1
    assert "weather" in result.content.lower()


# ── parse — ZIP (Google Takeout) ───────────────────────


@pytest.mark.asyncio
async def test_parse_takeout_zip(parser):
    convs = [_make_conversation("Chat A"), _make_conversation("Chat B")]
    zip_data = _make_takeout_zip(convs)
    result = await parser.parse(_record("/Takeout.zip"), zip_data)

    assert result.metadata["platform"] == "gemini"
    assert result.metadata["conversation_count"] == 2


@pytest.mark.asyncio
async def test_parse_corrupt_zip(parser):
    result = await parser.parse(_record("/Takeout.zip"), b"bad zip")
    assert "error" in result.metadata


@pytest.mark.asyncio
async def test_parse_zip_no_gemini_files(parser):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("random/file.txt", "nothing here")
    result = await parser.parse(_record("/Takeout.zip"), buf.getvalue())
    assert "error" in result.metadata


# ── parse — HTML fallback ──────────────────────────────


@pytest.mark.asyncio
async def test_parse_html(parser):
    html = b"""
    <html><body>
    <div class="conversation">
        <div class="user">What is AI?</div>
        <div class="model">AI stands for Artificial Intelligence...</div>
    </div>
    </body></html>
    """
    result = await parser.parse(_record("/gemini-chat.html"), html)

    assert result.metadata["platform"] == "gemini"
    assert result.metadata["format"] == "html"
    assert "AI" in result.content


@pytest.mark.asyncio
async def test_parse_empty_html(parser):
    result = await parser.parse(_record("/gemini-chat.html"), b"<html><body></body></html>")
    # After stripping tags, content might be empty
    assert result.metadata["platform"] == "gemini"
