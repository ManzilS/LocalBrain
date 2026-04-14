"""Tests for the ChatGPT conversation parser."""

from __future__ import annotations

import io
import json
import zipfile

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.chatgpt_ext import ChatGPTParser


@pytest.fixture
def parser():
    return ChatGPTParser()


def _record(path: str, mime: str = "application/json") -> FileRecord:
    return FileRecord(identity=FileIdentity(path=path), mime_type=mime)


# ── Sample data ────────────────────────────────────────


def _make_conversation(title="Test Chat", model="gpt-4"):
    """Build a minimal ChatGPT conversation with tree structure."""
    return {
        "title": title,
        "create_time": 1700000000.0,
        "update_time": 1700001000.0,
        "mapping": {
            "root": {
                "id": "root",
                "message": None,
                "parent": None,
                "children": ["msg1"],
            },
            "msg1": {
                "id": "msg1",
                "message": {
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": ["Hello, how are you?"]},
                    "metadata": {},
                },
                "parent": "root",
                "children": ["msg2"],
            },
            "msg2": {
                "id": "msg2",
                "message": {
                    "author": {"role": "assistant"},
                    "content": {"content_type": "text", "parts": ["I'm doing well, thanks!"]},
                    "metadata": {"model_slug": model},
                },
                "parent": "msg1",
                "children": [],
            },
        },
        "current_node": "msg2",
    }


def _make_zip(conversations: list[dict]) -> bytes:
    """Create a ZIP file containing conversations.json."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("conversations.json", json.dumps(conversations))
    return buf.getvalue()


# ── can_parse ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_can_parse_conversations_json(parser):
    assert await parser.can_parse(_record("/export/conversations.json"))


@pytest.mark.asyncio
async def test_can_parse_chatgpt_zip(parser):
    assert await parser.can_parse(_record("/downloads/chatgpt-export.zip"))


@pytest.mark.asyncio
async def test_cannot_parse_random_json(parser):
    assert not await parser.can_parse(_record("/data/settings.json"))


@pytest.mark.asyncio
async def test_cannot_parse_random_zip(parser):
    assert not await parser.can_parse(_record("/data/archive.zip"))


# ── parse — JSON ───────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_single_conversation(parser):
    conv = _make_conversation()
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/conversations.json"), data)

    assert result.metadata["platform"] == "chatgpt"
    assert result.metadata["conversation_count"] == 1
    assert result.metadata["total_messages"] == 2
    assert "gpt-4" in result.metadata["models_used"]
    assert "Hello, how are you?" in result.content
    assert "I'm doing well" in result.content


@pytest.mark.asyncio
async def test_parse_multiple_conversations(parser):
    convs = [
        _make_conversation("Chat A", "gpt-4"),
        _make_conversation("Chat B", "gpt-3.5-turbo"),
    ]
    data = json.dumps(convs).encode()
    result = await parser.parse(_record("/conversations.json"), data)

    assert result.metadata["conversation_count"] == 2
    assert result.metadata["total_messages"] == 4
    assert set(result.metadata["models_used"]) == {"gpt-4", "gpt-3.5-turbo"}


@pytest.mark.asyncio
async def test_parse_date_range(parser):
    conv = _make_conversation()
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/conversations.json"), data)

    assert "date_range" in result.metadata
    assert result.metadata["date_range"]["earliest"] is not None
    assert result.metadata["date_range"]["latest"] is not None


@pytest.mark.asyncio
async def test_parse_empty_conversations(parser):
    data = json.dumps([]).encode()
    result = await parser.parse(_record("/conversations.json"), data)
    assert result.metadata["conversation_count"] == 0


@pytest.mark.asyncio
async def test_parse_missing_current_node(parser):
    """Falls back to iterating all nodes when current_node is missing."""
    conv = _make_conversation()
    conv["current_node"] = None
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/conversations.json"), data)

    assert result.metadata["total_messages"] == 2  # Still extracts messages


@pytest.mark.asyncio
async def test_parse_null_timestamps(parser):
    conv = _make_conversation()
    conv["create_time"] = None
    conv["update_time"] = 0
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/conversations.json"), data)

    # Should not crash
    assert result.metadata["conversation_count"] == 1


@pytest.mark.asyncio
async def test_parse_corrupt_json(parser):
    result = await parser.parse(_record("/conversations.json"), b"not json{{{")
    assert "error" in result.metadata
    assert "json_decode" in result.metadata["error"]


@pytest.mark.asyncio
async def test_parse_wrong_top_level_type(parser):
    result = await parser.parse(_record("/conversations.json"), b'"just a string"')
    assert "error" in result.metadata


# ── parse — ZIP ────────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_zip_with_conversations(parser):
    convs = [_make_conversation()]
    zip_data = _make_zip(convs)
    result = await parser.parse(_record("/chatgpt-export.zip"), zip_data)

    assert result.metadata["platform"] == "chatgpt"
    assert result.metadata["conversation_count"] == 1
    assert "Hello, how are you?" in result.content


@pytest.mark.asyncio
async def test_parse_corrupt_zip(parser):
    result = await parser.parse(_record("/chatgpt-export.zip"), b"not a zip")
    assert "error" in result.metadata
    assert "corrupt_zip" in result.metadata["error"]


@pytest.mark.asyncio
async def test_parse_zip_without_conversations(parser):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no conversations here")
    result = await parser.parse(_record("/chatgpt-export.zip"), buf.getvalue())
    assert "error" in result.metadata


# ── Tree linearisation ─────────────────────────────────


@pytest.mark.asyncio
async def test_tree_with_branches(parser):
    """Verify only the active branch (current_node path) is followed."""
    conv = {
        "title": "Branched",
        "create_time": 1700000000.0,
        "update_time": 1700001000.0,
        "mapping": {
            "root": {"id": "root", "message": None, "parent": None, "children": ["a"]},
            "a": {
                "id": "a",
                "message": {"author": {"role": "user"}, "content": {"parts": ["Question"]}, "metadata": {}},
                "parent": "root",
                "children": ["b", "c"],
            },
            "b": {
                "id": "b",
                "message": {"author": {"role": "assistant"}, "content": {"parts": ["Answer B (active)"]}, "metadata": {}},
                "parent": "a",
                "children": [],
            },
            "c": {
                "id": "c",
                "message": {"author": {"role": "assistant"}, "content": {"parts": ["Answer C (branch)"]}, "metadata": {}},
                "parent": "a",
                "children": [],
            },
        },
        "current_node": "b",
    }
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/conversations.json"), data)

    assert "Answer B (active)" in result.content
    assert "Answer C (branch)" not in result.content
