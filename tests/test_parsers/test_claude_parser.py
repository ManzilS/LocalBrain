"""Tests for the Claude conversation parser."""

from __future__ import annotations

import io
import json
import zipfile

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.claude_ext import ClaudeParser


@pytest.fixture
def parser():
    return ClaudeParser()


def _record(path: str, mime: str = "application/json") -> FileRecord:
    return FileRecord(identity=FileIdentity(path=path), mime_type=mime)


def _make_conversation(title="Test Chat", msg_count=2):
    messages = []
    for i in range(msg_count):
        role = "human" if i % 2 == 0 else "assistant"
        messages.append({
            "uuid": f"msg-{i}",
            "sender": role,
            "text": f"Message {i} from {role}",
            "created_at": f"2024-01-15T10:{i:02d}:00Z",
            "updated_at": f"2024-01-15T10:{i:02d}:00Z",
        })

    return {
        "uuid": "conv-001",
        "name": title,
        "created_at": "2024-01-15T10:00:00Z",
        "updated_at": "2024-01-15T10:30:00Z",
        "chat_messages": messages,
    }


def _make_zip(conversations: list[dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("conversations.json", json.dumps(conversations))
    return buf.getvalue()


# ── can_parse ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_can_parse_claude_json(parser):
    assert await parser.can_parse(_record("/claude-export/conversations.json"))


@pytest.mark.asyncio
async def test_can_parse_claude_zip(parser):
    assert await parser.can_parse(_record("/downloads/claude-export.zip"))


@pytest.mark.asyncio
async def test_cannot_parse_random_json(parser):
    assert not await parser.can_parse(_record("/data/settings.json"))


@pytest.mark.asyncio
async def test_cannot_parse_chatgpt(parser):
    """Should not match ChatGPT files (no 'claude' in path)."""
    assert not await parser.can_parse(_record("/chatgpt/conversations.json"))


# ── parse — JSON ───────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_single_conversation(parser):
    conv = _make_conversation("My Chat", 4)
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/claude/conversations.json"), data)

    assert result.metadata["platform"] == "claude"
    assert result.metadata["conversation_count"] == 1
    assert result.metadata["total_messages"] == 4
    assert "Message 0 from human" in result.content
    assert "Message 1 from assistant" in result.content


@pytest.mark.asyncio
async def test_parse_multiple_conversations(parser):
    convs = [_make_conversation("A", 2), _make_conversation("B", 6)]
    data = json.dumps(convs).encode()
    result = await parser.parse(_record("/claude/data.json"), data)

    assert result.metadata["conversation_count"] == 2
    assert result.metadata["total_messages"] == 8


@pytest.mark.asyncio
async def test_parse_date_range(parser):
    conv = _make_conversation()
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/claude/data.json"), data)

    assert "date_range" in result.metadata
    assert "2024-01-15" in result.metadata["date_range"]["earliest"]


@pytest.mark.asyncio
async def test_parse_role_labels(parser):
    """Claude uses 'human'/'assistant' — should render as User/Assistant."""
    conv = _make_conversation()
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/claude/data.json"), data)

    assert "**User:**" in result.content
    assert "**Assistant:**" in result.content


@pytest.mark.asyncio
async def test_parse_empty_array(parser):
    result = await parser.parse(_record("/claude/data.json"), b"[]")
    assert result.metadata["conversation_count"] == 0


@pytest.mark.asyncio
async def test_parse_corrupt_json(parser):
    result = await parser.parse(_record("/claude/data.json"), b"{{bad json")
    assert "error" in result.metadata


@pytest.mark.asyncio
async def test_parse_single_object(parser):
    """Handle case where export is a single conversation (not wrapped in array)."""
    conv = _make_conversation("Solo")
    data = json.dumps(conv).encode()
    result = await parser.parse(_record("/claude/data.json"), data)

    assert result.metadata["conversation_count"] == 1


@pytest.mark.asyncio
async def test_parse_alternative_message_key(parser):
    """Support 'messages' key instead of 'chat_messages'."""
    conv = {
        "uuid": "c1",
        "name": "Alt Format",
        "created_at": "2024-01-01T00:00:00Z",
        "messages": [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there"},
        ],
    }
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/claude/data.json"), data)

    assert result.metadata["total_messages"] == 2
    assert "Hello" in result.content


@pytest.mark.asyncio
async def test_parse_with_attachments(parser):
    conv = {
        "uuid": "c1",
        "name": "With Files",
        "chat_messages": [
            {"sender": "human", "text": "See attached", "attachments": [{"name": "doc.pdf"}]},
            {"sender": "assistant", "text": "Got it"},
        ],
    }
    data = json.dumps([conv]).encode()
    result = await parser.parse(_record("/claude/data.json"), data)

    assert result.metadata["conversations"][0]["has_attachments"] is True


# ── parse — ZIP ────────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_zip(parser):
    convs = [_make_conversation()]
    zip_data = _make_zip(convs)
    result = await parser.parse(_record("/claude-export.zip"), zip_data)

    assert result.metadata["platform"] == "claude"
    assert result.metadata["conversation_count"] == 1


@pytest.mark.asyncio
async def test_parse_corrupt_zip(parser):
    result = await parser.parse(_record("/claude-export.zip"), b"bad zip")
    assert "error" in result.metadata
