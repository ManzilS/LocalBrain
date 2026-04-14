"""Tests for the generic AI conversation parser (Copilot, Perplexity, etc.)."""

from __future__ import annotations

import json

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.ai_generic_ext import AIGenericParser


@pytest.fixture
def parser():
    return AIGenericParser()


def _record(path: str, mime: str = "application/json") -> FileRecord:
    return FileRecord(identity=FileIdentity(path=path), mime_type=mime)


# ── can_parse ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_can_parse_copilot_json(parser):
    assert await parser.can_parse(_record("/copilot-export.json"))


@pytest.mark.asyncio
async def test_can_parse_copilot_csv(parser):
    assert await parser.can_parse(_record("/copilot-sessions.csv"))


@pytest.mark.asyncio
async def test_can_parse_perplexity_md(parser):
    assert await parser.can_parse(_record("/perplexity-search.md"))


@pytest.mark.asyncio
async def test_can_parse_perplexity_json(parser):
    assert await parser.can_parse(_record("/perplexity-export.json"))


@pytest.mark.asyncio
async def test_cannot_parse_random_file(parser):
    assert not await parser.can_parse(_record("/data/notes.json"))


@pytest.mark.asyncio
async def test_cannot_parse_chatgpt(parser):
    assert not await parser.can_parse(_record("/chatgpt/conversations.json"))


# ── parse — JSON (Copilot style) ──────────────────────


@pytest.mark.asyncio
async def test_parse_copilot_json(parser):
    data = json.dumps([
        {
            "id": "c1",
            "title": "Help with code",
            "messages": [
                {"role": "user", "content": "How do I sort a list in Python?"},
                {"role": "assistant", "content": "Use sorted() or list.sort()"},
            ],
        }
    ]).encode()
    result = await parser.parse(_record("/copilot-export.json"), data)

    assert result.metadata["platform"] == "copilot"
    assert result.metadata["conversation_count"] == 1
    assert result.metadata["total_messages"] == 2
    assert "sorted()" in result.content


@pytest.mark.asyncio
async def test_parse_perplexity_json_with_citations(parser):
    data = json.dumps([
        {
            "title": "Research query",
            "messages": [
                {"role": "user", "content": "What is CRISPR?"},
                {
                    "role": "assistant",
                    "content": "CRISPR is a gene-editing technology...",
                    "citations": ["https://nature.com/crispr", "https://science.org/crispr"],
                },
            ],
        }
    ]).encode()
    result = await parser.parse(_record("/perplexity-export.json"), data)

    assert result.metadata["platform"] == "perplexity"
    assert result.metadata["total_messages"] == 2
    assert result.metadata["conversations"][0]["citation_count"] == 2
    assert "nature.com" in result.content


@pytest.mark.asyncio
async def test_parse_multiple_conversations(parser):
    convs = [
        {"title": f"Chat {i}", "messages": [
            {"role": "user", "content": f"Question {i}"},
            {"role": "assistant", "content": f"Answer {i}"},
        ]}
        for i in range(5)
    ]
    data = json.dumps(convs).encode()
    result = await parser.parse(_record("/copilot-export.json"), data)

    assert result.metadata["conversation_count"] == 5
    assert result.metadata["total_messages"] == 10


@pytest.mark.asyncio
async def test_parse_empty_json_array(parser):
    result = await parser.parse(_record("/copilot-export.json"), b"[]")
    assert "error" in result.metadata


@pytest.mark.asyncio
async def test_parse_corrupt_json(parser):
    result = await parser.parse(_record("/copilot-export.json"), b"not json")
    assert "error" in result.metadata


# ── parse — Markdown (Perplexity style) ────────────────


@pytest.mark.asyncio
async def test_parse_perplexity_markdown(parser):
    md = b"""# Research: Quantum Computing

**Question:** What is quantum supremacy?
**Answer:** Quantum supremacy is when a quantum computer performs a task...

**Question:** Who achieved it first?
**Answer:** Google claimed quantum supremacy in 2019 with their Sycamore processor.

Sources:
- https://nature.com/quantum
- https://arxiv.org/quantum
"""
    result = await parser.parse(_record("/perplexity-search.md"), md)

    assert result.metadata["platform"] == "perplexity"
    assert result.metadata["format"] == "markdown"
    assert result.metadata["total_messages"] >= 2
    assert "quantum" in result.content.lower()
    assert result.metadata.get("citation_count", 0) >= 2


@pytest.mark.asyncio
async def test_parse_empty_markdown(parser):
    result = await parser.parse(_record("/perplexity-search.md"), b"")
    assert "error" in result.metadata


# ── parse — CSV (Copilot Studio style) ─────────────────


@pytest.mark.asyncio
async def test_parse_copilot_csv(parser):
    csv_data = b"""SessionId,Role,Content
session-1,user,Hello bot
session-1,assistant,Hi there! How can I help?
session-1,user,Tell me about sales
session-2,user,Different session
session-2,assistant,Sure thing
"""
    result = await parser.parse(_record("/copilot-sessions.csv"), csv_data)

    assert result.metadata["platform"] == "copilot"
    assert result.metadata["format"] == "csv"
    assert result.metadata["conversation_count"] == 2
    assert result.metadata["total_messages"] == 5
    assert "Hello bot" in result.content


@pytest.mark.asyncio
async def test_parse_csv_without_session_id(parser):
    csv_data = b"""Role,Content
user,Question one
assistant,Answer one
"""
    result = await parser.parse(_record("/copilot-sessions.csv"), csv_data)

    assert result.metadata["conversation_count"] == 1
    assert result.metadata["total_messages"] == 2


@pytest.mark.asyncio
async def test_parse_empty_csv(parser):
    result = await parser.parse(_record("/copilot-sessions.csv"), b"")
    assert "error" in result.metadata


# ── Platform detection ─────────────────────────────────


@pytest.mark.asyncio
async def test_platform_detected_copilot(parser):
    data = json.dumps([{"title": "x", "messages": [{"role": "user", "content": "hi"}]}]).encode()
    result = await parser.parse(_record("/microsoft-copilot-export.json"), data)
    assert result.metadata["platform"] == "copilot"


@pytest.mark.asyncio
async def test_platform_detected_perplexity(parser):
    data = json.dumps([{"title": "x", "messages": [{"role": "user", "content": "hi"}]}]).encode()
    result = await parser.parse(_record("/perplexity-data.json"), data)
    assert result.metadata["platform"] == "perplexity"
