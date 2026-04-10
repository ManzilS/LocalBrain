"""Tests for the plugin registry — parser discovery and lookup."""

from __future__ import annotations

import pytest

from src.core.registry import PluginRegistry


@pytest.fixture
def registry(tmp_path):
    """Create a registry loaded from the project's plugins.yaml."""
    r = PluginRegistry()
    r.discover("plugins.yaml")
    return r


def test_discovers_enabled_parsers(registry):
    # text and archive are enabled by default; pdf too
    names = [p.name for p in registry.parsers]
    assert "text" in names
    assert "archive" in names


def test_get_parser_by_mime(registry):
    parser = registry.get_parser("text/plain")
    assert parser is not None
    assert parser.name == "text"


def test_get_parser_unknown_mime(registry):
    assert registry.get_parser("video/mp4") is None


@pytest.mark.asyncio
async def test_find_parser_by_can_parse(registry):
    from src.core.models import FileIdentity, FileRecord

    record = FileRecord(
        identity=FileIdentity(path="/test.py"),
        mime_type="text/x-python",
    )
    parser = await registry.find_parser(record)
    assert parser is not None
    assert parser.name == "text"


def test_supported_mimes(registry):
    mimes = registry.supported_mimes
    assert "text/plain" in mimes


def test_empty_registry():
    r = PluginRegistry()
    assert r.parsers == []
    assert r.get_parser("text/plain") is None
