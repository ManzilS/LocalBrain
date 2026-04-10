"""Tests for the file watcher — debounce and settle logic."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.core.models import EventType
from src.ingress.watcher import FileWatcher, _CHANGE_MAP


def test_change_map_coverage():
    """Ensure all watchfiles Change types are mapped."""
    from watchfiles import Change

    assert Change.added in _CHANGE_MAP
    assert Change.modified in _CHANGE_MAP
    assert Change.deleted in _CHANGE_MAP


def test_change_map_values():
    from watchfiles import Change

    assert _CHANGE_MAP[Change.added] == EventType.created
    assert _CHANGE_MAP[Change.modified] == EventType.modified
    assert _CHANGE_MAP[Change.deleted] == EventType.deleted


@pytest.mark.asyncio
async def test_watcher_stop():
    """Watcher stop event should be settable."""
    settings = MagicMock()
    settings.debounce_ms = 100
    settings.settle_time_ms = 500
    settings.poll_interval_s = 60.0

    gate = MagicMock()
    gate.get_watch_roots.return_value = []

    resolver = MagicMock()
    watcher = FileWatcher(gate, resolver, settings)

    assert not watcher._stop.is_set()
    watcher.stop()
    assert watcher._stop.is_set()


@pytest.mark.asyncio
async def test_watcher_no_roots():
    """Watcher with no valid roots should exit cleanly."""
    settings = MagicMock()
    settings.debounce_ms = 100
    settings.settle_time_ms = 500
    settings.poll_interval_s = 60.0

    gate = MagicMock()
    gate.get_watch_roots.return_value = [Path("/nonexistent/path")]

    resolver = MagicMock()
    watcher = FileWatcher(gate, resolver, settings)

    events = []
    async for event in watcher.watch():
        events.append(event)
    assert events == []
