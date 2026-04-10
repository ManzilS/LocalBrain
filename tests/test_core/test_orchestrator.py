"""Tests for the orchestrator — lifecycle and wiring."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.core.orchestrator import Orchestrator
from src.utils.config import Settings


def test_orchestrator_init():
    s = Settings()
    o = Orchestrator(s)
    assert o.settings is s
    assert o.engine is None
    assert o.lance is None
    assert o.pipeline is None


def test_orchestrator_has_all_subsystem_slots():
    s = Settings()
    o = Orchestrator(s)
    assert hasattr(o, "scope_gate")
    assert hasattr(o, "identity_resolver")
    assert hasattr(o, "registry")
    assert hasattr(o, "deduplicator")
    assert hasattr(o, "scheduler")
    assert hasattr(o, "engine")
    assert hasattr(o, "lance")
    assert hasattr(o, "subscriptions")
    assert hasattr(o, "ref_counter")
    assert hasattr(o, "queue")
    assert hasattr(o, "pipeline")
    assert hasattr(o, "watcher")
    assert hasattr(o, "tombstone")
    assert hasattr(o, "journal_sync")
    assert hasattr(o, "reindex_manager")
