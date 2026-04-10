"""Tests for Settings loading and defaults."""

from __future__ import annotations

import os

from src.utils.config import Settings


def test_default_settings():
    s = Settings()
    assert s.host == "127.0.0.1"
    assert s.port == 8090
    assert s.log_level == "info"
    assert s.dev_mode is False
    assert s.debounce_ms == 300
    assert s.settle_time_ms == 5000
    assert s.backpressure_max == 10_000
    assert s.janitor_purge_days == 7
    assert s.janitor_reindex_threshold == 0.20


def test_env_override(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_PORT", "9999")
    monkeypatch.setenv("LOCALBRAIN_DEV_MODE", "true")
    monkeypatch.setenv("LOCALBRAIN_LOG_LEVEL", "debug")

    s = Settings()
    assert s.port == 9999
    assert s.dev_mode is True
    assert s.log_level == "debug"


def test_data_dir_default():
    s = Settings()
    assert ".localbrain" in s.data_dir
