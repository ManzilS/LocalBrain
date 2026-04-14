"""Tests for Settings loading, defaults, and validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

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


# ── Validation tests ───────────────────────────────────


def test_port_too_low(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_PORT", "0")
    with pytest.raises(ValidationError):
        Settings()


def test_port_too_high(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_PORT", "70000")
    with pytest.raises(ValidationError):
        Settings()


def test_port_boundary_valid(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_PORT", "1")
    assert Settings().port == 1
    monkeypatch.setenv("LOCALBRAIN_PORT", "65535")
    assert Settings().port == 65535


def test_negative_debounce_rejected(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_DEBOUNCE_MS", "-1")
    with pytest.raises(ValidationError):
        Settings()


def test_zero_poll_interval_rejected(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_POLL_INTERVAL_S", "0")
    with pytest.raises(ValidationError):
        Settings()


def test_negative_purge_days_rejected(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_JANITOR_PURGE_DAYS", "0")
    with pytest.raises(ValidationError):
        Settings()


def test_reindex_threshold_bounds(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_JANITOR_REINDEX_THRESHOLD", "1.5")
    with pytest.raises(ValidationError):
        Settings()

    monkeypatch.setenv("LOCALBRAIN_JANITOR_REINDEX_THRESHOLD", "-0.1")
    with pytest.raises(ValidationError):
        Settings()


def test_zero_request_timeout_rejected(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_REQUEST_TIMEOUT", "0")
    with pytest.raises(ValidationError):
        Settings()


def test_zero_rate_limit_rejected(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_RATE_LIMIT_RPM", "0")
    with pytest.raises(ValidationError):
        Settings()


def test_zero_max_body_size_rejected(monkeypatch):
    monkeypatch.setenv("LOCALBRAIN_MAX_BODY_SIZE", "0")
    with pytest.raises(ValidationError):
        Settings()
