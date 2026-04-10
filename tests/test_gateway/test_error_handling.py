"""Tests for structured error response formatting."""

from __future__ import annotations

from src.utils.errors import (
    AuthenticationError,
    FileAccessError,
    HandoffTimeoutError,
    LocalBrainError,
    ParserError,
    RateLimitError,
    ScopeGateError,
    UnsupportedFormatError,
    VaultIntegrityError,
)


def test_base_error_to_dict():
    err = LocalBrainError("something broke", details="extra info")
    d = err.to_dict()
    assert d["error"]["type"] == "internal_error"
    assert d["error"]["message"] == "something broke"
    assert d["error"]["details"] == "extra info"


def test_base_error_no_details():
    err = LocalBrainError("oops")
    d = err.to_dict()
    assert "details" not in d["error"]


def test_error_hierarchy_status_codes():
    assert FileAccessError("x").status_code == 403
    assert ParserError("x").status_code == 422
    assert UnsupportedFormatError("x").status_code == 415
    assert ScopeGateError("x").status_code == 403
    assert AuthenticationError("x").status_code == 401
    assert RateLimitError("x").status_code == 429
    assert HandoffTimeoutError("x").status_code == 504
    assert VaultIntegrityError("x").status_code == 500


def test_error_hierarchy_types():
    assert FileAccessError("x").error_type == "file_access_denied"
    assert UnsupportedFormatError("x").error_type == "unsupported_format"
    assert ScopeGateError("x").error_type == "scope_gate_denied"
    assert HandoffTimeoutError("x").error_type == "router_handoff_timeout"


def test_error_is_exception():
    err = ParserError("bad format")
    assert isinstance(err, Exception)
    assert isinstance(err, LocalBrainError)
    assert str(err) == "bad format"
