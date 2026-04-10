"""Tests for scope-gating logic."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from src.ingress.scope_gate import ScopeGate
from src.utils.errors import ScopeGateError


@pytest.fixture
def gate(tmp_path):
    config = {
        "watch_roots": [str(tmp_path)],
        "include_patterns": [],
        "exclude_patterns": ["**/node_modules/**", "**/.git/**"],
        "blocked_extensions": [".pem", ".key", ".env"],
        "max_file_size_bytes": 1024 * 1024,
        "follow_symlinks": False,
    }
    return ScopeGate(config)


def test_allowed_file(gate, tmp_path):
    f = tmp_path / "test.txt"
    f.touch()
    assert gate.is_allowed(f)


def test_outside_watch_root(gate):
    assert not gate.is_allowed("/some/other/path/file.txt")


def test_blocked_extension(gate, tmp_path):
    f = tmp_path / "secret.pem"
    f.touch()
    assert not gate.is_allowed(f)


def test_blocked_env_extension(gate, tmp_path):
    f = tmp_path / ".env"
    f.touch()
    assert not gate.is_allowed(f)


def test_excluded_pattern(gate, tmp_path):
    d = tmp_path / "node_modules" / "pkg"
    d.mkdir(parents=True)
    f = d / "index.js"
    f.touch()
    # The fnmatch should match the node_modules pattern
    assert not gate.is_allowed(f)


def test_size_check(gate):
    assert gate.check_size(100)
    assert not gate.check_size(2 * 1024 * 1024)


def test_enforce_raises(gate):
    with pytest.raises(ScopeGateError):
        gate.enforce("/outside/path.txt")


def test_enforce_size_raises(gate, tmp_path):
    f = tmp_path / "big.txt"
    f.touch()
    with pytest.raises(ScopeGateError, match="size limit"):
        gate.enforce(f, size=2 * 1024 * 1024)


def test_get_watch_roots(gate, tmp_path):
    roots = gate.get_watch_roots()
    assert len(roots) == 1
    assert roots[0] == tmp_path.resolve()


def test_include_patterns():
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        config = {
            "watch_roots": [td],
            "include_patterns": ["**/*.py"],
            "exclude_patterns": [],
            "blocked_extensions": [],
        }
        gate = ScopeGate(config)
        py_file = Path(td) / "script.py"
        py_file.touch()
        txt_file = Path(td) / "readme.txt"
        txt_file.touch()

        assert gate.is_allowed(py_file)
        # txt_file won't match **/*.py include pattern
        assert not gate.is_allowed(txt_file)


def test_from_file_missing(tmp_path):
    gate = ScopeGate.from_file(tmp_path / "nonexistent.json")
    # Should fall back to permissive defaults
    assert gate.get_watch_roots() is not None
