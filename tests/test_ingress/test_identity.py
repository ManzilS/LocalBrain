"""Tests for file identity resolution and change detection."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from src.core.models import FileIdentity
from src.ingress.identity import FileIdentityResolver, _fold_int64


@pytest.fixture
def resolver():
    return FileIdentityResolver()


def test_resolve_basic(resolver, tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("hello world")

    identity = resolver.resolve(f)
    assert identity.path == str(f.resolve())
    assert identity.size == 11
    assert identity.mtime > 0
    assert identity.head_hash != ""


def test_resolve_empty_file(resolver, tmp_path):
    f = tmp_path / "empty.txt"
    f.touch()

    identity = resolver.resolve(f)
    assert identity.size == 0
    assert identity.head_hash != ""  # xxHash of empty bytes


def test_has_changed_same(resolver, tmp_path):
    f = tmp_path / "stable.txt"
    f.write_text("content")

    id1 = resolver.resolve(f)
    id2 = resolver.resolve(f)
    assert not resolver.has_changed(id1, id2)


def test_has_changed_modified(resolver, tmp_path):
    f = tmp_path / "changing.txt"
    f.write_text("original")
    id1 = resolver.resolve(f)

    # Modify
    time.sleep(0.01)
    f.write_text("modified content!")
    id2 = resolver.resolve(f)

    assert resolver.has_changed(id1, id2)


def test_has_changed_different_size(resolver):
    a = FileIdentity(path="/a", inode=1, device=1, mtime=100, size=10, head_hash="h1")
    b = FileIdentity(path="/a", inode=1, device=1, mtime=100, size=20, head_hash="h1")
    assert resolver.has_changed(a, b)


def test_same_physical_file(resolver):
    a = FileIdentity(path="/a.txt", inode=42, device=1)
    b = FileIdentity(path="/b.txt", inode=42, device=1)
    assert resolver.same_physical_file(a, b)


def test_different_physical_file(resolver):
    a = FileIdentity(path="/a.txt", inode=42, device=1)
    b = FileIdentity(path="/b.txt", inode=99, device=1)
    assert not resolver.same_physical_file(a, b)


def test_same_physical_file_zero_inode(resolver):
    # inode=0 means we can't confirm same physical file
    a = FileIdentity(path="/a.txt", inode=0, device=1)
    b = FileIdentity(path="/b.txt", inode=0, device=1)
    assert not resolver.same_physical_file(a, b)


def test_fold_int64_in_range_unchanged():
    assert _fold_int64(0) == 0
    assert _fold_int64(12345) == 12345
    assert _fold_int64(-12345) == -12345
    assert _fold_int64((1 << 63) - 1) == (1 << 63) - 1


def test_fold_int64_overflow_collapses_to_signed_range():
    # Windows 128-bit FILE_ID_INFO values can exceed int64; folding must
    # produce a value SQLite's signed INTEGER can store.
    huge = (1 << 120) | 42
    folded = _fold_int64(huge)
    assert -(1 << 63) <= folded <= (1 << 63) - 1


def test_fold_int64_deterministic():
    # Equality comparisons must still work after folding.
    huge = (0xDEADBEEFCAFEBABE << 64) | 0x0123456789ABCDEF
    assert _fold_int64(huge) == _fold_int64(huge)
