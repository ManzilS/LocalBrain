"""Tests for xxHash fingerprinting utilities."""

from __future__ import annotations

from src.chunking.fingerprint import (
    chunk_fingerprint,
    file_fingerprint,
    head_hash,
    partial_fingerprint,
)


def test_file_fingerprint_deterministic():
    data = b"hello world"
    assert file_fingerprint(data) == file_fingerprint(data)


def test_file_fingerprint_differs_for_different_content():
    assert file_fingerprint(b"hello") != file_fingerprint(b"world")


def test_chunk_fingerprint_deterministic():
    assert chunk_fingerprint("hello") == chunk_fingerprint("hello")


def test_chunk_fingerprint_differs():
    assert chunk_fingerprint("hello") != chunk_fingerprint("world")


def test_head_hash_uses_first_n_bytes():
    data = b"A" * 8192
    h1 = head_hash(data, size=4096)
    h2 = head_hash(data[:4096])
    assert h1 == h2


def test_head_hash_small_file():
    data = b"tiny"
    h = head_hash(data)
    assert len(h) > 0


def test_partial_fingerprint_deterministic():
    data = b"X" * 10000
    assert partial_fingerprint(data) == partial_fingerprint(data)


def test_partial_fingerprint_differs():
    a = b"A" * 10000
    b_data = b"B" * 10000
    assert partial_fingerprint(a) != partial_fingerprint(b_data)


def test_partial_fingerprint_small_file():
    # Should work even if file is smaller than tail size
    data = b"small"
    h = partial_fingerprint(data)
    assert len(h) > 0
