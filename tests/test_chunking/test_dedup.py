"""Tests for chunk-level deduplication."""

from __future__ import annotations

from src.chunking.dedup import ChunkDeduplicator


def test_new_dedup_empty():
    d = ChunkDeduplicator()
    assert d.count == 0
    assert not d.is_duplicate("any")


def test_register_and_detect():
    d = ChunkDeduplicator()
    d.register("fp1")
    assert d.is_duplicate("fp1")
    assert not d.is_duplicate("fp2")


def test_register_many():
    d = ChunkDeduplicator()
    d.register_many(["a", "b", "c"])
    assert d.count == 3
    assert d.is_duplicate("b")


def test_remove():
    d = ChunkDeduplicator()
    d.register("fp1")
    d.remove("fp1")
    assert not d.is_duplicate("fp1")
    assert d.count == 0


def test_remove_nonexistent():
    d = ChunkDeduplicator()
    d.remove("nonexistent")  # Should not raise
    assert d.count == 0
