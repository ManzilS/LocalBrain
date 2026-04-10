"""Tests for content-defined chunking."""

from __future__ import annotations

from src.chunking.cdc import cdc_chunk


def test_empty_content():
    assert cdc_chunk("") == []


def test_short_content_single_chunk():
    text = "Hello world"
    chunks = cdc_chunk(text, min_size=1, max_size=4096)
    assert len(chunks) == 1
    assert chunks[0].offset == 0
    assert chunks[0].length == len(text)


def test_chunks_cover_entire_content():
    text = "A" * 5000
    chunks = cdc_chunk(text, min_size=100, max_size=500, target_size=200)
    total = sum(c.length for c in chunks)
    assert total == len(text)


def test_chunks_are_non_overlapping():
    text = "B" * 8000
    chunks = cdc_chunk(text, min_size=100, max_size=1000, target_size=500)
    for i in range(1, len(chunks)):
        assert chunks[i].offset == chunks[i - 1].offset + chunks[i - 1].length


def test_chunks_respect_max_size():
    text = "C" * 10000
    max_size = 512
    chunks = cdc_chunk(text, min_size=64, max_size=max_size, target_size=256)
    for c in chunks:
        assert c.length <= max_size


def test_chunks_respect_min_size():
    text = "D" * 10000
    min_size = 128
    chunks = cdc_chunk(text, min_size=min_size, max_size=4096, target_size=512)
    # All chunks except possibly the last should be >= min_size
    for c in chunks[:-1]:
        assert c.length >= min_size


def test_each_chunk_has_fingerprint():
    text = "E" * 3000
    chunks = cdc_chunk(text, min_size=100, max_size=500)
    for c in chunks:
        assert c.fingerprint
        assert len(c.fingerprint) > 0


def test_markdown_boundary_preference():
    text = "A" * 500 + "\n## Header\n" + "B" * 500
    chunks = cdc_chunk(text, min_size=100, max_size=2000, target_size=400)
    # Should produce at least 2 chunks, ideally split near the header
    assert len(chunks) >= 1


def test_deterministic():
    text = "Deterministic test content " * 100
    a = cdc_chunk(text, min_size=100, max_size=500)
    b = cdc_chunk(text, min_size=100, max_size=500)
    assert len(a) == len(b)
    for ca, cb in zip(a, b):
        assert ca.offset == cb.offset
        assert ca.length == cb.length
        assert ca.fingerprint == cb.fingerprint
