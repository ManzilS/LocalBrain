"""HybridSearchEngine RRF fusion + result-shape adapter."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.retrieval.hybrid_search import HybridSearchEngine, _as_hit
from src.retrieval.intent_router import IntentRouter


# ── _as_hit adapter ────────────────────────────────────────


def test_as_hit_accepts_vector_shape() -> None:
    raw = {"id": "c1", "file_id": "f1", "content": "x", "score": 0.1}
    hit = _as_hit(raw, source="vector")
    assert hit is not None
    assert hit["chunk_id"] == "c1"
    assert hit["source"] == "vector"


def test_as_hit_accepts_keyword_shape() -> None:
    raw = {"chunk_id": "c1", "path": "/x", "snippet": "…", "score": -1.2}
    hit = _as_hit(raw, source="keyword")
    assert hit is not None
    assert hit["chunk_id"] == "c1"
    assert hit["snippet"] == "…"


def test_as_hit_returns_none_on_missing_id() -> None:
    assert _as_hit({"content": "no id"}, source="vector") is None


# ── RRF fusion ─────────────────────────────────────────────


def _engine() -> HybridSearchEngine:
    # Settings are only consulted for the default intent router; we pass
    # our own so a minimal stand-in settings object is fine.
    settings = SimpleNamespace(
        enable_ms_graphrag_summarization=False,
        enable_hipporag_pagerank=False,
        enable_graphrag=False,
    )
    return HybridSearchEngine(
        sqlite=None, lance=None, kuzu=None, settings=settings, router=IntentRouter()
    )


def test_rrf_single_list_just_ranks() -> None:
    eng = _engine()
    hits = [
        _as_hit({"chunk_id": "a", "score": 1}, source="keyword"),
        _as_hit({"chunk_id": "b", "score": 2}, source="keyword"),
    ]
    out = eng._rrf([], hits, limit=10)
    assert [h["chunk_id"] for h in out] == ["a", "b"]
    assert out[0]["rrf_score"] > out[1]["rrf_score"]


def test_rrf_fuses_overlap() -> None:
    eng = _engine()
    vec = [_as_hit({"id": "shared", "score": 0.1}, source="vector")]
    kw = [_as_hit({"chunk_id": "shared", "score": -2.0, "snippet": "hi"}, source="keyword")]
    out = eng._rrf(vec, kw, limit=10)
    assert len(out) == 1
    merged = out[0]
    assert merged["chunk_id"] == "shared"
    assert merged["vector_score"] == 0.1
    assert merged["keyword_score"] == -2.0
    # Keyword metadata (snippet) should be preserved on the merged row.
    assert merged["snippet"] == "hi"


def test_rrf_union_when_disjoint() -> None:
    eng = _engine()
    vec = [_as_hit({"id": "v1", "score": 0.1}, source="vector")]
    kw = [_as_hit({"chunk_id": "k1", "score": -1.0}, source="keyword")]
    out = eng._rrf(vec, kw, limit=10)
    assert {h["chunk_id"] for h in out} == {"v1", "k1"}


def test_rrf_respects_limit() -> None:
    eng = _engine()
    vec = [_as_hit({"id": f"v{i}", "score": i}, source="vector") for i in range(5)]
    out = eng._rrf(vec, [], limit=2)
    assert len(out) == 2
