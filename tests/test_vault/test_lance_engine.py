"""Tests for the LanceDB vector engine."""

from __future__ import annotations

import pytest

from src.core.models import Chunk
from src.vault.lance_engine import LanceEngine


@pytest.fixture
async def lance(tmp_path):
    e = LanceEngine(str(tmp_path / "lance_test"), embedding_dim=4)
    await e.open()
    yield e
    await e.close()


def _make_chunk(chunk_id: str, file_id: str = "f1", embedding: list[float] | None = None) -> Chunk:
    return Chunk(
        id=chunk_id,
        file_id=file_id,
        content=f"content for {chunk_id}",
        fingerprint=f"fp_{chunk_id}",
        embedding=embedding or [0.1, 0.2, 0.3, 0.4],
    )


@pytest.mark.asyncio
async def test_upsert_and_count(lance):
    chunks = [_make_chunk("c1"), _make_chunk("c2")]
    written = await lance.upsert_embeddings(chunks)
    assert written == 2
    assert await lance.count() == 2


@pytest.mark.asyncio
async def test_upsert_skips_no_embedding(lance):
    c = Chunk(id="c1", file_id="f1", content="x", fingerprint="fp_c1", embedding=None)
    written = await lance.upsert_embeddings([c])
    assert written == 0


@pytest.mark.asyncio
async def test_search(lance):
    chunks = [
        _make_chunk("c1", embedding=[1.0, 0.0, 0.0, 0.0]),
        _make_chunk("c2", embedding=[0.0, 1.0, 0.0, 0.0]),
    ]
    await lance.upsert_embeddings(chunks)

    results = await lance.search([1.0, 0.0, 0.0, 0.0], limit=1)
    assert len(results) == 1
    assert results[0]["id"] == "c1"


@pytest.mark.asyncio
async def test_delete_by_chunk_ids(lance):
    await lance.upsert_embeddings([_make_chunk("c1"), _make_chunk("c2")])
    await lance.delete_by_chunk_ids(["c1"])
    assert await lance.count() == 1


@pytest.mark.asyncio
async def test_delete_by_file_id(lance):
    await lance.upsert_embeddings([_make_chunk("c1", "f1"), _make_chunk("c2", "f2")])
    await lance.delete_by_file_id("f1")
    assert await lance.count() == 1


@pytest.mark.asyncio
async def test_empty_operations(lance):
    assert await lance.upsert_embeddings([]) == 0
    await lance.delete_by_chunk_ids([])  # Should not raise
