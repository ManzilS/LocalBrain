"""Tests for the SQLite-backed persistent backpressure queue."""

from __future__ import annotations

import pytest

from src.core.models import Chunk, HandoffRequest
from src.router_handoff.backpressure_queue import BackpressureQueue


@pytest.fixture
async def queue(tmp_path):
    q = BackpressureQueue(str(tmp_path / "queue.db"), max_depth=5)
    await q.open()
    yield q
    await q.close()


def _make_request(action: str = "embed") -> HandoffRequest:
    return HandoffRequest(
        chunks=[Chunk(content="test", fingerprint="fp1")],
        action=action,
    )


@pytest.mark.asyncio
async def test_enqueue_and_depth(queue):
    await queue.enqueue(_make_request())
    assert await queue.depth() == 1


@pytest.mark.asyncio
async def test_dequeue(queue):
    await queue.enqueue(_make_request("embed"))
    items = await queue.dequeue(batch_size=1)
    assert len(items) == 1
    item_id, req = items[0]
    assert req.action == "embed"


@pytest.mark.asyncio
async def test_ack_removes(queue):
    await queue.enqueue(_make_request())
    items = await queue.dequeue()
    item_id, _ = items[0]
    await queue.ack(item_id)
    assert await queue.depth() == 0


@pytest.mark.asyncio
async def test_nack_unlocks(queue):
    await queue.enqueue(_make_request())
    items = await queue.dequeue()
    item_id, _ = items[0]

    # Item is locked, dequeue should return nothing
    items2 = await queue.dequeue()
    assert len(items2) == 0

    # Nack with retry_after=0 should make it immediately available
    await queue.nack(item_id, retry_after=0)
    items3 = await queue.dequeue()
    assert len(items3) == 1


@pytest.mark.asyncio
async def test_backpressure_full(queue):
    for i in range(5):
        await queue.enqueue(_make_request())
    assert await queue.is_full()


@pytest.mark.asyncio
async def test_backpressure_not_full(queue):
    await queue.enqueue(_make_request())
    assert not await queue.is_full()


@pytest.mark.asyncio
async def test_persistence(tmp_path):
    """Queue survives close and reopen."""
    path = str(tmp_path / "persist.db")

    q1 = BackpressureQueue(path, max_depth=100)
    await q1.open()
    await q1.enqueue(_make_request())
    await q1.close()

    q2 = BackpressureQueue(path, max_depth=100)
    await q2.open()
    assert await q2.depth() == 1
    await q2.close()
