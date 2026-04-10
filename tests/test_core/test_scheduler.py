"""Tests for the priority queue lane scheduler."""

from __future__ import annotations

import pytest

from src.core.models import EventType, FileIdentity, IngestEvent, QueueLane
from src.core.scheduler import Scheduler, classify_lane


def test_classify_lane_fast():
    assert classify_lane("/a/b.txt") == QueueLane.fast
    assert classify_lane("/a/b.py") == QueueLane.fast
    assert classify_lane("/a/b.md") == QueueLane.fast
    assert classify_lane("/a/b.json") == QueueLane.fast


def test_classify_lane_heavy():
    assert classify_lane("/a/b.pdf") == QueueLane.heavy
    assert classify_lane("/a/b.docx") == QueueLane.heavy
    assert classify_lane("/a/b.xlsx") == QueueLane.heavy


def test_classify_lane_background():
    assert classify_lane("/a/b.zip") == QueueLane.background
    assert classify_lane("/a/b.jpg") == QueueLane.background
    assert classify_lane("/a/b.mp3") == QueueLane.background


def test_classify_lane_tar_gz():
    assert classify_lane("/a/b.tar.gz") == QueueLane.background


def test_classify_lane_unknown():
    assert classify_lane("/a/b.xyz") == QueueLane.heavy  # Default


@pytest.mark.asyncio
async def test_enqueue():
    scheduler = Scheduler()
    event = IngestEvent(
        event_type=EventType.created,
        file_identity=FileIdentity(path="/test.txt"),
    )
    await scheduler.enqueue(event)

    depths = await scheduler.get_depths()
    assert depths["fast"] == 1


@pytest.mark.asyncio
async def test_enqueue_explicit_lane():
    scheduler = Scheduler()
    event = IngestEvent(
        event_type=EventType.created,
        file_identity=FileIdentity(path="/test.txt"),
    )
    await scheduler.enqueue(event, lane=QueueLane.background)

    depths = await scheduler.get_depths()
    assert depths["background"] == 1
    assert depths["fast"] == 0
