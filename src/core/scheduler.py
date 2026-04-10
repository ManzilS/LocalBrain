"""Priority queue lane manager.

Routes ingestion events into three lanes — fast, heavy, and background —
each with configurable concurrency.  Workers retry transient failures
with exponential backoff up to a configurable limit.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from src.core.models import IngestEvent, QueueLane
from src.core.state import IngestState

logger = logging.getLogger(__name__)

# Default worker counts per lane
_LANE_WORKERS: dict[QueueLane, int] = {
    QueueLane.fast: 4,
    QueueLane.heavy: 2,
    QueueLane.background: 1,
}

_MAX_RETRIES = 3
_BASE_RETRY_DELAY = 2.0  # seconds
_MAX_DEAD_LETTER = 1000

# Map file extensions to lanes
_FAST_EXTENSIONS = {
    ".txt", ".md", ".markdown", ".rst", ".py", ".js", ".ts", ".tsx",
    ".jsx", ".java", ".c", ".cpp", ".h", ".go", ".rs", ".rb", ".sh",
    ".yaml", ".yml", ".toml", ".json", ".xml", ".html", ".css",
    ".csv", ".log", ".sql", ".ini", ".cfg",
}

_HEAVY_EXTENSIONS = {".pdf", ".docx", ".xlsx", ".pptx", ".html", ".htm"}

_BACKGROUND_EXTENSIONS = {
    ".zip", ".tar", ".tar.gz", ".tgz", ".rar",
    ".jpg", ".jpeg", ".png", ".gif", ".tiff", ".webp", ".bmp",
    ".mp3", ".wav", ".ogg", ".flac", ".m4a",
    ".mp4", ".mkv", ".avi", ".mov",
}


def classify_lane(path: str) -> QueueLane:
    """Determine the queue lane for a file path based on its extension."""
    from pathlib import Path

    suffix = Path(path).suffix.lower()
    # Check multi-part extensions like .tar.gz
    name = Path(path).name.lower()
    if name.endswith(".tar.gz") or name.endswith(".tar.bz2") or name.endswith(".tar.xz"):
        return QueueLane.background

    if suffix in _FAST_EXTENSIONS:
        return QueueLane.fast
    if suffix in _HEAVY_EXTENSIONS:
        return QueueLane.heavy
    if suffix in _BACKGROUND_EXTENSIONS:
        return QueueLane.background
    return QueueLane.heavy  # Default: unknown formats go to heavy


class Scheduler:
    """Routes events into priority lanes and manages worker pools."""

    def __init__(self, *, max_retries: int = _MAX_RETRIES) -> None:
        self._queues: dict[QueueLane, asyncio.PriorityQueue[tuple[int, int, IngestState]]] = {
            lane: asyncio.PriorityQueue() for lane in QueueLane
        }
        self._tasks: list[asyncio.Task[None]] = []
        self._handler: Callable[[IngestState], Awaitable[IngestState]] | None = None
        self._running = False
        self._max_retries = max_retries
        self._dead_letter: list[tuple[IngestState, str]] = []  # capped at _MAX_DEAD_LETTER
        self._seq = 0  # Tie-breaker for PriorityQueue ordering

    def _add_dead_letter(self, state: IngestState, error: str) -> None:
        """Append to dead-letter list, evicting oldest if at capacity."""
        self._dead_letter.append((state, error))
        if len(self._dead_letter) > _MAX_DEAD_LETTER:
            self._dead_letter = self._dead_letter[-_MAX_DEAD_LETTER:]

    def set_handler(self, handler: Callable[[IngestState], Awaitable[IngestState]]) -> None:
        """Set the pipeline execution function that workers will call."""
        self._handler = handler

    async def enqueue(self, event: IngestEvent, lane: QueueLane | None = None) -> None:
        """Route an event into the appropriate lane queue."""
        if lane is None:
            lane = classify_lane(event.file_identity.path)

        state = IngestState(event=event)
        priority = {QueueLane.fast: 0, QueueLane.heavy: 1, QueueLane.background: 2}[lane]

        self._seq += 1
        await self._queues[lane].put((priority, self._seq, state))
        logger.debug("Enqueued %s → %s lane", event.file_identity.path, lane.value)

    async def _requeue(self, state: IngestState, lane: QueueLane) -> None:
        """Re-queue a failed item with the same priority."""
        priority = {QueueLane.fast: 0, QueueLane.heavy: 1, QueueLane.background: 2}[lane]
        self._seq += 1
        await self._queues[lane].put((priority, self._seq, state))

    async def start(self) -> None:
        """Spin up worker tasks for each lane."""
        assert self._handler is not None, "No handler set — call set_handler() first"
        self._running = True

        for lane, count in _LANE_WORKERS.items():
            for i in range(count):
                task = asyncio.create_task(
                    self._worker(lane), name=f"worker-{lane.value}-{i}"
                )
                self._tasks.append(task)

        logger.info(
            "Scheduler started: %s",
            ", ".join(f"{l.value}={c}" for l, c in _LANE_WORKERS.items()),
        )

    async def stop(self) -> None:
        """Gracefully shut down all workers."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("Scheduler stopped")

    async def get_depths(self) -> dict[str, int]:
        """Return current queue depth for each lane."""
        return {lane.value: q.qsize() for lane, q in self._queues.items()}

    @property
    def dead_letter(self) -> list[tuple[IngestState, str]]:
        """Items that exhausted all retries."""
        return list(self._dead_letter)

    @property
    def dead_letter_count(self) -> int:
        return len(self._dead_letter)

    # ── Worker ──────────────────────────────────────────

    async def _worker(self, lane: QueueLane) -> None:
        """Process items from a single lane queue with retry."""
        queue = self._queues[lane]
        while self._running:
            try:
                _priority, _seq, state = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            attempt = state.extras.get("_retry_count", 0)
            try:
                assert self._handler is not None
                # Reset error state on retry
                state.error = None
                state.early_exit = False
                state.phase = "ingress"

                result = await self._handler(state)

                if result.error:
                    # Pipeline returned an error state — decide whether to retry
                    if attempt < self._max_retries and self._is_retryable(result.error):
                        state.extras["_retry_count"] = attempt + 1
                        delay = _BASE_RETRY_DELAY * (2 ** attempt)
                        logger.warning(
                            "Retryable error (%s, attempt %d/%d): %s — %s (retry in %.1fs)",
                            lane.value,
                            attempt + 1,
                            self._max_retries,
                            state.event.file_identity.path,
                            result.error,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        await self._requeue(state, lane)
                    else:
                        logger.error(
                            "Pipeline failed (%s): %s — %s",
                            lane.value,
                            state.event.file_identity.path,
                            result.error,
                        )
                        self._add_dead_letter(state, result.error)

            except Exception as exc:
                if attempt < self._max_retries:
                    state.extras["_retry_count"] = attempt + 1
                    delay = _BASE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        "Unhandled error (%s, attempt %d/%d): %s — retrying in %.1fs",
                        lane.value,
                        attempt + 1,
                        self._max_retries,
                        state.event.file_identity.path,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    await self._requeue(state, lane)
                else:
                    logger.exception(
                        "Exhausted retries (%s): %s",
                        lane.value,
                        state.event.file_identity.path,
                    )
                    self._add_dead_letter(state, str(exc))
            finally:
                queue.task_done()

    @staticmethod
    def _is_retryable(error: str) -> bool:
        """Determine if an error is transient and worth retrying."""
        retryable_patterns = [
            "Failed to read file",
            "Cannot stat file",
            "timeout",
            "connection",
            "disk",
            "PermissionError",
            "OSError",
        ]
        error_lower = error.lower()
        return any(p.lower() in error_lower for p in retryable_patterns)
