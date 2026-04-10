"""SQLite-backed persistent queue with backpressure.

Survives process restarts.  When the queue exceeds the configured
maximum depth the ``is_full`` flag is set, signalling upstream
components to pause ingestion.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import aiosqlite

from src.core.models import HandoffRequest

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS handoff_queue (
    id          TEXT PRIMARY KEY,
    payload     TEXT    NOT NULL,
    created_at  REAL    NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    locked_until REAL   NOT NULL DEFAULT 0
);
"""


class BackpressureQueue:
    """Durable FIFO queue backed by a dedicated SQLite database."""

    def __init__(self, db_path: str, *, max_depth: int = 10_000) -> None:
        self._path = db_path
        self._max_depth = max_depth
        self._db: aiosqlite.Connection | None = None

    # ── Lifecycle ───────────────────────────────────────

    async def open(self) -> None:
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute(_DDL)
        await self._db.commit()
        depth = await self.depth()
        logger.info("Backpressure queue opened (%d pending): %s", depth, self._path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "BackpressureQueue not opened"
        return self._db

    # ── Enqueue / Dequeue ───────────────────────────────

    async def enqueue(self, request: HandoffRequest) -> str:
        """Add a handoff request to the queue.  Returns the item ID."""
        import uuid

        item_id = uuid.uuid4().hex
        payload = request.model_dump_json()
        await self.db.execute(
            "INSERT INTO handoff_queue (id, payload, created_at) VALUES (?, ?, ?)",
            (item_id, payload, time.time()),
        )
        await self.db.commit()
        return item_id

    async def dequeue(self, batch_size: int = 10) -> list[tuple[str, HandoffRequest]]:
        """Fetch and lock up to *batch_size* items.

        Returns a list of ``(item_id, HandoffRequest)`` tuples.
        """
        now = time.time()
        lock_until = now + 300

        async with self.db.execute(
            """SELECT * FROM handoff_queue
               WHERE locked_until < ?
               ORDER BY created_at ASC
               LIMIT ?""",
            (now, batch_size),
        ) as cur:
            rows = await cur.fetchall()

        results = []
        for row in rows:
            await self.db.execute(
                "UPDATE handoff_queue SET locked_until = ?, attempts = attempts + 1 WHERE id = ?",
                (lock_until, row["id"]),
            )
            try:
                request = HandoffRequest.model_validate_json(row["payload"])
            except Exception:
                logger.exception("Corrupt queue item %s — removing", row["id"])
                await self.db.execute(
                    "DELETE FROM handoff_queue WHERE id = ?", (row["id"],)
                )
                continue
            results.append((row["id"], request))

        await self.db.commit()
        return results

    async def ack(self, item_id: str) -> None:
        """Remove a successfully processed item."""
        await self.db.execute("DELETE FROM handoff_queue WHERE id = ?", (item_id,))
        await self.db.commit()

    async def nack(self, item_id: str, retry_after: float = 30.0) -> None:
        """Unlock an item for retry after *retry_after* seconds."""
        await self.db.execute(
            "UPDATE handoff_queue SET locked_until = ? WHERE id = ?",
            (time.time() + retry_after, item_id),
        )
        await self.db.commit()

    # ── Backpressure ────────────────────────────────────

    async def depth(self) -> int:
        async with self.db.execute("SELECT COUNT(*) as cnt FROM handoff_queue") as cur:
            row = await cur.fetchone()
            return row["cnt"] if row else 0

    async def is_full(self) -> bool:
        return (await self.depth()) >= self._max_depth
