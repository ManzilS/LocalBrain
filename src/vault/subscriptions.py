"""Subscription-based document-chunk mapping.

Documents do not "own" chunks — a many-to-many join table maps
Document_ID to Chunk_ID.  This allows shared chunks across documents
(deduplication) without data duplication.
"""

from __future__ import annotations

import aiosqlite


class SubscriptionManager:
    """Manages the ``file_chunks`` junction table."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def subscribe(self, file_id: str, chunk_ids: list[str]) -> None:
        """Link *file_id* to each chunk in *chunk_ids* with sequence order."""
        await self.subscribe_no_commit(file_id, chunk_ids)
        await self._db.commit()

    async def subscribe_no_commit(self, file_id: str, chunk_ids: list[str]) -> None:
        """Link *file_id* to chunks without committing (for transactions)."""
        for seq, chunk_id in enumerate(chunk_ids):
            await self._db.execute(
                """INSERT INTO file_chunks (file_id, chunk_id, sequence)
                   VALUES (?, ?, ?)
                   ON CONFLICT(file_id, chunk_id) DO UPDATE SET sequence=excluded.sequence""",
                (file_id, chunk_id, seq),
            )

    async def unsubscribe(self, file_id: str) -> list[str]:
        """Remove all chunk links for *file_id*.  Returns orphaned chunk_ids."""
        # Get the chunks this file was subscribed to
        async with self._db.execute(
            "SELECT chunk_id FROM file_chunks WHERE file_id = ?", (file_id,)
        ) as cur:
            rows = await cur.fetchall()
            chunk_ids = [r["chunk_id"] for r in rows]

        # Remove the subscriptions
        await self._db.execute("DELETE FROM file_chunks WHERE file_id = ?", (file_id,))
        await self._db.commit()

        return chunk_ids

    async def get_subscribers(self, chunk_id: str) -> list[str]:
        """Return file IDs that reference *chunk_id*."""
        async with self._db.execute(
            "SELECT file_id FROM file_chunks WHERE chunk_id = ?", (chunk_id,)
        ) as cur:
            rows = await cur.fetchall()
            return [r["file_id"] for r in rows]

    async def get_chunks(self, file_id: str) -> list[str]:
        """Return chunk IDs belonging to *file_id*, ordered by sequence."""
        async with self._db.execute(
            "SELECT chunk_id FROM file_chunks WHERE file_id = ? ORDER BY sequence",
            (file_id,),
        ) as cur:
            rows = await cur.fetchall()
            return [r["chunk_id"] for r in rows]

    async def get_chunk_count(self, file_id: str) -> int:
        async with self._db.execute(
            "SELECT COUNT(*) as cnt FROM file_chunks WHERE file_id = ?", (file_id,)
        ) as cur:
            row = await cur.fetchone()
            return row["cnt"] if row else 0
