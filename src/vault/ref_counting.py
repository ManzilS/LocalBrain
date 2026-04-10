"""ACID reference counting for shared chunks.

Every chunk maintains a ``ref_count``.  A chunk is only eligible for
deletion when its ref_count hits zero.  All writes are wrapped in
``BEGIN IMMEDIATE`` transactions to prevent corruption during power loss.
"""

from __future__ import annotations

import aiosqlite


class RefCounter:
    """Provides atomic increment/decrement on chunk reference counts."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def increment(self, chunk_id: str) -> int:
        """Atomically increment *chunk_id*'s ref_count. Returns new count."""
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            await self._db.execute(
                "UPDATE chunks SET ref_count = ref_count + 1 WHERE id = ?", (chunk_id,)
            )
            async with self._db.execute(
                "SELECT ref_count FROM chunks WHERE id = ?", (chunk_id,)
            ) as cur:
                row = await cur.fetchone()
                count = row["ref_count"] if row else 0
            await self._db.commit()
            return count
        except Exception:
            await self._db.rollback()
            raise

    async def decrement(self, chunk_id: str) -> int:
        """Atomically decrement *chunk_id*'s ref_count. Returns new count.

        The count will not go below zero.
        """
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            await self._db.execute(
                "UPDATE chunks SET ref_count = MAX(0, ref_count - 1) WHERE id = ?",
                (chunk_id,),
            )
            async with self._db.execute(
                "SELECT ref_count FROM chunks WHERE id = ?", (chunk_id,)
            ) as cur:
                row = await cur.fetchone()
                count = row["ref_count"] if row else 0
            await self._db.commit()
            return count
        except Exception:
            await self._db.rollback()
            raise

    async def get_count(self, chunk_id: str) -> int:
        async with self._db.execute(
            "SELECT ref_count FROM chunks WHERE id = ?", (chunk_id,)
        ) as cur:
            row = await cur.fetchone()
            return row["ref_count"] if row else 0

    async def get_orphans(self) -> list[str]:
        """Return chunk IDs with zero references — eligible for deletion."""
        async with self._db.execute(
            "SELECT id FROM chunks WHERE ref_count <= 0"
        ) as cur:
            rows = await cur.fetchall()
            return [r["id"] for r in rows]

    async def bulk_increment(self, chunk_ids: list[str]) -> None:
        """Increment ref_count for multiple chunks in one transaction."""
        if not chunk_ids:
            return
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            for cid in chunk_ids:
                await self._db.execute(
                    "UPDATE chunks SET ref_count = ref_count + 1 WHERE id = ?", (cid,)
                )
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise

    async def bulk_decrement(self, chunk_ids: list[str]) -> list[str]:
        """Decrement ref_count for multiple chunks. Returns newly-orphaned IDs."""
        if not chunk_ids:
            return []
        orphans: list[str] = []
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            for cid in chunk_ids:
                await self._db.execute(
                    "UPDATE chunks SET ref_count = MAX(0, ref_count - 1) WHERE id = ?",
                    (cid,),
                )
                async with self._db.execute(
                    "SELECT ref_count FROM chunks WHERE id = ?", (cid,)
                ) as cur:
                    row = await cur.fetchone()
                    if row and row["ref_count"] <= 0:
                        orphans.append(cid)
            await self._db.commit()
            return orphans
        except Exception:
            await self._db.rollback()
            raise
