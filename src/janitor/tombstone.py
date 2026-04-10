"""Tombstone cascade — soft deletes with deferred purge.

Deleted files are marked as "tombstones."  Data is only permanently
purged after 7 days, allowing Undo (Ctrl+Z) without re-running
expensive AI tasks.
"""

from __future__ import annotations

import logging

from src.vault.lance_engine import LanceEngine
from src.vault.ref_counting import RefCounter
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.subscriptions import SubscriptionManager

logger = logging.getLogger(__name__)


class TombstoneCascade:
    """Manages the soft-delete lifecycle for files and their chunks."""

    def __init__(
        self,
        engine: SQLiteEngine,
        lance: LanceEngine,
        subscriptions: SubscriptionManager,
        ref_counter: RefCounter,
    ) -> None:
        self._engine = engine
        self._lance = lance
        self._subs = subscriptions
        self._refs = ref_counter

    async def mark_deleted(self, file_id: str) -> None:
        """Soft-delete a file and decrement chunk ref counts."""
        # Unsubscribe from all chunks
        chunk_ids = await self._subs.unsubscribe(file_id)

        # Decrement ref counts
        orphans = await self._refs.bulk_decrement(chunk_ids)
        if orphans:
            logger.info(
                "File %s tombstoned — %d chunks orphaned (deferred purge)", file_id, len(orphans)
            )

        # Mark the file itself
        await self._engine.mark_tombstone(file_id)

    async def purge(self, older_than_days: int = 7) -> int:
        """Hard-delete tombstoned files and their orphaned chunks.

        Returns the total number of entities purged.
        """
        # 1. Purge tombstoned file records
        file_count = await self._engine.purge_tombstones(older_than_days)

        # 2. Find and purge orphaned chunks
        orphans = await self._refs.get_orphans()
        if orphans:
            # Remove from LanceDB
            try:
                await self._lance.delete_by_chunk_ids(orphans)
            except Exception:
                logger.exception("Failed to delete orphan chunks from LanceDB")

            # Remove from SQLite
            for oid in orphans:
                try:
                    await self._engine.db.execute("DELETE FROM chunks WHERE id = ?", (oid,))
                except Exception:
                    logger.exception("Failed to delete orphan chunk %s from SQLite", oid)
            await self._engine.db.commit()

        total = file_count + len(orphans)
        if total:
            logger.info(
                "Purged %d files and %d orphaned chunks (older than %d days)",
                file_count,
                len(orphans),
                older_than_days,
            )
        return total
