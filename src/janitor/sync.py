"""Journal-based filesystem-to-vault synchronisation.

Reads the change journal entries since the last high-water mark
timestamp and reconciles the vault against the filesystem.  This
avoids full disk scans while catching events the watcher may have missed.
"""

from __future__ import annotations

import logging
import os
import time

from src.core.models import EventType, FileIdentity, IngestEvent
from src.vault.sqlite_engine import SQLiteEngine

logger = logging.getLogger(__name__)


class JournalSync:
    """Detects drift between the vault and the physical filesystem."""

    def __init__(self, engine: SQLiteEngine) -> None:
        self._engine = engine
        self._high_water_mark: float = 0.0

    # ── Public API ──────────────────────────────────────

    async def sync(self) -> list[IngestEvent]:
        """Compare vault state against disk.  Returns corrective events."""
        events: list[IngestEvent] = []

        # 1. Check journal for recent changes
        journal_entries = await self._engine.get_journal_since(self._high_water_mark)
        if journal_entries:
            self._high_water_mark = max(e["timestamp"] for e in journal_entries)

        # 2. Scan tracked files for drift
        files = await self._engine.list_files(limit=10_000)
        for record in files:
            path = record.identity.path
            try:
                stat = os.stat(path)
            except OSError:
                # File vanished — emit delete event
                events.append(
                    IngestEvent(
                        event_type=EventType.deleted,
                        file_identity=record.identity,
                    )
                )
                continue

            # Check for modification
            if stat.st_mtime != record.identity.mtime or stat.st_size != record.identity.size:
                events.append(
                    IngestEvent(
                        event_type=EventType.modified,
                        file_identity=FileIdentity(
                            path=path,
                            inode=stat.st_ino,
                            device=stat.st_dev,
                            mtime=stat.st_mtime,
                            size=stat.st_size,
                            head_hash="",  # Stale — will be recomputed on re-ingestion
                        ),
                    )
                )

        self._high_water_mark = time.time()
        if events:
            logger.info("Journal sync produced %d corrective events", len(events))
        return events
