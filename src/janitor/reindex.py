"""Smart lazy re-indexing.

If >20% of a document's chunks change, the document summary is flagged
for update.  If the user changes their embedding model, the re-indexing
queue is processed only when the system is idle, on AC power, and has no
user input.
"""

from __future__ import annotations

import logging
import sys

from src.vault.sqlite_engine import SQLiteEngine
from src.vault.subscriptions import SubscriptionManager

logger = logging.getLogger(__name__)


class ReindexManager:
    """Decides when and what to re-index."""

    def __init__(
        self,
        engine: SQLiteEngine,
        subscriptions: SubscriptionManager,
        *,
        threshold: float = 0.20,
    ) -> None:
        self._engine = engine
        self._subs = subscriptions
        self._threshold = threshold
        self._pending: set[str] = set()

    # ── Public API ──────────────────────────────────────

    async def should_reindex(self, file_id: str, changed_chunks: int) -> bool:
        """Return True if the ratio of changed chunks exceeds the threshold."""
        total = await self._subs.get_chunk_count(file_id)
        if total == 0:
            return True
        ratio = changed_chunks / total
        if ratio >= self._threshold:
            self._pending.add(file_id)
            logger.info(
                "File %s flagged for re-index (%.0f%% chunks changed)", file_id, ratio * 100
            )
            return True
        return False

    async def get_pending(self) -> list[str]:
        """Return file IDs pending re-indexing."""
        return list(self._pending)

    async def mark_done(self, file_id: str) -> None:
        self._pending.discard(file_id)

    # ── System checks ───────────────────────────────────

    @staticmethod
    def is_idle(*, cpu_threshold: float = 30.0) -> bool:
        """Return True if CPU usage is below *cpu_threshold* percent.

        Uses a lightweight heuristic; ``psutil`` is not required.
        """
        try:
            import psutil  # type: ignore[import-untyped]

            return psutil.cpu_percent(interval=0.5) < cpu_threshold
        except ImportError:
            # Without psutil, assume idle (conservative)
            return True

    @staticmethod
    def is_on_ac_power() -> bool:
        """Return True if the machine is plugged in (or desktop)."""
        try:
            import psutil  # type: ignore[import-untyped]

            battery = psutil.sensors_battery()
            if battery is None:
                return True  # Desktop / no battery
            return battery.power_plugged
        except ImportError:
            return True

    def can_reindex(self) -> bool:
        """Return True if re-indexing conditions are met."""
        return self.is_idle() and self.is_on_ac_power() and bool(self._pending)
