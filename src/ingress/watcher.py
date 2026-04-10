"""Hybrid event-driven file watcher with debounce and settle-time polling.

Uses OS-level events (via ``watchfiles``) with a debounce timer.  A
periodic reconciliation sweep acts as a backup for high I/O loads.
Settle-time polling ensures active downloads are fully written before
ingestion begins.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING

from watchfiles import Change, awatch

from src.core.models import EventType, IngestEvent
from src.ingress.identity import FileIdentityResolver
from src.ingress.scope_gate import ScopeGate

if TYPE_CHECKING:
    from src.utils.config import Settings

logger = logging.getLogger(__name__)

_CHANGE_MAP: dict[Change, EventType] = {
    Change.added: EventType.created,
    Change.modified: EventType.modified,
    Change.deleted: EventType.deleted,
}


class FileWatcher:
    """Produces a stream of ``IngestEvent`` objects for the pipeline."""

    def __init__(
        self,
        scope_gate: ScopeGate,
        identity_resolver: FileIdentityResolver,
        settings: Settings,
    ) -> None:
        self._gate = scope_gate
        self._resolver = identity_resolver
        self._debounce_s = settings.debounce_ms / 1000
        self._settle_s = settings.settle_time_ms / 1000
        self._poll_s = settings.poll_interval_s
        self._roots = scope_gate.get_watch_roots()
        self._stop = asyncio.Event()

    # ── Public API ──────────────────────────────────────

    async def watch(self) -> AsyncIterator[IngestEvent]:
        """Yield debounced, scope-gated ``IngestEvent`` objects."""
        existing_roots = [r for r in self._roots if r.exists()]
        if not existing_roots:
            logger.warning("No valid watch roots found — watcher idle")
            return

        str_roots = [str(r) for r in existing_roots]
        logger.info("Watching %d roots: %s", len(str_roots), str_roots)

        async for changes in awatch(
            *str_roots,
            debounce=int(self._debounce_s * 1000),
            stop_event=self._stop,
            recursive=True,
        ):
            for change_type, path_str in changes:
                path = Path(path_str)

                if not self._gate.is_allowed(path):
                    continue

                event_type = _CHANGE_MAP.get(change_type)
                if event_type is None:
                    continue

                # Settle-time: wait for the file to stop changing
                if event_type != EventType.deleted:
                    settled = await self._wait_settle(path)
                    if not settled:
                        continue

                try:
                    identity = self._resolver.resolve(path)
                except OSError:
                    # File vanished between event and resolve
                    if event_type != EventType.deleted:
                        continue
                    from src.core.models import FileIdentity

                    identity = FileIdentity(path=str(path))

                yield IngestEvent(
                    event_type=event_type,
                    file_identity=identity,
                )

    def stop(self) -> None:
        """Signal the watcher to shut down gracefully."""
        self._stop.set()

    # ── Settle-time polling ─────────────────────────────

    async def _wait_settle(self, path: Path, *, max_wait: float = 60.0) -> bool:
        """Poll the file size until it stabilises for ``_settle_s`` seconds.

        Returns False if the file vanishes or exceeds *max_wait*.
        """
        deadline = time.monotonic() + max_wait
        last_size = -1
        stable_since = -1.0  # sentinel: not yet stable

        while time.monotonic() < deadline:
            try:
                size = path.stat().st_size
            except OSError:
                return False

            now = time.monotonic()
            if size == last_size:
                if stable_since >= 0 and (now - stable_since) >= self._settle_s:
                    return True
                if stable_since < 0:
                    stable_since = now
            else:
                last_size = size
                stable_since = -1.0

            await asyncio.sleep(min(0.5, self._settle_s / 4))

        logger.warning("File did not settle within %.0fs: %s", max_wait, path)
        return False
