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
        """Yield debounced, scope-gated ``IngestEvent`` objects.

        First performs an initial scan of every watch root so that files
        already present on disk at startup are ingested. Then switches to
        OS-level change notifications for anything that arrives afterwards.
        """
        existing_roots = [r for r in self._roots if r.exists()]
        missing_roots = [r for r in self._roots if not r.exists()]
        if missing_roots:
            logger.warning(
                "Skipping %d watch root(s) that do not exist: %s",
                len(missing_roots),
                [str(r) for r in missing_roots],
            )
        if not existing_roots:
            logger.warning("No valid watch roots found — watcher idle")
            return

        str_roots = [str(r) for r in existing_roots]

        # ── Initial scan: pick up files already on disk ─────
        logger.info("Initial scan of %d roots: %s", len(str_roots), str_roots)
        scanned = 0
        for root in existing_roots:
            async for event in self._scan_root(root):
                scanned += 1
                yield event
                # Cooperative yield every batch so we don't starve the loop
                if scanned % 50 == 0:
                    await asyncio.sleep(0)
        logger.info("Initial scan complete — emitted %d event(s)", scanned)

        # ── Live watch ─────────────────────────────────────
        logger.info("Watching %d roots: %s", len(str_roots), str_roots)

        async for changes in awatch(
            *str_roots,
            debounce=int(self._debounce_s * 1000),
            stop_event=self._stop,
            recursive=True,
        ):
            logger.debug("Watcher received %d raw change(s)", len(changes))
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

                logger.info("Event: %s %s", event_type.value, path)
                yield IngestEvent(
                    event_type=event_type,
                    file_identity=identity,
                )

    # ── Initial scan ────────────────────────────────────

    async def _scan_root(self, root: Path) -> AsyncIterator[IngestEvent]:
        """Emit a ``created`` event for every scope-allowed file under *root*.

        Uses :meth:`pathlib.Path.rglob` and applies the scope gate to each
        candidate. Files already tracked in the vault will be no-ops in the
        pipeline because their identity is unchanged.
        """
        try:
            iterator = root.rglob("*")
        except OSError as exc:
            logger.warning("Could not scan %s: %s", root, exc)
            return

        for path in iterator:
            # Cheap bail-outs first — avoid a stat() for every dir entry
            try:
                if not path.is_file():
                    continue
            except OSError:
                continue

            if not self._gate.is_allowed(path):
                continue

            try:
                identity = self._resolver.resolve(path)
            except OSError:
                continue

            yield IngestEvent(
                event_type=EventType.created,
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
