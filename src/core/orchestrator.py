"""Top-level orchestrator — wires the watcher, scheduler, and pipeline.

Mirrors the Router project's ``Orchestrator`` class.  Manages the
lifecycle of the full ingestion flow: watcher → scheduler → pipeline,
plus janitor background tasks.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from src.chunking.dedup import ChunkDeduplicator
from src.core.pipeline import IngestPipeline
from src.core.registry import PluginRegistry
from src.core.scheduler import Scheduler
from src.ingress.identity import FileIdentityResolver
from src.ingress.scope_gate import ScopeGate
from src.ingress.watcher import FileWatcher
from src.janitor.reindex import ReindexManager
from src.janitor.sync import JournalSync
from src.janitor.tombstone import TombstoneCascade
from src.router_handoff.backpressure_queue import BackpressureQueue
from src.vault.lance_engine import LanceEngine
from src.vault.ref_counting import RefCounter
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.subscriptions import SubscriptionManager

if TYPE_CHECKING:
    from src.utils.config import Settings

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates all LocalBrain subsystems."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

        # ── Subsystems (initialised in .start()) ────────
        self.scope_gate: ScopeGate | None = None
        self.identity_resolver = FileIdentityResolver()
        self.registry = PluginRegistry()
        self.deduplicator = ChunkDeduplicator()
        self.scheduler = Scheduler()

        self.engine: SQLiteEngine | None = None
        self.lance: LanceEngine | None = None
        self.subscriptions: SubscriptionManager | None = None
        self.ref_counter: RefCounter | None = None
        self.queue: BackpressureQueue | None = None
        self.pipeline: IngestPipeline | None = None

        self.watcher: FileWatcher | None = None
        self.tombstone: TombstoneCascade | None = None
        self.journal_sync: JournalSync | None = None
        self.reindex_manager: ReindexManager | None = None

        self._tasks: list[asyncio.Task] = []

    # ── Lifecycle ───────────────────────────────────────

    async def start(self) -> None:
        """Boot all subsystems and start background tasks."""
        from pathlib import Path

        data_dir = Path(self.settings.data_dir).expanduser()
        data_dir.mkdir(parents=True, exist_ok=True)

        # Scope gate
        self.scope_gate = ScopeGate.from_file(self.settings.access_config)

        # Plugin registry
        self.registry.discover(self.settings.plugins_config)

        # Vault
        self.engine = SQLiteEngine(str(data_dir / "vault.db"))
        await self.engine.open()

        self.lance = LanceEngine(str(data_dir / "lance"))
        await self.lance.open()

        self.subscriptions = SubscriptionManager(self.engine.db)
        self.ref_counter = RefCounter(self.engine.db)

        # Load known chunk fingerprints for dedup
        known = await self.engine.get_all_chunk_fingerprints()
        self.deduplicator.register_many(known)

        # Backpressure queue
        self.queue = BackpressureQueue(
            str(data_dir / "queue.db"),
            max_depth=self.settings.backpressure_max,
        )
        await self.queue.open()

        # Pipeline
        self.pipeline = IngestPipeline(
            scope_gate=self.scope_gate,
            identity_resolver=self.identity_resolver,
            registry=self.registry,
            deduplicator=self.deduplicator,
            engine=self.engine,
            subscriptions=self.subscriptions,
            ref_counter=self.ref_counter,
            queue=self.queue,
            scheduler=self.scheduler,
        )

        # Scheduler
        self.scheduler.set_handler(self.pipeline.execute)
        await self.scheduler.start()

        # Watcher
        self.watcher = FileWatcher(
            scope_gate=self.scope_gate,
            identity_resolver=self.identity_resolver,
            settings=self.settings,
        )

        # Janitor
        self.tombstone = TombstoneCascade(
            self.engine, self.lance, self.subscriptions, self.ref_counter
        )
        self.journal_sync = JournalSync(self.engine)
        self.reindex_manager = ReindexManager(
            self.engine,
            self.subscriptions,
            threshold=self.settings.janitor_reindex_threshold,
        )

        # Background tasks
        self._tasks.append(asyncio.create_task(self._watch_loop(), name="watcher"))
        self._tasks.append(asyncio.create_task(self._janitor_loop(), name="janitor"))

        logger.info("Orchestrator started — watching %d roots", len(self.scope_gate.get_watch_roots()))

    async def stop(self) -> None:
        """Gracefully shut down everything."""
        if self.watcher:
            self.watcher.stop()

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        await self.scheduler.stop()

        if self.queue:
            await self.queue.close()
        if self.lance:
            await self.lance.close()
        if self.engine:
            await self.engine.close()

        logger.info("Orchestrator stopped")

    # ── Background loops ────────────────────────────────

    async def _watch_loop(self) -> None:
        """Feed watcher events into the scheduler."""
        assert self.watcher is not None
        try:
            async for event in self.watcher.watch():
                await self.scheduler.enqueue(event)
        except asyncio.CancelledError:
            pass

    async def _janitor_loop(self) -> None:
        """Periodic maintenance: sync, purge, re-index."""
        assert self.tombstone is not None
        assert self.journal_sync is not None
        assert self.reindex_manager is not None

        interval = self.settings.janitor_interval_s
        try:
            while True:
                await asyncio.sleep(interval)

                # 1. Journal sync
                corrective = await self.journal_sync.sync()
                for event in corrective:
                    await self.scheduler.enqueue(event)

                # 2. Tombstone purge
                await self.tombstone.purge(self.settings.janitor_purge_days)

                # 3. Lazy re-index (only if idle + AC)
                if self.reindex_manager.can_reindex():
                    pending = await self.reindex_manager.get_pending()
                    for file_id in pending[:5]:  # Process in small batches
                        logger.info("Re-indexing file %s", file_id)
                        await self.reindex_manager.mark_done(file_id)

        except asyncio.CancelledError:
            pass
