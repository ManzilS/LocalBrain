"""Five-phase ingestion pipeline engine.

Each file event flows through:

  1. Ingress  — scope gate, identity, file-level dedup
  2. Parse    — select parser by format, extract text
  3. Chunk    — CDC splitting, chunk-level dedup
  4. Handoff  — queue chunks for Router AI processing
  5. Store    — persist to vault (SQLite + LanceDB)

Mirrors the Router project's ``Pipeline`` class — a single ``execute``
method that runs phases sequentially, updating ``IngestState`` at each
step.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import stat as stat_module
from pathlib import Path
from typing import TYPE_CHECKING

from src.chunking.cdc import cdc_chunk
from src.chunking.dedup import ChunkDeduplicator
from src.chunking.fingerprint import (
    chunk_fingerprint,
    file_fingerprint,
    partial_fingerprint,
)
from src.core.models import (
    Chunk,
    EventType,
    FileIdentity,
    FileRecord,
    FileStatus,
    HandoffRequest,
    IngestEvent,
)
from src.core.state import IngestState
from src.utils.errors import FileAccessError, IngestError

if TYPE_CHECKING:
    from src.core.registry import PluginRegistry
    from src.core.scheduler import Scheduler
    from src.ingress.identity import FileIdentityResolver
    from src.ingress.scope_gate import ScopeGate
    from src.router_handoff.backpressure_queue import BackpressureQueue
    from src.vault.ref_counting import RefCounter
    from src.vault.sqlite_engine import SQLiteEngine
    from src.vault.subscriptions import SubscriptionManager

logger = logging.getLogger(__name__)

# Files above this size use partial fingerprint instead of full read
_LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10 MB


class IngestPipeline:
    """Executes the five-phase ingestion flow for a single file event."""

    def __init__(
        self,
        scope_gate: ScopeGate,
        identity_resolver: FileIdentityResolver,
        registry: PluginRegistry,
        deduplicator: ChunkDeduplicator,
        engine: SQLiteEngine,
        subscriptions: SubscriptionManager,
        ref_counter: RefCounter,
        queue: BackpressureQueue,
        scheduler: Scheduler | None = None,
    ) -> None:
        self._gate = scope_gate
        self._resolver = identity_resolver
        self._registry = registry
        self._dedup = deduplicator
        self._engine = engine
        self._subs = subscriptions
        self._refs = ref_counter
        self._queue = queue
        self._scheduler = scheduler

    async def execute(self, state: IngestState) -> IngestState:
        """Run all phases.  Returns the updated state."""
        try:
            state = await self._phase_ingress(state)
            if state.early_exit:
                return state

            state = await self._phase_parse(state)
            if state.early_exit:
                return state

            state = await self._phase_chunk(state)
            if state.early_exit:
                return state

            state = await self._phase_handoff(state)
            state = await self._phase_store(state)

        except Exception as exc:
            state.error = str(exc)
            state.phase = "error"
            logger.exception("Pipeline failed for %s", state.event.file_identity.path)

        return state

    # ── Phase 1: Ingress ────────────────────────────────

    async def _phase_ingress(self, state: IngestState) -> IngestState:
        state.phase = "ingress"
        event = state.event
        path = event.file_identity.path

        # Handle deletions immediately
        if event.event_type == EventType.deleted:
            existing = await self._engine.get_file_by_path(path)
            if existing:
                await self._engine.mark_tombstone(existing.id)
                await self._engine.log_journal("DELETE", "file", existing.id)
            state.early_exit = True
            return state

        # Validate the path is a regular file (not a pipe, device, symlink to device)
        try:
            p = Path(path)
            file_stat = await asyncio.to_thread(p.stat)
        except OSError as exc:
            raise FileAccessError(f"Cannot stat file: {path}") from exc

        if not stat_module.S_ISREG(file_stat.st_mode):
            raise FileAccessError(f"Not a regular file: {path}")

        # Scope gate — check with actual file size from stat
        self._gate.enforce(path, size=file_stat.st_size)

        # Resolve full identity
        identity = await asyncio.to_thread(self._resolver.resolve, path)

        # Check for existing file
        existing = await self._engine.get_file_by_path(path)
        if existing:
            if not self._resolver.has_changed(existing.identity, identity):
                logger.debug("File unchanged, skipping: %s", path)
                state.early_exit = True
                return state
            state.file_record = existing
            state.file_record.identity = identity
            state.file_record.status = FileStatus.parsing
        else:
            mime = await asyncio.to_thread(self._detect_mime, path)
            state.file_record = FileRecord(
                identity=identity,
                mime_type=mime,
                status=FileStatus.parsing,
            )

        state.event.file_identity = identity
        return state

    # ── Phase 2: Parse ──────────────────────────────────

    async def _phase_parse(self, state: IngestState) -> IngestState:
        state.phase = "parse"
        assert state.file_record is not None

        path = Path(state.file_record.identity.path)

        # Non-blocking file read
        try:
            raw_bytes = await asyncio.to_thread(path.read_bytes)
        except OSError as exc:
            state.file_record.status = FileStatus.error
            state.error = f"Failed to read file: {exc}"
            state.early_exit = True
            return state

        # Re-check size after reading (file may have grown since ingress)
        if not self._gate.check_size(len(raw_bytes)):
            state.file_record.status = FileStatus.error
            state.error = f"File exceeds size limit after read: {len(raw_bytes)} bytes"
            state.early_exit = True
            return state

        # File-level fingerprint — use partial for large files
        if len(raw_bytes) > _LARGE_FILE_THRESHOLD:
            state.file_record.fingerprint = partial_fingerprint(raw_bytes)
        else:
            state.file_record.fingerprint = file_fingerprint(raw_bytes)

        # Find a parser
        parser = await self._registry.find_parser(state.file_record)
        if parser is None:
            logger.warning("No parser for %s (%s)", path, state.file_record.mime_type)
            state.file_record.status = FileStatus.error
            state.error = f"No parser for {state.file_record.mime_type}"
            state.early_exit = True
            return state

        result = await parser.parse(state.file_record, raw_bytes)
        state.raw_content = result.content
        state.file_record.metadata.update(result.metadata)

        # Handle archive sub-files — enqueue each back into the pipeline
        if result.sub_files and self._scheduler:
            await self._enqueue_sub_files(state, result.sub_files)

        return state

    # ── Phase 3: Chunk ──────────────────────────────────

    async def _phase_chunk(self, state: IngestState) -> IngestState:
        state.phase = "chunk"
        assert state.file_record is not None

        if not state.raw_content:
            state.file_record.status = FileStatus.chunked
            return state

        boundaries = cdc_chunk(state.raw_content)
        new_chunks: list[Chunk] = []

        for seq, boundary in enumerate(boundaries):
            text = state.raw_content[boundary.offset : boundary.offset + boundary.length]
            fp = chunk_fingerprint(text)

            if self._dedup.is_duplicate(fp):
                # Map to existing chunk instead of re-processing
                existing = await self._engine.get_chunk_by_fingerprint(fp)
                if existing:
                    new_chunks.append(existing)
                    continue

            chunk = Chunk(
                file_id=state.file_record.id,
                sequence=seq,
                content=text,
                fingerprint=fp,
                byte_offset=boundary.offset,
                byte_length=boundary.length,
            )
            new_chunks.append(chunk)
            self._dedup.register(fp)

        state.chunks = new_chunks
        state.file_record.status = FileStatus.chunked
        return state

    # ── Phase 4: Handoff ────────────────────────────────

    async def _phase_handoff(self, state: IngestState) -> IngestState:
        state.phase = "handoff"

        # Only queue chunks that need AI processing (no embedding yet)
        unprocessed = [c for c in state.chunks if c.embedding is None]
        if unprocessed and not await self._queue.is_full():
            request = HandoffRequest(
                chunks=unprocessed,
                file_record=state.file_record,
                action="embed",
            )
            item_id = await self._queue.enqueue(request)
            state.extras["handoff_item_id"] = item_id
            logger.debug("Queued %d chunks for embedding", len(unprocessed))
        elif await self._queue.is_full():
            logger.warning("Backpressure queue full — embedding deferred")

        return state

    # ── Phase 5: Store (atomic transaction) ─────────────

    async def _phase_store(self, state: IngestState) -> IngestState:
        state.phase = "store"
        assert state.file_record is not None

        db = self._engine.db

        # Wrap entire store phase in a single transaction
        await db.execute("BEGIN IMMEDIATE")
        try:
            # Persist chunks
            chunk_ids: list[str] = []
            for chunk in state.chunks:
                await self._engine.upsert_chunk_no_commit(chunk)
                chunk_ids.append(chunk.id)

            # Subscribe file to chunks
            await self._subs.subscribe_no_commit(state.file_record.id, chunk_ids)

            # Increment ref counts
            for cid in chunk_ids:
                await db.execute(
                    "UPDATE chunks SET ref_count = ref_count + 1 WHERE id = ?", (cid,)
                )

            # Persist file record
            state.file_record.status = FileStatus.indexed
            await self._engine.upsert_file_no_commit(state.file_record)

            # Journal entry
            import json
            import time

            await db.execute(
                "INSERT INTO journal (operation, entity_type, entity_id, timestamp, details) VALUES (?, ?, ?, ?, ?)",
                ("UPSERT", "file", state.file_record.id, time.time(), json.dumps({})),
            )

            await db.commit()
        except Exception:
            await db.rollback()
            state.file_record.status = FileStatus.error
            raise

        logger.info(
            "Stored %s — %d chunks", state.file_record.identity.path, len(state.chunks)
        )
        return state

    # ── Sub-file re-ingestion ───────────────────────────

    async def _enqueue_sub_files(self, state: IngestState, sub_files: list[bytes]) -> None:
        """Write archive sub-files to temp dir and enqueue them for ingestion."""
        import tempfile

        assert state.file_record is not None
        parent = Path(state.file_record.identity.path)
        archive_name = parent.stem

        # Parse the manifest from raw_content to get original filenames
        manifest_lines = state.raw_content.strip().split("\n") if state.raw_content else []

        tmp_dir = Path(tempfile.mkdtemp(prefix="localbrain_archive_"))
        enqueued = 0

        for i, raw in enumerate(sub_files):
            # Use original filename from manifest if available
            if i < len(manifest_lines) and manifest_lines[i]:
                name = Path(manifest_lines[i]).name
            else:
                name = f"sub_file_{i}"

            sub_path = tmp_dir / archive_name / name
            sub_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                await asyncio.to_thread(sub_path.write_bytes, raw)
            except OSError:
                logger.warning("Failed to write sub-file: %s", sub_path)
                continue

            event = IngestEvent(
                event_type=EventType.created,
                file_identity=FileIdentity(path=str(sub_path)),
                metadata={"source_archive": state.file_record.identity.path},
            )
            await self._scheduler.enqueue(event)
            enqueued += 1

        if enqueued:
            logger.info("Enqueued %d sub-files from %s", enqueued, parent.name)
            state.extras["sub_file_count"] = enqueued

    # ── MIME detection ──────────────────────────────────

    @staticmethod
    def _detect_mime(path: str) -> str:
        """Detect MIME type — extension-based with magic-byte fallback."""
        mime, _ = mimetypes.guess_type(path)
        if mime:
            return mime

        # Magic-byte fallback for common formats
        try:
            with open(path, "rb") as fh:
                header = fh.read(16)
        except OSError:
            return "application/octet-stream"

        if header[:4] == b"%PDF":
            return "application/pdf"
        if header[:2] == b"PK":
            return "application/zip"
        if header[:4] == b"\x89PNG":
            return "image/png"
        if header[:3] == b"\xff\xd8\xff":
            return "image/jpeg"
        if header[:4] == b"GIF8":
            return "image/gif"
        if header[:4] in (b"RIFF",) and len(header) >= 12 and header[8:12] == b"WAVE":
            return "audio/wav"
        if header[:3] == b"ID3" or header[:2] == b"\xff\xfb":
            return "audio/mpeg"

        return "application/octet-stream"
