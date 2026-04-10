"""SQLite state engine — high-frequency reads/writes for the vault.

Handles file records, chunk metadata, the persistent queue, and the
change journal.  All writes are wrapped in explicit transactions for
crash safety.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import aiosqlite

from src.core.models import (
    Chunk,
    FileIdentity,
    FileRecord,
    FileStatus,
    QueueItem,
    QueueLane,
)
from src.vault.schema import ensure_schema

logger = logging.getLogger(__name__)


class SQLiteEngine:
    """Async SQLite connection wrapper for the LocalBrain vault."""

    def __init__(self, db_path: str) -> None:
        self._path = db_path
        self._db: aiosqlite.Connection | None = None

    # ── Lifecycle ───────────────────────────────────────

    async def open(self) -> None:
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await ensure_schema(self._db)
        logger.info("SQLite vault opened: %s", self._path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "SQLiteEngine not opened"
        return self._db

    # ── File CRUD ───────────────────────────────────────

    async def upsert_file(self, record: FileRecord) -> None:
        await self.upsert_file_no_commit(record)
        await self.db.commit()

    async def upsert_file_no_commit(self, record: FileRecord) -> None:
        """Insert/update a file record without committing (for transactions)."""
        now = time.time()
        await self.db.execute(
            """INSERT INTO files
               (id, path, inode, device, mtime, size, head_hash, fingerprint,
                mime_type, status, created_at, updated_at, deleted_at, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 path=excluded.path, inode=excluded.inode, device=excluded.device,
                 mtime=excluded.mtime, size=excluded.size, head_hash=excluded.head_hash,
                 fingerprint=excluded.fingerprint, mime_type=excluded.mime_type,
                 status=excluded.status, updated_at=?, deleted_at=excluded.deleted_at,
                 metadata=excluded.metadata
            """,
            (
                record.id,
                record.identity.path,
                record.identity.inode,
                record.identity.device,
                record.identity.mtime,
                record.identity.size,
                record.identity.head_hash,
                record.fingerprint,
                record.mime_type,
                record.status.value,
                record.created_at,
                now,
                record.deleted_at,
                json.dumps(record.metadata),
                now,
            ),
        )

    async def get_file_by_id(self, file_id: str) -> FileRecord | None:
        async with self.db.execute("SELECT * FROM files WHERE id = ?", (file_id,)) as cur:
            row = await cur.fetchone()
            return self._row_to_file(row) if row else None

    async def get_file_by_path(self, path: str) -> FileRecord | None:
        async with self.db.execute("SELECT * FROM files WHERE path = ?", (path,)) as cur:
            row = await cur.fetchone()
            return self._row_to_file(row) if row else None

    async def get_file_by_identity(self, identity: FileIdentity) -> FileRecord | None:
        async with self.db.execute(
            "SELECT * FROM files WHERE inode = ? AND device = ? AND path = ?",
            (identity.inode, identity.device, identity.path),
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_file(row) if row else None

    async def list_files(
        self, *, status: FileStatus | None = None, limit: int = 100, offset: int = 0
    ) -> list[FileRecord]:
        if status:
            sql = "SELECT * FROM files WHERE status = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?"
            params: tuple = (status.value, limit, offset)
        else:
            sql = "SELECT * FROM files WHERE status != 'tombstone' ORDER BY updated_at DESC LIMIT ? OFFSET ?"
            params = (limit, offset)

        async with self.db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [self._row_to_file(r) for r in rows]

    async def mark_tombstone(self, file_id: str) -> None:
        now = time.time()
        await self.db.execute(
            "UPDATE files SET status = 'tombstone', deleted_at = ?, updated_at = ? WHERE id = ?",
            (now, now, file_id),
        )
        await self.db.commit()

    async def purge_tombstones(self, older_than_days: int = 7) -> int:
        cutoff = time.time() - (older_than_days * 86400)
        async with self.db.execute(
            "DELETE FROM files WHERE status = 'tombstone' AND deleted_at < ?", (cutoff,)
        ) as cur:
            count = cur.rowcount
        await self.db.commit()
        return count or 0

    # ── Chunk CRUD ──────────────────────────────────────

    async def upsert_chunk(self, chunk: Chunk) -> None:
        await self.upsert_chunk_no_commit(chunk)
        await self.db.commit()

    async def upsert_chunk_no_commit(self, chunk: Chunk) -> None:
        """Insert/update a chunk without committing (for transactions)."""
        await self.db.execute(
            """INSERT INTO chunks (id, content, fingerprint, byte_offset, byte_length, metadata)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(fingerprint) DO UPDATE SET
                 content=excluded.content, byte_offset=excluded.byte_offset,
                 byte_length=excluded.byte_length, metadata=excluded.metadata
            """,
            (
                chunk.id,
                chunk.content,
                chunk.fingerprint,
                chunk.byte_offset,
                chunk.byte_length,
                json.dumps(chunk.metadata),
            ),
        )

    async def get_chunk_by_fingerprint(self, fingerprint: str) -> Chunk | None:
        async with self.db.execute(
            "SELECT * FROM chunks WHERE fingerprint = ?", (fingerprint,)
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_chunk(row) if row else None

    async def get_chunks_for_file(self, file_id: str) -> list[Chunk]:
        async with self.db.execute(
            """SELECT c.* FROM chunks c
               JOIN file_chunks fc ON c.id = fc.chunk_id
               WHERE fc.file_id = ?
               ORDER BY fc.sequence""",
            (file_id,),
        ) as cur:
            rows = await cur.fetchall()
            return [self._row_to_chunk(r) for r in rows]

    async def get_all_chunk_fingerprints(self) -> list[str]:
        async with self.db.execute("SELECT fingerprint FROM chunks") as cur:
            rows = await cur.fetchall()
            return [r["fingerprint"] for r in rows]

    # ── Queue ───────────────────────────────────────────

    async def enqueue(self, item: QueueItem) -> None:
        await self.db.execute(
            """INSERT INTO queue (id, file_id, lane, priority, payload, created_at, attempts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                item.id,
                item.file_id,
                item.lane.value,
                item.priority,
                json.dumps(item.payload),
                item.created_at,
                item.attempts,
            ),
        )
        await self.db.commit()

    async def dequeue(self, lane: QueueLane, batch_size: int = 1) -> list[QueueItem]:
        now = time.time()
        lock_until = now + 300  # 5-minute lock
        async with self.db.execute(
            """SELECT * FROM queue
               WHERE lane = ? AND locked_until < ?
               ORDER BY priority DESC, created_at ASC
               LIMIT ?""",
            (lane.value, now, batch_size),
        ) as cur:
            rows = await cur.fetchall()

        items = []
        for row in rows:
            await self.db.execute(
                "UPDATE queue SET locked_until = ?, attempts = attempts + 1 WHERE id = ?",
                (lock_until, row["id"]),
            )
            items.append(
                QueueItem(
                    id=row["id"],
                    file_id=row["file_id"],
                    lane=QueueLane(row["lane"]),
                    priority=row["priority"],
                    payload=json.loads(row["payload"]),
                    created_at=row["created_at"],
                    attempts=row["attempts"] + 1,
                )
            )
        await self.db.commit()
        return items

    async def ack(self, item_id: str) -> None:
        await self.db.execute("DELETE FROM queue WHERE id = ?", (item_id,))
        await self.db.commit()

    async def nack(self, item_id: str, retry_after: float = 0) -> None:
        await self.db.execute(
            "UPDATE queue SET locked_until = ? WHERE id = ?",
            (time.time() + retry_after, item_id),
        )
        await self.db.commit()

    async def get_queue_depth(self, lane: QueueLane | None = None) -> int:
        if lane:
            sql = "SELECT COUNT(*) as cnt FROM queue WHERE lane = ?"
            params: tuple = (lane.value,)
        else:
            sql = "SELECT COUNT(*) as cnt FROM queue"
            params = ()
        async with self.db.execute(sql, params) as cur:
            row = await cur.fetchone()
            return row["cnt"] if row else 0

    # ── Journal ─────────────────────────────────────────

    async def log_journal(
        self, operation: str, entity_type: str, entity_id: str, details: dict | None = None
    ) -> None:
        await self.db.execute(
            "INSERT INTO journal (operation, entity_type, entity_id, timestamp, details) VALUES (?, ?, ?, ?, ?)",
            (operation, entity_type, entity_id, time.time(), json.dumps(details or {})),
        )
        await self.db.commit()

    async def get_journal_since(self, timestamp: float) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM journal WHERE timestamp > ? ORDER BY timestamp ASC", (timestamp,)
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # ── Row Mappers ─────────────────────────────────────

    @staticmethod
    def _row_to_file(row: aiosqlite.Row) -> FileRecord:
        return FileRecord(
            id=row["id"],
            identity=FileIdentity(
                path=row["path"],
                inode=row["inode"],
                device=row["device"],
                mtime=row["mtime"],
                size=row["size"],
                head_hash=row["head_hash"],
            ),
            fingerprint=row["fingerprint"],
            mime_type=row["mime_type"],
            status=FileStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            deleted_at=row["deleted_at"],
            metadata=json.loads(row["metadata"]),
        )

    @staticmethod
    def _row_to_chunk(row: aiosqlite.Row) -> Chunk:
        return Chunk(
            id=row["id"],
            content=row["content"],
            fingerprint=row["fingerprint"],
            byte_offset=row["byte_offset"],
            byte_length=row["byte_length"],
            metadata=json.loads(row["metadata"]),
        )
