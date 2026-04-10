"""SQL DDL and migration helpers for the LocalBrain vault.

All tables use strict typing and explicit foreign keys.  The schema is
designed for crash safety — every write path uses transactions.
"""

from __future__ import annotations

import aiosqlite

# ── DDL statements ──────────────────────────────────────

_CREATE_FILES = """
CREATE TABLE IF NOT EXISTS files (
    id          TEXT PRIMARY KEY,
    path        TEXT NOT NULL,
    inode       INTEGER NOT NULL DEFAULT 0,
    device      INTEGER NOT NULL DEFAULT 0,
    mtime       REAL    NOT NULL DEFAULT 0,
    size        INTEGER NOT NULL DEFAULT 0,
    head_hash   TEXT    NOT NULL DEFAULT '',
    fingerprint TEXT    NOT NULL DEFAULT '',
    mime_type   TEXT    NOT NULL DEFAULT 'application/octet-stream',
    status      TEXT    NOT NULL DEFAULT 'pending',
    created_at  REAL    NOT NULL,
    updated_at  REAL    NOT NULL,
    deleted_at  REAL,
    metadata    TEXT    NOT NULL DEFAULT '{}'
);
"""

_CREATE_CHUNKS = """
CREATE TABLE IF NOT EXISTS chunks (
    id          TEXT PRIMARY KEY,
    content     TEXT    NOT NULL DEFAULT '',
    fingerprint TEXT    NOT NULL UNIQUE,
    byte_offset INTEGER NOT NULL DEFAULT 0,
    byte_length INTEGER NOT NULL DEFAULT 0,
    ref_count   INTEGER NOT NULL DEFAULT 0,
    metadata    TEXT    NOT NULL DEFAULT '{}'
);
"""

_CREATE_FILE_CHUNKS = """
CREATE TABLE IF NOT EXISTS file_chunks (
    file_id  TEXT    NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    chunk_id TEXT    NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (file_id, chunk_id)
);
"""

_CREATE_QUEUE = """
CREATE TABLE IF NOT EXISTS queue (
    id          TEXT PRIMARY KEY,
    file_id     TEXT    NOT NULL,
    lane        TEXT    NOT NULL DEFAULT 'fast',
    priority    INTEGER NOT NULL DEFAULT 0,
    payload     TEXT    NOT NULL DEFAULT '{}',
    created_at  REAL    NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    locked_until REAL   NOT NULL DEFAULT 0
);
"""

_CREATE_JOURNAL = """
CREATE TABLE IF NOT EXISTS journal (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    operation   TEXT    NOT NULL,
    entity_type TEXT    NOT NULL,
    entity_id   TEXT    NOT NULL,
    timestamp   REAL    NOT NULL,
    details     TEXT    NOT NULL DEFAULT '{}'
);
"""

# ── Indices ─────────────────────────────────────────────

_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_files_path       ON files(path);",
    "CREATE INDEX IF NOT EXISTS idx_files_status      ON files(status);",
    "CREATE INDEX IF NOT EXISTS idx_files_fingerprint ON files(fingerprint);",
    "CREATE INDEX IF NOT EXISTS idx_chunks_fp         ON chunks(fingerprint);",
    "CREATE INDEX IF NOT EXISTS idx_fc_file           ON file_chunks(file_id);",
    "CREATE INDEX IF NOT EXISTS idx_fc_chunk          ON file_chunks(chunk_id);",
    "CREATE INDEX IF NOT EXISTS idx_queue_lane        ON queue(lane, priority);",
    "CREATE INDEX IF NOT EXISTS idx_queue_locked      ON queue(locked_until);",
    "CREATE INDEX IF NOT EXISTS idx_journal_ts        ON journal(timestamp);",
]

_ALL_DDL = [_CREATE_FILES, _CREATE_CHUNKS, _CREATE_FILE_CHUNKS, _CREATE_QUEUE, _CREATE_JOURNAL]


# ── Public API ──────────────────────────────────────────


async def ensure_schema(db: aiosqlite.Connection) -> None:
    """Create all tables and indices if they do not exist."""
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA foreign_keys=ON;")

    for ddl in _ALL_DDL:
        await db.execute(ddl)
    for idx in _INDICES:
        await db.execute(idx)

    await db.commit()


async def migrate(db: aiosqlite.Connection) -> None:
    """Run forward migrations (placeholder for future schema changes)."""
    # Future migrations go here, guarded by version checks.
    pass
