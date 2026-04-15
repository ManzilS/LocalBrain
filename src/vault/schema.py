"""SQL DDL and migration helpers for the LocalBrain vault.

All tables use strict typing and explicit foreign keys.  The schema is
designed for crash safety — every write path uses transactions.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

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

# ── Full-text search over chunk content ─────────────────
#
# Uses FTS5 in "external content" mode so the FTS index shares storage
# with the chunks table via rowid. Triggers keep the index synchronised
# on INSERT / UPDATE / DELETE of the chunks table.

_CREATE_CHUNKS_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='chunks',
    content_rowid='rowid',
    tokenize='porter unicode61'
);
"""

_FTS_TRIGGERS = [
    """CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
         INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
       END;""",
    """CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
         INSERT INTO chunks_fts(chunks_fts, rowid, content)
         VALUES ('delete', old.rowid, old.content);
       END;""",
    """CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
         INSERT INTO chunks_fts(chunks_fts, rowid, content)
         VALUES ('delete', old.rowid, old.content);
         INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
       END;""",
]

_ALL_DDL = [_CREATE_FILES, _CREATE_CHUNKS, _CREATE_FILE_CHUNKS, _CREATE_QUEUE, _CREATE_JOURNAL]


# ── Public API ──────────────────────────────────────────


async def ensure_schema(db: aiosqlite.Connection) -> None:
    """Create all tables, indices, and FTS structures if they do not exist."""
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA foreign_keys=ON;")

    for ddl in _ALL_DDL:
        await db.execute(ddl)
    for idx in _INDICES:
        await db.execute(idx)

    # FTS virtual table + triggers must be created AFTER the `chunks`
    # table because they reference it.
    await db.execute(_CREATE_CHUNKS_FTS)
    for trig in _FTS_TRIGGERS:
        await db.execute(trig)

    await _backfill_chunks_fts(db)

    await db.commit()


async def _backfill_chunks_fts(db: aiosqlite.Connection) -> None:
    """Populate the FTS index if upgrading an existing vault.

    When a vault created by an older build is opened for the first time
    with this schema, ``chunks`` may already have rows but ``chunks_fts``
    will be empty. The triggers only fire on future writes, so copy
    existing content over once.
    """
    async with db.execute("SELECT COUNT(*) FROM chunks") as cur:
        chunks_count = (await cur.fetchone())[0]
    if chunks_count == 0:
        return

    # External-content FTS5: ``SELECT COUNT(*) FROM chunks_fts`` proxies
    # back to the chunks table, so it always equals ``chunks_count`` and
    # can't tell us whether the index itself is populated. The shadow
    # ``chunks_fts_docsize`` table has one row per *indexed* document.
    async with db.execute("SELECT COUNT(*) FROM chunks_fts_docsize") as cur:
        fts_count = (await cur.fetchone())[0]
    if fts_count >= chunks_count:
        return

    logger.info(
        "Backfilling %d chunk(s) into FTS index (had %d)",
        chunks_count,
        fts_count,
    )
    # External-content FTS5 must be populated via the 'rebuild' command —
    # a plain INSERT only writes the shadow content row, not the index.
    await db.execute("INSERT INTO chunks_fts(chunks_fts) VALUES ('rebuild')")


async def migrate(db: aiosqlite.Connection) -> None:
    """Run forward migrations (placeholder for future schema changes)."""
    # Future migrations go here, guarded by version checks.
    pass
