"""LanceDB vector engine — immutable embedding storage and similarity search.

LanceDB handles the vector-heavy operations while SQLite manages
relational state.  This separation keeps ACID concerns clean.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import lancedb
import pyarrow as pa

from src.core.models import Chunk

logger = logging.getLogger(__name__)

_TABLE_NAME = "chunks"
_DEFAULT_DIM = 384

# Strict ID pattern — only hex UUIDs allowed
_SAFE_ID = re.compile(r"^[a-f0-9]+$")


def _make_schema(dim: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("file_id", pa.string()),
            pa.field("content", pa.string()),
            pa.field("fingerprint", pa.string()),
            pa.field("embedding", pa.list_(pa.float32(), list_size=dim)),
            pa.field("metadata", pa.string()),
        ]
    )


def _validate_id(value: str) -> str:
    """Validate that an ID is safe for use in LanceDB filter expressions."""
    if not _SAFE_ID.match(value):
        raise ValueError(f"Unsafe ID value for LanceDB filter: {value!r}")
    return value


class LanceEngine:
    """Async-friendly wrapper around LanceDB for vector storage."""

    def __init__(self, db_path: str, *, embedding_dim: int = _DEFAULT_DIM) -> None:
        self._path = db_path
        self._dim = embedding_dim
        self._db: Any = None
        self._table: Any = None

    # ── Lifecycle ───────────────────────────────────────

    async def open(self) -> None:
        Path(self._path).mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(self._path)

        if _TABLE_NAME in self._db.list_tables():
            self._table = self._db.open_table(_TABLE_NAME)
            # Validate existing schema dimension matches
            existing_schema = self._table.schema
            for field in existing_schema:
                if field.name == "embedding" and pa.types.is_fixed_size_list(field.type):
                    existing_dim = field.type.list_size
                    if existing_dim != self._dim:
                        logger.warning(
                            "Embedding dimension mismatch: table has %d, expected %d. "
                            "Re-creating table. Old embeddings will need re-indexing.",
                            existing_dim,
                            self._dim,
                        )
                        self._db.drop_table(_TABLE_NAME)
                        self._table = self._db.create_table(
                            _TABLE_NAME, schema=_make_schema(self._dim)
                        )
                        break
        else:
            try:
                self._table = self._db.create_table(
                    _TABLE_NAME, schema=_make_schema(self._dim)
                )
            except (ValueError, OSError) as exc:
                # A prior process may have crashed mid-init leaving an orphan
                # table directory that list_tables() does not surface. Recover
                # by dropping and re-creating.
                if "already exists" not in str(exc):
                    raise
                logger.warning(
                    "Orphan LanceDB table '%s' detected; dropping and recreating.",
                    _TABLE_NAME,
                )
                self._db.drop_table(_TABLE_NAME, ignore_missing=True)
                self._table = self._db.create_table(
                    _TABLE_NAME, schema=_make_schema(self._dim)
                )

        logger.info("LanceDB opened: %s (dim=%d)", self._path, self._dim)

    async def close(self) -> None:
        self._db = None
        self._table = None

    # ── Writes ──────────────────────────────────────────

    async def upsert_embeddings(self, chunks: list[Chunk]) -> int:
        """Batch insert/update chunks with embeddings. Returns count written."""
        if not chunks:
            return 0

        import json

        rows = []
        for c in chunks:
            if c.embedding is None:
                continue
            if len(c.embedding) != self._dim:
                logger.warning(
                    "Skipping chunk %s: embedding dim %d != expected %d",
                    c.id, len(c.embedding), self._dim,
                )
                continue
            rows.append(
                {
                    "id": c.id,
                    "file_id": c.file_id,
                    "content": c.content,
                    "fingerprint": c.fingerprint,
                    "embedding": c.embedding,
                    "metadata": json.dumps(c.metadata),
                }
            )

        if not rows:
            return 0

        self._table.add(rows)
        logger.debug("Upserted %d embeddings", len(rows))
        return len(rows)

    async def delete_by_chunk_ids(self, ids: list[str]) -> None:
        """Remove rows by chunk ID."""
        if not ids:
            return
        # Validate all IDs to prevent injection
        safe_ids = [_validate_id(i) for i in ids]
        id_list = ", ".join(f"'{i}'" for i in safe_ids)
        self._table.delete(f"id IN ({id_list})")
        logger.debug("Deleted %d chunks from LanceDB", len(ids))

    async def delete_by_file_id(self, file_id: str) -> None:
        """Remove all chunks belonging to a file."""
        safe_id = _validate_id(file_id)
        self._table.delete(f"file_id = '{safe_id}'")

    # ── Search ──────────────────────────────────────────

    async def search(
        self,
        query_embedding: list[float],
        *,
        limit: int = 10,
        file_id: str | None = None,
    ) -> list[dict]:
        """Vector similarity search.  Returns dicts with id, content, score."""
        q = self._table.search(query_embedding).limit(limit)

        if file_id:
            safe_id = _validate_id(file_id)
            q = q.where(f"file_id = '{safe_id}'")

        results = q.to_list()
        return [
            {
                "id": r["id"],
                "file_id": r["file_id"],
                "content": r["content"],
                "fingerprint": r["fingerprint"],
                "score": r.get("_distance", 0.0),
            }
            for r in results
        ]

    # ── Stats ───────────────────────────────────────────

    async def count(self) -> int:
        return self._table.count_rows()
