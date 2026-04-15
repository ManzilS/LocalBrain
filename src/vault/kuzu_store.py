"""KuzuDB-backed graph store for the GraphRAG context.

Kuzu's Python driver is synchronous; every method here offloads the
blocking call via ``asyncio.to_thread`` so it plays nicely with the
orchestrator's event loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Any

import kuzu

logger = logging.getLogger(__name__)


def entity_id(name: str, entity_type: str = "Concept") -> str:
    """Deterministic entity key — case-insensitive name + type namespace.

    Using a hash (not a raw lowercased string) avoids Cypher-injection
    surprises and keeps the ID width bounded.
    """
    key = f"{entity_type.lower()}::{name.strip().lower()}"
    return "ent_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


class KuzuStore:
    """Async-friendly wrapper around a Kuzu graph database."""

    def __init__(self, db_path: str) -> None:
        self._path = db_path
        self._db: kuzu.Database | None = None
        self._conn: kuzu.Connection | None = None
        # Kuzu's connection isn't documented as thread-safe; serialise.
        self._lock = asyncio.Lock()

    # ── Lifecycle ──────────────────────────────────────────

    async def open(self) -> None:
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)

        def _connect() -> tuple[kuzu.Database, kuzu.Connection]:
            db = kuzu.Database(self._path)
            return db, kuzu.Connection(db)

        self._db, self._conn = await asyncio.to_thread(_connect)
        await self._ensure_schema()
        logger.info("Kuzu vault opened: %s", self._path)

    async def close(self) -> None:
        # Kuzu releases resources when references drop.
        self._conn = None
        self._db = None

    async def _execute(self, query: str, params: dict[str, Any] | None = None):
        """Run a Cypher query on the worker thread with a lock held."""
        assert self._conn is not None, "KuzuStore not opened"
        conn = self._conn
        async with self._lock:
            return await asyncio.to_thread(conn.execute, query, params or {})

    @staticmethod
    def _rows(result: Any) -> list[list[Any]]:
        """Drain a Kuzu QueryResult into a list of rows."""
        rows: list[list[Any]] = []
        while result.has_next():
            rows.append(result.get_next())
        return rows

    # ── Schema ─────────────────────────────────────────────

    async def _ensure_schema(self) -> None:
        statements = [
            "CREATE NODE TABLE IF NOT EXISTS Entity ("
            "  id STRING, name STRING, lbl_type STRING, description STRING,"
            "  PRIMARY KEY (id)"
            ")",
            "CREATE NODE TABLE IF NOT EXISTS Chunk ("
            "  id STRING, content STRING, PRIMARY KEY (id)"
            ")",
            "CREATE REL TABLE IF NOT EXISTS Relates_To ("
            "  FROM Entity TO Entity, description STRING, weight DOUBLE"
            ")",
            "CREATE REL TABLE IF NOT EXISTS Extracted_From ("
            "  FROM Entity TO Chunk"
            ")",
            "CREATE NODE TABLE IF NOT EXISTS CommunitySummary ("
            "  id STRING, summary STRING, size INT64, created_at DOUBLE,"
            "  PRIMARY KEY (id)"
            ")",
            "CREATE REL TABLE IF NOT EXISTS Summarises ("
            "  FROM CommunitySummary TO Entity"
            ")",
        ]
        for stmt in statements:
            try:
                await self._execute(stmt)
            except RuntimeError as exc:
                # Older Kuzu builds don't support IF NOT EXISTS — fall back.
                if "already exists" in str(exc).lower():
                    continue
                raise

    # ── Entities ───────────────────────────────────────────

    async def upsert_entity(
        self, entity_id: str, name: str, entity_type: str, description: str = ""
    ) -> None:
        await self._execute(
            "MERGE (e:Entity {id: $id}) "
            "ON CREATE SET e.name = $name, e.lbl_type = $type, e.description = $descr "
            "ON MATCH  SET e.name = $name, e.lbl_type = $type, e.description = $descr",
            {"id": entity_id, "name": name, "type": entity_type, "descr": description},
        )

    async def upsert_chunk(self, chunk_id: str, content: str) -> None:
        await self._execute(
            "MERGE (c:Chunk {id: $id}) "
            "ON CREATE SET c.content = $content "
            "ON MATCH  SET c.content = $content",
            {"id": chunk_id, "content": content},
        )

    async def add_relationship(
        self,
        source_id: str,
        target_id: str,
        description: str = "",
        weight: float = 1.0,
    ) -> None:
        await self._execute(
            "MATCH (a:Entity {id: $src}), (b:Entity {id: $dst}) "
            "MERGE (a)-[r:Relates_To]->(b) "
            "ON CREATE SET r.description = $descr, r.weight = $weight "
            "ON MATCH  SET r.description = $descr, r.weight = $weight",
            {"src": source_id, "dst": target_id, "descr": description, "weight": weight},
        )

    async def link_entity_to_chunk(self, entity_id: str, chunk_id: str) -> None:
        await self._execute(
            "MATCH (e:Entity {id: $eid}), (c:Chunk {id: $cid}) "
            "MERGE (e)-[:Extracted_From]->(c)",
            {"eid": entity_id, "cid": chunk_id},
        )

    # ── Deletion / cascade ─────────────────────────────────

    async def delete_chunks(self, chunk_ids: list[str]) -> None:
        """Delete chunk nodes; orphan entities are removed in a sweep."""
        if not chunk_ids:
            return
        await self._execute(
            "MATCH (c:Chunk) WHERE c.id IN $ids DETACH DELETE c", {"ids": chunk_ids}
        )
        # Sweep entities that no longer reference any chunk.
        await self._execute(
            "MATCH (e:Entity) "
            "WHERE NOT EXISTS { MATCH (e)-[:Extracted_From]->(:Chunk) } "
            "  AND e.lbl_type <> 'Summary' "
            "DETACH DELETE e"
        )

    # ── Reads ──────────────────────────────────────────────

    async def get_context_for_entity(
        self, entity_id: str, hop_limit: int = 2, limit: int = 50
    ) -> list[dict[str, Any]]:
        # Kuzu doesn't allow parameterising the variable-length lower/upper
        # bounds, so we inline validated ints.
        hop = max(1, min(int(hop_limit), 5))
        lim = max(1, min(int(limit), 200))
        result = await self._execute(
            f"MATCH (a:Entity {{id: $id}})-[:Relates_To*1..{hop}]-(b:Entity) "
            f"RETURN DISTINCT b.id, b.name, b.description LIMIT {lim}",
            {"id": entity_id},
        )
        return [
            {"id": r[0], "name": r[1], "description": r[2]} for r in self._rows(result)
        ]

    async def get_all_entities(self) -> list[dict[str, Any]]:
        result = await self._execute(
            "MATCH (e:Entity) RETURN e.id, e.name, e.lbl_type, e.description"
        )
        return [
            {"id": r[0], "name": r[1], "type": r[2], "description": r[3]}
            for r in self._rows(result)
        ]

    async def count_entities(self) -> int:
        result = await self._execute("MATCH (e:Entity) RETURN COUNT(e)")
        rows = self._rows(result)
        return int(rows[0][0]) if rows else 0

    async def get_relationships(self) -> list[tuple[str, str, float]]:
        """Return (source_id, target_id, weight) tuples for all edges."""
        result = await self._execute(
            "MATCH (a:Entity)-[r:Relates_To]->(b:Entity) RETURN a.id, b.id, r.weight"
        )
        return [(r[0], r[1], float(r[2] or 1.0)) for r in self._rows(result)]

    # ── Community summaries ────────────────────────────────

    async def upsert_community_summary(
        self, summary_id: str, summary: str, size: int, created_at: float
    ) -> None:
        await self._execute(
            "MERGE (s:CommunitySummary {id: $id}) "
            "ON CREATE SET s.summary = $summary, s.size = $size, s.created_at = $ts "
            "ON MATCH  SET s.summary = $summary, s.size = $size, s.created_at = $ts",
            {"id": summary_id, "summary": summary, "size": size, "ts": created_at},
        )

    async def link_summary_to_entity(self, summary_id: str, entity_id: str) -> None:
        await self._execute(
            "MATCH (s:CommunitySummary {id: $sid}), (e:Entity {id: $eid}) "
            "MERGE (s)-[:Summarises]->(e)",
            {"sid": summary_id, "eid": entity_id},
        )

    async def get_community_summaries(self, limit: int = 20) -> list[dict[str, Any]]:
        lim = max(1, min(int(limit), 100))
        result = await self._execute(
            f"MATCH (s:CommunitySummary) "
            f"RETURN s.id, s.summary, s.size, s.created_at "
            f"ORDER BY s.created_at DESC LIMIT {lim}"
        )
        return [
            {"id": r[0], "summary": r[1], "size": int(r[2] or 0), "created_at": r[3]}
            for r in self._rows(result)
        ]

    async def clear_community_summaries(self) -> None:
        await self._execute("MATCH (s:CommunitySummary) DETACH DELETE s")
