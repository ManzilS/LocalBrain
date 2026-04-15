"""FastAPI route handlers for the LocalBrain API.

Provides endpoints for health checks, manual ingestion triggers, file
listing, semantic search, and janitor operations.
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from src.core.models import EventType, FileIdentity, FileStatus, IngestEvent
from src.utils.errors import LocalBrainError, RequestValidationError

router = APIRouter()


# ── Root / Health ───────────────────────────────────────


@router.get("/")
async def root() -> dict[str, str]:
    return {"service": "localbrain", "status": "running"}


@router.get("/health")
async def health(request: Request) -> dict[str, Any]:
    orch = request.app.state.orchestrator
    depths = await orch.scheduler.get_depths()
    queue_depth = await orch.queue.depth() if orch.queue else 0

    return {
        "status": "healthy",
        "watch_roots": [str(r) for r in (orch.scope_gate.get_watch_roots() if orch.scope_gate else [])],
        "queue_depths": depths,
        "handoff_queue_depth": queue_depth,
        "known_chunks": orch.deduplicator.count,
        "parsers": [p.name for p in orch.registry.parsers],
    }


# ── Ingestion ───────────────────────────────────────────


@router.post("/v1/ingest")
async def ingest_file(request: Request) -> dict[str, Any]:
    """Manually trigger ingestion of a file path."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        raise RequestValidationError("Missing 'path' in request body")

    from pathlib import Path

    p = Path(path).expanduser().resolve()
    if not p.exists():
        from src.utils.errors import FileNotFoundError

        raise FileNotFoundError(f"File not found: {p}")

    orch = request.app.state.orchestrator

    # Enforce scope gate before accepting the file
    if orch.scope_gate:
        try:
            orch.scope_gate.enforce(str(p))
        except Exception as exc:
            raise RequestValidationError(str(exc))

    event = IngestEvent(
        event_type=EventType.created,
        file_identity=FileIdentity(path=str(p)),
    )
    await orch.scheduler.enqueue(event)

    return {"status": "queued", "path": str(p), "event_id": event.id}


# ── Files ───────────────────────────────────────────────


@router.get("/v1/files")
async def list_files(
    request: Request,
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    orch = request.app.state.orchestrator
    try:
        file_status = FileStatus(status) if status else None
    except ValueError:
        raise RequestValidationError(f"Invalid status filter: {status!r}")
    files = await orch.engine.list_files(status=file_status, limit=limit, offset=offset)
    return {
        "files": [
            {
                "id": f.id,
                "path": f.identity.path,
                "mime_type": f.mime_type,
                "status": f.status.value,
                "size": f.identity.size,
                "updated_at": f.updated_at,
            }
            for f in files
        ],
        "count": len(files),
    }


@router.get("/v1/files/{file_id}")
async def get_file(request: Request, file_id: str) -> dict[str, Any]:
    orch = request.app.state.orchestrator
    record = await orch.engine.get_file_by_id(file_id)
    if record is None:
        from src.utils.errors import FileNotFoundError

        raise FileNotFoundError(f"File not found: {file_id}")

    chunks = await orch.engine.get_chunks_for_file(file_id)
    return {
        "file": {
            "id": record.id,
            "path": record.identity.path,
            "mime_type": record.mime_type,
            "status": record.status.value,
            "fingerprint": record.fingerprint,
            "size": record.identity.size,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "metadata": record.metadata,
        },
        "chunks": [
            {
                "id": c.id,
                "sequence": c.sequence,
                "fingerprint": c.fingerprint,
                "byte_offset": c.byte_offset,
                "byte_length": c.byte_length,
                "content_preview": c.content[:200] if c.content else "",
            }
            for c in chunks
        ],
    }


# ── Search ──────────────────────────────────────────────


@router.get("/v1/search")
async def search(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    mode: str = Query("keyword", pattern="^(keyword|semantic|hybrid)$"),
) -> dict[str, Any]:
    """Search ingested chunks.

    ``mode=keyword`` uses FTS5 full-text search.
    ``mode=hybrid`` uses the dual-speed Vector+BM25+KuzuDB search.
    ``mode=semantic`` is reserved for embedding-based search via the Router.
    """
    orch = request.app.state.orchestrator

    if mode == "semantic":
        return {
            "query": q,
            "mode": "semantic",
            "results": [],
            "count": 0,
            "note": (
                "Semantic search requires Router handoff for query embedding. "
                "Use mode=keyword or hybrid for local search."
            ),
        }

    if mode == "hybrid":
        if orch.hybrid_search is None:
            return {"error": "Hybrid search engine not initialized"}
        
        results = await orch.hybrid_search.search(query=q, limit=limit)
        return {
            "query": q,
            "mode": "hybrid",
            "results": results["chunks"],
            "graph_context": results.get("graph_context", []),
            "count": len(results["chunks"]),
        }

    results = await orch.engine.search_chunks(q, limit=limit)
    return {
        "query": q,
        "mode": "keyword",
        "results": results,
        "count": len(results),
    }


# ── Queue ───────────────────────────────────────────────


@router.get("/v1/queue")
async def queue_status(request: Request) -> dict[str, Any]:
    orch = request.app.state.orchestrator
    depths = await orch.scheduler.get_depths()
    handoff = await orch.queue.depth() if orch.queue else 0
    return {"lanes": depths, "handoff_queue": handoff}


# ── Janitor ─────────────────────────────────────────────


@router.post("/v1/janitor/sync")
async def janitor_sync(request: Request) -> dict[str, Any]:
    orch = request.app.state.orchestrator
    events = await orch.journal_sync.sync()
    for event in events:
        await orch.scheduler.enqueue(event)
    return {"corrective_events": len(events)}


@router.post("/v1/janitor/purge")
async def janitor_purge(request: Request) -> dict[str, Any]:
    orch = request.app.state.orchestrator
    settings = request.app.state.settings
    count = await orch.tombstone.purge(settings.janitor_purge_days)
    return {"purged": count}
