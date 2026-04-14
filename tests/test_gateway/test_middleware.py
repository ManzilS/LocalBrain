"""Tests for gateway middleware: auth, CORS, request context, lifespan."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.gateway.main import create_app
from src.utils.config import Settings


def _mock_orchestrator():
    """Create a minimal mocked orchestrator for gateway tests."""
    orch = MagicMock()
    orch.scheduler.get_depths = AsyncMock(return_value={"fast": 0, "heavy": 0, "background": 0})
    orch.queue = MagicMock()
    orch.queue.depth = AsyncMock(return_value=0)
    orch.deduplicator.count = 0
    orch.scope_gate = MagicMock()
    orch.scope_gate.get_watch_roots.return_value = []
    orch.registry.parsers = []
    orch.engine = AsyncMock()
    orch.engine.list_files = AsyncMock(return_value=[])
    orch.journal_sync = AsyncMock()
    orch.journal_sync.sync = AsyncMock(return_value=[])
    orch.tombstone = AsyncMock()
    orch.tombstone.purge = AsyncMock(return_value=0)
    return orch


# ── Auth Middleware ─────────────────────────────────────


def test_auth_required_when_api_key_set():
    """Requests without valid Bearer token get 401."""
    settings = Settings(api_key="secret123", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app, raise_server_exceptions=False)

    # No auth header
    resp = client.get("/v1/files")
    assert resp.status_code == 401

    # Wrong token
    resp = client.get("/v1/files", headers={"Authorization": "Bearer wrong"})
    assert resp.status_code == 401

    # Correct token
    resp = client.get("/v1/files", headers={"Authorization": "Bearer secret123"})
    assert resp.status_code == 200


def test_auth_skipped_for_health():
    """Health endpoint always accessible regardless of API key."""
    settings = Settings(api_key="secret123", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/health")
    assert resp.status_code == 200


def test_auth_skipped_for_root():
    """Root endpoint always accessible."""
    settings = Settings(api_key="secret123", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/")
    assert resp.status_code == 200


def test_auth_disabled_when_no_api_key():
    """All endpoints accessible when api_key is empty."""
    settings = Settings(api_key="", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/v1/files")
    assert resp.status_code == 200


# ── Request Context Middleware ──────────────────────────


def test_request_id_header_returned():
    """Response should include X-Request-ID header."""
    settings = Settings(api_key="", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app)
    resp = client.get("/")
    assert "X-Request-ID" in resp.headers


def test_custom_request_id_echoed():
    """If client sends X-Request-ID, it should be echoed back."""
    settings = Settings(api_key="", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app)
    resp = client.get("/", headers={"X-Request-ID": "my-custom-id"})
    assert resp.headers["X-Request-ID"] == "my-custom-id"


# ── CORS ────────────────────────────────────────────────


def test_cors_origins_parsed():
    """Verify CORS origins are correctly split and stripped."""
    settings = Settings(
        api_key="",
        dev_mode=True,
        cors_origins="http://localhost:3000, http://example.com",
    )
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app)
    resp = client.options(
        "/",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )
    # CORS preflight should succeed
    assert resp.status_code == 200


# ── Docs Gating ─────────────────────────────────────────


def test_docs_available_in_dev_mode():
    settings = Settings(api_key="", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app)
    resp = client.get("/docs")
    assert resp.status_code == 200


def test_docs_disabled_in_prod_mode():
    settings = Settings(api_key="", dev_mode=False)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app)
    resp = client.get("/docs")
    assert resp.status_code in (404, 405)


# ── Error Handler ───────────────────────────────────────


def test_invalid_file_status_returns_400():
    """Invalid status filter should return 400, not 500."""
    settings = Settings(api_key="", dev_mode=True)
    app = create_app(settings)
    app.state.orchestrator = _mock_orchestrator()

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/v1/files?status=bogus")
    assert resp.status_code == 400
