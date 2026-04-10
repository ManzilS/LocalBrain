"""Tests for FastAPI HTTP endpoints via TestClient."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.core.models import FileIdentity, FileRecord, FileStatus
from src.gateway.main import create_app
from src.utils.config import Settings


@pytest.fixture
def settings():
    return Settings(dev_mode=True, api_key="")


@pytest.fixture
def app(settings):
    app = create_app(settings)

    # Mock the orchestrator so we don't need real DB/watcher
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
    orch.engine.get_file_by_id = AsyncMock(return_value=None)
    orch.engine.get_chunks_for_file = AsyncMock(return_value=[])
    orch.journal_sync = AsyncMock()
    orch.journal_sync.sync = AsyncMock(return_value=[])
    orch.tombstone = AsyncMock()
    orch.tombstone.purge = AsyncMock(return_value=0)

    app.state.orchestrator = orch
    return app


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


def test_root(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "localbrain"


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "queue_depths" in data


def test_list_files_empty(client):
    resp = client.get("/v1/files")
    assert resp.status_code == 200
    assert resp.json()["count"] == 0


def test_get_file_not_found(client):
    resp = client.get("/v1/files/nonexistent")
    assert resp.status_code == 404


def test_ingest_missing_path(client):
    resp = client.post("/v1/ingest", json={})
    assert resp.status_code == 400


def test_ingest_file_not_found(client):
    resp = client.post("/v1/ingest", json={"path": "/definitely/not/a/real/file.txt"})
    assert resp.status_code == 404


def test_queue_status(client):
    resp = client.get("/v1/queue")
    assert resp.status_code == 200
    assert "lanes" in resp.json()


def test_janitor_sync(client):
    resp = client.post("/v1/janitor/sync")
    assert resp.status_code == 200
    assert "corrective_events" in resp.json()


def test_janitor_purge(client):
    resp = client.post("/v1/janitor/purge")
    assert resp.status_code == 200
    assert "purged" in resp.json()


def test_search_placeholder(client):
    resp = client.get("/v1/search?q=test")
    assert resp.status_code == 200
    assert resp.json()["query"] == "test"
