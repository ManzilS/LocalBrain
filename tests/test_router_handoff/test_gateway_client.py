"""Tests for the Router gateway client — using mock HTTP responses."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.models import Chunk, HandoffRequest, HandoffResponse
from src.router_handoff.gateway_client import GatewayClient
from src.utils.errors import HandoffError, HandoffTimeoutError


@pytest.fixture
def settings():
    s = MagicMock()
    s.router_url = "http://localhost:8080"
    s.router_api_key = "test-key"
    s.request_timeout = 5.0
    return s


@pytest.fixture
def client(settings):
    return GatewayClient(settings)


def test_client_init(client):
    assert client._base_url == "http://localhost:8080"
    assert client._api_key == "test-key"


@pytest.mark.asyncio
async def test_open_creates_httpx_client(client):
    await client.open()
    assert client._client is not None
    await client.close()


@pytest.mark.asyncio
async def test_close_clears_client(client):
    await client.open()
    await client.close()
    assert client._client is None


@pytest.mark.asyncio
async def test_send_for_embedding(client):
    """Mock a successful embedding response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"embeddings": [[0.1, 0.2]], "summary": None, "labels": None}

    await client.open()
    with patch.object(client._client, "post", new_callable=AsyncMock, return_value=mock_response):
        embeddings = await client.send_for_embedding(
            [Chunk(content="test", fingerprint="fp1")]
        )
        assert embeddings == [[0.1, 0.2]]
    await client.close()


@pytest.mark.asyncio
async def test_send_for_summary(client):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"embeddings": None, "summary": "A good summary", "labels": None}

    await client.open()
    with patch.object(client._client, "post", new_callable=AsyncMock, return_value=mock_response):
        summary = await client.send_for_summary(
            [Chunk(content="test", fingerprint="fp1")]
        )
        assert summary == "A good summary"
    await client.close()
