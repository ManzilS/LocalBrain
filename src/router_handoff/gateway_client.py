"""Async HTTP client for the Router app — the AI Gateway.

Makes asynchronous calls to the Router app for embeddings, OCR, and
summaries.  Respects backpressure and retries with exponential backoff
on transient failures.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx

from src.core.models import Chunk, HandoffRequest, HandoffResponse
from src.utils.errors import HandoffError, HandoffTimeoutError

if TYPE_CHECKING:
    from src.utils.config import Settings

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BASE_DELAY = 1.0  # seconds


class GatewayClient:
    """Sends chunks to the Router app and receives AI outputs."""

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.router_url.rstrip("/")
        self._api_key = settings.router_api_key
        self._timeout = settings.request_timeout
        self._client: httpx.AsyncClient | None = None

    # ── Lifecycle ───────────────────────────────────────

    async def open(self) -> None:
        headers: dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=headers,
            timeout=httpx.Timeout(self._timeout, connect=min(10.0, self._timeout)),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        logger.info("Gateway client ready: %s", self._base_url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── Public API ──────────────────────────────────────

    async def send(self, request: HandoffRequest) -> HandoffResponse:
        """Send a handoff request and return the parsed response."""
        payload = request.model_dump()
        return await self._post("/v1/chat/completions", payload)

    async def send_for_embedding(self, chunks: list[Chunk]) -> list[list[float]]:
        """Request embeddings for a batch of chunks."""
        request = HandoffRequest(chunks=chunks, action="embed")
        response = await self.send(request)
        return response.embeddings or []

    async def send_for_summary(self, chunks: list[Chunk]) -> str:
        """Request a summary over a set of chunks."""
        request = HandoffRequest(chunks=chunks, action="summarize")
        response = await self.send(request)
        return response.summary or ""

    # ── Internal ────────────────────────────────────────

    async def _post(self, path: str, payload: dict[str, Any]) -> HandoffResponse:
        """POST with exponential backoff retry."""
        assert self._client is not None, "GatewayClient not opened"

        import asyncio

        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = await self._client.post(path, json=payload)
                resp.raise_for_status()
                return HandoffResponse.model_validate(resp.json())
            except httpx.TimeoutException as exc:
                last_exc = exc
                logger.warning("Router timeout (attempt %d/%d)", attempt + 1, _MAX_RETRIES)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code >= 500:
                    last_exc = exc
                    logger.warning(
                        "Router %d (attempt %d/%d)",
                        exc.response.status_code,
                        attempt + 1,
                        _MAX_RETRIES,
                    )
                else:
                    raise HandoffError(
                        f"Router returned {exc.response.status_code}",
                        details=exc.response.text,
                    )
            except (ValueError, KeyError) as exc:
                # JSON decode or validation failure — not retryable
                raise HandoffError(f"Invalid response from Router: {exc}")
            except httpx.ConnectError as exc:
                last_exc = exc
                logger.warning("Router unreachable (attempt %d/%d)", attempt + 1, _MAX_RETRIES)

            if attempt < _MAX_RETRIES - 1:
                delay = _BASE_DELAY * (2**attempt)
                await asyncio.sleep(delay)

        if isinstance(last_exc, httpx.TimeoutException):
            raise HandoffTimeoutError("Router request timed out after retries")
        raise HandoffError(f"Router handoff failed after {_MAX_RETRIES} attempts")
