"""Application factory and lifecycle management.

Mirrors the Router project's ``main.py`` — creates the FastAPI app,
wires middleware, attaches the orchestrator to ``app.state``, and
manages startup/shutdown.
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.core.orchestrator import Orchestrator
from src.gateway.server import router
from src.utils.config import Settings
from src.utils.errors import LocalBrainError
from src.utils.logging import request_id_var, request_start_var, setup_logging

logger = logging.getLogger(__name__)


# ── Lifespan ────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Start the orchestrator on boot, shut it down on exit."""
    settings: Settings = app.state.settings
    orchestrator = Orchestrator(settings)
    app.state.orchestrator = orchestrator

    await orchestrator.start()
    logger.info("LocalBrain ready on %s:%d", settings.host, settings.port)

    yield

    await orchestrator.stop()
    logger.info("LocalBrain shut down")


# ── Middleware ──────────────────────────────────────────


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Inject a unique request ID and start timer into context vars."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        rid = request.headers.get("X-Request-ID", uuid.uuid4().hex[:12])
        request_id_var.set(rid)
        request_start_var.set(time.time())
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response


class AuthMiddleware(BaseHTTPMiddleware):
    """Bearer-token authentication (skipped for health endpoints)."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        settings: Settings = request.app.state.settings
        if not settings.api_key:
            return await call_next(request)

        # Skip auth for health/docs
        if request.url.path in ("/", "/health", "/docs", "/openapi.json"):
            return await call_next(request)

        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {settings.api_key}":
            from src.utils.errors import AuthenticationError

            err = AuthenticationError("Invalid or missing API key")
            return JSONResponse(err.to_dict(), status_code=err.status_code)

        return await call_next(request)


# ── Error handler ───────────────────────────────────────


async def localbrain_error_handler(request: Request, exc: LocalBrainError) -> JSONResponse:
    return JSONResponse(exc.to_dict(), status_code=exc.status_code)


# ── Factory ─────────────────────────────────────────────


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build and return the configured FastAPI application."""
    if settings is None:
        settings = Settings()

    setup_logging(level=settings.log_level, dev_mode=settings.dev_mode)

    app = FastAPI(
        title="LocalBrain",
        version="0.1.0",
        docs_url="/docs" if settings.dev_mode else None,
        lifespan=lifespan,
    )

    app.state.settings = settings

    # Middleware (order matters: outermost first)
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(AuthMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in settings.cors_origins.split(",")],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Error handlers
    app.add_exception_handler(LocalBrainError, localbrain_error_handler)  # type: ignore[arg-type]

    # Routes
    app.include_router(router)

    return app


# ── Entrypoint ──────────────────────────────────────────


def main() -> None:
    settings = Settings()
    app = create_app(settings)
    uvicorn.run(app, host=settings.host, port=settings.port, log_level=settings.log_level)


if __name__ == "__main__":
    main()
