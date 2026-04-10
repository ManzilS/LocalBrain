"""Structured logging with per-request context tracking.

Produces JSON in production and human-readable output in dev mode.
"""

from __future__ import annotations

import logging
import sys
import time
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")
request_start_var: ContextVar[float] = ContextVar("request_start", default=0.0)


class StructuredFormatter(logging.Formatter):
    """JSON formatter for production logs."""

    def format(self, record: logging.LogRecord) -> str:
        import json

        elapsed = ""
        start = request_start_var.get()
        if start:
            elapsed = f"{(time.time() - start) * 1000:.1f}ms"

        entry = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id_var.get(),
        }
        if elapsed:
            entry["elapsed"] = elapsed
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


class DevFormatter(logging.Formatter):
    """Human-readable formatter for local development."""

    def format(self, record: logging.LogRecord) -> str:
        elapsed = ""
        start = request_start_var.get()
        if start:
            elapsed = f" [{(time.time() - start) * 1000:.1f}ms]"

        rid = request_id_var.get()
        prefix = f"[{rid}] " if rid != "-" else ""
        base = f"{record.levelname:<7} {prefix}{record.getMessage()}{elapsed}"
        if record.exc_info and record.exc_info[1]:
            base += "\n" + self.formatException(record.exc_info)
        return base


def setup_logging(level: str = "info", *, dev_mode: bool = False) -> None:
    """Configure the root logger and quiet noisy third-party libraries."""

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(DevFormatter() if dev_mode else StructuredFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Quiet noisy libraries
    for name in ("watchfiles", "lancedb", "httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.WARNING)
