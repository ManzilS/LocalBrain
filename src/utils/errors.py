"""Structured error hierarchy for LocalBrain.

Every exception carries a machine-readable ``error_type`` and an HTTP
``status_code`` so the gateway can serialise errors consistently.
"""

from __future__ import annotations


class LocalBrainError(Exception):
    """Base error — all LocalBrain exceptions inherit from this."""

    status_code: int = 500
    error_type: str = "internal_error"

    def __init__(self, message: str, *, details: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        payload: dict = {"type": self.error_type, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return {"error": payload}


# ── Ingestion ───────────────────────────────────────────


class IngestError(LocalBrainError):
    status_code = 502
    error_type = "ingest_error"


class FileAccessError(IngestError):
    status_code = 403
    error_type = "file_access_denied"


class FileNotFoundError(IngestError):  # noqa: A001 — intentional shadow
    status_code = 404
    error_type = "file_not_found"


# ── Parsing ─────────────────────────────────────────────


class ParserError(LocalBrainError):
    status_code = 422
    error_type = "parser_error"


class UnsupportedFormatError(ParserError):
    status_code = 415
    error_type = "unsupported_format"


# ── Chunking ────────────────────────────────────────────


class ChunkingError(LocalBrainError):
    status_code = 500
    error_type = "chunking_error"


# ── Vault ───────────────────────────────────────────────


class VaultError(LocalBrainError):
    status_code = 500
    error_type = "vault_error"


class VaultIntegrityError(VaultError):
    status_code = 500
    error_type = "vault_integrity_error"


# ── Router Handoff ──────────────────────────────────────


class HandoffError(LocalBrainError):
    status_code = 502
    error_type = "router_handoff_error"


class HandoffTimeoutError(HandoffError):
    status_code = 504
    error_type = "router_handoff_timeout"


# ── Scope Gate ──────────────────────────────────────────


class ScopeGateError(LocalBrainError):
    status_code = 403
    error_type = "scope_gate_denied"


# ── HTTP / Auth ─────────────────────────────────────────


class RequestValidationError(LocalBrainError):
    status_code = 400
    error_type = "validation_error"


class AuthenticationError(LocalBrainError):
    status_code = 401
    error_type = "authentication_error"


class RateLimitError(LocalBrainError):
    status_code = 429
    error_type = "rate_limit_exceeded"
