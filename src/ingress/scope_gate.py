"""Scope-Gating — the first line of defence.

Before any scan the system loads ``access.config.json`` and evaluates
every file event against a strict whitelist of directories, blocked
extension patterns, and size limits.  Events outside scope are dropped
immediately.
"""

from __future__ import annotations

import json
import logging
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from src.utils.errors import ScopeGateError

logger = logging.getLogger(__name__)


class ScopeGate:
    """Evaluates whether a given path is within the user-defined scope."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._watch_roots: list[Path] = [
            Path(p).expanduser().resolve() for p in config.get("watch_roots", [])
        ]
        self._include: list[str] = config.get("include_patterns", [])
        self._exclude: list[str] = config.get("exclude_patterns", [])
        self._blocked_ext: list[str] = config.get("blocked_extensions", [])
        self._max_size: int = config.get("max_file_size_bytes", 100 * 1024 * 1024)
        self._follow_symlinks: bool = config.get("follow_symlinks", False)

    # ── Factory ─────────────────────────────────────────

    @classmethod
    def from_file(cls, path: str | Path) -> ScopeGate:
        """Load a ``ScopeGate`` from an ``access.config.json`` file."""
        p = Path(path)
        if not p.exists():
            logger.warning("access.config.json not found at %s — using permissive defaults", p)
            return cls({"watch_roots": ["."]})
        with open(p) as fh:
            return cls(json.load(fh))

    # ── Public API ──────────────────────────────────────

    def get_watch_roots(self) -> list[Path]:
        return list(self._watch_roots)

    def is_allowed(self, path: str | Path) -> bool:
        """Return True if *path* passes all scope checks."""
        p = Path(path).resolve()

        # 1. Must be under at least one watch root
        if not any(self._is_under(p, root) for root in self._watch_roots):
            return False

        # 2. Blocked extensions (also check dotfiles like ".env")
        suffix = p.suffix.lower() or (p.name.lower() if p.name.startswith(".") else "")
        if suffix in self._blocked_ext:
            return False

        # 3. Exclude patterns
        rel = str(p)
        for pattern in self._exclude:
            if fnmatch(rel, pattern):
                return False

        # 4. Include patterns (empty list = allow all)
        if self._include:
            if not any(fnmatch(rel, pat) for pat in self._include):
                return False

        # 5. Symlink policy
        if not self._follow_symlinks and p.is_symlink():
            return False

        return True

    def check_size(self, size: int) -> bool:
        """Return True if *size* is within the configured limit."""
        return size <= self._max_size

    def enforce(self, path: str | Path, *, size: int = 0) -> None:
        """Raise ``ScopeGateError`` if *path* is not allowed."""
        if not self.is_allowed(path):
            raise ScopeGateError(f"Path not in scope: {path}")
        if size and not self.check_size(size):
            raise ScopeGateError(
                f"File exceeds size limit ({size} > {self._max_size}): {path}"
            )

    # ── Internal ────────────────────────────────────────

    @staticmethod
    def _is_under(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False
