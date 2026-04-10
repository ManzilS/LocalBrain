"""Chunk-level deduplication via xxHash fingerprints.

If a chunk already exists in the vault the system maps the new document
to the existing chunk and skips AI processing entirely.  The deduplicator
keeps a bounded in-memory LRU cache of known fingerprints, backed by the
vault on startup.
"""

from __future__ import annotations

import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)

_DEFAULT_MAX_SIZE = 500_000  # Max fingerprints to cache in memory


class ChunkDeduplicator:
    """Fast membership test for chunk fingerprints with bounded LRU cache."""

    def __init__(self, max_size: int = _DEFAULT_MAX_SIZE) -> None:
        self._known: OrderedDict[str, None] = OrderedDict()
        self._max_size = max_size

    # ── Public API ──────────────────────────────────────

    def is_duplicate(self, fingerprint: str) -> bool:
        """Return True if *fingerprint* has already been registered."""
        if fingerprint in self._known:
            self._known.move_to_end(fingerprint)
            return True
        return False

    def register(self, fingerprint: str) -> None:
        """Mark *fingerprint* as known."""
        if fingerprint in self._known:
            self._known.move_to_end(fingerprint)
            return
        self._known[fingerprint] = None
        if len(self._known) > self._max_size:
            self._known.popitem(last=False)

    def register_many(self, fingerprints: list[str]) -> None:
        """Bulk load from the vault on startup."""
        for fp in fingerprints:
            self._known[fp] = None
        # If startup load exceeds max, trim oldest
        while len(self._known) > self._max_size:
            self._known.popitem(last=False)
        logger.info("Loaded %d known chunk fingerprints", len(fingerprints))

    def remove(self, fingerprint: str) -> None:
        """Forget *fingerprint* (used by the janitor on purge)."""
        self._known.pop(fingerprint, None)

    @property
    def count(self) -> int:
        return len(self._known)
