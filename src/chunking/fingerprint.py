"""xxHash-based fingerprinting for files and chunks.

xxHash is chosen for RAM-speed collision detection.  For massive files
the ``partial_fingerprint`` function hashes only head + tail + size to
avoid full-disk I/O bottlenecks.
"""

from __future__ import annotations

import xxhash

_HEAD_SIZE = 4096  # 4 KB


def file_fingerprint(data: bytes) -> str:
    """Full xxHash64 of the entire file content."""
    return xxhash.xxh64(data).hexdigest()


def chunk_fingerprint(text: str) -> str:
    """xxHash64 of a text chunk (UTF-8 encoded)."""
    return xxhash.xxh64(text.encode()).hexdigest()


def head_hash(data: bytes, size: int = _HEAD_SIZE) -> str:
    """xxHash64 of the first *size* bytes — used in FileIdentity."""
    return xxhash.xxh64(data[:size]).hexdigest()


def partial_fingerprint(data: bytes, *, tail_size: int = _HEAD_SIZE) -> str:
    """Hash head + tail + length for massive-file dedup without full I/O.

    The resulting digest is *not* a substitute for a full fingerprint but
    is useful as a fast pre-filter.
    """
    head = data[:_HEAD_SIZE]
    tail = data[-tail_size:] if len(data) > tail_size else b""
    size_bytes = len(data).to_bytes(8, "little")
    h = xxhash.xxh64()
    h.update(head)
    h.update(tail)
    h.update(size_bytes)
    return h.hexdigest()
