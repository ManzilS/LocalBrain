"""Content-Defined Chunking (CDC) for semantic text splitting.

Splits text at natural boundaries — paragraph breaks, Markdown headers,
or code-function declarations — so that small edits do not cause
"boundary shift" across the entire chunk set.

The algorithm uses a Gear-hash rolling window to find cut points, then
snaps them to the nearest structural boundary when one exists within a
tolerance window.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.chunking.fingerprint import chunk_fingerprint

# ── Structural boundary patterns (ranked by priority) ──

_BOUNDARIES: list[re.Pattern[str]] = [
    re.compile(r"\n#{1,6}\s"),       # Markdown headers
    re.compile(r"\n\n"),              # Double newline (paragraph)
    re.compile(r"\ndef "),            # Python function
    re.compile(r"\nclass "),          # Python class
    re.compile(r"\nfunction "),       # JS/TS function
    re.compile(r"\n\}\n"),            # Closing brace block
    re.compile(r"\n---+\n"),          # Horizontal rule
    re.compile(r"\n"),                # Single newline (last resort)
]

_SNAP_TOLERANCE = 128  # bytes around the gear-hash cut to search for a boundary


@dataclass(frozen=True, slots=True)
class ChunkBoundary:
    """Describes a single chunk's position within the source text."""

    offset: int
    length: int
    fingerprint: str


def _gear_hash_cut(text: str, start: int, min_size: int, max_size: int, mask: int) -> int:
    """Find the next Gear-hash cut point after *start + min_size*.

    Returns the offset of the cut within *text*, or ``start + max_size``
    if no cut is found within the window.
    """
    h = 0
    end = min(start + max_size, len(text))
    scan_from = start + min_size

    for i in range(start, end):
        h = ((h << 1) + ord(text[i])) & 0xFFFF_FFFF_FFFF_FFFF
        if i >= scan_from and (h & mask) == 0:
            return i + 1

    return end


def _snap_to_boundary(text: str, raw_cut: int, tolerance: int = _SNAP_TOLERANCE) -> int:
    """Move *raw_cut* to the nearest structural boundary within tolerance."""
    lo = max(0, raw_cut - tolerance)
    hi = min(len(text), raw_cut + tolerance)
    window = text[lo:hi]

    best_pos: int | None = None
    for pattern in _BOUNDARIES:
        m = pattern.search(window)
        if m:
            candidate = lo + m.end()
            if best_pos is None or abs(candidate - raw_cut) < abs(best_pos - raw_cut):
                best_pos = candidate
            break  # Use highest-priority boundary found

    return best_pos if best_pos is not None else raw_cut


def cdc_chunk(
    content: str,
    *,
    min_size: int = 256,
    max_size: int = 4096,
    target_size: int = 1024,
) -> list[ChunkBoundary]:
    """Split *content* into semantically-aware chunks using CDC.

    Parameters
    ----------
    content:
        The full text to chunk.
    min_size:
        Minimum chunk size in characters.
    max_size:
        Maximum chunk size in characters.
    target_size:
        Target average chunk size — controls the Gear-hash mask.

    Returns
    -------
    list[ChunkBoundary]:
        Ordered list of non-overlapping chunk boundaries with fingerprints.
    """
    if not content:
        return []

    # Derive mask from target_size: lower bits set → larger average chunks
    bits = max(1, target_size.bit_length() - 1)
    mask = (1 << bits) - 1

    boundaries: list[ChunkBoundary] = []
    pos = 0

    while pos < len(content):
        remaining = len(content) - pos
        if remaining <= max_size:
            # Final chunk — take everything
            chunk_text = content[pos:]
            boundaries.append(
                ChunkBoundary(
                    offset=pos,
                    length=len(chunk_text),
                    fingerprint=chunk_fingerprint(chunk_text),
                )
            )
            break

        raw_cut = _gear_hash_cut(content, pos, min_size, max_size, mask)
        cut = _snap_to_boundary(content, raw_cut)

        # Enforce min/max after snapping — clamp max first so min doesn't
        # push us past the max boundary
        cut = min(cut, pos + max_size)
        cut = max(cut, pos + min_size)

        chunk_text = content[pos:cut]
        boundaries.append(
            ChunkBoundary(
                offset=pos,
                length=len(chunk_text),
                fingerprint=chunk_fingerprint(chunk_text),
            )
        )
        pos = cut

    return boundaries
