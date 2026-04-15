"""Identity-linked file tracking.

Tracks files using a composite key: Inode + DeviceID + mtime + size +
4KB head hash.  This prevents "inode recycling" false positives and
recognises moved/renamed files without re-processing.
"""

from __future__ import annotations

import logging
import os
import stat as stat_module
from pathlib import Path

from src.chunking.fingerprint import head_hash as compute_head_hash
from src.core.models import FileIdentity

logger = logging.getLogger(__name__)

_HEAD_READ_SIZE = 4096

# SQLite stores INTEGER as signed 64-bit. On Windows, Python 3.12+ returns
# ``stat.st_ino`` values sourced from ``FILE_ID_INFO.FileId`` which is a
# 128-bit identifier — anything above 2**63-1 overflows. The same can happen
# for ``stat.st_dev`` on ReFS/dev drives. We fold wider values into int64 so
# they round-trip through SQLite while preserving equality comparisons
# (the only operations the identity key is ever used for).
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def _fold_int64(value: int) -> int:
    """Collapse an arbitrary-sized integer into the signed int64 range."""
    if _INT64_MIN <= value <= _INT64_MAX:
        return value
    # XOR-fold 64-bit chunks so the high bits still influence the result.
    unsigned = 0
    v = value if value >= 0 else (value & ((1 << (value.bit_length() + 1)) - 1))
    while v:
        unsigned ^= v & ((1 << 64) - 1)
        v >>= 64
    # Convert unsigned 64-bit back to signed int64.
    if unsigned >= (1 << 63):
        unsigned -= 1 << 64
    return unsigned


class FileIdentityResolver:
    """Resolves a path into a ``FileIdentity`` composite key."""

    def resolve(self, path: str | Path) -> FileIdentity:
        """Build a ``FileIdentity`` from the filesystem stat + head hash."""
        p = Path(path)
        stat = os.stat(p)

        inode = _fold_int64(stat.st_ino)
        device = _fold_int64(stat.st_dev)

        # Refuse to read non-regular files (pipes, devices, sockets can hang)
        if not stat_module.S_ISREG(stat.st_mode):
            return FileIdentity(
                path=str(p.resolve()),
                inode=inode,
                device=device,
                mtime=stat.st_mtime,
                size=stat.st_size,
                head_hash="",
            )

        raw = b""
        if stat.st_size > 0:
            with open(p, "rb") as fh:
                raw = fh.read(_HEAD_READ_SIZE)

        return FileIdentity(
            path=str(p.resolve()),
            inode=inode,
            device=device,
            mtime=stat.st_mtime,
            size=stat.st_size,
            head_hash=compute_head_hash(raw),
        )

    @staticmethod
    def has_changed(old: FileIdentity, new: FileIdentity) -> bool:
        """Return True if the file has been modified since the last identity snapshot.

        A change in any of mtime, size, or head_hash is treated as a
        modification.  Inode + device changes indicate a different
        physical file (move/replace).
        """
        if old.inode != new.inode or old.device != new.device:
            return True
        if old.mtime != new.mtime or old.size != new.size:
            return True
        if old.head_hash != new.head_hash:
            return True
        return False

    @staticmethod
    def same_physical_file(a: FileIdentity, b: FileIdentity) -> bool:
        """Return True if both identities refer to the same physical file
        (even if renamed/moved)."""
        return a.inode == b.inode and a.device == b.device and a.inode != 0
