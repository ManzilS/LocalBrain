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


class FileIdentityResolver:
    """Resolves a path into a ``FileIdentity`` composite key."""

    def resolve(self, path: str | Path) -> FileIdentity:
        """Build a ``FileIdentity`` from the filesystem stat + head hash."""
        p = Path(path)
        stat = os.stat(p)

        # Refuse to read non-regular files (pipes, devices, sockets can hang)
        if not stat_module.S_ISREG(stat.st_mode):
            return FileIdentity(
                path=str(p.resolve()),
                inode=stat.st_ino,
                device=stat.st_dev,
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
            inode=stat.st_ino,
            device=stat.st_dev,
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
