"""Archive parser — ZIP/TAR VFS sandbox extraction.

Creates a virtual manifest and limits extraction depth (max 3) and file
count (max 1,000) to defend against "archive bombs."  Extracted sub-files
are returned in ``ParseResult.sub_files`` for recursive pipeline
processing.
"""

from __future__ import annotations

import io
import logging
import tarfile
import zipfile
from pathlib import PurePosixPath

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)

_ARCHIVE_MIMES = {
    "application/zip",
    "application/x-tar",
    "application/gzip",
    "application/x-gzip",
    "application/x-bzip2",
    "application/x-xz",
}

_ARCHIVE_EXTENSIONS = {".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz"}

_MAX_DEPTH = 3
_MAX_FILES = 1000
_MAX_TOTAL_SIZE = 100 * 1024 * 1024  # 100 MB default


class ArchiveParser(ParserBase):
    name = "archive"
    supported_mimes = list(_ARCHIVE_MIMES)
    lane = "background"

    max_unpack_size: int = _MAX_TOTAL_SIZE
    max_depth: int = _MAX_DEPTH
    max_files: int = _MAX_FILES

    async def can_parse(self, file_record: FileRecord) -> bool:
        if file_record.mime_type in _ARCHIVE_MIMES:
            return True
        from pathlib import Path

        path = Path(file_record.identity.path)
        low = path.name.lower()
        return any(low.endswith(ext) for ext in _ARCHIVE_EXTENSIONS)

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        from pathlib import Path

        path = Path(file_record.identity.path)
        low = path.name.lower()

        if low.endswith(".zip"):
            return self._parse_zip(raw_bytes)
        elif any(low.endswith(ext) for ext in (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")):
            return self._parse_tar(raw_bytes)

        return ParseResult(content="", metadata={"error": "unknown_archive_format"})

    def _parse_zip(self, data: bytes) -> ParseResult:
        manifest: list[str] = []
        sub_files: list[bytes] = []
        total_size = 0

        try:
            zf_ctx = zipfile.ZipFile(io.BytesIO(data))
        except (zipfile.BadZipFile, EOFError) as exc:
            logger.warning("Failed to open zip archive: %s", exc)
            return ParseResult(content="", metadata={"error": "corrupt_zip_archive"})

        with zf_ctx as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue

                # Depth guard
                depth = len(PurePosixPath(info.filename).parts)
                if depth > self.max_depth:
                    continue

                # File count guard
                if len(sub_files) >= self.max_files:
                    logger.warning("Archive file limit reached (%d)", self.max_files)
                    break

                # Size guard
                if total_size + info.file_size > self.max_unpack_size:
                    logger.warning("Archive size limit reached (%d bytes)", self.max_unpack_size)
                    break

                manifest.append(info.filename)
                sub_files.append(zf.read(info.filename))
                total_size += info.file_size

        return ParseResult(
            content="\n".join(manifest),
            metadata={
                "type": "zip",
                "file_count": len(sub_files),
                "total_size": total_size,
            },
            sub_files=sub_files,
        )

    def _parse_tar(self, data: bytes) -> ParseResult:
        manifest: list[str] = []
        sub_files: list[bytes] = []
        total_size = 0

        try:
            tf_ctx = tarfile.open(fileobj=io.BytesIO(data))
        except (tarfile.TarError, EOFError) as exc:
            logger.warning("Failed to open tar archive: %s", exc)
            return ParseResult(content="", metadata={"error": "corrupt_tar_archive"})

        with tf_ctx as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue

                depth = len(PurePosixPath(member.name).parts)
                if depth > self.max_depth:
                    continue

                if len(sub_files) >= self.max_files:
                    logger.warning("Archive file limit reached (%d)", self.max_files)
                    break

                if total_size + member.size > self.max_unpack_size:
                    logger.warning("Archive size limit reached (%d bytes)", self.max_unpack_size)
                    break

                fh = tf.extractfile(member)
                if fh is None:
                    continue

                try:
                    content = fh.read()
                except Exception:
                    logger.warning("Failed to read tar member: %s", member.name)
                    continue
                finally:
                    fh.close()
                manifest.append(member.name)
                sub_files.append(content)
                total_size += len(content)

        return ParseResult(
            content="\n".join(manifest),
            metadata={
                "type": "tar",
                "file_count": len(sub_files),
                "total_size": total_size,
            },
            sub_files=sub_files,
        )
