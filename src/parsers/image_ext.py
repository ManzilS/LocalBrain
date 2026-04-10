"""Image parser — EXIF metadata extraction and OCR stub.

Extracts EXIF metadata via ``Pillow`` when available.  Actual OCR is
delegated to the Router app via the handoff queue (background lane).
"""

from __future__ import annotations

import io
import logging

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)

_IMAGE_MIMES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/tiff",
    "image/webp",
    "image/bmp",
}

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".tiff", ".tif", ".webp", ".bmp"}


class ImageParser(ParserBase):
    name = "image"
    supported_mimes = list(_IMAGE_MIMES)
    lane = "background"

    async def can_parse(self, file_record: FileRecord) -> bool:
        if file_record.mime_type in _IMAGE_MIMES:
            return True
        from pathlib import Path

        return Path(file_record.identity.path).suffix.lower() in _IMAGE_EXTENSIONS

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        metadata: dict = {}

        try:
            from PIL import Image  # type: ignore[import-untyped]
            from PIL.ExifTags import TAGS  # type: ignore[import-untyped]

            img = Image.open(io.BytesIO(raw_bytes))
            try:
                metadata["format"] = img.format
                metadata["size"] = list(img.size)
                metadata["mode"] = img.mode

                exif_data = img.getexif()
                if exif_data:
                    exif = {}
                    for tag_id, value in exif_data.items():
                        tag_name = TAGS.get(tag_id, str(tag_id))
                        exif[tag_name] = str(value)
                    metadata["exif"] = exif
            finally:
                img.close()
        except ImportError:
            logger.info("Pillow not installed — image metadata unavailable")
            metadata["error"] = "pillow_not_installed"
        except Exception as exc:
            logger.warning("Failed to extract image metadata: %s", exc)
            metadata["error"] = f"image_read_error: {exc}"

        # OCR content is produced by the Router handoff, not locally
        return ParseResult(
            content="",
            metadata={**metadata, "needs_ocr": True},
        )
