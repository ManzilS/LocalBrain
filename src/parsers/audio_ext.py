"""Audio parser stub — delegates transcription to the Router app.

Captures basic file metadata locally.  The actual transcription
(Whisper, etc.) is handled via the Router handoff queue.
"""

from __future__ import annotations

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

_AUDIO_MIMES = {
    "audio/mpeg",
    "audio/wav",
    "audio/ogg",
    "audio/flac",
    "audio/mp4",
    "audio/x-m4a",
    "audio/webm",
}

_AUDIO_EXTENSIONS = {".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac", ".wma", ".webm"}


class AudioParser(ParserBase):
    name = "audio"
    supported_mimes = list(_AUDIO_MIMES)
    lane = "background"

    async def can_parse(self, file_record: FileRecord) -> bool:
        if file_record.mime_type in _AUDIO_MIMES:
            return True
        from pathlib import Path

        return Path(file_record.identity.path).suffix.lower() in _AUDIO_EXTENSIONS

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        return ParseResult(
            content="",
            metadata={
                "size": len(raw_bytes),
                "needs_transcription": True,
            },
        )
