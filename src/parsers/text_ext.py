"""Plain text / Markdown / source code parser.

Fast-lane parser for files that are already text.  Handles encoding
detection via a simple UTF-8-first strategy with latin-1 fallback.
"""

from __future__ import annotations

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

_TEXT_MIMES = {
    "text/plain",
    "text/markdown",
    "text/x-python",
    "text/x-java",
    "text/x-c",
    "text/x-c++",
    "text/x-go",
    "text/x-rust",
    "text/x-ruby",
    "text/x-shellscript",
    "text/x-yaml",
    "text/x-toml",
    "text/csv",
    "text/html",
    "text/css",
    "application/json",
    "application/xml",
    "application/javascript",
    "application/typescript",
    "application/x-yaml",
}

_TEXT_EXTENSIONS = {
    ".txt", ".md", ".markdown", ".rst", ".py", ".js", ".ts", ".tsx",
    ".jsx", ".java", ".c", ".cpp", ".h", ".hpp", ".go", ".rs", ".rb",
    ".sh", ".bash", ".zsh", ".yaml", ".yml", ".toml", ".ini", ".cfg",
    ".json", ".xml", ".html", ".htm", ".css", ".scss", ".less",
    ".csv", ".tsv", ".log", ".sql", ".r", ".m", ".swift", ".kt",
    ".lua", ".pl", ".pm", ".php", ".ex", ".exs", ".erl", ".hs",
    ".ml", ".vim", ".el", ".clj", ".scala", ".dart", ".v", ".zig",
    ".dockerfile", ".makefile", ".cmake", ".gradle",
}

# Max file size this parser will attempt (default 50 MB)
_DEFAULT_MAX_SIZE = 50 * 1024 * 1024


class TextParser(ParserBase):
    name = "text"
    supported_mimes = list(_TEXT_MIMES)
    lane = "fast"

    max_file_size: int = _DEFAULT_MAX_SIZE

    async def can_parse(self, file_record: FileRecord) -> bool:
        if file_record.mime_type in _TEXT_MIMES:
            return True
        from pathlib import Path

        return Path(file_record.identity.path).suffix.lower() in _TEXT_EXTENSIONS

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        if len(raw_bytes) > self.max_file_size:
            return ParseResult(
                content="",
                metadata={"error": "file_too_large", "size": len(raw_bytes)},
            )

        text = self._decode(raw_bytes)
        from pathlib import Path

        ext = Path(file_record.identity.path).suffix.lower()
        return ParseResult(
            content=text,
            metadata={
                "extension": ext,
                "line_count": text.count("\n") + 1,
                "char_count": len(text),
            },
        )

    @staticmethod
    def _decode(data: bytes) -> str:
        """Decode bytes with UTF-8 first, falling back to latin-1."""
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1")
