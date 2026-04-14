"""Google Gemini conversation parser — Google Takeout export.

Handles the various formats Google uses for Gemini/Bard data exports
via Google Takeout.  The format is undocumented and changes without
notice, so this parser tries multiple strategies:

1. JSON array of conversations (most common)
2. Per-conversation JSON files inside a directory in a ZIP
3. HTML fallback (``chat.html`` with structured ``<div>`` blocks)

Export structure (Google Takeout ZIP):
    Takeout/
      Gemini Apps/
        conversations/
          2024-01-15T10:30:00-Conversation Title.json
          ...
      OR
      My Activity/
        Gemini Apps/
          MyActivity.json
"""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from typing import Any

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)

_PLATFORM = "gemini"


class GeminiParser(ParserBase):
    name = "gemini"
    supported_mimes = ["application/json", "text/html"]
    lane = "fast"

    async def can_parse(self, file_record: FileRecord) -> bool:
        low = file_record.identity.path.lower()
        if "gemini" in low or "bard" in low:
            if low.endswith((".json", ".zip", ".html")):
                return True
        if "takeout" in low and low.endswith(".zip"):
            return True
        return False

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        low = file_record.identity.path.lower()

        if low.endswith(".zip"):
            return self._parse_zip(raw_bytes)
        if low.endswith(".html"):
            return self._parse_html(raw_bytes)

        return self._parse_json(raw_bytes)

    # ── ZIP handling ───────────────────────────────────────

    def _parse_zip(self, data: bytes) -> ParseResult:
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
        except (zipfile.BadZipFile, EOFError) as exc:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": f"corrupt_zip: {exc}"})

        all_conversations: list[dict[str, Any]] = []
        all_text: list[str] = []

        with zf:
            for name in zf.namelist():
                low = name.lower()

                # Skip non-Gemini files in a Takeout archive
                if "gemini" not in low and "bard" not in low and "my activity" not in low:
                    continue

                if low.endswith(".json"):
                    try:
                        raw = json.loads(zf.read(name))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue

                    if isinstance(raw, list):
                        for item in raw:
                            conv, text = self._parse_single_conversation(item)
                            if conv:
                                all_conversations.append(conv)
                                all_text.append(text)
                    elif isinstance(raw, dict):
                        conv, text = self._parse_single_conversation(raw)
                        if conv:
                            all_conversations.append(conv)
                            all_text.append(text)

                elif low.endswith(".html"):
                    result = self._parse_html(zf.read(name))
                    if result.metadata.get("conversation_count", 0) > 0:
                        return result

        if not all_conversations:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "no gemini conversations found in archive"})

        return self._build_result(all_conversations, all_text)

    # ── JSON parsing ───────────────────────────────────────

    def _parse_json(self, data: bytes) -> ParseResult:
        try:
            raw = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": f"json_decode: {exc}"})

        all_conversations: list[dict[str, Any]] = []
        all_text: list[str] = []

        items = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []

        for item in items:
            conv, text = self._parse_single_conversation(item)
            if conv:
                all_conversations.append(conv)
                all_text.append(text)

        # Try Google Activity format: array of activity entries
        if not all_conversations and isinstance(raw, list):
            for item in raw:
                conv, text = self._parse_activity_entry(item)
                if conv:
                    all_conversations.append(conv)
                    all_text.append(text)

        if not all_conversations:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "unrecognized json format"})

        return self._build_result(all_conversations, all_text)

    # ── Single conversation parsing ────────────────────────

    def _parse_single_conversation(self, data: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """Parse a single Gemini conversation object. Returns (summary, text) or (None, '')."""
        if not isinstance(data, dict):
            return None, ""

        title = data.get("title") or data.get("name") or "Untitled"
        conv_id = data.get("id") or data.get("conversationId") or ""
        created = data.get("createdTime") or data.get("create_time") or data.get("created_at") or ""
        updated = data.get("lastModifiedTime") or data.get("update_time") or data.get("updated_at") or ""

        # Messages can appear under several keys
        messages = (
            data.get("messages")
            or data.get("chat_messages")
            or data.get("turns")
            or []
        )

        if not messages and not title:
            return None, ""

        conv_lines = [f"# {title}", ""]
        msg_count = 0

        for msg in messages:
            if not isinstance(msg, dict):
                continue

            # Gemini uses prompt/response OR role/content patterns
            prompt = msg.get("prompt") or msg.get("query")
            response = msg.get("response") or msg.get("answer")

            if prompt or response:
                if prompt:
                    conv_lines.append(f"**User:** {prompt}")
                    conv_lines.append("")
                    msg_count += 1
                if response:
                    conv_lines.append(f"**Gemini:** {response}")
                    conv_lines.append("")
                    msg_count += 1
            else:
                # Standard role/content format
                role = msg.get("role") or msg.get("sender") or "unknown"
                content = msg.get("content") or msg.get("text") or ""
                if isinstance(content, list):
                    content = "\n".join(str(p) for p in content if p)
                if content:
                    label = "User" if role.lower() in ("user", "human") else "Gemini"
                    conv_lines.append(f"**{label}:** {content}")
                    conv_lines.append("")
                    msg_count += 1

        summary = {
            "id": conv_id,
            "title": title,
            "created_at": created,
            "updated_at": updated,
            "message_count": msg_count,
        }

        return summary, "\n".join(conv_lines)

    def _parse_activity_entry(self, data: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """Parse a Google My Activity entry for Gemini."""
        if not isinstance(data, dict):
            return None, ""

        title = data.get("title", "")
        # Google Activity entries have "header": "Gemini Apps"
        header = data.get("header", "")
        if "gemini" not in header.lower() and "bard" not in header.lower():
            if "gemini" not in title.lower():
                return None, ""

        time_str = data.get("time", "")
        products = data.get("products", [])
        subtitles = data.get("subtitles", [])

        content_parts = [f"# {title}", ""]
        if subtitles:
            for sub in subtitles:
                if isinstance(sub, dict):
                    content_parts.append(sub.get("name", ""))

        summary = {
            "title": title,
            "created_at": time_str,
            "message_count": 1,
            "products": products,
        }

        return summary, "\n".join(content_parts)

    # ── HTML fallback ──────────────────────────────────────

    def _parse_html(self, data: bytes) -> ParseResult:
        """Parse HTML export — extract text from structured divs."""
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "html_decode_error"})

        # Extract conversation blocks: look for role markers
        conversations: list[str] = []
        current_conv: list[str] = []

        # Simple pattern: extract text between common Gemini HTML markers
        # Strip all HTML tags and preserve structure
        clean = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
        clean = re.sub(r"<style[^>]*>.*?</style>", "", clean, flags=re.DOTALL)
        clean = re.sub(r"<br\s*/?>", "\n", clean)
        clean = re.sub(r"</?(?:div|p|h[1-6]|section)[^>]*>", "\n", clean)
        clean = re.sub(r"<[^>]+>", "", clean)
        clean = re.sub(r"\n{3,}", "\n\n", clean).strip()

        if not clean:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "empty_html"})

        return ParseResult(
            content=clean,
            metadata={
                "platform": _PLATFORM,
                "format": "html",
                "char_count": len(clean),
                "conversation_count": 1,
                "total_messages": 0,
            },
        )

    # ── Result builder ─────────────────────────────────────

    def _build_result(
        self, conversations: list[dict[str, Any]], texts: list[str]
    ) -> ParseResult:
        content = "\n---\n\n".join(texts)
        total_messages = sum(c.get("message_count", 0) for c in conversations)

        timestamps = []
        for c in conversations:
            for key in ("created_at", "updated_at"):
                ts = c.get(key)
                if ts and isinstance(ts, str):
                    timestamps.append(ts)

        metadata: dict[str, Any] = {
            "platform": _PLATFORM,
            "conversation_count": len(conversations),
            "total_messages": total_messages,
            "conversations": conversations,
        }
        if timestamps:
            sorted_ts = sorted(timestamps)
            metadata["date_range"] = {
                "earliest": sorted_ts[0],
                "latest": sorted_ts[-1],
            }

        return ParseResult(content=content, metadata=metadata)
