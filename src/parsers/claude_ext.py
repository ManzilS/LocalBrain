"""Claude conversation parser — Anthropic data export.

Handles both raw JSON files and the ZIP archive from Claude.ai's
"Export data" feature.  Claude conversations use a simple linear
message array (no tree branching).

Export structure:
    export.zip/
      conversations.json   ← array of conversation objects

Each conversation object:
    {
      "uuid": str,
      "name": str,
      "created_at": "ISO 8601",
      "updated_at": "ISO 8601",
      "chat_messages": [
        {
          "uuid": str,
          "sender": "human" | "assistant",
          "text": str,
          "created_at": "ISO 8601",
          "updated_at": "ISO 8601",
          "attachments": [...],
          "files": [...]
        }
      ]
    }
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from typing import Any

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)

_PLATFORM = "claude"

# Claude uses "human"/"assistant" roles
_ROLE_MAP = {
    "human": "User",
    "assistant": "Assistant",
}


class ClaudeParser(ParserBase):
    name = "claude"
    supported_mimes = ["application/json"]
    lane = "fast"

    async def can_parse(self, file_record: FileRecord) -> bool:
        low = file_record.identity.path.lower()
        if "claude" in low and low.endswith(".json"):
            return True
        if "claude" in low and low.endswith(".zip"):
            return True
        return False

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        low = file_record.identity.path.lower()

        if low.endswith(".zip"):
            return self._parse_zip(raw_bytes)

        return self._parse_json(raw_bytes)

    # ── ZIP handling ───────────────────────────────────────

    def _parse_zip(self, data: bytes) -> ParseResult:
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
        except (zipfile.BadZipFile, EOFError) as exc:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": f"corrupt_zip: {exc}"})

        with zf:
            for name in zf.namelist():
                low = name.lower()
                if low.endswith("conversations.json") or low.endswith(".json"):
                    raw = zf.read(name)
                    result = self._parse_json(raw)
                    if result.metadata.get("conversation_count", 0) > 0:
                        return result

        return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "no conversation data in archive"})

    # ── Core JSON parsing ──────────────────────────────────

    def _parse_json(self, data: bytes) -> ParseResult:
        try:
            raw = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": f"json_decode: {exc}"})

        # Claude export is an array of conversations
        conversations = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []

        all_text: list[str] = []
        conv_summaries: list[dict[str, Any]] = []
        total_messages = 0
        timestamps: list[str] = []

        for conv in conversations:
            if not isinstance(conv, dict):
                continue

            title = conv.get("name") or conv.get("title") or "Untitled"
            conv_id = conv.get("uuid") or conv.get("id", "")
            created_at = conv.get("created_at", "")
            updated_at = conv.get("updated_at", "")

            if created_at:
                timestamps.append(created_at)
            if updated_at:
                timestamps.append(updated_at)

            # Messages can be under "chat_messages" or "messages"
            messages = conv.get("chat_messages") or conv.get("messages") or []
            msg_count = len(messages)
            total_messages += msg_count

            # Build readable text
            conv_lines = [f"# {title}", ""]
            has_attachments = False

            for msg in messages:
                if not isinstance(msg, dict):
                    continue

                role_raw = msg.get("sender") or msg.get("role") or "unknown"
                role_label = _ROLE_MAP.get(role_raw, role_raw.title())
                content = msg.get("text") or msg.get("content") or ""

                # Handle content that might be a list (some formats)
                if isinstance(content, list):
                    parts = []
                    for part in content:
                        if isinstance(part, str):
                            parts.append(part)
                        elif isinstance(part, dict) and "text" in part:
                            parts.append(part["text"])
                    content = "\n".join(parts)

                conv_lines.append(f"**{role_label}:** {content}")
                conv_lines.append("")

                # Track attachments
                if msg.get("attachments") or msg.get("files"):
                    has_attachments = True

            all_text.append("\n".join(conv_lines))
            conv_summaries.append({
                "id": conv_id,
                "title": title,
                "created_at": created_at,
                "updated_at": updated_at,
                "message_count": msg_count,
                "has_attachments": has_attachments,
            })

        content = "\n---\n\n".join(all_text)

        metadata: dict[str, Any] = {
            "platform": _PLATFORM,
            "conversation_count": len(conv_summaries),
            "total_messages": total_messages,
            "conversations": conv_summaries,
        }
        if timestamps:
            sorted_ts = sorted(timestamps)
            metadata["date_range"] = {
                "earliest": sorted_ts[0],
                "latest": sorted_ts[-1],
            }

        return ParseResult(content=content, metadata=metadata)
