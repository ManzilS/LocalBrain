"""ChatGPT conversation parser — OpenAI data export (conversations.json).

Handles both raw ``conversations.json`` files and the ZIP archive that
OpenAI's "Export data" feature produces.  ChatGPT conversations use a
*tree* structure (not linear) because users can edit messages and branch
conversations.  This parser linearises the tree by following the path
from root to ``current_node``.

Export structure:
    export.zip/
      conversations.json   ← array of conversation objects
      chat.html            ← (ignored — HTML duplicate)

Each conversation object:
    {
      "title": str,
      "create_time": float | None,
      "update_time": float | None,
      "mapping": { node_id: { "message": {...}, "parent": str|None, "children": [...] } },
      "current_node": str
    }
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from datetime import datetime, timezone
from typing import Any

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)

_PLATFORM = "chatgpt"


class ChatGPTParser(ParserBase):
    name = "chatgpt"
    supported_mimes = ["application/json"]
    lane = "heavy"

    async def can_parse(self, file_record: FileRecord) -> bool:
        low = file_record.identity.path.lower()
        if low.endswith("conversations.json"):
            return True
        # ZIP from OpenAI export
        if low.endswith(".zip") and "chatgpt" in low:
            return True
        return False

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        low = file_record.identity.path.lower()

        # If it's a ZIP, extract conversations.json
        if low.endswith(".zip"):
            return self._parse_zip(raw_bytes)

        return self._parse_conversations_json(raw_bytes)

    # ── ZIP handling ───────────────────────────────────────

    def _parse_zip(self, data: bytes) -> ParseResult:
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
        except (zipfile.BadZipFile, EOFError) as exc:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": f"corrupt_zip: {exc}"})

        with zf:
            for name in zf.namelist():
                if name.lower().endswith("conversations.json"):
                    return self._parse_conversations_json(zf.read(name))

        return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "no conversations.json in archive"})

    # ── Core JSON parsing ──────────────────────────────────

    def _parse_conversations_json(self, data: bytes) -> ParseResult:
        try:
            conversations = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": f"json_decode: {exc}"})

        if not isinstance(conversations, list):
            return ParseResult(content="", metadata={"platform": _PLATFORM, "error": "expected array at top level"})

        all_text: list[str] = []
        conv_summaries: list[dict[str, Any]] = []
        total_messages = 0
        models_used: set[str] = set()
        timestamps: list[float] = []

        for conv in conversations:
            if not isinstance(conv, dict):
                continue

            title = conv.get("title", "Untitled")
            create_time = conv.get("create_time")
            update_time = conv.get("update_time")
            mapping = conv.get("mapping", {})
            current_node = conv.get("current_node")

            if create_time and isinstance(create_time, (int, float)) and create_time > 0:
                timestamps.append(create_time)
            if update_time and isinstance(update_time, (int, float)) and update_time > 0:
                timestamps.append(update_time)

            # Linearise the conversation tree
            messages = self._linearise_tree(mapping, current_node)
            msg_count = len(messages)
            total_messages += msg_count

            # Build readable text
            conv_lines = [f"# {title}", ""]
            conv_model = None
            for msg in messages:
                role = msg.get("role", "unknown")
                content = msg.get("content", "")
                model = msg.get("model")
                if model:
                    models_used.add(model)
                    conv_model = model

                prefix = {"user": "**User:**", "assistant": "**Assistant:**", "system": "**System:**"}.get(role, f"**{role}:**")
                conv_lines.append(f"{prefix} {content}")
                conv_lines.append("")

            all_text.append("\n".join(conv_lines))
            conv_summaries.append({
                "title": title,
                "created_at": _ts_to_iso(create_time),
                "updated_at": _ts_to_iso(update_time),
                "message_count": msg_count,
                "model": conv_model,
            })

        content = "\n---\n\n".join(all_text)

        metadata: dict[str, Any] = {
            "platform": _PLATFORM,
            "conversation_count": len(conv_summaries),
            "total_messages": total_messages,
            "models_used": sorted(models_used),
            "conversations": conv_summaries,
        }
        if timestamps:
            metadata["date_range"] = {
                "earliest": _ts_to_iso(min(timestamps)),
                "latest": _ts_to_iso(max(timestamps)),
            }

        return ParseResult(content=content, metadata=metadata)

    # ── Tree linearisation ─────────────────────────────────

    @staticmethod
    def _linearise_tree(mapping: dict[str, Any], current_node: str | None) -> list[dict[str, Any]]:
        """Walk from ``current_node`` back to root, then reverse to get
        chronological order.  Falls back to all nodes if tree walk fails."""
        if not mapping:
            return []

        messages: list[dict[str, Any]] = []

        # Walk backwards from current_node to root
        if current_node and current_node in mapping:
            node_id: str | None = current_node
            visited: set[str] = set()
            while node_id and node_id in mapping and node_id not in visited:
                visited.add(node_id)
                node = mapping[node_id]
                msg = node.get("message")
                if msg and msg.get("content"):
                    content = msg["content"]
                    parts = content.get("parts", [])
                    text = "\n".join(str(p) for p in parts if isinstance(p, str))
                    if text.strip():
                        author = msg.get("author", {})
                        model_slug = msg.get("metadata", {}).get("model_slug")
                        messages.append({
                            "role": author.get("role", "unknown"),
                            "content": text.strip(),
                            "model": model_slug,
                        })
                node_id = node.get("parent")
            messages.reverse()
        else:
            # Fallback: iterate all nodes (unordered but captures everything)
            for node_id, node in mapping.items():
                msg = node.get("message")
                if not msg or not msg.get("content"):
                    continue
                parts = msg["content"].get("parts", [])
                text = "\n".join(str(p) for p in parts if isinstance(p, str))
                if text.strip():
                    author = msg.get("author", {})
                    model_slug = msg.get("metadata", {}).get("model_slug")
                    messages.append({
                        "role": author.get("role", "unknown"),
                        "content": text.strip(),
                        "model": model_slug,
                    })

        return messages


def _ts_to_iso(ts: float | int | None) -> str | None:
    """Convert a Unix timestamp to ISO 8601, or None."""
    if not ts or not isinstance(ts, (int, float)) or ts <= 0:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (OSError, ValueError, OverflowError):
        return None
