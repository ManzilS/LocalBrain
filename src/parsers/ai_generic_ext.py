"""Generic AI conversation parser — Copilot, Perplexity, and unknown platforms.

This is a catch-all parser for AI conversation exports that don't match
a platform-specific parser.  It handles:

- **Microsoft Copilot**: JSON arrays of conversations or CSV session logs
- **Perplexity AI**: Markdown exports with Q&A + citations
- **Generic JSON**: Any JSON with recognisable conversation structure
  (role/content message arrays)

The parser auto-detects the format and extracts conversations into the
standard LocalBrain schema.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from typing import Any

from src.core.models import FileRecord, ParseResult
from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)

# Platform detection keywords
_COPILOT_HINTS = {"copilot", "microsoft", "bing"}
_PERPLEXITY_HINTS = {"perplexity"}

# Patterns that indicate an AI conversation JSON
_CONVERSATION_KEYS = {"messages", "chat_messages", "turns", "conversation"}
_MESSAGE_KEYS = {"role", "content", "sender", "text"}


class AIGenericParser(ParserBase):
    name = "ai_generic"
    supported_mimes = ["application/json", "text/markdown", "text/csv"]
    lane = "fast"

    async def can_parse(self, file_record: FileRecord) -> bool:
        low = file_record.identity.path.lower()

        # Copilot
        if any(h in low for h in _COPILOT_HINTS):
            if low.endswith((".json", ".csv", ".md")):
                return True

        # Perplexity
        if any(h in low for h in _PERPLEXITY_HINTS):
            if low.endswith((".json", ".md")):
                return True

        return False

    async def parse(self, file_record: FileRecord, raw_bytes: bytes) -> ParseResult:
        low = file_record.identity.path.lower()
        platform = self._detect_platform(low)

        if low.endswith(".csv"):
            return self._parse_csv(raw_bytes, platform)
        if low.endswith(".md"):
            return self._parse_markdown(raw_bytes, platform)
        if low.endswith(".json"):
            return self._parse_json(raw_bytes, platform)

        return ParseResult(content="", metadata={"platform": platform, "error": "unsupported_format"})

    # ── Platform detection ─────────────────────────────────

    @staticmethod
    def _detect_platform(path_lower: str) -> str:
        if any(h in path_lower for h in _COPILOT_HINTS):
            return "copilot"
        if any(h in path_lower for h in _PERPLEXITY_HINTS):
            return "perplexity"
        return "ai_generic"

    # ── JSON parsing ───────────────────────────────────────

    def _parse_json(self, data: bytes, platform: str) -> ParseResult:
        try:
            raw = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return ParseResult(content="", metadata={"platform": platform, "error": f"json_decode: {exc}"})

        items = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []

        all_text: list[str] = []
        conv_summaries: list[dict[str, Any]] = []
        total_messages = 0

        for item in items:
            if not isinstance(item, dict):
                continue

            title = item.get("title") or item.get("name") or "Untitled"
            conv_id = item.get("id") or ""
            created = item.get("created_at") or item.get("createdAt") or item.get("timestamp") or ""

            # Find the messages array
            messages = None
            for key in ("messages", "chat_messages", "turns", "conversation"):
                if key in item and isinstance(item[key], list):
                    messages = item[key]
                    break

            # Maybe the item itself is a single message
            if messages is None and _MESSAGE_KEYS.intersection(item.keys()):
                messages = [item]

            if messages is None:
                continue

            conv_lines = [f"# {title}", ""]
            msg_count = 0
            citations: list[str] = []

            for msg in messages:
                if not isinstance(msg, dict):
                    continue

                role = msg.get("role") or msg.get("sender") or "unknown"
                content = msg.get("content") or msg.get("text") or ""

                if isinstance(content, list):
                    content = "\n".join(str(p) for p in content if p)
                if not content:
                    continue

                label = "User" if role.lower() in ("user", "human") else "AI"
                conv_lines.append(f"**{label}:** {content}")
                conv_lines.append("")
                msg_count += 1

                # Capture citations (Perplexity)
                if msg.get("citations"):
                    citations.extend(str(c) for c in msg["citations"])
                if msg.get("sources"):
                    citations.extend(str(s) for s in msg["sources"])

            if msg_count == 0:
                continue

            total_messages += msg_count

            if citations:
                conv_lines.append("**Sources:**")
                for i, cite in enumerate(citations, 1):
                    conv_lines.append(f"  {i}. {cite}")
                conv_lines.append("")

            all_text.append("\n".join(conv_lines))

            summary: dict[str, Any] = {
                "id": conv_id,
                "title": title,
                "created_at": created,
                "message_count": msg_count,
            }
            if citations:
                summary["citation_count"] = len(citations)
            conv_summaries.append(summary)

        if not conv_summaries:
            return ParseResult(content="", metadata={"platform": platform, "error": "no conversations found"})

        content = "\n---\n\n".join(all_text)
        return ParseResult(
            content=content,
            metadata={
                "platform": platform,
                "conversation_count": len(conv_summaries),
                "total_messages": total_messages,
                "conversations": conv_summaries,
            },
        )

    # ── Markdown parsing (Perplexity style) ────────────────

    def _parse_markdown(self, data: bytes, platform: str) -> ParseResult:
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            return ParseResult(content="", metadata={"platform": platform, "error": "decode_error"})

        if not text.strip():
            return ParseResult(content="", metadata={"platform": platform, "error": "empty_file"})

        # Split by headers or horizontal rules
        sections = re.split(r"\n#{1,2}\s+|\n---+\n", text)
        conversations: list[dict[str, Any]] = []

        # Count Q&A pairs
        q_count = len(re.findall(r"\*\*Q(?:uestion)?[:\*]|\*\*User[:\*]", text, re.IGNORECASE))
        a_count = len(re.findall(r"\*\*A(?:nswer)?[:\*]|\*\*(?:AI|Assistant|Perplexity)[:\*]", text, re.IGNORECASE))

        # Extract citations
        citations = re.findall(r"https?://\S+", text)

        total_messages = q_count + a_count
        if total_messages == 0:
            # Treat the whole file as a single conversation
            total_messages = max(1, len(sections))

        conversations.append({
            "title": text.split("\n")[0].strip("# ").strip() or "Untitled",
            "message_count": total_messages,
        })

        metadata: dict[str, Any] = {
            "platform": platform,
            "format": "markdown",
            "conversation_count": len(conversations),
            "total_messages": total_messages,
            "conversations": conversations,
        }
        if citations:
            metadata["citation_count"] = len(citations)
            metadata["citations"] = citations[:50]  # Cap to prevent huge metadata

        return ParseResult(content=text, metadata=metadata)

    # ── CSV parsing (Copilot Studio style) ─────────────────

    def _parse_csv(self, data: bytes, platform: str) -> ParseResult:
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            return ParseResult(content="", metadata={"platform": platform, "error": "decode_error"})

        reader = csv.DictReader(io.StringIO(text))
        rows = []
        try:
            for row in reader:
                rows.append(row)
        except csv.Error as exc:
            return ParseResult(content="", metadata={"platform": platform, "error": f"csv_parse: {exc}"})

        if not rows:
            return ParseResult(content="", metadata={"platform": platform, "error": "empty_csv"})

        # Group by session/conversation ID
        sessions: dict[str, list[dict[str, str]]] = {}
        session_key = None
        for candidate in ("SessionId", "session_id", "ConversationId", "conversation_id"):
            if candidate in rows[0]:
                session_key = candidate
                break

        for row in rows:
            sid = row.get(session_key, "default") if session_key else "default"
            sessions.setdefault(sid, []).append(row)

        all_text: list[str] = []
        conv_summaries: list[dict[str, Any]] = []
        total_messages = 0

        for sid, messages in sessions.items():
            conv_lines = [f"# Session: {sid}", ""]
            msg_count = 0

            for msg in messages:
                role = msg.get("Role") or msg.get("role") or msg.get("Sender") or "unknown"
                content = msg.get("Content") or msg.get("content") or msg.get("Message") or ""
                if content:
                    label = "User" if role.lower() in ("user", "human") else "AI"
                    conv_lines.append(f"**{label}:** {content}")
                    conv_lines.append("")
                    msg_count += 1

            if msg_count > 0:
                total_messages += msg_count
                all_text.append("\n".join(conv_lines))
                conv_summaries.append({
                    "id": sid,
                    "title": f"Session {sid}",
                    "message_count": msg_count,
                })

        content = "\n---\n\n".join(all_text)
        return ParseResult(
            content=content,
            metadata={
                "platform": platform,
                "format": "csv",
                "conversation_count": len(conv_summaries),
                "total_messages": total_messages,
                "conversations": conv_summaries,
            },
        )
