"""Plugin registry — discovers and loads parsers from ``plugins.yaml``.

Mirrors the Router project's ``PluginRegistry`` pattern: YAML-declared
plugins are resolved via ``importlib`` + ``inspect``, instantiated with
their settings, and indexed by MIME type for fast dispatch.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from pathlib import Path
from typing import Any

import yaml

from src.parsers.base import ParserBase

logger = logging.getLogger(__name__)


def load_plugins_config(path: str | Path) -> dict[str, Any]:
    """Read and return the raw ``plugins.yaml`` dict."""
    p = Path(path)
    if not p.exists():
        logger.warning("plugins.yaml not found at %s — no parsers loaded", p)
        return {}
    with open(p) as fh:
        return yaml.safe_load(fh) or {}


def _find_parser_class(module_path: str) -> type[ParserBase] | None:
    """Import *module_path* and return the first ``ParserBase`` subclass."""
    try:
        mod = importlib.import_module(module_path)
    except ImportError:
        logger.error("Failed to import parser module: %s", module_path)
        return None

    for _name, obj in inspect.getmembers(mod, inspect.isclass):
        if issubclass(obj, ParserBase) and obj is not ParserBase:
            return obj
    return None


class PluginRegistry:
    """Discovers, instantiates, and indexes parser plugins."""

    def __init__(self) -> None:
        self._parsers: list[ParserBase] = []
        self._by_mime: dict[str, ParserBase] = {}

    # ── Discovery ───────────────────────────────────────

    def discover(self, config_path: str | Path) -> None:
        """Load enabled parsers from ``plugins.yaml``."""
        raw = load_plugins_config(config_path)
        parsers_cfg = raw.get("parsers", {})

        for name, entry in parsers_cfg.items():
            if not entry.get("enabled", False):
                logger.debug("Parser '%s' disabled — skipping", name)
                continue

            module = entry.get("module", "")
            settings = entry.get("settings", {}) or {}

            cls = _find_parser_class(module)
            if cls is None:
                logger.warning("No ParserBase subclass in %s — skipping '%s'", module, name)
                continue

            instance = cls(**settings)
            self._parsers.append(instance)

            for mime in instance.supported_mimes:
                self._by_mime[mime] = instance

            logger.info("Loaded parser '%s' (%s) — lane=%s", name, cls.__name__, instance.lane)

    # ── Lookup ──────────────────────────────────────────

    def get_parser(self, mime_type: str) -> ParserBase | None:
        """Return the parser registered for *mime_type*, or None."""
        return self._by_mime.get(mime_type)

    async def find_parser(self, file_record: Any) -> ParserBase | None:
        """Try every registered parser's ``can_parse`` — first match wins."""
        for parser in self._parsers:
            if await parser.can_parse(file_record):
                return parser
        return None

    @property
    def parsers(self) -> list[ParserBase]:
        return list(self._parsers)

    @property
    def supported_mimes(self) -> list[str]:
        return list(self._by_mime.keys())
