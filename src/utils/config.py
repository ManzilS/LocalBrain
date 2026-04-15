"""Centralised settings loaded from environment variables and .env files."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """LocalBrain configuration — every field maps to a LOCALBRAIN_* env var."""

    model_config = {"env_prefix": "LOCALBRAIN_", "env_file": ".env"}

    # Server
    host: str = "127.0.0.1"
    port: int = Field(default=8090, ge=1, le=65535)
    log_level: str = "info"
    dev_mode: bool = False

    # Ingestion
    access_config: str = "access.config.json"
    plugins_config: str = "plugins.yaml"
    data_dir: str = str(Path.home() / ".localbrain")
    debounce_ms: int = Field(default=300, ge=0)
    settle_time_ms: int = Field(default=5000, ge=0)
    poll_interval_s: float = Field(default=60.0, gt=0)

    # Router handoff
    router_url: str = "http://localhost:8080"
    router_api_key: str = ""
    backpressure_max: int = Field(default=10_000, ge=1)

    # Janitor
    janitor_purge_days: int = Field(default=7, ge=1)
    janitor_reindex_threshold: float = Field(default=0.20, ge=0.0, le=1.0)
    janitor_interval_s: float = Field(default=300.0, gt=0)

    # Security
    api_key: str = ""
    cors_origins: str = "*"
    rate_limit_rpm: int = Field(default=120, ge=1)
    max_body_size: int = Field(default=10_485_760, ge=1)
    request_timeout: float = Field(default=30.0, gt=0)

    # Holy Grail Graph Architecture Toggles
    enable_graphrag: bool = True
    enable_lightrag_incremental: bool = True
    enable_ms_graphrag_summarization: bool = True
    enable_hipporag_pagerank: bool = True

