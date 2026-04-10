# LocalBrain

High-performance local file ingestion engine — watches, parses, chunks, and stores documents for semantic retrieval. Designed to run safely on consumer hardware.

## Quick Start

```bash
uv sync
uv run localbrain
```

The server starts on `http://127.0.0.1:8090` by default.

## Architecture

```
 File System
     │
     ▼
┌──────────────────────────────────────────────────────────────┐
│  Phase 1 — Ingress & Triage                                  │
│  ┌────────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │ ScopeGate  │→ │ FileIdentity │→ │ Debounce / Settle     │ │
│  └────────────┘  └──────────────┘  └───────────────────────┘ │
├──────────────────────────────────────────────────────────────┤
│  Phase 2 — Multi-Modal Parsing                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ Text/Code│  │   PDF    │  │  Office  │  │ Archive VFS │  │
│  │ (fast)   │  │ (heavy)  │  │ (heavy)  │  │ (background)│  │
│  └──────────┘  └──────────┘  └──────────┘  └─────────────┘  │
├──────────────────────────────────────────────────────────────┤
│  Phase 3 — Semantic Chunking                                 │
│  ┌──────────────────┐  ┌──────────────┐                      │
│  │ CDC Chunking     │→ │ Chunk Dedup  │                      │
│  │ (Gear Hash)      │  │ (xxHash)     │                      │
│  └──────────────────┘  └──────────────┘                      │
├──────────────────────────────────────────────────────────────┤
│  Phase 4 — Router Handoff                                    │
│  ┌──────────────────────────┐  ┌──────────────────────────┐  │
│  │ Backpressure Queue       │→ │ Gateway Client → Router  │  │
│  │ (SQLite-backed, durable) │  │ (Embeddings / Summaries) │  │
│  └──────────────────────────┘  └──────────────────────────┘  │
├──────────────────────────────────────────────────────────────┤
│  Phase 5 — The Vault (Dual-Engine Storage)                   │
│  ┌──────────────────┐  ┌──────────────────┐                  │
│  │ SQLite            │  │ LanceDB          │                  │
│  │ (state, metadata, │  │ (vectors, raw    │                  │
│  │  queue, journal)  │  │  text chunks)    │                  │
│  └──────────────────┘  └──────────────────┘                  │
├──────────────────────────────────────────────────────────────┤
│  Phase 6 — The Janitor (Maintenance)                         │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │ Journal Sync│  │ Tombstone   │  │ Lazy Re-index        │ │
│  │             │  │ Cascade     │  │ (idle + AC only)     │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

## Configuration

### `access.config.json` — Scope Gating

Controls which directories and files the brain can access:

```json
{
  "watch_roots": ["~/Documents", "~/Projects"],
  "exclude_patterns": ["**/node_modules/**", "**/.git/**"],
  "blocked_extensions": [".pem", ".key", ".wallet", ".env"],
  "max_file_size_bytes": 104857600
}
```

### `plugins.yaml` — Parser Plugins

Declares which file parsers are active:

```yaml
parsers:
  text:
    enabled: true
    module: src.parsers.text_ext
    settings:
      max_file_size: 52428800

  pdf:
    enabled: true
    module: src.parsers.pdf_ext

  archive:
    enabled: true
    module: src.parsers.archive_ext
    settings:
      max_depth: 3
      max_files: 1000
```

## Adding a New Parser

1. Copy `src/parsers/_template.py` to `src/parsers/<format>_ext.py`
2. Implement `can_parse()` and `parse()`
3. Set `supported_mimes` and `lane` (`fast` / `heavy` / `background`)
4. Add an entry to `plugins.yaml`

## Included Parsers

| Parser    | Formats                    | Lane       | Dependencies     |
|-----------|----------------------------|------------|------------------|
| text      | TXT, MD, Code (50+ ext)    | fast       | Built-in         |
| pdf       | PDF                        | heavy      | pymupdf (opt.)   |
| office    | DOCX, XLSX, PPTX           | heavy      | python-docx (opt.) |
| image     | JPEG, PNG, TIFF, WebP      | background | Pillow (opt.)    |
| audio     | MP3, WAV, OGG, FLAC        | background | Router handoff   |
| archive   | ZIP, TAR, TAR.GZ           | background | Built-in         |

## API Endpoints

| Method | Path                  | Description                          |
|--------|-----------------------|--------------------------------------|
| GET    | `/`                   | Service status                       |
| GET    | `/health`             | Detailed health (queues, watchers)   |
| POST   | `/v1/ingest`          | Manually trigger file ingestion      |
| GET    | `/v1/files`           | List tracked files                   |
| GET    | `/v1/files/{id}`      | File detail with chunks              |
| GET    | `/v1/search?q=...`    | Semantic search (requires Router)    |
| GET    | `/v1/queue`           | Queue depths by lane                 |
| POST   | `/v1/janitor/sync`    | Trigger journal sync                 |
| POST   | `/v1/janitor/purge`   | Trigger tombstone purge              |

## Environment Variables

| Variable                          | Default           | Description                        |
|-----------------------------------|-------------------|------------------------------------|
| `LOCALBRAIN_HOST`                 | `127.0.0.1`       | Server bind address                |
| `LOCALBRAIN_PORT`                 | `8090`            | Server port                        |
| `LOCALBRAIN_LOG_LEVEL`            | `info`            | Logging level                      |
| `LOCALBRAIN_DEV_MODE`             | `false`           | Enables `/docs` and verbose logs   |
| `LOCALBRAIN_DATA_DIR`             | `~/.localbrain`   | Database and queue storage         |
| `LOCALBRAIN_ACCESS_CONFIG`        | `access.config.json` | Scope-gate config path          |
| `LOCALBRAIN_PLUGINS_CONFIG`       | `plugins.yaml`    | Parser plugin config path          |
| `LOCALBRAIN_DEBOUNCE_MS`          | `300`             | File event debounce window         |
| `LOCALBRAIN_SETTLE_TIME_MS`       | `5000`            | Wait for file write completion     |
| `LOCALBRAIN_ROUTER_URL`           | `http://localhost:8080` | Router app endpoint           |
| `LOCALBRAIN_ROUTER_API_KEY`       | (empty)           | Bearer token for Router            |
| `LOCALBRAIN_BACKPRESSURE_MAX`     | `10000`           | Max handoff queue depth            |
| `LOCALBRAIN_JANITOR_PURGE_DAYS`   | `7`               | Tombstone retention period         |
| `LOCALBRAIN_JANITOR_REINDEX_THRESHOLD` | `0.20`       | Chunk change ratio for re-index    |
| `LOCALBRAIN_API_KEY`              | (empty)           | Bearer token for LocalBrain API    |
| `LOCALBRAIN_CORS_ORIGINS`         | `*`               | Allowed CORS origins               |
| `LOCALBRAIN_RATE_LIMIT_RPM`       | `120`             | Requests per minute limit          |

## Error Handling

All errors return structured JSON:

```json
{
  "error": {
    "type": "scope_gate_denied",
    "message": "Path not in scope: /etc/shadow",
    "details": "..."
  }
}
```

## Structured Logging

- **Production** (`DEV_MODE=false`): JSON lines with request ID, timestamps, and elapsed time
- **Development** (`DEV_MODE=true`): Human-readable with colour-friendly formatting

## Testing

```bash
uv run pytest tests/ -v
```

## Docker

```bash
docker build -t localbrain .
docker run -p 8090:8090 -v ~/.localbrain:/data localbrain
```
