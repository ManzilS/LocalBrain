# LocalBrain

High-performance local file ingestion engine — watches, parses, chunks, and stores documents for semantic retrieval. Designed to run safely on consumer hardware.

## Quick Start

### Prerequisites

- **Python 3.12 or newer** — check with `python --version`.
- **[uv](https://github.com/astral-sh/uv)** — the project's package/runtime manager.
  - Install on macOS/Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
  - Install on Windows (PowerShell): `irm https://astral.sh/uv/install.ps1 | iex`

### Install & Run

From the repo root:

```bash
# 1. Install dependencies and build the `localbrain` entry point
uv sync

# 2. Start the server
uv run localbrain
```

On first run you should see log lines ending with:

```
LocalBrain ready on 127.0.0.1:8090
Application startup complete.
```

The server listens on `http://127.0.0.1:8090`. Leave it running and open a
second terminal to interact with it.

### Verify It's Working

```bash
curl http://127.0.0.1:8090/health
```

Expected response (abbreviated):

```json
{
  "status": "healthy",
  "watch_roots": ["~/Documents", "~/Projects"],
  "queue_depths": {"fast": 0, "heavy": 0, "background": 0},
  "parsers": ["text", "pdf", "archive", "chatgpt", "claude", "gemini", "ai_generic"]
}
```

### Try an Ingestion

LocalBrain watches the directories listed in `access.config.json` (by default
`~/Documents` and `~/Projects`). Drop a text file into one of them:

```bash
# macOS/Linux
echo "hello from localbrain" > ~/Documents/localbrain_hello.txt

# Windows (Git Bash / WSL)
echo "hello from localbrain" > "$HOME/Documents/localbrain_hello.txt"
```

Wait a few seconds (debounce + settle), then list tracked files:

```bash
curl http://127.0.0.1:8090/v1/files
```

You should see your file with `"status": "indexed"`.

### Stopping the Server

Press `Ctrl+C` in the terminal running `uv run localbrain`.

### Troubleshooting

- **`error: Failed to spawn: 'localbrain'` / `program not found`** — you have
  an older checkout that lacked the `[build-system]` block in `pyproject.toml`.
  Run `git pull && uv sync` to rebuild the entry point.
- **`Table 'chunks' already exists` on startup** — a previous process was
  killed mid-init and left an orphan LanceDB table. Recent versions recover
  automatically; if you see it on an older checkout, delete
  `~/.localbrain/lance/chunks.lance` and restart.
- **Nothing appears in `/v1/files` after dropping a file** — the watcher only
  sees *new* events after the server starts. Modify the file (e.g. append a
  line) to trigger an event, and confirm the file's directory is listed under
  `watch_roots` in `/health`.
- **Port 8090 is in use** — set `LOCALBRAIN_PORT=8091 uv run localbrain` (or
  any free port).
- **Want to watch a different directory?** — edit `access.config.json` and
  restart the server.

## Architecture

```
 File System
     |
     v
+--------------------------------------------------------------+
|  Phase 1 -- Ingress & Triage                                 |
|  +------------+  +--------------+  +-----------------------+ |
|  | ScopeGate  |->| FileIdentity |->| Debounce / Settle     | |
|  +------------+  +--------------+  +-----------------------+ |
+--------------------------------------------------------------+
|  Phase 2 -- Multi-Modal Parsing                              |
|                                                              |
|  +----------+  +----------+  +----------+  +-------------+  |
|  | Text/Code|  |   PDF    |  |  Office  |  | Archive VFS |  |
|  | (fast)   |  | (heavy)  |  | (heavy)  |  | (background)|  |
|  +----------+  +----------+  +----------+  +-------------+  |
|                                                              |
|  +----------+  +----------+  +----------+  +-------------+  |
|  | ChatGPT  |  |  Claude  |  |  Gemini  |  |  Copilot /  |  |
|  | (fast)   |  | (fast)   |  | (fast)   |  | Perplexity  |  |
|  +----------+  +----------+  +----------+  +-------------+  |
+--------------------------------------------------------------+
|  Phase 3 -- Semantic Chunking                                |
|  +------------------+  +--------------+                      |
|  | CDC Chunking     |->| Chunk Dedup  |                      |
|  | (Gear Hash)      |  | (xxHash)     |                      |
|  +------------------+  +--------------+                      |
+--------------------------------------------------------------+
|  Phase 4 -- Router Handoff                                   |
|  +--------------------------+  +--------------------------+  |
|  | Backpressure Queue       |->| Gateway Client -> Router |  |
|  | (SQLite-backed, durable) |  | (Embeddings / Summaries) |  |
|  +--------------------------+  +--------------------------+  |
+--------------------------------------------------------------+
|  Phase 5 -- The Vault (Dual-Engine Storage)                  |
|  +------------------+  +------------------+                  |
|  | SQLite            |  | LanceDB          |                  |
|  | (state, metadata, |  | (vectors, raw    |                  |
|  |  queue, journal)  |  |  text chunks)    |                  |
|  +------------------+  +------------------+                  |
+--------------------------------------------------------------+
|  Phase 6 -- The Janitor (Maintenance)                        |
|  +-------------+  +-------------+  +----------------------+  |
|  | Journal Sync|  | Tombstone   |  | Lazy Re-index        |  |
|  |             |  | Cascade     |  | (idle + AC only)     |  |
|  +-------------+  +-------------+  +----------------------+  |
+--------------------------------------------------------------+
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

  # AI platform conversation parsers
  chatgpt:
    enabled: true
    module: src.parsers.chatgpt_ext

  claude:
    enabled: true
    module: src.parsers.claude_ext

  gemini:
    enabled: true
    module: src.parsers.gemini_ext

  ai_generic:
    enabled: true
    module: src.parsers.ai_generic_ext
```

## Adding a New Parser

1. Copy `src/parsers/_template.py` to `src/parsers/<format>_ext.py`
2. Implement `can_parse()` and `parse()`
3. Set `supported_mimes` and `lane` (`fast` / `heavy` / `background`)
4. Add an entry to `plugins.yaml`

## Included Parsers

### Document Parsers

| Parser    | Formats                    | Lane       | Dependencies     |
|-----------|----------------------------|------------|------------------|
| text      | TXT, MD, Code (50+ ext)    | fast       | Built-in         |
| pdf       | PDF                        | heavy      | pymupdf (opt.)   |
| office    | DOCX, XLSX, PPTX           | heavy      | python-docx (opt.) |
| image     | JPEG, PNG, TIFF, WebP      | background | Pillow (opt.)    |
| audio     | MP3, WAV, OGG, FLAC        | background | Router handoff   |
| archive   | ZIP, TAR, TAR.GZ           | background | Built-in         |

### AI Platform Conversation Parsers

| Parser     | Platforms                        | Formats              | Lane  |
|------------|----------------------------------|----------------------|-------|
| chatgpt    | ChatGPT (OpenAI)                 | JSON, ZIP            | fast  |
| claude     | Claude (Anthropic)               | JSON, ZIP            | fast  |
| gemini     | Gemini / Bard (Google)           | JSON, HTML, ZIP      | fast  |
| ai_generic | Copilot, Perplexity, and others  | JSON, Markdown, CSV  | fast  |

#### Importing AI Conversations

LocalBrain can ingest conversation exports from major AI platforms. Each platform's data export produces files in different formats — the parsers auto-detect and normalise them.

**ChatGPT (OpenAI)**

1. Go to [ChatGPT Settings](https://chat.openai.com/) → Data Controls → Export Data
2. Download the ZIP archive (contains `conversations.json`)
3. Drop the ZIP or extracted `conversations.json` into a watched directory

The ChatGPT parser handles the tree-structured conversation graph (edits create branches) by linearising the path from root to the active node. Extracts model names (gpt-4, gpt-3.5-turbo, etc.), timestamps, and conversation titles.

**Claude (Anthropic)**

1. Go to [Claude.ai](https://claude.ai/) → Settings → Export Data
2. Download the ZIP archive (contains `conversations.json`)
3. Drop the ZIP or extracted JSON into a watched directory

Supports both `chat_messages` and `messages` keys. Tracks file attachments per conversation.

**Google Gemini**

1. Go to [Google Takeout](https://takeout.google.com/) → select "Gemini Apps"
2. Download the Takeout ZIP archive
3. Drop the ZIP into a watched directory

Handles three format variants that Google uses (JSON prompt/response, JSON role/content, and Google Activity entries). Also parses HTML exports as a fallback.

**Microsoft Copilot**

- **JSON exports**: Place files with "copilot" in the filename into a watched directory
- **CSV session logs**: Copilot Studio exports with SessionId, Role, Content columns

**Perplexity AI**

- **Markdown exports**: Q&A format with citation URLs are automatically parsed
- **JSON exports**: Standard conversation structure with optional `citations` and `sources` arrays

#### Normalised Metadata

All AI parsers produce a common metadata schema:

```json
{
  "platform": "chatgpt",
  "conversation_count": 42,
  "total_messages": 386,
  "date_range": {
    "earliest": "2023-11-15T10:00:00+00:00",
    "latest": "2024-12-20T15:30:00+00:00"
  },
  "models_used": ["gpt-4", "gpt-4o", "gpt-3.5-turbo"],
  "conversations": [
    {
      "title": "Python async patterns",
      "created_at": "2024-01-15T10:00:00+00:00",
      "message_count": 12,
      "model": "gpt-4"
    }
  ]
}
```

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
| `LOCALBRAIN_PORT`                 | `8090`            | Server port (1–65535)              |
| `LOCALBRAIN_LOG_LEVEL`            | `info`            | Logging level                      |
| `LOCALBRAIN_DEV_MODE`             | `false`           | Enables `/docs` and verbose logs   |
| `LOCALBRAIN_DATA_DIR`             | `~/.localbrain`   | Database and queue storage         |
| `LOCALBRAIN_ACCESS_CONFIG`        | `access.config.json` | Scope-gate config path          |
| `LOCALBRAIN_PLUGINS_CONFIG`       | `plugins.yaml`    | Parser plugin config path          |
| `LOCALBRAIN_DEBOUNCE_MS`          | `300`             | File event debounce window (>= 0)  |
| `LOCALBRAIN_SETTLE_TIME_MS`       | `5000`            | Wait for file write completion (>= 0) |
| `LOCALBRAIN_POLL_INTERVAL_S`      | `60.0`            | Reconciliation poll interval (> 0) |
| `LOCALBRAIN_ROUTER_URL`           | `http://localhost:8080` | Router app endpoint           |
| `LOCALBRAIN_ROUTER_API_KEY`       | (empty)           | Bearer token for Router            |
| `LOCALBRAIN_BACKPRESSURE_MAX`     | `10000`           | Max handoff queue depth (>= 1)     |
| `LOCALBRAIN_JANITOR_PURGE_DAYS`   | `7`               | Tombstone retention period (>= 1)  |
| `LOCALBRAIN_JANITOR_REINDEX_THRESHOLD` | `0.20`       | Chunk change ratio for re-index (0–1) |
| `LOCALBRAIN_JANITOR_INTERVAL_S`   | `300.0`           | Janitor loop interval (> 0)        |
| `LOCALBRAIN_API_KEY`              | (empty)           | Bearer token for LocalBrain API    |
| `LOCALBRAIN_CORS_ORIGINS`         | `*`               | Allowed CORS origins (comma-separated) |
| `LOCALBRAIN_RATE_LIMIT_RPM`       | `120`             | Requests per minute limit (>= 1)   |
| `LOCALBRAIN_MAX_BODY_SIZE`        | `10485760`        | Max request body size in bytes (>= 1) |
| `LOCALBRAIN_REQUEST_TIMEOUT`      | `30.0`            | HTTP request timeout in seconds (> 0) |

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

285 tests covering all parsers, pipeline phases, vault operations, gateway endpoints, and edge cases.

## Docker

```bash
docker build -t localbrain .
docker run -p 8090:8090 -v ~/.localbrain:/data localbrain
```
