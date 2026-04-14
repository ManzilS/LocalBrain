# LocalBrain Architecture

## System Overview

```mermaid
graph TB
    subgraph External["External Systems"]
        FS["File System<br/>(OS Events)"]
        ROUTER["Router App<br/>(AI Gateway)"]
        CLIENT["API Clients<br/>(HTTP)"]
    end

    subgraph Gateway["Gateway Layer"]
        APP["FastAPI App<br/><i>gateway/main.py</i>"]
        MW_CTX["RequestContext<br/>Middleware"]
        MW_AUTH["Auth<br/>Middleware"]
        MW_CORS["CORS<br/>Middleware"]
        ROUTES["API Routes<br/><i>gateway/server.py</i>"]
        ERR["Error Handler<br/><i>utils/errors.py</i>"]
    end

    subgraph Core["Core Engine"]
        ORCH["Orchestrator<br/><i>core/orchestrator.py</i>"]
        SCHED["Scheduler<br/><i>core/scheduler.py</i>"]
        PIPE["IngestPipeline<br/><i>core/pipeline.py</i>"]
        REG["PluginRegistry<br/><i>core/registry.py</i>"]
    end

    subgraph Ingress["Ingress Layer"]
        WATCH["FileWatcher<br/><i>ingress/watcher.py</i>"]
        GATE["ScopeGate<br/><i>ingress/scope_gate.py</i>"]
        IDENT["FileIdentityResolver<br/><i>ingress/identity.py</i>"]
    end

    subgraph Parsers["Document Parsers"]
        TXT["TextParser"]
        PDF["PdfParser"]
        OFF["OfficeParser"]
        IMG["ImageParser"]
        AUD["AudioParser"]
        ARC["ArchiveParser"]
    end

    subgraph AIParsers["AI Platform Parsers"]
        GPT["ChatGPTParser<br/><i>chatgpt_ext.py</i>"]
        CLD["ClaudeParser<br/><i>claude_ext.py</i>"]
        GEM["GeminiParser<br/><i>gemini_ext.py</i>"]
        GEN["AIGenericParser<br/><i>ai_generic_ext.py</i><br/>(Copilot, Perplexity)"]
    end

    subgraph Chunking["Chunking Engine"]
        CDC["CDC Chunker<br/><i>chunking/cdc.py</i>"]
        DEDUP["ChunkDeduplicator<br/><i>chunking/dedup.py</i>"]
        FP["Fingerprinter<br/><i>chunking/fingerprint.py</i>"]
    end

    subgraph Vault["Dual Storage Vault"]
        SQLITE["SQLiteEngine<br/><i>vault/sqlite_engine.py</i>"]
        LANCE["LanceEngine<br/><i>vault/lance_engine.py</i>"]
        SUBS["SubscriptionManager<br/><i>vault/subscriptions.py</i>"]
        REFS["RefCounter<br/><i>vault/ref_counting.py</i>"]
        SCHEMA["Schema DDL<br/><i>vault/schema.py</i>"]
    end

    subgraph Handoff["Router Handoff"]
        BPQ["BackpressureQueue<br/><i>backpressure_queue.py</i>"]
        GWC["GatewayClient<br/><i>gateway_client.py</i>"]
    end

    subgraph Janitor["Maintenance"]
        TOMB["TombstoneCascade<br/><i>janitor/tombstone.py</i>"]
        SYNC["JournalSync<br/><i>janitor/sync.py</i>"]
        REIDX["ReindexManager<br/><i>janitor/reindex.py</i>"]
    end

    %% External connections
    FS -->|OS events| WATCH
    CLIENT -->|HTTP| APP
    GWC <-->|embeddings/OCR| ROUTER

    %% Gateway flow
    APP --> MW_CTX --> MW_AUTH --> MW_CORS --> ROUTES
    APP --> ERR
    ROUTES --> ORCH

    %% Core flow
    ORCH --> WATCH
    ORCH --> SCHED
    ORCH --> PIPE
    ORCH --> TOMB
    ORCH --> SYNC
    ORCH --> REIDX
    SCHED --> PIPE

    %% Ingress
    WATCH --> GATE
    WATCH --> IDENT
    PIPE --> GATE
    PIPE --> IDENT

    %% Parsing
    PIPE --> REG
    REG --> TXT
    REG --> PDF
    REG --> OFF
    REG --> IMG
    REG --> AUD
    REG --> ARC
    REG --> GPT
    REG --> CLD
    REG --> GEM
    REG --> GEN

    %% Chunking
    PIPE --> CDC
    PIPE --> DEDUP
    PIPE --> FP
    CDC --> FP

    %% Storage
    PIPE --> SQLITE
    PIPE --> SUBS
    PIPE --> REFS
    SQLITE --> SCHEMA
    LANCE --> LANCE

    %% Handoff
    PIPE --> BPQ
    BPQ --> GWC

    %% Janitor
    TOMB --> SQLITE
    TOMB --> LANCE
    TOMB --> SUBS
    TOMB --> REFS
    SYNC --> SQLITE
    REIDX --> SQLITE
    REIDX --> SUBS

    %% Styling
    classDef external fill:#f9f,stroke:#333,stroke-width:2px,color:#000
    classDef gateway fill:#bbf,stroke:#333,stroke-width:1px,color:#000
    classDef core fill:#fbb,stroke:#333,stroke-width:1px,color:#000
    classDef ingress fill:#bfb,stroke:#333,stroke-width:1px,color:#000
    classDef parser fill:#ffd,stroke:#333,stroke-width:1px,color:#000
    classDef chunk fill:#dff,stroke:#333,stroke-width:1px,color:#000
    classDef vault fill:#fdb,stroke:#333,stroke-width:1px,color:#000
    classDef handoff fill:#dbf,stroke:#333,stroke-width:1px,color:#000
    classDef janitor fill:#ddd,stroke:#333,stroke-width:1px,color:#000

    class FS,ROUTER,CLIENT external
    class APP,MW_CTX,MW_AUTH,MW_CORS,ROUTES,ERR gateway
    class ORCH,SCHED,PIPE,REG core
    class WATCH,GATE,IDENT ingress
    class TXT,PDF,OFF,IMG,AUD,ARC parser
    class GPT,CLD,GEM,GEN parser
    class CDC,DEDUP,FP chunk
    class SQLITE,LANCE,SUBS,REFS,SCHEMA vault
    class BPQ,GWC handoff
    class TOMB,SYNC,REIDX janitor
```

---

## 5-Phase Ingestion Pipeline

```mermaid
flowchart TD
    START(["IngestEvent Received"])

    subgraph P1["Phase 1: INGRESS"]
        P1A["stat() + S_ISREG check"]
        P1B["ScopeGate.enforce()"]
        P1C["FileIdentityResolver.resolve()"]
        P1D["Detect MIME (extension + magic bytes)"]
        P1E{"File changed?"}
        P1F["Skip — early exit"]
    end

    subgraph P2["Phase 2: PARSE"]
        P2A["asyncio.to_thread(read_bytes)"]
        P2B["Re-check size after read"]
        P2C["Registry.find_parser()"]
        P2D["Parser.parse() → ParseResult"]
        P2E{"sub_files?"}
        P2F["Enqueue sub-files to scheduler"]
        P2G["file_fingerprint() / partial_fingerprint()"]
    end

    subgraph P3["Phase 3: CHUNK"]
        P3A["cdc_chunk() — Gear-hash + boundary snapping"]
        P3B["chunk_fingerprint() per chunk"]
        P3C{"Deduplicator:<br/>is_duplicate?"}
        P3D["Skip duplicate chunk"]
        P3E["Create Chunk objects"]
    end

    subgraph P4["Phase 4: HANDOFF"]
        P4A{"BackpressureQueue<br/>is_full?"}
        P4B["Enqueue HandoffRequest"]
        P4C["Log backpressure warning"]
    end

    subgraph P5["Phase 5: STORE (Atomic)"]
        P5A["BEGIN IMMEDIATE"]
        P5B["upsert_chunk_no_commit() × N"]
        P5C["subscribe_no_commit()"]
        P5D["UPDATE ref_count + 1"]
        P5E["upsert_file_no_commit()"]
        P5F["COMMIT"]
        P5G["ROLLBACK on error"]
    end

    DONE(["✓ File Indexed"])
    ERR(["✗ Error / Dead-Letter"])

    START --> P1A --> P1B --> P1C --> P1D --> P1E
    P1E -->|No change| P1F
    P1E -->|Changed/New| P2A

    P2A --> P2B --> P2C --> P2D --> P2E
    P2E -->|Yes| P2F
    P2E -->|No| P2G
    P2F --> P2G

    P2G --> P3A --> P3B --> P3C
    P3C -->|Yes| P3D
    P3C -->|No| P3E
    P3D --> P3C

    P3E --> P4A
    P4A -->|No| P4B
    P4A -->|Yes| P4C
    P4B --> P5A
    P4C --> P5A

    P5A --> P5B --> P5C --> P5D --> P5E --> P5F --> DONE
    P5A -.->|Exception| P5G -.-> ERR

    style P1 fill:#e8f5e9,stroke:#2e7d32,color:#000
    style P2 fill:#e3f2fd,stroke:#1565c0,color:#000
    style P3 fill:#fff3e0,stroke:#e65100,color:#000
    style P4 fill:#f3e5f5,stroke:#6a1b9a,color:#000
    style P5 fill:#fce4ec,stroke:#b71c1c,color:#000
    style DONE fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#000
    style ERR fill:#ffcdd2,stroke:#b71c1c,stroke-width:2px,color:#000
```

---

## Scheduler & Worker Architecture

```mermaid
flowchart LR
    subgraph Input
        EVT["IngestEvent"]
    end

    subgraph Classification
        CL{"classify_lane()"}
    end

    subgraph Queues["Priority Queues"]
        FAST["FAST Lane<br/>PriorityQueue<br/>(text, markdown, code)"]
        HEAVY["HEAVY Lane<br/>PriorityQueue<br/>(PDF, Office)"]
        BG["BACKGROUND Lane<br/>PriorityQueue<br/>(image, audio, archive)"]
    end

    subgraph Workers["Worker Pools"]
        W1["Worker ×4"]
        W2["Worker ×2"]
        W3["Worker ×1"]
    end

    subgraph Retry["Error Handling"]
        RT{"Retryable?<br/>(≤3 attempts)"}
        BACK["Exponential Backoff<br/>2s → 4s → 8s"]
        DL["Dead Letter<br/>(max 1000)"]
    end

    subgraph Pipeline
        EXEC["Pipeline.execute()"]
    end

    EVT --> CL
    CL -->|text/code| FAST
    CL -->|pdf/office/AI convos| HEAVY
    CL -->|image/audio/archive| BG

    FAST --> W1
    HEAVY --> W2
    BG --> W3

    W1 --> EXEC
    W2 --> EXEC
    W3 --> EXEC

    EXEC -->|Error| RT
    RT -->|Yes| BACK --> FAST & HEAVY & BG
    RT -->|No / Exhausted| DL

    style FAST fill:#c8e6c9,color:#000
    style HEAVY fill:#fff9c4,color:#000
    style BG fill:#e1bee7,color:#000
    style DL fill:#ffcdd2,color:#000
```

---

## Data Storage Schema

```mermaid
erDiagram
    files {
        TEXT id PK
        TEXT path
        INTEGER inode
        INTEGER device
        REAL mtime
        INTEGER size
        TEXT head_hash
        TEXT fingerprint
        TEXT mime_type
        TEXT status
        REAL created_at
        REAL updated_at
        REAL deleted_at
        TEXT metadata
    }

    chunks {
        TEXT id PK
        TEXT content
        TEXT fingerprint UK
        INTEGER byte_offset
        INTEGER byte_length
        INTEGER ref_count
        TEXT metadata
    }

    file_chunks {
        TEXT file_id FK
        TEXT chunk_id FK
        INTEGER sequence
    }

    queue {
        TEXT id PK
        TEXT file_id
        TEXT lane
        INTEGER priority
        TEXT payload
        REAL created_at
        INTEGER attempts
        REAL locked_until
    }

    journal {
        INTEGER id PK
        TEXT operation
        TEXT entity_type
        TEXT entity_id
        REAL timestamp
        TEXT details
    }

    handoff_queue {
        TEXT id PK
        TEXT payload
        REAL created_at
        INTEGER attempts
        REAL locked_until
    }

    lance_chunks {
        TEXT id PK
        TEXT file_id
        VECTOR embedding
    }

    files ||--o{ file_chunks : "has"
    chunks ||--o{ file_chunks : "shared by"
    chunks ||--o| lance_chunks : "vector in"
    files ||--o{ queue : "queued as"
    files ||--o{ journal : "logged in"
```

---

## Background Task Lifecycle

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant W as FileWatcher
    participant S as Scheduler
    participant P as Pipeline
    participant J as Janitor Loop
    participant T as TombstoneCascade
    participant Sy as JournalSync
    participant R as ReindexManager
    participant V as Vault (SQLite)
    participant L as LanceDB

    Note over O: start()
    O->>V: SQLiteEngine.open()
    O->>L: LanceEngine.open()
    O->>S: Scheduler.start() (spawn workers)
    O->>W: create_task(_watch_loop)
    O->>J: create_task(_janitor_loop)

    loop Watch Loop (continuous)
        W->>W: awatch() — OS events
        W->>W: settle-time polling
        W->>S: enqueue(IngestEvent)
        S->>P: Pipeline.execute(state)
        P->>V: Atomic store (BEGIN IMMEDIATE)
        P->>L: Upsert embeddings (via handoff)
    end

    loop Janitor Loop (every 300s)
        J->>Sy: JournalSync.sync()
        Sy->>V: Compare vault vs filesystem
        Sy-->>S: Corrective events

        J->>T: TombstoneCascade.purge()
        T->>V: DELETE tombstoned files
        T->>L: DELETE orphaned embeddings

        J->>R: ReindexManager.can_reindex()
        R->>V: get_pending()
        R->>R: mark_done() (batch of 5)
    end

    Note over O: stop()
    O->>W: stop()
    O->>S: stop() (drain workers)
    O->>V: close()
    O->>L: close()
```

---

## File Tree

```
LocalBrain/
├── pyproject.toml              # Project config (UV, pytest)
├── Dockerfile                  # Multi-stage build
├── README.md                   # Documentation
├── access.config.json          # Scope-gate config (whitelist dirs)
├── plugins.yaml                # Parser plugin declarations
├── conftest.py                 # pytest shared fixtures
│
├── src/
│   ├── core/
│   │   ├── models.py           # 12 Pydantic v2 models + 3 enums
│   │   ├── state.py            # IngestState mutable container
│   │   ├── pipeline.py         # 5-phase ingestion engine
│   │   ├── scheduler.py        # 3-lane priority scheduler + retry
│   │   ├── orchestrator.py     # Top-level lifecycle wiring
│   │   └── registry.py         # YAML plugin discovery
│   │
│   ├── ingress/
│   │   ├── watcher.py          # Hybrid file watcher (watchfiles)
│   │   ├── scope_gate.py       # Whitelist/blocklist enforcement
│   │   └── identity.py         # Inode+device+mtime+size+hash tracking
│   │
│   ├── parsers/
│   │   ├── base.py             # Abstract ParserBase
│   │   ├── text_ext.py         # Plain text, markdown, code
│   │   ├── pdf_ext.py          # PDF (pymupdf + fallback)
│   │   ├── office_ext.py       # DOCX, XLSX, PPTX
│   │   ├── image_ext.py        # EXIF extraction (OCR via Router)
│   │   ├── audio_ext.py        # Metadata stub (transcription via Router)
│   │   ├── archive_ext.py      # ZIP/TAR VFS sandbox
│   │   ├── chatgpt_ext.py      # ChatGPT conversations (tree linearisation)
│   │   ├── claude_ext.py       # Claude conversations (linear messages)
│   │   ├── gemini_ext.py       # Gemini/Bard (3 format variants + HTML)
│   │   └── ai_generic_ext.py   # Copilot, Perplexity (JSON/MD/CSV)
│   │
│   ├── chunking/
│   │   ├── cdc.py              # Gear-hash CDC + boundary snapping
│   │   ├── dedup.py            # LRU fingerprint cache (500K cap)
│   │   └── fingerprint.py      # xxHash (file, chunk, partial, head)
│   │
│   ├── vault/
│   │   ├── schema.py           # DDL for 5 tables + 9 indices
│   │   ├── sqlite_engine.py    # Full CRUD + journal + queue
│   │   ├── lance_engine.py     # LanceDB vector store
│   │   ├── subscriptions.py    # Many-to-many file↔chunk mapping
│   │   └── ref_counting.py     # ACID reference counting
│   │
│   ├── router_handoff/
│   │   ├── backpressure_queue.py  # SQLite-backed durable queue
│   │   └── gateway_client.py      # httpx client with retry
│   │
│   ├── janitor/
│   │   ├── tombstone.py        # 7-day soft-delete cascade
│   │   ├── sync.py             # Journal-based drift detection
│   │   └── reindex.py          # Lazy re-index (idle + AC power)
│   │
│   ├── gateway/
│   │   ├── main.py             # FastAPI factory + middleware
│   │   └── server.py           # 9 API endpoints
│   │
│   └── utils/
│       ├── config.py           # Pydantic settings (env vars)
│       ├── errors.py           # 12-class error hierarchy
│       └── logging.py          # Structured JSON logging
│
└── tests/                      # 285 tests across 31 files
    ├── test_chunking/          # CDC, dedup, fingerprint
    ├── test_core/              # Models, pipeline, scheduler, orchestrator, registry
    ├── test_gateway/           # Server endpoints, error handling, middleware
    ├── test_ingress/           # Identity, scope gate, watcher
    ├── test_janitor/           # Tombstone, sync, reindex
    ├── test_parsers/           # Text, PDF, office, image, audio, archive,
    │                           # ChatGPT, Claude, Gemini, Copilot/Perplexity
    ├── test_router_handoff/    # Backpressure queue, gateway client
    ├── test_utils/             # Config (with validation), logging
    └── test_vault/             # SQLite, LanceDB, ref counting, subscriptions
```
