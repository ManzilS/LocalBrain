# LocalBrain

**A high-performance local file ingestion engine. Semantic chunking, hybrid SQLite + LanceDB storage, and a handoff to `router` for LLM access.**

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)]()
[![Status](https://img.shields.io/badge/status-active%20development-orange.svg)]()

---

## What it does

LocalBrain ingests local document collections — PDFs, markdown, text, source code, whatever you point it at — and builds a retrievable semantic index entirely on your machine.

- **Semantic chunking** that respects document structure (not just fixed token windows).
- **Hybrid storage:** SQLite for structured metadata and full-text lookup, [LanceDB](https://lancedb.com) for vector search.
- **Router handoff:** LLM calls go through [router](https://github.com/ManzilS/router) so you can pick local Ollama for privacy, hosted models for speed, or both.
- **Local by default:** embeddings, indexes, and model calls run on your machine. Hosted fallbacks are opt-in.

## Why

Most RAG tooling assumes you'll send documents to a third-party API. For a lot of teams — healthcare, legal, anyone with IP they can't leak — that's a non-starter. LocalBrain exists to make a serious local RAG stack something one person can stand up in an afternoon.

## Architecture

```
                     ┌──────────────────────────┐
  files (pdf, md, ─▶ │   ingestion pipeline     │
  code, txt, ...)    │   • loader               │
                     │   • semantic chunker     │
                     │   • embedder             │
                     └───────────┬──────────────┘
                                 │
                   ┌─────────────┼──────────────┐
                   ▼                            ▼
           ┌──────────────┐              ┌─────────────┐
           │   SQLite     │              │   LanceDB   │
           │ (metadata +  │              │  (vectors)  │
           │   FTS5)      │              └─────────────┘
           └──────────────┘
                   │
                   ▼
           ┌──────────────┐       ┌─────────────────┐
           │   query      │──────▶│  router (LLM)   │
           │   interface  │       │                 │
           └──────────────┘       └─────────────────┘
```

## Quickstart

```bash
git clone https://github.com/ManzilS/LocalBrain
cd LocalBrain
pip install -r requirements.txt

# ingest a folder
python -m localbrain ingest ~/Documents/my-notes

# query
python -m localbrain query "what did I decide about the router middleware order?"
```

## Example: programmatic use

```python
from localbrain import LocalBrain

brain = LocalBrain(data_dir="./brain_data")

# ingest
brain.ingest("./docs/", recursive=True)

# retrieve + answer
answer = brain.query(
    "explain the memory architecture we chose",
    top_k=5,
    llm="llama3",   # routed through router
)
print(answer.text)
for src in answer.sources:
    print(f"  - {src.path}:{src.chunk_range}")
```

## What's implemented today

- [x] PDF, markdown, text, and code ingestion
- [x] Semantic chunking with structural awareness
- [x] SQLite metadata + FTS5 full-text index
- [x] LanceDB vector store
- [x] Query interface with top-k retrieval and source attribution
- [x] Router handoff for LLM synthesis
- [ ] Incremental re-ingestion (watch mode)
- [ ] Re-ranking layer
- [ ] Web UI

## Design notes

**Why hybrid storage?** Vector similarity alone misses exact matches (names, IDs, quoted phrases). FTS5 alone misses paraphrases. Together they cover both shapes of query with very little extra complexity.

**Why semantic chunking?** Fixed-token chunkers split mid-sentence, mid-function, mid-bullet. Retrieval quality suffers in ways that don't show up in toy benchmarks. LocalBrain's chunker respects structural boundaries (headings, code blocks, list items) first and only falls back to size-based splits when it has to.

## License

MIT

## About

Built by [Manzil "Nick" Sapkota](https://github.com/ManzilS) — open to AI/ML Engineer roles. [Email](mailto:manzilsapkota@gmail.com) · [LinkedIn](https://www.linkedin.com/in/manzilsapkota/).
