"""End-to-end smoke test of LocalBrain's GraphRAG stack.

Runs entirely inside a temporary directory — never touches the user's
real Documents folder.

Exercises:
1. Ingestion → SQLite + LanceDB (Tier 1)
2. GraphExtractor (Tier 2 incremental graph build)
3. CommunitySummarizer (Tier 3 global summaries)
4. HybridSearchEngine intent routing (specific / multi-hop / global)
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import tempfile
from pathlib import Path

from src.core.models import EventType, FileIdentity, IngestEvent
from src.core.orchestrator import Orchestrator
from src.utils.config import Settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


async def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="localbrain_e2e_"))
    data_dir = root / "data"
    watch_dir = root / "watched"
    access_path = root / "access.config.json"
    data_dir.mkdir()
    watch_dir.mkdir()

    # Sandboxed scope-gate config — only the temp dir is allowed.
    access_path.write_text(
        json.dumps(
            {
                "watch_roots": [str(watch_dir)],
                "include_patterns": [],
                "exclude_patterns": [],
                "blocked_extensions": [],
                "max_file_size_bytes": 10_485_760,
                "follow_symlinks": False,
            }
        )
    )

    # Force-enable the GraphRAG stack for this run only.
    settings = Settings(
        data_dir=str(data_dir),
        access_config=str(access_path),
        debounce_ms=0,
        settle_time_ms=0,
        enable_graphrag=True,
        enable_lightrag_incremental=True,
        enable_ms_graphrag_summarization=True,
        enable_hipporag_pagerank=True,
    )

    orch = Orchestrator(settings)
    await orch.start()
    try:
        print("\n--- [1] TIER 1: INGESTION ---")
        doc = watch_dir / "frankenstein_summary.txt"
        doc.write_text(
            "Victor Frankenstein was a scientist who created an unnatural monster. "
            "The creature demanded a mate, causing Victor great distress and leading "
            "to a tragic confrontation in the frozen north."
        )

        await orch.scheduler.enqueue(
            IngestEvent(
                event_type=EventType.created,
                file_identity=FileIdentity(path=str(doc)),
            )
        )
        await asyncio.sleep(2)

        hits = await orch.engine.search_chunks("Victor", limit=5)
        print(f"SQLite FTS hits for 'Victor': {len(hits)}")

        print("\n--- [2] TIER 2: GRAPH EXTRACTION ---")
        processed = await orch.graph_extractor.run_batch()
        print(f"GraphExtractor processed {processed} chunk(s).")

        print("\n--- [3] TIER 3: COMMUNITY SUMMARIZATION ---")
        summarized = await orch.community_summarizer.run_batch()
        print(f"CommunitySummarizer generated {summarized} summary(ies).")

        print("\n--- [4] HYBRID SEARCH INTENT ROUTING ---")
        for label, query in [
            ("specific", "Where did the confrontation happen?"),
            ("multi_hop", "How does Victor connect to the frozen north?"),
            ("global_theme", "Give me a summary of the main themes."),
        ]:
            res = await orch.hybrid_search.search(query)
            print(
                f"  [{label}] lane={res['lane']} "
                f"chunks={len(res['chunks'])} graph_ctx={len(res['graph_context'])}"
            )

        print("\nAll tests completed.")
    finally:
        await orch.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
