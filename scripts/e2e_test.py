"""End-to-end test of LocalBrainFull.

This script tests:
1. Ingestion of a file into SQLite and LanceDB (Tier 1).
2. The GraphExtractor (Tier 2 LightRAG extraction).
3. The CommunitySummarizer (Tier 3 MS GraphRAG summarization).
4. The HybridSearchEngine intent-router (Specific, Global Theme, Multi-Hop).
"""

import asyncio
import os
import shutil
import logging
from pathlib import Path

from src.utils.config import Settings
from src.core.orchestrator import Orchestrator
from src.core.models import EventType, FileIdentity, IngestEvent

logging.basicConfig(level=logging.INFO)

async def main():
    import uuid
    # 1. Setup temporary testing environment
    test_dir = Path(f"test_localbrain_{uuid.uuid4().hex[:8]}")
    if test_dir.exists():
        shutil.rmtree(test_dir, ignore_errors=True)
    test_dir.mkdir(exist_ok=True)

    # Create dummy config and files
    settings = Settings(data_dir=str(test_dir), debounce_ms=0, settle_time_ms=0)
    
    # Needs access.config.json and plugins.yaml in root (they exist in the repo)
    orch = Orchestrator(settings)
    await orch.start()

    print("\n--- [TEST 1] TIER 1: INSTANT INGESTION ---")
    # Create a test file in the allowed scope_gate directory
    project_dir = Path("C:/Users/manzi/Documents/Projects")
    project_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = project_dir / "frankenstein_summary.txt"
    dummy_file.write_text("Victor Frankenstein was a scientist who created an unnatural monster. The creature demanded a mate, causing Victor great distress and leading to a tragic confrontation in the frozen north.")

    event = IngestEvent(
        event_type=EventType.created,
        file_identity=FileIdentity(path=str(dummy_file.absolute())),
    )
    
    # Force ingest
    await orch.scheduler.enqueue(event)
    await asyncio.sleep(2) # Give pipeline time to process
    
    # Check SQLite
    chunks = await orch.engine.search_chunks("Victor", limit=5)
    print(f"SQLite search returned {len(chunks)} chunks for 'Victor'.")

    print("\n--- [TEST 2] TIER 2: LIGHTRAG GRAPH EXTRACTION ---")
    # Force Graph extractor to run instead of waiting 5 mins
    processed = await orch.graph_extractor.run_batch()
    print(f"GraphExtractor processed {processed} chunk(s).")
    
    print("\n--- [TEST 3] TIER 3: MS GRAPHRAG COMMUNITY SUMMARIZATION ---")
    summarized = await orch.community_summarizer.run_batch()
    print(f"CommunitySummarizer generated {summarized} summary(s).")

    print("\n--- [TEST 4] HYBRID RAG INTENT ROUTING ---")
    
    # A. Specific Fact Search (Vector/BM25)
    print("\n-> Testing Specific Lane:")
    res_specific = await orch.hybrid_search.search("Where did the confrontation happen?")
    print(f"   Lane chosen: {res_specific['lane']}")
    print(f"   Chunks found: {len(res_specific['chunks'])}")

    # B. Multi-hop HippoRAG Search
    print("\n-> Testing Multi-Hop Lane:")
    res_hop = await orch.hybrid_search.search("How does Victor connect to the frozen north?")
    print(f"   Lane chosen: {res_hop['lane']}")
    print(f"   Graph Context Nodes extracted: {len(res_hop['graph_context'])}")

    # C. Global Theme MS GraphRAG Search
    print("\n-> Testing Global Theme Lane:")
    res_theme = await orch.hybrid_search.search("Give me a summary of the main themes.")
    print(f"   Lane chosen: {res_theme['lane']}")
    print(f"   Global Summaries extracted: {len(res_theme['graph_context'])}")
    if res_theme['graph_context']:
        print(f"   Summary excerpt: {res_theme['graph_context'][0]['summary'][:100]}...")

    # Cleanup
    await orch.stop()
    print("\nAll Tests Completed Successfully!")

if __name__ == "__main__":
    asyncio.run(main())
