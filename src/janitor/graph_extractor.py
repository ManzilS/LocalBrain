"""Graph extraction worker for background processing.

Runs in the background, continuously finding chunks that haven't been distilled
into the KuzuDB graph. It reads the chunks, simulates passing them to
an LLM (OpenMemoryLabs' Router), and extracts Entities + Relationships
to build the GraphRAG Context.
"""

import asyncio
import logging
import uuid
import re
from typing import List

from src.vault.sqlite_engine import SQLiteEngine
from src.vault.kuzu_store import KuzuStore
from src.utils.config import Settings

logger = logging.getLogger(__name__)

class GraphExtractor:
    def __init__(self, sqlite: SQLiteEngine, kuzu: KuzuStore, settings: Settings, batch_size: int = 5):
        self.sqlite = sqlite
        self.kuzu = kuzu
        self.settings = settings
        self.batch_size = batch_size
        self._processed = set()  # In memory tracking for simplicity. In prod, use a column in sqlite

    async def run_batch(self) -> int:
        """Find pending chunks and mock extract logic."""
        if not self.settings.enable_graphrag or not self.settings.enable_lightrag_incremental:
            return 0
            
        # We would typically do a SELECT on chunks that don't have a 'graph_extracted' flag
        # For demonstration of the "slower background method", we just pick random chunks.
        chunks_fp = await self.sqlite.get_all_chunk_fingerprints()
        pending = [c for c in chunks_fp if c not in self._processed][:self.batch_size]

        if not pending:
            return 0

        for fp in pending:
            chunk = await self.sqlite.get_chunk_by_fingerprint(fp)
            if not chunk or not chunk.content:
                self._processed.add(fp)
                continue
                
            # Simulate a slow "LLM Router" distillation (GraphRAG Extraction)
            await asyncio.sleep(0.5)

            # Heuristic Entity Extraction (since no real LLM API is available right now)
            # Find capitalized words > 4 chars as dummy entities
            words = re.findall(r'\b[A-Z][a-z]{3,}\b', chunk.content)
            entities = list(set(words))[:3]
            
            # Insert the chunk into graph
            self.kuzu.upsert_chunk(chunk.id, chunk.content)

            # Insert entities
            for ent in entities:
                ent_id = f"ent_{ent.lower()}"
                self.kuzu.upsert_entity(ent_id, ent, "Concept", f"Extracted from {chunk.id}")
                self.kuzu.link_entity_to_chunk(ent_id, chunk.id)
                logger.debug("GraphExtractor: Added Entity '%s'", ent)
                
            # Insert relationships between the found entities
            if len(entities) > 1:
                # connect first two
                a, b = entities[0], entities[1]
                self.kuzu.add_relationship(f"ent_{a.lower()}", f"ent_{b.lower()}", "Co-occurs in chunk", 0.8)

            self._processed.add(fp)
            
        logger.info("GraphExtractor: distilled %d chunks to KuzuDB", len(pending))
        return len(pending)
