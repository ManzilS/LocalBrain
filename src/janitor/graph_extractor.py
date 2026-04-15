"""Incremental GraphRAG extraction worker.

Walks ``chunks`` where ``graph_extracted_at IS NULL`` and pushes each
through a pluggable :class:`EntityExtractor` into the Kuzu graph.
Completion is persisted in SQLite, so restarts don't reprocess every
chunk.

The *quality* of the resulting graph is 100 % a function of the
extractor you plug in. The default :class:`HeuristicExtractor` is a
regex stub — fine for demos, not for real retrieval.
"""

from __future__ import annotations

import logging

from src.retrieval.extractors import EntityExtractor, ExtractionResult, HeuristicExtractor
from src.utils.config import Settings
from src.vault.kuzu_store import KuzuStore, entity_id
from src.vault.sqlite_engine import SQLiteEngine

logger = logging.getLogger(__name__)


class GraphExtractor:
    def __init__(
        self,
        sqlite: SQLiteEngine,
        kuzu: KuzuStore,
        settings: Settings,
        extractor: EntityExtractor | None = None,
    ) -> None:
        self.sqlite = sqlite
        self.kuzu = kuzu
        self.settings = settings
        self.extractor: EntityExtractor = extractor or HeuristicExtractor()

    async def run_batch(self) -> int:
        if not (
            self.settings.enable_graphrag
            and self.settings.enable_lightrag_incremental
        ):
            return 0

        pending = await self.sqlite.get_chunks_pending_graph_extraction(
            limit=self.settings.graph_extract_batch_size
        )
        if not pending:
            return 0

        processed_ids: list[str] = []
        for chunk in pending:
            if not chunk.content:
                processed_ids.append(chunk.id)  # nothing to do — still mark done
                continue
            try:
                result = await self.extractor.extract(chunk.id, chunk.content)
            except Exception:
                logger.exception(
                    "Extractor %r failed on chunk %s", self.extractor.name, chunk.id
                )
                # Don't mark done — let the next run retry.
                continue

            await self._persist(chunk.id, chunk.content, result)
            processed_ids.append(chunk.id)

        await self.sqlite.mark_graph_extracted(processed_ids)
        logger.info(
            "GraphExtractor[%s]: processed %d chunk(s)",
            self.extractor.name,
            len(processed_ids),
        )
        return len(processed_ids)

    async def _persist(
        self, chunk_id: str, content: str, result: ExtractionResult
    ) -> None:
        await self.kuzu.upsert_chunk(chunk_id, content)

        # Map extractor-level names → stable entity IDs.
        name_to_id: dict[str, str] = {}
        for ent in result.entities:
            eid = entity_id(ent.name, ent.entity_type)
            name_to_id[ent.name] = eid
            await self.kuzu.upsert_entity(eid, ent.name, ent.entity_type, ent.description)
            await self.kuzu.link_entity_to_chunk(eid, chunk_id)

        for rel in result.relations:
            src = name_to_id.get(rel.source) or entity_id(rel.source)
            dst = name_to_id.get(rel.target) or entity_id(rel.target)
            if src == dst:
                continue
            await self.kuzu.add_relationship(src, dst, rel.description, rel.weight)
