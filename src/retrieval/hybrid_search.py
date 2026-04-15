"""Hybrid Retrieval Engine w/ Intent Routing.

Implements the 'Holy Grail' intent-based RAG search:
1. LanceDB (Dense Vector Search) + SQLite FTS (BM25 Keyword Search)
2. Intent Router (Specific vs Multi-Hop vs Global Theme)
3. HippoRAG (Personalized PageRank traversal over KuzuDB for complex reasoning)
4. MS GraphRAG (Retrieving lazily generated Community Summaries)
"""

from typing import List, Dict, Any
from collections import defaultdict
import logging
import re

from src.utils.config import Settings
from src.vault.sqlite_engine import SQLiteEngine
from src.vault.lance_engine import LanceEngine
from src.vault.kuzu_store import KuzuStore

logger = logging.getLogger(__name__)

class HybridSearchEngine:
    def __init__(self, sqlite: SQLiteEngine, lance: LanceEngine, kuzu: KuzuStore, settings: Settings, rrf_k: int = 60) -> None:
        self.sqlite = sqlite
        self.lance = lance
        self.kuzu = kuzu
        self.settings = settings
        self.rrf_k = rrf_k

    def _determine_intent(self, query: str) -> str:
        """Simple NLP heuristic router."""
        q = query.lower()
        
        # 1. Look for MS GraphRAG Global Theme Intent
        if self.settings.enable_ms_graphrag_summarization:
            if any(w in q for w in ["summarize", "summary", "main themes", "overview", "big picture"]):
                return "global_theme"
                
        # 2. Look for HippoRAG Multi-Hop Intent
        if self.settings.enable_hipporag_pagerank and self.settings.enable_graphrag:
            if any(w in q for w in ["relate", "connect", "influence", "affect", "how does", "path between", "relationship"]):
                return "multi_hop"
                
        # 3. Default Specific Fact / Standard hybrid RAG
        return "specific"

    async def search(self, query: str, query_embedding: List[float] = None, limit: int = 10) -> Dict[str, Any]:
        """Route the query to the best RAG algorithmic lane."""
        
        intent = self._determine_intent(query)
        logger.info("HybridSearchEngine: Routed query '%s' to lane '%s'", query, intent)
        
        # --- Microsoft GraphRAG Global Community Lane ---
        if intent == "global_theme":
            summaries = []
            try:
                # Naive match for our summary nodes
                q = "MATCH (e:Entity) WHERE e.lbl_type = 'Community Summary' RETURN e.id, e.description"
                res = self.kuzu._conn.execute(q)
                while res.has_next():
                    row = res.get_next()
                    summaries.append({
                        "summary_id": row[0],
                        "summary": row[1]
                    })
            except Exception as e:
                logger.error("KuzuDB summary retrieval failed: %s", e)
                
            return {
                "query": query,
                "lane": "global_theme",
                "chunks": [], # We bypass the chunks entirely for high level overviews!
                "graph_context": summaries
            }
            
        # --- Standard Vector/BM25 Setup ---
        vector_results = []
        if query_embedding:
            try:
                vector_results = await self.lance.search(query_embedding, limit=limit)
            except Exception as e:
                logger.error("LanceDB search failed: %s", e)

        keyword_results = []
        if query.strip():
            try:
                keyword_results = await self.sqlite.search_chunks(query, limit=limit)
            except Exception as e:
                logger.error("SQLite FTS search failed: %s", e)

        # Merge using Reciprocal Rank Fusion (RRF)
        rrf_scores = defaultdict(float)
        items = {}
        for rank, res in enumerate(vector_results, start=1):
            chunk_id = res['id']
            rrf_scores[chunk_id] += 1.0 / (self.rrf_k + rank)
            if chunk_id not in items:
                items[chunk_id] = {"chunk_id": chunk_id, "content": res["content"], "vector_score": res["score"], "keyword_score": 0.0}

        for rank, res in enumerate(keyword_results, start=1):
            chunk_id = res['chunk_id']
            rrf_scores[chunk_id] += 1.0 / (self.rrf_k + rank)
            if chunk_id not in items:
                items[chunk_id] = {"chunk_id": chunk_id, "content": res["content"], "vector_score": 0.0, "keyword_score": res["score"]}
            else:
                items[chunk_id]["keyword_score"] = res["score"]

        sorted_chunks = sorted([(chunk_id, rrf_scores[chunk_id]) for chunk_id in rrf_scores], key=lambda x: x[1], reverse=True)[:limit]
        final_chunks = [dict(items[c_id], rrf_score=score) for c_id, score in sorted_chunks]

        # --- HippoRAG Multi-Hop PageRank Lane ---
        graph_context = []
        if intent == "multi_hop":
            try:
                entities = self.kuzu.get_all_entities()
                # Find "Query Nodes" (matches in the text)
                matched_entities = [e for e in entities if e["name"].lower() in query.lower()]
                
                # If we were using actual PageRank via KuzuDB Graph Algorithms, we would propagate 
                # scores from these matched_nodes. For now we mock it with a 2-hop spread.
                for ent in matched_entities:
                    related = self.kuzu.get_context_for_entity(ent["id"], hop_limit=2)
                    graph_context.append({
                        "hippo_query_node": ent["name"],
                        "multi_hop_pagerank_results": related
                    })
                    
            except Exception as e:
                logger.error("Kuzu Graph HippoRAG extraction failed: %s", e)

        return {
            "query": query,
            "lane": intent,
            "chunks": final_chunks,
            "graph_context": graph_context
        }
