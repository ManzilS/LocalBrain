"""KuzuDB state engine — handles the GraphRAG Context (Nodes, Relationships).

Manages Entities and their relationships extracted from chunks, facilitating
multi-hop traversal to enrich the retrieval context.
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any

import kuzu

logger = logging.getLogger(__name__)

class KuzuStore:
    """KuzuDB wrapper for the LocalBrain graph vault."""

    def __init__(self, db_path: str) -> None:
        self._path = db_path
        self._db: kuzu.Database | None = None
        self._conn: kuzu.Connection | None = None

    def open(self) -> None:
        """Initialize the database and connection."""
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._db = kuzu.Database(self._path)
        self._conn = kuzu.Connection(self._db)
        self._ensure_schema()
        logger.info("Kuzu vault opened: %s", self._path)

    def close(self) -> None:
        """Close the database and connection."""
        if self._db:
            # kuzu does not require explicit db close, just dropping references
            self._conn = None
            self._db = None

    def _ensure_schema(self) -> None:
        """Create Node and Rel tables if they do not exist."""
        assert self._conn is not None

        # Node Tables
        try:
            self._conn.execute("CREATE NODE TABLE Entity (id STRING, name STRING, lbl_type STRING, description STRING, PRIMARY KEY (id))")
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise
        
        try:
            self._conn.execute("CREATE NODE TABLE Chunk (id STRING, content STRING, PRIMARY KEY (id))")
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise

        # Rel Tables
        try:
            self._conn.execute("CREATE REL TABLE Relates_To (FROM Entity TO Entity, description STRING, weight DOUBLE)")
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise

        try:
            self._conn.execute("CREATE REL TABLE Extracted_From (FROM Entity TO Chunk)")
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise

    def upsert_entity(self, entity_id: str, name: str, entity_type: str, description: str) -> None:
        """Insert or update an Entity node."""
        assert self._conn is not None
        self._conn.execute("MERGE (e:Entity {id: $id})", {"id": entity_id})
        self._conn.execute(
            "MATCH (e:Entity {id: $id}) SET e.name = $name, e.lbl_type = $type, e.description = $description_val",
            {"id": entity_id, "name": name, "type": entity_type, "description_val": description}
        )

    def upsert_chunk(self, chunk_id: str, content: str) -> None:
        """Insert a Chunk node."""
        assert self._conn is not None
        self._conn.execute("MERGE (c:Chunk {id: $id})", {"id": chunk_id})
        self._conn.execute("MATCH (c:Chunk {id: $id}) SET c.content = $content", {"id": chunk_id, "content": content})

    def add_relationship(self, source_id: str, target_id: str, description: str = "", weight: float = 1.0) -> None:
        """Add a relationship between two Entities."""
        assert self._conn is not None
        self._conn.execute("MATCH (a:Entity {id: $source}), (b:Entity {id: $target}) MERGE (a)-[r:Relates_To]->(b)", {"source": source_id, "target": target_id})
        self._conn.execute("MATCH (a:Entity {id: $source})-[r:Relates_To]->(b:Entity {id: $target}) SET r.description = $description_val, r.weight = $weight", 
                           {"source": source_id, "target": target_id, "description_val": description, "weight": weight})

    def link_entity_to_chunk(self, entity_id: str, chunk_id: str) -> None:
        """Link an Entity to the Chunk it was extracted from."""
        assert self._conn is not None
        query = """
        MATCH (e:Entity {id: $entity}), (c:Chunk {id: $chunk})
        MERGE (e)-[:Extracted_From]->(c)
        """
        self._conn.execute(query, {"entity": entity_id, "chunk": chunk_id})

    def get_context_for_entity(self, entity_id: str, hop_limit: int = 1) -> List[Dict[str, Any]]:
        """Traverse the graph from an entity to find related contextual information."""
        assert self._conn is not None
        # Traverses up to hop_limit to find connected entities
        query = f"""
        MATCH (a:Entity {{id: $id}})-[r:Relates_To*1..{hop_limit}]-(b:Entity)
        RETURN b.id AS id, b.name AS name, b.description AS description
        LIMIT 50
        """
        results = self._conn.execute(query, {"id": entity_id})
        context = []
        while results.has_next():
            row = results.get_next()
            context.append({
                "id": row[0],
                "name": row[1],
                "description": row[2]
            })
        return context

    def get_all_entities(self) -> List[Dict[str, Any]]:
        """Return all entities."""
        assert self._conn is not None
        query = "MATCH (e:Entity) RETURN e.id, e.name, e.lbl_type, e.description"
        results = self._conn.execute(query)
        entities = []
        while results.has_next():
            row = results.get_next()
            entities.append({
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "description": row[3]
            })
        return entities
