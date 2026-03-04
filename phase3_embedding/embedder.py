"""
Phase 3 — Embedding & Vector Store
===================================
MFVectorStore: wraps SentenceTransformer(all-MiniLM-L6-v2) + ChromaDB.

Key design decisions
--------------------
* Cosine similarity via HNSW index (ChromaDB default).
* `upsert` by chunk_id — re-indexing is idempotent (safe to re-run daily).
* Optional metadata filter by fund_key or chunk_type for focused retrieval.
* Batch encoding to avoid OOM on larger chunk sets.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("phase3.embedder")

COLLECTION_NAME      = "mf_faq_chunks"
DEFAULT_PERSIST_DIR  = "data/vectorstore"
EMBED_MODEL_NAME     = "all-MiniLM-L6-v2"
DEFAULT_TOP_K        = 3
BATCH_SIZE           = 32           # encode chunks in batches


class MFVectorStore:
    """
    Manages embedding, upserting, and querying of mutual-fund FAQ chunks.

    Parameters
    ----------
    persist_dir : str
        Directory where ChromaDB persists its data on disk.
    model_name : str
        SentenceTransformer model to use for encoding.
    """

    def __init__(
        self,
        persist_dir: str = DEFAULT_PERSIST_DIR,
        model_name: str = EMBED_MODEL_NAME,
    ) -> None:
        logger.info("Loading embedding model '%s' …", model_name)
        self.embed_model = SentenceTransformer(model_name)

        logger.info("Connecting to ChromaDB at '%s' …", persist_dir)
        self.client = chromadb.PersistentClient(
            path=persist_dir,
            settings=Settings(anonymized_telemetry=False),
        )

        self.collection = self.client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(
            "Collection '%s' ready — %d documents already indexed.",
            COLLECTION_NAME,
            self.collection.count(),
        )

    # ── Indexing ─────────────────────────────────────────────────────────────

    def add_chunks(self, chunks: list[Any]) -> None:
        """
        Embed *chunks* (list of DocumentChunk dataclass instances or dicts)
        and upsert them into the vector store.
        """
        if not chunks:
            logger.warning("add_chunks called with empty list — nothing to index.")
            return

        # Accept both dataclass instances and plain dicts (from JSON)
        def _get(obj, key):
            return obj[key] if isinstance(obj, dict) else getattr(obj, key)

        ids       = [_get(c, "chunk_id")   for c in chunks]
        texts     = [_get(c, "content")    for c in chunks]
        metadatas = [
            {
                "fund_name":  _get(c, "fund_name"),
                "fund_key":   _get(c, "fund_key"),
                "source_url": _get(c, "source_url"),
                "chunk_type": _get(c, "chunk_type"),
                "scraped_at": _get(c, "scraped_at"),
            }
            for c in chunks
        ]

        # Batch encode
        logger.info("Encoding %d chunks in batches of %d …", len(texts), BATCH_SIZE)
        all_embeddings: list[list[float]] = []
        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i : i + BATCH_SIZE]
            vecs  = self.embed_model.encode(batch, show_progress_bar=False).tolist()
            all_embeddings.extend(vecs)

        self.collection.upsert(
            ids=ids,
            embeddings=all_embeddings,
            documents=texts,
            metadatas=metadatas,
        )
        logger.info(
            "Upserted %d chunks — collection now has %d documents.",
            len(chunks),
            self.collection.count(),
        )

    # ── Querying ─────────────────────────────────────────────────────────────

    def query(
        self,
        query_text: str,
        top_k: int = DEFAULT_TOP_K,
        filter_fund: Optional[str] = None,
        filter_chunk_type: Optional[str] = None,
    ) -> dict:
        """
        Embed *query_text*, search the collection, return raw ChromaDB result.
        """
        query_embedding = self.embed_model.encode(
            query_text, 
            show_progress_bar=False
        ).tolist()

        # Build optional metadata where-filter
        where_filter: Optional[dict] = None
        if filter_fund and filter_chunk_type:
            where_filter = {
                "$and": [
                    {"fund_key": {"$eq": filter_fund}},
                    {"chunk_type": {"$eq": filter_chunk_type}},
                ]
            }
        elif filter_fund:
            where_filter = {"fund_key": {"$eq": filter_fund}}
        elif filter_chunk_type:
            where_filter = {"chunk_type": {"$eq": filter_chunk_type}}

        result = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=min(top_k, self.collection.count()),
            where=where_filter,
            include=["documents", "metadatas", "distances"],
        )
        return result

    # ── Utilities ─────────────────────────────────────────────────────────────

    def count(self) -> int:
        """Return total number of indexed documents."""
        return self.collection.count()

    def delete_collection(self) -> None:
        """Drop and recreate the collection (full re-index)."""
        logger.warning("Deleting collection '%s' …", COLLECTION_NAME)
        self.client.delete_collection(COLLECTION_NAME)
        self.collection = self.client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info("Collection recreated (empty).")

    def get_all_metadata(self) -> list[dict]:
        """Return metadata of every indexed document (for inspection)."""
        result = self.collection.get(include=["metadatas"])
        return result.get("metadatas", [])
