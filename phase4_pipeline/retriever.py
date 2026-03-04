"""
Phase 4 — RAG Retriever
========================
`RAGRetriever` wraps `MFVectorStore` and `QueryProcessor` to:

  1. Analyse the query (fund identification + intent).
  2. Hit ChromaDB with an optional metadata filter.
  3. Filter out low-similarity results below a configurable threshold.
  4. Return a structured retrieval result ready for context building.

Design notes
------------
* Cosine distance returned by ChromaDB is in [0, 2]; similarity = 1 - dist.
  The RELEVANCE_THRESHOLD is applied on the similarity scale.
* When no results pass the threshold, a structured "no-context" dict is
  returned (NOT an exception) so callers can handle it gracefully.
* `RAGRetriever` does NOT depend on the LLM — it is purely retrieval logic.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from phase3_embedding.embedder import MFVectorStore
from phase4_pipeline.query_processor import QueryProcessor

if TYPE_CHECKING:
    pass

logger = logging.getLogger("phase4.retriever")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RELEVANCE_THRESHOLD = 0.35   # minimum cosine similarity to include a chunk
TOP_K               = 3      # default number of candidates to retrieve


class RAGRetriever:
    """
    Retrieves relevant chunks from the vector store for a given user query.

    Parameters
    ----------
    vector_store : MFVectorStore
        Initialised Phase 3 vector store.
    query_processor : QueryProcessor, optional
        Defaults to a new ``QueryProcessor()`` instance.
    top_k : int
        How many candidates to request from ChromaDB.
    relevance_threshold : float
        Minimum cosine similarity [0, 1] to keep a result.
    """

    def __init__(
        self,
        vector_store: MFVectorStore,
        query_processor: Optional[QueryProcessor] = None,
        top_k: int = TOP_K,
        relevance_threshold: float = RELEVANCE_THRESHOLD,
    ) -> None:
        self.vector_store        = vector_store
        self.query_processor     = query_processor or QueryProcessor()
        self.top_k               = top_k
        self.relevance_threshold = relevance_threshold

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve(self, query: str, forced_fund_keys: Optional[list] = None) -> dict:
        """
        Main retrieval method.

        Parameters
        ----------
        query : str
            Raw user question.
        forced_fund_keys : list[str] | None
            When provided (e.g. from sidebar selection), these fund keys are used
            as a hard filter on ChromaDB — overriding auto-detection from the
            query text. The global-search fallback is suppressed so that responses
            stay strictly within the selected fund(s) / category.

        Returns
        -------
        dict with keys:
          ``context``    : str | None  — newline-joined chunk texts (None if no results)
          ``sources``    : list[str]   — unique source URLs
          ``scraped_at`` : str         — latest scraped_at timestamp among results
          ``chunks``     : list[dict]  — full per-chunk details (content, metadata, similarity)
          ``fund_keys``  : list[str]   — list of identified / forced funds
          ``intent``     : str         — classified intent
          ``message``    : str | None  — set only when no relevant results found
        """
        # Step 1 — Analyse query
        intent = self.query_processor.classify_intent(query)

        # Determine which fund keys to use for filtering
        sidebar_forced = bool(forced_fund_keys)  # True when sidebar has a selection
        if sidebar_forced:
            # Use the sidebar selection directly — do NOT auto-detect from query text
            fund_keys = forced_fund_keys
        else:
            fund_keys = self.query_processor.identify_funds(query)

        logger.info(
            "Retrieving for query='%s...' funds=%s intent=%s forced=%s",
            query[:60], fund_keys, intent, sidebar_forced,
        )

        # Step 2 — Build ChromaDB where-filter
        # When the sidebar has forced a scope, always restrict by fund key(s).
        # Intent sub-filtering is only applied for single-fund queries with a
        # specific intent (not general/overview).
        where_filter: Optional[dict] = None
        if len(fund_keys) == 1:
            fund_key = fund_keys[0]
            if not sidebar_forced and intent not in ("general", "overview"):
                # Auto-detected single fund: also filter by intent chunk type
                where_filter = {
                    "$and": [
                        {"fund_key": {"$eq": fund_key}},
                        {"chunk_type": {"$eq": intent}},
                    ]
                }
            else:
                where_filter = {"fund_key": {"$eq": fund_key}}
        elif len(fund_keys) > 1:
            where_filter = {"fund_key": {"$in": fund_keys}}
        # else: no fund filter at all (all-fund global search)

        raw = self.vector_store.collection.query(
            query_embeddings=[self.vector_store.embed_model.encode(query).tolist()],
            n_results=self.top_k,
            where=where_filter,
            include=["documents", "metadatas", "distances"],
        )

        # Step 3 — Apply relevance threshold
        filtered = self._filter_by_threshold(raw)

        # If filtered query had no results AND this was NOT a sidebar-forced scope,
        # fall back to a global search so we still try to answer.
        # When sidebar forced, we deliberately do NOT fall back — the user wants
        # answers scoped to their selection only.
        if not filtered and where_filter is not None and not sidebar_forced:
            logger.debug("Filtered search for %s returned no results — retrying globally.", where_filter)
            raw = self.vector_store.query(query_text=query, top_k=self.top_k)
            filtered = self._filter_by_threshold(raw)

        # Step 4 — Build result dict
        if not filtered:
            return {
                "context":    None,
                "sources":    [],
                "scraped_at": None,
                "chunks":     [],
                "fund_keys":  fund_keys,
                "intent":     intent,
                "message":    "No relevant information found.",
            }

        context    = "\n\n".join(c["content"] for c in filtered)
        sources    = list(dict.fromkeys(c["metadata"]["source_url"] for c in filtered))
        scraped_at = max(c["metadata"]["scraped_at"] for c in filtered)

        return {
            "context":    context,
            "sources":    sources,
            "scraped_at": scraped_at,
            "chunks":     filtered,
            "fund_keys":  fund_keys,
            "intent":     intent,
            "message":    None,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _filter_by_threshold(self, raw_result: dict) -> list[dict]:
        """
        Convert raw ChromaDB distances → similarities and filter below threshold.

        Returns a list of dicts: ``{content, metadata, similarity}``.
        """
        if not raw_result.get("documents") or not raw_result["documents"][0]:
            return []

        filtered: list[dict] = []
        for doc, meta, dist in zip(
            raw_result["documents"][0],
            raw_result["metadatas"][0],
            raw_result["distances"][0],
        ):
            similarity = 1.0 - dist
            if similarity >= self.relevance_threshold:
                filtered.append({
                    "content":    doc,
                    "metadata":   meta,
                    "similarity": round(similarity, 4),
                })

        return filtered
