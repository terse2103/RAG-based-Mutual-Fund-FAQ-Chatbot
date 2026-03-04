"""
Phase 4 — RAG Chain
=====================
`RAGChain` is the single entry-point for the full query pipeline:

    query (str)
        → QueryProcessor.analyse()          [fund + intent detection]
        → RAGRetriever.retrieve()           [vector search + threshold filter]
        → _build_prompt()                   [assemble system + user messages]
        → (Phase 6 hook) generate_fn()      [LLM call — pluggable]
        → result dict

This module intentionally contains NO LLM code.  The `generate_fn`
parameter is a callable injected by Phase 6 so the pipeline stays
testable without any API credentials.

When `generate_fn` is ``None`` (the default) the chain returns the raw
retrieval result — useful for end-to-end testing of retrieval quality.

Usage (standalone demo — no LLM)
---------------------------------
    python -m phase4_pipeline.rag_chain

Usage (with Phase 6 generator injected)
-----------------------------------------
    from phase4_pipeline import RAGChain
    from phase6_generation.generator import ResponseGenerator

    gen = ResponseGenerator(api_key=GROQ_API_KEY)
    chain = RAGChain(generate_fn=gen.generate)
    result = chain.run("What is the expense ratio of Nippon ELSS?")
    print(result["answer"])
"""

from __future__ import annotations

import io
import logging
import sys
from typing import Callable, Optional

from phase3_embedding.embedder import MFVectorStore
from phase4_pipeline.query_processor import QueryProcessor
from phase4_pipeline.retriever import RAGRetriever

# ---------------------------------------------------------------------------
# Fund category → fund_key mapping (mirrors config.py in phase7_frontend)
# Kept here so this module has no dependency on the frontend package.
# ---------------------------------------------------------------------------
_CATEGORY_FUND_MAP: dict[str, list[str]] = {
    "Equity":  ["nippon_elss_tax_saver", "nippon_nifty_auto_index"],
    "Debt":    ["nippon_short_duration", "nippon_crisil_ibx_aaa"],
    "Hybrid":  ["nippon_balanced_advantage"],
    "Commodity": ["nippon_silver_etf_fof"],
}

# Force UTF-8 on Windows so emoji don't crash
if sys.stdout.encoding and sys.stdout.encoding.upper() not in ("UTF-8", "UTF8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logger = logging.getLogger("phase4.rag_chain")

# ---------------------------------------------------------------------------
# Default prompt template (used when generate_fn is provided)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TEMPLATE = """\
You are a Mutual Fund FAQ Assistant for Nippon India funds listed on INDMoney.
Answer questions using ONLY the context provided below.

STRICT RULES — NEVER VIOLATE:
1. Use ONLY information from the context. If the answer is not in the context say:
   "I don't have this information." (Sources will be handled by the UI)
2. NEVER provide investment advice, recommendations, or opinions.
3. NEVER compute, calculate, or compare returns across funds.
4. Keep responses to MAXIMUM 3 sentences.
5. NEVER acknowledge personal information (PAN, Aadhaar, account numbers, OTPs).
6. Stick to FACTS: expense ratio, exit load, minimum SIP, lock-in period,
   riskometer, benchmark, NAV, AUM, fund manager, holdings.

CONTEXT:
{context}
"""


class RAGChain:
    """
    Orchestrates the full RAG query pipeline (retrieval side).

    Parameters
    ----------
    vector_store : MFVectorStore, optional
        Pre-initialised store.  If omitted a default store is created at
        ``data/vectorstore``.
    query_processor : QueryProcessor, optional
        Defaults to ``QueryProcessor()``.
    retriever : RAGRetriever, optional
        Defaults to ``RAGRetriever(vector_store, query_processor)``.
    generate_fn : callable, optional
        Signature: ``(query, context, sources, scraped_at) -> str``
        Injected by Phase 6.  When ``None`` the ``answer`` key in the
        result dict is ``None`` and the raw context is returned instead.
    vectorstore_dir : str
        Path passed to ``MFVectorStore`` when auto-creating the store.
    """

    def __init__(
        self,
        vector_store: Optional[MFVectorStore]   = None,
        query_processor: Optional[QueryProcessor] = None,
        retriever: Optional[RAGRetriever]         = None,
        generate_fn: Optional[Callable]           = None,
        vectorstore_dir: str                      = "data/vectorstore",
    ) -> None:
        self.vector_store    = vector_store    or MFVectorStore(persist_dir=vectorstore_dir)
        self.query_processor = query_processor or QueryProcessor()
        self.retriever       = retriever       or RAGRetriever(
            vector_store    = self.vector_store,
            query_processor = self.query_processor,
        )
        self.generate_fn = generate_fn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        query: str,
        fund_filter: Optional[str] = None,
        category_filter: Optional[str] = None,
    ) -> dict:
        """
        Execute the full RAG pipeline for *query*.

        Parameters
        ----------
        query           : The user's question.
        fund_filter     : A specific fund_key selected in the sidebar
                          (e.g. ``"nippon_elss_tax_saver"``).
                          When set, retrieval is hard-constrained to this fund only,
                          regardless of whether the query mentions a fund name.
        category_filter : A category name selected in the sidebar
                          (``"Equity"``, ``"Debt"``, ``"Hybrid"``).
                          When set (and fund_filter is not set), retrieval is
                          hard-constrained to all funds in that category.

        Returns
        -------
        dict with keys:
          ``query``       : str          — original query
          ``fund_keys``   : list[str]    — identified / overridden fund keys
          ``intent``      : str          — classified intent
          ``context``     : str | None   — retrieved context text
          ``sources``     : list[str]    — source URLs
          ``scraped_at``  : str | None   — latest data timestamp
          ``chunks``      : list[dict]   — raw chunk details
          ``answer``      : str | None   — LLM answer (None if no generate_fn)
          ``prompt``      : str | None   — assembled prompt (None if no context)
          ``no_results``  : bool         — True when retrieval returned nothing
        """
        logger.info(
            "RAGChain.run | query='%s' fund_filter='%s' category_filter='%s'",
            query[:80], fund_filter, category_filter,
        )

        # ── Resolve sidebar selection to forced fund keys ──────────────────
        forced_fund_keys: Optional[list] = None

        if fund_filter and fund_filter not in ("", "All Funds"):
            # A specific fund is selected — restrict to exactly that fund key
            forced_fund_keys = [fund_filter]  # fund_filter IS the fund_key (from dropdown)
            logger.info("Sidebar fund filter applied: %s", forced_fund_keys)

        elif category_filter and category_filter not in ("", "All", "All Funds"):
            # A category is selected — restrict to all funds in that category
            forced_fund_keys = _CATEGORY_FUND_MAP.get(category_filter)
            if forced_fund_keys:
                logger.info("Sidebar category filter '%s' → funds: %s", category_filter, forced_fund_keys)
            else:
                logger.warning("Unknown category_filter '%s' — ignoring.", category_filter)
                forced_fund_keys = None

        # ── Retrieve (with or without forced scope) ────────────────────────
        result = self.retriever.retrieve(query, forced_fund_keys=forced_fund_keys)

        no_results = result["context"] is None

        # --- Build prompt if we have context ---
        prompt: Optional[str] = None
        answer: Optional[str] = None

        if not no_results:
            prompt = self._build_prompt(
                context    = result["context"],
                sources    = result["sources"],
                scraped_at = result["scraped_at"],
            )
            # --- Call LLM if injected ---
            if self.generate_fn is not None:
                try:
                    answer = self.generate_fn(
                        query      = query,
                        context    = result["context"],
                        sources    = result["sources"],
                        scraped_at = result["scraped_at"],
                    )
                except Exception as exc:
                    logger.error("generate_fn raised: %s", exc)
                    answer = (
                        f"⚠️ Generation error: {exc}\n\n"
                        f"Context retrieved:\n{result['context']}"
                    )

        return {
            "query":       query,
            "fund_keys":   result.get("fund_keys", []),
            "intent":      result.get("intent"),
            "context":     result.get("context"),
            "sources":     result.get("sources", []),
            "scraped_at":  result.get("scraped_at"),
            "chunks":      result.get("chunks", []),
            "answer":      answer,
            "prompt":      prompt,
            "no_results":  no_results,
        }

    def build_prompt(self, context: str, sources: list[str], scraped_at: str) -> str:
        """Public wrapper around ``_build_prompt`` (for testing / inspection)."""
        return self._build_prompt(context, sources, scraped_at)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_prompt(
        self,
        context: str,
        sources: list[str],
        scraped_at: Optional[str],
    ) -> str:
        """Assemble the system prompt with context substituted in."""
        source_url  = sources[0] if sources else "https://www.indmoney.com"
        scraped_date = scraped_at[:10] if scraped_at else "Unknown"
        return SYSTEM_PROMPT_TEMPLATE.format(
            context      = context,
            source_url   = source_url,
            scraped_date = scraped_date,
        )




# ---------------------------------------------------------------------------
# CLI demo (no LLM — shows retrieval quality only)
# ---------------------------------------------------------------------------

def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    demo_queries = [
        "What is the expense ratio of Nippon ELSS Tax Saver Fund?",
        "What is the minimum SIP for balanced advantage fund?",
        "Tell me about the risk level of Nippon Silver ETF FOF.",
        "What is the lock-in period for ELSS tax saver fund?",
        "What is the benchmark of Nifty Auto Index Fund?",
        "Compare returns of all funds.",         # off-topic / no-advice trigger
    ]

    chain = RAGChain()   # no generate_fn → answer will be None

    sep = "=" * 70
    print(f"\n{sep}")
    print("  Phase 4 — RAG Chain Demo (retrieval only, no LLM)")
    print(sep)

    for q in demo_queries:
        print(f"\n📝 Query: {q}")
        result = chain.run(q)
        print(f"   Funds identified: {result['fund_keys'] or '(none — global search)'}")
        print(f"   Intent          : {result['intent']}")

        if result["no_results"]:
            print("   ⚠️  No relevant chunks found above threshold.")
        else:
            print(f"   Chunks retrieved: {len(result['chunks'])}")
            for i, chunk in enumerate(result["chunks"], 1):
                sim  = chunk["similarity"]
                meta = chunk["metadata"]
                preview = chunk["content"][:100].replace("\n", " ")
                print(
                    f"   [{i}] sim={sim:.4f}  type={meta['chunk_type']}"
                    f"  fund={meta['fund_key']}"
                )
                print(f"       {preview}…")
            print(f"   Sources: {result['sources']}")
            print(f"   Data timestamp: {result['scraped_at']}")

    print(f"\n{sep}")
    print("  [OK] Phase 4 pipeline demo complete.")
    print(sep + "\n")


if __name__ == "__main__":
    _demo()
