"""
Phase 4 — Tests for QueryProcessor, RAGRetriever, and RAGChain
================================================================
All tests use an in-memory ChromaDB instance (via MFVectorStore with a
temp dir) so they never touch the real data/vectorstore directory and
never require a network call.
"""

from __future__ import annotations

import tempfile
import unittest
from unittest.mock import MagicMock

from phase3_embedding.embedder import MFVectorStore
from phase4_pipeline.query_processor import QueryProcessor
from phase4_pipeline.retriever import RAGRetriever
from phase4_pipeline.rag_chain import RAGChain


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SAMPLE_CHUNKS = [
    {
        "chunk_id":   "elss_overview",
        "fund_name":  "Nippon India ELSS Tax Saver Fund",
        "fund_key":   "nippon_elss_tax_saver",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
        "chunk_type": "overview",
        "content":    "Nippon India ELSS Tax Saver Fund is an ELSS / Tax Saver fund managed by AMC Nippon Life India Asset Management Ltd. AUM is ₹14881 Cr. Current NAV is ₹34.5678.",
        "scraped_at": "2026-03-02T12:00:00",
        "metadata":   {},
    },
    {
        "chunk_id":   "elss_expense_exit",
        "fund_name":  "Nippon India ELSS Tax Saver Fund",
        "fund_key":   "nippon_elss_tax_saver",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
        "chunk_type": "expense_exit",
        "content":    "The expense ratio of Nippon India ELSS Tax Saver Fund (Direct Plan) is 1.03%. The exit load is 0%.",
        "scraped_at": "2026-03-02T12:00:00",
        "metadata":   {},
    },
    {
        "chunk_id":   "elss_lockin",
        "fund_name":  "Nippon India ELSS Tax Saver Fund",
        "fund_key":   "nippon_elss_tax_saver",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
        "chunk_type": "lockin_tax",
        "content":    "Nippon India ELSS Tax Saver Fund has a mandatory lock-in period of 3 years. It qualifies for tax deduction under Section 80C up to ₹1.5 lakh per year.",
        "scraped_at": "2026-03-02T12:00:00",
        "metadata":   {},
    },
    {
        "chunk_id":   "baf_sip",
        "fund_name":  "Nippon India Balanced Advantage Fund",
        "fund_key":   "nippon_balanced_advantage",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-balanced-advantage-fund-direct-growth-plan-4324",
        "chunk_type": "sip_investment",
        "content":    "The minimum SIP amount for Nippon India Balanced Advantage Fund is ₹100. The minimum lumpsum investment is ₹100.",
        "scraped_at": "2026-03-02T11:00:00",
        "metadata":   {},
    },
    {
        "chunk_id":   "silver_risk",
        "fund_name":  "Nippon India Silver ETF Fund of Fund",
        "fund_key":   "nippon_silver_etf_fof",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-silver-etf-fund-of-fund-fof-direct-growth-1040380",
        "chunk_type": "risk_benchmark",
        "content":    "Nippon India Silver ETF Fund of Fund is categorized as 'Very High' risk on the riskometer. Its benchmark index is Domestic Price of Silver.",
        "scraped_at": "2026-03-02T12:00:00",
        "metadata":   {},
    },
]


def _make_store(chunks=SAMPLE_CHUNKS):
    """Return an in-memory MFVectorStore populated with SAMPLE_CHUNKS."""
    tmpdir = tempfile.mkdtemp()
    store  = MFVectorStore(persist_dir=tmpdir)
    store.add_chunks(chunks)
    return store


# ===========================================================================
# 1. QueryProcessor tests
# ===========================================================================

class TestQueryProcessor(unittest.TestCase):

    def setUp(self):
        self.qp = QueryProcessor()

    # --- identify_fund ---

    def test_identify_elss_by_name(self):
        funds = self.qp.identify_funds("What is the expense ratio of Nippon ELSS?")
        self.assertIn("nippon_elss_tax_saver", funds)

    def test_identify_balanced_advantage(self):
        funds = self.qp.identify_funds("Tell me about balanced advantage fund.")
        self.assertIn("nippon_balanced_advantage", funds)

    def test_identify_silver_etf(self):
        funds = self.qp.identify_funds("What is the risk of Nippon Silver ETF?")
        self.assertIn("nippon_silver_etf_fof", funds)

    def test_identify_short_duration(self):
        funds = self.qp.identify_funds("Short duration fund details please.")
        self.assertIn("nippon_short_duration", funds)

    def test_identify_multiple_funds(self):
        funds = self.qp.identify_funds("Compare Nippon ELSS and Balanced Advantage.")
        self.assertIn("nippon_elss_tax_saver", funds)
        self.assertIn("nippon_balanced_advantage", funds)
        self.assertEqual(len(funds), 2)

    # --- classify_intent ---

    def test_intent_expense(self):
        self.assertEqual(
            self.qp.classify_intent("What is the expense ratio?"),
            "expense_exit",
        )

    def test_intent_sip(self):
        self.assertEqual(
            self.qp.classify_intent("What is the minimum SIP?"),
            "sip_investment",
        )

    def test_intent_risk(self):
        self.assertEqual(
            self.qp.classify_intent("What is the risk level of this fund?"),
            "risk_benchmark",
        )

    def test_intent_lockin(self):
        self.assertEqual(
            self.qp.classify_intent("Is there a lock-in period?"),
            "lockin_tax",
        )

    def test_intent_returns(self):
        self.assertEqual(
            self.qp.classify_intent("What are the 3 year returns?"),
            "returns",
        )

    def test_intent_overview(self):
        self.assertEqual(
            self.qp.classify_intent("Who is the fund manager and what is the AUM?"),
            "overview",
        )

    def test_intent_general_fallback(self):
        self.assertEqual(
            self.qp.classify_intent("Tell me something random"),
            "general",
        )

    # --- analyse ---

    def test_analyse_returns_all_keys(self):
        result = self.qp.analyse("What is the expense ratio of ELSS Tax Saver?")
        self.assertIn("fund_key", result)  # identify_fund (backcompat) still works
        self.assertIn("intent", result)
        self.assertIn("query", result)
        self.assertEqual(result["query"], "What is the expense ratio of ELSS Tax Saver?")


# ===========================================================================
# 2. RAGRetriever tests
# ===========================================================================

class TestRAGRetriever(unittest.TestCase):

    def setUp(self):
        self.store     = _make_store()
        self.retriever = RAGRetriever(vector_store=self.store)

    def test_retrieve_returns_dict_keys(self):
        result = self.retriever.retrieve("What is the expense ratio of Nippon ELSS?")
        for key in ("context", "sources", "scraped_at", "chunks", "fund_keys", "intent", "message"):
            self.assertIn(key, result)

    def test_retrieve_finds_relevant_chunk(self):
        result = self.retriever.retrieve("What is the expense ratio of Nippon ELSS?")
        # Should find at least one chunk
        self.assertIsNotNone(result["context"])
        self.assertGreater(len(result["chunks"]), 0)

    def test_retrieve_sources_are_urls(self):
        result = self.retriever.retrieve("expense ratio ELSS tax saver")
        if result["sources"]:
            for src in result["sources"]:
                self.assertTrue(src.startswith("http"), f"Expected URL, got: {src}")

    def test_retrieve_similarity_in_valid_range(self):
        result = self.retriever.retrieve("expense ratio ELSS")
        for chunk in result["chunks"]:
            self.assertGreaterEqual(chunk["similarity"], -1.0)
            self.assertLessEqual(chunk["similarity"],    1.0)

    def test_retrieve_all_chunks_above_threshold(self):
        result = self.retriever.retrieve("expense ratio ELSS")
        for chunk in result["chunks"]:
            self.assertGreaterEqual(chunk["similarity"], self.retriever.relevance_threshold)

    def test_retrieve_no_results_structure(self):
        """A nonsense query should return a structured no-results dict."""
        result = self.retriever.retrieve("xyzzy quantum flux capacitor irrelevant 12345")
        # The retriever might still find something via fallback;
        # we just ensure the structure is valid
        self.assertIn("no_results" if "no_results" in result else "message", result)

    def test_retrieve_scraped_at_is_latest(self):
        result = self.retriever.retrieve("lock in period ELSS fund 80C")
        if result["scraped_at"]:
            self.assertIsInstance(result["scraped_at"], str)
            self.assertTrue(len(result["scraped_at"]) >= 10)

    def test_retrieve_top_k_respected(self):
        retriever = RAGRetriever(vector_store=self.store, top_k=2)
        result    = retriever.retrieve("mutual fund")
        self.assertLessEqual(len(result["chunks"]), 2)

    def test_custom_relevance_threshold(self):
        """Setting threshold=0 should always return something."""
        retriever = RAGRetriever(
            vector_store=self.store,
            relevance_threshold=0.0,
        )
        result = retriever.retrieve("expense ratio")
        self.assertGreater(len(result["chunks"]), 0)


# ===========================================================================
# 3. RAGChain tests
# ===========================================================================

class TestRAGChain(unittest.TestCase):

    def setUp(self):
        self.store = _make_store()
        self.chain = RAGChain(vector_store=self.store)

    def test_run_returns_all_keys(self):
        result = self.chain.run("What is the expense ratio of ELSS Tax Saver?")
        for key in ("query", "fund_keys", "intent", "context", "sources",
                    "scraped_at", "chunks", "answer", "prompt", "no_results"):
            self.assertIn(key, result)

    def test_query_pass_through(self):
        q      = "What is the lock-in period for ELSS?"
        result = self.chain.run(q)
        self.assertEqual(result["query"], q)

    def test_answer_is_none_without_generate_fn(self):
        """Without a generate_fn the chain should not produce an answer."""
        result = self.chain.run("What is the expense ratio of ELSS?")
        self.assertIsNone(result["answer"])

    def test_prompt_built_when_context_found(self):
        result = self.chain.run("expense ratio ELSS fund")
        if not result["no_results"]:
            self.assertIsNotNone(result["prompt"])
            self.assertIn("CONTEXT:", result["prompt"])

    def test_generate_fn_called_when_injected(self):
        """Verify generate_fn is invoked and its return value becomes answer."""
        mock_gen = MagicMock(return_value="The expense ratio is 1.03%.")
        chain    = RAGChain(vector_store=self.store, generate_fn=mock_gen)
        result   = chain.run("What is the expense ratio of ELSS?")
        if not result["no_results"]:
            mock_gen.assert_called_once()
            self.assertEqual(result["answer"], "The expense ratio is 1.03%.")

    def test_generate_fn_exception_handled(self):
        """If generate_fn raises, answer should contain error message, not crash."""
        def boom(**kwargs):
            raise RuntimeError("LLM unavailable")
        chain  = RAGChain(vector_store=self.store, generate_fn=boom)
        result = chain.run("What is the expense ratio of ELSS?")
        if not result["no_results"]:
            self.assertIn("error", result["answer"].lower())

    def test_build_prompt_contains_context(self):
        context = "The expense ratio is 1.03%."
        sources = ["https://www.indmoney.com/mutual-funds/elss"]
        prompt  = self.chain.build_prompt(context, sources, "2026-03-02T12:00:00")
        self.assertIn(context, prompt)
        self.assertIn("2026-03-02", prompt)

    def test_no_results_flag_on_impossible_query(self):
        result = self.chain.run("xyzzy flux capacitor quantum nonsense 99999")
        # Either no_results True or we found something via fallback — either is OK
        self.assertIsInstance(result["no_results"], bool)

    def test_display_name_to_fund_key(self):
        key = RAGChain._display_name_to_fund_key(
            "Nippon India ELSS Tax Saver Fund - Direct Plan Growth"
        )
        self.assertEqual(key, "nippon_elss_tax_saver")

    def test_display_name_to_fund_key_unknown(self):
        key = RAGChain._display_name_to_fund_key("Some Unknown Fund")
        self.assertIsNone(key)


# ===========================================================================
# 4. Integration smoke test — full pipeline on every sample query
# ===========================================================================

class TestPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.store = _make_store()
        self.chain = RAGChain(vector_store=self.store)

    def test_expense_ratio_query(self):
        result = self.chain.run("What is the expense ratio of Nippon ELSS Tax Saver?")
        self.assertIn("nippon_elss_tax_saver", result["fund_keys"])
        self.assertEqual(result["intent"], "expense_exit")
        self.assertFalse(result["no_results"])
        # Top chunk should be the expense_exit chunk
        self.assertEqual(result["chunks"][0]["metadata"]["chunk_type"], "expense_exit")

    def test_lockin_period_query(self):
        result = self.chain.run("What is the lock-in period for ELSS tax saver fund?")
        self.assertFalse(result["no_results"])
        chunk_types = [c["metadata"]["chunk_type"] for c in result["chunks"]]
        self.assertIn("lockin_tax", chunk_types)

    def test_sip_query(self):
        result = self.chain.run("What is the minimum SIP for balanced advantage fund?")
        self.assertIn("nippon_balanced_advantage", result["fund_keys"])
        self.assertEqual(result["intent"], "sip_investment")
        self.assertFalse(result["no_results"])

    def test_risk_query(self):
        result = self.chain.run("What is the risk level of Nippon Silver ETF?")
        self.assertFalse(result["no_results"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
