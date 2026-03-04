"""
Phase 3 — Tests for MFVectorStore
===================================
Uses an in-memory (temp-dir) ChromaDB instance so tests never touch
the real data/vectorstore directory.
"""

from __future__ import annotations

import tempfile
import unittest

from phase3_embedding.embedder import MFVectorStore


SAMPLE_CHUNKS = [
    {
        "chunk_id":   "elss_overview",
        "fund_name":  "Nippon India ELSS Tax Saver Fund",
        "fund_key":   "nippon_elss_tax_saver",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
        "chunk_type": "overview",
        "content":    "Nippon India ELSS Tax Saver Fund is an Equity fund managed by Nippon Mutual Fund with AUM ₹14881 Cr.",
        "scraped_at": "2026-03-02T16:38:06.832617",
        "metadata":   {"type": "overview"},
    },
    {
        "chunk_id":   "elss_expense_exit",
        "fund_name":  "Nippon India ELSS Tax Saver Fund",
        "fund_key":   "nippon_elss_tax_saver",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
        "chunk_type": "expense_exit",
        "content":    "The expense ratio of Nippon India ELSS Tax Saver Fund is 1.03%. The exit load is 0%.",
        "scraped_at": "2026-03-02T16:38:06.832617",
        "metadata":   {"type": "expense_exit"},
    },
    {
        "chunk_id":   "baf_sip",
        "fund_name":  "Nippon India Balanced Advantage Fund",
        "fund_key":   "nippon_balanced_advantage",
        "source_url": "https://www.indmoney.com/mutual-funds/nippon-india-balanced-advantage-fund-direct-growth-plan-4324",
        "chunk_type": "sip_investment",
        "content":    "The minimum SIP for Nippon India Balanced Advantage Fund is ₹100. Minimum lumpsum is ₹100.",
        "scraped_at": "2026-03-02T16:38:06.832617",
        "metadata":   {"type": "sip_investment"},
    },
]


class TestMFVectorStore(unittest.TestCase):

    def setUp(self):
        """Create a fresh in-memory store for each test."""
        self._tmpdir = tempfile.TemporaryDirectory()
        self.store   = MFVectorStore(persist_dir=self._tmpdir.name)

    def tearDown(self):
        # Explicitly close/reset the ChromaDB client so Windows releases
        # its file locks on the SQLite + HNSW files before rmtree runs.
        try:
            if hasattr(self.store, "_client") and self.store._client is not None:
                self.store._client.reset()   # drops all in-memory state + flushes WAL
                self.store._client = None
            if hasattr(self.store, "_collection"):
                self.store._collection = None
        except Exception:
            pass
        import gc, time
        gc.collect()          # encourage reference-count cleanup
        time.sleep(0.05)      # give Windows a moment to release handles
        try:
            self._tmpdir.cleanup()
        except PermissionError:
            pass  # best-effort on Windows; temp dir will be GC'd by OS

    # ── add_chunks ────────────────────────────────────────────────────────────

    def test_add_chunks_increases_count(self):
        self.assertEqual(self.store.count(), 0)
        self.store.add_chunks(SAMPLE_CHUNKS)
        self.assertEqual(self.store.count(), len(SAMPLE_CHUNKS))

    def test_upsert_is_idempotent(self):
        """Calling add_chunks twice with the same IDs must not duplicate."""
        self.store.add_chunks(SAMPLE_CHUNKS)
        self.store.add_chunks(SAMPLE_CHUNKS)   # second call — same IDs
        self.assertEqual(self.store.count(), len(SAMPLE_CHUNKS))

    def test_add_empty_list_does_not_crash(self):
        self.store.add_chunks([])
        self.assertEqual(self.store.count(), 0)

    # ── query ─────────────────────────────────────────────────────────────────

    def test_query_returns_results(self):
        self.store.add_chunks(SAMPLE_CHUNKS)
        result = self.store.query("What is the expense ratio?", top_k=2)
        self.assertIn("documents", result)
        self.assertGreater(len(result["documents"][0]), 0)

    def test_query_top_k_respected(self):
        self.store.add_chunks(SAMPLE_CHUNKS)
        for k in (1, 2, 3):
            result = self.store.query("mutual fund", top_k=k)
            self.assertLessEqual(len(result["documents"][0]), k)

    def test_query_similarity_in_range(self):
        """Cosine distance is in [0,2]; similarity = 1 - dist is in [-1, 1]."""
        self.store.add_chunks(SAMPLE_CHUNKS)
        result = self.store.query("expense ratio ELSS fund", top_k=1)
        distance = result["distances"][0][0]
        similarity = 1 - distance
        self.assertGreaterEqual(similarity, -1.0)
        self.assertLessEqual(similarity,  1.0)

    def test_query_most_relevant_is_top(self):
        """'expense ratio' query should rank the expense_exit chunk highest."""
        self.store.add_chunks(SAMPLE_CHUNKS)
        result = self.store.query("What is the expense ratio of ELSS Tax Saver?", top_k=3)
        top_meta = result["metadatas"][0][0]
        self.assertEqual(top_meta["chunk_type"], "expense_exit")

    # ── metadata filter ────────────────────────────────────────────────────────

    def test_fund_filter_restricts_results(self):
        self.store.add_chunks(SAMPLE_CHUNKS)
        result = self.store.query(
            "minimum SIP investment", top_k=3,
            filter_fund="nippon_balanced_advantage",
        )
        for meta in result["metadatas"][0]:
            self.assertEqual(meta["fund_key"], "nippon_balanced_advantage")

    def test_chunk_type_filter(self):
        self.store.add_chunks(SAMPLE_CHUNKS)
        result = self.store.query(
            "fund overview details", top_k=3,
            filter_chunk_type="overview",
        )
        for meta in result["metadatas"][0]:
            self.assertEqual(meta["chunk_type"], "overview")

    # ── utilities ─────────────────────────────────────────────────────────────

    def test_delete_collection_resets_count(self):
        self.store.add_chunks(SAMPLE_CHUNKS)
        self.store.delete_collection()
        self.assertEqual(self.store.count(), 0)

    def test_get_all_metadata(self):
        self.store.add_chunks(SAMPLE_CHUNKS)
        metas = self.store.get_all_metadata()
        self.assertEqual(len(metas), len(SAMPLE_CHUNKS))
        keys = {m["fund_key"] for m in metas}
        self.assertIn("nippon_elss_tax_saver",      keys)
        self.assertIn("nippon_balanced_advantage",  keys)


if __name__ == "__main__":
    unittest.main(verbosity=2)
