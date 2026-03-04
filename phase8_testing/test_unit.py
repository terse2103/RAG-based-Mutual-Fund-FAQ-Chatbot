"""
Phase 8 — Unit Tests (all modules)
=====================================
Covers every testable class in the project without requiring:
  - A live Groq API key
  - A populated ChromaDB vector store
  - A running Streamlit server
  - Network access

Test classes
------------
TestDocumentChunkDataclass   : phase2 — DocumentChunk dataclass fields
TestFundChunker              : phase2 — FundChunker.create_chunks()
TestQueryProcessorFundIdent  : phase4 — QueryProcessor.identify_fund(s)
TestQueryProcessorIntent     : phase4 — QueryProcessor.classify_intent()
TestResponseGuard            : phase6 — ResponseGuard sentence-limit + metadata
TestPIIFilter                : phase5 — PIIFilter (delegated from Phase 5 tests)
TestAdviceGuardrail          : phase5 — AdviceGuardrail (delegated)
TestSafetyGate               : phase5 — SafetyGate integration (delegated)
TestPerformanceMonitor       : phase8 — PerformanceMonitor + timer context
TestLoggerConfig             : phase8 — setup_logging() idempotency

Run:
    pytest phase8_testing/test_unit.py -v
"""

from __future__ import annotations

import time
import pytest


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures & helpers
# ═══════════════════════════════════════════════════════════════════════════

def _make_fund_data(
    fund_key: str = "test_fund",
    fund_name: str = "Test Fund",
    **field_overrides,
) -> dict:
    """Return a minimal valid fund_data dict for FundChunker.create_chunks()."""
    fields = {
        "fund_name":     fund_name,
        "category":      "Equity",
        "amc":           "Test AMC",
        "fund_manager":  "John Doe",
        "aum":           "₹1,000 Cr",
        "nav":           "42.50",
        "nav_date":      "2026-03-02",
        "expense_ratio": "0.50%",
        "exit_load":     "Nil",
        "min_sip":       "₹500",
        "min_lumpsum":   "₹5,000",
        "risk_level":    "Moderately High",
        "benchmark":     "Nifty 50 TRI",
        "lock_in":       "None",
        "returns": {"1Y": "12%", "3Y": "15%", "5Y": "18%"},
    }
    fields.update(field_overrides)
    return {
        "fund_key":   fund_key,
        "source_url": "https://www.indmoney.com/test-fund",
        "scraped_at": "2026-03-02T10:00:00",
        "fields":     fields,
        "raw_text":   "",
    }


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2 — DocumentChunk dataclass
# ═══════════════════════════════════════════════════════════════════════════

class TestDocumentChunkDataclass:

    def test_chunk_has_all_required_fields(self):
        from phase2_processing.chunker import DocumentChunk
        chunk = DocumentChunk(
            chunk_id="fund_overview",
            fund_name="Test Fund",
            fund_key="test_fund",
            source_url="https://example.com",
            chunk_type="overview",
            content="Some content here.",
            scraped_at="2026-03-02T10:00:00",
            metadata={"category": "Equity"},
        )
        assert chunk.chunk_id   == "fund_overview"
        assert chunk.fund_key   == "test_fund"
        assert chunk.chunk_type == "overview"
        assert "Some content" in chunk.content

    def test_chunk_metadata_is_dict(self):
        from phase2_processing.chunker import DocumentChunk
        chunk = DocumentChunk(
            chunk_id="x", fund_name="F", fund_key="k",
            source_url="http://x.com", chunk_type="overview",
            content="c", scraped_at="2026-01-01", metadata={"k": "v"},
        )
        assert isinstance(chunk.metadata, dict)


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2 — FundChunker
# ═══════════════════════════════════════════════════════════════════════════

class TestFundChunker:

    def setup_method(self):
        from phase2_processing.chunker import FundChunker
        self.chunker = FundChunker()

    def test_creates_at_least_one_chunk(self):
        fd = _make_fund_data()
        chunks = self.chunker.create_chunks(fd)
        assert len(chunks) >= 1

    def test_overview_chunk_has_fund_name(self):
        fd = _make_fund_data(fund_name="Nippon ELSS")
        chunks = self.chunker.create_chunks(fd)
        overview = next((c for c in chunks if c.chunk_type == "overview"), None)
        assert overview is not None, "Expected an overview chunk"
        assert "Nippon ELSS" in overview.content

    def test_expense_exit_chunk_has_expense_ratio(self):
        fd = _make_fund_data()
        chunks = self.chunker.create_chunks(fd)
        chunk = next((c for c in chunks if c.chunk_type == "expense_exit"), None)
        assert chunk is not None, "Expected expense_exit chunk"
        assert "0.50%" in chunk.content

    def test_sip_chunk_has_min_sip(self):
        fd = _make_fund_data()
        chunks = self.chunker.create_chunks(fd)
        chunk = next((c for c in chunks if c.chunk_type == "sip_investment"), None)
        assert chunk is not None
        assert "₹500" in chunk.content

    def test_risk_benchmark_chunk_has_benchmark(self):
        fd = _make_fund_data()
        chunks = self.chunker.create_chunks(fd)
        chunk = next((c for c in chunks if c.chunk_type == "risk_benchmark"), None)
        assert chunk is not None
        assert "Nifty 50 TRI" in chunk.content

    def test_returns_chunk_has_1y_return(self):
        fd = _make_fund_data()
        chunks = self.chunker.create_chunks(fd)
        chunk = next((c for c in chunks if c.chunk_type == "returns"), None)
        assert chunk is not None
        assert "12%" in chunk.content

    def test_chunk_ids_are_unique(self):
        fd = _make_fund_data(fund_key="nippon_elss")
        chunks = self.chunker.create_chunks(fd)
        ids = [c.chunk_id for c in chunks]
        assert len(ids) == len(set(ids)), "Chunk IDs must be unique"

    def test_chunk_id_starts_with_fund_key(self):
        fd = _make_fund_data(fund_key="my_test_fund")
        chunks = self.chunker.create_chunks(fd)
        for c in chunks:
            assert c.chunk_id.startswith("my_test_fund"), (
                f"Chunk '{c.chunk_id}' does not start with fund key"
            )

    def test_chunk_source_url_matches_fund_data(self):
        fd = _make_fund_data()
        chunks = self.chunker.create_chunks(fd)
        for c in chunks:
            assert c.source_url == "https://www.indmoney.com/test-fund"

    def test_holdings_chunk_created_when_holdings_present(self):
        fd = _make_fund_data()
        fd["fields"]["holdings"] = [
            {"name": "Reliance Industries", "weight": "8%"},
            {"name": "HDFC Bank", "weight": "7%"},
        ]
        chunks = self.chunker.create_chunks(fd)
        holdings_chunk = next((c for c in chunks if c.chunk_type == "holdings"), None)
        assert holdings_chunk is not None
        assert "Reliance Industries" in holdings_chunk.content

    def test_no_holdings_chunk_when_holdings_absent(self):
        fd = _make_fund_data()
        fd["fields"]["holdings"] = []   # empty list
        chunks = self.chunker.create_chunks(fd)
        holdings_chunk = next((c for c in chunks if c.chunk_type == "holdings"), None)
        assert holdings_chunk is None

    def test_missing_fields_use_na(self):
        """FundChunker must degrade gracefully when fields are missing."""
        fd = _make_fund_data()
        fd["fields"].pop("expense_ratio", None)
        # Should not raise
        chunks = self.chunker.create_chunks(fd)
        assert isinstance(chunks, list)


# ═══════════════════════════════════════════════════════════════════════════
# Phase 4 — QueryProcessor fund identification
# ═══════════════════════════════════════════════════════════════════════════

class TestQueryProcessorFundIdent:

    def setup_method(self):
        from phase4_pipeline.query_processor import QueryProcessor
        self.qp = QueryProcessor()

    def _assert_fund_detected(self, query: str, expected_key: str) -> None:
        """Assert expected_key appears in identify_funds() results."""
        keys = self.qp.identify_funds(query)
        assert expected_key in keys, (
            f"Expected '{expected_key}' in identify_funds('{query}'), got: {keys}"
        )

    def test_elss_identified(self):
        self._assert_fund_detected(
            "What is the expense ratio of Nippon ELSS Tax Saver?",
            "nippon_elss_tax_saver",
        )

    def test_balanced_advantage_identified(self):
        self._assert_fund_detected(
            "Tell me about the balanced advantage fund",
            "nippon_balanced_advantage",
        )

    def test_short_duration_identified(self):
        self._assert_fund_detected(
            "What is the min SIP for short duration fund?",
            "nippon_short_duration",
        )

    def test_silver_etf_identified(self):
        self._assert_fund_detected(
            "nippon silver etf fof fund",
            "nippon_silver_etf_fof",
        )

    def test_nifty_auto_identified(self):
        self._assert_fund_detected(
            "nifty auto index",
            "nippon_nifty_auto_index",
        )

    def test_crisil_ibx_identified(self):
        self._assert_fund_detected(
            "crisil ibx",
            "nippon_crisil_ibx_aaa",
        )

    def test_no_fund_returns_empty_or_none(self):
        keys = self.qp.identify_funds("How does a mutual fund work in general?")
        # May return something or nothing; if returned it must be a valid fund key
        valid = {
            "nippon_elss_tax_saver", "nippon_balanced_advantage",
            "nippon_short_duration", "nippon_silver_etf_fof",
            "nippon_nifty_auto_index", "nippon_crisil_ibx_aaa",
        }
        for k in keys:
            assert k in valid, f"Unexpected fund key returned: {k}"

    def test_identify_funds_returns_list(self):
        keys = self.qp.identify_funds("ELSS tax saver fund")
        assert isinstance(keys, list)

    def test_identify_funds_non_empty_for_known_fund(self):
        keys = self.qp.identify_funds("What is the AUM of Nippon ELSS?")
        assert len(keys) >= 1


# ═══════════════════════════════════════════════════════════════════════════
# Phase 4 — QueryProcessor intent classification
# ═══════════════════════════════════════════════════════════════════════════

class TestQueryProcessorIntent:

    def setup_method(self):
        from phase4_pipeline.query_processor import QueryProcessor
        self.qp = QueryProcessor()

    def test_expense_ratio_intent(self):
        assert self.qp.classify_intent("What is the expense ratio of ELSS?") == "expense_exit"

    def test_exit_load_intent(self):
        assert self.qp.classify_intent("What is the exit load?") == "expense_exit"

    def test_sip_intent(self):
        assert self.qp.classify_intent("What is the minimum SIP amount?") == "sip_investment"

    def test_lumpsum_intent(self):
        assert self.qp.classify_intent("What is the minimum lumpsum?") == "sip_investment"

    def test_risk_intent(self):
        assert self.qp.classify_intent("What is the risk level?") == "risk_benchmark"

    def test_benchmark_intent(self):
        assert self.qp.classify_intent("What is the benchmark index?") == "risk_benchmark"

    def test_lockin_intent(self):
        assert self.qp.classify_intent("What is the lock-in period?") == "lockin_tax"

    def test_returns_intent(self):
        assert self.qp.classify_intent("What are the 1 year returns?") == "returns"

    def test_nav_intent(self):
        assert self.qp.classify_intent("What is the current NAV?") == "overview"

    def test_holdings_intent(self):
        assert self.qp.classify_intent("What are the top holdings?") == "holdings"

    def test_general_intent_fallback(self):
        # Must contain no keywords from any intent category
        assert self.qp.classify_intent("Please give me information on this") == "general"

    def test_analyse_returns_dict(self):
        result = self.qp.analyse("What is the expense ratio of ELSS?")
        assert isinstance(result, dict)
        assert "fund_key" in result
        assert "intent" in result
        assert "query" in result


# ═══════════════════════════════════════════════════════════════════════════
# Phase 6 — ResponseGuard
# ═══════════════════════════════════════════════════════════════════════════

class TestResponseGuard:

    def setup_method(self):
        from phase6_generation.response_guard import ResponseGuard
        self.guard = ResponseGuard()

    def test_three_sentences_not_truncated(self):
        text = "Sentence one. Sentence two. Sentence three."
        result = self.guard.enforce_sentence_limit(text, limit=3)
        assert "Sentence one" in result
        assert "Sentence three" in result

    def test_four_sentences_truncated_to_three(self):
        text = "One. Two. Three. Four."
        result = self.guard.enforce_sentence_limit(text, limit=3)
        assert "Four" not in result

    def test_validate_combines_truncation_and_metadata(self):
        long_text = "A. B. C. D. E."
        result = self.guard.validate(long_text, ["https://x.com"], "2026-01-01T00:00:00")
        # Should be truncated to 3 sentences
        assert "D" not in result or "E" not in result

    def test_validate_returns_string(self):
        result = self.guard.validate("One sentence.", ["https://x.com"], "2026-01-01")
        assert isinstance(result, str)


# ═══════════════════════════════════════════════════════════════════════════
# Phase 8 — PerformanceMonitor
# ═══════════════════════════════════════════════════════════════════════════

class TestPerformanceMonitor:

    def setup_method(self):
        from phase8_testing.monitor import PerformanceMonitor
        self.m = PerformanceMonitor()

    def test_initial_stats_total_zero(self):
        stats = self.m.get_stats()
        assert stats["total_queries"] == 0

    def test_record_query_increments_total(self):
        self.m.record_query_result(blocked=False)
        assert self.m.get_stats()["total_queries"] == 1

    def test_blocked_query_increments_blocked_counter(self):
        self.m.record_query_result(blocked=True, block_reason="ADVICE")
        stats = self.m.get_stats()
        assert stats["blocked"] == 1

    def test_allow_rate_100_when_no_blocks(self):
        self.m.record_query_result(blocked=False)
        self.m.record_query_result(blocked=False)
        assert self.m.get_stats()["allow_rate_pct"] == 100.0

    def test_allow_rate_50_when_half_blocked(self):
        self.m.record_query_result(blocked=False)
        self.m.record_query_result(blocked=True)
        assert self.m.get_stats()["allow_rate_pct"] == 50.0

    def test_timer_records_stage_time(self):
        with self.m.timer("retrieval"):
            time.sleep(0.01)
        self.m.record_query_result(blocked=False)
        stats = self.m.get_stats()
        # recent_queries should have one entry
        assert len(stats["recent_queries"]) == 1

    def test_token_accumulation(self):
        # Must also record a query, so get_stats doesn't take the n==0 early path
        self.m.record_llm_usage(prompt_tokens=100, completion_tokens=50)
        self.m.record_llm_usage(prompt_tokens=120, completion_tokens=60)
        self.m.record_query_result(blocked=False)   # needed so n > 0
        stats = self.m.get_stats()
        assert stats["total_tokens_in"]  == 220
        assert stats["total_tokens_out"] == 110

    def test_reset_clears_all_counters(self):
        self.m.record_query_result(blocked=False)
        self.m.record_llm_usage(prompt_tokens=100, completion_tokens=50)
        self.m.reset()
        stats = self.m.get_stats()
        assert stats["total_queries"]   == 0
        assert stats["total_tokens_in"] == 0

    def test_recent_queries_capped_at_ten(self):
        for i in range(15):
            self.m.record_query_result(blocked=False)
        stats = self.m.get_stats()
        assert len(stats["recent_queries"]) <= 10

    def test_blocked_entry_has_block_reason(self):
        self.m.record_query_result(blocked=True, block_reason="PII")
        stats = self.m.get_stats()
        entry = stats["recent_queries"][-1]
        assert entry["block_reason"] == "PII"

    def test_get_stats_thread_safe(self):
        """Quick check: calling get_stats from multiple threads should not crash."""
        import threading
        errors = []

        def work():
            try:
                for _ in range(10):
                    self.m.record_query_result(blocked=False, total_ms=5.0)
                    self.m.get_stats()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=work) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert errors == [], f"Thread-safety errors: {errors}"


# ═══════════════════════════════════════════════════════════════════════════
# Phase 8 — LoggerConfig
# ═══════════════════════════════════════════════════════════════════════════

class TestLoggerConfig:

    def test_setup_logging_runs_without_error(self, tmp_path):
        from phase8_testing import logger_config
        # Reset the global guard so we can call setup_logging in tests
        original = logger_config._CONFIGURED
        logger_config._CONFIGURED = False
        try:
            logger_config.setup_logging(
                level="WARNING",
                log_file=str(tmp_path / "test.log"),
                enable_console=False,
            )
        finally:
            logger_config._CONFIGURED = original   # restore

    def test_setup_logging_is_idempotent(self, tmp_path):
        from phase8_testing import logger_config
        logger_config._CONFIGURED = False
        try:
            logger_config.setup_logging(log_file=str(tmp_path / "test2.log"), enable_console=False)
            # Second call must be a no-op (no duplicate handlers added)
            logger_config.setup_logging(log_file=str(tmp_path / "test2.log"), enable_console=False)
        finally:
            logger_config._CONFIGURED = True   # leave configured

    def test_get_logger_returns_logger(self):
        from phase8_testing.logger_config import get_logger
        log = get_logger("test.module")
        assert hasattr(log, "info")
        assert hasattr(log, "warning")
        assert hasattr(log, "error")
