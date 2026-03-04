"""
Phase 9 — Test Suite
====================
Covers ``DailyRefreshScheduler`` with:

Unit Tests
----------
TestIsDataStale          — stale / fresh / missing timestamp logic
TestGetStatusReport      — correct keys; scheduler running/stopped states
TestSaveLoadMetadata     — JSON round-trip for metadata persistence
TestTriggerManualRefresh — pipeline called; status updated; metadata saved

Integration Tests
-----------------
TestSchedulerStartStop   — APScheduler starts, registers cron job, stops cleanly
TestMaybeRefreshOnStartup — triggers only when stale, skips when fresh
TestOnceWorkflow          — full run_phase9 --once path (mocked pipeline)

Run with:
    pytest phase9_scheduler/test_scheduler.py -v
"""

from __future__ import annotations

import json
import logging
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, call, patch

# ── Unit under test ────────────────────────────────────────────────────────────
from phase9_scheduler.scheduler import DailyRefreshScheduler, _METADATA_PATH

logger = logging.getLogger("phase9.tests")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fresh_ts() -> str:
    """Return an ISO-8601 timestamp from 1 hour ago (within 24 h → fresh)."""
    return (datetime.now() - timedelta(hours=1)).isoformat()


def _stale_ts() -> str:
    """Return an ISO-8601 timestamp from 25 hours ago (stale)."""
    return (datetime.now() - timedelta(hours=25)).isoformat()


def _make_scheduler(tmp_path: Path, **kwargs) -> DailyRefreshScheduler:
    """Instantiate a scheduler with an isolated metadata file."""
    return DailyRefreshScheduler(
        metadata_path=tmp_path / "scrape_metadata.json",
        **kwargs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests: is_data_stale
# ─────────────────────────────────────────────────────────────────────────────

class TestIsDataStale(unittest.TestCase):
    """Verify stale-data detection logic."""

    def setUp(self):
        import tempfile, os
        self.tmp = Path(tempfile.mkdtemp())
        self.sched = _make_scheduler(self.tmp)

    def test_stale_when_never_refreshed(self):
        self.sched.last_refresh = None
        self.assertTrue(self.sched.is_data_stale(max_age_hours=24))

    def test_stale_when_older_than_threshold(self):
        self.sched.last_refresh = _stale_ts()  # 25 h ago
        self.assertTrue(self.sched.is_data_stale(max_age_hours=24))

    def test_fresh_when_within_threshold(self):
        self.sched.last_refresh = _fresh_ts()  # 1 h ago
        self.assertFalse(self.sched.is_data_stale(max_age_hours=24))

    def test_stale_with_invalid_timestamp(self):
        self.sched.last_refresh = "not-a-valid-date"
        self.assertTrue(self.sched.is_data_stale())

    def test_custom_threshold(self):
        """Data 30 min old should be stale under a 0.25 h threshold."""
        self.sched.last_refresh = (datetime.now() - timedelta(minutes=30)).isoformat()
        self.assertTrue(self.sched.is_data_stale(max_age_hours=0.25))
        self.assertFalse(self.sched.is_data_stale(max_age_hours=1))


# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests: get_status_report
# ─────────────────────────────────────────────────────────────────────────────

class TestGetStatusReport(unittest.TestCase):
    """``get_status_report`` must return the correct keys and types."""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.sched = _make_scheduler(self.tmp)

    def test_required_keys_present(self):
        report = self.sched.get_status_report()
        for key in ("last_refresh", "is_running", "next_run", "last_status", "is_stale"):
            self.assertIn(key, report, f"Missing key: {key}")

    def test_never_refreshed_string(self):
        self.sched.last_refresh = None
        report = self.sched.get_status_report()
        self.assertEqual(report["last_refresh"], "Never")

    def test_not_running_initially(self):
        report = self.sched.get_status_report()
        self.assertFalse(report["is_running"])

    def test_next_run_na_when_not_running(self):
        report = self.sched.get_status_report()
        self.assertEqual(report["next_run"], "N/A")

    def test_is_stale_reflects_last_refresh(self):
        self.sched.last_refresh = _fresh_ts()
        report = self.sched.get_status_report()
        self.assertFalse(report["is_stale"])

        self.sched.last_refresh = _stale_ts()
        report = self.sched.get_status_report()
        self.assertTrue(report["is_stale"])


# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests: Metadata persistence
# ─────────────────────────────────────────────────────────────────────────────

class TestSaveLoadMetadata(unittest.TestCase):
    """Verify JSON round-trip for ``_save_metadata`` / ``_load_metadata``."""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    def test_save_creates_file(self):
        sched = _make_scheduler(self.tmp)
        sched.last_refresh = "2026-03-03T10:00:00"
        sched.last_status = {"success": ["fund_a"], "failed": []}
        sched._save_metadata()
        self.assertTrue((self.tmp / "scrape_metadata.json").exists())

    def test_load_restores_state(self):
        # Write metadata manually
        meta = {
            "last_refresh": "2026-03-03T10:00:00",
            "last_status": {"success": ["fund_a", "fund_b"], "failed": []},
            "updated_at": datetime.now().isoformat(),
        }
        (self.tmp / "scrape_metadata.json").write_text(
            json.dumps(meta), encoding="utf-8"
        )
        sched = _make_scheduler(self.tmp)
        self.assertEqual(sched.last_refresh, "2026-03-03T10:00:00")
        self.assertEqual(sched.last_status["success"], ["fund_a", "fund_b"])

    def test_graceful_on_corrupt_json(self):
        (self.tmp / "scrape_metadata.json").write_text("NOT JSON", encoding="utf-8")
        sched = _make_scheduler(self.tmp)  # Should not raise
        self.assertIsNone(sched.last_refresh)

    def test_save_then_load_roundtrip(self):
        sched = _make_scheduler(self.tmp)
        ts = datetime.now().isoformat()
        sched.last_refresh = ts
        sched.last_status = {"success": ["f1", "f2", "f3"], "failed": []}
        sched._save_metadata()

        sched2 = _make_scheduler(self.tmp)
        self.assertEqual(sched2.last_refresh, ts)
        self.assertEqual(sched2.last_status["success"], ["f1", "f2", "f3"])


# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests: trigger_manual_refresh
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerManualRefresh(unittest.TestCase):
    """
    ``trigger_manual_refresh`` must call ``_run_refresh_pipeline`` and update
    ``last_refresh`` / ``last_status``.
    """

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    @patch.object(DailyRefreshScheduler, "_run_refresh_pipeline", autospec=True)
    def test_delegates_to_pipeline(self, mock_pipeline):
        sched = _make_scheduler(self.tmp)
        sched.trigger_manual_refresh()
        mock_pipeline.assert_called_once_with(sched)

    def test_last_refresh_updated_after_trigger(self):
        """trigger_manual_refresh sets last_refresh via the real pipeline path (mocked)."""
        sched = _make_scheduler(self.tmp)

        def fake_pipeline(self_inner):
            self_inner.last_refresh = "2026-03-03T10:00:00"

        with patch.object(DailyRefreshScheduler, "_run_refresh_pipeline", fake_pipeline):
            sched.trigger_manual_refresh()

        self.assertEqual(sched.last_refresh, "2026-03-03T10:00:00")


# ─────────────────────────────────────────────────────────────────────────────
# Integration Tests: Scheduler start / stop
# ─────────────────────────────────────────────────────────────────────────────

class TestSchedulerStartStop(unittest.TestCase):
    """Verify the APScheduler background process starts/stops correctly."""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    def test_scheduler_starts_and_registers_job(self):
        sched = _make_scheduler(self.tmp)
        sched.start()
        try:
            self.assertTrue(sched._scheduler.running)
            job = sched._scheduler.get_job("daily_scrape_refresh")
            self.assertIsNotNone(job, "Cron job not registered")
            self.assertEqual(job.id, "daily_scrape_refresh")
        finally:
            sched.stop()

    def test_scheduler_stops_cleanly(self):
        sched = _make_scheduler(self.tmp)
        sched.start()
        sched.stop()
        self.assertFalse(sched._scheduler.running)

    def test_double_stop_is_safe(self):
        """Calling stop twice should not raise."""
        sched = _make_scheduler(self.tmp)
        sched.start()
        sched.stop()
        sched.stop()  # Should not raise

    def test_job_trigger_is_cron_at_10am(self):
        """Confirm the cron trigger fires at hour=10, minute=0."""
        sched = _make_scheduler(self.tmp)
        sched.start()
        try:
            job = sched._scheduler.get_job("daily_scrape_refresh")
            # APScheduler stores the trigger; check its fields
            trigger = job.trigger
            # CronTrigger fields are stored in trigger.fields
            fields = {f.name: f for f in trigger.fields}
            self.assertEqual(str(fields["hour"]), "10")
            self.assertEqual(str(fields["minute"]), "0")
        finally:
            sched.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Integration Tests: maybe_refresh_on_startup
# ─────────────────────────────────────────────────────────────────────────────

class TestMaybeRefreshOnStartup(unittest.TestCase):

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    @patch.object(DailyRefreshScheduler, "_run_refresh_pipeline", autospec=True)
    def test_triggers_when_stale(self, mock_pipeline):
        sched = _make_scheduler(self.tmp)
        sched.last_refresh = _stale_ts()
        sched.maybe_refresh_on_startup(max_age_hours=24)
        mock_pipeline.assert_called_once()

    @patch.object(DailyRefreshScheduler, "_run_refresh_pipeline", autospec=True)
    def test_skips_when_fresh(self, mock_pipeline):
        sched = _make_scheduler(self.tmp)
        sched.last_refresh = _fresh_ts()
        sched.maybe_refresh_on_startup(max_age_hours=24)
        mock_pipeline.assert_not_called()

    @patch.object(DailyRefreshScheduler, "_run_refresh_pipeline", autospec=True)
    def test_triggers_when_never_refreshed(self, mock_pipeline):
        sched = _make_scheduler(self.tmp)
        sched.last_refresh = None
        sched.maybe_refresh_on_startup(max_age_hours=24)
        mock_pipeline.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# Integration Tests: _run_refresh_pipeline (mocked)
# ─────────────────────────────────────────────────────────────────────────────

class TestRunRefreshPipeline(unittest.TestCase):
    """
    Verify the internal pipeline orchestration logic without hitting
    real Playwright / ChromaDB instances.
    """

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())

    def _mock_fund_data(self, key: str) -> dict:
        return {
            "fund_key": key,
            "source_url": f"https://indmoney.com/{key}",
            "scraped_at": datetime.now().isoformat(),
            "fields": {
                "fund_name": f"Fund {key}",
                "category": "Equity",
                "amc": "Nippon",
                "fund_manager": "Manager A",
                "aum": "₹1000 Cr",
                "nav": "42.50",
                "nav_date": "2026-03-03",
                "expense_ratio": "0.65%",
                "exit_load": "Nil",
                "min_sip": "₹100",
                "min_lumpsum": "₹1000",
                "risk_level": "Very High",
                "benchmark": "Nifty 50",
                "lockin_period": "3 years",
                "elss_note": "ELSS funds qualify for 80C deduction.",
            },
            "raw_text": f"Full raw text for {key}",
        }

    @patch("phase9_scheduler.scheduler.asyncio.new_event_loop")
    @patch("phase9_scheduler.scheduler.asyncio.set_event_loop")
    @patch("phase9_scheduler.scheduler.INDMoneyScraper", create=True)
    def test_pipeline_success_path(self, MockScraper, mock_set_loop, mock_new_loop):
        """Happy path: scraper returns data, chunker creates chunks, upsert called."""
        # Build fake scraped data for 2 funds
        fund_keys = ["fund_a", "fund_b"]
        fake_data = [self._mock_fund_data(k) for k in fund_keys]

        # Mock the event loop so scraper runs synchronously in tests
        fake_loop = MagicMock()
        fake_loop.run_until_complete.return_value = fake_data
        mock_new_loop.return_value = fake_loop

        # Mock imports inside the method
        mock_scraper_instance = MagicMock()
        MockScraper.return_value = mock_scraper_instance

        # Mock chunker: returns 2 chunks per fund
        mock_chunk = MagicMock()
        mock_chunker = MagicMock()
        mock_chunker.create_chunks.return_value = [mock_chunk, mock_chunk]

        # Mock vector store
        mock_vs = MagicMock()

        sched = _make_scheduler(self.tmp, vector_store=mock_vs, chunker=mock_chunker)

        with patch("phase9_scheduler.scheduler.INDMoneyScraper", MockScraper):
            sched._run_refresh_pipeline()

        # Chunker called once per fund
        self.assertEqual(mock_chunker.create_chunks.call_count, len(fund_keys))
        # Vector store upsert called once with all chunks
        mock_vs.add_chunks.assert_called_once()
        calls_chunks = mock_vs.add_chunks.call_args[0][0]
        self.assertEqual(len(calls_chunks), 4)  # 2 funds × 2 chunks each

        # Status recorded
        self.assertIsNotNone(sched.last_refresh)
        self.assertEqual(sched.last_status["success"], fund_keys)
        self.assertEqual(sched.last_status["failed"], [])

    @patch("phase9_scheduler.scheduler.asyncio.new_event_loop")
    @patch("phase9_scheduler.scheduler.asyncio.set_event_loop")
    @patch("phase9_scheduler.scheduler.INDMoneyScraper", create=True)
    def test_chunking_failure_recorded(self, MockScraper, mock_set_loop, mock_new_loop):
        """If one fund's chunking fails, it's recorded in last_status['failed']."""
        fund_keys = ["fund_a", "fund_b"]
        fake_data = [self._mock_fund_data(k) for k in fund_keys]

        fake_loop = MagicMock()
        fake_loop.run_until_complete.return_value = fake_data
        mock_new_loop.return_value = fake_loop

        # Chunker raises for fund_b
        mock_chunker = MagicMock()
        mock_chunker.create_chunks.side_effect = [
            [MagicMock()],           # fund_a succeeds
            ValueError("bad data"),  # fund_b fails
        ]

        mock_vs = MagicMock()
        sched = _make_scheduler(self.tmp, vector_store=mock_vs, chunker=mock_chunker)

        with patch("phase9_scheduler.scheduler.INDMoneyScraper", MockScraper):
            sched._run_refresh_pipeline()

        self.assertEqual(sched.last_status["success"], ["fund_a"])
        self.assertEqual(len(sched.last_status["failed"]), 1)
        self.assertEqual(sched.last_status["failed"][0]["fund_key"], "fund_b")

    @patch("phase9_scheduler.scheduler.asyncio.new_event_loop")
    @patch("phase9_scheduler.scheduler.asyncio.set_event_loop")
    @patch("phase9_scheduler.scheduler.INDMoneyScraper", create=True)
    def test_scraper_exception_sets_error_status(self, MockScraper, mock_set_loop, mock_new_loop):
        """If the scraper itself throws, last_status has 'error' key."""
        fake_loop = MagicMock()
        fake_loop.run_until_complete.side_effect = RuntimeError("Playwright crash")
        mock_new_loop.return_value = fake_loop

        sched = _make_scheduler(self.tmp)

        with patch("phase9_scheduler.scheduler.INDMoneyScraper", MockScraper):
            sched._run_refresh_pipeline()

        self.assertIn("error", sched.last_status)
        self.assertIn("Playwright crash", sched.last_status["error"])


# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests: _purge_previous_day_data
# ─────────────────────────────────────────────────────────────────────────────

class TestPurgePreviousDayData(unittest.TestCase):
    """Verify that stale data files are deleted while keeping today's data."""

    def setUp(self):
        import tempfile
        from datetime import date
        self.tmp = Path(tempfile.mkdtemp())
        self.data_root = self.tmp / "data"
        self.raw_dir = self.data_root / "raw"
        self.clean_dir = self.data_root / "cleaned"
        self.chunk_dir = self.data_root / "chunks"
        
        for d in [self.raw_dir, self.clean_dir, self.chunk_dir]:
            d.mkdir(parents=True)
            
        self.today = date.today().isoformat()
        self.yesterday = (date.today() - timedelta(days=1)).isoformat()

    @patch("phase9_scheduler.scheduler._PROJECT_ROOT")
    def test_purges_stale_files_but_keeps_today(self, mock_root):
        mock_root.__truediv__.return_value = self.tmp
        # Setting up mock_root / "data" to return self.data_root
        mock_root.joinpath.side_effect = lambda *args: self.tmp.joinpath(*args)
        # Actually _PROJECT_ROOT / "data" uses / operator
        mock_root.__truediv__.side_effect = lambda x: self.tmp / x if x == "data" else self.tmp

        # Create files
        # Yesterday (should be purged)
        f1 = self.raw_dir / f"nippon_elss_{self.yesterday}.json"
        f2 = self.clean_dir / f"nippon_elss_{self.yesterday}.json"
        f3 = self.chunk_dir / f"nippon_elss_{self.yesterday}_chunks.json"
        
        # Today (should be kept)
        f4 = self.raw_dir / f"nippon_elss_{self.today}.json"
        f5 = self.chunk_dir / f"nippon_elss_{self.today}_chunks.json"
        
        # Unrelated but in same folder (should be kept)
        f6 = self.chunk_dir / "all_chunks_summary.json"
        
        for f in [f1, f2, f3, f4, f5, f6]:
            f.write_text("{}", encoding="utf-8")

        from phase9_scheduler.scheduler import DailyRefreshScheduler
        sched = DailyRefreshScheduler(metadata_path=self.tmp / "meta.json")
        
        with patch("phase9_scheduler.scheduler._PROJECT_ROOT", self.tmp):
            sched._purge_previous_day_data()

        # Check existence
        self.assertFalse(f1.exists(), "Yesterday's raw file should be purged")
        self.assertFalse(f2.exists(), "Yesterday's cleaned file should be purged")
        self.assertFalse(f3.exists(), "Yesterday's chunk file should be purged")
        
        self.assertTrue(f4.exists(), "Today's raw file should be kept")
        self.assertTrue(f5.exists(), "Today's chunk file should be kept")
        self.assertTrue(f6.exists(), "All chunks summary should be kept")

    @patch("phase9_scheduler.scheduler._PROJECT_ROOT")
    def test_handles_missing_directories_gracefully(self, mock_root):
        # Empty temp dir, no 'data' folder
        empty_tmp = Path(unittest.TestCase()._testMethodName + "_empty")
        # cleanup if exists
        import shutil
        if empty_tmp.exists(): shutil.rmtree(empty_tmp)
        empty_tmp.mkdir()
        
        from phase9_scheduler.scheduler import DailyRefreshScheduler
        sched = DailyRefreshScheduler(metadata_path=empty_tmp / "meta.json")
        
        with patch("phase9_scheduler.scheduler._PROJECT_ROOT", empty_tmp):
            sched._purge_previous_day_data() # Should not raise
        
        shutil.rmtree(empty_tmp)



# ─────────────────────────────────────────────────────────────────────────────
# Run directly
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    unittest.main(verbosity=2)
