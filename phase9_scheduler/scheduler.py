"""
Phase 9 — DailyRefreshScheduler
================================
Schedules and executes the full data-refresh pipeline:

    Phase 1 (Playwright scraper)
        → Phase 2 (Chunker)
            → Phase 3 (Embedder / ChromaDB upsert)

The scheduler runs on APScheduler's BackgroundScheduler (so it can coexist
with a long-lived Streamlit/FastAPI process) with a CronTrigger firing every
day at 10:00 AM IST (Asia/Kolkata).

The scheduler logic can be triggered in three ways:
  • Automatic daily trigger — via GitHub Actions (.github/workflows/daily_refresh.yml)
  • Manual/on-demand trigger  — via ``trigger_manual_refresh()`` (or UI "Refresh Data")
  • Startup trigger            — via ``maybe_refresh_on_startup()``
    (runs immediately if data is older than ``max_age_hours``)

Usage (standalone/CLI for GitHub Actions/Task Scheduler):
    python run_refresh.py

Usage (embedded in app.py):
    from phase9_scheduler.scheduler import DailyRefreshScheduler
    scheduler = DailyRefreshScheduler()
    scheduler.start()
    scheduler.maybe_refresh_on_startup()
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ── Phase imports ──────────────────────────────────────────────────────────────
# These are resolved at call time (inside methods) to avoid circular imports
# and to allow the scheduler module to be imported even if other phases are
# partially initialised.

logger = logging.getLogger("phase9.scheduler")

# Default path to write the refresh metadata JSON
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_METADATA_PATH = _PROJECT_ROOT / "data" / "scrape_metadata.json"


class DailyRefreshScheduler:
    """
    Orchestrates the daily scrape → chunk → embed pipeline.

    Parameters
    ----------
    vector_store : MFVectorStore, optional
        An already-initialised vectorstore.  If ``None``, a new one is created
        inside ``_run_refresh_pipeline`` using the default persist directory.
    chunker : FundChunker, optional
        An already-initialised chunker.  If ``None``, a fresh one is created.
    metadata_path : Path, optional
        Where to write ``scrape_metadata.json`` with run history.
    """

    def __init__(
        self,
        vector_store=None,
        chunker=None,
        metadata_path: Path = _METADATA_PATH,
    ) -> None:
        self._vector_store = vector_store
        self._chunker = chunker
        self._metadata_path = Path(metadata_path)

        self._scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        self.last_refresh: str | None = None   # ISO-8601 timestamp of last run
        self.last_status: dict[str, Any] = {}  # {"success": [...], "failed": [...]}

        # Load persisted last_refresh from metadata file (survives restarts)
        self._load_metadata()

    # ── Private helpers ────────────────────────────────────────────────────────

    def _get_vector_store(self):
        """Return the shared vector store or create a new one."""
        if self._vector_store is not None:
            return self._vector_store
        from phase3_embedding.index_builder import MFVectorStore
        return MFVectorStore()

    def _get_chunker(self):
        """Return the shared chunker or create a new one."""
        if self._chunker is not None:
            return self._chunker
        from phase2_processing.chunker import FundChunker
        return FundChunker()

    def _load_metadata(self) -> None:
        """Restore ``last_refresh`` from the metadata JSON file (if it exists)."""
        if self._metadata_path.exists():
            try:
                data = json.loads(self._metadata_path.read_text(encoding="utf-8"))
                self.last_refresh = data.get("last_refresh")
                self.last_status = data.get("last_status", {})
                logger.debug("Loaded metadata: last_refresh=%s", self.last_refresh)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load metadata file: %s", exc)

    def _save_metadata(self) -> None:
        """Persist run history to ``scrape_metadata.json``."""
        try:
            self._metadata_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "last_refresh": self.last_refresh,
                "last_status": self.last_status,
                "updated_at": datetime.now().isoformat(),
            }
            self._metadata_path.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("Could not save metadata file: %s", exc)

    # ── Core pipeline ──────────────────────────────────────────────────────────

    def _purge_previous_day_data(self) -> None:
        """
        Delete all data files from *previous* scrape runs (any date != today).

        Scans:
          • data/raw/        for  *_YYYY-MM-DD.json files
          • data/cleaned/    for  *_YYYY-MM-DD.json files
          • data/chunks/     for  *_YYYY-MM-DD_chunks.json files

        Files whose date stamp matches today's date are kept untouched.
        The ``all_chunks_summary.json`` file is always left alone.
        """
        today_str = date.today().isoformat()          # e.g. "2026-03-04"
        data_root = _PROJECT_ROOT / "data"

        directories = [
            (data_root / "raw",     "*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].json"),
            (data_root / "cleaned", "*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].json"),
            (data_root / "chunks",  "*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_chunks.json"),
        ]

        purged = 0
        for folder, pattern in directories:
            if not folder.exists():
                continue
            for fp in folder.glob(pattern):
                # Extract the date portion from the filename — it is always
                # the segment that looks like YYYY-MM-DD.
                stem_parts = fp.stem.split("_")          # e.g. ["nippon","elss","2026-03-03"]
                date_part  = next(
                    (p for p in stem_parts if len(p) == 10 and p[4] == "-" and p[7] == "-"),
                    None,
                )
                if date_part and date_part != today_str:
                    try:
                        fp.unlink()
                        purged += 1
                        logger.debug("  🗑  Purged stale file: %s", fp.name)
                    except OSError as exc:
                        logger.warning("  ⚠️  Could not delete %s: %s", fp.name, exc)

        logger.info("🗑  Purged %d stale data file(s) from previous run(s).", purged)

    def _run_refresh_pipeline(self) -> None:
        """
        Execute the full scrape → chunk → embed pipeline synchronously.

        This method is called both by the APScheduler job (in a background
        thread) and by ``trigger_manual_refresh`` (which is also invoked by
        the UI "Refresh Data" button via the POST /refresh endpoint).
        It deliberately bridges the async scraper into a sync context using
        ``asyncio.new_event_loop`` so it works safely from any thread.
        """
        logger.info("🔄 Daily refresh triggered at %s", datetime.now().isoformat())
        results: dict[str, Any] = {"success": [], "failed": []}

        # ── Purge previous day's stale data files first ────────────────────
        logger.info("🗑  Step 0/3 — Purging previous day's data files …")
        self._purge_previous_day_data()

        try:
            # ── Step 1: Scrape all 6 fund pages ───────────────────────────────
            logger.info("📡 Step 1/3 — Scraping all INDMoney fund pages …")
            from phase1_scraping.indmoney_scraper import INDMoneyScraper, save_results

            scraper = INDMoneyScraper()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                scraped_data: list[dict] = loop.run_until_complete(scraper.scrape_all())
                # PERSIST TO DISK: Save each fund's JSON to data/raw/
                save_results(scraped_data)
                
                # Populate data/cleaned/ (mirroring raw for now)
                today = date.today().isoformat()
                cleaned_dir = _PROJECT_ROOT / "data" / "cleaned"
                cleaned_dir.mkdir(parents=True, exist_ok=True)
                for fund_data in scraped_data:
                    fund_key = fund_data["fund_key"]
                    filepath = cleaned_dir / f"{fund_key}_{today}.json"
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(fund_data, f, indent=2, ensure_ascii=False)
            finally:
                loop.close()

            logger.info("  ✅ Scraped and persisted %d fund pages.", len(scraped_data))

            # ── Step 2: Chunk each fund's data ────────────────────────────────
            logger.info("📄 Step 2/3 — Chunking scraped data …")
            from dataclasses import asdict
            chunker = self._get_chunker()
            all_chunks: list = []
            chunks_dir = _PROJECT_ROOT / "data" / "chunks"
            chunks_dir.mkdir(parents=True, exist_ok=True)
            today = date.today().isoformat()

            for fund_data in scraped_data:
                fund_key = fund_data.get("fund_key", "unknown")
                try:
                    chunks = chunker.create_chunks(fund_data)
                    all_chunks.extend(chunks)
                    
                    # PERSIST TO DISK: Save chunks for this fund
                    chunk_file = chunks_dir / f"{fund_key}_{today}_chunks.json"
                    with open(chunk_file, "w", encoding="utf-8") as f:
                        json.dump([asdict(c) for c in chunks], f, indent=2, ensure_ascii=False)
                    
                    results["success"].append(fund_key)
                    logger.debug("  Chunked and saved %d chunks for '%s'.", len(chunks), fund_key)
                except Exception as exc:
                    results["failed"].append({"fund_key": fund_key, "error": str(exc)})
                    logger.error("  ❌ Chunking failed for '%s': %s", fund_key, exc)

            # Update summary file
            summary_file = chunks_dir / "all_chunks_summary.json"
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump({
                    "total_chunks": len(all_chunks),
                    "funds_processed": len(results["success"]),
                    "timestamp": datetime.now().isoformat()
                }, f, indent=2)

            logger.info(
                "  ✅ Total chunks created and persisted: %d  (success: %d, failed: %d)",
                len(all_chunks),
                len(results["success"]),
                len(results["failed"]),
            )

            # ── Step 3: Upsert embeddings into ChromaDB ───────────────────────
            if all_chunks:
                logger.info("📦 Step 3/3 — Upserting %d chunks into ChromaDB …", len(all_chunks))
                vector_store = self._get_vector_store()
                vector_store.add_chunks(all_chunks)
                logger.info("  ✅ ChromaDB upsert complete.")
            else:
                logger.warning("  ⚠️  No chunks to upsert — skipping ChromaDB step.")

            # ── Finalise ──────────────────────────────────────────────────────
            self.last_refresh = datetime.now().isoformat()
            self.last_status = results
            self._save_metadata()

            logger.info(
                "✅ Daily refresh complete — success: %d fund(s), failed: %d fund(s).",
                len(results["success"]),
                len(results["failed"]),
            )

        except Exception as exc:
            logger.error("❌ Daily refresh pipeline failed with unexpected error: %s", exc, exc_info=True)
            self.last_status = {"error": str(exc)}
            self._save_metadata()

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Register the daily cron job and start the BackgroundScheduler.

        The scheduler fires every day at **10:00 AM IST** (Asia/Kolkata).
        A ``misfire_grace_time`` of 3 600 seconds (1 hour) ensures the job
        still runs if the process was temporarily down at 10 AM.
        """
        self._scheduler.add_job(
            func=self._run_refresh_pipeline,
            trigger=CronTrigger(hour=10, minute=0, timezone="Asia/Kolkata"),
            id="daily_scrape_refresh",
            name="Daily INDMoney Scrape & Refresh",
            replace_existing=True,
            misfire_grace_time=3_600,  # 1-hour grace window
        )
        self._scheduler.start()
        logger.info("📅 Daily scheduler started — next run at 10:00 AM IST (Asia/Kolkata).")

    def stop(self) -> None:
        """Gracefully shut down the APScheduler instance."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("🛑 Scheduler stopped.")

    def trigger_manual_refresh(self) -> None:
        """
        Run the refresh pipeline immediately (blocking).

        This is the handler for the UI "Refresh Data" button, which hits the
        POST /refresh endpoint in the FastAPI server.  It runs the same
        end-to-end pipeline as the daily scheduled job, including purging
        stale data files from the previous scrape run.

        Safe to call from any thread (FastAPI request handler, the
        APScheduler background thread, or a standalone script).
        """
        logger.info("🔧 Manual refresh triggered by user via UI 'Refresh Data' button or admin.")
        self._run_refresh_pipeline()

    def maybe_refresh_on_startup(self, max_age_hours: int = 24) -> None:
        """
        Run a refresh on startup **only if data is stale**.

        Parameters
        ----------
        max_age_hours : int
            If the last refresh was more than this many hours ago (or never),
            an immediate refresh is executed.
        """
        if self.is_data_stale(max_age_hours):
            logger.info(
                "⚡ Data is stale (>%d h) or never refreshed — running startup refresh.",
                max_age_hours,
            )
            self._run_refresh_pipeline()
        else:
            logger.info(
                "✅ Data is fresh (last refresh: %s) — skipping startup refresh.",
                self.last_refresh,
            )

    def is_data_stale(self, max_age_hours: int = 24) -> bool:
        """
        Return ``True`` if the last refresh was more than ``max_age_hours`` ago.

        Parameters
        ----------
        max_age_hours : int
            Age threshold in hours.

        Returns
        -------
        bool
        """
        if not self.last_refresh:
            return True
        try:
            last = datetime.fromisoformat(self.last_refresh)
            age_hours = (datetime.now() - last).total_seconds() / 3_600
            return age_hours > max_age_hours
        except ValueError:
            return True

    def get_status_report(self) -> dict[str, Any]:
        """
        Return a human-readable status dictionary.

        Useful for surfacing scheduler health in the Streamlit sidebar or an
        admin API endpoint.

        Returns
        -------
        dict
            Keys: ``last_refresh``, ``is_running``, ``next_run``,
                  ``last_status``, ``is_stale``
        """
        next_run: str | None = None
        if self._scheduler.running:
            job = self._scheduler.get_job("daily_scrape_refresh")
            if job and job.next_run_time:
                next_run = job.next_run_time.isoformat()

        return {
            "last_refresh": self.last_refresh or "Never",
            "is_running": self._scheduler.running,
            "next_run": next_run or "N/A",
            "last_status": self.last_status,
            "is_stale": self.is_data_stale(),
        }
