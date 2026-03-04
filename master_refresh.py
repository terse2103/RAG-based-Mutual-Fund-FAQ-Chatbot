"""
Master Refresh Script
=====================
Orchestrates the COMPLETE end-to-end data refresh pipeline:

  Step 0 — Purge previous day's stale data files (raw / cleaned / chunks)
  Step 1 — Phase 1: Scrape all 6 INDMoney fund pages with Playwright
  Step 2 — Phase 2: Process & chunk the scraped data
  Step 3 — Phase 3: Upsert embeddings into ChromaDB

Running this script is equivalent to pressing "Refresh Data" in the UI,
which hits POST /refresh → DailyRefreshScheduler.trigger_manual_refresh().
Both paths call the same _run_refresh_pipeline() under the hood.
"""

import asyncio
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent
_METADATA_PATH = _PROJECT_ROOT / "data" / "scrape_metadata.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s"
)
logger = logging.getLogger("master_refresh")


def _purge_previous_day_data() -> None:
    """
    Delete all dated data files from previous scrape runs (any date != today).

    Scans:
      • data/raw/      for  *_YYYY-MM-DD.json
      • data/cleaned/  for  *_YYYY-MM-DD.json
      • data/chunks/   for  *_YYYY-MM-DD_chunks.json

    Files whose date stamp matches today are kept untouched.
    The ``all_chunks_summary.json`` and ``scrape_metadata.json`` are never removed.
    """
    today_str = date.today().isoformat()
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
            stem_parts = fp.stem.split("_")
            date_part = next(
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


def _save_metadata(success: bool) -> None:
    """Write run timestamp to scrape_metadata.json (read by the UI /status endpoint)."""
    try:
        _METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_refresh": datetime.now(timezone.utc).isoformat(),
            "last_status": {"success": success},
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _METADATA_PATH.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.warning("Could not save metadata: %s", exc)


async def run_refresh_pipeline() -> bool:
    """
    Execute the complete end-to-end data refresh pipeline:

      Step 0 — Purge previous day's stale data files
      Step 1 — Phase 1: Scrape all 6 INDMoney fund pages
      Step 2 — Phase 2: Chunk each fund's scraped data
      Step 3 — Phase 3: Upsert embeddings into ChromaDB

    Returns True on full success, False if any step fails.
    """
    logger.info("🚀 Starting Master Refresh Pipeline...")

    # ── Step 0: Purge stale data from previous runs ───────────────────────────
    logger.info("🗑  Step 0/3 — Purging previous day's data files...")
    _purge_previous_day_data()

    # ── Step 1: Scrape all fund pages ─────────────────────────────────────────
    logger.info("📡 Step 1/3 — Scraping all INDMoney fund pages...")
    try:
        from phase1_scraping.indmoney_scraper import INDMoneyScraper, save_results
        scraper = INDMoneyScraper()
        scraped_data: list[dict] = await scraper.scrape_all()
        
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

        logger.info("  ✅ Scraped and persisted %d fund page(s).", len(scraped_data))
    except Exception as e:
        logger.error("Phase 1 (Scraping) failed: %s", e, exc_info=True)
        _save_metadata(success=False)
        return False

    # ── Step 2: Chunk each fund's data ────────────────────────────────────────
    logger.info("📄 Step 2/3 — Chunking scraped data...")
    try:
        from phase2_processing.chunker import FundChunker
        from dataclasses import asdict
        
        chunker = FundChunker()
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
                
                logger.debug("  Chunked and saved %d chunks for '%s'.", len(chunks), fund_key)
            except Exception as exc:
                logger.error("  ❌ Chunking failed for '%s': %s", fund_key, exc)
        
        # Update summary file
        summary_file = chunks_dir / "all_chunks_summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump({
                "total_chunks": len(all_chunks),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, f, indent=2)

        logger.info("  ✅ Total chunks created and persisted: %d", len(all_chunks))
    except Exception as e:
        logger.error("Phase 2 (Chunking) failed: %s", e, exc_info=True)
        _save_metadata(success=False)
        return False

    # ── Step 3: Embed & upsert into ChromaDB ─────────────────────────────────
    logger.info("📦 Step 3/3 — Upserting %d chunks into ChromaDB...", len(all_chunks))
    try:
        from phase3_embedding.index_builder import MFVectorStore
        store = MFVectorStore()
        store.add_chunks(all_chunks)
        logger.info("  ✅ ChromaDB upsert complete.")
    except Exception as e:
        logger.error("Phase 3 (Indexing) failed: %s", e, exc_info=True)
        _save_metadata(success=False)
        return False

    _save_metadata(success=True)
    logger.info("✅ Master Refresh Pipeline completed successfully.")
    return True


if __name__ == "__main__":
    asyncio.run(run_refresh_pipeline())
