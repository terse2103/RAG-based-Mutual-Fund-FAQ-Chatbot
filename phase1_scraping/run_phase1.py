"""
Phase 1 — CLI entry point.

Usage:
    python -m phase1_scraping.run_phase1

This will:
  1. Launch a headless Chromium browser via Playwright.
  2. Scrape all 6 whitelisted INDMoney fund pages.
  3. Save structured JSON files to  data/raw/<fund_key>_<date>.json
  4. Save a scrape_metadata.json with status for each fund.
"""

import asyncio
import logging
import sys
import time

from phase1_scraping.indmoney_scraper import run_scraper
from phase1_scraping.config import ALLOWED_SOURCES

# ── Logging setup ────────────────────────────────────────────────────

LOG_FORMAT = "%(asctime)s | %(name)-10s | %(levelname)-7s | %(message)s"

def setup_logging():
    """Configure console + file logging."""
    from pathlib import Path
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/phase1_scrape.log", encoding="utf-8"),
        ],
    )


# ── Main ─────────────────────────────────────────────────────────────

async def main():
    setup_logging()
    logger = logging.getLogger("phase1")

    print("=" * 60)
    print("  Phase 1 — Data Ingestion & Scraping (Playwright)")
    print("=" * 60)
    print(f"\n  Funds to scrape: {len(ALLOWED_SOURCES)}")
    for key, cfg in ALLOWED_SOURCES.items():
        print(f"    • {cfg['fund_name']}")
    print()

    start = time.time()
    logger.info("Starting Phase 1 scraping pipeline...")

    try:
        results = await run_scraper()
    except Exception as exc:
        logger.error("Phase 1 pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start

    # ── Summary ──────────────────────────────────────────────────
    success = [r for r in results if r["scrape_status"] == "success"]
    partial = [r for r in results if r["scrape_status"] == "partial"]
    failed  = [r for r in results if r["scrape_status"] == "failed"]

    print("\n" + "=" * 60)
    print("  Scraping Complete!")
    print("=" * 60)
    print(f"  ✅ Success : {len(success)}")
    print(f"  ⚠️  Partial : {len(partial)}")
    print(f"  ❌ Failed  : {len(failed)}")
    print(f"  ⏱️  Elapsed : {elapsed:.1f}s")
    print()

    for r in results:
        status_icon = {"success": "✅", "partial": "⚠️", "failed": "❌"}.get(r["scrape_status"], "?")
        fields_count = len([v for v in r["fields"].values() if v is not None])
        print(f"  {status_icon} {r['fund_key']:<35} fields={fields_count}")
        if r["errors"]:
            for err in r["errors"]:
                print(f"       ⤷ {err}")

    print(f"\n  Output: data/raw/")
    print(f"  Metadata: data/scrape_metadata.json")
    print("=" * 60)

    logger.info(
        "Phase 1 complete — success=%d, partial=%d, failed=%d, time=%.1fs",
        len(success), len(partial), len(failed), elapsed,
    )

    return results


if __name__ == "__main__":
    asyncio.run(main())
