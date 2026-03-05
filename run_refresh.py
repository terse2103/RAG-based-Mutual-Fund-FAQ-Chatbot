"""
run_refresh.py — Standalone Daily Data Refresh
===============================================
This script is designed to be called by the Windows Task Scheduler at
10:00 AM IST every day. It runs the complete data refresh pipeline
(scrape → chunk → embed into ChromaDB) independently of whether the
FastAPI server is running.

Usage (manual):
    python run_refresh.py

Scheduled via Windows Task Scheduler (set up by setup_task_scheduler.bat):
    Trigger  : Daily at 10:00 AM
    Action   : python <project_root>\\run_refresh.py
    Start in : <project_root>
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# ── Ensure project root is on sys.path ────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Change CWD so relative paths (data/, vectorstore/, .env) resolve correctly
os.chdir(PROJECT_ROOT)

# ── Load .env (GROQ_API_KEY etc.) ─────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # dotenv not installed; rely on environment variables being set

# ── Logging ───────────────────────────────────────────────────────────────────
log_dir = PROJECT_ROOT / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / "run_refresh.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("run_refresh")


def main() -> int:
    """Run the full refresh pipeline. Returns 0 on success, 1 on failure."""
    logger.info("=" * 60)
    logger.info("  Daily Refresh — Triggered by Windows Task Scheduler")
    logger.info("  Project root : %s", PROJECT_ROOT)
    logger.info("=" * 60)

    try:
        from phase9_scheduler.scheduler import DailyRefreshScheduler
        scheduler = DailyRefreshScheduler()
        scheduler.trigger_manual_refresh()
        logger.info("✅ Daily refresh completed successfully.")
        return 0
    except Exception as exc:
        logger.error("❌ Daily refresh failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
