"""
Phase 9 — CLI Entry Point
=========================
Starts the DailyRefreshScheduler as a **blocking** foreground process.

The script:
  1. Configures logging (console + file).
  2. Instantiates DailyRefreshScheduler.
  3. Optionally runs an immediate refresh if ``--refresh-now`` flag is passed
     or if the data is stale (>24 h old).
  4. Starts the scheduler and blocks until Ctrl-C / SIGTERM.

Usage
-----
    # Start scheduler (auto-refresh on startup if stale, then daily at 10 AM)
    python -m phase9_scheduler.run_phase9

    # Force an immediate refresh, then stay running for daily jobs
    python -m phase9_scheduler.run_phase9 --refresh-now

    # Dry-run: only do a one-off refresh and exit (no ongoing schedule)
    python -m phase9_scheduler.run_phase9 --once
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

# ── Logging setup ──────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("phase9.run")

# ── Graceful-shutdown helper ───────────────────────────────────────────────────
_shutdown_requested = False


def _handle_signal(signum, frame):  # noqa: ANN001
    global _shutdown_requested
    logger.info("🛑 Shutdown signal received (%s). Stopping scheduler …", signum)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m phase9_scheduler.run_phase9",
        description="Phase 9 — Automated Daily Scheduler for the MF FAQ Chatbot",
    )
    parser.add_argument(
        "--refresh-now",
        action="store_true",
        help="Run an immediate full refresh before starting the cron schedule.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Run a single refresh and exit — useful for CI/CD pipelines "
            "or manual one-off data updates."
        ),
    )
    parser.add_argument(
        "--stale-hours",
        type=int,
        default=24,
        help=(
            "Number of hours after which data is considered stale. "
            "A startup refresh is triggered automatically if data is stale. "
            "(default: 24)"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("=" * 60)
    logger.info("  Phase 9 — Automated Daily Scheduler")
    logger.info("  Schedule: Daily at 10:00 AM IST (Asia/Kolkata)")
    logger.info("=" * 60)

    # Import here (after logging is configured) to get tidy log output
    from phase9_scheduler.scheduler import DailyRefreshScheduler

    scheduler = DailyRefreshScheduler()

    # ── One-off mode: refresh and exit ────────────────────────────────────────
    if args.once:
        logger.info("🏃 --once mode: running a single refresh then exiting.")
        scheduler.trigger_manual_refresh()
        report = scheduler.get_status_report()
        logger.info("Status after refresh: %s", report)
        logger.info("✅ Done. Exiting.")
        return

    # ── Start the background cron schedule ────────────────────────────────────
    scheduler.start()

    # ── Immediate refresh (explicit flag or stale data) ────────────────────────
    if args.refresh_now:
        logger.info("🔧 --refresh-now flag set — triggering immediate refresh …")
        scheduler.trigger_manual_refresh()
    else:
        scheduler.maybe_refresh_on_startup(max_age_hours=args.stale_hours)

    # ── Block until shutdown ───────────────────────────────────────────────────
    report = scheduler.get_status_report()
    logger.info(
        "📋 Scheduler status: last_refresh=%s | next_run=%s",
        report["last_refresh"],
        report["next_run"],
    )
    logger.info("⏳ Running … press Ctrl-C to stop.\n")

    try:
        while not _shutdown_requested:
            time.sleep(5)
    finally:
        scheduler.stop()
        logger.info("👋 Phase 9 scheduler exited cleanly.")


if __name__ == "__main__":
    main()
