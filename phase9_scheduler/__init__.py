"""
Phase 9 — Automated Daily Scheduler
=====================================
Provides ``DailyRefreshScheduler``: an APScheduler-backed class that
triggers the full scrape → chunk → embed pipeline daily at 10:00 AM IST.

Sub-modules
-----------
scheduler      : Core ``DailyRefreshScheduler`` class
run_phase9     : CLI entry point — spin up the blocking scheduler process
test_scheduler : Unit + integration tests for the scheduler module
"""

from phase9_scheduler.scheduler import DailyRefreshScheduler  # noqa: F401

__version__ = "1.0.0"
__all__ = ["DailyRefreshScheduler"]
