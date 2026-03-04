"""
Top-Level Scheduler Entry Point
================================
Thin wrapper kept at project root for backward compatibility.
The full implementation lives in `phase9_scheduler/`.

Usage:
    python scheduler.py                  # start with auto-startup refresh
    python scheduler.py --refresh-now    # force immediate refresh first
    python scheduler.py --once           # one-off refresh and exit
"""

from phase9_scheduler.run_phase9 import main

if __name__ == "__main__":
    main()
