"""
Phase 7 — Frontend & Backend: Application Runner
=================================================
Launch the FastAPI server (which serves both the REST API and the
vanilla HTML/CSS/JS chatbot UI) using Uvicorn.

Usage:
    python -m phase7_frontend.run_app
      OR
    python phase7_frontend/run_app.py
      OR
    uvicorn phase7_frontend.api_server:app --host 0.0.0.0 --port 8000 --reload

Then open http://localhost:8000 in your browser.
"""

from __future__ import annotations

import os
import sys

# ── Project root on sys.path ──────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Change to project root so relative file paths (data/vectorstore, .env) resolve
os.chdir(PROJECT_ROOT)

API_HOST = "0.0.0.0"
API_PORT = 8000


def main() -> None:
    print("=" * 60)
    print("  Phase 7 — Nippon India MF FAQ Chatbot")
    print("  Frontend & Backend (FastAPI + Vanilla HTML/JS)")
    print("=" * 60)
    print(f"  Project root : {PROJECT_ROOT}")
    print(f"  Server       : http://localhost:{API_PORT}")
    print(f"  API docs     : http://localhost:{API_PORT}/docs")
    print("=" * 60)
    print()

    try:
        import uvicorn
    except ImportError:
        print("  [ERROR] uvicorn is not installed. Run: pip install uvicorn[standard]")
        sys.exit(1)

    uvicorn.run(
        "phase7_frontend.api_server:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,
        log_level="info",
        access_log=True,
    )


if __name__ == "__main__":
    main()
