"""
Phase 7 — Frontend & Backend: FastAPI Server
=============================================
Standalone FastAPI application — no Streamlit dependency.

Serves
------
GET  /             — HTML chatbot UI (vanilla HTML / CSS / JS)
GET  /health       — liveness probe (used by JS to show status badge)
POST /chat         — main RAG endpoint (safety gate + retrieval + LLM)
GET  /funds        — returns the fund registry for the sidebar fund list

Run with:
    uvicorn phase7_frontend.api_server:app --host 0.0.0.0 --port 8000 --reload
      OR
    python -m phase7_frontend.run_app
"""

from __future__ import annotations

import os
import sys
import logging
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# ── project root on sys.path ──────────────────────────────────────────────
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dotenv import load_dotenv
load_dotenv(os.path.join(_project_root, ".env"))

# ── phase imports ─────────────────────────────────────────────────────────
from phase3_embedding.embedder import MFVectorStore
from phase4_pipeline.rag_chain import RAGChain
from phase6_generation.generator import ResponseGenerator
from phase5_privacy_safety.safety_gate import SafetyGate
from phase9_scheduler.scheduler import DailyRefreshScheduler
from phase7_frontend.config import VECTORSTORE_DIR, FUND_DISPLAY_MAP, FUND_CATEGORIES, FUND_URLS

logger = logging.getLogger("phase7.api")

# ── RAG chain & Scheduler singletons ──────────────────────────────────────
_chain: Optional[RAGChain] = None
_scheduler: Optional[DailyRefreshScheduler] = None
_init_lock = threading.Lock()


def _init_pipeline() -> tuple[RAGChain, DailyRefreshScheduler]:
    """Lazily initialise the pipeline (thread-safe singleton)."""
    global _chain, _scheduler
    with _init_lock:
        if _chain is None:
            store = MFVectorStore(
                persist_dir=os.path.join(_project_root, VECTORSTORE_DIR)
            )
            gen   = ResponseGenerator()
            _chain = RAGChain(vector_store=store, generate_fn=gen.generate)
            logger.info("RAG chain initialised ✓")
            
        if _scheduler is None:
            # We pass our initialized vector store to avoid re-initializing
            _scheduler = DailyRefreshScheduler(vector_store=_chain.vector_store)
            _scheduler.start()
            logger.info("Daily scheduler started ✓")
            
    return _chain, _scheduler


# ── App lifecycle ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        chain, scheduler = _init_pipeline()
        # Run startup refresh in a background thread so the server starts
        # immediately without blocking — if data is stale (>24 h) it refreshes.
        def _startup_refresh():
            try:
                scheduler.maybe_refresh_on_startup(max_age_hours=24)
            except Exception as exc:
                logger.warning("Startup refresh failed (non-fatal): %s", exc)

        t = threading.Thread(target=_startup_refresh, daemon=True, name="startup-refresh")
        t.start()
        logger.info("Startup refresh check initiated in background thread ✓")
    except Exception as exc:
        logger.error("Failed to init RAG pipeline: %s", exc, exc_info=True)
    yield
    if _scheduler:
        _scheduler.stop()
    logger.info("API server shut down.")


# ── FastAPI app ───────────────────────────────────────────────────────────
app = FastAPI(
    title="Nippon India MF FAQ API",
    description="RAG-powered FAQ chatbot — Frontend & Backend served by FastAPI",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HTML UI ───────────────────────────────────────────────────────────────
_HTML_PATH = os.path.join(os.path.dirname(__file__), "templates", "chatbot.html")


@app.get("/", include_in_schema=False)
def serve_ui():
    """Serve the vanilla HTML/CSS/JS chatbot UI."""
    return FileResponse(_HTML_PATH, media_type="text/html")


# ── Schemas ───────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    query:           str           = Field(..., min_length=1, max_length=500)
    fund_filter:     Optional[str] = Field(None, description="Fund key for a specific fund (e.g. 'nippon_elss_tax_saver'), or None for all")
    category_filter: Optional[str] = Field(None, description="Category name ('Equity', 'Debt', 'Hybrid') or None for all")


class ChatResponse(BaseModel):
    blocked:     bool
    block_reason: Optional[str] = None
    response:    Optional[str]  = None   # pre-built refusal (when blocked)
    answer:      Optional[str]  = None
    sources:     list[str]      = []
    scraped_at:  Optional[str]  = None
    intent:      Optional[str]  = None
    fund_keys:   list[str]      = []
    no_results:  bool           = False


# ── Endpoints ─────────────────────────────────────────────────────────────
@app.get("/health", summary="Liveness probe")
def health():
    chain_ready = _chain is not None
    return {
        "status":      "ok"     if chain_ready else "degraded",
        "backend":     "online" if chain_ready else "offline",
        "chain_ready": chain_ready,
    }


@app.post("/chat", response_model=ChatResponse, summary="Ask the RAG chatbot")
def chat(req: ChatRequest):
    """
    Process a query through the full pipeline:
      1. Safety gate (PII + advice + off-topic + injection checks)
      2. RAG chain (retrieval → LLM → post-processing)
    """
    # ── Step 1: safety gate ───────────────────────────────────────────────
    decision = SafetyGate.check(req.query)
    if not decision.allowed:
        logger.info("Query blocked | reason=%s | query='%s'", decision.block_reason, req.query[:80])
        return ChatResponse(
            blocked=True,
            block_reason=decision.block_reason,
            response=decision.response,
        )

    # ── Step 2: RAG pipeline ──────────────────────────────────────────────
    chain = _chain
    if chain is None:
        raise HTTPException(status_code=503, detail="Backend not ready — please retry shortly.")

    try:
        result = chain.run(
            query=req.query,
            fund_filter=req.fund_filter or None,
            category_filter=req.category_filter or None,
        )
        logger.info(
            "RAG result | no_results=%s | intent=%s | funds=%s",
            result.get("no_results"), result.get("intent"), result.get("fund_keys"),
        )
    except Exception as exc:
        logger.error("RAG chain error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}")

    return ChatResponse(
        blocked=False,
        answer=result.get("answer") or result.get("context", ""),
        sources=result.get("sources", []),
        scraped_at=result.get("scraped_at"),
        intent=result.get("intent"),
        fund_keys=result.get("fund_keys", []),
        no_results=result.get("no_results", False),
    )


@app.post("/refresh", summary="Trigger a full data refresh")
def trigger_refresh():
    """
    Manually trigger the complete end-to-end data refresh pipeline:
      Step 0 — Purge previous day's stale data files
      Step 1 — Phase 1: Scrape all 6 INDMoney fund pages
      Step 2 — Phase 2: Chunk the scraped data
      Step 3 — Phase 3: Upsert embeddings into ChromaDB

    Blocks until the pipeline completes (~30-60 s) so the UI receives
    an unambiguous success/failure response with the updated timestamp.
    """
    if _scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not ready.")

    try:
        _scheduler.trigger_manual_refresh()
        return {
            "status": "success",
            "message": "Full data refresh completed (purge → scrape → chunk → embed).",
            "last_refresh": _scheduler.last_refresh,
        }
    except Exception as exc:
        logger.error("Manual refresh failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Refresh failed: {exc}")


@app.get("/status", summary="Scheduler & data freshness status")
def get_status():
    """
    Returns the scheduler status including the last successful refresh
    timestamp, so the UI can display 'Data last updated: <datetime>'.
    """
    if _scheduler is None:
        return {
            "last_refresh": None,
            "is_running": False,
            "next_run": None,
            "is_stale": True,
        }
    report = _scheduler.get_status_report()
    return {
        "last_refresh": report.get("last_refresh"),   # ISO-8601 string or "Never"
        "is_running":   report.get("is_running"),
        "next_run":     report.get("next_run"),
        "is_stale":     report.get("is_stale"),
    }


@app.get("/funds", summary="Return fund registry")
def get_funds():
    """Returns all covered funds with their display name, key, category, and source URL."""
    funds = [
        {
            "display_name": display,
            "fund_key":     key,
            "category":     FUND_CATEGORIES.get(key, ""),
            "url":          FUND_URLS.get(key, "#"),
        }
        for display, key in FUND_DISPLAY_MAP.items()
        if key  # skip the "All Funds" entry
    ]
    return {"funds": funds, "total": len(funds)}
