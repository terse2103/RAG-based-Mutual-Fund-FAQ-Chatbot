"""
Vercel Entry Point — RAG-based Mutual Fund FAQ Chatbot
======================================================
This module is the ASGI entry point for Vercel's serverless Python runtime.

Key differences from local `phase7_frontend/api_server.py`:
  • APScheduler is DISABLED  — Vercel is stateless/serverless; cron jobs
    cannot run in background threads across invocations.
  • /refresh endpoint        — Returns a 503 with a clear explanation instead
    of launching Playwright (no browser binaries on Vercel).
  • ChromaDB                 — Reads from the pre-built vectorstore committed
    in `data/vectorstore/` (read-only, no writes needed for query serving).
  • SentenceTransformers     — Model is downloaded once and cached inside
    `/tmp` (Vercel's writable temp directory, 512 MB) on first cold start.

The HTML frontend, /chat, /health, /funds, and /status endpoints all work
exactly as in the local server.
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
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

# ── Path setup ────────────────────────────────────────────────────────────────
# On Vercel, the repo is mounted at /var/task; our project root is one level
# above this file (api/index.py → project root).
_THIS_DIR    = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))

if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Point os.getcwd() to project root so relative paths resolve correctly
os.chdir(_PROJECT_ROOT)

# ── Environment ───────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

# ── Override SentenceTransformers cache to writable /tmp on Vercel ────────────
# Vercel's filesystem is read-only except for /tmp.
os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", "/tmp/sentence_transformers")
os.environ.setdefault("HF_HOME", "/tmp/huggingface")
os.environ.setdefault("TRANSFORMERS_CACHE", "/tmp/transformers_cache")

# ── Serverless environment patches ────────────────────────────────────────────
# Vercel uses older Amazon Linux 2 environments with sqlite3 < 3.35, which 
# ChromaDB rejects. This overrides it with the newer pysqlite3-binary.
try:
    __import__('pysqlite3')
    import sys
    sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
except ImportError:
    pass

# ── Phase imports ─────────────────────────────────────────────────────────────
from phase3_embedding.embedder import MFVectorStore
from phase4_pipeline.rag_chain import RAGChain
from phase6_generation.generator import ResponseGenerator
from phase5_privacy_safety.safety_gate import SafetyGate
from phase7_frontend.config import VECTORSTORE_DIR, FUND_DISPLAY_MAP, FUND_CATEGORIES, FUND_URLS

logger = logging.getLogger("vercel.api")
logging.basicConfig(level=logging.INFO)

# ── RAG chain singleton ───────────────────────────────────────────────────────
_chain: Optional[RAGChain] = None
_init_lock = threading.Lock()
_last_refresh: Optional[str] = None  # Read from metadata file if available


def _load_last_refresh() -> Optional[str]:
    """Load the last_refresh timestamp from scrape_metadata.json (read-only)."""
    import json
    meta_path = os.path.join(_PROJECT_ROOT, "data", "scrape_metadata.json")
    try:
        with open(meta_path, encoding="utf-8") as f:
            data = json.load(f)
            return data.get("last_refresh")
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _init_pipeline() -> RAGChain:
    """Lazily initialise the RAG pipeline (thread-safe singleton)."""
    global _chain, _last_refresh
    with _init_lock:
        if _chain is None:
            vectorstore_path = os.path.join(_PROJECT_ROOT, VECTORSTORE_DIR)
            store = MFVectorStore(persist_dir=vectorstore_path)
            gen   = ResponseGenerator()
            _chain = RAGChain(vector_store=store, generate_fn=gen.generate)
            _last_refresh = _load_last_refresh()
            logger.info("RAG chain initialised on Vercel ✓")
    return _chain


# ── App lifecycle ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        _init_pipeline()
    except Exception as exc:
        logger.error("Failed to init RAG pipeline: %s", exc, exc_info=True)
    yield
    logger.info("Vercel function shut down.")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Nippon India MF FAQ API",
    description="RAG-powered FAQ chatbot — Deployed on Vercel",
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

# ── HTML UI ───────────────────────────────────────────────────────────────────
_HTML_PATH = os.path.join(_PROJECT_ROOT, "phase7_frontend", "templates", "chatbot.html")


@app.get("/", include_in_schema=False)
def serve_ui():
    """Serve the vanilla HTML/CSS/JS chatbot UI."""
    return FileResponse(_HTML_PATH, media_type="text/html")


# ── Schemas ───────────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    query:           str           = Field(..., min_length=1, max_length=500)
    fund_filter:     Optional[str] = Field(None, description="Fund key for a specific fund, or None")
    category_filter: Optional[str] = Field(None, description="Category name or None")


class ChatResponse(BaseModel):
    blocked:      bool
    block_reason: Optional[str] = None
    response:     Optional[str] = None
    answer:       Optional[str] = None
    sources:      list[str]     = []
    scraped_at:   Optional[str] = None
    intent:       Optional[str] = None
    fund_keys:    list[str]     = []
    no_results:   bool          = False


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health", summary="Liveness probe")
def health():
    chain_ready = _chain is not None
    return {
        "status":      "ok"     if chain_ready else "degraded",
        "backend":     "online" if chain_ready else "offline",
        "chain_ready": chain_ready,
        "deployment":  "vercel",
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


@app.post("/refresh", summary="Data refresh (disabled on Vercel)")
def trigger_refresh():
    """
    Data refresh is not available on Vercel's serverless platform.
    Vercel's ephemeral filesystem and lack of persistent background processes
    make live scraping with Playwright impossible.

    To update the data:
    1. Run the scrape + embed pipeline locally.
    2. Commit the updated `data/vectorstore/` directory to your repo.
    3. Re-deploy to Vercel.
    """
    return JSONResponse(
        status_code=503,
        content={
            "status": "unavailable",
            "message": (
                "Live data refresh is not available on Vercel (serverless environment). "
                "To update data, run the pipeline locally and re-deploy with the updated vectorstore."
            ),
        },
    )


@app.get("/status", summary="Scheduler & data freshness status")
def get_status():
    """Returns the last known data refresh timestamp from the committed metadata file."""
    return {
        "last_refresh": _last_refresh or "Embedded at deploy time",
        "is_running":   False,
        "next_run":     "N/A (Vercel — no background scheduler)",
        "is_stale":     False,
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
