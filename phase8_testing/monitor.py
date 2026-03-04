"""
Phase 8 — Runtime Performance Monitor
=======================================
Lightweight, zero-dependency instrumentation for tracking:
  - Query-level latency (safety gate + retrieval + LLM + total)
  - LLM token usage (prompt tokens, completion tokens)
  - Session-level stats (queries per session, block rate)
  - Health checks (vector store connectivity, LLM API reachability)

Design principles
-----------------
  * Pure Python stdlib only — no Prometheus, no OpenTelemetry dependency.
  * Thread-safe counters (uses threading.Lock).
  * Queryable via get_stats() so Streamlit can show a live admin panel.
  * Each timing context uses Python's time.perf_counter for sub-ms resolution.

Usage
-----
    from phase8_testing.monitor import PerformanceMonitor, timed

    monitor = PerformanceMonitor()

    with monitor.timer("retrieval"):
        results = retriever.retrieve(query)

    monitor.record_llm_usage(prompt_tokens=150, completion_tokens=80)
    monitor.record_query_result(blocked=False, latency_ms=320.5)
    print(monitor.get_stats())
"""

from __future__ import annotations

import time
import logging
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Optional

logger = logging.getLogger("phase8.monitor")


# ── Dataclasses ───────────────────────────────────────────────────────────

@dataclass
class QueryRecord:
    """Single query performance snapshot."""
    timestamp: float          # perf_counter epoch (relative)
    total_ms: float
    safety_ms: float
    retrieval_ms: float
    llm_ms: float
    blocked: bool
    block_reason: Optional[str]
    prompt_tokens: int
    completion_tokens: int


@dataclass
class HealthStatus:
    """Result of a health check."""
    component: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


# ── Monitor class ─────────────────────────────────────────────────────────

class PerformanceMonitor:
    """
    Thread-safe runtime monitor for the full RAG pipeline.

    Parameters
    ----------
    history_size : int
        How many recent queries to keep in the rolling window (default 100).
    """

    def __init__(self, history_size: int = 100) -> None:
        self._lock = threading.Lock()
        self._history: deque[QueryRecord] = deque(maxlen=history_size)

        # Accumulators (live)
        self._total_queries     = 0
        self._total_blocked     = 0
        self._total_tokens_in   = 0
        self._total_tokens_out  = 0
        self._total_latency_ms  = 0.0

        # Per-request staging (cleared after each record_query_result call)
        self._stage_ms: dict[str, float] = {}

    # ── Timer context manager ─────────────────────────────────────────────

    @contextmanager
    def timer(self, stage: str) -> Generator[None, None, None]:
        """
        Context manager: measure elapsed time for a pipeline stage.

        Stages: ``"safety"``, ``"retrieval"``, ``"llm"``, ``"total"``
        """
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            with self._lock:
                self._stage_ms[stage] = elapsed_ms
            logger.debug("Stage '%s' completed in %.1f ms", stage, elapsed_ms)

    # ── Recording ─────────────────────────────────────────────────────────

    def record_llm_usage(
        self,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
    ) -> None:
        """Record token usage from the LLM response."""
        with self._lock:
            self._total_tokens_in  += prompt_tokens
            self._total_tokens_out += completion_tokens

    def record_query_result(
        self,
        blocked: bool,
        block_reason: Optional[str] = None,
        total_ms: Optional[float] = None,
    ) -> None:
        """
        Finalise a query record using accumulated stage timings.
        Call this once per query at the end of the pipeline.
        """
        with self._lock:
            stages    = self._stage_ms
            t_total   = total_ms or stages.get("total", 0.0)
            t_safety  = stages.get("safety",    0.0)
            t_ret     = stages.get("retrieval", 0.0)
            t_llm     = stages.get("llm",       0.0)

            record = QueryRecord(
                timestamp=time.perf_counter(),
                total_ms=t_total,
                safety_ms=t_safety,
                retrieval_ms=t_ret,
                llm_ms=t_llm,
                blocked=blocked,
                block_reason=block_reason,
                prompt_tokens=0,
                completion_tokens=0,
            )
            self._history.append(record)
            self._total_queries    += 1
            self._total_latency_ms += t_total
            if blocked:
                self._total_blocked += 1

            # Reset staging for next query
            self._stage_ms = {}

        logger.info(
            "Query recorded | blocked=%s | total=%.0fms | "
            "safety=%.0fms | retrieval=%.0fms | llm=%.0fms",
            blocked, t_total, t_safety, t_ret, t_llm,
        )

    # ── Stats ─────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """
        Return a snapshot of all monitoring metrics.
        Safe to call from any thread.
        """
        with self._lock:
            n = self._total_queries
            recent = list(self._history)

        if n == 0:
            return {
                "total_queries":    0,
                "blocked":          0,
                "allow_rate_pct":   100.0,
                "avg_latency_ms":   0.0,
                "p95_latency_ms":   0.0,
                "total_tokens_in":  self._total_tokens_in,
                "total_tokens_out": self._total_tokens_out,
                "recent_queries":   [],
            }

        latencies = sorted(r.total_ms for r in recent)
        p95_idx   = max(0, int(len(latencies) * 0.95) - 1)

        return {
            "total_queries":     n,
            "blocked":           self._total_blocked,
            "allow_rate_pct":    round(100 * (n - self._total_blocked) / n, 1),
            "avg_latency_ms":    round(self._total_latency_ms / n, 1),
            "p95_latency_ms":    round(latencies[p95_idx], 1) if latencies else 0.0,
            "total_tokens_in":   self._total_tokens_in,
            "total_tokens_out":  self._total_tokens_out,
            "recent_queries":    [
                {
                    "total_ms":    round(r.total_ms, 1),
                    "blocked":     r.blocked,
                    "block_reason": r.block_reason,
                }
                for r in list(self._history)[-10:]   # last 10
            ],
        }

    def reset(self) -> None:
        """Reset all counters (useful between test runs)."""
        with self._lock:
            self._history.clear()
            self._total_queries    = 0
            self._total_blocked    = 0
            self._total_tokens_in  = 0
            self._total_tokens_out = 0
            self._total_latency_ms = 0.0
            self._stage_ms         = {}


# ── Health checks ─────────────────────────────────────────────────────────

def check_vectorstore_health(persist_dir: str = "data/vectorstore") -> HealthStatus:
    """Verify the ChromaDB vector store is reachable and has documents."""
    t0 = time.perf_counter()
    try:
        from phase3_embedding.embedder import MFVectorStore
        store = MFVectorStore(persist_dir=persist_dir)
        count = store.collection.count()
        latency = (time.perf_counter() - t0) * 1000
        healthy = count > 0
        return HealthStatus(
            component="ChromaDB VectorStore",
            healthy=healthy,
            latency_ms=round(latency, 1),
            error=None if healthy else f"Collection has {count} documents — may be empty",
        )
    except Exception as exc:
        return HealthStatus(
            component="ChromaDB VectorStore",
            healthy=False,
            error=str(exc),
        )


def check_groq_health() -> HealthStatus:
    """Verify the Groq API key is set and the client can be instantiated."""
    t0 = time.perf_counter()
    try:
        import os
        from groq import Groq
        key = os.getenv("GROQ_API_KEY")
        if not key:
            return HealthStatus(component="Groq API", healthy=False, error="GROQ_API_KEY not set")
        Groq(api_key=key)   # just instantiate — no network call
        latency = (time.perf_counter() - t0) * 1000
        return HealthStatus(component="Groq API", healthy=True, latency_ms=round(latency, 1))
    except Exception as exc:
        return HealthStatus(component="Groq API", healthy=False, error=str(exc))


def run_all_health_checks(persist_dir: str = "data/vectorstore") -> list[HealthStatus]:
    """Run all health checks and return results list."""
    return [
        check_vectorstore_health(persist_dir),
        check_groq_health(),
    ]


# ── Singleton convenience ─────────────────────────────────────────────────

# Shared monitor instance — import and use directly in any module
_global_monitor = PerformanceMonitor()


def get_monitor() -> PerformanceMonitor:
    """Return the shared global PerformanceMonitor instance."""
    return _global_monitor


# ── CLI self-test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    m = PerformanceMonitor()

    print("\n" + "=" * 55)
    print("  Phase 8 — Performance Monitor Demo")
    print("=" * 55)

    # Simulate 5 queries
    for i in range(5):
        with m.timer("safety"):      time.sleep(0.002)
        with m.timer("retrieval"):   time.sleep(0.015)
        with m.timer("llm"):         time.sleep(0.120)
        with m.timer("total"):       time.sleep(0.145)
        m.record_llm_usage(prompt_tokens=140, completion_tokens=60)
        m.record_query_result(blocked=(i == 2), block_reason="ADVICE" if i == 2 else None)

    stats = m.get_stats()
    print(f"\n  Total queries   : {stats['total_queries']}")
    print(f"  Blocked         : {stats['blocked']}")
    print(f"  Allow rate      : {stats['allow_rate_pct']}%")
    print(f"  Avg latency     : {stats['avg_latency_ms']} ms")
    print(f"  P95 latency     : {stats['p95_latency_ms']} ms")
    print(f"  Tokens in/out   : {stats['total_tokens_in']} / {stats['total_tokens_out']}")

    print("\n  Health checks:")
    for h in run_all_health_checks():
        icon = "✅" if h.healthy else "❌"
        print(f"  {icon} {h.component}: {h.error or f'{h.latency_ms} ms'}")

    print("\n" + "=" * 55 + "\n")
