"""
Phase 5 — Compliance Audit Logger
=====================================
Append-only structured audit trail for every query processed by the
safety gate.  Covers both blocked and allowed queries so the full
request lifecycle is auditable.

Log format
----------
Each entry is a JSON object written as a single line (JSONL format)
to ``logs/safety_audit.jsonl``.

Fields logged per entry
-----------------------
timestamp       : ISO-8601 UTC datetime
session_id      : Optional session identifier (from Streamlit)
query_sanitized : Original query with all PII redacted (safe to log)
decision        : "ALLOWED" | "BLOCKED"
block_reason    : null | "PII" | "ADVICE" | "COMPARISON" | "COMPUTATION"
                  | "OFF_TOPIC" | "PROMPT_INJ"
pii_types       : list of PII type labels detected (empty if none)
guardrail_phrase: matched keyword/phrase (null if not blocked)
latency_ms      : time taken by the safety gate (null if not measured)

Retention policy
----------------
The logger rotates the JSONL file daily, keeping 30 days of history.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

from phase5_privacy_safety.safety_gate import SafetyDecision
from phase5_privacy_safety.pii_filter import PIIFilter

logger = logging.getLogger("phase5.audit_logger")

# ── Audit log path ────────────────────────────────────────────────────────

_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
_AUDIT_LOG_DIR  = os.path.join(_PROJECT_ROOT, "logs")
_AUDIT_LOG_PATH = os.path.join(_AUDIT_LOG_DIR, "safety_audit.jsonl")


def _ensure_log_dir() -> None:
    os.makedirs(_AUDIT_LOG_DIR, exist_ok=True)


# ── JSONL writer setup ────────────────────────────────────────────────────

def _get_audit_writer() -> logging.Logger:
    """Return a stdlib Logger that writes raw JSONL lines, rotated daily."""
    audit_logger = logging.getLogger("phase5.audit_jsonl")
    if audit_logger.handlers:
        return audit_logger                 # already configured

    _ensure_log_dir()
    handler = TimedRotatingFileHandler(
        filename=_AUDIT_LOG_PATH,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        utc=True,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    audit_logger.addHandler(handler)
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = False          # don't bubble to root logger
    return audit_logger


# ── Main class ────────────────────────────────────────────────────────────

class AuditLogger:
    """
    Write a single JSONL audit entry for each safety gate decision.

    Usage
    -----
    decision = SafetyGate.check(query)
    AuditLogger.log(
        query=query,
        decision=decision,
        session_id=st.session_state.get("session_id"),
        latency_ms=elapsed_ms,
    )
    """

    _writer = None

    @classmethod
    def _get_writer(cls) -> logging.Logger:
        if cls._writer is None:
            cls._writer = _get_audit_writer()
        return cls._writer

    @classmethod
    def log(
        cls,
        query: str,
        decision: SafetyDecision,
        session_id: Optional[str] = None,
        latency_ms: Optional[float] = None,
    ) -> None:
        """
        Append one JSONL audit record.

        Parameters
        ----------
        query       : Raw user query (will be sanitized before logging).
        decision    : SafetyDecision returned by SafetyGate.check().
        session_id  : Optional Streamlit session identifier.
        latency_ms  : Time taken for the safety check in milliseconds.
        """
        try:
            # Sanitize before writing — never log raw PII
            safe_query = PIIFilter.sanitize(query)

            entry = {
                "timestamp":        datetime.now(tz=timezone.utc).isoformat(),
                "session_id":       session_id,
                "query_sanitized":  safe_query,
                "decision":         "ALLOWED" if decision.allowed else "BLOCKED",
                "block_reason":     decision.block_reason,
                "pii_types":        decision.pii_result.detected_types,
                "pii_keyword":      decision.pii_result.triggered_keyword,
                "guardrail_phrase": (
                    decision.guardrail_result.matched_phrase
                    if decision.guardrail_result else None
                ),
                "latency_ms":       round(latency_ms, 2) if latency_ms else None,
            }

            cls._get_writer().info(json.dumps(entry, ensure_ascii=False))
            logger.debug("Audit entry written | decision=%s", entry["decision"])

        except Exception as exc:
            # Never crash the app because of audit logging
            logger.error("Audit log write failed: %s", exc)

    @classmethod
    def read_recent(cls, n: int = 50) -> list[dict]:
        """
        Read the last *n* audit entries from the current log file.
        Useful for an admin dashboard or compliance report.

        Returns
        -------
        List of dicts, most recent first.
        """
        if not os.path.exists(_AUDIT_LOG_PATH):
            return []
        try:
            with open(_AUDIT_LOG_PATH, encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            entries = []
            for ln in reversed(lines[-n * 2:]):     # read last 2n lines, parse JSON
                try:
                    entries.append(json.loads(ln))
                    if len(entries) >= n:
                        break
                except json.JSONDecodeError:
                    continue
            return entries
        except Exception as exc:
            logger.error("Failed to read audit log: %s", exc)
            return []

    @classmethod
    def get_stats(cls, n: int = 500) -> dict:
        """
        Compute quick stats from the last *n* audit entries.

        Returns
        -------
        dict with keys: total, allowed, blocked, block_breakdown,
        top_pii_types, top_guardrail_phrases.
        """
        entries = cls.read_recent(n)
        if not entries:
            return {"total": 0, "allowed": 0, "blocked": 0, "block_breakdown": {}}

        total   = len(entries)
        allowed = sum(1 for e in entries if e.get("decision") == "ALLOWED")
        blocked = total - allowed

        breakdown: dict[str, int] = {}
        pii_types: dict[str, int] = {}

        for e in entries:
            reason = e.get("block_reason")
            if reason:
                breakdown[reason] = breakdown.get(reason, 0) + 1
            for pt in e.get("pii_types", []):
                pii_types[pt] = pii_types.get(pt, 0) + 1

        return {
            "total":           total,
            "allowed":         allowed,
            "blocked":         blocked,
            "block_breakdown": breakdown,
            "top_pii_types":   pii_types,
            "audit_log_path":  _AUDIT_LOG_PATH,
        }
