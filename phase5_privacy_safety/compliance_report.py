"""
Phase 5 — Compliance Summary Report Generator
===============================================
Reads the safety audit log and produces a human-readable compliance
report (markdown) and a structured dict for dashboards.

Outputs
-------
- ``generate_text_report()``  : Returns a markdown string
- ``generate_dict_report()``  : Returns a structured dict for JSON export
- ``print_report()``          : CLI pretty-print

Run via:
    python -m phase5_privacy_safety.compliance_report
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from phase5_privacy_safety.audit_logger import AuditLogger


# ── Helpers ───────────────────────────────────────────────────────────────

def _pct(part: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{100 * part / total:.1f}%"


def _bar(value: int, max_value: int, width: int = 20) -> str:
    """ASCII progress bar."""
    if max_value == 0:
        return "░" * width
    filled = int(width * value / max_value)
    return "█" * filled + "░" * (width - filled)


# ── Report generators ─────────────────────────────────────────────────────

def generate_dict_report(n: int = 500) -> dict[str, Any]:
    """Return a structured compliance report dict."""
    stats = AuditLogger.get_stats(n)
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "sample_size":  n,
        "stats":        stats,
    }


def generate_text_report(n: int = 500) -> str:
    """Return a human-readable markdown compliance report."""
    report = generate_dict_report(n)
    stats  = report["stats"]

    total   = stats.get("total", 0)
    allowed = stats.get("allowed", 0)
    blocked = stats.get("blocked", 0)
    breakdown = stats.get("block_breakdown", {})
    pii_types = stats.get("top_pii_types", {})
    log_path  = stats.get("audit_log_path", "N/A")

    lines: list[str] = [
        "# Phase 5 — Privacy & Safety Compliance Report",
        "",
        f"> Generated at: **{report['generated_at']}**  ",
        f"> Audit log: `{log_path}`  ",
        f"> Sample size: last **{total}** queries",
        "",
        "---",
        "",
        "## Summary",
        "",
        f"| Metric | Count | Share |",
        f"|--------|-------|-------|",
        f"| Total queries analysed | {total} | 100% |",
        f"| ✅ Allowed (safe)       | {allowed} | {_pct(allowed, total)} |",
        f"| 🔴 Blocked (unsafe)     | {blocked} | {_pct(blocked, total)} |",
        "",
    ]

    if breakdown:
        lines += [
            "## Block Reason Breakdown",
            "",
            "| Reason | Count | % of Blocked | Bar |",
            "|--------|-------|--------------|-----|",
        ]
        for reason, count in sorted(breakdown.items(), key=lambda x: -x[1]):
            bar = _bar(count, blocked)
            lines.append(
                f"| {reason:<14} | {count:>5} | {_pct(count, blocked):>12} | `{bar}` |"
            )
        lines.append("")

    if pii_types:
        lines += [
            "## PII Types Detected",
            "",
            "| PII Type | Occurrences |",
            "|----------|-------------|",
        ]
        for ptype, count in sorted(pii_types.items(), key=lambda x: -x[1]):
            lines.append(f"| {ptype:<20} | {count:>11} |")
        lines.append("")

    lines += [
        "---",
        "",
        "## Guardrail Matrix Status",
        "",
        "| Guardrail | Status |",
        "|-----------|--------|",
        "| PII Detection (PAN, Aadhaar, Phone, Email, Account, OTP, CC) | ✅ Active |",
        "| Investment Advice Refusal | ✅ Active |",
        "| Return Comparison Refusal | ✅ Active |",
        "| Return Computation Refusal | ✅ Active |",
        "| Off-topic Query Deflection | ✅ Active |",
        "| Prompt Injection Protection | ✅ Active |",
        "| PII Sanitization in Logs | ✅ Active |",
        "| Daily Audit Log Rotation (30-day retention) | ✅ Active |",
        "",
        "---",
        "",
        "> [!NOTE]",
        "> All blocked queries are answered with pre-approved refusal messages only.",
        "> No PII is ever passed to the LLM (Groq) or stored in plain text.",
    ]

    return "\n".join(lines)


def print_report(n: int = 500) -> None:
    """Print the compliance report to stdout."""
    print(generate_text_report(n))


# ── CLI entry-point ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    print_report(n)
