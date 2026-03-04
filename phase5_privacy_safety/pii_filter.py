"""
Phase 5 — Enhanced PII Filter
================================
Detects and blocks Indian Personally Identifiable Information (PII)
in user queries before they reach the LLM.

Detects
-------
- PAN card numbers        (e.g., ABCDE1234F)
- Aadhaar numbers         (e.g., 1234 5678 9012)
- Indian phone numbers    (e.g., +91-9876543210)
- Email addresses
- Bank account numbers    (9–18 digit numeric strings)
- OTP contexts            (4–6 digit codes near otp/verify/code)
- Credit/debit card numbers

Enhancements over the phase5_privacy stub
------------------------------------------
- Returns a ``PIICheckResult`` dataclass (not just bool) so callers
  know which PII type was detected — useful for audit logging.
- Adds ``sanitize()`` method that redacts PII inplace (for logging).
- Provides a structured refusal message keyed by PII type.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("phase5.pii_filter")


# ── PII pattern registry ──────────────────────────────────────────────────

_PII_PATTERNS: dict[str, str] = {
    "PAN Card":       r"[A-Z]{5}[0-9]{4}[A-Z]",
    "Aadhaar":        r"\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b",
    "Phone":          r"\b(?:\+91[\s\-]?)?[6-9]\d{9}\b",
    "Email":          r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    "Account Number": r"\b\d{9,18}\b",
    "OTP":            r"(?i)\b\d{4,6}\b(?=.*\b(?:otp|code|verify|verification)\b)",
    "Credit Card":    r"\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b",
}

# Contextual keywords — presence alone triggers a block
_PII_KEYWORDS: list[str] = [
    "my pan", "my aadhaar", "my account", "my phone number",
    "my email", "my otp", "my password", "here is my",
    "my debit card", "my credit card", "my bank", "my ifsc",
]

# Friendly redaction tokens
_REDACTION_MAP: dict[str, str] = {
    "PAN Card":       "[PAN-REDACTED]",
    "Aadhaar":        "[AADHAAR-REDACTED]",
    "Phone":          "[PHONE-REDACTED]",
    "Email":          "[EMAIL-REDACTED]",
    "Account Number": "[ACCOUNT-REDACTED]",
    "OTP":            "[OTP-REDACTED]",
    "Credit Card":    "[CARD-REDACTED]",
}


# ── Result dataclass ──────────────────────────────────────────────────────

@dataclass
class PIICheckResult:
    """Outcome of a PII scan."""
    contains_pii: bool
    detected_types: list[str] = field(default_factory=list)
    triggered_keyword: Optional[str] = None

    @property
    def primary_type(self) -> Optional[str]:
        """The first PII type found (for audit/display)."""
        return self.detected_types[0] if self.detected_types else self.triggered_keyword


# ── Main class ────────────────────────────────────────────────────────────

class PIIFilter:
    """
    Regex + keyword PII detector.

    Usage
    -----
    result = PIIFilter.scan("My PAN is ABCDE1234F")
    if result.contains_pii:
        print(PIIFilter.warning(result))

    # Legacy API (backwards-compatible with phase5_privacy stub)
    if PIIFilter.contains_pii("some text"):
        print(PIIFilter.get_pii_warning())
    """

    # Compiled patterns (class-level, compiled once)
    _compiled: dict[str, re.Pattern] = {
        label: re.compile(pattern, re.IGNORECASE)
        for label, pattern in _PII_PATTERNS.items()
    }

    # ── Primary API ───────────────────────────────────────────────────────

    @classmethod
    def scan(cls, text: str) -> PIICheckResult:
        """
        Full PII scan returning a structured result.

        Parameters
        ----------
        text : user query string

        Returns
        -------
        PIICheckResult with contains_pii flag, detected_types list,
        and triggered_keyword if a keyword match triggered the block.
        """
        detected: list[str] = []

        # 1. Regex scan — run every pattern
        for label, pattern in cls._compiled.items():
            if pattern.search(text):
                detected.append(label)
                logger.warning(
                    "PII detected | type=%s | query_preview='%s'",
                    label, text[:60],
                )

        # 2. Keyword scan
        text_lower = text.lower()
        triggered_kw: Optional[str] = None
        for kw in _PII_KEYWORDS:
            if kw in text_lower:
                triggered_kw = kw
                logger.warning(
                    "PII keyword triggered | kw='%s' | query_preview='%s'",
                    kw, text[:60],
                )
                break

        has_pii = bool(detected) or (triggered_kw is not None)
        return PIICheckResult(
            contains_pii=has_pii,
            detected_types=detected,
            triggered_keyword=triggered_kw,
        )

    @classmethod
    def sanitize(cls, text: str) -> str:
        """
        Redact all PII from *text* and return the sanitized version.
        Useful for safe logging of user queries.
        """
        sanitized = text
        for label, pattern in cls._compiled.items():
            replacement = _REDACTION_MAP.get(label, "[REDACTED]")
            sanitized = pattern.sub(replacement, sanitized)
        return sanitized

    @classmethod
    def warning(cls, result: PIICheckResult) -> str:
        """
        Return a user-facing refusal message tailored to the detected PII type.
        """
        pii_type = result.primary_type or "personal information"
        return (
            f"⚠️ **Privacy Protection Active**\n\n"
            f"Your message appears to contain **{pii_type}**.\n\n"
            "I am designed to answer questions about mutual fund facts only. "
            "I cannot process, store, or acknowledge any personal or financial "
            "identifiers including PAN, Aadhaar, phone numbers, email addresses, "
            "account numbers, or OTPs.\n\n"
            "Please rephrase your question focusing solely on fund details "
            "(expense ratio, exit load, minimum SIP, risk level, etc.)."
        )

    # ── Legacy API (backward-compatible with phase5_privacy.pii_filter) ──

    @classmethod
    def contains_pii(cls, text: str) -> bool:
        """Legacy boolean API — prefer ``scan()`` for new code."""
        return cls.scan(text).contains_pii

    @staticmethod
    def get_pii_warning() -> str:
        """Legacy string API — prefer ``warning(result)`` for new code."""
        return (
            "⚠️ **PII Policy Violation**\n\n"
            "I cannot process queries containing personal information such as "
            "PAN card numbers, Aadhaar, phone numbers, or bank account details. "
            "Please rephrase your question focusing only on mutual fund facts."
        )


# ── CLI smoke test ────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_cases = [
        ("Clean query",                    "What is the expense ratio of Nippon ELSS?"),
        ("PAN number",                     "My PAN is ABCDE1234F, what is NAV?"),
        ("Aadhaar number",                 "Aadhaar: 1234 5678 9012, show my portfolio"),
        ("Indian phone",                   "Call me at +91 9876543210"),
        ("Email address",                  "Send info to test@gmail.com"),
        ("PII keyword",                    "Here is my account number: 12345678"),
        ("Credit card",                    "My card is 4111 1111 1111 1111"),
        ("OTP context",                    "My OTP is 123456, please verify"),
    ]
    print("\n" + "="*60)
    print("  Phase 5 — PII Filter Smoke Test")
    print("="*60)
    for name, query in test_cases:
        result = PIIFilter.scan(query)
        flag = "🔴 BLOCKED" if result.contains_pii else "🟢 CLEAN  "
        types = ", ".join(result.detected_types) or result.triggered_keyword or "—"
        print(f"  {flag}  [{name}]  detected={types}")
    print("="*60 + "\n")
