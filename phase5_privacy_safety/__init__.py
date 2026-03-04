"""
Phase 5 — Privacy, Safety & Compliance Layer
==============================================
Provides the unified safety gate that every user query passes through
before reaching the RAG pipeline.

Public API
----------
SafetyGate       : Single entry-point — run ``SafetyGate.check(query)``
PIIFilter        : Regex-based Indian PII detector
AdviceGuardrail  : Investment-advice / comparison refusal
AuditLogger      : Append-only compliance log
"""

from phase5_privacy_safety.pii_filter import PIIFilter
from phase5_privacy_safety.advice_guardrail import AdviceGuardrail
from phase5_privacy_safety.safety_gate import SafetyGate, SafetyDecision
from phase5_privacy_safety.audit_logger import AuditLogger

__version__ = "1.0.0"

__all__ = [
    "PIIFilter",
    "AdviceGuardrail",
    "SafetyGate",
    "SafetyDecision",
    "AuditLogger",
]
