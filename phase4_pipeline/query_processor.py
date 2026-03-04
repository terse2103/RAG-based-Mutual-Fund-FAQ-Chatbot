"""
Phase 4 — Query Processor
==========================
Two responsibilities:
  1. `identify_fund`   — fuzzy-match user query against fund name aliases
                         to find which specific fund (if any) is being asked about.
  2. `classify_intent` — keyword scan to map the query to a chunk_type so
                         the retriever can optionally narrow its search.

Design notes
------------
* Uses `thefuzz` (python-Levenshtein optional but speeds things up) for
  partial-ratio fuzzy matching with a configurable score_cutoff.
* Intent classification is deterministic keyword lookup — fast and fully
  debuggable (no LLM needed at this stage).
* Both methods are pure functions on the instance (no state mutation) so
  the processor is safe to share across threads.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger("phase4.query_processor")

# ---------------------------------------------------------------------------
# Try to import thefuzz; fall back gracefully so tests still run without it
# ---------------------------------------------------------------------------
try:
    from thefuzz import fuzz, process as fuzz_process  # type: ignore
    _THEFUZZ_AVAILABLE = True
except ImportError:  # pragma: no cover
    _THEFUZZ_AVAILABLE = False
    logger.warning(
        "thefuzz not installed — fund identification will use simple substring matching. "
        "Install with: pip install thefuzz python-Levenshtein"
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Map of human-readable aliases → canonical fund_key values used in ChromaDB
FUND_ALIASES: dict[str, str] = {
    # ELSS Tax Saver
    "nippon elss":              "nippon_elss_tax_saver",
    "elss tax saver":           "nippon_elss_tax_saver",
    "tax saver fund":           "nippon_elss_tax_saver",
    "nippon india elss":        "nippon_elss_tax_saver",
    # Nifty Auto Index
    "nifty auto index":         "nippon_nifty_auto_index",
    "auto index fund":          "nippon_nifty_auto_index",
    "nippon auto index":        "nippon_nifty_auto_index",
    # Short Duration
    "short duration":           "nippon_short_duration",
    "nippon short duration":    "nippon_short_duration",
    # CRISIL IBX AAA
    "crisil ibx":               "nippon_crisil_ibx_aaa",
    "ibx aaa":                  "nippon_crisil_ibx_aaa",
    "nippon crisil":            "nippon_crisil_ibx_aaa",
    # Silver ETF FOF
    "silver etf":               "nippon_silver_etf_fof",
    "silver fund":              "nippon_silver_etf_fof",
    "nippon silver":            "nippon_silver_etf_fof",
    # Balanced Advantage
    "balanced advantage":       "nippon_balanced_advantage",
    "nippon baf":               "nippon_balanced_advantage",
    "baf fund":                 "nippon_balanced_advantage",
    "nippon balanced":          "nippon_balanced_advantage",
}

#: Map intent label → keyword triggers (checked in order; first match wins)
INTENT_KEYWORDS: dict[str, list[str]] = {
    "expense_exit":   ["expense ratio", "expense", "exit load", "exit", "charges", "ter"],
    "sip_investment": ["sip", "minimum sip", "lumpsum", "lump sum", "invest", "minimum investment"],
    "risk_benchmark": ["risk", "riskometer", "risk level", "benchmark", "index benchmark"],
    "lockin_tax":     ["lock-in", "lock in", "lockin", "elss lock", "tax saving", "tax benefit", "80c"],
    "overview":       ["aum", "fund manager", "nav", "net asset value", "about", "category", "amc"],
    "returns":        ["return", "returns", "performance", "cagr", "1 year", "3 year", "5 year"],
    "holdings":       ["holdings", "portfolio", "top holdings", "sector", "allocation"],
}

#: Minimum fuzzy score for fund identification (0–100)
FUZZY_SCORE_CUTOFF = 60


class QueryProcessor:
    """
    Stateless query analyser.

    Parameters
    ----------
    fund_aliases : dict, optional
        Override the default FUND_ALIASES mapping.
    intent_keywords : dict, optional
        Override the default INTENT_KEYWORDS mapping.
    fuzzy_cutoff : int
        Minimum partial-ratio score (0–100) to accept a fuzzy fund match.
    """

    def __init__(
        self,
        fund_aliases: Optional[dict[str, str]] = None,
        intent_keywords: Optional[dict[str, list[str]]] = None,
        fuzzy_cutoff: int = FUZZY_SCORE_CUTOFF,
    ) -> None:
        self.fund_aliases    = fund_aliases    or FUND_ALIASES
        self.intent_keywords = intent_keywords or INTENT_KEYWORDS
        self.fuzzy_cutoff    = fuzzy_cutoff

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def identify_funds(self, query: str) -> list[str]:
        """
        Return a list of canonical fund_keys mentioned in *query*.
        Returns an empty list if no fund is detected.
        """
        query_lower = query.lower()
        found_keys: set[str] = set()

        if _THEFUZZ_AVAILABLE:
            # We use extractBests to find all matches above the cutoff
            matches = fuzz_process.extractBests(
                query_lower,
                self.fund_aliases.keys(),
                scorer=fuzz.partial_ratio,
                score_cutoff=self.fuzzy_cutoff,
            )
            for m_text, m_score in matches:
                found_keys.add(self.fund_aliases[m_text])
        else:
            # Simple substring fallback
            for alias, fund_key in self.fund_aliases.items():
                if alias in query_lower:
                    found_keys.add(fund_key)

        return sorted(list(found_keys))

    def identify_fund(self, query: str) -> Optional[str]:
        """Backward compatibility for singular identification."""
        funds = self.identify_funds(query)
        return funds[0] if funds else None

    def classify_intent(self, query: str) -> str:
        """
        Return the intent label that best describes *query*.

        Returns one of the keys in INTENT_KEYWORDS, or ``"general"`` if
        no keyword matches.
        """
        query_lower = query.lower()
        for intent, keywords in self.intent_keywords.items():
            if any(kw in query_lower for kw in keywords):
                logger.debug("Intent classified as '%s' for query: '%s'", intent, query[:80])
                return intent
        logger.debug("Intent classified as 'general' for query: '%s'", query[:80])
        return "general"

    def analyse(self, query: str) -> dict:
        """
        Convenience method — returns both analysis results in one call.

        Returns
        -------
        dict with keys:
          ``fund_key``  : str | None   — canonical fund key or None
          ``intent``    : str          — classified intent label
          ``query``     : str          — original query (pass-through)
        """
        return {
            "fund_key": self.identify_fund(query),
            "intent":   self.classify_intent(query),
            "query":    query,
        }
