"""
Phase 7 — Frontend Configuration
==================================
All UI-level constants: fund registry (display names → keys),
suggested questions, and sidebar metadata.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Fund Registry — mirrors scraper config but as display names for the UI
# ---------------------------------------------------------------------------

FUND_DISPLAY_MAP: dict[str, str] = {
    "All Funds": "",
    "Nippon India ELSS Tax Saver Fund - Direct Plan Growth": "nippon_elss_tax_saver",
    "Nippon India Nifty Auto Index Fund - Direct Growth": "nippon_nifty_auto_index",
    "Nippon India Short Duration Fund - Direct Plan Growth": "nippon_short_duration",
    "Nippon India CRISIL IBX AAA Financial Svcs Dec 2026 Index Fund - Direct Growth": "nippon_crisil_ibx_aaa",
    "Nippon India Silver ETF Fund of Fund (FOF) - Direct Growth": "nippon_silver_etf_fof",
    "Nippon India Balanced Advantage Fund - Direct Growth Plan": "nippon_balanced_advantage",
}

FUND_CATEGORIES: dict[str, str] = {
    "nippon_elss_tax_saver": "Equity / Tax Saver",
    "nippon_nifty_auto_index": "Equity / Sectoral Index Fund",
    "nippon_short_duration": "Debt / Short Duration",
    "nippon_crisil_ibx_aaa": "Debt / Target Maturity",
    "nippon_silver_etf_fof": "Commodity / Silver ETF FOF",
    "nippon_balanced_advantage": "Hybrid / Balanced Advantage",
}

FUND_URLS: dict[str, str] = {
    "nippon_elss_tax_saver": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
    "nippon_nifty_auto_index": "https://www.indmoney.com/mutual-funds/nippon-india-nifty-auto-index-fund-direct-growth-1048613",
    "nippon_short_duration": "https://www.indmoney.com/mutual-funds/nippon-india-short-duration-fund-direct-plan-growth-plan-2268",
    "nippon_crisil_ibx_aaa": "https://www.indmoney.com/mutual-funds/nippon-india-crisil-ibx-aaa-financial-svcs-dec-2026-idx-fd-dir-growth-1048293",
    "nippon_silver_etf_fof": "https://www.indmoney.com/mutual-funds/nippon-india-silver-etf-fund-of-fund-fof-direct-growth-1040380",
    "nippon_balanced_advantage": "https://www.indmoney.com/mutual-funds/nippon-india-balanced-advantage-fund-direct-growth-plan-4324",
}

# ---------------------------------------------------------------------------
# Suggested starter questions (shown as clickable chips)
# ---------------------------------------------------------------------------

SUGGESTED_QUESTIONS: list[str] = [
    "What is the expense ratio of Nippon India ELSS Tax Saver Fund?",
    "What is the minimum SIP for the Balanced Advantage Fund?",
    "What is the lock-in period for the ELSS fund?",
    "What is the risk level of Nippon Silver ETF FOF?",
    "Who is the fund manager of the Short Duration Fund?",
    "What is the benchmark index of the Nifty Auto Index Fund?",
    "What is the exit load for Nippon ELSS Tax Saver Fund?",
    "What is the AUM of Nippon India Balanced Advantage Fund?",
]

# ---------------------------------------------------------------------------
# RAG Chain settings exposed to the UI
# ---------------------------------------------------------------------------

VECTORSTORE_DIR = "data/vectorstore"
TOP_K_DEFAULT = 3
SIMILARITY_THRESHOLD = 0.35

# ---------------------------------------------------------------------------
# App metadata
# ---------------------------------------------------------------------------

APP_TITLE = "Nippon India Mutual Fund FAQ Chatbot"
APP_SUBTITLE = "Factual Q&A powered by RAG + Groq · Data sourced exclusively from INDMoney"
APP_ICON = "🏦"
