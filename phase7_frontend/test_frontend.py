"""
Phase 7 — Frontend Unit & Smoke Tests
=======================================
Tests for the non-Streamlit, pure-Python parts of Phase 7:
  • config.py   : fund registry, display map correctness
  • styles.py   : CSS string completeness
  • ui_components.py : HTML helper functions return expected strings

Tests do NOT import streamlit and are safe to run anywhere.

Run:
    pytest phase7_frontend/test_frontend.py -v
"""

from __future__ import annotations

import pytest

# ── Phase 7 imports ──────────────────────────────────────────────────────
from phase7_frontend.config import (
    APP_ICON,
    APP_SUBTITLE,
    APP_TITLE,
    FUND_CATEGORIES,
    FUND_DISPLAY_MAP,
    FUND_URLS,
    SUGGESTED_QUESTIONS,
    VECTORSTORE_DIR,
)
from phase7_frontend.styles import MAIN_CSS


# ═══════════════════════════════════════════════════════════════════════════
# config.py tests
# ═══════════════════════════════════════════════════════════════════════════

class TestConfig:

    def test_fund_display_map_has_all_funds_entry(self):
        """'All Funds' must always be the first entry."""
        keys = list(FUND_DISPLAY_MAP.keys())
        assert keys[0] == "All Funds", "First entry must be 'All Funds'"

    def test_fund_display_map_has_six_funds(self):
        """Exactly 6 fund entries (plus 'All Funds')."""
        assert len(FUND_DISPLAY_MAP) == 7, f"Expected 7 entries, got {len(FUND_DISPLAY_MAP)}"

    def test_all_fund_keys_have_categories(self):
        """Every fund key (except empty 'All Funds') must have a category."""
        for display, key in FUND_DISPLAY_MAP.items():
            if display == "All Funds":
                continue
            assert key in FUND_CATEGORIES, (
                f"Fund key '{key}' (display='{display}') missing from FUND_CATEGORIES"
            )

    def test_all_fund_keys_have_urls(self):
        """Every fund key must have an INDMoney URL."""
        for display, key in FUND_DISPLAY_MAP.items():
            if display == "All Funds":
                continue
            assert key in FUND_URLS, (
                f"Fund key '{key}' missing from FUND_URLS"
            )
            assert FUND_URLS[key].startswith("https://www.indmoney.com"), (
                f"URL for '{key}' must point to indmoney.com"
            )

    def test_suggested_questions_non_empty(self):
        """At least 4 suggested questions must be defined."""
        assert len(SUGGESTED_QUESTIONS) >= 4

    def test_app_metadata_strings(self):
        assert APP_TITLE and isinstance(APP_TITLE, str)
        assert APP_SUBTITLE and isinstance(APP_SUBTITLE, str)
        assert APP_ICON and isinstance(APP_ICON, str)

    def test_vectorstore_dir_string(self):
        assert VECTORSTORE_DIR == "data/vectorstore"


# ═══════════════════════════════════════════════════════════════════════════
# styles.py tests
# ═══════════════════════════════════════════════════════════════════════════

class TestStyles:

    def test_main_css_is_string(self):
        assert isinstance(MAIN_CSS, str)

    def test_css_contains_style_tags(self):
        assert "<style>" in MAIN_CSS and "</style>" in MAIN_CSS

    def test_css_contains_hero_header_class(self):
        # CSS was redesigned: .hero-header renamed to .hero-card in the green-on-black theme
        assert ".hero-card" in MAIN_CSS

    def test_css_contains_fund_pill_class(self):
        assert ".fund-pill" in MAIN_CSS

    def test_css_contains_source_block_class(self):
        assert ".source-block" in MAIN_CSS

    def test_css_contains_metric_card_class(self):
        assert ".metric-card" in MAIN_CSS

    def test_css_contains_pii_warning_class(self):
        assert ".pii-warning" in MAIN_CSS

    def test_css_not_empty(self):
        assert len(MAIN_CSS) > 500, "CSS seems too short — check styles.py"


# ═══════════════════════════════════════════════════════════════════════════
# ui_components.py tests (pure-Python functions only — no Streamlit)
# ═══════════════════════════════════════════════════════════════════════════

class TestUIComponents:
    """
    Only test functions that don't call Streamlit.
    render_similarity_bar and render_pii_warning / render_no_results_response
    are pure Python — safe to test without mocking st.*.
    """

    def test_similarity_bar_full(self):
        """100 % similarity should produce a 100-wide bar."""
        from phase7_frontend.ui_components import render_similarity_bar
        html = render_similarity_bar(1.0)
        assert "width:100%;" in html
        assert "100%" in html

    def test_similarity_bar_zero(self):
        from phase7_frontend.ui_components import render_similarity_bar
        html = render_similarity_bar(0.0)
        assert "width:0%;" in html

    def test_similarity_bar_partial(self):
        from phase7_frontend.ui_components import render_similarity_bar
        html = render_similarity_bar(0.72)
        assert "width:72%;" in html
        assert "72%" in html

    def test_similarity_bar_returns_html_string(self):
        from phase7_frontend.ui_components import render_similarity_bar
        html = render_similarity_bar(0.5)
        assert isinstance(html, str)
        assert "<div" in html

    def test_pii_warning_contains_keywords(self):
        from phase7_frontend.ui_components import render_pii_warning
        warning = render_pii_warning()
        assert "PII" in warning or "Privacy" in warning
        assert "PAN" in warning or "personal" in warning.lower()

    def test_pii_warning_returns_string(self):
        from phase7_frontend.ui_components import render_pii_warning
        assert isinstance(render_pii_warning(), str)

    def test_no_results_response_returns_string(self):
        from phase7_frontend.ui_components import render_no_results_response
        msg = render_no_results_response()
        assert isinstance(msg, str)
        assert len(msg) > 20

    def test_no_results_contains_indmoney_link(self):
        from phase7_frontend.ui_components import render_no_results_response
        msg = render_no_results_response()
        assert "indmoney.com" in msg


# ═══════════════════════════════════════════════════════════════════════════
# Integration smoke test (no Streamlit, no network)
# ═══════════════════════════════════════════════════════════════════════════

class TestFundRegistryIntegrity:
    """Cross-validates the three fund registries are consistent."""

    def test_display_map_keys_match_categories(self):
        """All fund keys in FUND_DISPLAY_MAP must exist in FUND_CATEGORIES."""
        for display, key in FUND_DISPLAY_MAP.items():
            if not key:
                continue
            assert key in FUND_CATEGORIES, (
                f"KEY '{key}' found in FUND_DISPLAY_MAP but missing in FUND_CATEGORIES"
            )

    def test_display_map_keys_match_urls(self):
        """All fund keys in FUND_DISPLAY_MAP must exist in FUND_URLS."""
        for display, key in FUND_DISPLAY_MAP.items():
            if not key:
                continue
            assert key in FUND_URLS, (
                f"KEY '{key}' found in FUND_DISPLAY_MAP but missing in FUND_URLS"
            )

    def test_nippon_elss_key_present(self):
        assert "nippon_elss_tax_saver" in FUND_CATEGORIES

    def test_nippon_balanced_advantage_key_present(self):
        assert "nippon_balanced_advantage" in FUND_CATEGORIES
