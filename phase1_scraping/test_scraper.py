"""
Unit tests for Phase 1 — Scraper components.

Run:  python -m pytest phase1_scraping/test_scraper.py -v
"""

import json
import os
import pytest
from pathlib import Path

from phase1_scraping.config import ALLOWED_SOURCES, SCRAPER_CONFIG, RAW_DATA_DIR


# ── Config tests ─────────────────────────────────────────────────────

class TestConfig:
    """Validate the URL registry and scraper settings."""

    def test_allowed_sources_count(self):
        """There must be exactly 6 whitelisted funds."""
        assert len(ALLOWED_SOURCES) == 6

    def test_all_urls_are_indmoney(self):
        """Every URL must point to indmoney.com/mutual-funds/."""
        for key, cfg in ALLOWED_SOURCES.items():
            assert cfg["url"].startswith("https://www.indmoney.com/mutual-funds/"), (
                f"{key} has invalid URL: {cfg['url']}"
            )

    def test_all_funds_have_required_fields(self):
        """Each fund entry must have url, fund_name, and category."""
        required = {"url", "fund_name", "category"}
        for key, cfg in ALLOWED_SOURCES.items():
            missing = required - set(cfg.keys())
            assert not missing, f"{key} is missing fields: {missing}"

    def test_fund_keys_are_valid_identifiers(self):
        """Fund keys should be valid Python identifiers (snake_case)."""
        for key in ALLOWED_SOURCES:
            assert key.isidentifier(), f"Invalid fund key: {key}"

    def test_scraper_config_has_required_keys(self):
        """Scraper config must define all critical settings."""
        required = {
            "headless", "browser_args", "user_agent", "viewport",
            "navigation_timeout", "selector_timeout",
            "retry_attempts", "retry_backoff_base",
        }
        missing = required - set(SCRAPER_CONFIG.keys())
        assert not missing, f"SCRAPER_CONFIG is missing: {missing}"

    def test_timeouts_are_positive(self):
        assert SCRAPER_CONFIG["navigation_timeout"] > 0
        assert SCRAPER_CONFIG["selector_timeout"] > 0

    def test_retry_config(self):
        assert SCRAPER_CONFIG["retry_attempts"] >= 1
        assert SCRAPER_CONFIG["retry_backoff_base"] >= 1


# ── Scraper class tests (unit-level, no network) ────────────────────

class TestScraperInit:
    """Test scraper initialisation (no browser launch)."""

    def test_scraper_instantiates(self):
        from phase1_scraping.indmoney_scraper import INDMoneyScraper
        scraper = INDMoneyScraper()
        assert scraper.browser is None
        assert scraper.context is None

    def test_save_results_creates_directory(self, tmp_path, monkeypatch):
        """save_results should create the raw data directory."""
        import phase1_scraping.indmoney_scraper as mod

        fake_raw = str(tmp_path / "data" / "raw")
        fake_meta = str(tmp_path / "data" / "scrape_metadata.json")
        monkeypatch.setattr(mod, "RAW_DATA_DIR", fake_raw)
        monkeypatch.setattr(mod, "METADATA_FILE", fake_meta)

        sample = [{
            "fund_key": "test_fund",
            "source_url": "https://example.com",
            "scraped_at": "2026-03-02T10:00:00",
            "fields": {"fund_name": "Test Fund"},
            "raw_text": "Hello world",
            "scrape_status": "success",
            "errors": [],
        }]

        mod.save_results(sample)

        assert Path(fake_raw).exists()
        files = list(Path(fake_raw).glob("*.json"))
        assert len(files) == 1
        assert "test_fund" in files[0].name

        # Check metadata
        assert Path(fake_meta).exists()
        with open(fake_meta) as f:
            meta = json.load(f)
        assert "test_fund" in meta["funds"]
        assert meta["funds"]["test_fund"]["status"] == "success"


# ── Integration smoke test (requires Playwright + network) ──────────

class TestScraperIntegration:
    """
    Live integration tests — only run when PHASE1_INTEGRATION=1.
    These tests actually launch a browser and hit the real website.
    """

    @pytest.fixture(autouse=True)
    def _skip_unless_integration(self):
        if not os.environ.get("PHASE1_INTEGRATION"):
            pytest.skip("Set PHASE1_INTEGRATION=1 to run live tests")

    @pytest.mark.asyncio
    async def test_scrape_single_fund(self):
        from phase1_scraping.indmoney_scraper import INDMoneyScraper
        scraper = INDMoneyScraper()
        await scraper._launch_browser()

        config = ALLOWED_SOURCES["nippon_elss_tax_saver"]
        data = await scraper.scrape_fund_page(
            config["url"], "nippon_elss_tax_saver", config
        )

        await scraper._close_browser()

        assert data["scrape_status"] in ("success", "partial")
        assert data["fields"].get("fund_name")
        assert data["raw_text"]  # fallback text must exist

    @pytest.mark.asyncio
    async def test_scrape_all_funds(self):
        from phase1_scraping.indmoney_scraper import run_scraper
        results = await run_scraper()
        assert len(results) == 6
        for r in results:
            assert r["scrape_status"] in ("success", "partial", "failed")


# ── Data cleaner tests (offline — no network required) ──────────────

RAW_TEXT_SAMPLE = """
Nippon India ELSS Tax Saver Fund Overview

Get key fund statistics, minimum investment details, AUM, expense ratio, exit load, and tax treatment.

Expense ratio
1.03%
Benchmark
Nifty 500 TR INR
AUM
₹14881 Cr
Inception Date
1 January, 2013
Min Lumpsum/SIP
₹500/₹500
Exit Load
0%
Lock In
3 Years
TurnOver
14.11%
Risk
Very High Risk

NAV as on 27 Feb 2026

₹143.00

Period	1M	3M	6M	1Y	3Y	5Y
This Fund	3.46%	-1.74%	2.27%	17.84%	19.65%	16.89%

The NAV of the fund today is ₹143.00.
The fund managers are Rupesh Patel, Lokesh Maru, Ritesh Rathod, Kinjal Desai, Divya Sharma.
The expense ratio is 1.03%.
The AUM of the fund is ₹14881 Cr.
Nippon India ELSS Tax Saver Fund fund has generated a return of 17.84% in 1 year, 19.65% in 3 years, 16.89% in 5 years.
The top 3 holdings of the fund are ICICI Bank Ltd(7.36%), HDFC Bank Ltd(6.99%), Axis Bank Ltd(4.7%)
Minimum investment for lump sum payment is INR 500.00 and for SIP is INR 500.00.
There is a lock in period for Nippon India ELSS Tax Saver Fund of 3 Years

Sector Allocation

Equity 98.6%

Financial Services
38.7%
Industrial
11.5%
Consumer Cyclical
11.3%
Consumer Defensive
9.5%
"""


class TestDataCleaner:
    """Offline tests for RawTextExtractor using the ELSS sample text."""

    @pytest.fixture
    def extractor(self):
        from phase1_scraping.data_cleaner import RawTextExtractor
        return RawTextExtractor(RAW_TEXT_SAMPLE, fund_name="Nippon India ELSS Tax Saver Fund")

    def test_expense_ratio(self, extractor):
        assert extractor.extract_expense_ratio() == "1.03%"

    def test_nav(self, extractor):
        assert extractor.extract_nav() == "₹143.00"

    def test_nav_date(self, extractor):
        assert extractor.extract_nav_date() == "27 Feb 2026"

    def test_returns_all_three(self, extractor):
        returns = extractor.extract_returns()
        assert returns.get("1Y") == "17.84%"
        assert returns.get("3Y") == "19.65%"
        assert returns.get("5Y") == "16.89%"

    def test_fund_managers_all(self, extractor):
        managers = extractor.extract_fund_managers()
        assert managers is not None
        assert "Rupesh Patel" in managers
        assert "Lokesh Maru" in managers

    def test_holdings_top3(self, extractor):
        holdings = extractor.extract_holdings()
        assert len(holdings) >= 3
        names = [h["name"] for h in holdings]
        assert any("ICICI" in n for n in names)
        assert any("HDFC" in n for n in names)

    def test_min_sip_lumpsum(self, extractor):
        sip, lumpsum = extractor.extract_min_sip_lumpsum()
        assert sip == "₹500"
        assert lumpsum == "₹500"

    def test_lock_in(self, extractor):
        assert extractor.extract_lock_in() == "3 Years"

    def test_benchmark(self, extractor):
        assert extractor.extract_benchmark() == "Nifty 500 TR INR"

    def test_risk_level(self, extractor):
        assert "High" in extractor.extract_risk_level()

    def test_aum(self, extractor):
        aum = extractor.extract_aum()
        assert aum is not None and "14881" in aum

    def test_sector_allocation(self, extractor):
        sectors = extractor.extract_sector_allocation()
        assert len(sectors) >= 3
        sector_names = [s["sector"] for s in sectors]
        assert "Financial Services" in sector_names

    def test_clean_file_integration(self, tmp_path):
        """clean_file should produce valid output with all key fields non-null."""
        import json
        from phase1_scraping.data_cleaner import clean_file

        # Write a minimal raw JSON to tmp_path
        raw = {
            "fund_key": "nippon_elss_tax_saver",
            "source_url": "https://example.com",
            "scraped_at": "2026-03-02T10:00:00",
            "fields": {"fund_name": "Nippon India ELSS Tax Saver Fund"},
            "raw_text": RAW_TEXT_SAMPLE,
            "scrape_status": "success",
            "errors": [],
        }
        raw_file = tmp_path / "nippon_elss_tax_saver_2026-03-02.json"
        raw_file.write_text(json.dumps(raw), encoding="utf-8")

        result = clean_file(raw_file, tmp_path)
        f = result["fields"]

        assert f["expense_ratio"] == "1.03%"
        assert f["returns"]["1Y"] == "17.84%"
        assert f["returns"]["5Y"] == "16.89%"
        assert len(f["holdings"]) >= 3
        assert "cleaned_at" in result
