"""
INDMoney Playwright Scraper — Phase 1

Async scraper that extracts structured mutual fund data from the
6 whitelisted INDMoney pages using Playwright headless Chromium.

Extraction strategy:
  1. Navigate to fund page, wait for JS render (networkidle).
  2. Extract hero section: Fund name, NAV, NAV date, category, AMC.
  3. Click 'Overview' tab → Expense Ratio, Exit Load, Min SIP/Lumpsum,
     Lock-in, Risk Level, Benchmark, AUM.
  4. Scroll to 'Performance' section → Returns (1Y, 3Y, 5Y).
  5. Click 'Holdings' tab → Top 10 holdings.
  6. Click 'Sector Allocation' tab → Sector breakdown.
  7. Click 'About' tab → Fund Manager(s).
  8. Capture raw page text as fallback.
"""

import asyncio
import datetime
import json
import logging
import os
import re
import time
from pathlib import Path

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from phase1_scraping.config import ALLOWED_SOURCES, SCRAPER_CONFIG, RAW_DATA_DIR, METADATA_FILE

logger = logging.getLogger("scraper")


class INDMoneyScraper:
    """Playwright-based async scraper for INDMoney mutual-fund pages."""

    def __init__(self):
        self.playwright = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None

    # ── Browser lifecycle ────────────────────────────────────────────

    async def _launch_browser(self):
        """Start Playwright and open a headless Chromium browser context."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=SCRAPER_CONFIG["headless"],
            args=SCRAPER_CONFIG["browser_args"],
        )
        self.context = await self.browser.new_context(
            user_agent=SCRAPER_CONFIG["user_agent"],
            viewport=SCRAPER_CONFIG["viewport"],
        )
        logger.info("Browser launched (headless=%s)", SCRAPER_CONFIG["headless"])

    async def _close_browser(self):
        """Gracefully shut down the browser and Playwright."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed")

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    async def _safe_text(page: Page, selector: str, timeout: int = 5000) -> str | None:
        """Return inner text at *selector*, or None if not found."""
        try:
            element = page.locator(selector).first
            await element.wait_for(state="visible", timeout=timeout)
            text = await element.inner_text()
            return text.strip() if text else None
        except Exception:
            return None

    @staticmethod
    async def _safe_all_texts(page: Page, selector: str, timeout: int = 5000) -> list[str]:
        """Return list of inner texts for all matches of *selector*."""
        try:
            locator = page.locator(selector)
            await locator.first.wait_for(state="visible", timeout=timeout)
            count = await locator.count()
            return [
                (await locator.nth(i).inner_text()).strip()
                for i in range(count)
            ]
        except Exception:
            return []

    @staticmethod
    async def _click_tab(page: Page, tab_label: str):
        """Click a navigation tab by its visible text."""
        try:
            tab = page.locator(f"button:has-text('{tab_label}'), a:has-text('{tab_label}')").first
            await tab.click()
            await page.wait_for_timeout(1500)  # allow content to render
        except Exception as exc:
            logger.warning("Could not click tab '%s': %s", tab_label, exc)

    # ── Section isolation helpers ───────────────────────────────────

    @staticmethod
    def _extract_section(text: str, start_marker: str, end_markers: list[str] | None = None) -> str:
        """Extract text between start_marker and the first end_marker found.
        
        This prevents cross-section contamination when parsing the full body.
        """
        idx = text.find(start_marker)
        if idx == -1:
            # Try case-insensitive
            lower = text.lower()
            idx = lower.find(start_marker.lower())
        if idx == -1:
            return ""

        section = text[idx:]
        if end_markers:
            best_end = len(section)
            for marker in end_markers:
                pos = section.find(marker, len(start_marker))
                if pos == -1:
                    pos = section.lower().find(marker.lower(), len(start_marker))
                if pos != -1 and pos < best_end:
                    best_end = pos
            section = section[:best_end]
        return section

    # ── Field extraction helpers ─────────────────────────────────────

    async def _extract_hero(self, page: Page, fund_config: dict) -> dict:
        """Extract data visible in the hero / header section."""
        fund_name = await self._safe_text(page, "h1")
        if not fund_name:
            fund_name = fund_config["fund_name"]

        body_text = await page.inner_text("body")

        # NAV — first ₹ amount on the page is typically the NAV
        nav_text = None
        match = re.search(r"₹([\d,]+\.\d+)", body_text)
        if match:
            nav_text = f"₹{match.group(1)}"

        # NAV date — "NAV as on DD Mon YYYY" or "as on DD Mon YYYY"
        nav_date = None
        date_match = re.search(r"(?:NAV\s+)?as on\s+(\d{1,2}\s+\w+\s+\d{4})", body_text, re.IGNORECASE)
        if date_match:
            nav_date = date_match.group(1)

        # Category & AMC from breadcrumb
        category = await self._safe_text(page, "a[href*='funds'] p, [class*='category']", timeout=3000)
        amc = await self._safe_text(page, "a[href*='amc'] p, [class*='amc']", timeout=3000)

        return {
            "fund_name": fund_name,
            "nav": nav_text,
            "nav_date": nav_date,
            "category": category or fund_config["category"],
            "amc": amc,
        }

    async def _extract_overview(self, page: Page) -> dict:
        """Extract key fund info from the bottom Overview section.

        Strategy: prefer the structured 'label\nvalue' block that appears
        *after* the peer comparison table (near the bottom of the page)
        because the top-of-page table can contain noise values for the
        same labels (e.g. '1Y Returns' appearing near 'Expense ratio').
        """
        await self._click_tab(page, "Overview")
        full_text = await page.inner_text("body")

        # ── Isolate the bottom 'Fund Overview' block ──────────────────
        # This section has the pattern:
        #   "Expense ratio\n1.03%\nBenchmark\nNifty 500 TR INR\n..."
        # It appears after "<FundName> Overview" heading near the foot of page.
        overview_idx = full_text.rfind("Overview\n\nGet key fund")
        if overview_idx == -1:
            # Fallback: use the last 'Overview' occurrence
            overview_idx = full_text.rfind("Overview\n")
        overview_text = full_text[overview_idx:] if overview_idx != -1 else full_text
        # Trim at the 'About' section
        for stop in ["About Nippon", "About ", "Fund Manager", "AUM Change"]:
            stop_i = overview_text.find(stop, 50)
            if stop_i != -1:
                overview_text = overview_text[:stop_i]
                break

        def _find_value(label: str, text: str = overview_text) -> str | None:
            """Return value immediately following 'Label\n' (label→newline→value)."""
            pattern = re.compile(
                rf"{re.escape(label)}\s*\n\s*([^\n]+)", re.IGNORECASE
            )
            m = pattern.search(text)
            return m.group(1).strip() if m else None

        expense_ratio = _find_value("Expense ratio")
        # Guard against picking up non-numeric noise
        if expense_ratio and not re.match(r"^[\d.]+%$", expense_ratio):
            expense_ratio = None

        exit_load     = _find_value("Exit Load")
        min_investment = _find_value("Min Lumpsum/SIP")
        lock_in       = _find_value("Lock In") or _find_value("Lock-in")
        risk_level    = _find_value("Risk")
        benchmark     = _find_value("Benchmark")
        aum           = _find_value("AUM")
        turnover      = _find_value("TurnOver")

        # Also try FAQ text as the authoritative source for expense ratio
        if not expense_ratio:
            m = re.search(r"[Tt]he expense ratio is\s*([\d.]+%)", full_text)
            if m:
                expense_ratio = m.group(1)

        # Parse min SIP and lumpsum from "₹500/₹500" format
        min_sip = None
        min_lumpsum = None
        if min_investment and "/" in min_investment:
            parts = min_investment.split("/")
            min_lumpsum = parts[0].strip()
            min_sip     = parts[1].strip()
        elif min_investment:
            # Try FAQ: "lump sum payment is INR 500.00 and for SIP is INR 500.00"
            m = re.search(
                r"lump.?sum.*?(?:INR|₹)\s*([\d,]+(?:\.\d+)?).*?"
                r"SIP.*?(?:INR|₹)\s*([\d,]+(?:\.\d+)?)",
                full_text, re.IGNORECASE | re.DOTALL,
            )
            if m:
                min_lumpsum = f"₹{int(float(m.group(1).replace(',', '')))}"
                min_sip     = f"₹{int(float(m.group(2).replace(',', '')))}"
            else:
                min_sip = min_lumpsum = min_investment

        return {
            "expense_ratio": expense_ratio,
            "exit_load":     exit_load,
            "min_sip":       min_sip,
            "min_lumpsum":   min_lumpsum,
            "lock_in":       lock_in,
            "risk_level":    risk_level,
            "benchmark":     benchmark,
            "aum":           aum,
            "turnover":      turnover,
        }

    async def _extract_returns(self, page: Page) -> dict:
        """Extract 1Y / 3Y / 5Y returns.

        Primary  : FAQ text "return of X% in 1 year, Y% in 3 years, Z% in 5 years"
        Secondary: 'last 1, 3 and 5 years … CAGR return of X%, Y% and Z% respectively'
        Tertiary : Performance table 'This Fund' row (tab-delimited)
        """
        await self._click_tab(page, "Performance")
        full_text = await page.inner_text("body")
        returns = {}

        def _pct(val: str) -> str:
            return val if "%" in val else val + "%"

        # ── Primary: FAQ phrasing ──────────────────────────────────────
        m = re.search(
            r"return of\s+([\d.]+%?)\s+in 1 years?,?\s+"
            r"([\d.]+%?)\s+in 3 years?,?\s+"
            r"([\d.]+%?)\s+in 5 years?",
            full_text, re.IGNORECASE,
        )
        if m:
            returns["1Y"] = _pct(m.group(1))
            returns["3Y"] = _pct(m.group(2))
            returns["5Y"] = _pct(m.group(3))
            return {"returns": returns}

        # ── Secondary: 'since inception … 1, 3 and 5 years … X%, Y% and Z%' ──
        m = re.search(
            r"last 1,\s*3 and 5 years.*?CAGR return of\s*"
            r"([\d.]+%?),\s*([\d.]+%?) and\s*([\d.]+%?)\s+respectively",
            full_text, re.IGNORECASE | re.DOTALL,
        )
        if m:
            returns["1Y"] = _pct(m.group(1))
            returns["3Y"] = _pct(m.group(2))
            returns["5Y"] = _pct(m.group(3))
            return {"returns": returns}

        # ── Tertiary: performance table tab-delimited rows ─────────────
        lines = full_text.split("\n")
        for i, line in enumerate(lines):
            stripped = line.strip()
            if "1Y" in stripped and "3Y" in stripped and "5Y" in stripped:
                period_headers = [p.strip() for p in re.split(r"\t+", stripped) if p.strip()]
                for j in range(i + 1, min(i + 6, len(lines))):
                    if "this fund" in lines[j].lower():
                        fund_values = [v.strip() for v in re.split(r"\t+", lines[j]) if v.strip()]
                        values  = [v for v in fund_values if "fund" not in v.lower()]
                        headers = [h for h in period_headers if h not in ("Period",)]
                        for h, v in zip(headers, values):
                            if h in ("1Y", "3Y", "5Y"):
                                returns[h] = _pct(v)
                        break
                if returns:
                    break

        # ── Individual fallbacks for any still-missing periods ─────────
        if "5Y" not in returns:
            m5 = re.search(r"([\d.]+%?)\s+in 5 year", full_text, re.IGNORECASE)
            if m5:
                returns["5Y"] = _pct(m5.group(1))
        if "1Y" not in returns or "3Y" not in returns:
            m13 = re.search(
                r"return of\s+([\d.]+%?)\s+in 1 year,?\s+([\d.]+%?)\s+in 3 year",
                full_text, re.IGNORECASE,
            )
            if m13:
                returns.setdefault("1Y", _pct(m13.group(1)))
                returns.setdefault("3Y", _pct(m13.group(2)))

        return {"returns": returns}

    async def _extract_holdings(self, page: Page) -> list[dict]:
        """Extract top-10 holdings from the Holdings Details section."""
        await self._click_tab(page, "Holdings")
        await page.wait_for_timeout(1000)

        # Try clicking "See all" if available
        try:
            see_all = page.locator("text=/See all|View all/i").first
            await see_all.click(timeout=3000)
            await page.wait_for_timeout(1000)
        except Exception:
            pass

        full_text = await page.inner_text("body")
        holdings = []

        # Isolate the Holdings Details section
        holdings_section = self._extract_section(
            full_text,
            "Holdings Details",
            ["Portfolio Changes", "Overview", "Sector Allocation", "About"]
        )

        if not holdings_section:
            holdings_section = self._extract_section(
                full_text,
                "Holdings\n",
                ["Portfolio Changes", "See all", "Overview"]
            )

        if holdings_section:
            # The table rows look like: "ICICI Bank Ltd\t\n7.36%"
            # Or: "ICICI Bank Ltd\n\t\n7.36%\n\t\n\t\n2.3%"
            # We want stock names followed by their weight percentage
            lines = [l.strip() for l in holdings_section.split("\n") if l.strip()]

            i = 0
            while i < len(lines) and len(holdings) < 10:
                line = lines[i]
                # Skip header/label lines
                if line in ("Equity", "Debt", "Holdings", "Weight%", "Holdings Trend",
                            "1M Change", "See all", "See less"):
                    i += 1
                    continue

                # A holding name should be followed by a percentage weight
                if re.match(r"^[A-Z]", line) and "%" not in line:
                    # Look at next non-empty lines for a percentage
                    for j in range(i + 1, min(i + 4, len(lines))):
                        weight_match = re.match(r"^(\d+\.?\d*%?)$", lines[j])
                        if weight_match:
                            weight = weight_match.group(1)
                            if not weight.endswith("%"):
                                weight += "%"
                            wt_float = float(weight.rstrip("%"))
                            # Holdings typically between 0.1% and 20%
                            if 0.1 <= wt_float <= 25:
                                holdings.append({"name": line, "weight": weight})
                            break
                i += 1

        # Fallback: parse from "top 3 holdings" FAQ text
        if not holdings:
            faq_match = re.search(
                r"top (?:3|holdings).*?((?:[A-Z][A-Za-z\s]+Ltd\([\d.]+%\)[,\s]*)+)",
                full_text,
                re.IGNORECASE,
            )
            if faq_match:
                for m in re.finditer(r"([A-Z][A-Za-z\s]+Ltd)\(([\d.]+%)\)", faq_match.group(1)):
                    holdings.append({"name": m.group(1).strip(), "weight": m.group(2)})

        return holdings

    async def _extract_sector_allocation(self, page: Page) -> list[dict]:
        """Extract sector allocation from the Sector Allocation section."""
        await self._click_tab(page, "Sector Allocation")
        await page.wait_for_timeout(1000)
        full_text = await page.inner_text("body")

        sectors = []

        # Isolate the Sector Allocation section
        sector_section = self._extract_section(
            full_text,
            "Sector Allocation\n",
            ["Sector Changes", "Holdings Details", "Overview", "About ", "Compare"]
        )

        if sector_section:
            lines = [l.strip() for l in sector_section.split("\n") if l.strip()]
            i = 0
            while i < len(lines):
                line = lines[i]
                # Skip headers and labels
                if line in ("Sector Allocation", "Equity", "Debt & Cash",
                            "Debt", "Cash", "See all", "See less"):
                    i += 1
                    continue

                # Sector name followed by percentage on next line
                if (re.match(r"^[A-Z][A-Za-z\s&]+$", line)
                        and "%" not in line
                        and len(line) > 2):
                    if i + 1 < len(lines):
                        weight_match = re.match(r"^(\d+\.?\d*%?)$", lines[i + 1])
                        if weight_match:
                            weight = weight_match.group(1)
                            if not weight.endswith("%"):
                                weight += "%"
                            wt_float = float(weight.rstrip("%"))
                            # Sector allocations are typically between 0.5% and 100%
                            if 0.5 <= wt_float <= 100:
                                sectors.append({"sector": line, "weight": weight})
                                i += 2
                                continue
                i += 1

        return sectors

    async def _extract_fund_manager(self, page: Page) -> str | None:
        """Extract ALL fund manager names from the About section.

        Priority 1: FAQ text 'The fund managers are X, Y, Z'
        Priority 2: About section 'Fund Manager\nName' blocks (collects all)
        Priority 3: 'managed by X, Y, Z' anywhere in the page
        """
        await self._click_tab(page, "About")
        full_text = await page.inner_text("body")

        # ── Priority 1: FAQ phrasing with all names ──────────────────
        m = re.search(
            r"[Tt]he fund managers? are\s+(.+?)(?:\.|$)", full_text
        )
        if m:
            return m.group(1).strip().rstrip(".")

        # ── Priority 2: 'Fund Manager\nName\n\nFund Manager of…' blocks ──
        about_section = self._extract_section(
            full_text, "About ", ["AUM Change", "Compare ", "Frequently Asked"]
        ) or full_text

        managers = []
        fm_idx = about_section.find("Fund Manager\n")
        if fm_idx != -1:
            after = about_section[fm_idx + len("Fund Manager\n"):]
            for chunk in after.split("\n\n"):
                candidate = chunk.strip().split("\n")[0].strip()
                if (candidate
                        and re.match(r"^[A-Z][a-z]+ [A-Z]", candidate)
                        and "Fund Manager" not in candidate
                        and "Fund House" not in candidate
                        and len(candidate) < 60
                        and candidate not in managers):
                    managers.append(candidate)
                # Stop after the manager block ends
                if len(managers) >= 6:
                    break
            if managers:
                return ", ".join(managers)

        # ── Priority 3: 'managed by X, Y, Z' anywhere ────────────────
        m2 = re.search(r"managed by\s+([A-Za-z, ]+?)(?:\.|,\s*[A-Z]|\n)",
                        full_text, re.IGNORECASE)
        if m2:
            return m2.group(1).strip().rstrip(".")

        return None

    # ── Single-page scrape ───────────────────────────────────────────

    async def scrape_fund_page(self, url: str, fund_key: str, fund_config: dict) -> dict:
        """Scrape a single INDMoney fund page and return structured data."""
        page = await self.context.new_page()
        data = {
            "fund_key": fund_key,
            "source_url": url,
            "scraped_at": datetime.datetime.now().isoformat(),
            "fields": {},
            "raw_text": "",
            "scrape_status": "success",
            "errors": [],
        }

        try:
            logger.info("Scraping %s → %s", fund_key, url)
            await page.goto(
                url,
                wait_until="networkidle",
                timeout=60000,
            )
            # Wait for JS rendering to complete
            await page.wait_for_timeout(5000)

            # --- Extract each section ---
            try:
                hero = await self._extract_hero(page, fund_config)
                data["fields"].update(hero)
            except Exception as e:
                data["errors"].append(f"hero: {e}")
                logger.error("%s hero extraction failed: %s", fund_key, e)

            try:
                overview = await self._extract_overview(page)
                data["fields"].update(overview)
            except Exception as e:
                data["errors"].append(f"overview: {e}")
                logger.error("%s overview extraction failed: %s", fund_key, e)

            try:
                returns_data = await self._extract_returns(page)
                data["fields"].update(returns_data)
            except Exception as e:
                data["errors"].append(f"returns: {e}")
                logger.error("%s returns extraction failed: %s", fund_key, e)

            try:
                holdings = await self._extract_holdings(page)
                data["fields"]["holdings"] = holdings
            except Exception as e:
                data["errors"].append(f"holdings: {e}")
                logger.error("%s holdings extraction failed: %s", fund_key, e)

            try:
                sectors = await self._extract_sector_allocation(page)
                data["fields"]["sector_allocation"] = sectors
            except Exception as e:
                data["errors"].append(f"sector_allocation: {e}")
                logger.error("%s sector allocation failed: %s", fund_key, e)

            try:
                fund_manager = await self._extract_fund_manager(page)
                data["fields"]["fund_manager"] = fund_manager
            except Exception as e:
                data["errors"].append(f"fund_manager: {e}")
                logger.error("%s fund manager extraction failed: %s", fund_key, e)

            # Capture raw text as fallback
            try:
                data["raw_text"] = await page.inner_text("body")
            except Exception:
                data["raw_text"] = ""

            if data["errors"]:
                data["scrape_status"] = "partial"

        except Exception as exc:
            data["scrape_status"] = "failed"
            data["errors"].append(str(exc))
            logger.error("FAILED to scrape %s: %s", fund_key, exc)

        finally:
            await page.close()

        return data

    # ── Retry wrapper ────────────────────────────────────────────────

    async def _scrape_with_retry(self, url: str, fund_key: str, fund_config: dict) -> dict:
        """Retry scraping a fund page with exponential backoff."""
        max_retries = SCRAPER_CONFIG["retry_attempts"]
        backoff_base = SCRAPER_CONFIG["retry_backoff_base"]

        for attempt in range(1, max_retries + 1):
            result = await self.scrape_fund_page(url, fund_key, fund_config)
            if result["scrape_status"] != "failed":
                return result

            if attempt < max_retries:
                wait = backoff_base ** attempt
                logger.warning(
                    "Retry %d/%d for %s in %ds",
                    attempt, max_retries, fund_key, wait,
                )
                await asyncio.sleep(wait)

        return result  # return the last failed attempt

    # ── Scrape all pages ─────────────────────────────────────────────

    async def scrape_all(self) -> list[dict]:
        """Scrape all 6 whitelisted INDMoney fund pages sequentially."""
        await self._launch_browser()
        results = []

        for fund_key, config in ALLOWED_SOURCES.items():
            data = await self._scrape_with_retry(config["url"], fund_key, config)
            results.append(data)

        await self._close_browser()
        return results


# ── Persistence helpers ──────────────────────────────────────────────

def save_results(results: list[dict]) -> None:
    """Write each fund's scraped data to a dated JSON file + metadata."""
    today = datetime.date.today().isoformat()
    raw_dir = Path(RAW_DATA_DIR)
    raw_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "scrape_date": today,
        "scraped_at": datetime.datetime.now().isoformat(),
        "funds": {},
    }

    for fund_data in results:
        fund_key = fund_data["fund_key"]
        filename = f"{fund_key}_{today}.json"
        filepath = raw_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(fund_data, f, indent=2, ensure_ascii=False)

        metadata["funds"][fund_key] = {
            "file": filename,
            "status": fund_data["scrape_status"],
            "errors": fund_data["errors"],
            "scraped_at": fund_data["scraped_at"],
            "source_url": fund_data["source_url"],
        }
        logger.info("Saved %s → %s [%s]", fund_key, filepath, fund_data["scrape_status"])

    # Write metadata
    meta_path = Path(METADATA_FILE)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    logger.info("Metadata saved to %s", meta_path)


# ── Entry point ──────────────────────────────────────────────────────

async def run_scraper() -> list[dict]:
    """Run the full scraping pipeline and save outputs."""
    scraper = INDMoneyScraper()
    results = await scraper.scrape_all()
    save_results(results)
    return results
