"""
Phase 1 — Data Cleaner

Re-processes existing raw JSON files (data/raw/) to extract clean,
structured field values from raw_text using reliable FAQ-section patterns.

The raw_text contains a structured FAQ section at the bottom of each INDMoney
page that uses consistent phrasing — far more reliable than DOM selectors.

Usage:
    python -m phase1_scraping.data_cleaner          # clean all raw files
    python -m phase1_scraping.data_cleaner --verify  # print field summary
"""

import argparse
import datetime
import json
import logging
import re
import sys
from pathlib import Path
from typing import Optional

from phase1_scraping.config import ALLOWED_SOURCES, RAW_DATA_DIR

CLEANED_DATA_DIR = "data/cleaned"
logger = logging.getLogger("cleaner")


# ── Raw-text field extractor ─────────────────────────────────────────────────

class RawTextExtractor:
    """
    Extracts clean field values from raw_text using FAQ-section and
    Overview-section patterns.  These text patterns are stable across
    page re-renders and far more reliable than fragile DOM selectors.
    """

    def __init__(self, raw_text: str, fund_name: str = ""):
        self.text = raw_text
        self.fund_name = fund_name

    # ── Internal helpers ─────────────────────────────────────────────

    def _get_overview_section(self) -> str:
        """Extract the bottom '<FundName> Overview' section (label\\nvalue pairs)."""
        markers = [
            f"{self.fund_name} Overview",
            "Overview\n\nGet key fund statistics",
        ]
        for marker in markers:
            idx = self.text.rfind(marker)
            if idx != -1:
                section = self.text[idx:]
                for end in ["About Nippon", "About ", "Fund Manager", "AUM Change"]:
                    end_idx = section.find(end, len(marker))
                    if end_idx != -1:
                        return section[:end_idx]
                return section[:2000]
        return ""

    def _ov_value(self, label: str) -> Optional[str]:
        """Find 'Label\\nValue' in the Overview section."""
        section = self._get_overview_section() or self.text
        m = re.search(
            rf"{re.escape(label)}\s*\n\s*([^\n]+)", section, re.IGNORECASE
        )
        return m.group(1).strip() if m else None

    # ── Public extractors ────────────────────────────────────────────

    def extract_expense_ratio(self) -> Optional[str]:
        # Priority 1: FAQ "The expense ratio is X%"
        m = re.search(r"[Tt]he expense ratio is\s*([\d.]+%)", self.text)
        if m:
            return m.group(1)
        # Priority 2: clean Overview label
        val = self._ov_value("Expense ratio")
        if val and re.match(r"^[\d.]+%$", val):
            return val
        return None

    def extract_returns(self) -> dict:
        """Extract 1Y / 3Y / 5Y returns."""
        returns = {}

        # Primary: FAQ "return of X% in 1 year, Y% in 3 years, Z% in 5 years"
        m = re.search(
            r"return of\s+([\d.]+%?)\s+in 1 years?,?\s+"
            r"([\d.]+%?)\s+in 3 years?,?\s+"
            r"([\d.]+%?)\s+in 5 years?",
            self.text, re.IGNORECASE,
        )
        if m:
            for key, grp in zip(("1Y", "3Y", "5Y"), (1, 2, 3)):
                val = m.group(grp)
                returns[key] = val if "%" in val else val + "%"
            return returns

        # Secondary: "CAGR return of X% since inception. Over the last 1, 3 and 5
        # years the fund has given a CAGR return of Y%, Z% and W% respectively."
        m = re.search(
            r"last 1,\s*3 and 5 years.*?CAGR return of\s*"
            r"([\d.]+%?),\s*([\d.]+%?) and\s*([\d.]+%?)\s+respectively",
            self.text, re.IGNORECASE | re.DOTALL,
        )
        if m:
            for key, grp in zip(("1Y", "3Y", "5Y"), (1, 2, 3)):
                val = m.group(grp)
                returns[key] = val if "%" in val else val + "%"
            return returns

        # Tertiary: performance table "This Fund\t3.46%\t...\t17.84%\t19.65%\t16.89%"
        lines = self.text.split("\n")
        for i, line in enumerate(lines):
            if "1Y" in line and "3Y" in line and "5Y" in line and "Period" in line:
                headers = [h.strip() for h in re.split(r"\t+", line) if h.strip()]
                for j in range(i + 1, min(i + 6, len(lines))):
                    if "this fund" in lines[j].lower():
                        vals = [v.strip() for v in re.split(r"\t+", lines[j]) if v.strip()]
                        vals = [v for v in vals if "fund" not in v.lower()]
                        hdrs = [h for h in headers if h not in ("Period",)]
                        for h, v in zip(hdrs, vals):
                            if h in ("1Y", "3Y", "5Y"):
                                returns[h] = v if "%" in v else v + "%"
                        break
                if returns:
                    break
        return returns

    def extract_fund_managers(self) -> Optional[str]:
        # Primary: "The fund managers are X, Y, Z"
        m = re.search(r"[Tt]he fund managers? are\s+(.+?)(?:\.|$)", self.text)
        if m:
            return m.group(1).strip().rstrip(".")
        # Secondary: "managed by X, Y, Z"
        m = re.search(r"managed by\s+([A-Za-z, ]+?)(?:\.|,\s*[A-Z]|\n)", self.text, re.IGNORECASE)
        if m:
            return m.group(1).strip().rstrip(".")
        # Tertiary: collect names after "Fund Manager\n"
        fm_idx = self.text.find("Fund Manager\n")
        if fm_idx != -1:
            after = self.text[fm_idx + len("Fund Manager\n"):]
            names = []
            for chunk in after.split("\n\n"):
                first_line = chunk.strip().split("\n")[0].strip()
                if (first_line
                        and re.match(r"^[A-Z][a-z]+ [A-Z]", first_line)
                        and "Fund Manager" not in first_line
                        and len(first_line) < 50):
                    names.append(first_line)
                if len(names) >= 5:
                    break
            if names:
                return ", ".join(names)
        return None

    def extract_holdings(self) -> list:
        holdings = []

        # Primary: FAQ "top N holdings … are X(wt%), Y(wt%), Z(wt%)"
        m = re.search(
            r"top \d+ holdings.*?are\s+((?:[A-Za-z][A-Za-z\s]+\([\d.]+%\)[,\s]*)+)",
            self.text, re.IGNORECASE,
        )
        if m:
            for hm in re.finditer(r"([A-Za-z][A-Za-z\s]+)\(([\d.]+%)\)", m.group(1)):
                holdings.append({"name": hm.group(1).strip(), "weight": hm.group(2)})

        # Secondary: "Holdings Details" table
        hd_idx = self.text.rfind("Holdings Details")
        if hd_idx != -1:
            hd_section = self.text[hd_idx:]
            end = hd_section.find("Portfolio Changes")
            if end != -1:
                hd_section = hd_section[:end]
            lines = [ln.strip() for ln in hd_section.split("\n") if ln.strip()]
            existing = {h["name"].lower() for h in holdings}
            skip = {"Equity", "Debt", "Holdings", "Weight%", "Holdings Trend",
                    "1M Change", "Holdings Details", "See all", "See less"}
            i = 0
            while i < len(lines) and len(holdings) < 10:
                line = lines[i]
                if line in skip:
                    i += 1
                    continue
                if (re.match(r"^[A-Z][A-Za-z\s]+$", line)
                        and 5 < len(line) < 60
                        and line.lower() not in existing):
                    for j in range(i + 1, min(i + 4, len(lines))):
                        wm = re.match(r"^([\d.]+)%?$", lines[j])
                        if wm:
                            wt = float(wm.group(1))
                            if 0.1 <= wt <= 20:
                                holdings.append({"name": line, "weight": f"{wm.group(1)}%"})
                                existing.add(line.lower())
                            break
                i += 1

        return holdings[:10]

    def extract_sector_allocation(self) -> list:
        sectors = []
        known_sectors = {
            "Financial Services", "Industrial", "Consumer Cyclical",
            "Consumer Defensive", "Utilities", "Technology", "Tech",
            "Health", "Healthcare", "Energy", "Basic Materials",
            "Communication", "Real Estate", "Information Technology",
        }

        # Find last "Sector Allocation" block
        idx = self.text.rfind("Sector Allocation\n\nEquity")
        if idx == -1:
            idx = self.text.rfind("Sector Allocation\n")
        if idx == -1:
            return sectors

        section = self.text[idx:]
        for end_m in ["Sector Changes", "Holdings Details", "Portfolio Changes"]:
            end_i = section.find(end_m)
            if end_i != -1:
                section = section[:end_i]
                break

        lines = [ln.strip() for ln in section.split("\n") if ln.strip()]
        # First pass: only known sector names
        for i, line in enumerate(lines):
            if line in known_sectors and i + 1 < len(lines):
                wm = re.match(r"^([\d.]+)%?$", lines[i + 1])
                if wm:
                    wt = float(wm.group(1))
                    if 0.5 <= wt <= 100:
                        sectors.append({"sector": line, "weight": f"{wm.group(1)}%"})

        # Second pass (fallback): any "Name\nX%" pair
        if not sectors:
            skip = {"Sector Allocation", "Equity", "Debt & Cash", "Debt", "Cash",
                    "See all", "See less"}
            i = 0
            while i < len(lines):
                line = lines[i]
                if (re.match(r"^[A-Z][A-Za-z\s&]+$", line)
                        and "%" not in line and len(line) > 2
                        and line not in skip
                        and i + 1 < len(lines)):
                    wm = re.match(r"^([\d.]+)%?$", lines[i + 1])
                    if wm:
                        wt = float(wm.group(1))
                        if 0.5 <= wt <= 100:
                            sectors.append({"sector": line, "weight": f"{wm.group(1)}%"})
                i += 1

        return sectors[:15]

    def extract_aum(self) -> Optional[str]:
        m = re.search(r"[Tt]he AUM.*?is\s+(₹[\d,]+\s*Cr)", self.text)
        if m:
            return m.group(1)
        return self._ov_value("AUM")

    def extract_nav(self) -> Optional[str]:
        m = re.search(r"[Tt]he NAV.*?is\s+(₹[\d,]+\.?\d*)", self.text)
        if m:
            return m.group(1)
        m = re.search(r"(₹[\d,]+\.\d+)", self.text)
        return m.group(1) if m else None

    def extract_nav_date(self) -> Optional[str]:
        m = re.search(r"NAV as on\s+(\d{1,2}\s+\w+\s+\d{4})", self.text, re.IGNORECASE)
        return m.group(1) if m else None

    def extract_min_sip_lumpsum(self) -> tuple:
        # FAQ pattern
        m = re.search(
            r"[Mm]inimum investment.*?lump.?sum.*?(?:INR|₹)\s*([\d,]+(?:\.\d+)?)"
            r".*?SIP.*?(?:INR|₹)\s*([\d,]+(?:\.\d+)?)",
            self.text, re.IGNORECASE | re.DOTALL,
        )
        if m:
            lumpsum = f"₹{int(float(m.group(1).replace(',', '')))}"
            sip = f"₹{int(float(m.group(2).replace(',', '')))}"
            return sip, lumpsum
        # Overview label "Min Lumpsum/SIP\n₹500/₹500"
        val = self._ov_value("Min Lumpsum/SIP")
        if val and "/" in val:
            parts = val.split("/")
            return parts[1].strip(), parts[0].strip()
        return None, None

    def extract_exit_load(self) -> Optional[str]:
        val = self._ov_value("Exit Load")
        if val:
            return val
        m = re.search(r"[Ee]xit [Ll]oad.*?(\d+%)", self.text)
        return m.group(1) if m else None

    def extract_lock_in(self) -> Optional[str]:
        m = re.search(r"lock.?in period.*?of\s+(\d+\s+Years?)", self.text, re.IGNORECASE)
        if m:
            return m.group(1)
        return self._ov_value("Lock In")

    def extract_risk_level(self) -> Optional[str]:
        val = self._ov_value("Risk")
        if val and any(kw in val for kw in ("High", "Low", "Moderate")):
            return val
        m = re.search(r"(Very High|High|Moderate|Low|Very Low)\s+Risk", self.text, re.IGNORECASE)
        return m.group(0) if m else val

    def extract_benchmark(self) -> Optional[str]:
        return self._ov_value("Benchmark")

    def extract_turnover(self) -> Optional[str]:
        return self._ov_value("TurnOver")

    def extract_inception_date(self) -> Optional[str]:
        return self._ov_value("Inception Date")

    def extract_all(self, existing_fields: dict, fund_config: dict) -> dict:
        """Return a fully cleaned fields dict."""
        min_sip, min_lumpsum = self.extract_min_sip_lumpsum()
        return {
            "fund_name": (existing_fields.get("fund_name")
                          or fund_config.get("fund_name")),
            "nav":             self.extract_nav()          or existing_fields.get("nav"),
            "nav_date":        self.extract_nav_date()     or existing_fields.get("nav_date"),
            "category":        existing_fields.get("category") or fund_config.get("category"),
            "amc":             existing_fields.get("amc"),
            "expense_ratio":   self.extract_expense_ratio() or existing_fields.get("expense_ratio"),
            "exit_load":       self.extract_exit_load()    or existing_fields.get("exit_load"),
            "min_sip":         min_sip                     or existing_fields.get("min_sip"),
            "min_lumpsum":     min_lumpsum                 or existing_fields.get("min_lumpsum"),
            "lock_in":         self.extract_lock_in()      or existing_fields.get("lock_in"),
            "risk_level":      self.extract_risk_level()   or existing_fields.get("risk_level"),
            "benchmark":       self.extract_benchmark()    or existing_fields.get("benchmark"),
            "aum":             self.extract_aum()          or existing_fields.get("aum"),
            "turnover":        self.extract_turnover(),
            "inception_date":  self.extract_inception_date(),
            "fund_manager":    self.extract_fund_managers() or existing_fields.get("fund_manager"),
            "returns":         self.extract_returns()      or existing_fields.get("returns", {}),
            "holdings":        self.extract_holdings()     or existing_fields.get("holdings", []),
            "sector_allocation": (self.extract_sector_allocation()
                                  or existing_fields.get("sector_allocation", [])),
        }


# ── File-level helpers ───────────────────────────────────────────────────────

def clean_file(json_path: Path, output_dir: Path) -> dict:
    """Clean a single raw JSON file and write to output_dir."""
    with open(json_path, encoding="utf-8") as f:
        raw_data = json.load(f)

    fund_key       = raw_data["fund_key"]
    fund_config    = ALLOWED_SOURCES.get(fund_key, {})
    raw_text       = raw_data.get("raw_text", "")
    existing_fields = raw_data.get("fields", {})
    fund_name      = existing_fields.get("fund_name", fund_config.get("fund_name", ""))

    extractor     = RawTextExtractor(raw_text, fund_name=fund_name)
    cleaned_fields = extractor.extract_all(existing_fields, fund_config)

    cleaned_data = {
        **raw_data,
        "fields": cleaned_fields,
        "cleaned_at": datetime.datetime.now().isoformat(),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / json_path.name
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=2, ensure_ascii=False)

    logger.info("Cleaned %-35s → %s", fund_key, out_file)
    return cleaned_data


def clean_all(raw_dir: str = RAW_DATA_DIR,
              output_dir: str = CLEANED_DATA_DIR) -> list[dict]:
    """Process every raw JSON file and return cleaned records."""
    raw_path = Path(raw_dir)
    out_path = Path(output_dir)

    json_files = sorted(raw_path.glob("*.json"))
    if not json_files:
        logger.warning("No JSON files found in %s", raw_path)
        return []

    results = []
    for jf in json_files:
        try:
            cleaned = clean_file(jf, out_path)
            results.append(cleaned)
        except Exception as exc:
            logger.error("Failed to clean %s: %s", jf.name, exc)

    return results


def print_summary(results: list[dict]) -> None:
    """Print a human-readable field summary for each cleaned fund."""
    print("\n" + "=" * 68)
    print("  Phase 1 — Cleaned Field Summary")
    print("=" * 68)
    for r in results:
        f = r["fields"]
        fund_key = r["fund_key"]
        print(f"\n  ▸ {fund_key}")
        print(f"    fund_name    : {f.get('fund_name')}")
        print(f"    nav          : {f.get('nav')}  ({f.get('nav_date')})")
        print(f"    expense_ratio: {f.get('expense_ratio')}")
        print(f"    exit_load    : {f.get('exit_load')}")
        print(f"    min_sip      : {f.get('min_sip')}")
        print(f"    min_lumpsum  : {f.get('min_lumpsum')}")
        print(f"    lock_in      : {f.get('lock_in')}")
        print(f"    risk_level   : {f.get('risk_level')}")
        print(f"    benchmark    : {f.get('benchmark')}")
        print(f"    aum          : {f.get('aum')}")
        print(f"    fund_manager : {f.get('fund_manager')}")
        returns = f.get("returns", {})
        print(f"    returns      : 1Y={returns.get('1Y')}  3Y={returns.get('3Y')}  5Y={returns.get('5Y')}")
        holdings = f.get("holdings", [])
        print(f"    holdings     : {len(holdings)} stocks  → {[h['name'] for h in holdings[:3]]}")
        sectors = f.get("sector_allocation", [])
        print(f"    sectors      : {len(sectors)} sectors → {[s['sector'] for s in sectors[:3]]}")
    print("\n" + "=" * 68)


# ── CLI entry point ──────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-8s | %(levelname)-7s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    parser = argparse.ArgumentParser(description="Phase 1 — Data Cleaner")
    parser.add_argument(
        "--raw-dir", default=RAW_DATA_DIR,
        help="Directory containing raw scraped JSON files",
    )
    parser.add_argument(
        "--out-dir", default=CLEANED_DATA_DIR,
        help="Output directory for cleaned JSON files",
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Print field-by-field summary after cleaning",
    )
    args = parser.parse_args()

    print("=" * 68)
    print("  Phase 1 — Data Cleaner")
    print("=" * 68)

    results = clean_all(args.raw_dir, args.out_dir)

    print(f"\n  ✅ Cleaned {len(results)} fund files → {args.out_dir}/")

    if args.verify or True:   # always print summary
        print_summary(results)


if __name__ == "__main__":
    main()
