"""
URL Registry & Fund Metadata — Whitelist of allowed INDMoney pages.

Only these 6 URLs are permitted for scraping. The chatbot must NEVER
scrape or reference any URL outside this whitelist.
"""

ALLOWED_SOURCES = {
    "nippon_elss_tax_saver": {
        "url": "https://www.indmoney.com/mutual-funds/nippon-india-elss-tax-saver-fund-direct-plan-growth-option-2751",
        "fund_name": "Nippon India ELSS Tax Saver Fund - Direct Plan Growth",
        "category": "ELSS / Tax Saver",
    },
    "nippon_nifty_auto_index": {
        "url": "https://www.indmoney.com/mutual-funds/nippon-india-nifty-auto-index-fund-direct-growth-1048613",
        "fund_name": "Nippon India Nifty Auto Index Fund - Direct Growth",
        "category": "Index Fund / Sectoral",
    },
    "nippon_short_duration": {
        "url": "https://www.indmoney.com/mutual-funds/nippon-india-short-duration-fund-direct-plan-growth-plan-2268",
        "fund_name": "Nippon India Short Duration Fund - Direct Plan Growth",
        "category": "Debt / Short Duration",
    },
    "nippon_crisil_ibx_aaa": {
        "url": "https://www.indmoney.com/mutual-funds/nippon-india-crisil-ibx-aaa-financial-svcs-dec-2026-idx-fd-dir-growth-1048293",
        "fund_name": "Nippon India CRISIL IBX AAA Financial Svcs Dec 2026 Index Fund - Direct Growth",
        "category": "Debt / Target Maturity",
    },
    "nippon_silver_etf_fof": {
        "url": "https://www.indmoney.com/mutual-funds/nippon-india-silver-etf-fund-of-fund-fof-direct-growth-1040380",
        "fund_name": "Nippon India Silver ETF Fund of Fund (FOF) - Direct Growth",
        "category": "Commodity / Silver",
    },
    "nippon_balanced_advantage": {
        "url": "https://www.indmoney.com/mutual-funds/nippon-india-balanced-advantage-fund-direct-growth-plan-4324",
        "fund_name": "Nippon India Balanced Advantage Fund - Direct Growth Plan",
        "category": "Hybrid / Balanced Advantage",
    },
}

# Scraper settings
SCRAPER_CONFIG = {
    "headless": True,
    "browser_args": ["--no-sandbox", "--disable-dev-shm-usage"],
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "viewport": {"width": 1280, "height": 720},
    "navigation_timeout": 30000,   # 30 seconds
    "selector_timeout": 15000,     # 15 seconds
    "retry_attempts": 3,
    "retry_backoff_base": 2,       # Exponential: 2s → 4s → 8s
}

# Output directory for raw scraped data
RAW_DATA_DIR = "data/raw"
METADATA_FILE = "data/scrape_metadata.json"
