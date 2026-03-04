# Phase 9 — Automated Daily Scheduler

> **Objective:** Keep the MF FAQ Chatbot's knowledge base fresh by automatically re-scraping all 6 INDMoney fund pages every day at **10:00 AM IST**, re-chunking, re-embedding, and upserting into ChromaDB.

---

## 📁 Folder Structure

```
phase9_scheduler/
├── __init__.py         # Package init — exposes DailyRefreshScheduler
├── scheduler.py        # Core DailyRefreshScheduler class
├── run_phase9.py       # CLI entry point
├── test_scheduler.py   # Unit + integration test suite
└── README.md           # This file
```

---

## 🏗️ Architecture

```
APScheduler (CronTrigger: 10:00 AM IST)
    │
    ▼
DailyRefreshScheduler._run_refresh_pipeline()
    │
    ├─── Step 1 ─ Phase 1 (Playwright scraper) ─── 6 INDMoney fund pages
    │
    ├─── Step 2 ─ Phase 2 (Chunker) ─────────────── ~48 structured chunks
    │
    ├─── Step 3 ─ Phase 3 (ChromaDB upsert) ──────── vector store refresh
    │
    └─── Persist scrape_metadata.json ────────────── run history + status
```

---

## ⚙️ How to Run

### Start the daily scheduler (stays running, fires at 10 AM each day)

```bash
python -m phase9_scheduler.run_phase9
```

### Force an immediate refresh, then keep running for daily jobs

```bash
python -m phase9_scheduler.run_phase9 --refresh-now
```

### One-off refresh only (no ongoing schedule — useful for CI/CD)

```bash
python -m phase9_scheduler.run_phase9 --once
```

### Custom staleness threshold (e.g., treat data older than 6 h as stale)

```bash
python -m phase9_scheduler.run_phase9 --stale-hours 6
```

---

## 🔌 Embedding in Streamlit (`app.py`)

```python
import streamlit as st
from phase9_scheduler.scheduler import DailyRefreshScheduler

@st.cache_resource
def init_scheduler():
    scheduler = DailyRefreshScheduler()  # picks up shared vectorstore from Phase 3
    scheduler.start()
    scheduler.maybe_refresh_on_startup(max_age_hours=24)
    return scheduler

scheduler = init_scheduler()

# Sidebar admin panel
with st.sidebar:
    st.divider()
    st.markdown("### 🔄 Data Refresh")
    report = scheduler.get_status_report()
    st.markdown(f"**Last refresh:** `{report['last_refresh']}`")
    st.markdown(f"**Next scheduled run:** `{report['next_run']}`")
    st.markdown(f"**Data stale?** {'⚠️ Yes' if report['is_stale'] else '✅ No'}")
    if st.button("🔃 Refresh Now", help="Manually re-scrape all fund pages"):
        with st.spinner("Re-scraping all fund pages…"):
            scheduler.trigger_manual_refresh()
        st.success("✅ Data refreshed!")
        st.rerun()
```

---

## 🧪 Running Tests

```bash
# From the project root
pytest phase9_scheduler/test_scheduler.py -v
```

Expected output:

```
PASSED  TestIsDataStale::test_stale_when_never_refreshed
PASSED  TestIsDataStale::test_stale_when_older_than_threshold
PASSED  TestIsDataStale::test_fresh_when_within_threshold
PASSED  TestIsDataStale::test_stale_with_invalid_timestamp
PASSED  TestIsDataStale::test_custom_threshold
PASSED  TestGetStatusReport::test_required_keys_present
PASSED  TestGetStatusReport::test_never_refreshed_string
PASSED  TestGetStatusReport::test_not_running_initially
PASSED  TestGetStatusReport::test_next_run_na_when_not_running
PASSED  TestGetStatusReport::test_is_stale_reflects_last_refresh
PASSED  TestSaveLoadMetadata::test_save_creates_file
PASSED  TestSaveLoadMetadata::test_load_restores_state
PASSED  TestSaveLoadMetadata::test_graceful_on_corrupt_json
PASSED  TestSaveLoadMetadata::test_save_then_load_roundtrip
PASSED  TestTriggerManualRefresh::test_delegates_to_pipeline
PASSED  TestTriggerManualRefresh::test_last_refresh_updated_after_trigger
PASSED  TestSchedulerStartStop::test_scheduler_starts_and_registers_job
PASSED  TestSchedulerStartStop::test_scheduler_stops_cleanly
PASSED  TestSchedulerStartStop::test_double_stop_is_safe
PASSED  TestSchedulerStartStop::test_job_trigger_is_cron_at_10am
PASSED  TestMaybeRefreshOnStartup::test_triggers_when_stale
PASSED  TestMaybeRefreshOnStartup::test_skips_when_fresh
PASSED  TestMaybeRefreshOnStartup::test_triggers_when_never_refreshed
PASSED  TestRunRefreshPipeline::test_pipeline_success_path
PASSED  TestRunRefreshPipeline::test_chunking_failure_recorded
PASSED  TestRunRefreshPipeline::test_scraper_exception_sets_error_status
```

---

## ⚙️ Configuration

| Parameter | Value | Notes |
|-----------|-------|-------|
| **Schedule** | Daily at **10:00 AM IST** | `hour=10, minute=0, timezone="Asia/Kolkata"` |
| **Timezone** | `Asia/Kolkata` (IST) | Indian retail investor target |
| **Misfire grace** | 3 600 s (1 hour) | Job still fires if app was briefly down at 10 AM |
| **Startup check** | If data > 24 h old → immediate refresh | Configurable via `--stale-hours` |
| **Metadata file** | `data/scrape_metadata.json` | Persists `last_refresh` across restarts |
| **Log file** | `logs/scheduler.log` | Appended to on each run |
| **Manual trigger** | `scheduler.trigger_manual_refresh()` | Block-safe from any thread |

---

## 🔒 Failure Handling

| Scenario | Behaviour |
|----------|-----------|
| Page scraping timeout | Playwright retry (handled in Phase 1); failed fund logged |
| Chunking error for one fund | Recorded in `last_status["failed"]`; other funds continue |
| ChromaDB write error | Exception caught; `last_status["error"]` set; stale data served |
| Network fully down | Full pipeline exception → error logged; metadata updated |
| Process crash at 10 AM | APScheduler mis-fire grace window (1 h) re-triggers on restart |

---

## 📑 Metadata File (`data/scrape_metadata.json`)

```json
{
  "last_refresh": "2026-03-03T10:02:14.312481",
  "last_status": {
    "success": [
      "nippon_elss_tax_saver",
      "nippon_nifty_auto_index",
      "nippon_short_duration",
      "nippon_crisil_ibx_aaa",
      "nippon_silver_etf_fof",
      "nippon_balanced_advantage"
    ],
    "failed": []
  },
  "updated_at": "2026-03-03T10:02:14.312625"
}
```

---

## 🔗 Dependencies

- `apscheduler>=3.10.0` — BackgroundScheduler + CronTrigger
- `phase1_scraping.indmoney_scraper` — Playwright async scraper
- `phase2_processing.chunker` — FundChunker
- `phase3_embedding.index_builder` — MFVectorStore (ChromaDB)
