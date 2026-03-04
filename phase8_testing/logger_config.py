"""
Phase 8 — Centralised Logging Configuration
=============================================
Call ``setup_logging()`` once at app startup to configure handlers
for every logger in the project under a consistent format.

Format
------
    2026-03-02 19:00:00,123 | phase4.rag_chain | INFO | RAG chain loaded

Key events captured
-------------------
  * Scrape success / failure per fund          (phase1.*)
  * PII detections (query preview, type)       (phase5.pii_filter)
  * Safety gate decisions                      (phase5.safety_gate)
  * Retrieval chunk count + similarities       (phase4.retriever)
  * LLM call latency + model used             (phase6.generator)
  * Response post-processing                  (phase6.response_guard)
  * UI load time + session events             (phase7.app)

Usage
-----
    from phase8_testing.logger_config import setup_logging
    setup_logging()          # writes to logs/chatbot.log + console
    setup_logging(level="DEBUG")   # more verbosity
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Literal

# Project root (two levels up from this file)
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LOG_DIR      = os.path.join(_PROJECT_ROOT, "logs")
_LOG_FILE     = os.path.join(_LOG_DIR, "chatbot.log")

_LOG_FORMAT = "%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED = False   # guard against double-calling


def setup_logging(
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
    log_file: str | None = None,
    max_bytes: int = 10 * 1024 * 1024,   # 10 MB
    backup_count: int = 5,
    enable_console: bool = True,
) -> None:
    """
    Configure the root logger with a rotating file handler and optionally
    a console (StreamHandler) handler.

    Parameters
    ----------
    level         : Log level string — DEBUG | INFO | WARNING | ERROR.
    log_file      : Override the default log file path.
    max_bytes     : Max size of each log file before rotation (default 10 MB).
    backup_count  : Number of rotated log files to keep (default 5).
    enable_console: If True, also print logs to stdout.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return   # idempotent — safe to call multiple times
    _CONFIGURED = True

    os.makedirs(_LOG_DIR, exist_ok=True)
    target_file = log_file or _LOG_FILE

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_DATE_FORMAT)

    handlers: list[logging.Handler] = []

    # Rotating file handler
    file_handler = RotatingFileHandler(
        filename=target_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(numeric_level)
    handlers.append(file_handler)

    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(numeric_level)
        handlers.append(console_handler)

    # Apply to root logger
    root = logging.getLogger()
    root.setLevel(numeric_level)
    # Remove any existing handlers to avoid duplicate lines
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in handlers:
        root.addHandler(h)

    # Silence noisy third-party libraries at WARNING level
    for noisy in ["httpx", "httpcore", "urllib3", "chromadb", "sentence_transformers"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger("phase8.logger_config").info(
        "Logging initialised | level=%s | file=%s", level, target_file
    )


def get_logger(name: str) -> logging.Logger:
    """
    Shorthand for callers that want a pre-named logger.
    Call ``setup_logging()`` before using this.
    """
    return logging.getLogger(name)


# ── CLI self-test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    setup_logging(level="DEBUG")
    log = get_logger("phase8.logger_config.test")
    log.debug("DEBUG message visible when level=DEBUG")
    log.info("INFO message — default level")
    log.warning("WARNING message")
    log.error("ERROR message")
    print(f"\nLog file: {_LOG_FILE}")
