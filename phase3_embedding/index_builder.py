"""
Phase 3 — Index Builder
========================
Reads all per-fund chunk files produced by Phase 2 (data/chunks/*.json),
loads them into MFVectorStore (ChromaDB + SentenceTransformer embeddings),
and prints a post-build summary.

Usage
-----
    python -m phase3_embedding.index_builder            # build / refresh index
    python -m phase3_embedding.index_builder --rebuild  # drop & rebuild from scratch
    python -m phase3_embedding.index_builder --verify   # build + show sample query
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import io
from pathlib import Path

# Force UTF-8 output on Windows so emoji / Unicode symbols don't crash
if sys.stdout.encoding and sys.stdout.encoding.upper() not in ("UTF-8", "UTF8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


from phase3_embedding.embedder import MFVectorStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("phase3.index_builder")

CHUNKS_DIR    = Path("data/chunks")
VECTORSTORE_DIR = "data/vectorstore"


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_chunks_from_dir(chunks_dir: Path) -> list[dict]:
    """Load all chunk dicts from per-fund JSON files in *chunks_dir*."""
    chunk_files = sorted(chunks_dir.glob("*_chunks.json"))
    if not chunk_files:
        logger.error("No chunk files found in %s. Run Phase 2 first.", chunks_dir)
        sys.exit(1)

    all_chunks: list[dict] = []
    for fp in chunk_files:
        with open(fp, encoding="utf-8") as f:
            fund_chunks = json.load(f)
        logger.info("Loaded %3d chunks from %s", len(fund_chunks), fp.name)
        all_chunks.extend(fund_chunks)

    return all_chunks


def print_summary(store: MFVectorStore, all_chunks: list[dict]) -> None:
    """Print a concise post-index summary."""
    from collections import Counter
    type_counts = Counter(c["chunk_type"] for c in all_chunks)
    fund_counts = Counter(c["fund_key"]   for c in all_chunks)

    sep = "=" * 64
    print(f"\n{sep}")
    print("  Phase 3 — Vector Store Build Summary")
    print(sep)
    print(f"  Total chunks indexed : {store.count()}")
    print(f"  Funds covered        : {len(fund_counts)}")
    print()
    print("  Chunk type distribution:")
    for ct, n in sorted(type_counts.items()):
        print(f"    {ct:<22} {n:>4} chunks")
    print()
    print("  Per-fund chunk counts:")
    for fk, n in sorted(fund_counts.items()):
        print(f"    {fk:<40} {n:>4} chunks")
    print(sep + "\n")


def run_sample_query(store: MFVectorStore) -> None:
    """Fire a test query and show top-3 results."""
    query = "What is the expense ratio of Nippon ELSS Tax Saver Fund?"
    print(f"  Sample query: \"{query}\"\n")
    result = store.query(query, top_k=3)

    for i, (doc, meta, dist) in enumerate(zip(
        result["documents"][0],
        result["metadatas"][0],
        result["distances"][0],
    )):
        sim = 1 - dist
        print(f"  [{i+1}] similarity={sim:.4f}  fund={meta['fund_key']}  type={meta['chunk_type']}")
        print(f"       {doc[:120]}…\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 3 — Index Builder")
    parser.add_argument(
        "--chunks-dir", default=str(CHUNKS_DIR),
        help="Directory with per-fund chunk JSON files (default: data/chunks)",
    )
    parser.add_argument(
        "--persist-dir", default=VECTORSTORE_DIR,
        help="ChromaDB persistence directory (default: data/vectorstore)",
    )
    parser.add_argument(
        "--rebuild", action="store_true",
        help="Drop the existing collection and rebuild from scratch",
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Run a sample query after building to verify retrieval",
    )
    args = parser.parse_args()

    print("=" * 64)
    print("  Phase 3 — Embedding & Vector Store Builder")
    print("=" * 64)

    # --- Load chunks ---
    all_chunks = load_chunks_from_dir(Path(args.chunks_dir))
    logger.info("Total chunks loaded: %d", len(all_chunks))

    # --- Init store ---
    store = MFVectorStore(persist_dir=args.persist_dir)

    if args.rebuild:
        logger.warning("--rebuild flag set: dropping existing collection …")
        store.delete_collection()

    # --- Index ---
    store.add_chunks(all_chunks)

    # --- Summary ---
    print_summary(store, all_chunks)

    # --- Optional verification ---
    if args.verify:
        print("  Running sample retrieval query …\n")
        run_sample_query(store)

    print("  [OK] Phase 3 complete — vector store ready for Phase 4 (RAG pipeline).")


if __name__ == "__main__":
    main()
