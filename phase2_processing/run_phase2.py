import os
import json
import logging
from pathlib import Path
from dataclasses import asdict
from phase2_processing.chunker import FundChunker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s"
)
logger = logging.getLogger("phase2")

CLEANED_DATA_DIR = Path("data/cleaned")
CHUNKS_DATA_DIR = Path("data/chunks")

def process_all_files():
    """
    Reads all cleaned JSON files and generates chunks.
    """
    if not CLEANED_DATA_DIR.exists():
        logger.error(f"Cleaned data directory {CLEANED_DATA_DIR} does not exist.")
        return

    CHUNKS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    chunker = FundChunker()
    all_chunks = []

    json_files = list(CLEANED_DATA_DIR.glob("*.json"))
    if not json_files:
        logger.warning(f"No JSON files found in {CLEANED_DATA_DIR}.")
        return

    logger.info(f"Processing {len(json_files)} cleaned files...")

    for json_file in json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                fund_data = json.load(f)
            
            fund_key = fund_data.get("fund_key", json_file.stem)
            chunks = chunker.create_chunks(fund_data)
            
            # Save individual fund chunks
            fund_chunks_file = CHUNKS_DATA_DIR / f"{json_file.stem}_chunks.json"
            with open(fund_chunks_file, "w", encoding="utf-8") as f:
                json.dump([asdict(c) for c in chunks], f, indent=2, ensure_ascii=False)
            
            logger.info(f"Generated {len(chunks)} chunks for {fund_key} → {fund_chunks_file.name}")
            all_chunks.extend(chunks)
            
        except Exception as e:
            logger.error(f"Failed to process {json_file.name}: {e}")

    # Save summary of all chunks
    summary_file = CHUNKS_DATA_DIR / "all_chunks_summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump({
            "total_chunks": len(all_chunks),
            "funds_processed": len(json_files),
            "timestamp": fund_data.get("cleaned_at", "") # Just use last one for now
        }, f, indent=2)
    
    logger.info(f"✅ Total chunks generated: {len(all_chunks)}")
    logger.info(f"Summary saved to {summary_file.name}")

if __name__ == "__main__":
    process_all_files()
