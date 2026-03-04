import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timezone, timedelta

import pytest
import master_refresh

@pytest.fixture
def temp_project_root(tmp_path):
    with patch("master_refresh._PROJECT_ROOT", tmp_path), \
         patch("master_refresh._METADATA_PATH", tmp_path / "data" / "scrape_metadata.json"):
        yield tmp_path

def test_purge_previous_day_data(temp_project_root):
    data_dir = temp_project_root / "data"
    raw_dir = data_dir / "raw"
    cleaned_dir = data_dir / "cleaned"
    chunks_dir = data_dir / "chunks"

    for d in [raw_dir, cleaned_dir, chunks_dir]:
        d.mkdir(parents=True, exist_ok=True)

    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    f1 = raw_dir / f"test_{yesterday}.json"
    f2 = cleaned_dir / f"test_{yesterday}.json"
    f3 = chunks_dir / f"test_{yesterday}_chunks.json"

    f4 = raw_dir / f"test_{today}.json"
    f5 = chunks_dir / f"test_{today}_chunks.json"

    for f in [f1, f2, f3, f4, f5]:
        f.touch()

    master_refresh._purge_previous_day_data()

    assert not f1.exists()
    assert not f2.exists()
    assert not f3.exists()

    assert f4.exists()
    assert f5.exists()

def test_save_metadata(temp_project_root):
    master_refresh._save_metadata(success=True)
    
    meta_path = temp_project_root / "data" / "scrape_metadata.json"
    assert meta_path.exists()
    
    with open(meta_path) as f:
        data = json.load(f)
    
    assert data["last_status"]["success"] is True
    assert "last_refresh" in data
    assert "updated_at" in data

@pytest.mark.asyncio
async def test_run_refresh_pipeline(temp_project_root):
    # Mock INDMoneyScraper
    mock_scraper = MagicMock()
    mock_scraper_instance = MagicMock()
    mock_scraper.return_value = mock_scraper_instance
    
    async def mock_scrape_all():
        return [{"fund_key": "fund_a", "scrape_status": "success", "fields": {}}]
    
    mock_scraper_instance.scrape_all = mock_scrape_all

    # Mock save_results
    mock_save = MagicMock()

    # Mock FundChunker
    mock_chunker = MagicMock()
    mock_chunker_instance = MagicMock()
    mock_chunk = MagicMock()
    mock_chunker.return_value = mock_chunker_instance
    mock_chunker_instance.create_chunks.return_value = [mock_chunk]

    # Mock MFVectorStore
    mock_store = MagicMock()
    mock_store_instance = MagicMock()
    mock_store.return_value = mock_store_instance

    with patch('phase1_scraping.indmoney_scraper.INDMoneyScraper', mock_scraper), \
         patch('phase1_scraping.indmoney_scraper.save_results', mock_save), \
         patch('phase2_processing.chunker.FundChunker', mock_chunker), \
         patch('phase3_embedding.index_builder.MFVectorStore', mock_store), \
         patch('dataclasses.asdict', lambda x: {}):
        
        result = await master_refresh.run_refresh_pipeline()
    
    assert result is True
    
    # Check if files were persisted
    cleaned_file = temp_project_root / "data" / "cleaned" / f"fund_a_{date.today().isoformat()}.json"
    assert cleaned_file.exists()
    
    chunk_file = temp_project_root / "data" / "chunks" / f"fund_a_{date.today().isoformat()}_chunks.json"
    assert chunk_file.exists()

    summary_file = temp_project_root / "data" / "chunks" / "all_chunks_summary.json"
    assert summary_file.exists()
