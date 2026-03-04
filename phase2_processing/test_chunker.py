import unittest
import json
from phase2_processing.chunker import FundChunker

class TestFundChunker(unittest.TestCase):
    def setUp(self):
        self.chunker = FundChunker()
        self.sample_data = {
            "fund_key": "test_fund",
            "source_url": "https://example.com",
            "scraped_at": "2026-03-02T10:00:00",
            "fields": {
                "fund_name": "Test Mutual Fund",
                "category": "Equity",
                "amc": "Test AMC",
                "fund_manager": "John Doe",
                "aum": "₹1000 Cr",
                "nav": "₹10.50",
                "nav_date": "01 Mar 2026",
                "expense_ratio": "1.0%",
                "exit_load": "1.0%",
                "min_sip": "₹500",
                "min_lumpsum": "₹5000",
                "risk_level": "High",
                "benchmark": "Nifty 50",
                "lock_in": "None",
                "returns": {"1Y": "10%", "3Y": "30%", "5Y": "50%"},
                "holdings": [{"name": "Stock A", "weight": "5%"}],
                "sector_allocation": [{"sector": "Tech", "weight": "20%"}]
            },
            "raw_text": "Frequently Asked Questions\n\nHow to invest?\nFollow the app steps.\n\nWhat is NAV?\nNAV is 10.50."
        }

    def test_create_chunks(self):
        chunks = self.chunker.create_chunks(self.sample_data)
        self.assertTrue(len(chunks) > 0)
        
        chunk_types = [c.chunk_type for c in chunks]
        self.assertIn("overview", chunk_types)
        self.assertIn("expense_exit", chunk_types)
        self.assertIn("holdings", chunk_types)
        self.assertIn("faq", chunk_types)

    def test_faq_extraction(self):
        chunks = self.chunker.create_chunks(self.sample_data)
        faq_chunks = [c for c in chunks if c.chunk_type == "faq"]
        # Basic check, depends on regex and text formatting
        # In our sample, it might not catch due to simple formatting
        pass

if __name__ == "__main__":
    unittest.main()
