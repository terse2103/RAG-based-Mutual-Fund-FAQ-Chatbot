from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import re

@dataclass
class DocumentChunk:
    chunk_id: str
    fund_name: str
    fund_key: str
    source_url: str
    chunk_type: str
    content: str
    scraped_at: str
    metadata: Dict[str, Any]

class FundChunker:
    """
    Transforms cleaned fund data into retrieval-optimized chunks.
    """
    
    CHUNK_TEMPLATES = {
        "overview": (
            "{fund_name} is a {category} mutual fund managed by {amc}. "
            "The fund is managed by {fund_manager} and has an AUM of {aum}. "
            "Current NAV is {nav} as of {nav_date}."
        ),
        "expense_exit": (
            "The expense ratio of {fund_name} is {expense_ratio}. "
            "The exit load for this fund is {exit_load}."
        ),
        "sip_investment": (
            "The minimum SIP (Systematic Investment Plan) amount for {fund_name} is {min_sip}. "
            "The minimum lumpsum investment amount is {min_lumpsum}."
        ),
        "risk_benchmark": (
            "{fund_name} is categorized as '{risk_level}' on the riskometer. "
            "The fund's performance is benchmarked against {benchmark}."
        ),
        "lockin_tax": (
            "{fund_name} has a lock-in period of {lock_in}. "
            "This applies to investments made in the fund."
        ),
        "returns": (
            "{fund_name} has delivered the following historical returns: "
            "1-year return of {return_1y}, 3-year return of {return_3y}, and 5-year return of {return_5y}."
        ),
    }

    def _extract_faqs(self, raw_text: str) -> List[Dict[str, str]]:
        """Extract Q&A pairs from the FAQ section of the raw text."""
        faqs = []
        faq_section_start = raw_text.find("Frequently Asked Questions")
        if faq_section_start == -1:
            return faqs
            
        section = raw_text[faq_section_start:]
        # Patterns for questions: lines starting with "What", "How", "Who", "Is", "Can", etc., ending with "?"
        # and not being too long.
        q_pattern = r"\n(How|What|Who|Is|Can|Why|Where|When|Does)[^\n\?]+\?\n"
        
        matches = list(re.finditer(q_pattern, section))
        for i, match in enumerate(matches):
            question = match.group(0).strip()
            start_pos = match.end()
            end_pos = matches[i+1].start() if i+1 < len(matches) else len(section)
            
            answer = section[start_pos:end_pos].strip()
            # Clean up answer - take only the first few sentences if it's too long or has noise
            answer = answer.split("\n\n")[0].strip() # Take first paragraph
            
            if len(question) > 10 and len(answer) > 20:
                faqs.append({"question": question, "answer": answer})
        
        return faqs

    def create_chunks(self, fund_data: Dict[str, Any]) -> List[DocumentChunk]:
        """
        Creates a list of DocumentChunk objects from fund data.
        """
        chunks = []
        fields = fund_data.get("fields", {})
        fund_key = fund_data.get("fund_key", "unknown")
        source_url = fund_data.get("source_url", "")
        scraped_at = fund_data.get("scraped_at", "")
        fund_name = fields.get("fund_name", fund_key)
        
        # 1. Template-based chunks
        for chunk_type, template in self.CHUNK_TEMPLATES.items():
            try:
                # Prepare data for formatting
                format_data = {
                    "fund_name": fund_name,
                    "category": fields.get("category", "N/A"),
                    "amc": fields.get("amc", "N/A"),
                    "fund_manager": fields.get("fund_manager", "N/A"),
                    "aum": fields.get("aum", "N/A"),
                    "nav": fields.get("nav", "N/A"),
                    "nav_date": fields.get("nav_date", "N/A"),
                    "expense_ratio": fields.get("expense_ratio", "N/A"),
                    "exit_load": fields.get("exit_load", "N/A"),
                    "min_sip": fields.get("min_sip", "N/A"),
                    "min_lumpsum": fields.get("min_lumpsum", "N/A"),
                    "risk_level": fields.get("risk_level", "N/A"),
                    "benchmark": fields.get("benchmark", "N/A"),
                    "lock_in": fields.get("lock_in", "None"),
                    "return_1y": fields.get("returns", {}).get("1Y", "N/A"),
                    "return_3y": fields.get("returns", {}).get("3Y", "N/A"),
                    "return_5y": fields.get("returns", {}).get("5Y", "N/A"),
                }
                
                content = template.format(**format_data)
                
                # Only add if we have some meaningful data (not just N/As)
                if "N/A" in content and content.count("N/A") > 3:
                     continue

                chunks.append(DocumentChunk(
                    chunk_id=f"{fund_key}_{chunk_type}",
                    fund_name=fund_name,
                    fund_key=fund_key,
                    source_url=source_url,
                    chunk_type=chunk_type,
                    content=content,
                    scraped_at=scraped_at,
                    metadata={
                        "category": fields.get("category"),
                        "type": chunk_type
                    }
                ))
            except Exception as e:
                print(f"Error creating chunk {chunk_type} for {fund_key}: {e}")

        # 2. Holdings chunk
        holdings = fields.get("holdings", [])
        if holdings:
            holdings_text = f"The top holdings of {fund_name} include: "
            holdings_list = [f"{h['name']} ({h['weight']})" for h in holdings]
            holdings_text += ", ".join(holdings_list) + "."
            
            chunks.append(DocumentChunk(
                chunk_id=f"{fund_key}_holdings",
                fund_name=fund_name,
                fund_key=fund_key,
                source_url=source_url,
                chunk_type="holdings",
                content=holdings_text,
                scraped_at=scraped_at,
                metadata={"type": "holdings"}
            ))

        # 3. Sector Allocation chunk
        sectors = fields.get("sector_allocation", [])
        if sectors:
            sectors_text = f"The sector allocation for {fund_name} is as follows: "
            sectors_list = [f"{s['sector']} ({s['weight']})" for s in sectors]
            sectors_text += ", ".join(sectors_list) + "."
            
            chunks.append(DocumentChunk(
                chunk_id=f"{fund_key}_sectors",
                fund_name=fund_name,
                fund_key=fund_key,
                source_url=source_url,
                chunk_type="sectors",
                content=sectors_text,
                scraped_at=scraped_at,
                metadata={"type": "sectors"}
            ))

        # 4. FAQ chunks
        raw_text = fund_data.get("raw_text", "")
        if raw_text:
            faqs = self._extract_faqs(raw_text)
            for i, faq in enumerate(faqs):
                chunks.append(DocumentChunk(
                    chunk_id=f"{fund_key}_faq_{i}",
                    fund_name=fund_name,
                    fund_key=fund_key,
                    source_url=source_url,
                    chunk_type="faq",
                    content=f"Question: {faq['question']}\nAnswer: {faq['answer']}",
                    scraped_at=scraped_at,
                    metadata={"type": "faq", "index": i}
                ))

        return chunks
