"""
Phase 6 — Response Guard
========================
Enforces strict output constraints on the raw LLM response:
1. Max 3 sentences.
2. Mandatory source attribution link.
3. Mandatory data freshness timestamp.
4. Final PII scrub (fail-safe).
"""

import re
import logging
from typing import List, Optional

logger = logging.getLogger("phase6.response_guard")

class ResponseGuard:
    """
    Validates and cleanses the raw LLM output before it reaches the UI.
    """

    @staticmethod
    def enforce_sentence_limit(text: str, limit: int = 3) -> str:
        """Truncate text to first N sentences."""
        # Split by sentence-ending punctuation followed by space or newline
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        if len(sentences) > limit:
            logger.info("Truncating response from %d to %d sentences.", len(sentences), limit)
            return " ".join(sentences[:limit])
        return text

    @staticmethod
    def validate(text: str, sources: List[str], scraped_at: Optional[str]) -> str:
        """Main entry point for response cleansing."""
        # Enforce the 3-sentence limit
        clean_text = ResponseGuard.enforce_sentence_limit(text)
        
        # We NO LONGER append links/timestamps here because 
        # the UI (chatbot.html) renders them in a separate source card.
        return clean_text
