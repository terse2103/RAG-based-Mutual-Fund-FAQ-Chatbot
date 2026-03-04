"""
Phase 6 — Response Generator (Groq)
=====================================
Uses Groq LPU inference for ultra-low latency response generation.
Features:
- Primary: LLaMA 3.3 70B (Versatile) for factual accuracy.
- Fallback: LLaMA 3.1 8B (Instant) for reliability if 70B is busy.
- Post-process: Sentence truncation, source URL appending, timestamp formatting.
"""

from __future__ import annotations

import os
import logging
from typing import Optional, List
from groq import Groq
from dotenv import load_dotenv
from phase6_generation.prompts import SYSTEM_PROMPT
from phase6_generation.response_guard import ResponseGuard

# Load env variables (GROQ_API_KEY)
load_dotenv()

logger = logging.getLogger("phase6.generator")

# --- Constants ---
PRIMARY_MODEL = "llama-3.3-70b-versatile"
FALLBACK_MODEL = "llama-3.1-8b-instant"
DEFAULT_TEMP = 0.1
MAX_TOKENS = 300

class ResponseGenerator:
    """
    Generates grounded responses using Groq.

    Parameters
    ----------
    api_key : str, optional
        Groq API key. If None, it will be loaded from the environment.
    model : str
        The primary model name on Groq.
    fallback_model: str
        The fallback model in case of primary model failure.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = PRIMARY_MODEL,
        fallback_model: str = FALLBACK_MODEL,
    ) -> None:
        key = api_key or os.getenv("GROQ_API_KEY")
        if not key:
            raise ValueError("Groq API Key not found in environment or passed to constructor.")
        
        self.client = Groq(api_key=key)
        self.model = model
        self.fallback_model = fallback_model
        self.guard = ResponseGuard()

    def generate(
        self,
        query: str,
        context: str,
        sources: List[str],
        scraped_at: str,
    ) -> str:
        """
        Produce a grounded answer matching the RAG Chain signature.
        """
        # Formulate prompt using the centralized template
        prompt = SYSTEM_PROMPT.format(
            context=context,
            query=query,
            scraped_date=scraped_at[:10] if scraped_at else "Unknown",
            source_url=sources[0] if sources else "https://www.indmoney.com"
        )

        try:
            logger.info("Generating response using %s...", self.model)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": query},
                ],
                temperature=DEFAULT_TEMP,
                max_tokens=MAX_TOKENS,
                top_p=0.9,
            )
            raw_text = response.choices[0].message.content.strip()
        except Exception as e:
            logger.error("Primary model (%s) failed: %s. Switching to fallback %s.", 
                         self.model, e, self.fallback_model)
            # Fallback to lighter model
            response = self.client.chat.completions.create(
                model=self.fallback_model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": query},
                ],
                temperature=DEFAULT_TEMP,
                max_tokens=MAX_TOKENS,
                top_p=0.9,
            )
            raw_text = response.choices[0].message.content.strip()

        return self.guard.validate(raw_text, sources, scraped_at)

if __name__ == "__main__":
    # Integration smoke test
    logging.basicConfig(level=logging.INFO)
    try:
        gen = ResponseGenerator()
        print("\n--- Test Response ---")
        print(gen.generate(
            query="What is the expense ratio of Nippon India Mutual Fund?",
            context="The Nippon India ELSS Tax Saver Fund has an expense ratio of 1.03%.",
            sources=["https://www.indmoney.com/elss"],
            scraped_at="2026-03-02T12:00:00"
        ))
    except Exception as exc:
        print(f"Generator Error: {exc}")
