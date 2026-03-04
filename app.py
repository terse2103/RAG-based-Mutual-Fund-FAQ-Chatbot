"""
Main entry point — RAG-based Mutual Fund FAQ Chatbot
=====================================================
The frontend has been moved to a FastAPI + vanilla HTML/JS architecture.
Streamlit has been removed.

To run the chatbot:
    python -m phase7_frontend.run_app
      OR
    uvicorn phase7_frontend.api_server:app --host 0.0.0.0 --port 8000

Then open http://localhost:8000
"""

import sys
import os

if __name__ == "__main__":
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    os.chdir(PROJECT_ROOT)

    from phase7_frontend.run_app import main
    main()
