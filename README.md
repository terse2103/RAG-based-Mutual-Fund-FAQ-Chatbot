# RAG-based Mutual Fund FAQ Chatbot

## Scope

This project focuses on the Nippon India AMC (Asset Management Company) and covers six specific mutual fund schemes across different categories (Equity, Debt, Hybrid, Index, Commodity). All data is scraped exclusively from their official INDMoney web pages.

### Covered Schemes
- Nippon India ELSS Tax Saver Fund - Direct Plan Growth (Equity / ELSS)
- Nippon India Nifty Auto Index Fund - Direct Growth (Index / Sectoral)
- Nippon India Short Duration Fund - Direct Plan Growth (Debt / Short Duration)
- Nippon India CRISIL IBX AAA Financial Svcs Dec 2026 Index Fund - Direct Growth (Debt / Target Maturity)
- Nippon India Silver ETF Fund of Fund (FOF) - Direct Growth (Commodity)
- Nippon India Balanced Advantage Fund - Direct Growth Plan (Hybrid)

## Setup Steps

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd "RAG-based Mutual Fund FAQ Chatbot"
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Linux/Mac:
   source venv/bin/activate
   ```

3. **Install dependencies:**
   `requirements.txt` is currently optimized for Vercel deployment and heavily stripped down. To run the full scraping and embedding pipeline **locally**, install these extra packages:
   ```bash
   pip install -r requirements.txt
   pip install playwright apscheduler sentence-transformers torch
   playwright install chromium
   ```

4. **Add environment variables:**
   Create a `.env` file in the root directory and add your Groq API key:
   ```env
   GROQ_API_KEY=your_groq_api_key_here
   ```

5. **Run the application:**
   Launch the app using the master refresh script (for initial data ingestion) or start the app directly:
   ```bash
   python app.py
   ```
   This will start the FastAPI backend and serve the Vanilla HTML/JS Chatbot interface at `http://localhost:8000`.

## Vercel Deployment

This project is actively configured for Vercel deployment using the Serverless Node runtime (with Python wrapping).
Due to Vercel's strict stateless environment (500MB function size limit, read-only file system, and 10s execution limits), some features like backend **Data Refreshing** (via Playwright) are disabled on the live deployment. To update the knowledge base for a Vercel deployment, you must run `python app.py` (and trigger a refresh) natively on your local machine, and then commit the updated `/data` folder to GitHub to trigger a fresh Vercel build.

## Known Limitations

- **Data Delay:** Scraping and processing are done in batches (daily scheduler), so real-time price changes (like minute-by-minute NAV) might not be instantly reflected.
- **Limited Scope:** The chatbot only answers based on the 6 predefined Nippon India mutual fund links. It will reject queries outside this scope.
- **No Financial Advice:** The chatbot enforces safety constraints, strictly preventing it from offering investment advice, recommendations, or hypothetical return calculations.
- **Dynamic Content:** Heavy dynamic content on INDMoney requires JavaScript rendering (handled via Playwright); if page structures alter significantly, the scraper selectors may require updates.
