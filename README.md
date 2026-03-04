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
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Add environment variables:**
   Create a `.env` file in the root directory and add your Groq API key:
   ```env
   GROQ_API_KEY=your_groq_api_key_here
   ```

5. **Run the application:**
   Launch the app using the master refresh script or start the app directly:
   ```bash
   python app.py
   ```
   This will start the FastAPI backend and open the Streamlit UI/Chatbot interface.

## Known Limitations

- **Data Delay:** Scraping and processing are done in batches (daily scheduler), so real-time price changes (like minute-by-minute NAV) might not be instantly reflected.
- **Limited Scope:** The chatbot only answers based on the 6 predefined Nippon India mutual fund links. It will reject queries outside this scope.
- **No Financial Advice:** The chatbot enforces safety constraints, strictly preventing it from offering investment advice, recommendations, or hypothetical return calculations.
- **Dynamic Content:** Heavy dynamic content on INDMoney requires JavaScript rendering (handled via Playwright); if page structures alter significantly, the scraper selectors may require updates.
