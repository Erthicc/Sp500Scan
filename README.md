# S&P 500 Daily Analyzer

**Automated daily S&P 500 scanner** — fetches ~14 months daily OHLCV for every S&P 500 company, computes technical indicators (MACD, RSI, Bollinger Bands, ADX, ATR, OBV, EMA/SMA alignment, Momentum, wave heuristic), ranks stocks using configurable weights, and publishes a web dashboard to GitHub Pages.

**Not financial advice (NFA).** This project is for research and educational purposes only. Always do your own due diligence before trading.

This repository intentionally does not include a license file. Without a license, the default is “All rights reserved” — others can view the code but do not have the legal right to reuse, modify or distribute it.

## Features
- Pulls S&P 500 constituents automatically from Wikipedia.
- Primary data source: Yahoo Finance (`yfinance`) with Stooq fallback.
- Indicators: MACD (histogram & crossover), RSI (14), RSI slope, Bollinger Bands, ADX, ATR, OBV trend, volume spike, SMA/EMA alignment, 14-day momentum, wave strength vs SMA20.
- RSI is treated to **favor undervalued (RSI < 40)** per configuration.
- Composite ranking with configurable weights.
- Dashboard with search, top-N selection, charts (Chart.js), and downloadable `top_picks.json`.
- Daily scheduled run (after market close) and automatic deploy to GitHub Pages.

## Running locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python download_sp500_list.py
export JOB_INDEX=0 JOB_TOTAL=1
python worker.py
python finalize.py
python -m http.server 8000 --directory public
# open http://localhost:8000
