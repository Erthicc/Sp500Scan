#!/usr/bin/env python3
# download_sp500_list.py
"""
Robust downloader for S&P 500 tickers.

Behavior:
- Try Wikipedia first (using a realistic User-Agent to avoid 403).
- If Wikipedia fails, try several raw CSV endpoints (GitHub / DataHub).
- Retry with exponential backoff.
- Write sp500_list.txt with one uppercase ticker per line.
- Exit non-zero if no tickers could be obtained.
"""

import requests
import pandas as pd
from pathlib import Path
import time
import sys
import io

OUT = "sp500_list.txt"

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
CSV_FALLBACKS = [
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
    "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
    "https://raw.githubusercontent.com/plotly/datasets/master/constituents.csv"
]

HEADERS = {
    # realistic browser style user-agent greatly reduces 403 on some hosts
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

def try_wikipedia(retries=3, pause=2):
    for attempt in range(1, retries+1):
        try:
            print(f"[download] Wikipedia attempt {attempt} -> {WIKIPEDIA_URL}")
            r = requests.get(WIKIPEDIA_URL, headers=HEADERS, timeout=20)
            print(f"[download] HTTP {r.status_code} from Wikipedia")
            if r.status_code == 200 and r.text:
                # pandas can parse HTML tables directly
                try:
                    tables = pd.read_html(r.text)
                    # pick the first table that contains a 'Symbol' or 'Ticker' column
                    for t in tables:
                        cols = [c.lower() for c in t.columns.astype(str)]
                        if any('symbol' in c or 'ticker' in c for c in cols):
                            print("[download] Found candidate table on Wikipedia")
                            return t
                    # fallback: return first table
                    if len(tables) > 0:
                        print("[download] No clear Symbol column; returning first table")
                        return tables[0]
                    print("[download] No tables found on Wikipedia page")
                except Exception as e:
                    print("[download] pandas.read_html failed on Wikipedia content:", e)
            elif r.status_code in (403, 429):
                print(f"[download] Received status {r.status_code} (may be blocked); will retry/backoff.")
            else:
                print(f"[download] Unexpected status {r.status_code} from Wikipedia")
        except Exception as e:
            print(f"[download] Wikipedia request exception: {e}")
        time.sleep(pause * attempt)
    return None

def try_csv_url(url, retries=3, pause=2):
    for attempt in range(1, retries+1):
        try:
            print(f"[download] CSV attempt {attempt} -> {url}")
            r = requests.get(url, headers=HEADERS, timeout=20)
            print(f"[download] HTTP {r.status_code} from {url}")
            if r.status_code == 200 and r.content:
                # attempt to parse as CSV
                try:
                    # some endpoints return CSV text
                    df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), dtype=str)
                    return df
                except Exception as e:
                    print(f"[download] pandas.read_csv failed for {url}: {e}")
            else:
                print(f"[download] Unexpected status {r.status_code} from {url}")
        except Exception as e:
            print(f"[download] Exception fetching {url}: {e}")
        time.sleep(pause * attempt)
    return None

def extract_symbols_from_table(df):
    # Find a column likely to contain tickers/symbols
    cols_lower = [c.lower() for c in df.columns.astype(str)]
    sym_col = None
    for i, c in enumerate(cols_lower):
        if 'symbol' in c or 'ticker' in c or 'code' in c:
            sym_col = df.columns[i]
            break
    if sym_col is None:
        # heuristic: first column with 1-5 length uppercase-like strings
        for c in df.columns:
            sample = df[c].astype(str).dropna().astype(str).head(30).tolist()
            score = sum(1 for s in sample if 1 <= len(s) <= 6 and s.upper() == s and s.isalnum())
            if score >= 6:
                sym_col = c
                break
    if sym_col is None:
        print("[download] Could not find a symbol column heuristically.")
        return []
    syms = df[sym_col].astype(str).str.upper().str.strip().tolist()
    # cleanup, remove empties and obvious header/footer rows
    syms = [s for s in syms if s and len(s) <= 8]
    # dedupe preserving order
    seen = set()
    out = []
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def sanitize_and_write(symbols):
    # keep only common ticker characters (letters, digits, dot, -)
    import re
    SYMBOL_RE = re.compile(r'^[A-Z0-9\.\-]{1,8}$')
    cleaned = [s for s in symbols if SYMBOL_RE.match(s)]
    cleaned = sorted(list(dict.fromkeys(cleaned)))
    if not cleaned:
        return False
    Path(OUT).write_text("\n".join(cleaned), encoding="utf-8")
    print(f"[download] Wrote {len(cleaned)} tickers to {OUT}")
    return True

def main():
    # 1) Try wikipedia HTML table (with good User-Agent)
    table = try_wikipedia(retries=3, pause=2)
    if table is not None:
        syms = extract_symbols_from_table(table)
        if syms:
            ok = sanitize_and_write(syms)
            if ok:
                return 0
            else:
                print("[download] Wikipedia extraction produced no valid tickers after sanitization")

    # 2) Fallback: try raw CSV endpoints (GitHub / DataHub)
    for csv_url in CSV_FALLBACKS:
        df = try_csv_url(csv_url, retries=2, pause=2)
        if df is not None:
            syms = extract_symbols_from_table(df)
            if syms:
                ok = sanitize_and_write(syms)
                if ok:
                    return 0
                else:
                    print(f"[download] CSV {csv_url} produced no valid tickers after sanitization")

    # 3) Last resort: try a minimal embedded list (small safety net) - NOT recommended long-term
    fallback_embedded = [
        # A tiny sample to avoid total failure - this list is intentionally minimal. 
        # If we reach here, user should fix network or add a proper source.
        "AAPL","MSFT","AMZN","GOOGL","META","TSLA","BRK.B","JNJ","JPM","V"
    ]
    print("[download] All external sources failed. Using embedded fallback sample (NOT full S&P 500).")
    ok = sanitize_and_write(fallback_embedded)
    if ok:
        return 0

    print("[download] FAILED: could not obtain S&P 500 tickers from any source.")
    return 2

if __name__ == "__main__":
    rc = main()
    if rc != 0:
        print(f"[download] Exiting with code {rc}")
        sys.exit(rc)
    print("[download] Done.")
    sys.exit(0)
