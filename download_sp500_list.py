#!/usr/bin/env python3
# download_sp500_list.py
"""
Download S&P 500 tickers from Wikipedia and write sp500_list.txt
"""
import requests
import pandas as pd
from pathlib import Path
import time

OUT = "sp500_list.txt"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def fetch_wikipedia_table(retries=3, pause=3):
    for attempt in range(1, retries+1):
        try:
            r = requests.get(WIKI_URL, timeout=20)
            r.raise_for_status()
            tables = pd.read_html(r.text)
            # usually the first table is the constituents
            for t in tables:
                # find a column name likely to be 'Symbol' or 'Ticker'
                cols = [c.lower() for c in t.columns.astype(str)]
                if any('symbol' in c or 'ticker' in c for c in cols):
                    return t
            # fallback to the first table
            return tables[0] if tables else None
        except Exception as e:
            print(f"[download] Attempt {attempt} failed: {e}")
            time.sleep(pause)
    return None

def main():
    t = fetch_wikipedia_table()
    if t is None:
        print("[download] Failed to fetch S&P 500 table from Wikipedia.")
        raise SystemExit(2)
    # find symbol column
    cols_lower = [c.lower() for c in t.columns.astype(str)]
    sym_col = None
    for i,c in enumerate(cols_lower):
        if 'symbol' in c or 'ticker' in c:
            sym_col = t.columns[i]
            break
    if sym_col is None:
        print("[download] Could not find a symbol column in the Wikipedia table.")
        raise SystemExit(3)

    syms = t[sym_col].astype(str).str.upper().str.strip().tolist()
    Path(OUT).write_text("\n".join(sorted(set(syms))), encoding="utf-8")
    print(f"[download] Wrote {len(syms)} tickers to {OUT}")

if __name__ == "__main__":
    main()
