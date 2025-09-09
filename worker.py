#!/usr/bin/env python3
# worker.py
"""
S&P500 worker: fetches ~14 months daily OHLCV, computes indicators and writes raw-results.json
"""
import os, sys, json, time, math, traceback, subprocess
from pathlib import Path
from datetime import datetime, timedelta
import re
from io import StringIO

# imports
try:
    import pandas as pd
    import numpy as np
    import yfinance as yf
    from ta.trend import ADXIndicator
    from ta.volatility import AverageTrueRange
    from ta.volume import OnBalanceVolumeIndicator
    import requests
except Exception as e:
    print("IMPORT ERROR:", e)
    traceback.print_exc()
    # write artifact so aggregator isn't missing files
    Path(".").mkdir(parents=True, exist_ok=True)
    JOB_INDEX = int(os.environ.get("JOB_INDEX","0"))
    out = {"results": [], "attempted_count": 0, "processed_count": 0, "errors":[f"IMPORT ERROR: {e}"], "job_index": JOB_INDEX, "ts": datetime.utcnow().isoformat()+"Z"}
    with open(f"raw-results-{JOB_INDEX}.json","w",encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    sys.exit(0)

# config
JOB_INDEX = int(os.environ.get("JOB_INDEX","0"))
JOB_TOTAL = int(os.environ.get("JOB_TOTAL","1"))
OUT_FN = f"raw-results-{JOB_INDEX}.json"
LIST_FN = "sp500_list.txt"
VOL_SPIKE_MULT = float(os.environ.get("VOL_SPIKE_MULT", "1.5"))
RECENT_DAYS = int(os.environ.get("RECENT_DAYS", "8"))
SLOPE_DAYS = int(os.environ.get("SLOPE_DAYS","14"))
MIN_ROWS = 60

SYMBOL_RE = re.compile(r'^[A-Z0-9\.\-]{1,10}$')

def ensure_list():
    if Path(LIST_FN).exists():
        print("[worker] Found", LIST_FN)
        return True
    if Path("download_sp500_list.py").exists():
        try:
            subprocess.run([sys.executable, "download_sp500_list.py"], check=True, timeout=120)
        except Exception as e:
            print("[worker] Running download script failed:", e)
    if Path(LIST_FN).exists():
        return True
    # fallback: try wiki directly
    try:
        import pandas as pd
        r = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", timeout=20)
        r.raise_for_status()
        tables = pd.read_html(r.text)
        for t in tables:
            cols = [c.lower() for c in t.columns.astype(str)]
            for i,c in enumerate(cols):
                if 'symbol' in c or 'ticker' in c:
                    syms = t[t.columns[i]].astype(str).str.upper().str.strip().tolist()
                    Path(LIST_FN).write_text("\n".join(sorted(set(syms))), encoding="utf-8")
                    return True
    except Exception as e:
        print("[worker] fallback wiki fetch failed:", e)
    return Path(LIST_FN).exists()

def load_tickers():
    tickers = []
    try:
        with open(LIST_FN,"r",encoding="utf-8") as fh:
            for ln in fh:
                s = ln.strip().upper()
                if not s: continue
                if SYMBOL_RE.match(s):
                    tickers.append(s)
    except Exception as e:
        print("load_tickers error:", e)
    return sorted(list(dict.fromkeys(tickers)))

def chunk_round_robin(lst, total, index):
    if total <= 1: return lst[:]
    return [t for i,t in enumerate(lst) if (i % total) == index]

def normalize_cols(df):
    # handle tuple columns and variant names
    cols = []
    for c in df.columns:
        if isinstance(c, tuple):
            s = "_".join([str(x) for x in c if x is not None])
        else:
            s = str(c)
        s = s.strip()
        low = s.lower()
        if 'open' in low and 'adj' not in low:
            cols.append('Open')
        elif 'high' in low:
            cols.append('High')
        elif 'low' in low:
            cols.append('Low')
        elif 'close' in low and 'adj' in low:
            cols.append('Adj Close')
        elif 'close' in low:
            cols.append('Close')
        elif 'volume' in low:
            cols.append('Volume')
        else:
            cols.append(s)
    df.columns = cols
    return df

def fetch_stooq(ticker, days=440):
    end = datetime.utcnow().date()
    start = end - timedelta(days=days)
    s = ticker.upper()
    if not s.endswith(".US"):
        s = s + ".US"
    url = f"https://stooq.com/q/d/l/?s={s}&d1={start.strftime('%Y%m%d')}&d2={end.strftime('%Y%m%d')}&i=d"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200 or not r.text:
            return None
        df = pd.read_csv(StringIO(r.text), parse_dates=['Date'])
        if df.empty: return None
        df = df.rename(columns=lambda c: str(c).capitalize() if isinstance(c, str) else str(c))
        df = df.set_index('Date').sort_index()
        df = normalize_cols(df)
        if set(['Open','High','Low','Close','Volume']).issubset(df.columns):
            return df[['Open','High','Low','Close','Volume']]
    except Exception as e:
        print("stooq fetch error", ticker, e)
    return None

def fetch_with_retries(ticker, retries=3):
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            df = yf.download(ticker, period="14mo", interval="1d", progress=False, threads=False)
            if df is None or getattr(df,"shape",(0,))[0] == 0:
                raise ValueError("empty df")
            df = normalize_cols(df)
            if not set(['Open','High','Low','Close','Volume']).issubset(df.columns):
                raise ValueError("missing columns")
            return df[['Open','High','Low','Close','Volume']], "yfinance"
        except Exception as e:
            last_exc = e
            print(f"[fetch] {ticker} attempt {attempt} failed: {e}")
            time.sleep(1 * attempt)
    # stooq fallback
    st = fetch_stooq(ticker)
    if st is not None:
        return st, "stooq"
    raise last_exc if last_exc is not None else RuntimeError("fetch failed")

def compute_indicators(df):
    try:
        df = df.copy()
        df = normalize_cols(df)
        if df.shape[0] < 30:
            return None
        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df), index=df.index)

        out = {}
        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - signal
        out['macd_hist'] = float(macd_hist.iloc[-1])
        out['macd_slope'] = float((macd_hist.iloc[-1] - macd_hist.iloc[-(SLOPE_DAYS+1)]) / SLOPE_DAYS) if len(macd_hist) >= SLOPE_DAYS+1 else 0.0
        out['macd_bull'] = int((macd_line.tail(RECENT_DAYS) > signal.tail(RECENT_DAYS)).any() and macd_line.iloc[-1] > signal.iloc[-1])

        # RSI 14
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        roll_up = up.rolling(window=14).mean()
        roll_down = down.rolling(window=14).mean()
        rs = roll_up / (roll_down + 1e-9)
        rsi = 100 - (100 / (1 + rs))
        out['rsi'] = float(rsi.iloc[-1])
        out['rsi_slope'] = float((rsi.iloc[-1] - rsi.iloc[-(SLOPE_DAYS+1)])/SLOPE_DAYS) if len(rsi) >= SLOPE_DAYS+1 else 0.0

        # SMA20 EMA50 and EMA200 for longer-term
        sma20 = close.rolling(window=20).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        ema200 = close.ewm(span=200, adjust=False).mean()
        out['sma20'] = float(sma20.iloc[-1])
        out['ema50'] = float(ema50.iloc[-1])
        out['ema200'] = float(ema200.iloc[-1])
        out['above_trend'] = int((close.iloc[-1] > sma20.iloc[-1]) and (close.iloc[-1] > ema50.iloc[-1]))

        # Bollinger
        std20 = close.rolling(window=20).std()
        bb_upper = sma20 + 2 * std20
        out['bb_breakout'] = int(close.iloc[-1] > bb_upper.iloc[-1])

        # ADX
        try:
            adx = ADXIndicator(high=high, low=low, close=close, window=14).adx().iloc[-1]
            out['adx'] = float(adx)
        except Exception:
            out['adx'] = 0.0

        # ATR
        try:
            atr = AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range().iloc[-1]
            out['atr'] = float(atr)
        except Exception:
            out['atr'] = float((high - low).rolling(14).mean().iloc[-1])

        # OBV slope
        try:
            obv = OnBalanceVolumeIndicator(close=close, volume=vol).on_balance_volume()
            out['obv_slope'] = float((obv.iloc[-1] - obv.iloc[-(SLOPE_DAYS+1)]) / SLOPE_DAYS) if len(obv) >= SLOPE_DAYS+1 else 0.0
        except Exception:
            out['obv_slope'] = 0.0

        # Volume metrics
        vol20 = vol.rolling(window=20).mean()
        out['avg_vol20'] = float(vol20.iloc[-1]) if not vol20.empty else float(vol.iloc[-1])
        out['vol_spike'] = int(float(vol.iloc[-1]) > VOL_SPIKE_MULT * out['avg_vol20'])

        # Momentum: 14-day % change
        out['mom14'] = float((close.iloc[-1] - close.iloc[-15]) / close.iloc[-15]) if len(close) > 15 else 0.0

        # wave_strength heuristic vs sma20
        try:
            recent_close = close.tail(60)
            peaks = recent_close[(recent_close.shift(1) < recent_close) & (recent_close.shift(-1) < recent_close)]
            out['wave_strength'] = float(peaks.iloc[-1] / sma20.iloc[-1]) if len(peaks) > 0 and sma20.iloc[-1] > 0 else 1.0
        except Exception:
            out['wave_strength'] = 1.0

        out['last_close'] = float(close.iloc[-1])
        return out
    except Exception:
        traceback.print_exc()
        return None

def main():
    errors = []
    results = []
    attempted = 0
    processed = 0
    print(f"[worker] job {JOB_INDEX}/{JOB_TOTAL} start")
    ok = ensure_list()
    if not ok:
        print("[worker] S&P 500 list missing, aborting")
        out = {"results": [], "attempted_count": 0, "processed_count": 0, "errors": ["sp500 list missing"], "job_index": JOB_INDEX, "ts": datetime.utcnow().isoformat()+"Z"}
        with open(OUT_FN,"w",encoding="utf-8") as fh:
            json.dump(out, fh, indent=2)
        return

    tickers = load_tickers()
    print(f"[worker] loaded {len(tickers)} tickers")
    assigned = chunk_round_robin(tickers, JOB_TOTAL, JOB_INDEX)
    print(f"[worker] assigned {len(assigned)} tickers: sample {assigned[:6]}")
    attempted = len(assigned)
    for i, ticker in enumerate(assigned, 1):
        try:
            print(f"[worker] ({i}/{attempted}) fetching {ticker}")
            df, source = fetch_with_retries(ticker, retries=3)
            if df is None or df.empty or df.shape[0] < MIN_ROWS:
                print(f"[worker] {ticker} insufficient data from {source}")
                errors.append(f"{ticker}: insufficient data from {source}")
                continue
            indicators = compute_indicators(df)
            if indicators is None:
                errors.append(f"{ticker}: indicator compute failed")
                continue
            indicators['ticker'] = ticker
            indicators['fetch_source'] = source
            indicators['ts'] = datetime.utcnow().isoformat()+"Z"
            results.append(indicators)
            processed += 1
            time.sleep(0.05)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"[worker] exception {ticker}: {e}\n{tb}")
            errors.append(f"{ticker}: {e}")
            continue

    out = {"results": results, "attempted_count": attempted, "processed_count": processed, "errors": errors, "job_index": JOB_INDEX, "ts": datetime.utcnow().isoformat()+"Z"}
    with open(OUT_FN,"w",encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    print(f"[worker] wrote {OUT_FN} attempted={attempted} processed={processed} errors={len(errors)}")

if __name__ == "__main__":
    main()
