#!/usr/bin/env python3
# finalize.py
"""
Aggregate worker artifacts, score, write public/top_picks.json,
archive the JSON, and generate per-ticker JSON history files used by the frontend.

Key outputs:
- public/top_picks.json
- public/data/<TICKER>.json  (indicators + last 365 days OHLCV)
- public/data/archive/<timestamp>_top_picks.json
- (dashboard build uses public/ as build target)
"""
import glob, json, os, math, time
from datetime import datetime, timedelta
from pathlib import Path
import traceback

# We'll import yfinance here for per-ticker history fetch.
try:
    import yfinance as yf
    import pandas as pd
except Exception as e:
    print("Missing module:", e)
    raise

PUBLIC_DIR = "public"
DATA_DIR = os.path.join(PUBLIC_DIR, "data")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
JSON_OUT = os.path.join(PUBLIC_DIR, "top_picks.json")

# Features & weights (same style as before)
NUMERIC_FEATURES = ['macd_hist','macd_slope','rsi','rsi_slope','wave_strength','adx','atr','obv_slope','mom14']
BOOL_FEATURES = ['macd_bull','bb_breakout','vol_spike','above_trend']
ALL_FEATURES = NUMERIC_FEATURES + BOOL_FEATURES

WEIGHTS = {
    'macd_hist': 2.0, 'macd_slope': 1.5, 'rsi': 1.5, 'rsi_slope': 1.0,
    'wave_strength': 2.0, 'adx': 1.0, 'atr': 1.0, 'obv_slope': 1.0, 'mom14': 1.5,
    'macd_bull': 3.0, 'bb_breakout': 1.0, 'vol_spike': 0.7, 'above_trend': 1.0
}

# How many days of history to fetch for charting
HISTORY_DAYS = 440  # ~14 months
# Per-ticker fetch retry / pause
FETCH_RETRIES = 2
FETCH_PAUSE = 1.0

def safef(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def min_max(xs):
    if not xs:
        return []
    mn = min(xs); mx = max(xs)
    if math.isclose(mn, mx):
        return [0.5]*len(xs)
    return [(x-mn)/(mx-mn) for x in xs]

def find_artifacts():
    files = sorted(glob.glob("**/raw-results-*.json", recursive=True))
    print(f"[finalize] found {len(files)} artifact files")
    return files

def safe_load(fn):
    try:
        with open(fn, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print("[finalize] load error", fn, e)
        return None

def build_explanation(row):
    expl=[]
    if int(row.get('macd_bull',0)): expl.append("recent MACD bullish crossover")
    if int(row.get('bb_breakout',0)): expl.append("Bollinger upper-band breakout")
    try:
        if float(row.get('adx',0)) > 25: expl.append("strong trend")
    except Exception:
        pass
    if int(row.get('vol_spike',0)): expl.append("volume spike")
    if int(row.get('above_trend',0)): expl.append("price above SMA/EMA")
    try:
        r = float(row.get('rsi',50))
        if r < 30: expl.append("RSI oversold")
        if r > 70: expl.append("RSI overbought")
    except Exception:
        pass
    try:
        if float(row.get('obv_slope',0)) > 0: expl.append("rising OBV")
    except Exception:
        pass
    try:
        if float(row.get('wave_strength',1)) > 1.05: expl.append("strong wave vs SMA20")
    except Exception:
        pass
    return "; ".join(expl) if expl else "no significant signals"

def aggregate_and_write():
    Path(PUBLIC_DIR).mkdir(parents=True, exist_ok=True)
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)

    files = find_artifacts()
    total_attempted = 0
    total_processed = 0
    errors = []
    rows = []

    for f in files:
        j = safe_load(f)
        if not j:
            continue
        total_attempted += int(j.get("attempted_count",0))
        total_processed += int(j.get("processed_count",0))
        errlist = j.get("errors",[])
        if errlist:
            errors.extend([f"{f}: {e}" for e in errlist])
        for r in j.get("results",[]):
            rows.append(r)

    print(f"[finalize] aggregated attempted={total_attempted} processed={total_processed} rows={len(rows)} errors={len(errors)}")

    if not rows:
        out = {
            "generated_at": datetime.utcnow().isoformat()+"Z",
            "count_total": total_attempted,
            "count_results": total_processed,
            "failed_count": max(0, total_attempted-total_processed),
            "errors": errors,
            "top": []
        }
        with open(JSON_OUT, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=2)
        print("[finalize] wrote empty top_picks.json")
        return out

    # Build numeric & bool arrays
    numeric = {f:[] for f in NUMERIC_FEATURES}
    bools = {f:[] for f in BOOL_FEATURES}
    extras = []

    for r in rows:
        for f in NUMERIC_FEATURES:
            numeric[f].append(safef(r.get(f,0.0)))
        for b in BOOL_FEATURES:
            bools[b].append(1 if int(r.get(b,0)) else 0)
        extras.append({
            "avg_vol20": safef(r.get("avg_vol20",0.0)),
            "last_close": safef(r.get("last_close",0.0)),
            "rsi": safef(r.get("rsi",50.0))
        })

    # Transform RSI to prefer RSI < 40 (your requested behavior)
    if 'rsi' in numeric:
        transformed = []
        for r in numeric['rsi']:
            rclamped = max(0.0, min(100.0, r))
            if rclamped <= 40.0:
                transformed.append((40.0 - rclamped) / 40.0)  # 1 @ r=0, 0 @ r=40
            else:
                transformed.append(0.0)
        numeric['rsi'] = transformed
        print("[finalize] transformed RSI sample:", numeric['rsi'][:6])

    # Normalize numeric features
    norm = {}
    for f in NUMERIC_FEATURES:
        norm[f] = min_max(numeric[f])

    # invert ATR (lower volatility better)
    if 'atr' in norm:
        norm['atr'] = [1.0 - v for v in norm['atr']]

    # Build composite scores
    abs_sum = sum(abs(WEIGHTS.get(k,0)) for k in ALL_FEATURES) or 1.0
    items = []
    for i, r in enumerate(rows):
        feat_vec = []
        for f in NUMERIC_FEATURES:
            feat_vec.append(norm.get(f, [0]*len(rows))[i])
        for b in BOOL_FEATURES:
            feat_vec.append(bools.get(b, [0]*len(rows))[i])
        raw_score = sum(v * WEIGHTS.get(k, 0) for v, k in zip(feat_vec, ALL_FEATURES))
        composite = raw_score / abs_sum
        items.append({
            "ticker": r.get("ticker"),
            "raw": composite,
            "features_numeric": {f: numeric[f][i] for f in NUMERIC_FEATURES},
            "features_bool": {b: bools[b][i] for b in BOOL_FEATURES},
            "extras": extras[i]
        })

    # normalize composite 0..1
    comps = [it['raw'] for it in items]
    mn = min(comps); mx = max(comps)
    if math.isclose(mn,mx):
        for it in items:
            it['score01'] = 0.5
    else:
        for it in items:
            it['score01'] = (it['raw'] - mn) / (mx - mn)

    for it in items:
        it['score_0_100'] = round(it['score01'] * 100, 2)
        it['score_0_10'] = round(it['score01'] * 10, 2)
        merged = {}
        merged.update(it['features_numeric'])
        merged.update(it['features_bool'])
        merged['rsi'] = it['extras'].get('rsi', 50.0)
        it['explanation'] = build_explanation(merged)

    # sort with tiebreakers (score, macd_hist, avg volume)
    items.sort(key=lambda it: (it['score_0_100'], it['features_numeric'].get('macd_hist',0), it['extras'].get('avg_vol20',0)), reverse=True)

    # write top picks JSON (top 500)
    top = []
    for it in items[:500]:
        top.append({
            "ticker": it['ticker'],
            "score_0_100": it['score_0_100'],
            "score_0_10": it['score_0_10'],
            "features": it['features_numeric'],
            "bools": it['features_bool'],
            "explanation": it['explanation'],
            "avg_vol20": it['extras'].get('avg_vol20'),
            "last_close": it['extras'].get('last_close')
        })

    out = {
        "generated_at": datetime.utcnow().isoformat()+"Z",
        "count_total": total_attempted,
        "count_results": total_processed,
        "failed_count": max(0, total_attempted-total_processed),
        "errors": errors,
        "top": top
    }

    # write top_picks.json
    with open(JSON_OUT, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    print("[finalize] wrote", JSON_OUT)

    # archive with timestamp
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_fn = os.path.join(ARCHIVE_DIR, f"{ts}_top_picks.json")
    with open(archive_fn, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    print("[finalize] archived to", archive_fn)

    # Generate per-ticker JSON files (indicators + recent OHLCV)
    # We will attempt to fetch HISTORY_DAYS of daily OHLCV via yfinance for each ticker
    # If a fetch fails, we still write indicator-only JSON.
    print("[finalize] fetching per-ticker historical OHLCV for chart pages (this may take a while)...")
    for i, it in enumerate(items):
        ticker = it['ticker']
        try:
            hist = None
            for attempt in range(1, FETCH_RETRIES+1):
                try:
                    yf_obj = yf.Ticker(ticker)
                    df = yf_obj.history(period=f"{HISTORY_DAYS}d", interval="1d", auto_adjust=False, actions=False)
                    if df is not None and not df.empty:
                        # keep last 365-ish rows
                        df = df.reset_index()
                        df['Date'] = df['Date'].dt.strftime("%Y-%m-%d")
                        hist = df[['Date','Open','High','Low','Close','Volume']].to_dict(orient='records')
                        break
                except Exception as e:
                    print(f"[finalize] {ticker} fetch attempt {attempt} error: {e}")
                time.sleep(FETCH_PAUSE * attempt)
            data_obj = {
                "ticker": ticker,
                "indicators": {
                    **it['features_numeric'],
                    **it['features_bool'],
                    "explanation": it['explanation'],
                    "score_0_100": it['score_0_100'],
                    "score_0_10": it['score_0_10'],
                    "last_close": it['extras'].get('last_close'),
                    "avg_vol20": it['extras'].get('avg_vol20')
                },
                "history": hist or []
            }
            out_fn = os.path.join(DATA_DIR, f"{ticker}.json")
            with open(out_fn, "w", encoding="utf-8") as fh:
                json.dump(data_obj, fh, indent=2)
            # small progress print occasionally
            if (i+1) % 50 == 0:
                print(f"[finalize] saved {i+1} per-ticker JSON files...")
        except Exception as e:
            print(f"[finalize] failed per-ticker for {ticker}: {e}")
            traceback.print_exc()
            try:
                # write indicator-only JSON
                data_obj = {
                    "ticker": ticker,
                    "indicators": {
                        **it['features_numeric'],
                        **it['features_bool'],
                        "explanation": it['explanation'],
                        "score_0_100": it['score_0_100'],
                        "score_0_10": it['score_0_10'],
                        "last_close": it['extras'].get('last_close'),
                        "avg_vol20": it['extras'].get('avg_vol20')
                    },
                    "history": []
                }
                out_fn = os.path.join(DATA_DIR, f"{ticker}.json")
                with open(out_fn, "w", encoding="utf-8") as fh:
                    json.dump(data_obj, fh, indent=2)
            except Exception:
                pass

    print("[finalize] per-ticker files generation complete.")

    return out

if __name__ == "__main__":
    aggregate_and_write()
