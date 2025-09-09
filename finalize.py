#!/usr/bin/env python3
# finalize.py
"""
Aggregate raw-results-*.json and generate public/top_picks.json + dashboard files.
RSI is transformed to prefer RSI < 40 (undervalued).
"""
import glob, json, os, math
from datetime import datetime
from pathlib import Path

PUBLIC_DIR = "public"
JSON_OUT = os.path.join(PUBLIC_DIR, "top_picks.json")
INDEX_OUT = os.path.join(PUBLIC_DIR, "index.html")
APP_JS_OUT = os.path.join(PUBLIC_DIR, "app.js")
STYLE_OUT = os.path.join(PUBLIC_DIR, "style.css")

NUMERIC_FEATURES = ['macd_hist','macd_slope','rsi','rsi_slope','wave_strength','adx','atr','obv_slope','mom14']
BOOL_FEATURES = ['macd_bull','bb_breakout','vol_spike','above_trend']
ALL_FEATURES = NUMERIC_FEATURES + BOOL_FEATURES

WEIGHTS = {
    'macd_hist': 2.0, 'macd_slope': 1.5, 'rsi': 1.5, 'rsi_slope': 1.0,
    'wave_strength': 2.0, 'adx': 1.0, 'atr': 1.0, 'obv_slope': 1.0,
    'mom14': 1.5,
    'macd_bull': 3.0, 'bb_breakout': 1.0, 'vol_spike': 0.7, 'above_trend': 1.0
}

def find_artifacts():
    files = sorted(glob.glob("**/raw-results-*.json", recursive=True))
    print(f"[finalize] found {len(files)} artifact files")
    return files

def load_json(fn):
    try:
        with open(fn, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print("[finalize] load error", fn, e)
        return None

def safef(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def min_max_scale(xs):
    if not xs: return []
    mn, mx = min(xs), max(xs)
    if math.isclose(mn, mx): return [0.5]*len(xs)
    return [(v-mn)/(mx-mn) for v in xs]

def build_explanation(row):
    expl=[]
    if int(row.get('macd_bull',0)): expl.append("MACD bullish crossover")
    if int(row.get('bb_breakout',0)): expl.append("Bollinger breakout")
    try:
        if float(row.get('adx',0))>25: expl.append("strong trend (ADX)")
    except: pass
    if int(row.get('vol_spike',0)): expl.append("volume spike")
    if int(row.get('above_trend',0)): expl.append("price above short trend")
    try:
        rsi = float(row.get('rsi',50))
        if rsi < 30: expl.append("RSI oversold")
    except: pass
    return "; ".join(expl) if expl else "no major signals"

def aggregate():
    files = find_artifacts()
    total_attempted=0
    total_processed=0
    errors=[]
    rows=[]
    for f in files:
        j = load_json(f)
        if not j: continue
        total_attempted += int(j.get("attempted_count",0))
        total_processed += int(j.get("processed_count",0))
        errs = j.get("errors",[])
        if errs:
            errors.extend([f"{f}: {e}" for e in errs])
        for r in j.get("results",[]):
            rows.append(r)
    print(f"[finalize] aggregated attempted={total_attempted} processed={total_processed} rows={len(rows)} errors={len(errors)}")
    Path(PUBLIC_DIR).mkdir(parents=True, exist_ok=True)

    if not rows:
        out = {"generated_at": datetime.utcnow().isoformat()+"Z","count_total":total_attempted,"count_results":total_processed,"failed_count":max(0,total_attempted-total_processed),"errors":errors,"top":[]}
        with open(JSON_OUT,"w",encoding="utf-8") as fh: json.dump(out, fh, indent=2)
        write_placeholder()
        return out

    # build numeric matrices
    numeric = {f:[] for f in NUMERIC_FEATURES}
    bools = {f:[] for f in BOOL_FEATURES}
    extras=[]
    for r in rows:
        for f in NUMERIC_FEATURES:
            numeric[f].append(safef(r.get(f,0.0)))
        for b in BOOL_FEATURES:
            bools[b].append(1 if int(r.get(b,0)) else 0)
        extras.append({"avg_vol20":safef(r.get('avg_vol20',0.0)),"last_close":safef(r.get('last_close',0.0))})

    # transform RSI: prefer RSI < 40
    if 'rsi' in numeric:
        raw = numeric['rsi']
        transformed=[]
        for r in raw:
            rcl = max(0.0, min(100.0, r))
            if rcl <= 40.0:
                transformed.append((40.0 - rcl) / 40.0)   # 1.0 @ 0; 0.0 @ 40
            else:
                transformed.append(0.0)
        numeric['rsi'] = transformed
        print("[finalize] transformed RSI sample:", numeric['rsi'][:6])

    # normalize numeric
    norm={}
    for f in NUMERIC_FEATURES:
        norm[f]=min_max_scale(numeric[f])

    # invert atr
    if 'atr' in norm:
        norm['atr']=[1.0 - v for v in norm['atr']]

    items=[]
    abs_sum = sum(abs(WEIGHTS.get(f,0)) for f in ALL_FEATURES) or 1.0
    for i, r in enumerate(rows):
        feat_vec = []
        for f in NUMERIC_FEATURES:
            feat_vec.append(norm[f][i] if i < len(norm[f]) else 0.0)
        for b in BOOL_FEATURES:
            feat_vec.append(bools[b][i] if i < len(bools[b]) else 0)
        raw_score = sum(v * WEIGHTS.get(k,0) for v,k in zip(feat_vec, ALL_FEATURES))
        composite = raw_score / abs_sum
        items.append({"ticker": r.get('ticker'), "raw": composite, "features_numeric": {f:numeric[f][i] for f in NUMERIC_FEATURES}, "features_bool": {b:bools[b][i] for b in BOOL_FEATURES}, "extras": extras[i]})

    comps = [it['raw'] for it in items]
    mn, mx = min(comps), max(comps)
    if math.isclose(mn,mx):
        for it in items: it['score01']=0.5
    else:
        for it in items:
            it['score01']=(it['raw']-mn)/(mx-mn)

    for it in items:
        it['score100']=round(it['score01']*100,2)
        it['score10']=round(it['score01']*10,2)
        merged = {}
        merged.update(it['features_numeric'])
        merged.update(it['features_bool'])
        it['explanation']=build_explanation(merged)

    items.sort(key=lambda x:(x['score100'], x['features_numeric'].get('macd_hist',0), x['extras'].get('avg_vol20',0)), reverse=True)

    top = []
    for it in items[:500]:
        top.append({"ticker":it['ticker'],"score_0_100":it['score100'],"score_0_10":it['score10'],"features":it['features_numeric'],"bools":it['features_bool'],"explanation":it['explanation'],"avg_vol20":it['extras'].get('avg_vol20'),"last_close":it['extras'].get('last_close')})

    out = {"generated_at": datetime.utcnow().isoformat()+"Z","count_total":total_attempted,"count_results":total_processed,"failed_count":max(0,total_attempted-total_processed),"errors":errors,"top":top}
    with open(JSON_OUT,"w",encoding="utf-8") as fh: json.dump(out, fh, indent=2)
    print("[finalize] wrote", JSON_OUT, "with", len(top), "items")
    write_dashboard()
    return out

def write_placeholder():
    html = "<!doctype html><html><head><meta charset='utf-8'><title>No results</title></head><body><h2>No results</h2><p>Check workflow logs and artifacts</p></body></html>"
    Path(INDEX_OUT).write_text(html, encoding="utf-8")
    Path(STYLE_OUT).write_text("body{font-family:Arial;padding:20px}", encoding="utf-8")

def write_dashboard():
    # nicer HTML using Google Fonts and Chart.js from CDN
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>S&P 500 Daily Scan</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <div class="wrap">
    <header><h1>S&amp;P 500 Daily Scan</h1><div id="meta"></div></header>
    <section class="controls">
      <input id="search" placeholder="Search ticker or explanation" />
      <label>Top <input id="topN" type="number" value="50" min="1" max="500" /></label>
    </section>

    <section class="charts">
      <canvas id="histChart" height="120"></canvas>
      <canvas id="pieChart" height="120"></canvas>
    </section>

    <section id="resultsSection">
      <table id="results">
        <thead><tr><th>#</th><th>Ticker</th><th>Score</th><th>Last</th><th>AvgVol20</th><th>Explanation</th></tr></thead>
        <tbody></tbody>
      </table>
    </section>

    <footer>Generated at <span id="generated_at"></span> — <a href="top_picks.json">top_picks.json</a></footer>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="app.js"></script>
</body>
</html>
"""
    css = """
:root{--bg:#0f1724;--card:#0b1220;--accent:#4f46e5;--muted:#9aa4b2;--glass:rgba(255,255,255,0.03)}
*{box-sizing:border-box}
body{font-family:Inter,system-ui,-apple-system,'Segoe UI',Roboto,Arial;background:linear-gradient(180deg,#071022 0%,#071a2a 100%);color:#e6eef6;margin:0;padding:28px}
.wrap{max-width:1200px;margin:0 auto}
header{display:flex;align-items:center;justify-content:space-between}
header h1{font-weight:800;margin:0;font-size:20px}
.controls{margin:14px 0;display:flex;gap:12px;align-items:center}
.controls input{padding:8px 12px;border-radius:8px;border:1px solid rgba(255,255,255,0.06);background:rgba(255,255,255,0.02);color:inherit}
.charts{display:flex;gap:12px;margin:12px 0}
.charts canvas{background:var(--card);border-radius:8px;padding:10px}
#results{width:100%;border-collapse:collapse;background:var(--card);border-radius:10px;overflow:hidden}
#results thead th{padding:12px;text-align:left;border-bottom:1px solid rgba(255,255,255,0.04);font-weight:600}
#results tbody td{padding:10px;border-bottom:1px solid rgba(255,255,255,0.02)}
#results tbody tr:hover{background:rgba(255,255,255,0.02)}
footer{margin-top:12px;color:var(--muted)}
"""
    js = r"""
(async function(){
  const meta=document.getElementById('meta'), gen=document.getElementById('generated_at'), tbody=document.querySelector('#results tbody'), search=document.getElementById('search'), topN=document.getElementById('topN');
  let raw=null;
  async function load(){
    try{
      const res = await fetch('top_picks.json',{cache:'no-store'});
      if(!res.ok) throw new Error('Fetch failed '+res.status);
      raw = await res.json();
      document.getElementById('generated_at').textContent = raw.generated_at || '';
      meta.textContent = `Results: ${raw.count_results || 0} (attempted ${raw.count_total || 0})`;
      render();
      drawCharts(raw.top || []);
    }catch(e){
      meta.textContent = 'Error loading data: '+e;
      console.error(e);
    }
  }
  function render(){
    const q=(search.value||'').toLowerCase().trim();
    const limit = Math.max(1,Math.min(500,parseInt(topN.value||50)));
    tbody.innerHTML='';
    let shown=0;
    for(let i=0;i<(raw.top||[]).length && shown<limit;i++){
      const it = raw.top[i];
      const txt = (it.ticker+' '+(it.explanation||'')).toLowerCase();
      if(q && !txt.includes(q)) continue;
      const tr=document.createElement('tr');
      tr.innerHTML = `<td>${i+1}</td><td>${it.ticker}</td><td>${it.score_0_100}</td><td>${it.last_close||''}</td><td>${it.avg_vol20||''}</td><td>${it.explanation||''}</td>`;
      tbody.appendChild(tr); shown++;
    }
  }
  function drawCharts(items){
    const scores = items.map(i=>i.score_0_100);
    const labels = items.map(i=>i.ticker);
    const ctx=document.getElementById('histChart').getContext('2d');
    new Chart(ctx,{type:'bar',data:{labels:labels.slice(0,20),datasets:[{label:'Score (top 20)',data:scores.slice(0,20),backgroundColor:'rgba(79,70,229,0.8)'}]},options:{plugins:{legend:{display:false}},scales:{x:{display:false}}}});
    const pieCtx = document.getElementById('pieChart').getContext('2d');
    const bins=[0,0,0,0,0];
    items.forEach(it=>{const s=it.score_0_100; if(s>80) bins[0]++; else if(s>60) bins[1]++; else if(s>40) bins[2]++; else if(s>20) bins[3]++; else bins[4]++;});
    new Chart(pieCtx,{type:'doughnut',data:{labels:['80-100','60-80','40-60','20-40','0-20'],datasets:[{data:bins,backgroundColor:['#4f46e5','#6366f1','#60a5fa','#93c5fd','#c7d2fe']}]},options:{plugins:{legend:{position:'bottom'}}}});
  }
  search.addEventListener('input', ()=> load());
  topN.addEventListener('change', ()=> load());
  await load();
})();
"""
    Path(INDEX_OUT).write_text(html, encoding="utf-8")
    Path(STYLE_OUT).write_text(css, encoding="utf-8")
    Path(APP_JS_OUT).write_text(js, encoding="utf-8")
    print("[finalize] dashboard written to", PUBLIC_DIR)

def safef(v):
    try: return float(v)
    except: return 0.0

if __name__ == "__main__":
    aggregate()
