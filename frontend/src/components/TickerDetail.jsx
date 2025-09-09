import React, {useEffect, useState} from "react";
import {Chart, registerables} from "chart.js";
Chart.register(...registerables);

export default function TickerDetail({ticker}){
  const [data, setData] = useState(null);

  useEffect(()=>{
    if(!ticker) return;
    fetch(`/data/${ticker}.json`, {cache:'no-store'}).then(r=>r.json()).then(j=>setData(j)).catch(e=>console.error(e));
  },[ticker]);

  useEffect(()=>{
    if(!data || !data.history) return;
    // draw chart in canvas
    const ctx = document.getElementById('histChart')?.getContext('2d');
    if(!ctx) return;
    const labels = data.history.map(h=>h.Date);
    const closes = data.history.map(h=>h.Close);
    // eslint-disable-next-line no-unused-vars
    const chart = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets: [{ label: `${ticker} Close`, data: closes, tension: 0.2 }] },
      options: { plugins:{legend:{display:false}}, scales:{x:{display:false}} }
    });
    return ()=>{ chart.destroy(); };
  },[data, ticker]);

  if(!ticker) return <div>Select a ticker to view details</div>;
  if(!data) return <div>Loading {ticker}…</div>;

  return (
    <div>
      <h2 className="text-xl font-semibold mb-2">{ticker}</h2>
      <div className="mb-2">Score: <strong>{data.indicators.score_0_100}</strong></div>
      <div className="mb-2 text-slate-400">{data.indicators.explanation}</div>
      <canvas id="histChart" height="150"></canvas>

      <div className="mt-3 text-sm">
        <div>Last Close: {data.indicators.last_close}</div>
        <div>Avg Vol(20): {data.indicators.avg_vol20}</div>
        <div className="mt-2">
          <strong>Key indicators</strong>
          <ul className="mt-1">
            {Object.entries(data.indicators).map(([k,v]) => (
              <li key={k}><strong>{k}</strong>: {String(v)}</li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}
