import React, {useState} from "react";

export default function TopTable({data, onSelect}){
  const [q, setQ] = useState("");
  const [topN, setTopN] = useState(50);

  const filtered = (data || []).filter(it=>{
    if(!q) return true;
    return (it.ticker + " " + (it.explanation||"")).toLowerCase().includes(q.toLowerCase());
  }).slice(0, topN);

  return (
    <div>
      <div className="flex justify-between mb-3">
        <input className="search" placeholder="Search ticker or explanation" value={q} onChange={e=>setQ(e.target.value)} />
        <div>
          <label className="mr-2">Top</label>
          <input type="number" className="search w-20" value={topN} onChange={e=>setTopN(Number(e.target.value))} />
        </div>
      </div>

      <table className="table text-sm">
        <thead>
          <tr className="text-left text-slate-300">
            <th>#</th><th>Ticker</th><th>Score</th><th>Last</th><th>AvgVol20</th><th>Explanation</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((it, idx)=>(
            <tr key={it.ticker} className="hover:bg-slate-700 cursor-pointer" onClick={()=>onSelect(it.ticker)}>
              <td className="px-2 py-2">{idx+1}</td>
              <td className="px-2 py-2 font-semibold">{it.ticker}</td>
              <td className="px-2 py-2">{it.score_0_100}</td>
              <td className="px-2 py-2">{it.last_close}</td>
              <td className="px-2 py-2">{it.avg_vol20}</td>
              <td className="px-2 py-2">{it.explanation}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
