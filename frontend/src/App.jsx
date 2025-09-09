import React, {useState, useEffect} from "react";
import TopTable from "./components/TopTable";
import TickerDetail from "./components/TickerDetail";

export default function App(){
  const [data, setData] = useState(null);
  const [selected, setSelected] = useState(null);

  useEffect(()=>{
    fetch("/top_picks.json", {cache:'no-store'}).then(r=>r.json()).then(j=>setData(j)).catch(e=>console.error(e));
  },[]);

  return (
    <div className="container">
      <header className="header">
        <h1 className="text-3xl font-bold">S&P 500 Daily Scan</h1>
        <div className="text-sm text-slate-400">Generated at <span>{data?.generated_at}</span></div>
      </header>

      <div className="grid md:grid-cols-3 gap-6">
        <div className="md:col-span-2 card">
          <TopTable data={data?.top || []} onSelect={setSelected} />
        </div>
        <div className="card">
          <TickerDetail ticker={selected} />
        </div>
      </div>
    </div>
  );
}
