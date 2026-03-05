import { useEffect, useState } from "react";
import { api } from "../api/client.js";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { Button } from "../components/Button.jsx";

export default function Races(){
  const [year, setYear] = useState("");
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");

  async function load(){
    setErr("");
    setData(null);
    try{
      const res = await api.races(year || undefined, 50, 0);
      setData(res);
    }catch(e){
      setErr(String(e.message || e));
    }
  }

  useEffect(() => { load(); }, []);

  return (
    <Card
      title="Races"
      right={
        <div style={{display:"flex", gap:8, alignItems:"center"}}>
          <div style={{width:140}}><Input placeholder="Year (e.g. 2020)" value={year} onChange={e=>setYear(e.target.value)} /></div>
          <Button onClick={load}>Load</Button>
        </div>
      }
    >
      {err && <div style={{color:"salmon"}}>{err}</div>}
      {!data && !err && <div>Loading…</div>}
      {data && (
        <>
          <div style={{color:"var(--muted)", marginBottom:10}}>Showing year: <b style={{color:"var(--text)"}}>{data.year}</b></div>
          <div style={{display:"grid", gap:10}}>
            {data.results.map(r => (
              <div key={r.raceId} style={{padding:12, border:"1px solid var(--line)", borderRadius:14, background:"rgba(255,255,255,0.02)"}}>
                <div style={{display:"flex", justifyContent:"space-between", gap:10}}>
                  <div style={{fontWeight:800}}>{r.round}. {r.name}</div>
                  <div style={{color:"var(--muted)"}}>raceId: {r.raceId}</div>
                </div>
                <div style={{color:"var(--muted)", fontSize:13}}>{r.date} {r.time || ""}</div>
              </div>
            ))}
          </div>
        </>
      )}
    </Card>
  );
}