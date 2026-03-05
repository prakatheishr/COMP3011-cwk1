import { useState } from "react";
import { api } from "../api/client.js";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { Button } from "../components/Button.jsx";

export default function RaceResults(){
  const [raceId, setRaceId] = useState("");
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");

  async function load(){
    setErr(""); setData(null);
    try{
      const res = await api.raceResults(raceId);
      setData(res);
    }catch(e){
      setErr(String(e.message || e));
    }
  }

  return (
    <Card
      title="Race Results"
      right={
        <div style={{display:"flex", gap:8, alignItems:"center"}}>
          <div style={{width:160}}><Input placeholder="raceId (e.g. 1027)" value={raceId} onChange={e=>setRaceId(e.target.value)} /></div>
          <Button onClick={load} disabled={!raceId}>Load</Button>
        </div>
      }
    >
      {err && <div style={{color:"salmon"}}>{err}</div>}
      {!data && !err && <div>Enter a raceId (from Races page) and load.</div>}
      {data && (
        <>
          <div style={{color:"var(--muted)", marginBottom:10}}>
            <b style={{color:"var(--text)"}}>{data.race.year} {data.race.name}</b> • {data.race.date}
          </div>

          <div style={{display:"grid", gap:8}}>
            {data.results.slice(0, 10).map(r => (
              <div key={r.resultId} style={{display:"flex", justifyContent:"space-between", gap:10, padding:10, border:"1px solid var(--line)", borderRadius:14, background:"rgba(255,255,255,0.02)"}}>
                <div>
                  <b>{r.positionOrder}.</b> {r.driverName} <span style={{color:"var(--muted)"}}>({r.constructorName})</span>
                </div>
                <div style={{color:"var(--muted)"}}>{r.points} pts • {r.status}</div>
              </div>
            ))}
          </div>
        </>
      )}
    </Card>
  );
}