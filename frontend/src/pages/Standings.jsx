import { useState } from "react";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { Button } from "../components/Button.jsx";
import { api } from "../api/client.js";

export default function Standings(){

  const [year, setYear] = useState("");
  const [type, setType] = useState("drivers");
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");

  async function load(){
    setErr("");
    setData(null);

    try{
      let res;

      if(type === "drivers"){
        res = await api.driverStandings(year);
      }else{
        res = await api.constructorStandings(year);
      }

      setData(res);

    }catch(e){
      setErr(String(e.message || e));
    }
  }

  return (
    <Card
      title="Championship Standings"
      right={
        <div style={{display:"flex", gap:8, alignItems:"center"}}>

          <div style={{width:120}}>
            <Input
              placeholder="Year"
              value={year}
              onChange={e => setYear(e.target.value)}
            />
          </div>

          <select
            value={type}
            onChange={e => setType(e.target.value)}
            style={{
              padding:"8px",
              borderRadius:"10px",
              border:"1px solid var(--line)",
              background:"rgba(255,255,255,0.03)",
              color:"var(--text)"
            }}
          >
            <option value="drivers">Drivers</option>
            <option value="constructors">Constructors</option>
          </select>

          <Button onClick={load} disabled={!year}>
            Load
          </Button>

        </div>
      }
    >

      {err && <div style={{color:"salmon"}}>{err}</div>}

      {!data && !err && (
        <div>Enter a season year to view championship standings.</div>
      )}

      {data && (
        <div style={{display:"grid", gap:8}}>

          {data.results.map(r => (
            <div
              key={type === "drivers" ? r.driverId : r.constructorId}
              style={{
                display:"flex",
                justifyContent:"space-between",
                gap:10,
                padding:10,
                border:"1px solid var(--line)",
                borderRadius:14,
                background:"rgba(255,255,255,0.02)"
              }}
            >

              <div>
                <b>{r.rank}.</b>{" "}
                {type === "drivers" ? r.driverName : r.constructorName}
                <span style={{color:"var(--muted)"}}> ({r.nationality})</span>
              </div>

              <div style={{color:"var(--muted)"}}>
                {r.points} pts • {r.wins} wins
              </div>

            </div>
          ))}

        </div>
      )}

    </Card>
  );
}