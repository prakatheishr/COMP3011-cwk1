import { useEffect, useState } from "react";
import { api } from "../api/client.js";
import { Card } from "../components/Card.jsx";

export default function Drivers(){
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");
  useEffect(() => {
    api.drivers(50,0).then(setData).catch(e => setErr(String(e.message || e)));
  }, []);

  return (
    <div className="grid">
      <Card title="Drivers">
        {err && <div style={{color:"salmon"}}>{err}</div>}
        {!data && !err && <div>Loading…</div>}
        {data && (
          <div style={{display:"grid", gap:10}}>
            {data.results.map(d => (
              <div key={d.driverId} style={{padding:12, border:"1px solid var(--line)", borderRadius:14, background:"rgba(255,255,255,0.02)"}}>
                <div style={{fontWeight:800}}>{d.forename} {d.surname}</div>
                <div style={{color:"var(--muted)", fontSize:13}}>{d.nationality} • {d.dob}</div>
              </div>
            ))}
          </div>
        )}
      </Card>

      <Card title="API Status">
        <Status/>
        <div style={{marginTop:10, color:"var(--muted)", fontSize:13}}>
          Tip: use the Insights page to generate recap/impact in plain or radio mode.
        </div>
      </Card>
    </div>
  );
}

function Status(){
  const [h, setH] = useState(null);
  const [err, setErr] = useState("");
  useEffect(() => {
    api.health().then(setH).catch(e => setErr(String(e.message || e)));
  }, []);
  if (err) return <div style={{color:"salmon"}}>{err}</div>;
  if (!h) return <div>Loading…</div>;
  return <pre style={{margin:0, whiteSpace:"pre-wrap"}}>{JSON.stringify(h, null, 2)}</pre>;
}