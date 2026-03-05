import { useEffect, useMemo, useState } from "react";
import { api } from "../api/client.js";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { Button } from "../components/Button.jsx";

export default function Drivers(){
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");
  const [q, setQ] = useState("");

useEffect(() => {
  let cancelled = false;

  async function loadAllDrivers(){
    setErr("");
    setData(null);

    try{
      const limit = 200;
      let offset = 0;
      let all = [];

      while(true){
        const page = await api.drivers(limit, offset); // expects { results: [...] }
        const batch = page?.results ?? [];
        all = all.concat(batch);

        if(batch.length < limit) break; // last page
        offset += limit;

        // safety: avoid infinite loop if API misbehaves
        if(offset > 5000) break;
      }

      if(!cancelled){
        setData({ results: all });
      }
    }catch(e){
      if(!cancelled){
        setErr(String(e.message || e));
      }
    }
  }

  loadAllDrivers();
  return () => { cancelled = true; };
}, []);

  const drivers = data?.results ?? [];

  const filtered = useMemo(() => {
    const s = q.trim().toLowerCase();
    if (!s) return drivers;
    return drivers.filter((d) => {
      const full = `${d.forename} ${d.surname}`.toLowerCase();
      return (
        full.includes(s) ||
        (d.surname || "").toLowerCase().includes(s) ||
        (d.nationality || "").toLowerCase().includes(s)
      );
    });
  }, [q, drivers]);

  return (
    <div className="grid">
      <Card
        title="Drivers"
        right={
          <div style={{display:"flex", gap:8, alignItems:"center"}}>
            <div style={{width:260}}>
              <Input
                placeholder="Search drivers (e.g., Hamilton, Verstappen, British)…"
                value={q}
                onChange={(e) => setQ(e.target.value)}
              />
            </div>
            <Button onClick={() => setQ("")} disabled={!q}>
              Clear
            </Button>
          </div>
        }
      >
        {err && <div style={{color:"salmon"}}>{err}</div>}
        {!data && !err && <div>Loading…</div>}

        {data && (
          <>
            <div style={{color:"var(--muted)", fontSize:13, marginBottom:10}}>
              Showing <b style={{color:"var(--text)"}}>{filtered.length}</b> of {drivers.length}
            </div>

            <div style={{display:"grid", gap:10}}>
              {filtered.map(d => (
                <div
                  key={d.driverId}
                  style={{
                    padding:12,
                    border:"1px solid var(--line)",
                    borderRadius:14,
                    background:"rgba(255,255,255,0.02)"
                  }}
                >
                  <div style={{fontWeight:800}}>
                    {d.forename} {d.surname}
                  </div>
                  <div style={{color:"var(--muted)", fontSize:13}}>
                    {d.nationality} • {d.dob}
                  </div>
                </div>
              ))}
            </div>
          </>
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