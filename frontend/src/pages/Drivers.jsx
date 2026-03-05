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

const [q, setQ] = useState("");
const filtered = useMemo(() => {
  const s = q.trim().toLowerCase();
  if (!s) return drivers;
  return drivers.filter((d) => {
    const full = `${d.forename} ${d.surname}`.toLowerCase();
    return (
      full.includes(s) ||
      d.surname.toLowerCase().includes(s) ||
      d.nationality.toLowerCase().includes(s)
    );
  });
}, [q, drivers]);

return (
  <div>
    <div className="flex items-center gap-3 mb-4">
      <input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Search drivers (e.g., Hamilton, Verstappen, British)…"
        className="w-full rounded-xl bg-black/30 border border-white/10 px-4 py-2 outline-none"
      />
      <button
        onClick={() => setQ("")}
        className="rounded-xl px-4 py-2 bg-white/10 hover:bg-white/15"
      >
        Clear
      </button>
    </div>

    <div className="text-sm opacity-70 mb-2">
      Showing {filtered.length} of {drivers.length}
    </div>

    {/* render filtered instead of drivers */}
    {filtered.map(/* ... */)}
  </div>
);