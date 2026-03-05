import { useState } from "react";
import { api } from "../api/client.js";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { Select } from "../components/Select.jsx";
import { Button } from "../components/Button.jsx";

export default function Insights(){
  const [raceId, setRaceId] = useState("");
  const [year, setYear] = useState("");
  const [compareTo, setCompareTo] = useState("");
  const [mode, setMode] = useState("recap");
  const [format, setFormat] = useState("radio");
  const [generator, setGenerator] = useState("template");
  const [out, setOut] = useState(null);
  const [err, setErr] = useState("");

  async function runRace(){
    setErr(""); setOut(null);
    try{
      const res = await api.raceInsights({ raceId, mode, format, generator });
      setOut(res);
    }catch(e){ setErr(String(e.message||e)); }
  }

  async function runSeason(){
    setErr(""); setOut(null);
    try{
      const res = await api.seasonInsights({
        year,
        compareTo: compareTo || undefined,
        mode,
        format,
        generator
      });
      setOut(res);
    }catch(e){ setErr(String(e.message||e)); }
  }

  async function saveRaceAsNote(){
    setErr("");
    try{
      const res = await api.saveRaceInsightAsNote(raceId, { mode, format, generator });
      alert(`Saved note id: ${res?.saved_note?.id ?? "ok"}`);
    }catch(e){ setErr(String(e.message||e)); }
  }

  async function saveSeasonAsNote(){
    setErr("");
    try{
      const payload = { mode, format, generator };
      if (compareTo) payload.compare_to = Number(compareTo);
      const res = await api.saveSeasonInsightAsNote(year, payload);
      alert(`Saved note id: ${res?.saved_note?.id ?? "ok"}`);
    }catch(e){ setErr(String(e.message||e)); }
  }

  return (
    <div className="grid">
      <Card title="Insight Controls">
        <div style={{display:"grid", gap:10}}>
          <div>
            <div style={{color:"var(--muted)", fontSize:13, marginBottom:6}}>mode</div>
            <Select value={mode} onChange={e=>setMode(e.target.value)}
              options={[
                {value:"recap", label:"recap (what happened)"},
                {value:"impact", label:"impact (why it mattered)"},
              ]}
            />
          </div>

          <div>
            <div style={{color:"var(--muted)", fontSize:13, marginBottom:6}}>format</div>
            <Select value={format} onChange={e=>setFormat(e.target.value)}
              options={[
                {value:"plain", label:"plain"},
                {value:"radio", label:"radio"},
              ]}
            />
          </div>

          <div>
            <div style={{color:"var(--muted)", fontSize:13, marginBottom:6}}>generator</div>
            <Select value={generator} onChange={e=>setGenerator(e.target.value)}
              options={[
                {value:"template", label:"template"},
                {value:"llm", label:"llm (ollama)"},
              ]}
            />
          </div>

          <hr style={{borderColor:"var(--line)", width:"100%"}}/>

          <div style={{display:"grid", gap:8}}>
            <div style={{display:"flex", gap:8}}>
              <div style={{flex:1}}><Input placeholder="raceId" value={raceId} onChange={e=>setRaceId(e.target.value)} /></div>
              <Button onClick={runRace} disabled={!raceId}>Race insight</Button>
            </div>
            <div style={{display:"flex", gap:8}}>
              <div style={{flex:1}}><Input placeholder="season year" value={year} onChange={e=>setYear(e.target.value)} /></div>
              <div style={{width:140}}><Input placeholder="compare_to (optional)" value={compareTo} onChange={e=>setCompareTo(e.target.value)} /></div>
              <Button onClick={runSeason} disabled={!year}>Season insight</Button>
            </div>

            <div style={{display:"flex", gap:8}}>
              <Button variant="secondary" onClick={saveRaceAsNote} disabled={!raceId}>Save race as note</Button>
              <Button variant="secondary" onClick={saveSeasonAsNote} disabled={!year}>Save season as note</Button>
            </div>

            <div style={{color:"var(--muted)", fontSize:12}}>
              “Save as note” works only if you implemented `POST /races/{raceId}/notes` and `POST /seasons/{year}/notes`.
            </div>
          </div>
        </div>

        {err && <div style={{color:"salmon", marginTop:12}}>{err}</div>}
      </Card>

      <Card title="Output">
        {!out && !err && <div style={{color:"var(--muted)"}}>Run an insight to view output.</div>}
        {out && (
          <>
            <div style={{fontWeight:800, marginBottom:10}}>Insight</div>
            <div style={{padding:12, border:"1px solid var(--line)", borderRadius:14, background:"rgba(255,255,255,0.02)", marginBottom:12}}>
              {out.insight}
            </div>
            <div style={{color:"var(--muted)", fontSize:12, marginBottom:8}}>facts (debug-friendly)</div>
            <pre style={{margin:0, whiteSpace:"pre-wrap"}}>{JSON.stringify(out.facts, null, 2)}</pre>
          </>
        )}
      </Card>
    </div>
  );
}