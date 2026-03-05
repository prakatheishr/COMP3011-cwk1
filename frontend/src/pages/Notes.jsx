import { useEffect, useState } from "react";
import { api } from "../api/client.js";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { TextArea } from "../components/TextArea.jsx";
import { Select } from "../components/Select.jsx";
import { Button } from "../components/Button.jsx";

export default function Notes(){
  const [entityType, setEntityType] = useState("");
  const [entityId, setEntityId] = useState("");
  const [notes, setNotes] = useState(null);
  const [err, setErr] = useState("");

  const [newType, setNewType] = useState("race");
  const [newEntityId, setNewEntityId] = useState("");
  const [newTitle, setNewTitle] = useState("");
  const [newContent, setNewContent] = useState("");

  const [selected, setSelected] = useState(null);
  const [editTitle, setEditTitle] = useState("");
  const [editContent, setEditContent] = useState("");

  async function load(){
    setErr(""); setNotes(null);
    try{
      const res = await api.notesList({
        entityType: entityType || undefined,
        entityId: entityId || undefined,
      });
      setNotes(res);
    }catch(e){ setErr(String(e.message||e)); }
  }

  useEffect(() => { load(); }, []);

  async function create(){
    setErr("");
    try{
      const res = await api.noteCreate({
        entity_type: newType,
        entity_id: Number(newEntityId),
        title: newTitle,
        content: newContent
      });
      setNewTitle(""); setNewContent("");
      await load();
      alert(`Created note ${res.id}`);
    }catch(e){ setErr(String(e.message||e)); }
  }

  async function del(id){
    setErr("");
    try{
      await api.noteDelete(id);
      setSelected(null);
      await load();
    }catch(e){ setErr(String(e.message||e)); }
  }

  async function save(){
    if (!selected) return;
    setErr("");
    try{
      const res = await api.noteUpdate(selected.id, { title: editTitle, content: editContent });
      setSelected(res);
      await load();
    }catch(e){ setErr(String(e.message||e)); }
  }

  async function tldr(generator){
    if (!selected) return;
    setErr("");
    try{
      const res = await api.noteTldr(selected.id, generator);
      setSelected(res.note);
      await load();
    }catch(e){ setErr(String(e.message||e)); }
  }

  return (
    <div className="grid">
      <Card title="Notes">
        <div style={{display:"grid", gap:10}}>
          <div style={{display:"flex", gap:8}}>
            <div style={{flex:1}}>
              <Input placeholder="filter entity_type (race/driver/season)" value={entityType} onChange={e=>setEntityType(e.target.value)} />
            </div>
            <div style={{width:160}}>
              <Input placeholder="filter entity_id" value={entityId} onChange={e=>setEntityId(e.target.value)} />
            </div>
            <Button onClick={load}>Refresh</Button>
          </div>

          {err && <div style={{color:"salmon"}}>{err}</div>}
          {!notes && !err && <div>Loading…</div>}

          {notes && (
            <div style={{display:"grid", gap:10}}>
              {notes.results.map(n => (
                <div key={n.id} style={{padding:12, border:"1px solid var(--line)", borderRadius:14, background:"rgba(255,255,255,0.02)"}}>
                  <div style={{display:"flex", justifyContent:"space-between", gap:10}}>
                    <div style={{fontWeight:800}} onClick={() => { setSelected(n); setEditTitle(n.title); setEditContent(n.content); }} role="button">
                      #{n.id} • {n.title}
                    </div>
                    <Button variant="secondary" onClick={() => del(n.id)}>Delete</Button>
                  </div>
                  <div style={{color:"var(--muted)", fontSize:12}}>
                    {n.entity_type}:{n.entity_id} • updated {n.updated_at}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </Card>

      <Card title="Create / Edit">
        <div style={{display:"grid", gap:12}}>
          <div style={{display:"grid", gap:8}}>
            <div style={{fontWeight:800}}>Create note</div>
            <Select value={newType} onChange={e=>setNewType(e.target.value)}
              options={[
                {value:"race", label:"race"},
                {value:"driver", label:"driver"},
                {value:"season", label:"season"},
              ]}
            />
            <Input placeholder="entity_id (raceId/driverId/year)" value={newEntityId} onChange={e=>setNewEntityId(e.target.value)} />
            <Input placeholder="title" value={newTitle} onChange={e=>setNewTitle(e.target.value)} />
            <TextArea placeholder="content" value={newContent} onChange={e=>setNewContent(e.target.value)} />
            <Button onClick={create} disabled={!newEntityId || !newTitle || !newContent}>Create</Button>
          </div>

          <hr style={{borderColor:"var(--line)", width:"100%"}}/>

          <div style={{display:"grid", gap:8}}>
            <div style={{fontWeight:800}}>Edit selected</div>
            {!selected && <div style={{color:"var(--muted)"}}>Click a note on the left to edit it.</div>}
            {selected && (
              <>
                <Input value={editTitle} onChange={e=>setEditTitle(e.target.value)} />
                <TextArea value={editContent} onChange={e=>setEditContent(e.target.value)} />
                <div style={{display:"flex", gap:8}}>
                  <Button onClick={save}>Save</Button>
                  <Button variant="secondary" onClick={()=>tldr("template")}>TL;DR (template)</Button>
                  <Button variant="secondary" onClick={()=>tldr("llm")}>TL;DR (llm)</Button>
                </div>
                <div style={{color:"var(--muted)", fontSize:12}}>
                  ai_summary: {selected.ai_summary ? selected.ai_summary : "(none)"} 
                </div>
              </>
            )}
          </div>
        </div>
      </Card>
    </div>
  );
}