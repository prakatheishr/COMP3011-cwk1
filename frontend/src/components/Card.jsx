export function Card({ title, children, right }) {
  return (
    <div style={{
      background:"var(--panel)",
      border:"1px solid var(--line)",
      borderRadius:"var(--radius)",
      boxShadow:"var(--shadow)",
      padding:16,
      position:"relative",
      overflow:"hidden"
    }}>
      <div style={{display:"flex", alignItems:"center", justifyContent:"space-between", gap:10, marginBottom:12}}>
        <div style={{fontWeight:800}}>{title}</div>
        {right}
      </div>
      {children}
    </div>
  );
}