export function Select({ options, ...props }){
  return (
    <select {...props} style={{
      width:"100%",
      padding:"10px 12px",
      borderRadius:12,
      border:"1px solid var(--line)",
      background:"rgba(255,255,255,0.03)",
      color:"var(--text)",
      outline:"none"
    }}>
      {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  );
}