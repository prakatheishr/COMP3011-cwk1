export function TextArea(props){
  return (
    <textarea {...props} style={{
      width:"100%",
      minHeight:120,
      padding:"10px 12px",
      borderRadius:12,
      border:"1px solid var(--line)",
      background:"rgba(255,255,255,0.03)",
      color:"var(--text)",
      outline:"none",
      resize:"vertical"
    }}/>
  );
}