export function Button({ variant="primary", ...props }){
  const base = {
    padding:"10px 12px",
    borderRadius:12,
    border:"1px solid var(--line)",
    cursor:"pointer",
    fontWeight:700,
  };
  const styles = variant === "primary"
    ? { ...base, background:"var(--red)", color:"white", borderColor:"rgba(225,6,0,0.6)" }
    : { ...base, background:"rgba(255,255,255,0.03)", color:"var(--text)" };

  return <button {...props} style={styles} />;
}