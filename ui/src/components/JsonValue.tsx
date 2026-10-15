import { useEffect, useRef, useState } from "react";

function Scalar({ value }: { value: unknown }) {
  const [expanded, setExpanded] = useState(false);
  const text = JSON.stringify(value) ?? "undefined";
  const long = text.length > 500;
  return <><span className={`json-scalar ${value === null ? "null" : typeof value}`}>{long && !expanded ? text.slice(0, 500) + "…" : text}</span>{long ? <button type="button" className="json-expand" onClick={() => setExpanded(current => !current)}>{expanded ? "Show less" : "Show full value"}</button> : null}</>;
}
function TreeNode({ value, name, depth }: { value: unknown; name: string; depth: number }) {
  const [open, setOpen] = useState(depth < 2);
  const [limit, setLimit] = useState(50);
  if (value === null || typeof value !== "object") return <div className="json-tree-row" style={{ paddingLeft: depth * 12 }}><span className="json-key">{name}</span><Scalar value={value} /></div>;
  const keys = Object.keys(value);
  const array = Array.isArray(value);
  return <details className="json-tree-node" open={open} onToggle={event => { if (event.target === event.currentTarget) setOpen(event.currentTarget.open); }}><summary style={{ paddingLeft: depth * 12 }}><span className="json-key">{name}</span><span className="json-summary">{array ? "Array" : "Object"}({keys.length})</span></summary>
    {open ? keys.length ? <>{keys.slice(0, limit).map(key => <TreeNode key={key} name={key} value={(value as Record<string, unknown>)[key]} depth={depth + 1} />)}{keys.length > limit ? <button type="button" className="json-expand" style={{ marginLeft: (depth + 1) * 12 }} onClick={() => setLimit(current => current + 50)}>Show {Math.min(50, keys.length - limit)} more fields</button> : null}</> : <div className="json-tree-row" style={{ paddingLeft: (depth + 1) * 12 }}><span className="json-summary">empty</span></div> : null}
  </details>;
}
export function JsonValue({ value, label = "root" }: { value: unknown; label?: string }) {
  const [copy, setCopy] = useState("idle");
  const timeout = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timeout.current), []);
  const copyJson = async () => {
    try { await navigator.clipboard.writeText(JSON.stringify(value, null, 2) ?? "null"); setCopy("copied"); }
    catch { setCopy("failed"); }
    clearTimeout(timeout.current); timeout.current = setTimeout(() => setCopy("idle"), 1400);
  };
  const download = () => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }));
    const link = document.createElement("a"); link.href = url; link.download = `${label}.json`; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  return <div className="json-tree-shell"><div className="json-actions"><button type="button" className="json-copy-button" title={copy === "copied" ? "Copied" : copy === "failed" ? "Copy failed" : "Copy JSON"} aria-label={copy === "copied" ? "Copied JSON" : copy === "failed" ? "Copy failed" : "Copy JSON"} onClick={() => void copyJson()}>{copy === "copied" ? "✓" : <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.3" aria-hidden="true"><rect x="6" y="5" width="7" height="8" rx="1"/><path d="M9 5V3H3v7h3"/></svg>}</button><button type="button" className="json-copy-button" title="Download JSON" aria-label="Download JSON" onClick={download}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.3" aria-hidden="true"><path d="M8 2v8m-3-3 3 3 3-3M3 11v3h10v-3"/></svg></button></div><div className="json-tree"><TreeNode value={value} name={label} depth={0} /></div></div>;
}
