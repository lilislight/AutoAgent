import { useState, type ReactNode } from "react";

export function WorkflowIcon() {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect x="3" y="3" width="8" height="8" rx="2" /><path d="M7 11v6a2 2 0 0 0 2 2h4" /><rect x="13" y="13" width="8" height="8" rx="2" /></svg>;
}

export function SessionIcon() {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M8 17H5l-3 3V5a2 2 0 0 1 2-2h13a2 2 0 0 1 2 2v3M9 8h11a2 2 0 0 1 2 2v12l-3-3H9a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2Z" /></svg>;
}

export function SearchIcon() {
  return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><circle cx="8.5" cy="8.5" r="5.5" /><path d="m12.5 12.5 4 4" /></svg>;
}


export function Sidebar({ view, onChange, children }: { view: "workflows" | "executions" | "agents"; onChange: (view: "workflows" | "executions" | "agents") => void; children: ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);
  const label = view;
  return <><nav className="activity-bar" aria-label="Main navigation">
    <button className={`activity-button ${view === "workflows" ? "active" : ""}`} aria-label="View workflows" aria-current={view === "workflows" ? "page" : undefined} title="Workflows" onClick={() => { setCollapsed(false); onChange("workflows"); }}><WorkflowIcon /></button>
    <button className={`activity-button ${view === "executions" ? "active" : ""}`} aria-label="View executions" aria-current={view === "executions" ? "page" : undefined} title="Executions" onClick={() => { setCollapsed(false); onChange("executions"); }}><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><path d="M3 11a9 9 0 1 1 2.6 7M3 4v7h7m2-5v6l4 2" /></svg></button>
    <button className="directory-toggle" onClick={() => setCollapsed(current => !current)} aria-label={`${collapsed ? "Expand" : "Collapse"} ${label} sidebar`} title={`${collapsed ? "Expand" : "Collapse"} ${label} sidebar`} aria-expanded={!collapsed} aria-controls={`directory-panel-${view}`}><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M9 4v16" /><path d="m16 9-3 3 3 3" className={`collapse-chevron ${collapsed ? "collapsed" : ""}`} /></svg></button>
  </nav><nav id={`directory-panel-${view}`} className={`directory ${collapsed ? "collapsed" : ""}`} aria-label={`${label} directory`} aria-hidden={collapsed} inert={collapsed}>{children}</nav></>;
}
