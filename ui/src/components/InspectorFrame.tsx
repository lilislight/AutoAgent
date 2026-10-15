import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { useExecutionNavigation } from './ExecutionNavigation';
import { InspectorResizeHandle } from './InspectorResizeHandle';
import type { InspectorTab } from '../types';
export type { InspectorTab } from '../types';
const rememberedTabs = new Map<string, InspectorTab>();
export function InspectorPresence({ open, hidden = false, children }: { open: boolean; hidden?: boolean; children: ReactNode }) {
  return open ? <div className={`inspector-presence ${hidden ? 'is-hidden' : ''}`} aria-hidden={hidden || undefined} inert={hidden}>{children}</div> : null;
}
export function CopyId({ value }: { value: string | undefined }) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  return <button type="button" className="copy-id" title={value ? `Copy ${value}` : undefined} disabled={!value} onClick={async () => { try { await navigator.clipboard.writeText(value!); setCopied(true); clearTimeout(timer.current); timer.current = setTimeout(() => setCopied(false), 1400); } catch { setCopied(false); } }}><code>{copied ? "Copied" : value ?? "—"}</code></button>;
}
export function InspectorFrame({ kind, title, context, tabs, initial, onClose, onBack, backLabel = 'Node occurrence', footer, children }: { kind: string; title: string; context: ReactNode; tabs: InspectorTab[]; initial?: InspectorTab; onClose: () => void; onBack?: () => void; backLabel?: string; footer?: ReactNode; children: (tab: InspectorTab) => ReactNode }) {
  const navigation = useExecutionNavigation();
  const { active } = navigation;
  const exact = navigation.exact && navigation.inspectorKind === kind;
  const [selected, setSelected] = useState<InspectorTab>(() => initial ?? rememberedTabs.get(kind) ?? tabs[0]);
  const startingTab = useRef(selected);
  const tab = tabs.includes(selected) ? selected : tabs[0];
  useEffect(() => { setSelected(initial ?? startingTab.current); }, [initial]);
  const close = useRef<HTMLButtonElement>(null);
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const id = useId();
  useEffect(() => { if (!active) return; const previous = document.activeElement; close.current?.focus({ preventScroll: true }); return () => { if (previous instanceof HTMLElement && previous.isConnected && !previous.closest('[inert]')) previous.focus({ preventScroll: true }); }; }, [active]);
  const pick = (value: InspectorTab) => { setSelected(value); rememberedTabs.set(kind, value); navigation.setSection?.(kind, value); };
  const kindLabel = kind.startsWith('stage:') ? 'EXECUTION STAGE' : ({ 'node-execution': 'NODE OCCURRENCE', 'edge-execution': 'EDGE EVALUATION', call: 'OPERATOR CALL', 'system-command': 'SYSTEM COMMAND', stage: 'EXECUTION STAGE', wait: 'DURABLE WAIT' } as Record<string, string>)[kind] ?? kind.toUpperCase();
  if (!active) return null;
  return <aside className="inspector" role="dialog" aria-modal="false" aria-labelledby={`${id}-heading`}><InspectorResizeHandle /><div className="inspector-heading"><div>{onBack ? <button className="inspector-back" onClick={onBack}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><path d="m7 3-5 5 5 5M2 8h12" /></svg>{backLabel}</button> : null}<span className="eyebrow">{kindLabel}</span><h2 id={`${id}-heading`} title={title}>{title}</h2><div className="inspector-context">{context}</div></div><button ref={close} className="icon-button" aria-label="Close inspector" onClick={onClose}>×</button></div>
    <div className="inspector-tabs" role="tablist" aria-label="Inspector sections">{tabs.map((value, index) => <button key={value} ref={element => { tabRefs.current[index] = element; }} id={`${id}-tab-${index}`} role="tab" aria-selected={tab === value} aria-controls={`${id}-panel`} tabIndex={tab === value ? 0 : -1} className={tab === value ? "active" : ""} onClick={() => pick(value)} onKeyDown={event => { let next: number | undefined; if (event.key === 'ArrowRight') next = (index + 1) % tabs.length; if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length; if (event.key === 'Home') next = 0; if (event.key === 'End') next = tabs.length - 1; if (next !== undefined) { event.preventDefault(); pick(tabs[next]); tabRefs.current[next]?.focus(); } }}>{value}</button>)}</div>
    <div key={tab} className="inspector-content" id={`${id}-panel`} role="tabpanel" aria-labelledby={`${id}-tab-${tabs.indexOf(tab)}`}>{navigation.replaySequence !== undefined ? <p className="inspector-replay-note">Saved record details · Graph is replaying Event #{navigation.replaySequence}.</p> : null}{children(tab)}</div>
    {footer || exact ? <div className="inspector-footer">{footer}{exact ? <InspectorLinks tab={tab} /> : null}</div> : null}</aside>;
}

function InspectorLinks({ tab }: { tab: InspectorTab }) {
  const navigation = useExecutionNavigation();
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  return <div className="inspector-view-links"><button onClick={navigation.showGraph}>Show in Graph</button><button disabled={!navigation.showTimeline} onClick={navigation.showTimeline}>Show in Timeline</button><button onClick={async () => {
    try { const url = new URL(window.location.href); url.searchParams.set('section', tab); await navigator.clipboard.writeText(url.href); setCopied(true); clearTimeout(timer.current); timer.current = setTimeout(() => setCopied(false), 1400); } catch { setCopied(false); }
  }}>{copied ? 'Copied' : 'Copy link'}</button></div>;
}
