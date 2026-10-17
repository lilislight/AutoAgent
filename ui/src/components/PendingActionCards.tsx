import { useEffect, useRef, useState, type CSSProperties } from 'react';
import type { PendingResponse } from '../types';

type Item = { wait: PendingResponse; leaving: boolean };
const identity = (wait: PendingResponse) => `${wait.invocation_id}:${wait.wait_id}`;

export function PendingActionCards({ waits, invocationId, selected, onPick }: {
  waits: PendingResponse[]; invocationId: string; selected?: string;
  onPick: (wait: PendingResponse) => void;
}) {
  const [items, setItems] = useState<Item[]>(() => waits.map(wait => ({ wait, leaving: false })));
  const scroll = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const element = scroll.current;
    if (!element) return;
    const wheel = (event: WheelEvent) => event.stopPropagation();
    element.addEventListener('wheel', wheel, { passive: true });
    return () => element.removeEventListener('wheel', wheel);
  }, [items.length > 0]);
  useEffect(() => {
    setItems(previous => {
      const incoming = new Map(waits.map(wait => [identity(wait), wait]));
      const next = previous.map(item => {
        const wait = incoming.get(identity(item.wait));
        incoming.delete(identity(item.wait));
        return wait ? item.wait === wait && !item.leaving ? item : { wait, leaving: false } : item.leaving ? item : { ...item, leaving: true };
      });
      next.push(...[...incoming.values()].map(wait => ({ wait, leaving: false })));
      return next.length === previous.length && next.every((item, index) => item === previous[index]) ? previous : next;
    });
  }, [waits]);
  useEffect(() => {
    const leaving = new Set(items.filter(item => item.leaving).map(item => identity(item.wait)));
    if (!leaving.size) return;
    const timer = setTimeout(() => setItems(current => current.filter(item => !item.leaving || !leaving.has(identity(item.wait)))), window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 210);
    return () => clearTimeout(timer);
  }, [items]);
  if (!items.length) return null;
  return <div className="graph-pending-actions" aria-label="Pending actions">
    <div className="pending-actions-heading"><span>Awaiting response</span><span>{waits.length || items.length}</span></div>
    <div ref={scroll} className="pending-actions-scroll" role="list" aria-label="Pending responses" tabIndex={0}>
    {items.map(({ wait, leaving }, index) => <div key={identity(wait)} role="listitem" className={`pending-action-entry ${leaving ? 'is-leaving' : ''}`} aria-hidden={leaving || undefined} inert={leaving} style={{ '--pending-enter-delay': `${Math.min(index, 7) * 18}ms` } as CSSProperties}>
      <div className="pending-action-clip"><button type="button" className={`pending-action-card ${selected === wait.wait_id ? 'is-selected' : ''}`} aria-pressed={selected === wait.wait_id} disabled={leaving} onClick={() => onPick(wait)} title={`Open ${wait.node_id ?? wait.occurrence_id} · Wait ${wait.wait_id}`}>
        <svg className="pending-action-symbol" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><path d="M4 3.5h10a1.5 1.5 0 0 1 1.5 1.5v6a1.5 1.5 0 0 1-1.5 1.5H8l-4 3v-3A1.5 1.5 0 0 1 2.5 11V5A1.5 1.5 0 0 1 4 3.5Z" /><path d="M6 7h6M6 9.5h4" /></svg>
        <span><strong>{wait.node_id ?? wait.occurrence_id}</strong><small>Wait {wait.wait_id.slice(0, 8)}{wait.invocation_id !== invocationId ? ` · Run ${wait.invocation_id.slice(0, 8)}` : ''}</small></span>
      </button></div>
    </div>)}</div>
  </div>;
}
