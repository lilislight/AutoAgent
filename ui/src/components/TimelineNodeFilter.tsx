import { useEffect, useId, useRef, useState } from 'react';

export function TimelineNodeFilter({ nodes, value, onChange }: { nodes: string[]; value: string | null; onChange: (value: string | null) => void }) {
  const [open, setOpen] = useState(false);
  const container = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const menuId = useId();
  useEffect(() => {
    if (!open) return;
    const close = (event: PointerEvent) => { if (!container.current?.contains(event.target as Node)) setOpen(false); };
    document.addEventListener('pointerdown', close);
    return () => document.removeEventListener('pointerdown', close);
  }, [open]);
  useEffect(() => { if (open) container.current?.querySelector<HTMLElement>('[aria-checked="true"]')?.focus(); }, [open]);
  return <div className="timeline-node-filter" ref={container} onKeyDown={event => {
    if (event.key === 'Escape') { event.preventDefault(); setOpen(false); trigger.current?.focus(); }
    if (!open || !['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const items = [...container.current!.querySelectorAll<HTMLButtonElement>('[role="menuitemradio"]')];
    const current = items.indexOf(document.activeElement as HTMLButtonElement);
    const index = event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 : (current + (event.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
    items[index]?.focus();
  }} onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false); }}>
    <button ref={trigger} className="timeline-filter-trigger" aria-label="Filter Node runs" aria-haspopup="menu" aria-expanded={open} aria-controls={open ? menuId : undefined} title={value ?? 'All nodes'} onClick={() => setOpen(current => !current)} onKeyDown={event => { if (!open && ['ArrowDown', 'ArrowUp'].includes(event.key)) { event.preventDefault(); setOpen(true); } }}><span>{value ?? 'All nodes'}</span><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg></button>
    {open ? <div className="timeline-filter-menu" id={menuId} role="menu" aria-label="Filter Node runs">{[null, ...nodes].map(node => <button key={node ?? ''} role="menuitemradio" tabIndex={-1} aria-checked={node === value} onClick={() => { onChange(node); setOpen(false); trigger.current?.focus(); }}><span>{node ?? 'All nodes'}</span>{node === value ? <span aria-hidden="true">✓</span> : null}</button>)}</div> : null}
  </div>;
}
