import { useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { AgentIcon } from './AgentIcon.js';

const storageKey = 'autoagent.agent-directory-width';

function savedWidth() {
  try {
    const value = Number(localStorage.getItem(storageKey));
    if (Number.isFinite(value) && value >= 240 && value <= 480) return value;
  } catch { /* Resizing also works when storage is unavailable. */ }
  return 300;
}

export function AgentDirectory({ active, open, narrow, onClose, children }: { active: boolean; open: boolean; narrow: boolean; onClose: () => void; children: ReactNode }) {
  const nav = useRef<HTMLElement>(null);
  const drag = useRef<{ pointer: number; x: number; width: number } | null>(null);
  const [preferred, setPreferred] = useState(savedWidth);
  const [available, setAvailable] = useState(() => window.innerWidth);
  const [resizing, setResizing] = useState(false);
  const maximum = Math.max(1, narrow ? Math.min(360, available - 48) : Math.min(480, available - 280));
  const minimum = Math.min(240, maximum);
  const width = Math.max(minimum, Math.min(maximum, preferred));
  const clamp = (value: number) => Math.max(minimum, Math.min(maximum, value));
  const remember = (value: number) => {
    try { localStorage.setItem(storageKey, String(value)); } catch { /* Optional preference. */ }
  };

  useLayoutEffect(() => {
    const parent = nav.current?.parentElement;
    if (!parent) return;
    const measure = () => { if (parent.clientWidth > 0) setAvailable(parent.clientWidth); };
    measure();
    const observer = new ResizeObserver(measure); observer.observe(parent);
    return () => observer.disconnect();
  }, []);
  useEffect(() => {
    if (!active || !open || narrow) { drag.current = null; setResizing(false); }
  }, [active, open, narrow]);
  useEffect(() => {
    if (!resizing) return;
    document.body.classList.add('agent-directory-resizing');
    return () => document.body.classList.remove('agent-directory-resizing');
  }, [resizing]);

  useLayoutEffect(() => {
    const element = nav.current;
    if (!active || !open || !narrow || !element) return;
    const focusable = () => [...element.querySelectorAll<HTMLElement>('button:not(:disabled),a[href],input:not(:disabled),select:not(:disabled),textarea:not(:disabled),[tabindex="0"]')]
      .filter(item => item.getClientRects().length && !item.closest('[inert]'));
    const frame = requestAnimationFrame(() => focusable()[0]?.focus({ preventScroll: true }));
    const keydown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || document.querySelector('dialog[open]')) return;
      if (event.key === 'Escape') { event.preventDefault(); onClose(); }
      if (event.key !== 'Tab') return;
      const items = focusable(), first = items[0], last = items.at(-1);
      if (event.shiftKey && (document.activeElement === first || !element.contains(document.activeElement))) {
        event.preventDefault(); last?.focus();
      } else if (!event.shiftKey && (document.activeElement === last || !element.contains(document.activeElement))) {
        event.preventDefault(); first?.focus();
      }
    };
    document.addEventListener('keydown', keydown);
    return () => {
      cancelAnimationFrame(frame);
      document.removeEventListener('keydown', keydown);
      if (!document.querySelector('dialog[open]')) document.getElementById('agent-sidebar-toggle')?.focus({ preventScroll: true });
    };
  }, [active, open, narrow, onClose]);

  return <nav ref={nav} id="agent-conversations" className={`directory agent-directory ${open ? 'is-open' : 'is-collapsed'} ${narrow ? 'is-overlay' : ''} ${resizing ? 'is-resizing' : ''}`}
    style={{ '--directory-expanded-width': `${width}px` } as CSSProperties} aria-label="Conversations"
    role={narrow ? 'dialog' : undefined} aria-modal={narrow && open ? true : undefined} aria-hidden={!open} inert={!open}>
    <div className="agent-directory-body">
      {narrow ? <div className="agent-directory-mobile-header"><span>Conversations</span><button type="button" aria-label="Close conversations" onClick={onClose}><AgentIcon name="close" /></button></div> : null}
      {children}
    </div>
    {!narrow ?
    <div className="agent-directory-resize" role="separator" aria-label="Resize conversations sidebar" aria-orientation="vertical"
      aria-controls="agent-conversations" aria-valuemin={minimum} aria-valuemax={maximum} aria-valuenow={width} tabIndex={active && open ? 0 : -1}
      onPointerDown={event => {
        if (event.button !== 0) return;
        event.preventDefault(); event.currentTarget.setPointerCapture(event.pointerId);
        drag.current = { pointer: event.pointerId, x: event.clientX, width }; setResizing(true);
      }} onPointerMove={event => {
        const start = drag.current;
        if (start?.pointer === event.pointerId) setPreferred(clamp(start.width + event.clientX - start.x));
      }} onPointerUp={event => {
        const start = drag.current;
        if (start?.pointer !== event.pointerId) return;
        const next = clamp(start.width + event.clientX - start.x);
        setPreferred(next); remember(next); drag.current = null; setResizing(false);
        event.currentTarget.releasePointerCapture(event.pointerId);
      }} onLostPointerCapture={() => { drag.current = null; setResizing(false); }}
      onPointerCancel={() => { drag.current = null; setResizing(false); }}
      onDoubleClick={() => { setPreferred(300); remember(300); }}
      onKeyDown={event => {
        const next = event.key === 'ArrowLeft' ? width - (event.shiftKey ? 40 : 10)
          : event.key === 'ArrowRight' ? width + (event.shiftKey ? 40 : 10)
            : event.key === 'Home' ? minimum : event.key === 'End' ? maximum : null;
        if (next === null) return;
        event.preventDefault(); const value = clamp(next); setPreferred(value); remember(value);
      }} /> : null}
  </nav>;
}
