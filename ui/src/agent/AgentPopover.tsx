import { useEffect, useId, useLayoutEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { AgentSheet } from './AgentSheet.js';

/** A non-modal, viewport-contained panel shared by conversation controls. */
export function AgentPopover({ sheet, ...props }: Parameters<typeof AnchoredPopover>[0] & { sheet?: boolean }) {
  return sheet ? <AgentSheet {...props} /> : <AnchoredPopover {...props} />;
}

function AnchoredPopover({ anchor, label, children, onClose, className = '' }: {
  anchor: RefObject<HTMLButtonElement | null>; label: string; children: ReactNode; onClose: () => void; className?: string;
}) {
  const panel = useRef<HTMLDivElement>(null), close = useRef<HTMLButtonElement>(null), title = useId();
  const [position, setPosition] = useState({ left: 0, top: 0, maxHeight: 400, side: 'top', ready: false });
  useLayoutEffect(() => {
    const element = panel.current, trigger = anchor.current;
    if (!element || !trigger) return;
    const place = () => {
      const bounds = trigger.getBoundingClientRect(), height = element.getBoundingClientRect().height;
      const above = bounds.top - 16, below = innerHeight - bounds.bottom - 16;
      const top = above >= height || above >= below;
      const maxHeight = Math.max(80, top ? above : below);
      setPosition({ left: Math.max(12, Math.min(bounds.left, innerWidth - element.offsetWidth - 12)),
        top: top ? Math.max(12, bounds.top - Math.min(height, maxHeight) - 8) : bounds.bottom + 8,
        maxHeight, side: top ? 'top' : 'bottom', ready: true });
    };
    place();
    const resize = new ResizeObserver(place); resize.observe(element); resize.observe(trigger);
    window.addEventListener('resize', place); window.addEventListener('scroll', place, true);
    return () => { resize.disconnect(); window.removeEventListener('resize', place); window.removeEventListener('scroll', place, true); };
  }, [anchor]);
  useLayoutEffect(() => { if (position.ready) close.current?.focus({ preventScroll: true }); }, [position.ready]);
  useEffect(() => {
    const dismiss = (event: Event) => { const target = event.target; if (target instanceof Node && !panel.current?.contains(target) && !anchor.current?.contains(target)) onClose(); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); onClose(); anchor.current?.focus({ preventScroll: true }); } };
    document.addEventListener('pointerdown', dismiss); document.addEventListener('focusin', dismiss); document.addEventListener('keydown', escape);
    return () => { document.removeEventListener('pointerdown', dismiss); document.removeEventListener('focusin', dismiss); document.removeEventListener('keydown', escape); };
  }, [anchor, onClose]);
  return createPortal(<section ref={panel} role="dialog" aria-labelledby={title} className={`agent-popover ${className}`} data-side={position.side}
    style={{ left: position.left, top: position.top, maxHeight: position.maxHeight, visibility: position.ready ? 'visible' : 'hidden' }}>
    <div className="agent-popover-heading"><strong id={title}>{label}</strong><button ref={close} type="button" aria-label={`Close ${label.toLowerCase()}`} onClick={() => { onClose(); anchor.current?.focus({ preventScroll: true }); }}>×</button></div>
    {children}
  </section>, document.body);
}
