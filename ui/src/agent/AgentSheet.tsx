import { useId, useLayoutEffect, useRef, type ReactNode, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { AgentIcon } from './AgentIcon.js';

export function AgentSheet({ anchor, label, onClose, children, className = '' }: {
  anchor: RefObject<HTMLButtonElement | null>; label: string; onClose: () => void;
  children: ReactNode; className?: string;
}) {
  const dialog = useRef<HTMLDialogElement>(null), close = useRef<HTMLButtonElement>(null), id = useId();
  useLayoutEffect(() => {
    const element = dialog.current;
    if (!element) return;
    const trigger = anchor.current;
    if (document.activeElement instanceof HTMLElement && document.activeElement !== trigger) document.activeElement.blur();
    element.showModal();
    close.current?.focus({ preventScroll: true });
    return () => {
      element.close();
      trigger?.focus({ preventScroll: true });
    };
  }, [anchor]);

  return createPortal(<dialog ref={dialog} className={`agent-sheet ${className}`} aria-labelledby={id}
    onCancel={event => { event.preventDefault(); onClose(); }}
    onClick={event => { if (event.target === event.currentTarget) onClose(); }}>
    <section className="agent-sheet-content">
      <div className="agent-sheet-heading"><h2 id={id}>{label}</h2><button ref={close} type="button"
        aria-label={`Close ${label.toLowerCase()}`} onClick={onClose}><AgentIcon name="close" /></button></div>
      {children}
    </section>
  </dialog>, document.body);
}
