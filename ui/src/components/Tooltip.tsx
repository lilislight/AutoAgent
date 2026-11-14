import { Children, cloneElement, createContext, useCallback, useContext, useEffect, useId, useLayoutEffect, useRef, useState, type HTMLAttributes, type ReactElement, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { placeTooltip } from '../tooltipPlacement.js';

type Tip = { id: string; anchor: HTMLElement; content: ReactNode; placement?: 'side' | 'top' };
const Context = createContext<{
  current: string | null;
  show: (tip: Tip, immediate: boolean) => void;
  hide: (id?: string) => void;
} | null>(null);

/** One tooltip at a time, shared by pointer and keyboard users. */
export function TooltipProvider({ children }: { children: ReactNode }) {
  const [tip, setTip] = useState<Tip | null>(null);
  const [position, setPosition] = useState({ left: 0, top: 0, side: 'right', maxWidth: 420, id: '' });
  const pending = useRef<Tip | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const panel = useRef<HTMLDivElement>(null);
  const show = useCallback((next: Tip, immediate: boolean) => {
    clearTimeout(timer.current);
    pending.current = next;
    const publish = () => { if (next.anchor.isConnected && !next.anchor.closest('[inert]')) setTip(next); };
    if (immediate) publish(); else timer.current = setTimeout(publish, 80);
  }, []);
  const hide = useCallback((id?: string) => {
    if (id && pending.current?.id !== id) return;
    clearTimeout(timer.current);
    pending.current = null; setTip(null);
  }, []);
  useEffect(() => () => clearTimeout(timer.current), []);
  useLayoutEffect(() => {
    if (!tip || !panel.current) return;
    const element = panel.current;
    // Native popovers also stay visible above a modal dialog's top layer.
    if (typeof element.showPopover === 'function') element.showPopover();
    const update = () => {
      if (!tip.anchor.isConnected || tip.anchor.closest('[inert]')) { hide(); return; }
      const anchor = tip.anchor.getBoundingClientRect(), box = element.getBoundingClientRect();
      setPosition({ ...placeTooltip(anchor, box, { width: innerWidth, height: innerHeight }, tip.placement), id: tip.id });
    };
    update();
    const observer = new ResizeObserver(update); observer.observe(element); observer.observe(tip.anchor);
    return () => { observer.disconnect(); if (typeof element.hidePopover === 'function' && element.matches(':popover-open')) element.hidePopover(); };
  }, [tip, hide]);
  useEffect(() => {
    const close = () => hide();
    const key = (event: KeyboardEvent) => { if (event.key === 'Escape') { if (panel.current) { event.preventDefault(); event.stopPropagation(); } close(); } };
    const pointer = (event: PointerEvent) => { if (!panel.current?.contains(event.target as Node)) close(); };
    const scroll = (event: Event) => { if (!panel.current?.contains(event.target as Node)) close(); };
    // Cancel an imminent hint as well as a visible one during fast operations.
    window.addEventListener('blur', close); window.addEventListener('resize', close); window.addEventListener('scroll', scroll, true);
    document.addEventListener('keydown', key, true); document.addEventListener('pointerdown', pointer);
    return () => { window.removeEventListener('blur', close); window.removeEventListener('resize', close); window.removeEventListener('scroll', scroll, true); document.removeEventListener('keydown', key, true); document.removeEventListener('pointerdown', pointer); };
  }, [hide]);
  return <Context.Provider value={{ current: tip?.id ?? null, show, hide }}>{children}{tip ? createPortal(
    <div ref={panel} id={tip.id} role="tooltip" popover={typeof HTMLElement.prototype.showPopover === 'function' ? 'manual' : undefined} className="ui-tooltip" data-side={position.side} style={{ left: position.left, top: position.top, maxWidth: position.id === tip.id ? position.maxWidth : 420, visibility: position.id === tip.id ? 'visible' : 'hidden' }}>{tip.content}</div>, document.body,
  ) : null}</Context.Provider>;
}

export function Tooltip({ children, content, disabled = false, placement }: { children: ReactElement<HTMLAttributes<HTMLElement>>; content: ReactNode; disabled?: boolean; placement?: 'side' | 'top' }) {
  const context = useContext(Context), id = useId();
  const child = Children.only(children), props = child.props;
  useEffect(() => () => context?.hide(id), [context?.hide, id]);
  useEffect(() => { if (disabled) context?.hide(id); }, [disabled, context?.hide, id]);
  if (!context) return child;
  const show = (anchor: HTMLElement, immediate: boolean) => { if (!disabled && content) context.show({ id, anchor, content, placement }, immediate); };
  return cloneElement(child, {
    title: undefined,
    'aria-describedby': [props['aria-describedby'], context.current === id ? id : null].filter(Boolean).join(' ') || undefined,
    onPointerEnter: event => { props.onPointerEnter?.(event); if (event.pointerType !== 'touch') show(event.currentTarget, false); },
    onPointerLeave: event => { props.onPointerLeave?.(event); context.hide(id); },
    onFocus: event => { props.onFocus?.(event); show(event.currentTarget, true); },
    onBlur: event => { props.onBlur?.(event); context.hide(id); },
    onClick: event => { context.hide(id); props.onClick?.(event); },
  });
}

export function ValueTip({ label, value }: { label: string; value: string }) {
  return <span className="value-tip"><strong>{label}</strong><code>{value}</code></span>;
}
