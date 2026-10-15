import { useEffect, useRef, useState } from 'react';
import { constrainInspectorWidth, DEFAULT_INSPECTOR_WIDTH, INSPECTOR_WIDTH_KEY } from '../inspectorWidth';

export function InspectorResizeHandle() {
  const [width, setWidth] = useState(DEFAULT_INSPECTOR_WIDTH);
  const handle = useRef<HTMLDivElement>(null);
  const dragging = useRef<{ pointer: number; x: number; width: number } | null>(null);
  const preferred = useRef(DEFAULT_INSPECTOR_WIDTH);
  const available = () => Math.max(1, (handle.current?.closest('.inspector-presence')?.parentElement?.clientWidth ?? window.innerWidth) - 32);
  const change = (value: number) => {
    preferred.current = value;
    const next = constrainInspectorWidth(value, available());
    document.documentElement.style.setProperty('--inspector-width', `${next}px`);
    setWidth(next);
  };
  const persist = () => {
    try { localStorage.setItem(INSPECTOR_WIDTH_KEY, String(preferred.current)); } catch { /* Storage can be disabled. */ }
  };
  const stop = () => {
    if (!dragging.current) return;
    dragging.current = null;
    document.body.classList.remove('is-resizing-inspector');
    persist();
  };
  useEffect(() => {
    let initial = DEFAULT_INSPECTOR_WIDTH;
    try { const saved = Number(localStorage.getItem(INSPECTOR_WIDTH_KEY)); if (saved > 0) initial = saved; } catch { /* Use the default. */ }
    change(initial);
    const parent = handle.current?.closest('.inspector-presence')?.parentElement;
    const observer = new ResizeObserver(() => change(preferred.current));
    if (parent) observer.observe(parent);
    return () => { observer.disconnect(); document.body.classList.remove('is-resizing-inspector'); };
  }, []);
  return <div ref={handle} className="inspector-resize-handle" role="separator" tabIndex={0} aria-label="Resize Inspector" aria-orientation="vertical" aria-valuemin={Math.min(320, available())} aria-valuemax={Math.min(900, available())} aria-valuenow={width} aria-valuetext={`${width} pixels`} title="Drag to resize · Double-click to reset"
    onPointerDown={event => {
      if (event.button !== 0) return;
      dragging.current = { pointer: event.pointerId, x: event.clientX, width: handle.current?.closest('.inspector-presence')?.getBoundingClientRect().width ?? width };
      event.currentTarget.setPointerCapture(event.pointerId);
      document.body.classList.add('is-resizing-inspector');
      event.preventDefault();
    }}
    onPointerMove={event => { const start = dragging.current; if (start?.pointer === event.pointerId) change(start.width + start.x - event.clientX); }}
    onPointerUp={event => { stop(); if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId); }}
    onPointerCancel={stop} onLostPointerCapture={stop}
    onDoubleClick={() => { change(DEFAULT_INSPECTOR_WIDTH); persist(); }}
    onKeyDown={event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault();
      change(event.key === 'Home' ? 320 : event.key === 'End' ? available() : width + (event.key === 'ArrowLeft' ? 40 : -40));
      persist();
    }}><span /></div>;
}
