import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from 'react';

/** Keep a closing branch briefly so its height can animate without retaining it. */
export function TimelineReveal({ open, children }: { open: boolean; children: ReactNode }) {
  const [retained, setRetained] = useState(open);
  const [height, setHeight] = useState(0);
  const content = useRef<HTMLDivElement>(null);
  const mounted = open || retained;
  useEffect(() => {
    if (open) { setRetained(true); return; }
    const delay = window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 250;
    const timer = setTimeout(() => setRetained(false), delay);
    return () => clearTimeout(timer);
  }, [open]);
  useLayoutEffect(() => {
    const element = content.current;
    if (!element || !mounted) { setHeight(0); return; }
    const measure = () => setHeight(element.getBoundingClientRect().height);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [mounted]);
  return <div className={`timeline-reveal ${open ? 'is-open' : ''}`} style={{ height: open ? height : 0 }}>
    <div ref={content} className="timeline-reveal-content" inert={!open} aria-hidden={!open}>{mounted ? children : null}</div>
  </div>;
}
