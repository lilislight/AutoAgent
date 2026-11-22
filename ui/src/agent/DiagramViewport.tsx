import { useCallback, useEffect, useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import { RefreshIcon, ToolButton } from '../components/ToolButton.js';

interface DiagramImage { url: string; width?: number; height?: number; alt: string }

/** Zoom the rendered image without running Mermaid again or resizing the chat. */
export function DiagramViewport({ image, actions, onError }: { image: DiagramImage; actions?: ReactNode; onError?: () => void }) {
  const viewport = useRef<HTMLDivElement>(null), picture = useRef<HTMLImageElement>(null);
  const [bounds, setBounds] = useState({ width: 0, height: 640 }), [scale, setScale] = useState<number | null>(null);
  const [intrinsic, setIntrinsic] = useState({ width: 300, height: 150 });
  const anchor = useRef<{ x: number; y: number; horizontal: number; vertical: number } | null>(null);
  const drag = useRef<{ pointer: number; x: number; y: number; left: number; top: number } | null>(null);
  const [dragging, setDragging] = useState(false);
  const width = image.width ?? intrinsic.width, height = image.height ?? intrinsic.height;
  const fit = Math.min(1, bounds.width > 24 ? (bounds.width - 24) / width : 1, (bounds.height - 14) / height);
  const minimum = Math.min(.1, fit), zoom = scale ?? fit;
  const imageWidth = width * zoom, imageHeight = height * zoom;

  useLayoutEffect(() => {
    const area = viewport.current;
    if (!area) return;
    const measure = () => {
      const maximum = parseFloat(getComputedStyle(area).maxHeight);
      const next = { width: area.clientWidth, height: Number.isFinite(maximum) ? maximum : Math.min(innerHeight * .65, 640) };
      setBounds(current => current.width === next.width && current.height === next.height ? current : next);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(area);
    window.addEventListener('resize', measure);
    return () => { observer.disconnect(); window.removeEventListener('resize', measure); };
  }, []);

  const zoomBy = useCallback((factor: number, point?: { x: number; y: number }) => {
    const area = viewport.current, img = picture.current;
    if (!area || !img) return;
    const frame = area.getBoundingClientRect(), content = img.getBoundingClientRect();
    const x = point?.x ?? frame.left + area.clientWidth / 2;
    const y = point?.y ?? frame.top + area.clientHeight / 2;
    anchor.current = { x, y, horizontal: (x - content.left) / content.width, vertical: (y - content.top) / content.height };
    setScale(current => Math.min(4, Math.max(minimum, (current ?? fit) * factor)));
  }, [fit, minimum]);

  useLayoutEffect(() => {
    const point = anchor.current, area = viewport.current, img = picture.current;
    anchor.current = null;
    if (!point || !area || !img) return;
    const content = img.getBoundingClientRect();
    area.scrollLeft += content.left + point.horizontal * content.width - point.x;
    area.scrollTop += content.top + point.vertical * content.height - point.y;
  }, [imageWidth, imageHeight]);

  useEffect(() => {
    const area = viewport.current;
    if (!area) return;
    const wheel = (event: WheelEvent) => {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      zoomBy(Math.exp(-Math.max(-100, Math.min(100, event.deltaY)) * .005), { x: event.clientX, y: event.clientY });
    };
    area.addEventListener('wheel', wheel, { passive: false });
    return () => area.removeEventListener('wheel', wheel);
  }, [zoomBy]);

  const reset = () => {
    anchor.current = null; setScale(null);
    viewport.current?.scrollTo(0, 0);
  };
  const endDrag = () => { drag.current = null; setDragging(false); };
  return <>
    <div className="agent-code-actions">
      <div className="agent-diagram-zoom" role="group" aria-label="Diagram zoom">
        <ToolButton className="agent-diagram-control" label="Zoom out" disabled={zoom <= minimum} onClick={() => zoomBy(1 / 1.25)}>
          <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true"><path d="M4 10h12" /></svg>
        </ToolButton>
        <output className="agent-diagram-scale" aria-label="Diagram zoom level" aria-live="polite">{Math.round(zoom * 100)}%</output>
        <ToolButton className="agent-diagram-control" label="Zoom in" disabled={zoom >= 4} onClick={() => zoomBy(1.25)}>
          <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true"><path d="M4 10h12M10 4v12" /></svg>
        </ToolButton>
        <ToolButton className="agent-diagram-control" label="Fit diagram" onClick={reset}><RefreshIcon /></ToolButton>
      </div>
      {actions}
    </div>
    <div ref={viewport} className={`agent-mermaid-scroll${dragging ? ' is-panning' : ''}`} role="region" aria-label="Mermaid diagram" tabIndex={0}
      style={{ height: Math.max(160, height * fit + 14) }}
      onKeyDown={event => {
        if (event.ctrlKey || event.metaKey || event.altKey) return;
        if (event.key === '+' || event.key === '=') { event.preventDefault(); zoomBy(1.25); }
        else if (event.key === '-') { event.preventDefault(); zoomBy(1 / 1.25); }
        else if (event.key === '0') { event.preventDefault(); reset(); }
      }}
      onPointerDown={event => {
        if (event.pointerType === 'touch' || event.button !== 0) return;
        const area = event.currentTarget;
        if (area.scrollWidth <= area.clientWidth && area.scrollHeight <= area.clientHeight) return;
        event.preventDefault(); area.focus({ preventScroll: true });
        drag.current = { pointer: event.pointerId, x: event.clientX, y: event.clientY, left: area.scrollLeft, top: area.scrollTop };
        area.setPointerCapture(event.pointerId); setDragging(true);
      }}
      onPointerMove={event => {
        const current = drag.current;
        if (!current || current.pointer !== event.pointerId) return;
        event.currentTarget.scrollLeft = current.left - (event.clientX - current.x);
        event.currentTarget.scrollTop = current.top - (event.clientY - current.y);
      }}
      onPointerUp={event => { if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId); endDrag(); }}
      onPointerCancel={endDrag} onLostPointerCapture={endDrag}>
      <div className="agent-mermaid-canvas" style={{ width: imageWidth, height: imageHeight }}>
        <img ref={picture} src={image.url} width={imageWidth} height={imageHeight} alt={image.alt} draggable={false} onError={onError}
          onLoad={event => { if (!image.width || !image.height) setIntrinsic({ width: event.currentTarget.naturalWidth || 300, height: event.currentTarget.naturalHeight || 150 }); }} />
      </div>
    </div>
  </>;
}
