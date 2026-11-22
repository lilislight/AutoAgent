import { useEffect, useId, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { landmarkAt, type ConversationLandmark } from './conversationLandmarks.js';

function revealMark(mark?: HTMLElement | null) {
  const list = mark?.parentElement;
  if (!mark || !list) return;
  const box = mark.getBoundingClientRect(), viewport = list.getBoundingClientRect();
  if (box.top < viewport.top) list.scrollTop -= viewport.top - box.top;
  else if (box.bottom > viewport.bottom) list.scrollTop += box.bottom - viewport.bottom;
}

function TurnPreview({ item, anchor, id }: { item: ConversationLandmark; anchor: HTMLButtonElement; id: string }) {
  const panel = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: 0, top: 0, ready: false });
  useLayoutEffect(() => {
    const element = panel.current;
    if (!element) return;
    const place = () => {
      const button = anchor.getBoundingClientRect(), box = element.getBoundingClientRect();
      setPosition({ left: Math.max(12, Math.min(button.right + 10, innerWidth - box.width - 12)),
        top: Math.max(12, Math.min(button.top + button.height / 2 - box.height / 2, innerHeight - box.height - 12)), ready: true });
    };
    place();
    const observer = new ResizeObserver(place); observer.observe(element); observer.observe(anchor);
    window.addEventListener('resize', place);
    window.addEventListener('scroll', place, true);
    return () => { observer.disconnect(); window.removeEventListener('resize', place); window.removeEventListener('scroll', place, true); };
  }, [anchor]);
  return createPortal(<div ref={panel} id={id} role="tooltip" className="agent-turn-preview" style={{ left: position.left, top: position.top, visibility: position.ready ? 'visible' : 'hidden' }}>
    <strong>{item.question}</strong><p>{item.answer}</p>
  </div>, document.body);
}

export function ConversationNavigator({ items, rendered = items, area, content, active, onJump }: {
  items: ConversationLandmark[]; area: RefObject<HTMLDivElement | null>; content: RefObject<HTMLDivElement | null>;
  rendered?: ConversationLandmark[]; active: boolean; onJump: (item: ConversationLandmark) => void;
}) {
  const nav = useRef<HTMLElement>(null), tooltip = useId();
  const [selected, setSelected] = useState<string | null>(null);
  const [preview, setPreview] = useState<{ id: string; anchor: HTMLButtonElement } | null>(null);
  const stamp = items.map(item => item.id).join('\n');
  const renderedStamp = rendered.map(item => item.id).join('\n');
  const renderedIds = new Set(rendered.map(item => item.id));
  useEffect(() => {
    const root = area.current, body = content.current;
    if (!root || !body || !active || !renderedStamp) return;
    let frame = 0, positions: number[] = [], ids: string[] = [];
    const update = () => {
      frame = 0;
      const index = root.scrollHeight - root.scrollTop - root.clientHeight <= 8 ? ids.length - 1
        : landmarkAt(positions, root.scrollTop + Math.min(96, root.clientHeight * .2));
      setSelected(ids[index] ?? null);
    };
    const measure = () => {
      const nodes = [...body.querySelectorAll<HTMLElement>('[data-conversation-message]')];
      const top = root.getBoundingClientRect().top;
      ids = nodes.map(node => node.dataset.conversationMessage!);
      positions = nodes.map(node => root.scrollTop + node.getBoundingClientRect().top - top);
      update();
    };
    const scroll = () => { if (!frame) frame = requestAnimationFrame(update); };
    measure();
    const observer = new ResizeObserver(measure); observer.observe(body); observer.observe(root);
    root.addEventListener('scroll', scroll, { passive: true });
    return () => { observer.disconnect(); root.removeEventListener('scroll', scroll); cancelAnimationFrame(frame); };
  }, [renderedStamp, area, content, active]);
  useEffect(() => {
    const list = nav.current?.querySelector<HTMLElement>('.agent-turn-marks');
    if (!list) return;
    const reveal = () => revealMark(list.querySelector<HTMLElement>('[aria-current=location]'));
    reveal();
    const observer = new ResizeObserver(reveal); observer.observe(list);
    return () => observer.disconnect();
  }, [selected, stamp]);
  const current = preview && items.find(item => item.id === preview.id);
  const focus = (index: number) => {
    const buttons = nav.current?.querySelectorAll<HTMLButtonElement>('.agent-turn-mark');
    const target = buttons?.[index];
    target?.focus({ preventScroll: true }); revealMark(target);
  };
  if (items.length < 2) return null;
  return <nav ref={nav} className="agent-turn-navigator" aria-label="Conversation turns" onPointerLeave={() => setPreview(null)} onKeyDown={event => {
    if (event.key === 'Escape') { setPreview(null); event.stopPropagation(); }
    const index = Number((event.target as HTMLElement).closest<HTMLButtonElement>('[data-turn-index]')?.dataset.turnIndex);
    if (!Number.isFinite(index)) return;
    const next = event.key === 'ArrowDown' ? Math.min(items.length - 1, index + 1) : event.key === 'ArrowUp' ? Math.max(0, index - 1) : event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 : null;
    if (next !== null) { event.preventDefault(); focus(next); }
  }}>
    <div className="agent-turn-marks">{items.map((item, index) => <button key={item.id} type="button" className={`agent-turn-mark ${item.id === selected ? 'is-current' : ''}`} data-turn-index={index}
      aria-label={`Go to question ${index + 1}: ${item.question}`} aria-current={item.id === selected ? 'location' : undefined} aria-controls={renderedIds.has(item.id) ? item.anchor : undefined} aria-describedby={preview?.id === item.id ? tooltip : undefined}
      onPointerEnter={event => { if (event.pointerType !== 'touch') setPreview({ id: item.id, anchor: event.currentTarget }); }}
      onFocus={event => setPreview({ id: item.id, anchor: event.currentTarget })} onBlur={() => setPreview(null)}
      onClick={() => { setPreview(null); setSelected(item.id); onJump(item); }}><span /></button>)}</div>
    {current && preview ? <TurnPreview item={current} anchor={preview.anchor} id={tooltip} /> : null}
  </nav>;
}
